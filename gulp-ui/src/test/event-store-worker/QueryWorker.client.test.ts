import type {
	DocumentStreamStateChange,
	QuerySocketCloseDetails,
	QuerySocketErrorDetails,
	QuerySocketMessage,
	QuerySocketState,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryWorkerCommand,
	QueryWorkerEvent,
} from "@/event-stream";
import type { QueryTransportCallbacks } from "@/event-store-worker/QueryTransport.types";
import { QueryWorkerClient } from "@/event-store-worker/QueryWorker.client";
import type { Source } from "@/entities/Source";

class FakeQueryWorker {
	public onmessage: ((event: MessageEvent<QueryWorkerEvent>) => void) | null =
		null;
	public onerror: ((event: ErrorEvent) => void) | null = null;
	public readonly commands: QueryWorkerCommand[] = [];
	public terminateCalls = 0;

	/**
	 * Records one main-to-worker command.
	 * @param command Typed QueryWorker command.
	 */
	public postMessage(command: QueryWorkerCommand): void {
		this.commands.push(command);
	}

	/** Records permanent worker termination. */
	public terminate(): void {
		this.terminateCalls += 1;
	}

	/**
	 * Delivers one worker-to-client event.
	 * @param event Typed QueryWorker event.
	 */
	public emitWorkerEvent(event: QueryWorkerEvent): void {
		this.onmessage?.(
			new MessageEvent<QueryWorkerEvent>("message", { data: event }),
		);
	}
}

describe("QueryWorkerClient", () => {
	it("correlates controls, readiness, and compact persistence events", async () => {
		const socketStates: QuerySocketState[] = [];
		const socketCloses: QuerySocketCloseDetails[] = [];
		const socketErrors: QuerySocketErrorDetails[] = [];
		const forwarded: QuerySocketMessage[] = [];
		const streamChanges: DocumentStreamStateChange[] = [];
		const sourceSummaries: QuerySourceSummaryEvent[] = [];
		const streamFailures: QueryStreamFailureEvent[] = [];
		const protocolErrors: QuerySocketErrorDetails[] = [];
		let socketOpenCount = 0;
		const callbacks: QueryTransportCallbacks = {
			onSocketState: (state) => socketStates.push(state),
			onSocketOpen: () => {
				socketOpenCount += 1;
			},
			onSocketClose: (details) => socketCloses.push(details),
			onSocketError: (details) => socketErrors.push(details),
			onForwardMessage: (message) => forwarded.push(message),
			onStreamStateChange: (change) => streamChanges.push(change),
			onSourceSummary: (event) => sourceSummaries.push(event),
			onStreamFailure: (event) => streamFailures.push(event),
			onProtocolError: (details) => protocolErrors.push(details),
		};
		const worker = new FakeQueryWorker();
		const client = new QueryWorkerClient(
			worker,
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "worker-client",
				coordinatorId: "test-coordinator",
			},
			callbacks,
		);
		const initializeCommand = worker.commands[0];
		if (initializeCommand.type !== "INITIALIZE") {
			throw new Error("Expected INITIALIZE command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: initializeCommand.controlId,
		});

		const ready = client.awaitReady(100);
		worker.emitWorkerEvent({ type: "SOCKET_STATE", state: "ready" });
		await ready;
		expect(socketStates).toEqual(["ready"]);

		const registration = client.registerDocumentStream({
			requestId: "worker-request",
			operationId: "operation-one",
			operationEpoch: 7,
			owner: "persist",
		});
		const registerCommand = worker.commands.find(
			(command) => command.type === "REGISTER_STREAM",
		);
		if (!registerCommand || registerCommand.type !== "REGISTER_STREAM") {
			throw new Error("Expected REGISTER_STREAM command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: registerCommand.controlId,
		});
		await registration;

		const message: QuerySocketMessage = {
			type: "stats_update",
			req_id: "worker-request",
		};
		worker.emitWorkerEvent({ type: "FORWARD_MESSAGE", message });
		expect(forwarded).toEqual([message]);
		const sourceSummary: QuerySourceSummaryEvent = {
			requestId: "worker-request",
			operationId: "operation-one",
			operationEpoch: 7,
			summary: {
				sourceId: "source-one" as Source.Id,
				revision: 2,
				status: "complete",
				eventCount: 1,
				minTimestamp: 10,
				maxTimestamp: 10,
				hashMin: 4,
				hashMax: 4,
			},
		};
		worker.emitWorkerEvent({ type: "SOURCE_SUMMARY", event: sourceSummary });
		const streamFailure: QueryStreamFailureEvent = {
			requestId: "another-request",
			operationId: "operation-one",
			operationEpoch: 7,
			reason: "quota",
			message: "capacity exhausted",
		};
		worker.emitWorkerEvent({ type: "STREAM_FAILURE", event: streamFailure });
		const renderWindow = client.requestRenderWindow({
			operationId: "operation-one",
			operationEpoch: 7,
			viewportRevision: 11,
			projectionRevision: 13,
			frame: { min: 0, max: 1_000 },
			width: 100,
			scale: 1,
			scrollX: 0,
			horizontalOverscanPixels: 10,
			sources: [],
		});
		const renderCommand = worker.commands.find(
			(command) => command.type === "REQUEST_RENDER_WINDOW",
		);
		if (!renderCommand || renderCommand.type !== "REQUEST_RENDER_WINDOW") {
			throw new Error("Expected REQUEST_RENDER_WINDOW command.");
		}
		const projection = {
			operationEpoch: 7,
			viewportRevision: 11,
			projectionRevision: 13,
			sources: [],
			evictedSourceIds: [],
		};
		worker.emitWorkerEvent({
			type: "RENDER_WINDOW_RESULT",
			projectionRequestId: renderCommand.projectionRequestId,
			projection,
		});
		expect(await renderWindow).toEqual(projection);

		const completion = client.completeDocumentStream("worker-request");
		const completeCommand = worker.commands.find(
			(command) => command.type === "COMPLETE_STREAM",
		);
		if (!completeCommand || completeCommand.type !== "COMPLETE_STREAM") {
			throw new Error("Expected COMPLETE_STREAM command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: completeCommand.controlId,
		});
		await completion;

		const operationRelease = client.releaseOperation("operation-one");
		const releaseCommand = worker.commands.find(
			(command) => command.type === "RELEASE_OPERATION",
		);
		if (!releaseCommand || releaseCommand.type !== "RELEASE_OPERATION") {
			throw new Error("Expected RELEASE_OPERATION command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: releaseCommand.controlId,
		});
		await operationRelease;

		client.destroy();
		expect(worker.terminateCalls).toBe(1);
		expect(socketOpenCount).toBe(0);
		expect(socketCloses).toHaveLength(0);
		expect(socketErrors).toHaveLength(0);
		expect(sourceSummaries).toEqual([sourceSummary]);
		expect(streamFailures).toEqual([streamFailure]);
		expect(streamChanges).toHaveLength(0);
		expect(protocolErrors).toHaveLength(0);
	});

	it("surfaces a correlated initialization failure and terminates the worker", async () => {
		const protocolErrors: QuerySocketErrorDetails[] = [];
		const worker = new FakeQueryWorker();
		new QueryWorkerClient(
			worker,
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "worker-client-failure",
				coordinatorId: "test-coordinator",
			},
			{
				onSocketState: () => undefined,
				onSocketOpen: () => undefined,
				onSocketClose: () => undefined,
				onSocketError: () => undefined,
				onForwardMessage: () => undefined,
				onStreamStateChange: () => undefined,
				onSourceSummary: () => undefined,
				onStreamFailure: () => undefined,
				onProtocolError: (details) => protocolErrors.push(details),
			},
		);
		const initializeCommand = worker.commands[0];
		if (initializeCommand.type !== "INITIALIZE") {
			throw new Error("Expected INITIALIZE command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: initializeCommand.controlId,
			error: "IndexedDB initialization failed",
		});
		await Promise.resolve();

		expect(worker.terminateCalls).toBe(1);
		expect(protocolErrors).toEqual([
			{ message: "IndexedDB initialization failed" },
		]);
	});
});
