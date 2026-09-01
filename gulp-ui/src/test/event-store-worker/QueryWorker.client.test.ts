import type {
	DocumentStreamStateChange,
	QuerySocketCloseDetails,
	QuerySocketErrorDetails,
	QuerySocketMessage,
	QuerySocketState,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryWorkerCommand,
	QueryWorkerDiagnostics,
	QueryWorkerEvent,
} from "@/event-stream";
import type { QueryTransportCallbacks } from "@/event-store-worker/QueryTransport.types";
import { QueryWorkerClient } from "@/event-store-worker/QueryWorker.client";
import type { Source } from "@/entities/Source";
import type {
	EventSourceQueryCacheReconciliation,
	EventSourceSummary,
} from "@/event-store/EventStore.types";

const SOURCE_ONE = "source-one" as Source.Id;

/** Creates one complete source authority summary for worker protocol tests. */
function createSourceSummary(): EventSourceSummary {
	return {
		sourceId: SOURCE_ONE,
		generation: 1,
		revision: 2,
		authorityRevision: 2,
		status: "complete",
		eventCount: 1,
		minTimestamp: 10,
		maxTimestamp: 10,
		hashMin: 4,
		hashMax: 4,
		activeCacheGeneration: 1,
		cacheHistory: [1],
		cacheHistoryCursor: 0,
		nextGeneration: 2,
	};
}

/** Creates one page-free startup cache result for correlation tests. */
function createCacheReconciliation(): EventSourceQueryCacheReconciliation {
	return {
		match: "exact",
		summary: createSourceSummary(),
		cache: {
			sourceId: SOURCE_ONE,
			generation: 1,
			provenance: {
				query: { string: "", filters: [] },
				fingerprint: "source-query-v1:test",
				normalization: {
					field: "gulp.event_code",
					hashFunction: "fnv1a",
					requestedFields: [],
				},
				serverSnapshot: {
					total: 1,
					minNanoseconds: "10",
					maxNanoseconds: "10",
					timeUpdated: null,
					millisecondPrecision: false,
				},
			},
			state: "exact",
			createdAt: 1,
			lastActivatedAt: 2,
			eventCount: 1,
			minTimestamp: 10,
			maxTimestamp: 10,
		},
	};
}

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
				cacheNamespaceId: "test-user",
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
			summary: createSourceSummary(),
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

		const diagnostics: QueryWorkerDiagnostics = {
			capturedAtMs: 100,
			activeStreams: 1,
			loadedOperations: 1,
			rawBacklog: {
				currentBytes: 0,
				maximumBytes: 2_000,
				maximumStreamBytes: 2_000,
			},
			normalizedBacklog: {
				currentDocuments: 0,
				maximumDocuments: 20_000,
			},
			commits: {
				active: 0,
				maximumActive: 2,
				completed: 3,
				averageLatencyMs: 4,
				maximumLatencyMs: 6,
			},
			pages: {
				created: 2,
				documents: 30_000,
				capacity: 32_768,
				fillRatio: 30_000 / 32_768,
			},
			projections: {
				started: 1,
				completed: 1,
				superseded: 0,
				retries: 0,
			},
		};
		const diagnosticsRequest = client.getQueryDiagnostics();
		const diagnosticsCommand = worker.commands.find(
			(command) => command.type === "GET_QUERY_DIAGNOSTICS",
		);
		if (
			!diagnosticsCommand ||
			diagnosticsCommand.type !== "GET_QUERY_DIAGNOSTICS"
		) {
			throw new Error("Expected GET_QUERY_DIAGNOSTICS command.");
		}
		worker.emitWorkerEvent({
			type: "QUERY_DIAGNOSTICS_RESULT",
			diagnosticsRequestId: diagnosticsCommand.diagnosticsRequestId,
			diagnostics,
		});
		expect(await diagnosticsRequest).toEqual(diagnostics);

		const diagnosticsReset = client.resetQueryDiagnostics();
		const resetCommand = worker.commands.find(
			(command) => command.type === "RESET_QUERY_DIAGNOSTICS",
		);
		if (!resetCommand || resetCommand.type !== "RESET_QUERY_DIAGNOSTICS") {
			throw new Error("Expected RESET_QUERY_DIAGNOSTICS command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: resetCommand.controlId,
		});
		await diagnosticsReset;

		const finalization = { queryCachesBySourceId: {} };
		const completion = client.completeDocumentStream(
			"worker-request",
			finalization,
		);
		const completeCommand = worker.commands.find(
			(command) => command.type === "COMPLETE_STREAM",
		);
		if (!completeCommand || completeCommand.type !== "COMPLETE_STREAM") {
			throw new Error("Expected COMPLETE_STREAM command.");
		}
		expect(completeCommand.finalization).toEqual(finalization);
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

	it("correlates startup reconciliation, history navigation, and cache clearing", async () => {
		const worker = new FakeQueryWorker();
		const client = new QueryWorkerClient(
			worker,
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "worker-cache-client",
				cacheNamespaceId: "test-user",
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
				onProtocolError: () => undefined,
			},
		);
		const initialization = worker.commands[0];
		if (initialization.type !== "INITIALIZE") {
			throw new Error("Expected INITIALIZE command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: initialization.controlId,
		});

		const reconciliation = createCacheReconciliation();
		const reconciliationPromise = client.reconcileSourceQueryCache({
			operationId: "operation-one",
			sourceId: SOURCE_ONE,
			desired: {
				fingerprint: "source-query-v1:test",
				shapeFingerprint: "source-query-shape-v1:test",
				range: { min: "0", max: "100" },
				serverSnapshot: reconciliation.cache.provenance.serverSnapshot,
			},
		});
		const reconciliationCommand = worker.commands.find(
			(command) => command.type === "RECONCILE_SOURCE_QUERY_CACHE",
		);
		if (
			!reconciliationCommand ||
			reconciliationCommand.type !== "RECONCILE_SOURCE_QUERY_CACHE"
		) {
			throw new Error("Expected RECONCILE_SOURCE_QUERY_CACHE command.");
		}
		expect(reconciliationCommand).toMatchObject({
			operationId: "operation-one",
			sourceId: SOURCE_ONE,
			desired: {
				fingerprint: "source-query-v1:test",
				shapeFingerprint: "source-query-shape-v1:test",
			},
		});
		worker.emitWorkerEvent({
			type: "SOURCE_QUERY_CACHE_RESULT",
			cacheRequestId: reconciliationCommand.cacheRequestId,
			result: reconciliation,
		});
		expect(await reconciliationPromise).toEqual(reconciliation);

		const navigationPromise = client.navigateSourceQueryCache({
			operationId: "operation-one",
			sourceId: SOURCE_ONE,
			direction: "undo",
		});
		const navigationCommand = worker.commands.find(
			(command) => command.type === "NAVIGATE_SOURCE_QUERY_CACHE",
		);
		if (
			!navigationCommand ||
			navigationCommand.type !== "NAVIGATE_SOURCE_QUERY_CACHE"
		) {
			throw new Error("Expected NAVIGATE_SOURCE_QUERY_CACHE command.");
		}
		worker.emitWorkerEvent({
			type: "SOURCE_QUERY_CACHE_RESULT",
			cacheRequestId: navigationCommand.cacheRequestId,
			result: null,
		});
		expect(await navigationPromise).toBeNull();

		const clearPromise = client.clearSourceQueryCache(
			"operation-one",
			SOURCE_ONE,
		);
		const clearCommand = worker.commands.find(
			(command) => command.type === "CLEAR_SOURCE_QUERY_CACHE",
		);
		if (!clearCommand || clearCommand.type !== "CLEAR_SOURCE_QUERY_CACHE") {
			throw new Error("Expected CLEAR_SOURCE_QUERY_CACHE command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: clearCommand.controlId,
		});
		await clearPromise;

		const deletePromise = client.deleteSourceEventStore(
			"operation-one",
			SOURCE_ONE,
		);
		const deleteCommand = worker.commands.find(
			(command) => command.type === "DELETE_SOURCE_EVENT_STORE",
		);
		if (!deleteCommand || deleteCommand.type !== "DELETE_SOURCE_EVENT_STORE") {
			throw new Error("Expected DELETE_SOURCE_EVENT_STORE command.");
		}
		worker.emitWorkerEvent({
			type: "CONTROL_RESULT",
			controlId: deleteCommand.controlId,
		});
		await deletePromise;

		client.destroy();
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
				cacheNamespaceId: "test-user",
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
