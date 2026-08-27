import { EventStoreRepository } from "@/event-store/EventStore.repository";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import type { EventStoreViewportDescriptor } from "@/event-store/EventStore.viewport-projector";
import { QuerySocketSession } from "@/event-store-worker/QuerySocket.session";
import type { QueryTransportCallbacks } from "@/event-store-worker/QueryTransport.types";
import type {
	DocumentStreamDescriptor,
	DocumentStreamStateChange,
	QueryRenderWindowDescriptor,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type { Source } from "@/entities/Source";
import { TextDecoder, TextEncoder } from "util";

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

interface DeferredProjection {
	descriptor: EventStoreViewportDescriptor;
	resolve: (projection: RenderWindowProjection) => void;
}

/** Creates one empty projection carrying the supplied request revisions. */
function createProjection(
	descriptor: EventStoreViewportDescriptor,
): RenderWindowProjection {
	return {
		operationEpoch: descriptor.operationEpoch,
		viewportRevision: descriptor.viewportRevision,
		projectionRevision: descriptor.projectionRevision,
		sources: [],
		evictedSourceIds: [],
	};
}

/** Creates one operation-qualified viewport request. */
function createDescriptor(projectionRevision: number): QueryRenderWindowDescriptor {
	return {
		operationId: "operation-one",
		operationEpoch: 2,
		viewportRevision: projectionRevision,
		projectionRevision,
		frame: { min: 0, max: 1_000 },
		width: 100,
		scale: 1,
		scrollX: 0,
		horizontalOverscanPixels: 10,
		sources: [],
	};
}

/** Waits for one asynchronous scheduler condition. */
async function waitFor(predicate: () => boolean): Promise<void> {
	const deadline = Date.now() + 1_000;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("Timed out waiting for scheduler.");
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
}

const callbacks: QueryTransportCallbacks = {
	onSocketState: () => undefined,
	onSocketOpen: () => undefined,
	onSocketClose: () => undefined,
	onSocketError: () => undefined,
	onForwardMessage: () => undefined,
	onStreamStateChange: () => undefined,
	onSourceSummary: () => undefined,
	onStreamFailure: () => undefined,
	onProtocolError: () => undefined,
};

describe("QuerySocketSession render windows", () => {
	it("advertises the two-chunk query acknowledgement window during authentication", () => {
		const send = jest.fn();
		const socket = {
			close: jest.fn(),
			readyState: WebSocket.OPEN,
			send,
			onopen: null,
			onclose: null,
			onerror: null,
			onmessage: null,
		} as unknown as WebSocket;
		const session = new QuerySocketSession(
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "socket-auth",
				cacheNamespaceId: "test-user",
			},
			callbacks,
			() => socket,
		);

		session.connect();
		socket.onopen?.(new Event("open"));

		expect(send).toHaveBeenCalledWith(
			JSON.stringify({
				token: "token",
				ws_id: "socket-auth",
				data: { query_chunk_ack_window: 2 },
			}),
		);
		session.destroy();
	});

	it("publishes an active same-viewport snapshot before its trailing refresh", async () => {
		const deferred: DeferredProjection[] = [];
		const session = new QuerySocketSession(
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "socket-same-viewport",
				cacheNamespaceId: "test-user",
			},
			callbacks,
			undefined,
			{
				createOperationStore: () => {
					const backend = new MemoryEventStoreBackend();
					return {
						backend,
						repository: new EventStoreRepository(backend),
					};
				},
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
				createViewportProjector: () => ({
					requestRenderWindow: (descriptor) =>
						new Promise<RenderWindowProjection>((resolve) => {
							deferred.push({ descriptor, resolve });
						}),
				}),
			},
		);
		const activeDescriptor = {
			...createDescriptor(1),
			viewportRevision: 10,
		};
		const trailingDescriptor = {
			...createDescriptor(2),
			viewportRevision: 10,
		};
		const active = session.requestRenderWindow(activeDescriptor);
		await waitFor(() => deferred.length === 1);
		const trailing = session.requestRenderWindow(trailingDescriptor);

		deferred[0].resolve(createProjection(deferred[0].descriptor));
		expect((await active)?.projectionRevision).toBe(1);
		await waitFor(() => deferred.length === 2);
		deferred[1].resolve(createProjection(deferred[1].descriptor));
		expect((await trailing)?.projectionRevision).toBe(2);
		session.destroy();
	});

	it("runs one active projection and keeps only the newest pending request", async () => {
		const deferred: DeferredProjection[] = [];
		const session = new QuerySocketSession(
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "socket-session",
				cacheNamespaceId: "test-user",
			},
			callbacks,
			undefined,
			{
				createOperationStore: () => {
					const backend = new MemoryEventStoreBackend();
					return {
						backend,
						repository: new EventStoreRepository(backend),
					};
				},
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
				createViewportProjector: () => ({
					requestRenderWindow: (descriptor) =>
						new Promise<RenderWindowProjection>((resolve) => {
							deferred.push({ descriptor, resolve });
						}),
				}),
			},
		);

		const first = session.requestRenderWindow(createDescriptor(1));
		await waitFor(() => deferred.length === 1);
		const supersededPending = session.requestRenderWindow(createDescriptor(2));
		const newestPending = session.requestRenderWindow(createDescriptor(3));
		expect(await supersededPending).toBeNull();

		deferred[0].resolve(createProjection(deferred[0].descriptor));
		expect(await first).toBeNull();
		await waitFor(() => deferred.length === 2);
		expect(deferred.map(({ descriptor }) => descriptor.projectionRevision)).toEqual([
			1,
			3,
		]);
		deferred[1].resolve(createProjection(deferred[1].descriptor));
		expect((await newestPending)?.projectionRevision).toBe(3);
		expect(session.getQueryDiagnostics().projections).toEqual({
			started: 2,
			completed: 1,
			superseded: 2,
			retries: 0,
		});
		session.resetQueryDiagnostics();
		expect(session.getQueryDiagnostics().projections).toEqual({
			started: 0,
			completed: 0,
			superseded: 0,
			retries: 0,
		});
		session.destroy();
	});

	it("acknowledges cancellation after guards install but before rollback finishes", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		let finishRollback = (): void => undefined;
		const rollback = new Promise<void>((resolve) => {
			finishRollback = resolve;
		});
		let cancelGenerationCalls = 0;
		repository.cancelGeneration = async (): Promise<void> => {
			cancelGenerationCalls += 1;
			await rollback;
		};
		const states: DocumentStreamStateChange[] = [];
		const session = new QuerySocketSession(
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "socket-cancel",
				cacheNamespaceId: "test-user",
			},
			{
				...callbacks,
				onStreamStateChange: (change) => states.push(change),
			},
			undefined,
			{
				createOperationStore: () => ({ backend, repository }),
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
			},
		);
		const descriptor: DocumentStreamDescriptor = {
			requestId: "cancel-before-cleanup",
			operationId: "operation-one",
			operationEpoch: 2,
			owner: "persist",
			kind: "replace",
			sourceIds: ["source-one" as Source.Id],
		};
		await session.registerDocumentStream(descriptor);

		await session.cancelDocumentStream(descriptor.requestId);
		expect(states.at(-1)?.state).toBe("canceled");
		await waitFor(() => cancelGenerationCalls === 1);
		let cleanupFinished = false;
		void rollback.then(() => {
			cleanupFinished = true;
		});
		expect(cleanupFinished).toBe(false);

		finishRollback();
		await waitFor(() => cleanupFinished);
		session.destroy();
	});

	it("forwards query_done without completing before the final docs chunk commits", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const appendPages = repository.appendPages.bind(repository);
		let releaseFinalCommit = (): void => undefined;
		const finalCommitGate = new Promise<void>((resolve) => {
			releaseFinalCommit = resolve;
		});
		repository.appendPages = async (input) => {
			await finalCommitGate;
			return appendPages(input);
		};
		const states: DocumentStreamStateChange[] = [];
		const forwardedTypes: string[] = [];
		const session = new QuerySocketSession(
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "socket-query-done",
				cacheNamespaceId: "test-user",
			},
			{
				...callbacks,
				onForwardMessage: (message) => forwardedTypes.push(message.type),
				onStreamStateChange: (change) => states.push(change),
			},
			undefined,
			{
				createOperationStore: () => ({ backend, repository }),
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
			},
		);
		const descriptor: DocumentStreamDescriptor = {
			requestId: "query-done-after-last",
			operationId: "operation-one",
			operationEpoch: 2,
			owner: "persist",
			kind: "replace",
			sourceIds: ["source-one" as Source.Id],
		};
		await session.registerDocumentStream(descriptor);
		const send = jest.fn((_raw: string): void => undefined);
		const socket = {
			close: jest.fn(),
			readyState: WebSocket.OPEN,
			send,
		} as unknown as WebSocket;
		const internalSession = session as unknown as {
			socket: WebSocket | null;
			handleSocketMessage: (socket: WebSocket, rawData: unknown) => void;
		};
		internalSession.socket = socket;

		internalSession.handleSocketMessage(
			socket,
			JSON.stringify({
				type: "docs_chunk",
				req_id: descriptor.requestId,
				operation_id: descriptor.operationId,
				payload: {
					docs: [
						{
							_id: "final-event",
							"gulp.source_id": "source-one",
							"gulp.timestamp": 1_700_000_000_000,
							"gulp.event_code": 7,
						},
					],
					last: true,
					chunk_num: 0,
					name: "query-name",
					request_last: true,
				},
			}),
		);
		await waitFor(() =>
			send.mock.calls.some(([raw]) => {
				const message = JSON.parse(raw) as { stage?: string };
				return message.stage === "accepted";
			}),
		);
		internalSession.handleSocketMessage(
			socket,
			JSON.stringify({
				type: "query_done",
				req_id: descriptor.requestId,
				operation_id: descriptor.operationId,
				payload: { obj: {} },
			}),
		);

		expect(forwardedTypes.includes("query_done")).toBe(true);
		expect(states.at(-1)?.state).not.toBe("completed");
		expect(
			send.mock.calls.some(([raw]) => {
				const message = JSON.parse(raw) as { stage?: string };
				return message.stage === "durable";
			}),
		).toBe(false);
		releaseFinalCommit();
		await waitFor(() => states.at(-1)?.state === "completed");
		expect(
			send.mock.calls.map(([raw]) => JSON.parse(raw)),
		).toEqual([
			{
				type: "docs_chunk_ack",
				req_id: descriptor.requestId,
				name: "query-name",
				chunk_num: 0,
				stage: "accepted",
			},
			{
				type: "docs_chunk_ack",
				req_id: descriptor.requestId,
				name: "query-name",
				chunk_num: 0,
				stage: "durable",
			},
		]);
		expect(await repository.getSourceSummary("source-one" as Source.Id)).toMatchObject(
			{
				status: "complete",
				eventCount: 1,
			},
		);
		session.destroy();
	});

	it("drains an invalidated active projection before releasing its operation", async () => {
		const deferred: DeferredProjection[] = [];
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const closeRepository = jest.spyOn(repository, "close");
		const closeRevisionBus = jest.fn();
		const session = new QuerySocketSession(
			{
				socketUrl: "ws://gulp.test/ws",
				token: "token",
				wsId: "socket-release",
				cacheNamespaceId: "test-user",
			},
			callbacks,
			undefined,
			{
				createOperationStore: () => ({ backend, repository }),
				createRevisionBus: () => ({
					publish: () => undefined,
					close: closeRevisionBus,
				}),
				createViewportProjector: () => ({
					requestRenderWindow: (descriptor) =>
						new Promise<RenderWindowProjection>((resolve) => {
							deferred.push({ descriptor, resolve });
						}),
				}),
			},
		);

		const projection = session.requestRenderWindow(createDescriptor(8));
		await waitFor(() => deferred.length === 1);
		let releaseFinished = false;
		const release = session.releaseOperation("operation-one").then(() => {
			releaseFinished = true;
		});
		await Promise.resolve();
		expect(releaseFinished).toBe(false);
		expect(closeRepository).not.toHaveBeenCalled();

		deferred[0].resolve(createProjection(deferred[0].descriptor));
		expect(await projection).toBeNull();
		await release;
		expect(closeRepository).toHaveBeenCalledTimes(1);
		expect(closeRevisionBus).toHaveBeenCalledTimes(1);
		session.destroy();
	});
});
