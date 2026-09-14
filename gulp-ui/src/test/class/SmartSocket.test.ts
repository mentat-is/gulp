import { SmartSocket } from "@/class/SmartSocket";
import type {
	DocumentStreamStateChange,
	QuerySourceSummaryEvent,
	QueryRenderWindowDescriptor,
	QueryWorkerInitialization,
} from "@/event-stream";
import type {
	QueryTransport,
	QueryTransportCallbacks,
} from "@/event-store-worker/QueryTransport.types";
import { renderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import { EVENT_STORE_LOD_ROLLBACK_KEY } from "@/event-store/EventStore.rollout";
import type { User } from "@/entities/User";
import type { Request } from "@/entities/Request";
import { TextDecoder, TextEncoder } from "util";

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

jest.mock("@/dto/Logger.class", () => ({
	Logger: { log: () => undefined },
}));
jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Settings: {
			server: "ws://gulp.test",
			token: "test-token",
		},
	},
}));
jest.mock("@/entities/Doc", () => ({ Doc: {} }));
jest.mock("@/entities/Operation", () => ({ Operation: {} }));
jest.mock("@/entities/Request", () => ({ Request: {} }));
jest.mock("@/entities/User", () => ({ User: {} }));
jest.mock("@/ui/Icon", () => ({ Icon: () => null }));
jest.mock("@/lib/WindowBridge", () => ({
	WindowBridge: { getOrCreateCoordinatorId: () => "test-coordinator" },
}));
jest.mock("@/event-store-worker/QueryWorker.factory", () => {
	const requireActual = (
		jest as unknown as {
			requireActual: <TModule>(moduleName: string) => TModule;
		}
	).requireActual;
	const directModule = requireActual<
		typeof import("@/event-store-worker/DirectQueryTransport")
	>("@/event-store-worker/DirectQueryTransport");
	const backendModule = requireActual<
		typeof import("@/event-store/MemoryEventStore.backend")
	>("@/event-store/MemoryEventStore.backend");
	const repositoryModule = requireActual<
		typeof import("@/event-store/EventStore.repository")
	>("@/event-store/EventStore.repository");
	return {
		createQueryTransport: (
			initialization: QueryWorkerInitialization,
			callbacks: QueryTransportCallbacks,
		): QueryTransport =>
			new directModule.DirectQueryTransport(initialization, callbacks, {
				createOperationStore: () => {
					const backend = new backendModule.MemoryEventStoreBackend();
					return {
						backend,
						repository: new repositoryModule.EventStoreRepository(backend),
					};
				},
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
			}),
	};
});

class FakeWebSocket {
	public static readonly OPEN = 1;
	public static instances: FakeWebSocket[] = [];
	public readonly url: string;
	public readyState = 0;
	public onopen: ((event: Event) => void) | null = null;
	public onclose: ((event: CloseEvent) => void) | null = null;
	public onerror: ((event: Event) => void) | null = null;
	public onmessage: ((event: MessageEvent) => void) | null = null;
	public closeCalls = 0;
	public readonly sent: string[] = [];

	/**
	 * Records one direct fallback socket.
	 * @param url WebSocket URL under test.
	 */
	public constructor(url: string) {
		this.url = url;
		FakeWebSocket.instances.push(this);
	}

	/**
	 * Records one serialized client payload.
	 * @param data Serialized WebSocket payload.
	 */
	public send(data: string): void {
		this.sent.push(data);
	}

	/** Records one socket close request. */
	public close(): void {
		this.closeCalls += 1;
	}

	/** Simulates the browser WebSocket open lifecycle event. */
	public triggerOpen(): void {
		this.readyState = FakeWebSocket.OPEN;
		this.onopen?.(new Event("open"));
	}

	/**
	 * Delivers one JSON-serializable server message.
	 * @param message Message value to serialize.
	 */
	public triggerMessage(message: object): void {
		this.onmessage?.(
			new MessageEvent("message", { data: JSON.stringify(message) }),
		);
	}
}

/** Creates one valid raw event whose source violates a registered authority. */
function createInvalidSourceDocument(): Record<string, unknown> {
	return {
		_id: "outside",
		"gulp.source_id": "outside-source",
		"gulp.timestamp": 1_700_000_000_000,
		"gulp.event_code": 4,
	};
}

/** Creates one empty render-window request for projection-delivery tests. */
function createProjectionDescriptor(): QueryRenderWindowDescriptor {
	return {
		operationId: "operation-one",
		operationEpoch: 5,
		viewportRevision: 6,
		projectionRevision: 7,
		frame: { min: 0, max: 1_000 },
		width: 100,
		scale: 1,
		scrollX: 0,
		horizontalOverscanPixels: 10,
		sources: [],
	};
}

describe("SmartSocket lifecycle", () => {
	const originalWebSocket = globalThis.WebSocket;
	const originalWorker = globalThis.Worker;
	const originalSecureContextDescriptor = Object.getOwnPropertyDescriptor(
		globalThis,
		"isSecureContext",
	);

	beforeEach(() => {
		Reflect.deleteProperty(SmartSocket.Class, "instance");
		renderWindowProjectionBus.clear();
		globalThis.localStorage?.removeItem(EVENT_STORE_LOD_ROLLBACK_KEY);
		FakeWebSocket.instances = [];
		Reflect.deleteProperty(globalThis, "Worker");
		Object.defineProperty(globalThis, "WebSocket", {
			configurable: true,
			value: FakeWebSocket,
		});
	});

	afterEach(() => {
		const socket = Reflect.get(
			SmartSocket.Class,
			"instance",
		) as SmartSocket.Class | undefined;
		socket?.destroy();
		renderWindowProjectionBus.clear();
		globalThis.localStorage?.removeItem(EVENT_STORE_LOD_ROLLBACK_KEY);
		Object.defineProperty(globalThis, "WebSocket", {
			configurable: true,
			value: originalWebSocket,
		});
		if (originalWorker) {
			Object.defineProperty(globalThis, "Worker", {
				configurable: true,
				value: originalWorker,
			});
		} else {
			Reflect.deleteProperty(globalThis, "Worker");
		}
		if (originalSecureContextDescriptor) {
			Object.defineProperty(
				globalThis,
				"isSecureContext",
				originalSecureContextDescriptor,
			);
		} else {
			Reflect.deleteProperty(globalThis, "isSecureContext");
		}
	});

	it("releases the closed singleton so a detached route can reconnect", () => {
		const first = new SmartSocket.Class("detached-one");
		expect(SmartSocket.Class.instance).toBe(first);

		first.destroy();
		expect(Reflect.get(SmartSocket.Class, "instance")).toBeUndefined();

		const second = new SmartSocket.Class("detached-two");
		expect(second).not.toBe(first);
		expect(SmartSocket.Class.instance).toBe(second);
	});

	it("routes request-scoped summaries and stream states without global fan-out", () => {
		const socket = new SmartSocket.Class("request-routing");
		const summarySubscribers = Array.from({ length: 200 }, () => jest.fn());
		const stateSubscribers = Array.from({ length: 200 }, () => jest.fn());
		const cleanups = summarySubscribers.flatMap((summarySubscriber, index) => {
			const requestId = `request-${index}` as Request.Id;
			return [
				socket.subscribeSourceSummariesForRequest(
					requestId,
					summarySubscriber,
				),
				socket.subscribeDocumentStreamStatesForRequest(
					requestId,
					stateSubscribers[index],
				),
			];
		});
		const publisher = socket as unknown as {
			publishSourceSummary(event: QuerySourceSummaryEvent): void;
			publishStreamState(change: DocumentStreamStateChange): void;
		};
		publisher.publishSourceSummary({
			requestId: "request-137",
		} as QuerySourceSummaryEvent);
		publisher.publishStreamState({
			descriptor: { requestId: "request-137" },
			state: "streaming",
		} as DocumentStreamStateChange);

		expect(summarySubscribers[137]).toHaveBeenCalledTimes(1);
		expect(stateSubscribers[137]).toHaveBeenCalledTimes(1);
		expect(
			summarySubscribers.filter((subscriber) => subscriber.mock.calls.length > 0),
		).toHaveLength(1);
		expect(
			stateSubscribers.filter((subscriber) => subscriber.mock.calls.length > 0),
		).toHaveLength(1);

		for (const cleanup of cleanups) {
			cleanup();
			cleanup();
		}
		publisher.publishSourceSummary({
			requestId: "request-137",
		} as QuerySourceSummaryEvent);
		publisher.publishStreamState({
			descriptor: { requestId: "request-137" },
			state: "completed",
		} as DocumentStreamStateChange);
		expect(summarySubscribers[137]).toHaveBeenCalledTimes(1);
		expect(stateSubscribers[137]).toHaveBeenCalledTimes(1);

		const destroyedSummarySubscriber = jest.fn();
		const destroyedStateSubscriber = jest.fn();
		socket.subscribeSourceSummariesForRequest(
			"destroyed-request" as Request.Id,
			destroyedSummarySubscriber,
		);
		socket.subscribeDocumentStreamStatesForRequest(
			"destroyed-request" as Request.Id,
			destroyedStateSubscriber,
		);
		socket.destroy();
		publisher.publishSourceSummary({
			requestId: "destroyed-request",
		} as QuerySourceSummaryEvent);
		publisher.publishStreamState({
			descriptor: { requestId: "destroyed-request" },
			state: "completed",
		} as DocumentStreamStateChange);
		expect(destroyedSummarySubscriber).not.toHaveBeenCalled();
		expect(destroyedStateSubscriber).not.toHaveBeenCalled();
	});

	it("rotates the worker singleton when the authenticated user changes", () => {
		const first = new SmartSocket.Class(
			"shared-websocket-id",
			"user-one" as User.Id,
		);
		const firstWebSocket = FakeWebSocket.instances[0];
		const second = new SmartSocket.Class(
			"shared-websocket-id",
			"user-two" as User.Id,
		);

		expect(second).not.toBe(first);
		expect(firstWebSocket.closeCalls).toBe(1);
		expect(SmartSocket.Class.instance).toBe(second);
	});

	it("keeps the realm singleton when only a detached websocket id differs", () => {
		const userId = "shared-user" as User.Id;
		const main = new SmartSocket.Class("main-websocket-id", userId);
		const detached = new SmartSocket.Class("detached-websocket-id", userId);

		expect(detached).toBe(main);
		expect(FakeWebSocket.instances).toHaveLength(1);
		expect(FakeWebSocket.instances[0].closeCalls).toBe(0);
	});

	it("does not enable RAM passthrough solely for an insecure origin", () => {
		Object.defineProperty(globalThis, "isSecureContext", {
			configurable: true,
			value: false,
		});
		const socket = new SmartSocket.Class("legacy-client");
		const directSocket = FakeWebSocket.instances[0];
		const received: SmartSocket.Message.Entity[] = [];
		socket.con(
			SmartSocket.Message.Type.DOCUMENTS_CHUNK,
			() => true,
			(message) => received.push(message),
		);
		directSocket.triggerOpen();

		directSocket.triggerMessage({
			type: "docs_chunk",
			req_id: "legacy-request",
			"gulp.operation_id": "operation-one",
			payload: { docs: [{ _id: "one" }], chunk_num: 0, last: true },
		});

		expect(received).toHaveLength(0);
		expect(FakeWebSocket.instances).toHaveLength(1);
	});

	it("enables RAM passthrough after the persisted EventStore rollback", () => {
		globalThis.localStorage?.setItem(EVENT_STORE_LOD_ROLLBACK_KEY, "false");
		const socket = new SmartSocket.Class("fallback-client");
		const directSocket = FakeWebSocket.instances[0];
		const received: SmartSocket.Message.Entity[] = [];
		socket.con(
			SmartSocket.Message.Type.DOCUMENTS_CHUNK,
			() => true,
			(message) => received.push(message),
		);
		directSocket.triggerOpen();

		directSocket.triggerMessage({
			type: "docs_chunk",
			req_id: "fallback-request",
			"gulp.operation_id": "operation-one",
			payload: { docs: [{ _id: "one" }], chunk_num: 0, last: true },
		});

		expect(received).toHaveLength(1);
	});

	it("does not expose persist-owned document chunks through EventEmitter", async () => {
		const socket = new SmartSocket.Class("persist-client");
		const directSocket = FakeWebSocket.instances[0];
		const listener = jest.fn();
		const summaries: string[] = [];
		const states: string[] = [];
		const projectionInvalidations: string[][] = [];
		socket.on(SmartSocket.Message.Type.DOCUMENTS_CHUNK, listener);
		const unsubscribeSummaries = socket.subscribeSourceSummaries((event) => {
			summaries.push(`${event.operationEpoch}:${event.summary.status}`);
		});
		const unsubscribeStates = socket.subscribeDocumentStreamStates((change) => {
			states.push(change.state);
		});
		const unsubscribeProjectionInvalidations =
			socket.subscribeProjectionInvalidations((event) => {
				projectionInvalidations.push([...event.sourceIds]);
			});
		await socket.registerDocumentStream({
			requestId: "persist-request",
			operationId: "operation-one",
			operationEpoch: 3,
			owner: "persist",
			kind: "replace",
			sourceIds: ["source-one"],
		});
		const completed = new Promise<void>((resolve) => {
			const unsubscribe = socket.subscribeDocumentStreamStates((change) => {
				if (
					change.descriptor.requestId === "persist-request" &&
					change.state === "completed"
				) {
					unsubscribe();
					resolve();
				}
			});
		});

		directSocket.triggerMessage({
			type: "docs_chunk",
			req_id: "persist-request",
			payload: {
				docs: [
					{
						_id: "one",
						"gulp.source_id": "source-one",
						"gulp.timestamp": 1_700_000_000_000,
						"gulp.event_code": 4,
					},
				],
				chunk_num: 0,
				last: true,
			},
		});
		await completed;

		expect(listener).not.toHaveBeenCalled();
		expect(summaries.includes("3:complete")).toBe(true);
		expect(projectionInvalidations).toEqual([["source-one"]]);
		expect(states).toEqual([
			"registered",
			"streaming",
			"flushing",
			"completed",
		]);
		unsubscribeSummaries();
		unsubscribeStates();
		unsubscribeProjectionInvalidations();
	});

	it("publishes compact ingestion progress without forwarding documents", async () => {
		const socket = new SmartSocket.Class("ingestion-progress-client");
		const directSocket = FakeWebSocket.instances[0];
		const documentListener = jest.fn();
		socket.on(SmartSocket.Message.Type.DOCUMENTS_CHUNK, documentListener);
		let unsubscribeProgress = (): void => undefined;
		const progress = new Promise<Record<string, number>>((resolve) => {
			unsubscribeProgress = socket.subscribeIngestionProgress((event) => {
				if (event.requestId === "ingestion-progress-request") {
					resolve({ ...event.sourceEventCounts });
				}
			});
		});
		await socket.registerDocumentStream({
			requestId: "ingestion-progress-request",
			operationId: "operation-one",
			operationEpoch: 5,
			owner: "persist",
			kind: "ingestion-build",
			completionMode: "explicit",
			sourceIds: [],
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});

		directSocket.triggerMessage({
			type: "docs_chunk",
			req_id: "ingestion-progress-request",
			payload: {
				docs: [
					{
						_id: "one",
						"gulp.source_id": "source-one",
						"gulp.timestamp": 1_700_000_000_000,
						"gulp.event_code": 4,
					},
				],
				chunk_num: 0,
				last: false,
			},
		});

		expect(await progress).toEqual({ "source-one": 1 });
		expect(documentListener).not.toHaveBeenCalled();
		unsubscribeProgress();
		await socket.cancelDocumentStream("ingestion-progress-request");
	});

	it("publishes typed persistence failures without forwarding bad documents", async () => {
		const socket = new SmartSocket.Class("failure-client");
		const directSocket = FakeWebSocket.instances[0];
		const listener = jest.fn();
		socket.on(SmartSocket.Message.Type.DOCUMENTS_CHUNK, listener);
		await socket.registerDocumentStream({
			requestId: "failure-request",
			operationId: "operation-one",
			operationEpoch: 9,
			owner: "persist",
			kind: "replace",
			sourceIds: ["source-one"],
		});
		const failure = new Promise<string>((resolve) => {
			const unsubscribe = socket.subscribeDocumentStreamFailures((event) => {
				if (event.requestId !== "failure-request") return;
				unsubscribe();
				resolve(`${event.operationEpoch}:${event.reason}`);
			});
		});

		directSocket.triggerMessage({
			type: "docs_chunk",
			req_id: "failure-request",
			payload: {
				docs: [createInvalidSourceDocument()],
				chunk_num: 0,
				last: true,
			},
		});

		expect(await failure).toBe("9:normalization");
		expect(listener).not.toHaveBeenCalled();
	});

	it("resolves awaitReady after forwarding the authentication acknowledgement", async () => {
		const socket = new SmartSocket.Class("ready-client");
		const directSocket = FakeWebSocket.instances[0];
		const observedTypes: string[] = [];
		socket.on(SmartSocket.Message.Type.WS_CONNECTED, () => {
			observedTypes.push(SmartSocket.Message.Type.WS_CONNECTED);
		});
		const ready = socket.awaitReady(100);
		directSocket.triggerOpen();
		directSocket.triggerMessage({
			type: "ws_connected",
			payload: { obj: {} },
		});

		await ready;
		expect(observedTypes).toEqual([SmartSocket.Message.Type.WS_CONNECTED]);
		expect(JSON.parse(directSocket.sent[0])).toEqual({
			token: "test-token",
			ws_id: "ready-client",
			data: { query_chunk_ack_window: 2 },
		});
	});

	it("publishes direct render-window results on the single-consumer bus", async () => {
		const socket = new SmartSocket.Class("projection-client");
		const received: RenderWindowProjection[] = [];
		const unsubscribe = renderWindowProjectionBus.subscribe((projection) => {
			received.push(projection);
			return true;
		});
		const projection = await socket.requestRenderWindow(
			createProjectionDescriptor(),
		);

		expect(projection).not.toBeNull();
		expect(received).toEqual([projection]);
		unsubscribe();
	});

	it("rejects a render window when no Canvas owns the projection bus", async () => {
		const socket = new SmartSocket.Class("unclaimed-projection-client");

		await expect(
			socket.requestRenderWindow(createProjectionDescriptor()),
		).rejects.toThrow("not accepted by the active Canvas");
	});

	it("rejects a render window explicitly refused by Canvas fences", async () => {
		const socket = new SmartSocket.Class("rejected-projection-client");
		const unsubscribe = renderWindowProjectionBus.subscribe(() => false);

		await expect(
			socket.requestRenderWindow(createProjectionDescriptor()),
		).rejects.toThrow("not accepted by the active Canvas");
		unsubscribe();
	});

});
