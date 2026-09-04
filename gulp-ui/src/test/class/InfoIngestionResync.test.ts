import { Info } from "@/class/Info";
import { SmartSocket } from "@/class/SmartSocket";
import type { App } from "@/entities/App";
import type { Operation } from "@/entities/Operation";
import type { Query } from "@/entities/Query";
import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import type {
	DocumentStreamFinalization,
	DocumentStreamDescriptor,
	DocumentStreamStateChange,
	QueryIngestionProgressEvent,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryStreamFailureReason,
} from "@/event-stream";
import { DataStore } from "@/store/DataStore";
import { requestStore } from "@/store/request.store";
import { generateUUID } from "@/ui/utils";
import { ingestWorkerManager } from "@/workers/IngestWorker.manager";
import { toast } from "sonner";
import {
	clearEventStoreFallbackRequest,
	getEventStoreFallbackRequest,
} from "@/event-store/EventStoreFallback.request";

jest.mock("@/class/IndexedDB", () => ({
	GulpIndexedDB: class MockGulpIndexedDB {},
}));

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("sonner", () => {
	const mockToast = Object.assign(jest.fn(), {
		error: jest.fn(),
		info: jest.fn(),
		loading: jest.fn(),
		success: jest.fn(),
		warning: jest.fn(),
	});
	return { toast: mockToast };
});

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {},
}));

jest.mock("@/dto/Logger.class", () => ({
	Logger: {
		log: jest.fn(),
		warn: jest.fn(),
		error: jest.fn(),
		fatal: jest.fn(),
		assert: jest.fn((value: unknown) => value),
	},
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: jest.fn(() => "generated-request-id"),
	NodeFile: class MockNodeFile {},
	Refractor: {
		array: (...values: unknown[]): unknown[] => values,
	},
	stringToHexColor: (): string => "#000000",
}));

jest.mock("@/overlays/Highlights", () => ({
	Highlights: {},
}));

jest.mock("@/components/Pointers", () => ({
	Pointers: {},
}));

jest.mock("@/components/CustomParameters", () => ({
	CustomParameters: {},
}));

jest.mock("@/class/RenderEngine", () => ({
	RenderEngine: {
		clearAllCaches: jest.fn(),
		reset: jest.fn(),
	},
}));

jest.mock("@/class/SmartSocket", () => ({
	SmartSocket: {
		Class: {
			instance: null,
		},
		Message: {
			Type: {
				STATS_UPDATE: "stats_update",
				COLLAB_CREATE: "collab_create",
				DOCUMENTS_CHUNK: "docs_chunk",
				INGEST_SOURCE_DONE: "ingest_source_done",
			},
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: jest.fn(() => undefined),
		},
	},
}));

jest.mock("@/entities/Operation", () => ({
	Operation: {
		Entity: {
			selected: (app: {
				target?: { operations?: Array<{ selected?: boolean }> };
			}) => app.target?.operations?.find((operation) => operation.selected),
		},
	},
}));

jest.mock("@/entities/App", () => ({
	App: {},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {},
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {},
	},
}));

jest.mock("@/entities/Link", () => ({
	Link: {
		Entity: {},
	},
}));

jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);

jest.mock("@/workers/IngestWorker.manager", () => ({
	ingestWorkerManager: { enqueue: jest.fn() },
}));

jest.mock("@/workers/DataWorker.class", () => ({
	DataWorker: {},
}));

const operationId = "operation-resync-lifecycle" as Operation.Id;
const ingestionRequestId = "ingestion-resync-lifecycle" as Request.Id;
const queryRequestId = "query-resync-lifecycle" as Request.Id;
const sourceId = "source-resync-lifecycle" as Source.Id;
const secondarySourceId = "source-resync-lifecycle-secondary" as Source.Id;

type EventStreamSocket = Pick<
	SmartSocket.Class,
	| "registerDocumentStream"
	| "cancelDocumentStream"
	| "completeDocumentStream"
>;

interface ConditionalSocketListener {
	id: string;
	event: string;
	condition: (message: SmartSocket.Message.Entity) => boolean;
	handler: (message: SmartSocket.Message.Entity) => void;
	once: boolean;
}

interface IngestionLifecycleSocketHarness {
	socket: EventStreamSocket;
	completeDocumentStream: EventStreamSocket["completeDocumentStream"] & {
		mock: {
			calls: Array<[string, DocumentStreamFinalization?]>;
		};
	};
	cancelDocumentStream: EventStreamSocket["cancelDocumentStream"] & {
		mock: { calls: Array<[string]> };
	};
	emitMessage: (event: string, message: SmartSocket.Message.Entity) => void;
	emitSourceSummary: (event: QuerySourceSummaryEvent) => void;
	emitStreamState: (change: DocumentStreamStateChange) => void;
	listenerCount: (event: string) => number;
	subscriptionCount: () => number;
}

/**
 * Creates an in-memory SmartSocket surface for ingestion lifecycle listeners.
 * @returns Socket mock plus helpers for dispatching websocket and worker events.
 */
function createIngestionLifecycleSocketHarness(): IngestionLifecycleSocketHarness {
	const listeners: ConditionalSocketListener[] = [];
	const sourceSummarySubscribers = new Set<
		(event: QuerySourceSummaryEvent) => void
	>();
	const streamStateSubscribers = new Set<
		(change: DocumentStreamStateChange) => void
	>();
	let listenerSequence = 0;
	const completeDocumentStream = jest.fn(
		(
			_requestId: string,
			_finalization?: DocumentStreamFinalization,
		): Promise<void> => Promise.resolve(),
	);
	const cancelDocumentStream = jest.fn(
		(_requestId: string): Promise<void> => Promise.resolve(),
	);
	const registerListener = (
		event: SmartSocket.Message.Type,
		condition: (message: SmartSocket.Message.Entity) => boolean,
		handler: (message: SmartSocket.Message.Entity) => void,
		once: boolean,
	): string => {
		const id = `ingestion-listener-${++listenerSequence}`;
		listeners.push({ id, event, condition, handler, once });
		return id;
	};
	const socket = {
		registerDocumentStream: jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		),
		cancelDocumentStream,
		completeDocumentStream,
		con: (
			event: SmartSocket.Message.Type,
			condition: (message: SmartSocket.Message.Entity) => boolean,
			handler: (message: SmartSocket.Message.Entity) => void,
		): string => registerListener(event, condition, handler, false),
		conce: (
			event: SmartSocket.Message.Type,
			condition: (message: SmartSocket.Message.Entity) => boolean,
			handler: (message: SmartSocket.Message.Entity) => void,
		): string => registerListener(event, condition, handler, true),
		coff: (event: SmartSocket.Message.Type, listenerId: string): void => {
			const index = listeners.findIndex(
				(listener) => listener.event === event && listener.id === listenerId,
			);
			if (index >= 0) listeners.splice(index, 1);
		},
		subscribeSourceSummaries: (
			subscriber: (event: QuerySourceSummaryEvent) => void,
		): (() => void) => {
			sourceSummarySubscribers.add(subscriber);
			return () => sourceSummarySubscribers.delete(subscriber);
		},
		subscribeDocumentStreamStates: (
			subscriber: (change: DocumentStreamStateChange) => void,
		): (() => void) => {
			streamStateSubscribers.add(subscriber);
			return () => streamStateSubscribers.delete(subscriber);
		},
	};

	return {
		socket: socket as unknown as EventStreamSocket,
		completeDocumentStream,
		cancelDocumentStream,
		emitMessage: (event, message) => {
			for (const listener of [...listeners]) {
				if (listener.event !== event || !listener.condition(message)) continue;
				if (listener.once) {
					const index = listeners.findIndex(
						(candidate) => candidate.id === listener.id,
					);
					if (index >= 0) listeners.splice(index, 1);
				}
				listener.handler(message);
			}
		},
		emitSourceSummary: (event) => {
			for (const subscriber of sourceSummarySubscribers) subscriber(event);
		},
		emitStreamState: (change) => {
			for (const subscriber of streamStateSubscribers) subscriber(change);
		},
		listenerCount: (event) =>
			listeners.filter((listener) => listener.event === event).length,
		subscriptionCount: () =>
			sourceSummarySubscribers.size + streamStateSubscribers.size,
	};
}

/**
 * Builds a complete worker summary for one ingested source.
 * @param id Source identifier represented by the summary.
 * @param eventCount Exact number of persisted documents.
 * @returns Source summary event owned by the external ingestion request.
 */
function createSourceSummaryEvent(
	id: Source.Id,
	eventCount: number,
): QuerySourceSummaryEvent {
	return {
		requestId: ingestionRequestId,
		operationId,
		operationEpoch: DataStore.getRenderOperationEpoch(),
		summary: {
			sourceId: id,
			generation: 1,
			revision: 1,
			authorityRevision: 1,
			status: "complete",
			eventCount,
			minTimestamp: 1_000,
			maxTimestamp: 2_000,
			hashMin: 1,
			hashMax: 2,
			activeCacheGeneration: 1,
			cacheHistory: [],
			cacheHistoryCursor: -1,
			nextGeneration: 2,
		},
	};
}

/**
 * Builds one backend lifecycle message for the external ingestion request.
 * @param event Websocket message type to dispatch.
 * @param status Terminal lifecycle status exposed in both payload shapes.
 * @returns Websocket fixture correlated with the current operation and request.
 */
function createIngestionLifecycleMessage(
	event: string,
	status: Request.Status,
): SmartSocket.Message.Entity {
	return {
		type: event,
		req_id: ingestionRequestId,
		operation_id: operationId,
		payload: {
			status,
			obj: {
				req_type: "ext_query",
				status,
			},
		},
	} as unknown as SmartSocket.Message.Entity;
}

/**
 * Builds the QueryWorker descriptor used by external ingestion state updates.
 * @returns Persisted ingestion stream descriptor for the current test request.
 */
function createIngestionStreamDescriptor(): DocumentStreamDescriptor {
	return {
		requestId: ingestionRequestId,
		operationId,
		operationEpoch: DataStore.getRenderOperationEpoch(),
		owner: "persist",
		kind: "ingestion-build",
		sourceIds: [],
	};
}

/**
 * Registers the private persisted-ingestion listener through its test seam.
 * @param info Info instance that owns the lifecycle.
 * @returns Cleanup callback registered by Info.
 */
function registerPersistedIngestionListeners(info: Info): () => void {
	return (
		info as unknown as {
			_register_persisted_ingestion_listeners: (
				requestId: Request.Id,
				currentOperationId: Operation.Id,
				operationEpoch: number,
			) => () => void;
		}
	)._register_persisted_ingestion_listeners(
		ingestionRequestId,
		operationId,
		DataStore.getRenderOperationEpoch(),
	);
}

/**
 * Replaces the mocked singleton used by Info event-stream registration.
 * @param socket Minimal query transport surface, or null to disconnect it.
 * @returns Nothing.
 */
function setEventStreamSocket(socket: EventStreamSocket | null): void {
	(
		SmartSocket.Class as unknown as {
			instance: EventStreamSocket | null;
		}
	).instance = socket;
}

/**
 * Creates the minimum application state required by the Info constructor.
 * @returns Application state fixture.
 */
function createApp(): App.Type {
	return {
		timeline: {
			scale: 1,
		},
		general: {
			loadings: requestStore.getLoadings(),
		},
	} as App.Type;
}

/**
 * Creates a source-linked backend request fixture.
 * @param id Request identifier.
 * @param reqType Backend request type.
 * @returns Ongoing request fixture.
 */
function createRequest(id: Request.Id, reqType: string): Request.Type {
	return {
		id,
		req_type: reqType,
		status: Request.Status.ONGOING,
		data: {
			sources: [["context-id", "context-name", sourceId, "source-name"]],
		},
	} as Request.Type;
}

/**
 * Creates an Info instance with isolated state callbacks.
 * @param app Optional application fixture to attach to the instance.
 * @returns Info instance under test.
 */
function createInfo(app: App.Type = createApp()): Info {
	return new Info({
		app,
		setInfo: jest.fn(),
		timeline: { current: document.createElement("div") },
	});
}

describe("Info ingestion resynchronization lifecycle", () => {
	beforeEach(() => {
		jest.useFakeTimers();
		requestStore.reset();
		setEventStreamSocket(null);
	});

	afterEach(() => {
		jest.useRealTimers();
		requestStore.reset();
		setEventStreamSocket(null);
		DataStore.resetRenderOperation();
	});

	it("does not start source polling for an ongoing query request", async () => {
		const info = createInfo();
		jest
			.spyOn(info, "request_list")
			.mockResolvedValue([createRequest(queryRequestId, "query")]);
		const sync = jest.spyOn(info, "sync").mockResolvedValue(undefined);

		await info.resync_ingestion_state(operationId);
		jest.advanceTimersByTime(10_000);
		await Promise.resolve();

		expect(sync).not.toHaveBeenCalled();
		expect(requestStore.getLoadings().byFileId.size).toBe(0);
	});

	it("applies ongoing ingestion websocket statistics to source activity", () => {
		const info = createInfo();
		const request = createRequest(ingestionRequestId, "ingest");
		request.data = {
			...request.data,
			records_ingested: 18_000,
		};

		info.request_add(request);

		expect(requestStore.getRequestIdByFile(sourceId)).toBe(ingestionRequestId);
		expect(info.ingestionProgress.get(sourceId)).toBe(18_000);

		request.data.records_ingested = 24_000;
		info.request_add(request);
		expect(info.ingestionProgress.get(sourceId)).toBe(24_000);

		request.data.records_ingested = 23_000;
		info.request_add(request);
		expect(info.ingestionProgress.get(sourceId)).toBe(24_000);
	});

	it("polls only while a reconstructed ingestion marker owns its source", async () => {
		const info = createInfo();
		jest
			.spyOn(info, "request_list")
			.mockResolvedValue([createRequest(ingestionRequestId, "ingest")]);
		const sync = jest.spyOn(info, "sync").mockResolvedValue(undefined);

		await info.resync_ingestion_state(operationId);
		jest.advanceTimersByTime(5_000);
		await Promise.resolve();
		expect(sync).toHaveBeenCalledTimes(1);

		info.delLoadingByFile(sourceId);
		info.setLoading(queryRequestId, sourceId);
		jest.advanceTimersByTime(5_000);
		await Promise.resolve();

		expect(sync).toHaveBeenCalledTimes(1);
	});

	it("does not start a late poll after lifecycle cleanup", async () => {
		const info = createInfo();
		let resolveRequests: (requests: Request.Type[]) => void = () => undefined;
		const requests = new Promise<Request.Type[]>((resolve) => {
			resolveRequests = resolve;
		});
		jest.spyOn(info, "request_list").mockReturnValue(requests);
		const sync = jest.spyOn(info, "sync").mockResolvedValue(undefined);

		const resync = info.resync_ingestion_state(operationId);
		info.stop_ingestion_resync_poll();
		resolveRequests([createRequest(ingestionRequestId, "ingest")]);
		await resync;
		jest.advanceTimersByTime(10_000);
		await Promise.resolve();

		expect(sync).not.toHaveBeenCalled();
		expect(requestStore.getLoadings().byFileId.size).toBe(0);
	});
});

describe("Info event stream registration", () => {
	beforeEach(() => {
		requestStore.reset();
		clearEventStoreFallbackRequest();
		localStorage.removeItem("gulp:event-store-lod-enabled");
		(
			generateUUID as unknown as {
				mockReturnValue: (value: Request.Id) => void;
			}
		).mockReturnValue("generated-request-id" as Request.Id);
		(
			toast.error as unknown as {
				mockClear: () => void;
			}
		).mockClear();
		(
			toast.warning as unknown as {
				mockClear: () => void;
			}
		).mockClear();
		DataStore.resetRenderOperation(operationId);
	});

	afterEach(() => {
		clearEventStoreFallbackRequest();
		localStorage.removeItem("gulp:event-store-lod-enabled");
		setEventStreamSocket(null);
		DataStore.resetRenderOperation();
		requestStore.reset();
	});

	it("maps replace enrichment to a replace stream and forwards dedupe policy", async () => {
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		const cancelDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const completeDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const socket: EventStreamSocket = {
			registerDocumentStream,
			cancelDocumentStream,
			completeDocumentStream,
		};
		setEventStreamSocket(socket);
		const info = createInfo();

		const registered = await info.beginEventDocumentStream({
			requestId: ingestionRequestId,
			sourceIds: [sourceId],
			operationId,
			kind: "enrichment",
			mode: "replace",
			deduplicateSequences: false,
			expectedEvents: 0,
			sourceSettingsById: {
				[sourceId]: {
					field: "gulp.event_code",
					hash_function: "fnv1a",
				},
			},
		});

		expect(registered).toBe(true);
		expect(registerDocumentStream.mock.calls).toHaveLength(1);
		expect(registerDocumentStream.mock.calls[0][0]).toMatchObject({
			kind: "replace",
			deduplicateSequences: false,
			operationEpoch: DataStore.getRenderOperationEpoch(),
		});
		expect(cancelDocumentStream.mock.calls).toHaveLength(0);
	});

	it("routes ingestion through the page builder without changing realtime streams", async () => {
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		setEventStreamSocket({
			registerDocumentStream,
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		const info = createInfo();

		await info.beginEventDocumentStream({
			requestId: ingestionRequestId,
			sourceIds: [],
			operationId,
			kind: "ingestion",
			mode: "upsert",
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		await info.beginEventDocumentStream({
			requestId: queryRequestId,
			sourceIds: [],
			operationId,
			kind: "realtime",
			mode: "upsert",
		});

		expect(registerDocumentStream.mock.calls[0][0]).toMatchObject({
			kind: "ingestion-build",
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		expect(registerDocumentStream.mock.calls[1][0]).toMatchObject({
			kind: "realtime-ingest",
		});
	});

	it("tracks monotonic source progress only for ingestion stream kinds", async () => {
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		setEventStreamSocket({
			registerDocumentStream,
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		const info = createInfo();
		const realtimeRequestId = "realtime-progress-request" as Request.Id;

		await info.beginEventDocumentStream({
			requestId: ingestionRequestId,
			sourceIds: [],
			operationId,
			kind: "ingestion",
			mode: "upsert",
		});
		await info.beginEventDocumentStream({
			requestId: realtimeRequestId,
			sourceIds: [secondarySourceId],
			operationId,
			kind: "realtime",
			mode: "upsert",
		});
		await info.beginEventDocumentStream({
			requestId: queryRequestId,
			sourceIds: [sourceId],
			operationId,
			kind: "query",
			mode: "upsert",
		});

		const emitProgress = (
			overrides: Partial<QueryIngestionProgressEvent> = {},
		): void => {
			info.handleEventIngestionProgress({
				requestId: ingestionRequestId,
				operationId,
				operationEpoch: DataStore.getRenderOperationEpoch(),
				kind: "ingestion-build",
				sourceEventCounts: { [sourceId]: 1_000 },
				...overrides,
			});
		};

		emitProgress();
		emitProgress({ sourceEventCounts: { [sourceId]: 750 } });
		emitProgress({
			requestId: realtimeRequestId,
			kind: "realtime-ingest",
			sourceEventCounts: { [secondarySourceId]: 1_250 },
		});
		emitProgress({
			requestId: queryRequestId,
			sourceEventCounts: { [sourceId]: 9_999 },
		});

		expect(info.ingestionProgress.get(sourceId)).toBe(1_000);
		expect(info.ingestionProgress.get(secondarySourceId)).toBe(1_250);

		info.handleEventDocumentStreamState({
			descriptor: registerDocumentStream.mock.calls[0][0],
			state: "canceled",
		});
		info.handleEventDocumentStreamState({
			descriptor: registerDocumentStream.mock.calls[1][0],
			state: "completed",
		});

		expect(info.ingestionProgress.has(sourceId)).toBe(false);
		expect(info.ingestionProgress.has(secondarySourceId)).toBe(false);
	});

	it("forces file ingestion builds to the default event-code hash", async () => {
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		setEventStreamSocket({
			registerDocumentStream,
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		const app = createApp();
		app.target = {
			operations: [{ id: operationId, selected: true } as Operation.Type],
			files: [],
			contexts: [],
		} as unknown as App.Type["target"];
		app.general.ws_id = "file-ingestion-test";
		const info = createInfo(app);
		(
			info as unknown as {
				_register_ingest_source_done_listeners: () => void;
				_register_persisted_ingestion_listeners: () => () => void;
			}
		)._register_ingest_source_done_listeners = () => undefined;
		(
			info as unknown as {
				_register_persisted_ingestion_listeners: () => () => void;
			}
		)._register_persisted_ingestion_listeners = () => () => undefined;
		const enqueue = ingestWorkerManager.enqueue as unknown as {
			mockClear: () => void;
			mock: {
				calls: Array<[
					{ settings: { field?: string; hash_function?: string } },
				]>;
			};
		};
		enqueue.mockClear();

		await info.file_ingest({
			context: "context",
			file: new File(["line"], "events.log"),
			settings: {
				offset: 0,
				custom_parameters: {},
				field: "category",
				hash_function: "djb2",
			},
		});

		expect(registerDocumentStream.mock.calls[0][0]).toMatchObject({
			kind: "ingestion-build",
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		expect(enqueue).toHaveBeenCalledTimes(1);
		expect(enqueue.mock.calls[0][0].settings).toMatchObject({
			field: "category",
			hash_function: "djb2",
		});
	});

	it("dispatches raw queries to IndexedDB persistence on an insecure origin", async () => {
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		const socket = {
			registerDocumentStream,
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
			conce: (): string => "insecure-stats-listener",
			coff: (): void => undefined,
		};
		setEventStreamSocket(socket as unknown as EventStreamSocket);

		const app = createApp();
		app.target = {
			operations: [
				{
					id: operationId,
					index: "operation-index",
					name: "Operation",
					selected: true,
				} as Operation.Type,
			],
			files: [],
			contexts: [],
		} as unknown as App.Type["target"];
		app.general.ws_id = "insecure-stream-test";
		const info = createInfo(app);

		const previousApi = globalThis.api;
		const secureContextDescriptor = Object.getOwnPropertyDescriptor(
			globalThis,
			"isSecureContext",
		);
		const previousRollout = localStorage.getItem(
			"gulp:event-store-lod-enabled",
		);
		const apiMock = jest.fn(async (_path: string, _options: unknown) => ({
			status: "pending",
			data: { total_hits: 1, docs: [] },
		}));
		Object.defineProperty(globalThis, "isSecureContext", {
			configurable: true,
			value: false,
		});
		localStorage.removeItem("gulp:event-store-lod-enabled");
		globalThis.api = apiMock as unknown as typeof globalThis.api;

		try {
			await info.query_file(
				{ string: "all", filters: [] } as Query.Type,
				{ preview: false },
			);
			expect(registerDocumentStream.mock.calls[0][0]).toMatchObject({
				requestId: "generated-request-id",
				owner: "persist",
			});
			expect(apiMock.mock.calls[0][0]).toBe("/query_raw");
			expect(toast.warning).not.toHaveBeenCalled();
		} finally {
			globalThis.api = previousApi;
			if (secureContextDescriptor) {
				Object.defineProperty(
					globalThis,
					"isSecureContext",
					secureContextDescriptor,
				);
			} else {
				Reflect.deleteProperty(globalThis, "isSecureContext");
			}
			if (previousRollout === null) {
				localStorage.removeItem("gulp:event-store-lod-enabled");
			} else {
				localStorage.setItem(
					"gulp:event-store-lod-enabled",
					previousRollout,
				);
			}
		}
	});

	it("uses RAM passthrough and warns once after a persisted rollback", async () => {
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		setEventStreamSocket({
			registerDocumentStream,
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		localStorage.setItem("gulp:event-store-lod-enabled", "false");
		const info = createInfo();

		for (const requestId of [queryRequestId, ingestionRequestId]) {
			expect(
				await info.beginEventDocumentStream({
					requestId,
					sourceIds: [],
					operationId,
					kind: "query",
					mode: "upsert",
				}),
			).toBe(true);
		}

		expect(registerDocumentStream.mock.calls).toHaveLength(2);
		expect(
			registerDocumentStream.mock.calls.every(
				([descriptor]) => descriptor.owner === "passthrough",
			),
		).toBe(true);
		expect(toast.warning).toHaveBeenCalledTimes(1);
		expect(
			(
				toast.warning as unknown as {
					mock: { calls: Array<[unknown, unknown]> };
				}
			).mock.calls[0][0],
		).toBe("eventStore.legacyFallback");
	});

	for (const reason of ["storage", "quota"] as const) {
		it(`does not cancel the backend when ${reason} persistence fails before dispatch`, async () => {
			const registerDocumentStream = jest.fn(
				(_descriptor: DocumentStreamDescriptor): Promise<void> =>
					Promise.resolve(),
			);
			setEventStreamSocket({
				registerDocumentStream,
				cancelDocumentStream: jest.fn(() => Promise.resolve()),
				completeDocumentStream: jest.fn(() => Promise.resolve()),
			});
			const info = createInfo();
			const requestCancel = jest.fn(() => Promise.resolve());
			info.request_cancel = requestCancel as typeof info.request_cancel;
			await info.beginEventDocumentStream({
				requestId: queryRequestId,
				sourceIds: [],
				operationId,
				kind: "query",
				mode: "upsert",
			});

			info.handleEventDocumentStreamFailure({
				requestId: queryRequestId,
				operationId,
				operationEpoch: DataStore.getRenderOperationEpoch(),
				reason,
				message: "registration failed",
			});

			expect(requestCancel).not.toHaveBeenCalled();
			expect(localStorage.getItem("gulp:event-store-lod-enabled")).toBeNull();
			expect(getEventStoreFallbackRequest()).toMatchObject({
				reason,
				message: "registration failed",
				estimatedEventCount: 0,
			});
			expect(
				info.attachEventDocumentStreamCleanup(queryRequestId, jest.fn()),
			).toBe(false);
		});
	}

	it("cancels exactly once after dispatch and clears dispatch state", async () => {
		setEventStreamSocket({
			registerDocumentStream: jest.fn(() => Promise.resolve()),
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		const info = createInfo();
		const requestCancel = jest.fn(() => Promise.resolve());
		info.request_cancel = requestCancel as typeof info.request_cancel;
		await info.beginEventDocumentStream({
			requestId: queryRequestId,
			sourceIds: [],
			operationId,
			kind: "query",
			mode: "upsert",
		});
		(
			info as unknown as {
				markEventDocumentStreamDispatched: (requestId: Request.Id) => boolean;
			}
		).markEventDocumentStreamDispatched(queryRequestId);
		const failure: QueryStreamFailureEvent = {
			requestId: queryRequestId,
			operationId,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			reason: "storage",
			message: "commit failed",
		};

		info.handleEventDocumentStreamFailure(failure);
		info.handleEventDocumentStreamFailure(failure);

		expect(requestCancel.mock.calls).toEqual([[queryRequestId]]);
	});

	it("keeps backend ingestion running when its browser cache fails", async () => {
		const cancelDocumentStream = jest.fn(() => Promise.resolve());
		setEventStreamSocket({
			registerDocumentStream: jest.fn(() => Promise.resolve()),
			cancelDocumentStream,
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		const info = createInfo();
		const requestCancel = jest.fn(() => Promise.resolve());
		const cleanup = jest.fn();
		const previousWarn = console.warn;
		const warn = jest.fn();
		console.warn = warn;
		info.request_cancel = requestCancel as typeof info.request_cancel;
		await info.beginEventDocumentStream({
			requestId: ingestionRequestId,
			sourceIds: [],
			operationId,
			kind: "ingestion",
			mode: "upsert",
			completionMode: "explicit",
		});
		expect(
			info.attachEventDocumentStreamCleanup(ingestionRequestId, cleanup),
		).toBe(true);
		(
			info as unknown as {
				markEventDocumentStreamDispatched: (requestId: Request.Id) => boolean;
			}
		).markEventDocumentStreamDispatched(ingestionRequestId);

		info.handleEventDocumentStreamFailure({
			requestId: ingestionRequestId,
			operationId,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			reason: "backlog",
			message: "local cache is behind",
		});

		expect(requestCancel).not.toHaveBeenCalled();
		expect(cancelDocumentStream).not.toHaveBeenCalled();
		expect(cleanup).not.toHaveBeenCalled();
		expect(toast.error).not.toHaveBeenCalled();
		expect(warn).toHaveBeenCalledTimes(1);
		await info.cancelEventDocumentStream(ingestionRequestId);
		expect(cleanup).toHaveBeenCalledTimes(1);
		console.warn = previousWarn;
	});

	it("removes dispatch state on normal terminal cleanup", async () => {
		setEventStreamSocket({
			registerDocumentStream: jest.fn(() => Promise.resolve()),
			cancelDocumentStream: jest.fn(() => Promise.resolve()),
			completeDocumentStream: jest.fn(() => Promise.resolve()),
		});
		const info = createInfo();
		const requestCancel = jest.fn(() => Promise.resolve());
		info.request_cancel = requestCancel as typeof info.request_cancel;
		await info.beginEventDocumentStream({
			requestId: queryRequestId,
			sourceIds: [],
			operationId,
			kind: "query",
			mode: "upsert",
		});
		(
			info as unknown as {
				markEventDocumentStreamDispatched: (requestId: Request.Id) => boolean;
			}
		).markEventDocumentStreamDispatched(queryRequestId);
		const descriptor: DocumentStreamDescriptor = {
			requestId: queryRequestId,
			operationId,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			owner: "persist",
			kind: "query-upsert",
			sourceIds: [],
		};
		info.handleEventDocumentStreamState({ descriptor, state: "completed" });

		info.handleEventDocumentStreamFailure({
			requestId: queryRequestId,
			operationId,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			reason: "storage",
			message: "late failure",
		});

		expect(requestCancel).not.toHaveBeenCalled();
	});

	it("shows one localized warning for an in-place replacement summary", () => {
		const info = createInfo();
		const event = {
			requestId: queryRequestId,
			operationId,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			replacementMode: "in-place" as const,
			summary: {
				sourceId,
				generation: 1,
				revision: 1,
				authorityRevision: 1,
				status: "building" as const,
				eventCount: 0,
				minTimestamp: 0,
				maxTimestamp: 0,
				hashMin: 0,
				hashMax: 0,
				activeCacheGeneration: null,
				cacheHistory: [],
				cacheHistoryCursor: -1,
				nextGeneration: 2,
			},
		};

		info.handleEventSourceSummary(event);
		expect(toast.warning).toHaveBeenCalledTimes(1);
		const warningCalls = (
			toast.warning as unknown as {
				mock: { calls: Array<[unknown, unknown]> };
			}
		).mock.calls;
		expect(warningCalls[0][0]).toBe("source.cacheRebuildInPlace");
		expect(warningCalls[0][1]).toMatchObject({
			id: `event-store-in-place-${queryRequestId}`,
			richColors: true,
		});
	});

	for (const [reason, key] of [
		["backlog", "eventStore.streamFailed.backlog"],
		["normalization", "eventStore.streamFailed.normalization"],
		["disconnected", "eventStore.streamFailed.disconnected"],
	] as const satisfies ReadonlyArray<
		readonly [Exclude<QueryStreamFailureReason, "superseded">, string]
	>) {
		it(`shows the localized ${reason} stream failure without technical details`, () => {
			const info = createInfo();
			info.request_cancel = jest.fn() as typeof info.request_cancel;
			const event: QueryStreamFailureEvent = {
				requestId: queryRequestId,
				operationId,
				operationEpoch: DataStore.getRenderOperationEpoch(),
				reason,
				message: "technical worker detail",
			};

			info.handleEventDocumentStreamFailure(event);

			expect(toast.error).toHaveBeenCalledTimes(1);
			const errorCalls = (
				toast.error as unknown as {
					mock: { calls: Array<[unknown, unknown]> };
				}
			).mock.calls;
			expect(errorCalls[0][0]).toBe(key);
			expect(errorCalls[0][0] === event.message).toBe(false);
			expect(errorCalls[0][1]).toMatchObject({
				id: `event-store-stream-failure-${queryRequestId}`,
				richColors: true,
			});
		});
	}

	it("does not show a user-facing failure for a superseded stream", () => {
		const info = createInfo();
		info.request_cancel = jest.fn() as typeof info.request_cancel;

		info.handleEventDocumentStreamFailure({
			requestId: queryRequestId,
			operationId,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			reason: "superseded",
			message: "replaced by a newer request",
		});

		expect(toast.error).not.toHaveBeenCalled();
	});

	it("cancels a registration acknowledged after its operation became stale", async () => {
		let acknowledgeRegistration = (): void => undefined;
		const registration = new Promise<void>((resolve) => {
			acknowledgeRegistration = resolve;
		});
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => registration,
		);
		const cancelDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const completeDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const socket: EventStreamSocket = {
			registerDocumentStream,
			cancelDocumentStream,
			completeDocumentStream,
		};
		setEventStreamSocket(socket);
		const info = createInfo();
		const pendingRegistration = info.beginEventDocumentStream({
			requestId: queryRequestId,
			sourceIds: [],
			operationId,
			kind: "query",
			mode: "upsert",
		});

		DataStore.resetRenderOperation("new-operation" as Operation.Id);
		acknowledgeRegistration();

		expect(await pendingRegistration).toBe(false);
		expect(cancelDocumentStream.mock.calls).toEqual([[queryRequestId]]);
	});

	it("dispatches backend cancellation without waiting for persistence cleanup", async () => {
		let finishCancellation = (): void => undefined;
		const cancellation = new Promise<void>((resolve) => {
			finishCancellation = resolve;
		});
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		const cancelDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => cancellation,
		);
		const completeDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		setEventStreamSocket({
			registerDocumentStream,
			cancelDocumentStream,
			completeDocumentStream,
		});
		const info = createInfo();
		await info.beginEventDocumentStream({
			requestId: queryRequestId,
			sourceIds: [],
			operationId,
			kind: "query",
			mode: "upsert",
		});
		(
			info as unknown as {
				registerQueryStream: (
					requestId: Request.Id,
					sourceIds: Source.Id[],
					operationId: Operation.Id,
					operationEpoch: number,
				) => unknown;
			}
		).registerQueryStream(
			queryRequestId,
			[sourceId],
			operationId,
			DataStore.getRenderOperationEpoch(),
		);
		const requestCancel = jest.fn(
			(_requestId: Request.Id): Promise<unknown> => Promise.resolve(),
		);
		info.request_cancel = requestCancel as typeof info.request_cancel;

		let localCleanupFinished = false;
		const closing = info
			.cancelManagedRequest(queryRequestId, { cancelBackend: true })
			.then(() => {
				localCleanupFinished = true;
			});
		await Promise.resolve();
		expect(cancelDocumentStream.mock.calls).toEqual([[queryRequestId]]);
		expect(requestCancel.mock.calls).toEqual([[queryRequestId]]);
		expect(localCleanupFinished).toBe(false);

		finishCancellation();
		await closing;
		expect(localCleanupFinished).toBe(true);
	});

	it("retains request cleanup metadata until explicit completion is acknowledged", async () => {
		let rejectCompletion = true;
		const registerDocumentStream = jest.fn(
			(_descriptor: DocumentStreamDescriptor): Promise<void> => Promise.resolve(),
		);
		const cancelDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const completeDocumentStream = jest.fn(
			(_requestId: string): Promise<void> =>
				rejectCompletion
					? Promise.reject(new Error("completion rejected"))
					: Promise.resolve(),
		);
		setEventStreamSocket({
			registerDocumentStream,
			cancelDocumentStream,
			completeDocumentStream,
		});
		const info = createInfo();
		expect(
			await info.beginEventDocumentStream({
				requestId: ingestionRequestId,
				sourceIds: [],
				operationId,
				kind: "ingestion",
				mode: "upsert",
				completionMode: "explicit",
			}),
		).toBe(true);
		const firstCleanup = jest.fn();
		const secondCleanup = jest.fn();
		expect(
			info.attachEventDocumentStreamCleanup(ingestionRequestId, firstCleanup),
		).toBe(true);

		let completionError: unknown;
		try {
			await info.completeEventDocumentStream(ingestionRequestId);
		} catch (error) {
			completionError = error;
		}
		expect(completionError).toBeInstanceOf(Error);
		expect(
			info.attachEventDocumentStreamCleanup(ingestionRequestId, secondCleanup),
		).toBe(true);
		expect(firstCleanup).not.toHaveBeenCalled();

		rejectCompletion = false;
		await info.completeEventDocumentStream(ingestionRequestId);

		expect(firstCleanup).toHaveBeenCalledTimes(1);
		expect(secondCleanup).toHaveBeenCalledTimes(1);
		expect(
			info.attachEventDocumentStreamCleanup(ingestionRequestId, jest.fn()),
		).toBe(false);
	});

	it("finalizes ext_query ingestion after durable completion and clears every source", async () => {
		const harness = createIngestionLifecycleSocketHarness();
		setEventStreamSocket(harness.socket);
		const info = createInfo();
		info.setLoading(ingestionRequestId, sourceId);
		info.setLoading(ingestionRequestId, secondarySourceId);
		info.ingestionProgress.set(sourceId, 18);
		info.ingestionProgress.set(secondarySourceId, 24);
		registerPersistedIngestionListeners(info);
		harness.emitSourceSummary(createSourceSummaryEvent(sourceId, 18));
		harness.emitSourceSummary(createSourceSummaryEvent(secondarySourceId, 24));

		harness.emitMessage(
			SmartSocket.Message.Type.STATS_UPDATE,
			createIngestionLifecycleMessage(
				SmartSocket.Message.Type.STATS_UPDATE,
				Request.Status.DONE,
			),
		);
		await Promise.resolve();

		expect(harness.completeDocumentStream).toHaveBeenCalledTimes(1);
		expect(harness.completeDocumentStream).toHaveBeenCalledWith(
			ingestionRequestId,
			{ queryCachesBySourceId: {} },
		);
		expect(info.ingestionProgress.get(sourceId)).toBe(18);
		expect(info.ingestionProgress.get(secondarySourceId)).toBe(24);
		expect(harness.subscriptionCount()).toBe(2);

		harness.emitStreamState({
			descriptor: createIngestionStreamDescriptor(),
			state: "completed",
		});

		expect(info.ingestionProgress.has(sourceId)).toBe(false);
		expect(info.ingestionProgress.has(secondarySourceId)).toBe(false);
		expect(requestStore.hasLoadingForFile(sourceId)).toBe(false);
		expect(requestStore.hasLoadingForFile(secondarySourceId)).toBe(false);
		expect(harness.cancelDocumentStream).not.toHaveBeenCalled();
		expect(harness.subscriptionCount()).toBe(0);
	});

	for (const terminalStatus of [
		Request.Status.FAILED,
		Request.Status.CANCELED,
	] as const) {
		it(`cancels an ext_query ingestion when stats become ${terminalStatus}`, async () => {
			const harness = createIngestionLifecycleSocketHarness();
			setEventStreamSocket(harness.socket);
			const info = createInfo();
			info.setLoading(ingestionRequestId, sourceId);
			info.setLoading(ingestionRequestId, secondarySourceId);
			info.ingestionProgress.set(sourceId, 18);
			info.ingestionProgress.set(secondarySourceId, 24);
			registerPersistedIngestionListeners(info);
			harness.emitSourceSummary(createSourceSummaryEvent(sourceId, 18));
			harness.emitSourceSummary(createSourceSummaryEvent(secondarySourceId, 24));

			harness.emitMessage(
				SmartSocket.Message.Type.STATS_UPDATE,
				createIngestionLifecycleMessage(
					SmartSocket.Message.Type.STATS_UPDATE,
					terminalStatus,
				),
			);
			await Promise.resolve();

			expect(harness.completeDocumentStream).not.toHaveBeenCalled();
			expect(harness.cancelDocumentStream.mock.calls).toEqual([
				[ingestionRequestId],
			]);
			expect(info.ingestionProgress.has(sourceId)).toBe(false);
			expect(info.ingestionProgress.has(secondarySourceId)).toBe(false);
			expect(requestStore.hasLoadingForFile(sourceId)).toBe(false);
			expect(requestStore.hasLoadingForFile(secondarySourceId)).toBe(false);
			expect(harness.subscriptionCount()).toBe(0);
		});
	}

	it("does not treat query_done as an EventStore completion fence", async () => {
		const harness = createIngestionLifecycleSocketHarness();
		setEventStreamSocket(harness.socket);
		const info = createInfo();
		info.setLoading(ingestionRequestId, sourceId);
		info.ingestionProgress.set(sourceId, 18);
		const release = registerPersistedIngestionListeners(info);
		harness.emitSourceSummary(createSourceSummaryEvent(sourceId, 18));

		harness.emitMessage(
			"query_done",
			createIngestionLifecycleMessage("query_done", Request.Status.DONE),
		);
		await Promise.resolve();

		expect(harness.completeDocumentStream).not.toHaveBeenCalled();
		expect(harness.cancelDocumentStream).not.toHaveBeenCalled();
		expect(info.ingestionProgress.get(sourceId)).toBe(18);
		expect(requestStore.hasLoadingForFile(sourceId)).toBe(true);
		expect(harness.subscriptionCount()).toBe(2);
		release();
	});

	it("resolves an external LOD query only after source and durable stream completion", async () => {
		let registeredDescriptor: DocumentStreamDescriptor | null = null;
		let sourceDoneCondition:
			| ((message: SmartSocket.Message.Entity) => boolean)
			| null = null;
		let sourceDoneHandler:
			| ((message: SmartSocket.Message.Entity) => void)
			| null = null;
		let streamStateHandler:
			| ((change: DocumentStreamStateChange) => void)
			| null = null;
		const registerDocumentStream = jest.fn(
			(descriptor: DocumentStreamDescriptor): Promise<void> => {
				registeredDescriptor = descriptor;
				return Promise.resolve();
			},
		);
		const cancelDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const completeDocumentStream = jest.fn(
			(_requestId: string): Promise<void> => Promise.resolve(),
		);
		const socket = {
			registerDocumentStream,
			cancelDocumentStream,
			completeDocumentStream,
			con: (
				_event: SmartSocket.Message.Type,
				_condition: (message: SmartSocket.Message.Entity) => boolean,
				_handler: (message: SmartSocket.Message.Entity) => void,
			): string => "external-global-ingestion-listener",
			conce: (
				_event: SmartSocket.Message.Type,
				condition: (message: SmartSocket.Message.Entity) => boolean,
				handler: (message: SmartSocket.Message.Entity) => void,
			): string => {
				sourceDoneCondition = condition;
				sourceDoneHandler = handler;
				return "external-source-done-listener";
			},
			coff: (_event: SmartSocket.Message.Type, _listenerId: string): void =>
				undefined,
			subscribeDocumentStreamStates: (
				handler: (change: DocumentStreamStateChange) => void,
			): (() => void) => {
				streamStateHandler = handler;
				return () => {
					streamStateHandler = null;
				};
			},
		};
		setEventStreamSocket(socket as unknown as EventStreamSocket);

		const app = createApp();
		app.target = {
			operations: [
				{
					id: operationId,
					index: "operation-index",
					name: "Operation",
					selected: true,
				} as Operation.Type,
			],
		} as App.Type["target"];
		app.general.ws_id = "event-stream-test";
		const info = createInfo(app);
		(
			info as unknown as {
				_register_persisted_ingestion_listeners: () => () => void;
			}
		)._register_persisted_ingestion_listeners = () => () => undefined;

		const previousApi = globalThis.api;
		const rollbackKey = "gulp:event-store-lod-enabled";
		const previousRollout = localStorage.getItem(rollbackKey);
		localStorage.removeItem(rollbackKey);
		globalThis.api = (async () => ({
			status: "pending",
			data: undefined,
		})) as unknown as typeof globalThis.api;

		try {
			let queryResolved = false;
			const onIngestionStarted = jest.fn(
				(_requestId: Request.Id): void => undefined,
			);
			const queryResult = info
				.query_external("external-plugin", {}, false, "{}", {}, {
					onIngestionStarted,
				})
				.then((result) => {
					queryResolved = true;
					return result;
				});
			for (let iteration = 0; iteration < 4; iteration++) {
				await Promise.resolve();
			}
			const descriptor =
				registeredDescriptor as unknown as DocumentStreamDescriptor | null;
			const matchesSourceDone = sourceDoneCondition as unknown as
				| ((message: SmartSocket.Message.Entity) => boolean)
				| null;
			const handleSourceDone = sourceDoneHandler as unknown as
				| ((message: SmartSocket.Message.Entity) => void)
				| null;
			const handleStreamState = streamStateHandler as unknown as
				| ((change: DocumentStreamStateChange) => void)
				| null;
			if (
				!descriptor ||
				!matchesSourceDone ||
				!handleSourceDone ||
				!handleStreamState
			) {
				throw new Error(
					`Expected external query lifecycle listeners: descriptor=${Boolean(descriptor)}, condition=${Boolean(matchesSourceDone)}, source=${Boolean(handleSourceDone)}, state=${Boolean(handleStreamState)}.`,
				);
			}
			expect(onIngestionStarted).toHaveBeenCalledTimes(1);
			expect(onIngestionStarted).toHaveBeenCalledWith(descriptor.requestId);
			expect(queryResolved).toBe(false);
			const sourceDoneMessage = {
				type: SmartSocket.Message.Type.INGEST_SOURCE_DONE,
				req_id: descriptor.requestId,
				payload: { status: Request.Status.DONE },
			} as unknown as SmartSocket.Message.Entity;
			if (matchesSourceDone(sourceDoneMessage)) {
				handleSourceDone(sourceDoneMessage);
			}
			await Promise.resolve();
			expect(queryResolved).toBe(false);

			handleStreamState({
				descriptor,
				state: "completed",
			});

			expect(await queryResult).toBeNull();
			expect(queryResolved).toBe(true);
			expect(cancelDocumentStream.mock.calls).toHaveLength(0);
		} finally {
			globalThis.api = previousApi;
			if (previousRollout === null) {
				localStorage.removeItem(rollbackKey);
			} else {
				localStorage.setItem(rollbackKey, previousRollout);
			}
		}
	});
});
