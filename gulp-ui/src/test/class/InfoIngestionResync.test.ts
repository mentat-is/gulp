import { Info } from "@/class/Info";
import { SmartSocket } from "@/class/SmartSocket";
import type { App } from "@/entities/App";
import type { Operation } from "@/entities/Operation";
import type { Query } from "@/entities/Query";
import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import type {
	DocumentStreamDescriptor,
	DocumentStreamStateChange,
	QueryStreamFailureEvent,
	QueryStreamFailureReason,
} from "@/event-stream";
import { DataStore } from "@/store/DataStore";
import { requestStore } from "@/store/request.store";
import { generateUUID } from "@/ui/utils";
import { toast } from "sonner";

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
				INGEST_SOURCE_DONE: "ingest_source_done",
			},
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {},
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
	ingestWorkerManager: {},
}));

jest.mock("@/workers/DataWorker.class", () => ({
	DataWorker: {},
}));

const operationId = "operation-resync-lifecycle" as Operation.Id;
const ingestionRequestId = "ingestion-resync-lifecycle" as Request.Id;
const queryRequestId = "query-resync-lifecycle" as Request.Id;
const sourceId = "source-resync-lifecycle" as Source.Id;

type EventStreamSocket = Pick<
	SmartSocket.Class,
	| "registerDocumentStream"
	| "cancelDocumentStream"
	| "completeDocumentStream"
>;

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
			expect(localStorage.getItem("gulp:event-store-lod-enabled")).toBe(
				"false",
			);
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
				revision: 1,
				status: "building" as const,
				eventCount: 0,
				minTimestamp: 0,
				maxTimestamp: 0,
				hashMin: 0,
				hashMax: 0,
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
			const queryResult = info
				.query_external("external-plugin", {}, false, "{}", {})
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
