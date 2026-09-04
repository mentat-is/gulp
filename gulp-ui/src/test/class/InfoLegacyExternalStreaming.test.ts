import { Info } from "@/class/Info";
import { SmartSocket } from "@/class/SmartSocket";
import type { App } from "@/entities/App";
import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import type {
	DocumentStreamDescriptor,
	DocumentStreamFinalization,
} from "@/event-stream";
import { DataStore } from "@/store/DataStore";
import { requestStore } from "@/store/request.store";
import { toast } from "sonner";

const mockEventsBySource = new Map<string, Doc.Type[]>();

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
	generateUUID: jest.fn(() => "legacy-external-request"),
	NodeFile: class MockNodeFile {},
	Refractor: {
		array: (...values: unknown[]): unknown[] => values,
	},
	stringToHexColor: (): string => "#000000",
}));

jest.mock("@/overlays/Highlights", () => ({ Highlights: {} }));
jest.mock("@/components/Pointers", () => ({ Pointers: {} }));
jest.mock("@/components/CustomParameters", () => ({ CustomParameters: {} }));

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
			},
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			events: (_app: App.Type, sourceId: Source.Id): Doc.Type[] =>
				mockEventsBySource.get(sourceId) ?? [],
			id: (app: App.Type, sourceId: Source.Id): Source.Type | undefined =>
				app.target.files.find((source) => source.id === sourceId),
			normalize: (_app: App.Type, source: Source.Type): Source.Type => source,
			renderedFrame: (): { min: number; max: number } => ({ min: 1, max: 4 }),
		},
	},
}));

jest.mock("@/entities/Operation", () => ({
	Operation: {
		Entity: {
			selected: (app: App.Type): Operation.Type | undefined =>
				app.target.operations.find((operation) => operation.selected),
		},
	},
}));

jest.mock("@/entities/App", () => ({ App: {} }));
jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			normalize: (documents: Doc.Type[]): Doc.Type[] => documents,
		},
	},
}));
jest.mock("@/entities/Note", () => ({ Note: { Entity: {} } }));
jest.mock("@/entities/Link", () => ({ Link: { Entity: {} } }));
jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);
jest.mock("@/workers/IngestWorker.manager", () => ({
	ingestWorkerManager: {},
}));
jest.mock("@/workers/DataWorker.class", () => ({ DataWorker: {} }));

const operationId = "legacy-external-operation" as Operation.Id;
const requestId = "legacy-external-request" as Request.Id;
const firstSourceId = "legacy-external-source-a" as Source.Id;
const secondSourceId = "legacy-external-source-b" as Source.Id;

type SocketCondition = (message: SmartSocket.Message.Entity) => boolean;
type SocketHandler = (message: SmartSocket.Message.Entity) => void;

interface SocketRegistration {
	event: SmartSocket.Message.Type;
	condition: SocketCondition;
	handler: SocketHandler;
	once: boolean;
}

interface LegacyStreamingRegistrar {
	_register_streaming_listeners: (
		id: Request.Id,
		operationId: Operation.Id,
		operationEpoch: number,
	) => () => void;
}

/** Captures compatibility websocket listeners registered by Info. */
class CapturingSocket {
	public readonly registrations: SocketRegistration[] = [];
	public readonly registerDocumentStream = jest.fn<
		Promise<void>,
		[DocumentStreamDescriptor]
	>(() => Promise.resolve());
	public readonly cancelDocumentStream = jest.fn<Promise<void>, [string]>(() =>
		Promise.resolve(),
	);
	public readonly completeDocumentStream = jest.fn<
		Promise<void>,
		[string, DocumentStreamFinalization?]
	>(() => Promise.resolve());
	/** Records compatibility listener cleanup. */
	public readonly coff = jest.fn(
		(_event: SmartSocket.Message.Type, _listenerId: string): void => undefined,
	);

	/** Registers a reusable compatibility listener. */
	public con = (
		event: SmartSocket.Message.Type,
		condition: SocketCondition,
		handler: SocketHandler,
	): string => this.capture(event, condition, handler, false);

	/** Registers a one-shot compatibility listener. */
	public conce = (
		event: SmartSocket.Message.Type,
		condition: SocketCondition,
		handler: SocketHandler,
	): string => this.capture(event, condition, handler, true);

	/** Dispatches a fixture through every matching captured listener. */
	public dispatch(message: SmartSocket.Message.Entity): void {
		for (const registration of [...this.registrations]) {
			if (registration.event !== message.type) continue;
			if (!registration.condition(message)) continue;
			registration.handler(message);
		}
	}

	/** Stores one listener and returns its stable mock identifier. */
	private capture(
		event: SmartSocket.Message.Type,
		condition: SocketCondition,
		handler: SocketHandler,
		once: boolean,
	): string {
		const listenerId = `listener-${this.registrations.length + 1}`;
		this.registrations.push({ event, condition, handler, once });
		return listenerId;
	}
}

/** Creates a minimal source entity used by legacy completion metadata updates. */
function createSource(id: Source.Id, name: string): Source.Type {
	return {
		id,
		name,
		selected: false,
		total: 0,
		timestamp: { min: 0, max: 0 },
		nanotimestamp: { min: 0n, max: 0n },
	} as unknown as Source.Type;
}

/** Creates application state with two external-query sources. */
function createApp(): App.Type {
	return {
		timeline: {
			scale: 1,
			frame: { min: 0, max: 0 },
		},
		general: {
			loadings: requestStore.getLoadings(),
			requests: [],
		},
		target: {
			operations: [
				{
					id: operationId,
					name: "External operation",
					selected: true,
				} as Operation.Type,
			],
			files: [
				createSource(firstSourceId, "external-a.log"),
				createSource(secondSourceId, "external-b.log"),
			],
		},
		settings: {},
		hidden: {},
	} as unknown as App.Type;
}

/** Creates an Info instance whose React state updates execute synchronously. */
function createInfo(app: App.Type): Info {
	return new Info({
		app,
		setInfo: ((updater: React.SetStateAction<App.Type>): void => {
			if (typeof updater === "function") {
				updater(app);
			}
		}) as React.Dispatch<React.SetStateAction<App.Type>>,
		timeline: { current: document.createElement("div") },
	});
}

/** Creates one normalized external-query document. */
function createDocument(
	id: string,
	sourceId: Source.Id,
	timestamp: number,
): Doc.Type {
	return {
		_id: id as Doc.Id,
		"gulp.source_id": sourceId,
		"gulp.event_code": 1,
		gulp_timestamp: timestamp,
		number_hash: timestamp,
	} as Doc.Type;
}

/** Creates a websocket message fixture with the required transport fields. */
function createMessage(
	type: SmartSocket.Message.Type,
	payload: SmartSocket.Message.Entity["payload"],
): SmartSocket.Message.Entity {
	return {
		"@timestamp": Date.now(),
		type,
		private: true,
		payload,
		ws_id: "legacy-external-ws",
		user_id: "legacy-external-user",
		req_id: requestId,
		operation_id: operationId,
		"gulp.operation_id": operationId,
	} as SmartSocket.Message.Entity;
}

/** Lets the streaming listener's promise chain settle. */
async function settleStreamingLifecycle(): Promise<void> {
	for (let iteration = 0; iteration < 8; iteration++) {
		await Promise.resolve();
	}
}

/** Registers the explicit worker lifecycle paired with the legacy passthrough listener. */
async function registerLegacyStream(info: Info): Promise<void> {
	const registered = await info.beginEventDocumentStream({
		requestId,
		sourceIds: [],
		operationId,
		kind: "ingestion",
		mode: "upsert",
		completionMode: "explicit",
	});
	expect(registered).toBe(true);
	(info as unknown as LegacyStreamingRegistrar)._register_streaming_listeners(
		requestId,
		operationId,
		DataStore.getRenderOperationEpoch(),
	);
}

describe("Info legacy external-query streaming", () => {
	let socket: CapturingSocket;

	beforeEach(() => {
		requestStore.reset();
		mockEventsBySource.clear();
		localStorage.setItem("gulp:event-store-lod-enabled", "false");
		DataStore.resetRenderOperation(operationId);
		socket = new CapturingSocket();
		(
			SmartSocket.Class as unknown as {
				instance: CapturingSocket | null;
			}
		).instance = socket;
		(
			toast.success as unknown as {
				mock: { calls: unknown[][] };
			}
		).mock.calls.splice(0);
	});

	afterEach(() => {
		(
			SmartSocket.Class as unknown as {
				instance: CapturingSocket | null;
			}
		).instance = null;
		requestStore.reset();
		mockEventsBySource.clear();
		localStorage.removeItem("gulp:event-store-lod-enabled");
		DataStore.resetRenderOperation();
	});

	it("tracks and finalizes every source of an ext_query stream", async () => {
		const app = createApp();
		const info = createInfo(app);
		const addEvents = jest.fn<Promise<void>, [Doc.Type[], number?]>(
			async (events: Doc.Type[]) => {
				for (const event of events) {
					const sourceId = event["gulp.source_id"] as Source.Id;
					const existing = mockEventsBySource.get(sourceId) ?? [];
					mockEventsBySource.set(sourceId, [...existing, event]);
				}
			},
		);
		info.events_add_async = addEvents;

		await registerLegacyStream(info);

		for (const [sourceId, name] of [
			[firstSourceId, "external-a.log"],
			[secondSourceId, "external-b.log"],
		] as const) {
			socket.dispatch(
				createMessage(SmartSocket.Message.Type.COLLAB_CREATE, {
					obj: { id: sourceId, name, type: "source" },
				}),
			);
		}

		socket.dispatch(
			createMessage(SmartSocket.Message.Type.DOCUMENTS_CHUNK, {
				obj: {},
				docs: [
					createDocument("a-1", firstSourceId, 4),
					createDocument("b-1", secondSourceId, 3),
					createDocument("a-2", firstSourceId, 2),
				],
			}),
		);
		socket.dispatch(
			createMessage(SmartSocket.Message.Type.DOCUMENTS_CHUNK, {
				obj: {},
				docs: [createDocument("b-2", secondSourceId, 1)],
				last: true,
			}),
		);

		expect(info.ingestionProgress.get(firstSourceId)).toBe(2);
		expect(info.ingestionProgress.get(secondSourceId)).toBe(2);
		expect(requestStore.hasLoadingForFile(firstSourceId)).toBe(true);
		expect(requestStore.hasLoadingForFile(secondSourceId)).toBe(true);

		socket.dispatch(
			createMessage(SmartSocket.Message.Type.STATS_UPDATE, {
				obj: { req_type: "ext_query", status: Request.Status.DONE },
			}),
		);
		await settleStreamingLifecycle();

		expect(addEvents).toHaveBeenCalled();
		expect(socket.completeDocumentStream).toHaveBeenCalledTimes(1);
		expect(socket.completeDocumentStream.mock.calls[0][0]).toBe(requestId);
		expect(socket.cancelDocumentStream).not.toHaveBeenCalled();
		expect(info.ingestionProgress.has(firstSourceId)).toBe(false);
		expect(info.ingestionProgress.has(secondSourceId)).toBe(false);
		expect(requestStore.hasLoadingForFile(firstSourceId)).toBe(false);
		expect(requestStore.hasLoadingForFile(secondSourceId)).toBe(false);
		const firstSource = info.app.target.files.find(
			(source) => source.id === firstSourceId,
		);
		const secondSource = info.app.target.files.find(
			(source) => source.id === secondSourceId,
		);
		expect(firstSource?.selected).toBe(true);
		expect(firstSource?.total).toBe(2);
		expect(secondSource?.selected).toBe(true);
		expect(secondSource?.total).toBe(2);
		expect(toast.success).toHaveBeenCalledTimes(2);
	});

	for (const status of [Request.Status.FAILED, Request.Status.CANCELED]) {
		it(`cancels and clears every source when an ext_query stream is ${status}`, async () => {
			const info = createInfo(createApp());
			info.events_add_async = jest.fn<Promise<void>, [Doc.Type[], number?]>(
				() => Promise.resolve(),
			);
			await registerLegacyStream(info);

			for (const sourceId of [firstSourceId, secondSourceId]) {
				socket.dispatch(
					createMessage(SmartSocket.Message.Type.COLLAB_CREATE, {
						obj: { id: sourceId, type: "source" },
					}),
				);
			}
			socket.dispatch(
				createMessage(SmartSocket.Message.Type.DOCUMENTS_CHUNK, {
					obj: {},
					docs: [
						createDocument("a-failed", firstSourceId, 2),
						createDocument("b-failed", secondSourceId, 1),
					],
					last: true,
				}),
			);

			expect(info.ingestionProgress.has(firstSourceId)).toBe(true);
			expect(info.ingestionProgress.has(secondSourceId)).toBe(true);

			socket.dispatch(
				createMessage(SmartSocket.Message.Type.STATS_UPDATE, {
					obj: { req_type: "ext_query", status },
				}),
			);
			await settleStreamingLifecycle();

			expect(socket.cancelDocumentStream).toHaveBeenCalledTimes(1);
			expect(socket.cancelDocumentStream.mock.calls[0][0]).toBe(requestId);
			expect(socket.completeDocumentStream).not.toHaveBeenCalled();
			expect(info.ingestionProgress.has(firstSourceId)).toBe(false);
			expect(info.ingestionProgress.has(secondSourceId)).toBe(false);
			expect(requestStore.hasLoadingForFile(firstSourceId)).toBe(false);
			expect(requestStore.hasLoadingForFile(secondSourceId)).toBe(false);
			expect(toast.success).not.toHaveBeenCalled();
		});
	}
});
