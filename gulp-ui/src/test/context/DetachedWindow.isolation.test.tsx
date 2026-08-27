import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { MemoryRouter } from "react-router-dom";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import type { Note } from "@/entities/Note";
import type { Link } from "@/entities/Link";
import { DetachedWindow } from "@/context/DetachedWindow.context";
import { WindowBridge } from "@/lib/WindowBridge";
import { DataStore } from "@/store/DataStore";
import { documentIdentityRegistry } from "@/store/DocumentIdentityRegistry";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "detached-isolation-operation" as Operation.Id;
const CONTEXT_ID = "detached-isolation-context" as Context.Id;
const SOURCE_ID = "detached-isolation-source" as Source.Id;

let mockApp: App.Type;
const mockDocsById = new Map<Doc.Id, Doc.Type>();
const mockSetDialogsDocked = jest.fn();
const mockSpawnBanner = jest.fn();
const mockInfo = {
	app: null as App.Type | null,
	setInfoByKey: jest.fn(),
	setTimelineFrame: jest.fn(),
	setTimelineScale: jest.fn(),
	setTimelineTarget: jest.fn(),
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => ({
			Info: mockInfo,
			app: mockApp,
			spawnBanner: mockSpawnBanner,
			spawnDialog: jest.fn(),
			setDialogsDocked: mockSetDialogsDocked,
		}),
	},
}));

jest.mock("next-themes", () => ({
	useTheme: (): { theme: string } => ({ theme: "classic-dark" }),
}));

jest.mock("@/entities/Operation", () => ({
	Operation: {
		Entity: {
			selected: (app: App.Type): Operation.Type | undefined =>
				app.target.operations.find((operation) => operation.selected),
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: (): null => null,
		},
	},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			id: (_app: App.Type, id: Doc.Id): Doc.Type | undefined =>
				mockDocsById.get(id),
			ids: (_app: App.Type, ids: Doc.Id[]): Doc.Type[] =>
				ids
					.map((id) => mockDocsById.get(id))
					.filter((event): event is Doc.Type => Boolean(event)),
		},
	},
}));

jest.mock("@/ui/Note", () => ({
	NotePoint: {
		FetchEventBanner: (): null => null,
	},
}));

jest.mock("@/dialogs/Event.dialog", () => ({
	DisplayEventDialog: (): null => null,
}));

jest.mock("@/banners/FilterFile.banner", () => ({
	FilterFileBanner: (): null => null,
}));

jest.mock("@/utils", () => {
	const records = {
		notes: {
			key: "notes",
			kind: "notes",
			windowName: "GulpNotes-test",
			features: "width=800,height=600",
		},
		table: {
			key: "table",
			kind: "table",
			windowName: "GulpTable-test",
			features: "width=800,height=600",
		},
		dashboard: {
			key: "dashboard",
			kind: "dashboard",
			windowName: "GulpDashboard-test",
			features: "width=800,height=600",
		},
	};

	return {
		getPersistedDetachedWindowRecord: (kind: keyof typeof records) =>
			records[kind],
		persistDetachedWindow: jest.fn(),
		readPersistedDetachedWindows: (): never[] => [],
		removePersistedDetachedWindow: jest.fn(),
		resolveEventRenderColor: (): undefined => undefined,
	};
});

/**
 * Synchronous BroadcastChannel implementation used to observe coordinator
 * routing without browser scheduling noise.
 */
class FakeBroadcastChannel {
	public static readonly channels = new Map<
		string,
		Set<FakeBroadcastChannel>
	>();

	public onmessage: ((event: MessageEvent) => void) | null = null;
	public onmessageerror: ((event: MessageEvent) => void) | null = null;

	public constructor(public readonly name: string) {
		const peers =
			FakeBroadcastChannel.channels.get(name) ??
			new Set<FakeBroadcastChannel>();
		peers.add(this);
		FakeBroadcastChannel.channels.set(name, peers);
	}

	public postMessage(message: unknown): void {
		FakeBroadcastChannel.channels.get(this.name)?.forEach((peer) => {
			if (peer !== this) {
				peer.onmessage?.({ data: message } as MessageEvent);
			}
		});
	}

	public close(): void {
		FakeBroadcastChannel.channels.get(this.name)?.delete(this);
	}

	public addEventListener(): void {}
	public removeEventListener(): void {}
	public dispatchEvent(): boolean {
		return true;
	}
}

interface OpenedWindow {
	window: Window;
	focus: JestMock<void, []>;
	url: string;
}

/**
 * Creates the minimal selected operation/source state required for an active
 * detached-window coordinator.
 */
function createApp(): App.Type {
	const operation = {
		id: OPERATION_ID,
		name: "Operation",
		selected: true,
	} as Operation.Type;
	const context = {
		id: CONTEXT_ID,
		operation_id: OPERATION_ID,
		name: "Context",
	} as Context.Type;
	const source = {
		id: SOURCE_ID,
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		name: "Source",
		selected: true,
		pinned: false,
		total: 1,
		timestamp: { min: 0, max: 100 },
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
		settings: { frequency_sample: 0 },
	} as Source.Type;

	return {
		target: {
			operations: [operation],
			contexts: [context],
			files: [source],
			events: new Map(),
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		timeline: {
			frame: { min: 0, max: 100 },
			target: null,
			filter: "",
			scale: 1,
			isScrollReversed: false,
			dialogSize: 480,
			pointers: [],
			renderVersion: 0,
		},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: false,
			toasts: false,
		},
		general: {
			server: "",
			ws_id: "main-socket",
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: {
				byRequestId: new Map(),
				byFileId: new Map(),
			},
			tableViewSource: null,
		},
		settings: {},
	} as App.Type;
}

/**
 * Captures the coordinator API without coupling assertions to rendered markup.
 */
function CaptureCoordinator({
	onCapture,
}: {
	onCapture: (value: ReturnType<typeof DetachedWindow.use>) => void;
}) {
	onCapture(DetachedWindow.use());
	return null;
}

describe("DetachedWindow coordinator client isolation", () => {
	const originalBroadcastChannel = globalThis.BroadcastChannel;
	const originalRequestIdleCallback = window.requestIdleCallback;
	const originalCancelIdleCallback = window.cancelIdleCallback;
	const originalWindowOpen = window.open;
	const originalWindowFocus = window.focus;
	const retainedBridges: WindowBridge.Bridge[] = [];
	const idleCallbacks = new Map<number, IdleRequestCallback>();
	const openedWindows: OpenedWindow[] = [];
	let nextIdleId = 1;
	let container: HTMLDivElement;
	let root: Root;
	let coordinator: ReturnType<typeof DetachedWindow.use> | null;

	/**
	 * Retains a bridge so the test closes every channel endpoint.
	 */
	function retain<TBridge extends WindowBridge.Bridge>(
		bridge: TBridge,
	): TBridge {
		retainedBridges.push(bridge);
		return bridge;
	}

	/**
	 * Flushes the coordinator's coalesced state projection.
	 */
	function flushIdleCallbacks(): void {
		act(() => {
			const callbacks = [...idleCallbacks.values()];
			idleCallbacks.clear();
			callbacks.forEach((callback) =>
				callback({
					didTimeout: false,
					timeRemaining: () => 50,
				}),
			);
		});
	}

	/**
	 * Settles bounded EventStore reads and flushes their coalesced dialog update.
	 * @returns Promise resolved after pending detached projections are published.
	 */
	async function flushEventStoreReads(): Promise<void> {
		await act(async () => {
			await Promise.resolve();
			await Promise.resolve();
		});
		flushIdleCallbacks();
	}

	/**
	 * Registers an already-opened route as one detached coordinator client.
	 */
	function registerOpenedClient(
		opened: OpenedWindow,
		kind: WindowBridge.DetachedClientKind,
	): WindowBridge.Message[] {
		const url = new URL(opened.url);
		const clientId = url.searchParams.get("bridgeId");
		const coordinatorId = url.searchParams.get("coordinatorId");
		if (!clientId || !coordinatorId) {
			throw new Error(`Detached route lacks bridge identity: ${opened.url}`);
		}

		const messages: WindowBridge.Message[] = [];
		retain(
			WindowBridge.createClient(
				clientId,
				clientId,
				(message) => messages.push(message),
				coordinatorId,
			),
		);
		const controlSender = retain(
			WindowBridge.create(clientId, () => {}, coordinatorId),
		);
		act(() => {
			controlSender.send(WindowBridge.MessageType.DETACHED_READY, {
				kind,
				needsBootstrap: true,
			});
		});
		flushIdleCallbacks();
		expect(
			messages.some(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_BOOTSTRAP,
			),
		).toBe(true);
		return messages;
	}

	/**
	 * Returns the popup produced by one coordinator open action.
	 */
	function newlyOpenedWindow(action: () => void): OpenedWindow {
		const previousCount = openedWindows.length;
		act(action);
		const opened = openedWindows.at(-1);
		if (!opened || openedWindows.length !== previousCount + 1) {
			throw new Error("Expected one new detached popup");
		}
		return opened;
	}

	beforeEach(() => {
		FakeBroadcastChannel.channels.clear();
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: FakeBroadcastChannel,
		});
		Object.defineProperty(window, "requestIdleCallback", {
			configurable: true,
			value: (callback: IdleRequestCallback): number => {
				const id = nextIdleId++;
				idleCallbacks.set(id, callback);
				return id;
			},
		});
		Object.defineProperty(window, "cancelIdleCallback", {
			configurable: true,
			value: (id: number): void => {
				idleCallbacks.delete(id);
			},
		});

		openedWindows.length = 0;
		mockSetDialogsDocked.mock.calls.splice(0);
		mockSpawnBanner.mock.calls.splice(0);
		Object.defineProperty(window, "open", {
			configurable: true,
			value: jest.fn<
				Window | null,
				[url?: string | URL, target?: string, features?: string]
			>((url) => {
				const focus = jest.fn();
				const popup = {
					closed: false,
					focus,
					close: jest.fn(),
					addEventListener: jest.fn(),
				} as unknown as Window;
				openedWindows.push({
					window: popup,
					focus,
					url: String(url),
				});
				return popup;
			}),
		});
		Object.defineProperty(window, "focus", {
			configurable: true,
			value: jest.fn(),
		});

		localStorage.setItem("__token", "detached-isolation-token");
		mockApp = createApp();
		mockInfo.app = mockApp;
		mockDocsById.clear();
		documentIdentityRegistry.clearDocumentReferences();
		DataStore.events.clear();
		DataStore.notes = [];
		DataStore.links = [];
		coordinator = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
		act(() => {
			root.render(
				<MemoryRouter
					initialEntries={[`/operations/${OPERATION_ID}`]}
					future={{
						v7_relativeSplatPath: true,
						v7_startTransition: true,
					}}
				>
					<DetachedWindow.Provider>
						<CaptureCoordinator
							onCapture={(value) => {
								coordinator = value;
							}}
						/>
					</DetachedWindow.Provider>
				</MemoryRouter>,
			);
		});
	});

	afterEach(() => {
		act(() => root.unmount());
		retainedBridges.splice(0).forEach((bridge) => bridge.destroy());
		container.remove();
		idleCallbacks.clear();
		localStorage.removeItem("__token");
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: originalBroadcastChannel,
		});
		Object.defineProperty(window, "requestIdleCallback", {
			configurable: true,
			value: originalRequestIdleCallback,
		});
		Object.defineProperty(window, "cancelIdleCallback", {
			configurable: true,
			value: originalCancelIdleCallback,
		});
		Object.defineProperty(window, "open", {
			configurable: true,
			value: originalWindowOpen,
		});
		Object.defineProperty(window, "focus", {
			configurable: true,
			value: originalWindowFocus,
		});
	});

	it("does not replay Notes state when other clients register or refocus", async () => {
		if (!coordinator) throw new Error("Coordinator was not captured");

		const notesWindow = newlyOpenedWindow(() =>
			coordinator!.openNotesWindow(),
		);
		const notesMessages = registerOpenedClient(notesWindow, "notes");
		notesMessages.length = 0;

		const tableWindow = newlyOpenedWindow(() =>
			coordinator!.openTableWindow(SOURCE_ID),
		);
		const tableMessages = registerOpenedClient(tableWindow, "table");
		expect(notesMessages).toEqual([]);
		act(() => coordinator!.openTableWindow(SOURCE_ID));
		expect(tableWindow.focus).toHaveBeenCalledTimes(1);
		expect(
			tableMessages.some(
				(message) =>
					message.type === WindowBridge.MessageType.TABLE_SELECT_SOURCE,
			),
		).toBe(true);
		expect(notesMessages).toEqual([]);

		const dashboardWindow = newlyOpenedWindow(() =>
			coordinator!.openDashboardWindow(),
		);
		registerOpenedClient(dashboardWindow, "dashboard");
		expect(notesMessages).toEqual([]);
		act(() => coordinator!.openDashboardWindow());
		expect(dashboardWindow.focus).toHaveBeenCalledTimes(1);
		expect(notesMessages).toEqual([]);

		const assistantTarget = {
			key: "built-in",
			title: "Assistant",
		};
		const assistantWindow = newlyOpenedWindow(() =>
			coordinator!.openAssistantWindow(assistantTarget),
		);
		registerOpenedClient(assistantWindow, "assistant");
		expect(notesMessages).toEqual([]);
		act(() => coordinator!.openAssistantWindow(assistantTarget));
		expect(assistantWindow.focus).toHaveBeenCalledTimes(1);
		expect(notesMessages).toEqual([]);

		const sourceEvents = Array.from({ length: 13 }, (_, index) => ({
			_id: `dialog-event-${13 - index}` as Doc.Id,
			"gulp.source_id": SOURCE_ID,
			"gulp.event_code": 200 + index,
			gulp_timestamp: 13 - index,
			number_hash: index,
		} as Doc.Type));
		sourceEvents.forEach((event) => mockDocsById.set(event._id, event));
		documentIdentityRegistry.indexDocuments(SOURCE_ID, sourceEvents);
		DataStore.replaceSourceEvents(SOURCE_ID, sourceEvents);
		const firstEvent = sourceEvents[6];
		const relatedEvent = sourceEvents[0];
		DataStore.links = [
			{
				id: "dialog-link" as Link.Id,
				type: "link",
				operation_id: OPERATION_ID,
				owner_user_id: "dialog-user",
				description: "",
				tags: [],
				doc_id_from: firstEvent._id,
				doc_ids: [relatedEvent._id],
				name: "Dialog link",
				color: "#fff",
				glyph_id: "dialog-glyph" as never,
			},
		];
		const originalGetByIds = DataStore.Events.getByIds.bind(DataStore.Events);
		const originalGetNeighbors =
			DataStore.Events.getNeighbors.bind(DataStore.Events);
		const getByIdsCalls: Doc.Id[][] = [];
		const getNeighborsCalls: Array<
			Parameters<typeof DataStore.Events.getNeighbors>
		> = [];
		DataStore.Events.getByIds = async (documentIds): Promise<Doc.Type[]> => {
			getByIdsCalls.push([...documentIds]);
			return originalGetByIds(documentIds);
		};
		DataStore.Events.getNeighbors = async (...args) => {
			getNeighborsCalls.push(args);
			return originalGetNeighbors(...args);
		};
		const dialogWindow = newlyOpenedWindow(() => {
			coordinator!.openDialogWindow(
				{ kind: "event", event: firstEvent },
				"width=640,height=640",
			);
		});
		const dialogMessages = registerOpenedClient(dialogWindow, "dialog");
		await flushEventStoreReads();
		const dialogBootstrap = dialogMessages.find(
			(message) =>
				message.type === WindowBridge.MessageType.DETACHED_BOOTSTRAP,
		);
		const hydratedDialog = dialogMessages.toReversed().find(
			(message) => message.type === WindowBridge.MessageType.DIALOG_CHANGED,
		);
		const navigation = hydratedDialog
			? (hydratedDialog.payload as WindowBridge.DialogChangedPayload).navigation
			: (
					dialogBootstrap?.payload as WindowBridge.DetachedBootstrapPayload
				).dialog?.navigation;
		const hydratedPayload = hydratedDialog?.payload as
			| WindowBridge.DialogChangedPayload
			| undefined;
		expect(
			getByIdsCalls.some(
				(documentIds) =>
					documentIds.length === 1 && documentIds[0] === relatedEvent._id,
			),
		).toBe(true);
		expect(
			getNeighborsCalls.some(
				([sourceId, documentId, before, after]) =>
					sourceId === SOURCE_ID &&
					documentId === firstEvent._id &&
					before === 5 &&
					after === 5,
			),
		).toBe(true);
		expect(hydratedPayload?.events.map((event) => event._id)).toEqual([
			firstEvent._id,
			relatedEvent._id,
		]);
		expect(navigation?.items).toHaveLength(
			WindowBridge.DETACHED_DIALOG_NAVIGATION_WINDOW_SIZE,
		);
		expect(
			navigation?.items.map((item) => item?.id ?? null),
		).toEqual(
			sourceEvents
				.slice(1, 12)
				.toReversed()
				.map((event) => event._id),
		);
		expect(navigation?.previousEventId).toBe(sourceEvents[7]._id);
		expect(navigation?.nextEventId).toBe(sourceEvents[5]._id);
		expect(notesMessages).toEqual([]);
		act(() => dialogWindow.window.focus());
		expect(dialogWindow.focus).toHaveBeenCalledTimes(1);
		expect(notesMessages).toEqual([]);
		dialogMessages.length = 0;

		act(() => {
			DataStore.upsertNote({
				id: "unrelated-note" as Note.Id,
				type: "note",
				operation_id: OPERATION_ID,
				context_id: CONTEXT_ID,
				name: "Unrelated",
				text: "",
				tags: [],
				color: null,
				source_id: SOURCE_ID,
				time_pin: 0,
				owner_user_id: "user" as never,
				glyph_id: "glyph" as never,
				edits: [],
				doc: {
					_id: "unrelated-event" as Doc.Id,
					"gulp.source_id": SOURCE_ID,
				} as Doc.Type,
			} as Note.Type);
		});
		flushIdleCallbacks();
		expect(dialogMessages).toEqual([]);

		act(() => {
			DataStore.upsertNote({
				id: "dialog-note" as Note.Id,
				type: "note",
				operation_id: OPERATION_ID,
				context_id: CONTEXT_ID,
				name: "Dialog note",
				text: "",
				tags: [],
				color: null,
				source_id: SOURCE_ID,
				time_pin: 0,
				owner_user_id: "user" as never,
				glyph_id: "glyph" as never,
				edits: [],
				doc: firstEvent,
			} as Note.Type);
		});
		flushIdleCallbacks();
		const noteDialogMessage = dialogMessages.find(
			(message) =>
				message.type === WindowBridge.MessageType.DIALOG_CHANGED,
		);
		expect(noteDialogMessage).not.toBeUndefined();
		expect(
			(
				noteDialogMessage?.payload as WindowBridge.DialogChangedPayload
			).navigation?.items.find((item) => item?.id === firstEvent._id)
				?.hasNotes,
		).toBe(true);
		dialogMessages.length = 0;

		const dialogUrl = new URL(dialogWindow.url);
		const dialogClientId = dialogUrl.searchParams.get("bridgeId");
		const coordinatorId = dialogUrl.searchParams.get("coordinatorId");
		if (!dialogClientId || !coordinatorId) {
			throw new Error("Dialog route lacks bridge identity");
		}
		const actionSender = retain(
			WindowBridge.createClient(
				dialogClientId,
				dialogClientId,
				() => {},
				coordinatorId,
			),
		);
		act(() => {
			actionSender.send(WindowBridge.MessageType.EVENT_TARGET_REQUEST, {
				docId: sourceEvents[5]._id,
			});
			actionSender.send(WindowBridge.MessageType.NAVIGATE_TO_EVENT, {
				docId: "missing-dialog-event" as Doc.Id,
				operationId: OPERATION_ID,
			});
			actionSender.send(WindowBridge.MessageType.DOCK_DIALOG, {});
		});
		await flushEventStoreReads();
		expect(
			(
				mockInfo.setTimelineTarget.mock.calls as unknown as Array<
					[Doc.Type | null]
				>
			).at(-1)?.[0],
		).toBe(sourceEvents[5]);
		expect(mockSpawnBanner).toHaveBeenCalledTimes(1);
		expect(window.focus).toHaveBeenCalledTimes(1);
		expect(
			(mockSetDialogsDocked.mock.calls as unknown as unknown[][]).some(
				(call) => call[0] === true,
			),
		).toBe(true);
		expect(notesMessages).toEqual([]);

		const secondEvent = {
			...firstEvent,
			_id: "dialog-event-two" as Doc.Id,
			gulp_timestamp: 2,
		};
		mockDocsById.set(secondEvent._id, secondEvent);
		act(() => {
			coordinator!.openDialogWindow(
				{ kind: "event", event: secondEvent },
				"width=640,height=640",
			);
		});
		expect(
			dialogMessages.some(
				(message) =>
					message.type === WindowBridge.MessageType.DIALOG_CHANGED,
			),
		).toBe(true);
		expect(notesMessages).toEqual([]);
		DataStore.Events.getByIds = originalGetByIds;
		DataStore.Events.getNeighbors = originalGetNeighbors;
	});
});
