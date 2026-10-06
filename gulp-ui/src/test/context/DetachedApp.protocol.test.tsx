import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { App } from "@/entities/App";
import { WindowBridge } from "@/lib/WindowBridge";
import { DetachedAppProvider } from "@/context/DetachedApp.provider";
import { Doc } from "@/entities/Doc";
import { DataStore } from "@/store/DataStore";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type { Link } from "@/entities/Link";

const mockSocketConstruct = jest.fn<
	void,
	[string, unknown?, string?]
>();
const mockSocketDestroy = jest.fn<void, [string]>();
let mockOwnsIndependentDataStore = false;
let mockApplicationContextValue: {
	app: App.Type;
	detachedEventDetail: Doc.Type | null;
	setDetachedNotesRenderedSync: (enabled: boolean) => void;
	requestMainEventAction: (
		payload: WindowBridge.OpenEventActionRequestPayload,
	) => boolean;
} | null = null;

jest.mock("@/entities/App", () => ({
	App: {
		Base: {
			target: {
				operations: [],
				contexts: [],
				files: [],
				events: new Map(),
				notes: [],
				links: [],
				highlights: [],
				glyphs: [],
				mappings: [],
				plugins: [],
			},
			timeline: {
				frame: { min: 0, max: 0 },
				target: null,
				filter: "",
				scale: 1,
				isScrollReversed: false,
				dialogSize: 320,
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
				server: "https://gulp.test",
				ws_id: "",
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
		},
	},
}));

jest.mock("@/context/Application.context", () => ({
	Application: {
		Context: {
			Provider: ({
				children,
				value,
			}: {
				children: ReactNode;
				value: typeof mockApplicationContextValue;
			}) => {
				mockApplicationContextValue = value;
				return children;
			},
		},
	},
}));

jest.mock("@/class/Info", () => ({
	Info: class MockInfo {
		public app: unknown;
		public setInfo: unknown;

		public constructor({
			app,
			setInfo,
		}: {
			app: unknown;
			setInfo: unknown;
		}) {
			this.app = app;
			this.setInfo = setInfo;
		}

		public setLoading(): void {}
		public delLoading(): void {}
		public notes_upsert_from_collab(): void {}
	},
}));

jest.mock("@/class/SmartSocket", () => {
	class MockSmartSocket {
		public static instance: MockSmartSocket | undefined;
		public readonly mockWsId: string;

		public constructor(
			mockWsId: string,
			mockUserId?: unknown,
			mockCacheNamespaceId?: string,
		) {
			this.mockWsId = mockWsId;
			mockSocketConstruct(mockWsId, mockUserId, mockCacheNamespaceId);
			MockSmartSocket.instance = this;
		}

		public destroy(): void {
			mockSocketDestroy(this.mockWsId);
			if (MockSmartSocket.instance === this) {
				MockSmartSocket.instance = undefined;
			}
		}
	}

	return {
		SmartSocket: {
			Class: MockSmartSocket,
		},
	};
});

jest.mock("@/class/RenderEngine", () => ({
	RenderEngine: {
		reset: jest.fn(),
		resetSourceNotes: jest.fn(),
	},
}));

jest.mock("@/entities/Color", () => ({
	Color: {
		Themer: {
			setTheme: jest.fn(),
		},
	},
}));

jest.mock("@/entities/Operation", () => ({
	Operation: {
		Entity: {
			selected: (app: {
				target: { operations: Array<{ selected?: boolean }> };
			}) => app.target.operations.find((operation) => operation.selected),
		},
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {
			invalidateCache: jest.fn(),
			removeIndexedNotes: jest.fn(),
		},
	},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			clearIndex: jest.fn(),
			add: jest.fn(),
			flag: {
				KEY: "gulp-test-flags",
				invalidateCache: jest.fn(),
			},
		},
	},
}));

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		events: new Map(),
		getRenderOperationId: jest.fn(() => null),
		resetRenderOperation: jest.fn(),
		openEventStoreNamespace: jest.fn(),
		replaceNotes: jest.fn(),
		replaceLinks: jest.fn(),
		upsertLink: jest.fn(),
		deleteLinks: jest.fn(() => []),
		relayLinksRealtimeDelta: jest.fn(),
		publishNotesSnapshotReady: jest.fn(),
		deleteNotes: () => [],
		relayNotesRealtimeDelta: jest.fn(),
	},
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Context: {
			Provider: ({ children }: { children: ReactNode }) => children,
		},
		Provider: ({ children }: { children: ReactNode }) => children,
	},
}));

jest.mock("@/context/Boundary.context", () => ({
	Boundary: {
		Provider: ({ children }: { children: ReactNode }) => children,
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		Provider: ({ children }: { children: ReactNode }) => children,
		use: () => ({ t: (key: string) => key }),
	},
}));

jest.mock("@/context/detachedDataStore", () => ({
	ownsIndependentDetachedDataStore: () =>
		mockOwnsIndependentDataStore,
}));

jest.mock("@/context/detachedTheme", () => ({
	applyDetachedThemeSnapshot: (
		_document: Document,
		payload: WindowBridge.ThemeChangePayload,
	) => new Set(Object.keys(payload.cssVariables)),
}));

type MockCalls<TArguments extends unknown[]> = {
	mock: { calls: TArguments[] };
};
const mockDocClearIndex =
	Doc.Entity.clearIndex as unknown as MockCalls<[]>;
const mockDocAdd =
	Doc.Entity.add as unknown as MockCalls<[unknown, Doc.Type[]]>;
const mockReplaceNotes =
	DataStore.replaceNotes as unknown as MockCalls<[unknown[], string]>;
const mockReplaceLinks =
	DataStore.replaceLinks as unknown as MockCalls<[unknown[], string]>;
const mockUpsertLink =
	DataStore.upsertLink as unknown as MockCalls<[Link.Type, string]>;
const mockDeleteLinks =
	DataStore.deleteLinks as unknown as MockCalls<[Link.Id[], string]>;
const mockRelayLinksRealtimeDelta =
	DataStore.relayLinksRealtimeDelta as unknown as MockCalls<[
		WindowBridge.LinksDeltaPayload,
	]>;
const mockResetRenderOperation =
	DataStore.resetRenderOperation as unknown as MockCalls<[Operation.Id | null]>;
const mockOpenEventStoreNamespace =
	DataStore.openEventStoreNamespace as unknown as MockCalls<[
		{ cacheNamespaceId: string; operationId: Operation.Id },
	]>;

/**
 * In-memory BroadcastChannel used by the mounted provider and test coordinator.
 */
class FakeBroadcastChannel {
	public static readonly channels = new Map<
		string,
		Set<FakeBroadcastChannel>
	>();
	public static readonly createdNames: string[] = [];

	public onmessage: ((event: MessageEvent) => void) | null = null;
	public onmessageerror: ((event: MessageEvent) => void) | null = null;

	public constructor(public readonly name: string) {
		FakeBroadcastChannel.createdNames.push(name);
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

describe("DetachedAppProvider bridge lifecycle", () => {
	const coordinatorId = "main-coordinator";
	const clientId = "notes-client";
	const originalBroadcastChannel = globalThis.BroadcastChannel;
	const controlMessages: WindowBridge.Message[] = [];
	const privateMessages: WindowBridge.Message[] = [];
	let mainControlBridge: WindowBridge.Bridge;
	let mainPrivateBridge: WindowBridge.Bridge;
	let container: HTMLDivElement;
	let root: Root;

	beforeAll(() => {
		(globalThis as typeof globalThis & {
			IS_REACT_ACT_ENVIRONMENT: boolean;
		}).IS_REACT_ACT_ENVIRONMENT = true;
	});

	beforeEach(() => {
		jest.useFakeTimers();
		FakeBroadcastChannel.channels.clear();
		FakeBroadcastChannel.createdNames.splice(0);
		controlMessages.splice(0);
		privateMessages.splice(0);
		mockSocketConstruct.mock.calls.splice(0);
		mockSocketDestroy.mock.calls.splice(0);
		mockOwnsIndependentDataStore = false;
		mockApplicationContextValue = null;
		mockDocClearIndex.mock.calls.splice(0);
		mockDocAdd.mock.calls.splice(0);
		mockReplaceNotes.mock.calls.splice(0);
		mockReplaceLinks.mock.calls.splice(0);
		mockUpsertLink.mock.calls.splice(0);
		mockDeleteLinks.mock.calls.splice(0);
		mockRelayLinksRealtimeDelta.mock.calls.splice(0);
		mockResetRenderOperation.mock.calls.splice(0);
		mockOpenEventStoreNamespace.mock.calls.splice(0);
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: FakeBroadcastChannel,
		});
		localStorage.removeItem(
			WindowBridge.getMainContextStorageKey(coordinatorId),
		);
		mainControlBridge = WindowBridge.create(
			"main",
			(message) => controlMessages.push(message),
			coordinatorId,
		);
		mainPrivateBridge = WindowBridge.createClient(
			clientId,
			"main",
			(message) => privateMessages.push(message),
			coordinatorId,
		);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		mainPrivateBridge.destroy();
		mainControlBridge.destroy();
		container.remove();
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: originalBroadcastChannel,
		});
		jest.useRealTimers();
	});

	it("requests bootstrap on control, then heartbeats on the private channel", () => {
		const getReadyMessages = (): WindowBridge.Message[] =>
			controlMessages.filter(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_READY,
			);

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>active detached app</div>
				</DetachedAppProvider>,
			);
		});

		expect(FakeBroadcastChannel.createdNames.slice(-2)).toEqual([
			`gulp-window-bridge:client:${coordinatorId}:${clientId}`,
			`gulp-window-bridge:control:${coordinatorId}`,
		]);
		expect(getReadyMessages()[0]).toEqual({
			type: WindowBridge.MessageType.DETACHED_READY,
				payload: {
					kind: "notes",
					needsBootstrap: true,
					notesRenderedSyncEnabled: false,
				},
			senderId: clientId,
		});

		act(() => jest.advanceTimersByTime(1_000));
		expect(getReadyMessages()[1]).toMatchObject({
			type: WindowBridge.MessageType.DETACHED_READY,
			payload: {
				kind: "notes",
				needsBootstrap: true,
			},
			senderId: clientId,
		});

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {
							"--accent": "#ffffff",
							"--theme-mode": "dark",
						},
					},
				},
			);
		});

		expect(container.textContent?.includes("active detached app")).toBe(true);
		act(() => jest.advanceTimersByTime(29_999));
		expect(getReadyMessages()).toHaveLength(2);
		expect(privateMessages).toHaveLength(0);
		act(() => jest.advanceTimersByTime(1));
		expect(getReadyMessages()).toHaveLength(2);
		expect(privateMessages[0]).toEqual({
			type: WindowBridge.MessageType.DETACHED_HEARTBEAT,
			payload: {},
			senderId: clientId,
		});

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.MAIN_CONTEXT_STATUS,
				{ status: "idle" },
			);
		});
		act(() => jest.advanceTimersByTime(60_000));
		expect(getReadyMessages()).toHaveLength(2);
		expect(privateMessages).toHaveLength(1);
		expect(container.textContent?.includes("detachedApp.idle")).toBe(true);
	});

	it("applies authoritative Links snapshots once in the independent Notes realm", () => {
		mockOwnsIndependentDataStore = true;
		const link = {
			id: "notes-link" as Link.Id,
			type: "link",
			operation_id: "notes-operation" as Operation.Id,
			doc_id_from: "origin-event" as Doc.Id,
			doc_ids: [],
			owner_user_id: "user",
			description: "Description",
			tags: [],
			color: "#123456",
			glyph_id: "glyph" as Link.Type["glyph_id"],
		} as Link.Type;
		const linksPayload: WindowBridge.LinksChangedPayload = {
			operationId: null,
			links: [link],
		};

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>notes links</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
				},
			);
			mainPrivateBridge.send(
				WindowBridge.MessageType.LINKS_CHANGED,
				linksPayload,
			);
			mainPrivateBridge.send(
				WindowBridge.MessageType.LINKS_CHANGED,
				linksPayload,
			);
		});

		expect(mockReplaceLinks.mock.calls).toEqual([[[link], "deferred"]]);
	});

	it("applies and relays realtime Links deltas in the independent Notes realm", () => {
		mockOwnsIndependentDataStore = true;
		const operationId = "notes-delta-operation" as Operation.Id;
		const link = {
			id: "notes-delta-link" as Link.Id,
			type: "link",
			operation_id: operationId,
			doc_id_from: "origin-event" as Doc.Id,
			doc_ids: [],
			owner_user_id: "user",
			description: "Description",
			tags: [],
			color: "#123456",
			glyph_id: "glyph" as Link.Type["glyph_id"],
		} as Link.Type;
		const createDelta: WindowBridge.LinksDeltaPayload = {
			type: "links-sync",
			mutation: "create",
			operationId,
			sequence: 1,
			links: [link],
		};
		const deleteDelta: WindowBridge.LinksDeltaPayload = {
			type: "links-sync",
			mutation: "delete",
			operationId,
			sequence: 2,
			linkIds: [link.id],
		};

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>notes links delta</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
					context: {
						operation: {
							id: operationId,
							name: "Notes operation",
							selected: true,
						} as Operation.Type,
						contexts: [],
						sources: [],
						cacheNamespaceId: null,
					},
				},
			);
			mainPrivateBridge.send(
				WindowBridge.MessageType.LINKS_DELTA,
				createDelta,
			);
			mainPrivateBridge.send(
				WindowBridge.MessageType.LINKS_DELTA,
				deleteDelta,
			);
		});

		expect(mockUpsertLink.mock.calls).toEqual([[link, "deferred"]]);
		expect(mockDeleteLinks.mock.calls).toEqual([[[link.id], "deferred"]]);
		expect(mockRelayLinksRealtimeDelta.mock.calls).toEqual([
			[createDelta],
			[deleteDelta],
		]);
	});

	it("accepts the same rendered sources after visible-only sync is re-enabled", () => {
		const renderedSourceId = "rendered-source" as Source.Id;
		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>notes detached app</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
				},
			);
			mockApplicationContextValue?.setDetachedNotesRenderedSync(true);
			mainPrivateBridge.send(
				WindowBridge.MessageType.RENDERED_SOURCES_CHANGED,
				{ sourceIds: [renderedSourceId] },
			);
		});
		expect(mockApplicationContextValue?.app.timeline.renderedSourceIds).toEqual([
			renderedSourceId,
		]);

		act(() => {
			mockApplicationContextValue?.setDetachedNotesRenderedSync(false);
			mockApplicationContextValue?.setDetachedNotesRenderedSync(true);
		});
		expect(mockApplicationContextValue?.app.timeline.renderedSourceIds).toEqual(
			[],
		);

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.RENDERED_SOURCES_CHANGED,
				{ sourceIds: [renderedSourceId] },
			);
		});
		expect(mockApplicationContextValue?.app.timeline.renderedSourceIds).toEqual([
			renderedSourceId,
		]);
	});

	it("hydrates compact Table source and query projections to equivalent UI state", () => {
		const operationId = "table-operation" as Operation.Id;
		const sourceId =
			"table-source" as WindowBridge.DetachedTableSourceSummary["id"];
		const contextId =
			"table-context" as WindowBridge.DetachedTableSourceSummary["context_id"];
		const payload: WindowBridge.DetachedBootstrapPayload = {
			status: "active",
			theme: {
				theme: "classic-dark",
				mode: "dark",
				cssVariables: {},
			},
			context: {
				operation: {
					id: operationId,
					name: "Table operation",
					selected: true,
				} as Operation.Type,
				contexts: [
					{
						id: contextId,
						operation_id: operationId,
						type: "context",
						name: "Table context",
						color: "#123456",
						selected: true,
					},
				],
				sources: [
					{
						projection: "table",
						id: sourceId,
						operation_id: operationId,
						context_id: contextId,
						type: "source",
						name: "Table source",
						selected: true,
						pinned: true,
						glyph_id: null as never,
						plugin: "table-plugin",
						time_updated: 42,
						timestamp: { min: 1, max: 2 },
						nanotimestamp: { min: "1000000", max: "2000000" },
						total: 331_000,
						settings: {
							offset: 250,
							field: "_id",
							hash_function: "fnv1a",
							frequency_sample: 1000,
						},
					},
				],
				cacheNamespaceId: null,
			},
			queryContext: {
				filters: {
					[sourceId]: {
						string: "",
						filters: [],
						text_filter: "needle",
					},
				},
				frame: { min: 10, max: 20 },
			},
		};

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="table"
				>
					<div>table detached app</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				payload,
			);
		});

		const hydratedSource = mockApplicationContextValue?.app.target.files[0];
		expect(hydratedSource).toMatchObject({
			id: sourceId,
			plugin: "table-plugin",
			time_updated: 42,
			pinned: true,
		});
		expect(hydratedSource?.appliedQuery).toMatchObject({
			text_filter: "needle",
		});
		expect(hydratedSource?.settings).toMatchObject({
			offset: 250,
			field: "_id",
			hash_function: "fnv1a",
		});
		expect(hydratedSource?.nanotimestamp).toEqual({
			min: 1_000_000n,
			max: 2_000_000n,
		});
		expect(hydratedSource?.queryCache !== undefined).toBe(true);
		expect(hydratedSource?.granted_user_ids).toEqual([]);
		expect(hydratedSource?._sampleDataCached.sample_data).toBeNull();
		expect(mockApplicationContextValue?.app.timeline.frame).toEqual({
			min: 10,
			max: 20,
		});
	});

	it("does not duplicate READY when the stored main status is already active", () => {
		WindowBridge.persistMainStatus("active", coordinatorId);

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>detached app</div>
				</DetachedAppProvider>,
			);
		});

		expect(
			controlMessages.filter(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_READY,
			),
		).toHaveLength(1);
	});

	it("lets the live active bootstrap win over the delayed storage fallback", () => {
		const readyMessages = () =>
			controlMessages.filter(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_READY,
			);
		const mainStatusStorageKey =
			WindowBridge.getMainContextStorageKey(coordinatorId);

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>detached app</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "initializing",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
				},
			);
		});

		act(() => {
			WindowBridge.persistMainStatus("active", coordinatorId);
			window.dispatchEvent(
				new StorageEvent("storage", {
					key: mainStatusStorageKey,
					newValue: localStorage.getItem(mainStatusStorageKey),
				}),
			);
			jest.advanceTimersByTime(249);
		});
		expect(readyMessages()).toHaveLength(1);

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
				},
			);
			jest.advanceTimersByTime(1);
		});

		expect(readyMessages()).toHaveLength(1);
		expect(container.textContent?.includes("detached app")).toBe(true);
	});

	it("requests recovery from storage when the live bootstrap is absent", () => {
		const readyMessages = () =>
			controlMessages.filter(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_READY,
			);
		const mainStatusStorageKey =
			WindowBridge.getMainContextStorageKey(coordinatorId);

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="dashboard"
				>
					<div>detached app</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "initializing",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
				},
			);
			WindowBridge.persistMainStatus("active", coordinatorId);
			window.dispatchEvent(
				new StorageEvent("storage", {
					key: mainStatusStorageKey,
					newValue: localStorage.getItem(mainStatusStorageKey),
				}),
			);
			jest.advanceTimersByTime(249);
		});
		expect(readyMessages()).toHaveLength(1);

		act(() => jest.advanceTimersByTime(1));
		expect(readyMessages()).toHaveLength(2);
		expect(readyMessages()[1].payload).toMatchObject({
			needsBootstrap: true,
		});
	});

	it("stops reconnect chatter after an authoritative initializing bootstrap", () => {
		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="notes"
				>
					<div>detached app</div>
				</DetachedAppProvider>,
			);
		});

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "initializing",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
				},
			);
		});
		act(() => {
			jest.advanceTimersByTime(10_000);
		});
		const readyMessages = () =>
			controlMessages.filter(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_READY,
			);
		expect(readyMessages()).toHaveLength(1);

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.MAIN_CONTEXT_STATUS,
				{ status: "initializing", reconnectRequired: true },
			);
		});
		act(() => {
			jest.advanceTimersByTime(1_000);
		});
		expect(readyMessages()).toHaveLength(2);
		expect(readyMessages()[1].payload).toMatchObject({
			needsBootstrap: true,
		});
	});

	it("hydrates compact dialog events and collaboration state", () => {
		mockOwnsIndependentDataStore = true;
		const event = {
			_id: "dialog-event",
			"gulp.source_id": "dialog-source",
			gulp_timestamp: 100,
		} as Doc.Type;

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="dialog"
				>
					<div>dialog</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
					dialog: {
						dialog: { kind: "event", event },
						events: [event],
						notes: [],
						links: [],
						eventColors: {
							[event._id]: "#123456",
						},
						navigation: null,
					},
				},
			);
		});

		expect(mockDocClearIndex.mock.calls).toHaveLength(1);
		expect(mockDocAdd.mock.calls[0][1]).toEqual([event]);
		expect(mockReplaceNotes.mock.calls[0]).toEqual([[], "deferred"]);
		expect(mockReplaceLinks.mock.calls[0]).toEqual([[], "deferred"]);
		expect(container.textContent?.includes("dialog")).toBe(true);
	});

	it("sends Event actions privately and applies refreshed detail results", () => {
		const event = {
			_id: "dialog-action-event",
			"gulp.source_id": "dialog-source",
			gulp_timestamp: 100,
		} as Doc.Type;

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="dialog"
				>
					<div>dialog action</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
					dialog: {
						dialog: { kind: "event", event },
						events: [event],
						notes: [],
						links: [],
						eventColors: {},
						navigation: null,
					},
				},
			);
		});

		const actionPayload: WindowBridge.OpenEventActionRequestPayload = {
			eventId: event._id,
			action: { kind: "filter", field: "host.name", value: "server" },
		};
		act(() => {
			expect(
				mockApplicationContextValue?.requestMainEventAction(actionPayload),
			).toBe(true);
		});
		expect(
			privateMessages.some(
				(message) =>
					message.type ===
						WindowBridge.MessageType.OPEN_EVENT_ACTION_REQUEST &&
					message.senderId === clientId &&
					message.payload === actionPayload,
			),
		).toBe(true);

		const enrichedEvent = {
			...event,
			"enriched.field": "value",
		} as Doc.Type;
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.EVENT_DETAIL_CHANGED,
				{ event: enrichedEvent },
			);
		});
		expect(mockApplicationContextValue?.detachedEventDetail).toEqual(
			enrichedEvent,
		);
	});

	it("opens and closes an independent reader with the propagated user namespace", () => {
		mockOwnsIndependentDataStore = true;
		const operationId = "detached-operation" as Operation.Id;

		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="table"
				>
					<div>table</div>
				</DetachedAppProvider>,
			);
		});
		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
					context: {
						operation: {
							id: operationId,
							selected: true,
						} as Operation.Type,
						contexts: [],
						sources: [],
						cacheNamespaceId: "server:test:user:detached-user",
					},
				},
			);
		});

		expect(mockResetRenderOperation.mock.calls[0]).toEqual([operationId]);
		expect(mockOpenEventStoreNamespace.mock.calls[0]).toEqual([
			{
				cacheNamespaceId: "server:test:user:detached-user",
				operationId,
			},
		]);

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.MAIN_CONTEXT_STATUS,
				{ status: "auth_lost" },
			);
		});
		expect(
			mockResetRenderOperation.mock.calls.some(
				([selectedOperationId]) => selectedOperationId === null,
			),
		).toBe(true);
	});

	it("owns a private reactive SmartSocket only for assistant clients", () => {
		act(() => {
			root.render(
				<DetachedAppProvider
					initialApp={App.Base}
					bridgeId={clientId}
					coordinatorId={coordinatorId}
					kind="assistant"
					assistantQuerySyncEnabled
				>
					<div>assistant</div>
				</DetachedAppProvider>,
			);
		});

		expect(mockSocketConstruct).not.toHaveBeenCalled();
		expect(
			controlMessages.find(
				(message) =>
					message.type === WindowBridge.MessageType.DETACHED_READY,
			)?.payload,
		).toEqual({
			kind: "assistant",
			needsBootstrap: true,
			assistantQuerySyncEnabled: true,
		});

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				{
					status: "active",
					theme: {
						theme: "classic-dark",
						mode: "dark",
						cssVariables: {},
					},
					context: {
						operation: null,
						contexts: [],
						sources: [],
						cacheNamespaceId: "server:test:user:assistant-user",
					},
					wsId: "main-ws-one",
				},
			);
		});

		expect(mockSocketConstruct).toHaveBeenCalledTimes(1);
		const firstDetachedWsId = mockSocketConstruct.mock.calls[0][0] as string;
		expect(firstDetachedWsId).not.toBe("main-ws-one");
		expect(mockSocketConstruct.mock.calls[0][2]).toBe(
			"server:test:user:assistant-user",
		);

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.ASSISTANT_CONNECTION_CHANGED,
				{ wsId: "main-ws-two" },
			);
		});

		expect(mockSocketDestroy.mock.calls).toEqual([[firstDetachedWsId]]);
		expect(mockSocketConstruct).toHaveBeenCalledTimes(2);
		const secondDetachedWsId = mockSocketConstruct.mock.calls[1][0] as string;
		expect(secondDetachedWsId).not.toBe("main-ws-two");
		expect(secondDetachedWsId).not.toBe(firstDetachedWsId);

		act(() => {
			mainPrivateBridge.send(
				WindowBridge.MessageType.MAIN_CONTEXT_STATUS,
				{ status: "auth_lost" },
			);
		});
		expect(mockSocketDestroy.mock.calls).toEqual([
			[firstDetachedWsId],
			[secondDetachedWsId],
		]);
	});
});
