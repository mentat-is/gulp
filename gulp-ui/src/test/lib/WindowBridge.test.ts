import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import type { User } from "@/entities/User";
import { WindowBridge } from "@/lib/WindowBridge";
import { DataStore } from "@/store/DataStore";

const OPERATION_ID = "operation" as Operation.Id;
const CONTEXT_ID = "context" as Context.Id;

/**
 * Creates one selected operation fixture for detached snapshot tests.
 * @returns Selected operation entity.
 */
function createOperation(): Operation.Type {
	return {
		id: OPERATION_ID,
		name: "Operation",
		selected: true,
	} as Operation.Type;
}

/**
 * Creates one selected source fixture for detached snapshot tests.
 * @param id Stable source identifier.
 * @param name Source name used by timeline text filtering.
 * @param total Backend-advertised event total.
 * @returns Source entity fixture.
 */
function createSource(id: string, name: string, total = 10): Source.Type {
	return {
		id: id as Source.Id,
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		name,
		selected: true,
		pinned: false,
		total,
		timestamp: { min: 0, max: 100 },
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
		settings: {
			frequency_sample: 0,
			field: "_id",
			hash_function: "fnv1a",
		},
	} as Source.Type;
}

/**
 * Creates one event document fixture.
 * @param id Stable document identifier.
 * @param sourceId Owning source ID.
 * @returns Document entity fixture.
 */
function createDocument(id: string, sourceId: Source.Id): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: 100,
		"gulp.source_id": sourceId,
	} as Doc.Type;
}

/**
 * Creates one note fixture attached to a source event.
 * @param id Stable note identifier.
 * @param sourceId Owning source ID.
 * @returns Note entity fixture.
 */
function createNote(id: string, sourceId: Source.Id): Note.Type {
	return {
		id: id as Note.Id,
		type: "note",
		operation_id: OPERATION_ID,
		tags: [],
		name: id,
		context_id: CONTEXT_ID,
		color: null,
		source_id: sourceId,
		doc: createDocument(`${id}-doc`, sourceId),
		time_pin: 100,
		owner_user_id: "user" as User.Id,
		text: id,
		glyph_id: "glyph" as Glyph.Id,
		edits: [],
	} as Note.Type;
}

/**
 * Creates a minimal app fixture with a scale that must not cross the bridge.
 * @returns Application fixture.
 */
function createApp(): App.Type {
	return {
		target: {
			operations: [createOperation()],
			contexts: [
				{
					id: CONTEXT_ID,
					operation_id: OPERATION_ID,
					name: "Context",
					color: "#123456",
				} as Context.Type,
			],
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
			frame: { min: 100, max: 200 },
			target: null,
			filter: "needle",
			scale: 42,
			isScrollReversed: false,
			dialogSize: 320,
			pointers: [],
			renderVersion: 7,
		},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: true,
			toasts: false,
		},
		general: {
			server: "",
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
	} as App.Type;
}

describe("WindowBridge capability-scoped projections", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation(OPERATION_ID);
	});

	it("detects subscribers before scheduling capability-scoped state work", () => {
		const notesClient = {
			kind: "notes",
			tableSyncEnabled: false,
			notesRenderedSyncEnabled: false,
			assistantQuerySyncEnabled: false,
		} satisfies WindowBridge.DetachedClientCapabilities;
		const syncedTableClient = {
			...notesClient,
			kind: "table",
			tableSyncEnabled: true,
		} satisfies WindowBridge.DetachedClientCapabilities;
		const queryAssistantClient = {
			...notesClient,
			kind: "assistant",
			assistantQuerySyncEnabled: true,
		} satisfies WindowBridge.DetachedClientCapabilities;

		expect(
			WindowBridge.hasStateDomainSubscriber([notesClient], "status"),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber([notesClient], "context"),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber([notesClient], "event"),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber([notesClient], "query"),
		).toBe(false);
		expect(
			WindowBridge.hasStateDomainSubscriber([notesClient], "rendered"),
		).toBe(false);
		expect(
			WindowBridge.hasStateDomainSubscriber(
				[{ ...notesClient, notesRenderedSyncEnabled: true }],
				"rendered",
			),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber([syncedTableClient], "query"),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber(
				[queryAssistantClient],
				"query",
			),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber(
				[queryAssistantClient],
				"assistant",
			),
		).toBe(true);
		expect(
			WindowBridge.hasStateDomainSubscriber([notesClient], "dialog"),
		).toBe(false);
	});

	it("projects only selected operation context and serializable source metadata", () => {
		const app = createApp();
		const selectedSource = createSource("selected", "Selected");
		selectedSource.settings.color_override = {
			field: "process.name",
			values: { shell: "#fff" },
			lookup: { 123: "#fff" },
		};
		const unselectedSource = {
			...createSource("unselected", "Unselected"),
			selected: false,
		};
		app.target.files = [selectedSource, unselectedSource];

		const context = WindowBridge.createContextSnapshot(app, OPERATION_ID);

		expect(context.operation?.id).toBe(OPERATION_ID);
		expect(context.contexts.map((item) => item.id)).toEqual([CONTEXT_ID]);
		expect(context.sources.map((source) => source.id)).toEqual([
			selectedSource.id,
		]);
		const projectedSource = context.sources[0];
		if (!projectedSource || !("nanotimestamp" in projectedSource)) {
			throw new Error("Expected full detached source projection");
		}
		expect(projectedSource.nanotimestamp).toEqual({
			min: "0",
			max: "100000000",
		});
		expect(projectedSource._sampleDataCached.sample_data).toBeNull();
		expect(
			Object.prototype.hasOwnProperty.call(
				projectedSource.settings,
				"color_override",
			),
		).toBe(false);
		expect(Object.prototype.hasOwnProperty.call(context, "filters")).toBe(false);
	});

	it("projects only Notes source and context fields for Notes clients", () => {
		const app = createApp();
		const source = createSource("selected", "Selected");
		app.target.files = [source];

		const context = WindowBridge.createContextSnapshot(
			app,
			OPERATION_ID,
			{ compactNotes: true },
		);

		expect(context.sources).toEqual([
			{
				id: source.id,
				operation_id: source.operation_id,
				context_id: source.context_id,
				type: "source",
				name: source.name,
				color: source.color,
				selected: true,
				settings: {
					field: source.settings.field,
					hash_function: source.settings.hash_function,
				},
			},
		]);
		expect(context.contexts).toEqual([
			{
				id: CONTEXT_ID,
				operation_id: OPERATION_ID,
				type: "context",
				name: "Context",
				color: "#123456",
				selected: true,
			},
		]);
		expect(
			Object.prototype.hasOwnProperty.call(
				context.sources[0],
				"granted_user_ids",
			),
		).toBe(false);
		expect(
			Object.prototype.hasOwnProperty.call(
				context.sources[0],
				"_sampleDataCached",
			),
		).toBe(false);
		expect(
			Object.prototype.hasOwnProperty.call(context.sources[0], "total"),
		).toBe(false);
	});

	it("supports operation-only assistants and compact dialog user preferences", () => {
		const app = createApp();
		app.target.files = [createSource("selected", "Selected")];
		app.target.contexts.push({
			id: "unused-context" as Context.Id,
			operation_id: OPERATION_ID,
			name: "Unused",
		} as Context.Type);
		app.general.user = {
			id: "dialog-user" as User.Id,
			user_data: { show_preview: false, largePreference: "not-sent" },
		} as unknown as User.Type;

		const assistantContext = WindowBridge.createContextSnapshot(
			app,
			OPERATION_ID,
			{ operationOnly: true },
		);
		const dialogContext = WindowBridge.createContextSnapshot(
			app,
			OPERATION_ID,
			{ includeDialogUser: true },
		);

		expect(assistantContext.sources).toEqual([]);
		expect(assistantContext.contexts).toEqual([]);
		expect(assistantContext.user).toBeUndefined();
		expect(dialogContext.contexts.map((context) => context.id)).toEqual([
			CONTEXT_ID,
		]);
		expect(dialogContext.user).toEqual({
			id: app.general.user.id,
			showPreview: false,
		});
	});

	it("projects frame and selected-source filters without canvas scale", () => {
		const app = createApp();
		const selectedSource = createSource("selected", "Selected");
		const unselectedSource = {
			...createSource("unselected", "Unselected"),
			selected: false,
		};
		app.target.files = [selectedSource, unselectedSource];
		selectedSource.appliedQuery = {
			string: "needle",
			filters: [],
			text_filter: "needle",
		};
		unselectedSource.appliedQuery = {
			string: "ignored",
			filters: [],
			text_filter: "ignored",
		};

		const query = WindowBridge.createQueryContext(app, OPERATION_ID);

		expect(query.frame).toEqual(app.timeline.frame);
		expect(Object.keys(query.filters)).toEqual([selectedSource.id]);
		expect(Object.prototype.hasOwnProperty.call(query, "scale")).toBe(false);
		expect(Object.prototype.hasOwnProperty.call(query, "renderVersion")).toBe(
			false,
		);
	});

	it("keeps canvas-rendered membership separate from the notes snapshot", () => {
		const renderedSource = createSource("rendered", "Needle rendered source");
		const hiddenEmptySource = createSource("hidden", "Needle hidden source");
		const app = createApp();
		app.target.files = [renderedSource, hiddenEmptySource];
		DataStore.replaceSourceEvents(renderedSource.id, [
			createDocument("rendered-doc", renderedSource.id),
		]);
		DataStore.replaceSourceEvents(hiddenEmptySource.id, []);
		const renderedNote = createNote("rendered-note", renderedSource.id);
		const hiddenNote = createNote("hidden-note", hiddenEmptySource.id);
		DataStore.replaceNotes([renderedNote, hiddenNote], "immediate");

		const renderedSourceIds = WindowBridge.createRenderedSourceIds(
			app,
			OPERATION_ID,
		);
		const notesPayload = WindowBridge.createNotesChangedPayload(
			app,
			OPERATION_ID,
		);

		expect(renderedSourceIds).toEqual([renderedSource.id]);
		expect(notesPayload.operationId).toBe(OPERATION_ID);
		expect(notesPayload.notes.map((note) => note.id)).toEqual([
			renderedNote.id,
			hiddenNote.id,
		]);
	});

	it("filters realtime note upserts before private delivery", () => {
		const selectedSource = createSource("selected", "Selected");
		const unselectedSource = createSource("unselected", "Unselected");
		const selectedNote = createNote("selected-note", selectedSource.id);
		const unselectedNote = createNote("unselected-note", unselectedSource.id);
		const selectedSourceIds = new Set([selectedSource.id]);

		for (const mutation of ["create", "update"] as const) {
			const change: WindowBridge.NotesDeltaPayload = {
				type: "notes-sync",
				mutation,
				operationId: OPERATION_ID,
				sequence: 1,
				notes: [selectedNote, unselectedNote],
			};

			const filtered = WindowBridge.filterNotesDeltaForSelectedSources(
				change,
				selectedSourceIds,
			);

			expect(filtered).not.toBe(change);
			expect(filtered?.mutation).toBe(mutation);
			if (filtered?.mutation !== "delete") {
				expect(filtered?.notes).toEqual([selectedNote]);
			}
			expect(change.notes).toEqual([selectedNote, unselectedNote]);
			expect(
				WindowBridge.filterNotesDeltaForSelectedSources(
					change,
					new Set<Source.Id>(),
				),
			).toBeNull();
		}

		const selectedOnlyChange: WindowBridge.NotesDeltaPayload = {
			type: "notes-sync",
			mutation: "update",
			operationId: OPERATION_ID,
			sequence: 2,
			notes: [selectedNote],
		};
		expect(
			WindowBridge.filterNotesDeltaForSelectedSources(
				selectedOnlyChange,
				selectedSourceIds,
			),
		).toBe(selectedOnlyChange);

		const deleteChange: WindowBridge.NotesDeltaPayload = {
			type: "notes-sync",
			mutation: "delete",
			operationId: OPERATION_ID,
			sequence: 3,
			noteIds: [selectedNote.id],
		};
		expect(
			WindowBridge.filterNotesDeltaForSelectedSources(
				deleteChange,
				selectedSourceIds,
			),
		).toBe(deleteChange);
	});

	it("stores only lightweight lifecycle fallback state", () => {
		WindowBridge.persistMainStatus("auth_lost", "test-main");

		const replay = WindowBridge.readStoredMainContext("test-main");

		expect(replay?.status).toEqual({ status: "auth_lost" });
		expect(Object.prototype.hasOwnProperty.call(replay ?? {}, "snapshot")).toBe(
			false,
		);
	});
});

describe("WindowBridge channel routing", () => {
	it("keeps client state off the shared control channel", () => {
		const originalBroadcastChannel = globalThis.BroadcastChannel;
		const channels = new Map<string, Set<FakeBroadcastChannel>>();

		class FakeBroadcastChannel {
			public onmessage: ((event: MessageEvent) => void) | null = null;

			public constructor(public readonly name: string) {
				const peers = channels.get(name) ?? new Set<FakeBroadcastChannel>();
				peers.add(this);
				channels.set(name, peers);
			}

			public postMessage(message: unknown): void {
				channels.get(this.name)?.forEach((peer) => {
					if (peer !== this) {
						peer.onmessage?.({ data: message } as MessageEvent);
					}
				});
			}

			public close(): void {
				channels.get(this.name)?.delete(this);
			}

			public addEventListener(): void {}
			public removeEventListener(): void {}
			public dispatchEvent(): boolean {
				return true;
			}
			public onmessageerror: ((event: MessageEvent) => void) | null = null;
		}

		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: FakeBroadcastChannel,
		});

		const controlMessages: WindowBridge.Message[] = [];
		const otherCoordinatorMessages: WindowBridge.Message[] = [];
		const firstClientMessages: WindowBridge.Message[] = [];
		const unrelatedClientMessages: WindowBridge.Message[] = [];
		const controlReceiver = WindowBridge.create(
			"control-receiver",
			(message) => controlMessages.push(message),
			"main-one",
		);
		const otherCoordinatorReceiver = WindowBridge.create(
			"other-main",
			(message) => otherCoordinatorMessages.push(message),
			"main-two",
		);
		const firstClientReceiver = WindowBridge.createClient(
			"first-client",
			"first-client",
			(message) => firstClientMessages.push(message),
			"main-one",
		);
		const unrelatedClientReceiver = WindowBridge.createClient(
			"other-client",
			"other-client",
			(message) => unrelatedClientMessages.push(message),
			"main-one",
		);
		const controlSender = WindowBridge.create(
			"control-sender",
			() => {},
			"main-one",
		);
		const firstClientSender = WindowBridge.createClient(
			"first-client",
			"main",
			() => {},
			"main-one",
		);

		controlSender.send(WindowBridge.MessageType.NAVIGATE_TO_EVENT, {
			docId: "event" as Doc.Id,
			operationId: OPERATION_ID,
		});
		controlSender.send(WindowBridge.MessageType.TIMELINE_ZOOM_REQUEST, {
			min: 1_000,
			max: 2_000,
		});
		controlSender.send(WindowBridge.MessageType.OPEN_FILTER_REQUEST, {
			sourceIds: [],
			query: { string: "", filters: [] },
		});
		firstClientSender.send(WindowBridge.MessageType.THEME_CHANGE, {
			theme: "classic-light",
			mode: "light",
			cssVariables: { "--accent": "#111" },
		});

		expect(controlMessages.map((message) => message.type)).toEqual([
			WindowBridge.MessageType.NAVIGATE_TO_EVENT,
			WindowBridge.MessageType.TIMELINE_ZOOM_REQUEST,
			WindowBridge.MessageType.OPEN_FILTER_REQUEST,
		]);
		expect(firstClientMessages.map((message) => message.type)).toEqual([
			WindowBridge.MessageType.THEME_CHANGE,
		]);
		expect(unrelatedClientMessages).toEqual([]);
		expect(otherCoordinatorMessages).toEqual([]);

		controlReceiver.destroy();
		otherCoordinatorReceiver.destroy();
		firstClientReceiver.destroy();
		unrelatedClientReceiver.destroy();
		controlSender.destroy();
		firstClientSender.destroy();
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: originalBroadcastChannel,
		});
	});
});
