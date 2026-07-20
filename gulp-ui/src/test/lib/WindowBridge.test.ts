import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type { User } from "@/entities/User";
import { WindowBridge } from "@/lib/WindowBridge";
import { DataStore } from "@/store/DataStore";

const OPERATION_ID = "operation" as Operation.Id;
const CONTEXT_ID = "context" as Context.Id;

interface SentBridgeMessage {
	type: WindowBridge.MessageType;
	payload: unknown;
}

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
		settings: { frequency_sample: 0 },
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
				} as Context.Type,
			],
			files: [],
			events: new Map(),
			filters: {},
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
			cache: { data: new Map(), filters: {} },
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

/**
 * Creates an in-memory bridge that records sent messages.
 * @param sentMessages Mutable message sink.
 * @returns Bridge implementation for unit tests.
 */
function createRecordingBridge(sentMessages: SentBridgeMessage[]): WindowBridge.Bridge {
	return {
		send<TType extends WindowBridge.MessageType>(
			type: TType,
			payload: WindowBridge.MessagePayload[TType],
		): void {
			sentMessages.push({ type, payload });
		},
		destroy(): void {},
	};
}

describe("WindowBridge detached app snapshots", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation(OPERATION_ID);
	});

	it("does not include movement-coupled scale or viewport source IDs", () => {
		const app = createApp();
		const sentMessages: SentBridgeMessage[] = [];

		WindowBridge.sendMainContext(
			createRecordingBridge(sentMessages),
			app,
			"active",
			OPERATION_ID,
		);

		const snapshotMessage = sentMessages.find(
			(message) => message.type === WindowBridge.MessageType.APP_SNAPSHOT,
		);
		const snapshot =
			snapshotMessage?.payload as WindowBridge.AppSnapshotPayload | undefined;

		expect(snapshot === undefined).toBe(false);
		if (!snapshot) {
			throw new Error("APP_SNAPSHOT was not sent");
		}

		expect(Object.prototype.hasOwnProperty.call(snapshot, "visibleSourceIds")).toBe(
			false,
		);
		expect(snapshot.timeline).toEqual({
			frame: app.timeline.frame,
			filter: app.timeline.filter,
			renderVersion: app.timeline.renderVersion,
		});
		expect(snapshot.renderedSourceIds).toEqual([]);
		expect(Object.prototype.hasOwnProperty.call(snapshot.timeline, "scale")).toBe(
			false,
		);
	});

	it("sends canvas-rendered source IDs separately from selected-source notes", () => {
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
		const sentMessages: SentBridgeMessage[] = [];

		WindowBridge.sendMainContext(
			createRecordingBridge(sentMessages),
			app,
			"active",
			OPERATION_ID,
		);

		const snapshot = sentMessages.find(
			(message) => message.type === WindowBridge.MessageType.APP_SNAPSHOT,
		)?.payload as WindowBridge.AppSnapshotPayload | undefined;
		const notesPayload = sentMessages.find(
			(message) => message.type === WindowBridge.MessageType.NOTES_CHANGED,
		)?.payload as WindowBridge.NotesChangedPayload | undefined;

		expect(snapshot?.selectedSourceIds).toEqual([
			renderedSource.id,
			hiddenEmptySource.id,
		]);
		expect(snapshot?.renderedSourceIds).toEqual([renderedSource.id]);
		expect(notesPayload?.notes.map((note) => note.id)).toEqual([
			renderedNote.id,
			hiddenNote.id,
		]);
	});
});
