import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type { User } from "@/entities/User";
import { DataStore } from "@/store/DataStore";

jest.mock("@/dto/Dataset", () => ({
	Default: { Icon: { NOTE: "StickyNote" } },
}));
jest.mock("@/dto/Logger.class", () => ({
	Logger: { log: jest.fn() },
}));
jest.mock("@/entities/addon/Parser", () => ({
	Parser: {
		useUUID: (value: { id?: string } | string): string =>
			typeof value === "string" ? value : value.id ?? "",
	},
}));
jest.mock("@/entities/Source", () => ({
	Source: { Entity: { id: jest.fn() } },
}));
jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: { normalize: (documents: unknown[]): unknown[] => documents } },
}));
jest.mock("@/entities/Context", () => ({ Context: { Entity: {} } }));
jest.mock("@/entities/Operation", () => ({ Operation: { Entity: {} } }));
jest.mock("@/entities/User", () => ({ User: {} }));
jest.mock("@/entities/Glyph", () => ({ Glyph: { List: new Map() } }));
jest.mock("@/ui/Banner", () => ({ Banner: () => null }));
jest.mock("@/ui/Toggle", () => ({ Toggle: () => null }));
jest.mock("@/context/Application.context", () => ({ Application: { use: jest.fn() } }));
jest.mock("@/ui/Button", () => ({ Button: () => null }));
jest.mock("sonner", () => ({ toast: jest.fn() }));
jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		IconExtractor: {
			activate: (fallback: string) => (): string => fallback,
		},
		Settings: { default: { field: "_id", hash_function: "fnv1a" } },
	},
}));
jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: (key: string) => key }) },
}));
jest.mock("@/entities/Color", () => ({
	Color: { Themer: { theme: { FONT_ACCENT: "#ffffff" } } },
}));

const APP = {} as App.Type;
const SOURCE_ID = "note-test-source" as Source.Id;
const OPERATION_ID = "note-test-operation" as Operation.Id;
const CONTEXT_ID = "note-test-context" as Context.Id;
const USER_ID = "note-test-user" as User.Id;
const GLYPH_ID = "note-test-glyph" as Glyph.Id;

/**
 * Creates a minimal note fixture for source index tests.
 * @param id Stable note identifier.
 * @param docId Stable document identifier linked by the note.
 * @param timestamp Timeline timestamp used for ordering.
 * @returns Canonical note fixture owned by the test source.
 */
function createNote(id: string, docId: string, timestamp: number): Note.Type {
	return {
		id: id as Note.Id,
		type: "note",
		operation_id: OPERATION_ID,
		tags: [],
		name: id,
		context_id: CONTEXT_ID,
		color: null,
		source_id: SOURCE_ID,
		doc: {
			_id: docId as Doc.Id,
			gulp_timestamp: timestamp,
			"gulp.source_id": SOURCE_ID,
			"gulp.event_code": 1,
			number_hash: timestamp,
		},
		time_pin: timestamp,
		owner_user_id: USER_ID,
		text: id,
		glyph_id: GLYPH_ID,
		edits: [],
	};
}

describe("Note source index", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation();
		Note.Entity.invalidateCache();
	});

	it("rebuilds a stale source entry after notes are replaced", () => {
		expect(Note.Entity.findByFile(APP, SOURCE_ID)).toEqual([]);

		const createdNote = createNote("created-note", "event-a", 100);
		DataStore.replaceNotes([createdNote], "immediate");

		expect(Note.Entity.findByFile(APP, SOURCE_ID)).toEqual([createdNote]);
	});
});
