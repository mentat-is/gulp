import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type { User } from "@/entities/User";
import type { Link } from "@/entities/Link";
import type { Note } from "@/entities/Note";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import {
	acknowledgeNotesChangeSequence,
	advanceNotesChangeSequence,
	applyNotesLinksWIndowsUpdateDelta,
	buildLinksListFilters,
	buildNotesListFilters,
	getSearchMatchingLinkIds,
	getSearchMatchingNoteIds,
	linkMatchesNotesLinksWIndowsSearch,
	noteMatchesNotesLinksWIndowsFilter,
} from "@/utils";

jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: {} },
}));
jest.mock("@/entities/Source", () => ({
	Source: { Entity: { rendered: () => [] } },
}));

const OPERATION_ID = "notes-links-windows-operation" as Operation.Id;
const CONTEXT_ALPHA_ID = "notes-links-windows-context-alpha" as Context.Id;
const CONTEXT_BETA_ID = "notes-links-windows-context-beta" as Context.Id;
const SOURCE_ALPHA_ID = "notes-links-windows-source-alpha" as Source.Id;
const SOURCE_BETA_ID = "notes-links-windows-source-beta" as Source.Id;
const USER_ID = "notes-links-windows-user" as User.Id;
const GLYPH_ID = "notes-links-windows-glyph" as Glyph.Id;

const sourceById = new Map<Source.Id, Source.Type>([
	[
		SOURCE_ALPHA_ID,
		{
			id: SOURCE_ALPHA_ID,
			name: "Alpha Source",
			appliedQuery: null,
			queryCache: createEmptyQueryCacheState(),
		} as Source.Type,
	],
	[
		SOURCE_BETA_ID,
		{
			id: SOURCE_BETA_ID,
			name: "Beta Source",
			appliedQuery: null,
			queryCache: createEmptyQueryCacheState(),
		} as Source.Type,
	],
]);

const contextById = new Map<Context.Id, Context.Type>([
	[
		CONTEXT_ALPHA_ID,
		{
			id: CONTEXT_ALPHA_ID,
			name: "Alpha Context",
		} as Context.Type,
	],
	[
		CONTEXT_BETA_ID,
		{
			id: CONTEXT_BETA_ID,
			name: "Beta Context",
		} as Context.Type,
	],
]);

/**
 * Creates a canonical note fixture for Notes & Links window filtering and paging tests.
 * @param index Stable numeric fixture index.
 * @param overrides Optional fields that customize the generated note.
 * @returns Note fixture linked to a deterministic source and context.
 */
function createNote(
	index: number,
	overrides: Partial<Note.Type> = {},
): Note.Type {
	const sourceId =
		index % 2 === 0 ? SOURCE_ALPHA_ID : SOURCE_BETA_ID;
	const contextId =
		index % 2 === 0 ? CONTEXT_ALPHA_ID : CONTEXT_BETA_ID;
	return {
		id: `notes-links-windows-note-${index}` as Note.Id,
		type: "note",
		operation_id: OPERATION_ID,
		tags: index % 2 === 0 ? ["even", "shared"] : ["odd", "shared"],
		name: `Note title ${index}`,
		context_id: contextId,
		color: null,
		source_id: sourceId,
		doc: {
			_id: `notes-links-windows-doc-${index}` as Doc.Id,
			gulp_timestamp: index,
			"gulp.source_id": sourceId,
			"gulp.event_code": 1,
			number_hash: index,
		},
		time_pin: index,
		owner_user_id: USER_ID,
		text: `Note body ${index}`,
		glyph_id: GLYPH_ID,
		edits: [],
		...overrides,
	};
}

/** Creates a minimal link for free-text search assertions. */
function createLink(overrides: Partial<Link.Type> = {}): Link.Type {
	return {
		id: "notes-links-windows-link" as Link.Id,
		type: "link",
		owner_user_id: USER_ID,
		description: "Routine relationship",
		operation_id: OPERATION_ID,
		tags: ["reviewed"],
		color: "orange",
		doc_id_from: "notes-links-windows-link-origin" as Doc.Id,
		doc_ids: [],
		glyph_id: GLYPH_ID,
		name: "Related activity",
		...overrides,
	} as Link.Type;
}

describe("NotesLinksWIndows pagination helpers", () => {
	it("matches link title, description, and exact tags case-insensitively", () => {
		const link = createLink({
			name: "Suspicious Login",
			description: "Credential access investigation",
			tags: ["Needs-Review"],
		});

		expect(linkMatchesNotesLinksWIndowsSearch(link, "LOGIN")).toBe(true);
		expect(linkMatchesNotesLinksWIndowsSearch(link, "access invest")).toBe(true);
		expect(linkMatchesNotesLinksWIndowsSearch(link, "needs-review")).toBe(true);
		expect(linkMatchesNotesLinksWIndowsSearch(link, "needs")).toBe(false);
		expect(linkMatchesNotesLinksWIndowsSearch(link, "   ")).toBe(true);
	});

	it("builds backend page and count filters for matching Links IDs", () => {
		const links = [
			createLink({ id: "link-1" as Link.Id, name: "Needle" }),
			createLink({ id: "link-2" as Link.Id, name: "Other" }),
		];
		const matchingIds = getSearchMatchingLinkIds(links, "needle");
		const { pageFilter, countFilter } = buildLinksListFilters({
			searchMatchingIds: matchingIds,
			pageSize: 20,
			currentPage: 3,
		});

		expect(matchingIds).toEqual([links[0].id]);
		expect(countFilter).toEqual({ ids: matchingIds });
		expect(pageFilter).toEqual({
			ids: matchingIds,
			limit: 20,
			offset: 40,
		});
	});

	it("omits the IDs filter when the Links search is empty", () => {
		const matchingIds = getSearchMatchingLinkIds([createLink()], "  ");
		const { pageFilter, countFilter } = buildLinksListFilters({
			searchMatchingIds: matchingIds,
			pageSize: 50,
			currentPage: 1,
		});

		expect(matchingIds).toBeUndefined();
		expect(countFilter).toEqual({});
		expect(pageFilter.ids).toBeUndefined();
		expect(pageFilter.limit).toBe(50);
	});

	const freeTextCases: Array<
		[string, Partial<Note.Type>, string]
	> = [
		["title", { name: "Needle title" }, "needle"],
		["text", { text: "Needle body" }, "needle"],
		["source name", {}, "alpha source"],
		["context name", {}, "alpha context"],
		["exact tag", { tags: ["Needle"] }, "needle"],
	];

	freeTextCases.forEach(([field, overrides, search]) => {
		it(`preserves free-text OR matching through ${field}`, () => {
			const note = createNote(2, overrides);
			expect(
				noteMatchesNotesLinksWIndowsFilter(note, {
					search,
					selectedTags: [],
					sourceIds: new Set([SOURCE_ALPHA_ID]),
					sourceById,
					contextById,
				}),
			).toBe(true);
		});
	});

	it("keeps tag matching exact and combines selected tags with AND", () => {
		const note = createNote(2, { tags: ["needle-long", "shared"] });
		expect(
			noteMatchesNotesLinksWIndowsFilter(note, {
				search: "needle",
				selectedTags: [],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				sourceById,
				contextById,
			}),
		).toBe(false);
		expect(
			noteMatchesNotesLinksWIndowsFilter(note, {
				search: "",
				selectedTags: ["needle-long", "shared"],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				sourceById,
				contextById,
			}),
		).toBe(true);
	});

	it("builds existing GulpCollabFilter fields for the requested page and count", () => {
		const ids = [createNote(1).id, createNote(2).id];
		const documentIds = [createNote(2).doc._id];
		const { pageFilter, countFilter } = buildNotesListFilters({
			searchMatchingIds: ids,
			sourceIds: [SOURCE_ALPHA_ID],
			documentIds,
			selectedTags: ["shared", "even"],
			pageSize: 50,
			currentPage: 3,
		});

		expect(countFilter).toEqual({
			ids,
			doc_ids: documentIds,
			source_ids: [SOURCE_ALPHA_ID],
			tags: ["shared", "even"],
			tags_and: true,
		});
		expect(pageFilter).toEqual({
			...countFilter,
			limit: 50,
			offset: 100,
			sort: [["time_pin", "desc"]],
		});
	});

	it("matches notes only when their event is included in the document filter", () => {
		const note = createNote(2);
		expect(
			noteMatchesNotesLinksWIndowsFilter(note, {
				search: "",
				selectedTags: [],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				documentIds: new Set([note.doc._id]),
				sourceById,
				contextById,
			}),
		).toBe(true);
		expect(
			noteMatchesNotesLinksWIndowsFilter(note, {
				search: "",
				selectedTags: [],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				documentIds: new Set(),
				sourceById,
				contextById,
			}),
		).toBe(false);
	});

	it("limits a 3,000-note result to a 50-record backend page", () => {
		const notes = Array.from({ length: 3000 }, (_, index) => createNote(index));
		const searchIds = getSearchMatchingNoteIds(
			notes,
			"note",
			sourceById,
			contextById,
		);
		const { pageFilter } = buildNotesListFilters({
			searchMatchingIds: searchIds,
			sourceIds: [SOURCE_ALPHA_ID, SOURCE_BETA_ID],
			selectedTags: [],
			pageSize: 50,
			currentPage: 1,
		});

		expect(searchIds).toHaveLength(3000);
		expect(pageFilter.limit).toBe(50);
		expect(pageFilter.offset).toBe(0);
	});

	it("derives all filtered cross-page IDs from the canonical note snapshot", () => {
		const notes = Array.from({ length: 3000 }, (_, index) => createNote(index));
		const matchingIds = notes
			.filter((note) =>
				noteMatchesNotesLinksWIndowsFilter(note, {
					search: "",
					selectedTags: ["even"],
					sourceIds: new Set([SOURCE_ALPHA_ID]),
					sourceById,
					contextById,
				}),
			)
			.map((note) => note.id);

		expect(matchingIds).toHaveLength(1500);
		expect(matchingIds[0]).toBe(notes[0].id);
		expect(matchingIds.at(-1)).toBe(notes[2998].id);
	});

	it("acknowledges only changes covered by a successful refresh", () => {
		expect(acknowledgeNotesChangeSequence(7, 7)).toBeNull();
		expect(acknowledgeNotesChangeSequence(7, 8)).toBeNull();
		expect(acknowledgeNotesChangeSequence(8, 7)).toBe(8);
		expect(acknowledgeNotesChangeSequence(null, 7)).toBeNull();
	});

	it("marks only realtime creation and deletion as stale", () => {
		const note = createNote(1);
		expect(
			advanceNotesChangeSequence(null, {
				type: "notes-sync",
				mutation: "snapshot-ready",
				operationId: OPERATION_ID,
			}),
		).toBeNull();
		expect(
			advanceNotesChangeSequence(null, {
				type: "notes-sync",
				mutation: "update",
				operationId: OPERATION_ID,
				sequence: 5,
				notes: [note],
			}),
		).toBeNull();
		expect(
			advanceNotesChangeSequence(null, {
				type: "notes-sync",
				mutation: "create",
				operationId: OPERATION_ID,
				sequence: 6,
				notes: [note],
			}),
		).toBe(6);
		expect(
			advanceNotesChangeSequence(6, {
				type: "notes-sync",
				mutation: "delete",
				operationId: OPERATION_ID,
				sequence: 7,
				noteIds: [note.id],
			}),
		).toBe(7);
	});

	it("patches an update locally when filter membership and sort position stay stable", () => {
		const note = createNote(2);
		const updatedNote = {
			...note,
			name: "Updated title",
			text: "Updated body",
		};
		const result = applyNotesLinksWIndowsUpdateDelta({
			queryNotes: [note],
			pageNotes: [note],
			updatedNotes: [updatedNote],
			filter: {
				search: "",
				selectedTags: [],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				sourceById,
				contextById,
			},
		});

		expect(result.requiresRefresh).toBe(false);
		expect(result.queryNotes).toEqual([updatedNote]);
		expect(result.pageNotes).toEqual([updatedNote]);
	});

	it("requests a refresh when an update changes membership or sort position", () => {
		const note = createNote(2);
		const pageNotes = [note];
		const movedOutsideFilter = {
			...note,
			source_id: SOURCE_BETA_ID,
			doc: {
				...note.doc,
				"gulp.source_id": SOURCE_BETA_ID,
			},
		};
		const membershipResult = applyNotesLinksWIndowsUpdateDelta({
			queryNotes: [note],
			pageNotes,
			updatedNotes: [movedOutsideFilter],
			filter: {
				search: "",
				selectedTags: [],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				sourceById,
				contextById,
			},
		});
		const sortResult = applyNotesLinksWIndowsUpdateDelta({
			queryNotes: [note],
			pageNotes: [note],
			updatedNotes: [{ ...note, time_pin: note.time_pin + 1 }],
			filter: {
				search: "",
				selectedTags: [],
				sourceIds: new Set([SOURCE_ALPHA_ID]),
				sourceById,
				contextById,
			},
		});

		expect(membershipResult.requiresRefresh).toBe(true);
		expect(membershipResult.pageNotes).toBe(pageNotes);
		expect(membershipResult.pageNotes[0]).toBe(note);
		expect(sortResult.requiresRefresh).toBe(true);
		expect(sortResult.pageNotes[0]).toBe(note);
	});
});
