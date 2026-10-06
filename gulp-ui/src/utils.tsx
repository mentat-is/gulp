import type { MinMax } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import { Doc } from "@/entities/Doc";
import type { Note } from "@/entities/Note";
import { Source } from "@/entities/Source";
import type { NotesSyncChange } from "@/store/DataStore";
import { DataStore } from "@/store/DataStore";

export type PersistedDetachedWindowKind = "notes" | "table" | "dashboard";

export interface PersistedDetachedWindowRecord {
	key: string;
	kind: PersistedDetachedWindowKind;
	windowName: string;
	features: string;
}

export interface EventFieldTableRow {
	/** Dot-separated field path displayed in the Event dialog table. */
	key: string;
	/** String representation displayed beside the field path. */
	value: string;
}

export interface ConfiguredEventFieldViews {
	/** Filtered and ordered JSON preserving the source Raw shape. */
	raw: Record<string, unknown>;
	/** Filtered leaf values in the user's complete field order. */
	table: EventFieldTableRow[];
	/** Filtered JSON with dotted field names reconstructed as a hierarchy. */
	tree: Record<string, unknown>;
}

interface EventFieldEntry {
	path: string;
	normalizedPath: string;
	value: unknown;
	discoveryIndex: number;
}

interface FilteredEventValue {
	included: boolean;
	value: unknown;
	rank: number;
}

const UNKNOWN_EVENT_FIELD_RANK = Number.MAX_SAFE_INTEGER;

/**
 * Removes numeric array segments from a persisted event field identity.
 * @param path Concrete dot-separated JSON path.
 * @returns Source-stable field path without array indices.
 */
export function normalizeEventFieldPath(path: string): string {
	return path
		.split(".")
		.filter((part) => !/^\d+$/.test(part))
		.join(".");
}

/** Checks whether a JSON value is a plain object suitable for field traversal. */
function isEventFieldObject(value: unknown): value is Record<string, unknown> {
	return Object.prototype.toString.call(value) === "[object Object]";
}

/**
 * Collects concrete leaf paths from a JSON value while preserving discovery order.
 * Empty objects and arrays are treated as leaf values so they remain configurable.
 */
function collectEventFieldEntries(
	value: unknown,
	parentPath = "",
	entries: EventFieldEntry[] = [],
): EventFieldEntry[] {
	if (Array.isArray(value)) {
		if (value.length === 0 && parentPath) {
			entries.push({
				path: parentPath,
				normalizedPath: normalizeEventFieldPath(parentPath),
				value,
				discoveryIndex: entries.length,
			});
			return entries;
		}

		value.forEach((entry, index) => {
			collectEventFieldEntries(
				entry,
				parentPath ? `${parentPath}.${index}` : String(index),
				entries,
			);
		});
		return entries;
	}

	if (isEventFieldObject(value)) {
		const objectEntries = Object.entries(value);
		if (objectEntries.length === 0 && parentPath) {
			entries.push({
				path: parentPath,
				normalizedPath: normalizeEventFieldPath(parentPath),
				value,
				discoveryIndex: entries.length,
			});
			return entries;
		}

		objectEntries.forEach(([key, entry]) => {
			collectEventFieldEntries(
				entry,
				parentPath ? `${parentPath}.${key}` : key,
				entries,
			);
		});
		return entries;
	}

	if (parentPath) {
		entries.push({
			path: parentPath,
			normalizedPath: normalizeEventFieldPath(parentPath),
			value,
			discoveryIndex: entries.length,
		});
	}
	return entries;
}

/**
 * Collects the unique source-stable leaf field paths present in one event.
 * @param json Event JSON after active display transformations.
 * @returns Alphabetically ordered field catalog without array indices.
 */
export function getEventLeafFieldPaths(
	json: Record<string, unknown>,
): string[] {
	return Array.from(
		new Set(
			collectEventFieldEntries(json).map((entry) => entry.normalizedPath),
		),
	).sort((left, right) => left.localeCompare(right));
}

/**
 * Checks whether a leaf field or one of its structural ancestors is hidden.
 * @param path Normalized leaf field path.
 * @param hiddenFields Configured hidden field identities.
 * @returns True when the field must be omitted.
 */
function isEventFieldHidden(
	path: string,
	hiddenFields: ReadonlySet<string>,
): boolean {
	const parts = path.split(".");
	for (let index = parts.length; index > 0; index--) {
		if (hiddenFields.has(parts.slice(0, index).join("."))) return true;
	}
	return false;
}

/**
 * Resolves the configured rank for a field, placing unknown fields last.
 * @param path Normalized field path.
 * @param ranks User-configured field ranks.
 * @returns Numeric sort rank.
 */
function getEventFieldRank(path: string, ranks: ReadonlyMap<string, number>): number {
	return ranks.get(path) ?? UNKNOWN_EVENT_FIELD_RANK;
}

/**
 * Recursively filters and orders JSON while preserving its Raw object shape.
 * @param value Current JSON value.
 * @param path Concrete path of the current value.
 * @param hiddenFields Hidden normalized paths.
 * @param ranks Configured field ranks.
 * @returns Inclusion state, transformed value, and earliest descendant rank.
 */
function filterAndOrderRawEventValue(
	value: unknown,
	path: string,
	hiddenFields: ReadonlySet<string>,
	ranks: ReadonlyMap<string, number>,
): FilteredEventValue {
	if (Array.isArray(value)) {
		if (value.length === 0) {
			const normalizedPath = normalizeEventFieldPath(path);
			return {
				included: !isEventFieldHidden(normalizedPath, hiddenFields),
				value: [],
				rank: getEventFieldRank(normalizedPath, ranks),
			};
		}

		const children = value.map((entry, index) =>
			filterAndOrderRawEventValue(
				entry,
				path ? `${path}.${index}` : String(index),
				hiddenFields,
				ranks,
			),
		);
		const includedChildren = children.filter((child) => child.included);
		if (includedChildren.length === 0) {
			return { included: false, value: [], rank: UNKNOWN_EVENT_FIELD_RANK };
		}

		return {
			included: true,
			value: children.map((child, index) => {
				if (child.included) return child.value;
				const original = value[index];
				if (Array.isArray(original)) return [];
				if (isEventFieldObject(original)) return {};
				return null;
			}),
			rank: Math.min(...includedChildren.map((child) => child.rank)),
		};
	}

	if (isEventFieldObject(value)) {
		const children = Object.entries(value)
			.map(([key, entry], discoveryIndex) => ({
				key,
				discoveryIndex,
				result: filterAndOrderRawEventValue(
					entry,
					path ? `${path}.${key}` : key,
					hiddenFields,
					ranks,
				),
			}))
			.filter((child) => child.result.included)
			.sort(
				(left, right) =>
					left.result.rank - right.result.rank ||
					left.discoveryIndex - right.discoveryIndex,
			);

		if (children.length === 0 && path) {
			const normalizedPath = normalizeEventFieldPath(path);
			if (Object.keys(value).length === 0 && !isEventFieldHidden(normalizedPath, hiddenFields)) {
				return {
					included: true,
					value: {},
					rank: getEventFieldRank(normalizedPath, ranks),
				};
			}
			return { included: false, value: {}, rank: UNKNOWN_EVENT_FIELD_RANK };
		}

		return {
			included: path === "" || children.length > 0,
			value: Object.fromEntries(
				children.map((child) => [child.key, child.result.value]),
			),
			rank:
				children.length > 0
					? Math.min(...children.map((child) => child.result.rank))
					: UNKNOWN_EVENT_FIELD_RANK,
		};
	}

	const normalizedPath = normalizeEventFieldPath(path);
	return {
		included: !isEventFieldHidden(normalizedPath, hiddenFields),
		value,
		rank: getEventFieldRank(normalizedPath, ranks),
	};
}

/** Assigns a concrete dotted leaf path into the reconstructed Tree value. */
function assignEventTreePath(
	tree: Record<string, unknown>,
	path: string,
	value: unknown,
): void {
	const parts = path.split(".");
	let current: Record<string, unknown> | unknown[] = tree;

	for (let index = 0; index < parts.length; index++) {
		const part = parts[index];
		const isLast = index === parts.length - 1;
		const nextPart = parts[index + 1];
		const nextIsArray = nextPart !== undefined && /^\d+$/.test(nextPart);

		if (Array.isArray(current)) {
			const arrayIndex = Number(part);
			if (!Number.isInteger(arrayIndex)) return;
			if (isLast) {
				current[arrayIndex] = value;
				return;
			}
			const existing = current[arrayIndex];
			if (existing !== undefined && typeof existing !== "object") return;
			current[arrayIndex] = existing ?? (nextIsArray ? [] : {});
			current = current[arrayIndex] as Record<string, unknown> | unknown[];
			continue;
		}

		if (isLast) {
			current[part] = value;
			return;
		}

		const existing = current[part];
		if (existing !== undefined && typeof existing !== "object") return;
		current[part] = existing ?? (nextIsArray ? [] : {});
		current = current[part] as Record<string, unknown> | unknown[];
	}
}

/** Converts a leaf value into the Event dialog table's display text. */
function stringifyEventFieldTableValue(value: unknown): string {
	if (value == null) return String(value);
	if (typeof value === "bigint") return value.toString();
	if (Array.isArray(value) || isEventFieldObject(value)) {
		return JSON.stringify(value) ?? String(value);
	}
	return String(value);
}

/**
 * Builds the configured Raw, Table, and Tree representations for one event.
 * @param json Event JSON after active display transformations.
 * @param orderedFields Complete canonical source field order.
 * @param hiddenFields Fields excluded by the source-scoped configuration.
 * @returns Three synchronized Event dialog representations.
 */
export function buildConfiguredEventFieldViews(
	json: Record<string, unknown>,
	orderedFields: readonly string[],
	hiddenFields: ReadonlySet<string>,
): ConfiguredEventFieldViews {
	const ranks = new Map(
		orderedFields.map((field, index) => [field, index] as const),
	);
	const visibleEntries = collectEventFieldEntries(json)
		.filter((entry) => !isEventFieldHidden(entry.normalizedPath, hiddenFields))
		.sort(
			(left, right) =>
				getEventFieldRank(left.normalizedPath, ranks) -
					getEventFieldRank(right.normalizedPath, ranks) ||
				left.discoveryIndex - right.discoveryIndex,
		);
	const rawResult = filterAndOrderRawEventValue(json, "", hiddenFields, ranks);
	const tree: Record<string, unknown> = {};
	visibleEntries.forEach((entry) => assignEventTreePath(tree, entry.path, entry.value));

	return {
		raw: rawResult.value as Record<string, unknown>,
		table: visibleEntries.map((entry) => ({
			key: entry.path,
			value: stringifyEventFieldTableValue(entry.value),
		})),
		tree,
	};
}

/**
 * Computes the number_hash range for a source when the ingestion-maintained cache is unavailable.
 * @param app Current application state used to read canonical source documents.
 * @param sourceId Source whose documents define the range.
 * @returns Finite hash range, or null when the source has no numeric hash values.
 */
function computeEventHashRange(
	_app: App.Type,
	sourceId: Source.Id,
): MinMax | null {
	const summary = DataStore.getEventSourceSummary(sourceId);
	return summary && summary.eventCount > 0
		? { min: summary.hashMin, max: summary.hashMax }
		: null;
}

/**
 * Resolves the display color for a document without depending on legacy canvas renderer caches.
 * @param app Current application state containing selected source metadata.
 * @param event Document whose source palette and numeric hash determine the color.
 * @returns Concrete CSS color for the event, or null when its source is unavailable.
 */
export function resolveEventRenderColor(
	app: App.Type,
	event: Doc.Type,
): string | null {
	const source = Source.Entity.id(app, event["gulp.source_id"]);
	if (!source) return null;

	const range =
		Doc.Entity.hashRange(source.id) ??
		computeEventHashRange(app, source.id) ??
		{ min: event.number_hash, max: event.number_hash };

	return Source.Entity.resolveColor(source, event.number_hash, range);
}

/**
 * Returns ordered source IDs that participate in the current rendered Canvas source set.
 * This follows the same selected-source, timeline text filter, pin ordering, and
 * hidden-empty-source rules used by Canvas rendering without depending on viewport
 * scroll or zoom state.
 *
 * @param app - Current application state.
 * @param operationId - Optional operation scope for detached-window snapshots.
 * @returns Ordered source IDs rendered as timeline rows.
 */
export function getCanvasRenderedSourceIds(
	app: App.Type,
	operationId?: string | null,
): Source.Id[] {
	return Source.Entity.rendered(app)
		.filter((source) => !operationId || source.operation_id === operationId)
		.map((source) => source.id);
}

/**
 * Returns the source IDs that participate in the current rendered Canvas source set.
 * Detached windows receive this set from the main tab because they intentionally do
 * not mirror render event batches.
 *
 * @param app - Current application state.
 * @param operationId - Optional operation scope for the rendered-source fallback.
 * @returns Set containing source IDs rendered as timeline rows.
 */
export function getCanvasRenderedSourceIdSet(
	app: App.Type,
	operationId?: string | null,
): Set<Source.Id> {
	return new Set(
		app.timeline.renderedSourceIds ??
			getCanvasRenderedSourceIds(app, operationId),
	);
}

export interface NotesWindowFilter {
	search: string;
	selectedTags: readonly string[];
	sourceIds: ReadonlySet<Source.Id>;
	documentIds?: ReadonlySet<Doc.Id>;
	sourceById: ReadonlyMap<Source.Id, Source.Type>;
	contextById: ReadonlyMap<Context.Id, Context.Type>;
}

/** Inputs required to apply realtime note updates without reloading a page. */
export interface NotesWindowUpdateDeltaOptions {
	queryNotes: Note.Type[];
	pageNotes: Note.Type[];
	updatedNotes: readonly Note.Type[];
	filter: NotesWindowFilter;
}

/** Result of applying a realtime note update to Notes window state. */
export interface NotesWindowUpdateDeltaResult {
	queryNotes: Note.Type[];
	pageNotes: Note.Type[];
	requiresRefresh: boolean;
}

/** Inputs used to build one paginated note-list request and its count request. */
export interface NotesListFilterOptions {
	searchMatchingIds: Note.Id[] | undefined;
	sourceIds: Source.Id[];
	documentIds?: Doc.Id[];
	selectedTags: string[];
	pageSize: number;
	currentPage: number;
}

/**
 * Resolves the source id attached to a note, tolerating older payload shapes.
 * @param note Note whose owning source should be resolved.
 * @returns Source id from the note entity or its embedded document.
 */
export function getNoteSourceId(note: Note.Type): Source.Id {
	return note.source_id ?? note.doc["gulp.source_id"];
}

/**
 * Determines whether a note matches the complete Notes window filter.
 * @param note Canonical DataStore note to evaluate.
 * @param filter Active search, tag, source, and document restrictions.
 * @returns True when the note should participate in the filtered result set.
 */
export function noteMatchesNotesWindowFilter(
	note: Note.Type,
	filter: NotesWindowFilter,
): boolean {
	const sourceId = getNoteSourceId(note);
	if (!filter.sourceIds.has(sourceId)) return false;
	if (filter.documentIds && !filter.documentIds.has(note.doc._id)) return false;

	const lowercaseTags = note.tags.map((tag) => tag.toLowerCase());
	const matchesTags =
		filter.selectedTags.length === 0 ||
		filter.selectedTags.every((tag) =>
			lowercaseTags.includes(tag.toLowerCase()),
		);
	if (!matchesTags) return false;

	const search = filter.search.trim().toLowerCase();
	if (!search) return true;

	const sourceName = filter.sourceById.get(sourceId)?.name.toLowerCase() ?? "";
	const contextName =
		filter.contextById.get(note.context_id)?.name.toLowerCase() ?? "";
	return (
		note.name.toLowerCase().includes(search) ||
		note.text.toLowerCase().includes(search) ||
		sourceName.includes(search) ||
		contextName.includes(search) ||
		lowercaseTags.includes(search)
	);
}

/**
 * Applies existing-note websocket updates to the acknowledged query snapshot and
 * visible page. Updates that change filter membership or backend sort position
 * require a fresh page because their cross-page position cannot be known locally.
 * @param options Current Notes state, update batch, and active semantic filter.
 * @returns Locally patched arrays and whether the backend page must be refreshed.
 */
export function applyNotesWindowUpdateDelta(
	options: NotesWindowUpdateDeltaOptions,
): NotesWindowUpdateDeltaResult {
	if (options.updatedNotes.length === 0) {
		return {
			queryNotes: options.queryNotes,
			pageNotes: options.pageNotes,
			requiresRefresh: false,
		};
	}

	const updatesById = new Map(
		options.updatedNotes.map((note) => [note.id, note]),
	);
	const queryNotesById = new Map(
		options.queryNotes.map((note) => [note.id, note]),
	);
	const locallyPatchableIds = new Set<Note.Id>();
	let requiresRefresh = false;

	updatesById.forEach((updatedNote, noteId) => {
		const previousNote = queryNotesById.get(noteId);
		if (!previousNote) {
			requiresRefresh = true;
			return;
		}

		const membershipChanged =
			noteMatchesNotesWindowFilter(previousNote, options.filter) !==
			noteMatchesNotesWindowFilter(updatedNote, options.filter);
		const sortPositionChanged =
			previousNote.time_pin !== updatedNote.time_pin;
		if (membershipChanged || sortPositionChanged) {
			requiresRefresh = true;
			return;
		}
		locallyPatchableIds.add(noteId);
	});

	let queryNotesChanged = false;
	const queryNotes = options.queryNotes.map((note) => {
		const updatedNote = updatesById.get(note.id);
		if (!updatedNote || updatedNote === note) return note;
		queryNotesChanged = true;
		return updatedNote;
	});

	let pageNotesChanged = false;
	const pageNotes = options.pageNotes.map((note) => {
		if (!locallyPatchableIds.has(note.id)) return note;
		const updatedNote = updatesById.get(note.id);
		if (!updatedNote || updatedNote === note) return note;
		pageNotesChanged = true;
		return updatedNote;
	});

	return {
		queryNotes: queryNotesChanged
			? queryNotes
			: options.queryNotes,
		pageNotes: pageNotesChanged
			? pageNotes
			: options.pageNotes,
		requiresRefresh,
	};
}

/**
 * Collects DataStore note IDs matching only the free-text OR search.
 * Other filters are passed to GulpCollabFilter through their native fields.
 * @param notes Canonical notes snapshot acknowledged by the table.
 * @param search Applied free-text search value.
 * @param sourceById Sources used to resolve searchable display names.
 * @param contextById Contexts used to resolve searchable display names.
 * @returns Matching IDs, or undefined when no free-text filter is active.
 */
export function getSearchMatchingNoteIds(
	notes: readonly Note.Type[],
	search: string,
	sourceById: ReadonlyMap<Source.Id, Source.Type>,
	contextById: ReadonlyMap<Context.Id, Context.Type>,
): Note.Id[] | undefined {
	const normalizedSearch = search.trim();
	if (!normalizedSearch) return undefined;

	const allSourceIds = new Set(notes.map((note) => getNoteSourceId(note)));
	return notes
		.filter((note) =>
			noteMatchesNotesWindowFilter(note, {
				search: normalizedSearch,
				selectedTags: [],
				sourceIds: allSourceIds,
				sourceById,
				contextById,
			}),
		)
		.map((note) => note.id);
}

/**
 * Builds list and count filters using only existing GulpCollabFilter fields.
 * @param options Current Notes window query and pagination state.
 * @returns Page filter and matching count filter.
 */
export function buildNotesListFilters(
	options: NotesListFilterOptions,
): { pageFilter: Note.ListFilter; countFilter: Note.ListFilter } {
	const countFilter: Note.ListFilter = {
		source_ids: options.sourceIds,
	};
	if (options.documentIds) {
		countFilter.doc_ids = options.documentIds;
	}
	if (options.searchMatchingIds) {
		countFilter.ids = options.searchMatchingIds;
	}
	if (options.selectedTags.length > 0) {
		countFilter.tags = options.selectedTags;
		countFilter.tags_and = true;
	}

	return {
		countFilter,
		pageFilter: {
			...countFilter,
			limit: options.pageSize,
			offset: options.pageSize * (options.currentPage - 1),
			sort: [["time_pin", "desc"]],
		},
	};
}

/**
 * Clears a pending note-change sequence only when the completed refresh covered it.
 * @param pendingSequence Latest create/delete sequence awaiting acknowledgement.
 * @param refreshedSequence Sequence captured when the successful refresh started.
 * @returns Null when fully acknowledged, otherwise the still-pending sequence.
 */
export function acknowledgeNotesChangeSequence(
	pendingSequence: number | null,
	refreshedSequence: number,
): number | null {
	return pendingSequence !== null && pendingSequence <= refreshedSequence
		? null
		: pendingSequence;
}

/**
 * Advances the stale-page marker only for realtime note creation or deletion.
 * @param pendingSequence Latest create/delete sequence awaiting acknowledgement.
 * @param change Explicit note synchronization event for the active operation.
 * @returns Latest pending sequence, unchanged for snapshots and updates.
 */
export function advanceNotesChangeSequence(
	pendingSequence: number | null,
	change: NotesSyncChange,
): number | null {
	if (
		change.mutation === "snapshot-ready" ||
		change.mutation === "update"
	) {
		return pendingSequence;
	}
	return Math.max(pendingSequence ?? 0, change.sequence);
}

const DETACHED_WINDOWS_STORAGE_PREFIX = "gulp-detached-windows:";

const DETACHED_WINDOW_RECORDS: Record<
	PersistedDetachedWindowKind,
	PersistedDetachedWindowRecord
> = {
	notes: {
		key: "notes",
		kind: "notes",
		windowName: "GulpNotes",
		features: "width=1480,height=760,left=100,top=100",
	},
	table: {
		key: "table",
		kind: "table",
		windowName: "GulpTableView",
		features: "width=800,height=600,left=150,top=150",
	},
	dashboard: {
		key: "dashboard",
		kind: "dashboard",
		windowName: "GulpDashboardView",
		features: "width=1480,height=820,left=180,top=120",
	},
};

/**
 * Gets the persisted metadata used to reopen or reconnect a detached routed window.
 *
 * @param kind - Detached route kind.
 * @returns Detached window persistence record for the requested kind.
 */
export function getPersistedDetachedWindowRecord(
	kind: PersistedDetachedWindowKind,
	coordinatorId: string,
): PersistedDetachedWindowRecord {
	const record = DETACHED_WINDOW_RECORDS[kind];
	return {
		...record,
		windowName: `${record.windowName}-${coordinatorId.replace(
			/[^a-zA-Z0-9_-]/g,
			"_",
		)}`,
	};
}

/**
 * Reads the detached windows that should be reattached after main-tab reloads or login redirects.
 *
 * @returns Persisted detached window records.
 */
export function readPersistedDetachedWindows(
	coordinatorId: string,
): PersistedDetachedWindowRecord[] {
	try {
		const raw = localStorage.getItem(
			`${DETACHED_WINDOWS_STORAGE_PREFIX}${coordinatorId}`,
		);
		if (!raw) return [];

		const records = JSON.parse(raw) as PersistedDetachedWindowRecord[];
		if (!Array.isArray(records)) return [];

		return records.filter((record) =>
			["notes", "table", "dashboard"].includes(record.kind) &&
			!!record.key &&
			!!record.windowName &&
			!!record.features,
		);
	} catch (error) {
		console.warn("Failed to read persisted detached windows", {
			error,
		});
		return [];
	}
}

/**
 * Writes detached window records used for post-login reattachment.
 *
 * @param records - Persisted detached window records.
 * @returns Nothing.
 */
function writePersistedDetachedWindows(
	records: PersistedDetachedWindowRecord[],
	coordinatorId: string,
): void {
	localStorage.setItem(
		`${DETACHED_WINDOWS_STORAGE_PREFIX}${coordinatorId}`,
		JSON.stringify(records),
	);
}

/**
 * Stores or updates a detached window record in localStorage.
 *
 * @param record - Detached window metadata needed to reattach by window name.
 * @returns Nothing.
 */
export function persistDetachedWindow(
	record: PersistedDetachedWindowRecord,
	coordinatorId: string,
): void {
	const records = readPersistedDetachedWindows(coordinatorId);
	writePersistedDetachedWindows([
		...records.filter((item) => item.key !== record.key),
		record,
	], coordinatorId);
}

/**
 * Removes a detached window record from localStorage.
 *
 * @param key - Detached window key to remove.
 * @returns Nothing.
 */
export function removePersistedDetachedWindow(
	key: string,
	coordinatorId: string,
): void {
	const records = readPersistedDetachedWindows(coordinatorId);
	writePersistedDetachedWindows(
		records.filter((record) => record.key !== key),
		coordinatorId,
	);
}
