import type { MinMax } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import { Doc } from "@/entities/Doc";
import type { Note } from "@/entities/Note";
import { Source } from "@/entities/Source";
import type { NotesSyncChange } from "@/store/DataStore";

export type PersistedDetachedWindowKind = "notes" | "table" | "dashboard";

export interface PersistedDetachedWindowRecord {
	key: string;
	kind: PersistedDetachedWindowKind;
	windowName: string;
	features: string;
}

/** Browser wheel delta values that can be normalized without retaining an event. */
export interface WheelDeltaInput {
	deltaX: number;
	deltaY: number;
	deltaMode: number;
}

/** Wheel deltas converted into CSS pixels. */
export interface NormalizedWheelDelta {
	deltaX: number;
	deltaY: number;
}

/** Inputs required to apply one coalesced Canvas wheel batch. */
export interface TimelineWheelViewportInput {
	scale: number;
	scrollX: number;
	panDeltaX: number;
	zoomDeltaY: number;
	cursorX: number;
	canvasWidth: number;
	timeRange: number;
	minimumMillisecondsPerPixel: number;
	isScrollReversed: boolean;
}

/** Result of applying coalesced horizontal pan and anchored timeline zoom. */
export interface TimelineWheelViewportResult {
	scale: number;
	scrollX: number;
	scaleChanged: boolean;
}

const WHEEL_ZOOM_PIXELS_PER_STEP = 100;
const WHEEL_ZOOM_DECREASE_FACTOR = 7 / 8;
const WHEEL_ZOOM_INCREASE_FACTOR = 9 / 8;

/**
 * Computes the number_hash range for a source when the ingestion-maintained cache is unavailable.
 * @param app Current application state used to read canonical source documents.
 * @param sourceId Source whose documents define the range.
 * @returns Finite hash range, or null when the source has no numeric hash values.
 */
function computeEventHashRange(
	app: App.Type,
	sourceId: Source.Id,
): MinMax | null {
	const events = Doc.Entity.get(app, sourceId);
	let min = Infinity;
	let max = -Infinity;

	for (const event of events) {
		const value = event.number_hash;
		if (typeof value !== "number" || !Number.isFinite(value)) continue;
		if (value < min) min = value;
		if (value > max) max = value;
	}

	return Number.isFinite(min) && Number.isFinite(max) ? { min, max } : null;
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
 * Converts browser wheel deltas from pixel, line, or page units into CSS pixels.
 * @param input Wheel delta values and their declared unit.
 * @param lineHeightPx Cached CSS line height used for line-mode deltas.
 * @param pageHeightPx Cached viewport height used for page-mode deltas.
 * @returns Pixel-normalized horizontal and vertical deltas.
 */
export function normalizeWheelDelta(
	input: WheelDeltaInput,
	lineHeightPx: number,
	pageHeightPx: number,
): NormalizedWheelDelta {
	const safeLineHeight =
		Number.isFinite(lineHeightPx) && lineHeightPx > 0 ? lineHeightPx : 16;
	const safePageHeight =
		Number.isFinite(pageHeightPx) && pageHeightPx > 0
			? pageHeightPx
			: safeLineHeight;
	const multiplier =
		input.deltaMode === 1
			? safeLineHeight
			: input.deltaMode === 2
				? safePageHeight
				: 1;

	return {
		deltaX: input.deltaX * multiplier,
		deltaY: input.deltaY * multiplier,
	};
}

/**
 * Applies coalesced wheel pan and zoom while preserving the cursor's content point.
 * Pixel magnitude maps continuously to the former one-eighth zoom step so browser
 * event bursts remain deterministic without rendering once per native event.
 * @param input Current viewport, normalized wheel contributions, and scale limits.
 * @returns Updated horizontal scroll and scale.
 */
export function applyTimelineWheelViewport(
	input: TimelineWheelViewportInput,
): TimelineWheelViewportResult {
	const pannedScrollX = input.scrollX + input.panDeltaX;
	if (!Number.isFinite(input.zoomDeltaY) || input.zoomDeltaY === 0) {
		return {
			scale: input.scale,
			scrollX: pannedScrollX,
			scaleChanged: false,
		};
	}

	const directedZoomDelta = input.isScrollReversed
		? -input.zoomDeltaY
		: input.zoomDeltaY;
	const zoomMagnitude = Math.abs(directedZoomDelta) / WHEEL_ZOOM_PIXELS_PER_STEP;
	const zoomFactor = directedZoomDelta > 0
		? Math.pow(WHEEL_ZOOM_DECREASE_FACTOR, zoomMagnitude)
		: Math.pow(WHEEL_ZOOM_INCREASE_FACTOR, zoomMagnitude);
	const safeCanvasWidth = Math.max(1, input.canvasWidth);
	const maximumScale = input.timeRange > 0
		? input.timeRange /
			(input.minimumMillisecondsPerPixel * safeCanvasWidth)
		: 9_999_999;
	const requestedScale = input.scale * zoomFactor;
	const nextScale = Math.max(0.01, Math.min(maximumScale, requestedScale));
	if (nextScale === input.scale) {
		return {
			scale: input.scale,
			scrollX: pannedScrollX,
			scaleChanged: false,
		};
	}

	const contentX = pannedScrollX + input.cursorX;
	return {
		scale: nextScale,
		scrollX: Math.round(
			contentX * (nextScale / input.scale) - input.cursorX,
		),
		scaleChanged: true,
	};
}

/**
 * Determines whether a capture-phase scroll can move the Canvas wrapper.
 * Descendant scroll containers change their own content only and should keep the
 * cached wrapper rectangle valid.
 * @param eventTarget Target that emitted the captured scroll event.
 * @param wrapper Current Canvas wrapper node.
 * @returns True for document, viewport, or ancestor scrolling.
 */
export function shouldInvalidateCanvasGeometry(
	eventTarget: EventTarget | null,
	wrapper: Node | null,
): boolean {
	return !(
		typeof Node !== "undefined" &&
		eventTarget instanceof Node &&
		wrapper?.contains(eventTarget)
	);
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
 * @returns Set containing source IDs rendered as timeline rows.
 */
export function getCanvasRenderedSourceIdSet(app: App.Type): Set<Source.Id> {
	return new Set(app.timeline.renderedSourceIds ?? getCanvasRenderedSourceIds(app));
}

export interface NotesWindowFilter {
	search: string;
	selectedTags: readonly string[];
	sourceIds: ReadonlySet<Source.Id>;
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
 * @param filter Active search, tag, and source restrictions.
 * @returns True when the note should participate in the filtered result set.
 */
export function noteMatchesNotesWindowFilter(
	note: Note.Type,
	filter: NotesWindowFilter,
): boolean {
	const sourceId = getNoteSourceId(note);
	if (!filter.sourceIds.has(sourceId)) return false;

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
