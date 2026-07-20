import type { MinMax } from "@/class/Info";
import type { App } from "@/entities/App";
import { Doc } from "@/entities/Doc";
import { Source } from "@/entities/Source";

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
 * Returns the source IDs that participate in the current rendered Canvas source set.
 * This follows the same selected-source, timeline text filter, pin ordering, and
 * hidden-empty-source rules used by Canvas rendering without depending on viewport
 * scroll or zoom state.
 *
 * @param app - Current application state.
 * @returns Set containing source IDs rendered as timeline rows.
 */
export function getCanvasRenderedSourceIdSet(app: App.Type): Set<Source.Id> {
	return new Set(Source.Entity.selected(app).map((source) => source.id));
}

const DETACHED_WINDOWS_STORAGE_KEY = "gulp-detached-windows";

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
 * Applies the active theme attributes and CSS custom properties from one document to another.
 *
 * @param sourceDoc - Document that provides the currently computed theme variables.
 * @param targetDoc - Document that receives the theme attributes and variables.
 * @param nextTheme - Theme name to write to the target document root.
 * @returns True when the target document was updated, or false when either document is not ready.
 */
export function applyThemeToWindow(
	sourceDoc: Document,
	targetDoc: Document,
	nextTheme: string | undefined,
): boolean {
	const sourceRoot = sourceDoc.documentElement;
	const targetRoot = targetDoc.documentElement;

	if (!sourceRoot || !targetRoot) {
		return false;
	}

	targetRoot.setAttribute("data-theme", nextTheme ?? "dark");

	const styles = getComputedStyle(sourceRoot);
	for (let index = 0; index < styles.length; index++) {
		const key = styles[index];
		if (key.startsWith("--")) {
			targetRoot.style.setProperty(key, styles.getPropertyValue(key));
		}
	}

	return true;
}

/**
 * Gets the persisted metadata used to reopen or reconnect a detached routed window.
 *
 * @param kind - Detached route kind.
 * @returns Detached window persistence record for the requested kind.
 */
export function getPersistedDetachedWindowRecord(
	kind: PersistedDetachedWindowKind,
): PersistedDetachedWindowRecord {
	return { ...DETACHED_WINDOW_RECORDS[kind] };
}

/**
 * Reads the detached windows that should be reattached after main-tab reloads or login redirects.
 *
 * @returns Persisted detached window records.
 */
export function readPersistedDetachedWindows(): PersistedDetachedWindowRecord[] {
	try {
		const raw = localStorage.getItem(DETACHED_WINDOWS_STORAGE_KEY);
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
): void {
	localStorage.setItem(DETACHED_WINDOWS_STORAGE_KEY, JSON.stringify(records));
}

/**
 * Stores or updates a detached window record in localStorage.
 *
 * @param record - Detached window metadata needed to reattach by window name.
 * @returns Nothing.
 */
export function persistDetachedWindow(
	record: PersistedDetachedWindowRecord,
): void {
	const records = readPersistedDetachedWindows();
	writePersistedDetachedWindows([
		...records.filter((item) => item.key !== record.key),
		record,
	]);
}

/**
 * Removes a detached window record from localStorage.
 *
 * @param key - Detached window key to remove.
 * @returns Nothing.
 */
export function removePersistedDetachedWindow(key: string): void {
	const records = readPersistedDetachedWindows();
	writePersistedDetachedWindows(records.filter((record) => record.key !== key));
}
