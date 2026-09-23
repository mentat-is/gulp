import type { Source } from "@/entities/Source";
import type { XY } from "@/dto/XY.dto";

export const CANVAS_SOURCE_ROW_HEIGHT = 48;
/** Height reserved for the timeline ruler above the drawable data area. */
export const TIMELINE_RULER_HEIGHT_PX = 25;
/** Two source rows retained above and below the visible viewport. */
export const CANVAS_SOURCE_OVERSCAN = CANVAS_SOURCE_ROW_HEIGHT * 2;

/** Vertical bounds available to overlays below the timeline ruler. */
export interface CanvasDataAreaBounds {
	top: number;
	height: number;
}

/**
 * Returns the full-height timeline data area below the ruler.
 * @param canvasHeight Current Canvas height in pixels.
 * @returns Top offset and non-negative drawable height.
 */
export function getCanvasDataAreaBounds(
	canvasHeight: number,
): CanvasDataAreaBounds {
	return {
		top: TIMELINE_RULER_HEIGHT_PX,
		height: Math.max(0, canvasHeight - TIMELINE_RULER_HEIGHT_PX),
	};
}

/** Inclusive source-index range intersecting the vertically rendered Canvas area. */
export interface CanvasViewportSourceRange {
	firstIndex: number;
	lastIndex: number;
}

/** Canvas bounds and logical size required to translate viewport client coordinates. */
export interface CanvasSurfaceGeometry
	extends Pick<DOMRectReadOnly, "left" | "top" | "width" | "height"> {
	coordinateWidth: number;
	coordinateHeight: number;
}

/** Minimal Canvas element contract required to capture transform-aware geometry. */
export interface CanvasSurfaceElement {
	clientWidth: number;
	clientHeight: number;
	getBoundingClientRect: () => DOMRect;
}

/**
 * Captures visual bounds and untransformed Canvas coordinate dimensions together.
 * @param surface Canvas element whose ancestor may be CSS-transformed.
 * @returns Geometry that maps viewport client pixels into Canvas logical pixels.
 */
export function getCanvasSurfaceGeometry(
	surface: CanvasSurfaceElement,
): CanvasSurfaceGeometry {
	const bounds = surface.getBoundingClientRect();
	return {
		left: bounds.left,
		top: bounds.top,
		width: bounds.width,
		height: bounds.height,
		coordinateWidth: surface.clientWidth || bounds.width,
		coordinateHeight: surface.clientHeight || bounds.height,
	};
}

/**
 * Converts one client-space pointer coordinate into primary-Canvas CSS pixels.
 * @param clientX Horizontal native event coordinate in the viewport.
 * @param clientY Vertical native event coordinate in the viewport.
 * @param surface Primary Canvas visual bounds and logical coordinate dimensions.
 * @returns Fractional, transform-aware Canvas-relative pointer coordinate.
 */
export function getCanvasPointerPosition(
	clientX: number,
	clientY: number,
	surface: CanvasSurfaceGeometry,
): XY {
	const scaleX = surface.width > 0 ? surface.coordinateWidth / surface.width : 1;
	const scaleY =
		surface.height > 0 ? surface.coordinateHeight / surface.height : 1;
	return {
		x: (clientX - surface.left) * scaleX,
		y: (clientY - surface.top) * scaleY,
	};
}

/**
 * Resolves the rendered source row beneath a Canvas-relative pointer hotspot.
 * @param viewportY Fractional vertical pointer coordinate in primary-Canvas pixels.
 * @param scrollY Current vertical Canvas scroll offset.
 * @param sourceCount Number of source rows in the rendered source order.
 * @returns Zero-based source row index, or null outside the rendered rows.
 */
export function getCanvasSourceRowIndex(
	viewportY: number,
	scrollY: number,
	sourceCount: number,
): number | null {
	if (
		!Number.isFinite(viewportY) ||
		!Number.isFinite(scrollY) ||
		sourceCount <= 0
	) {
		return null;
	}

	const sourceY = viewportY + scrollY;
	if (sourceY < 0) return null;

	const lowerRowIndex = Math.floor(sourceY / CANVAS_SOURCE_ROW_HEIGHT);
	const rowOffset = sourceY - lowerRowIndex * CANVAS_SOURCE_ROW_HEIGHT;
	// Each row paints its one-pixel bottom separator. Keep that visible pixel
	// associated with the source that owns it instead of the following row.
	const rowIndex =
		lowerRowIndex > 0 && rowOffset < 1
			? lowerRowIndex - 1
			: lowerRowIndex;
	return rowIndex >= 0 && rowIndex < sourceCount ? rowIndex : null;
}

/**
 * Calculates the source rows intersecting the Canvas viewport and its overscan.
 * @param sourceCount Total number of ordered source rows.
 * @param scrollY Current vertical Canvas scroll offset in pixels.
 * @param canvasHeight Current Canvas backing height in pixels.
 * @param overscan Additional pixels rendered above and below the viewport.
 * @returns Inclusive clamped source range, or null when no source can be visible.
 */
export function getCanvasViewportSourceRange(
	sourceCount: number,
	scrollY: number,
	canvasHeight: number,
	overscan = CANVAS_SOURCE_OVERSCAN,
): CanvasViewportSourceRange | null {
	if (sourceCount <= 0 || canvasHeight <= 0) return null;

	const firstIndex = Math.max(
		0,
		Math.ceil(
			(scrollY - CANVAS_SOURCE_ROW_HEIGHT / 2 - overscan) /
				CANVAS_SOURCE_ROW_HEIGHT,
		),
	);
	const lastIndex = Math.min(
		sourceCount - 1,
		Math.floor(
			(scrollY + canvasHeight - CANVAS_SOURCE_ROW_HEIGHT / 2 + overscan) /
				CANVAS_SOURCE_ROW_HEIGHT,
		),
	);

	return firstIndex <= lastIndex ? { firstIndex, lastIndex } : null;
}

/**
 * Returns the ordered IDs for source rows intersecting the Canvas viewport.
 * @param sources Sources ordered by their Canvas row position.
 * @param scrollY Current vertical Canvas scroll offset in pixels.
 * @param canvasHeight Current Canvas backing height in pixels.
 * @returns Source IDs in row order, including the standard Canvas overscan.
 */
export function getCanvasViewportSourceIds(
	sources: readonly Source.Type[],
	scrollY: number,
	canvasHeight: number,
): Source.Id[] {
	const range = getCanvasViewportSourceRange(
		sources.length,
		scrollY,
		canvasHeight,
	);
	if (!range) return [];

	return sources
		.slice(range.firstIndex, range.lastIndex + 1)
		.map((source) => source.id);
}
