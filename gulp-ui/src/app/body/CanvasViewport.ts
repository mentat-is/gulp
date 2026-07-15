import type { Source } from "@/entities/Source";

export const CANVAS_SOURCE_ROW_HEIGHT = 48;
export const CANVAS_SOURCE_OVERSCAN = 48;

/** Inclusive source-index range intersecting the vertically rendered Canvas area. */
export interface CanvasViewportSourceRange {
	firstIndex: number;
	lastIndex: number;
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
