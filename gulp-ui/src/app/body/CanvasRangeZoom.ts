/** Inputs required to zoom the timeline to a mouse-selected horizontal range. */
export interface TimelineRangeZoomViewportInput {
	currentScale: number;
	currentScrollX: number;
	viewportWidth: number;
	selectionStartX: number;
	selectionEndX: number;
	clampScale: (requestedScale: number) => number;
}

/** Canonical scale and horizontal scroll produced by a range zoom. */
export interface TimelineRangeZoomViewportResult {
	scale: number;
	scrollX: number;
}

/** Side effects required to commit a selected range and schedule its repaint. */
export interface TimelineRangeZoomCommitActions {
	setScale: (scale: number) => void;
	setScrollX: (scrollX: number) => void;
	requestRender: () => void;
}

/**
 * Resolves the canonical viewport that expands a selected horizontal range.
 * @param input Current viewport, selected coordinates, and canonical scale clamp.
 * @returns Range-filling viewport, or null when the selection has no usable width.
 */
export function applyTimelineRangeZoomViewport(
	input: TimelineRangeZoomViewportInput,
): TimelineRangeZoomViewportResult | null {
	const selectionStartX = Math.min(
		input.selectionStartX,
		input.selectionEndX,
	);
	const selectionWidth = Math.abs(
		input.selectionEndX - input.selectionStartX,
	);
	if (
		!Number.isFinite(selectionStartX) ||
		!Number.isFinite(selectionWidth) ||
		selectionWidth <= 0 ||
		!Number.isFinite(input.viewportWidth) ||
		input.viewportWidth <= 0 ||
		!Number.isFinite(input.currentScale) ||
		input.currentScale <= 0 ||
		!Number.isFinite(input.currentScrollX)
	) {
		return null;
	}

	const requestedScale =
		(input.viewportWidth * input.currentScale) / selectionWidth;
	const scale = input.clampScale(requestedScale);
	if (!Number.isFinite(scale) || scale <= 0) return null;

	return {
		scale,
		scrollX: Math.round(
			(input.currentScrollX + selectionStartX) *
				(scale / input.currentScale),
		),
	};
}

/**
 * Commits one range-derived viewport and explicitly schedules its Canvas render.
 * @param input Current viewport and mouse-selected horizontal range.
 * @param actions Canonical state writers and render invalidation callback.
 * @returns True when a valid range was committed.
 */
export function commitTimelineRangeZoomViewport(
	input: TimelineRangeZoomViewportInput,
	actions: TimelineRangeZoomCommitActions,
): boolean {
	const viewport = applyTimelineRangeZoomViewport(input);
	if (!viewport) return false;

	actions.setScale(viewport.scale);
	actions.setScrollX(viewport.scrollX);
	actions.requestRender();
	return true;
}
