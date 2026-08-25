/** Inputs required to apply one Navigator button or keyboard zoom step. */
export interface NavigatorZoomStepInput {
	currentScale: number;
	currentScrollX: number;
	viewportWidth: number;
	zoomOut: boolean;
	clampScale: (requestedScale: number) => number;
}

/** Canonical scale and center-preserving horizontal scroll after one zoom step. */
export interface NavigatorZoomStepResult {
	scale: number;
	scrollX: number;
}

/**
 * Applies one Navigator zoom step through the canonical timeline-scale clamp.
 * @param input Current viewport geometry, direction, and canonical clamp.
 * @returns Clamped scale and horizontal scroll retaining the viewport center.
 */
export function applyNavigatorZoomStep(
	input: NavigatorZoomStepInput,
): NavigatorZoomStepResult {
	const requestedScale = input.zoomOut
		? input.currentScale - input.currentScale / 4
		: input.currentScale + input.currentScale / 4;
	const scale = input.clampScale(requestedScale);
	const centerOffset = input.currentScrollX + input.viewportWidth / 2;
	const scaledOffset = (centerOffset * scale) / input.currentScale;

	return {
		scale,
		scrollX: Math.round(
			input.currentScrollX + scaledOffset - centerOffset,
		),
	};
}
