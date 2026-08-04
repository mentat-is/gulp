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
	const zoomFactor =
		directedZoomDelta > 0
			? Math.pow(WHEEL_ZOOM_DECREASE_FACTOR, zoomMagnitude)
			: Math.pow(WHEEL_ZOOM_INCREASE_FACTOR, zoomMagnitude);
	const safeCanvasWidth = Math.max(1, input.canvasWidth);
	const maximumScale =
		input.timeRange > 0
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
