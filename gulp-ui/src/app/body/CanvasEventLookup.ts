import type { Engine } from "@/class/Engine.dto";

/** Geometry and source semantics captured at the start of one exact Canvas lookup. */
export interface CanvasEventLookupGeometry {
	sourceId: string;
	frameMin: number;
	frameMax: number;
	canvasWidth: number;
	scale: number;
	scrollX: number;
	sourceOffset: number;
	renderEngine: Engine.List;
}

/** Current source fields that can invalidate a pending exact-event lookup. */
export interface CanvasEventLookupSourceState {
	sourceId: string;
	sourceOffset: number;
	renderEngine: Engine.List;
}

/** Inclusive exact-event timestamp bounds for one rendered Canvas hit area. */
export interface CanvasEventTimestampRange {
	min: number;
	max: number;
}

const adjacentFloatBuffer = new ArrayBuffer(8);
const adjacentFloatView = new DataView(adjacentFloatBuffer);

/**
 * Determines whether an exact event should take precedence over a Canvas overlay.
 * Zoomed-in and magnified views expose event pixels precisely enough to select the
 * raw event beneath a note or link badge; the default view remains badge-first.
 * @param magnifierActive Whether the Canvas magnifier is currently visible.
 * @param timelineScale Current horizontal timeline scale.
 * @returns True when exact event lookup should run before badge handling.
 */
export function shouldPreferCanvasEventOverOverlay(
	magnifierActive: boolean,
	timelineScale: number,
): boolean {
	return magnifierActive || timelineScale > 1;
}

/**
 * Returns the greatest representable float strictly smaller than a value.
 * @param value Exclusive upper bound produced by the inverse pixel transform.
 * @returns Adjacent lower IEEE-754 value.
 */
function nextFloatDown(value: number): number {
	if (Number.isNaN(value) || value === Number.NEGATIVE_INFINITY) return value;
	if (value === Number.POSITIVE_INFINITY) return Number.MAX_VALUE;
	if (Object.is(value, 0) || Object.is(value, -0)) return -Number.MIN_VALUE;

	adjacentFloatView.setFloat64(0, value, false);
	let bits = adjacentFloatView.getBigUint64(0, false);
	bits = value > 0 ? bits - 1n : bits + 1n;
	adjacentFloatView.setBigUint64(0, bits, false);
	return adjacentFloatView.getFloat64(0, false);
}

/**
 * Returns the smallest representable float strictly greater than a value.
 * @param value Inclusive boundary to advance.
 * @returns Adjacent greater IEEE-754 value.
 */
function nextFloatUp(value: number): number {
	if (Number.isNaN(value) || value === Number.POSITIVE_INFINITY) return value;
	if (value === Number.NEGATIVE_INFINITY) return -Number.MAX_VALUE;
	if (Object.is(value, 0) || Object.is(value, -0)) return Number.MIN_VALUE;

	adjacentFloatView.setFloat64(0, value, false);
	let bits = adjacentFloatView.getBigUint64(0, false);
	bits = value > 0 ? bits + 1n : bits - 1n;
	adjacentFloatView.setBigUint64(0, bits, false);
	return adjacentFloatView.getFloat64(0, false);
}

/**
 * Maps an exact event timestamp through the same rounded X transform used by Canvas.
 * @param timestamp Raw event timestamp without the source offset.
 * @param geometry Immutable viewport geometry captured for the lookup.
 * @returns Rounded canvas-relative X pixel.
 */
export function getCanvasEventPixel(
	timestamp: number,
	geometry: CanvasEventLookupGeometry,
): number {
	const frameRange = geometry.frameMax - geometry.frameMin;
	const virtualWidth = geometry.canvasWidth * geometry.scale;
	if (frameRange <= 0 || virtualWidth <= 0) return -geometry.scrollX;
	return (
		Math.round(
			((timestamp + geometry.sourceOffset - geometry.frameMin) /
				frameRange) *
				virtualWidth,
		) - geometry.scrollX
	);
}

/**
 * Inverts the rounded Canvas transform into exact timestamps for a pixel hit area.
 * Graph rows pass a width of 16 so events whose right-aligned marker covers the
 * clicked pixel are included; default and height rows pass zero.
 * @param pixel Rounded canvas-relative click X.
 * @param geometry Immutable viewport geometry captured for the lookup.
 * @param graphPixelWidth Additional pixels covered to the right by a graph marker.
 * @returns Inclusive raw timestamp range suitable for IndexedDB lookup.
 */
export function getCanvasEventTimestampRange(
	pixel: number,
	geometry: CanvasEventLookupGeometry,
	graphPixelWidth = 0,
): CanvasEventTimestampRange | null {
	const frameRange = geometry.frameMax - geometry.frameMin;
	const virtualWidth = geometry.canvasWidth * geometry.scale;
	if (frameRange <= 0 || virtualWidth <= 0) return null;

	const firstRoundedPixel = pixel + geometry.scrollX;
	const lastRoundedPixel = firstRoundedPixel + Math.max(0, graphPixelWidth);
	const timestampPerVirtualPixel = frameRange / virtualWidth;
	let min =
		geometry.frameMin +
		(firstRoundedPixel - 0.5) * timestampPerVirtualPixel -
		geometry.sourceOffset;
	let max =
		geometry.frameMin +
		(lastRoundedPixel + 0.5) * timestampPerVirtualPixel -
		geometry.sourceOffset;

	// Correct inverse-rounding boundaries against the exact forward floating-point
	// expression. This keeps IndexedDB's inclusive range pixel-equivalent even at
	// values that land mathematically on a half-pixel boundary.
	for (let adjustment = 0; adjustment < 128; adjustment++) {
		if (getCanvasEventPixel(min, geometry) >= pixel) break;
		min = nextFloatUp(min);
	}
	for (let adjustment = 0; adjustment < 128; adjustment++) {
		const previous = nextFloatDown(min);
		if (getCanvasEventPixel(previous, geometry) < pixel) break;
		min = previous;
	}
	for (let adjustment = 0; adjustment < 128; adjustment++) {
		if (getCanvasEventPixel(max, geometry) <= pixel + graphPixelWidth) break;
		max = nextFloatDown(max);
	}
	for (let adjustment = 0; adjustment < 128; adjustment++) {
		const next = nextFloatUp(max);
		if (getCanvasEventPixel(next, geometry) > pixel + graphPixelWidth) break;
		max = next;
	}

	if (
		min > max ||
		getCanvasEventPixel(min, geometry) > pixel + graphPixelWidth ||
		getCanvasEventPixel(max, geometry) < pixel
	) {
		return null;
	}

	return { min, max };
}

/**
 * Checks whether a source still has the identity and render transform captured by a lookup.
 * @param geometry Lookup geometry captured before asynchronous storage access.
 * @param currentSource Current application source state, or null after removal.
 * @returns True only while source identity, offset, and engine remain unchanged.
 */
export function isCanvasEventLookupSourceCurrent(
	geometry: CanvasEventLookupGeometry,
	currentSource: CanvasEventLookupSourceState | null,
): boolean {
	return (
		currentSource !== null &&
		currentSource.sourceId === geometry.sourceId &&
		currentSource.sourceOffset === geometry.sourceOffset &&
		currentSource.renderEngine === geometry.renderEngine
	);
}

/**
 * Checks the cheap source-boundary guard applied before an exact IndexedDB lookup.
 * Graph markers extend left from their event X coordinate, so their minimum clickable
 * coordinate includes the complete graph hit width.
 * @param pixel Canvas-relative click coordinate.
 * @param sourceMinimumTimestamp Raw minimum source timestamp.
 * @param sourceMaximumTimestamp Raw maximum source timestamp.
 * @param geometry Current source-adjusted Canvas transform.
 * @param graphPixelWidth Additional graph-marker width extending left from an event.
 * @returns True when an event from the source can occupy the requested X coordinate.
 */
export function isCanvasPixelWithinEventSourceRange(
	pixel: number,
	sourceMinimumTimestamp: number,
	sourceMaximumTimestamp: number,
	geometry: CanvasEventLookupGeometry,
	graphPixelWidth = 0,
): boolean {
	const minimumPixel =
		getCanvasEventPixel(sourceMinimumTimestamp, geometry) -
		Math.max(0, graphPixelWidth);
	const maximumPixel = getCanvasEventPixel(sourceMaximumTimestamp, geometry);
	return pixel >= minimumPixel && pixel <= maximumPixel;
}

/**
 * Checks a persisted event against the exact default or graph hit rule.
 * @param timestamp Raw event timestamp.
 * @param pixel Clicked Canvas pixel.
 * @param geometry Immutable viewport geometry captured for the lookup.
 * @param graphPixelWidth Graph marker width, or zero for an exact pixel line.
 * @returns Whether the event occupies the requested hit area.
 */
export function isEventInCanvasHitArea(
	timestamp: number,
	pixel: number,
	geometry: CanvasEventLookupGeometry,
	graphPixelWidth = 0,
): boolean {
	const eventPixel = getCanvasEventPixel(timestamp, geometry);
	return graphPixelWidth > 0
		? pixel >= eventPixel - graphPixelWidth && pixel <= eventPixel
		: pixel === eventPixel;
}
