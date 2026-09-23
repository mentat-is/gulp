import type { Range } from "@/class/Info";
import type { Highlight } from "@/entities/Highlight";

/** Document-space geometry for one highlight overlay. */
export interface HighlightLayout {
	left: number;
	width: number;
}

/** CSS-ready horizontal geometry for one highlight. */
export interface HighlightCssPosition {
	left: string;
	width: string;
}

/**
 * Determines whether a highlight overlay follows horizontal timeline scroll.
 * @param fixed Whether the overlay is fixed to its layout.
 * @param layoutWidth Optional standalone layout width.
 * @param native Whether the highlight already uses native pixel coordinates.
 * @returns True only for the main timeline overlay.
 */
export function shouldTranslateHighlightOverlay(
	fixed = false,
	layoutWidth?: number,
	native = false,
): boolean {
	return !fixed && layoutWidth === undefined && !native;
}

/**
 * Computes document-space highlight geometry without applying viewport scroll.
 * @param highlight Highlight entity or native pixel selection.
 * @param frameMin Minimum timestamp represented by the layout.
 * @param frameMax Maximum timestamp represented by the layout.
 * @param width Pixel width represented by the layout.
 * @returns Stable left position and width in document space.
 */
export function getHighlightLayout(
	highlight: Highlight.Type,
	frameMin: number,
	frameMax: number,
	width: number,
): HighlightLayout {
	const range =
		"range" in highlight && Array.isArray(highlight.range)
			? (highlight.range as Range)
			: (highlight.time_range.map((timestamp) => {
					const duration = frameMax - frameMin;
					if (duration === 0) return 0;
					return ((timestamp - frameMin) / duration) * width;
				}) as Range);
	const left = Math.min(...range);

	return {
		left,
		width: Math.max(...range) - left,
	};
}

/**
 * Converts pixel geometry to stable frame percentages or preserves native pixels.
 * @param layout Normalized highlight geometry.
 * @param layoutWidth Pixel width represented by the full timeline frame.
 * @param native Whether the highlight comes from a live pixel selection.
 * @returns CSS values for left offset and width.
 */
export function getHighlightCssPosition(
	layout: HighlightLayout,
	layoutWidth: number,
	native = false,
): HighlightCssPosition {
	if (native) {
		return {
			left: `${layout.left}px`,
			width: `${layout.width}px`,
		};
	}

	if (!Number.isFinite(layoutWidth) || layoutWidth <= 0) {
		return { left: "0%", width: "0%" };
	}

	return {
		left: `${(layout.left / layoutWidth) * 100}%`,
		width: `${(layout.width / layoutWidth) * 100}%`,
	};
}

/**
 * Computes the virtual timeline width represented by a live Canvas viewport.
 * @param viewportWidth Physical Canvas viewport width in CSS pixels.
 * @param scale Current transient or settled timeline scale.
 * @returns Virtual timeline width in CSS pixels.
 */
export function getHighlightTimelineWidth(
	viewportWidth: number,
	scale: number,
): number {
	if (!Number.isFinite(viewportWidth) || !Number.isFinite(scale)) return 1;
	return Math.max(1, viewportWidth) * Math.max(0.01, scale);
}
