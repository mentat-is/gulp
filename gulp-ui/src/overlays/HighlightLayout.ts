import type { Range } from "@/class/Info";
import type { Highlight } from "@/entities/Highlight";

/** Document-space geometry for one highlight overlay. */
export interface HighlightLayout {
	left: number;
	width: number;
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
					return Math.round(((timestamp - frameMin) / duration) * width);
				}) as Range);
	const left = Math.min(...range);

	return {
		left,
		width: Math.max(...range) - left,
	};
}
