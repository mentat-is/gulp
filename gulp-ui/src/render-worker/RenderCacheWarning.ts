/** Cache-warning icon size shared by worker and legacy Canvas renderers. */
export const RENDER_CACHE_WARNING_ICON_SIZE = 14;

/** Horizontal gap between the complete source label and its cache warning. */
export const RENDER_CACHE_WARNING_LABEL_GAP = 5;

/** Canvas color used for the cache-warning glyph. */
export const RENDER_CACHE_WARNING_COLOR = "#facc15";

/** Asset key resolved through the existing CanvasIcon bitmap cache. */
export const RENDER_CACHE_WARNING_ICON_KEY = "icon:AlertTriangle";

/** Compact Canvas hit rectangle for one cache warning. */
export interface RenderCacheWarningRect {
	x: number;
	y: number;
	width: number;
	height: number;
}

/**
 * Positions one warning immediately after the full source label.
 * @param labelWidth Measured source label width using the Canvas source font.
 * @param centerY Vertical center of the source row.
 * @returns Shared 14-pixel draw and hit-test rectangle.
 */
export function getRenderCacheWarningRect(
	labelWidth: number,
	centerY: number,
): RenderCacheWarningRect {
	return {
		x: 10 + labelWidth + RENDER_CACHE_WARNING_LABEL_GAP,
		y: centerY - 10,
		width: RENDER_CACHE_WARNING_ICON_SIZE,
		height: RENDER_CACHE_WARNING_ICON_SIZE,
	};
}
