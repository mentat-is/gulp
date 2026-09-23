import {
	getHighlightCssPosition,
	getHighlightLayout,
	getHighlightTimelineWidth,
	shouldTranslateHighlightOverlay,
} from "@/overlays/HighlightLayout";
import type { Highlight } from "@/entities/Highlight";

/**
 * Creates the minimum highlight fixture needed by layout tests.
 * @param timeRange Highlight timestamps.
 * @returns Strict highlight fixture.
 */
function createHighlight(timeRange: [number, number]): Highlight.Type {
	return { time_range: timeRange } as Highlight.Type;
}

describe("highlight document-space layout", () => {
	it("maps timestamps without applying viewport scroll", () => {
		expect(getHighlightLayout(createHighlight([100, 300]), 0, 400, 400)).toEqual({
			left: 100,
			width: 200,
		});
	});

	it("normalizes reversed native pixel selections", () => {
		const nativeHighlight = {
			...createHighlight([0, 0]),
			range: [200, 50],
		} as Highlight.Type;

		expect(getHighlightLayout(nativeHighlight, 0, 400, 400)).toEqual({
			left: 50,
			width: 150,
		});
	});

	it("keeps fixed, standalone, and native overlays outside the scroll path", () => {
		expect(shouldTranslateHighlightOverlay()).toBe(true);
		expect(shouldTranslateHighlightOverlay(true)).toBe(false);
		expect(shouldTranslateHighlightOverlay(false, 800)).toBe(false);
		expect(shouldTranslateHighlightOverlay(false, undefined, true)).toBe(false);
	});

	it("uses frame percentages for stored highlights and pixels for native selections", () => {
		const layout = { left: 50, width: 150 };

		expect(getHighlightCssPosition(layout, 400)).toEqual({
			left: "12.5%",
			width: "37.5%",
		});
		expect(getHighlightCssPosition(layout, 400, true)).toEqual({
			left: "50px",
			width: "150px",
		});
	});

	it("keeps stored highlight percentages stable across live scale widths", () => {
		const highlight = createHighlight([1, 2]);
		const baseLayout = getHighlightLayout(highlight, 0, 3, 100);
		const zoomedLayout = getHighlightLayout(highlight, 0, 3, 1_000);

		expect(getHighlightCssPosition(baseLayout, 100)).toEqual(
			getHighlightCssPosition(zoomedLayout, 1_000),
		);
	});

	it("tracks transient zoom through the virtual timeline width", () => {
		expect(getHighlightTimelineWidth(400, 1)).toBe(400);
		expect(getHighlightTimelineWidth(400, 2.5)).toBe(1_000);
		expect(getHighlightTimelineWidth(400, 0.5)).toBe(200);
	});
});
