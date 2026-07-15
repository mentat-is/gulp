import {
	getHighlightLayout,
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
});
