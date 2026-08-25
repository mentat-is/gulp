import {
	applyTimelineWheelViewport,
	shouldInvalidateCanvasGeometry,
} from "@/app/body/CanvasWheelViewport";

describe("Canvas coalesced wheel viewport", () => {
	it("applies horizontal pan and anchored zoom from the same display tick", () => {
		const result = applyTimelineWheelViewport({
			scale: 1,
			scrollX: 100,
			panDeltaX: 20,
			zoomDeltaY: 100,
			cursorX: 50,
			timeRange: 100_000,
			minimumVisibleDurationMilliseconds: 10,
			isScrollReversed: false,
		});

		expect(result.scale).toBe(0.875);
		expect(result.scrollX).toBe(99);
		expect(result.scaleChanged).toBe(true);
	});

	it("maps aggregated burst magnitude to deterministic continuous zoom", () => {
		const createResult = (zoomDeltaY: number) =>
			applyTimelineWheelViewport({
				scale: 1,
				scrollX: 0,
				panDeltaX: 0,
				zoomDeltaY,
				cursorX: 0,
				timeRange: 100_000,
				minimumVisibleDurationMilliseconds: 10,
				isScrollReversed: false,
			});

		expect(createResult(100).scale).toBe(0.875);
		expect(createResult(200).scale).toBe(0.875 ** 2);
		expect(createResult(-200).scale).toBe(1.125 ** 2);
	});

	it("caps zoom-in at a ten-millisecond visible duration", () => {
		const result = applyTimelineWheelViewport({
			scale: 9_000,
			scrollX: 0,
			panDeltaX: 0,
			zoomDeltaY: -100,
			cursorX: 500,
			timeRange: 100_000,
			minimumVisibleDurationMilliseconds: 10,
			isScrollReversed: false,
		});

		expect(result.scale).toBe(10_000);
		expect(100_000 / result.scale).toBe(10);
		expect(result.scaleChanged).toBe(true);
	});

	it("keeps cached geometry for descendant scrolling only", () => {
		const ancestor = document.createElement("div");
		const wrapper = document.createElement("div");
		const internalScrollbar = document.createElement("div");
		ancestor.append(wrapper);
		wrapper.append(internalScrollbar);

		expect(
			shouldInvalidateCanvasGeometry(internalScrollbar, wrapper),
		).toBe(false);
		expect(shouldInvalidateCanvasGeometry(wrapper, wrapper)).toBe(false);
		expect(shouldInvalidateCanvasGeometry(ancestor, wrapper)).toBe(true);
		expect(shouldInvalidateCanvasGeometry(document, wrapper)).toBe(true);
	});
});
