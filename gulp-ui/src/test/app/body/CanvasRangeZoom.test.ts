import {
	applyTimelineRangeZoomViewport,
	commitTimelineRangeZoomViewport,
} from "@/app/body/CanvasRangeZoom";

describe("Canvas range zoom viewport", () => {
	it("expands the selected range and keeps its left edge at the viewport origin", () => {
		const result = applyTimelineRangeZoomViewport({
			currentScale: 2,
			currentScrollX: 100,
			viewportWidth: 1_000,
			selectionStartX: 200,
			selectionEndX: 700,
			clampScale: (scale) => scale,
		});

		expect(result).toEqual({ scale: 4, scrollX: 600 });
	});

	it("uses the clamped scale for the matching horizontal scroll", () => {
		const result = applyTimelineRangeZoomViewport({
			currentScale: 2,
			currentScrollX: 100,
			viewportWidth: 1_000,
			selectionStartX: 200,
			selectionEndX: 300,
			clampScale: () => 10,
		});

		expect(result).toEqual({ scale: 10, scrollX: 1_500 });
	});

	it("commits scale and scroll before explicitly requesting one render", () => {
		const calls: string[] = [];
		const setScale = jest.fn((scale: number) => calls.push(`scale:${scale}`));
		const setScrollX = jest.fn((scrollX: number) =>
			calls.push(`scroll:${scrollX}`),
		);
		const requestRender = jest.fn(() => calls.push("render"));

		const committed = commitTimelineRangeZoomViewport(
			{
				currentScale: 1,
				currentScrollX: 50,
				viewportWidth: 800,
				selectionStartX: 100,
				selectionEndX: 500,
				clampScale: (scale) => scale,
			},
			{ setScale, setScrollX, requestRender },
		);

		expect(committed).toBe(true);
		expect(calls).toEqual(["scale:2", "scroll:300", "render"]);
		expect(requestRender).toHaveBeenCalledTimes(1);
	});

	it("rejects a zero-width selection without mutating or rendering", () => {
		const actions = {
			setScale: jest.fn(),
			setScrollX: jest.fn(),
			requestRender: jest.fn(),
		};

		const committed = commitTimelineRangeZoomViewport(
			{
				currentScale: 1,
				currentScrollX: 0,
				viewportWidth: 800,
				selectionStartX: 200,
				selectionEndX: 200,
				clampScale: (scale) => scale,
			},
			actions,
		);

		expect(committed).toBe(false);
		expect(actions.setScale).not.toHaveBeenCalled();
		expect(actions.setScrollX).not.toHaveBeenCalled();
		expect(actions.requestRender).not.toHaveBeenCalled();
	});
});
