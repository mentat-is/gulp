import { applyNavigatorZoomStep } from "@/app/body/NavigatorZoom";

describe("Navigator zoom step", () => {
	it("uses the canonical clamp beyond the former fixed scale ceiling", () => {
		const clampScale = jest.fn((requestedScale: number) => requestedScale);
		const result = applyNavigatorZoomStep({
			currentScale: 80_000_000,
			currentScrollX: 1_000_000,
			viewportWidth: 1_000,
			zoomOut: false,
			clampScale,
		});

		expect(clampScale).toHaveBeenCalledWith(100_000_000);
		expect(result).toEqual({
			scale: 100_000_000,
			scrollX: 1_250_125,
		});
		expect(result.scale > 9_999_999).toBe(true);
	});

	it("preserves the viewport center when the canonical clamp stops zoom-in", () => {
		const result = applyNavigatorZoomStep({
			currentScale: 90_000_000,
			currentScrollX: 450,
			viewportWidth: 1_000,
			zoomOut: false,
			clampScale: () => 100_000_000,
		});

		expect(result).toEqual({ scale: 100_000_000, scrollX: 556 });
	});
});
