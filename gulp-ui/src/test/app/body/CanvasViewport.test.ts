import {
	getCanvasPointerPosition,
	getCanvasSourceRowIndex,
	getCanvasSurfaceGeometry,
	getCanvasViewportSourceIds,
	getCanvasViewportSourceRange,
} from "@/app/body/CanvasViewport";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";

/**
 * Creates the minimum source fixture required by the viewport ID selector.
 * @param id Source fixture identifier.
 * @returns Strictly typed source fixture.
 */
function createSource(id: string): Source.Type {
	return {
		id,
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
	} as Source.Type;
}

describe("Canvas viewport source range", () => {
	it("keeps client pointer coordinates fractional and relative to the canvas content", () => {
		expect(
			getCanvasPointerPosition(110.75, 96.5, {
				left: 10.25,
				top: 48.75,
				width: 200,
				height: 100,
				coordinateWidth: 200,
				coordinateHeight: 100,
			}),
		).toEqual({ x: 100.5, y: 47.75 });
	});

	it("compensates ancestor scaling when mapping client pixels into the canvas", () => {
		expect(
			getCanvasPointerPosition(108, 54, {
				left: 10,
				top: 5,
				width: 196,
				height: 98,
				coordinateWidth: 200,
				coordinateHeight: 100,
			}),
		).toEqual({ x: 100, y: 50 });
	});

	it("captures transformed bounds alongside the logical canvas size", () => {
		const bounds = {
			left: 10,
			top: 5,
			width: 196,
			height: 98,
		} as DOMRect;
		expect(
			getCanvasSurfaceGeometry({
				clientWidth: 200,
				clientHeight: 100,
				getBoundingClientRect: () => bounds,
			}),
		).toEqual({
			left: 10,
			top: 5,
			width: 196,
			height: 98,
			coordinateWidth: 200,
			coordinateHeight: 100,
		});
	});

	it("keeps the painted separator pixel assigned to its owning source", () => {
		expect(getCanvasSourceRowIndex(47.999, 0, 2)).toBe(0);
		expect(getCanvasSourceRowIndex(48, 0, 2)).toBe(0);
		expect(getCanvasSourceRowIndex(48.999, 0, 2)).toBe(0);
		expect(getCanvasSourceRowIndex(49, 0, 2)).toBe(1);
		expect(getCanvasSourceRowIndex(47.999, 0, 1)).toBe(0);
		expect(getCanvasSourceRowIndex(48.999, 0, 1)).toBe(0);
		expect(getCanvasSourceRowIndex(49, 0, 1)).toBeNull();
	});

	it("applies fractional vertical scroll before resolving source boundaries", () => {
		expect(getCanvasSourceRowIndex(73.749, -25.75, 2)).toBe(0);
		expect(getCanvasSourceRowIndex(73.75, -25.75, 2)).toBe(0);
		expect(getCanvasSourceRowIndex(74.749, -25.75, 2)).toBe(0);
		expect(getCanvasSourceRowIndex(74.75, -25.75, 2)).toBe(1);
		expect(getCanvasSourceRowIndex(25.749, -25.75, 2)).toBeNull();
	});

	it("includes two 48-pixel rows of overscan above and below", () => {
		expect(getCanvasViewportSourceRange(20, 96, 96)).toEqual({
			firstIndex: 0,
			lastIndex: 5,
		});
	});

	it("clamps the range at the first and final source rows", () => {
		expect(getCanvasViewportSourceRange(3, 0, 500)).toEqual({
			firstIndex: 0,
			lastIndex: 2,
		});
	});

	it("returns ordered source IDs for the calculated range", () => {
		const sources = Array.from({ length: 8 }, (_, index) =>
			createSource(`source-${index}`),
		);

		expect(getCanvasViewportSourceIds(sources, 96, 96)).toEqual([
			"source-0",
			"source-1",
			"source-2",
			"source-3",
			"source-4",
			"source-5",
		]);
	});
});
