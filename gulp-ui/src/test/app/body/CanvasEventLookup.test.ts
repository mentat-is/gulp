import {
	getCanvasEventPixel,
	getCanvasEventTimestampRange,
	isCanvasEventLookupSourceCurrent,
	isCanvasPixelWithinEventSourceRange,
	isEventInCanvasHitArea,
	shouldPreferCanvasEventWhileMagnifying,
} from "@/app/body/CanvasEventLookup";

const geometry = {
	sourceId: "source-a",
	frameMin: 1_000,
	frameMax: 2_000,
	canvasWidth: 1_000,
	scale: 2,
	scrollX: 300,
	sourceOffset: 25,
	renderEngine: "default" as const,
};

describe("Canvas exact event lookup transform", () => {
	it("uses the magnifier as explicit exact-event click-through intent", () => {
		expect(shouldPreferCanvasEventWhileMagnifying(false)).toBe(false);
		expect(shouldPreferCanvasEventWhileMagnifying(true)).toBe(true);
	});

	it("inverts every timestamp to a range containing exactly its rounded pixel", () => {
		for (let timestamp = 1_000; timestamp <= 2_000; timestamp += 0.125) {
			const pixel = getCanvasEventPixel(timestamp, geometry);
			const range = getCanvasEventTimestampRange(pixel, geometry);
			expect(range !== null).toBe(true);
			expect(timestamp >= (range?.min ?? Number.POSITIVE_INFINITY)).toBe(true);
			expect(timestamp <= (range?.max ?? Number.NEGATIVE_INFINITY)).toBe(true);
			expect(isEventInCanvasHitArea(timestamp, pixel, geometry)).toBe(true);
		}
	});

	it("keeps millisecond events selectable in a ten-millisecond viewport", () => {
		const frameMin = 1_743_025_550_000;
		const highZoomGeometry = {
			...geometry,
			frameMin,
			frameMax: frameMin + 1_000,
			canvasWidth: 1_000,
			scale: 100,
			scrollX: 0,
			sourceOffset: 0,
		};

		for (let offset = 0; offset <= 10; offset++) {
			const timestamp = frameMin + offset;
			const pixel = getCanvasEventPixel(timestamp, highZoomGeometry);
			const range = getCanvasEventTimestampRange(pixel, highZoomGeometry);

			expect(range !== null).toBe(true);
			expect(timestamp >= (range?.min ?? Number.POSITIVE_INFINITY)).toBe(true);
			expect(timestamp <= (range?.max ?? Number.NEGATIVE_INFINITY)).toBe(true);
		}
	});

	it("excludes the upper rounding boundary owned by the adjacent pixel", () => {
		const pixel = 400;
		const range = getCanvasEventTimestampRange(pixel, geometry);
		expect(range !== null).toBe(true);
		const upperBoundary =
			geometry.frameMin +
			(pixel + geometry.scrollX + 0.5) *
				((geometry.frameMax - geometry.frameMin) /
					(geometry.canvasWidth * geometry.scale)) -
			geometry.sourceOffset;
		expect((range?.max ?? upperBoundary) < upperBoundary).toBe(true);
		expect(getCanvasEventPixel(upperBoundary, geometry)).toBe(pixel + 1);
	});

	it("expands graph lookups across the current sixteen-pixel hit area", () => {
		const eventTimestamp = 1_500;
		const eventPixel = getCanvasEventPixel(eventTimestamp, geometry);
		const clickedPixel = eventPixel - 16;
		const range = getCanvasEventTimestampRange(clickedPixel, geometry, 16);
		expect(eventTimestamp >= (range?.min ?? Number.POSITIVE_INFINITY)).toBe(true);
		expect(eventTimestamp <= (range?.max ?? Number.NEGATIVE_INFINITY)).toBe(true);
		expect(
			isEventInCanvasHitArea(eventTimestamp, clickedPixel, geometry, 16),
		).toBe(true);
	});

	it("keeps the graph marker clickable at the source's left boundary", () => {
		const graphGeometry = { ...geometry, renderEngine: "graph" as const };
		const sourceMinimum = 1_250;
		const sourceMaximum = 1_750;
		const minimumPixel = getCanvasEventPixel(sourceMinimum, graphGeometry);

		expect(
			isCanvasPixelWithinEventSourceRange(
				minimumPixel - 16,
				sourceMinimum,
				sourceMaximum,
				graphGeometry,
				16,
			),
		).toBe(true);
		expect(
			isCanvasPixelWithinEventSourceRange(
				minimumPixel - 17,
				sourceMinimum,
				sourceMaximum,
				graphGeometry,
				16,
			),
		).toBe(false);
	});

	it("invalidates a pending lookup when source offset or engine changes", () => {
		expect(
			isCanvasEventLookupSourceCurrent(geometry, {
				sourceId: geometry.sourceId,
				sourceOffset: geometry.sourceOffset,
				renderEngine: geometry.renderEngine,
			}),
		).toBe(true);
		expect(
			isCanvasEventLookupSourceCurrent(geometry, {
				sourceId: geometry.sourceId,
				sourceOffset: geometry.sourceOffset + 1,
				renderEngine: geometry.renderEngine,
			}),
		).toBe(false);
		expect(
			isCanvasEventLookupSourceCurrent(geometry, {
				sourceId: geometry.sourceId,
				sourceOffset: geometry.sourceOffset,
				renderEngine: "graph",
			}),
		).toBe(false);
	});

	it("rejects degenerate viewport geometry", () => {
		expect(
			getCanvasEventTimestampRange(0, { ...geometry, frameMax: geometry.frameMin }),
		).toBe(null);
		expect(
			getCanvasEventTimestampRange(0, { ...geometry, scale: 0 }),
		).toBe(null);
	});
});
