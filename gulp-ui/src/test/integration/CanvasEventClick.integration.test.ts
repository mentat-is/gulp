import {
	getCanvasEventLookupPixels,
	getCanvasEventPixel,
	getCanvasEventTimestampRange,
	isCanvasPixelWithinEventSourceRange,
	type CanvasEventLookupGeometry,
} from "@/app/body/CanvasEventLookup";
import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { EventStoreFacade } from "@/event-store/EventStore.facade";

const SOURCE_ID = "canvas-click-source" as Source.Id;

describe("Canvas exact event click integration", () => {
	it("resolves fractional deep-zoom clicks across flagged marker columns", async () => {
		const frameMin = 1_743_025_550_000;
		const frameRange = 100_000;
		const event: Doc.Type = {
			_id: "flagged-event" as Doc.Id,
			"gulp.source_id": SOURCE_ID,
			gulp_timestamp: frameMin + frameRange / 2,
			"gulp.event_code": 1,
			number_hash: 1,
		};
		const eventStore = new EventStoreFacade({
			resolveSource: () => [event],
			resolveSourceRevision: () => 1,
		});

		for (const visibleDuration of [2_000, 1_000, 500, 10]) {
			const scale = frameRange / visibleDuration;
			const canvasWidth = 1_000;
			const virtualEventPixel = Math.round(
				((event.gulp_timestamp - frameMin) / frameRange) *
					canvasWidth *
					scale,
			);
			const geometry: CanvasEventLookupGeometry = {
				sourceId: SOURCE_ID,
				frameMin,
				frameMax: frameMin + frameRange,
				canvasWidth,
				scale,
				scrollX: virtualEventPixel - 500 + 0.25,
				sourceOffset: 0,
				renderEngine: "default",
			};
			const eventPixel = getCanvasEventPixel(event.gulp_timestamp, geometry);

			for (const pointerX of [
				eventPixel - 0.5,
				eventPixel + 0.5,
				eventPixel + 1.5,
			]) {
				const lookupPixels = getCanvasEventLookupPixels(
					pointerX,
					geometry.scrollX,
				).filter((pixel) =>
					isCanvasPixelWithinEventSourceRange(
						pixel,
						event.gulp_timestamp,
						event.gulp_timestamp,
						geometry,
					),
				);
				let selectedEvent: Doc.Type | null = null;

				for (const pixel of lookupPixels) {
					const range = getCanvasEventTimestampRange(pixel, geometry);
					if (!range) continue;
					const page = await eventStore.queryPixelBucket(SOURCE_ID, range, 100);
					selectedEvent = page.events[0] ?? null;
					if (selectedEvent) break;
				}

				expect(selectedEvent?._id).toBe(event._id);
			}
		}
	});
});
