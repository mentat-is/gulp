import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import type { EventNeighborWindow } from "@/event-store/EventStore.reader";
import { hasDialogNavigationCoverage } from "@/dialogs/navigationCache";

const SOURCE_ID = "navigation-cache-source" as Source.Id;

/** Creates one descending-window event at a deterministic timestamp. */
function createEvent(index: number): Doc.Type {
	return {
		_id: `event-${index}` as Doc.Id,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": index,
		gulp_timestamp: 100 - index,
		number_hash: index,
	} as Doc.Type;
}

/** Creates a retained neighbor window around the requested anchor index. */
function createNeighborWindow(
	events: Doc.Type[],
	currentIndex: number,
): EventNeighborWindow {
	return {
		events,
		currentIndex,
		sourceRevision: 7,
	};
}

describe("Event Dialog navigation cache", () => {
	it("refreshes only after the visible radius crosses a retained edge", () => {
		const events = Array.from({ length: 51 }, (_, index) => createEvent(index));
		const neighborWindow = createNeighborWindow(events, 25);

		expect(
			hasDialogNavigationCoverage(neighborWindow, events[5], 7),
		).toBe(true);
		expect(
			hasDialogNavigationCoverage(neighborWindow, events[4], 7),
		).toBe(false);
	});

	it("keeps a short retained side when the source edge was reached", () => {
		const events = Array.from({ length: 30 }, (_, index) => createEvent(index));
		const neighborWindow = createNeighborWindow(events, 3);

		expect(
			hasDialogNavigationCoverage(neighborWindow, events[0], 7),
		).toBe(true);
	});

	it("invalidates a covered event when its source revision changes", () => {
		const events = Array.from({ length: 51 }, (_, index) => createEvent(index));
		const neighborWindow = createNeighborWindow(events, 25);

		expect(
			hasDialogNavigationCoverage(neighborWindow, events[25], 8),
		).toBe(false);
	});
});
