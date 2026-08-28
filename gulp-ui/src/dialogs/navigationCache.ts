import type { Doc } from "@/entities/Doc";
import type { EventNeighborWindow } from "@/event-store/EventStore.reader";
import { WindowBridge } from "@/lib/WindowBridge";

const NAVIGATION_CACHE_RADIUS = Math.floor(
	WindowBridge.DIALOG_NAVIGATION_CACHE_WINDOW_SIZE / 2,
);
const NAVIGATION_VISIBLE_RADIUS = Math.floor(
	WindowBridge.DIALOG_NAVIGATION_VISIBLE_WINDOW_SIZE / 2,
);

/**
 * Finds an event inside a descending bounded navigation window.
 * @param neighborWindow Window returned by the EventStore neighbor API.
 * @param eventId Event identity to locate.
 * @returns Descending window index, or -1 when the event is not retained.
 */
export function findDialogNavigationEventIndex(
	neighborWindow: EventNeighborWindow | null,
	eventId: Doc.Id,
): number {
	return (
		neighborWindow?.events.findIndex((candidate) => candidate._id === eventId) ??
		-1
	);
}

/**
 * Checks whether a retained neighbor read can render a centered visible strip.
 * Short sides are reusable only when the original read reached a source edge.
 * @param neighborWindow Previously hydrated descending neighbor window.
 * @param event Event that should become the visible center.
 * @param sourceRevision Current durable revision for the event source.
 * @returns True when no new bounded EventStore read is required.
 */
export function hasDialogNavigationCoverage(
	neighborWindow: EventNeighborWindow | null,
	event: Doc.Type,
	sourceRevision: number,
): boolean {
	if (!neighborWindow || neighborWindow.currentIndex < 0) return false;
	if (
		sourceRevision > 0 &&
		neighborWindow.sourceRevision !== sourceRevision
	) {
		return false;
	}

	const selectedIndex = findDialogNavigationEventIndex(
		neighborWindow,
		event._id,
	);
	if (selectedIndex < 0) return false;
	if (
		neighborWindow.events[selectedIndex]?.["gulp.source_id"] !==
		event["gulp.source_id"]
	) {
		return false;
	}

	const retainedNewerCount = selectedIndex;
	const retainedOlderCount = neighborWindow.events.length - selectedIndex - 1;
	const reachedNewerSourceEdge =
		neighborWindow.currentIndex < NAVIGATION_CACHE_RADIUS;
	const reachedOlderSourceEdge =
		neighborWindow.events.length - neighborWindow.currentIndex - 1 <
		NAVIGATION_CACHE_RADIUS;

	return (
		(retainedNewerCount >= NAVIGATION_VISIBLE_RADIUS ||
			reachedNewerSourceEdge) &&
		(retainedOlderCount >= NAVIGATION_VISIBLE_RADIUS ||
			reachedOlderSourceEdge)
	);
}
