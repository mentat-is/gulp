import type { Source } from "@/entities/Source";

export type RawQueryQueuePriority = "visible" | "background";

export interface RawQueryQueueSource {
	sourceId: Source.Id;
	sequence: number;
}

export interface RawQueryViewportState {
	sourceIds: ReadonlySet<Source.Id>;
	sourceOrder: ReadonlyMap<Source.Id, number>;
}

export interface RawQueryQueueSelection {
	index: number;
	priority: RawQueryQueuePriority;
}

/**
 * Checks whether a queued raw query source is in the latest Canvas viewport.
 * @param sourceId Source id to classify.
 * @param viewport Latest viewport source ids and row ordering.
 * @returns True before the first viewport report or when the source is visible.
 */
export function isRawQuerySourceVisible(
	sourceId: Source.Id,
	viewport: RawQueryViewportState,
): boolean {
	return viewport.sourceIds.size === 0 || viewport.sourceIds.has(sourceId);
}

/**
 * Checks whether any queued raw query source currently has viewport priority.
 * @param queue Pending per-source raw query queue.
 * @param viewport Latest viewport source ids and row ordering.
 * @returns True when at least one pending source is visible.
 */
export function hasRawQueryViewportWork(
	queue: readonly RawQueryQueueSource[],
	viewport: RawQueryViewportState,
): boolean {
	return queue.some((item) => isRawQuerySourceVisible(item.sourceId, viewport));
}

/**
 * Selects the next queued raw query using the latest viewport priority.
 * @param queue Pending per-source raw query queue.
 * @param viewport Latest viewport source ids and row ordering.
 * @returns Queue index and current priority, or null when no source is queued.
 */
export function selectNextRawQueryQueueItem(
	queue: readonly RawQueryQueueSource[],
	viewport: RawQueryViewportState,
): RawQueryQueueSelection | null {
	let visibleIndex = -1;
	let visibleOrder = Number.POSITIVE_INFINITY;
	let visibleSequence = Number.POSITIVE_INFINITY;
	let backgroundIndex = -1;
	let backgroundSequence = Number.POSITIVE_INFINITY;

	for (let index = 0; index < queue.length; index++) {
		const item = queue[index];
		if (isRawQuerySourceVisible(item.sourceId, viewport)) {
			const order = viewport.sourceOrder.get(item.sourceId) ?? item.sequence;
			if (
				order < visibleOrder ||
				(order === visibleOrder && item.sequence < visibleSequence)
			) {
				visibleIndex = index;
				visibleOrder = order;
				visibleSequence = item.sequence;
			}
			continue;
		}

		if (item.sequence < backgroundSequence) {
			backgroundIndex = index;
			backgroundSequence = item.sequence;
		}
	}

	if (visibleIndex !== -1) {
		return { index: visibleIndex, priority: "visible" };
	}
	if (backgroundIndex !== -1) {
		return { index: backgroundIndex, priority: "background" };
	}
	return null;
}
