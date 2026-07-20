import type { Source } from "@/entities/Source";
import {
	hasRawQueryViewportWork,
	selectNextRawQueryQueueItem,
	type RawQueryQueueSource,
	type RawQueryViewportState,
} from "@/class/RawQueryScheduler";

/**
 * Creates a typed source id for scheduler tests.
 * @param value Source id string.
 * @returns Branded source id.
 */
function sourceId(value: string): Source.Id {
	return value as Source.Id;
}

/**
 * Creates ordered raw query queue fixtures.
 * @param ids Source ids in enqueue order.
 * @returns Queue entries with monotonic sequence values.
 */
function createQueue(ids: string[]): RawQueryQueueSource[] {
	return ids.map((id, index) => ({
		sourceId: sourceId(id),
		sequence: index,
	}));
}

/**
 * Creates viewport state with row order matching the provided ids.
 * @param ids Source ids in current Canvas viewport order.
 * @returns Viewport source set and order map.
 */
function createViewport(ids: string[]): RawQueryViewportState {
	return {
		sourceIds: new Set(ids.map(sourceId)),
		sourceOrder: new Map(ids.map((id, index) => [sourceId(id), index])),
	};
}

describe("RawQueryScheduler", () => {
	it("fills a four-request pool from queued background work", () => {
		const queue = createQueue([
			"source-0",
			"source-1",
			"source-2",
			"source-3",
			"source-4",
			"source-5",
		]);
		const viewport = createViewport(["not-queued"]);
		const started: string[] = [];

		while (started.length < 4) {
			const selection = selectNextRawQueryQueueItem(queue, viewport);
			if (!selection) break;

			const [item] = queue.splice(selection.index, 1);
			started.push(item.sourceId);
			expect(selection.priority).toBe("background");
		}

		expect(started).toEqual([
			"source-0",
			"source-1",
			"source-2",
			"source-3",
		]);
	});

	it("treats all queued sources as viewport work before the first viewport report", () => {
		const queue = createQueue(["source-0", "source-1"]);
		const viewport = createViewport([]);

		expect(hasRawQueryViewportWork(queue, viewport)).toBe(true);
		expect(selectNextRawQueryQueueItem(queue, viewport)).toEqual({
			index: 0,
			priority: "visible",
		});
	});

	it("reprioritizes queued sources against the latest viewport", () => {
		const queue = createQueue(["source-0", "source-1", "source-2", "source-3"]);

		expect(selectNextRawQueryQueueItem(queue, createViewport(["source-0"]))).toEqual({
			index: 0,
			priority: "visible",
		});
		expect(selectNextRawQueryQueueItem(queue, createViewport(["source-3"]))).toEqual({
			index: 3,
			priority: "visible",
		});
	});

	it("orders visible queued sources by current viewport row order", () => {
		const queue = createQueue(["source-3", "source-1", "source-2"]);
		const viewport = createViewport(["source-1", "source-2", "source-3"]);
		const started: string[] = [];

		while (queue.length > 0) {
			const selection = selectNextRawQueryQueueItem(queue, viewport);
			if (!selection) break;

			const [item] = queue.splice(selection.index, 1);
			started.push(item.sourceId);
			expect(selection.priority).toBe("visible");
		}

		expect(started).toEqual(["source-1", "source-2", "source-3"]);
	});

	it("falls back to oldest background work when no queued source is visible", () => {
		const queue = createQueue(["source-2", "source-0", "source-1"]);
		const selection = selectNextRawQueryQueueItem(
			queue,
			createViewport(["source-9"]),
		);

		expect(selection).toEqual({
			index: 0,
			priority: "background",
		});
		expect(hasRawQueryViewportWork(queue, createViewport(["source-9"]))).toBe(
			false,
		);
	});
});
