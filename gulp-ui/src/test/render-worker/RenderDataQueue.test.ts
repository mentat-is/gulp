import {
	MAX_RENDER_DATA_QUEUE_DEPTH,
	RenderDataQueue,
} from "@/render-worker/RenderDataQueue";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

/**
 * Creates an empty atomic projection for deterministic queue coalescing tests.
 * @param viewportRevision Viewport revision owned by the projection.
 * @param projectionRevision Projection revision within that viewport.
 * @returns Complete zero-event render window.
 */
function createRenderWindow(
	viewportRevision: number,
	projectionRevision: number,
): RenderWindowProjection {
	return {
		operationEpoch: 1,
		viewportRevision,
		projectionRevision,
		sources: [],
		evictedSourceIds: [],
	};
}

describe("RenderDataQueue", () => {
	it("never exceeds one in-flight plus one coalesced pending envelope", () => {
		const queue = new RenderDataQueue();
		const observedDepths: number[] = [];
		/** Records the current aggregate depth after one deterministic transition. */
		const recordDepth = (): void => {
			observedDepths.push(queue.depth.aggregate);
		};

		queue.enqueue({ globalRevision: 1 });
		recordDepth();
		const firstPatch = queue.dispatch(10);
		recordDepth();
		queue.enqueue({ globalRevision: 2 });
		recordDepth();
		queue.enqueue({ globalRevision: 3 });
		recordDepth();
		queue.enqueue({ globalRevision: 4 });
		recordDepth();

		expect(firstPatch?.globalRevision).toBe(1);
		expect(queue.dispatch(11)).toBeNull();
		expect(queue.depth).toEqual({ inFlight: 1, pending: 1, aggregate: 2 });
		expect(
			Math.max(...observedDepths) <= MAX_RENDER_DATA_QUEUE_DEPTH,
		).toBe(true);

		expect(queue.acknowledge(10)).toBe(true);
		const coalescedPatch = queue.dispatch(12);
		expect(coalescedPatch?.globalRevision).toBe(4);
		expect(queue.depth).toEqual({ inFlight: 1, pending: 0, aggregate: 1 });
		expect(queue.acknowledge(12)).toBe(true);
		expect(queue.depth.aggregate).toBe(0);
	});

	it("clears both queue slots at an operation reset", () => {
		const queue = new RenderDataQueue();
		queue.enqueue({ globalRevision: 1 });
		queue.dispatch(1);
		queue.enqueue({ globalRevision: 2 });

		queue.clear();

		expect(queue.depth).toEqual({ inFlight: 0, pending: 0, aggregate: 0 });
		expect(queue.acknowledge(1)).toBe(false);
	});

	it("keeps one newest pending render window while preserving queue depth two", () => {
		const queue = new RenderDataQueue();
		queue.enqueue({ globalRevision: 1 });
		expect(queue.dispatch(1)).not.toBeNull();

		const viewportTwo = createRenderWindow(2, 1);
		const viewportThree = createRenderWindow(3, 2);
		const lateViewportOne = createRenderWindow(1, 99);
		queue.enqueue({ globalRevision: 2, renderWindow: viewportTwo });
		queue.enqueue({ globalRevision: 3, renderWindow: viewportThree });
		queue.enqueue({ globalRevision: 4, renderWindow: lateViewportOne });

		expect(queue.depth).toEqual({ inFlight: 1, pending: 1, aggregate: 2 });
		expect(
			queue.depth.aggregate <= MAX_RENDER_DATA_QUEUE_DEPTH,
		).toBe(true);

		queue.acknowledge(1);
		const pending = queue.dispatch(2);
		expect(pending?.renderWindow).toBe(viewportThree);
		expect(pending?.renderWindow?.sources).toHaveLength(0);
	});

	it("rejects a malformed render window before it enters either queue slot", () => {
		const queue = new RenderDataQueue();
		const malformed = {
			...createRenderWindow(1, 1),
			viewportRevision: Number.NaN,
		} as unknown as RenderWindowProjection;

		let thrownError: unknown;
		try {
			queue.enqueue({ globalRevision: 1, renderWindow: malformed });
		} catch (error) {
			thrownError = error;
		}
		expect(thrownError).toEqual(
			new Error("Malformed render-window projection"),
		);
		expect(queue.depth.aggregate).toBe(0);
	});

	it("does not queue a stale window behind a newer in-flight projection", () => {
		const queue = new RenderDataQueue();
		const current = createRenderWindow(5, 2);
		queue.enqueue({ globalRevision: 2, renderWindow: current });
		expect(queue.dispatch(1)?.renderWindow).toBe(current);

		queue.enqueue({
			globalRevision: 3,
			renderWindow: createRenderWindow(4, 99),
		});
		queue.acknowledge(1);

		expect(queue.dispatch(2)?.renderWindow).toBeUndefined();
	});
});
