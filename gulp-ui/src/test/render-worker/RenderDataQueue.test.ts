import {
	MAX_RENDER_DATA_QUEUE_DEPTH,
	RenderDataQueue,
} from "@/render-worker/RenderDataQueue";

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
});
