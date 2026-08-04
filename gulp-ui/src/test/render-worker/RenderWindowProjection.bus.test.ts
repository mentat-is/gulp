import { RenderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

const projection = {
	operationEpoch: 1,
	viewportRevision: 2,
	projectionRevision: 3,
	sources: [],
	evictedSourceIds: [],
} satisfies RenderWindowProjection;

describe("RenderWindowProjectionBus", () => {
	it("delivers synchronously without retaining a projection", () => {
		const bus = new RenderWindowProjectionBus();
		const received: RenderWindowProjection[] = [];
		const unsubscribe = bus.subscribe((value) => {
			received.push(value);
		});

		expect(bus.publish(projection)).toBe(true);
		expect(received).toEqual([projection]);
		unsubscribe();
		expect(bus.publish(projection)).toBe(false);
	});

	it("reports an explicit Canvas rejection to the publisher", () => {
		const bus = new RenderWindowProjectionBus();
		const listener = jest.fn(() => false);
		bus.subscribe(listener);

		expect(bus.publish(projection)).toBe(false);
		expect(listener).toHaveBeenCalled();
		expect(listener.mock.calls).toEqual([[projection]]);
	});

	it("keeps only the newest Canvas claim", () => {
		const bus = new RenderWindowProjectionBus();
		const first = jest.fn();
		const second = jest.fn();
		const releaseFirst = bus.subscribe(first);
		bus.subscribe(second);

		releaseFirst();
		bus.publish(projection);

		expect(first).not.toHaveBeenCalled();
		expect(second.mock.calls).toEqual([[projection]]);
	});
});
