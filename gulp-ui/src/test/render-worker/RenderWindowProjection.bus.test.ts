import { RenderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type { EventStoreViewportDescriptor } from "@/event-store/EventStore.viewport-projector";

const projection = {
	operationEpoch: 1,
	viewportRevision: 2,
	projectionRevision: 3,
	sources: [],
	evictedSourceIds: [],
} satisfies RenderWindowProjection;
const descriptor: EventStoreViewportDescriptor = {
	operationEpoch: 1, viewportRevision: 2, projectionRevision: 3,
	frame: { min: 0, max: 100 }, width: 100, scale: 1, scrollX: 0,
	horizontalOverscanPixels: 100, sources: [],
};

describe("RenderWindowProjectionBus", () => {
	it("delivers synchronously without retaining a projection", () => {
		const bus = new RenderWindowProjectionBus();
		const received: RenderWindowProjection[] = [];
		const unsubscribe = bus.subscribe((value) => {
			received.push(value);
		});

		expect(bus.publish(projection, descriptor)).toBe(true);
		expect(received).toEqual([projection]);
		unsubscribe();
		expect(bus.publish(projection, descriptor)).toBe(false);
	});

	it("reports an explicit Canvas rejection to the publisher", () => {
		const bus = new RenderWindowProjectionBus();
		const listener = jest.fn(() => false);
		bus.subscribe(listener);

		expect(bus.publish(projection, descriptor)).toBe(false);
		expect(listener).toHaveBeenCalled();
		expect(listener.mock.calls).toEqual([[projection, descriptor]]);
	});

	it("keeps only the newest Canvas claim", () => {
		const bus = new RenderWindowProjectionBus();
		const first = jest.fn();
		const second = jest.fn();
		const releaseFirst = bus.subscribe(first);
		bus.subscribe(second);

		releaseFirst();
		bus.publish(projection, descriptor);

		expect(first).not.toHaveBeenCalled();
		expect(second.mock.calls).toEqual([[projection, descriptor]]);
	});
});
