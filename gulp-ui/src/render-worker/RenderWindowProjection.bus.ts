import type { RenderWindowProjection } from "./RenderWorker.types";
import type { EventStoreViewportDescriptor } from "@/event-store/EventStore.viewport-projector";

export type RenderWindowProjectionListener = (
	projection: RenderWindowProjection,
	descriptor: EventStoreViewportDescriptor,
) => boolean | void;

/**
 * Ephemeral single-consumer bridge from QueryWorker (or benchmark writer) to Canvas.
 * It never retains projections because their buffers are transferred immediately to
 * the render worker and must have exactly one owner.
 */
export class RenderWindowProjectionBus {
	private listener: RenderWindowProjectionListener | null = null;

	/**
	 * Claims delivery for the currently mounted primary Canvas in this browser realm.
	 * @param listener Synchronous consumer that transfers projection buffers onward.
	 * @returns Cleanup that only releases the matching active claim.
	 */
	public subscribe(listener: RenderWindowProjectionListener): () => void {
		this.listener = listener;
		return () => {
			if (this.listener === listener) this.listener = null;
		};
	}

	/**
	 * Delivers one projection synchronously without retaining it on the main thread.
	 * @param projection Complete viewport replacement produced by EventStore LOD.
	 * @param descriptor Original transform and source settings used by the projector.
	 * @returns True when an active Canvas accepted the projection.
	 */
	public publish(projection: RenderWindowProjection, descriptor: EventStoreViewportDescriptor): boolean {
		const listener = this.listener;
		if (!listener) return false;
		return listener(projection, descriptor) !== false;
	}

	/** Releases the current claim during operation teardown and focused tests. */
	public clear(): void {
		this.listener = null;
	}
}

export const renderWindowProjectionBus = new RenderWindowProjectionBus();
