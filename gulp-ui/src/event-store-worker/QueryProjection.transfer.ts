import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

/**
 * Collects every transferable projection buffer exactly once for zero-copy delivery.
 * @param projection Complete viewport projection produced in QueryWorker.
 * @returns ArrayBuffer transfer list safe for `postMessage`.
 */
export function getRenderWindowProjectionTransferables(
	projection: RenderWindowProjection,
): ArrayBuffer[] {
	const buffers = new Set<ArrayBuffer>();
	for (const source of projection.sources) {
		const views: ArrayBufferView[] = [
			source.representativeHandles,
			source.representativeTimestamps,
			source.representativeHashes,
			source.heightTimestamps,
			source.heightCounts,
			source.graphTimestamps,
			source.graphCounts,
		];
		for (const view of views) {
			if (view.buffer instanceof ArrayBuffer) buffers.add(view.buffer);
		}
	}
	return [...buffers];
}
