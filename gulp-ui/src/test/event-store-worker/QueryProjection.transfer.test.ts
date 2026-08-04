import { getRenderWindowProjectionTransferables } from "@/event-store-worker/QueryProjection.transfer";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

describe("getRenderWindowProjectionTransferables", () => {
	it("returns all seven unique ArrayBuffers for every projected source", () => {
		const projection: RenderWindowProjection = {
			operationEpoch: 1,
			viewportRevision: 2,
			projectionRevision: 3,
			evictedSourceIds: [],
			sources: [
				{
					sourceId: "source-one",
					sourceRevision: 4,
					coveredRange: { min: 0, max: 10 },
					bucketPixelWidth: 1,
					degradedLod: false,
					loadedCount: 1,
					hashRange: { min: 7, max: 7 },
					representativeHandles: new Uint32Array([1]),
					representativeTimestamps: new Float64Array([5]),
					representativeHashes: new Float64Array([7]),
					heightTimestamps: new Float64Array([5]),
					heightCounts: new Float64Array([1]),
					heightMaximumCount: 1,
					graphTimestamps: new Float64Array([5]),
					graphCounts: new Float64Array([1]),
				},
			],
		};

		const buffers = getRenderWindowProjectionTransferables(projection);
		expect(buffers).toHaveLength(7);
		expect(new Set(buffers).size).toBe(7);
	});
});
