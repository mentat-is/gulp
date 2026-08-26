import {
	isRenderWindowDataRequest,
	isRenderWindowProjection,
} from "@/render-worker/RenderWorker.request";
import { EVENT_RENDER_PROJECTION_LIMIT } from "@/event-store/EventStore.types";
import {
	RENDER_PROTOCOL_VERSION,
	type RenderWindowProjection,
} from "@/render-worker/RenderWorker.types";

/**
 * Creates one valid projection whose buffers can be selectively corrupted by tests.
 * @returns Complete single-source render-window fixture.
 */
function createProjection(): RenderWindowProjection {
	return {
		operationEpoch: 3,
		viewportRevision: 8,
		projectionRevision: 5,
		sources: [
			{
				sourceId: "source-a",
				sourceGeneration: 2,
				sourceRevision: 4,
				sourceStatus: "complete",
				coveredRange: { min: 100, max: 200 },
				bucketPixelWidth: 1,
				degradedLod: false,
				loadedCount: 10,
				hashRange: { min: -2, max: 7 },
				representativeHandles: Uint32Array.of(1, 2),
				representativeTimestamps: Float64Array.of(180, 120),
				representativeHashes: Float64Array.of(-2, 7),
				heightTimestamps: Float64Array.of(100, 150),
				heightCounts: Float64Array.of(4, 6),
				heightMaximumCount: 6,
				graphTimestamps: Float64Array.of(100),
				graphCounts: Float64Array.of(10),
			},
		],
		evictedSourceIds: ["source-b"],
	};
}

describe("render-window request validation", () => {
	it("accepts a complete atomic projection and matching operation envelope", () => {
		const projection = createProjection();

		expect(isRenderWindowProjection(projection)).toBe(true);
		expect(
			isRenderWindowDataRequest({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: projection.operationEpoch,
				sequence: 12,
				type: "DATA_PATCH",
				payload: { globalRevision: 5, renderWindow: projection },
			}),
		).toBe(true);
	});

	it("rejects mismatched representative and aggregate columns", () => {
		const projection = createProjection();
		projection.sources[0].representativeHashes = Float64Array.of(-2);
		expect(isRenderWindowProjection(projection)).toBe(false);

		projection.sources[0].representativeHashes = Float64Array.of(-2, 7);
		projection.sources[0].heightCounts = new Float64Array(0);
		expect(isRenderWindowProjection(projection)).toBe(false);
	});

	it("rejects duplicate, evicted, and malformed source identities", () => {
		const duplicate = createProjection();
		duplicate.sources.push(duplicate.sources[0]);
		expect(isRenderWindowProjection(duplicate)).toBe(false);

		const evicted = createProjection();
		evicted.evictedSourceIds.push("source-a");
		expect(isRenderWindowProjection(evicted)).toBe(false);

		const repeatedEviction = createProjection();
		repeatedEviction.evictedSourceIds.push("source-b");
		expect(isRenderWindowProjection(repeatedEviction)).toBe(false);
	});

	it("rejects stale protocol versions and operation mismatches", () => {
		const projection = createProjection();
		const request = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: projection.operationEpoch,
			sequence: 12,
			type: "DATA_PATCH",
			payload: { globalRevision: 5, renderWindow: projection },
		};

		expect(
			isRenderWindowDataRequest({ ...request, protocolVersion: 4 }),
		).toBe(false);
		expect(
			isRenderWindowDataRequest({ ...request, operationEpoch: 4 }),
		).toBe(false);
	});

	it("rejects a projection above the atomic one-million-record limit", () => {
		const projection = createProjection();
		const oversizedLength = EVENT_RENDER_PROJECTION_LIMIT + 1;
		projection.sources[0].loadedCount = oversizedLength;
		projection.sources[0].representativeHandles = new Uint32Array(oversizedLength);
		projection.sources[0].representativeTimestamps = new Float64Array(
			oversizedLength,
		);
		projection.sources[0].representativeHashes = new Float64Array(oversizedLength);

		expect(isRenderWindowProjection(projection)).toBe(false);
	});
});
