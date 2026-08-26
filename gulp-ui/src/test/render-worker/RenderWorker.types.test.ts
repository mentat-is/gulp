import {
	DirtyCanvas,
	RENDER_PROTOCOL_VERSION,
	type RenderAckMessage,
	type RenderWindowProjection,
} from "@/render-worker/RenderWorker.types";

describe("render-worker protocol constants", () => {
	it("uses an explicit protocol version for every wire envelope", () => {
		expect(RENDER_PROTOCOL_VERSION).toBe(7);
	});

	it("describes one atomic multi-source viewport projection", () => {
		const projection = {
			operationEpoch: 2,
			viewportRevision: 11,
			projectionRevision: 7,
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
					hashRange: { min: -5, max: 5 },
					representativeHandles: Uint32Array.of(3),
					representativeTimestamps: Float64Array.of(150),
					representativeHashes: Float64Array.of(2),
					heightTimestamps: Float64Array.of(100),
					heightCounts: Float64Array.of(10),
					heightMaximumCount: 10,
					graphTimestamps: Float64Array.of(100),
					graphCounts: Float64Array.of(10),
				},
			],
			evictedSourceIds: ["source-b"],
		} satisfies RenderWindowProjection;

		expect(projection.sources[0].representativeHandles).toEqual(
			Uint32Array.of(3),
		);
		expect(projection.evictedSourceIds).toEqual(["source-b"]);
	});

	it("requires authoritative accepted and rejected source telemetry in ACKs", () => {
		const acknowledgement: RenderAckMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "ACK",
			payload: {
				acknowledgedSequence: 1,
				globalRevision: 4,
				sourceRevisions: [
					{ sourceId: "source-a", revision: 3, dataSequence: 1 },
				],
				rejectedSourceIds: ["source-b"],
			},
		};

		expect(acknowledgement.payload.sourceRevisions[0].revision).toBe(3);
		expect(acknowledgement.payload.sourceRevisions[0].dataSequence).toBe(1);
		expect(acknowledgement.payload.rejectedSourceIds).toEqual(["source-b"]);
	});

	it("coalesces independent dirty reasons without losing late invalidations", () => {
		let dirtyMask = DirtyCanvas.None;
		dirtyMask |= DirtyCanvas.Size;
		dirtyMask |= DirtyCanvas.Viewport;
		dirtyMask |= DirtyCanvas.Viewport;

		const capturedMask = dirtyMask;
		dirtyMask = DirtyCanvas.None;
		dirtyMask |= DirtyCanvas.Assets;

		expect(capturedMask).toBe(DirtyCanvas.Size | DirtyCanvas.Viewport);
		expect(dirtyMask).toBe(DirtyCanvas.Assets);
		expect(capturedMask & DirtyCanvas.Assets).toBe(DirtyCanvas.None);
	});
});
