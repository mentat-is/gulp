import {
	DirtyCanvas,
	RENDER_PROTOCOL_VERSION,
	type RenderAckMessage,
} from "@/render-worker/RenderWorker.types";

describe("render-worker protocol constants", () => {
	it("uses an explicit protocol version for every wire envelope", () => {
		expect(RENDER_PROTOCOL_VERSION).toBe(6);
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
				sourceRevisions: [{ sourceId: "source-a", revision: 3 }],
				rejectedSourceIds: ["source-b"],
			},
		};

		expect(acknowledgement.payload.sourceRevisions[0].revision).toBe(3);
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
