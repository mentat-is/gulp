import { shouldStreamWorkerSourceSnapshot } from "@/app/body/WorkerSourceSnapshotPolicy";

describe("Worker source snapshot policy", () => {
	it("streams event snapshots for newly known sources", () => {
		expect(
			shouldStreamWorkerSourceSnapshot({
				streamMissingEvents: true,
				wasKnownSource: false,
				graphSignature: "non-graph",
				sourceRevision: 0,
				acknowledgedSourceRevision: 0,
			}),
		).toBe(true);
	});

	it("streams event snapshots when a known source has no acknowledged worker event view", () => {
		expect(
			shouldStreamWorkerSourceSnapshot({
				streamMissingEvents: true,
				wasKnownSource: true,
				graphSignature: "non-graph",
				previousGraphSignature: "non-graph",
				sourceRevision: 3,
				acknowledgedSourceRevision: 0,
			}),
		).toBe(true);
	});

	it("streams event snapshots when the acknowledged worker event view is stale", () => {
		expect(
			shouldStreamWorkerSourceSnapshot({
				streamMissingEvents: true,
				wasKnownSource: true,
				graphSignature: "non-graph",
				previousGraphSignature: "non-graph",
				sourceRevision: 4,
				acknowledgedSourceRevision: 3,
			}),
		).toBe(true);
	});

	it("does not restream stable acknowledged sources during membership sync", () => {
		expect(
			shouldStreamWorkerSourceSnapshot({
				streamMissingEvents: true,
				wasKnownSource: true,
				graphSignature: "non-graph",
				previousGraphSignature: "non-graph",
				sourceRevision: 3,
				acknowledgedSourceRevision: 3,
			}),
		).toBe(false);
	});

	it("streams graph sources when the graph sample signature changes", () => {
		expect(
			shouldStreamWorkerSourceSnapshot({
				streamMissingEvents: true,
				wasKnownSource: true,
				graphSignature: "graph:new",
				previousGraphSignature: "graph:old",
				sourceRevision: 3,
				acknowledgedSourceRevision: 3,
			}),
		).toBe(true);
	});
});
