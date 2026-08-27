import {
	type RenderDataPatch,
	type RenderEventBatch,
	type RenderNoteRecord,
	type RenderSource,
	type RenderWindowProjection,
	type RenderWindowSourceProjection,
} from "@/render-worker/RenderWorker.types";
import { WorkerRenderRepository } from "@/render-worker/WorkerRenderRepository";
import { MessageChannel as NodeMessageChannel } from "worker_threads";

const originalMessageChannel = globalThis.MessageChannel;

/**
 * Sends an array buffer through a real structured-clone transfer list.
 * @param buffer Transfer-owned buffer to send through the test channel.
 * @returns Buffer received at the opposite message port.
 */
function transferArrayBuffer(buffer: ArrayBuffer): Promise<ArrayBuffer> {
	const channel = new MessageChannel();
	return new Promise<ArrayBuffer>((resolve) => {
		channel.port2.onmessage = (event: MessageEvent<ArrayBuffer>) => {
			channel.port1.close();
			channel.port2.close();
			resolve(event.data);
		};
		channel.port1.postMessage(buffer, [buffer]);
	});
}

/**
 * Creates deterministic render-only source metadata for repository tests.
 * @param overrides Source fields to replace in the default fixture.
 * @returns Complete render-source fixture.
 */
function createRenderSource(overrides: Partial<RenderSource> = {}): RenderSource {
	return {
		id: "source-a",
		revision: 1,
		rowOrder: 0,
		name: "Source A",
		contextLabel: "Context",
		contextColor: "#556677",
		contextTintAlpha: 0.1,
		timestamp: { min: 0, max: 240_000 },
		total: 3,
		loadingKind: "none",
		cacheFreshness: "fresh",
		offset: 0,
		engine: "default",
		frequency: 1,
		gradient: ["#000000", "#ffffff"],
		overrides: [],
		customPaletteColors: [],
		...overrides,
	};
}

/**
 * Creates a compact transferable event batch with deterministic defaults.
 * @param overrides Batch fields to replace in the default fixture.
 * @returns Complete event-batch fixture backed by typed arrays.
 */
function createEventBatch(
	overrides: Partial<RenderEventBatch> = {},
): RenderEventBatch {
	return {
		sourceId: "source-a",
		baseRevision: 0,
		revision: 1,
		mode: "replace",
		handles: Uint32Array.from([11, 12, 13]),
		timestamps: Float64Array.from([130_000, 70_000, 10_000]),
		hashes: Float64Array.from([4, 8, -2]),
		...overrides,
	};
}

/**
 * Creates a data patch containing one source and optional event mutations.
 * @param source Source metadata included in the patch.
 * @param events Event batches included in the patch.
 * @param globalRevision Global patch revision.
 * @returns Repository data patch fixture.
 */
function createSourcePatch(
	source: RenderSource,
	events: RenderEventBatch[] = [],
	globalRevision = 1,
): RenderDataPatch {
	return {
		globalRevision,
		sources: {
			revision: source.revision,
			records: [source],
			removedSourceIds: [],
		},
		events,
	};
}

/**
 * Creates one deterministic source projection backed by exact-size typed arrays.
 * @param overrides Projection fields to replace in the default fixture.
 * @returns Complete viewport-resolution source projection.
 */
function createRenderWindowSourceProjection(
	overrides: Partial<RenderWindowSourceProjection> = {},
): RenderWindowSourceProjection {
	return {
		sourceId: "source-a",
		sourceAuthorityRevision: 1,
		sourceGeneration: 1,
		sourceRevision: 1,
		sourceStatus: "complete",
		coveredRange: { min: 0, max: 240_000 },
		bucketPixelWidth: 1,
		degradedLod: false,
		loadedCount: 3,
		hashRange: { min: -2, max: 8 },
		representativeHandles: Uint32Array.of(11, 12, 13),
		representativeTimestamps: Float64Array.of(130_000, 70_000, 10_000),
		representativeHashes: Float64Array.of(4, 8, -2),
		heightTimestamps: Float64Array.of(0, 60_000, 120_000),
		heightCounts: Float64Array.of(1, 1, 1),
		heightMaximumCount: 1,
		graphTimestamps: Float64Array.of(0, 60_000, 120_000),
		graphCounts: Float64Array.of(1, 1, 1),
		...overrides,
	};
}

/**
 * Creates one complete multi-source render-window replacement.
 * @param overrides Root projection fields to replace in the default fixture.
 * @returns Atomic render-window projection fixture.
 */
function createRenderWindowProjection(
	overrides: Partial<RenderWindowProjection> = {},
): RenderWindowProjection {
	return {
		operationEpoch: 1,
		viewportRevision: 1,
		projectionRevision: 1,
		sources: [createRenderWindowSourceProjection()],
		evictedSourceIds: [],
		...overrides,
	};
}

describe("WorkerRenderRepository", () => {
	let repository: WorkerRenderRepository;

	beforeAll(() => {
		globalThis.MessageChannel =
			NodeMessageChannel as unknown as typeof MessageChannel;
	});

	afterAll(() => {
		globalThis.MessageChannel = originalMessageChannel;
	});

	beforeEach(() => {
		repository = new WorkerRenderRepository();
	});

	afterEach(() => {
		repository.dispose();
	});

	it("commits an initial revision-zero replacement snapshot", async () => {
		const result = await repository.applyDataPatch({
			globalRevision: 0,
			sources: {
				revision: 1,
				records: [createRenderSource()],
				removedSourceIds: [],
			},
			events: [
				createEventBatch({
					revision: 0,
					handles: Uint32Array.of(1),
					timestamps: Float64Array.of(10),
					hashes: Float64Array.of(5),
				}),
			],
		});

		expect(result.committedEvents).toBe(true);
		expect(repository.getEvents("source-a")?.revision).toBe(0);
	});

	it("commits typed replacement buffers and precomputes hot-path metadata", async () => {
		const result = await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);
		const eventView = repository.getEvents("source-a");

		expect(result).toMatchObject({
			committedEvents: true,
			committedSources: true,
			resync: [],
		});
		expect(eventView?.handles).toBeInstanceOf(Uint32Array);
		expect(eventView?.timestamps).toBeInstanceOf(Float64Array);
		expect(eventView?.hashes).toBeInstanceOf(Float64Array);
		expect(eventView?.hashMin).toBe(-2);
		expect(eventView?.hashMax).toBe(8);
		expect(Array.from(eventView?.heightTimestamps ?? [])).toEqual([
			0, 60_000, 120_000,
		]);
		expect(Array.from(eventView?.heightCounts ?? [])).toEqual([1, 1, 1]);
		expect(repository.getGlobalRevision()).toBe(1);
	});

	it("assembles snapshot chunks and exposes them only after commit", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [], 1),
		);
		await repository.applyDataPatch({
			globalRevision: 2,
			eventSnapshots: [
				{
					kind: "begin",
					snapshotId: 7,
					sourceId: "source-a",
					revision: 1,
					totalEvents: 3,
				},
			],
		});
		await repository.applyDataPatch({
			globalRevision: 2,
			eventSnapshots: [
				{
					kind: "chunk",
					snapshotId: 7,
					sourceId: "source-a",
					offset: 0,
					handles: Uint32Array.of(11, 12),
					timestamps: Float64Array.of(130_000, 70_000),
					hashes: Float64Array.of(4, 8),
				},
			],
		});
		await repository.applyDataPatch({
			globalRevision: 2,
			eventSnapshots: [
				{
					kind: "chunk",
					snapshotId: 7,
					sourceId: "source-a",
					offset: 2,
					handles: Uint32Array.of(13),
					timestamps: Float64Array.of(10_000),
					hashes: Float64Array.of(-2),
				},
			],
		});

		expect(repository.getEvents("source-a")).toBeUndefined();
		expect(repository.getGlobalRevision()).toBe(1);
		expect(repository.getAcknowledgedGlobalRevision()).toBe(2);

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			eventSnapshots: [
				{
					kind: "commit",
					snapshotId: 7,
					sourceId: "source-a",
					revision: 1,
				},
			],
		});

		expect(result.committedEvents).toBe(true);
		expect(Array.from(repository.getEvents("source-a")?.handles ?? [])).toEqual([
			11, 12, 13,
		]);
		expect(repository.getGlobalRevision()).toBe(2);
	});

	it("retains bounded snapshot receive headroom for the next edge delta", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [], 1),
		);
		await repository.applyDataPatch({
			globalRevision: 2,
			eventSnapshots: [
				{
					kind: "begin",
					snapshotId: 8,
					sourceId: "source-a",
					revision: 1,
					totalEvents: 3,
				},
				{
					kind: "chunk",
					snapshotId: 8,
					sourceId: "source-a",
					offset: 0,
					handles: Uint32Array.of(11, 12, 13),
					timestamps: Float64Array.of(130_000, 70_000, 10_000),
					hashes: Float64Array.of(4, 8, -2),
				},
				{
					kind: "commit",
					snapshotId: 8,
					sourceId: "source-a",
					revision: 1,
				},
			],
		});
		const snapshotView = repository.getEvents("source-a");
		const snapshotBuffer = snapshotView?.handles.buffer;

		const deltaResult = await repository.applyDataPatch({
			globalRevision: 3,
			events: [
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.of(14),
					timestamps: Float64Array.of(5_000),
					hashes: Float64Array.of(3),
				}),
			],
		});

		expect(deltaResult.committedEvents).toBe(true);
		expect(repository.getEvents("source-a")?.handles.buffer).toBe(snapshotBuffer);
		expect(Array.from(repository.getEvents("source-a")?.handles ?? [])).toEqual([
			11, 12, 13, 14,
		]);
	});

	it("detaches transferable typed-array storage without detaching canonical data", async () => {
		const canonicalHandles = [1, 2, 3];
		const projection = Uint32Array.from(canonicalHandles);
		const transferredBuffer = transferArrayBuffer(
			projection.buffer as ArrayBuffer,
		);

		expect(projection.byteLength).toBe(0);
		expect(canonicalHandles).toEqual([1, 2, 3]);
		expect(Array.from(new Uint32Array(await transferredBuffer))).toEqual([
			1, 2, 3,
		]);
	});

	it("invalidates same-length replacements when the source revision advances", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);
		const previousView = repository.getEvents("source-a");
		const replacement = createEventBatch({
			revision: 2,
			handles: Uint32Array.from([21, 22, 23]),
			timestamps: Float64Array.from([230_000, 170_000, 110_000]),
			hashes: Float64Array.from([9, 10, 11]),
		});

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [replacement],
		});
		const nextView = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(nextView).not.toBe(previousView);
		expect(nextView?.revision).toBe(2);
		expect(Array.from(nextView?.handles ?? [])).toEqual([21, 22, 23]);
		expect(Array.from(nextView?.timestamps ?? [])).toEqual([
			230_000, 170_000, 110_000,
		]);
	});

	it("rejects stale batches and requests a source resync for an upsert gap", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);
		const staleResult = await repository.applyDataPatch({
			globalRevision: 2,
			events: [createEventBatch({ revision: 0 })],
		});
		const gapResult = await repository.applyDataPatch({
			globalRevision: 3,
			events: [
				createEventBatch({
					baseRevision: 99,
					revision: 3,
					mode: "upsert",
					handles: Uint32Array.of(11),
					timestamps: Float64Array.of(180_000),
					hashes: Float64Array.of(6),
				}),
			],
		});

		expect(staleResult.committedEvents).toBe(false);
		expect(gapResult.committedEvents).toBe(false);
		expect(gapResult.resync).toEqual([
			{
				scope: "source",
				id: "source-a",
				expectedRevision: 1,
				receivedRevision: 99,
			},
		]);
		expect(repository.getEvents("source-a")?.revision).toBe(1);
	});

	it("refreshes graph samples from an authoritative same-revision snapshot", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				createEventBatch({
					sampleTimestamps: Float64Array.of(60_000),
					sampleCounts: Float64Array.of(25),
				}),
			],
		});

		expect(result.committedEvents).toBe(true);
		expect(
			Array.from(repository.getEvents("source-a")?.sampleCounts ?? []),
		).toEqual([25]);
	});

	it("derives graph samples at the configured frequency when no cache exists", async () => {
		await repository.applyDataPatch(
			createSourcePatch(
				createRenderSource({ engine: "graph", frequency: 60_000 }),
				[createEventBatch()],
			),
		);

		const eventView = repository.getEvents("source-a");

		expect(Array.from(eventView?.sampleTimestamps ?? [])).toEqual([
			0, 60_000, 120_000,
		]);
		expect(Array.from(eventView?.sampleCounts ?? [])).toEqual([1, 1, 1]);
		expect(eventView?.samplesAreDerived).toBe(true);
	});

	it("re-derives graph samples when an upsert explicitly clears cached samples", async () => {
		await repository.applyDataPatch(
			createSourcePatch(
				createRenderSource({ engine: "graph", frequency: 60_000 }),
				[
					createEventBatch({
						sampleTimestamps: Float64Array.of(0),
						sampleCounts: Float64Array.of(99),
					}),
				],
			),
		);

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.of(11),
					timestamps: Float64Array.of(130_000),
					hashes: Float64Array.of(5),
					sampleTimestamps: new Float64Array(0),
					sampleCounts: new Float64Array(0),
				}),
			],
		});
		const eventView = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(Array.from(eventView?.sampleTimestamps ?? [])).toEqual([
			0, 60_000, 120_000,
		]);
		expect(Array.from(eventView?.sampleCounts ?? [])).toEqual([1, 1, 1]);
		expect(eventView?.samplesAreDerived).toBe(true);
	});

	it("merges stable-handle upserts and preserves descending timestamp order", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.from([12, 14]),
					timestamps: Float64Array.from([250_000, 100_000]),
					hashes: Float64Array.from([80, 40]),
				}),
			],
		});
		const eventView = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(Array.from(eventView?.handles ?? [])).toEqual([12, 11, 14, 13]);
		expect(Array.from(eventView?.timestamps ?? [])).toEqual([
			250_000, 130_000, 100_000, 10_000,
		]);
		expect(Array.from(eventView?.hashes ?? [])).toEqual([80, 4, 40, -2]);
	});

	it("appends strictly older query pages without rebuilding committed metadata", async () => {
		await repository.applyDataPatch(
			createSourcePatch(
				createRenderSource({ timestamp: { min: -120_000, max: 240_000 } }),
				[createEventBatch()],
			),
		);
		const previousView = repository.getEvents("source-a");

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.from([15, 14]),
					timestamps: Float64Array.from([-20_000, -10_000]),
					hashes: Float64Array.from([20, -20]),
				}),
			],
		});
		const eventView = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(Array.from(eventView?.handles ?? [])).toEqual([11, 12, 13, 14, 15]);
		expect(Array.from(eventView?.timestamps ?? [])).toEqual([
			130_000, 70_000, 10_000, -10_000, -20_000,
		]);
		expect(eventView?.hashMin).toBe(-20);
		expect(eventView?.hashMax).toBe(20);
		expect(eventView?.handleIndexes[14]).toBe(4);
		expect(eventView?.heightTimestamps).not.toBe(previousView?.heightTimestamps);
		expect(Array.from(eventView?.heightCounts ?? [])).toEqual([2, 1, 1, 1]);
	});

	it("reuses spare backing buffers and performs only delta-sized work for older pages", async () => {
		const eventCount = 50_000;
		await repository.applyDataPatch(
			createSourcePatch(
				createRenderSource({
					timestamp: { min: -120_000, max: eventCount },
					total: eventCount,
					overrides: [{ hash: -1, color: "#ff0000" }],
				}),
				[
					createEventBatch({
						handles: Uint32Array.from(
							{ length: eventCount },
							(_, index) => index + 1,
						),
						timestamps: Float64Array.from(
							{ length: eventCount },
							(_, index) => eventCount - index,
						),
						hashes: Float64Array.from(
							{ length: eventCount },
							(_, index) => index,
						),
					}),
				],
			),
		);
		const previousView = repository.getEvents("source-a");
		const originalNow = performance.now;
		let performanceChecks = 0;
		Object.defineProperty(performance, "now", {
			configurable: true,
			value: (): number => {
				performanceChecks++;
				return 0;
			},
		});

		try {
			await repository.applyDataPatch({
				globalRevision: 2,
				events: [
					createEventBatch({
						baseRevision: 1,
						revision: 2,
						mode: "upsert",
						handles: Uint32Array.of(eventCount + 2, eventCount + 1),
						timestamps: Float64Array.of(-2, -1),
						hashes: Float64Array.of(-1, 10),
					}),
				],
			});
		} finally {
			Object.defineProperty(performance, "now", {
				configurable: true,
				value: originalNow,
			});
		}
		const nextView = repository.getEvents("source-a");

		expect(nextView?.handles.buffer).toBe(previousView?.handles.buffer);
		expect(nextView?.timestamps.buffer).toBe(previousView?.timestamps.buffer);
		expect(nextView?.hashes.buffer).toBe(previousView?.hashes.buffer);
		expect(nextView?.handleIndexes.buffer).toBe(
			previousView?.handleIndexes.buffer,
		);
		expect(nextView?.overrideIndexes.buffer).toBe(
			previousView?.overrideIndexes.buffer,
		);
		expect(nextView?.heightTimestamps.buffer).toBe(
			previousView?.heightTimestamps.buffer,
		);
		expect(nextView?.handles.length).toBe(eventCount + 2);
		expect(nextView?.handleIndexes[eventCount + 2]).toBe(eventCount + 2);
		expect(performanceChecks < 1_000).toBe(true);
	});

	it("advances aligned outward bounds before incrementally applying the following delta", async () => {
		const source = createRenderSource({
			engine: "graph",
			frequency: 60_000,
		});
		await repository.applyDataPatch(
			createSourcePatch(source, [createEventBatch()]),
		);
		const initialView = repository.getEvents("source-a");
		const expandedSource = createRenderSource({
			revision: 2,
			engine: "graph",
			frequency: 60_000,
			timestamp: { min: -60_000, max: 300_000 },
			gradient: ["#112233", "#ddeeff"],
		});

		const metadataResult = await repository.applyDataPatch(
			createSourcePatch(expandedSource, [], 2),
		);
		const expandedView = repository.getEvents("source-a");
		const deltaResult = await repository.applyDataPatch({
			globalRevision: 3,
			events: [
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.of(14),
					timestamps: Float64Array.of(-10_000),
					hashes: Float64Array.of(12),
				}),
			],
		});
		const deltaView = repository.getEvents("source-a");

		expect(metadataResult.committedEvents).toBe(true);
		expect(expandedView?.handles).toBe(initialView?.handles);
		expect(expandedView?.heightTimestamps).toBe(initialView?.heightTimestamps);
		expect(expandedView?.colorSourceRevision).toBe(2);
		expect(deltaResult.committedEvents).toBe(true);
		expect(deltaView?.handles.buffer).toBe(initialView?.handles.buffer);
		expect(Array.from(deltaView?.timestamps ?? [])).toEqual([
			130_000,
			70_000,
			10_000,
			-10_000,
		]);
		expect(Array.from(deltaView?.sampleCounts ?? [])).toEqual([1, 1, 1, 1]);
	});

	it("applies a queued replacement and upsert sequentially for one source", async () => {
		await repository.applyDataPatch(createSourcePatch(createRenderSource()));

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				createEventBatch(),
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.from([12, 14]),
					timestamps: Float64Array.from([250_000, 100_000]),
					hashes: Float64Array.from([80, 40]),
				}),
			],
		});

		expect(result.committedEvents).toBe(true);
		expect(
			Array.from(repository.getEvents("source-a")?.timestamps ?? []),
		).toEqual([250_000, 130_000, 100_000, 10_000]);
	});

	it("commits same-timestamp hash deltas through the stable-handle fast path", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);
		const previousView = repository.getEvents("source-a");

		const result = await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				createEventBatch({
					baseRevision: 1,
					revision: 2,
					mode: "upsert",
					handles: Uint32Array.of(12),
					timestamps: Float64Array.of(70_000),
					hashes: Float64Array.of(80),
				}),
			],
		});
		const nextView = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(nextView?.revision).toBe(2);
		expect(nextView?.handles).toBe(previousView?.handles);
		expect(nextView?.timestamps).toBe(previousView?.timestamps);
		expect(nextView?.hashes).toBe(previousView?.hashes);
		expect(Array.from(nextView?.hashes ?? [])).toEqual([4, 80, -2]);
		expect(nextView?.hashMin).toBe(-2);
		expect(nextView?.hashMax).toBe(80);
	});

	it("cancels staged preprocessing promptly after an operation reset", async () => {
		let clock = 0;
		const originalNow = performance.now;
		Object.defineProperty(performance, "now", {
			configurable: true,
			value: (): number => {
				clock += 5;
				return clock;
			},
		});
		try {
			const eventCount = 20_000;
			const pendingPatch = repository.applyDataPatch(
				createSourcePatch(createRenderSource(), [
					createEventBatch({
						handles: Uint32Array.from(
							{ length: eventCount },
							(_, index) => index + 1,
						),
						timestamps: Float64Array.from(
							{ length: eventCount },
							(_, index) => eventCount - index,
						),
						hashes: Float64Array.from(
							{ length: eventCount },
							(_, index) => index,
						),
					}),
				]),
			);

			repository.resetOperation();
			const result = await pendingPatch;

			expect(result.committedEvents).toBe(false);
			expect(result.committedSources).toBe(false);
			expect(repository.getEvents("source-a")).toBeUndefined();
			expect(repository.getGlobalRevision()).toBe(0);
			expect(clock <= 5).toBe(true);
		} finally {
			Object.defineProperty(performance, "now", {
				configurable: true,
				value: originalNow,
			});
		}
	});

	it("tombstones a removed source so concurrent and queued batches cannot revive it", async () => {
		let clock = 0;
		const originalNow = performance.now;
		Object.defineProperty(performance, "now", {
			configurable: true,
			value: (): number => {
				clock += 5;
				return clock;
			},
		});
		try {
			const eventCount = 20_000;
			const pendingEvents = repository.applyDataPatch(
				createSourcePatch(createRenderSource(), [
					createEventBatch({
						handles: Uint32Array.from(
							{ length: eventCount },
							(_, index) => index + 1,
						),
						timestamps: Float64Array.from(
							{ length: eventCount },
							(_, index) => eventCount - index,
						),
						hashes: Float64Array.from(
							{ length: eventCount },
							(_, index) => index,
						),
					}),
				]),
			);
			await repository.applyDataPatch({
				globalRevision: 2,
				sources: {
					revision: 2,
					records: [],
					removedSourceIds: ["source-a"],
				},
			});
			const pendingResult = await pendingEvents;
			const queuedResult = await repository.applyDataPatch({
				globalRevision: 3,
				events: [createEventBatch({ revision: 2 })],
			});

			expect(pendingResult.committedEvents).toBe(false);
			expect(queuedResult.committedEvents).toBe(false);
			expect(repository.getEvents("source-a")).toBeUndefined();
		} finally {
			Object.defineProperty(performance, "now", {
				configurable: true,
				value: originalNow,
			});
		}
	});

	it("rejects malformed correlated event buffers before committing", async () => {
		await repository.applyDataPatch(createSourcePatch(createRenderSource()));

		await expect(
			repository.applyDataPatch({
				globalRevision: 2,
				events: [
					createEventBatch({
						handles: Uint32Array.of(1, 2),
						timestamps: Float64Array.of(10),
						hashes: Float64Array.of(2),
					}),
				],
			}),
		).rejects.toThrow("Malformed event buffers for source source-a");
		expect(repository.getEvents("source-a")).toBeUndefined();
	});

	it("restages source-dependent cache metadata without retransferring events", async () => {
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);
		const previousView = repository.getEvents("source-a");
		const updatedSource = createRenderSource({
			revision: 2,
			overrides: [{ hash: 8, color: "#ff0000" }],
		});

		const result = await repository.applyDataPatch(
			createSourcePatch(updatedSource, [], 2),
		);
		const nextView = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(Array.from(nextView?.overrideIndexes ?? [])).toEqual([1]);
		expect(nextView?.colorSourceRevision).toBe(2);
		expect(nextView?.handles).toBe(previousView?.handles);
		expect(nextView?.timestamps).toBe(previousView?.timestamps);
	});

	it("adopts exact LOD columns without a dense index or spare capacity", async () => {
		const source = createRenderSource({
			overrides: [{ hash: 8, color: "#ff0000" }],
		});
		await repository.applyDataPatch(createSourcePatch(source));
		const handles = Uint32Array.of(0xffff_fffe, 12);
		const timestamps = Float64Array.of(130_000, 70_000);
		const hashes = Float64Array.of(8, -2);
		const heightTimestamps = Float64Array.of(0, 60_000, 120_000);
		const heightCounts = Float64Array.of(2, 3, 1);
		const graphTimestamps = Float64Array.of(0, 120_000);
		const graphCounts = Float64Array.of(5, 1);
		const projection = createRenderWindowProjection({
			operationEpoch: 7,
			viewportRevision: 3,
			projectionRevision: 2,
			sources: [
				createRenderWindowSourceProjection({
					sourceRevision: 8,
					loadedCount: 30_000_000,
					representativeHandles: handles,
					representativeTimestamps: timestamps,
					representativeHashes: hashes,
					heightTimestamps,
					heightCounts,
					heightMaximumCount: 30,
					graphTimestamps,
					graphCounts,
				}),
			],
		});

		const result = await repository.applyDataPatch(
			{ globalRevision: 2, renderWindow: projection },
			7,
		);
		const view = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(view?.loadedCount).toBe(30_000_000);
		expect(view?.handles).toBe(handles);
		expect(view?.timestamps).toBe(timestamps);
		expect(view?.hashes).toBe(hashes);
		expect(view?.heightTimestamps).toBe(heightTimestamps);
		expect(view?.heightCounts).toBe(heightCounts);
		expect(view?.maxHeightCount).toBe(30);
		expect(view?.sampleTimestamps).toBe(graphTimestamps);
		expect(view?.sampleCounts).toBe(graphCounts);
		expect(view?.handles.buffer.byteLength).toBe(handles.byteLength);
		expect(view?.handleIndexes.length).toBe(0);
		expect(Array.from(view?.overrideIndexes ?? [])).toEqual([0]);
		expect(Array.from(view?.heightSuffixCounts ?? [])).toEqual([6, 4, 1]);
		expect(Array.from(view?.sampleSuffixCounts ?? [])).toEqual([6, 1]);
		expect(view?.hashMin).toBe(-2);
		expect(view?.hashMax).toBe(8);

		const spareHandles = new Uint32Array(8).subarray(0, 1);
		spareHandles[0] = 21;
		await expect(
			repository.applyDataPatch(
				{
					globalRevision: 3,
					renderWindow: createRenderWindowProjection({
						operationEpoch: 7,
						viewportRevision: 4,
						projectionRevision: 3,
						sources: [
							createRenderWindowSourceProjection({
								sourceRevision: 9,
								loadedCount: 1,
								representativeHandles: spareHandles,
								representativeTimestamps: Float64Array.of(50_000),
								representativeHashes: Float64Array.of(4),
							}),
						],
					}),
				},
				7,
			),
		).rejects.toThrow("Render window retains spare buffers for source source-a");
		expect(repository.getEvents("source-a")).toBe(view);
	});

	it("rejects obsolete render windows and stale source revisions", async () => {
		await repository.applyDataPatch(createSourcePatch(createRenderSource()));
		const initialProjection = createRenderWindowProjection({
			operationEpoch: 4,
			viewportRevision: 5,
			projectionRevision: 10,
			sources: [
				createRenderWindowSourceProjection({ sourceRevision: 8 }),
			],
		});
		await repository.applyDataPatch(
			{ globalRevision: 2, renderWindow: initialProjection },
			4,
		);
		const committedView = repository.getEvents("source-a");

		const staleEpoch = await repository.applyDataPatch(
			{
				globalRevision: 3,
				renderWindow: createRenderWindowProjection({
					operationEpoch: 3,
					viewportRevision: 99,
					projectionRevision: 99,
					sources: [
						createRenderWindowSourceProjection({ sourceRevision: 9 }),
					],
				}),
			},
			3,
		);
		const staleViewport = await repository.applyDataPatch(
			{
				globalRevision: 4,
				renderWindow: createRenderWindowProjection({
					operationEpoch: 4,
					viewportRevision: 4,
					projectionRevision: 99,
					sources: [
						createRenderWindowSourceProjection({ sourceRevision: 9 }),
					],
				}),
			},
			4,
		);
		const repeatedProjection = await repository.applyDataPatch(
			{ globalRevision: 5, renderWindow: initialProjection },
			4,
		);
		const staleSource = await repository.applyDataPatch(
			{
				globalRevision: 6,
				renderWindow: createRenderWindowProjection({
					operationEpoch: 4,
					viewportRevision: 6,
					projectionRevision: 11,
					sources: [
						createRenderWindowSourceProjection({ sourceRevision: 7 }),
					],
				}),
			},
			4,
		);
		repository.setViewportState({
			width: 100,
			height: 48,
			frame: { min: 0, max: 1_000 },
			scale: 1,
			scrollX: 0,
			scrollY: 0,
			viewportRevision: 7,
			inputSequence: 1,
			inputTimestamp: 0,
		});
		const supersededByControl = await repository.applyDataPatch(
			{
				globalRevision: 7,
				renderWindow: createRenderWindowProjection({
					operationEpoch: 4,
					viewportRevision: 6,
					projectionRevision: 12,
					sources: [
						createRenderWindowSourceProjection({ sourceRevision: 9 }),
					],
				}),
			},
			4,
		);

		expect(staleEpoch.committedEvents).toBe(false);
		expect(staleViewport.committedEvents).toBe(false);
		expect(repeatedProjection.committedEvents).toBe(false);
		expect(staleSource.committedEvents).toBe(false);
		expect(supersededByControl.committedEvents).toBe(false);
		expect(repository.getEvents("source-a")).toBe(committedView);
	});

	it("publishes multi-source windows atomically and applies explicit evictions", async () => {
		const sourceA = createRenderSource();
		const sourceB = createRenderSource({ id: "source-b", rowOrder: 1 });
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: {
				revision: 1,
				records: [sourceA, sourceB],
				removedSourceIds: [],
			},
		});
		await repository.applyDataPatch({
			globalRevision: 2,
			renderWindow: createRenderWindowProjection({
				sources: [
					createRenderWindowSourceProjection(),
					createRenderWindowSourceProjection({ sourceId: "source-b" }),
				],
			}),
		});
		const previousA = repository.getEvents("source-a");

		await expect(
			repository.applyDataPatch({
				globalRevision: 3,
				renderWindow: createRenderWindowProjection({
					viewportRevision: 2,
					projectionRevision: 2,
					sources: [
						createRenderWindowSourceProjection({ sourceRevision: 2 }),
						createRenderWindowSourceProjection({
							sourceId: "source-missing",
						}),
					],
				}),
			}),
		).rejects.toThrow("Render window references unknown source source-missing");
		expect(repository.getEvents("source-a")).toBe(previousA);

		const replacementHandles = Uint32Array.of(21);
		const evictionResult = await repository.applyDataPatch({
			globalRevision: 4,
			renderWindow: createRenderWindowProjection({
				viewportRevision: 2,
				projectionRevision: 2,
				sources: [
					createRenderWindowSourceProjection({
						representativeHandles: replacementHandles,
						representativeTimestamps: Float64Array.of(100_000),
						representativeHashes: Float64Array.of(4),
						loadedCount: 1,
					}),
				],
				evictedSourceIds: ["source-b"],
			}),
		});

		expect(evictionResult.committedEvents).toBe(true);
		expect(repository.getEvents("source-a")?.handles).toBe(replacementHandles);
		expect(repository.getEvents("source-b")).toBeUndefined();
		expect(repository.getSource("source-b")).toBe(sourceB);
	});

	it("refreshes LOD override metadata without copying projection columns", async () => {
		await repository.applyDataPatch(createSourcePatch(createRenderSource()));
		await repository.applyDataPatch({
			globalRevision: 2,
			renderWindow: createRenderWindowProjection(),
		});
		const previous = repository.getEvents("source-a");
		const updatedSource = createRenderSource({
			revision: 2,
			overrides: [{ hash: 8, color: "#ff0000" }],
		});

		const result = await repository.applyDataPatch(
			createSourcePatch(updatedSource, [], 3),
		);
		const refreshed = repository.getEvents("source-a");

		expect(result.committedEvents).toBe(true);
		expect(Array.from(refreshed?.overrideIndexes ?? [])).toEqual([1]);
		expect(refreshed?.handles).toBe(previous?.handles);
		expect(refreshed?.timestamps).toBe(previous?.timestamps);
		expect(refreshed?.heightCounts).toBe(previous?.heightCounts);
		expect(refreshed?.sampleCounts).toBe(previous?.sampleCounts);
	});

	it("ignores stale overlay collection revisions independently", async () => {
		const currentNote: RenderNoteRecord = {
			id: "note-current",
			sourceId: "source-a",
			timestamp: 10,
			name: "Current",
			color: "#ffffff",
			iconKey: "note",
		};
		const staleNote: RenderNoteRecord = {
			...currentNote,
			id: "note-stale",
			name: "Stale",
		};

		await repository.applyDataPatch({
			globalRevision: 2,
			overlays: { notes: { revision: 2, records: [currentNote] } },
		});
		const staleResult = await repository.applyDataPatch({
			globalRevision: 3,
			overlays: { notes: { revision: 1, records: [staleNote] } },
		});

		expect(staleResult.committedOverlays).toBe(false);
		expect(repository.getNotes()).toEqual([currentNote]);
	});

	it("refreshes same-revision overlays when document dependencies change", async () => {
		const originalNote: RenderNoteRecord = {
			id: "note-a",
			sourceId: "source-a",
			timestamp: 10,
			name: "Original",
			color: "",
			iconKey: "note",
		};
		const refreshedNote: RenderNoteRecord = {
			...originalNote,
			timestamp: 20,
			name: "Refreshed",
		};

		await repository.applyDataPatch({
			globalRevision: 1,
			overlays: { notes: { revision: 1, records: [originalNote] } },
		});
		const result = await repository.applyDataPatch({
			globalRevision: 2,
			overlays: { notes: { revision: 1, records: [refreshedNote] } },
		});

		expect(result.committedOverlays).toBe(true);
		expect(repository.getNotes()).toEqual([refreshedNote]);
	});

	it("returns the topmost hit region and preserves grouped note identities", () => {
		repository.setHitRegions([
			{ kind: "link", id: "link-a", x: 10, y: 10, width: 20, height: 20 },
			{
				kind: "note",
				id: "note-a",
				noteIds: ["note-a", "note-b"],
				x: 15,
				y: 15,
				width: 20,
				height: 20,
			},
		]);

		expect(repository.hitTest(20, 20)).toEqual({
			kind: "note",
			id: "note-a",
			noteIds: ["note-a", "note-b"],
		});
		expect(repository.hitTest(10, 10)).toEqual({
			kind: "link",
			id: "link-a",
		});
		expect(repository.hitTest(100, 100)).toBeNull();
	});

	it("clears operation state and closes every worker-owned bitmap", async () => {
		const close = jest.fn<void, []>();
		const bitmap = { close } as unknown as ImageBitmap;
		await repository.applyDataPatch(
			createSourcePatch(createRenderSource(), [createEventBatch()]),
		);
		repository.setAsset("note", "#ffffff", bitmap);

		repository.resetOperation();

		expect(close).toHaveBeenCalledTimes(1);
		expect(repository.getSource("source-a")).toBeUndefined();
		expect(repository.getEvents("source-a")).toBeUndefined();
		expect(repository.getGlobalRevision()).toBe(0);
		expect(repository.hitTest(10, 10)).toBeNull();
	});
});
