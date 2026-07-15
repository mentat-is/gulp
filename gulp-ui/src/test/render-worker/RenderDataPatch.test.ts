import {
	isOperationEpochAdvance,
	mergeRenderDataPatches,
} from "@/render-worker/RenderDataPatch";
import {
	type RenderDataPatch,
	type RenderEventBatch,
	type RenderNoteRecord,
	type RenderSource,
} from "@/render-worker/RenderWorker.types";

/**
 * Creates a compact event delta for client-side coalescing tests.
 * @param sourceId Source whose pending queue owns the batch.
 * @param baseRevision Revision the worker must have before applying the batch.
 * @param revision Revision produced by the batch.
 * @param handles Stable document handles carried by the batch.
 * @param timestamps Updated timestamp for each handle.
 * @returns Complete upsert batch with deterministic hash values.
 */
function createUpsertBatch(
	sourceId: string,
	baseRevision: number,
	revision: number,
	handles: number[],
	timestamps: number[],
): RenderEventBatch {
	return {
		sourceId,
		baseRevision,
		revision,
		mode: "upsert",
		handles: Uint32Array.from(handles),
		timestamps: Float64Array.from(timestamps),
		hashes: Float64Array.from(handles.map((handle) => handle * 10)),
	};
}

/**
 * Creates compact source metadata for coalesced partial-patch tests.
 * @param id Stable source identity.
 * @param name Display name used to distinguish newer records.
 * @returns Complete worker source metadata record.
 */
function createRenderSource(id: string, name: string): RenderSource {
	return {
		id,
		revision: 1,
		rowOrder: 0,
		name,
		contextLabel: "Context",
		contextColor: "#334455",
		contextTintAlpha: 0.1,
		timestamp: { min: 0, max: 1_000 },
		total: 0,
		loadingKind: "none",
		offset: 0,
		engine: "default",
		frequency: 100,
		gradient: ["#000000", "#ffffff"],
		overrides: [],
		customPaletteColors: [],
	};
}

describe("mergeRenderDataPatches", () => {
	it("accepts only safe, strictly increasing client reset epochs", () => {
		expect(isOperationEpochAdvance(4, 5)).toBe(true);
		expect(isOperationEpochAdvance(4, 4)).toBe(false);
		expect(isOperationEpochAdvance(4, 3)).toBe(false);
		expect(isOperationEpochAdvance(4, Number.NaN)).toBe(false);
		expect(isOperationEpochAdvance(4, Number.POSITIVE_INFINITY)).toBe(false);
	});

	it("preserves sequential same-source upserts as immutable typed-array batches", () => {
		const previousBatch = createUpsertBatch(
			"source-a",
			4,
			5,
			[1, 2],
			[100, 200],
		);
		const nextBatch = createUpsertBatch(
			"source-a",
			5,
			6,
			[2, 3],
			[250, 300],
		);

		const merged = mergeRenderDataPatches(
			{ globalRevision: 5, events: [previousBatch] },
			{ globalRevision: 6, events: [nextBatch] },
		);
		expect(merged.globalRevision).toBe(6);
		expect(merged.events).toHaveLength(2);
		expect(merged.events?.[0]).toBe(previousBatch);
		expect(merged.events?.[1]).toBe(nextBatch);
		expect(merged.events?.[0].handles).toBe(previousBatch.handles);
		expect(merged.events?.[1].handles).toBe(nextBatch.handles);
		expect(Array.from(previousBatch.timestamps)).toEqual([100, 200]);
		expect(Array.from(nextBatch.timestamps)).toEqual([250, 300]);
	});

	it("keeps an upsert ordered behind a pending replacement without rebuilding it", () => {
		const replacement: RenderEventBatch = {
			sourceId: "source-a",
			baseRevision: 0,
			revision: 1,
			mode: "replace",
			handles: Uint32Array.of(1),
			timestamps: Float64Array.of(100),
			hashes: Float64Array.of(10),
			sampleTimestamps: Float64Array.of(50),
			sampleCounts: Float64Array.of(4),
		};
		const upsert = createUpsertBatch("source-a", 1, 2, [2], [200]);

		const merged = mergeRenderDataPatches(
			{ globalRevision: 1, events: [replacement] },
			{ globalRevision: 2, events: [upsert] },
		);
		const replacementBatch = merged.events?.[0];
		const upsertBatch = merged.events?.[1];

		expect(merged.events).toHaveLength(2);
		expect(replacementBatch).toBe(replacement);
		expect(replacementBatch?.mode).toBe("replace");
		expect(Array.from(replacementBatch?.sampleTimestamps ?? [])).toEqual([50]);
		expect(Array.from(replacementBatch?.sampleCounts ?? [])).toEqual([4]);
		expect(upsertBatch).toBe(upsert);
		expect(upsertBatch?.baseRevision).toBe(1);
		expect(upsertBatch?.revision).toBe(2);
	});

	it("keeps independent source batches while a replacement supersedes one source", () => {
		const previous: RenderDataPatch = {
			globalRevision: 2,
			events: [
				createUpsertBatch("source-a", 1, 2, [1], [100]),
				createUpsertBatch("source-b", 1, 2, [2], [200]),
			],
		};
		const replacement: RenderEventBatch = {
			sourceId: "source-a",
			baseRevision: 2,
			revision: 3,
			mode: "replace",
			handles: Uint32Array.of(9),
			timestamps: Float64Array.of(900),
			hashes: Float64Array.of(90),
		};

		const merged = mergeRenderDataPatches(previous, {
			globalRevision: 3,
			events: [replacement],
		});

		expect(merged.events).toHaveLength(2);
		expect(merged.events?.find((batch) => batch.sourceId === "source-a")).toBe(
			replacement,
		);
		expect(
			merged.events?.find((batch) => batch.sourceId === "source-b")?.revision,
		).toBe(2);
	});

	it("uses the newest value for each independently revisioned patch section", () => {
		const firstNote: RenderNoteRecord = {
			id: "note-a",
			sourceId: "source-a",
			timestamp: 10,
			name: "First",
			color: "#ffffff",
			iconKey: "note",
		};
		const secondNote: RenderNoteRecord = {
			...firstNote,
			id: "note-b",
			name: "Second",
		};
		const previous: RenderDataPatch = {
			globalRevision: 8,
			sources: { revision: 3, records: [], removedSourceIds: [] },
			overlays: {
				notes: { revision: 1, records: [firstNote] },
				highlights: { revision: 4, records: [] },
			},
		};
		const next: RenderDataPatch = {
			globalRevision: 9,
			overlays: {
				notes: { revision: 2, records: [secondNote] },
				links: { revision: 5, records: [] },
			},
		};

		const merged = mergeRenderDataPatches(previous, next);

		expect(merged.sources).toBe(previous.sources);
		expect(merged.overlays?.notes).toBe(next.overlays?.notes);
		expect(merged.overlays?.links).toBe(next.overlays?.links);
		expect(merged.overlays?.highlights).toBe(previous.overlays?.highlights);
	});

	it("preserves an unsent source removal across a newer metadata refresh", () => {
		const merged = mergeRenderDataPatches(
			{
				globalRevision: 10,
				sources: {
					revision: 4,
					records: [],
					removedSourceIds: ["source-removed"],
				},
			},
			{
				globalRevision: 11,
				sources: {
					revision: 5,
					records: [],
					removedSourceIds: [],
				},
			},
		);

		expect(merged.sources).toEqual({
			revision: 5,
			records: [],
			removedSourceIds: ["source-removed"],
		});
	});

	it("merges partial source records by id and applies combined removals", () => {
		const previousSourceA = createRenderSource("source-a", "Source A old");
		const nextSourceA = createRenderSource("source-a", "Source A new");
		const sourceB = createRenderSource("source-b", "Source B");
		const sourceC = createRenderSource("source-c", "Source C");
		const sourceD = createRenderSource("source-d", "Source D");

		const merged = mergeRenderDataPatches(
			{
				globalRevision: 10,
				sources: {
					revision: 4,
					records: [previousSourceA, sourceB, sourceC],
					removedSourceIds: ["source-c"],
				},
			},
			{
				globalRevision: 11,
				sources: {
					revision: 5,
					records: [nextSourceA, sourceD],
					removedSourceIds: ["source-b"],
				},
			},
		);

		expect(merged.sources?.records.map((source) => source.id)).toEqual([
			"source-a",
			"source-d",
		]);
		expect(merged.sources?.records[0]).toBe(nextSourceA);
		expect(merged.sources?.removedSourceIds).toEqual([
			"source-c",
			"source-b",
		]);
	});

	it("bounds pending snapshot memory by discarding superseded transaction fragments", () => {
		const staleHandles = Uint32Array.of(1, 2);
		const merged = mergeRenderDataPatches(
			{
				globalRevision: 4,
				events: [createUpsertBatch("source-a", 3, 4, [1], [100])],
				eventSnapshots: [
					{
						kind: "begin",
						snapshotId: 10,
						sourceId: "source-a",
						revision: 4,
						totalEvents: 2,
					},
					{
						kind: "chunk",
						snapshotId: 10,
						sourceId: "source-a",
						offset: 0,
						handles: staleHandles,
						timestamps: Float64Array.of(200, 100),
						hashes: Float64Array.of(20, 10),
					},
				],
			},
			{
				globalRevision: 5,
				eventSnapshots: [
					{
						kind: "begin",
						snapshotId: 11,
						sourceId: "source-a",
						revision: 5,
						totalEvents: 3,
					},
				],
			},
		);

		expect(merged.events).toBeUndefined();
		expect(merged.eventSnapshots).toEqual([
			{
				kind: "begin",
				snapshotId: 11,
				sourceId: "source-a",
				revision: 5,
				totalEvents: 3,
			},
		]);
		expect(staleHandles.byteLength > 0).toBe(true);
	});
});
