import {
	canReuseCanvasRenderWindow,
	canAdoptCanvasRenderWindow,
	createCanvasRenderWindowCoverage,
} from "@/app/body/CanvasRenderWindowCoverage";
import type { Source } from "@/entities/Source";
import type { EventSourceSummary } from "@/event-store/EventStore.types";
import type { QueryRenderWindowDescriptor } from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

const SOURCE_ID = "coverage-source" as Source.Id;
const SECOND_SOURCE_ID = "coverage-source-2" as Source.Id;

/** Creates a request whose one-canvas-width overscan covers the next normal pan. */
function createDescriptor(scrollX = 100): QueryRenderWindowDescriptor {
	return {
		operationId: "operation",
		operationEpoch: 3,
		viewportRevision: 4,
		projectionRevision: 5,
		frame: { min: 0, max: 4_000 },
		width: 100,
		scale: 4,
		scrollX,
		horizontalOverscanPixels: 100,
		maxRepresentatives: 1_000_000,
		evictedSourceIds: [],
		sources: [
			{
				sourceId: SOURCE_ID,
				offset: 0,
				engine: "default",
				frequency: 0,
				overrideHashes: [],
				settingsRevision: 2,
				overrideRevision: 1,
			},
		],
	};
}

/** Creates an accepted complete projection covering raw timestamps 0 through 3,000. */
function createProjection(): RenderWindowProjection {
	return {
		operationEpoch: 3,
		viewportRevision: 4,
		projectionRevision: 5,
		evictedSourceIds: [],
		sources: [
			{
				sourceId: SOURCE_ID,
				sourceAuthorityRevision: 7,
				sourceGeneration: 8,
				sourceRevision: 9,
				sourceStatus: "complete",
				coveredRange: { min: 0, max: 3_000 },
				bucketPixelWidth: 1,
				degradedLod: false,
				frameEventCount: 140_000,
				loadedCount: 140_000,
				hashRange: { min: 0, max: 100 },
				representativeHandles: new Uint32Array([1]),
				representativeTimestamps: new Float64Array([1_000]),
				representativeHashes: new Float64Array([1]),
				heightTimestamps: new Float64Array(0),
				heightCounts: new Float64Array(0),
				heightMaximumCount: 0,
				graphTimestamps: new Float64Array(0),
				graphCounts: new Float64Array(0),
			},
		],
	};
}

/** Creates the current durable source fence used by the Canvas reuse decision. */
function createSummary(
	overrides: Partial<EventSourceSummary> = {},
): EventSourceSummary {
	return {
		sourceId: SOURCE_ID,
		generation: 8,
		authorityRevision: 7,
		revision: 9,
		status: "complete",
		eventCount: 140_000,
		minTimestamp: 0,
		maxTimestamp: 4_000,
		hashMin: 0,
		hashMax: 100,
		activeCacheGeneration: 8,
		cacheHistory: [8],
		cacheHistoryCursor: 0,
		nextGeneration: 9,
		...overrides,
	};
}

describe("CanvasRenderWindowCoverage", () => {
	it("adopts advancing data after pan or zoom inside coverage without requiring an idle viewport", () => {
		const requested = createDescriptor();
		const projection = createProjection();
		projection.sources[0].sourceStatus = "building";
		const current = { ...createDescriptor(150), viewportRevision: 100 };
		expect(canAdoptCanvasRenderWindow(requested, projection, current)).toBe(true);
		expect(canAdoptCanvasRenderWindow(requested, projection, { ...current, scale: 5 })).toBe(true);
		expect(canAdoptCanvasRenderWindow(requested, projection, { ...current, scrollX: 350 })).toBe(false);
		expect(canAdoptCanvasRenderWindow(requested, projection, { ...current, frame: { min: 1, max: 4000 } })).toBe(false);
		expect(canAdoptCanvasRenderWindow(requested, projection, { ...current, operationEpoch: 4 })).toBe(false);
		expect(canAdoptCanvasRenderWindow(requested, projection, { ...current, sources: [{ ...current.sources[0], offset: 1 }] })).toBe(false);
		expect(canAdoptCanvasRenderWindow(requested, projection, { ...current, sources: [{ ...current.sources[0], overrideHashes: [1] }] })).toBe(false);
	});

	it("rejects an old projection when a newly visible vertical row is absent", () => {
		const requested = createDescriptor();
		const current = createDescriptor(150);
		current.sources = [
			...current.sources,
			{
				...current.sources[0],
				sourceId: SECOND_SOURCE_ID,
			},
		];

		expect(
			canAdoptCanvasRenderWindow(requested, createProjection(), current),
		).toBe(false);
	});

	it("rejects a visible row that failed current source-authority validation", () => {
		const requested = createDescriptor();
		const projection = createProjection();

		expect(
			canAdoptCanvasRenderWindow(
				requested,
				projection,
				requested,
				new Set<string>(),
			),
		).toBe(false);
	});

	it("reuses an accepted window while consecutive pans stay inside overscan", () => {
		const descriptor = createDescriptor();
		const coverage = createCanvasRenderWindowCoverage(
			descriptor,
			createProjection(),
		);
		const summary = createSummary();

		expect(
			canReuseCanvasRenderWindow(coverage, createDescriptor(150), () => summary),
		).toBe(true);
		expect(
			canReuseCanvasRenderWindow(coverage, createDescriptor(200), () => summary),
		).toBe(true);
	});

	it("requests a new projection after leaving the covered timestamp range", () => {
		const descriptor = createDescriptor();
		const coverage = createCanvasRenderWindowCoverage(
			descriptor,
			createProjection(),
		);

		expect(
			canReuseCanvasRenderWindow(coverage, createDescriptor(201), () =>
				createSummary(),
			),
		).toBe(false);
	});

	it("rejects reuse when source authority or projection settings change", () => {
		const descriptor = createDescriptor();
		const coverage = createCanvasRenderWindowCoverage(
			descriptor,
			createProjection(),
		);
		const baseChangedSettings = createDescriptor(150);
		const changedSettings: QueryRenderWindowDescriptor = {
			...baseChangedSettings,
			sources: [
				{
					...baseChangedSettings.sources[0],
					settingsRevision: 3,
				},
			],
		};

		expect(
			canReuseCanvasRenderWindow(coverage, createDescriptor(150), () =>
				createSummary({ revision: 10 }),
			),
		).toBe(false);
		expect(
			canReuseCanvasRenderWindow(coverage, changedSettings, () => createSummary()),
		).toBe(false);
	});

	it("never reuses a building projection", () => {
		const descriptor = createDescriptor();
		const projection = createProjection();
		projection.sources[0].sourceStatus = "building";
		const coverage = createCanvasRenderWindowCoverage(descriptor, projection);

		expect(
			canReuseCanvasRenderWindow(coverage, createDescriptor(150), () =>
				createSummary({ status: "building" }),
			),
		).toBe(false);
	});
	it("draws an advancing build during pan while progress requests still require fresh data", () => {
		const projection = createProjection();
		projection.sources[0].sourceStatus = "building";
		const coverage = createCanvasRenderWindowCoverage(createDescriptor(), projection);
		const advanced = createSummary({ status: "building", revision: 10, eventCount: 150000 });
		expect(canReuseCanvasRenderWindow(coverage, createDescriptor(150), () => advanced, true)).toBe(true);
		expect(canReuseCanvasRenderWindow(coverage, createDescriptor(150), () => advanced)).toBe(false);
		expect(canReuseCanvasRenderWindow(coverage, createDescriptor(201), () => advanced, true)).toBe(false);
		expect(canReuseCanvasRenderWindow(coverage, createDescriptor(150), () => ({ ...advanced, generation: 9 }), true)).toBe(false);
		expect(canReuseCanvasRenderWindow(coverage, createDescriptor(150), () => ({ ...advanced, status: "incomplete" }), true)).toBe(false);
	});

});
