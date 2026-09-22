import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { TextEncoder as NodeTextEncoder } from "util";
import {
	EVENT_PAGE_SIZE,
	type AppendEventStorePagesInput,
	type BeginEventStoreGenerationInput,
	type EventSourceSummary,
	type EventStoreViewportDescriptor,
	type UpsertEventStoreDocumentsInput,
} from "@/event-store";
import {
	createEmptyRendererBenchmarkDiagnostics,
	evaluateRendererBenchmarkResourceGate,
	generateRendererBenchmarkSourcePage,
	getRendererBenchmarkDocumentAtPercentile,
	getRendererBenchmarkSourceEventCount,
	getRendererBenchmarkTarget,
	RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT,
	RENDERER_BENCHMARK_TARGETS,
	RendererBenchmarkEventStore,
} from "@/page/RendererTest.benchmark";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

const SOURCE_A = "aaaaaaaa-0000-4000-8000-00000000000a" as Source.Id;
const SOURCE_B = "aaaaaaaa-0000-4000-8000-00000000000b" as Source.Id;

if (typeof TextEncoder === "undefined") {
	Object.defineProperty(globalThis, "TextEncoder", {
		configurable: true,
		value: NodeTextEncoder,
	});
}

/** In-memory writer spy that records only counts and bounded page sizes. */
class RendererBenchmarkWriterSpy {
	public readonly pageSizes: number[] = [];
	public readonly finalizedSources: Source.Id[] = [];
	public readonly cancelledSources: Source.Id[] = [];
	public beginCount = 0;
	public upsertCount = 0;
	public closed = false;
	public quotaAfterSuccessfulPageWrites: number | null = null;
	public failUpsertWithQuota = false;
	public reportEncodedBytes = false;
	public readonly warmSummaries = new Map<Source.Id, EventSourceSummary>();
	private readonly counts = new Map<Source.Id, number>();
	private readonly encodedBytes = new Map<Source.Id, number>();
	private readonly revisions = new Map<Source.Id, number>();

	/** Clears one source counter. */
	public async clearSource(sourceId: Source.Id): Promise<void> {
		this.counts.delete(sourceId);
		this.revisions.delete(sourceId);
		this.encodedBytes.delete(sourceId);
	}

	/** Opens one empty source generation. */
	public async beginGeneration(
		input: BeginEventStoreGenerationInput,
	): Promise<EventSourceSummary> {
		this.beginCount++;
		this.counts.set(input.sourceId, 0);
		this.revisions.set(input.sourceId, 1);
		return this.summary(input.sourceId, "building");
	}

	/** Returns a compact persisted authority for the warm-cache path. */
	public async getSourceSummary(sourceId: Source.Id): Promise<EventSourceSummary | null> {
		return this.warmSummaries.get(sourceId) ?? null;
	}

	/** Records one exact bounded page append. */
	public async appendPages(
		input: AppendEventStorePagesInput,
	): Promise<EventSourceSummary> {
		if (
			this.quotaAfterSuccessfulPageWrites !== null &&
			this.pageSizes.length >= this.quotaAfterSuccessfulPageWrites
		) {
			throw new DOMException("origin is full", "QuotaExceededError");
		}
		const appended = input.pages.reduce(
			(total, page) => total + page.activeCount,
			0,
		);
		for (const page of input.pages) this.pageSizes.push(page.activeCount);
		this.encodedBytes.set(
			input.sourceId,
			(this.encodedBytes.get(input.sourceId) ?? 0) + 123 * input.pages.length,
		);
		this.counts.set(
			input.sourceId,
			(this.counts.get(input.sourceId) ?? 0) + appended,
		);
		this.bumpRevision(input.sourceId);
		return this.summary(input.sourceId, "building");
	}

	/** Applies a small document delta by source. */
	public async upsertDocuments(
		input: UpsertEventStoreDocumentsInput,
	): Promise<EventSourceSummary[]> {
		if (this.failUpsertWithQuota) {
			throw new DOMException("origin is full", "QuotaExceededError");
		}
		this.upsertCount++;
		const touched = new Set<Source.Id>();
		for (const document of input.documents) {
			const sourceId = document["gulp.source_id"];
			this.counts.set(sourceId, (this.counts.get(sourceId) ?? 0) + 1);
			this.bumpRevision(sourceId);
			touched.add(sourceId);
		}
		return [...touched].map((sourceId) => this.summary(sourceId, "complete"));
	}

	/** Marks one generation complete. */
	public async finalizeGeneration(
		sourceId: Source.Id,
		_generation: number,
	): Promise<EventSourceSummary> {
		this.finalizedSources.push(sourceId);
		this.bumpRevision(sourceId);
		return this.summary(sourceId, "complete");
	}

	/** Records the storage-only transition to an in-place replacement. */
	public async discardGenerationRollback(
		sourceId: Source.Id,
		_generation: number,
	): Promise<EventSourceSummary> {
		this.bumpRevision(sourceId);
		return this.summary(sourceId, "building");
	}

	/** Records generation cancellation. */
	public async cancelGeneration(
		sourceId: Source.Id,
		_generation: number,
	): Promise<void> {
		this.cancelledSources.push(sourceId);
		this.counts.set(sourceId, 0);
	}

	/** Returns an incomplete summary for error cleanup. */
	public async markGenerationIncomplete(
		sourceId: Source.Id,
		_generation: number,
	): Promise<EventSourceSummary> {
		this.bumpRevision(sourceId);
		return this.summary(sourceId, "incomplete");
	}

	/** Records adapter close. */
	public close(): void {
		this.closed = true;
	}

	/** Advances the source-local revision. */
	private bumpRevision(sourceId: Source.Id): void {
		this.revisions.set(sourceId, (this.revisions.get(sourceId) ?? 0) + 1);
	}

	/** Builds one compact source summary. */
	private summary(
		sourceId: Source.Id,
		status: EventSourceSummary["status"],
	): EventSourceSummary {
		const revision = this.revisions.get(sourceId) ?? 0;
		return {
			sourceId,
			encodedBytes: this.reportEncodedBytes ? this.encodedBytes.get(sourceId) ?? 0 : undefined,
			generation: 1,
			revision,
			authorityRevision: revision,
			status,
			eventCount: this.counts.get(sourceId) ?? 0,
			minTimestamp: 1,
			maxTimestamp: 2,
			hashMin: -1,
			hashMax: 1,
			activeCacheGeneration: null,
			cacheHistory: [],
			cacheHistoryCursor: -1,
			nextGeneration: 2,
		};
	}
}

/** Stateless projector spy returning four exact-size representatives. */
class RendererBenchmarkProjectorSpy {
	public requestCount = 0;

	/** Returns one small projection with descriptor revisions preserved. */
	public async requestRenderWindow(
		descriptor: EventStoreViewportDescriptor,
	): Promise<RenderWindowProjection> {
		this.requestCount++;
		return {
			operationEpoch: descriptor.operationEpoch,
			viewportRevision: descriptor.viewportRevision,
			projectionRevision: descriptor.projectionRevision,
			sources: descriptor.sources.map((source) => ({
				sourceId: source.sourceId,
				sourceAuthorityRevision: 1,
				sourceGeneration: 1,
				sourceRevision: 1,
				sourceStatus: "complete",
				coveredRange: descriptor.frame,
				bucketPixelWidth: 1,
				degradedLod: false,
				loadedCount: 4,
				frameEventCount: 4,
				hashRange: { min: -1, max: 1 },
				representativeHandles: new Uint32Array([1, 2, 3, 4]),
				representativeTimestamps: new Float64Array([4, 3, 2, 1]),
				representativeHashes: new Float64Array([1, 0, -1, 0]),
				heightTimestamps: new Float64Array(0),
				heightCounts: new Float64Array(0),
				heightMaximumCount: 0,
				graphTimestamps: new Float64Array(0),
				graphCounts: new Float64Array(0),
			})),
			evictedSourceIds: [],
		};
	}
}

/** Creates a deterministic viewport request for adapter projection tests. */
function createViewportDescriptor(): EventStoreViewportDescriptor {
	return {
		operationEpoch: 2,
		viewportRevision: 3,
		projectionRevision: 4,
		frame: { min: 0, max: 10 },
		width: 800,
		scale: 1,
		scrollX: 0,
		horizontalOverscanPixels: 800,
		sources: [{
			sourceId: SOURCE_A,
			offset: 0,
			engine: "default",
			frequency: 1_000,
			overrideHashes: [],
			settingsRevision: 0,
			overrideRevision: 0,
		}],
	};
}

describe("RendererTest bounded EventStore benchmark", () => {
	it("accepts only the mandatory 10M, 20M, and 30M targets", () => {
		expect(getRendererBenchmarkTarget("?docs=10m")).toBe(10_000_000);
		expect(getRendererBenchmarkTarget("?docs=20000000")).toBe(20_000_000);
		expect(getRendererBenchmarkTarget("?docs=30m")).toBe(30_000_000);
		expect(getRendererBenchmarkTarget("?docs=1000000")).toBe(10_000_000);
	});

	it("distributes every large target exactly without allocating event arrays", () => {
		for (const target of RENDERER_BENCHMARK_TARGETS) {
			const counts = Array.from({ length: 8 }, (_, sourceIndex) =>
				getRendererBenchmarkSourceEventCount(target, sourceIndex, 8),
			);
			expect(counts.reduce((total, count) => total + count, 0)).toBe(target);
			expect(Math.max(...counts) - Math.min(...counts) <= 1).toBe(true);
		}
	});

	it("generates exact source pages capped at 16,384 documents", () => {
		const first = generateRendererBenchmarkSourcePage({
			sourceId: SOURCE_A,
			sourceIndex: 0,
			sourceCount: 1,
			targetEvents: EVENT_PAGE_SIZE + 3,
			sourceOffset: 0,
			pageSize: EVENT_PAGE_SIZE * 2,
			documentMinTimestamp: 1_000,
			frameSpanMs: 10_000,
		});
		const second = generateRendererBenchmarkSourcePage({
			sourceId: SOURCE_A,
			sourceIndex: 0,
			sourceCount: 1,
			targetEvents: EVENT_PAGE_SIZE + 3,
			sourceOffset: EVENT_PAGE_SIZE,
			pageSize: EVENT_PAGE_SIZE,
			documentMinTimestamp: 1_000,
			frameSpanMs: 10_000,
		});
		const thirtyMillionPage = generateRendererBenchmarkSourcePage({
			sourceId: SOURCE_B,
			sourceIndex: 1,
			sourceCount: 8,
			targetEvents: 30_000_000,
			sourceOffset: 0,
			pageSize: EVENT_PAGE_SIZE,
			documentMinTimestamp: 1_000,
			frameSpanMs: 10_000,
		});

		expect(first.documents.length).toBe(EVENT_PAGE_SIZE);
		expect(first.handles.length).toBe(EVENT_PAGE_SIZE);
		expect(second.documents.length).toBe(3);
		expect(second.handles[0]).toBe(EVENT_PAGE_SIZE + 1);
		expect(thirtyMillionPage.documents.length).toBe(EVENT_PAGE_SIZE);
		expect(
			first.documents[0].gulp_timestamp >=
				first.documents[first.documents.length - 1].gulp_timestamp,
		).toBe(true);
		expect(first.documents[0]._id === second.documents[0]._id).toBe(false);
	});

	it("commits and projects only bounded batches while preserving resource gates", async () => {
		const writer = new RendererBenchmarkWriterSpy();
		const projector = new RendererBenchmarkProjectorSpy();
		let published = 0;
		const adapter = new RendererBenchmarkEventStore({
			writer,
			projector,
			publishProjection: () => {
				published++;
				return true;
			},
			readMainCacheDocuments: () => 1,
		});
		const targetEvents = EVENT_PAGE_SIZE * 2 + 3;
		let progress = await adapter.start({
			targetEvents,
			sourceIds: [SOURCE_A, SOURCE_B],
			generation: 1,
			documentMinTimestamp: 1_000,
			frameSpanMs: 100_000,
		});
		while (!progress.done) progress = await adapter.appendNextPage();
		const diagnostics = await adapter.finalize();
		const projected = await adapter.publishRenderWindow(
			createViewportDescriptor(),
		);
		const projectedDiagnostics = adapter.getDiagnostics();

		expect(diagnostics.persistedEvents).toBe(targetEvents);
		expect(Math.max(...writer.pageSizes) <= EVENT_PAGE_SIZE).toBe(true);
		expect(writer.finalizedSources).toEqual([SOURCE_A, SOURCE_B]);
		expect(projected).toBe(true);
		expect(projector.requestCount).toBe(1);
		expect(published).toBe(1);
		expect(projectedDiagnostics.maximumPendingDocuments).toBe(EVENT_PAGE_SIZE);
		expect(projectedDiagnostics.maximumMainCacheDocuments).toBe(1);
		expect(projectedDiagnostics.maximumRenderRepresentatives).toBe(4);
		expect(projectedDiagnostics.maximumRenderBatchesInFlight).toBe(1);
		expect(projectedDiagnostics.resourceGatePassed).toBe(true);
		expect(evaluateRendererBenchmarkResourceGate(projectedDiagnostics).passed).toBe(
			true,
		);
		adapter.close();
		expect(writer.closed).toBe(true);
	});

	it("uses committed repository byte totals without rebuilding page summaries", async () => {
		const writer = new RendererBenchmarkWriterSpy();
		writer.reportEncodedBytes = true;
		const adapter = new RendererBenchmarkEventStore({
			writer,
			projector: new RendererBenchmarkProjectorSpy(),
		});
		let progress = await adapter.start({
			targetEvents: EVENT_PAGE_SIZE + 1,
			sourceIds: [SOURCE_A, SOURCE_B],
			generation: 1,
			documentMinTimestamp: 1_000,
			frameSpanMs: 100_000,
		});
		while (!progress.done) progress = await adapter.appendNextPage();
		const diagnostics = await adapter.finalize();

		expect(writer.pageSizes).toEqual([EVENT_PAGE_SIZE / 2 + 1, EVENT_PAGE_SIZE / 2]);
		expect(diagnostics.encodedBytes).toBe(246);
		adapter.close();
	});

	it("reuses only complete source-exact warm caches without writing a page", async () => {
		const writer = new RendererBenchmarkWriterSpy();
		const options = {
			targetEvents: 3,
			sourceIds: [SOURCE_A, SOURCE_B],
			generation: 1,
			documentMinTimestamp: 1_000,
			frameSpanMs: 100_000,
			cacheMode: "warm" as const,
		};
		for (let sourceIndex = 0; sourceIndex < options.sourceIds.length; sourceIndex++) {
			const sourceId = options.sourceIds[sourceIndex];
			const base = await writer.beginGeneration({
				sourceId,
				generation: 1,
				sourceOrdinal: sourceIndex + 1,
			});
			writer.warmSummaries.set(sourceId, {
				...base,
				status: "complete",
				eventCount: getRendererBenchmarkSourceEventCount(3, sourceIndex, 2),
				minTimestamp: getRendererBenchmarkDocumentAtPercentile(
					sourceId, sourceIndex, 2, 0, 3, 1_000, 100_000,
				)?.gulp_timestamp ?? 0,
				maxTimestamp: getRendererBenchmarkDocumentAtPercentile(
					sourceId, sourceIndex, 2, 1, 3, 1_000, 100_000,
				)?.gulp_timestamp ?? 0,
				encodedBytes: 123,
			});
		}
		const adapter = new RendererBenchmarkEventStore({
			writer,
			projector: new RendererBenchmarkProjectorSpy(),
		});
		const progress = await adapter.start(options);
		expect(progress.status).toBe("complete");
		expect(progress.done).toBe(true);
		expect(progress.diagnostics.cacheMode).toBe("warm");
		expect(progress.diagnostics.persistedEvents).toBe(3);
		expect(progress.diagnostics.processedEvents).toBe(0);
		expect(progress.diagnostics.encodedBytes).toBe(246);
		expect(writer.pageSizes).toEqual([]);
		const changedSummary = writer.warmSummaries.get(SOURCE_B);
		if (!changedSummary) throw new Error("Expected warm source summary.");
		writer.warmSummaries.set(SOURCE_B, {
			...changedSummary,
			maxTimestamp: (changedSummary.maxTimestamp ?? 0) + 1,
		});
		expect((await adapter.start(options)).diagnostics.skipReason?.code).toBe("cache-miss");
		adapter.close();
	});

	it("reports a warm-cache miss without silently switching to cold ingestion", async () => {
		const writer = new RendererBenchmarkWriterSpy();
		const adapter = new RendererBenchmarkEventStore({
			writer,
			projector: new RendererBenchmarkProjectorSpy(),
		});
		const progress = await adapter.start({
			targetEvents: 10_000_000,
			sourceIds: [SOURCE_A, SOURCE_B],
			generation: 1,
			documentMinTimestamp: 1_000,
			frameSpanMs: 100_000,
			cacheMode: "warm",
		});
		expect(progress.status).toBe("skipped");
		expect(progress.diagnostics.skipReason?.code).toBe("cache-miss");
		expect(writer.pageSizes).toEqual([]);
		adapter.close();
	});

	it("cancels partial generations after a concrete IndexedDB quota failure", async () => {
		const writer = new RendererBenchmarkWriterSpy();
		writer.quotaAfterSuccessfulPageWrites = 1;
		const adapter = new RendererBenchmarkEventStore({
			writer,
			projector: new RendererBenchmarkProjectorSpy(),
		});
		let progress = await adapter.start({
			targetEvents: EVENT_PAGE_SIZE + 1,
			sourceIds: [SOURCE_A],
			generation: 1,
			documentMinTimestamp: 1_000,
			frameSpanMs: 100_000,
		});
		while (!progress.done) progress = await adapter.appendNextPage();

		expect(progress.status).toBe("skipped");
		expect(progress.diagnostics.skipReason?.code).toBe("quota-exhausted");
		expect(progress.diagnostics.processedEvents).toBe(EVENT_PAGE_SIZE);
		expect(progress.diagnostics.persistedEvents).toBe(0);
		expect(writer.cancelledSources).toEqual([SOURCE_A]);
		adapter.close();
	});

	it("reports a concrete IndexedDB quota failure during a measured query delta", async () => {
		const writer = new RendererBenchmarkWriterSpy();
		const adapter = new RendererBenchmarkEventStore({
			writer,
			projector: new RendererBenchmarkProjectorSpy(),
		});
		let progress = await adapter.start({
			targetEvents: 1,
			sourceIds: [SOURCE_A],
			generation: 1,
			documentMinTimestamp: 1_000,
			frameSpanMs: 10_000,
		});
		while (!progress.done) progress = await adapter.appendNextPage();
		await adapter.finalize();
		writer.failUpsertWithQuota = true;
		let rejected = false;
		try {
			await adapter.upsertDocuments([{
				_id: "delta" as Doc.Id,
				gulp_timestamp: 500,
				"gulp.source_id": SOURCE_A,
				"gulp.event_code": 1,
				number_hash: 1,
			}]);
		} catch {
			rejected = true;
		}

		expect(rejected).toBe(true);
		expect(writer.upsertCount).toBe(0);
		expect(adapter.getDiagnostics().skipReason?.code).toBe("quota-exhausted");
		adapter.close();
	});

	it("reports every bounded resource failure independently", () => {
		const diagnostics = createEmptyRendererBenchmarkDiagnostics(10_000_000);
		diagnostics.maximumMainCacheDocuments = 2_049;
		diagnostics.maximumPendingDocuments = 32_769;
		diagnostics.maximumRenderRepresentatives =
			RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT + 1;
		diagnostics.maximumRenderBatchesInFlight = 3;
		const gate = evaluateRendererBenchmarkResourceGate(diagnostics);

		expect(gate.passed).toBe(false);
		expect(gate.mainCachePassed).toBe(false);
		expect(gate.pendingDocumentsPassed).toBe(false);
		expect(gate.renderRepresentativesPassed).toBe(false);
		expect(gate.renderBatchesPassed).toBe(false);
	});
});
