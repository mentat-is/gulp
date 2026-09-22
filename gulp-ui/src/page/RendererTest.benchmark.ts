import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import {
	encodeEventPage,
	EVENT_MAIN_CACHE_SIZE,
	EVENT_PAGE_SIZE,
	EVENT_PENDING_DOCUMENT_LIMIT,
	EVENT_RENDER_PROJECTION_LIMIT,
	EVENT_RENDER_REPRESENTATIVE_LIMIT,
	EventStoreRepository,
	EventStoreViewportProjector,
	IndexedDbEventStoreBackend,
	type EventSourceSummary,
	type EventStoreNamespace,
	type EventStoreViewportDescriptor,
	type EventStoreWriter,
} from "@/event-store";
import { renderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

export const RENDERER_BENCHMARK_TARGETS = [
	10_000_000,
	20_000_000,
	30_000_000,
] as const;
export const DEFAULT_RENDERER_BENCHMARK_TARGET = RENDERER_BENCHMARK_TARGETS[0];
export const RENDERER_BENCHMARK_CACHE_NAMESPACE_ID = "renderer-test";
export const RENDERER_BENCHMARK_CLOCK_MS = Date.UTC(2025, 0, 1);

/** Raw typed-array bytes stored by one document-index locator entry. */
const DOCUMENT_INDEX_BYTES_PER_EVENT = 6 * Uint32Array.BYTES_PER_ELEMENT;

export type RendererBenchmarkRunStatus =
	| "idle"
	| "writing"
	| "complete"
	| "skipped"
	| "failed"
	| "cancelled";

export type RendererBenchmarkSkipCode = "quota-exhausted" | "cache-miss";
export type RendererBenchmarkCacheMode = "cold" | "warm";

/** Structured reason a requested benchmark workload did not run. */
export interface RendererBenchmarkSkipReason {
	code: RendererBenchmarkSkipCode;
}

/** Storage, memory, cache, and bounded-queue telemetry for one benchmark run. */
export interface RendererBenchmarkDiagnostics {
	status: RendererBenchmarkRunStatus;
	cacheMode: RendererBenchmarkCacheMode;
	targetEvents: number;
	processedEvents: number;
	persistedEvents: number;
	encodedBytes: number;
	writeElapsedMs: number;
	writeEventsPerSecond: number;
	writeEncodedBytesPerSecond: number;
	skipReason: RendererBenchmarkSkipReason | null;
	failureReason: string | null;
	heapSupported: boolean;
	heapStartBytes: number;
	heapPeakBytes: number;
	heapEndBytes: number;
	heapDeltaBytes: number;
	mainHeapSupported: boolean;
	mainHeapStartBytes: number;
	mainHeapPeakBytes: number;
	mainHeapEndBytes: number;
	mainHeapDeltaBytes: number;
	mainCacheDocuments: number;
	maximumMainCacheDocuments: number;
	maximumPendingDocuments: number;
	maximumRenderRepresentatives: number;
	maximumRenderItems: number;
	maximumRenderBatchesInFlight: number;
	projectionBucketPixelWidth: number;
	projectionDegraded: boolean;
	resourceGatePassed: boolean;
}

/** Deterministic acceptance summary derived from machine-readable diagnostics. */
export interface RendererBenchmarkResourceGate {
	passed: boolean;
	mainCachePassed: boolean;
	pendingDocumentsPassed: boolean;
	renderRepresentativesPassed: boolean;
	renderBatchesPassed: boolean;
}

/** Progress emitted after one bounded source page commits. */
export interface RendererBenchmarkWriteProgress {
	status: RendererBenchmarkRunStatus;
	persistedEvents: number;
	targetEvents: number;
	done: boolean;
	diagnostics: RendererBenchmarkDiagnostics;
}

/** Immutable parameters used to begin one IndexedDB benchmark generation. */
export interface RendererBenchmarkStartOptions {
	targetEvents: number;
	sourceIds: readonly Source.Id[];
	generation: number;
	documentMinTimestamp: number;
	frameSpanMs: number;
	cacheMode?: RendererBenchmarkCacheMode;
}

/** Parameters for deterministic generation of one source-local exact page. */
export interface RendererBenchmarkSourcePageInput {
	sourceId: Source.Id;
	sourceIndex: number;
	sourceCount: number;
	targetEvents: number;
	sourceOffset: number;
	pageSize: number;
	documentMinTimestamp: number;
	frameSpanMs: number;
}

/** Bounded synthetic page plus source-local handles ready for page encoding. */
export interface RendererBenchmarkSourcePage {
	documents: Doc.Type[];
	handles: Uint32Array;
}

interface RendererBenchmarkSourceCursor {
	sourceId: Source.Id;
	sourceIndex: number;
	eventCount: number;
	sourceOffset: number;
	pageId: number;
	revision: number;
}

/** Minimal writer lifecycle required by the transition benchmark adapter. */
export interface RendererBenchmarkWriter extends EventStoreWriter {
	/** Reads one compact persisted source authority for warm-cache validation. */
	getSourceSummary?(sourceId: Source.Id): Promise<EventSourceSummary | null>;
	/** Releases the operation-scoped persistence connection. */
	close(): void;
}

/** Stateless LOD projector contract injectable into focused benchmark tests. */
export interface RendererBenchmarkProjector {
	/** Produces one complete viewport replacement from persisted pages. */
	requestRenderWindow(
		descriptor: EventStoreViewportDescriptor,
	): Promise<RenderWindowProjection>;
}

export interface RendererBenchmarkEventStoreDependencies {
	writer: RendererBenchmarkWriter;
	projector: RendererBenchmarkProjector;
	publishSourceSummary?: (summary: EventSourceSummary) => void;
	publishProjection?: (projection: RenderWindowProjection, descriptor: EventStoreViewportDescriptor) => boolean;
	readHeapUsageBytes?: () => number | null;
	readMainCacheDocuments?: () => number;
	readNowMs?: () => number;
}

/**
 * Creates the zeroed machine-readable state published before a benchmark starts.
 * @param targetEvents Selected 10M, 20M, or 30M target.
 * @param cacheMode Requested cold ingestion or compatible warm reuse.
 * @returns Detached diagnostics safe to expose on `window` and DOM datasets.
 */
export function createEmptyRendererBenchmarkDiagnostics(
	targetEvents: number = DEFAULT_RENDERER_BENCHMARK_TARGET,
	cacheMode: RendererBenchmarkCacheMode = "cold",
): RendererBenchmarkDiagnostics {
	return {
		status: "idle",
		cacheMode,
		targetEvents,
		processedEvents: 0,
		persistedEvents: 0,
		encodedBytes: 0,
		writeElapsedMs: 0,
		writeEventsPerSecond: 0,
		writeEncodedBytesPerSecond: 0,
		skipReason: null,
		failureReason: null,
		heapSupported: false,
		heapStartBytes: 0,
		heapPeakBytes: 0,
		heapEndBytes: 0,
		heapDeltaBytes: 0,
		mainHeapSupported: false,
		mainHeapStartBytes: 0,
		mainHeapPeakBytes: 0,
		mainHeapEndBytes: 0,
		mainHeapDeltaBytes: 0,
		mainCacheDocuments: 0,
		maximumMainCacheDocuments: 0,
		maximumPendingDocuments: 0,
		maximumRenderRepresentatives: 0,
		maximumRenderItems: 0,
		maximumRenderBatchesInFlight: 0,
		projectionBucketPixelWidth: 1,
		projectionDegraded: false,
		resourceGatePassed: true,
	};
}

/**
 * Evaluates bounded RAM-proxy structures used by the renderer benchmark.
 * Chromium heap bytes remain telemetry because no device-independent RAM threshold exists.
 * @param diagnostics Completed or live benchmark measurements.
 * @returns Stable pass/fail fields suitable for DOM datasets and automation.
 */
export function evaluateRendererBenchmarkResourceGate(
	diagnostics: RendererBenchmarkDiagnostics,
): RendererBenchmarkResourceGate {
	const mainCachePassed =
		diagnostics.maximumMainCacheDocuments <= EVENT_MAIN_CACHE_SIZE;
	const pendingDocumentsPassed =
		diagnostics.maximumPendingDocuments <= EVENT_PENDING_DOCUMENT_LIMIT;
	const renderRepresentativesPassed =
		diagnostics.maximumRenderRepresentatives <=
		EVENT_RENDER_PROJECTION_LIMIT;
	const renderBatchesPassed = diagnostics.maximumRenderBatchesInFlight <= 2;
	return {
		passed:
			mainCachePassed &&
			pendingDocumentsPassed &&
			renderRepresentativesPassed &&
			renderBatchesPassed,
		mainCachePassed,
		pendingDocumentsPassed,
		renderRepresentativesPassed,
		renderBatchesPassed,
	};
}

interface ChromiumPerformanceMemory {
	usedJSHeapSize: number;
}

interface PerformanceWithMemory extends Performance {
	memory?: ChromiumPerformanceMemory;
}

/**
 * Reads an exact supported benchmark target from the URL.
 * Values may be expressed as `10m`, `20m`, `30m`, or their integer equivalents.
 * @param search Browser search string.
 * @param fallback Target used when the URL value is absent or unsupported.
 * @returns One of the mandatory 10M, 20M, or 30M benchmark targets.
 */
export function getRendererBenchmarkTarget(
	search: string,
	fallback: number = DEFAULT_RENDERER_BENCHMARK_TARGET,
): number {
	const rawValue = new URLSearchParams(search).get("docs")?.trim().toLowerCase();
	const parsed = rawValue?.endsWith("m")
		? Number(rawValue.slice(0, -1)) * 1_000_000
		: Number(rawValue);
	return RENDERER_BENCHMARK_TARGETS.includes(
		parsed as (typeof RENDERER_BENCHMARK_TARGETS)[number],
	)
		? parsed
		: fallback;
}

/**
 * Returns the exact number of modulo-distributed events owned by one source.
 * @param targetEvents Total synthetic events across all sources.
 * @param sourceIndex Zero-based source index.
 * @param sourceCount Number of benchmark sources.
 * @returns Source-local event count without enumerating global events.
 */
export function getRendererBenchmarkSourceEventCount(
	targetEvents: number,
	sourceIndex: number,
	sourceCount: number,
): number {
	if (
		!Number.isSafeInteger(targetEvents) ||
		targetEvents < 0 ||
		!Number.isSafeInteger(sourceIndex) ||
		sourceIndex < 0 ||
		!Number.isSafeInteger(sourceCount) ||
		sourceCount <= 0 ||
		sourceIndex >= sourceCount ||
		targetEvents <= sourceIndex
	) {
		return 0;
	}
	return Math.floor((targetEvents - 1 - sourceIndex) / sourceCount) + 1;
}

/**
 * Creates one deterministic synthetic document without retaining neighboring events.
 * @param sourceId Source that owns the event.
 * @param sourceIndex Zero-based source index used by global modulo distribution.
 * @param sourceCount Total benchmark source count.
 * @param sourceAscendingIndex Event position from oldest zero to newest.
 * @param targetEvents Total global benchmark count.
 * @param documentMinTimestamp Minimum benchmark timestamp.
 * @param frameSpanMs Synthetic timeline span.
 * @returns One minimal canonical document.
 */
export function createRendererBenchmarkDocument(
	sourceId: Source.Id,
	sourceIndex: number,
	sourceCount: number,
	sourceAscendingIndex: number,
	targetEvents: number,
	documentMinTimestamp: number,
	frameSpanMs: number,
): Doc.Type {
	const globalIndex = sourceIndex + sourceAscendingIndex * sourceCount;
	const fraction = targetEvents <= 0 ? 0 : globalIndex / targetEvents;
	const jitter = -350 - sourceIndex * 17;
	return {
		_id: `rdoc-${globalIndex.toString(16).padStart(12, "0")}` as Doc.Id,
		gulp_timestamp:
			documentMinTimestamp + Math.floor(fraction * frameSpanMs) + jitter,
		"gulp.source_id": sourceId,
		"gulp.event_code": RENDERER_BENCHMARK_EVENT_CODES[
			globalIndex % RENDERER_BENCHMARK_EVENT_CODES.length
		],
		number_hash: (globalIndex % 2048) - 1024,
	};
}

/**
 * Resolves a deterministic event at an ascending-time source percentile.
 * @param sourceId Source that owns the event.
 * @param sourceIndex Zero-based source index.
 * @param sourceCount Total benchmark source count.
 * @param percentile Position from oldest zero to newest one.
 * @param targetEvents Total global benchmark count.
 * @param documentMinTimestamp Minimum benchmark timestamp.
 * @param frameSpanMs Synthetic timeline span.
 * @returns One generated document, or undefined for an empty source.
 */
export function getRendererBenchmarkDocumentAtPercentile(
	sourceId: Source.Id,
	sourceIndex: number,
	sourceCount: number,
	percentile: number,
	targetEvents: number,
	documentMinTimestamp: number,
	frameSpanMs: number,
): Doc.Type | undefined {
	const sourceEvents = getRendererBenchmarkSourceEventCount(
		targetEvents,
		sourceIndex,
		sourceCount,
	);
	if (sourceEvents === 0) return undefined;
	const boundedPercentile = Math.max(0, Math.min(1, percentile));
	const ascendingIndex = Math.floor(boundedPercentile * (sourceEvents - 1));
	return createRendererBenchmarkDocument(
		sourceId,
		sourceIndex,
		sourceCount,
		ascendingIndex,
		targetEvents,
		documentMinTimestamp,
		frameSpanMs,
	);
}

/**
 * Generates one exact-size descending source page and its stable handles.
 * The returned document array never exceeds the EventStore page bound.
 * @param input Source cursor and deterministic timeline parameters.
 * @returns Bounded page input for `encodeEventPage`.
 */
export function generateRendererBenchmarkSourcePage(
	input: RendererBenchmarkSourcePageInput,
): RendererBenchmarkSourcePage {
	const sourceEvents = getRendererBenchmarkSourceEventCount(
		input.targetEvents,
		input.sourceIndex,
		input.sourceCount,
	);
	const remaining = Math.max(0, sourceEvents - input.sourceOffset);
	const count = Math.min(
		EVENT_PAGE_SIZE,
		Math.max(0, Math.floor(input.pageSize)),
		remaining,
	);
	const documents = new Array<Doc.Type>(count);
	const handles = new Uint32Array(count);
	for (let index = 0; index < count; index++) {
		const descendingSourceIndex = input.sourceOffset + index;
		const ascendingSourceIndex =
			sourceEvents - 1 - descendingSourceIndex;
		documents[index] = createRendererBenchmarkDocument(
			input.sourceId,
			input.sourceIndex,
			input.sourceCount,
			ascendingSourceIndex,
			input.targetEvents,
			input.documentMinTimestamp,
			input.frameSpanMs,
		);
		handles[index] = descendingSourceIndex + 1;
	}
	return { documents, handles };
}

/**
 * Formats byte counts for compact benchmark diagnostics.
 * @param bytes Non-negative byte count.
 * @returns IEC value with at most two decimals.
 */
export function formatRendererBenchmarkBytes(bytes: number): string {
	const safeBytes = Math.max(0, bytes);
	const units = ["B", "KiB", "MiB", "GiB", "TiB"];
	let value = safeBytes;
	let unitIndex = 0;
	while (value >= 1024 && unitIndex < units.length - 1) {
		value /= 1024;
		unitIndex++;
	}
	return `${value.toFixed(unitIndex === 0 ? 0 : 2)} ${units[unitIndex]}`;
}

const RENDERER_BENCHMARK_EVENT_CODES = [
	1, 2, 3, 4, 5, 6, 7, 8, 4624, 4625, 4672, 4776, 4688, 4698, 1102,
] as const;

/**
 * Creates the concrete IndexedDB benchmark adapter for one operation namespace.
 * @param namespace Coordinator and operation identity shared with the main reader.
 * @param callbacks Main-realm summary/cache hooks retained outside this adapter.
 * @returns Bounded writer and viewport-projector adapter.
 */
export function createRendererBenchmarkEventStore(
	namespace: EventStoreNamespace,
	callbacks: Pick<
		RendererBenchmarkEventStoreDependencies,
		"publishSourceSummary" | "readMainCacheDocuments"
	> = {},
): RendererBenchmarkEventStore {
	const backend = new IndexedDbEventStoreBackend(namespace);
	const repository = new EventStoreRepository(backend);
	return new RendererBenchmarkEventStore({
		writer: repository,
		projector: new EventStoreViewportProjector(backend),
		publishProjection: (projection, descriptor) =>
			renderWindowProjectionBus.publish(projection, descriptor),
		...callbacks,
	});
}

/**
 * Owns one bounded synthetic EventStore generation without retaining source arrays.
 * This adapter is intentionally isolated so generation can move into QueryWorker
 * without changing RendererTest UI and gate orchestration.
 */
export class RendererBenchmarkEventStore {
	private readonly writer: RendererBenchmarkWriter;
	private readonly projector: RendererBenchmarkProjector;
	private readonly publishSourceSummary: (summary: EventSourceSummary) => void;
	private readonly publishProjection: (
		projection: RenderWindowProjection,
		descriptor: EventStoreViewportDescriptor,
	) => boolean;
	private readonly readHeapUsageBytes: () => number | null;
	private readonly readMainCacheDocuments: () => number;
	private readonly readNowMs: () => number;
	private cursors: RendererBenchmarkSourceCursor[] = [];
	private options: RendererBenchmarkStartOptions | null = null;
	private nextSourceIndex = 0;
	private status: RendererBenchmarkRunStatus = "idle";
	private processedEvents = 0;
	private persistedEvents = 0;
	private encodedBytes = 0;
	private readonly encodedBytesBySource = new Map<Source.Id, number>();
	private writeStartedAtMs = -1;
	private writeFinishedAtMs = 0;
	private skipReason: RendererBenchmarkSkipReason | null = null;
	private heapStartBytes = 0;
	private heapPeakBytes = 0;
	private heapEndBytes = 0;
	private maximumMainCacheDocuments = 0;
	private maximumPendingDocuments = 0;
	private maximumRenderRepresentatives = 0;
	private maximumRenderItems = 0;
	private renderBatchesInFlight = 0;
	private maximumRenderBatchesInFlight = 0;
	private projectionBucketPixelWidth = 1;
	private projectionDegraded = false;
	private disposed = false;

	/**
	 * Creates an adapter over explicit persistence and projection dependencies.
	 * @param dependencies Writer, projector, and publication hooks.
	 */
	public constructor(dependencies: RendererBenchmarkEventStoreDependencies) {
		this.writer = dependencies.writer;
		this.projector = dependencies.projector;
		this.publishSourceSummary = dependencies.publishSourceSummary ?? (() => undefined);
		this.publishProjection = dependencies.publishProjection ?? (() => false);
		this.readHeapUsageBytes =
			dependencies.readHeapUsageBytes ?? readRendererBenchmarkHeapUsage;
		this.readMainCacheDocuments =
			dependencies.readMainCacheDocuments ?? (() => 0);
		this.readNowMs = dependencies.readNowMs ?? (() => performance.now());
	}

	/**
	 * Opens a cold generation or validates and adopts complete warm sources.
	 * @param options Target, source identities, and deterministic timeline parameters.
	 * @returns Initial admitted or skipped progress snapshot.
	 */
	public async start(
		options: RendererBenchmarkStartOptions,
	): Promise<RendererBenchmarkWriteProgress> {
		this.assertUsable();
		this.assertStartOptions(options);
		if (this.status === "writing") {
			throw new Error("Renderer benchmark generation is already active.");
		}
		this.resetRunState(options);
		try {
			if (options.cacheMode === "warm") {
				return await this.startWarmCache(options);
			}
			for (const sourceId of options.sourceIds) {
				await this.writer.clearSource(sourceId);
			}

			for (
				let sourceIndex = 0;
				sourceIndex < options.sourceIds.length;
				sourceIndex++
			) {
				const sourceId = options.sourceIds[sourceIndex];
				const summary = await this.writer.beginGeneration({
					sourceId,
					generation: options.generation,
					sourceOrdinal: sourceIndex + 1,
				});
				this.publishSourceSummary(summary);
				this.cursors.push({
					sourceId,
					sourceIndex,
					eventCount: getRendererBenchmarkSourceEventCount(
						options.targetEvents,
						sourceIndex,
						options.sourceIds.length,
					),
					sourceOffset: 0,
					pageId: 0,
					revision: summary.revision,
				});
			}
			this.status = "writing";
			this.writeStartedAtMs = this.readNowMs();
			return this.getProgress();
		} catch (error) {
			this.status = "failed";
			this.writeFinishedAtMs = this.readNowMs();
			await this.markActiveGenerationsIncomplete();
			throw error;
		}
	}

	/** Reuses only a complete cache matching every source count and event time edge. */
	private async startWarmCache(
		options: RendererBenchmarkStartOptions,
	): Promise<RendererBenchmarkWriteProgress> {
		const summaries: EventSourceSummary[] = [];
		for (let sourceIndex = 0; sourceIndex < options.sourceIds.length; sourceIndex++) {
			const sourceId = options.sourceIds[sourceIndex];
			const summary = await this.writer.getSourceSummary?.(sourceId) ?? null;
			const expectedCount = getRendererBenchmarkSourceEventCount(
				options.targetEvents,
				sourceIndex,
				options.sourceIds.length,
			);
			const oldest = getRendererBenchmarkDocumentAtPercentile(
				sourceId,
				sourceIndex,
				options.sourceIds.length,
				0,
				options.targetEvents,
				options.documentMinTimestamp,
				options.frameSpanMs,
			);
			const newest = getRendererBenchmarkDocumentAtPercentile(
				sourceId,
				sourceIndex,
				options.sourceIds.length,
				1,
				options.targetEvents,
				options.documentMinTimestamp,
				options.frameSpanMs,
			);
			if (
				!summary || summary.status !== "complete" ||
				summary.eventCount !== expectedCount ||
				summary.minTimestamp !== (oldest?.gulp_timestamp ?? null) ||
				summary.maxTimestamp !== (newest?.gulp_timestamp ?? null)
			) {
				this.status = "skipped";
				this.skipReason = { code: "cache-miss" };
				return this.getProgress();
			}
			summaries.push(summary);
		}
		for (let sourceIndex = 0; sourceIndex < summaries.length; sourceIndex++) {
			const summary = summaries[sourceIndex];
			this.cursors.push({
				sourceId: summary.sourceId,
				sourceIndex,
				eventCount: summary.eventCount,
				sourceOffset: summary.eventCount,
				pageId: 0,
				revision: summary.revision,
			});
			this.encodedBytes += summary.encodedBytes ?? 0;
			if (summary.encodedBytes !== undefined) {
				this.encodedBytesBySource.set(summary.sourceId, summary.encodedBytes);
			}
			this.publishSourceSummary(summary);
		}
		this.persistedEvents = options.targetEvents;
		this.status = "complete";
		return this.getProgress();
	}

	/**
	 * Encodes and commits at most one exact EventStore page.
	 * @returns Progress after the bounded page, or a terminal status when complete/skipped.
	 */
	public async appendNextPage(): Promise<RendererBenchmarkWriteProgress> {
		this.assertUsable();
		if (this.status !== "writing" || !this.options) return this.getProgress();
		const cursor = this.findNextWritableCursor();
		if (!cursor) return this.getProgress();

		const pageInput = generateRendererBenchmarkSourcePage({
			sourceId: cursor.sourceId,
			sourceIndex: cursor.sourceIndex,
			sourceCount: this.cursors.length,
			targetEvents: this.options.targetEvents,
			sourceOffset: cursor.sourceOffset,
			pageSize: EVENT_PAGE_SIZE,
			documentMinTimestamp: this.options.documentMinTimestamp,
			frameSpanMs: this.options.frameSpanMs,
		});
		this.maximumPendingDocuments = Math.max(
			this.maximumPendingDocuments,
			pageInput.documents.length,
		);
		this.sampleHeapUsage();
		try {
			const page = encodeEventPage({
				sourceId: cursor.sourceId,
				generation: this.options.generation,
				pageId: cursor.pageId,
				revision: cursor.revision + 1,
				documents: pageInput.documents,
				handles: pageInput.handles,
			});
			const summary = await this.writer.appendPages({
				sourceId: cursor.sourceId,
				generation: this.options.generation,
				pages: [page],
			});
			// Real repositories expose exact cumulative bytes; synthetic writer spies
			// retain a bounded estimate without rebuilding LOD and HeightMap records.
			if (summary.encodedBytes !== undefined) {
				this.encodedBytesBySource.set(cursor.sourceId, summary.encodedBytes);
				this.encodedBytes = [...this.encodedBytesBySource.values()].reduce(
					(total, bytes) => total + bytes,
					0,
				);
			} else {
				this.encodedBytes += page.encodedByteLength +
					page.activeCount * DOCUMENT_INDEX_BYTES_PER_EVENT;
			}
			cursor.sourceOffset += page.activeCount;
			cursor.pageId++;
			cursor.revision = summary.revision;
			this.processedEvents += page.activeCount;
			this.persistedEvents += page.activeCount;
			this.publishSourceSummary(summary);
			this.sampleHeapUsage();
			return this.getProgress();
		} catch (error) {
			if (isQuotaError(error)) {
				await this.skipAfterQuotaExhaustion();
				return this.getProgress();
			}
			this.status = "failed";
			this.writeFinishedAtMs = this.readNowMs();
			await this.markActiveGenerationsIncomplete();
			throw error;
		}
	}

	/**
	 * Finalizes every fully written source and publishes compact source summaries.
	 * @returns Complete diagnostics after final heap measurement.
	 */
	public async finalize(): Promise<RendererBenchmarkDiagnostics> {
		this.assertUsable();
		if (this.status !== "writing" || !this.options) {
			return this.getDiagnostics();
		}
		if (!this.isWriteComplete()) {
			throw new Error("Renderer benchmark cannot finalize before every page commits.");
		}
		try {
			for (const cursor of this.cursors) {
				const summary = await this.writer.finalizeGeneration(
					cursor.sourceId,
					this.options.generation,
				);
				cursor.revision = summary.revision;
				this.publishSourceSummary(summary);
			}
		} catch (error) {
			this.status = "failed";
			this.writeFinishedAtMs = this.readNowMs();
			await this.markActiveGenerationsIncomplete();
			throw error;
		}
		this.status = "complete";
		this.writeFinishedAtMs = this.readNowMs();
		this.sampleHeapUsage(true);
		return this.getDiagnostics();
	}

	/**
	 * Applies one bounded measured delta to finalized benchmark generations.
	 * @param documents New or enriched benchmark events, capped to the worker backlog bound.
	 * @returns Updated source summaries.
	 */
	public async upsertDocuments(
		documents: readonly Doc.Type[],
	): Promise<EventSourceSummary[]> {
		this.assertUsable();
		if (this.status !== "complete") {
			throw new Error("Renderer benchmark upserts require a complete generation.");
		}
		if (documents.length > EVENT_PENDING_DOCUMENT_LIMIT) {
			throw new RangeError(
				`Renderer benchmark delta exceeds ${EVENT_PENDING_DOCUMENT_LIMIT} documents.`,
			);
		}
		this.maximumPendingDocuments = Math.max(
			this.maximumPendingDocuments,
			documents.length,
		);
		this.sampleHeapUsage();
		let summaries: EventSourceSummary[];
		try {
			summaries = await this.writer.upsertDocuments({ documents });
		} catch (error) {
			if (isQuotaError(error)) {
				this.skipReason = { code: "quota-exhausted" };
			}
			throw error;
		}
		for (const summary of summaries) this.publishSourceSummary(summary);
		const summaryCounts = new Map(
			this.cursors.map((cursor) => [cursor.sourceId, cursor.eventCount]),
		);
		for (const summary of summaries) {
			summaryCounts.set(summary.sourceId, summary.eventCount);
			if (summary.encodedBytes !== undefined) {
				this.encodedBytesBySource.set(summary.sourceId, summary.encodedBytes);
			}
			const cursor = this.cursors.find(
				(candidate) => candidate.sourceId === summary.sourceId,
			);
			if (cursor) {
				cursor.eventCount = summary.eventCount;
				cursor.revision = summary.revision;
			}
		}
		this.persistedEvents = [...summaryCounts.values()].reduce(
			(total, count) => total + count,
			0,
		);
		if (this.encodedBytesBySource.size > 0) {
			this.encodedBytes = [...this.encodedBytesBySource.values()].reduce(
				(total, bytes) => total + bytes,
				0,
			);
		}
		this.sampleHeapUsage();
		return summaries;
	}

	/**
	 * Builds one exact-size viewport projection without retaining it in the adapter.
	 * @param descriptor Current Canvas viewport and visible source settings.
	 * @returns Complete render-window projection.
	 */
	public async requestRenderWindow(
		descriptor: EventStoreViewportDescriptor,
	): Promise<RenderWindowProjection> {
		this.assertUsable();
		const projection = await this.projector.requestRenderWindow(descriptor);
		this.recordProjectionMetrics(projection);
		return projection;
	}

	/**
	 * Builds one bounded projection while tracking in-flight render batches.
	 * @param descriptor Current Canvas viewport and source settings.
	 * @returns Projection ready for transfer, or null when the batch cap is reached.
	 */
	public async projectRenderWindow(
		descriptor: EventStoreViewportDescriptor,
	): Promise<RenderWindowProjection | null> {
		if (this.renderBatchesInFlight >= 2) return null;
		this.renderBatchesInFlight++;
		this.maximumRenderBatchesInFlight = Math.max(
			this.maximumRenderBatchesInFlight,
			this.renderBatchesInFlight,
		);
		try {
			return await this.requestRenderWindow(descriptor);
		} finally {
			this.renderBatchesInFlight--;
		}
	}

	/**
	 * Builds and synchronously hands one projection to the Canvas bus.
	 * @param descriptor Current Canvas viewport and visible source settings.
	 * @returns True when the mounted Canvas accepted the projection.
	 */
	public async publishRenderWindow(
		descriptor: EventStoreViewportDescriptor,
	): Promise<boolean> {
		const projection = await this.projectRenderWindow(descriptor);
		return projection ? this.publishProjection(projection, descriptor) : false;
	}

	/**
	 * Cancels a partially written generation and releases its persisted pages.
	 * @returns Terminal cancelled diagnostics.
	 */
	public async cancel(): Promise<RendererBenchmarkDiagnostics> {
		if (this.disposed) return this.getDiagnostics();
		if (this.status === "writing" && this.options) {
			await this.cancelActiveGenerations();
			this.persistedEvents = 0;
			this.status = "cancelled";
			this.writeFinishedAtMs = this.readNowMs();
		}
		this.sampleHeapUsage(true);
		return this.getDiagnostics();
	}

	/**
	 * Returns a detached diagnostics snapshot with live cache and heap measurements.
	 * @returns Current benchmark resource telemetry.
	 */
	public getDiagnostics(): RendererBenchmarkDiagnostics {
		this.sampleHeapUsage(this.status !== "writing");
		const writeElapsedMs = this.writeStartedAtMs < 0
			? 0
			: Math.max(
				0,
				(this.writeFinishedAtMs || this.readNowMs()) - this.writeStartedAtMs,
			);
		const writeSeconds = writeElapsedMs / 1_000;
		const mainCacheDocuments = this.readMainCacheDocuments();
		this.maximumMainCacheDocuments = Math.max(
			this.maximumMainCacheDocuments,
			mainCacheDocuments,
		);
		const diagnostics: RendererBenchmarkDiagnostics = {
			status: this.status,
			cacheMode: this.options?.cacheMode ?? "cold",
			targetEvents: this.options?.targetEvents ?? 0,
			processedEvents: this.processedEvents,
			persistedEvents: this.persistedEvents,
			encodedBytes: this.encodedBytes,
			writeElapsedMs,
			writeEventsPerSecond:
				writeSeconds > 0 ? this.processedEvents / writeSeconds : 0,
			writeEncodedBytesPerSecond:
				writeSeconds > 0 ? this.encodedBytes / writeSeconds : 0,
			skipReason: this.skipReason ? { ...this.skipReason } : null,
			failureReason: null,
			heapSupported: this.heapStartBytes > 0,
			heapStartBytes: this.heapStartBytes,
			heapPeakBytes: this.heapPeakBytes,
			heapEndBytes: this.heapEndBytes,
			heapDeltaBytes: Math.max(0, this.heapEndBytes - this.heapStartBytes),
			mainHeapSupported: false,
			mainHeapStartBytes: 0,
			mainHeapPeakBytes: 0,
			mainHeapEndBytes: 0,
			mainHeapDeltaBytes: 0,
			mainCacheDocuments,
			maximumMainCacheDocuments: this.maximumMainCacheDocuments,
			maximumPendingDocuments: this.maximumPendingDocuments,
			maximumRenderRepresentatives: this.maximumRenderRepresentatives,
			maximumRenderItems: this.maximumRenderItems,
			maximumRenderBatchesInFlight: this.maximumRenderBatchesInFlight,
			projectionBucketPixelWidth: this.projectionBucketPixelWidth,
			projectionDegraded: this.projectionDegraded,
			resourceGatePassed: true,
		};
		const resourceGate = evaluateRendererBenchmarkResourceGate(diagnostics);
		diagnostics.resourceGatePassed = resourceGate.passed;
		return diagnostics;
	}

	/** Releases the operation-scoped writer connection. */
	public close(): void {
		if (this.disposed) return;
		this.disposed = true;
		this.writer.close();
	}

	/** Resets all run-local counters before opening benchmark generations. */
	private resetRunState(options: RendererBenchmarkStartOptions): void {
		this.options = { ...options, sourceIds: [...options.sourceIds] };
		this.cursors = [];
		this.nextSourceIndex = 0;
		this.status = "idle";
		this.processedEvents = 0;
		this.persistedEvents = 0;
		this.encodedBytes = 0;
		this.encodedBytesBySource.clear();
		this.writeStartedAtMs = -1;
		this.writeFinishedAtMs = 0;
		this.skipReason = null;
		const initialHeap = this.readHeapUsageBytes() ?? 0;
		this.heapStartBytes = initialHeap;
		this.heapPeakBytes = initialHeap;
		this.heapEndBytes = initialHeap;
		this.maximumMainCacheDocuments = 0;
		this.maximumPendingDocuments = 0;
		this.maximumRenderRepresentatives = 0;
		this.maximumRenderItems = 0;
		this.renderBatchesInFlight = 0;
		this.maximumRenderBatchesInFlight = 0;
		this.projectionBucketPixelWidth = 1;
		this.projectionDegraded = false;
	}

	/** Selects the next source with a not-yet-written exact page. */
	private findNextWritableCursor(): RendererBenchmarkSourceCursor | null {
		for (let offset = 0; offset < this.cursors.length; offset++) {
			const index = (this.nextSourceIndex + offset) % this.cursors.length;
			const cursor = this.cursors[index];
			if (cursor.sourceOffset >= cursor.eventCount) continue;
			this.nextSourceIndex = (index + 1) % this.cursors.length;
			return cursor;
		}
		return null;
	}

	/** Checks whether every source cursor reached its deterministic event count. */
	private isWriteComplete(): boolean {
		return this.cursors.every(
			(cursor) => cursor.sourceOffset >= cursor.eventCount,
		);
	}

	/** Handles a concrete IndexedDB quota failure during a benchmark write. */
	private async skipAfterQuotaExhaustion(): Promise<void> {
		this.skipReason = { code: "quota-exhausted" };
		await this.cancelActiveGenerations();
		this.persistedEvents = 0;
		this.status = "skipped";
		this.writeFinishedAtMs = this.readNowMs();
	}

	/** Cancels every source generation opened by the current run. */
	private async cancelActiveGenerations(): Promise<void> {
		if (!this.options) return;
		for (const cursor of this.cursors) {
			try {
				await this.writer.cancelGeneration(
					cursor.sourceId,
					this.options.generation,
				);
			} catch {
				// Another source may have failed before its generation was opened.
			}
		}
	}

	/** Marks committed partial data incomplete after a non-quota failure. */
	private async markActiveGenerationsIncomplete(): Promise<void> {
		if (!this.options) return;
		for (const cursor of this.cursors) {
			try {
				await this.writer.markGenerationIncomplete(
					cursor.sourceId,
					this.options.generation,
				);
			} catch {
				// Preserve the original write error when cleanup cannot advance a source.
			}
		}
	}

	/** Records projection residency before buffers transfer to Canvas. */
	private recordProjectionMetrics(projection: RenderWindowProjection): void {
		let representatives = 0;
		let items = 0;
		for (const source of projection.sources) {
			representatives += source.representativeHandles.length;
			items +=
				source.representativeHandles.length +
				source.heightCounts.length +
				source.graphCounts.length;
			this.projectionBucketPixelWidth = Math.max(
				this.projectionBucketPixelWidth,
				source.bucketPixelWidth,
			);
			this.projectionDegraded ||= source.degradedLod;
		}
		this.maximumRenderRepresentatives = Math.max(
			this.maximumRenderRepresentatives,
			representatives,
		);
		this.maximumRenderItems = Math.max(this.maximumRenderItems, items);
	}

	/** Samples optional Chromium heap telemetry without retaining allocation objects. */
	private sampleHeapUsage(markEnd = false): void {
		const heapUsage = this.readHeapUsageBytes();
		if (heapUsage === null) return;
		this.heapPeakBytes = Math.max(this.heapPeakBytes, heapUsage);
		if (markEnd) this.heapEndBytes = heapUsage;
	}

	/** Builds one immutable public progress result. */
	private getProgress(): RendererBenchmarkWriteProgress {
		return {
			status: this.status,
			persistedEvents: this.persistedEvents,
			targetEvents: this.options?.targetEvents ?? 0,
			done: this.status !== "writing" || this.isWriteComplete(),
			diagnostics: this.getDiagnostics(),
		};
	}

	/** Throws after close so callers cannot accidentally reuse detached resources. */
	private assertUsable(): void {
		if (this.disposed) {
			throw new Error("Renderer benchmark EventStore adapter is closed.");
		}
	}

	/** Validates finite, bounded run parameters before clearing existing data. */
	private assertStartOptions(options: RendererBenchmarkStartOptions): void {
		if (
			!Number.isSafeInteger(options.targetEvents) ||
			options.targetEvents < 0 ||
			!Number.isSafeInteger(options.generation) ||
			options.generation < 0 ||
			options.sourceIds.length === 0 ||
			new Set(options.sourceIds).size !== options.sourceIds.length ||
			!Number.isFinite(options.documentMinTimestamp) ||
			!Number.isFinite(options.frameSpanMs) ||
			options.frameSpanMs <= 0
		) {
			throw new RangeError("Renderer benchmark start options are malformed.");
		}
		const maximumSourceEvents = Math.max(
			...options.sourceIds.map((_, sourceIndex) =>
				getRendererBenchmarkSourceEventCount(
					options.targetEvents,
					sourceIndex,
					options.sourceIds.length,
				),
			),
		);
		if (maximumSourceEvents > 0xffff_fffe) {
			throw new RangeError("Renderer benchmark target exhausts uint32 source handles.");
		}
	}
}

/** Reads Chromium's optional heap counter for the adapter's current realm. */
function readRendererBenchmarkHeapUsage(): number | null {
	const memory = (performance as PerformanceWithMemory).memory;
	return memory && Number.isFinite(memory.usedJSHeapSize)
		? memory.usedJSHeapSize
		: null;
}

/** Identifies browser quota failures without depending on a specific DOMException realm. */
function isQuotaError(error: unknown): boolean {
	return (
		typeof DOMException !== "undefined" &&
		error instanceof DOMException &&
		(error.name === "QuotaExceededError" || error.name === "NS_ERROR_DOM_QUOTA_REACHED")
	);
}

export const RENDERER_BENCHMARK_CACHE_LIMIT = EVENT_MAIN_CACHE_SIZE;
export const RENDERER_BENCHMARK_PENDING_LIMIT = EVENT_PENDING_DOCUMENT_LIMIT;
export const RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT =
	EVENT_RENDER_PROJECTION_LIMIT;
export const RENDERER_BENCHMARK_WORKING_SET_LIMIT =
	EVENT_RENDER_REPRESENTATIVE_LIMIT;

/** Namespace used by the benchmark reader, writer, and detached realms. */
export function createRendererBenchmarkNamespace(
	operationId: Operation.Id,
): EventStoreNamespace {
	return {
		cacheNamespaceId: RENDERER_BENCHMARK_CACHE_NAMESPACE_ID,
		operationId,
	};
}
