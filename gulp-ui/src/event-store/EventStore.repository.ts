import type { Doc } from "@/entities/Doc";
import {
	coversRequestedSourceRange,
	sourceSnapshotsMatch,
	type QueryCacheProvenance,
} from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
import {
	buildEventPageHeight,
	buildEventPageLod,
	createEventPageCatalogEntry,
	decodeEventAt,
	encodeEventPage,
	getEncodedEventPageByteLength,
	hashEventDocumentId,
	type DocumentHash64,
} from "./EventPage.codec";
import type {
	EventStoreBackend,
	EventStoreWriteOperation,
} from "./EventStore.backend";
import type {
	EventNeighborWindow,
	EventSourceEdge,
	EventStoreReader,
	EventTimestampNeighborWindow,
	EventTimestampRange,
} from "./EventStore.reader";
import {
	compareEventOrderValues,
	normalizeEventSequence,
} from "./EventStore.order";
import {
	EVENT_MAXIMUM_SOURCE_HANDLE,
	EVENT_DOCUMENT_HASH_PREFIX_BITS,
	EVENT_PAGE_HEIGHT_RECORD_KIND,
	EVENT_PAGE_LOD_RECORD_KIND,
	EVENT_PAGE_SIZE,
	EVENT_PIXEL_PAGE_SIZE,
	EVENT_SOURCE_QUERY_CACHE_LIMIT,
	type EncodedEventPage,
	type EncodedEventPageHeight,
	type EncodedEventPageLod,
	type DesiredQueryCacheLookup,
	type EventObsoleteGenerationMarker,
	type EventDocumentIndexSegmentCatalogEntry,
	type EventPageCatalogEntry,
	type EventPageCursor,
	type EventPixelPage,
	type EventSourceManifest,
	type EventSourceCacheHeadState,
	type EventSourceQueryCacheActivation,
	type EventSourceQueryCacheManifest,
	type EventSourceQueryCacheMetadata,
	type EventSourceQueryCacheRecord,
	type EventSourceQueryCacheReconciliation,
	type EventSourceSummary,
} from "./EventStore.types";

const EVENT_DOCUMENT_INDEX_PHYSICAL_SHARD_BITS = 8;
const EVENT_DOCUMENT_INDEX_PHYSICAL_SHARD_SHIFT =
	EVENT_DOCUMENT_HASH_PREFIX_BITS - EVENT_DOCUMENT_INDEX_PHYSICAL_SHARD_BITS;
const EVENT_DOCUMENT_INDEX_SEGMENT_LIMIT = 32;
const EVENT_UPSERT_SOURCE_PAGE_SLICE_LIMIT = 8;
const EVENT_UPSERT_TARGET_SOURCE_SLICE_LIMIT = 8;
const EVENT_GENERATION_CLEANUP_OPERATION_LIMIT = 256;
const EVENT_INDEX_CLEANUP_OPERATION_LIMIT = 256;
const EVENT_INDEX_STAGE_OPERATION_LIMIT = 64;
const EVENT_INDEX_STAGE_BYTE_LIMIT = 16 * 1024 ** 2;

/**
 * Reads the persisted range-independent identity of one newly written cache record.
 * Older v4 records deliberately remain eligible only for exact or latest matching.
 * @param provenance Query provenance read from a cache record.
 * @returns Canonical query-shape fingerprint, or null for a legacy v4 record.
 */
function getQueryCacheShapeFingerprint(
	provenance: QueryCacheProvenance,
): string | null {
	return provenance.shapeFingerprint ?? null;
}

/**
 * Compares cached and desired full-source snapshots without allowing malformed
 * persisted nanosecond values to abort startup reconciliation.
 * @param provenance Cached query provenance.
 * @param desired Desired lookup carrying current server metadata.
 * @returns True only when both snapshots describe the same source authority.
 */
function queryCacheSnapshotMatches(
	provenance: QueryCacheProvenance,
	desired: DesiredQueryCacheLookup,
): boolean {
	try {
		return sourceSnapshotsMatch(
			provenance.serverSnapshot,
			desired.serverSnapshot,
		);
	} catch {
		return false;
	}
}

/**
 * Checks whether one fresh cache has matching non-range semantics and fully covers
 * the desired slice of the current source.
 * @param record Complete exact cache candidate.
 * @param desired Desired semantic shape, range, and server metadata.
 * @returns True when the candidate can be reused without a query mismatch.
 */
function queryCacheCoversDesiredLookup(
	record: EventSourceQueryCacheRecord,
	desired: DesiredQueryCacheLookup,
): boolean {
	const cachedRange =
		record.provenance.materializedRange ??
		record.provenance.query.source_config?.range;
	return (
		cachedRange !== undefined &&
		getQueryCacheShapeFingerprint(record.provenance) ===
			desired.shapeFingerprint &&
		queryCacheSnapshotMatches(record.provenance, desired) &&
		coversRequestedSourceRange(
			cachedRange,
			desired.range,
			desired.serverSnapshot,
		)
	);
}

type DocumentIndexSegmentCatalogEntry = EventDocumentIndexSegmentCatalogEntry;

type StoredEventGenerationManifest = EventSourceQueryCacheManifest;

type StoredEventSourceHeadState = EventSourceCacheHeadState;

interface StoredEventSourceRollback
	extends StoredEventGenerationManifest,
		StoredEventSourceHeadState {}

interface StoredEventSourceManifest
	extends StoredEventGenerationManifest,
		StoredEventSourceHeadState {
	rollback?: StoredEventSourceRollback;
	inPlaceReplacement?: true;
	pendingQueryCache?: QueryCacheProvenance;
}

/** Bounded binary ID locator segment addressable by a 12-bit hash-prefix shard. */
export interface EncodedDocumentIndexSegment {
	sourceId: Source.Id;
	sourceOrdinal: number;
	generation: number;
	shard: number;
	segmentId: number;
	level: number;
	count: number;
	hashHighs: Uint32Array;
	hashLows: Uint32Array;
	pageIds: Uint32Array;
	offsets: Uint32Array;
	handles: Uint32Array;
	pageRevisions: Uint32Array;
	encodedByteLength: number;
}

interface DocumentIndexEntry {
	prefix: number;
	high: number;
	low: number;
	pageId: number;
	offset: number;
	handle: number;
	pageRevision: number;
}

interface DocumentIndexUpdate {
	catalog: DocumentIndexSegmentCatalogEntry[];
	encodedByteDelta: number;
	stagedDeleteOperations: EventStoreWriteOperation[];
	obsoleteDeleteOperations: EventStoreWriteOperation[];
}

interface UpsertDocumentSlice {
	documents: Doc.Type[];
	sourcePageKeys: Set<string>;
	targetSourceIds: Set<Source.Id>;
}

interface PendingEventInsertion {
	document: Doc.Type;
	handle: number;
}

interface DeduplicatedAppendPreparation {
	incomingPages: EncodedEventPage[];
	existingLocations: Map<Doc.Id, EventLocation>;
	maximumHandle: number;
}

export interface BeginEventStoreGenerationInput {
	sourceId: Source.Id;
	generation?: number;
	sourceOrdinal?: number;
	/** Query provenance that opts this replacement into the retained cache pool. */
	queryCache?: QueryCacheProvenance;
}

export interface ActivateEventSourceQueryCacheInput {
	sourceId: Source.Id;
	generation: number;
}

export interface ReconcileEventSourceQueryCacheInput {
	sourceId: Source.Id;
	desired: DesiredQueryCacheLookup;
	/** Startup discards old history; reuse preserves this session's history. */
	mode?: "initial" | "reuse";
}

export interface AppendEventStorePagesInput {
	sourceId: Source.Id;
	generation: number;
	pages: readonly EncodedEventPage[];
}

export interface UpsertEventStoreDocumentsInput {
	documents: readonly Doc.Type[];
	/** Optional source lease authority enforced before any mutation is prepared. */
	allowedSourceIds?: readonly Source.Id[];
}

export interface EventStoreWriter {
	beginGeneration(
		input: BeginEventStoreGenerationInput,
	): Promise<EventSourceSummary>;
	appendPages(input: AppendEventStorePagesInput): Promise<EventSourceSummary>;
	upsertDocuments(
		input: UpsertEventStoreDocumentsInput,
	): Promise<EventSourceSummary[]>;
	finalizeGeneration(
		sourceId: Source.Id,
		generation: number,
		queryCache?: QueryCacheProvenance,
	): Promise<EventSourceSummary>;
	cleanupObsoleteGenerations?(sourceId: Source.Id): Promise<void>;
	discardGenerationRollback(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceSummary>;
	cancelGeneration(sourceId: Source.Id, generation: number): Promise<void>;
	markGenerationIncomplete(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceSummary>;
	clearSource(sourceId: Source.Id): Promise<void>;
}

/** Source-local query-cache pool controls implemented by the persistent repository. */
export interface EventSourceQueryCacheStore {
	/** Lists compact cache metadata without reading event pages. */
	listQueryCaches(sourceId: Source.Id): Promise<EventSourceQueryCacheMetadata[]>;
	/** Makes one retained generation authoritative without copying its pages. */
	activateQueryCache(
		input: ActivateEventSourceQueryCacheInput,
	): Promise<EventSourceQueryCacheActivation>;
	/** Activates the preceding source-local history entry. */
	undoQueryCache(sourceId: Source.Id): Promise<EventSourceQueryCacheActivation | null>;
	/** Activates the following source-local history entry. */
	redoQueryCache(sourceId: Source.Id): Promise<EventSourceQueryCacheActivation | null>;
	/** Selects the reusable startup cache and prunes alternative generations. */
	reconcileQueryCache(
		input: ReconcileEventSourceQueryCacheInput,
	): Promise<EventSourceQueryCacheReconciliation | null>;
	/** Retains selected generations and schedules bounded cleanup for the rest. */
	pruneQueryCaches(
		sourceId: Source.Id,
		retainedGenerations: readonly number[],
	): Promise<void>;
	/** Invalidates a pool while preserving a monotonic empty source head. */
	resetQueryCachePool(sourceId: Source.Id): Promise<EventSourceSummary | null>;
}

export interface EventStoreRepositoryOptions {
	hashDocumentId?: (documentId: Doc.Id) => DocumentHash64;
	/** Maximum retained completed query generations per source. */
	queryCacheLimit?: number;
	/** Clock used for deterministic cache creation and activation metadata. */
	now?: () => number;
}

interface EventLocation {
	sourceId: Source.Id;
	generation: number;
	pageId: number;
	offset: number;
	handle: number;
	timestamp: number;
	sequence: number;
	revision: number;
}

type EventNeighborSide = "before" | "after";

interface EventNeighborPageCandidate {
	catalogEntry: EventPageCatalogEntry;
	optimisticLocation: EventLocation | null;
}

interface DocumentLookupContext {
	manifests: Map<Source.Id, StoredEventSourceManifest>;
}

/** Raised when queued work targets a source generation that is no longer active. */
export class StaleEventStoreGenerationError extends Error {
	/** Creates a typed stale-generation error. */
	public constructor(message: string) {
		super(message);
		this.name = "StaleEventStoreGenerationError";
	}
}

/** Raised when paginated reads use a cursor from an older source revision. */
export class StaleEventStoreCursorError extends Error {
	/** Creates a typed stale-cursor error. */
	public constructor(message: string) {
		super(message);
		this.name = "StaleEventStoreCursorError";
	}
}

/** Raised when committed metadata points to missing or malformed page data. */
export class EventStoreCorruptionError extends Error {
	/** Creates a typed persistent-data corruption error. */
	public constructor(message: string) {
		super(message);
		this.name = "EventStoreCorruptionError";
	}
}

/** Raised when an upsert would mutate a source outside the caller's held leases. */
export class EventStoreSourceAuthorityError extends Error {
	/** Creates a typed source-authority violation. */
	public constructor(message: string) {
		super(message);
		this.name = "EventStoreSourceAuthorityError";
	}
}

/**
 * Coordinates exact page persistence and bounded reads without retaining source-wide
 * document collections. Callers serialize source-scoped writers and may pass their
 * held source authority to prevent unlocked cross-source mutations.
 */
export class EventStoreRepository
	implements EventStoreReader, EventStoreWriter, EventSourceQueryCacheStore
{
	private readonly backend: EventStoreBackend;
	private readonly hashDocumentId: (documentId: Doc.Id) => DocumentHash64;
	private readonly queryCacheLimit: number;
	private readonly now: () => number;
	private readonly textDecoder = new TextDecoder();
	private sourceOrdinalAllocation: Promise<void> = Promise.resolve();
	private nextSourceOrdinal: number | null = null;

	/**
	 * Creates an event repository over an explicit persistence backend.
	 * @param backend Atomic storage implementation.
	 * @param options Testable document-hash override.
	 */
	public constructor(
		backend: EventStoreBackend,
		options: EventStoreRepositoryOptions = {},
	) {
		this.backend = backend;
		this.hashDocumentId = options.hashDocumentId ?? hashEventDocumentId;
		this.queryCacheLimit = this.normalizeQueryCacheLimit(
			options.queryCacheLimit ?? EVENT_SOURCE_QUERY_CACHE_LIMIT,
		);
		this.now = options.now ?? Date.now;
	}

	/** Releases the underlying persistence connection. */
	public close(): void {
		this.backend.close();
	}

	/**
	 * Opens a new visible source generation and retains the previous complete generation
	 * as rollback authority. Query-backed replacements first free one bounded history slot.
	 * @param input Source, optional generation/ordinal, and optional query provenance.
	 * @returns Empty building-generation summary.
	 */
	public async beginGeneration(
		input: BeginEventStoreGenerationInput,
	): Promise<EventSourceSummary> {
		await this.cleanupObsoleteGenerations(input.sourceId);
		if (input.queryCache) {
			await this.prepareQueryCacheSlot(input.sourceId);
		}
		const current = await this.getStoredManifest(input.sourceId);
		const currentHead = this.getHeadState(current);
		const generation = input.generation ?? currentHead.nextGeneration;
		this.assertGeneration(generation);
		if (generation < currentHead.nextGeneration) {
			throw new StaleEventStoreGenerationError(
				`Generation ${generation} precedes next generation ${currentHead.nextGeneration}.`,
			);
		}

		let rollback: StoredEventSourceRollback | undefined;
		if (current?.status === "building" || current?.status === "incomplete") {
			await this.commitGenerationDeletesBounded(current);
			rollback = current.rollback;
		} else if (current) {
			rollback = this.toRollbackManifest(current);
		}
		const retainedHead = this.getHeadState(rollback ?? current);

		const sourceOrdinal =
			input.sourceOrdinal ??
			current?.sourceOrdinal ??
			rollback?.sourceOrdinal ??
			(await this.allocateSourceOrdinal());
		this.assertSourceOrdinal(sourceOrdinal);
		const manifest: StoredEventSourceManifest = {
			sourceId: input.sourceId,
			sourceOrdinal,
			generation,
			revision: (current?.revision ?? rollback?.revision ?? 0) + 1,
			authorityRevision:
				Math.max(
					current?.authorityRevision ?? 0,
					rollback?.authorityRevision ?? 0,
				) + 1,
			status: "building",
			eventCount: 0,
			// Handles are revision-scoped: a replacement generation starts a compact
			// namespace and becomes authoritative only when its revision is published.
			nextHandle: 1,
			minTimestamp: 0,
			maxTimestamp: 0,
			hashMin: 0,
			hashMax: 0,
			encodedBytes: 0,
			pages: [],
			documentIndexSegments: [],
			activeCacheGeneration: retainedHead.activeCacheGeneration,
			cacheHistory: retainedHead.cacheHistory,
			cacheHistoryCursor: retainedHead.cacheHistoryCursor,
			nextGeneration: Math.max(retainedHead.nextGeneration, generation + 1),
			...(retainedHead.pendingCacheCleanup?.length
				? { pendingCacheCleanup: retainedHead.pendingCacheCleanup }
				: {}),
			...(rollback ? { rollback } : {}),
			...(input.queryCache ? { pendingQueryCache: input.queryCache } : {}),
		};
		await this.backend.commit([
			{
				type: "put",
				store: "sources",
				key: input.sourceId,
				value: manifest,
			},
		]);
		return this.toSummary(manifest);
	}

	/**
	 * Persists exact pages and LOD summaries after removing older duplicates through
	 * bounded page-image transactions. The final manifest adopts only pre-staged ID
	 * index segments, so interrupted staging remains invisible to readers.
	 * @param input Active source generation and append-only pages.
	 * @returns Updated partial source summary.
	 */
	public async appendPages(
		input: AppendEventStorePagesInput,
	): Promise<EventSourceSummary> {
		const manifest = await this.requireBuildingManifest(
			input.sourceId,
			input.generation,
		);
		if (input.pages.length === 0) return this.toSummary(manifest);

		const prepared = await this.prepareDeduplicatedBuildingAppend(
			manifest,
			input.pages,
		);
		const deduplicatedManifest = await this.applyBuildingDuplicateTombstones(
			manifest,
			prepared.existingLocations,
		);
		const finalPages = prepared.incomingPages;
		for (const page of finalPages) {
			page.revision = deduplicatedManifest.revision + 1;
		}
		const finalLods = finalPages.map((page) => buildEventPageLod(page));
		const finalHeights = finalPages.map((page) => buildEventPageHeight(page));
		const operations: EventStoreWriteOperation[] = [];
		let pageStorageByteDelta = 0;
		for (let index = 0; index < finalPages.length; index++) {
			const page = finalPages[index];
			const lod = finalLods[index];
			const height = finalHeights[index];
			const pageKey = this.createPageKey(page.sourceId, page.generation, page.pageId);
			operations.push(
				{ type: "put", store: "pages", key: pageKey, value: page },
				{
					type: "put",
					store: "page_lod",
					key: this.createPageSummaryKey(
						page.sourceId,
						page.generation,
						page.pageId,
						EVENT_PAGE_LOD_RECORD_KIND,
					),
					value: lod,
				},
				{
					type: "put",
					store: "page_lod",
					key: this.createPageSummaryKey(
						page.sourceId,
						page.generation,
						page.pageId,
						EVENT_PAGE_HEIGHT_RECORD_KIND,
					),
					value: height,
				},
			);
			pageStorageByteDelta +=
				page.encodedByteLength + lod.encodedByteLength + height.encodedByteLength;
		}

		const indexUpdate = await this.prepareAppendDocumentIndex(
			deduplicatedManifest,
			finalPages,
		);
		const catalogByPage = new Map(
			deduplicatedManifest.pages.map((entry) => [entry.pageId, entry]),
		);
		for (const page of finalPages) {
			catalogByPage.set(page.pageId, createEventPageCatalogEntry(page));
		}
		const nextManifest = this.recalculateManifest({
			...deduplicatedManifest,
			revision: deduplicatedManifest.revision + 1,
			nextHandle: Math.max(
				deduplicatedManifest.nextHandle,
				prepared.maximumHandle + 1,
			),
			encodedBytes:
				deduplicatedManifest.encodedBytes +
				pageStorageByteDelta +
				indexUpdate.encodedByteDelta,
			pages: [...catalogByPage.values()].sort((left, right) =>
				this.compareCatalogEntries(left, right),
			),
			documentIndexSegments: indexUpdate.catalog,
		});
		operations.push({
			type: "put",
			store: "sources",
			key: input.sourceId,
			value: nextManifest,
		});
		try {
			await this.backend.commit(operations);
		} catch (error) {
			await this.commitIndexOperationsBounded(
				indexUpdate.stagedDeleteOperations,
			).catch(() => undefined);
			throw error;
		}
		await this.commitIndexOperationsBounded(
			indexUpdate.obsoleteDeleteOperations,
		).catch(() => undefined);
		return this.toSummary(nextManifest);
	}

	/**
	 * Applies deduplicated enrichment/realtime mutations to complete source manifests.
	 * Same-source timestamp moves retain handles, cross-source moves allocate a new handle,
	 * and every affected active query cache is atomically marked as mutated.
	 * @param input Canonical documents whose final duplicate wins by ID.
	 * @returns Updated summaries for every touched source.
	 */
	public async upsertDocuments(
		input: UpsertEventStoreDocumentsInput,
	): Promise<EventSourceSummary[]> {
		const documentsById = new Map<Doc.Id, Doc.Type>();
		for (const document of input.documents) {
			this.assertUpsertDocument(document);
			documentsById.set(document._id, document);
		}
		if (documentsById.size === 0) return [];
		this.assertUpsertTargetAuthority(
			[...documentsById.values()],
			input.allowedSourceIds,
		);
		const slices = await this.createUpsertDocumentSlices([
			...documentsById.values(),
		]);
		const latestSummaries = new Map<Source.Id, EventSourceSummary>();
		for (const documents of slices) {
			const summaries = await this.upsertDocumentSlice(
				documents,
				input.allowedSourceIds,
			);
			for (const summary of summaries) {
				latestSummaries.set(summary.sourceId, summary);
			}
		}
		return [...latestSummaries.values()];
	}

	/**
	 * Rejects target sources not covered by the caller's source leases.
	 * @param documents Deduplicated documents about to be upserted.
	 * @param allowedSourceIds Source IDs whose mutation leases the caller holds.
	 */
	private assertUpsertTargetAuthority(
		documents: readonly Doc.Type[],
		allowedSourceIds: readonly Source.Id[] | undefined,
	): void {
		if (allowedSourceIds === undefined) return;
		const allowed = new Set(allowedSourceIds);
		for (const document of documents) {
			const targetSourceId = document["gulp.source_id"];
			if (!allowed.has(targetSourceId)) {
				throw new EventStoreSourceAuthorityError(
					`Upsert target source ${targetSourceId} is outside the held source leases.`,
				);
			}
		}
	}

	/**
	 * Applies one mutation slice whose source-page and target-source fan-out is bounded.
	 * Intermediate source revisions remain internal to this repository call; the caller
	 * receives only the final summary for each touched source.
	 * @param documents Deduplicated documents assigned to one bounded slice.
	 * @returns Final source summaries committed by this slice.
	 */
	private async upsertDocumentSlice(
		documents: readonly Doc.Type[],
		allowedSourceIds: readonly Source.Id[] | undefined,
	): Promise<EventSourceSummary[]> {
		const documentsById = new Map(
			documents.map((document) => [document._id, document]),
		);

		const context = await this.createLookupContext();
		const existingLocations = await this.findDocumentLocations(
			[...documentsById.keys()],
			undefined,
			context,
		);
		if (allowedSourceIds !== undefined) {
			const allowed = new Set(allowedSourceIds);
			for (const document of documents) {
				const existingSourceId = existingLocations.get(document._id)?.sourceId;
				if (existingSourceId !== undefined && !allowed.has(existingSourceId)) {
					throw new EventStoreSourceAuthorityError(
						`Document ${document._id} currently belongs to unlocked source ${existingSourceId}.`,
					);
				}
			}
		}
		const touchedSources = new Set<Source.Id>();
		const originalPages = new Map<Source.Id, Map<number, EncodedEventPage>>();
		const modifiedPages = new Map<Source.Id, Map<number, EncodedEventPage>>();
		const insertions = new Map<Source.Id, PendingEventInsertion[]>();
		const nextHandles = new Map<Source.Id, number>();

		/** Returns complete source authority or rejects a stale upsert. */
		const requireComplete = (sourceId: Source.Id): StoredEventSourceManifest => {
			const manifest = context.manifests.get(sourceId);
			if (!manifest || manifest.status !== "complete") {
				throw new StaleEventStoreGenerationError(
					`Source ${sourceId} does not have a complete generation for upsert.`,
				);
			}
			return manifest;
		};
		/** Loads and clones one exact page at most once in this mutation. */
		const loadMutablePage = async (
			sourceId: Source.Id,
			pageId: number,
		): Promise<EncodedEventPage> => {
			let sourcePages = modifiedPages.get(sourceId);
			if (!sourcePages) {
				sourcePages = new Map();
				modifiedPages.set(sourceId, sourcePages);
			}
			const existing = sourcePages.get(pageId);
			if (existing) return existing;
			const manifest = requireComplete(sourceId);
			const page = await this.loadPage(sourceId, manifest.generation, pageId);
			let sourceOriginalPages = originalPages.get(sourceId);
			if (!sourceOriginalPages) {
				sourceOriginalPages = new Map();
				originalPages.set(sourceId, sourceOriginalPages);
			}
			sourceOriginalPages.set(pageId, page);
			const mutable = this.cloneEncodedPage(page);
			sourcePages.set(pageId, mutable);
			return mutable;
		};
		/** Allocates one non-zero source-local uint32 handle. */
		const allocateHandle = (sourceId: Source.Id): number => {
			const manifest = requireComplete(sourceId);
			const handle = nextHandles.get(sourceId) ?? manifest.nextHandle;
			if (handle <= 0 || handle > EVENT_MAXIMUM_SOURCE_HANDLE) {
				throw new RangeError(`Source ${sourceId} exhausted its uint32 handles.`);
			}
			nextHandles.set(sourceId, handle + 1);
			return handle;
		};
		/** Queues one new document for a source-local page append. */
		const insert = (
			sourceId: Source.Id,
			document: Doc.Type,
			handle: number,
		): void => {
			let pending = insertions.get(sourceId);
			if (!pending) {
				pending = [];
				insertions.set(sourceId, pending);
			}
			pending.push({ document, handle });
			touchedSources.add(sourceId);
		};

		for (const [documentId, document] of documentsById) {
			const targetSourceId = document["gulp.source_id"];
			requireComplete(targetSourceId);
			const existing = existingLocations.get(documentId);
			if (!existing) {
				insert(targetSourceId, document, allocateHandle(targetSourceId));
				continue;
			}

			requireComplete(existing.sourceId);
			const page = await loadMutablePage(existing.sourceId, existing.pageId);
			if (
				existing.offset >= page.count ||
				page.tombstones[existing.offset] !== 0 ||
				this.decodeDocumentId(page, existing.offset) !== documentId
			) {
				throw new EventStoreCorruptionError(
					`Upsert locator for ${documentId} is stale inside its committed revision.`,
				);
			}
			touchedSources.add(existing.sourceId);
			const sameSource = existing.sourceId === targetSourceId;
			if (
				sameSource &&
				page.timestamps[existing.offset] === document.gulp_timestamp &&
				page.sequences[existing.offset] ===
					normalizeEventSequence(document["event.sequence"])
			) {
				page.hashes[existing.offset] = document.number_hash;
				page.eventCodes[existing.offset] = document["gulp.event_code"];
				continue;
			}

			page.tombstones[existing.offset] = 1;
			page.activeCount--;
			insert(
				targetSourceId,
				document,
				sameSource ? existing.handle : allocateHandle(targetSourceId),
			);
		}

		const operations: EventStoreWriteOperation[] = [];
		const nextManifests: StoredEventSourceManifest[] = [];
		const indexUpdates: DocumentIndexUpdate[] = [];
		try {
			for (const sourceId of touchedSources) {
				const manifest = requireComplete(sourceId);
				const sourceModifiedPages = modifiedPages.get(sourceId) ?? new Map();
				const sourceOriginalPages = originalPages.get(sourceId) ?? new Map();
				const replacementPages: EncodedEventPage[] = [];
				let pageStorageByteDelta = 0;
				for (const page of sourceModifiedPages.values()) {
					page.revision = manifest.revision + 1;
					const finalized = this.finalizeMutatedPage(page);
					replacementPages.push(finalized);
					const original = sourceOriginalPages.get(page.pageId);
					if (!original) {
						throw new EventStoreCorruptionError(
							"Missing original page for an upsert.",
						);
					}
					const originalLod = buildEventPageLod(original);
					const originalHeight = buildEventPageHeight(original);
					pageStorageByteDelta -=
						original.encodedByteLength +
						originalLod.encodedByteLength +
						originalHeight.encodedByteLength;
				}

				const pending = insertions.get(sourceId) ?? [];
				let nextPageId =
					manifest.pages.reduce(
						(maximum, page) => Math.max(maximum, page.pageId),
						-1,
					) + 1;
				for (let start = 0; start < pending.length; start += EVENT_PAGE_SIZE) {
					const chunk = pending.slice(start, start + EVENT_PAGE_SIZE);
					const page = encodeEventPage({
						sourceId,
						generation: manifest.generation,
						pageId: nextPageId++,
						revision: manifest.revision + 1,
						documents: chunk.map((entry) => entry.document),
						handles: Uint32Array.from(chunk, (entry) => entry.handle),
					});
					replacementPages.push(page);
				}

				for (const page of replacementPages) {
					const lod = buildEventPageLod(page);
					const height = buildEventPageHeight(page);
					const key = this.createPageKey(
						sourceId,
						manifest.generation,
						page.pageId,
					);
					operations.push(
						{ type: "put", store: "pages", key, value: page },
						{
							type: "put",
							store: "page_lod",
							key: this.createPageSummaryKey(
								sourceId,
								manifest.generation,
								page.pageId,
								EVENT_PAGE_LOD_RECORD_KIND,
							),
							value: lod,
						},
						{
							type: "put",
							store: "page_lod",
							key: this.createPageSummaryKey(
								sourceId,
								manifest.generation,
								page.pageId,
								EVENT_PAGE_HEIGHT_RECORD_KIND,
							),
							value: height,
						},
					);
					pageStorageByteDelta +=
						page.encodedByteLength +
						lod.encodedByteLength +
						height.encodedByteLength;
				}

				const indexUpdate = await this.prepareAppendDocumentIndex(
					manifest,
					replacementPages,
				);
				indexUpdates.push(indexUpdate);
				const catalogByPage = new Map(
					manifest.pages.map((entry) => [entry.pageId, entry]),
				);
				for (const page of replacementPages) {
					catalogByPage.set(page.pageId, createEventPageCatalogEntry(page));
				}
				const pages = [...catalogByPage.values()].sort((left, right) =>
					this.compareCatalogEntries(left, right),
				);
				const nextManifest = this.recalculateManifest({
					...manifest,
					revision: manifest.revision + 1,
					nextHandle: nextHandles.get(sourceId) ?? manifest.nextHandle,
					encodedBytes:
						manifest.encodedBytes +
						pageStorageByteDelta +
						indexUpdate.encodedByteDelta,
					pages,
					documentIndexSegments: indexUpdate.catalog,
				});
				operations.push({
					type: "put",
					store: "sources",
					key: sourceId,
					value: nextManifest,
				});
				if (manifest.activeCacheGeneration === manifest.generation) {
					const cacheRecord = await this.getQueryCacheRecord(
						sourceId,
						manifest.generation,
					);
					if (!cacheRecord) {
						throw new EventStoreCorruptionError(
							`Active query cache ${sourceId}/${manifest.generation} is missing.`,
						);
					}
					operations.push({
						type: "put",
						store: "source_query_caches",
						key: this.createQueryCacheKey(sourceId, manifest.generation),
						value: {
							...cacheRecord,
							manifest: this.toGenerationManifest(nextManifest),
							state: "mutated",
						} satisfies EventSourceQueryCacheRecord,
					});
				}
				nextManifests.push(nextManifest);
			}
		} catch (error) {
			await this.commitIndexOperationsBounded(
				indexUpdates.flatMap((update) => update.stagedDeleteOperations),
			).catch(() => undefined);
			throw error;
		}

		try {
			await this.backend.commit(operations);
		} catch (error) {
			await this.commitIndexOperationsBounded(
				indexUpdates.flatMap((update) => update.stagedDeleteOperations),
			).catch(() => undefined);
			throw error;
		}
		await this.commitIndexOperationsBounded(
			indexUpdates.flatMap((update) => update.obsoleteDeleteOperations),
		).catch(() => undefined);
		return nextManifests.map((manifest) => this.toSummary(manifest));
	}

	/**
	 * Partitions a deduplicated upsert by existing source pages and target sources.
	 * This prevents a scattered 16,384-document chunk from cloning every page of a
	 * multi-million-event source in one IndexedDB write-set.
	 * @param documents Canonical deduplicated upsert input.
	 * @returns Stable document slices with bounded page-image fan-out.
	 */
	private async createUpsertDocumentSlices(
		documents: readonly Doc.Type[],
	): Promise<Doc.Type[][]> {
		const context = await this.createLookupContext();
		const existingLocations = await this.findDocumentLocations(
			documents.map((document) => document._id),
			undefined,
			context,
		);
		const slices: UpsertDocumentSlice[] = [];
		let current: UpsertDocumentSlice = {
			documents: [],
			sourcePageKeys: new Set(),
			targetSourceIds: new Set(),
		};
		/** Commits the current bounded logical slice to the output list. */
		const flush = (): void => {
			if (current.documents.length === 0) return;
			slices.push(current);
			current = {
				documents: [],
				sourcePageKeys: new Set(),
				targetSourceIds: new Set(),
			};
		};

		for (const document of documents) {
			const existing = existingLocations.get(document._id);
			const sourcePageKey = existing
				? JSON.stringify([
						existing.sourceId,
						existing.generation,
						existing.pageId,
					])
				: null;
			const targetSourceId = document["gulp.source_id"];
			const addsSourcePage =
				sourcePageKey !== null && !current.sourcePageKeys.has(sourcePageKey);
			const addsTargetSource = !current.targetSourceIds.has(targetSourceId);
			if (
				current.documents.length > 0 &&
				(current.documents.length >= EVENT_PAGE_SIZE ||
					(addsSourcePage &&
						current.sourcePageKeys.size >= EVENT_UPSERT_SOURCE_PAGE_SLICE_LIMIT) ||
					(addsTargetSource &&
						current.targetSourceIds.size >=
							EVENT_UPSERT_TARGET_SOURCE_SLICE_LIMIT))
			) {
				flush();
			}
			current.documents.push(document);
			if (sourcePageKey !== null) current.sourcePageKeys.add(sourcePageKey);
			current.targetSourceIds.add(targetSourceId);
		}
		flush();
		return slices.map((slice) => slice.documents);
	}

	/**
	 * Atomically detaches a legacy retained rollback before bounded deletion. Pooled
	 * query replacements reject this destructive fallback so Undo and cancel stay safe.
	 * @param sourceId Source whose replacement must continue without dual storage.
	 * @param generation Expected active building generation.
	 * @returns Revision-fenced building summary after rollback authority is removed.
	 */
	public async discardGenerationRollback(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceSummary> {
		const manifest = await this.requireBuildingManifest(sourceId, generation);
		if (manifest.pendingQueryCache || manifest.cacheHistory.length > 0) {
			throw new Error(
				"A pooled query replacement cannot discard its rollback cache.",
			);
		}
		if (!manifest.rollback) return this.toSummary(manifest);
		const rollback = manifest.rollback;
		const { rollback: _rollback, ...building } = manifest;
		const inPlaceManifest: StoredEventSourceManifest = {
			...building,
			revision: manifest.revision + 1,
			inPlaceReplacement: true,
			obsoleteGenerations: this.mergeObsoleteGenerationMarkers(
				manifest.obsoleteGenerations,
				[this.createObsoleteGenerationMarker(rollback)],
			),
		};
		await this.backend.commit([
			{
				type: "put",
				store: "sources",
				key: sourceId,
				value: inPlaceManifest,
			},
		]);
		await this.cleanupObsoleteGenerations(sourceId);
		return this.toSummary(inPlaceManifest);
	}

	/**
	 * Makes a building generation authoritative. Query-backed generations atomically
	 * append a cache record and history entry; legacy replacements retain old cleanup.
	 * @param sourceId Source being finalized.
	 * @param generation Expected active generation.
	 * @param queryCache Optional provenance learned only when an explicit ingestion ends.
	 * @returns Complete source summary.
	 */
	public async finalizeGeneration(
		sourceId: Source.Id,
		generation: number,
		queryCache?: QueryCacheProvenance,
	): Promise<EventSourceSummary> {
		const manifest = await this.requireBuildingManifest(sourceId, generation);
		const finalizedQueryCache = queryCache ?? manifest.pendingQueryCache;
		const completeGeneration: StoredEventGenerationManifest = {
			...this.toGenerationManifest(manifest),
			revision: manifest.revision + 1,
			status: "complete",
		};
		let complete: StoredEventSourceManifest;
		const operations: EventStoreWriteOperation[] = [];
		if (finalizedQueryCache) {
			const retainedHistory = manifest.cacheHistory.slice(
				0,
				manifest.cacheHistoryCursor + 1,
			);
			retainedHistory.push(generation);
			const evictedGenerations: number[] = [];
			while (retainedHistory.length > this.queryCacheLimit) {
				const evicted = retainedHistory.shift();
				if (evicted !== undefined) evictedGenerations.push(evicted);
			}
			const pendingCacheCleanup = this.mergeObsoleteGenerationMarkers(
				manifest.pendingCacheCleanup,
				evictedGenerations.map((value) => ({ generation: value })),
			);
			complete = {
				...completeGeneration,
				authorityRevision: manifest.authorityRevision,
				activeCacheGeneration: generation,
				cacheHistory: retainedHistory,
				cacheHistoryCursor: retainedHistory.length - 1,
				nextGeneration: Math.max(manifest.nextGeneration, generation + 1),
				...(pendingCacheCleanup.length > 0 ? { pendingCacheCleanup } : {}),
			};
			const activatedAt = this.now();
			const cacheRecord: EventSourceQueryCacheRecord = {
				sourceId,
				generation,
				manifest: completeGeneration,
				provenance: finalizedQueryCache,
				state: "exact",
				createdAt: activatedAt,
				lastActivatedAt: activatedAt,
			};
			operations.push({
				type: "put",
				store: "source_query_caches",
				key: this.createQueryCacheKey(sourceId, generation),
				value: cacheRecord,
			});
		} else {
			const staleCacheGenerations = manifest.cacheHistory.filter(
				(candidate) => candidate !== generation,
			);
			complete = {
				...completeGeneration,
				authorityRevision: manifest.authorityRevision,
				activeCacheGeneration: null,
				cacheHistory: [],
				cacheHistoryCursor: -1,
				nextGeneration: Math.max(manifest.nextGeneration, generation + 1),
				...(manifest.rollback ||
				manifest.obsoleteGenerations?.length ||
				staleCacheGenerations.length > 0
					? {
							obsoleteGenerations: this.mergeObsoleteGenerationMarkers(
								manifest.obsoleteGenerations,
								[
									...(manifest.rollback
										? [this.createObsoleteGenerationMarker(manifest.rollback)]
										: []),
									...staleCacheGenerations.map((value) => ({
										generation: value,
									})),
								],
							),
						}
					: {}),
			};
		}
		operations.push({
			type: "put",
			store: "sources",
			key: sourceId,
			value: complete,
		});
		await this.backend.commit(operations);
		return this.toSummary(complete);
	}

	/**
	 * Reclaims obsolete generations in idempotent batches and removes each marker and
	 * query-cache record only after every corresponding physical record is deleted.
	 * @param sourceId Source whose pending cleanup should be resumed.
	 * @returns Promise settled when no obsolete-generation marker remains.
	 */
	public async cleanupObsoleteGenerations(sourceId: Source.Id): Promise<void> {
		while (true) {
			const manifest = await this.getStoredManifest(sourceId);
			const marker =
				manifest?.pendingCacheCleanup?.[0] ??
				manifest?.obsoleteGenerations?.[0];
			if (!manifest || !marker) return;
			if (manifest.activeCacheGeneration === marker.generation) {
				throw new EventStoreCorruptionError(
					`Active cache generation ${marker.generation} cannot be reclaimed.`,
				);
			}
			await this.commitObsoleteGenerationDeletesBounded(sourceId, marker);
			const current = await this.getStoredManifest(sourceId);
			if (!current) return;
			const remainingCaches = (current.pendingCacheCleanup ?? []).filter(
				(candidate) => candidate.generation !== marker.generation,
			);
			const remainingLegacy = (current.obsoleteGenerations ?? []).filter(
				(candidate) => candidate.generation !== marker.generation,
			);
			const next: StoredEventSourceManifest = { ...current };
			if (remainingCaches.length > 0) {
				next.pendingCacheCleanup = remainingCaches;
			} else delete next.pendingCacheCleanup;
			if (remainingLegacy.length > 0) next.obsoleteGenerations = remainingLegacy;
			else delete next.obsoleteGenerations;
			await this.backend.commit([
				{
					type: "delete",
					store: "source_query_caches",
					key: this.createQueryCacheKey(sourceId, marker.generation),
				},
				{
					type: "put",
					store: "sources",
					key: sourceId,
					value: next,
				},
			]);
		}
	}

	/**
	 * Cancels a building generation. A retained rollback and its cache history are
	 * restored; a legacy in-place replacement remains incomplete for a later refetch.
	 * @param sourceId Source whose query was canceled.
	 * @param generation Expected building generation.
	 * @returns Nothing.
	 */
	public async cancelGeneration(
		sourceId: Source.Id,
		generation: number,
	): Promise<void> {
		const manifest = await this.requireBuildingManifest(sourceId, generation);
		if (manifest.inPlaceReplacement) {
			const incomplete: StoredEventSourceManifest = {
				...manifest,
				revision: manifest.revision + 1,
				status: "incomplete",
			};
			await this.backend.commit([
				{
					type: "put",
					store: "sources",
					key: sourceId,
					value: incomplete,
				},
			]);
			return;
		}
		if (manifest.rollback) {
			const restored: StoredEventSourceManifest = manifest.pendingQueryCache
				? {
						...manifest.rollback,
						revision: manifest.revision + 1,
						authorityRevision: manifest.authorityRevision + 1,
						status: "complete",
						pendingCacheCleanup: this.mergeObsoleteGenerationMarkers(
							manifest.pendingCacheCleanup,
							[this.createObsoleteGenerationMarker(manifest)],
						),
					}
				: {
						...manifest.rollback,
						revision: manifest.revision + 1,
						authorityRevision: manifest.authorityRevision + 1,
						status: "complete",
						obsoleteGenerations: this.mergeObsoleteGenerationMarkers(
							manifest.obsoleteGenerations,
							[this.createObsoleteGenerationMarker(manifest)],
						),
					};
			await this.backend.commit([
				{
					type: "put",
					store: "sources",
					key: sourceId,
					value: restored,
				},
			]);
			await this.cleanupObsoleteGenerations(sourceId);
			return;
		}
		await this.commitGenerationDeletesBounded(manifest);
		await this.backend.commit([
			{ type: "delete", store: "sources", key: sourceId },
		]);
	}

	/**
	 * Handles an interrupted replace. Pooled replacements restore their complete
	 * rollback; legacy streams retain partial pages as incomplete for a later refetch.
	 * @param sourceId Source whose stream disconnected.
	 * @param generation Expected building generation.
	 * @returns Incomplete source summary.
	 */
	public async markGenerationIncomplete(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceSummary> {
		const manifest = await this.requireBuildingManifest(sourceId, generation);
		if (manifest.pendingQueryCache && manifest.rollback) {
			await this.cancelGeneration(sourceId, generation);
			const restored = await this.requireCompleteSourceManifest(sourceId);
			return this.toSummary(restored);
		}
		const incomplete: StoredEventSourceManifest = {
			...manifest,
			revision: manifest.revision + 1,
			status: "incomplete",
		};
		await this.backend.commit([
			{
				type: "put",
				store: "sources",
				key: sourceId,
				value: incomplete,
			},
		]);
		return this.toSummary(incomplete);
	}

	/**
	 * Removes every active, rollback, retained-cache, and pending-cleanup generation.
	 * @param sourceId Source whose local cache must be released.
	 * @returns Nothing.
	 */
	public async clearSource(sourceId: Source.Id): Promise<void> {
		const manifest = await this.getStoredManifest(sourceId);
		const cacheRecords = await this.getQueryCacheRecords(sourceId);
		const generations = new Set<number>(
			cacheRecords.map((record) => record.generation),
		);
		if (manifest) {
			generations.add(manifest.generation);
			if (manifest.rollback) generations.add(manifest.rollback.generation);
			for (const marker of [
				...(manifest.pendingCacheCleanup ?? []),
				...(manifest.obsoleteGenerations ?? []),
			]) {
				generations.add(marker.generation);
			}
		}
		for (const generation of generations) {
			await this.commitObsoleteGenerationDeletesBounded(sourceId, { generation });
		}
		await this.commitDeleteOperationsBounded(
			cacheRecords.map((record) => ({
				type: "delete" as const,
				store: "source_query_caches" as const,
				key: this.createQueryCacheKey(sourceId, record.generation),
			})),
		);
		if (!manifest && cacheRecords.length === 0) return;
		await this.backend.commit([
			{ type: "delete", store: "sources", key: sourceId },
		]);
	}

	/**
	 * Resets a source's query-cache pool while preserving monotonic logical authority.
	 * The empty incomplete head becomes visible before bounded physical reclamation, so
	 * a following rebase generation cannot move backwards relative to Canvas state.
	 * @param sourceId Source whose query caches and physical pages must be invalidated.
	 * @returns Empty authority summary, or null when no source head exists.
	 */
	public async resetQueryCachePool(
		sourceId: Source.Id,
	): Promise<EventSourceSummary | null> {
		const current = await this.getStoredManifest(sourceId);
		if (!current) {
			await this.clearSource(sourceId);
			return null;
		}
		const cacheRecords = await this.getQueryCacheRecords(sourceId);
		const cleanupMarkers = this.mergeObsoleteGenerationMarkers(
			[
				...(current.pendingCacheCleanup ?? []),
				...(current.obsoleteGenerations ?? []),
			],
			[
				{ generation: current.generation },
				...(current.rollback
					? [{ generation: current.rollback.generation }]
					: []),
				...cacheRecords.map((record) => ({ generation: record.generation })),
			],
		);
		const reset: StoredEventSourceManifest = {
			sourceId,
			sourceOrdinal: current.sourceOrdinal,
			generation: current.generation,
			revision: current.revision + 1,
			authorityRevision: current.authorityRevision + 1,
			status: "incomplete",
			eventCount: 0,
			nextHandle: 1,
			minTimestamp: 0,
			maxTimestamp: 0,
			hashMin: 0,
			hashMax: 0,
			encodedBytes: 0,
			pages: [],
			documentIndexSegments: [],
			activeCacheGeneration: null,
			cacheHistory: [],
			cacheHistoryCursor: -1,
			nextGeneration: current.nextGeneration,
			...(cleanupMarkers.length > 0
				? { pendingCacheCleanup: cleanupMarkers }
				: {}),
		};
		await this.backend.commit([
			{ type: "put", store: "sources", key: sourceId, value: reset },
		]);
		await this.cleanupObsoleteGenerations(sourceId);
		return this.toSummary(reset);
	}

	/**
	 * Returns source authority without loading exact pages.
	 * @param sourceId Source whose manifest is requested.
	 * @returns Compact active-generation summary, or null when absent.
	 */
	public async getSourceSummary(
		sourceId: Source.Id,
	): Promise<EventSourceSummary | null> {
		const manifest = await this.getStoredManifest(sourceId);
		return manifest ? this.toSummary(manifest) : null;
	}

	/**
	 * Reads the first or last canonical event by selecting its page from the manifest.
	 * Timestamp ties use event sequence before stable page and offset tie breakers.
	 * @param sourceId Source whose edge is requested.
	 * @param newest Whether to select the newest rather than the oldest event.
	 * @returns Exact edge and the source revision used to select it.
	 */
	public async getSourceEdge(
		sourceId: Source.Id,
		newest: boolean,
	): Promise<EventSourceEdge> {
		const manifest = await this.getStoredManifest(sourceId);
		if (!manifest) return { event: null, sourceRevision: 0 };

		let selectedLocation: EventLocation | null = null;
		for (const catalogEntry of manifest.pages) {
			if (catalogEntry.activeCount === 0) continue;
			const candidate: EventLocation = {
				sourceId,
				generation: manifest.generation,
				pageId: catalogEntry.pageId,
				offset: newest ? 0 : Math.max(0, catalogEntry.count - 1),
				handle: catalogEntry.representativeHandle,
				timestamp: newest
					? catalogEntry.maxTimestamp
					: catalogEntry.minTimestamp,
				sequence: newest
					? catalogEntry.firstSequence
					: catalogEntry.lastSequence,
				revision: catalogEntry.revision,
			};
			if (!selectedLocation) {
				selectedLocation = candidate;
				continue;
			}
			const comparison = this.compareLocations(candidate, selectedLocation);
			if (newest ? comparison < 0 : comparison > 0) {
				selectedLocation = candidate;
			}
		}
		if (!selectedLocation) {
			return { event: null, sourceRevision: manifest.revision };
		}

		const page = await this.loadPage(
			sourceId,
			manifest.generation,
			selectedLocation.pageId,
		);
		let offset = newest ? 0 : page.count - 1;
		while (
			offset >= 0 &&
			offset < page.count &&
			page.tombstones[offset] !== 0
		) {
			offset += newest ? 1 : -1;
		}
		const event = offset >= 0 && offset < page.count
			? decodeEventAt(page, offset)
			: null;
		if (!event) {
			throw new EventStoreCorruptionError(
				`Active source edge page ${sourceId}/${selectedLocation.pageId} has no event.`,
			);
		}
		return { event, sourceRevision: manifest.revision };
	}

	/**
	 * Lists compact query-cache metadata without loading any exact event page.
	 * @param sourceId Source whose retained query results are requested.
	 * @returns Cache entries ordered from oldest to newest creation.
	 */
	public async listQueryCaches(
		sourceId: Source.Id,
	): Promise<EventSourceQueryCacheMetadata[]> {
		const records = await this.getQueryCacheRecords(sourceId);
		return records
			.sort(
				(left, right) =>
					left.createdAt - right.createdAt || left.generation - right.generation,
			)
			.map((record) => this.toQueryCacheMetadata(record));
	}

	/**
	 * Makes one retained cache authoritative without copying its physical pages.
	 * The target must still belong to the source's navigable history.
	 * @param input Source and physical cache generation to activate.
	 * @returns Activated cache metadata and monotonic source summary.
	 */
	public async activateQueryCache(
		input: ActivateEventSourceQueryCacheInput,
	): Promise<EventSourceQueryCacheActivation> {
		const current = await this.requireCompleteSourceManifest(input.sourceId);
		const cursor = current.cacheHistory.indexOf(input.generation);
		if (cursor < 0) {
			throw new StaleEventStoreGenerationError(
				`Query cache ${input.sourceId}/${input.generation} is not in active history.`,
			);
		}
		return this.activateQueryCacheRecord(
			current,
			input.generation,
			current.cacheHistory,
			cursor,
		);
	}

	/**
	 * Activates the preceding cache in one source's history without backend I/O.
	 * @param sourceId Source whose history cursor moves backward.
	 * @returns Activation result, or null when Undo is unavailable.
	 */
	public async undoQueryCache(
		sourceId: Source.Id,
	): Promise<EventSourceQueryCacheActivation | null> {
		const current = await this.requireCompleteSourceManifest(sourceId);
		if (current.cacheHistoryCursor <= 0) return null;
		const cursor = current.cacheHistoryCursor - 1;
		return this.activateQueryCacheRecord(
			current,
			current.cacheHistory[cursor],
			current.cacheHistory,
			cursor,
		);
	}

	/**
	 * Activates the following cache in one source's history without backend I/O.
	 * @param sourceId Source whose history cursor moves forward.
	 * @returns Activation result, or null when Redo is unavailable.
	 */
	public async redoQueryCache(
		sourceId: Source.Id,
	): Promise<EventSourceQueryCacheActivation | null> {
		const current = await this.requireCompleteSourceManifest(sourceId);
		if (current.cacheHistoryCursor >= current.cacheHistory.length - 1) return null;
		const cursor = current.cacheHistoryCursor + 1;
		return this.activateQueryCacheRecord(
			current,
			current.cacheHistory[cursor],
			current.cacheHistory,
			cursor,
		);
	}

	/**
	 * Selects a complete compatible cache with a fresh source snapshot. Startup may
	 * retain a stale exact or latest complete cache as a warning baseline rather than
	 * immediately querying data that is already available. Runtime reuse remains
	 * strict and preserves the current Undo/Redo history on a miss.
	 * @param input Source and desired query identity, range, and server snapshot.
	 * @returns A compatible or initial warning activation, or null when the backend
	 * must be queried.
	 */
	public async reconcileQueryCache(
		input: ReconcileEventSourceQueryCacheInput,
	): Promise<EventSourceQueryCacheReconciliation | null> {
		let current = await this.getStoredManifest(input.sourceId);
		if (current?.status === "building") {
			await this.cancelGeneration(input.sourceId, current.generation);
			current = await this.getStoredManifest(input.sourceId);
		} else if (current?.status === "incomplete") {
			if (!current.rollback) {
				current =
					(await this.resetQueryCachePool(input.sourceId)) === null
						? undefined
						: await this.getStoredManifest(input.sourceId);
			} else {
				await this.commitGenerationDeletesBounded(current);
				const restored: StoredEventSourceManifest = {
					...current.rollback,
					revision: current.revision + 1,
					authorityRevision: current.authorityRevision + 1,
					status: "complete",
				};
				await this.backend.commit([
					{
						type: "put",
						store: "sources",
						key: input.sourceId,
						value: restored,
					},
				]);
				current = restored;
			}
		}
		if (current) {
			await this.cleanupObsoleteGenerations(input.sourceId);
			current = await this.getStoredManifest(input.sourceId);
		}
		const records = await this.getQueryCacheRecords(input.sourceId);
		if (records.length === 0) return null;
		const reusableByRecency = records
			.filter(
				(record) =>
					record.state === "exact" && record.manifest.status === "complete",
			)
			.sort(
				(left, right) =>
					right.lastActivatedAt - left.lastActivatedAt ||
					right.createdAt - left.createdAt ||
					right.generation - left.generation,
			);
		if (reusableByRecency.length === 0) {
			if (input.mode !== "reuse") await this.resetQueryCachePool(input.sourceId);
			return null;
		}
		const exactCandidates = reusableByRecency.filter(
			(record) => record.provenance.fingerprint === input.desired.fingerprint,
		);
		const freshExact = exactCandidates.find((record) =>
			queryCacheSnapshotMatches(record.provenance, input.desired),
		);
		const freshCovered = reusableByRecency.find((record) =>
			queryCacheCoversDesiredLookup(record, input.desired),
		);
		const staleExact = exactCandidates[0];
		const chosen =
			freshExact ??
			freshCovered ??
			(input.mode === "reuse"
				? undefined
				: staleExact ?? reusableByRecency[0]);
		if (!chosen) {
			if (input.mode !== "reuse") await this.resetQueryCachePool(input.sourceId);
			return null;
		}
		const match = freshExact
			? "exact"
			: freshCovered
				? "covered"
				: staleExact === chosen
					? "exact"
					: "latest";
		if (input.mode === "reuse" && current) {
			if (current.activeCacheGeneration === chosen.generation) {
				return { summary: this.toSummary(current), cache: this.toQueryCacheMetadata(chosen), match };
			}
			if (!current.cacheHistory.includes(chosen.generation)) return null;
			return { ...await this.activateQueryCache({ sourceId: input.sourceId, generation: chosen.generation }), match };
		}
		const reconstructed = this.createSourceHeadFromCache(chosen);
		const base = current ?? {
			...reconstructed,
			nextGeneration: Math.max(
				reconstructed.nextGeneration,
				...records.map((record) => record.generation + 1),
			),
		};
		const pendingCacheCleanup = records
			.filter((record) => record.generation !== chosen.generation)
			.map((record) => ({ generation: record.generation }));
		const activation = await this.activateQueryCacheRecord(
			base,
			chosen.generation,
			[chosen.generation],
			0,
			pendingCacheCleanup,
		);
		await this.cleanupObsoleteGenerations(input.sourceId);
		return { ...activation, match };
	}

	/**
	 * Retains an explicit history subset and reclaims every other complete cache.
	 * The active cache cannot be removed by pruning.
	 * @param sourceId Source whose cache pool is bounded.
	 * @param retainedGenerations Ordered generations to keep navigable.
	 * @returns Promise settled after bounded physical cleanup completes.
	 */
	public async pruneQueryCaches(
		sourceId: Source.Id,
		retainedGenerations: readonly number[],
	): Promise<void> {
		const current = await this.requireCompleteSourceManifest(sourceId);
		const retained = [...new Set(retainedGenerations)];
		if (retained.length > this.queryCacheLimit) {
			throw new RangeError(
				`At most ${this.queryCacheLimit} query caches can be retained per source.`,
			);
		}
		if (
			current.activeCacheGeneration !== null &&
			!retained.includes(current.activeCacheGeneration)
		) {
			throw new Error("The active query cache cannot be pruned.");
		}
		const records = await this.getQueryCacheRecords(sourceId);
		const available = new Set(records.map((record) => record.generation));
		if (retained.some((generation) => !available.has(generation))) {
			throw new StaleEventStoreGenerationError(
				"A retained query-cache generation is unavailable.",
			);
		}
		const activeCursor =
			current.activeCacheGeneration === null
				? -1
				: retained.indexOf(current.activeCacheGeneration);
		const pendingCacheCleanup = this.mergeObsoleteGenerationMarkers(
			current.pendingCacheCleanup,
			records
				.filter((record) => !retained.includes(record.generation))
				.map((record) => ({ generation: record.generation })),
		);
		const next: StoredEventSourceManifest = {
			...current,
			cacheHistory: retained,
			cacheHistoryCursor: activeCursor,
			...(pendingCacheCleanup.length > 0 ? { pendingCacheCleanup } : {}),
		};
		await this.backend.commit([
			{ type: "put", store: "sources", key: sourceId, value: next },
		]);
		await this.cleanupObsoleteGenerations(sourceId);
	}

	/**
	 * Resolves one exact document through binary hash segments and verifies its UTF-8 ID.
	 * @param documentId Canonical document identifier.
	 * @param sourceId Optional collision discriminator.
	 * @returns Matching active-generation document, or null.
	 */
	public async getById(
		documentId: Doc.Id,
		sourceId?: Source.Id,
	): Promise<Doc.Type | null> {
		const context = await this.createLookupContext();
		const location = await this.findDocumentLocation(
			documentId,
			sourceId,
			context,
		);
		return location ? this.decodeLocation(location, new Map()) : null;
	}

	/**
	 * Resolves several IDs while loading manifests and index segments only once.
	 * @param documentIds Document identifiers in caller order.
	 * @returns Found documents in caller order.
	 */
	public async getByIds(documentIds: readonly Doc.Id[]): Promise<Doc.Type[]> {
		if (documentIds.length === 0) return [];
		const context = await this.createLookupContext();
		const locations = await this.findDocumentLocations(documentIds, undefined, context);

		const orderedLocations = documentIds.flatMap((documentId) => {
			const location = locations.get(documentId);
			return location ? [location] : [];
		});
		return this.decodeLocations(orderedLocations);
	}

	/**
	 * Resolves one non-zero source-local render handle without loading source documents.
	 * @param sourceId Source owning the handle.
	 * @param handle Stable uint32 handle.
	 * @returns Matching active document, or null.
	 */
	public async getByHandle(
		sourceId: Source.Id,
		handle: number,
	): Promise<Doc.Type | null> {
		if (!Number.isInteger(handle) || handle <= 0) return null;
		const manifest = await this.getStoredManifest(sourceId);
		if (!manifest) return null;
		const location = await this.findHandleLocation(manifest, handle);
		return location ? this.decodeLocation(location, new Map()) : null;
	}

	/**
	 * Reads a centered bounded window using the canonical cross-page ordering.
	 * @param sourceId Source containing the current document.
	 * @param documentIdOrHandle Current document identifier or revision-scoped handle.
	 * @param before Maximum newer neighbors.
	 * @param after Maximum older neighbors.
	 * @returns Neighbor events and the current event's window index.
	 */
	public async getNeighbors(
		sourceId: Source.Id,
		documentIdOrHandle: Doc.Id | number,
		before: number,
		after: number,
	): Promise<EventNeighborWindow> {
		const manifest = await this.getStoredManifest(sourceId);
		if (!manifest) return { events: [], currentIndex: -1, sourceRevision: 0 };
		const current =
			typeof documentIdOrHandle === "number"
				? await this.findHandleLocation(manifest, documentIdOrHandle)
				: await this.findDocumentLocation(
						documentIdOrHandle,
						sourceId,
						await this.createLookupContext(),
					);
		if (!current) {
			return {
				events: [],
				currentIndex: -1,
				sourceRevision: manifest.revision,
			};
		}
		const currentPage = await this.loadPage(
			current.sourceId,
			current.generation,
			current.pageId,
		);
		const currentDocument = decodeEventAt(currentPage, current.offset) ?? null;
		if (!currentDocument) {
			return {
				events: [],
				currentIndex: -1,
				sourceRevision: manifest.revision,
			};
		}

		const beforeLimit = Math.max(
			0,
			Math.min(EVENT_PIXEL_PAGE_SIZE, Math.floor(before)),
		);
		const afterLimit = Math.max(
			0,
			Math.min(EVENT_PIXEL_PAGE_SIZE, Math.floor(after)),
		);
		const newer = await this.collectNeighborLocations(
			manifest,
			current,
			currentPage,
			beforeLimit,
			"before",
		);
		const older = await this.collectNeighborLocations(
			manifest,
			current,
			currentPage,
			afterLimit,
			"after",
		);
		const newerDocuments = await this.decodeLocations(newer);
		const olderDocuments = await this.decodeLocations(older);
		return {
			events: [...newerDocuments, currentDocument, ...olderDocuments],
			currentIndex: newerDocuments.length,
			sourceRevision: manifest.revision,
		};
	}

	/**
	 * Reads bounded query neighbors around a timestamp that has no active document.
	 * Query events at the exact timestamp are placed on the newer side so a caller
	 * can insert its transient anchor deterministically after them.
	 * @param sourceId Source whose active generation is inspected.
	 * @param timestamp Timestamp used as the virtual insertion point.
	 * @param before Maximum newer neighbors.
	 * @param after Maximum older neighbors.
	 * @returns Bounded canonical query events on each side of the timestamp.
	 */
	public async getTimestampNeighbors(
		sourceId: Source.Id,
		timestamp: number,
		before: number,
		after: number,
	): Promise<EventTimestampNeighborWindow> {
		const manifest = await this.getStoredManifest(sourceId);
		if (!manifest) {
			return { newer: [], older: [], sourceRevision: 0 };
		}
		if (!Number.isFinite(timestamp)) {
			throw new RangeError("Event neighbor timestamp must be finite.");
		}
		const beforeLimit = Math.max(
			0,
			Math.min(EVENT_PIXEL_PAGE_SIZE, Math.floor(before)),
		);
		const afterLimit = Math.max(
			0,
			Math.min(EVENT_PIXEL_PAGE_SIZE, Math.floor(after)),
		);
		const lastPageId = manifest.pages.reduce(
			(maximum, page) => Math.max(maximum, page.pageId),
			0,
		);
		const anchor: EventLocation = {
			sourceId,
			generation: manifest.generation,
			pageId: lastPageId + 1,
			offset: 0,
			handle: 0,
			timestamp,
			sequence: Number.POSITIVE_INFINITY,
			revision: manifest.revision,
		};
		const newer = await this.collectNeighborLocations(
			manifest,
			anchor,
			null,
			beforeLimit,
			"before",
		);
		const older = await this.collectNeighborLocations(
			manifest,
			anchor,
			null,
			afterLimit,
			"after",
		);
		return {
			newer: await this.decodeLocations(newer),
			older: await this.decodeLocations(older),
			sourceRevision: manifest.revision,
		};
	}

	/**
	 * Reads only pages whose timestamp bounds can still improve a bounded neighbor side.
	 * Fully overlapping page ranges remain exact and are visited one at a time, while the
	 * normal non-overlapping append layout stops after the immediately adjacent pages.
	 * @param manifest Source authority and compact page catalog.
	 * @param current Exact current-event location.
	 * @param currentPage Already loaded page containing the current event, or null for a timestamp anchor.
	 * @param limit Maximum locations retained on this side.
	 * @param side Newer (`before`) or older (`after`) canonical side.
	 * @returns Canonically sorted bounded neighbor locations.
	 */
	private async collectNeighborLocations(
		manifest: StoredEventSourceManifest,
		current: EventLocation,
		currentPage: EncodedEventPage | null,
		limit: number,
		side: EventNeighborSide,
	): Promise<EventLocation[]> {
		if (limit === 0) return [];
		const locations: EventLocation[] = [];
		if (currentPage) {
			this.collectPageNeighborLocations(currentPage, current, side, limit, locations);
		}

		const candidates = manifest.pages
			.filter(
				(catalogEntry) =>
					catalogEntry.activeCount > 0 &&
					catalogEntry.pageId !== current.pageId,
			)
			.flatMap((catalogEntry) => {
				const optimisticLocation = this.createNeighborPageBound(
					manifest,
					catalogEntry,
					current,
					side,
				);
				return optimisticLocation === undefined
					? []
					: [{ catalogEntry, optimisticLocation }];
			});
		candidates.sort((left, right) =>
			this.compareNeighborPageCandidates(left, right, side),
		);

		for (const candidate of candidates) {
			if (
				candidate.optimisticLocation &&
				locations.length >= limit &&
				!this.canNeighborPageImprove(
					candidate.optimisticLocation,
					locations,
					side,
				)
			) {
				// Candidate bounds are ordered by closeness, so no later bounded page
				// can improve the retained window either.
				break;
			}
			const page = await this.loadPage(
				manifest.sourceId,
				manifest.generation,
				candidate.catalogEntry.pageId,
			);
			this.collectPageNeighborLocations(page, current, side, limit, locations);
		}
		return locations;
	}

	/**
	 * Builds an optimistic closest-event bound for one page and neighbor side.
	 * `null` denotes a page spanning the current canonical order key whose exact
	 * nearest value cannot be bounded from the catalog alone; such pages must be
	 * read for correctness.
	 * @param manifest Active source generation.
	 * @param catalogEntry Candidate page metadata.
	 * @param current Current event location.
	 * @param side Requested canonical side.
	 * @returns Undefined when the page cannot contribute, null when unbounded, or a bound.
	 */
	private createNeighborPageBound(
		manifest: StoredEventSourceManifest,
		catalogEntry: EventPageCatalogEntry,
		current: EventLocation,
		side: EventNeighborSide,
	): EventLocation | null | undefined {
		const base = {
			sourceId: manifest.sourceId,
			generation: manifest.generation,
			pageId: catalogEntry.pageId,
			handle: catalogEntry.representativeHandle,
			revision: catalogEntry.revision,
		};
		const first: EventLocation = {
			...base,
			offset: 0,
			timestamp: catalogEntry.maxTimestamp,
			sequence: catalogEntry.firstSequence,
		};
		const last: EventLocation = {
			...base,
			offset: Math.max(0, catalogEntry.count - 1),
			timestamp: catalogEntry.minTimestamp,
			sequence: catalogEntry.lastSequence,
		};
		const firstComparison = this.compareLocations(first, current);
		const lastComparison = this.compareLocations(last, current);
		if (side === "before") {
			if (firstComparison > 0) return undefined;
			if (lastComparison < 0) return last;
			return null;
		}

		if (lastComparison < 0) return undefined;
		if (firstComparison > 0) return first;
		return null;
	}

	/**
	 * Orders unbounded overlapping pages first, then bounded pages from closest to farthest.
	 * @param left First page candidate.
	 * @param right Second page candidate.
	 * @param side Requested canonical side.
	 * @returns Array-sort comparison.
	 */
	private compareNeighborPageCandidates(
		left: EventNeighborPageCandidate,
		right: EventNeighborPageCandidate,
		side: EventNeighborSide,
	): number {
		if (!left.optimisticLocation && !right.optimisticLocation) {
			return left.catalogEntry.pageId - right.catalogEntry.pageId;
		}
		if (!left.optimisticLocation) return -1;
		if (!right.optimisticLocation) return 1;
		return side === "before"
			? this.compareLocations(
					right.optimisticLocation,
					left.optimisticLocation,
				)
			: this.compareLocations(
					left.optimisticLocation,
					right.optimisticLocation,
				);
	}

	/**
	 * Checks whether a page's optimistic closest location can beat the retained far edge.
	 * @param optimisticLocation Closest catalog-level page bound.
	 * @param locations Current bounded canonical locations.
	 * @param side Requested canonical side.
	 * @returns True when loading the page can still change the result.
	 */
	private canNeighborPageImprove(
		optimisticLocation: EventLocation,
		locations: readonly EventLocation[],
		side: EventNeighborSide,
	): boolean {
		const retainedFarEdge =
			side === "before" ? locations[0] : locations[locations.length - 1];
		const comparison = this.compareLocations(
			optimisticLocation,
			retainedFarEdge,
		);
		return side === "before" ? comparison > 0 : comparison < 0;
	}

	/**
	 * Extracts only the closest active offsets from one sorted exact page.
	 * @param page Candidate exact page.
	 * @param current Current event location.
	 * @param side Requested canonical side.
	 * @param limit Maximum retained locations.
	 * @param locations Mutable canonical result list.
	 * @returns Nothing.
	 */
	private collectPageNeighborLocations(
		page: EncodedEventPage,
		current: EventLocation,
		side: EventNeighborSide,
		limit: number,
		locations: EventLocation[],
	): void {
		if (side === "before") {
			const endExclusive =
				page.pageId === current.pageId
					? current.offset
					: this.findFirstPageOffsetNotPreceding(page, current);
			let retained = 0;
			for (let offset = endExclusive - 1; offset >= 0 && retained < limit; offset--) {
				if (page.tombstones[offset] !== 0) continue;
				this.insertSortedLocation(
					locations,
					this.createEventLocation(page, offset),
				);
				if (locations.length > limit) locations.shift();
				retained++;
			}
			return;
		}

		const startOffset =
			page.pageId === current.pageId
				? current.offset + 1
				: this.findFirstPageOffsetFollowing(page, current);
		let retained = 0;
		for (
			let offset = startOffset;
			offset < page.count && retained < limit;
			offset++
		) {
			if (page.tombstones[offset] !== 0) continue;
			this.insertSortedLocation(
				locations,
				this.createEventLocation(page, offset),
			);
			if (locations.length > limit) locations.pop();
			retained++;
		}
	}

	/**
	 * Creates a lightweight active location for one already-validated page offset.
	 * @param page Exact page containing the active event.
	 * @param offset Active event offset.
	 * @returns Canonical comparison location.
	 */
	private createEventLocation(page: EncodedEventPage, offset: number): EventLocation {
		return {
			sourceId: page.sourceId,
			generation: page.generation,
			pageId: page.pageId,
			offset,
			handle: page.handles[offset],
			timestamp: page.timestamps[offset],
			sequence: page.sequences[offset],
			revision: page.revision,
		};
	}

	/**
	 * Reads an exact inclusive time range while retaining at most `limit + 1` locations.
	 * Overlapping page ranges are merged through timestamp/page/offset ordering.
	 * @param sourceId Source whose pixel interval is queried.
	 * @param range Inclusive timestamp bounds.
	 * @param limit Requested result bound, capped at the standard pixel page size.
	 * @param cursor Optional source-revision cursor.
	 * @returns Exact bounded page, total matching count, and continuation cursor.
	 */
	public async queryPixelBucket(
		sourceId: Source.Id,
		range: EventTimestampRange,
		limit: number,
		cursor?: EventPageCursor,
	): Promise<EventPixelPage> {
		const manifest = await this.getStoredManifest(sourceId);
		if (!manifest) {
			return { events: [], total: 0, nextCursor: null, sourceRevision: 0 };
		}
		const safeLimit = Math.max(
			1,
			Math.min(EVENT_PIXEL_PAGE_SIZE, Math.floor(limit)),
		);
		const minimum = Math.min(range.min, range.max);
		const maximum = Math.max(range.min, range.max);
		if (Number.isNaN(minimum) || Number.isNaN(maximum)) {
			throw new RangeError("Event timestamp range cannot contain NaN.");
		}

		let cursorLocation: EventLocation | null = null;
		if (cursor) {
			if (
				cursor.sourceId !== sourceId ||
				cursor.sourceRevision !== manifest.revision
			) {
				throw new StaleEventStoreCursorError(
					"Event cursor does not belong to the active source revision.",
				);
			}
			const context = await this.createLookupContext();
			cursorLocation = await this.findDocumentLocation(
				cursor.documentId,
				sourceId,
				context,
			);
			if (
				!cursorLocation ||
				cursorLocation.timestamp !== cursor.timestamp ||
				cursorLocation.timestamp < minimum ||
				cursorLocation.timestamp > maximum
			) {
				throw new StaleEventStoreCursorError(
					"Event cursor target is unavailable in the requested range.",
				);
			}
		}

		let total = 0;
		let eligibleAfterCursor = 0;
		const selected: EventLocation[] = [];
		for (const catalogEntry of manifest.pages) {
			if (
				catalogEntry.activeCount === 0 ||
				catalogEntry.maxTimestamp < minimum ||
				catalogEntry.minTimestamp > maximum
			) {
				continue;
			}
			const fullyContained =
				catalogEntry.minTimestamp >= minimum &&
				catalogEntry.maxTimestamp <= maximum;
			const worstSelected = selected[selected.length - 1];
			if (fullyContained && worstSelected && selected.length >= safeLimit + 1) {
				const bestPossibleLocation: EventLocation = {
					sourceId,
					generation: manifest.generation,
					pageId: catalogEntry.pageId,
					offset: 0,
					handle: catalogEntry.representativeHandle,
					timestamp: catalogEntry.maxTimestamp,
					sequence: catalogEntry.firstSequence,
					revision: catalogEntry.revision,
				};
				if (this.compareLocations(bestPossibleLocation, worstSelected) >= 0) {
					total += catalogEntry.activeCount;
					eligibleAfterCursor = Math.max(eligibleAfterCursor, safeLimit + 1);
					continue;
				}
			}
			const page = await this.loadPage(
				sourceId,
				manifest.generation,
				catalogEntry.pageId,
			);
			let pageTotal = 0;
			let pageEligible = 0;
			const firstOffset = this.findFirstDescendingAtMost(page.timestamps, maximum);
			for (let offset = firstOffset; offset < page.count; offset++) {
				const timestamp = page.timestamps[offset];
				if (timestamp < minimum) break;
				if (page.tombstones[offset] !== 0) continue;
				pageTotal++;
				const location: EventLocation = {
					sourceId,
					generation: manifest.generation,
					pageId: page.pageId,
					offset,
					handle: page.handles[offset],
					timestamp,
					sequence: page.sequences[offset],
					revision: page.revision,
				};
				if (cursorLocation && this.compareLocations(location, cursorLocation) <= 0) {
					continue;
				}
				eligibleAfterCursor = Math.min(
					safeLimit + 1,
					eligibleAfterCursor + 1,
				);
				pageEligible++;
				this.insertBoundedLocation(selected, location, safeLimit + 1);
				if (fullyContained && pageEligible >= safeLimit + 1) break;
			}
			total += fullyContained ? catalogEntry.activeCount : pageTotal;
		}

		const visibleLocations = selected.slice(0, safeLimit);
		const events = await this.decodeLocations(visibleLocations);
		const finalEvent = events[events.length - 1];
		return {
			events,
			total,
			nextCursor:
				eligibleAfterCursor > events.length && finalEvent
					? {
						sourceId,
						sourceRevision: manifest.revision,
						timestamp: finalEvent.gulp_timestamp,
						documentId: finalEvent._id,
					}
					: null,
			sourceRevision: manifest.revision,
		};
	}

	/**
	 * Deduplicates one bounded building-generation append without retaining an ID set
	 * proportional to the source. Later incoming occurrences win; exact locations of
	 * older committed occurrences are returned for bounded tombstone transactions.
	 * @param manifest Active building-generation authority.
	 * @param pages Newly encoded append-only pages.
	 * @returns Final incoming pages, existing duplicate locations, and maximum handle.
	 */
	private async prepareDeduplicatedBuildingAppend(
		manifest: StoredEventSourceManifest,
		pages: readonly EncodedEventPage[],
	): Promise<DeduplicatedAppendPreparation> {
		const existingPageIds = new Set(manifest.pages.map((page) => page.pageId));
		const incomingPageIds = new Set<number>();
		const incomingHandles = new Set<number>();
		const incomingPages: EncodedEventPage[] = [];
		const winners = new Map<Doc.Id, { pageIndex: number; offset: number }>();
		let maximumHandle = Math.max(0, manifest.nextHandle - 1);

		for (const sourcePage of pages) {
			this.assertEncodedPage(
				sourcePage,
				manifest.sourceId,
				manifest.generation,
			);
			if (
				existingPageIds.has(sourcePage.pageId) ||
				incomingPageIds.has(sourcePage.pageId)
			) {
				throw new Error(`Event page ${sourcePage.pageId} is not append-only.`);
			}
			incomingPageIds.add(sourcePage.pageId);
			const pageIndex = incomingPages.length;
			const page = this.cloneEncodedPage(sourcePage);
			incomingPages.push(page);
			for (let offset = 0; offset < page.count; offset++) {
				if (page.tombstones[offset] !== 0) continue;
				const handle = page.handles[offset];
				if (handle < manifest.nextHandle || incomingHandles.has(handle)) {
					throw new Error(
						`Event handle ${handle} is not append-only for source ${manifest.sourceId}.`,
					);
				}
				incomingHandles.add(handle);
				maximumHandle = Math.max(maximumHandle, handle);

				const documentId = this.decodeDocumentId(page, offset);
				const previous = winners.get(documentId);
				if (previous) {
					const previousPage = incomingPages[previous.pageIndex];
					if (previousPage.tombstones[previous.offset] === 0) {
						previousPage.tombstones[previous.offset] = 1;
						previousPage.activeCount--;
					}
				}
				winners.set(documentId, { pageIndex, offset });
			}
		}

		const locations = await this.findDocumentLocations(
			[...winners.keys()],
			manifest.sourceId,
			{ manifests: new Map([[manifest.sourceId, manifest]]) },
		);
		return {
			incomingPages: incomingPages.map((page) =>
				this.finalizeMutatedPage(page),
			),
			existingLocations: locations,
			maximumHandle,
		};
	}

	/**
	 * Tombstones committed duplicates in bounded groups before publishing incoming pages.
	 * Intermediate building revisions are internal stream checkpoints and are never
	 * exposed as completed source summaries by {@link appendPages}.
	 * @param manifest Initial building-generation authority.
	 * @param locations Exact committed duplicate locations keyed by document ID.
	 * @returns Latest building manifest after every bounded page group is committed.
	 */
	private async applyBuildingDuplicateTombstones(
		manifest: StoredEventSourceManifest,
		locations: ReadonlyMap<Doc.Id, EventLocation>,
	): Promise<StoredEventSourceManifest> {
		if (locations.size === 0) return manifest;
		const locationsByPage = new Map<
			number,
			Array<{ documentId: Doc.Id; location: EventLocation }>
		>();
		for (const [documentId, location] of locations) {
			if (
				location.sourceId !== manifest.sourceId ||
				location.generation !== manifest.generation
			) {
				throw new EventStoreCorruptionError(
					`Building duplicate ${documentId} resolved outside its active generation.`,
				);
			}
			const pageLocations = locationsByPage.get(location.pageId) ?? [];
			pageLocations.push({ documentId, location });
			locationsByPage.set(location.pageId, pageLocations);
		}

		const pageGroups = [...locationsByPage.entries()].sort(
			([leftPageId], [rightPageId]) => leftPageId - rightPageId,
		);
		let currentManifest = manifest;
		for (
			let start = 0;
			start < pageGroups.length;
			start += EVENT_UPSERT_SOURCE_PAGE_SLICE_LIMIT
		) {
			currentManifest = await this.requireBuildingManifest(
				manifest.sourceId,
				manifest.generation,
			);
			const revision = currentManifest.revision + 1;
			const replacementPages: EncodedEventPage[] = [];
			const operations: EventStoreWriteOperation[] = [];
			let pageStorageByteDelta = 0;
			for (const [pageId, duplicateLocations] of pageGroups.slice(
				start,
				start + EVENT_UPSERT_SOURCE_PAGE_SLICE_LIMIT,
			)) {
				const catalogEntry = currentManifest.pages.find(
					(entry) => entry.pageId === pageId,
				);
				if (!catalogEntry) {
					throw new EventStoreCorruptionError(
						`Missing catalog entry for duplicate page ${manifest.sourceId}/${pageId}.`,
					);
				}
				const original = await this.loadPage(
					manifest.sourceId,
					manifest.generation,
					pageId,
				);
				if (original.revision !== catalogEntry.revision) {
					throw new EventStoreCorruptionError(
						`Duplicate page ${manifest.sourceId}/${pageId} has a stale revision.`,
					);
				}
				const replacement = this.cloneEncodedPage(original);
				replacement.revision = revision;
				for (const { documentId, location } of duplicateLocations) {
					if (
						location.offset >= replacement.count ||
						replacement.tombstones[location.offset] !== 0 ||
						this.decodeDocumentId(replacement, location.offset) !== documentId
					) {
						throw new EventStoreCorruptionError(
							`Building duplicate locator for ${documentId} is stale.`,
						);
					}
					replacement.tombstones[location.offset] = 1;
					replacement.activeCount--;
				}
				const finalized = this.finalizeMutatedPage(replacement);
				const originalLod = buildEventPageLod(original);
				const replacementLod = buildEventPageLod(finalized);
				const originalHeight = buildEventPageHeight(original);
				const replacementHeight = buildEventPageHeight(finalized);
				const pageKey = this.createPageKey(
					manifest.sourceId,
					manifest.generation,
					pageId,
				);
				operations.push(
					{ type: "put", store: "pages", key: pageKey, value: finalized },
					{
						type: "put",
						store: "page_lod",
						key: this.createPageSummaryKey(
							manifest.sourceId,
							manifest.generation,
							pageId,
							EVENT_PAGE_LOD_RECORD_KIND,
						),
						value: replacementLod,
					},
					{
						type: "put",
						store: "page_lod",
						key: this.createPageSummaryKey(
							manifest.sourceId,
							manifest.generation,
							pageId,
							EVENT_PAGE_HEIGHT_RECORD_KIND,
						),
						value: replacementHeight,
					},
				);
				pageStorageByteDelta +=
					finalized.encodedByteLength +
					replacementLod.encodedByteLength -
					original.encodedByteLength -
					originalLod.encodedByteLength +
					replacementHeight.encodedByteLength -
					originalHeight.encodedByteLength;
				replacementPages.push(finalized);
			}

			const indexUpdate = await this.prepareAppendDocumentIndex(
				currentManifest,
				replacementPages,
			);
			const catalogByPage = new Map(
				currentManifest.pages.map((entry) => [entry.pageId, entry]),
			);
			for (const page of replacementPages) {
				catalogByPage.set(page.pageId, createEventPageCatalogEntry(page));
			}
			const nextManifest = this.recalculateManifest({
				...currentManifest,
				revision,
				encodedBytes:
					currentManifest.encodedBytes +
					pageStorageByteDelta +
					indexUpdate.encodedByteDelta,
				pages: [...catalogByPage.values()].sort((left, right) =>
					this.compareCatalogEntries(left, right),
				),
				documentIndexSegments: indexUpdate.catalog,
			});
			operations.push({
				type: "put",
				store: "sources",
				key: manifest.sourceId,
				value: nextManifest,
			});
			try {
				await this.backend.commit(operations);
			} catch (error) {
				await this.commitIndexOperationsBounded(
					indexUpdate.stagedDeleteOperations,
				).catch(() => undefined);
				throw error;
			}
			await this.commitIndexOperationsBounded(
				indexUpdate.obsoleteDeleteOperations,
			).catch(() => undefined);
			currentManifest = nextManifest;
		}
		return currentManifest;
	}

	/**
	 * Adds one binary delta per touched shard and performs leveled pairwise compaction.
	 * This keeps lookup segments bounded without repeatedly rewriting the entire shard.
	 * @param manifest Active generation authority.
	 * @param pages Newly appended exact pages.
	 * @returns Prepared index mutations, next catalog, and storage-byte delta.
	 */
	private async prepareAppendDocumentIndex(
		manifest: StoredEventSourceManifest,
		pages: readonly EncodedEventPage[],
	): Promise<DocumentIndexUpdate> {
		const entriesByShard = this.collectDocumentIndexEntries(pages);
		const effectivePageRevisions = new Map(
			manifest.pages.map((page) => [page.pageId, page.revision]),
		);
		for (const page of pages) {
			effectivePageRevisions.set(page.pageId, page.revision);
		}
		let catalog = [...manifest.documentIndexSegments];
		const stagedDeleteOperations: EventStoreWriteOperation[] = [];
		const obsoleteDeleteOperations: EventStoreWriteOperation[] = [];
		let pendingStageOperations: EventStoreWriteOperation[] = [];
		let pendingStageDeletes: EventStoreWriteOperation[] = [];
		let pendingStageBytes = 0;
		let encodedByteDelta = 0;
		/** Persists one bounded index stage and remembers its rollback deletes. */
		const flushStage = async (): Promise<void> => {
			if (pendingStageOperations.length === 0) return;
			await this.backend.commit(pendingStageOperations);
			stagedDeleteOperations.push(...pendingStageDeletes);
			pendingStageOperations = [];
			pendingStageDeletes = [];
			pendingStageBytes = 0;
		};

		try {
			for (const [shard, additions] of entriesByShard) {
				const initialCatalog = catalog.filter((entry) => entry.shard === shard);
				const nextSegmentId =
					initialCatalog.reduce(
						(maximum, entry) => Math.max(maximum, entry.segmentId),
						-1,
					) + 1;
				let entries = additions;
				let level = 0;
				let existingSegments: EncodedDocumentIndexSegment[] | null = null;
				while (true) {
					const sameLevelCatalog = catalog.filter(
						(entry) => entry.shard === shard && entry.level === level,
					);
					if (sameLevelCatalog.length === 0) break;
					existingSegments ??= await this.loadDocumentIndexShard(
						manifest.sourceId,
						manifest.generation,
						shard,
						manifest.documentIndexSegments,
					);
					const sameLevelIds = new Set(
						sameLevelCatalog.map((entry) => entry.segmentId),
					);
					entries = this.deduplicateDocumentIndexEntries([
						...existingSegments
							.filter((segment) => sameLevelIds.has(segment.segmentId))
							.flatMap((segment) => this.decodeDocumentIndexEntries(segment))
							.filter(
								(entry) =>
									effectivePageRevisions.get(entry.pageId) ===
									entry.pageRevision,
							),
						...entries,
					]);
					for (const entry of sameLevelCatalog) {
						obsoleteDeleteOperations.push({
							type: "delete",
							store: "document_index_pages",
							key: this.createDocumentIndexKey(
								manifest.sourceId,
								manifest.generation,
								entry.shard,
								entry.segmentId,
							),
						});
						encodedByteDelta -= entry.encodedByteLength;
					}
					catalog = catalog.filter(
						(entry) =>
							entry.shard !== shard || !sameLevelIds.has(entry.segmentId),
					);
					level++;
					if (level >= EVENT_DOCUMENT_INDEX_SEGMENT_LIMIT) {
						throw new EventStoreCorruptionError(
							`Document index shard ${manifest.sourceId}/${shard} exhausted its compaction levels.`,
						);
					}
				}

				const segment = this.encodeDocumentIndexSegment(
					manifest,
					shard,
					nextSegmentId,
					level,
					entries,
				);
				const segmentKey = this.createDocumentIndexKey(
					manifest.sourceId,
					manifest.generation,
					shard,
					segment.segmentId,
				);
				pendingStageOperations.push({
					type: "put",
					store: "document_index_pages",
					key: segmentKey,
					value: segment,
				});
				pendingStageDeletes.push({
					type: "delete",
					store: "document_index_pages",
					key: segmentKey,
				});
				pendingStageBytes += segment.encodedByteLength;
				if (
					pendingStageOperations.length >= EVENT_INDEX_STAGE_OPERATION_LIMIT ||
					pendingStageBytes >= EVENT_INDEX_STAGE_BYTE_LIMIT
				) {
					await flushStage();
				}
				catalog.push({
					shard,
					segmentId: segment.segmentId,
					level,
					encodedByteLength: segment.encodedByteLength,
				});
				encodedByteDelta += segment.encodedByteLength;
			}
			await flushStage();
		} catch (error) {
			await this.commitIndexOperationsBounded(stagedDeleteOperations).catch(
				() => undefined,
			);
			throw error;
		}

		catalog.sort(
			(left, right) =>
				left.shard - right.shard ||
				left.level - right.level ||
				left.segmentId - right.segmentId,
		);
		return {
			catalog,
			encodedByteDelta,
			stagedDeleteOperations,
			obsoleteDeleteOperations,
		};
	}

	/**
	 * Removes duplicate locators created by append deltas before synchronous compaction.
	 * @param entries Candidate entries sharing one shard.
	 * @returns Latest page-revision entry for each page offset.
	 */
	private deduplicateDocumentIndexEntries(
		entries: readonly DocumentIndexEntry[],
	): DocumentIndexEntry[] {
		const latest = new Map<string, DocumentIndexEntry>();
		for (const entry of entries) {
			const key = `${entry.pageId}:${entry.offset}`;
			const current = latest.get(key);
			if (!current || entry.pageRevision >= current.pageRevision) {
				latest.set(key, entry);
			}
		}
		return [...latest.values()];
	}

	/**
	 * Collects active locators by physical shard while retaining full 12-bit hashes.
	 * @param pages Exact pages to index.
	 * @returns Locator entries grouped by the high eight bits of the logical prefix.
	 */
	private collectDocumentIndexEntries(
		pages: readonly EncodedEventPage[],
	): Map<number, DocumentIndexEntry[]> {
		const entriesByShard = new Map<number, DocumentIndexEntry[]>();
		for (const page of pages) {
			for (let offset = 0; offset < page.count; offset++) {
				if (page.tombstones[offset] !== 0) continue;
				const hash = this.hashDocumentId(this.decodeDocumentId(page, offset));
				const shard = this.getDocumentIndexShard(hash.prefix);
				let entries = entriesByShard.get(shard);
				if (!entries) {
					entries = [];
					entriesByShard.set(shard, entries);
				}
				entries.push({
					prefix: hash.prefix,
					high: hash.high,
					low: hash.low,
					pageId: page.pageId,
					offset,
					handle: page.handles[offset],
					pageRevision: page.revision,
				});
			}
		}
		return entriesByShard;
	}

	/**
	 * Encodes sorted locator entries into one transferable binary segment.
	 * @param manifest Segment owner.
	 * @param shard Addressable prefix shard.
	 * @param segmentId Shard-local append sequence.
	 * @param level Leveled-compaction tier.
	 * @param entries Locator entries to encode.
	 * @returns Compact binary index segment.
	 */
	private encodeDocumentIndexSegment(
		manifest: StoredEventGenerationManifest,
		shard: number,
		segmentId: number,
		level: number,
		entries: readonly DocumentIndexEntry[],
	): EncodedDocumentIndexSegment {
		const sorted = [...entries].sort(
			(left, right) =>
				left.high - right.high ||
				left.low - right.low ||
				right.pageRevision - left.pageRevision ||
				right.pageId - left.pageId ||
				right.offset - left.offset,
		);
		const hashHighs = new Uint32Array(sorted.length);
		const hashLows = new Uint32Array(sorted.length);
		const pageIds = new Uint32Array(sorted.length);
		const offsets = new Uint32Array(sorted.length);
		const handles = new Uint32Array(sorted.length);
		const pageRevisions = new Uint32Array(sorted.length);
		for (let index = 0; index < sorted.length; index++) {
			const entry = sorted[index];
			hashHighs[index] = entry.high;
			hashLows[index] = entry.low;
			pageIds[index] = entry.pageId;
			offsets[index] = entry.offset;
			handles[index] = entry.handle;
			pageRevisions[index] = entry.pageRevision;
		}
		return {
			sourceId: manifest.sourceId,
			sourceOrdinal: manifest.sourceOrdinal,
			generation: manifest.generation,
			shard,
			segmentId,
			level,
			count: sorted.length,
			hashHighs,
			hashLows,
			pageIds,
			offsets,
			handles,
			pageRevisions,
			encodedByteLength:
				hashHighs.byteLength +
				hashLows.byteLength +
				pageIds.byteLength +
				offsets.byteLength +
				handles.byteLength +
				pageRevisions.byteLength,
		};
	}

	/**
	 * Expands one binary segment for synchronous threshold compaction.
	 * @param segment Segment being merged.
	 * @returns Plain locator entries.
	 */
	private decodeDocumentIndexEntries(
		segment: EncodedDocumentIndexSegment,
	): DocumentIndexEntry[] {
		return Array.from({ length: segment.count }, (_, index) => ({
			prefix:
				segment.hashHighs[index] >>>
				(32 - EVENT_DOCUMENT_HASH_PREFIX_BITS),
			high: segment.hashHighs[index],
			low: segment.hashLows[index],
			pageId: segment.pageIds[index],
			offset: segment.offsets[index],
			handle: segment.handles[index],
			pageRevision: segment.pageRevisions[index],
		}));
	}

	/**
	 * Commits index cleanup in small transactions so segment staging never creates a
	 * source-wide delete write-set or retains binary segment values in memory.
	 * @param operations Delete-only staged or obsolete segment operations.
	 * @returns Promise settled after every bounded cleanup transaction.
	 */
	private async commitIndexOperationsBounded(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		for (
			let offset = 0;
			offset < operations.length;
			offset += EVENT_INDEX_CLEANUP_OPERATION_LIMIT
		) {
			await this.backend.commit(
				operations.slice(offset, offset + EVENT_INDEX_CLEANUP_OPERATION_LIMIT),
			);
		}
	}

	/**
	 * Creates reusable lookup state without cloning source-wide binary indexes.
	 * @returns Active manifests for page-at-a-time index reads.
	 */
	private async createLookupContext(): Promise<DocumentLookupContext> {
		const manifests = await this.backend.getAll<StoredEventSourceManifest>("sources");
		return {
			manifests: new Map(manifests.map((manifest) => [manifest.sourceId, manifest])),
		};
	}

	/**
	 * Resolves one revision-scoped handle through compact catalog handle ranges.
	 * @param manifest Active source authority.
	 * @param handle Non-zero source-local handle.
	 * @returns Newest active matching location, or null.
	 */
	private async findHandleLocation(
		manifest: StoredEventSourceManifest,
		handle: number,
	): Promise<EventLocation | null> {
		if (!Number.isInteger(handle) || handle <= 0) return null;
		let best: EventLocation | null = null;
		for (const catalogEntry of manifest.pages) {
			if (
				handle < catalogEntry.minimumHandle ||
				handle > catalogEntry.maximumHandle
			) {
				continue;
			}
			const page = await this.loadPage(
				manifest.sourceId,
				manifest.generation,
				catalogEntry.pageId,
			);
			for (let offset = 0; offset < page.count; offset++) {
				if (page.tombstones[offset] !== 0 || page.handles[offset] !== handle) continue;
				const location: EventLocation = {
					sourceId: manifest.sourceId,
					generation: page.generation,
					pageId: page.pageId,
					offset,
					handle,
					timestamp: page.timestamps[offset],
					sequence: page.sequences[offset],
					revision: page.revision,
				};
				if (!best || this.isLocationNewer(location, best)) best = location;
			}
		}
		return best;
	}

	/**
	 * Finds and exact-ID-verifies the newest active locator matching one document.
	 * @param documentId Document identifier being resolved.
	 * @param sourceId Optional source discriminator.
	 * @param context Preloaded active manifests.
	 * @returns Verified locator or null.
	 */
	private async findDocumentLocation(
		documentId: Doc.Id,
		sourceId: Source.Id | undefined,
		context: DocumentLookupContext,
	): Promise<EventLocation | null> {
		const locations = await this.findDocumentLocations(
			[documentId],
			sourceId,
			context,
		);
		return locations.get(documentId) ?? null;
	}

	/**
	 * Resolves several exact IDs through only the addressed prefix shards.
	 * Candidate locators are grouped by page so exact pages are loaded one at a time.
	 * @param documentIds Requested canonical IDs.
	 * @param sourceId Optional source discriminator.
	 * @param context Active source manifests.
	 * @returns Newest verified location for each found ID.
	 */
	private async findDocumentLocations(
		documentIds: readonly Doc.Id[],
		sourceId: Source.Id | undefined,
		context: DocumentLookupContext,
	): Promise<Map<Doc.Id, EventLocation>> {
		const requestedIds = new Set(documentIds);
		const hashesByShard = new Map<number, Map<string, DocumentHash64>>();
		for (const documentId of requestedIds) {
			const hash = this.hashDocumentId(documentId);
			const shard = this.getDocumentIndexShard(hash.prefix);
			let hashes = hashesByShard.get(shard);
			if (!hashes) {
				hashes = new Map();
				hashesByShard.set(shard, hashes);
			}
			hashes.set(this.createHashKey(hash), hash);
		}

		const locations = new Map<Doc.Id, EventLocation>();
		for (const manifest of context.manifests.values()) {
			if (sourceId && manifest.sourceId !== sourceId) continue;
			const currentPageRevisions = new Map(
				manifest.pages.map((page) => [page.pageId, page.revision]),
			);
			for (const [shard, hashes] of hashesByShard) {
				const segments = await this.loadDocumentIndexShard(
					manifest.sourceId,
					manifest.generation,
					shard,
					manifest.documentIndexSegments,
				);
				const candidatesByPage = new Map<
					number,
					Array<{ segment: EncodedDocumentIndexSegment; index: number }>
				>();
				for (const segment of segments) {
					for (const hash of hashes.values()) {
						let index = this.findFirstHashIndex(segment, hash);
						while (
							index < segment.count &&
							segment.hashHighs[index] === hash.high &&
							segment.hashLows[index] === hash.low
						) {
							const pageId = segment.pageIds[index];
							if (
								currentPageRevisions.get(pageId) !==
								segment.pageRevisions[index]
							) {
								index++;
								continue;
							}
							let candidates = candidatesByPage.get(pageId);
							if (!candidates) {
								candidates = [];
								candidatesByPage.set(pageId, candidates);
							}
							candidates.push({ segment, index });
							index++;
						}
					}
				}

				for (const [pageId, candidates] of candidatesByPage) {
					const page = await this.loadPage(
						manifest.sourceId,
						manifest.generation,
						pageId,
					);
					for (const { segment, index } of candidates) {
						const offset = segment.offsets[index];
						if (offset >= page.count || page.tombstones[offset] !== 0) continue;
						const candidateId = this.decodeDocumentId(page, offset);
						if (!requestedIds.has(candidateId)) continue;
						const location: EventLocation = {
							sourceId: manifest.sourceId,
							generation: manifest.generation,
							pageId,
							offset,
							handle: page.handles[offset],
							timestamp: page.timestamps[offset],
							sequence: page.sequences[offset],
							revision: page.revision,
						};
						const current = locations.get(candidateId);
						if (!current || this.isLocationNewer(location, current)) {
							locations.set(candidateId, location);
						}
					}
				}
			}
		}
		return locations;
	}

	/**
	 * Returns the first sorted index entry not less than a 64-bit hash.
	 * @param segment Sorted binary locator segment.
	 * @param hash Target document hash.
	 * @returns Candidate entry offset.
	 */
	private findFirstHashIndex(
		segment: EncodedDocumentIndexSegment,
		hash: DocumentHash64,
	): number {
		let left = 0;
		let right = segment.count;
		while (left < right) {
			const middle = (left + right) >>> 1;
			const comparison =
				segment.hashHighs[middle] - hash.high ||
				segment.hashLows[middle] - hash.low;
			if (comparison < 0) left = middle + 1;
			else right = middle;
		}
		return left;
	}

	/**
	 * Decodes selected locations one exact page at a time while preserving caller order.
	 * @param locations Canonically sorted locations.
	 * @returns Active canonical documents in location order.
	 */
	private async decodeLocations(
		locations: readonly EventLocation[],
	): Promise<Doc.Type[]> {
		const documents = new Array<Doc.Type | undefined>(locations.length);
		const locationsByPage = new Map<
			string,
			Array<{ location: EventLocation; resultIndex: number }>
		>();
		locations.forEach((location, resultIndex) => {
			const pageKey = JSON.stringify([
				location.sourceId,
				location.generation,
				location.pageId,
			]);
			const pageLocations = locationsByPage.get(pageKey) ?? [];
			pageLocations.push({ location, resultIndex });
			locationsByPage.set(pageKey, pageLocations);
		});
		for (const pageLocations of locationsByPage.values()) {
			const first = pageLocations[0].location;
			const page = await this.loadPage(
				first.sourceId,
				first.generation,
				first.pageId,
			);
			for (const { location, resultIndex } of pageLocations) {
				documents[resultIndex] = decodeEventAt(page, location.offset);
			}
		}
		return documents.filter((document): document is Doc.Type => Boolean(document));
	}

	/**
	 * Decodes one location through a per-call exact page cache.
	 * @param location Page and offset to resolve.
	 * @param pageCache Cache bounded by the caller's result cardinality.
	 * @returns Active document, or null for a tombstone.
	 */
	private async decodeLocation(
		location: EventLocation,
		pageCache: Map<string, Promise<EncodedEventPage>>,
	): Promise<Doc.Type | null> {
		const page = await this.loadPageCached(
			location.sourceId,
			location.generation,
			location.pageId,
			pageCache,
		);
		return decodeEventAt(page, location.offset) ?? null;
	}

	/**
	 * Loads one exact page and fails loudly when committed metadata is inconsistent.
	 * @param sourceId Source owning the page.
	 * @param generation Source generation owning the page.
	 * @param pageId Page identifier.
	 * @returns Exact encoded page.
	 */
	private async loadPage(
		sourceId: Source.Id,
		generation: number,
		pageId: number,
	): Promise<EncodedEventPage> {
		const page = await this.backend.get<EncodedEventPage>(
			"pages",
			this.createPageKey(sourceId, generation, pageId),
		);
		if (!page) {
			throw new EventStoreCorruptionError(
				`Missing event page ${sourceId}/${generation}/${pageId}.`,
			);
		}
		return page;
	}

	/**
	 * Loads only one directly addressed document-hash shard.
	 * @param sourceId Source owning the shard.
	 * @param generation Source generation owning the shard.
	 * @param shard High-bit prefix shard.
	 * @param catalog Manifest-owned segment catalog used for exact multi-key reads.
	 * @returns At most the configured bounded number of binary segments.
	 */
	private async loadDocumentIndexShard(
		sourceId: Source.Id,
		generation: number,
		shard: number,
		catalog: readonly DocumentIndexSegmentCatalogEntry[],
	): Promise<EncodedDocumentIndexSegment[]> {
		const activeCatalog = catalog.filter((entry) => entry.shard === shard);
		if (activeCatalog.length > EVENT_DOCUMENT_INDEX_SEGMENT_LIMIT) {
			throw new EventStoreCorruptionError(
				`Document index shard ${sourceId}/${generation}/${shard} exceeds its bound.`,
			);
		}
		const segments = await this.backend.getMany<EncodedDocumentIndexSegment>(
			"document_index_pages",
			activeCatalog.map((entry) =>
				this.createDocumentIndexKey(
					sourceId,
					generation,
					shard,
					entry.segmentId,
				),
			),
		);
		if (segments.some((segment) => segment === undefined)) {
			throw new EventStoreCorruptionError(
				`Document index shard ${sourceId}/${generation}/${shard} is incomplete.`,
			);
		}
		return segments as EncodedDocumentIndexSegment[];
	}

	/**
	 * Loads one exact page once within a bounded lookup call.
	 * @param sourceId Source owning the page.
	 * @param generation Source generation owning the page.
	 * @param pageId Page identifier.
	 * @param cache Promise cache scoped to one reader method.
	 * @returns Exact encoded page.
	 */
	private loadPageCached(
		sourceId: Source.Id,
		generation: number,
		pageId: number,
		cache: Map<string, Promise<EncodedEventPage>>,
	): Promise<EncodedEventPage> {
		const cacheKey = `${sourceId}:${generation}:${pageId}`;
		let page = cache.get(cacheKey);
		if (!page) {
			page = this.loadPage(sourceId, generation, pageId);
			cache.set(cacheKey, page);
		}
		return page;
	}

	/**
	 * Decodes only one ID column slice from an exact page.
	 * @param page Encoded page containing the ID.
	 * @param offset Event offset.
	 * @returns Canonical branded document identifier.
	 */
	private decodeDocumentId(page: EncodedEventPage, offset: number): Doc.Id {
		return this.textDecoder.decode(
			page.idBytes.subarray(page.idOffsets[offset], page.idOffsets[offset + 1]),
		) as Doc.Id;
	}

	/**
	 * Creates an unambiguous map key for one unsigned 64-bit document hash.
	 * @param hash Document hash pair.
	 * @returns Stable lookup key.
	 */
	private createHashKey(hash: DocumentHash64): string {
		return `${hash.high}:${hash.low}`;
	}

	/**
	 * Maps the complete 12-bit prefix onto a compact eight-bit physical shard.
	 * Full prefix/hash ordering remains inside each binary segment, while reducing
	 * IndexedDB record churn to at most 256 touched records per append page.
	 * @param prefix Full configured document hash prefix.
	 * @returns High-bit shard number.
	 */
	private getDocumentIndexShard(prefix: number): number {
		return prefix >>> EVENT_DOCUMENT_INDEX_PHYSICAL_SHARD_SHIFT;
	}

	/**
	 * Inserts a location in canonical order and retains only a bounded prefix.
	 * @param locations Mutable sorted location list.
	 * @param location Candidate location.
	 * @param maximumLength Maximum retained prefix length.
	 * @returns Nothing.
	 */
	private insertBoundedLocation(
		locations: EventLocation[],
		location: EventLocation,
		maximumLength: number,
	): void {
		const insertionIndex = this.findLocationInsertionIndex(locations, location);
		if (insertionIndex >= maximumLength && locations.length >= maximumLength) return;
		locations.splice(insertionIndex, 0, location);
		if (locations.length > maximumLength) locations.pop();
	}

	/**
	 * Inserts a location into an unbounded canonical list.
	 * @param locations Mutable sorted location list.
	 * @param location Candidate location.
	 * @returns Nothing.
	 */
	private insertSortedLocation(
		locations: EventLocation[],
		location: EventLocation,
	): void {
		locations.splice(this.findLocationInsertionIndex(locations, location), 0, location);
	}

	/**
	 * Locates the canonical insertion index for a bounded location list.
	 * @param locations Existing sorted locations.
	 * @param location Candidate location.
	 * @returns Zero-based insertion index.
	 */
	private findLocationInsertionIndex(
		locations: readonly EventLocation[],
		location: EventLocation,
	): number {
		let left = 0;
		let right = locations.length;
		while (left < right) {
			const middle = (left + right) >>> 1;
			if (this.compareLocations(locations[middle], location) <= 0) {
				left = middle + 1;
			} else {
				right = middle;
			}
		}
		return left;
	}

	/**
	 * Compares locations by descending timestamp, descending sequence, and stable
	 * page/offset tie breakers.
	 * @param left First location.
	 * @param right Second location.
	 * @returns Negative when left precedes right.
	 */
	private compareLocations(left: EventLocation, right: EventLocation): number {
		return (
			compareEventOrderValues(
				left.timestamp,
				left.sequence,
				right.timestamp,
				right.sequence,
			) ||
			left.pageId - right.pageId ||
			left.offset - right.offset
		);
	}

	/**
	 * Orders page catalog entries by their first canonical event.
	 * @param left First page catalog entry.
	 * @param right Second page catalog entry.
	 * @returns Negative when the left page begins before the right page.
	 */
	private compareCatalogEntries(
		left: EventPageCatalogEntry,
		right: EventPageCatalogEntry,
	): number {
		return (
			compareEventOrderValues(
				left.maxTimestamp,
				left.firstSequence,
				right.maxTimestamp,
				right.firstSequence,
			) || left.pageId - right.pageId
		);
	}

	/**
	 * Compares one encoded offset with a canonical location without decoding its ID.
	 * @param page Page containing the candidate offset.
	 * @param offset Candidate page offset.
	 * @param location Reference canonical location.
	 * @returns Canonical order comparison.
	 */
	private comparePageOffsetToLocation(
		page: EncodedEventPage,
		offset: number,
		location: EventLocation,
	): number {
		return this.compareLocations(
			this.createEventLocation(page, offset),
			location,
		);
	}

	/**
	 * Finds the first offset that does not canonically precede a location.
	 * @param page Canonically sorted exact page.
	 * @param location Reference location.
	 * @returns First offset whose comparison is greater than or equal to zero.
	 */
	private findFirstPageOffsetNotPreceding(
		page: EncodedEventPage,
		location: EventLocation,
	): number {
		let left = 0;
		let right = page.count;
		while (left < right) {
			const middle = (left + right) >>> 1;
			if (this.comparePageOffsetToLocation(page, middle, location) < 0) {
				left = middle + 1;
			} else {
				right = middle;
			}
		}
		return left;
	}

	/**
	 * Finds the first offset that canonically follows a location.
	 * @param page Canonically sorted exact page.
	 * @param location Reference location.
	 * @returns First offset whose comparison is greater than zero.
	 */
	private findFirstPageOffsetFollowing(
		page: EncodedEventPage,
		location: EventLocation,
	): number {
		let left = 0;
		let right = page.count;
		while (left < right) {
			const middle = (left + right) >>> 1;
			if (this.comparePageOffsetToLocation(page, middle, location) <= 0) {
				left = middle + 1;
			} else {
				right = middle;
			}
		}
		return left;
	}

	/**
	 * Chooses the last-written locator for duplicate IDs or handles.
	 * @param candidate Candidate locator.
	 * @param current Current winning locator.
	 * @returns True when candidate should replace current.
	 */
	private isLocationNewer(candidate: EventLocation, current: EventLocation): boolean {
		return (
			candidate.revision > current.revision ||
			(candidate.revision === current.revision &&
				(candidate.pageId > current.pageId ||
					(candidate.pageId === current.pageId &&
						candidate.offset > current.offset)))
		);
	}

	/**
	 * Finds the first descending timestamp not greater than a maximum.
	 * @param timestamps Descending timestamp column.
	 * @param maximum Inclusive upper bound.
	 * @returns First candidate offset.
	 */
	private findFirstDescendingAtMost(
		timestamps: Float64Array,
		maximum: number,
	): number {
		let left = 0;
		let right = timestamps.length;
		while (left < right) {
			const middle = (left + right) >>> 1;
			if (timestamps[middle] > maximum) left = middle + 1;
			else right = middle;
		}
		return left;
	}

	/**
	 * Reads the active source manifest from persistent storage.
	 * @param sourceId Source identifier.
	 * @returns Stored active manifest, or undefined.
	 */
	private async getStoredManifest(
		sourceId: Source.Id,
	): Promise<StoredEventSourceManifest | undefined> {
		const manifest = await this.backend.get<StoredEventSourceManifest>(
			"sources",
			sourceId,
		);
		return manifest
			? { ...manifest, ...this.getHeadState(manifest) }
			: undefined;
	}

	/**
	 * Requires complete source authority before history navigation or pruning.
	 * @param sourceId Source expected to expose a completed generation.
	 * @returns Complete persisted source head.
	 */
	private async requireCompleteSourceManifest(
		sourceId: Source.Id,
	): Promise<StoredEventSourceManifest> {
		const manifest = await this.getStoredManifest(sourceId);
		if (!manifest || manifest.status !== "complete") {
			throw new StaleEventStoreGenerationError(
				`Source ${sourceId} does not have complete cache authority.`,
			);
		}
		return manifest;
	}

	/**
	 * Normalizes source-head fields so memory fixtures and new databases share defaults.
	 * @param manifest Optional persisted source or rollback state.
	 * @returns Complete logical cache-head state.
	 */
	private getHeadState(
		manifest:
			| (Partial<StoredEventSourceHeadState> & Partial<EventSourceManifest>)
			| undefined,
	): StoredEventSourceHeadState {
		const cacheHistory = [...(manifest?.cacheHistory ?? [])];
		const requestedCursor = manifest?.cacheHistoryCursor ?? cacheHistory.length - 1;
		const cacheHistoryCursor =
			cacheHistory.length === 0
				? -1
				: Math.max(0, Math.min(cacheHistory.length - 1, requestedCursor));
		return {
			authorityRevision:
				manifest?.authorityRevision ?? manifest?.revision ?? 0,
			activeCacheGeneration: manifest?.activeCacheGeneration ?? null,
			cacheHistory,
			cacheHistoryCursor,
			nextGeneration: Math.max(
				1,
				manifest?.nextGeneration ?? 0,
				(manifest?.generation ?? 0) + 1,
			),
			...(manifest?.pendingCacheCleanup?.length
				? { pendingCacheCleanup: [...manifest.pendingCacheCleanup] }
				: {}),
		};
	}

	/**
	 * Frees redo entries and one oldest non-active slot before a pooled replacement.
	 * @param sourceId Source about to receive a new query cache.
	 * @returns Promise settled after bounded reclamation of detached generations.
	 */
	private async prepareQueryCacheSlot(sourceId: Source.Id): Promise<void> {
		const current = await this.getStoredManifest(sourceId);
		if (!current || current.status !== "complete") return;
		const retained = current.cacheHistory.slice(
			0,
			current.cacheHistoryCursor + 1,
		);
		const detached = current.cacheHistory.slice(current.cacheHistoryCursor + 1);
		while (retained.length >= this.queryCacheLimit) {
			const removableIndex = retained.findIndex(
				(generation) => generation !== current.activeCacheGeneration,
			);
			if (removableIndex < 0) break;
			const [generation] = retained.splice(removableIndex, 1);
			detached.push(generation);
		}
		if (detached.length === 0) return;
		const activeCursor =
			current.activeCacheGeneration === null
				? -1
				: retained.indexOf(current.activeCacheGeneration);
		const pendingCacheCleanup = this.mergeObsoleteGenerationMarkers(
			current.pendingCacheCleanup,
			detached.map((generation) => ({ generation })),
		);
		await this.backend.commit([
			{
				type: "put",
				store: "sources",
				key: sourceId,
				value: {
					...current,
					cacheHistory: retained,
					cacheHistoryCursor: activeCursor,
					pendingCacheCleanup,
				} satisfies StoredEventSourceManifest,
			},
		]);
		await this.cleanupObsoleteGenerations(sourceId);
	}

	/**
	 * Activates one stored record and advances logical authority in one transaction.
	 * @param current Current complete source head or a reconstructed orphan head.
	 * @param generation Physical cache generation to activate.
	 * @param history Ordered history to persist with the activation.
	 * @param cursor Cursor selecting the target generation in history.
	 * @param cleanup Additional generations detached by startup reconciliation.
	 * @returns Activated cache and compact source summary.
	 */
	private async activateQueryCacheRecord(
		current: StoredEventSourceManifest,
		generation: number,
		history: readonly number[],
		cursor: number,
		cleanup: readonly EventObsoleteGenerationMarker[] = [],
	): Promise<EventSourceQueryCacheActivation> {
		const cacheRecord = await this.getQueryCacheRecord(
			current.sourceId,
			generation,
		);
		if (!cacheRecord) {
			throw new StaleEventStoreGenerationError(
				`Query cache ${current.sourceId}/${generation} is unavailable.`,
			);
		}
		if (cacheRecord.manifest.status !== "complete") {
			throw new EventStoreCorruptionError(
				`Query cache ${current.sourceId}/${generation} is not complete.`,
			);
		}
		if (
			(current.pendingCacheCleanup ?? []).some(
				(marker) => marker.generation === generation,
			)
		) {
			throw new StaleEventStoreGenerationError(
				`Query cache ${current.sourceId}/${generation} is pending cleanup.`,
			);
		}
		if (history[cursor] !== generation) {
			throw new Error("The query-cache history cursor does not select its target.");
		}
		const lastActivatedAt = this.now();
		const activatedRecord: EventSourceQueryCacheRecord = {
			...cacheRecord,
			lastActivatedAt,
		};
		const pendingCacheCleanup = this.mergeObsoleteGenerationMarkers(
			current.pendingCacheCleanup,
			cleanup,
		).filter((marker) => marker.generation !== generation);
		const activated: StoredEventSourceManifest = {
			...cacheRecord.manifest,
			revision: Math.max(current.revision, cacheRecord.manifest.revision) + 1,
			authorityRevision: current.authorityRevision + 1,
			status: "complete",
			activeCacheGeneration: generation,
			cacheHistory: [...history],
			cacheHistoryCursor: cursor,
			nextGeneration: Math.max(current.nextGeneration, generation + 1),
			...(pendingCacheCleanup.length > 0 ? { pendingCacheCleanup } : {}),
		};
		await this.backend.commit([
			{
				type: "put",
				store: "source_query_caches",
				key: this.createQueryCacheKey(current.sourceId, generation),
				value: activatedRecord,
			},
			{
				type: "put",
				store: "sources",
				key: current.sourceId,
				value: activated,
			},
		]);
		return {
			summary: this.toSummary(activated),
			cache: this.toQueryCacheMetadata(activatedRecord),
		};
	}

	/**
	 * Reconstructs minimal source authority when cache records outlive a missing head.
	 * @param cacheRecord Cache used to recreate the source head.
	 * @returns Complete source manifest suitable for one activation transaction.
	 */
	private createSourceHeadFromCache(
		cacheRecord: EventSourceQueryCacheRecord,
	): StoredEventSourceManifest {
		return {
			...cacheRecord.manifest,
			authorityRevision: cacheRecord.manifest.revision,
			activeCacheGeneration: cacheRecord.generation,
			cacheHistory: [cacheRecord.generation],
			cacheHistoryCursor: 0,
			nextGeneration: cacheRecord.generation + 1,
		};
	}

	/**
	 * Reads every compact cache record for one source by compound-key prefix.
	 * @param sourceId Source whose records are requested.
	 * @returns Structured-cloned records without reading exact page stores.
	 */
	private getQueryCacheRecords(
		sourceId: Source.Id,
	): Promise<EventSourceQueryCacheRecord[]> {
		return this.backend.getAllByPrefix<EventSourceQueryCacheRecord>(
			"source_query_caches",
			[sourceId],
		);
	}

	/**
	 * Reads one query-cache record by its physical generation.
	 * @param sourceId Source owning the cache.
	 * @param generation Physical generation key.
	 * @returns Matching cache record, or undefined.
	 */
	private getQueryCacheRecord(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceQueryCacheRecord | undefined> {
		return this.backend.get<EventSourceQueryCacheRecord>(
			"source_query_caches",
			this.createQueryCacheKey(sourceId, generation),
		);
	}

	/**
	 * Projects a persisted record into the page-free public cache metadata contract.
	 * @param record Complete compact cache record.
	 * @returns Metadata suitable for main-thread cache selection and query restoration.
	 */
	private toQueryCacheMetadata(
		record: EventSourceQueryCacheRecord,
	): EventSourceQueryCacheMetadata {
		return {
			sourceId: record.sourceId,
			generation: record.generation,
			provenance: record.provenance,
			state: record.state,
			createdAt: record.createdAt,
			lastActivatedAt: record.lastActivatedAt,
			eventCount: record.manifest.eventCount,
			minTimestamp: record.manifest.minTimestamp,
			maxTimestamp: record.manifest.maxTimestamp,
		};
	}

	/**
	 * Requires one exact building generation for append/finalization work.
	 * @param sourceId Source identifier.
	 * @param generation Expected active generation.
	 * @returns Matching building manifest.
	 */
	private async requireBuildingManifest(
		sourceId: Source.Id,
		generation: number,
	): Promise<StoredEventSourceManifest> {
		const manifest = await this.getStoredManifest(sourceId);
		if (
			!manifest ||
			manifest.generation !== generation ||
			manifest.status !== "building"
		) {
			throw new StaleEventStoreGenerationError(
				`Source ${sourceId} generation ${generation} is not building.`,
			);
		}
		return manifest;
	}

	/**
	 * Allocates the next non-zero source ordinal through a tiny repository-local queue.
	 * QueryWorker may open different sources concurrently even though it is the sole writer.
	 * @returns Compact source ordinal.
	 */
	private async allocateSourceOrdinal(): Promise<number> {
		let allocated = 0;
		const allocation = this.sourceOrdinalAllocation.then(async () => {
			if (this.nextSourceOrdinal === null) {
				const manifests = await this.backend.getAll<StoredEventSourceManifest>(
					"sources",
				);
				let maximum = 0;
				for (const manifest of manifests) {
					maximum = Math.max(maximum, manifest.sourceOrdinal);
				}
				this.nextSourceOrdinal = maximum + 1;
			}
			allocated = this.nextSourceOrdinal;
			this.nextSourceOrdinal++;
		});
		this.sourceOrdinalAllocation = allocation.catch(() => undefined);
		await allocation;
		return allocated;
	}

	/**
	 * Validates one generation identifier.
	 * @param generation Generation to validate.
	 * @returns Nothing.
	 */
	private assertGeneration(generation: number): void {
		if (!Number.isSafeInteger(generation) || generation < 0) {
			throw new RangeError("EventStore generation must be a non-negative integer.");
		}
	}

	/**
	 * Normalizes an injectable history cap while preserving at least one active cache.
	 * @param limit Requested per-source completed-cache limit.
	 * @returns Positive safe-integer cache limit.
	 */
	private normalizeQueryCacheLimit(limit: number): number {
		if (!Number.isSafeInteger(limit) || limit <= 0) {
			throw new RangeError("EventStore query-cache limit must be a positive integer.");
		}
		return limit;
	}

	/**
	 * Validates a non-zero uint32 source ordinal.
	 * @param sourceOrdinal Source ordinal to validate.
	 * @returns Nothing.
	 */
	private assertSourceOrdinal(sourceOrdinal: number): void {
		if (
			!Number.isInteger(sourceOrdinal) ||
			sourceOrdinal <= 0 ||
			sourceOrdinal > 0xffff_ffff
		) {
			throw new RangeError("EventStore source ordinal must be a non-zero uint32.");
		}
	}

	/**
	 * Validates the minimal fields persisted by an upsert mutation.
	 * @param document Canonical document supplied by QueryWorker.
	 * @returns Nothing.
	 */
	private assertUpsertDocument(document: Doc.Type): void {
		if (!document._id || !document["gulp.source_id"]) {
			throw new TypeError("EventStore upsert requires document and source IDs.");
		}
		if (
			!Number.isFinite(document.gulp_timestamp) ||
			!Number.isFinite(document.number_hash) ||
			!Number.isFinite(document["gulp.event_code"]) ||
			(document["event.sequence"] !== undefined &&
				!Number.isFinite(document["event.sequence"]))
		) {
			throw new TypeError("EventStore upsert numeric fields must be finite.");
		}
	}

	/**
	 * Copies every page column before applying in-place logical mutations.
	 * @param page Backend-owned page clone.
	 * @returns Independent mutable page image.
	 */
	private cloneEncodedPage(page: EncodedEventPage): EncodedEventPage {
		return {
			...page,
			handles: page.handles.slice(),
			timestamps: page.timestamps.slice(),
			sequences: page.sequences.slice(),
			hashes: page.hashes.slice(),
			eventCodes: page.eventCodes.slice(),
			idOffsets: page.idOffsets.slice(),
			idBytes: page.idBytes.slice(),
			tombstones: page.tombstones.slice(),
		};
	}

	/**
	 * Recomputes page metadata and compacts when tombstones exceed 25 percent.
	 * @param page Mutated exact page.
	 * @returns Final exact-size page image.
	 */
	private finalizeMutatedPage(page: EncodedEventPage): EncodedEventPage {
		const tombstoneCount = page.count - page.activeCount;
		if (page.count > 0 && tombstoneCount * 4 > page.count) {
			const documents: Doc.Type[] = [];
			const handles: number[] = [];
			for (let offset = 0; offset < page.count; offset++) {
				const document = decodeEventAt(page, offset);
				if (!document) continue;
				documents.push(document);
				handles.push(page.handles[offset]);
			}
			return encodeEventPage({
				sourceId: page.sourceId,
				generation: page.generation,
				pageId: page.pageId,
				revision: page.revision,
				documents,
				handles: Uint32Array.from(handles),
			});
		}

		let minTimestamp = Number.POSITIVE_INFINITY;
		let maxTimestamp = Number.NEGATIVE_INFINITY;
		let hashMin = Number.POSITIVE_INFINITY;
		let hashMax = Number.NEGATIVE_INFINITY;
		let activeCount = 0;
		for (let offset = 0; offset < page.count; offset++) {
			if (page.tombstones[offset] !== 0) continue;
			activeCount++;
			minTimestamp = Math.min(minTimestamp, page.timestamps[offset]);
			maxTimestamp = Math.max(maxTimestamp, page.timestamps[offset]);
			hashMin = Math.min(hashMin, page.hashes[offset]);
			hashMax = Math.max(hashMax, page.hashes[offset]);
		}
		page.activeCount = activeCount;
		page.minTimestamp = activeCount === 0 ? 0 : minTimestamp;
		page.maxTimestamp = activeCount === 0 ? 0 : maxTimestamp;
		page.hashMin = activeCount === 0 ? 0 : hashMin;
		page.hashMax = activeCount === 0 ? 0 : hashMax;
		page.encodedByteLength = getEncodedEventPageByteLength(page);
		return page;
	}

	/**
	 * Recomputes exact source totals and ranges from its compact page catalog.
	 * @param manifest Source manifest with updated page entries.
	 * @returns Manifest with consistent dataset metadata.
	 */
	private recalculateManifest(
		manifest: StoredEventSourceManifest,
	): StoredEventSourceManifest {
		let eventCount = 0;
		let minTimestamp = Number.POSITIVE_INFINITY;
		let maxTimestamp = Number.NEGATIVE_INFINITY;
		let hashMin = Number.POSITIVE_INFINITY;
		let hashMax = Number.NEGATIVE_INFINITY;
		for (const page of manifest.pages) {
			eventCount += page.activeCount;
			if (page.activeCount === 0) continue;
			minTimestamp = Math.min(minTimestamp, page.minTimestamp);
			maxTimestamp = Math.max(maxTimestamp, page.maxTimestamp);
			hashMin = Math.min(hashMin, page.hashMin);
			hashMax = Math.max(hashMax, page.hashMax);
		}
		return {
			...manifest,
			eventCount,
			minTimestamp: eventCount === 0 ? 0 : minTimestamp,
			maxTimestamp: eventCount === 0 ? 0 : maxTimestamp,
			hashMin: eventCount === 0 ? 0 : hashMin,
			hashMax: eventCount === 0 ? 0 : hashMax,
		};
	}

	/**
	 * Validates encoded page identity, column lengths, ordering, and handles.
	 * @param page Page received from the QueryWorker codec.
	 * @param sourceId Expected source.
	 * @param generation Expected generation.
	 * @returns Nothing.
	 */
	private assertEncodedPage(
		page: EncodedEventPage,
		sourceId: Source.Id,
		generation: number,
	): void {
		if (page.sourceId !== sourceId || page.generation !== generation) {
			throw new StaleEventStoreGenerationError(
				"Encoded page does not belong to the active source generation.",
			);
		}
		if (!Number.isSafeInteger(page.pageId) || page.pageId < 0) {
			throw new RangeError("Event page ID must be a non-negative integer.");
		}
		if (
			page.handles.length !== page.count ||
			page.timestamps.length !== page.count ||
			page.sequences.length !== page.count ||
			page.hashes.length !== page.count ||
			page.eventCodes.length !== page.count ||
			page.idOffsets.length !== page.count + 1 ||
			page.tombstones.length !== page.count ||
			page.idOffsets[page.count] !== page.idBytes.length
		) {
			throw new EventStoreCorruptionError("Encoded event page columns are inconsistent.");
		}
		let activeCount = 0;
		for (let index = 0; index < page.count; index++) {
			if (!Number.isFinite(page.sequences[index])) {
				throw new EventStoreCorruptionError(
					"Encoded event page contains a non-finite event sequence.",
				);
			}
			if (
				index > 0 &&
				compareEventOrderValues(
					page.timestamps[index - 1],
					page.sequences[index - 1],
					page.timestamps[index],
					page.sequences[index],
				) > 0
			) {
				throw new EventStoreCorruptionError(
					"Encoded event page order is not timestamp-descending and sequence-descending.",
				);
			}
			if (page.idOffsets[index] > page.idOffsets[index + 1]) {
				throw new EventStoreCorruptionError("Encoded event ID offsets are invalid.");
			}
			if (page.tombstones[index] === 0) {
				activeCount++;
				const handle = page.handles[index];
				if (handle === 0 || handle > EVENT_MAXIMUM_SOURCE_HANDLE) {
					throw new RangeError("Active event handle exceeds the uint32 contract.");
				}
			}
		}
		if (activeCount !== page.activeCount) {
			throw new EventStoreCorruptionError("Encoded event active count is invalid.");
		}
	}

	/**
	 * Converts one stored manifest into a rollback-safe generation snapshot.
	 * @param manifest Stored manifest with optional rollback data.
	 * @returns Generation manifest including its compact index catalog.
	 */
	private toGenerationManifest(
		manifest: StoredEventSourceManifest,
	): StoredEventGenerationManifest {
		const {
			rollback: _rollback,
			inPlaceReplacement: _inPlaceReplacement,
			obsoleteGenerations: _obsoleteGenerations,
			authorityRevision: _authorityRevision,
			activeCacheGeneration: _activeCacheGeneration,
			cacheHistory: _cacheHistory,
			cacheHistoryCursor: _cacheHistoryCursor,
			nextGeneration: _nextGeneration,
			pendingCacheCleanup: _pendingCacheCleanup,
			pendingQueryCache: _pendingQueryCache,
			...generationManifest
		} = manifest;
		return generationManifest;
	}

	/**
	 * Captures physical generation and cache-head navigation state for cancellation.
	 * @param manifest Complete source authority about to be replaced.
	 * @returns Rollback snapshot without transient building metadata.
	 */
	private toRollbackManifest(
		manifest: StoredEventSourceManifest,
	): StoredEventSourceRollback {
		const {
			rollback: _rollback,
			inPlaceReplacement: _inPlaceReplacement,
			pendingQueryCache: _pendingQueryCache,
			obsoleteGenerations: _obsoleteGenerations,
			...rollbackManifest
		} = manifest;
		return rollbackManifest;
	}

	/**
	 * Projects source authority into the cross-realm summary contract.
	 * @param manifest Source manifest to summarize.
	 * @returns Serializable compact source summary.
	 */
	private toSummary(manifest: StoredEventSourceManifest): EventSourceSummary {
		return {
			sourceId: manifest.sourceId,
			encodedBytes: manifest.encodedBytes,
			generation: manifest.generation,
			revision: manifest.revision,
			authorityRevision: manifest.authorityRevision,
			status: manifest.status,
			eventCount: manifest.eventCount,
			minTimestamp: manifest.minTimestamp,
			maxTimestamp: manifest.maxTimestamp,
			hashMin: manifest.hashMin,
			hashMax: manifest.hashMax,
			activeCacheGeneration: manifest.activeCacheGeneration,
			cacheHistory: [...manifest.cacheHistory],
			cacheHistoryCursor: manifest.cacheHistoryCursor,
			nextGeneration: manifest.nextGeneration,
		};
	}

	/**
	 * Creates the compound key for one immutable source query cache.
	 * @param sourceId Source owning the query result.
	 * @param generation Physical result generation.
	 * @returns Compound IndexedDB primary key.
	 */
	private createQueryCacheKey(
		sourceId: Source.Id,
		generation: number,
	): IDBValidKey {
		return [sourceId, generation];
	}

	/**
	 * Creates primary keys for exact event pages.
	 * @param sourceId Source owning the page.
	 * @param generation Source generation.
	 * @param pageId Page identifier.
	 * @returns Compound IndexedDB primary key.
	 */
	private createPageKey(
		sourceId: Source.Id,
		generation: number,
		pageId: number,
	): IDBValidKey {
		return [sourceId, generation, pageId];
	}

	/**
	 * Creates a discriminated key for one page-local render summary.
	 * @param sourceId Source owning the summary.
	 * @param generation Source generation.
	 * @param pageId Exact page identifier.
	 * @param kind Fixed micro-LOD or height-count payload kind.
	 * @returns Compound IndexedDB primary key in the shared `page_lod` store.
	 */
	private createPageSummaryKey(
		sourceId: Source.Id,
		generation: number,
		pageId: number,
		kind:
			| typeof EVENT_PAGE_LOD_RECORD_KIND
			| typeof EVENT_PAGE_HEIGHT_RECORD_KIND,
	): IDBValidKey {
		return [sourceId, generation, pageId, kind];
	}

	/**
	 * Creates one directly addressable document-index segment key.
	 * @param sourceId Source owning the segment.
	 * @param generation Source generation.
	 * @param shard Prefix shard.
	 * @param segmentId Shard-local segment sequence.
	 * @returns Compound IndexedDB primary key.
	 */
	private createDocumentIndexKey(
		sourceId: Source.Id,
		generation: number,
		shard: number,
		segmentId: number,
	): IDBValidKey {
		return [sourceId, generation, shard, segmentId];
	}

	/**
	 * Creates deletion operations for every page owned by one manifest generation.
	 * @param manifest Generation whose physical records are removed.
	 * @returns Prepared multi-store deletes.
	 */
	private createGenerationDeleteOperations(
		manifest: StoredEventGenerationManifest,
	): EventStoreWriteOperation[] {
		const operations: EventStoreWriteOperation[] = [];
		for (const page of manifest.pages) {
			const key = this.createPageKey(
				manifest.sourceId,
				manifest.generation,
				page.pageId,
			);
			operations.push(
				{ type: "delete", store: "pages", key },
				{
					type: "delete",
					store: "page_lod",
					key: this.createPageSummaryKey(
						manifest.sourceId,
						manifest.generation,
						page.pageId,
						EVENT_PAGE_LOD_RECORD_KIND,
					),
				},
				{
					type: "delete",
					store: "page_lod",
					key: this.createPageSummaryKey(
						manifest.sourceId,
						manifest.generation,
						page.pageId,
						EVENT_PAGE_HEIGHT_RECORD_KIND,
					),
				},
			);
		}
		for (const segment of manifest.documentIndexSegments) {
			operations.push({
				type: "delete",
				store: "document_index_pages",
				key: this.createDocumentIndexKey(
					manifest.sourceId,
					manifest.generation,
					segment.shard,
					segment.segmentId,
				),
			});
		}
		return operations;
	}

	/**
	 * Captures only the physical addresses required to reclaim one generation later.
	 * @param manifest Generation being detached from source authority.
	 * @returns Compact idempotent cleanup marker stored with the active manifest.
	 */
	private createObsoleteGenerationMarker(
		manifest: StoredEventGenerationManifest,
	): EventObsoleteGenerationMarker {
		return { generation: manifest.generation };
	}

	/**
	 * Merges cleanup markers by generation while retaining deterministic order.
	 * @param existing Markers already persisted by an earlier interrupted cleanup.
	 * @param additions Newly detached generations.
	 * @returns Stable unique cleanup marker list.
	 */
	private mergeObsoleteGenerationMarkers(
		existing: readonly EventObsoleteGenerationMarker[] | undefined,
		additions: readonly EventObsoleteGenerationMarker[],
	): EventObsoleteGenerationMarker[] {
		const byGeneration = new Map<number, EventObsoleteGenerationMarker>();
		for (const marker of [...(existing ?? []), ...additions]) {
			byGeneration.set(marker.generation, marker);
		}
		return [...byGeneration.values()].sort(
			(left, right) => left.generation - right.generation,
		);
	}

	/**
	 * Deletes one non-authoritative generation through bounded idempotent write sets.
	 * @param manifest Detached generation whose pages, summaries, and indexes are removed.
	 * @returns Promise settled after every cleanup batch commits.
	 */
	private async commitGenerationDeletesBounded(
		manifest: StoredEventGenerationManifest,
	): Promise<void> {
		const operations = this.createGenerationDeleteOperations(manifest);
		await this.commitDeleteOperationsBounded(operations);
	}

	/**
	 * Deletes one persisted cleanup marker through bounded idempotent write sets.
	 * @param sourceId Source owning the obsolete generation.
	 * @param marker Persisted generation cleanup marker.
	 * @returns Promise settled after every cleanup batch commits.
	 */
	private async commitObsoleteGenerationDeletesBounded(
		sourceId: Source.Id,
		marker: EventObsoleteGenerationMarker,
	): Promise<void> {
		while (true) {
			const pages = await this.backend.getAllByPrefix<EncodedEventPage>(
				"pages",
				[sourceId, marker.generation],
				EVENT_GENERATION_CLEANUP_OPERATION_LIMIT,
			);
			if (pages.length === 0) break;
			await this.backend.commit(
				pages.map((page) => ({
					type: "delete" as const,
					store: "pages" as const,
					key: this.createPageKey(sourceId, marker.generation, page.pageId),
				})),
			);
		}
		while (true) {
			const summaries = await this.backend.getAllByPrefix<
				EncodedEventPageLod | EncodedEventPageHeight
			>(
				"page_lod",
				[sourceId, marker.generation],
				EVENT_GENERATION_CLEANUP_OPERATION_LIMIT,
			);
			if (summaries.length === 0) break;
			await this.backend.commit(
				summaries.map((summary) => ({
					type: "delete" as const,
					store: "page_lod" as const,
					key: this.createPageSummaryKey(
						sourceId,
						marker.generation,
						summary.pageId,
						"blockCount" in summary
							? EVENT_PAGE_LOD_RECORD_KIND
							: EVENT_PAGE_HEIGHT_RECORD_KIND,
					),
				})),
			);
		}
		while (true) {
			const segments = await this.backend.getAllByPrefix<
				EncodedDocumentIndexSegment
			>(
				"document_index_pages",
				[sourceId, marker.generation],
				EVENT_GENERATION_CLEANUP_OPERATION_LIMIT,
			);
			if (segments.length === 0) break;
			await this.backend.commit(
				segments.map((segment) => ({
					type: "delete" as const,
					store: "document_index_pages" as const,
					key: this.createDocumentIndexKey(
						sourceId,
						marker.generation,
						segment.shard,
						segment.segmentId,
					),
				})),
			);
		}
	}

	/**
	 * Commits one prepared delete list in fixed-size transactions.
	 * @param operations Idempotent physical deletes.
	 * @returns Promise settled after every bounded transaction commits.
	 */
	private async commitDeleteOperationsBounded(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		for (
			let offset = 0;
			offset < operations.length;
			offset += EVENT_GENERATION_CLEANUP_OPERATION_LIMIT
		) {
			await this.backend.commit(
				operations.slice(
					offset,
					offset + EVENT_GENERATION_CLEANUP_OPERATION_LIMIT,
				),
			);
		}
	}
}
