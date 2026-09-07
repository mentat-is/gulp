import type { Source } from "@/entities/Source";
import {
	RENDER_GRAPH_BUCKET_PIXEL_WIDTH,
	type RenderEngineKind,
	type RenderTimeRange,
	type RenderWindowProjection,
	type RenderWindowSourceProjection,
} from "@/render-worker/RenderWorker.types";
import {
	buildEventPageHeight,
	buildEventPageLod,
	createEventPageCatalogEntry,
} from "./EventPage.codec";
import type {
	EventStoreBackend,
	EventStoreWriteOperation,
} from "./EventStore.backend";
import {
	EVENT_RENDER_PROJECTION_LIMIT,
	EVENT_PAGE_HEIGHT_RECORD_KIND,
	EVENT_PAGE_LOD_RECORD_KIND,
	type EncodedEventPage,
	type EncodedEventPageHeight,
	type EncodedEventPageLod,
	type EventPageCatalogEntry,
	type EventSourceHeadManifest,
	type EventSourceManifest,
} from "./EventStore.types";

const DEFAULT_MAXIMUM_REPRESENTATIVES = EVENT_RENDER_PROJECTION_LIMIT;
export const EVENT_HEIGHT_MAXIMUM_WORKING_BUCKET_LIMIT = 65_536;
const PROJECTION_GUARD_CHECK_MASK = 0xff;
const EXACT_PAGE_CACHE_BYTE_LIMIT = 16 * 1024 * 1024;
const LOD_PAGE_CACHE_BYTE_LIMIT = 4 * 1024 * 1024;
const HEIGHT_PAGE_CACHE_BYTE_LIMIT = 4 * 1024 * 1024;
const TRANSIENT_PAGE_RECORD = Symbol("EventStoreViewportTransientPage");

/** Reports whether the viewport request owning projection work is still current. */
export type EventStoreProjectionGuard = () => boolean;

export interface EventStoreViewportSourceDescriptor {
	sourceId: Source.Id;
	offset: number;
	engine: RenderEngineKind;
	frequency: number;
	overrideHashes: readonly number[] | Float64Array;
	settingsRevision: number;
	overrideRevision: number;
}

export interface EventStoreViewportDescriptor {
	operationEpoch: number;
	viewportRevision: number;
	projectionRevision: number;
	frame: RenderTimeRange;
	width: number;
	scale: number;
	scrollX: number;
	horizontalOverscanPixels: number;
	sources: readonly EventStoreViewportSourceDescriptor[];
	maxRepresentatives?: number;
	evictedSourceIds?: readonly Source.Id[];
}

/** Worker-local partial page whose durable base manifest has not changed yet. */
export interface EventStoreViewportTransientPage {
	baseGeneration: number;
	baseRevision: number;
	page: EncodedEventPage;
}

export interface EventStoreOverridePreparationDescriptor {
	sourceId: Source.Id;
	sourceRevision: number;
	overrideRevision: number;
	overrideHashes: readonly number[] | Float64Array;
	startPageIndex?: number;
	maximumPages?: number;
}

export interface EventStoreOverridePreparationResult {
	sourceId: Source.Id;
	sourceRevision: number;
	overrideRevision: number;
	processedPages: number;
	nextPageIndex: number;
	complete: boolean;
}

/** Raised until every visible page has persisted the requested override revision. */
export class EventStoreOverrideProjectionPendingError extends Error {
	/** Creates a typed pending-override error. */
	public constructor(message: string) {
		super(message);
		this.name = "EventStoreOverrideProjectionPendingError";
	}
}

/** Raised when persistence changes while a revision-fenced projection is being built. */
export class StaleEventStoreProjectionError extends Error {
	/** Creates a typed stale-projection error. */
	public constructor(message: string) {
		super(message);
		this.name = "StaleEventStoreProjectionError";
	}
}

interface ProjectionTransform {
	frame: RenderTimeRange;
	virtualWidth: number;
	scrollX: number;
	firstPixel: number;
	lastPixel: number;
	bucketPixelWidth: number;
	offset: number;
}

interface RepresentativeCandidate {
	bucket: number;
	handle: number;
	timestamp: number;
	hash: number;
	isOverride: boolean;
}

interface AggregateProjection {
	timestamps: Float64Array<ArrayBuffer>;
	counts: Float64Array<ArrayBuffer>;
}

interface HeightAggregateProjection extends AggregateProjection {
	maximumCount: number;
}

interface CachedHeightMaximum {
	generation: number;
	revision: number;
	minimumTimestamp: number;
	maximumTimestamp: number;
	maximumCount: number;
	minimumBucketTimestamp: number;
	minimumBucketCount: number;
	maximumBucketTimestamp: number;
	maximumBucketCount: number;
	pageRevisions: ReadonlyMap<number, number>;
}

interface CachedFrameEventCount {
	key: string;
	count: number;
}

interface HeightMaximumStatistics {
	maximumCount: number;
	minimumBucketTimestamp: number;
	minimumBucketCount: number;
	maximumBucketTimestamp: number;
	maximumBucketCount: number;
}

interface ProjectionPageCacheEntry<TValue> {
	sourceId: Source.Id;
	encodedBytes: number;
	value: TValue;
}

interface TransientProjectionPageRecord {
	exact: EncodedEventPage;
	lod: EncodedEventPageLod;
	height: EncodedEventPageHeight;
}

interface ProjectionPageCatalogEntry extends EventPageCatalogEntry {
	[TRANSIENT_PAGE_RECORD]?: TransientProjectionPageRecord;
}

/**
 * Retains immutable projection pages in access order under an explicit byte cap.
 * Values are never exposed outside QueryWorker, so cache hits avoid another
 * IndexedDB structured clone without changing transfer ownership.
 */
class BoundedProjectionPageCache<TValue> {
	private readonly entries = new Map<string, ProjectionPageCacheEntry<TValue>>();
	private encodedBytes = 0;

	/**
	 * Creates one least-recently-used cache with a strict encoded-byte ceiling.
	 * @param byteLimit Maximum retained encoded bytes.
	 */
	public constructor(private readonly byteLimit: number) {}

	/**
	 * Returns one cached value and promotes it to most-recently-used.
	 * @param key Revision-qualified page identity.
	 * @returns Cached immutable value, or undefined after a miss.
	 */
	public get(key: string): TValue | undefined {
		const entry = this.entries.get(key);
		if (!entry) return undefined;
		this.entries.delete(key);
		this.entries.set(key, entry);
		return entry.value;
	}

	/**
	 * Stores one immutable value unless it cannot fit inside the cache by itself.
	 * @param key Revision-qualified page identity.
	 * @param sourceId Source owning the page.
	 * @param encodedBytes Encoded payload size used for capacity accounting.
	 * @param value Immutable page value.
	 * @returns Nothing.
	 */
	public set(
		key: string,
		sourceId: Source.Id,
		encodedBytes: number,
		value: TValue,
	): void {
		if (!Number.isFinite(encodedBytes) || encodedBytes <= 0) return;
		const size = Math.max(1, Math.floor(encodedBytes));
		const existing = this.entries.get(key);
		if (existing) {
			this.encodedBytes -= existing.encodedBytes;
			this.entries.delete(key);
		}
		if (size > this.byteLimit) return;
		while (this.encodedBytes + size > this.byteLimit) {
			const oldest = this.entries.entries().next().value as
				| [string, ProjectionPageCacheEntry<TValue>]
				| undefined;
			if (!oldest) break;
			this.entries.delete(oldest[0]);
			this.encodedBytes -= oldest[1].encodedBytes;
		}
		this.entries.set(key, { sourceId, encodedBytes: size, value });
		this.encodedBytes += size;
	}

	/**
	 * Releases entries for a source that left the vertically retained viewport.
	 * @param sourceId Source whose pages should be evicted.
	 * @returns Nothing.
	 */
	public deleteSource(sourceId: Source.Id): void {
		for (const [key, entry] of this.entries) {
			if (entry.sourceId !== sourceId) continue;
			this.entries.delete(key);
			this.encodedBytes -= entry.encodedBytes;
		}
	}

	/**
	 * Returns the current bounded encoded working-set size.
	 * @returns Retained encoded bytes.
	 */
	public get sizeBytes(): number {
		return this.encodedBytes;
	}
}

export interface EventStoreViewportProjectorDiagnostics {
	maximumHeightWorkingBuckets: number;
	exactPageCacheBytes: number;
	lodPageCacheBytes: number;
	heightPageCacheBytes: number;
}

/**
 * Produces viewport-resolution render replacements directly from persisted exact and
 * LOD pages. It keeps only bounded per-source aggregate caches, never document objects.
 */
export class EventStoreViewportProjector {
	private readonly backend: EventStoreBackend;
	private readonly heightMaximumBySource = new Map<string, CachedHeightMaximum>();
	private readonly frameEventCountBySource = new Map<
		string,
		CachedFrameEventCount
	>();
	private readonly exactPageCache = new BoundedProjectionPageCache<EncodedEventPage>(
		EXACT_PAGE_CACHE_BYTE_LIMIT,
	);
	private readonly lodPageCache =
		new BoundedProjectionPageCache<EncodedEventPageLod>(LOD_PAGE_CACHE_BYTE_LIMIT);
	private readonly heightPageCache =
		new BoundedProjectionPageCache<EncodedEventPageHeight>(
			HEIGHT_PAGE_CACHE_BYTE_LIMIT,
		);
	private maximumHeightWorkingBuckets = 0;

	/**
	 * Creates a projector with bounded aggregate caches over one operation-scoped backend.
	 * @param backend Persistence backend shared with EventStoreRepository.
	 */
	public constructor(backend: EventStoreBackend) {
		this.backend = backend;
	}

	/**
	 * Returns bounded working-set telemetry for capacity and overlap regression tests.
	 * @returns Height sweep high-water mark and current page-cache byte usage.
	 */
	public getDiagnostics(): EventStoreViewportProjectorDiagnostics {
		return {
			maximumHeightWorkingBuckets: this.maximumHeightWorkingBuckets,
			exactPageCacheBytes: this.exactPageCache.sizeBytes,
			lodPageCacheBytes: this.lodPageCache.sizeBytes,
			heightPageCacheBytes: this.heightPageCache.sizeBytes,
		};
	}

	/**
	 * Builds one atomic multi-source projection with a global representative cap.
	 * @param descriptor Viewport transform, visible sources, and revision authority.
	 * @param isCurrent Optional guard that rejects work superseded by a newer viewport.
	 * @param transientPages Optional bounded replacement-builder pages not yet durable.
	 * @returns Exact-size typed arrays ready for zero-copy renderer adoption.
	 */
	public async requestRenderWindow(
		descriptor: EventStoreViewportDescriptor,
		isCurrent: EventStoreProjectionGuard = () => true,
		transientPages: ReadonlyMap<Source.Id, EventStoreViewportTransientPage> =
			new Map(),
	): Promise<RenderWindowProjection> {
		this.assertDescriptor(descriptor);
		this.assertProjectionCurrent(isCurrent);
		for (const sourceId of descriptor.evictedSourceIds ?? []) {
			this.heightMaximumBySource.delete(sourceId);
			this.frameEventCountBySource.delete(sourceId);
			this.exactPageCache.deleteSource(sourceId);
			this.lodPageCache.deleteSource(sourceId);
			this.heightPageCache.deleteSource(sourceId);
		}
		const bucketPixelWidth = this.calculateBucketPixelWidth(descriptor);
		const manifests = await this.backend.getMany<EventSourceHeadManifest>(
			"sources",
			descriptor.sources.map((source) => source.sourceId),
		);
		this.assertProjectionCurrent(isCurrent);
		const sources: RenderWindowSourceProjection[] = [];
		for (let index = 0; index < descriptor.sources.length; index++) {
			this.assertProjectionCurrent(isCurrent);
			const source = descriptor.sources[index];
			const manifest = this.withTransientPage(
				manifests[index],
				source,
				transientPages.get(source.sourceId),
			);
			sources.push(
				await this.projectSource(
					descriptor,
					source,
					manifest,
					bucketPixelWidth,
					isCurrent,
				),
			);
		}
		this.assertProjectionCurrent(isCurrent);
		await this.assertProjectionSourcesCurrent(sources, isCurrent);
		this.assertProjectionCurrent(isCurrent);
		return {
			operationEpoch: descriptor.operationEpoch,
			viewportRevision: descriptor.viewportRevision,
			projectionRevision: descriptor.projectionRevision,
			sources,
			evictedSourceIds: [...(descriptor.evictedSourceIds ?? [])],
		};
	}

	/**
	 * Adds one worker-owned partial builder page to a matching building manifest.
	 * The symbol-backed records remain request-local and can never be persisted.
	 * @param manifest Durable source authority read for this projection.
	 * @param source Viewport settings used to derive engine-specific preview data.
	 * @param transient Optional partial builder page tied to the durable authority.
	 * @returns Effective preview manifest, or the original durable manifest.
	 */
	private withTransientPage(
		manifest: EventSourceHeadManifest | undefined,
		source: EventStoreViewportSourceDescriptor,
		transient: EventStoreViewportTransientPage | undefined,
	): EventSourceHeadManifest | undefined {
		if (
			!manifest ||
			manifest.status !== "building" ||
			!transient ||
			transient.baseGeneration !== manifest.generation ||
			transient.baseRevision !== manifest.revision ||
			transient.page.sourceId !== manifest.sourceId ||
			transient.page.generation !== manifest.generation ||
			transient.page.activeCount === 0 ||
			manifest.pages.some((catalog) => catalog.pageId === transient.page.pageId)
		) {
			return manifest;
		}
		const overrides = new Set(source.overrideHashes);
		const catalog = createEventPageCatalogEntry(
			transient.page,
		) as ProjectionPageCatalogEntry;
		catalog[TRANSIENT_PAGE_RECORD] = {
			exact: transient.page,
			lod: buildEventPageLod(transient.page, {
				overrideHashes: overrides,
				overrideRevision: source.overrideRevision,
				overrideSignature: this.createOverrideSignature(
					source.overrideRevision,
					overrides,
				),
			}),
			height: buildEventPageHeight(transient.page),
		};
		this.frameEventCountBySource.delete(source.sourceId);
		const wasEmpty = manifest.eventCount === 0;
		return {
			...manifest,
			eventCount: manifest.eventCount + transient.page.activeCount,
			nextHandle: Math.max(
				manifest.nextHandle,
				catalog.maximumHandle + 1,
			),
			minTimestamp: wasEmpty
				? transient.page.minTimestamp
				: Math.min(manifest.minTimestamp, transient.page.minTimestamp),
			maxTimestamp: wasEmpty
				? transient.page.maxTimestamp
				: Math.max(manifest.maxTimestamp, transient.page.maxTimestamp),
			hashMin: wasEmpty
				? transient.page.hashMin
				: Math.min(manifest.hashMin, transient.page.hashMin),
			hashMax: wasEmpty
				? transient.page.hashMax
				: Math.max(manifest.hashMax, transient.page.hashMax),
			pages: [...manifest.pages, catalog],
		};
	}

	/**
	 * Precomputes override representatives in bounded page slices before a new color
	 * revision is published. Repeated calls continue from the returned page index.
	 * @param descriptor Source revision, override identity, and bounded page slice.
	 * @returns Progress cursor tied to the supplied source revision.
	 */
	public async prepareOverrideRevision(
		descriptor: EventStoreOverridePreparationDescriptor,
	): Promise<EventStoreOverridePreparationResult> {
		if (
			!Number.isSafeInteger(descriptor.sourceRevision) ||
			descriptor.sourceRevision < 0 ||
			!Number.isSafeInteger(descriptor.overrideRevision) ||
			descriptor.overrideRevision < 0 ||
			(descriptor.startPageIndex !== undefined &&
				(!Number.isSafeInteger(descriptor.startPageIndex) ||
					descriptor.startPageIndex < 0)) ||
			(descriptor.maximumPages !== undefined &&
				(!Number.isSafeInteger(descriptor.maximumPages) ||
					descriptor.maximumPages <= 0)) ||
			![...descriptor.overrideHashes].every((hash) => Number.isFinite(hash))
		) {
			throw new RangeError("Override preparation descriptor is malformed.");
		}
		const manifest = await this.backend.get<EventSourceHeadManifest>(
			"sources",
			descriptor.sourceId,
		);
		if (!manifest || manifest.revision !== descriptor.sourceRevision) {
			throw new StaleEventStoreProjectionError(
				"Override preparation source revision is stale.",
			);
		}
		const startPageIndex = Math.max(
			0,
			Math.min(
				manifest.pages.length,
				Math.floor(descriptor.startPageIndex ?? 0),
			),
		);
		const maximumPages = Math.max(
			1,
			Math.min(256, Math.floor(descriptor.maximumPages ?? 16)),
		);
		const endPageIndex = Math.min(
			manifest.pages.length,
			startPageIndex + maximumPages,
		);
		const overrideHashes = new Set(descriptor.overrideHashes);
		const overrideSignature = this.createOverrideSignature(
			descriptor.overrideRevision,
			overrideHashes,
		);
		const operations: EventStoreWriteOperation[] = [];
		const preparedCatalogs: EventPageCatalogEntry[] = [];
		const preparedLods: Array<{
			catalog: EventPageCatalogEntry;
			lod: EncodedEventPageLod;
		}> = [];
		for (let index = startPageIndex; index < endPageIndex; index++) {
			const catalog = manifest.pages[index];
			const page = await this.loadExactPage(
				manifest.sourceId,
				manifest.generation,
				catalog,
			);
			this.assertExactPageRevision(page, catalog);
			preparedCatalogs.push(catalog);
			const lod = buildEventPageLod(page, {
				overrideHashes,
				overrideRevision: descriptor.overrideRevision,
				overrideSignature,
			});
			preparedLods.push({ catalog, lod });
			operations.push({
				type: "put",
				store: "page_lod",
				key: [
					manifest.sourceId,
					manifest.generation,
					catalog.pageId,
					EVENT_PAGE_LOD_RECORD_KIND,
				],
				value: lod,
			});
		}
		await this.assertManifestRevisionCurrent(manifest, preparedCatalogs);
		await this.backend.commit(operations);
		await this.assertManifestRevisionCurrent(manifest, preparedCatalogs);
		for (const { catalog, lod } of preparedLods) {
			this.lodPageCache.set(
				this.createProjectionPageCacheKey(
					manifest.sourceId,
					manifest.generation,
					catalog,
				),
				manifest.sourceId,
				lod.encodedByteLength,
				lod,
			);
		}
		return {
			sourceId: descriptor.sourceId,
			sourceRevision: descriptor.sourceRevision,
			overrideRevision: descriptor.overrideRevision,
			processedPages: endPageIndex - startPageIndex,
			nextPageIndex: endPageIndex,
			complete: endPageIndex === manifest.pages.length,
		};
	}

	/**
	 * Projects one visible source according to its active render engine.
	 * @param descriptor Shared viewport descriptor.
	 * @param source Source-specific transform and settings.
	 * @param manifest Source manifest read with the other visible rows.
	 * @param bucketPixelWidth Globally selected output bucket width.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns One complete source projection.
	 */
	private async projectSource(
		descriptor: EventStoreViewportDescriptor,
		source: EventStoreViewportSourceDescriptor,
		manifest: EventSourceHeadManifest | undefined,
		bucketPixelWidth: number,
		isCurrent: EventStoreProjectionGuard,
	): Promise<RenderWindowSourceProjection> {
		this.assertProjectionCurrent(isCurrent);
		const transform = this.createTransform(
			descriptor,
			source.offset,
			bucketPixelWidth,
		);
		const coveredRange = this.getCoveredRange(transform);
		if (!manifest) {
			return this.createEmptyProjection(
				source.sourceId,
				coveredRange,
				bucketPixelWidth,
			);
		}
		const frameEventCount = await this.countFrameEvents(
			manifest,
			descriptor.frame,
			source.offset,
			isCurrent,
		);

		let representatives: RepresentativeCandidate[] = [];
		let height: HeightAggregateProjection = {
			timestamps: new Float64Array(0),
			counts: new Float64Array(0),
			maximumCount: 0,
		};
		let graph = { timestamps: new Float64Array(0), counts: new Float64Array(0) };
		if (source.engine === "default") {
			const overrides = new Set(source.overrideHashes);
			representatives = await this.projectDefault(
				manifest,
				transform,
				overrides,
				source.overrideRevision,
				this.createOverrideSignature(source.overrideRevision, overrides),
				isCurrent,
			);
		} else if (source.engine === "height") {
			height = await this.projectHeightAggregates(
				manifest,
				coveredRange,
				transform,
				isCurrent,
			);
		} else {
			const timePerGraphBucket =
				((descriptor.frame.max - descriptor.frame.min) /
					(descriptor.width * descriptor.scale)) *
				RENDER_GRAPH_BUCKET_PIXEL_WIDTH;
			const duration = Math.max(
				0.001,
				Math.round(Math.max(source.frequency, timePerGraphBucket) * 1000) / 1000,
			);
			graph = this.collapseAggregates(
				await this.projectAggregates(
					manifest,
					coveredRange,
					duration,
					isCurrent,
				),
				transform,
				isCurrent,
			);
		}
		this.assertProjectionCurrent(isCurrent);

		return {
			sourceId: source.sourceId,
			sourceAuthorityRevision: manifest.authorityRevision,
			sourceGeneration: manifest.generation,
			sourceRevision: manifest.revision,
			sourceStatus: manifest.status,
			coveredRange,
			bucketPixelWidth,
			degradedLod: bucketPixelWidth > 1,
			loadedCount: manifest.eventCount,
			frameEventCount,
			hashRange: { min: manifest.hashMin, max: manifest.hashMax },
			representativeHandles: Uint32Array.from(
				representatives,
				(candidate) => candidate.handle,
			),
			representativeTimestamps: Float64Array.from(
				representatives,
				(candidate) => candidate.timestamp,
			),
			representativeHashes: Float64Array.from(
				representatives,
				(candidate) => candidate.hash,
			),
			heightTimestamps: height.timestamps,
			heightCounts: height.counts,
			heightMaximumCount: height.maximumCount,
			graphTimestamps: graph.timestamps,
			graphCounts: graph.counts,
		};
	}

	/**
	 * Counts active events inside the complete selected frame for one source.
	 * Fully covered pages use their catalog count; only boundary pages read exact data.
	 * @param manifest Stable source authority used by the current projection.
	 * @param frame Selected rendered frame in milliseconds.
	 * @param offset Source render offset applied to raw event timestamps.
	 * @param isCurrent Guard rejecting superseded projection work.
	 * @returns Exact active event count inside the source-adjusted frame.
	 */
	private async countFrameEvents(
		manifest: EventSourceHeadManifest,
		frame: RenderTimeRange,
		offset: number,
		isCurrent: EventStoreProjectionGuard,
	): Promise<number> {
		const minimum = Math.min(frame.min - offset, frame.max - offset);
		const maximum = Math.max(frame.min - offset, frame.max - offset);
		const cacheKey = JSON.stringify([
			manifest.authorityRevision,
			manifest.generation,
			manifest.revision,
			minimum,
			maximum,
			offset,
		]);
		const cached = this.frameEventCountBySource.get(manifest.sourceId);
		if (cached?.key === cacheKey) return cached.count;

		if (
			manifest.eventCount === 0 ||
			manifest.maxTimestamp < minimum ||
			manifest.minTimestamp > maximum
		) {
			this.frameEventCountBySource.set(manifest.sourceId, {
				key: cacheKey,
				count: 0,
			});
			return 0;
		}
		if (
			manifest.minTimestamp >= minimum &&
			manifest.maxTimestamp <= maximum
		) {
			this.frameEventCountBySource.set(manifest.sourceId, {
				key: cacheKey,
				count: manifest.eventCount,
			});
			return manifest.eventCount;
		}

		let count = 0;
		for (const catalog of manifest.pages) {
			this.assertProjectionCurrent(isCurrent);
			if (
				catalog.activeCount === 0 ||
				catalog.maxTimestamp < minimum ||
				catalog.minTimestamp > maximum
			) {
				continue;
			}
			if (
				catalog.minTimestamp >= minimum &&
				catalog.maxTimestamp <= maximum
			) {
				count += catalog.activeCount;
				continue;
			}

			const page = await this.loadExactPage(
				manifest.sourceId,
				manifest.generation,
				catalog,
			);
			this.assertProjectionCurrent(isCurrent);
			this.assertExactPageRevision(page, catalog);
			for (let offsetIndex = 0; offsetIndex < page.count; offsetIndex++) {
				if ((offsetIndex & PROJECTION_GUARD_CHECK_MASK) === 0) {
					this.assertProjectionCurrent(isCurrent);
				}
				if (page.tombstones[offsetIndex] !== 0) continue;
				const timestamp = page.timestamps[offsetIndex];
				if (timestamp >= minimum && timestamp <= maximum) count++;
			}
		}
		this.frameEventCountBySource.set(manifest.sourceId, {
			key: cacheKey,
			count,
		});
		return count;
	}

	/**
	 * Selects pixel-equivalent default-engine representatives through page, microblock,
	 * and exact refinement levels.
	 * @param manifest Active source manifest.
	 * @param transform Source-adjusted viewport transform.
	 * @param overrides Hashes that take precedence inside a collapsed bucket.
	 * @param overrideRevision Requested persisted override revision.
	 * @param overrideSignature Deterministic identity of the requested overrides.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Representatives sorted by descending raw timestamp.
	 */
	private async projectDefault(
		manifest: EventSourceManifest,
		transform: ProjectionTransform,
		overrides: ReadonlySet<number>,
		overrideRevision: number,
		overrideSignature: string,
		isCurrent: EventStoreProjectionGuard,
	): Promise<RepresentativeCandidate[]> {
		const buckets = new Map<number, RepresentativeCandidate>();
		for (const catalog of manifest.pages) {
			this.assertProjectionCurrent(isCurrent);
			if (catalog.activeCount === 0) continue;
			const pageMinimumBucket = this.getBucket(catalog.minTimestamp, transform);
			const pageMaximumBucket = this.getBucket(catalog.maxTimestamp, transform);
			if (pageMinimumBucket === null && pageMaximumBucket === null) {
				const range = this.getCoveredRange(transform);
				if (
					catalog.maxTimestamp < range.min ||
					catalog.minTimestamp > range.max
				) {
					continue;
				}
			}
			let lod: EncodedEventPageLod | null = null;
			if (overrides.size > 0) {
				lod = await this.loadLodPage(
					manifest.sourceId,
					manifest.generation,
					catalog,
					overrideRevision,
					overrideSignature,
				);
				this.assertProjectionCurrent(isCurrent);
				this.assertLodPageRevision(lod, catalog);
				if (
					lod.overrideRevision !== overrideRevision ||
					lod.overrideSignature !== overrideSignature
				) {
					throw new EventStoreOverrideProjectionPendingError(
						`Override revision ${overrideRevision} is not prepared for ${manifest.sourceId}/${catalog.pageId}.`,
					);
				}
			}
			if (
				pageMinimumBucket !== null &&
				pageMinimumBucket === pageMaximumBucket
			) {
				const hasPageOverride = (lod?.pageOverrideHandle ?? 0) !== 0;
				this.mergeRepresentative(buckets, {
					bucket: pageMinimumBucket,
					handle: hasPageOverride
						? lod!.pageOverrideHandle
						: catalog.representativeHandle,
					timestamp: hasPageOverride
						? lod!.pageOverrideTimestamp
						: catalog.representativeTimestamp,
					hash: hasPageOverride
						? lod!.pageOverrideHash
						: catalog.representativeHash,
					isOverride: hasPageOverride,
				});
				continue;
			}

			lod ??= await this.loadLodPage(
				manifest.sourceId,
				manifest.generation,
				catalog,
				overrides.size > 0 ? overrideRevision : undefined,
				overrides.size > 0 ? overrideSignature : undefined,
			);
			this.assertProjectionCurrent(isCurrent);
			this.assertLodPageRevision(lod, catalog);
			let exactPage: EncodedEventPage | null = null;
			for (let block = 0; block < lod.blockCount; block++) {
				this.assertProjectionCurrent(isCurrent);
				if (lod.counts[block] === 0) continue;
				const minimumBucket = this.getBucket(lod.minTimestamps[block], transform);
				const maximumBucket = this.getBucket(lod.maxTimestamps[block], transform);
				if (minimumBucket === null && maximumBucket === null) {
					const range = this.getCoveredRange(transform);
					if (
						lod.maxTimestamps[block] < range.min ||
						lod.minTimestamps[block] > range.max
					) {
						continue;
					}
				}
				if (minimumBucket !== null && minimumBucket === maximumBucket) {
					const hasBlockOverride = lod.overrideBlockHandles[block] !== 0;
					this.mergeRepresentative(buckets, {
						bucket: minimumBucket,
						handle: hasBlockOverride
							? lod.overrideBlockHandles[block]
							: lod.representativeHandles[block],
						timestamp: hasBlockOverride
							? lod.overrideBlockTimestamps[block]
							: lod.representativeTimestamps[block],
						hash: hasBlockOverride
							? lod.overrideBlockHashes[block]
							: lod.representativeHashes[block],
						isOverride: hasBlockOverride,
					});
					continue;
				}

				exactPage ??= await this.loadExactPage(
					manifest.sourceId,
					manifest.generation,
					catalog,
				);
				this.assertProjectionCurrent(isCurrent);
				this.assertExactPageRevision(exactPage, catalog);
				for (
					let offset = lod.startOffsets[block];
					offset < lod.endOffsets[block];
					offset++
				) {
					if ((offset & PROJECTION_GUARD_CHECK_MASK) === 0) {
						this.assertProjectionCurrent(isCurrent);
					}
					if (exactPage.tombstones[offset] !== 0) continue;
					const bucket = this.getBucket(exactPage.timestamps[offset], transform);
					if (bucket === null) continue;
					const hash = exactPage.hashes[offset];
					this.mergeRepresentative(buckets, {
						bucket,
						handle: exactPage.handles[offset],
						timestamp: exactPage.timestamps[offset],
						hash,
						isOverride: overrides.has(hash),
					});
				}
			}
		}
		this.assertProjectionCurrent(isCurrent);
		return [...buckets.values()].sort(
			(left, right) =>
				right.timestamp - left.timestamp || left.handle - right.handle,
		);
	}

	/**
	 * Merges persistent page-local minute counts directly into bounded output buckets.
	 * A non-minute-aligned source uses microblock refinement to retain the legacy
	 * source-minimum anchor. Only one LOD/exact page is resident at a time.
	 * @param manifest Active source manifest.
	 * @param coveredRange Inclusive raw timestamp range.
	 * @param transform Source-adjusted viewport transform.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Ascending viewport-bounded minute aggregates.
	 */
	private async projectHeightAggregates(
		manifest: EventSourceManifest,
		coveredRange: RenderTimeRange,
		transform: ProjectionTransform,
		isCurrent: EventStoreProjectionGuard,
	): Promise<HeightAggregateProjection> {
		this.assertProjectionCurrent(isCurrent);
		let projection: AggregateProjection;
		if (manifest.minTimestamp % 60_000 !== 0) {
			projection = this.collapseAggregates(
				await this.projectAggregates(
					manifest,
					coveredRange,
					60_000,
					isCurrent,
				),
				transform,
				isCurrent,
			);
		} else {
			const collapsed = new Map<
				number,
				{ timestamp: number; count: number }
			>();
			for (const catalog of manifest.pages) {
				this.assertProjectionCurrent(isCurrent);
				if (
					catalog.activeCount === 0 ||
					catalog.maxTimestamp < coveredRange.min ||
					catalog.minTimestamp > coveredRange.max
				) {
					continue;
				}
				const height = await this.loadHeightPage(
					manifest.sourceId,
					manifest.generation,
					catalog,
				);
				this.assertProjectionCurrent(isCurrent);
				this.assertHeightPageRevision(height, catalog);
				for (let index = 0; index < height.minuteTimestamps.length; index++) {
					if ((index & PROJECTION_GUARD_CHECK_MASK) === 0) {
						this.assertProjectionCurrent(isCurrent);
					}
					const timestamp = height.minuteTimestamps[index];
					if (timestamp < coveredRange.min || timestamp > coveredRange.max) {
						continue;
					}
					const bucket = this.getBucket(timestamp, transform);
					if (bucket === null) continue;
					const current = collapsed.get(bucket);
					collapsed.set(bucket, {
						timestamp: Math.max(current?.timestamp ?? timestamp, timestamp),
						count: (current?.count ?? 0) + height.minuteCounts[index],
					});
				}
			}
			const buckets = new Map<number, number>();
			for (const value of collapsed.values()) {
				this.assertProjectionCurrent(isCurrent);
				buckets.set(value.timestamp, value.count);
			}
			projection = this.encodeAggregates(buckets, isCurrent);
		}
		const maximumCount =
			manifest.status === "complete"
				? await this.getGlobalHeightMaximum(manifest, isCurrent)
				: this.getMaximumAggregateCount(projection.counts, isCurrent);
		this.assertProjectionCurrent(isCurrent);
		return { ...projection, maximumCount };
	}

	/**
	 * Returns the source-wide height maximum for stable pan-independent normalization.
	 * The scalar is cached only for the exact manifest generation and revision.
	 * @param manifest Active source authority.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Maximum count in any source-minimum-anchored one-minute bucket.
	 */
	private async getGlobalHeightMaximum(
		manifest: EventSourceManifest,
		isCurrent: EventStoreProjectionGuard,
	): Promise<number> {
		this.assertProjectionCurrent(isCurrent);
		const cached = this.heightMaximumBySource.get(manifest.sourceId);
		if (
			cached?.generation === manifest.generation &&
			cached.revision === manifest.revision
		) {
			return cached.maximumCount;
		}
		const extended = cached
			? await this.tryExtendCachedHeightMaximum(manifest, cached, isCurrent)
			: null;
		this.assertProjectionCurrent(isCurrent);
		if (extended) {
			this.heightMaximumBySource.set(manifest.sourceId, extended);
			return extended.maximumCount;
		}
		const statistics =
			manifest.eventCount === 0
				? this.createEmptyHeightMaximumStatistics()
				: await this.calculateHeightMaximumStatistics(
						manifest,
						manifest.pages,
						isCurrent,
					);
		this.assertProjectionCurrent(isCurrent);
		const calculated: CachedHeightMaximum = {
			generation: manifest.generation,
			revision: manifest.revision,
			minimumTimestamp: manifest.minTimestamp,
			maximumTimestamp: manifest.maxTimestamp,
			...statistics,
			pageRevisions: this.createPageRevisionMap(manifest.pages),
		};
		this.heightMaximumBySource.set(manifest.sourceId, calculated);
		return calculated.maximumCount;
	}

	/**
	 * Reuses an exact cached maximum when a completed source only gained chronological
	 * pages. Any page mutation, removal, reordering, or older append requests a bounded
	 * full recalculation instead.
	 * @param manifest Current complete source manifest.
	 * @param cached Previous exact source maximum.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Extended cache entry, or null when the fast path is unsafe.
	 */
	private async tryExtendCachedHeightMaximum(
		manifest: EventSourceManifest,
		cached: CachedHeightMaximum,
		isCurrent: EventStoreProjectionGuard,
	): Promise<CachedHeightMaximum | null> {
		this.assertProjectionCurrent(isCurrent);
		if (
			cached.generation !== manifest.generation ||
			cached.minimumTimestamp !== manifest.minTimestamp ||
			manifest.maxTimestamp < cached.maximumTimestamp
		) {
			return null;
		}
		const currentByPage = new Map(
			manifest.pages.map((catalog) => [catalog.pageId, catalog]),
		);
		for (const [pageId, revision] of cached.pageRevisions) {
			this.assertProjectionCurrent(isCurrent);
			if (currentByPage.get(pageId)?.revision !== revision) return null;
		}
		const added = manifest.pages.filter(
			(catalog) => !cached.pageRevisions.has(catalog.pageId),
		);
		if (currentByPage.size !== cached.pageRevisions.size + added.length) {
			return null;
		}
		if (
			added.some(
				(catalog) =>
					catalog.activeCount > 0 &&
					catalog.minTimestamp < cached.maximumTimestamp,
			)
		) {
			return null;
		}
		const activeAdded = added.filter((catalog) => catalog.activeCount > 0);
		if (activeAdded.length === 0) {
			return {
				...cached,
				revision: manifest.revision,
				maximumTimestamp: manifest.maxTimestamp,
				pageRevisions: this.createPageRevisionMap(manifest.pages),
			};
		}
		const addedStatistics = await this.calculateHeightMaximumStatistics(
			manifest,
			activeAdded,
			isCurrent,
		);
		this.assertProjectionCurrent(isCurrent);
		const sharesMaximumBucket =
			addedStatistics.minimumBucketTimestamp === cached.maximumBucketTimestamp;
		const sharedBucketCount = sharesMaximumBucket
			? cached.maximumBucketCount + addedStatistics.minimumBucketCount
			: 0;
		let maximumBucketCount = addedStatistics.maximumBucketCount;
		if (
			addedStatistics.maximumBucketTimestamp === cached.maximumBucketTimestamp
		) {
			maximumBucketCount += cached.maximumBucketCount;
		}
		return {
			generation: manifest.generation,
			revision: manifest.revision,
			minimumTimestamp: manifest.minTimestamp,
			maximumTimestamp: manifest.maxTimestamp,
			maximumCount: Math.max(
				cached.maximumCount,
				addedStatistics.maximumCount,
				sharedBucketCount,
			),
			minimumBucketTimestamp: cached.minimumBucketTimestamp,
			minimumBucketCount: cached.minimumBucketCount,
			maximumBucketTimestamp: addedStatistics.maximumBucketTimestamp,
			maximumBucketCount,
			pageRevisions: this.createPageRevisionMap(manifest.pages),
		};
	}

	/**
	 * Calculates an exact source maximum in sparse, fixed-size timestamp windows.
	 * Empty time ranges are skipped and no more than the configured bucket limit is
	 * resident, even when every persisted page overlaps every other page.
	 * @param manifest Source authority supplying the legacy bucket anchor.
	 * @param catalogs Exact set of pages to aggregate.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Exact maximum plus boundary counts used by chronological appends.
	 */
	private async calculateHeightMaximumStatistics(
		manifest: EventSourceManifest,
		catalogs: readonly EventPageCatalogEntry[],
		isCurrent: EventStoreProjectionGuard,
	): Promise<HeightMaximumStatistics> {
		this.assertProjectionCurrent(isCurrent);
		const activeCatalogs = catalogs.filter((catalog) => catalog.activeCount > 0);
		if (activeCatalogs.length === 0) {
			return this.createEmptyHeightMaximumStatistics();
		}
		const duration = 60_000;
		const anchor = manifest.minTimestamp;
		let nextMaximumTimestamp = Number.NEGATIVE_INFINITY;
		for (const catalog of activeCatalogs) {
			this.assertProjectionCurrent(isCurrent);
			nextMaximumTimestamp = Math.max(
				nextMaximumTimestamp,
				catalog.maxTimestamp,
			);
		}
		const statistics: HeightMaximumStatistics = {
			maximumCount: 0,
			minimumBucketTimestamp: Number.POSITIVE_INFINITY,
			minimumBucketCount: 0,
			maximumBucketTimestamp: Number.NEGATIVE_INFINITY,
			maximumBucketCount: 0,
		};
		while (Number.isFinite(nextMaximumTimestamp)) {
			this.assertProjectionCurrent(isCurrent);
			const maximumBucketIndex = Math.floor(
				(nextMaximumTimestamp - anchor) / duration,
			);
			const minimumBucketIndex = Math.max(
				0,
				maximumBucketIndex - EVENT_HEIGHT_MAXIMUM_WORKING_BUCKET_LIMIT + 1,
			);
			const bucketCount = maximumBucketIndex - minimumBucketIndex + 1;
			this.maximumHeightWorkingBuckets = Math.max(
				this.maximumHeightWorkingBuckets,
				bucketCount,
			);
			const counts = new Float64Array(bucketCount);
			const windowMinimum = anchor + minimumBucketIndex * duration;
			const windowMaximumExclusive =
				anchor + (maximumBucketIndex + 1) * duration;
			let followingMaximumTimestamp = Number.NEGATIVE_INFINITY;
			for (const catalog of activeCatalogs) {
				this.assertProjectionCurrent(isCurrent);
				if (catalog.maxTimestamp < windowMinimum) {
					followingMaximumTimestamp = Math.max(
						followingMaximumTimestamp,
						catalog.maxTimestamp,
					);
					continue;
				}
				if (catalog.minTimestamp >= windowMaximumExclusive) continue;
				followingMaximumTimestamp = Math.max(
					followingMaximumTimestamp,
					await this.accumulateHeightWindow(
						manifest,
						catalog,
						minimumBucketIndex,
						counts,
						isCurrent,
					),
				);
			}
			for (let index = 0; index < counts.length; index++) {
				if ((index & PROJECTION_GUARD_CHECK_MASK) === 0) {
					this.assertProjectionCurrent(isCurrent);
				}
				const count = counts[index];
				if (count === 0) continue;
				const timestamp = anchor + (minimumBucketIndex + index) * duration;
				statistics.maximumCount = Math.max(statistics.maximumCount, count);
				if (timestamp < statistics.minimumBucketTimestamp) {
					statistics.minimumBucketTimestamp = timestamp;
					statistics.minimumBucketCount = count;
				}
				if (timestamp > statistics.maximumBucketTimestamp) {
					statistics.maximumBucketTimestamp = timestamp;
					statistics.maximumBucketCount = count;
				}
			}
			nextMaximumTimestamp = followingMaximumTimestamp;
		}
		return statistics;
	}

	/**
	 * Adds one page to a bounded height window and identifies its next lower event.
	 * UTC-aligned sources read only their compact height record; legacy non-aligned
	 * sources read one exact page to preserve the source-minimum bucket anchor.
	 * @param manifest Source authority and bucket anchor.
	 * @param catalog Page contributing to the current window.
	 * @param minimumBucketIndex First global bucket represented by counts.
	 * @param counts Fixed-size mutable bucket counts.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Greatest timestamp below the window, or negative infinity.
	 */
	private async accumulateHeightWindow(
		manifest: EventSourceManifest,
		catalog: EventPageCatalogEntry,
		minimumBucketIndex: number,
		counts: Float64Array,
		isCurrent: EventStoreProjectionGuard,
	): Promise<number> {
		this.assertProjectionCurrent(isCurrent);
		const duration = 60_000;
		const anchor = manifest.minTimestamp;
		const maximumBucketIndex = minimumBucketIndex + counts.length - 1;
		const catalogMinimumBucket = Math.floor(
			(catalog.minTimestamp - anchor) / duration,
		);
		const catalogMaximumBucket = Math.floor(
			(catalog.maxTimestamp - anchor) / duration,
		);
		if (catalogMinimumBucket === catalogMaximumBucket) {
			counts[catalogMinimumBucket - minimumBucketIndex] += catalog.activeCount;
			return Number.NEGATIVE_INFINITY;
		}
		let followingMaximumTimestamp = Number.NEGATIVE_INFINITY;
		if (anchor % duration === 0) {
			const height = await this.loadHeightPage(
				manifest.sourceId,
				manifest.generation,
				catalog,
			);
			this.assertProjectionCurrent(isCurrent);
			this.assertHeightPageRevision(height, catalog);
			for (let index = 0; index < height.minuteTimestamps.length; index++) {
				if ((index & PROJECTION_GUARD_CHECK_MASK) === 0) {
					this.assertProjectionCurrent(isCurrent);
				}
				const timestamp = height.minuteTimestamps[index];
				const bucketIndex = Math.floor((timestamp - anchor) / duration);
				if (bucketIndex < minimumBucketIndex) {
					followingMaximumTimestamp = Math.max(
						followingMaximumTimestamp,
						timestamp,
					);
				} else if (bucketIndex <= maximumBucketIndex) {
					counts[bucketIndex - minimumBucketIndex] +=
						height.minuteCounts[index];
				}
			}
			return followingMaximumTimestamp;
		}
		const page = await this.loadExactPage(
			manifest.sourceId,
			manifest.generation,
			catalog,
		);
		this.assertProjectionCurrent(isCurrent);
		this.assertExactPageRevision(page, catalog);
		for (let offset = 0; offset < page.count; offset++) {
			if ((offset & PROJECTION_GUARD_CHECK_MASK) === 0) {
				this.assertProjectionCurrent(isCurrent);
			}
			if (page.tombstones[offset] !== 0) continue;
			const timestamp = page.timestamps[offset];
			const bucketIndex = Math.floor((timestamp - anchor) / duration);
			if (bucketIndex < minimumBucketIndex) {
				followingMaximumTimestamp = Math.max(
					followingMaximumTimestamp,
					timestamp,
				);
			} else if (bucketIndex <= maximumBucketIndex) {
				counts[bucketIndex - minimumBucketIndex]++;
			}
		}
		return followingMaximumTimestamp;
	}

	/**
	 * Builds a compact page-revision fence for safe chronological cache extension.
	 * @param catalogs Manifest page catalog.
	 * @returns Page ID to revision mapping.
	 */
	private createPageRevisionMap(
		catalogs: readonly EventPageCatalogEntry[],
	): ReadonlyMap<number, number> {
		return new Map(catalogs.map((catalog) => [catalog.pageId, catalog.revision]));
	}

	/**
	 * Creates boundary-safe zero statistics for an empty source.
	 * @returns Empty exact height statistics.
	 */
	private createEmptyHeightMaximumStatistics(): HeightMaximumStatistics {
		return {
			maximumCount: 0,
			minimumBucketTimestamp: 0,
			minimumBucketCount: 0,
			maximumBucketTimestamp: 0,
			maximumBucketCount: 0,
		};
	}

	/**
	 * Returns the largest count already present in one bounded projection.
	 * @param counts Bounded aggregate count column.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Largest aggregate count.
	 */
	private getMaximumAggregateCount(
		counts: Float64Array,
		isCurrent: EventStoreProjectionGuard,
	): number {
		let maximum = 0;
		for (let index = 0; index < counts.length; index++) {
			if ((index & PROJECTION_GUARD_CHECK_MASK) === 0) {
				this.assertProjectionCurrent(isCurrent);
			}
			maximum = Math.max(maximum, counts[index]);
		}
		return maximum;
	}

	/**
	 * Produces exact anchored time aggregates, using catalog or LOD counts only when
	 * the complete summarized interval belongs to one output bucket.
	 * @param manifest Active source manifest.
	 * @param coveredRange Inclusive raw timestamp range.
	 * @param duration Aggregate duration in source timestamp units.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Ascending exact-size aggregate arrays.
	 */
	private async projectAggregates(
		manifest: EventSourceManifest,
		coveredRange: RenderTimeRange,
		duration: number,
		isCurrent: EventStoreProjectionGuard,
	): Promise<AggregateProjection> {
		this.assertProjectionCurrent(isCurrent);
		const buckets = new Map<number, number>();
		/** Maps an event timestamp to its source-anchored aggregate bucket. */
		const getAggregateBucket = (timestamp: number): number =>
			manifest.minTimestamp +
			Math.floor((timestamp - manifest.minTimestamp) / duration) * duration;
		for (const catalog of manifest.pages) {
			this.assertProjectionCurrent(isCurrent);
			if (
				catalog.activeCount === 0 ||
				catalog.maxTimestamp < coveredRange.min ||
				catalog.minTimestamp > coveredRange.max
			) {
				continue;
			}
			if (
				catalog.minTimestamp >= coveredRange.min &&
				catalog.maxTimestamp <= coveredRange.max &&
				getAggregateBucket(catalog.minTimestamp) ===
					getAggregateBucket(catalog.maxTimestamp)
			) {
				this.addAggregateCount(
					buckets,
					getAggregateBucket(catalog.minTimestamp),
					catalog.activeCount,
				);
				continue;
			}

			const lod = await this.loadLodPage(
				manifest.sourceId,
				manifest.generation,
				catalog,
			);
			this.assertProjectionCurrent(isCurrent);
			this.assertLodPageRevision(lod, catalog);
			let exactPage: EncodedEventPage | null = null;
			for (let block = 0; block < lod.blockCount; block++) {
				this.assertProjectionCurrent(isCurrent);
				if (
					lod.counts[block] === 0 ||
					lod.maxTimestamps[block] < coveredRange.min ||
					lod.minTimestamps[block] > coveredRange.max
				) {
					continue;
				}
				if (
					lod.minTimestamps[block] >= coveredRange.min &&
					lod.maxTimestamps[block] <= coveredRange.max &&
					getAggregateBucket(lod.minTimestamps[block]) ===
						getAggregateBucket(lod.maxTimestamps[block])
				) {
					this.addAggregateCount(
						buckets,
						getAggregateBucket(lod.minTimestamps[block]),
						lod.counts[block],
					);
					continue;
				}

				exactPage ??= await this.loadExactPage(
					manifest.sourceId,
					manifest.generation,
					catalog,
				);
				this.assertProjectionCurrent(isCurrent);
				this.assertExactPageRevision(exactPage, catalog);
				for (
					let offset = lod.startOffsets[block];
					offset < lod.endOffsets[block];
					offset++
				) {
					if ((offset & PROJECTION_GUARD_CHECK_MASK) === 0) {
						this.assertProjectionCurrent(isCurrent);
					}
					const timestamp = exactPage.timestamps[offset];
					if (
						exactPage.tombstones[offset] !== 0 ||
						timestamp < coveredRange.min ||
						timestamp > coveredRange.max
					) {
						continue;
					}
					this.addAggregateCount(buckets, getAggregateBucket(timestamp), 1);
				}
			}
		}

		this.assertProjectionCurrent(isCurrent);
		return this.encodeAggregates(buckets, isCurrent);
	}

	/**
	 * Collapses aggregate timestamps to the same globally capped output buckets used
	 * by representatives. Counts remain exact for the collapsed pixel interval.
	 * @param projection Exact time aggregates.
	 * @param transform Source-adjusted viewport transform.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Ascending aggregate arrays with at most one record per output bucket.
	 */
	private collapseAggregates(
		projection: AggregateProjection,
		transform: ProjectionTransform,
		isCurrent: EventStoreProjectionGuard,
	): AggregateProjection {
		const collapsed = new Map<
			number,
			{ timestamp: number; count: number }
		>();
		for (let index = 0; index < projection.timestamps.length; index++) {
			if ((index & PROJECTION_GUARD_CHECK_MASK) === 0) {
				this.assertProjectionCurrent(isCurrent);
			}
			const timestamp = projection.timestamps[index];
			const bucket = this.getBucket(timestamp, transform);
			if (bucket === null) continue;
			const current = collapsed.get(bucket);
			collapsed.set(bucket, {
				timestamp: Math.max(current?.timestamp ?? timestamp, timestamp),
				count: (current?.count ?? 0) + projection.counts[index],
			});
		}
		const buckets = new Map<number, number>();
		for (const value of collapsed.values()) {
			this.assertProjectionCurrent(isCurrent);
			buckets.set(value.timestamp, value.count);
		}
		return this.encodeAggregates(buckets, isCurrent);
	}

	/**
	 * Encodes an aggregate map into exact-size ascending typed arrays.
	 * @param buckets Timestamp-to-count aggregate map.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Exact-size ascending aggregate columns.
	 */
	private encodeAggregates(
		buckets: ReadonlyMap<number, number>,
		isCurrent: EventStoreProjectionGuard,
	): AggregateProjection {
		this.assertProjectionCurrent(isCurrent);
		const timestamps = [...buckets.keys()].sort((left, right) => left - right);
		this.assertProjectionCurrent(isCurrent);
		return {
			timestamps: Float64Array.from(timestamps),
			counts: Float64Array.from(
				timestamps,
				(timestamp) => buckets.get(timestamp) ?? 0,
			),
		};
	}

	/** Adds one count to an aggregate map. */
	private addAggregateCount(
		buckets: Map<number, number>,
		timestamp: number,
		count: number,
	): void {
		buckets.set(timestamp, (buckets.get(timestamp) ?? 0) + count);
	}

	/** Keeps the newest override, or otherwise the newest event, for one bucket. */
	private mergeRepresentative(
		buckets: Map<number, RepresentativeCandidate>,
		candidate: RepresentativeCandidate,
	): void {
		const current = buckets.get(candidate.bucket);
		if (
			!current ||
			(candidate.isOverride && !current.isOverride) ||
			(candidate.isOverride === current.isOverride &&
				(candidate.timestamp > current.timestamp ||
					(candidate.timestamp === current.timestamp &&
						candidate.handle < current.handle)))
		) {
			buckets.set(candidate.bucket, candidate);
		}
	}

	/** Creates a deterministic persisted identity for one override revision. */
	private createOverrideSignature(
		revision: number,
		hashes: ReadonlySet<number>,
	): string {
		return `${revision}:${[...hashes]
			.sort((left, right) => left - right)
			.join(",")}`;
	}

	/**
	 * Calculates a power-of-two bucket width before any event page is loaded. The
	 * bound covers default representatives plus height and graph aggregate records.
	 */
	private calculateBucketPixelWidth(
		descriptor: EventStoreViewportDescriptor,
	): number {
		const projectedSourceCount = descriptor.sources.length;
		if (projectedSourceCount === 0) return 1;
		const maximum =
			descriptor.maxRepresentatives ?? DEFAULT_MAXIMUM_REPRESENTATIVES;
		const pixelSpan =
			Math.ceil(descriptor.width + descriptor.horizontalOverscanPixels * 2) + 1;
		let bucketPixelWidth = 1;
		while (
			Math.ceil(pixelSpan / bucketPixelWidth) * projectedSourceCount > maximum
		) {
			bucketPixelWidth *= 2;
		}
		return bucketPixelWidth;
	}

	/** Creates the monotonic source-specific pixel transform. */
	private createTransform(
		descriptor: EventStoreViewportDescriptor,
		offset: number,
		bucketPixelWidth: number,
	): ProjectionTransform {
		const overscan = Math.floor(descriptor.horizontalOverscanPixels);
		return {
			frame: descriptor.frame,
			virtualWidth: descriptor.width * descriptor.scale,
			scrollX: descriptor.scrollX,
			firstPixel: -overscan,
			lastPixel: Math.ceil(descriptor.width + overscan),
			bucketPixelWidth,
			offset,
		};
	}

	/** Maps one raw timestamp into an overscanned output bucket. */
	private getBucket(
		timestamp: number,
		transform: ProjectionTransform,
	): number | null {
		const adjustedTimestamp = timestamp + transform.offset;
		if (
			adjustedTimestamp < transform.frame.min ||
			adjustedTimestamp > transform.frame.max
		) {
			return null;
		}
		const frameRange = transform.frame.max - transform.frame.min;
		const pixel =
			Math.round(
				((adjustedTimestamp - transform.frame.min) / frameRange) *
					transform.virtualWidth,
			) - transform.scrollX;
		if (pixel < transform.firstPixel || pixel > transform.lastPixel) return null;
		return Math.floor(
			(pixel - transform.firstPixel) / transform.bucketPixelWidth,
		);
	}

	/** Inverts the overscanned pixel edges into a raw source timestamp range. */
	private getCoveredRange(transform: ProjectionTransform): RenderTimeRange {
		const frameRange = transform.frame.max - transform.frame.min;
		const first =
			transform.frame.min +
			((transform.firstPixel + transform.scrollX - 0.5) /
				transform.virtualWidth) *
				frameRange -
			transform.offset;
		const last =
			transform.frame.min +
			((transform.lastPixel + transform.scrollX + 0.5) /
				transform.virtualWidth) *
				frameRange -
			transform.offset;
		const frameMinimum = transform.frame.min - transform.offset;
		const frameMaximum = transform.frame.max - transform.offset;
		const minimum = Math.min(first, last);
		const maximum = Math.max(first, last);
		return {
			min: Math.min(frameMaximum, Math.max(frameMinimum, minimum)),
			max: Math.max(frameMinimum, Math.min(frameMaximum, maximum)),
		};
	}

	/**
	 * Rechecks every source after a multi-source projection to prevent publishing a
	 * mixture assembled across source mutations.
	 * @param projections Prepared source projections and their revision fences.
	 * @param isCurrent Guard rejecting work superseded by a newer viewport.
	 * @returns Promise settled only while every source fence remains current.
	 */
	private async assertProjectionSourcesCurrent(
		projections: readonly RenderWindowSourceProjection[],
		isCurrent: EventStoreProjectionGuard,
	): Promise<void> {
		this.assertProjectionCurrent(isCurrent);
		const manifests = await this.backend.getMany<EventSourceHeadManifest>(
			"sources",
			projections.map((projection) => projection.sourceId),
		);
		this.assertProjectionCurrent(isCurrent);
		for (let index = 0; index < projections.length; index++) {
			this.assertProjectionCurrent(isCurrent);
			const projection = projections[index];
			const current = manifests[index];
			if (
				!current &&
				projection.sourceGeneration === 0 &&
				projection.sourceRevision === 0 &&
				projection.loadedCount === 0
			) {
				continue;
			}
			const generationChanged =
				(current?.generation ?? 0) !== projection.sourceGeneration;
			const authorityChanged =
				(current?.authorityRevision ?? 0) !==
				projection.sourceAuthorityRevision;
			const revisionMovedBackwards =
				(current?.revision ?? 0) < projection.sourceRevision;
			const terminalChanged =
				projection.sourceStatus !== "building" &&
				((current?.revision ?? 0) !== projection.sourceRevision ||
					current?.status !== projection.sourceStatus ||
					(current?.eventCount ?? 0) !== projection.loadedCount);
			if (
				authorityChanged ||
				generationChanged ||
				revisionMovedBackwards ||
				terminalChanged
			) {
				throw new StaleEventStoreProjectionError(
					`Source ${projection.sourceId} changed while its viewport was projected.`,
				);
			}
		}
	}

	/**
	 * Rejects projection work after a newer viewport takes ownership of the worker slot.
	 * @param isCurrent Guard owned by the newest-wins request scheduler.
	 * @returns Nothing while the request remains current.
	 * @throws StaleEventStoreProjectionError when a newer viewport superseded the work.
	 */
	private assertProjectionCurrent(
		isCurrent: EventStoreProjectionGuard,
	): void {
		if (isCurrent()) return;
		throw new StaleEventStoreProjectionError(
			"Viewport projection was superseded by a newer request.",
		);
	}

	/**
	 * Rechecks one override preparation fence immediately around its LOD commit.
	 * The QueryWorker must additionally hold its per-source mutation lease because
	 * IndexedDB offers no compare-and-swap transaction through this backend contract.
	 */
	private async assertManifestRevisionCurrent(
		expected: EventSourceManifest,
		preparedCatalogs: readonly EventPageCatalogEntry[],
	): Promise<void> {
		const current = await this.backend.get<EventSourceManifest>(
			"sources",
			expected.sourceId,
		);
		if (
			!current ||
			current.generation !== expected.generation ||
			current.revision !== expected.revision
		) {
			throw new StaleEventStoreProjectionError(
				`Source ${expected.sourceId} changed during override preparation.`,
			);
		}
		const revisionsByPage = new Map(
			current.pages.map((catalog) => [catalog.pageId, catalog.revision]),
		);
		for (const catalog of preparedCatalogs) {
			if (revisionsByPage.get(catalog.pageId) !== catalog.revision) {
				throw new StaleEventStoreProjectionError(
					`Page ${expected.sourceId}/${catalog.pageId} changed during override preparation.`,
				);
			}
		}
	}

	/** Validates an exact page against the manifest catalog snapshot. */
	private assertExactPageRevision(
		page: EncodedEventPage,
		catalog: EventPageCatalogEntry,
	): void {
		if (page.revision !== catalog.revision) {
			throw new StaleEventStoreProjectionError(
				`Exact page ${page.sourceId}/${page.pageId} changed during projection.`,
			);
		}
	}

	/** Validates a page-local LOD record against the manifest catalog snapshot. */
	private assertLodPageRevision(
		lod: EncodedEventPageLod,
		catalog: EventPageCatalogEntry,
	): void {
		if (lod.revision !== catalog.revision) {
			throw new StaleEventStoreProjectionError(
				`LOD page ${lod.sourceId}/${lod.pageId} changed during projection.`,
			);
		}
	}

	/** Validates a page-local height record against the manifest catalog snapshot. */
	private assertHeightPageRevision(
		height: EncodedEventPageHeight,
		catalog: EventPageCatalogEntry,
	): void {
		if (height.revision !== catalog.revision) {
			throw new StaleEventStoreProjectionError(
				`Height page ${height.sourceId}/${height.pageId} changed during projection.`,
			);
		}
	}

	/**
	 * Creates a revision-qualified key for one immutable projection page.
	 * @param sourceId Source owning the page.
	 * @param generation Physical source generation.
	 * @param catalog Manifest catalog entry describing the page revision.
	 * @returns Stable cache key.
	 */
	private createProjectionPageCacheKey(
		sourceId: Source.Id,
		generation: number,
		catalog: EventPageCatalogEntry,
	): string {
		return JSON.stringify([
			sourceId,
			generation,
			catalog.pageId,
			catalog.revision,
		]);
	}

	/**
	 * Loads one persisted exact page through the bounded revision-qualified cache.
	 * @param sourceId Source owning the page.
	 * @param generation Physical source generation.
	 * @param catalog Manifest catalog entry identifying the page.
	 * @returns Exact encoded page.
	 */
	private async loadExactPage(
		sourceId: Source.Id,
		generation: number,
		catalog: EventPageCatalogEntry,
	): Promise<EncodedEventPage> {
		const transient = (catalog as ProjectionPageCatalogEntry)[
			TRANSIENT_PAGE_RECORD
		];
		if (transient) return transient.exact;
		const cacheKey = this.createProjectionPageCacheKey(
			sourceId,
			generation,
			catalog,
		);
		const cached = this.exactPageCache.get(cacheKey);
		if (cached) return cached;
		const page = await this.backend.get<EncodedEventPage>("pages", [
			sourceId,
			generation,
			catalog.pageId,
		]);
		if (!page) {
			throw new Error(`Missing exact event page ${sourceId}/${catalog.pageId}.`);
		}
		if (page.revision === catalog.revision) {
			this.exactPageCache.set(
				cacheKey,
				sourceId,
				page.encodedByteLength,
				page,
			);
		}
		return page;
	}

	/**
	 * Loads one persisted microblock page through the bounded revision-qualified cache.
	 * @param sourceId Source owning the page.
	 * @param generation Physical source generation.
	 * @param catalog Manifest catalog entry identifying the page.
	 * @param expectedOverrideRevision Optional prepared color-override revision.
	 * @param expectedOverrideSignature Optional prepared override hash signature.
	 * @returns Encoded LOD page.
	 */
	private async loadLodPage(
		sourceId: Source.Id,
		generation: number,
		catalog: EventPageCatalogEntry,
		expectedOverrideRevision?: number,
		expectedOverrideSignature?: string,
	): Promise<EncodedEventPageLod> {
		const transient = (catalog as ProjectionPageCatalogEntry)[
			TRANSIENT_PAGE_RECORD
		];
		if (transient) return transient.lod;
		const cacheKey = this.createProjectionPageCacheKey(
			sourceId,
			generation,
			catalog,
		);
		const cached = this.lodPageCache.get(cacheKey);
		if (
			cached &&
			(expectedOverrideRevision === undefined ||
				(cached.overrideRevision === expectedOverrideRevision &&
					cached.overrideSignature === expectedOverrideSignature))
		) {
			return cached;
		}
		const lod = await this.backend.get<EncodedEventPageLod>("page_lod", [
			sourceId,
			generation,
			catalog.pageId,
			EVENT_PAGE_LOD_RECORD_KIND,
		]);
		if (!lod) {
			throw new Error(`Missing LOD event page ${sourceId}/${catalog.pageId}.`);
		}
		if (lod.revision === catalog.revision) {
			this.lodPageCache.set(
				cacheKey,
				sourceId,
				lod.encodedByteLength,
				lod,
			);
		}
		return lod;
	}

	/**
	 * Loads one persisted height-count page through its independent bounded cache.
	 * @param sourceId Source owning the page.
	 * @param generation Physical source generation.
	 * @param catalog Manifest catalog entry identifying the page.
	 * @returns Encoded height-count page.
	 */
	private async loadHeightPage(
		sourceId: Source.Id,
		generation: number,
		catalog: EventPageCatalogEntry,
	): Promise<EncodedEventPageHeight> {
		const transient = (catalog as ProjectionPageCatalogEntry)[
			TRANSIENT_PAGE_RECORD
		];
		if (transient) return transient.height;
		const cacheKey = this.createProjectionPageCacheKey(
			sourceId,
			generation,
			catalog,
		);
		const cached = this.heightPageCache.get(cacheKey);
		if (cached) return cached;
		const height = await this.backend.get<EncodedEventPageHeight>("page_lod", [
			sourceId,
			generation,
			catalog.pageId,
			EVENT_PAGE_HEIGHT_RECORD_KIND,
		]);
		if (!height) {
			throw new Error(`Missing height event page ${sourceId}/${catalog.pageId}.`);
		}
		if (height.revision === catalog.revision) {
			this.heightPageCache.set(
				cacheKey,
				sourceId,
				height.encodedByteLength,
				height,
			);
		}
		return height;
	}

	/** Creates an empty but protocol-valid source projection. */
	private createEmptyProjection(
		sourceId: Source.Id,
		coveredRange: RenderTimeRange,
		bucketPixelWidth: number,
	): RenderWindowSourceProjection {
		return {
			sourceId,
			sourceAuthorityRevision: 0,
			sourceGeneration: 0,
			sourceRevision: 0,
			sourceStatus: "incomplete",
			coveredRange,
			bucketPixelWidth,
			degradedLod: bucketPixelWidth > 1,
			loadedCount: 0,
			frameEventCount: 0,
			hashRange: { min: 0, max: 0 },
			representativeHandles: new Uint32Array(0),
			representativeTimestamps: new Float64Array(0),
			representativeHashes: new Float64Array(0),
			heightTimestamps: new Float64Array(0),
			heightCounts: new Float64Array(0),
			heightMaximumCount: 0,
			graphTimestamps: new Float64Array(0),
			graphCounts: new Float64Array(0),
		};
	}

	/** Validates finite transform values and non-negative revision authority. */
	private assertDescriptor(descriptor: EventStoreViewportDescriptor): void {
		if (
			!Number.isSafeInteger(descriptor.operationEpoch) ||
			descriptor.operationEpoch < 0 ||
			!Number.isSafeInteger(descriptor.viewportRevision) ||
			descriptor.viewportRevision < 0 ||
			!Number.isSafeInteger(descriptor.projectionRevision) ||
			descriptor.projectionRevision < 0
		) {
			throw new RangeError("Viewport projection revisions must be non-negative integers.");
		}
		if (
			!Number.isFinite(descriptor.frame.min) ||
			!Number.isFinite(descriptor.frame.max) ||
			descriptor.frame.max <= descriptor.frame.min ||
			!Number.isFinite(descriptor.width) ||
			descriptor.width <= 0 ||
			!Number.isFinite(descriptor.scale) ||
			descriptor.scale <= 0 ||
			!Number.isFinite(descriptor.scrollX) ||
			!Number.isFinite(descriptor.horizontalOverscanPixels) ||
			descriptor.horizontalOverscanPixels < 0
		) {
			throw new RangeError("Viewport projection transform must be finite and positive.");
		}
		const maximum =
			descriptor.maxRepresentatives ?? DEFAULT_MAXIMUM_REPRESENTATIVES;
		if (!Number.isSafeInteger(maximum) || maximum < descriptor.sources.length) {
			throw new RangeError(
				"Viewport representative cap must accommodate every visible source.",
			);
		}
		for (const source of descriptor.sources) {
			if (
				!Number.isFinite(source.offset) ||
				!Number.isFinite(source.frequency) ||
				!Number.isSafeInteger(source.settingsRevision) ||
				source.settingsRevision < 0 ||
				!Number.isSafeInteger(source.overrideRevision) ||
				source.overrideRevision < 0 ||
				![...source.overrideHashes].every((hash) => Number.isFinite(hash))
			) {
				throw new RangeError("Viewport source settings are malformed.");
			}
		}
	}
}
