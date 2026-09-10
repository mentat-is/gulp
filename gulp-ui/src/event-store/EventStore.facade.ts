import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { EventDocumentCache } from "./EventStore.cache";
import { compareEventDocuments } from "./EventStore.order";
import type {
	EventNeighborWindow,
	EventSourceEdge,
	EventStoreReader,
	EventTimestampNeighborWindow,
	EventTimestampRange,
} from "./EventStore.reader";
import {
	EVENT_PIXEL_PAGE_SIZE,
	type EventPageCursor,
	type EventPixelPage,
	type EventSourceSummary,
} from "./EventStore.types";

export interface EventStoreFacadeFallbacks {
	resolveById?: (documentId: Doc.Id) => Doc.Type | undefined;
	resolveSource?: (sourceId: Source.Id) => readonly Doc.Type[];
	resolveSupplementalById?: (documentId: Doc.Id) => Doc.Type | undefined;
	resolveSupplementalSource?: (sourceId: Source.Id) => readonly Doc.Type[];
	resolveSourceRevision?: (sourceId: Source.Id) => number;
}

/** Async, bounded event access facade retained on DataStore during migration. */
export class EventStoreFacade implements EventStoreReader {
	private readonly cache: EventDocumentCache;
	private readonly fallbacks: EventStoreFacadeFallbacks;
	private reader: EventStoreReader | null = null;

	/**
	 * Creates a facade with optional legacy resolvers used only during rollout.
	 * @param fallbacks Read-only access to current canonical RAM structures.
	 * @param cache Main-realm bounded cache, injectable for focused tests.
	 */
	public constructor(
		fallbacks: EventStoreFacadeFallbacks = {},
		cache = new EventDocumentCache(),
	) {
		this.fallbacks = fallbacks;
		this.cache = cache;
	}

	/** Returns the bounded document count retained by this facade. */
	public get cacheSize(): number {
		return this.cache.size;
	}

	/**
	 * Installs the IndexedDB-backed reader for the selected operation namespace.
	 * @param reader Reader opened by this browser realm.
	 * @returns Nothing.
	 */
	public useReader(reader: EventStoreReader): void {
		this.reader = reader;
		this.cache.clear();
	}

	/**
	 * Releases the selected reader and every cached event at an operation boundary.
	 * @returns Nothing.
	 */
	public reset(): void {
		this.reader = null;
		this.cache.clear();
	}

	/**
	 * Retains a small interaction result without adding it to a source-wide RAM array.
	 * @param documents Exact documents returned by a bounded backend or IndexedDB read.
	 * @returns The retained documents in caller order.
	 */
	public rememberDocuments(documents: readonly Doc.Type[]): Doc.Type[] {
		return this.cache.setMany(documents);
	}

	/**
	 * Evicts bounded interaction copies whose canonical transient owner was cleared.
	 * @param documentIds Document identifiers to forget.
	 * @returns Number of cache entries removed.
	 */
	public forgetDocuments(documentIds: readonly Doc.Id[]): number {
		let removed = 0;
		for (const documentId of documentIds) {
			if (this.cache.delete(documentId)) removed++;
		}
		return removed;
	}

	/**
	 * Reads one already-bounded interaction document without starting async storage work.
	 * @param documentId Document identifier to inspect.
	 * @returns Cached document, or undefined when an IndexedDB lookup is still required.
	 */
	public peekDocument(documentId: Doc.Id): Doc.Type | undefined {
		return (
			this.cache.get(documentId) ??
			this.fallbacks.resolveSupplementalById?.(documentId) ??
			this.fallbacks.resolveById?.(documentId)
		);
	}

	/**
	 * Returns compact source authority without reading any exact event page.
	 * @param sourceId Source whose summary is requested.
	 * @returns Persisted or rollout-fallback source summary.
	 */
	public async getSourceSummary(
		sourceId: Source.Id,
	): Promise<EventSourceSummary | null> {
		if (this.reader) return this.reader.getSourceSummary(sourceId);
		const events = this.fallbacks.resolveSource?.(sourceId);
		if (!events) return null;
		let hashMin = Number.POSITIVE_INFINITY;
		let hashMax = Number.NEGATIVE_INFINITY;
		for (const event of events) {
			hashMin = Math.min(hashMin, event.number_hash);
			hashMax = Math.max(hashMax, event.number_hash);
		}
		return {
			sourceId,
			generation: 0,
			revision: this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0,
			authorityRevision: this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0,
			status: "complete",
			eventCount: events.length,
			minTimestamp: events[events.length - 1]?.gulp_timestamp ?? 0,
			maxTimestamp: events[0]?.gulp_timestamp ?? 0,
			hashMin: events.length === 0 ? 0 : hashMin,
			hashMax: events.length === 0 ? 0 : hashMax,
			activeCacheGeneration: null,
			cacheHistory: [],
			cacheHistoryCursor: -1,
			nextGeneration: 1,
		};
	}

	/**
	 * Resolves one canonical source edge without materializing the full source.
	 * @param sourceId Source whose edge is requested.
	 * @param newest Whether to select the newest rather than the oldest event.
	 * @returns Exact edge and the source revision used to select it.
	 */
	public async getSourceEdge(
		sourceId: Source.Id,
		newest: boolean,
	): Promise<EventSourceEdge> {
		if (this.reader) {
			const edge = await this.reader.getSourceEdge(sourceId, newest);
			if (edge.event) this.cache.set(edge.event);
			return edge;
		}

		let selected: Doc.Type | null = null;
		for (const event of this.fallbacks.resolveSource?.(sourceId) ?? []) {
			if (!selected) {
				selected = event;
				continue;
			}
			const comparison = compareEventDocuments(event, selected);
			if (newest ? comparison < 0 : comparison > 0) selected = event;
		}
		if (selected) this.cache.set(selected);
		return {
			event: selected,
			sourceRevision: this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0,
		};
	}

	/**
	 * Resolves one event through the bounded cache and current operation reader.
	 * @param documentId Canonical document identifier.
	 * @param sourceId Optional source collision discriminator.
	 * @returns Matching minimal document, or null when unavailable.
	 */
	public async getById(
		documentId: Doc.Id,
		sourceId?: Source.Id,
	): Promise<Doc.Type | null> {
		const cached = this.cache.get(documentId);
		if (!sourceId || cached?.["gulp.source_id"] === sourceId) {
			if (cached) return cached;
		}
		const persisted = await this.reader?.getById(documentId, sourceId);
		if (persisted) return this.cache.set(persisted);
		const supplemental =
			this.fallbacks.resolveSupplementalById?.(documentId);
		if (
			supplemental &&
			(!sourceId || supplemental["gulp.source_id"] === sourceId)
		) {
			return supplemental;
		}
		const fallback = this.fallbacks.resolveById?.(documentId);
		if (fallback && (!sourceId || fallback["gulp.source_id"] === sourceId)) {
			return this.cache.set(fallback);
		}
		return null;
	}

	/**
	 * Resolves a bounded ID collection while preserving caller order for found IDs.
	 * @param documentIds Document identifiers to resolve.
	 * @returns Matching documents in caller ID order.
	 */
	public async getByIds(documentIds: readonly Doc.Id[]): Promise<Doc.Type[]> {
		const uniqueIds = [...new Set(documentIds)];
		const documentsById = new Map<Doc.Id, Doc.Type>();
		const missingIds: Doc.Id[] = [];
		for (const documentId of uniqueIds) {
			const cached = this.cache.get(documentId);
			if (cached) documentsById.set(documentId, cached);
			else missingIds.push(documentId);
		}
		if (this.reader && missingIds.length > 0) {
			const persisted = await this.reader.getByIds(missingIds);
			for (const document of persisted) {
				this.cache.set(document);
				documentsById.set(document._id, document);
			}
		}
		for (const documentId of missingIds) {
			if (documentsById.has(documentId)) continue;
			const supplemental =
				this.fallbacks.resolveSupplementalById?.(documentId);
			if (supplemental) {
				documentsById.set(documentId, supplemental);
				continue;
			}
			const fallback = this.fallbacks.resolveById?.(documentId);
			if (!fallback) continue;
			this.cache.set(fallback);
			documentsById.set(documentId, fallback);
		}
		return documentIds
			.map((documentId) => documentsById.get(documentId))
			.filter((document): document is Doc.Type => Boolean(document));
	}

	/**
	 * Resolves one source-local stable handle.
	 * @param sourceId Source that owns the handle.
	 * @param handle Non-zero source-local handle.
	 * @returns Matching minimal document, or null without an IndexedDB reader.
	 */
	public async getByHandle(
		sourceId: Source.Id,
		handle: number,
	): Promise<Doc.Type | null> {
		if (this.reader) {
			const document = await this.reader.getByHandle(sourceId, handle);
			return document ? this.cache.set(document) : null;
		}
		return null;
	}

	/**
	 * Reads a centered, bounded source window without materializing the source.
	 * @param sourceId Source containing the current event.
	 * @param documentIdOrHandle Current document identifier or revision-scoped handle.
	 * @param before Number of newer descending entries to include.
	 * @param after Number of older descending entries to include.
	 * @returns Descending event window and current index.
	 */
	public async getNeighbors(
		sourceId: Source.Id,
		documentIdOrHandle: Doc.Id | number,
		before: number,
		after: number,
	): Promise<EventNeighborWindow> {
		if (this.reader) {
			const persistedWindow = await this.reader.getNeighbors(
				sourceId,
				documentIdOrHandle,
				before,
				after,
			);
			const supplementalEvents =
				this.fallbacks.resolveSupplementalSource?.(sourceId) ?? [];
			if (
				persistedWindow.currentIndex >= 0 ||
				typeof documentIdOrHandle === "number"
			) {
				const current = persistedWindow.events[persistedWindow.currentIndex];
				const merged = current
					? this.mergeNeighborEvents(
							current,
							persistedWindow.events,
							supplementalEvents,
							before,
							after,
							persistedWindow.sourceRevision,
						)
					: persistedWindow;
				this.cache.setMany(merged.events);
				return merged;
			}

			const supplemental = supplementalEvents.find(
				(event) => event._id === documentIdOrHandle,
			);
			if (!supplemental) return persistedWindow;
			const timestampWindow = await this.reader.getTimestampNeighbors(
				sourceId,
				supplemental.gulp_timestamp,
				before,
				after,
			);
			const merged = this.mergeNeighborEvents(
				supplemental,
				[...timestampWindow.newer, ...timestampWindow.older],
				supplementalEvents,
				before,
				after,
				timestampWindow.sourceRevision,
			);
			this.cache.setMany(merged.events);
			return merged;
		}
		const events = this.mergeDescendingEvents(
			this.fallbacks.resolveSource?.(sourceId) ?? [],
			this.fallbacks.resolveSupplementalSource?.(sourceId) ?? [],
		);
		const currentIndex =
			typeof documentIdOrHandle === "number"
				? -1
				: events.findIndex((event) => event._id === documentIdOrHandle);
		if (currentIndex === -1) {
			return {
				events: [],
				currentIndex: -1,
				sourceRevision: this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0,
			};
		}
		const start = Math.max(0, currentIndex - Math.max(0, before));
		const end = Math.min(events.length, currentIndex + Math.max(0, after) + 1);
		const windowEvents = events.slice(start, end);
		this.cache.setMany(windowEvents);
		return {
			events: windowEvents,
			currentIndex: currentIndex - start,
			sourceRevision: this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0,
		};
	}

	/**
	 * Merges canonical query order with timestamp-sorted supplemental events.
	 * Query events win duplicate IDs and timestamp ties.
	 * @param queryEvents Canonical descending query events.
	 * @param supplementalEvents Explicitly included note targets.
	 * @returns Descending unique event sequence.
	 */
	private mergeDescendingEvents(
		queryEvents: readonly Doc.Type[],
		supplementalEvents: readonly Doc.Type[],
	): Doc.Type[] {
		if (supplementalEvents.length === 0) return [...queryEvents];
		const queryIds = new Set(queryEvents.map((event) => event._id));
		const supplemental = [...supplementalEvents]
			.filter((event) => !queryIds.has(event._id))
			.sort(
				(left, right) =>
					compareEventDocuments(left, right) ||
					String(left._id).localeCompare(String(right._id)),
			);
		const merged: Doc.Type[] = [];
		let queryIndex = 0;
		let supplementalIndex = 0;
		while (
			queryIndex < queryEvents.length ||
			supplementalIndex < supplemental.length
		) {
			const queryEvent = queryEvents[queryIndex];
			const supplementalEvent = supplemental[supplementalIndex];
			if (
				queryEvent &&
				(!supplementalEvent ||
					compareEventDocuments(queryEvent, supplementalEvent) <= 0)
			) {
				merged.push(queryEvent);
				queryIndex++;
			} else if (supplementalEvent) {
				merged.push(supplementalEvent);
				supplementalIndex++;
			}
		}
		return merged;
	}

	/**
	 * Centers one event in a bounded window after merging explicit note targets.
	 * @param current Event that must remain present in the returned window.
	 * @param queryEvents Descending active-query neighbors.
	 * @param supplementalEvents Explicit note targets for the source.
	 * @param before Maximum newer entries.
	 * @param after Maximum older entries.
	 * @param sourceRevision Durable source revision associated with the query window.
	 * @returns Bounded descending window centered on the supplied event.
	 */
	private mergeNeighborEvents(
		current: Doc.Type,
		queryEvents: readonly Doc.Type[],
		supplementalEvents: readonly Doc.Type[],
		before: number,
		after: number,
		sourceRevision: number,
	): EventNeighborWindow {
		const merged = this.mergeDescendingEvents(queryEvents, supplementalEvents);
		if (!merged.some((event) => event._id === current._id)) {
			merged.push(current);
			merged.sort(
				(left, right) =>
					compareEventDocuments(left, right) ||
					String(left._id).localeCompare(String(right._id)),
			);
		}
		const currentIndex = merged.findIndex((event) => event._id === current._id);
		const safeBefore = Math.max(0, Math.floor(before));
		const safeAfter = Math.max(0, Math.floor(after));
		const start = Math.max(0, currentIndex - safeBefore);
		const events = merged.slice(start, currentIndex + safeAfter + 1);
		return {
			events,
			currentIndex: currentIndex - start,
			sourceRevision,
		};
	}

	/**
	 * Reads bounded canonical events around a timestamp without requiring an exact ID.
	 * Supplemental events are merged by `getNeighbors`, so this lower-level method
	 * preserves the reader contract for callers that need only query authority.
	 * @param sourceId Source whose event order is inspected.
	 * @param timestamp Virtual insertion timestamp.
	 * @param before Maximum newer events.
	 * @param after Maximum older events.
	 * @returns Query events on either side of the timestamp.
	 */
	public async getTimestampNeighbors(
		sourceId: Source.Id,
		timestamp: number,
		before: number,
		after: number,
	): Promise<EventTimestampNeighborWindow> {
		if (this.reader) {
			const window = await this.reader.getTimestampNeighbors(
				sourceId,
				timestamp,
				before,
				after,
			);
			this.cache.setMany([...window.newer, ...window.older]);
			return window;
		}
		const events = this.fallbacks.resolveSource?.(sourceId) ?? [];
		const safeBefore = Math.max(0, Math.floor(before));
		const safeAfter = Math.max(0, Math.floor(after));
		const newer =
			safeBefore === 0
				? []
				: events
						.filter((event) => event.gulp_timestamp >= timestamp)
						.slice(-safeBefore);
		const older = events
			.filter((event) => event.gulp_timestamp < timestamp)
			.slice(0, safeAfter);
		this.cache.setMany([...newer, ...older]);
		return {
			newer,
			older,
			sourceRevision: this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0,
		};
	}

	/**
	 * Queries one exact timestamp interval and returns no more than the requested page.
	 * @param sourceId Source whose events can occupy the Canvas pixel.
	 * @param range Inclusive raw timestamp interval.
	 * @param limit Requested bound, capped to the product pixel page size.
	 * @param cursor Optional keyset cursor tied to the current source revision.
	 * @returns Exact event page, total count, and continuation cursor.
	 */
	public async queryPixelBucket(
		sourceId: Source.Id,
		range: EventTimestampRange,
		limit: number,
		cursor?: EventPageCursor,
	): Promise<EventPixelPage> {
		const safeLimit = Math.max(
			1,
			Math.min(EVENT_PIXEL_PAGE_SIZE, Math.floor(limit)),
		);
		if (this.reader) {
			const page = await this.reader.queryPixelBucket(
				sourceId,
				range,
				safeLimit,
				cursor,
			);
			this.cache.setMany(page.events);
			return page;
		}

		const sourceRevision = this.fallbacks.resolveSourceRevision?.(sourceId) ?? 0;
		const matches = (this.fallbacks.resolveSource?.(sourceId) ?? []).filter(
			(event) =>
				event.gulp_timestamp >= Math.min(range.min, range.max) &&
				event.gulp_timestamp <= Math.max(range.min, range.max),
		);
		const start = cursor
			? Math.max(
					0,
					matches.findIndex((event) => event._id === cursor.documentId) + 1,
				)
			: 0;
		const events = matches.slice(start, start + safeLimit);
		this.cache.setMany(events);
		const finalEvent = events[events.length - 1];
		return {
			events,
			total: matches.length,
			nextCursor:
				start + events.length < matches.length && finalEvent
					? {
						sourceId,
						sourceRevision,
						timestamp: finalEvent.gulp_timestamp,
						documentId: finalEvent._id,
					}
					: null,
			sourceRevision,
		};
	}
}
