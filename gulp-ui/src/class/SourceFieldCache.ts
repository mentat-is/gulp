import { GulpIndexedDB } from "@/class/IndexedDB";
import type { Filter } from "@/entities/Filter";
import type { Source } from "@/entities/Source";

/** Default safety expiration for cached source field metadata. */
export const SOURCE_FIELD_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

/** IndexedDB store name used for source field metadata. */
const SOURCE_FIELD_CACHE_STORE = "gulp_source_field_cache";

/** IndexedDB instance dedicated to source field metadata. */
const sourceFieldCacheDB = new GulpIndexedDB(
	"gulp_DB",
	SOURCE_FIELD_CACHE_STORE,
);

/** Timestamp value used to compare a source with a stored field cache entry. */
export type SourceFieldCacheUpdatedAt = number | string | null;

/** Persisted source field metadata entry. */
export interface SourceFieldCacheEntry {
	/** Field map returned by /query_fields_by_source. */
	fields: Filter.Options;
	/** Backend source update marker, when available. */
	source_updated_at: SourceFieldCacheUpdatedAt;
	/** Stable source metadata fingerprint used when no update marker is available. */
	source_fingerprint: string;
	/** Wall-clock time when this entry was stored. */
	cached_at: number;
}

/** Storage API used by SourceFieldCache and replaced by in-memory tests. */
export interface SourceFieldCacheStorage {
	/** Reads one cache entry by key. */
	GetConfiguration(configurationKey: string, storeName?: string): Promise<unknown>;
	/** Stores one cache entry by key. */
	UpdateConfiguration(
		configuration: SourceFieldCacheEntry,
		configurationKey: string,
		storeName?: string,
	): Promise<unknown>;
	/** Removes one cache entry by key. */
	DeleteConfiguration(configurationKey: string, storeName?: string): Promise<unknown>;
}

/** Cache operations required by the source field resolver. */
export interface SourceFieldLookupCache {
	/** Reads cached fields for one source. */
	get(server: string, source: Source.Type): Promise<Filter.Options | null>;
	/** Stores fetched fields for one source. */
	set(server: string, source: Source.Type, fields: Filter.Options): Promise<void>;
}

/** Backend fetcher used when cached source fields are unavailable. */
export type SourceFieldFetcher = (source: Source.Type) => Promise<Filter.Options>;

/**
 * Checks whether a value is a plain field map object.
 * @param value Value read from IndexedDB.
 * @returns True when the value can be used as Filter.Options.
 */
function isFieldOptions(value: unknown): value is Filter.Options {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Checks whether a value is a valid source field cache entry.
 * @param value Value read from IndexedDB.
 * @returns True when the value has the persisted entry shape.
 */
function isSourceFieldCacheEntry(value: unknown): value is SourceFieldCacheEntry {
	if (typeof value !== "object" || value === null) return false;

	const entry = value as Partial<SourceFieldCacheEntry>;
	const updatedAt = entry.source_updated_at;

	return (
		isFieldOptions(entry.fields) &&
		(typeof updatedAt === "number" ||
			typeof updatedAt === "string" ||
			updatedAt === null) &&
		typeof entry.source_fingerprint === "string" &&
		typeof entry.cached_at === "number"
	);
}

/**
 * Normalizes source update metadata into a persisted comparison value.
 * @param value Raw source time_updated value.
 * @returns Comparable update marker, or null when unavailable.
 */
function getSourceUpdatedAt(
	value: Source.Type["time_updated"],
): SourceFieldCacheUpdatedAt {
	if (typeof value === "number" && Number.isFinite(value)) return value;
	if (typeof value === "string" && value.trim()) return value;
	return null;
}

/**
 * Builds a stable source fingerprint for cache invalidation when time_updated is missing.
 * @param source Source whose field metadata is cached.
 * @returns String fingerprint derived from stable source metadata.
 */
export function getSourceFieldFingerprint(source: Source.Type): string {
	return JSON.stringify({
		id: source.id,
		operation_id: source.operation_id,
		context_id: source.context_id,
		name: source.name ?? null,
		plugin: source.plugin ?? null,
	});
}

/**
 * Builds the persistent cache key for one source on one backend server.
 * @param server Backend base URL used by the current UI session.
 * @param source Source whose field metadata is cached.
 * @returns Stable IndexedDB key.
 */
export function getSourceFieldCacheKey(
	server: string,
	source: Source.Type,
): string {
	return [
		"source_fields",
		encodeURIComponent(server || "default"),
		source.operation_id,
		source.context_id,
		source.id,
	].join(":");
}

/**
 * Persistent cache for /query_fields_by_source results.
 */
export class SourceFieldCache {
	private readonly storage: SourceFieldCacheStorage;
	private readonly ttlMs: number;
	private readonly now: () => number;

	/**
	 * Creates a source field cache.
	 * @param storage IndexedDB-compatible storage implementation.
	 * @param ttlMs Entry expiration in milliseconds.
	 * @param now Clock function used for cache age checks.
	 */
	constructor(
		storage: SourceFieldCacheStorage = sourceFieldCacheDB,
		ttlMs = SOURCE_FIELD_CACHE_TTL_MS,
		now: () => number = () => Date.now(),
	) {
		this.storage = storage;
		this.ttlMs = ttlMs;
		this.now = now;
	}

	/**
	 * Reads a valid cached field map for one source.
	 * @param server Backend base URL used by the current UI session.
	 * @param source Source whose fields should be loaded.
	 * @returns Cached fields, or null when missing, stale, expired, or unavailable.
	 */
	async get(server: string, source: Source.Type): Promise<Filter.Options | null> {
		const key = getSourceFieldCacheKey(server, source);

		try {
			const entry = await this.storage.GetConfiguration(key);
			if (!isSourceFieldCacheEntry(entry)) {
				if (entry !== undefined) {
					await this.delete(key);
				}
				return null;
			}

			if (this.isExpired(entry)) {
				await this.delete(key);
				return null;
			}

			if (!this.matchesSource(entry, source)) {
				return null;
			}

			return entry.fields;
		} catch {
			return null;
		}
	}

	/**
	 * Stores field metadata for one source.
	 * @param server Backend base URL used by the current UI session.
	 * @param source Source whose fields were fetched.
	 * @param fields Field metadata returned by the backend.
	 * @returns Promise resolved when best-effort persistence completes.
	 */
	async set(
		server: string,
		source: Source.Type,
		fields: Filter.Options,
	): Promise<void> {
		const key = getSourceFieldCacheKey(server, source);
		const entry: SourceFieldCacheEntry = {
			fields,
			source_updated_at: getSourceUpdatedAt(source.time_updated),
			source_fingerprint: getSourceFieldFingerprint(source),
			cached_at: this.now(),
		};

		try {
			await this.storage.UpdateConfiguration(entry, key);
		} catch {
			// IndexedDB persistence is an optimization; callers should keep using server data.
		}
	}

	/**
	 * Checks whether a persisted entry is older than the configured TTL.
	 * @param entry Persisted cache entry.
	 * @returns True when the entry should be refreshed from the backend.
	 */
	private isExpired(entry: SourceFieldCacheEntry): boolean {
		return this.now() - entry.cached_at > this.ttlMs;
	}

	/**
	 * Checks whether a persisted entry still matches the current source metadata.
	 * @param entry Persisted cache entry.
	 * @param source Current source metadata.
	 * @returns True when the entry is fresh for the source.
	 */
	private matchesSource(
		entry: SourceFieldCacheEntry,
		source: Source.Type,
	): boolean {
		const sourceUpdatedAt = getSourceUpdatedAt(source.time_updated);
		if (sourceUpdatedAt !== null || entry.source_updated_at !== null) {
			return entry.source_updated_at === sourceUpdatedAt;
		}

		return entry.source_fingerprint === getSourceFieldFingerprint(source);
	}

	/**
	 * Deletes a cache entry without surfacing IndexedDB failures.
	 * @param key Persistent cache key to remove.
	 * @returns Promise resolved after best-effort deletion.
	 */
	private async delete(key: string): Promise<void> {
		try {
			await this.storage.DeleteConfiguration(key);
		} catch {
			// Deletion is best-effort; stale entries will be ignored on future reads.
		}
	}
}

/** Shared production cache instance for source field metadata. */
export const sourceFieldCache = new SourceFieldCache();

/**
 * Resolves source field metadata with IndexedDB lookup and in-flight request deduplication.
 */
export class SourceFieldResolver {
	private readonly cache: SourceFieldLookupCache;
	private readonly inFlightRequests = new Map<string, Promise<Filter.Options>>();

	/**
	 * Creates a source field resolver.
	 * @param cache Persistent cache used before and after backend requests.
	 */
	constructor(cache: SourceFieldLookupCache = sourceFieldCache) {
		this.cache = cache;
	}

	/**
	 * Loads source fields from cache or a deduplicated backend fetcher.
	 * @param server Backend base URL used by the current UI session.
	 * @param source Source whose fields should be loaded.
	 * @param fetchFields Backend fetcher used on cache miss.
	 * @returns Field metadata keyed by field name.
	 */
	async getFields(
		server: string,
		source: Source.Type,
		fetchFields: SourceFieldFetcher,
	): Promise<Filter.Options> {
		const key = getSourceFieldCacheKey(server, source);
		const existingRequest = this.inFlightRequests.get(key);
		if (existingRequest) {
			return existingRequest;
		}

		const request = this.resolveFields(server, source, fetchFields);
		this.inFlightRequests.set(key, request);
		return request.finally(() => {
			this.inFlightRequests.delete(key);
		});
	}

	/**
	 * Resolves one source field map from the persistent cache or backend.
	 * @param server Backend base URL used by the current UI session.
	 * @param source Source whose fields should be loaded.
	 * @param fetchFields Backend fetcher used on cache miss.
	 * @returns Field metadata keyed by field name.
	 */
	private async resolveFields(
		server: string,
		source: Source.Type,
		fetchFields: SourceFieldFetcher,
	): Promise<Filter.Options> {
		const cachedFields = await this.cache.get(server, source);
		if (cachedFields) {
			return cachedFields;
		}

		const fields = (await fetchFields(source)) ?? {};
		await this.cache.set(server, source, fields);
		return fields;
	}
}

/** Shared production resolver for source field metadata. */
export const sourceFieldResolver = new SourceFieldResolver();
