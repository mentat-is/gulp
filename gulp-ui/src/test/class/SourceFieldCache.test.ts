import type { Filter } from "@/entities/Filter";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import {
	getSourceFieldCacheKey,
	SourceFieldCache,
	type SourceFieldCacheEntry,
	type SourceFieldCacheStorage,
	SourceFieldResolver,
} from "@/class/SourceFieldCache";

class MemorySourceFieldCacheStorage implements SourceFieldCacheStorage {
	readonly entries = new Map<string, unknown>();

	/**
	 * Reads one in-memory cache entry.
	 * @param configurationKey Cache key to read.
	 * @returns Stored entry or undefined.
	 */
	async GetConfiguration(configurationKey: string): Promise<unknown> {
		return this.entries.get(configurationKey);
	}

	/**
	 * Writes one in-memory cache entry.
	 * @param configuration Cache entry to write.
	 * @param configurationKey Cache key to write.
	 * @returns Nothing.
	 */
	async UpdateConfiguration(
		configuration: SourceFieldCacheEntry,
		configurationKey: string,
	): Promise<void> {
		this.entries.set(configurationKey, configuration);
	}

	/**
	 * Deletes one in-memory cache entry.
	 * @param configurationKey Cache key to delete.
	 * @returns Nothing.
	 */
	async DeleteConfiguration(configurationKey: string): Promise<void> {
		this.entries.delete(configurationKey);
	}
}

const SERVER = "http://localhost:8080";

/**
 * Creates a minimal source fixture for cache tests.
 * @param overrides Source fields to override.
 * @returns Source fixture.
 */
function createSource(overrides: Partial<Source.Type> = {}): Source.Type {
	return {
		id: "source-a" as Source.Id,
		operation_id: "operation-a" as Source.Type["operation_id"],
		context_id: "context-a" as Source.Type["context_id"],
		time_updated: 10,
		total: 100,
		name: "Source A",
		plugin: "test-plugin",
		timestamp: { min: 1, max: 2 },
		nanotimestamp: { min: BigInt(1), max: BigInt(2) },
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
		...overrides,
	} as Source.Type;
}

describe("SourceFieldCache", () => {
	it("returns cached fields while source freshness metadata matches", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource();
		const fields: Filter.Options = { "event.code": "long" };

		await cache.set(SERVER, source, fields);

		expect(await cache.get(SERVER, source)).toEqual(fields);
	});

	it("expires old entries and removes them from storage", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 50, () => 100);
		const source = createSource();
		const key = getSourceFieldCacheKey(SERVER, source);

		await cache.set(SERVER, source, { "event.code": "long" });
		const expiredCache = new SourceFieldCache(storage, 50, () => 151);

		expect(await expiredCache.get(SERVER, source)).toBeNull();
		expect(storage.entries.has(key)).toBe(false);
	});

	it("invalidates entries when source time_updated changes", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource({ time_updated: 10 });

		await cache.set(SERVER, source, { "event.code": "long" });

		expect(
			await cache.get(SERVER, createSource({ time_updated: 11 })),
		).toBeNull();
	});

	it("keeps entries with matching time_updated when mutable source counters change", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource({ time_updated: 10, total: 100 });
		const fields: Filter.Options = { "event.code": "long" };

		await cache.set(SERVER, source, fields);

		expect(
			await cache.get(
				SERVER,
				createSource({
					time_updated: 10,
					total: 200,
					timestamp: { min: 5, max: 10 },
					nanotimestamp: { min: BigInt(5), max: BigInt(10) },
				}),
			),
		).toEqual(fields);
	});

	it("invalidates entries when fallback fingerprint changes without time_updated", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource({ time_updated: null, plugin: "test-plugin" });

		await cache.set(SERVER, source, { "event.code": "long" });

		expect(
			await cache.get(
				SERVER,
				createSource({ time_updated: null, plugin: "changed-plugin" }),
			),
		).toBeNull();
	});

	it("ignores corrupt entries and deletes them", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource();
		const key = getSourceFieldCacheKey(SERVER, source);
		storage.entries.set(key, { fields: null });

		expect(await cache.get(SERVER, source)).toBeNull();
		expect(storage.entries.has(key)).toBe(false);
	});

	it("does not persist empty field maps and removes an existing entry", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource();
		const key = getSourceFieldCacheKey(SERVER, source);

		await cache.set(SERVER, source, { "event.code": "long" });
		expect(storage.entries.has(key)).toBe(true);

		await cache.set(SERVER, source, {});

		expect(storage.entries.has(key)).toBe(false);
		expect(await cache.get(SERVER, source)).toBeNull();
	});

	it("discards empty field maps persisted by older clients", async () => {
		const storage = new MemorySourceFieldCacheStorage();
		const cache = new SourceFieldCache(storage, 1000, () => 100);
		const source = createSource();
		const key = getSourceFieldCacheKey(SERVER, source);
		storage.entries.set(key, {
			fields: {},
			source_updated_at: source.time_updated,
			source_fingerprint: "legacy",
			cached_at: 100,
		} satisfies SourceFieldCacheEntry);

		expect(await cache.get(SERVER, source)).toBeNull();
		expect(storage.entries.has(key)).toBe(false);
	});
});

describe("SourceFieldResolver", () => {
	it("shares concurrent same-source backend requests", async () => {
		const source = createSource();
		const fields: Filter.Options = { "event.code": "long" };
		const cache = {
			get: jest.fn<Promise<Filter.Options | null>, [string, Source.Type]>(
				async () => null,
			),
			set: jest.fn<Promise<void>, [string, Source.Type, Filter.Options]>(
				async () => undefined,
			),
		};
		const fetchFields = jest.fn<Promise<Filter.Options>, [Source.Type]>(
			async () => fields,
		);
		const resolver = new SourceFieldResolver(cache);

		const [first, second] = await Promise.all([
			resolver.getFields(SERVER, source, fetchFields),
			resolver.getFields(SERVER, source, fetchFields),
		]);

		expect(first).toEqual(fields);
		expect(second).toEqual(fields);
		expect(fetchFields).toHaveBeenCalledTimes(1);
		expect(cache.set).toHaveBeenCalledTimes(1);
	});

	it("uses separate backend requests for different sources", async () => {
		const firstSource = createSource({ id: "source-a" as Source.Id });
		const secondSource = createSource({ id: "source-b" as Source.Id });
		const cache = {
			get: jest.fn<Promise<Filter.Options | null>, [string, Source.Type]>(
				async () => null,
			),
			set: jest.fn<Promise<void>, [string, Source.Type, Filter.Options]>(
				async () => undefined,
			),
		};
		const fetchFields = jest.fn<Promise<Filter.Options>, [Source.Type]>(
			async (source) => ({ [source.id]: "keyword" }) as Filter.Options,
		);
		const resolver = new SourceFieldResolver(cache);

		const [first, second] = await Promise.all([
			resolver.getFields(SERVER, firstSource, fetchFields),
			resolver.getFields(SERVER, secondSource, fetchFields),
		]);

		expect(first).toEqual({ "source-a": "keyword" });
		expect(second).toEqual({ "source-b": "keyword" });
		expect(fetchFields).toHaveBeenCalledTimes(2);
	});

	it("returns cached fields without calling the backend fetcher", async () => {
		const source = createSource();
		const fields: Filter.Options = { "source.ip": "ip" };
		const cache = {
			get: jest.fn<Promise<Filter.Options | null>, [string, Source.Type]>(
				async () => fields,
			),
			set: jest.fn<Promise<void>, [string, Source.Type, Filter.Options]>(
				async () => undefined,
			),
		};
		const fetchFields = jest.fn<Promise<Filter.Options>, [Source.Type]>(
			async () => ({ "event.code": "long" }),
		);
		const resolver = new SourceFieldResolver(cache);

		expect(await resolver.getFields(SERVER, source, fetchFields)).toEqual(fields);
		expect(fetchFields).toHaveBeenCalledTimes(0);
		expect(cache.set).toHaveBeenCalledTimes(0);
	});

	it("retries after an empty response and caches the later completed catalog", async () => {
		const source = createSource();
		const completedFields: Filter.Options = { "event.code": "long" };
		const cache = {
			get: jest.fn<Promise<Filter.Options | null>, [string, Source.Type]>(
				async () => null,
			),
			set: jest.fn<Promise<void>, [string, Source.Type, Filter.Options]>(
				async () => undefined,
			),
		};
		let fetchCount = 0;
		const fetchFields = jest.fn<Promise<Filter.Options>, [Source.Type]>(
			async () => {
				fetchCount += 1;
				return fetchCount === 1 ? {} : completedFields;
			},
		);
		const resolver = new SourceFieldResolver(cache);

		expect(await resolver.getFields(SERVER, source, fetchFields)).toEqual({});
		expect(await resolver.getFields(SERVER, source, fetchFields)).toEqual(
			completedFields,
		);
		expect(fetchFields).toHaveBeenCalledTimes(2);
		expect(cache.set).toHaveBeenCalledTimes(1);
		expect(cache.set).toHaveBeenCalledWith(SERVER, source, completedFields);
	});
});
