import type { Operation } from "@/entities/Operation";
import { EVENT_STORE_NAMES } from "@/event-store/EventStore.backend";
import {
	createEventStoreDatabaseName,
	EVENT_STORE_DATABASE_VERSION,
	IndexedDbEventStoreBackend,
	shouldRebuildEventStoreCache,
} from "@/event-store/IndexedDbEventStore.backend";

describe("IndexedDbEventStoreBackend", () => {
	it("creates an operation-scoped and separator-safe database name", () => {
		expect(
			createEventStoreDatabaseName({
				cacheNamespaceId: "user:one",
				operationId: "operation/two" as Operation.Id,
			}),
		).toBe("gulp-event-store:user%3Aone:operation%2Ftwo");
	});

	it("rebuilds older caches for the version-six event-sequence column", () => {
		expect(EVENT_STORE_DATABASE_VERSION).toBe(6);
		expect(EVENT_STORE_NAMES).toEqual([
			"sources",
			"source_query_caches",
			"pages",
			"page_lod",
			"document_index_pages",
			"stream_journal",
		]);
		expect(shouldRebuildEventStoreCache(0)).toBe(false);
		expect(shouldRebuildEventStoreCache(1)).toBe(true);
		expect(shouldRebuildEventStoreCache(2)).toBe(true);
		expect(shouldRebuildEventStoreCache(3)).toBe(true);
		expect(shouldRebuildEventStoreCache(4)).toBe(true);
		expect(shouldRebuildEventStoreCache(5)).toBe(true);
		expect(shouldRebuildEventStoreCache(6)).toBe(false);
	});

	it("rejects reads when IndexedDB is unavailable or the backend is closed", async () => {
		const backend = new IndexedDbEventStoreBackend(
			{
				cacheNamespaceId: "user:test",
				operationId: "operation" as Operation.Id,
			},
			null as unknown as IDBFactory,
		);
		await expect(backend.get("sources", "source")).rejects.toThrow(
			"IndexedDB is unavailable",
		);

		backend.close();
		await expect(backend.get("sources", "source")).rejects.toThrow(
			"EventStore backend is closed",
		);
	});
});
