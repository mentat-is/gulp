import type { Operation } from "@/entities/Operation";
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
				coordinatorId: "coordinator:one",
				operationId: "operation/two" as Operation.Id,
			}),
		).toBe("gulp-event-store:coordinator%3Aone:operation%2Ftwo");
	});

	it("rebuilds version one and two caches for the version-three page schema", () => {
		expect(EVENT_STORE_DATABASE_VERSION).toBe(3);
		expect(shouldRebuildEventStoreCache(0)).toBe(false);
		expect(shouldRebuildEventStoreCache(1)).toBe(true);
		expect(shouldRebuildEventStoreCache(2)).toBe(true);
		expect(shouldRebuildEventStoreCache(3)).toBe(false);
	});

	it("rejects reads when IndexedDB is unavailable or the backend is closed", async () => {
		const backend = new IndexedDbEventStoreBackend(
			{
				coordinatorId: "coordinator",
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
