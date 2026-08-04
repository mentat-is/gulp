import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { EventDocumentCache } from "@/event-store/EventStore.cache";

const SOURCE_ID = "cache-source" as Source.Id;

/** Creates one minimal event cache fixture. */
function createDocument(id: string): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: 1,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": 1,
		number_hash: 1,
	};
}

describe("EventDocumentCache", () => {
	it("evicts the least recently used document", () => {
		const cache = new EventDocumentCache(2);
		cache.set(createDocument("first"));
		cache.set(createDocument("second"));
		expect(cache.get("first" as Doc.Id)?._id).toBe("first");
		cache.set(createDocument("third"));

		expect(cache.get("second" as Doc.Id)).toBeUndefined();
		expect(cache.get("first" as Doc.Id)?._id).toBe("first");
		expect(cache.size).toBe(2);
	});

	it("replaces an existing document without growing", () => {
		const cache = new EventDocumentCache(2);
		cache.set(createDocument("same"));
		cache.set({ ...createDocument("same"), number_hash: 99 });

		expect(cache.size).toBe(1);
		expect(cache.get("same" as Doc.Id)?.number_hash).toBe(99);
	});
});
