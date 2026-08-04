import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { EventDocumentCache } from "@/event-store/EventStore.cache";
import { EventStoreFacade } from "@/event-store/EventStore.facade";

const SOURCE_ID = "facade-source" as Source.Id;

/** Creates one descending event fixture. */
function createDocument(id: string, timestamp: number): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": timestamp,
		number_hash: timestamp,
	};
}

describe("EventStoreFacade rollout fallback", () => {
	const events = [
		createDocument("newest", 300),
		createDocument("middle", 200),
		createDocument("oldest", 100),
	];
	const facade = new EventStoreFacade({
		resolveById: (documentId) =>
			events.find((event) => event._id === documentId),
		resolveSource: () => events,
		resolveSourceRevision: () => 7,
	});

	it("resolves ID and centered neighbor reads asynchronously", async () => {
		expect((await facade.getById("middle" as Doc.Id))?._id).toBe("middle");
		const neighbors = await facade.getNeighbors(
			SOURCE_ID,
			"middle" as Doc.Id,
			1,
			1,
		);
		expect(neighbors.events.map((event) => event._id)).toEqual([
			"newest",
			"middle",
			"oldest",
		]);
		expect(neighbors.currentIndex).toBe(1);
		expect(neighbors.sourceRevision).toBe(7);
	});

	it("caps pixel results and returns a revision-bound cursor", async () => {
		const firstPage = await facade.queryPixelBucket(
			SOURCE_ID,
			{ min: 100, max: 300 },
			2,
		);
		expect(firstPage.events.map((event) => event._id)).toEqual([
			"newest",
			"middle",
		]);
		expect(firstPage.total).toBe(3);
		expect(firstPage.nextCursor?.sourceRevision).toBe(7);

		const secondPage = await facade.queryPixelBucket(
			SOURCE_ID,
			{ min: 100, max: 300 },
			2,
			firstPage.nextCursor ?? undefined,
		);
		expect(secondPage.events.map((event) => event._id)).toEqual(["oldest"]);
	});

	it("keeps explicit interaction results inside the bounded cache", async () => {
		const boundedFacade = new EventStoreFacade(
			{},
			new EventDocumentCache(2),
		);
		boundedFacade.rememberDocuments(events);

		expect(boundedFacade.cacheSize).toBe(2);
		expect(boundedFacade.peekDocument(events[2]._id)).toEqual(events[2]);
		expect(await boundedFacade.getById(events[0]._id)).toBeNull();
		expect(await boundedFacade.getById(events[2]._id)).toEqual(events[2]);
	});
});
