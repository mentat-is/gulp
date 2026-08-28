import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { EventDocumentCache } from "@/event-store/EventStore.cache";
import { EventStoreFacade } from "@/event-store/EventStore.facade";
import type { EventStoreReader } from "@/event-store/EventStore.reader";

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

describe("EventStoreFacade supplemental navigation", () => {
	const queryEvents = [
		createDocument("newest", 300),
		createDocument("middle", 200),
		createDocument("oldest", 100),
	];
	const supplementalEvents = [
		createDocument("included-newer", 250),
		createDocument("included-older", 150),
	];
	const reader: EventStoreReader = {
		getSourceSummary: async () => null,
		getById: async (documentId) =>
			queryEvents.find((event) => event._id === documentId) ?? null,
		getByIds: async (documentIds) =>
			documentIds.flatMap((documentId) => {
				const event = queryEvents.find((candidate) => candidate._id === documentId);
				return event ? [event] : [];
			}),
		getByHandle: async () => null,
		getNeighbors: async (_sourceId, documentId) => {
			const currentIndex = queryEvents.findIndex(
				(event) => event._id === documentId,
			);
			return {
				events: currentIndex === -1 ? [] : queryEvents,
				currentIndex,
				sourceRevision: 11,
			};
		},
		getTimestampNeighbors: async (_sourceId, timestamp) => ({
			newer: queryEvents.filter((event) => event.gulp_timestamp >= timestamp),
			older: queryEvents.filter((event) => event.gulp_timestamp < timestamp),
			sourceRevision: 11,
		}),
		queryPixelBucket: async () => ({
			events: [],
			total: 0,
			nextCursor: null,
			sourceRevision: 11,
		}),
	};

	it("merges supplemental events into a persisted event window", async () => {
		const facade = new EventStoreFacade({
			resolveSupplementalById: (documentId) =>
				supplementalEvents.find((event) => event._id === documentId),
			resolveSupplementalSource: () => supplementalEvents,
		});
		facade.useReader(reader);

		const window = await facade.getNeighbors(
			SOURCE_ID,
			"middle" as Doc.Id,
			2,
			2,
		);

		expect(window.events.map((event) => event._id)).toEqual([
			"newest",
			"included-newer",
			"middle",
			"included-older",
			"oldest",
		]);
		expect(window.currentIndex).toBe(2);
	});

	it("centers a supplemental event through timestamp-anchored query reads", async () => {
		const facade = new EventStoreFacade({
			resolveSupplementalById: (documentId) =>
				supplementalEvents.find((event) => event._id === documentId),
			resolveSupplementalSource: () => supplementalEvents,
		});
		facade.useReader(reader);

		const window = await facade.getNeighbors(
			SOURCE_ID,
			"included-newer" as Doc.Id,
			1,
			2,
		);

		expect(window.events.map((event) => event._id)).toEqual([
			"newest",
			"included-newer",
			"middle",
			"included-older",
		]);
		expect(window.currentIndex).toBe(1);
		expect(window.sourceRevision).toBe(11);
	});

	it("keeps query order ahead of supplemental IDs at equal timestamps", async () => {
		const equalQueryEvents = [
			createDocument("query-first", 200),
			createDocument("query-second", 200),
			createDocument("query-older", 100),
		];
		const equalSupplementalEvents = [
			createDocument("included-a", 200),
			createDocument("included-z", 200),
		];
		const equalReader: EventStoreReader = {
			...reader,
			getById: async () => null,
			getNeighbors: async () => ({
				events: [],
				currentIndex: -1,
				sourceRevision: 12,
			}),
			getTimestampNeighbors: async () => ({
				newer: equalQueryEvents.slice(0, 2),
				older: equalQueryEvents.slice(2),
				sourceRevision: 12,
			}),
		};
		const facade = new EventStoreFacade({
			resolveSupplementalById: (documentId) =>
				equalSupplementalEvents.find((event) => event._id === documentId),
			resolveSupplementalSource: () => equalSupplementalEvents,
		});
		facade.useReader(equalReader);

		const window = await facade.getNeighbors(
			SOURCE_ID,
			"included-a" as Doc.Id,
			2,
			2,
		);

		expect(window.events.map((event) => event._id)).toEqual([
			"query-first",
			"query-second",
			"included-a",
			"included-z",
			"query-older",
		]);
		expect(window.currentIndex).toBe(2);
	});
});
