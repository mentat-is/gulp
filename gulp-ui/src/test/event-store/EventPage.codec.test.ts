import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { TextDecoder, TextEncoder } from "util";
import {
	buildEventPageLod,
	createEventPageCatalogEntry,
	decodeEventAt,
	encodeEventPage,
	hashEventDocumentId,
} from "@/event-store/EventPage.codec";

const SOURCE_ID = "source-codec" as Source.Id;

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

/** Creates one minimal canonical document for codec coverage. */
function createDocument(id: string, timestamp: number, hash: number): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": hash + 10,
		number_hash: hash,
	};
}

describe("EventPage codec", () => {
	it("encodes, sorts, and decodes exact documents", () => {
		const page = encodeEventPage({
			sourceId: SOURCE_ID,
			generation: 2,
			pageId: 7,
			revision: 4,
			documents: [
				createDocument("older", 100, 1),
				createDocument("newer", 300, 3),
				createDocument("middle", 200, 2),
			],
			handles: new Uint32Array([10, 30, 20]),
		});

		expect(page.timestamps).toEqual(new Float64Array([300, 200, 100]));
		expect(page.handles).toEqual(new Uint32Array([30, 20, 10]));
		expect(decodeEventAt(page, 0)?._id).toBe("newer");
		expect(decodeEventAt(page, 2)?._id).toBe("older");
		expect(page.encodedByteLength > 0).toBe(true);
	});

	it("keeps the final occurrence of an ID before page sorting", () => {
		const page = encodeEventPage({
			sourceId: SOURCE_ID,
			generation: 1,
			pageId: 1,
			revision: 1,
			documents: [
				createDocument("duplicate", 300, 1),
				createDocument("stable", 200, 2),
				createDocument("duplicate", 100, 9),
			],
			handles: new Uint32Array([1, 2, 3]),
		});

		expect(page.count).toBe(2);
		expect(Array.from(page.handles)).toEqual([2, 3]);
		expect(decodeEventAt(page, 1)?.number_hash).toBe(9);
	});

	it("orders equal timestamps by descending event.sequence", () => {
		const documents = [
			{ ...createDocument("third", 100, 3), "event.sequence": 30 },
			{ ...createDocument("first", 100, 1), "event.sequence": 10 },
			{ ...createDocument("second", 100, 2), "event.sequence": 20 },
		];
		const page = encodeEventPage({
			sourceId: SOURCE_ID,
			generation: 1,
			pageId: 1,
			revision: 1,
			documents,
		});

		expect(Array.from(page.sequences)).toEqual([30, 20, 10]);
		expect(
			Array.from({ length: page.count }, (_, offset) =>
				decodeEventAt(page, offset)?._id,
			),
		).toEqual(["third", "second", "first"]);
		expect(decodeEventAt(page, 1)?.["event.sequence"]).toBe(20);
	});

	it("skips tombstoned documents and builds bounded LOD blocks", () => {
		const documents = Array.from({ length: 600 }, (_, index) =>
			createDocument(`event-${index}`, 10_000 - index, index),
		);
		const page = encodeEventPage({
			sourceId: SOURCE_ID,
			generation: 1,
			pageId: 1,
			revision: 1,
			documents,
		});
		page.tombstones[0] = 1;
		page.activeCount--;

		const lod = buildEventPageLod(page);
		const catalog = createEventPageCatalogEntry(page);

		expect(lod.blockCount <= 256).toBe(true);
		expect(lod.counts.reduce((total, count) => total + count, 0)).toBe(599);
		expect(catalog.representativeHandle).toBe(page.handles[1]);
		expect(decodeEventAt(page, 0)).toBeUndefined();
	});

	it("creates stable hash prefixes and keeps IDs available for collision checks", () => {
		const first = hashEventDocumentId("event-a" as Doc.Id);
		const second = hashEventDocumentId("event-a" as Doc.Id);
		const different = hashEventDocumentId("event-b" as Doc.Id);

		expect(first).toEqual(second);
		expect(first.prefix >= 0).toBe(true);
		expect(first.prefix < 4096).toBe(true);
		expect(first.high === different.high && first.low === different.low).toBe(false);
	});
});
