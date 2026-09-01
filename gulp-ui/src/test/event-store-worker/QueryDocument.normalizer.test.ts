import type { Doc } from "@/entities/Doc";
import {
	normalizeQueryDocuments,
	normalizeQueryDocumentsCooperatively,
} from "@/event-store-worker/QueryDocument.normalizer";
import type { DocumentStreamDescriptor } from "@/event-stream";

const descriptor: DocumentStreamDescriptor = {
	requestId: "normalizer-request",
	operationId: "operation-one",
	operationEpoch: 1,
	owner: "persist",
	kind: "query-upsert",
	sourceIds: [],
	defaultSourceSettings: { field: "category", hash_function: "djb2" },
	sourceSettingsById: {
		"source-special": { field: "nested.value", hash_function: "sdbm" },
	},
};

/** Returns the only normalized document produced by one raw fixture. */
function normalizeOne(
	raw: Record<string, unknown>,
	stream = descriptor,
): Doc.Type {
	const groups = normalizeQueryDocuments([raw], stream);
	const documents = [...groups.values()].flat();
	if (documents.length !== 1) throw new Error("Expected one normalized document.");
	return documents[0];
}

describe("normalizeQueryDocuments", () => {
	it("preserves DataWorker timestamp-unit semantics", () => {
		const cases: Array<[number, number]> = [
			[1_700_000_000, 1_700_000_000_000],
			[1_700_000_000_000, 1_700_000_000_000],
			[1_700_000_000_000_000, 1_700_000_000_000],
			[1_700_000_000_000_000_000, 1_700_000_000_000],
			[12_345_678_901, 12_346],
		];
		for (const [timestamp, expected] of cases) {
			const document = normalizeOne({
				_id: "timestamp",
				"gulp.source_id": "source-dynamic",
				"gulp.timestamp": timestamp,
				"gulp.event_code": 1,
				category: "network",
			});
			expect(document.gulp_timestamp).toBe(expected);
		}
	});

	it("prefers @timestamp when backend timestamp fields disagree", () => {
		const document = normalizeOne({
			_id: "timestamp-authority",
			"gulp.source_id": "source-dynamic",
			"@timestamp": "2025-03-26T09:24:39.186065+00:00",
			"gulp.timestamp": 1_700_000_000_000,
			gulp_timestamp: 1_600_000_000_000,
			"gulp.event_code": 1,
			category: "network",
		});

		expect(document.gulp_timestamp).toBe(1_742_981_079_186);
	});

	it("preserves event.sequence for deterministic timestamp ties", () => {
		const document = normalizeOne({
			_id: "sequence",
			"gulp.source_id": "source-dynamic",
			"gulp.timestamp": 1_700_000_000_000,
			"event.sequence": "42",
			"gulp.event_code": 1,
			category: "network",
		});

		expect(document["event.sequence"]).toBe(42);
	});

	it("uses default settings for discovered sources and source-specific overrides", () => {
		const dynamic = normalizeOne({
			_id: "dynamic",
			"gulp.source_id": "source-dynamic",
			"gulp.timestamp": 1_700_000_000_000,
			"gulp.event_code": 1,
			category: "network",
		});
		const special = normalizeOne({
			_id: "special",
			"gulp.source_id": "source-special",
			"gulp.timestamp": 1_700_000_000_000,
			"gulp.event_code": 1,
			"nested.value": "network",
		});

		expect(dynamic.number_hash).not.toBe(0);
		expect(special.number_hash).not.toBe(0);
		expect(special.number_hash).not.toBe(dynamic.number_hash);
	});

	it("normalizes bigint render fields as timestamps and enforces declared authority", () => {
		const bigintHash = normalizeOne({
			_id: "bigint",
			"gulp.source_id": "source-dynamic",
			"gulp.timestamp": 1_700_000_000_000,
			"gulp.event_code": 1,
			category: 1_700_000_000_000_000_000n,
		});
		expect(bigintHash.number_hash).toBe(1_700_000_000_000);

		let authorityError: unknown;
		try {
			normalizeOne(
				{
					_id: "outside",
					"gulp.source_id": "outside-source",
					"gulp.timestamp": 1_700_000_000_000,
					"gulp.event_code": 1,
				},
				{ ...descriptor, sourceIds: ["only-source"] },
			);
		} catch (error) {
			authorityError = error;
		}
		expect(authorityError instanceof Error).toBe(true);
		expect((authorityError as Error).message.includes("outside the registered stream")).toBe(true);
	});

	it("yields between bounded slices without changing order or values", async () => {
		const rawDocuments = [
			{
				_id: "first",
				"gulp.source_id": "source-dynamic",
				"gulp.timestamp": 1_700_000_000_003,
				"gulp.event_code": 1,
				category: "network",
			},
			{
				_id: "second",
				"gulp.source_id": "source-special",
				"gulp.timestamp": 1_700_000_000_002,
				"gulp.event_code": 2,
				"nested.value": "process",
			},
			{
				_id: "third",
				"gulp.source_id": "source-dynamic",
				"gulp.timestamp": 1_700_000_000_001,
				"gulp.event_code": 3,
				category: "file",
			},
		];
		let clock = 0;
		const yieldToEventLoop = jest.fn(async () => undefined);
		const cooperative = await normalizeQueryDocumentsCooperatively(
			rawDocuments,
			descriptor,
			{
				maximumSliceMs: 4,
				now: () => (clock += 3),
				yieldToEventLoop,
			},
		);

		expect(cooperative).toEqual(normalizeQueryDocuments(rawDocuments, descriptor));
		expect(yieldToEventLoop).toHaveBeenCalledTimes(1);
	});
});
