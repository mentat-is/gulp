import {
	cloneQueryForCache,
	createEmptyQueryCacheState,
	fingerprintSourceQuery,
	sourceSnapshotsMatch,
	type QueryCacheNormalization,
} from "@/entities/QueryCache";
import type { Query } from "@/entities/Query";

const normalization: QueryCacheNormalization = {
	field: "event.code",
	hashFunction: "fnv1a",
	requestedFields: ["gulp.timestamp", "event.code"],
};

/** Creates one source-scoped query fixture with overridable semantic fields. */
function createQuery(overrides: Partial<Query.Type> = {}): Query.Type {
	return {
		string: "display only",
		filters: [{ field: "event.code", operator: "eq", value: "1" } as never],
		source_config: {
			operation_id: "operation-1",
			source_ids: ["source-1"],
			range: { min: "1000000", max: "2000000" },
		},
		...overrides,
	};
}

describe("source query cache identity", () => {
	it("does not warn when a source has no active query cache", () => {
		expect(createEmptyQueryCacheState()).toMatchObject({
			activeGeneration: null,
			freshness: "fresh",
			warningReason: null,
		});
	});

	it("ignores display labels and object key order", () => {
		const first = createQuery({
			raw: { bool: { must: [{ term: { b: 2, a: 1 } }] } },
		});
		const second = createQuery({
			string: "another label",
			raw: { bool: { must: [{ term: { a: 1, b: 2 } }] } },
		});

		expect(
			fingerprintSourceQuery({
				query: first,
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
		).toBe(
			fingerprintSourceQuery({
				query: second,
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
		);
	});

	it("distinguishes operation, source, query semantics, and normalization", () => {
		const base = fingerprintSourceQuery({
			query: createQuery(),
			operationId: "operation-1",
			sourceId: "source-1",
			normalization,
		});
		const variants = [
			fingerprintSourceQuery({
				query: createQuery(),
				operationId: "operation-2",
				sourceId: "source-1",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery(),
				operationId: "operation-1",
				sourceId: "source-2",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery({
					source_config: {
						operation_id: "operation-1",
						source_ids: ["source-1"],
						range: { min: "1000001", max: "2000000" },
					},
				}),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery({ text_filter: "needle" }),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery({
					filters: [
						{
							field: "event.code",
							operator: "eq",
							value: "2",
						} as never,
					],
				}),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery({ raw: { term: { field: "value" } } }),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery({ isManual: true }),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
			}),
			fingerprintSourceQuery({
				query: createQuery(),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization: { ...normalization, hashFunction: "djb2" },
			}),
			fingerprintSourceQuery({
				query: createQuery(),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization: {
					...normalization,
					requestedFields: [...normalization.requestedFields].reverse(),
				},
			}),
			fingerprintSourceQuery({
				query: createQuery(),
				operationId: "operation-1",
				sourceId: "source-1",
				normalization,
				backendFilter: { term: { "event.code": 1 } },
			}),
		];

		variants.forEach((fingerprint) => expect(fingerprint).not.toBe(base));
	});

	it("creates an independent serializable query snapshot", () => {
		const query = createQuery({ raw: { value: 1n } });
		const snapshot = cloneQueryForCache(query);

		expect(snapshot).not.toBe(query);
		expect(snapshot.raw).toEqual({ value: "1" });
		(snapshot.raw as { value: string }).value = "changed";
		expect(query.raw).toEqual({ value: 1n });
	});

	it("compares full-source metadata with millisecond precision tolerance", () => {
		const cached = {
			total: 10,
			minNanoseconds: "1000000",
			maxNanoseconds: "2000000",
			timeUpdated: 10,
			millisecondPrecision: true,
		};
		expect(
			sourceSnapshotsMatch(cached, {
				...cached,
				minNanoseconds: "1999999",
				maxNanoseconds: "2999999",
			}),
		).toBe(true);
		expect(sourceSnapshotsMatch(cached, { ...cached, total: 11 })).toBe(false);
		expect(
			sourceSnapshotsMatch(cached, { ...cached, timeUpdated: "10" }),
		).toBe(true);
	});

	it("compares an update marker only when both source snapshots provide one", () => {
		const cached = {
			total: 10,
			minNanoseconds: "1000000",
			maxNanoseconds: "2000000",
			timeUpdated: 10,
			millisecondPrecision: false,
		};

		expect(
			sourceSnapshotsMatch(cached, { ...cached, timeUpdated: null }),
		).toBe(true);
		expect(
			sourceSnapshotsMatch(
				{ ...cached, timeUpdated: null },
				{ ...cached, timeUpdated: 11 },
			),
		).toBe(true);
		expect(
			sourceSnapshotsMatch(cached, { ...cached, timeUpdated: 11 }),
		).toBe(false);
	});
});
