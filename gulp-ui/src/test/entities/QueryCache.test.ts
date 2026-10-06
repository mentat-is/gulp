import {
	cloneQueryForCache,
	createEmptyQueryCacheState,
	fingerprintSourceQuery,
	fingerprintSourceQueryShape,
	hasSourceQueryCacheWarning,
	isQueryCacheIndicatorProjectionCompatible,
	coversRequestedSourceRange,
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
	it("keeps legacy default caches and rejects unversioned custom projections", () => {
		expect(
			isQueryCacheIndicatorProjectionCompatible({
				field: "gulp.event_code",
				hashFunction: "fnv1a",
				requestedFields: ["gulp.event_code"],
			}),
		).toBe(true);
		expect(
			isQueryCacheIndicatorProjectionCompatible(normalization),
		).toBe(false);
		expect(
			isQueryCacheIndicatorProjectionCompatible({
				...normalization,
				indicatorValueVersion: 1,
			}),
		).toBe(true);
	});

	it("includes the custom-value projection version in cache identity", () => {
		const base = fingerprintSourceQuery({
			query: createQuery(),
			operationId: "operation-1",
			sourceId: "source-1",
			normalization,
		});
		const versioned = fingerprintSourceQuery({
			query: createQuery(),
			operationId: "operation-1",
			sourceId: "source-1",
			normalization: { ...normalization, indicatorValueVersion: 1 },
		});

		expect(versioned).not.toBe(base);
	});

	it("recognizes warnings only for settled stale caches owned by the exact source", () => {
		const createWarningSource = (
			overrides: Partial<ReturnType<typeof createEmptyQueryCacheState>> = {},
			sourceIds: string[] = ["source-1"],
		) => ({
			id: "source-1",
			appliedQuery: createQuery({
				source_config: {
					operation_id: "operation-1",
					source_ids: sourceIds,
					range: { min: "1000000", max: "2000000" },
				},
			}),
			queryCache: {
				...createEmptyQueryCacheState(),
				activeGeneration: 3,
				freshness: "source-changed" as const,
				warningReason: "source-changed" as const,
				...overrides,
			},
		});

		expect(hasSourceQueryCacheWarning(createWarningSource())).toBe(true);
		expect(
			hasSourceQueryCacheWarning(
				createWarningSource({ activeGeneration: null }),
			),
		).toBe(false);
		expect(
			hasSourceQueryCacheWarning(createWarningSource({ isActivating: true })),
		).toBe(false);
		expect(
			hasSourceQueryCacheWarning(
				createWarningSource({ freshness: "fresh", warningReason: null }),
			),
		).toBe(false);
		expect(
			hasSourceQueryCacheWarning(
				createWarningSource({}, ["source-other"]),
			),
		).toBe(false);
		expect(
			hasSourceQueryCacheWarning(
				createWarningSource({}, ["source-1", "source-other"]),
			),
		).toBe(false);
	});

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

	it("fingerprints query shape without display text or source range", () => {
		const base = fingerprintSourceQueryShape({
			query: createQuery(),
			normalization,
		});
		expect(
			fingerprintSourceQueryShape({
				query: createQuery({
					string: "different display text",
					source_config: {
						operation_id: "operation-1",
						source_ids: ["source-1"],
						range: { min: "3000000", max: "4000000" },
					},
				}),
				normalization,
			}),
		).toBe(base);
		expect(
			fingerprintSourceQueryShape({
				query: createQuery({
					source_config: {
						operation_id: "operation-1",
						source_ids: ["source-2"],
						range: { min: "1000000", max: "2000000" },
					},
				}),
				normalization,
			}),
		).not.toBe(base);
		expect(
			fingerprintSourceQueryShape({
				query: createQuery({ text_filter: "needle" }),
				normalization,
			}),
		).not.toBe(base);
		expect(
			fingerprintSourceQueryShape({
				query: createQuery(),
				normalization: { ...normalization, hashFunction: "djb2" },
			}),
		).not.toBe(base);
		expect(
			fingerprintSourceQueryShape({
				query: createQuery({
					fieldTypeMap: { "event.code": "long" } as never,
				}),
				normalization,
			}),
		).toBe(base);
		expect(
			fingerprintSourceQueryShape({
				query: createQuery(),
				normalization,
				backendFilter: { term: { "event.code": 1 } },
			}),
		).not.toBe(
			fingerprintSourceQueryShape({
				query: createQuery(),
				normalization,
				backendFilter: { term: { "event.code": "1" } },
			}),
		);
	});

	it("checks inclusive source-local range coverage with exact bigint arithmetic", () => {
		const snapshot = {
			total: 10,
			minNanoseconds: "100",
			maxNanoseconds: "200",
			timeUpdated: null,
			millisecondPrecision: false,
		};
		const coveredCases = [
			{
				cached: { min: "100", max: "200" },
				desired: { min: "120", max: "180" },
			},
			{
				cached: { min: "100", max: "200" },
				desired: { min: "0", max: "300" },
			},
			{
				cached: { min: "100", max: "200" },
				desired: { min: "300", max: "400" },
			},
			{
				cached: {
					min: "9007199254740993000",
					max: "9007199254740993000",
				},
				desired: {
					min: "9007199254740993000",
					max: "9007199254740993000",
				},
				source: {
					...snapshot,
					minNanoseconds: "9007199254740993000",
					maxNanoseconds: "9007199254740993000",
				},
			},
		];
		coveredCases.forEach(({ cached, desired, source }) =>
			expect(
				coversRequestedSourceRange(cached, desired, source ?? snapshot),
			).toBe(true),
		);

		expect(
			coversRequestedSourceRange(
				{ min: "100", max: "150" },
				{ min: "140", max: "180" },
				snapshot,
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: "100", max: "149" },
				{ min: "150", max: "150" },
				snapshot,
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: "200", max: "100" },
				{ min: "100", max: "200" },
				snapshot,
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: "100", max: "200" },
				{ min: "invalid", max: "200" },
				snapshot,
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: "100", max: "200" },
				{ min: "", max: "200" },
				snapshot,
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: "100", max: "200" },
				{ min: "100", max: "200" },
				{ ...snapshot, minNanoseconds: " " },
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: 9_007_199_254_740_992, max: 9_007_199_254_740_994 },
				{ min: "100", max: "200" },
				snapshot,
			),
		).toBe(false);
		expect(
			coversRequestedSourceRange(
				{ min: "500", max: "600" },
				{ min: "100", max: "200" },
				{ ...snapshot, total: 0 },
			),
		).toBe(true);
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

	it("still compares complete-source ranges when both snapshots are empty", () => {
		const cached = {
			total: 0,
			minNanoseconds: "100",
			maxNanoseconds: "200",
			timeUpdated: null,
			millisecondPrecision: false,
		};
		expect(
			sourceSnapshotsMatch(cached, {
				...cached,
				minNanoseconds: "300",
				maxNanoseconds: "400",
			}),
		).toBe(false);
	});
});
