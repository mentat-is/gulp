import type { App } from "@/entities/App";
import { Filter } from "@/entities/Filter";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {},
}));

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			/** Converts millisecond fixtures to the nanoseconds used by query ranges. */
			toNanos: (value: number): bigint => BigInt(value) * 1_000_000n,
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			/** Resolves a source fixture by object or identifier. */
			id: (app: App.Type, source: Source.Type | Source.Id): Source.Type =>
				typeof source === "object"
					? source
					: app.target.files.find((file) => file.id === source)!,
		},
	},
}));

const sourceId = "frame-query-source" as Source.Id;

/**
 * Creates a source with a one-second positive render offset.
 * @returns Source fixture used by query-range tests.
 */
function createSource(): Source.Type {
	return {
		id: sourceId,
		name: "frame-query.csv",
		operation_id: "frame-query-operation",
		settings: { offset: 1_000 },
		timestamp: { min: 0, max: 100_000 },
		nanotimestamp: { min: 0n, max: 100_000_000_000n },
	} as Source.Type;
}

/**
 * Creates the minimum application state needed to resolve the source fixture.
 * @param source Source stored in target.files.
 * @returns Application fixture for Filter.Entity helpers.
 */
function createApp(source: Source.Type): App.Type {
	return {
		target: {
			files: [source],
		},
	} as App.Type;
}

describe("timeline frame query scope", () => {
	it("converts the rendered frame to an offset-adjusted source range and preserves filters", () => {
		const source = createSource();
		const filter = {
			id: "severity-filter",
			type: "eq",
			operator: "must",
			field: "event.severity",
			value: "high",
			enabled: true,
		} as Filter.Type;
		const query: Query.Type = {
			string: "existing filter",
			text_filter: "*failed*",
			filters: [filter],
			source_config: {
				operation_id: source.operation_id,
				source_ids: [source.id],
				range: { min: "0", max: "100000000000" },
			},
		};

		const scoped = Filter.Entity.scopeToSource(
			createApp(source),
			source,
			query,
			{ min: 12_000, max: 18_000 },
		);

		expect(scoped.source_config).toEqual({
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: { min: "11000000000", max: "17000000000" },
		});
		expect(scoped.text_filter).toBe(query.text_filter);
		expect(scoped.filters).toBe(query.filters);
	});

	it("uses the new frame as the base of a manual query without retaining the old generated scope", () => {
		const source = createSource();
		const oldSourceConfig = {
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: { min: "0", max: "100000000000" },
		};
		const query: Query.Type = {
			string: "manual filter",
			filters: [],
			isManual: true,
			source_config: oldSourceConfig,
			raw: {
				bool: {
					must: [
						{ term: { "gulp.operation_id": source.operation_id } },
						{ terms: { "gulp.source_id": [source.id] } },
						{
							range: {
								"gulp.timestamp": {
									gte: oldSourceConfig.range.min,
									lte: oldSourceConfig.range.max,
								},
							},
						},
						{ term: { "event.kind": "alert" } },
						{
							range: {
								"gulp.timestamp": {
									gte: "12000000000",
									lte: "13000000000",
								},
							},
						},
					],
				},
			},
		};

		const scoped = Filter.Entity.scopeToSource(
			createApp(source),
			source,
			query,
			{ min: 12_000, max: 18_000 },
		);
		const generated = Filter.Entity.query(scoped);

		expect(scoped.raw).toEqual({
			bool: {
				must: [
					{ term: { "event.kind": "alert" } },
					{
						range: {
							"gulp.timestamp": {
								gte: "12000000000",
								lte: "13000000000",
							},
						},
					},
				],
			},
		});
		expect(generated).toEqual({
			bool: {
				must: [
					{ term: { "gulp.operation_id": source.operation_id } },
					{ terms: { "gulp.source_id": [source.id] } },
					{
						range: {
							"gulp.timestamp": {
								gte: "11000000000",
								lte: "17000000000",
							},
						},
					},
					{
						bool: {
							must: [
								{ term: { "event.kind": "alert" } },
								{
									range: {
										"gulp.timestamp": {
											gte: "12000000000",
											lte: "13000000000",
										},
									},
								},
							],
						},
					},
				],
			},
		});
	});
});
