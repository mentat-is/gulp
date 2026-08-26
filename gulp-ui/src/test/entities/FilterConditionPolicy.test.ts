import type { App } from "@/entities/App";
import { Filter } from "@/entities/Filter";
import type { Source } from "@/entities/Source";

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			toNanos: (value: number): bigint => BigInt(value) * 1_000_000n,
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: (app: App.Type, source: Source.Type | Source.Id): Source.Type =>
				typeof source === "object"
					? source
					: app.target.files.find((file) => file.id === source)!,
		},
	},
}));

/** Creates a leaf filter fixture with optional field overrides. */
function createFilter(overrides: Partial<Filter.Type> = {}): Filter.Type {
	return Filter.Entity.createCondition("condition" as Filter.Id, overrides);
}

describe("filter condition policy", () => {
	const conditionCases: Array<[
		string | undefined,
		readonly Filter.Condition[],
	]> = [
		["keyword", ["regexp", "wildcard", "range", "LTE", "GTE", "eq"]],
		["wildcard", ["regexp", "wildcard", "range", "LTE", "GTE", "eq"]],
		["text", ["eq", "wildcard"]],
		["byte", ["eq", "range", "LTE", "GTE"]],
		["short", ["eq", "range", "LTE", "GTE"]],
		["long", ["eq", "range", "LTE", "GTE"]],
		["integer", ["eq", "range", "LTE", "GTE"]],
		["unsigned_long", ["eq", "range", "LTE", "GTE"]],
		["half_float", ["eq", "range", "LTE", "GTE"]],
		["float", ["eq", "range", "LTE", "GTE"]],
		["double", ["eq", "range", "LTE", "GTE"]],
		["scaled_float", ["eq", "range", "LTE", "GTE"]],
		["date", ["eq", "range", "LTE", "GTE"]],
		["date_nanos", ["eq", "range", "LTE", "GTE"]],
		["ip", ["eq", "range", "LTE", "GTE"]],
		["boolean", ["eq"]],
		["object", ["eq"]],
		[undefined, ["eq"]],
	];

	conditionCases.forEach(([fieldType, expected]) => {
		it(`allows the expected conditions for ${fieldType ?? "unknown"}`, () => {
			expect(Filter.Entity.getAllowedConditions(fieldType)).toEqual(expected);
		});
	});

	it("creates equality conditions by default", () => {
		expect(Filter.Entity.createCondition("new-filter" as Filter.Id)).toEqual({
			id: "new-filter",
			type: "eq",
			field: "",
			case_insensitive: true,
			value: "",
			operator: "must",
			enabled: true,
		});
	});

	it("resets an incompatible condition when assigning a field", () => {
		const wildcard = createFilter({ type: "wildcard", field: "message" });

		expect(
			Filter.Entity.assignField(wildcard, "event.sequence", {
				"event.sequence": "long",
			}),
		).toMatchObject({ field: "event.sequence", type: "eq" });
		expect(
			Filter.Entity.assignField(wildcard, "event.category", {
				"event.category": "keyword",
			}),
		).toMatchObject({ field: "event.category", type: "wildcard" });
	});

	it("normalizes incompatible conditions recursively without rewriting valid items", () => {
		const valid = createFilter({
			id: "valid" as Filter.Id,
			type: "wildcard",
			field: "message",
		});
		const invalid = createFilter({
			id: "invalid" as Filter.Id,
			type: "regexp",
			field: "event.sequence",
		});
		const group: Filter.Group = {
			id: "group" as Filter.Id,
			type: "group",
			operator: "must",
			children: [invalid],
			enabled: true,
		};

		const normalized = Filter.Entity.normalizeConditions([valid, group], {
			message: "text",
			"event.sequence": "long",
		});

		expect(normalized[0]).toBe(valid);
		expect((normalized[1] as Filter.Group).children[0]).toMatchObject({
			field: "event.sequence",
			type: "eq",
		});
	});
});

describe("filter clause generation", () => {
	it("uses term queries for equality and incompatible legacy conditions", () => {
		expect(
			Filter.Entity.buildItemClause(
				createFilter({ field: "event.sequence", value: 0 }),
				{ "event.sequence": "long" },
			),
		).toEqual({ term: { "event.sequence": 0 } });
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "wildcard",
					field: "event.sequence",
					value: "12*",
				}),
				{ "event.sequence": "long" },
			),
		).toEqual({ term: { "event.sequence": "12*" } });
		expect(
			Filter.Entity.buildItemClause(
				createFilter({ field: "gulp.timestamp_invalid", value: false }),
				{ "gulp.timestamp_invalid": "boolean" },
			),
		).toEqual({ term: { "gulp.timestamp_invalid": false } });
	});

	it("converts numeric range boundaries and omits empty endpoints", () => {
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "range",
					field: "event.duration",
					value: "10.5, 20",
				}),
				{ "event.duration": "double" },
			),
		).toEqual({
			range: { "event.duration": { gte: 10.5, lte: 20 } },
		});
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "range",
					field: "event.duration",
					value: "10,",
				}),
				{ "event.duration": "long" },
			),
		).toEqual({ range: { "event.duration": { gte: 10 } } });
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "range",
					field: "event.duration",
					value: "invalid,20",
				}),
				{ "event.duration": "long" },
			),
		).toBeNull();
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "LTE",
					field: "event.duration",
					value: "   ",
				}),
				{ "event.duration": "long" },
			),
		).toBeNull();
	});

	it("preserves date, IP, and keyword comparison boundaries as strings", () => {
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "GTE",
					field: "@timestamp",
					value: "2026-08-25T10:00:00Z",
				}),
				{ "@timestamp": "date_nanos" },
			),
		).toEqual({
			range: { "@timestamp": { gte: "2026-08-25T10:00:00Z" } },
		});
		expect(
			Filter.Entity.buildItemClause(
				createFilter({
					type: "LTE",
					field: "source.ip",
					value: "10.0.0.25",
				}),
				{ "source.ip": "ip" },
			),
		).toEqual({ range: { "source.ip": { lte: "10.0.0.25" } } });
		expect(
			Filter.Entity.buildItemClause(
				createFilter({ type: "GTE", field: "event.code", value: "B" }),
				{ "event.code": "keyword" },
			),
		).toEqual({ range: { "event.code": { gte: "B" } } });
	});
});
