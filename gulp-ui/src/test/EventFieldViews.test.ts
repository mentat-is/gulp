import {
	buildConfiguredEventFieldViews,
	getEventLeafFieldPaths,
	normalizeEventFieldPath,
} from "@/utils";

jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: {} },
}));

jest.mock("@/entities/Source", () => ({
	Source: { Entity: {} },
}));

describe("Event field view configuration", () => {
	it("groups dotted fields by branch while retaining the global user order", () => {
		const views = buildConfiguredEventFieldViews(
			{
				"gulp.context_id": "context",
				"process.pid": 12234,
				"gulp.event_code": "CONNECT",
			},
			["gulp.event_code", "process.pid", "gulp.context_id"],
			new Set(),
		);

		expect(Object.keys(views.raw)).toEqual([
			"gulp.event_code",
			"process.pid",
			"gulp.context_id",
		]);
		expect(views.table.map((row) => row.key)).toEqual([
			"gulp.event_code",
			"process.pid",
			"gulp.context_id",
		]);
		expect(Object.keys(views.tree)).toEqual(["gulp", "process"]);
		expect(Object.keys(views.tree.gulp as Record<string, unknown>)).toEqual([
			"event_code",
			"context_id",
		]);
	});

	it("filters nested fields and prunes branches without changing the Raw shape", () => {
		const views = buildConfiguredEventFieldViews(
			{
				agent: { name: "sensor", type: "edr" },
				"event.action": "connect",
			},
			["agent.type", "event.action", "agent.name"],
			new Set(["agent.name"]),
		);

		expect(views.raw).toEqual({
			agent: { type: "edr" },
			"event.action": "connect",
		});
		expect(views.table).toEqual([
			{ key: "agent.type", value: "edr" },
			{ key: "event.action", value: "connect" },
		]);
		expect(views.tree).toEqual({
			agent: { type: "edr" },
			event: { action: "connect" },
		});
	});

	it("normalizes array indices while preserving array item and child order", () => {
		const json = {
			items: [
				{ name: "first", pid: 1 },
				{ pid: 2, name: "second" },
			],
		};

		expect(normalizeEventFieldPath("items.12.name")).toBe("items.name");
		expect(getEventLeafFieldPaths(json)).toEqual(["items.name", "items.pid"]);

		const views = buildConfiguredEventFieldViews(
			json,
			["items.pid", "items.name"],
			new Set(["items.name"]),
		);

		expect(views.raw).toEqual({ items: [{ pid: 1 }, { pid: 2 }] });
		expect(views.table.map((row) => row.key)).toEqual([
			"items.0.pid",
			"items.1.pid",
		]);
		expect(views.tree).toEqual({ items: [{ pid: 1 }, { pid: 2 }] });
	});

	it("returns empty views when every leaf field is hidden", () => {
		const views = buildConfiguredEventFieldViews(
			{ alpha: 1, nested: { beta: 2 } },
			["alpha", "nested.beta"],
			new Set(["alpha", "nested.beta"]),
		);

		expect(views.raw).toEqual({});
		expect(views.table).toEqual([]);
		expect(views.tree).toEqual({});
	});
});
