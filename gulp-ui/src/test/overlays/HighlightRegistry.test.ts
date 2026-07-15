import { HighlightRegistry } from "@/overlays/HighlightRegistry";

describe("HighlightRegistry", () => {
	it("applies scroll lazily without rewriting document-space entries", () => {
		const registry = new HighlightRegistry();
		registry.replace([
			{ id: "first", left: 100, width: 50, index: 0, color: "red" },
			{ id: "second", left: 240, width: 80, index: 1, color: "blue" },
		]);

		registry.setViewportOffset(25);
		expect(registry.list()).toEqual([
			[75, 50, 0, "red"],
			[215, 80, 1, "blue"],
		]);

		registry.setViewportOffset(40);
		expect(registry.list()).toEqual([
			[60, 50, 0, "red"],
			[200, 80, 1, "blue"],
		]);
	});

	it("removes deleted and stale entries immediately", () => {
		const registry = new HighlightRegistry();
		registry.set({ id: "first", left: 10, width: 20, index: 0, color: "red" });
		registry.set({ id: "second", left: 30, width: 40, index: 1, color: "blue" });

		registry.remove("first");
		expect(registry.list()).toEqual([[30, 40, 1, "blue"]]);

		registry.replace([
			{ id: "third", left: 50, width: 60, index: 0, color: "green" },
		]);
		expect(registry.list()).toEqual([[50, 60, 0, "green"]]);
	});
});
