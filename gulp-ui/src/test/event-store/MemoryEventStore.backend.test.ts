import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";

describe("MemoryEventStoreBackend", () => {
	it("preserves IndexedDB-style clone boundaries", async () => {
		const backend = new MemoryEventStoreBackend();
		const value = {
			label: "original",
			numbers: new Uint32Array([1, 2, 3]),
		};
		await backend.commit([
			{ type: "put", store: "pages", key: ["source", 1, 1], value },
		]);
		value.label = "mutated";
		value.numbers[0] = 99;

		const firstRead = await backend.get<typeof value>("pages", ["source", 1, 1]);
		expect(firstRead?.label).toBe("original");
		expect(firstRead?.numbers).toEqual(new Uint32Array([1, 2, 3]));
		if (firstRead) firstRead.numbers[1] = 88;

		const secondRead = await backend.get<typeof value>("pages", ["source", 1, 1]);
		expect(secondRead?.numbers).toEqual(new Uint32Array([1, 2, 3]));
	});

	it("applies prepared multi-store writes and clears deterministically", async () => {
		const backend = new MemoryEventStoreBackend();
		await backend.commit([
			{ type: "put", store: "sources", key: "second", value: { order: 2 } },
			{ type: "put", store: "sources", key: "first", value: { order: 1 } },
			{ type: "put", store: "pages", key: 1, value: { page: 1 } },
		]);

		expect(await backend.getAll("sources")).toEqual([
			{ order: 1 },
			{ order: 2 },
		]);
		expect(backend.getStoreSize("pages")).toBe(1);

		await backend.commit([
			{ type: "delete", store: "sources", key: "first" },
			{ type: "clear", store: "pages" },
		]);
		expect(await backend.getAll("sources")).toEqual([{ order: 2 }]);
		expect(backend.getStoreSize("pages")).toBe(0);
	});
});
