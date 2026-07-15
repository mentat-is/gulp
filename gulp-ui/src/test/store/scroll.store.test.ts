import { ScrollStore } from "@/store/scroll.store";

describe("ScrollStore axis subscriptions", () => {
	it("notifies only the changed axis and the combined subscriber", () => {
		const store = new ScrollStore();
		let combinedCount = 0;
		let xCount = 0;
		let yCount = 0;
		store.subscribe(() => combinedCount++);
		store.subscribeX(() => xCount++);
		store.subscribeY(() => yCount++);

		store.setScrollX(24);
		expect(combinedCount).toBe(1);
		expect(xCount).toBe(1);
		expect(yCount).toBe(0);

		store.setScrollY(48);
		expect(combinedCount).toBe(2);
		expect(xCount).toBe(1);
		expect(yCount).toBe(1);

		store.setScroll(30, 54);
		expect(combinedCount).toBe(3);
		expect(xCount).toBe(2);
		expect(yCount).toBe(2);
		expect(store.getSnapshot()).toEqual({ x: 30, y: 54 });
	});

	it("supports functional updates and ignores unchanged values", () => {
		const store = new ScrollStore();
		let combinedCount = 0;
		store.subscribe(() => combinedCount++);

		store.setScrollX((currentX) => currentX + 5);
		store.setScrollY((currentY) => currentY - 4);
		expect(store.getSnapshot()).toEqual({ x: 5, y: -30 });
		expect(combinedCount).toBe(2);

		store.setScroll(5, -30);
		store.setScrollX(5);
		store.setScrollY(-30);
		expect(combinedCount).toBe(2);
	});
});
