import { PointerPositionStore } from "@/components/PointerPosition.store";

describe("PointerPositionStore", () => {
	it("publishes complete snapshots only when a display field changes", () => {
		const store = new PointerPositionStore();
		let notificationCount = 0;
		const unsubscribe = store.subscribe(() => {
			notificationCount++;
		});
		const firstSnapshot = {
			x: 12,
			y: 34,
			timestamp: 1_725_000_000_000,
			isRightSide: false,
		};

		store.setPosition(firstSnapshot);
		expect(store.getSnapshot()).toEqual(firstSnapshot);
		expect(notificationCount).toBe(1);

		store.setPosition({ ...firstSnapshot });
		expect(notificationCount).toBe(1);

		store.setPosition({ ...firstSnapshot, isRightSide: true });
		expect(notificationCount).toBe(2);

		unsubscribe();
		store.setPosition({ ...firstSnapshot, timestamp: firstSnapshot.timestamp + 1 });
		expect(notificationCount).toBe(2);
	});
});
