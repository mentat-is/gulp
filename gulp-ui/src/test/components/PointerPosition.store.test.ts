import {
	PointerPositionStore,
	projectLocalPointerTimestamp,
} from "@/components/PointerPosition.store";

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

	it("reprojects a stationary pointer when the timeline scroll changes", () => {
		const snapshot = {
			x: 200,
			y: 40,
			timestamp: 50,
			isRightSide: false,
		};
		const projection = {
			frame: { min: 0, max: 2_400 },
			viewportWidth: 600,
			scale: 4,
			scrollX: 1_000,
		};

		expect(projectLocalPointerTimestamp(snapshot, projection)).toEqual({
			...snapshot,
			timestamp: 1_200,
		});
		expect(
			projectLocalPointerTimestamp(snapshot, {
				...projection,
				scrollX: 1_100,
			}),
		).toEqual({
			...snapshot,
			timestamp: 1_300,
		});
	});
});
