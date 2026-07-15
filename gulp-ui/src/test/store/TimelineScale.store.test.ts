import { TimelineScaleStore } from "@/store/TimelineScale.store";

describe("TimelineScaleStore", () => {
	it("publishes changed commits and ignores duplicate snapshots", () => {
		const store = new TimelineScaleStore(1);
		const listener = jest.fn();
		const unsubscribe = store.subscribe(listener);

		expect(store.getSnapshot()).toBe(1);
		expect(store.commit(2)).toBe(true);
		expect(store.getSnapshot()).toBe(2);
		expect(listener).toHaveBeenCalledTimes(1);
		expect(store.commit(2)).toBe(false);
		expect(listener).toHaveBeenCalledTimes(1);

		unsubscribe();
		store.commit(3);
		expect(listener).toHaveBeenCalledTimes(1);
	});

	it("synchronizes React snapshots without notifying and remains instance-local", () => {
		const first = new TimelineScaleStore(1);
		const second = new TimelineScaleStore(4);
		const firstListener = jest.fn();
		first.subscribe(firstListener);

		first.synchronize(2);

		expect(first.getSnapshot()).toBe(2);
		expect(second.getSnapshot()).toBe(4);
		expect(firstListener).toHaveBeenCalledTimes(0);
	});

	it("rebases stale React work and accepts an explicit same-value reset", () => {
		const store = new TimelineScaleStore(1);
		store.commit(2);

		expect(store.reconcileReactSnapshot(1)).toBe(2);
		expect(store.reconcileReactSnapshot(1)).toBe(2);
		store.commit(3);
		store.prepareReactCommit(2);
		expect(store.reconcileReactSnapshot(2)).toBe(2);
		expect(store.getSnapshot()).toBe(2);
	});

	it("retains an intended state commit across an intervening stale snapshot", () => {
		const store = new TimelineScaleStore(1);
		store.commit(2);
		store.prepareReactCommit(3, true);

		expect(store.reconcileReactSnapshot(1)).toBe(3);
		expect(store.reconcileReactSnapshot(3)).toBe(3);
	});
});
