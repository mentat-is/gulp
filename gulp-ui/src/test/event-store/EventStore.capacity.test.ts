import {
	calculateEventStoreCapacity,
	EVENT_CAPACITY_RECHECK_BYTES,
	EVENT_CAPACITY_RECHECK_EVENTS,
	EventStoreCapacityTelemetryUnavailableError,
	EventStoreCapacityTracker,
	estimateEventStoreCapacity,
	getEventStoreByteP95,
	simulateEventStoreCapacity,
} from "@/event-store/EventStore.capacity";

const GIBIBYTE = 1024 ** 3;

describe("EventStore capacity", () => {
	it("reserves one GiB when that exceeds twenty percent", () => {
		const capacity = calculateEventStoreCapacity({
			quotaBytes: 10 * 1024 ** 3,
			usageBytes: 1024 ** 3,
			currentEventCount: 1_000,
		});

		expect(capacity.reserveBytes).toBe(2 * 1024 ** 3);
		expect(capacity.conservativeBytesPerEvent).toBe(160);
		expect(capacity.effectiveMaximumEvents).toBe(
			1_000 + Math.floor((7 * 1024 ** 3) / 160),
		);
	});

	it("uses measured p95 with IndexedDB overhead", () => {
		const samples = [80, 90, 100, 110, 120, 130, 140, 150, 200, 300];
		expect(getEventStoreByteP95(samples)).toBe(300);

		const capacity = calculateEventStoreCapacity({
			quotaBytes: 20 * 1024 ** 3,
			usageBytes: 2 * 1024 ** 3,
			currentEventCount: 0,
			encodedBytesPerEventSamples: samples,
		});
		expect(capacity.conservativeBytesPerEvent).toBe(450);
	});

	it("never reports negative storage or event capacity", () => {
		const capacity = calculateEventStoreCapacity({
			quotaBytes: 100,
			usageBytes: 200,
			currentEventCount: -10,
		});
		expect(capacity.availableBytes).toBe(0);
		expect(capacity.maximumAdditionalEvents).toBe(0);
		expect(capacity.currentEventCount).toBe(0);
	});

	it("simulates candidate query growth without mutating capacity", () => {
		const capacity = calculateEventStoreCapacity({
			quotaBytes: 5 * GIBIBYTE,
			usageBytes: GIBIBYTE,
			currentEventCount: 10,
		});
		const points = simulateEventStoreCapacity(capacity, [1, -4, 100_000_000]);
		expect(points[0].estimatedAdditionalBytes).toBe(160);
		expect(points[1].additionalEvents).toBe(0);
		expect(points[2].fits).toBe(false);
	});

	it("rechecks after first estimate and either committed-write threshold", async () => {
		const tracker = new EventStoreCapacityTracker();
		expect(tracker.shouldReestimate()).toBe(true);
		await tracker.estimate(0, {
			estimate: async () => ({ quota: 5 * GIBIBYTE, usage: GIBIBYTE }),
		});
		expect(tracker.shouldReestimate()).toBe(false);

		tracker.recordCommittedWrite(1_000, EVENT_CAPACITY_RECHECK_EVENTS);
		expect(tracker.shouldReestimate()).toBe(true);
		tracker.markEstimated();
		tracker.recordCommittedWrite(EVENT_CAPACITY_RECHECK_BYTES, 1);
		expect(tracker.shouldReestimate()).toBe(true);
	});

	it("uses bounded measured write samples for subsequent p95 estimates", async () => {
		const tracker = new EventStoreCapacityTracker();
		for (let index = 0; index < 300; index++) {
			tracker.recordCommittedWrite(100 + index, 1);
		}
		expect(tracker.bytePerEventSamples.length).toBe(256);
		const capacity = await tracker.estimate(300, {
			estimate: async () => ({ quota: 5 * GIBIBYTE, usage: GIBIBYTE }),
		});
		expect(capacity.conservativeBytesPerEvent > 160).toBe(true);
	});

	it("ignores zero-byte in-place writes when estimating bytes per event", async () => {
		const tracker = new EventStoreCapacityTracker();
		for (let index = 0; index < 300; index++) {
			tracker.recordCommittedWrite(0, 10_000);
		}
		expect(tracker.bytePerEventSamples).toEqual([]);

		const initialCapacity = await tracker.estimate(3_000_000, {
			estimate: async () => ({ quota: 5 * GIBIBYTE, usage: GIBIBYTE }),
		});
		expect(initialCapacity.conservativeBytesPerEvent).toBe(160);

		tracker.recordCommittedWrite(200, 2);
		const measuredCapacity = await tracker.estimate(3_000_002, {
			estimate: async () => ({ quota: 5 * GIBIBYTE, usage: GIBIBYTE }),
		});
		expect(measuredCapacity.conservativeBytesPerEvent).toBe(150);
	});

	for (const [caseName, storageManager] of [
		[
			"missing quota",
			{ estimate: async (): Promise<StorageEstimate> => ({ usage: 0 }) },
		],
		[
			"zero quota",
			{ estimate: async (): Promise<StorageEstimate> => ({ quota: 0, usage: 0 }) },
		],
		[
			"non-finite quota",
			{
				estimate: async (): Promise<StorageEstimate> => ({
					quota: Number.NaN,
					usage: 0,
				}),
			},
		],
		[
			"rejected estimate",
			{
				estimate: async (): Promise<StorageEstimate> => {
					throw new Error("estimate blocked");
				},
			},
		],
	] as const) {
		it(
			`fails closed without misreporting ${caseName} as exhausted quota`,
			async () => {
				await expect(
					estimateEventStoreCapacity(30_000, [], storageManager),
				).rejects.toThrow("Browser storage quota telemetry is unavailable");
			},
		);
	}

	it("fails closed when StorageManager is absent", async () => {
		const storageDescriptor = Object.getOwnPropertyDescriptor(
			globalThis.navigator,
			"storage",
		);
		Object.defineProperty(globalThis.navigator, "storage", {
			configurable: true,
			value: undefined,
		});
		try {
			let telemetryError: unknown;
			try {
				await estimateEventStoreCapacity(30_000);
			} catch (error) {
				telemetryError = error;
			}
			expect(telemetryError).toBeInstanceOf(
				EventStoreCapacityTelemetryUnavailableError,
			);
		} finally {
			if (storageDescriptor) {
				Object.defineProperty(
					globalThis.navigator,
					"storage",
					storageDescriptor,
				);
			} else {
				Reflect.deleteProperty(globalThis.navigator, "storage");
			}
		}
	});
});
