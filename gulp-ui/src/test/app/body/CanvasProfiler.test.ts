import { CanvasProfiler } from "@/app/body/CanvasProfiler";

/**
 * Creates one minimal worker-frame profiler payload.
 * @param drawDurationMs Worker draw duration to record.
 * @returns Complete frame metrics accepted by the profiler.
 */
function createFrameMetrics(drawDurationMs: number) {
	return {
		drawDurationMs,
		dataRevision: 1,
		viewportRevision: 1,
	};
}

describe("CanvasProfiler", () => {
	let releaseCapture: (() => void) | null = null;

	beforeEach(() => {
		window.localStorage.removeItem("gulp.canvasPerf");
		CanvasProfiler.resetWorkerMetrics();
	});

	afterEach(() => {
		releaseCapture?.();
		releaseCapture = null;
		CanvasProfiler.resetWorkerMetrics();
	});

	it("does not collect hot-path samples without a capture consumer", () => {
		CanvasProfiler.recordWorkerFrame(createFrameMetrics(4));
		CanvasProfiler.recordLocalPointerLatency(3);

		const snapshot = CanvasProfiler.getWorkerSnapshot();
		expect(snapshot.frameCount).toBe(0);
		expect(snapshot.localPointerSampleCount).toBe(0);
	});

	it("caches the disabled profiler preference across hot-path recorders", () => {
		const currentTime = Date.now();
		const originalDateNow = Date.now;
		const originalGetItem = Storage.prototype.getItem;
		let preferenceReadCount = 0;
		Date.now = () => currentTime + 2_000;
		/** Counts profiler preference reads while preserving the native storage result. */
		Storage.prototype.getItem = function getItem(key: string): string | null {
			preferenceReadCount++;
			return originalGetItem.call(this, key);
		};

		try {
			for (let index = 0; index < 20; index++) {
				CanvasProfiler.recordWorkerFrame(createFrameMetrics(4));
				CanvasProfiler.recordLocalPointerLatency(3);
				CanvasProfiler.recordInputTimestamp(index, performance.timeOrigin);
			}

			expect(preferenceReadCount).toBe(1);
		} finally {
			Storage.prototype.getItem = originalGetItem;
			Date.now = originalDateNow;
		}
	});

	it("observes a runtime profiler opt-in after the bounded cache interval", () => {
		const currentTime = Date.now();
		const originalDateNow = Date.now;
		let mockedTime = currentTime;
		Date.now = () => mockedTime;

		try {
			expect(CanvasProfiler.start("before-opt-in")).toBeNull();
			window.localStorage.setItem("gulp.canvasPerf", "1");
			expect(CanvasProfiler.start("cached-before-refresh")).toBeNull();

			mockedTime = currentTime + 1_001;
			expect(CanvasProfiler.start("after-opt-in")).not.toBeNull();
		} finally {
			window.localStorage.removeItem("gulp.canvasPerf");
			Date.now = originalDateNow;
		}
	});

	it("retains the newest fixed-size worker sample window", () => {
		releaseCapture = CanvasProfiler.beginCapture();
		for (let value = 0; value <= 3_600; value++) {
			CanvasProfiler.recordWorkerFrame(createFrameMetrics(value));
		}

		const snapshot = CanvasProfiler.getWorkerSnapshot();
		expect(snapshot.frameCount).toBe(3_601);
		expect(snapshot.drawAverageMs).toBe(1_800.5);
		expect(snapshot.drawP95Ms).toBe(3_420);
	});

	it("aggregates input, worker-phase, pointer, and queue metrics", () => {
		releaseCapture = CanvasProfiler.beginCapture();
		const inputSequence = 9;
		CanvasProfiler.recordInputTimestamp(
			inputSequence,
			performance.timeOrigin + performance.now() - 10,
		);
		CanvasProfiler.recordLocalPointerLatency(5);
		CanvasProfiler.recordWorkerFrame({
			...createFrameMetrics(7),
			inputSequence,
			workerReceiveDuration: 2,
			workerWaitDuration: 3,
			dataLatencyMs: 4,
			inFlightQueueDepth: 2,
			pendingQueueDepth: 6,
		});

		const snapshot = CanvasProfiler.getWorkerSnapshot();
		expect(snapshot.localPointerLatencyP95Ms).toBe(5);
		expect(snapshot.endToEndInputLatencyP95Ms >= 10).toBe(true);
		expect(snapshot.inputLatencyP95Ms).toBe(
			snapshot.endToEndInputLatencyP95Ms,
		);
		expect(snapshot.workerReceiveP95Ms).toBe(2);
		expect(snapshot.workerWaitP95Ms).toBe(3);
		expect(snapshot.dataLatencyP95Ms).toBe(4);
		expect(snapshot.maxInFlightQueueDepth).toBe(2);
		expect(snapshot.maxPendingQueueDepth).toBe(6);
		expect(snapshot.maxQueueDepth).toBe(8);
	});

	it("retains a client queue peak after the queue drains before rendering", () => {
		releaseCapture = CanvasProfiler.beginCapture();
		CanvasProfiler.recordWorkerQueueDepth(2, 3);
		CanvasProfiler.recordWorkerFrame({
			...createFrameMetrics(2),
			inFlightQueueDepth: 0,
			pendingQueueDepth: 0,
		});

		const snapshot = CanvasProfiler.getWorkerSnapshot();
		expect(snapshot.maxQueueDepth).toBe(5);
		expect(snapshot.maxInFlightQueueDepth).toBe(2);
		expect(snapshot.maxPendingQueueDepth).toBe(3);
	});

	it("resets client queue transition peaks at the measurement boundary", () => {
		releaseCapture = CanvasProfiler.beginCapture();
		CanvasProfiler.recordWorkerQueueDepth(2, 3);
		CanvasProfiler.resetWorkerMetrics();

		const snapshot = CanvasProfiler.getWorkerSnapshot();
		expect(snapshot.maxQueueDepth).toBe(0);
		expect(snapshot.maxInFlightQueueDepth).toBe(0);
		expect(snapshot.maxPendingQueueDepth).toBe(0);
	});

	it("records one end-to-end latency sample for each input sequence", () => {
		releaseCapture = CanvasProfiler.beginCapture();
		const inputSequence = 4;
		CanvasProfiler.recordInputTimestamp(
			inputSequence,
			performance.timeOrigin + performance.now() - 5,
		);
		CanvasProfiler.recordWorkerFrame({
			...createFrameMetrics(4),
			inputSequence,
			inputLatencyMs: 1,
		});
		CanvasProfiler.recordWorkerFrame({
			...createFrameMetrics(4),
			inputSequence,
			inputLatencyMs: 0,
		});

		expect(CanvasProfiler.getWorkerSnapshot().inputLatencySampleCount).toBe(1);
	});

	it("restarts Long Task observation at a metrics reset boundary", () => {
		const originalObserver = Object.getOwnPropertyDescriptor(
			globalThis,
			"PerformanceObserver",
		);
		class MockPerformanceObserver {
			static readonly supportedEntryTypes = ["longtask"];
			static readonly instances: MockPerformanceObserver[] = [];
			readonly disconnect = jest.fn<void, []>();
			readonly observe = jest.fn<void, [PerformanceObserverInit]>();
			readonly takeRecords = jest.fn<PerformanceEntryList, []>(() => []);

			constructor(_callback: PerformanceObserverCallback) {
				MockPerformanceObserver.instances.push(this);
			}
		}
		Object.defineProperty(globalThis, "PerformanceObserver", {
			configurable: true,
			value: MockPerformanceObserver as unknown as typeof PerformanceObserver,
		});

		try {
			releaseCapture = CanvasProfiler.beginCapture();
			const firstObserver = MockPerformanceObserver.instances[0];
			CanvasProfiler.resetWorkerMetrics();

			expect(firstObserver.takeRecords).toHaveBeenCalledTimes(1);
			expect(firstObserver.disconnect).toHaveBeenCalledTimes(1);
			expect(MockPerformanceObserver.instances).toHaveLength(2);
			expect(CanvasProfiler.getWorkerSnapshot().mainLongTaskSupported).toBe(true);
		} finally {
			releaseCapture?.();
			releaseCapture = null;
			if (originalObserver) {
				Object.defineProperty(globalThis, "PerformanceObserver", originalObserver);
			} else {
				Reflect.deleteProperty(globalThis, "PerformanceObserver");
			}
		}
	});

	it("drains pending Long Task entries into a terminal snapshot", () => {
		const originalObserver = Object.getOwnPropertyDescriptor(
			globalThis,
			"PerformanceObserver",
		);
		const pendingEntry = { duration: 72 } as PerformanceEntry;
		class MockPerformanceObserver {
			static readonly supportedEntryTypes = ["longtask"];
			private hasPendingEntry = true;
			readonly disconnect = jest.fn<void, []>();
			readonly observe = jest.fn<void, [PerformanceObserverInit]>();
			readonly takeRecords = jest.fn<PerformanceEntryList, []>(() => {
				if (!this.hasPendingEntry) return [];
				this.hasPendingEntry = false;
				return [pendingEntry];
			});

			/** Creates the deterministic observer used by this pending-entry test. */
			constructor(_callback: PerformanceObserverCallback) {}
		}
		Object.defineProperty(globalThis, "PerformanceObserver", {
			configurable: true,
			value: MockPerformanceObserver as unknown as typeof PerformanceObserver,
		});

		try {
			releaseCapture = CanvasProfiler.beginCapture();
			const snapshot = CanvasProfiler.getWorkerSnapshot();

			expect(snapshot.mainLongTaskSupported).toBe(true);
			expect(snapshot.mainLongTaskCount).toBe(1);
			expect(snapshot.mainLongestTaskMs).toBe(72);
			expect(CanvasProfiler.getWorkerSnapshot().mainLongTaskCount).toBe(1);
		} finally {
			releaseCapture?.();
			releaseCapture = null;
			if (originalObserver) {
				Object.defineProperty(globalThis, "PerformanceObserver", originalObserver);
			} else {
				Reflect.deleteProperty(globalThis, "PerformanceObserver");
			}
		}
	});

	it("records portable animation-frame gaps and dropped-frame counts", () => {
		const originalRequestAnimationFrame = window.requestAnimationFrame;
		const originalCancelAnimationFrame = window.cancelAnimationFrame;
		const callbacks: FrameRequestCallback[] = [];
		window.requestAnimationFrame = (callback: FrameRequestCallback): number => {
			callbacks.push(callback);
			return callbacks.length;
		};
		window.cancelAnimationFrame = () => undefined;

		try {
			releaseCapture = CanvasProfiler.beginCapture();
			for (const timestamp of [0, 16.7, 33.4, 84.4, 185.4]) {
				const callback = callbacks.shift();
				if (!callback) throw new Error("Missing profiler animation-frame callback");
				callback(timestamp);
			}

			const snapshot = CanvasProfiler.getWorkerSnapshot();
			expect(snapshot.mainFrameIntervalCount).toBe(4);
			expect(snapshot.mainMaxFrameIntervalMs).toBe(101);
			expect(snapshot.mainDroppedFrameCount).toBe(7);
			expect(snapshot.mainStallsOver50Ms).toBe(2);
			expect(snapshot.mainStallsOver100Ms).toBe(1);
		} finally {
			releaseCapture?.();
			releaseCapture = null;
			window.requestAnimationFrame = originalRequestAnimationFrame;
			window.cancelAnimationFrame = originalCancelAnimationFrame;
		}
	});

	it("publishes immutable renderer backend status updates", () => {
		const listener = jest.fn<void, [ReturnType<typeof CanvasProfiler.getRendererBackendStatus>]>();
		const unsubscribe = CanvasProfiler.subscribeRendererBackendStatus(listener);
		CanvasProfiler.setRendererBackendStatus({
			requestedBackend: "worker",
			activeBackend: "legacy",
			fallbackReason: "worker failed",
		});

		expect(listener).toHaveBeenCalledTimes(2);
		expect(listener.mock.calls[1][0]).toEqual({
			requestedBackend: "worker",
			activeBackend: "legacy",
			fallbackReason: "worker failed",
		});
		unsubscribe();
	});
});
