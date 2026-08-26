import type { RendererBackendStatus } from "./CanvasRendererBackend";

export interface CanvasProfilerCounts {
	visibleSources: number;
	renderedRows: number;
	visibleNotes: number;
	visibleLinks: number;
	interactiveRects: number;
	graphSamples: number;
	graphBuckets: number;
	renderedEventPixels: number;
}

export interface CanvasWorkerFrameMetrics {
	drawDurationMs: number;
	/** Compatibility latency supplied by callers that do not use input sequences yet. */
	inputLatencyMs?: number;
	/** Latest coalesced raw-input sequence represented by this frame. */
	inputSequence?: number;
	/** Main-to-worker message delivery duration measured by the worker. */
	workerReceiveDuration?: number;
	/** Worker receipt-to-animation-frame wait duration. */
	workerWaitDuration?: number;
	dataLatencyMs?: number;
	/** Compatibility aggregate queue depth. */
	queueDepth?: number;
	/** Number of data messages currently sent but not acknowledged. */
	inFlightQueueDepth?: number;
	/** Number of keyed data messages waiting for an in-flight slot. */
	pendingQueueDepth?: number;
	dataRevision: number;
	viewportRevision: number;
}

export interface CanvasWorkerProfilerSnapshot {
	frameCount: number;
	drawAverageMs: number;
	drawP95Ms: number;
	localPointerSampleCount: number;
	localPointerLatencyP95Ms: number;
	inputLatencySampleCount: number;
	endToEndInputLatencyP95Ms: number;
	/** Compatibility alias for endToEndInputLatencyP95Ms. */
	inputLatencyP95Ms: number;
	workerReceiveP95Ms: number;
	workerWaitP95Ms: number;
	dataLatencySampleCount: number;
	dataLatencyP95Ms: number;
	maxQueueDepth: number;
	maxInFlightQueueDepth: number;
	maxPendingQueueDepth: number;
	mainFrameIntervalP95Ms: number;
	mainFrameIntervalP99Ms: number;
	mainFrameIntervalCount: number;
	mainMaxFrameIntervalMs: number;
	workerMaxFrameIntervalMs: number;
	mainDroppedFrameCount: number;
	mainDroppedFrameRatio: number;
	mainStallsOver50Ms: number;
	mainStallsOver100Ms: number;
	resyncCount: number;
	workerErrorCount: number;
	fallbackCount: number;
	mainLongTaskSupported: boolean;
	mainLongTaskCount: number;
	mainLongestTaskMs: number;
}

export interface CanvasWorkerSourceSynchronization {
	sourceId: string;
	expectedRevision: number;
	acknowledgedRevision: number;
	acknowledgedDataSequence: number;
	acknowledgedAtMs: number;
}

export interface CanvasWorkerSynchronizationSnapshot {
	activeBackend: "worker" | "legacy";
	ready: boolean;
	operationEpoch: number;
	expectedGlobalRevision: number;
	acknowledgedGlobalRevision: number;
	acknowledgedDataSequence: number;
	acknowledgedDataAtMs: number;
	renderedGlobalRevision: number;
	renderedDataSequence: number;
	renderedDataAtMs: number;
	inFlightDataMessages: number;
	pendingDataMessages: number;
	pendingProjectionTasks: number;
	pendingSourceResynchronizations: number;
	visibleProjectionDepth: number;
	backgroundProjectionDepth: number;
	oldestProjectionAgeMs: number;
	oldestTerminalTargetAgeMs: number;
	projectionSliceDurationMs: number;
	requestedViewportRevision: number;
	renderedViewportRevision: number;
	sources: readonly CanvasWorkerSourceSynchronization[];
}

type CanvasProfilerStep =
	| "clear"
	| "visibleSources"
	| "ruler"
	| "rows"
	| "highlightsFlags"
	| "notes"
	| "links";

type CanvasProfilerStepDurations = Record<CanvasProfilerStep, number>;

const EMPTY_STEPS: CanvasProfilerStepDurations = {
	clear: 0,
	visibleSources: 0,
	ruler: 0,
	rows: 0,
	highlightsFlags: 0,
	notes: 0,
	links: 0,
};

const SAMPLE_LIMIT = 120;
const WORKER_SAMPLE_LIMIT = 3_600;
const SLOW_FRAME_MS = 16;
const VERY_SLOW_FRAME_MS = 33;
const TARGET_FRAME_DURATION_MS = 1_000 / 60;
const MAIN_STALL_FRAME_MS = 50;
const MAIN_SEVERE_STALL_FRAME_MS = 100;
const PROFILER_PREFERENCE_REFRESH_MS = 1_000;

/**
 * Fixed-capacity numeric sample window with constant-time insertion and averaging.
 */
class NumericRingBuffer {
	private readonly values: Float64Array;
	private nextIndex = 0;
	private sampleCount = 0;
	private total = 0;

	/**
	 * Creates a bounded numeric sample window.
	 * @param capacity Maximum number of retained measurements.
	 */
	constructor(private readonly capacity: number) {
		this.values = new Float64Array(capacity);
	}

	/**
	 * Returns the number of retained measurements.
	 * @returns Current sample count.
	 */
	get length(): number {
		return this.sampleCount;
	}

	/**
	 * Appends one finite non-negative measurement, overwriting the oldest at capacity.
	 * @param value Measurement to retain.
	 * @returns True when the value was accepted.
	 */
	push(value: number): boolean {
		if (!Number.isFinite(value) || value < 0) return false;
		if (this.sampleCount === this.capacity) {
			this.total -= this.values[this.nextIndex];
		} else {
			this.sampleCount++;
		}
		this.values[this.nextIndex] = value;
		this.total += value;
		this.nextIndex = (this.nextIndex + 1) % this.capacity;
		return true;
	}

	/**
	 * Removes every retained measurement without reallocating storage.
	 * @returns Nothing.
	 */
	clear(): void {
		this.nextIndex = 0;
		this.sampleCount = 0;
		this.total = 0;
	}

	/**
	 * Computes the arithmetic mean of retained measurements.
	 * @returns Mean value, or zero for an empty buffer.
	 */
	average(): number {
		return this.sampleCount === 0 ? 0 : this.total / this.sampleCount;
	}

	/**
	 * Materializes retained values in oldest-to-newest order.
	 * @returns Numeric sample array suitable for percentile calculation.
	 */
	toArray(): number[] {
		const result = new Array<number>(this.sampleCount);
		const startIndex =
			this.sampleCount === this.capacity ? this.nextIndex : 0;
		for (let index = 0; index < this.sampleCount; index++) {
			result[index] = this.values[(startIndex + index) % this.capacity];
		}
		return result;
	}
}

/**
 * Dev-only profiler for Canvas frame timings.
 */
export class CanvasProfiler {
	private static samples: number[] = [];
	private static readonly workerDrawSamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly localPointerLatencySamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly workerInputLatencySamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly workerReceiveSamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly workerWaitSamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly workerDataLatencySamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly mainFrameIntervalSamples = new NumericRingBuffer(
		WORKER_SAMPLE_LIMIT,
	);
	private static readonly inputTimestamps = new Map<number, number>();
	private static workerFrameCount = 0;
	private static workerMaxQueueDepth = 0;
	private static workerMaxInFlightQueueDepth = 0;
	private static workerMaxPendingQueueDepth = 0;
	private static workerResyncCount = 0;
	private static workerErrorCount = 0;
	private static workerFallbackCount = 0;
	private static mainFrameIntervalCount = 0;
	private static mainDroppedFrameCount = 0;
	private static mainStallsOver50Ms = 0;
	private static mainStallsOver100Ms = 0;
	private static mainMaxFrameIntervalMs = 0;
	private static workerMaxFrameIntervalMs = 0;
	private static workerPreviousFrameTimestamp: number | null = null;
	private static mainPreviousFrameTimestamp: number | null = null;
	private static mainFrameRequest: number | null = null;
	private static captureReferenceCount = 0;
	private static cachedProfilerPreference = false;
	private static profilerPreferenceCheckedAt = Number.NEGATIVE_INFINITY;
	private static mainLongTaskSupported = false;
	private static mainLongTaskCount = 0;
	private static mainLongestTaskMs = 0;
	private static mainLongTaskObserver: PerformanceObserver | null = null;
	private static rendererBackendStatus: RendererBackendStatus = {
		requestedBackend: "auto",
		activeBackend: "initializing",
		fallbackReason: null,
	};
	private static rendererBackendListeners = new Set<
		(status: RendererBackendStatus) => void
	>();
	private static workerLastNotificationAt = 0;
	private static workerNotificationTimeout: ReturnType<typeof setTimeout> | null =
		null;
	private static workerListeners = new Set<
		(snapshot: CanvasWorkerProfilerSnapshot) => void
	>();
	private static workerSynchronizationProviders = new Set<
		() => CanvasWorkerSynchronizationSnapshot
	>();
	private static slowFrames = 0;
	private static verySlowFrames = 0;

	private readonly startTime: number;
	private readonly steps: CanvasProfilerStepDurations = { ...EMPTY_STEPS };
	private lastMark: number;
	private counts: CanvasProfilerCounts = {
		visibleSources: 0,
		renderedRows: 0,
		visibleNotes: 0,
		visibleLinks: 0,
		interactiveRects: 0,
		graphSamples: 0,
		graphBuckets: 0,
		renderedEventPixels: 0,
	};

	private constructor(private readonly reason: string) {
		this.startTime = performance.now();
		this.lastMark = this.startTime;
	}

	/**
	 * Creates a profiler frame only when the dev flag is enabled.
	 * @param reason Render trigger reason.
	 * @returns A profiler instance, or null when profiling is disabled.
	 */
	static start(reason: string): CanvasProfiler | null {
		if (!CanvasProfiler.isEnabled()) return null;
		return new CanvasProfiler(reason);
	}

	/**
	 * Records timing data emitted by the dedicated render worker.
	 * @param metrics Worker draw, input latency, revision, and queue measurements.
	 * @returns Nothing.
	 */
	static recordWorkerFrame(metrics: CanvasWorkerFrameMetrics): void {
		if (!CanvasProfiler.isCaptureActive()) return;
		CanvasProfiler.ensureCaptureObservation();
		CanvasProfiler.workerFrameCount++;
		const inFlightQueueDepth =
			metrics.inFlightQueueDepth ?? metrics.queueDepth ?? 0;
		const pendingQueueDepth = metrics.pendingQueueDepth ?? 0;
		const aggregateQueueDepth =
			metrics.queueDepth ?? inFlightQueueDepth + pendingQueueDepth;
		CanvasProfiler.accumulateWorkerQueueDepth(
			inFlightQueueDepth,
			pendingQueueDepth,
			aggregateQueueDepth,
		);
		CanvasProfiler.workerDrawSamples.push(metrics.drawDurationMs);
		const inputLatencyMs = CanvasProfiler.resolveInputLatency(metrics);
		if (inputLatencyMs !== undefined) {
			CanvasProfiler.workerInputLatencySamples.push(inputLatencyMs);
		}
		if (metrics.workerReceiveDuration !== undefined) {
			CanvasProfiler.workerReceiveSamples.push(metrics.workerReceiveDuration);
		}
		if (metrics.workerWaitDuration !== undefined) {
			CanvasProfiler.workerWaitSamples.push(metrics.workerWaitDuration);
		}
		if (metrics.dataLatencyMs !== undefined) {
			CanvasProfiler.workerDataLatencySamples.push(metrics.dataLatencyMs);
		}

		const now = performance.now();
		if (CanvasProfiler.workerPreviousFrameTimestamp !== null) {
			CanvasProfiler.workerMaxFrameIntervalMs = Math.max(
				CanvasProfiler.workerMaxFrameIntervalMs,
				now - CanvasProfiler.workerPreviousFrameTimestamp,
			);
		}
		CanvasProfiler.workerPreviousFrameTimestamp = now;
		CanvasProfiler.scheduleWorkerNotification(now);

		if (metrics.drawDurationMs <= SLOW_FRAME_MS || !CanvasProfiler.isEnabled()) {
			return;
		}

		console.info("[CanvasWorkerPerf]", {
			...CanvasProfiler.getWorkerSnapshot(),
			...metrics,
		});
	}

	/**
	 * Records one client-side data-queue transition independently of rendered frames.
	 * @param inFlightQueueDepth Data messages sent and awaiting acknowledgement.
	 * @param pendingQueueDepth Keyed data messages waiting for an in-flight slot.
	 * @returns Nothing.
	 */
	static recordWorkerQueueDepth(
		inFlightQueueDepth: number,
		pendingQueueDepth: number,
	): void {
		if (!CanvasProfiler.isCaptureActive()) return;
		CanvasProfiler.ensureCaptureObservation();
		CanvasProfiler.accumulateWorkerQueueDepth(
			inFlightQueueDepth,
			pendingQueueDepth,
			inFlightQueueDepth + pendingQueueDepth,
		);
		CanvasProfiler.scheduleWorkerNotification(performance.now());
	}

	/**
	 * Records raw-input-to-local-pointer-update latency from the main-thread coordinator.
	 * @param latencyMs Elapsed milliseconds from native input receipt to DOM update.
	 * @returns Nothing.
	 */
	static recordLocalPointerLatency(latencyMs: number): void {
		if (!CanvasProfiler.isCaptureActive()) return;
		CanvasProfiler.ensureCaptureObservation();
		if (CanvasProfiler.localPointerLatencySamples.push(latencyMs)) {
			CanvasProfiler.scheduleWorkerNotification(performance.now());
		}
	}

	/**
	 * Associates a coalesced input sequence with its absolute high-resolution timestamp.
	 * @param inputSequence Monotonic input sequence sent to the render worker.
	 * @param inputTimestamp Absolute timeOrigin-plus-now timestamp captured by the listener.
	 * @returns Nothing.
	 */
	static recordInputTimestamp(
		inputSequence: number,
		inputTimestamp: number,
	): void {
		if (
			!CanvasProfiler.isCaptureActive() ||
			!Number.isFinite(inputSequence) ||
			!Number.isFinite(inputTimestamp) ||
			inputSequence < 0 ||
			inputTimestamp < 0
		) {
			return;
		}
		CanvasProfiler.inputTimestamps.set(inputSequence, inputTimestamp);
		if (CanvasProfiler.inputTimestamps.size > WORKER_SAMPLE_LIMIT) {
			const oldestSequence = CanvasProfiler.inputTimestamps.keys().next().value;
			if (typeof oldestSequence === "number") {
				CanvasProfiler.inputTimestamps.delete(oldestSequence);
			}
		}
	}

	/**
	 * Starts one reference-counted benchmark capture session and portable observers.
	 * @returns Idempotent function that releases the capture session.
	 */
	static beginCapture(): () => void {
		CanvasProfiler.captureReferenceCount++;
		CanvasProfiler.ensureCaptureObservation();
		let released = false;
		return () => {
			if (released) return;
			released = true;
			CanvasProfiler.captureReferenceCount = Math.max(
				0,
				CanvasProfiler.captureReferenceCount - 1,
			);
			CanvasProfiler.stopCaptureObservationWhenIdle();
		};
	}

	/**
	 * Subscribes to aggregate worker frame measurements for the renderer harness.
	 * @param listener Callback invoked after every reported worker frame.
	 * @returns Function that removes the listener.
	 */
	static subscribeWorker(
		listener: (snapshot: CanvasWorkerProfilerSnapshot) => void,
	): () => void {
		const releaseCapture = CanvasProfiler.beginCapture();
		CanvasProfiler.workerListeners.add(listener);
		listener(CanvasProfiler.getWorkerSnapshot());
		return () => {
			CanvasProfiler.workerListeners.delete(listener);
			if (
				CanvasProfiler.workerListeners.size === 0 &&
				CanvasProfiler.workerNotificationTimeout !== null
			) {
				clearTimeout(CanvasProfiler.workerNotificationTimeout);
				CanvasProfiler.workerNotificationTimeout = null;
			}
			releaseCapture();
		};
	}

	/**
	 * Publishes the requested renderer, active renderer, and latest fallback reason.
	 * @param status Current renderer backend lifecycle state.
	 * @returns Nothing.
	 */
	static setRendererBackendStatus(status: RendererBackendStatus): void {
		const previous = CanvasProfiler.rendererBackendStatus;
		if (
			previous.requestedBackend === status.requestedBackend &&
			previous.activeBackend === status.activeBackend &&
			previous.fallbackReason === status.fallbackReason
		) {
			return;
		}
		CanvasProfiler.rendererBackendStatus = { ...status };
		CanvasProfiler.rendererBackendListeners.forEach((listener) =>
			listener({ ...CanvasProfiler.rendererBackendStatus }),
		);
	}

	/**
	 * Reads the latest renderer backend diagnostics.
	 * @returns Immutable copy of the current backend status.
	 */
	static getRendererBackendStatus(): RendererBackendStatus {
		return { ...CanvasProfiler.rendererBackendStatus };
	}

	/**
	 * Subscribes to renderer backend lifecycle diagnostics.
	 * @param listener Callback invoked immediately and after backend changes.
	 * @returns Function that removes the listener.
	 */
	static subscribeRendererBackendStatus(
		listener: (status: RendererBackendStatus) => void,
	): () => void {
		CanvasProfiler.rendererBackendListeners.add(listener);
		listener(CanvasProfiler.getRendererBackendStatus());
		return () => {
			CanvasProfiler.rendererBackendListeners.delete(listener);
		};
	}

	/**
	 * Registers a live worker synchronization provider owned by one mounted canvas.
	 * @param provider Callback that reads current client, projection, and revision state.
	 * @returns Function that removes the provider when its canvas unmounts.
	 */
	static registerWorkerSynchronizationProvider(
		provider: () => CanvasWorkerSynchronizationSnapshot,
	): () => void {
		CanvasProfiler.workerSynchronizationProviders.add(provider);
		return () => {
			CanvasProfiler.workerSynchronizationProviders.delete(provider);
		};
	}

	/**
	 * Reads synchronization state directly from the most recently mounted worker canvas.
	 * @returns Live ACK, queue, projection, and rendered revision state, or null without a worker.
	 */
	static getWorkerSynchronizationSnapshot(): CanvasWorkerSynchronizationSnapshot | null {
		let latestSnapshot: CanvasWorkerSynchronizationSnapshot | null = null;
		CanvasProfiler.workerSynchronizationProviders.forEach((provider) => {
			latestSnapshot = provider();
		});
		return latestSnapshot;
	}

	/**
	 * Records one worker-requested authoritative data replay.
	 * @returns Nothing.
	 */
	static recordWorkerResync(): void {
		CanvasProfiler.workerResyncCount++;
		CanvasProfiler.notifyWorkerListeners();
	}

	/**
	 * Records one protocol, asset, staging, or render error reported by the worker.
	 * @returns Nothing.
	 */
	static recordWorkerError(): void {
		CanvasProfiler.workerErrorCount++;
		CanvasProfiler.notifyWorkerListeners();
	}

	/**
	 * Records one fatal transferred-canvas recovery into the legacy backend.
	 * @returns Nothing.
	 */
	static recordWorkerFallback(): void {
		CanvasProfiler.workerFallbackCount++;
		CanvasProfiler.notifyWorkerListeners();
	}

	/**
	 * Returns the current aggregate worker timing snapshot.
	 * @returns Worker frame count, average, p95, and queue measurements.
	 */
	static getWorkerSnapshot(): CanvasWorkerProfilerSnapshot {
		CanvasProfiler.recordMainLongTaskEntries(
			CanvasProfiler.mainLongTaskObserver?.takeRecords() ?? [],
		);
		const endToEndInputLatencyP95Ms = CanvasProfiler.percentile(
			CanvasProfiler.workerInputLatencySamples.toArray(),
			0.95,
		);
		const mainFrameIntervals = CanvasProfiler.mainFrameIntervalSamples.toArray();
		const mainFrameOpportunities =
			CanvasProfiler.mainFrameIntervalCount +
			CanvasProfiler.mainDroppedFrameCount;
		return {
			frameCount: CanvasProfiler.workerFrameCount,
			drawAverageMs: CanvasProfiler.workerDrawSamples.average(),
			drawP95Ms: CanvasProfiler.percentile(
				CanvasProfiler.workerDrawSamples.toArray(),
				0.95,
			),
			localPointerSampleCount: CanvasProfiler.localPointerLatencySamples.length,
			localPointerLatencyP95Ms: CanvasProfiler.percentile(
				CanvasProfiler.localPointerLatencySamples.toArray(),
				0.95,
			),
			inputLatencySampleCount: CanvasProfiler.workerInputLatencySamples.length,
			endToEndInputLatencyP95Ms,
			inputLatencyP95Ms: endToEndInputLatencyP95Ms,
			workerReceiveP95Ms: CanvasProfiler.percentile(
				CanvasProfiler.workerReceiveSamples.toArray(),
				0.95,
			),
			workerWaitP95Ms: CanvasProfiler.percentile(
				CanvasProfiler.workerWaitSamples.toArray(),
				0.95,
			),
			dataLatencySampleCount: CanvasProfiler.workerDataLatencySamples.length,
			dataLatencyP95Ms: CanvasProfiler.percentile(
				CanvasProfiler.workerDataLatencySamples.toArray(),
				0.95,
			),
			maxQueueDepth: CanvasProfiler.workerMaxQueueDepth,
			maxInFlightQueueDepth: CanvasProfiler.workerMaxInFlightQueueDepth,
			maxPendingQueueDepth: CanvasProfiler.workerMaxPendingQueueDepth,
			mainFrameIntervalP95Ms: CanvasProfiler.percentile(
				mainFrameIntervals,
				0.95,
			),
			mainFrameIntervalP99Ms: CanvasProfiler.percentile(
				mainFrameIntervals,
				0.99,
			),
			mainFrameIntervalCount: CanvasProfiler.mainFrameIntervalCount,
			mainMaxFrameIntervalMs: CanvasProfiler.mainMaxFrameIntervalMs,
			workerMaxFrameIntervalMs: CanvasProfiler.workerMaxFrameIntervalMs,
			mainDroppedFrameCount: CanvasProfiler.mainDroppedFrameCount,
			mainDroppedFrameRatio:
				mainFrameOpportunities === 0
					? 0
					: CanvasProfiler.mainDroppedFrameCount / mainFrameOpportunities,
			mainStallsOver50Ms: CanvasProfiler.mainStallsOver50Ms,
			mainStallsOver100Ms: CanvasProfiler.mainStallsOver100Ms,
			resyncCount: CanvasProfiler.workerResyncCount,
			workerErrorCount: CanvasProfiler.workerErrorCount,
			fallbackCount: CanvasProfiler.workerFallbackCount,
			mainLongTaskSupported: CanvasProfiler.mainLongTaskSupported,
			mainLongTaskCount: CanvasProfiler.mainLongTaskCount,
			mainLongestTaskMs: CanvasProfiler.mainLongestTaskMs,
		};
	}

	/**
	 * Clears worker aggregates before a deterministic benchmark run.
	 * @returns Nothing.
	 */
	static resetWorkerMetrics(): void {
		CanvasProfiler.profilerPreferenceCheckedAt = Number.NEGATIVE_INFINITY;
		const restartLongTaskObservation = CanvasProfiler.isCaptureActive();
		CanvasProfiler.mainLongTaskObserver?.takeRecords();
		CanvasProfiler.mainLongTaskObserver?.disconnect();
		CanvasProfiler.mainLongTaskObserver = null;
		CanvasProfiler.mainLongTaskSupported = false;
		CanvasProfiler.workerDrawSamples.clear();
		CanvasProfiler.localPointerLatencySamples.clear();
		CanvasProfiler.workerInputLatencySamples.clear();
		CanvasProfiler.workerReceiveSamples.clear();
		CanvasProfiler.workerWaitSamples.clear();
		CanvasProfiler.workerDataLatencySamples.clear();
		CanvasProfiler.mainFrameIntervalSamples.clear();
		CanvasProfiler.inputTimestamps.clear();
		CanvasProfiler.workerFrameCount = 0;
		CanvasProfiler.workerMaxQueueDepth = 0;
		CanvasProfiler.workerMaxInFlightQueueDepth = 0;
		CanvasProfiler.workerMaxPendingQueueDepth = 0;
		CanvasProfiler.workerSynchronizationProviders.forEach((provider) => {
			const synchronization = provider();
			CanvasProfiler.accumulateWorkerQueueDepth(
				synchronization.inFlightDataMessages,
				synchronization.pendingDataMessages,
				synchronization.inFlightDataMessages +
					synchronization.pendingDataMessages,
			);
		});
		CanvasProfiler.workerResyncCount = 0;
		CanvasProfiler.workerErrorCount = 0;
		CanvasProfiler.workerFallbackCount = 0;
		CanvasProfiler.mainFrameIntervalCount = 0;
		CanvasProfiler.mainDroppedFrameCount = 0;
		CanvasProfiler.mainStallsOver50Ms = 0;
		CanvasProfiler.mainStallsOver100Ms = 0;
		CanvasProfiler.mainMaxFrameIntervalMs = 0;
		CanvasProfiler.workerMaxFrameIntervalMs = 0;
		CanvasProfiler.workerPreviousFrameTimestamp = null;
		CanvasProfiler.mainPreviousFrameTimestamp = null;
		CanvasProfiler.mainLongTaskCount = 0;
		CanvasProfiler.mainLongestTaskMs = 0;
		CanvasProfiler.workerLastNotificationAt = 0;
		if (CanvasProfiler.workerNotificationTimeout !== null) {
			clearTimeout(CanvasProfiler.workerNotificationTimeout);
			CanvasProfiler.workerNotificationTimeout = null;
		}
		if (restartLongTaskObservation) {
			CanvasProfiler.startMainLongTaskObservation();
		}
	}

	/**
	 * Records elapsed time since the previous mark for a named render step.
	 * @param step Render step that just completed.
	 * @returns Nothing.
	 */
	mark(step: CanvasProfilerStep): void {
		const now = performance.now();
		this.steps[step] += now - this.lastMark;
		this.lastMark = now;
	}

	/**
	 * Stores render output counters reported by RenderEngine.
	 * @param counts Per-frame render counters.
	 * @returns Nothing.
	 */
	setCounts(counts: CanvasProfilerCounts): void {
		this.counts = counts;
	}

	/**
	 * Completes the frame and logs slow-frame diagnostics.
	 * @returns Nothing.
	 */
	finish(): void {
		const total = performance.now() - this.startTime;
		CanvasProfiler.samples.push(total);
		if (CanvasProfiler.samples.length > SAMPLE_LIMIT) {
			CanvasProfiler.samples.shift();
		}

		if (total > SLOW_FRAME_MS) {
			CanvasProfiler.slowFrames++;
		}
		if (total > VERY_SLOW_FRAME_MS) {
			CanvasProfiler.verySlowFrames++;
		}

		if (total <= SLOW_FRAME_MS) {
			return;
		}

		const sortedSamples = [...CanvasProfiler.samples].sort((a, b) => a - b);
		const p95Index = Math.max(
			0,
			Math.ceil(sortedSamples.length * 0.95) - 1,
		);
		const average =
			CanvasProfiler.samples.reduce((sum, value) => sum + value, 0) /
			CanvasProfiler.samples.length;
		const flatSteps = Object.fromEntries(
			Object.entries(this.steps).map(([step, value]) => [
				`${step}Ms`,
				Number(value.toFixed(2)),
			]),
		);
		const [topStep = "unknown", topStepMs = 0] = Object.entries(
			this.steps,
		).reduce<[string, number]>(
			(currentTopStep, [step, value]) =>
				value > currentTopStep[1] ? [step, value] : currentTopStep,
			["unknown", 0],
		);

		console.info("[CanvasPerf]", {
			reason: this.reason,
			total: Number(total.toFixed(2)),
			avg: Number(average.toFixed(2)),
			p95: Number((sortedSamples[p95Index] ?? total).toFixed(2)),
			topStep,
			topStepMs: Number(topStepMs.toFixed(2)),
			slowFrames: CanvasProfiler.slowFrames,
			verySlowFrames: CanvasProfiler.verySlowFrames,
			...flatSteps,
			...this.counts,
			steps: Object.fromEntries(
				Object.entries(this.steps).map(([step, value]) => [
					step,
					Number(value.toFixed(2)),
				]),
			),
			counts: this.counts,
		});
	}

	/**
	 * Publishes one throttled aggregate snapshot to renderer-harness subscribers.
	 * @returns Nothing.
	 */
	private static notifyWorkerListeners(): void {
		if (CanvasProfiler.workerListeners.size === 0) return;
		if (CanvasProfiler.workerNotificationTimeout !== null) {
			clearTimeout(CanvasProfiler.workerNotificationTimeout);
			CanvasProfiler.workerNotificationTimeout = null;
		}
		CanvasProfiler.workerLastNotificationAt = performance.now();
		const snapshot = CanvasProfiler.getWorkerSnapshot();
		CanvasProfiler.workerListeners.forEach((listener) => listener(snapshot));
	}

	/**
	 * Schedules a profiler notification no more frequently than once per 250 milliseconds.
	 * @param now Current main-thread high-resolution timestamp.
	 * @returns Nothing.
	 */
	private static scheduleWorkerNotification(now: number): void {
		if (CanvasProfiler.workerListeners.size === 0) return;
		const elapsed = now - CanvasProfiler.workerLastNotificationAt;
		if (CanvasProfiler.workerLastNotificationAt === 0 || elapsed >= 250) {
			CanvasProfiler.notifyWorkerListeners();
			return;
		}
		if (CanvasProfiler.workerNotificationTimeout !== null) return;
		CanvasProfiler.workerNotificationTimeout = setTimeout(() => {
			CanvasProfiler.workerNotificationTimeout = null;
			CanvasProfiler.notifyWorkerListeners();
		}, Math.max(0, 250 - elapsed));
	}

	/**
	 * Resolves end-to-end input latency and discards coalesced sequence timestamps.
	 * @param metrics Frame metrics containing an optional rendered input sequence.
	 * @returns Raw-input-to-frame-acknowledgement latency when available.
	 */
	private static resolveInputLatency(
		metrics: CanvasWorkerFrameMetrics,
	): number | undefined {
		const inputSequence = metrics.inputSequence;
		if (inputSequence === undefined) return metrics.inputLatencyMs;
		const inputTimestamp = CanvasProfiler.inputTimestamps.get(inputSequence);
		for (const sequence of CanvasProfiler.inputTimestamps.keys()) {
			if (sequence <= inputSequence) {
				CanvasProfiler.inputTimestamps.delete(sequence);
			}
		}
		if (inputTimestamp === undefined) return undefined;
		return Math.max(0, CanvasProfiler.getAbsoluteNow() - inputTimestamp);
	}

	/**
	 * Retains peak queue occupancy observed at any client transition.
	 * @param inFlightQueueDepth Sent data messages awaiting acknowledgement.
	 * @param pendingQueueDepth Keyed data messages waiting for a slot.
	 * @param aggregateQueueDepth Total data messages represented by the transition.
	 * @returns Nothing.
	 */
	private static accumulateWorkerQueueDepth(
		inFlightQueueDepth: number,
		pendingQueueDepth: number,
		aggregateQueueDepth: number,
	): void {
		if (
			!Number.isFinite(inFlightQueueDepth) ||
			!Number.isFinite(pendingQueueDepth) ||
			!Number.isFinite(aggregateQueueDepth) ||
			inFlightQueueDepth < 0 ||
			pendingQueueDepth < 0 ||
			aggregateQueueDepth < 0
		) {
			return;
		}
		CanvasProfiler.workerMaxQueueDepth = Math.max(
			CanvasProfiler.workerMaxQueueDepth,
			aggregateQueueDepth,
		);
		CanvasProfiler.workerMaxInFlightQueueDepth = Math.max(
			CanvasProfiler.workerMaxInFlightQueueDepth,
			inFlightQueueDepth,
		);
		CanvasProfiler.workerMaxPendingQueueDepth = Math.max(
			CanvasProfiler.workerMaxPendingQueueDepth,
			pendingQueueDepth,
		);
	}

	/**
	 * Returns an absolute high-resolution timestamp shared across window and worker origins.
	 * @returns performance.timeOrigin plus performance.now in milliseconds.
	 */
	private static getAbsoluteNow(): number {
		return performance.timeOrigin + performance.now();
	}

	/**
	 * Starts portable frame-gap and optional Long Task observation for an active capture.
	 * @returns Nothing.
	 */
	private static ensureCaptureObservation(): void {
		CanvasProfiler.startMainFrameObservation();
		CanvasProfiler.startMainLongTaskObservation();
	}

	/**
	 * Starts the main-window animation-frame monitor when the browser provides it.
	 * @returns Nothing.
	 */
	private static startMainFrameObservation(): void {
		if (
			CanvasProfiler.mainFrameRequest !== null ||
			typeof window === "undefined" ||
			typeof window.requestAnimationFrame !== "function"
		) {
			return;
		}
		CanvasProfiler.mainFrameRequest = window.requestAnimationFrame(
			CanvasProfiler.observeMainFrame,
		);
	}

	/**
	 * Records one visible main-window frame gap and schedules the next observation.
	 * @param timestamp Animation-frame timestamp supplied by the browser.
	 * @returns Nothing.
	 */
	private static observeMainFrame(timestamp: number): void {
		CanvasProfiler.mainFrameRequest = null;
		if (!CanvasProfiler.isCaptureActive()) {
			CanvasProfiler.mainPreviousFrameTimestamp = null;
			CanvasProfiler.stopCaptureObservationWhenIdle();
			return;
		}
		if (typeof document !== "undefined" && document.visibilityState === "hidden") {
			CanvasProfiler.mainPreviousFrameTimestamp = null;
		} else if (CanvasProfiler.mainPreviousFrameTimestamp !== null) {
			CanvasProfiler.recordMainFrameInterval(
				timestamp - CanvasProfiler.mainPreviousFrameTimestamp,
			);
		}
		CanvasProfiler.mainPreviousFrameTimestamp = timestamp;
		CanvasProfiler.startMainFrameObservation();
	}

	/**
	 * Aggregates one portable main-thread frame interval.
	 * @param intervalMs Elapsed milliseconds between visible animation frames.
	 * @returns Nothing.
	 */
	private static recordMainFrameInterval(intervalMs: number): void {
		if (!CanvasProfiler.mainFrameIntervalSamples.push(intervalMs)) return;
		CanvasProfiler.mainFrameIntervalCount++;
		CanvasProfiler.mainMaxFrameIntervalMs = Math.max(
			CanvasProfiler.mainMaxFrameIntervalMs,
			intervalMs,
		);
		CanvasProfiler.mainDroppedFrameCount += Math.max(
			0,
			Math.round(intervalMs / TARGET_FRAME_DURATION_MS) - 1,
		);
		if (intervalMs > MAIN_STALL_FRAME_MS) {
			CanvasProfiler.mainStallsOver50Ms++;
		}
		if (intervalMs > MAIN_SEVERE_STALL_FRAME_MS) {
			CanvasProfiler.mainStallsOver100Ms++;
		}
		CanvasProfiler.scheduleWorkerNotification(performance.now());
	}

	/**
	 * Stops capture observers after the final benchmark and diagnostic consumer leaves.
	 * @returns Nothing.
	 */
	private static stopCaptureObservationWhenIdle(): void {
		if (CanvasProfiler.isCaptureActive()) return;
		if (
			CanvasProfiler.mainFrameRequest !== null &&
			typeof window !== "undefined" &&
			typeof window.cancelAnimationFrame === "function"
		) {
			window.cancelAnimationFrame(CanvasProfiler.mainFrameRequest);
		}
		CanvasProfiler.mainFrameRequest = null;
		CanvasProfiler.mainPreviousFrameTimestamp = null;
		CanvasProfiler.mainLongTaskObserver?.disconnect();
		CanvasProfiler.mainLongTaskObserver = null;
	}

	/**
	 * Starts the benchmark-only main-thread long-task observer when supported.
	 * @returns Nothing.
	 */
	private static startMainLongTaskObservation(): void {
		if (CanvasProfiler.mainLongTaskObserver) return;
		CanvasProfiler.mainLongTaskSupported =
			typeof PerformanceObserver !== "undefined" &&
			PerformanceObserver.supportedEntryTypes?.includes("longtask") === true;
		if (!CanvasProfiler.mainLongTaskSupported) {
			return;
		}
		try {
			CanvasProfiler.mainLongTaskObserver = new PerformanceObserver((list) => {
				CanvasProfiler.recordMainLongTaskEntries(list.getEntries());
				CanvasProfiler.notifyWorkerListeners();
			});
			CanvasProfiler.mainLongTaskObserver.observe({ entryTypes: ["longtask"] });
		} catch {
			CanvasProfiler.mainLongTaskObserver?.disconnect();
			CanvasProfiler.mainLongTaskObserver = null;
			CanvasProfiler.mainLongTaskSupported = false;
		}
	}

	/**
	 * Aggregates delivered or explicitly drained Long Task entries.
	 * @param entries Browser performance entries not previously recorded.
	 * @returns Nothing.
	 */
	private static recordMainLongTaskEntries(
		entries: readonly PerformanceEntry[],
	): void {
		for (const entry of entries) {
			CanvasProfiler.mainLongTaskCount++;
			CanvasProfiler.mainLongestTaskMs = Math.max(
				CanvasProfiler.mainLongestTaskMs,
				entry.duration,
			);
		}
	}

	/**
	 * Computes a nearest-rank percentile without mutating the source samples.
	 * @param samples Measurements to inspect.
	 * @param percentile Percentile expressed from zero through one.
	 * @returns Percentile measurement, or zero for an empty collection.
	 */
	private static percentile(samples: number[], percentile: number): number {
		if (samples.length === 0) return 0;
		const sorted = [...samples].sort((a, b) => a - b);
		const index = Math.max(
			0,
			Math.min(sorted.length - 1, Math.ceil(sorted.length * percentile) - 1),
		);
		return sorted[index];
	}

	/**
	 * Checks whether benchmark or verbose diagnostic capture is currently active.
	 * @returns True while at least one capture reference or the development flag exists.
	 */
	private static isCaptureActive(): boolean {
		return CanvasProfiler.captureReferenceCount > 0 || CanvasProfiler.isEnabled();
	}

	/**
	 * Checks whether verbose Canvas performance diagnostics are enabled.
	 * @returns True when the local development profiler flag is active.
	 */
	private static isEnabled(): boolean {
		if (typeof window === "undefined") return false;
		const now = Date.now();
		const preferenceAge = now - CanvasProfiler.profilerPreferenceCheckedAt;
		if (
			preferenceAge >= 0 &&
			preferenceAge < PROFILER_PREFERENCE_REFRESH_MS
		) {
			return CanvasProfiler.cachedProfilerPreference;
		}
		CanvasProfiler.profilerPreferenceCheckedAt = now;
		try {
			CanvasProfiler.cachedProfilerPreference =
				window.localStorage.getItem("gulp.canvasPerf") === "1";
		} catch (_) {
			CanvasProfiler.cachedProfilerPreference = false;
		}
		return CanvasProfiler.cachedProfilerPreference;
	}
}
