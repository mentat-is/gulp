import type { RendererBackendStatus } from "@/app/body/CanvasRendererBackend";
import type { CanvasWorkerProfilerSnapshot } from "@/app/body/CanvasProfiler";
import type { RendererBenchmarkDiagnostics } from "./RendererTest.benchmark";

export type RendererInteractionScenario =
	| "pointer"
	| "pan"
	| "zoom"
	| "vertical"
	| "mixed";

export type ReleaseGateCriterionStatus = "passed" | "failed" | "unsupported";

export type RendererRefreshRatePreconditionStatus =
	| "passed"
	| "insufficient-samples"
	| "outside-target-range"
	| "unstable";

/** Warm-up display cadence used to validate the release-gate environment. */
export interface RendererRefreshRateBaseline {
	status: RendererRefreshRatePreconditionStatus;
	sampleCount: number;
	medianFrameIntervalMs: number;
	estimatedRefreshRateHz: number;
	p95JitterPercent: number;
}

export interface ReleaseGateResult {
	durationMs: number;
	mainThreadFps: number;
	workerReady: boolean;
	deltaSent: boolean;
	deltaSynchronized: boolean;
	workerStats: CanvasWorkerProfilerSnapshot;
	bootstrapStats: CanvasWorkerProfilerSnapshot;
	totalResyncCount: number;
	totalWorkerErrorCount: number;
	totalFallbackCount: number;
	maximumQueueDepth: number;
	maximumInFlightQueueDepth: number;
	maximumPendingQueueDepth: number;
	backendStatus: RendererBackendStatus;
	scenario: RendererInteractionScenario;
	burst: number;
	refreshRateBaseline: RendererRefreshRateBaseline;
	benchmarkDiagnostics?: RendererBenchmarkDiagnostics;
}

export interface ReleaseGateCriterion {
	id: string;
	label: string;
	measured: string;
	status: ReleaseGateCriterionStatus;
}

type RendererTestTranslate = (
	key: string,
	vars?: Record<string, string | number>,
) => string;

export const DEFAULT_RENDERER_INTERACTION_SCENARIO: RendererInteractionScenario =
	"mixed";
export const DEFAULT_RENDERER_INTERACTION_BURST = 1;
export const MAX_RENDERER_INTERACTION_BURST = 16;
/** Lowest display refresh rate accepted by the documented 60 Hz release gate. */
export const MIN_RENDERER_REFRESH_RATE_HZ = 55;
/** Highest display refresh rate accepted by the documented 60 Hz release gate. */
export const MAX_RENDERER_REFRESH_RATE_HZ = 65;
/** Minimum warm-up intervals required before display cadence is considered representative. */
export const MIN_RENDERER_REFRESH_RATE_SAMPLES = 120;
/** Maximum p95 absolute interval deviation accepted as a stable display cadence. */
export const MAX_RENDERER_REFRESH_RATE_JITTER_PERCENT = 20;

/**
 * Calculates the median of an already sorted numeric collection.
 * @param sortedValues Ascending finite numeric values.
 * @returns Median value, or zero for an empty collection.
 */
function getSortedMedian(sortedValues: readonly number[]): number {
	if (sortedValues.length === 0) return 0;
	const midpoint = Math.floor(sortedValues.length / 2);
	if (sortedValues.length % 2 === 1) return sortedValues[midpoint];
	return (sortedValues[midpoint - 1] + sortedValues[midpoint]) / 2;
}

/**
 * Calculates a nearest-rank percentile from an already sorted collection.
 * @param sortedValues Ascending finite numeric values.
 * @param percentile Percentile expressed from zero through one.
 * @returns Requested percentile, or zero for an empty collection.
 */
function getSortedPercentile(
	sortedValues: readonly number[],
	percentile: number,
): number {
	if (sortedValues.length === 0) return 0;
	const index = Math.max(
		0,
		Math.min(
			sortedValues.length - 1,
			Math.ceil(sortedValues.length * percentile) - 1,
		),
	);
	return sortedValues[index];
}

/**
 * Derives a stable display refresh baseline from consecutive warm-up rAF intervals.
 * @param frameIntervalsMs Consecutive animation-frame intervals captured during warm-up.
 * @returns Median cadence, estimated refresh rate, jitter, and blocking precondition status.
 */
export function evaluateRendererRefreshRateBaseline(
	frameIntervalsMs: readonly number[],
): RendererRefreshRateBaseline {
	const validIntervals = frameIntervalsMs
		.filter((interval) => Number.isFinite(interval) && interval > 0)
		.sort((left, right) => left - right);
	const medianFrameIntervalMs = getSortedMedian(validIntervals);
	const absoluteDeviations = validIntervals
		.map((interval) => Math.abs(interval - medianFrameIntervalMs))
		.sort((left, right) => left - right);
	const p95AbsoluteDeviationMs = getSortedPercentile(absoluteDeviations, 0.95);
	const estimatedRefreshRateHz = medianFrameIntervalMs > 0
		? 1_000 / medianFrameIntervalMs
		: 0;
	const p95JitterPercent = medianFrameIntervalMs > 0
		? (p95AbsoluteDeviationMs / medianFrameIntervalMs) * 100
		: 0;
	const comparisonEpsilonHz =
		Number.EPSILON * Math.max(1, estimatedRefreshRateHz) * 4;
	const comparisonEpsilonPercent =
		Number.EPSILON * Math.max(1, p95JitterPercent) * 4;

	let status: RendererRefreshRatePreconditionStatus = "passed";
	if (validIntervals.length < MIN_RENDERER_REFRESH_RATE_SAMPLES) {
		status = "insufficient-samples";
	} else if (
		p95JitterPercent >
		MAX_RENDERER_REFRESH_RATE_JITTER_PERCENT + comparisonEpsilonPercent
	) {
		status = "unstable";
	} else if (
		estimatedRefreshRateHz <
			MIN_RENDERER_REFRESH_RATE_HZ - comparisonEpsilonHz ||
		estimatedRefreshRateHz >
			MAX_RENDERER_REFRESH_RATE_HZ + comparisonEpsilonHz
	) {
		status = "outside-target-range";
	}

	return {
		status,
		sampleCount: validIntervals.length,
		medianFrameIntervalMs,
		estimatedRefreshRateHz,
		p95JitterPercent,
	};
}

/**
 * Creates the pre-warm-up refresh baseline used by unsupported and readiness failures.
 * @returns Empty insufficient-sample baseline.
 */
export function createEmptyRendererRefreshRateBaseline(): RendererRefreshRateBaseline {
	return {
		status: "insufficient-samples",
		sampleCount: 0,
		medianFrameIntervalMs: 0,
		estimatedRefreshRateHz: 0,
		p95JitterPercent: 0,
	};
}

/**
 * Formats one refresh-rate baseline using localized status templates.
 * @param baseline Evaluated warm-up display cadence.
 * @param translate Active locale translation function.
 * @returns Localized cadence measurement and precondition explanation.
 */
export function formatRendererRefreshRateBaseline(
	baseline: RendererRefreshRateBaseline,
	translate: RendererTestTranslate,
): string {
	if (baseline.status === "insufficient-samples") {
		return translate("rendererTest.refreshRateInsufficient", {
			count: baseline.sampleCount,
			minimum: MIN_RENDERER_REFRESH_RATE_SAMPLES,
		});
	}

	const measurement = translate("rendererTest.refreshRateMeasurement", {
		hz: baseline.estimatedRefreshRateHz.toFixed(2),
		median: baseline.medianFrameIntervalMs.toFixed(2),
		jitter: baseline.p95JitterPercent.toFixed(2),
	});
	if (baseline.status === "unstable") {
		return translate("rendererTest.refreshRateUnstable", {
			measurement,
			jitterLimit: MAX_RENDERER_REFRESH_RATE_JITTER_PERCENT,
		});
	}
	if (baseline.status === "outside-target-range") {
		return translate("rendererTest.refreshRateOutsideRange", {
			measurement,
			minimum: MIN_RENDERER_REFRESH_RATE_HZ,
			maximum: MAX_RENDERER_REFRESH_RATE_HZ,
		});
	}
	return translate("rendererTest.refreshRatePassed", {
		measurement,
		minimum: MIN_RENDERER_REFRESH_RATE_HZ,
		maximum: MAX_RENDERER_REFRESH_RATE_HZ,
		jitterLimit: MAX_RENDERER_REFRESH_RATE_JITTER_PERCENT,
	});
}

/**
 * Reads a supported interaction scenario from one URL search string.
 * @param search Browser URL search string.
 * @returns Supported scenario, defaulting to the mixed production workload.
 */
export function getRendererInteractionScenario(
	search: string,
): RendererInteractionScenario {
	const requestedScenario = new URLSearchParams(search).get("scenario");
	return requestedScenario === "pointer" ||
		requestedScenario === "pan" ||
		requestedScenario === "zoom" ||
		requestedScenario === "vertical"
		? requestedScenario
		: DEFAULT_RENDERER_INTERACTION_SCENARIO;
}

/**
 * Reads and bounds the number of native events emitted during each benchmark tick.
 * @param search Browser URL search string.
 * @returns Integer burst size from one through sixteen.
 */
export function getRendererInteractionBurst(search: string): number {
	const requestedBurst = Number.parseInt(
		new URLSearchParams(search).get("burst") ?? "",
		10,
	);
	if (!Number.isFinite(requestedBurst)) {
		return DEFAULT_RENDERER_INTERACTION_BURST;
	}
	return Math.max(
		1,
		Math.min(MAX_RENDERER_INTERACTION_BURST, requestedBurst),
	);
}

/**
 * Reports whether one scenario exercises local pointer rendering.
 * @param scenario Selected benchmark interaction scenario.
 * @returns True for pointer-only and mixed workloads.
 */
export function rendererScenarioUsesPointer(
	scenario: RendererInteractionScenario,
): boolean {
	return scenario === "pointer" || scenario === "mixed";
}

/**
 * Reports whether one scenario produces viewport frames during measurement.
 * @param scenario Selected benchmark interaction scenario.
 * @returns True for every workload except pointer-only input.
 */
export function rendererScenarioUsesViewport(
	scenario: RendererInteractionScenario,
): boolean {
	return scenario !== "pointer";
}

/**
 * Creates a fully zeroed profiler snapshot for unsupported and pre-start failures.
 * @returns Empty snapshot matching the public profiler contract.
 */
export function createEmptyWorkerSnapshot(): CanvasWorkerProfilerSnapshot {
	return {
		frameCount: 0,
		drawAverageMs: 0,
		drawP95Ms: 0,
		localPointerSampleCount: 0,
		localPointerLatencyP95Ms: 0,
		inputLatencySampleCount: 0,
		endToEndInputLatencyP95Ms: 0,
		inputLatencyP95Ms: 0,
		workerReceiveP95Ms: 0,
		workerWaitP95Ms: 0,
		dataLatencySampleCount: 0,
		dataLatencyP95Ms: 0,
		maxQueueDepth: 0,
		maxInFlightQueueDepth: 0,
		maxPendingQueueDepth: 0,
		mainFrameIntervalP95Ms: 0,
		mainFrameIntervalP99Ms: 0,
		mainFrameIntervalCount: 0,
		mainMaxFrameIntervalMs: 0,
		mainDroppedFrameCount: 0,
		mainDroppedFrameRatio: 0,
		mainStallsOver50Ms: 0,
		mainStallsOver100Ms: 0,
		resyncCount: 0,
		workerErrorCount: 0,
		fallbackCount: 0,
		mainLongTaskSupported: false,
		mainLongTaskCount: 0,
		mainLongestTaskMs: 0,
	};
}

/**
 * Builds one pass/fail criterion from a boolean condition.
 * @param id Stable criterion identifier.
 * @param label Localized criterion label.
 * @param measured Human-readable measurement and threshold.
 * @param passed Whether the measurement met its release threshold.
 * @returns Release criterion with an explicit status.
 */
function createCriterion(
	id: string,
	label: string,
	measured: string,
	passed: boolean,
): ReleaseGateCriterion {
	return { id, label, measured, status: passed ? "passed" : "failed" };
}

/**
 * Converts one completed benchmark result into portable release criteria.
 * @param result Completed post-warm-up benchmark measurements.
 * @param translate Active locale translation function.
 * @returns Ordered criteria including a neutral unsupported Long Task result.
 */
export function buildReleaseGateCriteria(
	result: ReleaseGateResult,
	translate: RendererTestTranslate,
): ReleaseGateCriterion[] {
	const stats = result.workerStats;
	const criteria: ReleaseGateCriterion[] = [
		createCriterion(
			"requested-backend",
			translate("rendererTest.requestedBackend"),
			result.backendStatus.requestedBackend,
			result.backendStatus.requestedBackend === "worker",
		),
		createCriterion(
			"active-backend",
			translate("rendererTest.activeBackend"),
			result.backendStatus.activeBackend,
			result.backendStatus.activeBackend === "worker",
		),
		createCriterion(
			"worker-ready",
			translate("rendererTest.gateWorkerReady"),
			translate(
				result.workerReady ? "rendererTest.ready" : "rendererTest.notReady",
			),
			result.workerReady,
		),
		createCriterion(
			"refresh-rate-precondition",
			translate("rendererTest.refreshRatePrecondition"),
			formatRendererRefreshRateBaseline(
				result.refreshRateBaseline,
				translate,
			),
			result.refreshRateBaseline.status === "passed",
		),
	];

	if (result.refreshRateBaseline.status !== "passed") {
		return criteria;
	}

	criteria.push(
		createCriterion(
			"main-fps",
			translate("rendererTest.fps"),
			`${result.mainThreadFps.toFixed(2)} / ≥ 59`,
			result.mainThreadFps >= 59,
		),
		createCriterion(
			"main-frame-p95",
			translate("rendererTest.frameIntervalP95"),
			`${stats.mainFrameIntervalP95Ms.toFixed(2)}ms / ≤ 20ms`,
			stats.mainFrameIntervalCount > 0 && stats.mainFrameIntervalP95Ms <= 20,
		),
		createCriterion(
			"main-frame-max",
			translate("rendererTest.maximumFrameInterval"),
			`${stats.mainMaxFrameIntervalMs.toFixed(2)}ms / ≤ 50ms`,
			stats.mainFrameIntervalCount > 0 && stats.mainMaxFrameIntervalMs <= 50,
		),
		createCriterion(
			"dropped-frame-ratio",
			translate("rendererTest.droppedFrameRatio"),
			`${(stats.mainDroppedFrameRatio * 100).toFixed(2)}% / ≤ 1%`,
			stats.mainFrameIntervalCount > 0 && stats.mainDroppedFrameRatio <= 0.01,
		),
	);

	if (rendererScenarioUsesPointer(result.scenario)) {
		criteria.push(
			createCriterion(
				"local-pointer-latency",
				translate("rendererTest.localPointerLatencyP95"),
				`${stats.localPointerLatencyP95Ms.toFixed(2)}ms / ≤ 20ms`,
				stats.localPointerSampleCount > 0 &&
					stats.localPointerLatencyP95Ms <= 20,
			),
		);
	}

	if (rendererScenarioUsesViewport(result.scenario)) {
		criteria.push(
			createCriterion(
				"worker-draw",
				translate("rendererTest.workerDrawP95"),
				`${stats.drawP95Ms.toFixed(2)}ms / ≤ 16.7ms`,
				stats.frameCount > 0 && stats.drawP95Ms <= 16.7,
			),
			createCriterion(
				"input-latency",
				translate("rendererTest.endToEndLatencyP95"),
				`${stats.endToEndInputLatencyP95Ms.toFixed(2)}ms / ≤ 33.4ms`,
				stats.inputLatencySampleCount > 0 &&
					stats.endToEndInputLatencyP95Ms <= 33.4,
			),
		);
	}

	criteria.push(
		createCriterion(
			"data-latency",
			translate("rendererTest.dataLatencyP95"),
			`${stats.dataLatencyP95Ms.toFixed(2)}ms / ≤ 33.4ms`,
			result.deltaSent &&
				result.deltaSynchronized &&
				stats.dataLatencySampleCount > 0 &&
				stats.dataLatencyP95Ms <= 33.4,
		),
		createCriterion(
			"data-queue",
			translate("rendererTest.maxDataQueue"),
			`${result.maximumQueueDepth} / ≤ 2`,
			result.maximumQueueDepth <= 2,
		),
	);

	criteria.push(
		stats.mainLongTaskSupported
			? createCriterion(
					"long-task",
					translate("rendererTest.gateMainLongestTask"),
					`${stats.mainLongestTaskMs.toFixed(2)}ms / ≤ 50ms`,
					stats.mainLongestTaskMs <= 50,
				)
			: {
					id: "long-task",
					label: translate("rendererTest.gateMainLongestTask"),
					measured: translate("rendererTest.unsupported"),
					status: "unsupported",
				},
		createCriterion(
			"resync",
			translate("rendererTest.gateResyncs"),
			`${result.totalResyncCount} / 0`,
			result.totalResyncCount === 0,
		),
		createCriterion(
			"worker-errors",
			translate("requests.errors"),
			`${result.totalWorkerErrorCount} / 0`,
			result.totalWorkerErrorCount === 0,
		),
		createCriterion(
			"fallbacks",
			translate("rendererTest.gateFallbacks"),
			`${result.totalFallbackCount} / 0`,
			result.totalFallbackCount === 0 &&
				result.backendStatus.fallbackReason === null,
		),
	);
	return criteria;
}

/**
 * Evaluates all blocking criteria while treating unsupported optional APIs neutrally.
 * @param result Completed renderer release-gate result.
 * @returns True when no supported release criterion failed.
 */
export function didRendererReleaseGatePass(result: ReleaseGateResult): boolean {
	return buildReleaseGateCriteria(result, (key) => key).every(
		(criterion) => criterion.status !== "failed",
	);
}
