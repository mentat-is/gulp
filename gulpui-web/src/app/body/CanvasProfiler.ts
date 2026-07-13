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
const SLOW_FRAME_MS = 16;
const VERY_SLOW_FRAME_MS = 33;

/**
 * Dev-only profiler for Canvas frame timings.
 */
export class CanvasProfiler {
	private static samples: number[] = [];
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
		if (typeof window === "undefined") {
			return null;
		}

		try {
			if (window.localStorage.getItem("gulp.canvasPerf") !== "1") {
				return null;
			}
		} catch (_) {
			return null;
		}

		return new CanvasProfiler(reason);
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
}
