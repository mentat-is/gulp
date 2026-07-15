import {
	buildReleaseGateCriteria,
	createEmptyWorkerSnapshot,
	didRendererReleaseGatePass,
	evaluateRendererRefreshRateBaseline,
	getRendererInteractionBurst,
	getRendererInteractionScenario,
	MIN_RENDERER_REFRESH_RATE_SAMPLES,
	type ReleaseGateResult,
} from "@/page/RendererTest.metrics";

/**
 * Creates one release result whose portable criteria all pass.
 * @returns Deterministic result suitable for criterion boundary tests.
 */
function createPassingResult(): ReleaseGateResult {
	const workerStats = {
		...createEmptyWorkerSnapshot(),
		frameCount: 120,
		drawAverageMs: 5,
		drawP95Ms: 10,
		inputLatencySampleCount: 120,
		endToEndInputLatencyP95Ms: 20,
		inputLatencyP95Ms: 20,
		workerReceiveP95Ms: 1,
		workerWaitP95Ms: 5,
		dataLatencySampleCount: 1,
		dataLatencyP95Ms: 15,
		maxInFlightQueueDepth: 2,
		mainFrameIntervalCount: 1_800,
		mainFrameIntervalP95Ms: 16.7,
		mainFrameIntervalP99Ms: 18,
		mainMaxFrameIntervalMs: 40,
		mainDroppedFrameRatio: 0.01,
	};
	return {
		durationMs: 30_000,
		mainThreadFps: 59,
		workerReady: true,
		deltaSent: true,
		deltaSynchronized: true,
		workerStats,
		bootstrapStats: workerStats,
		totalResyncCount: 0,
		totalWorkerErrorCount: 0,
		totalFallbackCount: 0,
		maximumQueueDepth: 2,
		maximumInFlightQueueDepth: 2,
		maximumPendingQueueDepth: 0,
		backendStatus: {
			requestedBackend: "worker",
			activeBackend: "worker",
			fallbackReason: null,
		},
		scenario: "pan",
		burst: 1,
		refreshRateBaseline: evaluateRendererRefreshRateBaseline(
			new Array(MIN_RENDERER_REFRESH_RATE_SAMPLES).fill(1_000 / 60),
		),
	};
}

describe("RendererTest metrics", () => {
	it("parses deterministic interaction scenarios with a mixed fallback", () => {
		expect(getRendererInteractionScenario("?scenario=pointer")).toBe("pointer");
		expect(getRendererInteractionScenario("?scenario=vertical")).toBe("vertical");
		expect(getRendererInteractionScenario("?scenario=unknown")).toBe("mixed");
		expect(getRendererInteractionScenario("")).toBe("mixed");
	});

	it("bounds native event bursts to the supported range", () => {
		expect(getRendererInteractionBurst("")).toBe(1);
		expect(getRendererInteractionBurst("?burst=0")).toBe(1);
		expect(getRendererInteractionBurst("?burst=4")).toBe(4);
		expect(getRendererInteractionBurst("?burst=99")).toBe(16);
		expect(getRendererInteractionBurst("?burst=invalid")).toBe(1);
	});

	it("accepts refresh-rate and jitter samples at their documented boundaries", () => {
		for (const refreshRateHz of [55, 65]) {
			const baseline = evaluateRendererRefreshRateBaseline(
				new Array(MIN_RENDERER_REFRESH_RATE_SAMPLES).fill(
					1_000 / refreshRateHz,
				),
			);

			expect(baseline.status).toBe("passed");
			expect(
				Math.abs(baseline.estimatedRefreshRateHz - refreshRateHz) < 0.000001,
			).toBe(true);
		}

		const baseIntervalMs = 1_000 / 60;
		const jitterBoundary = evaluateRendererRefreshRateBaseline([
			...new Array(MIN_RENDERER_REFRESH_RATE_SAMPLES - 7).fill(
				baseIntervalMs,
			),
			...new Array(7).fill(baseIntervalMs * 1.2),
		]);
		expect(jitterBoundary.status).toBe("passed");
		expect(Math.abs(jitterBoundary.p95JitterPercent - 20) < 0.000001).toBe(
			true,
		);
	});

	it("rejects high-refresh and unstable 60 Hz warm-up cadence", () => {
		const highRefreshBaseline = evaluateRendererRefreshRateBaseline(
			new Array(MIN_RENDERER_REFRESH_RATE_SAMPLES).fill(1_000 / 120),
		);
		const unstableBaseline = evaluateRendererRefreshRateBaseline(
			Array.from(
				{ length: MIN_RENDERER_REFRESH_RATE_SAMPLES },
				(_, index) => index % 5 === 0 ? 1_000 / 30 : 1_000 / 60,
			),
		);

		expect(highRefreshBaseline.status).toBe("outside-target-range");
		expect(unstableBaseline.status).toBe("unstable");
		expect(unstableBaseline.p95JitterPercent > 20).toBe(true);
		const highRefreshResult = createPassingResult();
		highRefreshResult.refreshRateBaseline = highRefreshBaseline;
		expect(didRendererReleaseGatePass(highRefreshResult)).toBe(false);
	});

	it("fails the gate when warm-up cannot provide enough cadence samples", () => {
		const result = createPassingResult();
		result.refreshRateBaseline = evaluateRendererRefreshRateBaseline(
			new Array(MIN_RENDERER_REFRESH_RATE_SAMPLES - 1).fill(1_000 / 60),
		);
		const criteria = buildReleaseGateCriteria(result, (key) => key);

		expect(result.refreshRateBaseline.status).toBe("insufficient-samples");
		expect(criteria.map((criterion) => criterion.id)).toEqual([
			"requested-backend",
			"active-backend",
			"worker-ready",
			"refresh-rate-precondition",
		]);
		expect(didRendererReleaseGatePass(result)).toBe(false);
	});

	it("treats an unsupported Long Task API as neutral instead of passed", () => {
		const result = createPassingResult();
		const criteria = buildReleaseGateCriteria(result, (key) => key);
		const longTask = criteria.find((criterion) => criterion.id === "long-task");
		const workerReady = criteria.find(
			(criterion) => criterion.id === "worker-ready",
		);

		expect(longTask?.status).toBe("unsupported");
		expect(workerReady?.measured).toBe("rendererTest.ready");
		expect(didRendererReleaseGatePass(result)).toBe(true);
	});

	it("passes exact portable thresholds and fails values beyond them", () => {
		const passingResult = createPassingResult();
		expect(didRendererReleaseGatePass(passingResult)).toBe(true);

		const failingResult: ReleaseGateResult = {
			...passingResult,
			workerStats: {
				...passingResult.workerStats,
				mainMaxFrameIntervalMs: 50.01,
			},
		};
		expect(didRendererReleaseGatePass(failingResult)).toBe(false);
	});

	it("fails an active legacy backend or recorded fallback", () => {
		const passingResult = createPassingResult();
		const fallbackResult: ReleaseGateResult = {
			...passingResult,
			totalFallbackCount: 1,
			backendStatus: {
				requestedBackend: "worker",
				activeBackend: "legacy",
				fallbackReason: "fatal worker error",
			},
		};

		expect(didRendererReleaseGatePass(fallbackResult)).toBe(false);
	});

	it("fails when pending work pushes the aggregate data queue above two", () => {
		const passingResult = createPassingResult();
		const queuedResult: ReleaseGateResult = {
			...passingResult,
			maximumQueueDepth: 5,
			maximumInFlightQueueDepth: 2,
			maximumPendingQueueDepth: 3,
		};
		const queueCriterion = buildReleaseGateCriteria(
			queuedResult,
			(key) => key,
		).find((criterion) => criterion.id === "data-queue");

		expect(queueCriterion?.measured).toBe("5 / ≤ 2");
		expect(queueCriterion?.status).toBe("failed");
		expect(didRendererReleaseGatePass(queuedResult)).toBe(false);
	});

	it("fails data qualification until the measured delta is synchronized", () => {
		const result: ReleaseGateResult = {
			...createPassingResult(),
			deltaSynchronized: false,
		};
		const dataLatency = buildReleaseGateCriteria(
			result,
			(key) => key,
		).find((criterion) => criterion.id === "data-latency");

		expect(dataLatency?.status).toBe("failed");
		expect(didRendererReleaseGatePass(result)).toBe(false);
	});
});
