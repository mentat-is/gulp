import type { Source } from "@/entities/Source";
import type { QueryWorkerDiagnostics } from "@/event-stream";
import {
	evaluateRendererQueryReplayGate,
	getRendererQueryReplayProfile,
	RENDERER_QUERY_REPLAY_TARGET_EVENTS,
	type RendererQueryReplayRenderDiagnostics,
	type RendererQueryReplayResult,
} from "@/page/RendererTest.query-replay";

const SOURCE_ID = "renderer-replay-source" as Source.Id;

/** Creates one passing terminal QueryWorker result for gate boundary tests. */
function createSteadyResult(): RendererQueryReplayResult {
	const diagnostics: QueryWorkerDiagnostics = {
		capturedAtMs: 1,
		activeStreams: 0,
		loadedOperations: 1,
		rawBacklog: {
			currentBytes: 0,
			maximumBytes: 32 * 1024 ** 2,
			maximumStreamBytes: 32 * 1024 ** 2,
		},
		normalizedBacklog: {
			currentDocuments: 0,
			maximumDocuments: 32_768,
		},
		commits: {
			active: 0,
			maximumActive: 2,
			completed: 857,
			averageLatencyMs: 10,
			maximumLatencyMs: 25,
		},
		pages: {
			created: 855,
			documents: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
			capacity: 855 * 16_384,
			fillRatio:
				RENDERER_QUERY_REPLAY_TARGET_EVENTS / (855 * 16_384),
		},
		projections: {
			started: 10,
			completed: 10,
			superseded: 0,
			retries: 0,
		},
	};
	return {
		status: "complete",
		processedEvents: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
		queryDoneReceived: true,
		streamCompleted: true,
		latestSummary: {
			requestId: "renderer-query-replay",
			operationId: "operation",
			operationEpoch: 1,
			summary: {
				sourceId: SOURCE_ID,
				generation: 1,
				revision: 857,
				authorityRevision: 857,
				status: "complete",
				eventCount: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
				minTimestamp: 1,
				maxTimestamp: 2,
				hashMin: 0,
				hashMax: 8_000,
				activeCacheGeneration: 1,
				cacheHistory: [1],
				cacheHistoryCursor: 0,
				nextGeneration: 2,
			},
		},
		failure: null,
		diagnostics,
	};
}

/** Creates passing render-side diagnostics for the steady profile. */
function createRenderDiagnostics(): RendererQueryReplayRenderDiagnostics {
	return {
		buildingFrames: 10,
		maximumBuildingFrameIntervalMs: 1_999,
		completeSummaryToProjectionMs: 4_999,
		terminalProjectionEventCount: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
		terminalAckToFrameMs: 249,
		maximumIntervalWithoutFrameMs: 100,
		oldestTerminalTargetAgeMs: 0,
		acknowledgedDataSequence: 20,
		renderedDataSequence: 20,
		resyncCount: 0,
		workerErrorCount: 0,
		fallbackCount: 0,
		maximumRenderQueueDepth: 2,
	};
}

describe("RendererTest query replay", () => {
	it("selects replay profiles without changing existing benchmark URLs", () => {
		expect(getRendererQueryReplayProfile("?queryReplay=steady")).toBe("steady");
		expect(getRendererQueryReplayProfile("?queryReplay=burst")).toBe("burst");
		expect(getRendererQueryReplayProfile("?events=30m")).toBeNull();
	});

	it("accepts the exact 14M, 855-page steady boundary", () => {
		const gate = evaluateRendererQueryReplayGate(
			"steady",
			createSteadyResult(),
			createRenderDiagnostics(),
			128 * 1024 ** 2,
			64 * 1024 ** 2,
		);
		expect(gate.passed).toBe(true);
	});

	it("rejects stale render counts and terminal timing above the gate", () => {
		const render = createRenderDiagnostics();
		render.terminalProjectionEventCount -= 1;
		render.terminalAckToFrameMs = 251;
		const gate = evaluateRendererQueryReplayGate(
			"steady",
			createSteadyResult(),
			render,
			128 * 1024 ** 2,
			64 * 1024 ** 2,
		);
		expect(gate.eventCount).toBe(false);
		expect(gate.terminalFrame).toBe(false);
		expect(gate.passed).toBe(false);
	});

	it("accepts only a controlled backlog result for the burst profile", () => {
		const result = createSteadyResult();
		result.status = "backlog";
		result.streamCompleted = false;
		result.failure = {
			requestId: "renderer-query-replay",
			operationId: "operation",
			operationEpoch: 1,
			reason: "backlog",
			message: "bounded",
		};
		const gate = evaluateRendererQueryReplayGate(
			"burst",
			result,
			createRenderDiagnostics(),
			128 * 1024 ** 2,
			64 * 1024 ** 2,
		);
		expect(gate.passed).toBe(true);
		result.diagnostics.rawBacklog.maximumStreamBytes = 64 * 1024 ** 2 + 1;
		expect(
			evaluateRendererQueryReplayGate(
				"burst",
				result,
				createRenderDiagnostics(),
				128 * 1024 ** 2,
				64 * 1024 ** 2,
			).rawBacklog,
		).toBe(false);
	});
});
