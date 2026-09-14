import type { Source } from "@/entities/Source";
import type { QueryWorkerDiagnostics } from "@/event-stream";
import {
	evaluateRendererQueryReplayGate,
	getRendererQueryReplayProfile,
	getRendererQueryReplayConfiguration,
	getRendererQueryReplaySources,
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
		journalWrites: {
			active: 0,
			maximumActive: 0,
			completed: 0,
			currentBytes: 0,
			maximumBytes: 0,
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
		maximumVisibleProgressLatencyMs: 1_999,
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
		expect(getRendererQueryReplayProfile("?queryReplay=ingestion")).toBe(
			"ingestion",
		);
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

	it("measures visible committed progress independently of gaps between offscreen updates", () => {
		const render = createRenderDiagnostics();
		render.maximumBuildingFrameIntervalMs = 30_000;
		const evaluate = () => evaluateRendererQueryReplayGate("steady", createSteadyResult(), render, 128 * 1024 ** 2, 64 * 1024 ** 2);
		expect(evaluate().buildingCadence).toBe(true);
		render.maximumVisibleProgressLatencyMs = 2_001;
		expect(evaluate().buildingCadence).toBe(false);
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

	it("requires ingestion streams and journal writes to drain at completion", () => {
		const result = createSteadyResult();
		result.queryDoneReceived = false;
		result.diagnostics.journalWrites.maximumActive = 200;
		result.diagnostics.journalWrites.maximumBytes = 256 * 1024 ** 2;
		const gate = evaluateRendererQueryReplayGate(
			"ingestion",
			result,
			createRenderDiagnostics(),
			4 * 1024 ** 2,
			1024 ** 2,
		);
		expect(gate.passed).toBe(true);
		expect(gate.terminalDrain).toBe(true);

		result.diagnostics.journalWrites.active = 1;
		expect(
			evaluateRendererQueryReplayGate(
				"ingestion",
				result,
				createRenderDiagnostics(),
				4 * 1024 ** 2,
				1024 ** 2,
			).passed,
		).toBe(false);
	});
});

describe("multi-source replay configuration", () => {
	it("allocates exactly twenty million events over 140 uniform or skewed sources", () => {
		for (const distribution of ["uniform", "skewed"] as const) {
			const config = getRendererQueryReplayConfiguration(
				`?docs=20m&sources=140&distribution=${distribution}`,
			);
			const sources = getRendererQueryReplaySources({
				...config,
				sourceId: SOURCE_ID,
			});
			expect(config).toEqual({
				targetEvents: 20_000_000,
				sourceCount: 140,
				distribution,
				cacheWarnings: false,
				commitDelayMs: 0,
				ingestionConcurrency: 1,
			});
			expect(sources).toHaveLength(140);
			expect(sources.reduce((sum, source) => sum + source.count, 0)).toBe(
				20_000_000,
			);
			expect(sources[139].offset + sources[139].count).toBe(20_000_000);
			if (distribution === "skewed") {
				expect(sources[0].count).toBe(18_000_000);
			}
		}
	});

	it("enables cache warnings for the 500k/202-source interaction profile", () => {
		expect(
			getRendererQueryReplayConfiguration(
				"?docs=500000&sources=202&cacheWarnings=all",
			),
		).toEqual({
			targetEvents: 500_000,
			sourceCount: 202,
			distribution: "uniform",
			cacheWarnings: true,
			commitDelayMs: 0,
			ingestionConcurrency: 1,
		});
	});

	it("defaults the ingestion profile to 200 sources with delayed commits", () => {
		expect(
			getRendererQueryReplayConfiguration("?queryReplay=ingestion"),
		).toEqual({
			targetEvents: 2_000_000,
			sourceCount: 200,
			distribution: "uniform",
			cacheWarnings: false,
			commitDelayMs: 10,
			ingestionConcurrency: 5,
		});
	});

	it("allows the ingestion profile to reproduce the former unbounded admission", () => {
		expect(
			getRendererQueryReplayConfiguration(
				"?queryReplay=ingestion&ingestionConcurrency=200",
			).ingestionConcurrency,
		).toBe(200);
	});
});
