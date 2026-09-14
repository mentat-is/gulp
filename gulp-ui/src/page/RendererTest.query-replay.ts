import type { Source } from "@/entities/Source";
import type {
	QueryRenderWindowDescriptor,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryWorkerDiagnostics,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

export const RENDERER_QUERY_REPLAY_TARGET_EVENTS = 14_000_000;
export const RENDERER_QUERY_REPLAY_CHUNK_EVENTS = 10_000;

export type RendererQueryReplayProfile = "steady" | "burst" | "ingestion";
export type RendererQueryReplayStatus =
	| "idle"
	| "running"
	| "complete"
	| "backlog"
	| "failed"
	| "cancelled";

/** Immutable synthetic QuerySocketSession replay configuration. */
export interface RendererQueryReplayStartOptions {
	profile: RendererQueryReplayProfile;
	cacheNamespaceId: string;
	operationId: string;
	operationEpoch: number;
	sourceId: Source.Id;
	targetEvents: number;
	sourceCount?: number;
	distribution?: "uniform" | "skewed";
	cacheWarnings?: boolean;
	chunkEvents: number;
	documentMinTimestamp: number;
	frameSpanMs: number;
	rawGlobalByteLimit?: number;
	rawStreamByteLimit?: number;
	commitDelayMs?: number;
	ingestionConcurrency?: number;
}

/** Terminal dedicated-worker result with no exact document payloads. */
export interface RendererQueryReplayResult {
	targetEvents?: number;
	sourceSummaries?: QuerySourceSummaryEvent[];
	expectedPages?: number;
	cacheHits?: number;
	sampledDocuments?: number;
	status: RendererQueryReplayStatus;
	processedEvents: number;
	queryDoneReceived: boolean;
	streamCompleted: boolean;
	latestSummary: QuerySourceSummaryEvent | null;
	failure: QueryStreamFailureEvent | null;
	diagnostics: QueryWorkerDiagnostics;
}

/** Compact replay progress published after a bounded steady or burst pump. */
export interface RendererQueryReplayProgress {
	processedEvents: number;
	totalEvents: number;
	diagnostics: QueryWorkerDiagnostics;
}

/** Render-side measurements combined with QueryWorker replay diagnostics. */
export interface RendererQueryReplayRenderDiagnostics {
	buildingFrames: number;
	maximumBuildingFrameIntervalMs: number;
	maximumVisibleProgressLatencyMs: number;
	completeSummaryToProjectionMs: number;
	terminalProjectionEventCount: number;
	terminalAckToFrameMs: number;
	maximumIntervalWithoutFrameMs: number;
	oldestTerminalTargetAgeMs: number;
	acknowledgedDataSequence: number;
	renderedDataSequence: number;
	resyncCount: number;
	workerErrorCount: number;
	fallbackCount: number;
	maximumRenderQueueDepth: number;
}

/** Machine-readable pass/fail criteria for steady and burst replay profiles. */
export interface RendererQueryReplayGate {
	passed: boolean;
	eventCount: boolean;
	pageCount: boolean;
	buildingCadence: boolean;
	terminalProjection: boolean;
	terminalFrame: boolean;
	workerHealth: boolean;
	rawBacklog: boolean;
	terminalDrain: boolean;
}

/**
 * Evaluates the configured steady replay gate or controlled burst-overload gate.
 * @param profile Active replay profile.
 * @param result Terminal QuerySocketSession result.
 * @param render Render-side frame and convergence diagnostics.
 * @param rawGlobalByteLimit Configured global accepted-backlog ceiling.
 * @param rawStreamByteLimit Configured per-stream accepted-backlog ceiling.
 * @returns Stable machine-readable gate fields.
 */
export function evaluateRendererQueryReplayGate(
	profile: RendererQueryReplayProfile,
	result: RendererQueryReplayResult,
	render: RendererQueryReplayRenderDiagnostics,
	rawGlobalByteLimit: number,
	rawStreamByteLimit: number,
): RendererQueryReplayGate {
	const expectedPages = result.expectedPages ?? Math.ceil(
		(result.targetEvents ?? RENDERER_QUERY_REPLAY_TARGET_EVENTS) / 16_384,
	);
	const ingestion = profile === "ingestion";
	const eventCount =
		profile === "burst"
			? result.status === "backlog" && result.failure?.reason === "backlog"
			: result.status === "complete" &&
				result.streamCompleted &&
				(ingestion || result.queryDoneReceived) &&
				(result.sourceSummaries ?? (result.latestSummary ? [result.latestSummary] : [])).every((event) => event.summary.status === "complete") &&
				(result.sourceSummaries ?? (result.latestSummary ? [result.latestSummary] : [])).reduce((sum, event) => sum + event.summary.eventCount, 0) === result.processedEvents &&
				(ingestion || render.terminalProjectionEventCount === result.processedEvents);
	const pageCount =
		profile === "burst"
			? true
			: result.processedEvents === (result.targetEvents ?? RENDERER_QUERY_REPLAY_TARGET_EVENTS) &&
				(result.sourceSummaries ? result.diagnostics.pages.created >= expectedPages && result.diagnostics.pages.created <= Math.ceil(result.processedEvents / 10000) + result.sourceSummaries.length : result.diagnostics.pages.created === expectedPages);
	const buildingCadence =
		profile !== "steady"
			? true
			: render.buildingFrames > 0 &&
				render.maximumVisibleProgressLatencyMs <= 2_000;
	const terminalProjection =
		profile !== "steady" ||
		(render.completeSummaryToProjectionMs >= 0 &&
			render.completeSummaryToProjectionMs <= 5_000);
	const terminalFrame =
		profile !== "steady" ||
		(render.terminalAckToFrameMs >= 0 && render.terminalAckToFrameMs <= 250);
	const workerHealth =
		profile !== "steady" ||
		(render.resyncCount === 0 &&
			render.workerErrorCount === 0 &&
			render.fallbackCount === 0 &&
			render.maximumRenderQueueDepth <= 2);
	const rawBacklog =
		ingestion ||
		(result.diagnostics.rawBacklog.maximumBytes <= rawGlobalByteLimit &&
			result.diagnostics.rawBacklog.maximumStreamBytes <= rawStreamByteLimit);
	const terminalDrain =
		profile === "burst" ||
		(result.diagnostics.activeStreams === 0 &&
			result.diagnostics.rawBacklog.currentBytes === 0 &&
			result.diagnostics.normalizedBacklog.currentDocuments === 0 &&
			result.diagnostics.journalWrites.active === 0 &&
			result.diagnostics.journalWrites.currentBytes === 0);
	return {
		passed:
			eventCount &&
			pageCount &&
			buildingCadence &&
			terminalProjection &&
			terminalFrame &&
			workerHealth &&
			rawBacklog &&
			terminalDrain,
		eventCount,
		pageCount,
		buildingCadence,
		terminalProjection,
		terminalFrame,
		workerHealth,
		rawBacklog,
		terminalDrain,
	};
}

/** Main-to-worker commands used only by `/renderer-test`. */
export type RendererQueryReplayWorkerCommand =
	| { type: "START"; options: RendererQueryReplayStartOptions }
	| { type: "VERIFY_RESULTS" }
	| {
			type: "REQUEST_PROJECTION";
			projectionRequestId: string;
			descriptor: QueryRenderWindowDescriptor;
	  }
	| { type: "CANCEL" };

/** Dedicated replay-worker events containing summaries, projections, or telemetry. */
export type RendererQueryReplayWorkerEvent =
	| { type: "SOURCE_SUMMARY"; event: QuerySourceSummaryEvent }
	| { type: "PROGRESS"; progress: RendererQueryReplayProgress }
	| {
			type: "PROJECTION_RESULT";
			projectionRequestId: string;
			projection: RenderWindowProjection | null;
			error?: string;
	  }
	| { type: "COMPLETE"; result: RendererQueryReplayResult }
	| { type: "ERROR"; message: string };

/** Application-facing hooks for the renderer replay harness. */
export interface RendererQueryReplayClientCallbacks {
	onSourceSummary: (event: QuerySourceSummaryEvent) => void;
	onProgress: (progress: RendererQueryReplayProgress) => void;
}

interface RendererQueryReplayWorkerLike {
	onmessage:
		| ((event: MessageEvent<RendererQueryReplayWorkerEvent>) => void)
		| null;
	onerror: ((event: ErrorEvent) => void) | null;
	postMessage(command: RendererQueryReplayWorkerCommand): void;
	terminate(): void;
}

interface PendingReplayProjection {
	resolve: (projection: RenderWindowProjection | null) => void;
	reject: (error: Error) => void;
}

/** Reads a bounded replay workload; defaults retain the established 14M profile. */
export function getRendererQueryReplayConfiguration(search: string): {
	targetEvents: number;
	sourceCount: number;
	distribution: "uniform" | "skewed";
	cacheWarnings: boolean;
	commitDelayMs: number;
	ingestionConcurrency: number;
} {
	const params = new URLSearchParams(search);
	const ingestion = params.get("queryReplay") === "ingestion";
	const value = (params.get("docs") ?? (ingestion ? "2000000" : "14000000")).toLowerCase();
	const count = value.endsWith("m")
		? Number(value.slice(0, -1)) * 1_000_000
		: Number(value);
	const sources = Number(params.get("sources") ?? (ingestion ? "200" : "1"));
	const commitDelayMs = Number(params.get("commitDelayMs") ?? (ingestion ? "10" : "0"));
	const ingestionConcurrency = Number(
		params.get("ingestionConcurrency") ?? (ingestion ? "5" : "1"),
	);
	return {
		targetEvents:
			Number.isSafeInteger(count) && count > 0 && count <= 30_000_000
				? count
				: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
		sourceCount:
			Number.isInteger(sources) && sources > 0 && sources <= 1_000
				? sources
				: 1,
		distribution:
			params.get("distribution") === "skewed" ? "skewed" : "uniform",
		cacheWarnings: params.get("cacheWarnings") === "all",
		commitDelayMs:
			Number.isFinite(commitDelayMs) && commitDelayMs >= 0 && commitDelayMs <= 1_000
				? commitDelayMs
				: ingestion
					? 10
					: 0,
		ingestionConcurrency:
			Number.isSafeInteger(ingestionConcurrency) &&
			ingestionConcurrency > 0 &&
			ingestionConcurrency <= 200
				? ingestionConcurrency
				: ingestion
					? 5
					: 1,
	};
}

/** Allocates an exact global count across sources without retaining event objects. */
export function getRendererQueryReplaySources(options: Pick<RendererQueryReplayStartOptions, "sourceId" | "sourceCount" | "targetEvents" | "distribution">): Array<{ sourceId: Source.Id; count: number; offset: number; requestId: string }> {
	const count = options.sourceCount ?? 1;
	const first = options.distribution === "skewed" && count > 1 ? options.targetEvents - Math.floor(options.targetEvents / 10) : 0;
	const remaining = options.targetEvents - first;
	const divisor = first ? count - 1 : count;
	let offset = 0;
	return Array.from({ length: count }, (_, index) => {
		const position = first ? index - 1 : index;
		const events = first && index === 0 ? first : Math.floor(remaining / divisor) + (position < remaining % divisor ? 1 : 0);
		const source = { sourceId: (index === 0 ? options.sourceId : `${options.sourceId}-${index}`) as Source.Id, count: events, offset, requestId: `renderer-query-replay-${index}` };
		offset += events;
		return source;
	});
}

/**
 * Reads an explicit replay profile from the renderer-test query string.
 * @param search URL query string.
 * @returns Steady or burst profile, or null for the existing 10/20/30M benchmark.
 */
export function getRendererQueryReplayProfile(
	search: string,
): RendererQueryReplayProfile | null {
	const value = new URLSearchParams(search).get("queryReplay");
	return value === "steady" || value === "burst" || value === "ingestion"
		? value
		: null;
}

/** Main-thread correlated facade for the dedicated synthetic replay worker. */
export class RendererQueryReplayClient {
	private readonly pendingProjections = new Map<
		string,
		PendingReplayProjection
	>();
	private completionResolve: ((result: RendererQueryReplayResult) => void) | null =
		null;
	private completionReject: ((error: Error) => void) | null = null;
	private projectionCounter = 0;
	private disposed = false;
	private verificationRequested = false;

	/**
	 * Attaches a dedicated worker and compact replay callbacks.
	 * @param worker Worker implementation, injectable for focused tests.
	 * @param callbacks Summary and progress handlers.
	 */
	public constructor(
		private readonly worker: RendererQueryReplayWorkerLike,
		private readonly callbacks: RendererQueryReplayClientCallbacks,
	) {
		this.worker.onmessage = (event) => this.handleEvent(event.data);
		this.worker.onerror = (event) => {
			this.fail(new Error(event.message || "Renderer query replay worker failed."));
		};
	}

	/**
	 * Starts one replay and resolves with its terminal controlled result.
	 * @param options Synthetic source, chunking, and limit configuration.
	 * @returns Terminal replay result.
	 */
	public start(
		options: RendererQueryReplayStartOptions,
	): Promise<RendererQueryReplayResult> {
		if (this.disposed) {
			return Promise.reject(new Error("Renderer query replay is disposed."));
		}
		if (this.completionResolve) {
			return Promise.reject(new Error("Renderer query replay is already active."));
		}
		return new Promise<RendererQueryReplayResult>((resolve, reject) => {
			this.completionResolve = resolve;
			this.completionReject = reject;
			try {
				this.worker.postMessage({ type: "START", options });
			} catch (error) {
				this.fail(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	/**
	 * Requests one real IndexedDB-backed projection from the replay QuerySocketSession.
	 * @param descriptor Current Canvas viewport descriptor.
	 * @returns Transferable projection, or null when superseded.
	 */
	public requestProjection(
		descriptor: QueryRenderWindowDescriptor,
	): Promise<RenderWindowProjection | null> {
		if (this.disposed) {
			return Promise.reject(new Error("Renderer query replay is disposed."));
		}
		this.projectionCounter += 1;
		const projectionRequestId = `renderer_replay_projection_${this.projectionCounter}`;
		return new Promise<RenderWindowProjection | null>((resolve, reject) => {
			this.pendingProjections.set(projectionRequestId, { resolve, reject });
			try {
				this.worker.postMessage({
					type: "REQUEST_PROJECTION",
					projectionRequestId,
					descriptor,
				});
			} catch (error) {
				this.pendingProjections.delete(projectionRequestId);
				reject(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	/** Starts sample and cache verification after the terminal render measurement. */
	public verifyResults(): void {
		if (this.disposed || this.verificationRequested) return;
		this.verificationRequested = true;
		this.worker.postMessage({ type: "VERIFY_RESULTS" });
	}

	/** Permanently cancels the replay and rejects unresolved projections. */
	public dispose(): void {
		if (this.disposed) return;
		this.disposed = true;
		try {
			this.worker.postMessage({ type: "CANCEL" });
		} catch {
			// Termination below is authoritative when the worker no longer accepts input.
		}
		this.worker.terminate();
		this.worker.onmessage = null;
		this.worker.onerror = null;
		this.fail(new Error("Renderer query replay was cancelled."));
	}

	/** Dispatches one typed worker event to callbacks or correlated promises. */
	private handleEvent(event: RendererQueryReplayWorkerEvent): void {
		switch (event.type) {
			case "SOURCE_SUMMARY":
				this.callbacks.onSourceSummary(event.event);
				break;
			case "PROGRESS":
				this.callbacks.onProgress(event.progress);
				break;
			case "PROJECTION_RESULT": {
				const pending = this.pendingProjections.get(event.projectionRequestId);
				if (!pending) return;
				this.pendingProjections.delete(event.projectionRequestId);
				if (event.error) pending.reject(new Error(event.error));
				else pending.resolve(event.projection);
				break;
			}
			case "COMPLETE":
				this.completionResolve?.(event.result);
				this.completionResolve = null;
				this.completionReject = null;
				break;
			case "ERROR":
				this.fail(new Error(event.message));
				break;
		}
	}

	/** Rejects every unresolved operation after a worker or protocol failure. */
	private fail(error: Error): void {
		this.completionReject?.(error);
		this.completionResolve = null;
		this.completionReject = null;
		for (const pending of this.pendingProjections.values()) pending.reject(error);
		this.pendingProjections.clear();
	}
}
