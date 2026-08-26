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

export type RendererQueryReplayProfile = "steady" | "burst";
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
	coordinatorId: string;
	operationId: string;
	operationEpoch: number;
	sourceId: Source.Id;
	targetEvents: number;
	chunkEvents: number;
	documentMinTimestamp: number;
	frameSpanMs: number;
	rawGlobalByteLimit?: number;
	rawStreamByteLimit?: number;
}

/** Terminal dedicated-worker result with no exact document payloads. */
export interface RendererQueryReplayResult {
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
}

/**
 * Evaluates the deterministic 14M steady gate or controlled burst-overload gate.
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
	const expectedPages = Math.ceil(
		RENDERER_QUERY_REPLAY_TARGET_EVENTS / 16_384,
	);
	const eventCount =
		profile === "burst"
			? result.status === "backlog" && result.failure?.reason === "backlog"
			: result.status === "complete" &&
				result.streamCompleted &&
				result.queryDoneReceived &&
				result.latestSummary?.summary.status === "complete" &&
				result.latestSummary.summary.eventCount === result.processedEvents &&
				render.terminalProjectionEventCount === result.processedEvents;
	const pageCount =
		profile === "burst"
			? true
			: result.processedEvents === RENDERER_QUERY_REPLAY_TARGET_EVENTS &&
				result.diagnostics.pages.created === expectedPages;
	const buildingCadence =
		profile === "burst"
			? true
			: render.buildingFrames > 0 &&
				render.maximumBuildingFrameIntervalMs <= 2_000;
	const terminalProjection =
		profile === "burst" ||
		(render.completeSummaryToProjectionMs >= 0 &&
			render.completeSummaryToProjectionMs <= 5_000);
	const terminalFrame =
		profile === "burst" ||
		(render.terminalAckToFrameMs >= 0 && render.terminalAckToFrameMs <= 250);
	const workerHealth =
		profile === "burst" ||
		(render.resyncCount === 0 &&
			render.workerErrorCount === 0 &&
			render.fallbackCount === 0 &&
			render.maximumRenderQueueDepth <= 2);
	const rawBacklog =
		result.diagnostics.rawBacklog.maximumBytes <= rawGlobalByteLimit &&
		result.diagnostics.rawBacklog.maximumStreamBytes <= rawStreamByteLimit;
	return {
		passed:
			eventCount &&
			pageCount &&
			buildingCadence &&
			terminalProjection &&
			terminalFrame &&
			workerHealth &&
			rawBacklog,
		eventCount,
		pageCount,
		buildingCadence,
		terminalProjection,
		terminalFrame,
		workerHealth,
		rawBacklog,
	};
}

/** Main-to-worker commands used only by `/renderer-test`. */
export type RendererQueryReplayWorkerCommand =
	| { type: "START"; options: RendererQueryReplayStartOptions }
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

/**
 * Reads an explicit replay profile from the renderer-test query string.
 * @param search URL query string.
 * @returns Steady or burst profile, or null for the existing 10/20/30M benchmark.
 */
export function getRendererQueryReplayProfile(
	search: string,
): RendererQueryReplayProfile | null {
	const value = new URLSearchParams(search).get("queryReplay");
	return value === "steady" || value === "burst" ? value : null;
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
