import { Timeline } from "@/app/body/Timeline";
import { CanvasProfiler } from "@/app/body/CanvasProfiler";
import { getCanvasViewportSourceIds } from "@/app/body/CanvasViewport";
import { Application } from "@/context/Application.context";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Glyph } from "@/entities/Glyph";
import { Operation } from "@/entities/Operation";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { Source } from "@/entities/Source";
import type { User } from "@/entities/User";
import { Internal } from "@/entities/addon/Internal";
import {
	EVENT_RENDER_PROJECTION_LIMIT,
	type EventSourceSummary,
} from "@/event-store";
import {
	QUERY_RAW_GLOBAL_BACKLOG_BYTE_LIMIT,
	QUERY_RAW_STREAM_BACKLOG_BYTE_LIMIT,
} from "@/event-store-worker/QueryPersistenceCoordinator";
import type { QuerySourceSummaryEvent } from "@/event-stream";
import { renderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import { DataStore } from "@/store/DataStore";
import { scrollStore } from "@/store/scroll.store";
import { Stack } from "@/ui/Stack";
import {
	useCallback,
	useEffect,
	useRef,
	useState,
} from "react";
import { createOfflineRendererTestApp } from "./RendererTest.fixture";
import {
	evaluateRendererQueryReplayGate,
	getRendererQueryReplayConfiguration,
	getRendererQueryReplaySources,
	RendererQueryReplayClient,
	RENDERER_QUERY_REPLAY_CHUNK_EVENTS,
	RENDERER_QUERY_REPLAY_TARGET_EVENTS,
	type RendererQueryReplayGate,
	type RendererQueryReplayProfile,
	type RendererQueryReplayProgress,
	type RendererQueryReplayRenderDiagnostics,
	type RendererQueryReplayResult,
} from "./RendererTest.query-replay";
import { createRendererQueryReplayWorker } from "./RendererTest.query-replay.factory";
import s from "./styles/RendererTest.module.css";

const REPLAY_CACHE_NAMESPACE_ID = "renderer-query-replay";
const REPLAY_OPERATION_ID =
	"bbbbbbbb-0000-4000-8000-000000000001" as Operation.Id;
const REPLAY_CONTEXT_ID =
	"bbbbbbbb-0000-4000-8000-000000000002" as Context.Id;
const REPLAY_SOURCE_ID =
	"bbbbbbbb-0000-4000-8000-000000000003" as Source.Id;
const REPLAY_USER_ID = "renderer-query-replay-user" as User.Id;
const REPLAY_CONFIGURATION = getRendererQueryReplayConfiguration(typeof window === "undefined" ? "" : window.location.search);
const REPLAY_TARGET = REPLAY_CONFIGURATION.targetEvents;
const REPLAY_TIME_SPAN_MS = 7 * 24 * 60 * 60 * 1_000;
const PROJECTION_POLL_MS = 250;
const BUILDING_FRAME_POLL_MS = 16;
const TERMINAL_POLL_MS = 10;
const TERMINAL_GATE_TIMEOUT_MS = 6_000;

interface RendererQueryReplayPublishedDiagnostics {
	performance?: ReturnType<typeof CanvasProfiler.getWorkerSnapshot>;
	baselinePerformance?: ReturnType<typeof CanvasProfiler.getWorkerSnapshot>;
	profile: RendererQueryReplayProfile;
	status: "running" | "complete" | "failed";
	processedEvents: number;
	result: RendererQueryReplayResult | null;
	render: RendererQueryReplayRenderDiagnostics;
	gate: RendererQueryReplayGate | null;
}

declare global {
	interface Window {
		/** Latest synthetic QuerySocketSession replay diagnostics for automation. */
		__rendererQueryReplayDiagnostics?: RendererQueryReplayPublishedDiagnostics;
	}
}

/** Creates the configured multi-source operation fixture used by the replay profile. */
function createReplayFixture(
	documentMinTimestamp: number,
	frameMaxTimestamp: number,
): { operation: Operation.Type; context: Context.Type; source: Source.Type } {
	const operation: Operation.Type = {
		id: REPLAY_OPERATION_ID,
		index: "renderer_query_replay",
		name: "Renderer Query Replay",
		glyph_id: null as unknown as Glyph.Id,
		selected: true,
	};
	const context: Context.Type = {
		id: REPLAY_CONTEXT_ID,
		operation_id: REPLAY_OPERATION_ID,
		name: "renderer-query-replay",
		color: "#4f86c6",
		type: "context",
		owner_user_id: REPLAY_USER_ID,
		granted_user_ids: [],
		granted_user_group_ids: [],
		time_created: Date.now(),
		time_updated: Date.now(),
		glyph_id: null as unknown as Glyph.Id,
		selected: true,
	};
	const source: Source.Type = {
		id: REPLAY_SOURCE_ID,
		operation_id: REPLAY_OPERATION_ID,
		context_id: REPLAY_CONTEXT_ID,
		name: "query_raw_replay.json",
		plugin: "renderer-query-replay",
		type: "source",
		owner_user_id: REPLAY_USER_ID,
		granted_user_ids: [],
		granted_user_group_ids: [],
		time_created: Date.now(),
		time_updated: Date.now(),
		glyph_id: null as unknown as Glyph.Id,
		selected: true,
		pinned: false,
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
		settings: { ...Internal.Settings.default },
		total: REPLAY_TARGET,
		timestamp: {
			min: documentMinTimestamp,
			max: frameMaxTimestamp,
		},
		nanotimestamp: {
			min: Internal.Transformator.toNanos(documentMinTimestamp),
			max: Internal.Transformator.toNanos(frameMaxTimestamp),
		},
		_sampleDataCached: {
			frequency_sample: Internal.Settings.default.frequency_sample,
			min_timestamp: documentMinTimestamp,
			max_timestampe: frameMaxTimestamp,
			sample_data: null,
		},
	} as Source.Type;
	return { operation, context, source };
}

/** Returns zeroed render diagnostics before the first projected frame. */
function createEmptyRenderDiagnostics(): RendererQueryReplayRenderDiagnostics {
	return {
		buildingFrames: 0,
		maximumBuildingFrameIntervalMs: 0,
		maximumVisibleProgressLatencyMs: 0,
		completeSummaryToProjectionMs: -1,
		terminalProjectionEventCount: 0,
		terminalAckToFrameMs: -1,
		maximumIntervalWithoutFrameMs: 0,
		oldestTerminalTargetAgeMs: 0,
		acknowledgedDataSequence: 0,
		renderedDataSequence: 0,
		resyncCount: 0,
		workerErrorCount: 0,
		fallbackCount: 0,
		maximumRenderQueueDepth: 0,
	};
}

/** Publishes machine-readable replay state to `window`, DOM data, and React. */
function publishReplayDiagnostics(
	diagnostics: RendererQueryReplayPublishedDiagnostics,
): void {
	window.__rendererQueryReplayDiagnostics = diagnostics;
	document.documentElement.dataset.rendererQueryReplayStatus = diagnostics.status;
	document.documentElement.dataset.rendererQueryReplayProfile = diagnostics.profile;
	document.documentElement.dataset.rendererQueryReplayResult = diagnostics.gate
		? diagnostics.gate.passed
			? "passed"
			: "failed"
		: "pending";
	const journalWrites = diagnostics.result?.diagnostics.journalWrites;
	document.documentElement.dataset.rendererQueryReplayJournalActive = String(
		journalWrites?.active ?? 0,
	);
	document.documentElement.dataset.rendererQueryReplayJournalMaximumActive = String(
		journalWrites?.maximumActive ?? 0,
	);
	document.documentElement.dataset.rendererQueryReplayJournalMaximumBytes = String(
		journalWrites?.maximumBytes ?? 0,
	);
	if (!diagnostics.gate) return;
	requestAnimationFrame(() => {
		document.dispatchEvent(
			new CustomEvent<RendererQueryReplayPublishedDiagnostics>(
				"renderer-query-replay-complete",
				{ detail: diagnostics },
			),
		);
	});
}

/**
 * Runs the dedicated-worker `docs_chunk` replay selected by `/renderer-test`.
 * @param profile Configurable steady path or controlled burst overload.
 * @returns Timeline plus compact machine-readable diagnostics overlay.
 */
export function RendererQueryReplayPage({
	profile,
}: {
	profile: RendererQueryReplayProfile;
}) {
	const { app, Info, setInfo } = Application.use();
	const latestAppRef = useRef<App.Type>(app);
	latestAppRef.current = app;
	const clientRef = useRef<RendererQueryReplayClient | null>(null);
	const sourceRef = useRef<Source.Type | null>(null);
	const latestSummaryRef = useRef<QuerySourceSummaryEvent | null>(null);
	const summariesRef = useRef(new Map<string, QuerySourceSummaryEvent>());
	const pendingVisibleProgressRef = useRef(new Map<Source.Id, { revision: number; receivedAt: number }>());
	const replaySourcesRef = useRef<Source.Type[]>([]);
	const completeSummaryAtRef = useRef<number | null>(null);
	const firstBuildingSummaryAtRef = useRef<number | null>(null);
	const lastBuildingFrameAtRef = useRef<number | null>(null);
	const lastBuildingFrameRevisionRef = useRef<number>(0);
	const lastBuildingFrameSequenceRef = useRef<number>(0);
	const projectionInFlightRef = useRef<Promise<void> | null>(null);
	const projectionTrailingRef = useRef<boolean>(false);
	const projectionRevisionRef = useRef<number>(0);
	const ackObservedAtRef = useRef<number | null>(null);
	const terminalAckSequenceRef = useRef<number>(0);
	const resultRef = useRef<RendererQueryReplayResult | null>(null);
	const processedEventsRef = useRef<number>(0);
	const baselinePerformanceRef = useRef<ReturnType<typeof CanvasProfiler.getWorkerSnapshot> | undefined>(undefined);
	const lastTelemetryAtRef = useRef(0);
	const renderDiagnosticsRef = useRef<RendererQueryReplayRenderDiagnostics>(
		createEmptyRenderDiagnostics(),
	);
	const requestProjectionRef = useRef<() => void>(() => undefined);
	const [published, setPublished] =
		useState<RendererQueryReplayPublishedDiagnostics>(() => ({
			profile,
			status: "running",
			processedEvents: 0,
			result: null,
			render: createEmptyRenderDiagnostics(),
			gate: null,
		}));

	/** Refreshes render-side counters from Canvas without starting observations. */
	const refreshRenderDiagnostics = useCallback(() => {
		const worker = CanvasProfiler.getWorkerSnapshot();
		const synchronization = CanvasProfiler.getWorkerSynchronizationSnapshot();
		const current = renderDiagnosticsRef.current;
		renderDiagnosticsRef.current = {
			...current,
			maximumIntervalWithoutFrameMs: worker.workerMaxFrameIntervalMs,
			oldestTerminalTargetAgeMs:
				synchronization?.oldestTerminalTargetAgeMs ?? 0,
			acknowledgedDataSequence:
				synchronization?.acknowledgedDataSequence ?? 0,
			renderedDataSequence: synchronization?.renderedDataSequence ?? 0,
			resyncCount: worker.resyncCount,
			workerErrorCount: worker.workerErrorCount,
			fallbackCount: worker.fallbackCount,
			maximumRenderQueueDepth: worker.maxQueueDepth,
		};
		return renderDiagnosticsRef.current;
	}, []);

	/** Publishes current live or terminal diagnostics to automation and the overlay. */
	const publishCurrent = useCallback(
		(result: RendererQueryReplayResult | null, terminal: boolean): void => {
			const render = { ...refreshRenderDiagnostics() };
			const gate =
				terminal && result
					? evaluateRendererQueryReplayGate(
							profile,
							result,
							render,
							QUERY_RAW_GLOBAL_BACKLOG_BYTE_LIMIT,
							QUERY_RAW_STREAM_BACKLOG_BYTE_LIMIT,
						)
					: null;
			const diagnostics: RendererQueryReplayPublishedDiagnostics = {
				profile,
				performance: CanvasProfiler.getWorkerSnapshot(),
				baselinePerformance: baselinePerformanceRef.current,
				status: terminal ? (gate?.passed ? "complete" : "failed") : "running",
				processedEvents: result?.processedEvents ?? processedEventsRef.current,
				result,
				render,
				gate,
			};
			setPublished(diagnostics);
			publishReplayDiagnostics(diagnostics);
		},
		[profile, refreshRenderDiagnostics],
	);

	/** Requests one latest viewport projection with one active and one trailing slot. */
	const requestProjection = useCallback((): void => {
		if (renderDiagnosticsRef.current.terminalProjectionEventCount === REPLAY_TARGET) return;
		const client = clientRef.current;
		const source = sourceRef.current;
		const summaryAtRequest = latestSummaryRef.current;
		const synchronization = CanvasProfiler.getWorkerSynchronizationSnapshot();
		if (!client || !source || !summaryAtRequest || !synchronization?.ready) return;
		if (projectionInFlightRef.current) {
			projectionTrailingRef.current = true;
			return;
		}
		const { frame, width, scale, scrollX } = synchronization.viewport;
		if (frame.max <= frame.min) return;
		const visible = new Set(getCanvasViewportSourceIds(
			Source.Entity.rendered(latestAppRef.current),
			scrollStore.getY(),
			document.getElementById("canvas")?.clientHeight ?? 900,
		));
		// Keep active builders observable, but do not repeatedly scan all completed
		// offscreen sources. The terminal pass still validates every source together.
		const allComplete = summariesRef.current.size === replaySourcesRef.current.length &&
			[...summariesRef.current.values()].every((event) => event.summary.status === "complete");
		const projectedSources = replaySourcesRef.current.filter((candidate) => {
			const summary = summariesRef.current.get(candidate.id)?.summary;
			return summary && synchronization.sources.some((known) => known.sourceId === candidate.id) &&
				(allComplete || visible.has(candidate.id) || summary.status === "building");
		});
		const projectedIds = new Set(projectedSources.map((candidate) => candidate.id));
		const descriptor: Parameters<RendererQueryReplayClient["requestProjection"]>[0] = {
				operationId: REPLAY_OPERATION_ID,
				operationEpoch: DataStore.getRenderOperationEpoch(),
				viewportRevision: synchronization.requestedViewportRevision,
				projectionRevision: ++projectionRevisionRef.current,
				frame,
				width: Math.max(1, width),
				scale: Math.max(0.01, scale),
				scrollX,
				horizontalOverscanPixels: Math.max(1, width),
				maxRepresentatives: EVENT_RENDER_PROJECTION_LIMIT,
				evictedSourceIds: replaySourcesRef.current.filter((candidate) => !projectedIds.has(candidate.id)).map((candidate) => candidate.id),
				sources: projectedSources.map((candidate) => ({
					sourceId: candidate.id, offset: candidate.settings.offset, engine: candidate.settings.render_engine, frequency: candidate.settings.frequency_sample,
					overrideHashes: [], settingsRevision: 0, overrideRevision: 0,
				})),
			};
		const request = client.requestProjection(descriptor)
			.then((projection) => {
				if (!projection || !renderWindowProjectionBus.publish(projection, descriptor)) return;
				const now = performance.now();
				const complete = projection.sources.length === replaySourcesRef.current.length && projection.sources.every((candidate) => {
					const summary = summariesRef.current.get(candidate.sourceId)?.summary;
					return candidate.sourceStatus === "complete" && summary?.generation === candidate.sourceGeneration && summary.revision === candidate.sourceRevision;
				});
				if (complete && renderDiagnosticsRef.current.terminalProjectionEventCount !== REPLAY_TARGET) {
					renderDiagnosticsRef.current.completeSummaryToProjectionMs = Math.max(0, now - (completeSummaryAtRef.current ?? now));
					renderDiagnosticsRef.current.terminalProjectionEventCount = projection.sources.reduce((sum, candidate) => sum + candidate.loadedCount, 0);
				}
			})
			.catch((error: unknown) => {
				console.error("[RendererQueryReplay] projection failed", error);
			})
			.finally(() => {
				if (projectionInFlightRef.current === request) {
					projectionInFlightRef.current = null;
				}
				if (projectionTrailingRef.current) {
					projectionTrailingRef.current = false;
					requestProjectionRef.current();
				}
			});
		projectionInFlightRef.current = request;
	}, []);
	requestProjectionRef.current = requestProjection;

	useEffect(() => {
		CanvasProfiler.resetWorkerMetrics();
		const releaseCapture = CanvasProfiler.beginCapture();
		const frameMaxTimestamp = Date.now();
		const documentMinTimestamp = frameMaxTimestamp - REPLAY_TIME_SPAN_MS;
		const fixture = createReplayFixture(
			documentMinTimestamp,
			frameMaxTimestamp,
		);
		sourceRef.current = fixture.source;
		const allocations = getRendererQueryReplaySources({ ...REPLAY_CONFIGURATION, sourceId: REPLAY_SOURCE_ID });
		const contexts = Array.from({ length: Math.min(3, allocations.length) }, (_, index) => ({ ...fixture.context, id: `${REPLAY_CONTEXT_ID}-${index}` as Context.Id, name: `Replay context ${index + 1}` }));
		replaySourcesRef.current = allocations.map((entry, index) => ({
			...fixture.source,
			id: entry.sourceId,
			total: entry.count,
			context_id: contexts[index % contexts.length].id,
			name: `query_source_${index + 1}.json`,
			appliedQuery: REPLAY_CONFIGURATION.cacheWarnings
				? {
						string: "renderer cache-warning replay",
						filters: [],
						source_config: {
							operation_id: REPLAY_OPERATION_ID,
							source_ids: [entry.sourceId],
							range: {
								min: documentMinTimestamp,
								max: frameMaxTimestamp,
							},
						},
					}
				: null,
			queryCache: REPLAY_CONFIGURATION.cacheWarnings
				? {
						...createEmptyQueryCacheState(),
						activeGeneration: 1,
						freshness: "source-changed",
						warningReason: "source-changed",
					}
				: createEmptyQueryCacheState(),
		}));
		DataStore.resetRenderOperation(REPLAY_OPERATION_ID);
		DataStore.openEventStoreNamespace({
			cacheNamespaceId: REPLAY_CACHE_NAMESPACE_ID,
			operationId: REPLAY_OPERATION_ID,
		});
		setInfo(((previous: App.Type) => ({
			...createOfflineRendererTestApp(previous),
			hidden: { ...previous.hidden, sourcesWithNoEvents: false },
			target: {
				...previous.target,
				operations: [fixture.operation],
				contexts,
				files: replaySourcesRef.current,
				notes: [],
				links: [],
				highlights: [],
			},
			timeline: {
				...previous.timeline,
				target: null,
				frame: {
					min: documentMinTimestamp,
					max: frameMaxTimestamp,
				},
			},
		})) as unknown as App.Type);

		const client = new RendererQueryReplayClient(
			createRendererQueryReplayWorker(),
			{
				onSourceSummary: (event) => {
					latestSummaryRef.current = event;
					summariesRef.current.set(event.summary.sourceId, event);
					DataStore.publishEventSourceSummary(
						event.summary as EventSourceSummary,
						"deferred",
					);
					if (event.summary.status === "building") {
						firstBuildingSummaryAtRef.current ??= performance.now();
						const visible = getCanvasViewportSourceIds(
							Source.Entity.rendered(latestAppRef.current), scrollStore.getY(),
							document.getElementById("canvas")?.clientHeight ?? 900,
						);
						if (event.summary.eventCount > 0 && visible.includes(event.summary.sourceId) &&
							!pendingVisibleProgressRef.current.has(event.summary.sourceId)) {
							pendingVisibleProgressRef.current.set(event.summary.sourceId, {
								revision: event.summary.revision, receivedAt: performance.now(),
							});
						}
					} else if (event.summary.status === "complete" && summariesRef.current.size === replaySourcesRef.current.length && [...summariesRef.current.values()].every((entry) => entry.summary.status === "complete")) {
						const completedAt = performance.now();
						completeSummaryAtRef.current = completedAt;
						if (lastBuildingFrameAtRef.current !== null) {
							renderDiagnosticsRef.current
								.maximumBuildingFrameIntervalMs = Math.max(
								renderDiagnosticsRef.current
									.maximumBuildingFrameIntervalMs,
								completedAt - lastBuildingFrameAtRef.current,
							);
						}
					}
					requestProjectionRef.current();
				},
				onProgress: (progress: RendererQueryReplayProgress) => {
					processedEventsRef.current = progress.processedEvents;
					if (performance.now() - lastTelemetryAtRef.current >= 1000) {
						lastTelemetryAtRef.current = performance.now();
						publishCurrent(null, false);
					}
					setPublished((previous) => {
						const next = {
							...previous,
							processedEvents: progress.processedEvents,
						};
						publishReplayDiagnostics(next);
						return next;
					});
					requestProjectionRef.current();
				},
			},
		);
		clientRef.current = client;
		const startFrame = window.setTimeout(() => {
			baselinePerformanceRef.current = CanvasProfiler.getWorkerSnapshot();
			CanvasProfiler.resetWorkerMetrics();
			publishCurrent(null, false);
			void client
				.start({
					profile,
					cacheNamespaceId: REPLAY_CACHE_NAMESPACE_ID,
					operationId: REPLAY_OPERATION_ID,
					operationEpoch: DataStore.getRenderOperationEpoch(),
					sourceId: REPLAY_SOURCE_ID,
					...REPLAY_CONFIGURATION,
					chunkEvents: RENDERER_QUERY_REPLAY_CHUNK_EVENTS,
					documentMinTimestamp,
					frameSpanMs: REPLAY_TIME_SPAN_MS,
				})
				.then((result) => {
					resultRef.current = result;
					if (profile !== "steady") publishCurrent(result, true);
					else requestProjectionRef.current();
				})
				.catch((error: unknown) => {
					console.error("[RendererQueryReplay] replay failed", error);
					setPublished((previous) => ({ ...previous, status: "failed" }));
				});
		}, Math.max(0, Number(new URLSearchParams(window.location.search).get("delay") ?? 0)));
		return () => {
			window.clearTimeout(startFrame);
			client.dispose();
			clientRef.current = null;
			DataStore.closeEventStoreNamespace();
			releaseCapture();
		};
	}, [profile, publishCurrent, setInfo]);

	useEffect(() => {
		const projectionPoll = window.setInterval(() => {
			const status = latestSummaryRef.current?.summary.status;
			if (
				status === "building" ||
				(status === "complete" &&
					renderDiagnosticsRef.current.terminalProjectionEventCount !==
						REPLAY_TARGET)
			) {
				requestProjectionRef.current();
			}
		}, PROJECTION_POLL_MS);
		return () => window.clearInterval(projectionPoll);
	}, []);

	useEffect(() => {
		if (profile !== "steady") return;
		const buildingFramePoll = window.setInterval(() => {
			const synchronization =
				CanvasProfiler.getWorkerSynchronizationSnapshot();
			if (!synchronization) return;
			const visible = new Set(getCanvasViewportSourceIds(
				Source.Entity.rendered(latestAppRef.current), scrollStore.getY(),
				document.getElementById("canvas")?.clientHeight ?? 900,
			));
			// Measure committed visible data waiting for its frame. Offscreen sources
			// and time spent waiting for the next persisted page are not render stalls.
			for (const [sourceId, pending] of pendingVisibleProgressRef.current) {
				if (!visible.has(sourceId)) {
					pendingVisibleProgressRef.current.delete(sourceId);
					continue;
				}
				const acknowledged = synchronization.sources.find((source) => source.sourceId === sourceId);
				const rendered = acknowledged && acknowledged.acknowledgedRevision >= pending.revision &&
					synchronization.renderedDataSequence >= acknowledged.acknowledgedDataSequence;
				const observedAt = rendered ? synchronization.renderedDataAtMs : performance.now();
				renderDiagnosticsRef.current.maximumVisibleProgressLatencyMs = Math.max(
					renderDiagnosticsRef.current.maximumVisibleProgressLatencyMs,
					observedAt - pending.receivedAt,
				);
				if (rendered) pendingVisibleProgressRef.current.delete(sourceId);
			}
			if (resultRef.current || ![...summariesRef.current.values()].some((event) => event.summary.status === "building")) return;
			if (!synchronization || synchronization.renderedDataSequence <= lastBuildingFrameSequenceRef.current || synchronization.renderedDataAtMs <= 0) return;
			const renderedAt = synchronization!.renderedDataAtMs;
			const previous =
				lastBuildingFrameAtRef.current ??
				firstBuildingSummaryAtRef.current ??
				renderedAt;
			renderDiagnosticsRef.current.buildingFrames += 1;
			renderDiagnosticsRef.current.maximumBuildingFrameIntervalMs = Math.max(
				renderDiagnosticsRef.current.maximumBuildingFrameIntervalMs,
				renderedAt - previous,
			);
			lastBuildingFrameAtRef.current = renderedAt;
			lastBuildingFrameRevisionRef.current = synchronization.renderedDataSequence;
			lastBuildingFrameSequenceRef.current = synchronization.renderedDataSequence;
		}, BUILDING_FRAME_POLL_MS);
		return () => window.clearInterval(buildingFramePoll);
	}, [profile]);

	useEffect(() => {
		if (profile !== "steady") return;
		const terminalPoll = window.setInterval(() => {
			const result = resultRef.current;
			const terminalSummaries = [...summariesRef.current.values()];
			if (terminalSummaries.length !== replaySourcesRef.current.length ||
				!terminalSummaries.every((event) => event.summary.status === "complete")) return;
			const synchronization = CanvasProfiler.getWorkerSynchronizationSnapshot();
			const now = performance.now();
			const acknowledged = terminalSummaries.map((event) => synchronization?.sources.find((candidate) => candidate.sourceId === event.summary.sourceId));
			if (ackObservedAtRef.current === null && renderDiagnosticsRef.current.terminalProjectionEventCount === REPLAY_TARGET && acknowledged.every((source, index) => source && source.acknowledgedRevision >= terminalSummaries[index].summary.revision)) {
				ackObservedAtRef.current = Math.max(...acknowledged.map((source) => source!.acknowledgedAtMs));
				terminalAckSequenceRef.current = Math.max(...acknowledged.map((source) => source!.acknowledgedDataSequence));
			}
			if (
				ackObservedAtRef.current !== null &&
				(synchronization?.renderedDataSequence ?? 0) >=
					terminalAckSequenceRef.current
			) {
				// Capture the first converged frame before test-only sample/cache reads;
				// later input frames must not overwrite its latency.
				if (renderDiagnosticsRef.current.terminalAckToFrameMs < 0) {
					renderDiagnosticsRef.current.terminalAckToFrameMs = Math.max(
						0, (synchronization?.renderedDataAtMs ?? now) - ackObservedAtRef.current,
					);
				}
				clientRef.current?.verifyResults();
				if (result) {
					window.clearInterval(terminalPoll);
					publishCurrent(result, true);
				}
				return;
			}
			if (
				completeSummaryAtRef.current !== null &&
				now - completeSummaryAtRef.current >= TERMINAL_GATE_TIMEOUT_MS
			) {
				clientRef.current?.verifyResults();
				if (result) {
					window.clearInterval(terminalPoll);
					publishCurrent(result, true);
				}
			}
		}, TERMINAL_POLL_MS);
		return () => window.clearInterval(terminalPoll);
	}, [profile, publishCurrent]);

	return (
		<Stack gap={12} className={s.page} ai="stretch">
			<Timeline disableSessionAutosave />
			<div className={s.overlay}>
				<div className={s.badge}>QuerySocketSession replay</div>
				<div className={s.row}>
					<span className={s.label}>profile</span>
					<span className={s.value}>{profile}</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>events</span>
					<span className={s.value}>
						{published.processedEvents.toLocaleString()} / {REPLAY_TARGET.toLocaleString()}
					</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>pages</span>
					<span className={s.value}>
						{published.result?.diagnostics.pages.created ?? 0}
					</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>raw high-water</span>
					<span className={s.value}>
						{published.result?.diagnostics.rawBacklog.maximumBytes ?? 0}
					</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>journal writes</span>
					<span className={s.value}>
						{published.result?.diagnostics.journalWrites.maximumActive ?? 0} / {published.result?.diagnostics.journalWrites.completed ?? 0}
					</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>status</span>
					<span className={s.value}>
						{published.gate
							? published.gate.passed
								? "passed"
								: "failed"
							: published.status}
					</span>
				</div>
			</div>
		</Stack>
	);
}
