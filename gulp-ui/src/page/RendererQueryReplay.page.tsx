import { Timeline } from "@/app/body/Timeline";
import { CanvasProfiler } from "@/app/body/CanvasProfiler";
import { Application } from "@/context/Application.context";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Glyph } from "@/entities/Glyph";
import { Operation } from "@/entities/Operation";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
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
const REPLAY_TIME_SPAN_MS = 7 * 24 * 60 * 60 * 1_000;
const PROJECTION_POLL_MS = 250;
const BUILDING_FRAME_POLL_MS = 16;
const TERMINAL_POLL_MS = 10;
const TERMINAL_GATE_TIMEOUT_MS = 6_000;

interface RendererQueryReplayPublishedDiagnostics {
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

/** Creates the single 14M-source operation fixture used by the replay profile. */
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
		name: "query_raw_14m.json",
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
		total: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
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
 * @param profile Steady 14M path or controlled burst overload.
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
		const client = clientRef.current;
		const source = sourceRef.current;
		const summaryAtRequest = latestSummaryRef.current;
		const synchronization = CanvasProfiler.getWorkerSynchronizationSnapshot();
		if (!client || !source || !summaryAtRequest || !synchronization?.ready) return;
		if (projectionInFlightRef.current) {
			projectionTrailingRef.current = true;
			return;
		}
		const frame = latestAppRef.current.timeline.frame;
		if (frame.max <= frame.min) return;
		const request = client
			.requestProjection({
				operationId: REPLAY_OPERATION_ID,
				operationEpoch: DataStore.getRenderOperationEpoch(),
				viewportRevision: synchronization.requestedViewportRevision,
				projectionRevision: ++projectionRevisionRef.current,
				frame,
				width: Math.max(1, Info.width),
				scale: Math.max(0.01, latestAppRef.current.timeline.scale),
				scrollX: scrollStore.getX(),
				horizontalOverscanPixels: Math.max(1, Info.width),
				maxRepresentatives: EVENT_RENDER_PROJECTION_LIMIT,
				sources: [
					{
						sourceId: source.id,
						offset: source.settings.offset,
						engine: source.settings.render_engine,
						frequency: source.settings.frequency_sample,
						overrideHashes: [],
						settingsRevision: 0,
						overrideRevision: 0,
					},
				],
			})
			.then((projection) => {
				if (!projection || !renderWindowProjectionBus.publish(projection)) return;
				const projectedSource = projection.sources.find(
					(candidate) => candidate.sourceId === source.id,
				);
				if (!projectedSource) return;
				const now = performance.now();
				if (
					projectedSource.sourceStatus === "complete" &&
					projectedSource.sourceGeneration ===
						summaryAtRequest.summary.generation &&
					projectedSource.sourceRevision ===
						summaryAtRequest.summary.revision
				) {
					renderDiagnosticsRef.current.completeSummaryToProjectionMs =
						Math.max(0, now - (completeSummaryAtRef.current ?? now));
					renderDiagnosticsRef.current.terminalProjectionEventCount =
						projectedSource.loadedCount;
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
	}, [Info.width]);
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
		DataStore.resetRenderOperation(REPLAY_OPERATION_ID);
		DataStore.openEventStoreNamespace({
			cacheNamespaceId: REPLAY_CACHE_NAMESPACE_ID,
			operationId: REPLAY_OPERATION_ID,
		});
		setInfo(((previous: App.Type) => ({
			...createOfflineRendererTestApp(previous),
			target: {
				...previous.target,
				operations: [fixture.operation],
				contexts: [fixture.context],
				files: [fixture.source],
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
					DataStore.publishEventSourceSummary(
						event.summary as EventSourceSummary,
						"deferred",
					);
					if (event.summary.status === "building") {
						firstBuildingSummaryAtRef.current ??= performance.now();
					} else if (event.summary.status === "complete") {
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
		const startFrame = requestAnimationFrame(() => {
			void client
				.start({
					profile,
					cacheNamespaceId: REPLAY_CACHE_NAMESPACE_ID,
					operationId: REPLAY_OPERATION_ID,
					operationEpoch: DataStore.getRenderOperationEpoch(),
					sourceId: REPLAY_SOURCE_ID,
					targetEvents: RENDERER_QUERY_REPLAY_TARGET_EVENTS,
					chunkEvents: RENDERER_QUERY_REPLAY_CHUNK_EVENTS,
					documentMinTimestamp,
					frameSpanMs: REPLAY_TIME_SPAN_MS,
				})
				.then((result) => {
					resultRef.current = result;
					if (profile === "burst") publishCurrent(result, true);
					else requestProjectionRef.current();
				})
				.catch((error: unknown) => {
					console.error("[RendererQueryReplay] replay failed", error);
					setPublished((previous) => ({ ...previous, status: "failed" }));
				});
		});
		return () => {
			cancelAnimationFrame(startFrame);
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
						RENDERER_QUERY_REPLAY_TARGET_EVENTS)
			) {
				requestProjectionRef.current();
			}
		}, PROJECTION_POLL_MS);
		return () => window.clearInterval(projectionPoll);
	}, []);

	useEffect(() => {
		if (profile !== "steady") return;
		const buildingFramePoll = window.setInterval(() => {
			if (latestSummaryRef.current?.summary.status !== "building") return;
			const synchronization =
				CanvasProfiler.getWorkerSynchronizationSnapshot();
			const source = synchronization?.sources.find(
				(candidate) => candidate.sourceId === REPLAY_SOURCE_ID,
			);
			if (
				!source ||
				source.acknowledgedRevision <= lastBuildingFrameRevisionRef.current ||
				source.acknowledgedDataSequence <=
					lastBuildingFrameSequenceRef.current ||
				(synchronization?.renderedDataSequence ?? 0) <
					source.acknowledgedDataSequence ||
				(synchronization?.renderedDataAtMs ?? 0) <= 0
			) {
				return;
			}
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
			lastBuildingFrameRevisionRef.current = source.acknowledgedRevision;
			lastBuildingFrameSequenceRef.current = source.acknowledgedDataSequence;
		}, BUILDING_FRAME_POLL_MS);
		return () => window.clearInterval(buildingFramePoll);
	}, [profile]);

	useEffect(() => {
		if (profile !== "steady") return;
		const terminalPoll = window.setInterval(() => {
			const result = resultRef.current;
			const summary = result?.latestSummary;
			if (!result || summary?.summary.status !== "complete") return;
			const synchronization = CanvasProfiler.getWorkerSynchronizationSnapshot();
			const source = synchronization?.sources.find(
				(candidate) => candidate.sourceId === REPLAY_SOURCE_ID,
			);
			const now = performance.now();
			if (
				ackObservedAtRef.current === null &&
				source &&
				source.acknowledgedRevision >= summary.summary.revision
			) {
				ackObservedAtRef.current = source.acknowledgedAtMs;
				terminalAckSequenceRef.current = source.acknowledgedDataSequence;
			}
			if (
				ackObservedAtRef.current !== null &&
				(synchronization?.renderedDataSequence ?? 0) >=
					terminalAckSequenceRef.current
			) {
				renderDiagnosticsRef.current.terminalAckToFrameMs = Math.max(
					0,
					(synchronization?.renderedDataAtMs ?? now) -
						ackObservedAtRef.current,
				);
				window.clearInterval(terminalPoll);
				publishCurrent(result, true);
				return;
			}
			if (
				completeSummaryAtRef.current !== null &&
				now - completeSummaryAtRef.current >= TERMINAL_GATE_TIMEOUT_MS
			) {
				window.clearInterval(terminalPoll);
				publishCurrent(result, true);
			}
		}, TERMINAL_POLL_MS);
		return () => window.clearInterval(terminalPoll);
	}, [profile, publishCurrent]);

	return (
		<Stack gap={12} className={s.page} ai="stretch">
			<Timeline disableSessionAutosave />
			<div className={s.overlay}>
				<div className={s.badge}>QuerySocketSession 14M replay</div>
				<div className={s.row}>
					<span className={s.label}>profile</span>
					<span className={s.value}>{profile}</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>events</span>
					<span className={s.value}>
						{published.processedEvents.toLocaleString()} / 14,000,000
					</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>pages</span>
					<span className={s.value}>
						{published.result?.diagnostics.pages.created ?? 0} / 855
					</span>
				</div>
				<div className={s.row}>
					<span className={s.label}>raw high-water</span>
					<span className={s.value}>
						{published.result?.diagnostics.rawBacklog.maximumBytes ?? 0}
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
