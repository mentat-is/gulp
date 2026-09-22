/**
 * RendererTest.page.tsx
 *
 * Renderer stress-test mode — triggered by navigating to /renderer-test.
 *
 * Bypasses authentication, creates a fully mock operation / context / source and
 * writes synthetic DOCUMENTS_CHUNK-equivalent events to bounded IndexedDB pages.
 * Canvas receives only viewport LOD projections, so the Timeline pipeline can be
 * exercised without a running backend server or source-wide RAM arrays.
 *
 * Navigate to http://localhost:3000/renderer-test to activate.
 */

import {
    memo,
    useEffect,
    useRef,
    useState,
    useCallback,
} from 'react';
import { Application } from '@/context/Application.context';
import { Timeline } from '@/app/body/Timeline';
import { Stack } from '@/ui/Stack';
import { Algorhithm } from '@/ui/utils';
import { App } from '@/entities/App';
import { Doc } from '@/entities/Doc';
import { Source } from '@/entities/Source';
import { Context } from '@/entities/Context';
import { Operation } from '@/entities/Operation';
import { createEmptyQueryCacheState } from '@/entities/QueryCache';
import { Note } from '@/entities/Note';
import { Link } from '@/entities/Link';
import { Highlight } from '@/entities/Highlight';
import { Glyph } from '@/entities/Glyph';
import type { User } from '@/entities/User';
import { DataStore } from '@/store/DataStore';
import { scrollStore } from '@/store/scroll.store';
import { Internal } from '@/entities/addon/Internal';
import { Locale } from '@/locales';
import {
    CanvasProfiler,
    type CanvasWorkerProfilerSnapshot,
    type CanvasWorkerSynchronizationSnapshot,
} from '@/app/body/CanvasProfiler';
import { getCanvasViewportSourceIds } from '@/app/body/CanvasViewport';
import type { RendererBackendStatus } from '@/app/body/CanvasRendererBackend';
import {
    buildReleaseGateCriteria,
    createEmptyRendererRefreshRateBaseline,
    createEmptyWorkerSnapshot,
    didRendererReleaseGatePass,
    evaluateRendererRefreshRateBaseline,
    formatRendererRefreshRateBaseline,
    getRendererInteractionBurst,
    getRendererInteractionScenario,
    rendererScenarioUsesViewport,
    type ReleaseGateResult,
    type RendererInteractionScenario,
    type RendererRefreshRateBaseline,
} from './RendererTest.metrics';
import s from './styles/RendererTest.module.css';
import { createOfflineRendererTestApp } from './RendererTest.fixture';
import {
    createEmptyRendererBenchmarkDiagnostics,
    createRendererBenchmarkNamespace,
    DEFAULT_RENDERER_BENCHMARK_TARGET,
    RENDERER_BENCHMARK_CLOCK_MS,
    evaluateRendererBenchmarkResourceGate,
    getRendererBenchmarkDocumentAtPercentile,
    getRendererBenchmarkSourceEventCount,
    getRendererBenchmarkTarget,
    RENDERER_BENCHMARK_CACHE_LIMIT,
    RENDERER_BENCHMARK_PENDING_LIMIT,
    RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT,
    RENDERER_BENCHMARK_TARGETS,
    type RendererBenchmarkDiagnostics,
} from './RendererTest.benchmark';
import {
    type RendererBenchmarkWorkerStore,
} from './RendererTest.worker-store';
import { createRendererBenchmarkWorkerStore } from './RendererTest.worker-store.factory';
import { createRendererBenchmarkViewportDescriptor } from './RendererTest.viewport';
import { RendererQueryReplayPage } from './RendererQueryReplay.page';
import { getRendererQueryReplayProfile } from './RendererTest.query-replay';

declare global {
    interface Window {
        /** Latest terminal renderer gate result retained for passive automation. */
        __rendererReleaseGateResult?: ReleaseGateResult;
        /** Live bounded-storage diagnostics retained for passive benchmark automation. */
        __rendererBenchmarkDiagnostics?: RendererBenchmarkDiagnostics;
    }
}

// ─── tuneable constants ──────────────────────────────────────────────────────

/** Simulated time window (ms) spread across all docs. */
const TIME_WINDOW_MS = 7 * 24 * 60 * 60 * 1000; // 1 week

/** Maximum progressive projection rate while exact pages are still committing. */
const STREAM_PROJECTION_INTERVAL_MS = 250;

/** Poll interval used to react to viewport revisions after the dataset commits. */
const VIEWPORT_PROJECTION_POLL_MS = 50;

/** Optional synthetic notes to render on top of docs. */
const DEFAULT_TOTAL_NOTES = 50;
const MIN_TOTAL_NOTES = 0;
const MAX_TOTAL_NOTES = 1_000_000;
const NOTE_STEP = 1;

/** Optional synthetic links to render between docs. */
const DEFAULT_TOTAL_LINKS = 5;
const MIN_TOTAL_LINKS = 0;
const MAX_TOTAL_LINKS = 1_000;
const LINK_STEP = 1;
const LINK_FOCUS_SCALE = 2.5;

/** Optional synthetic highlights to render across the timeline. */
const DEFAULT_TOTAL_HIGHLIGHTS = 5;
const MIN_TOTAL_HIGHLIGHTS = 0;
const MAX_TOTAL_HIGHLIGHTS = 1_000_000;
const HIGHLIGHT_STEP = 1;

/** Number of simulated sources. */
const SOURCE_COUNT = 8;

/** Fixed post-synchronization warm-up interval for the release benchmark. */
const RELEASE_GATE_WARM_UP_MS = 5_000;

/** Fixed interaction measurement interval from the release criteria. */
const RELEASE_GATE_MEASUREMENT_MS = 30_000;

/** Maximum wait for ACK-backed worker synchronization after canonical ingestion. */
const RELEASE_GATE_WORKER_SYNCHRONIZATION_TIMEOUT_MS = 300_000;

/** Size of the normal event delta injected during the measured interaction window. */
const RELEASE_GATE_DELTA_DOCS = 10_000;

/** Timeline/source space reserved below bootstrap events for the measured delta. */
const RELEASE_GATE_DELTA_RESERVE_MS = 20_000;

type RendererBenchmarkEngineMode = "default" | "height" | "graph" | "mixed";

type ReleaseGatePhase =
    | "disabled"
    | "preparing"
    | "injecting"
    | "synchronizing"
    | "warming"
    | "measuring"
    | "complete";

interface ReleaseGateFixture {
    documentMinTs: number;
    delta: Doc.Type[];
}

interface ReleaseGateProgress {
    enabled: boolean;
    supported: boolean;
    phase: ReleaseGatePhase;
    phaseElapsedMs: number;
    result: ReleaseGateResult | null;
}

interface ReleaseGateInteractionState {
    lastVerticalSlot: number;
    lastZoomSlot: number;
    geometry: ReleaseGateInteractionGeometry | null;
}

interface ReleaseGateInteractionGeometry {
    target: HTMLElement;
    viewportWidth: number;
    viewportHeight: number;
    left: number;
    top: number;
    width: number;
    height: number;
}

interface ReleaseGateSynchronizationTarget {
    operationEpoch: number;
    globalRevision: number;
    sourceRevisions: ReadonlyMap<string, number>;
}

// ─── mock entity IDs ─────────────────────────────────────────────────────────

const MOCK_OPERATION_ID = 'aaaaaaaa-0000-4000-8000-000000000001' as Operation.Id;
const MOCK_CONTEXT_A_ID = 'aaaaaaaa-0000-4000-8000-000000000002' as Context.Id;
const MOCK_CONTEXT_B_ID = 'aaaaaaaa-0000-4000-8000-000000000003' as Context.Id;
const MOCK_USER_ID = 'renderer-test-user' as User.Id;
const EMPTY_GLYPH_ID = null as unknown as Glyph.Id;

const SOURCE_IDS: Source.Id[] = Array.from({ length: SOURCE_COUNT }, (_, i) =>
    `aaaaaaaa-0000-4000-8000-0000000000${(10 + i).toString(16).padStart(2, '0')}` as Source.Id,
);

const SOURCE_NAMES = [
    'Security.evtx',
    'System.evtx',
    'Application.evtx',
    'Microsoft-Windows-Sysmon.evtx',
    'Microsoft-Windows-PowerShell.evtx',
    'network_capture.pcap',
    'auth.log',
    'kernel.log',
];

const EVENT_CODES = [1, 2, 3, 4, 5, 6, 7, 8, 4624, 4625, 4672, 4776, 4688, 4698, 1102];

// ─── helpers ─────────────────────────────────────────────────────────────────

function buildMockOperation(): Operation.Type {
    return {
        id: MOCK_OPERATION_ID,
        index: 'renderer_test',
        name: 'Renderer Test Operation',
        glyph_id: EMPTY_GLYPH_ID,
        selected: true,
    };
}

function buildMockContexts(): Context.Type[] {
    return [
        {
            id: MOCK_CONTEXT_A_ID,
            operation_id: MOCK_OPERATION_ID,
            name: 'renderer-test-context-a',
            color: '#4f86c6',
            type: 'context',
            owner_user_id: MOCK_USER_ID,
            granted_user_ids: [],
            granted_user_group_ids: [],
            time_created: Date.now(),
            time_updated: Date.now(),
            glyph_id: EMPTY_GLYPH_ID,
            selected: true,
        },
        {
            id: MOCK_CONTEXT_B_ID,
            operation_id: MOCK_OPERATION_ID,
            name: 'renderer-test-context-b',
            color: '#9b7ef3',
            type: 'context',
            owner_user_id: MOCK_USER_ID,
            granted_user_ids: [],
            granted_user_group_ids: [],
            time_created: Date.now(),
            time_updated: Date.now(),
            glyph_id: EMPTY_GLYPH_ID,
            selected: true,
        },
    ];
}

/**
 * Reads the deterministic engine scenario selected through `?engine=`.
 * @returns Supported benchmark mode, defaulting to the normal event renderer.
 */
function getRendererBenchmarkEngineMode(): RendererBenchmarkEngineMode {
    const requestedMode = new URLSearchParams(window.location.search).get("engine");
    return requestedMode === "height" ||
        requestedMode === "graph" ||
        requestedMode === "mixed"
        ? requestedMode
        : "default";
}

/**
 * Checks whether the current URL opted into the blocking renderer release gate.
 * @returns True only for `?gate=1` or `?gate=true`.
 */
function isRendererReleaseGateEnabled(): boolean {
    const gateValue = new URLSearchParams(window.location.search).get("gate");
    return gateValue === "1" || gateValue === "true";
}

/**
 * Checks the browser primitives required by the dedicated render-worker backend.
 * @returns True when worker canvas ownership transfer can be attempted.
 */
function supportsRendererReleaseGate(): boolean {
    return (
        typeof Worker !== "undefined" &&
        typeof MessageChannel !== "undefined" &&
        typeof OffscreenCanvas !== "undefined" &&
        typeof createImageBitmap === "function" &&
        typeof HTMLCanvasElement !== "undefined" &&
        typeof HTMLCanvasElement.prototype.transferControlToOffscreen === "function"
    );
}

/**
 * Persists and dispatches one terminal gate result after React can commit its UI state.
 * @param result Completed release-gate result.
 * @param passed Whether every supported release criterion passed.
 * @returns Nothing.
 */
function publishRendererReleaseGateCompletion(
    result: ReleaseGateResult,
    passed: boolean,
): void {
    window.__rendererReleaseGateResult = result;
    document.documentElement.dataset.rendererReleaseGate = passed
        ? "passed"
        : "failed";
    document.documentElement.dataset.rendererReleaseGatePhase = "complete";
    document.documentElement.dataset.rendererReleaseGateResult = passed
        ? "passed"
        : "failed";
    requestAnimationFrame(() => {
        document.dispatchEvent(new CustomEvent<ReleaseGateResult>(
            "renderer-release-gate-complete",
            { detail: result },
        ));
    });
}

/**
 * Resolves a localized label for the current automated release-gate phase.
 * @param phase Current release-gate lifecycle phase.
 * @param translate Active locale translation function.
 * @returns Localized phase label.
 */
function getReleaseGatePhaseLabel(
    phase: ReleaseGatePhase,
    translate: Locale.ContextValue["t"],
): string {
    const phaseKeys: Record<ReleaseGatePhase, string> = {
        disabled: "rendererTest.gatePreparing",
        preparing: "rendererTest.gatePreparing",
        injecting: "rendererTest.gateInjecting",
        synchronizing: "rendererTest.gateSynchronizing",
        warming: "rendererTest.gateWarming",
        measuring: "rendererTest.gateMeasuring",
        complete: "rendererTest.gateComplete",
    };
    return translate(phaseKeys[phase]);
}

/**
 * Resolves a localized label for one deterministic interaction workload.
 * @param scenario Selected pointer, viewport, or mixed benchmark workload.
 * @param translate Active locale translation function.
 * @returns Localized scenario label.
 */
function getRendererInteractionScenarioLabel(
    scenario: RendererInteractionScenario,
    translate: Locale.ContextValue["t"],
): string {
    const scenarioKeys: Record<RendererInteractionScenario, string> = {
        pointer: "rendererTest.scenarioPointer",
        pan: "rendererTest.scenarioPan",
        zoom: "rendererTest.scenarioZoom",
        vertical: "rendererTest.scenarioVertical",
        mixed: "rendererTest.scenarioMixed",
    };
    return translate(scenarioKeys[scenario]);
}

/**
 * Verifies that every canonical benchmark data revision has been ACKed and rendered.
 * @param synchronization Live state read directly from the mounted worker client.
 * @param target Canonical operation, global, and per-source revisions to await.
 * @returns True only when projections and data queues are empty at the target frame.
 */
function isReleaseGateDataSynchronized(
    synchronization: CanvasWorkerSynchronizationSnapshot | null,
    target: ReleaseGateSynchronizationTarget,
): boolean {
    if (
        !synchronization?.ready ||
        synchronization.operationEpoch !== target.operationEpoch ||
        synchronization.expectedGlobalRevision < target.globalRevision ||
        synchronization.acknowledgedGlobalRevision < target.globalRevision ||
        synchronization.renderedGlobalRevision < target.globalRevision ||
        synchronization.inFlightDataMessages !== 0 ||
        synchronization.pendingDataMessages !== 0 ||
        synchronization.pendingProjectionTasks !== 0 ||
        synchronization.pendingSourceResynchronizations !== 0 ||
        synchronization.backgroundProjectionDepth !== 0 ||
        synchronization.visibleProjectionDepth !== 0
    ) {
        return false;
    }

    const synchronizedSources = new Map(
        synchronization.sources.map((source) => [source.sourceId, source]),
    );
    for (const [sourceId, expectedRevision] of target.sourceRevisions) {
        const source = synchronizedSources.get(sourceId);
        if (
            !source ||
            source.expectedRevision < expectedRevision ||
            source.acknowledgedRevision < expectedRevision
        ) {
            return false;
        }
    }
    return true;
}

/**
 * Verifies data synchronization and the latest requested interactive viewport.
 * @param synchronization Live state read directly from the mounted worker client.
 * @param target Canonical operation, global, and per-source revisions to await.
 * @returns True only when data and viewport state are both visible in worker frames.
 */
function isReleaseGateWorkerSynchronized(
    synchronization: CanvasWorkerSynchronizationSnapshot | null,
    target: ReleaseGateSynchronizationTarget,
): boolean {
    return Boolean(
        isReleaseGateDataSynchronized(synchronization, target) &&
        synchronization &&
        synchronization.renderedViewportRevision >=
            synchronization.requestedViewportRevision
    );
}

/**
 * Resolves the source engine for one benchmark scenario and row.
 * @param mode Engine scenario requested by the benchmark URL.
 * @param sourceIndex Zero-based source row index.
 * @returns Concrete render-engine identifier for the source settings DTO.
 */
function getRendererBenchmarkSourceEngine(
    mode: RendererBenchmarkEngineMode,
    sourceIndex: number,
): Source.Type["settings"]["render_engine"] {
    if (mode !== "mixed") return mode;
    return (["default", "height", "graph"] as const)[sourceIndex % 3];
}

/**
 * Creates eight selected source fixtures for one deterministic engine scenario.
 * @param frameMinTs Minimum configured source and timeline timestamp.
 * @param frameMaxTs Maximum benchmark timeline timestamp.
 * @param totalDocs Total documents distributed across all sources.
 * @param engineMode Engine scenario selected for this benchmark run.
 * @returns Canonical source fixtures ready for the timeline application state.
 */
function buildMockSources(
    frameMinTs: number,
    frameMaxTs: number,
    totalDocs: number,
    engineMode: RendererBenchmarkEngineMode,
): Source.Type[] {
    return SOURCE_IDS.map((id, i) => {
        const contextId = i < SOURCE_COUNT / 2 ? MOCK_CONTEXT_A_ID : MOCK_CONTEXT_B_ID;
        const settings = {
            ...Internal.Settings.default,
            render_engine: getRendererBenchmarkSourceEngine(engineMode, i),
        };
        return {
            id,
            operation_id: MOCK_OPERATION_ID,
            context_id: contextId,
            name: SOURCE_NAMES[i] ?? `source_${i}.log`,
            plugin: 'win_evtx',
            type: 'source',
            owner_user_id: MOCK_USER_ID,
            granted_user_ids: [],
            granted_user_group_ids: [],
            time_created: Date.now(),
            time_updated: Date.now(),
            glyph_id: EMPTY_GLYPH_ID,
            selected: true,
            pinned: false,
            appliedQuery: null,
            queryCache: createEmptyQueryCacheState(),
            settings,
            total: getRendererBenchmarkSourceEventCount(
                totalDocs,
                i,
                SOURCE_COUNT,
            ),
            timestamp: { min: frameMinTs, max: frameMaxTs },
            nanotimestamp: {
                min: Internal.Transformator.toNanos(frameMinTs),
                max: Internal.Transformator.toNanos(frameMaxTs),
            },
            _sampleDataCached: {
                frequency_sample: settings.frequency_sample,
                min_timestamp: frameMinTs,
                max_timestampe: frameMaxTs,
                sample_data : null
                }
        } as Source.Type;
    });
}

/**
 * Generates the measured 10,000-document delta strictly before existing fixtures.
 * Keeping the batch timestamp-disjoint exercises the normal incremental upsert path
 * without introducing a benchmark-harness full-array merge on the main thread.
 * @param documentMinTs Minimum timestamp created for the active benchmark run.
 * @returns Deterministic new documents distributed across all benchmark sources.
 */
function generateReleaseGateDelta(documentMinTs: number): Doc.Type[] {
    const docs: Doc.Type[] = new Array(RELEASE_GATE_DELTA_DOCS);
    for (let index = 0; index < RELEASE_GATE_DELTA_DOCS; index++) {
        const sourceIndex = index % SOURCE_COUNT;
        docs[index] = {
            _id: `rgate-delta-${index.toString(16).padStart(8, "0")}` as Doc.Id,
            gulp_timestamp: documentMinTs - index - 1_000,
            "gulp.source_id": SOURCE_IDS[sourceIndex],
            "gulp.event_code": EVENT_CODES[index % EVENT_CODES.length],
            number_hash: ((index * 31) % 2048) - 1024,
        } as Doc.Type;
    }
    return docs;
}

/**
 * Returns the DOM bridge that owns the primary transferred canvas event listeners.
 * @returns Timeline wrapper element, or null before the canvas has mounted.
 */
function getReleaseGateInteractionTarget(): HTMLElement | null {
    const primaryCanvas = document.getElementById("canvas");
    return primaryCanvas?.parentElement ?? null;
}

/**
 * Reads and caches benchmark target geometry until the target or viewport changes.
 * @param state Mutable interaction state retaining the current geometry snapshot.
 * @returns Cached target coordinates, or null before the timeline has mounted.
 */
function getReleaseGateInteractionGeometry(
    state: ReleaseGateInteractionState,
): ReleaseGateInteractionGeometry | null {
    const target = getReleaseGateInteractionTarget();
    if (!target) {
        state.geometry = null;
        return null;
    }
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    if (
        state.geometry?.target === target &&
        state.geometry.viewportWidth === viewportWidth &&
        state.geometry.viewportHeight === viewportHeight
    ) {
        return state.geometry;
    }
    const rect = target.getBoundingClientRect();
    state.geometry = {
        target,
        viewportWidth,
        viewportHeight,
        left: rect.left,
        top: rect.top,
        width: Math.max(1, rect.width || target.clientWidth),
        height: Math.max(1, rect.height || target.clientHeight),
    };
    return state.geometry;
}

/**
 * Emits one deterministic display-tick interaction sample through the same DOM
 * bridge used by real pan, cursor-anchored zoom, keyboard scroll, and hover input.
 * @param elapsedMs Elapsed time in the current scripted phase.
 * @param state Mutable cadence markers that prevent repeated low-frequency actions.
 * @param scenario Isolated or mixed native-input workload to emit.
 * @param burst Number of native events emitted within this display tick.
 * @returns True when the mounted timeline bridge accepted the interaction sample.
 */
function dispatchReleaseGateInteraction(
    elapsedMs: number,
    state: ReleaseGateInteractionState,
    scenario: RendererInteractionScenario,
    burst: number,
): boolean {
    const geometry = getReleaseGateInteractionGeometry(state);
    if (!geometry) return false;
    const { target, left, top, width, height } = geometry;
    const zoomSlot = Math.floor(elapsedMs / 600);
    const shouldZoom = scenario === "zoom" ||
        (scenario === "mixed" && zoomSlot !== state.lastZoomSlot);
    if (shouldZoom) {
        state.lastZoomSlot = zoomSlot;
    }

    const verticalSlot = Math.floor(
        elapsedMs / (scenario === "vertical" ? 100 : 400),
    );
    const shouldMoveVertically = scenario === "vertical" ||
        (scenario === "mixed" && verticalSlot !== state.lastVerticalSlot);
    if (shouldMoveVertically) {
        state.lastVerticalSlot = verticalSlot;
    }

    for (let eventIndex = 0; eventIndex < burst; eventIndex++) {
        const eventElapsedMs = elapsedMs + eventIndex / burst * (1_000 / 60);
        const cursorX = left +
            width * (0.5 + Math.sin(eventElapsedMs / 1_700) * 0.35);
        const cursorY = top +
            height * (0.5 + Math.cos(eventElapsedMs / 1_300) * 0.35);

        if (scenario === "pan" || scenario === "mixed") {
            const panDirection = Math.sin(eventElapsedMs / 1_200) >= 0 ? 1 : -1;
            target.dispatchEvent(new WheelEvent("wheel", {
                bubbles: true,
                cancelable: true,
                clientX: cursorX,
                clientY: cursorY,
                deltaMode: 0,
                deltaX: panDirection * 12,
                deltaY: 0,
            }));
        }

        if (shouldZoom) {
            target.dispatchEvent(new WheelEvent("wheel", {
                bubbles: true,
                cancelable: true,
                clientX: cursorX,
                clientY: cursorY,
                deltaMode: 0,
                deltaX: 0,
                deltaY: zoomSlot % 2 === 0 ? -10 : 10,
            }));
        }

        if (shouldMoveVertically) {
            target.dispatchEvent(new KeyboardEvent("keydown", {
                bubbles: true,
                cancelable: true,
                key: verticalSlot % 2 === 0 ? "ArrowDown" : "ArrowUp",
            }));
        }

        if (scenario === "pointer" || scenario === "mixed") {
            target.dispatchEvent(new MouseEvent("mousemove", {
                bubbles: true,
                clientX: cursorX,
                clientY: cursorY,
            }));
        }
    }
    return true;
}

/**
 * Resolves one deterministic benchmark event without consulting a source-wide array.
 * @param sourceId Source owning the synthetic event.
 * @param percentile Ascending-time percentile from zero through one.
 * @param targetEvents Total benchmark target across every source.
 * @param documentMinTimestamp Minimum synthetic document timestamp.
 * @param frameSpanMs Synthetic timeline span.
 * @returns Recreated canonical event, or undefined when the source is empty.
 */
function getBenchmarkDocumentAtPercentile(
    sourceId: Source.Id,
    percentile: number,
    targetEvents: number,
    documentMinTimestamp: number,
    frameSpanMs: number,
): Doc.Type | undefined {
    const sourceIndex = SOURCE_IDS.indexOf(sourceId);
    if (sourceIndex < 0) return undefined;
    return getRendererBenchmarkDocumentAtPercentile(
        sourceId,
        sourceIndex,
        SOURCE_COUNT,
        percentile,
        targetEvents,
        documentMinTimestamp,
        frameSpanMs,
    );
}

/**
 * Publishes live benchmark diagnostics without adding translated UI strings.
 * @param diagnostics Detached storage, memory, cache, and LOD measurements.
 * @param terminal Whether automation should receive the terminal completion event.
 * @returns Nothing.
 */
function publishRendererBenchmarkDiagnostics(
    diagnostics: RendererBenchmarkDiagnostics,
    terminal = false,
): void {
    window.__rendererBenchmarkDiagnostics = diagnostics;
    const resourceGate = evaluateRendererBenchmarkResourceGate(diagnostics);
    const dataset = document.documentElement.dataset;
    dataset.rendererBenchmarkStatus = diagnostics.status;
    dataset.rendererBenchmarkCacheMode = diagnostics.cacheMode;
    dataset.rendererBenchmarkTargetEvents = String(diagnostics.targetEvents);
    dataset.rendererBenchmarkProcessedEvents = String(diagnostics.processedEvents);
    dataset.rendererBenchmarkPersistedEvents = String(diagnostics.persistedEvents);
    dataset.rendererBenchmarkEncodedBytes = String(diagnostics.encodedBytes);
    dataset.rendererBenchmarkWriteElapsedMs = String(diagnostics.writeElapsedMs);
    dataset.rendererBenchmarkWriteEventsPerSecond = String(
        diagnostics.writeEventsPerSecond,
    );
    dataset.rendererBenchmarkWriteEncodedBytesPerSecond = String(
        diagnostics.writeEncodedBytesPerSecond,
    );
    dataset.rendererBenchmarkHeapPeakBytes = String(diagnostics.heapPeakBytes);
    dataset.rendererBenchmarkHeapDeltaBytes = String(diagnostics.heapDeltaBytes);
    dataset.rendererBenchmarkHeapSupported = String(diagnostics.heapSupported);
    dataset.rendererBenchmarkMainHeapPeakBytes = String(diagnostics.mainHeapPeakBytes);
    dataset.rendererBenchmarkMainHeapSupported = String(diagnostics.mainHeapSupported);
    dataset.rendererBenchmarkMainCacheDocuments = String(
        diagnostics.mainCacheDocuments,
    );
    dataset.rendererBenchmarkMaximumMainCacheDocuments = String(
        diagnostics.maximumMainCacheDocuments,
    );
    dataset.rendererBenchmarkMaximumPendingDocuments = String(
        diagnostics.maximumPendingDocuments,
    );
    dataset.rendererBenchmarkMaximumRepresentatives = String(
        diagnostics.maximumRenderRepresentatives,
    );
    dataset.rendererBenchmarkMaximumRenderBatches = String(
        diagnostics.maximumRenderBatchesInFlight,
    );
    dataset.rendererBenchmarkCacheLimit = String(RENDERER_BENCHMARK_CACHE_LIMIT);
    dataset.rendererBenchmarkPendingLimit = String(RENDERER_BENCHMARK_PENDING_LIMIT);
    dataset.rendererBenchmarkRepresentativeLimit = String(
        RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT,
    );
    dataset.rendererBenchmarkResourceLimits = resourceGate.passed
        ? 'passed'
        : 'failed';
    dataset.rendererBenchmarkSkipReason = diagnostics.skipReason?.code ?? '';
    dataset.rendererBenchmarkFailureReason = diagnostics.failureReason ?? '';
    dataset.rendererBenchmarkEncodedBytesPerEvent = String(
        diagnostics.processedEvents > 0
            ? diagnostics.encodedBytes / diagnostics.processedEvents
            : 0,
    );
    if (!terminal) return;
    requestAnimationFrame(() => {
        document.dispatchEvent(
            new CustomEvent<RendererBenchmarkDiagnostics>(
                'renderer-benchmark-complete',
                { detail: diagnostics },
            ),
        );
    });
}

// ─── performance HUD ─────────────────────────────────────────────────────────

interface PerfStats {
    injected: number;
    total: number;
    fps: number;
    elapsed: number; // ms
    done: boolean;
}

interface PerfOverlayProps {
    stats: PerfStats;
    workerStats: CanvasWorkerProfilerSnapshot;
    targetDocs: number;
    onTargetDocsChange: (docs: number) => void;
    onRestart: () => void;
    isRunning: boolean;
    renderNotes: boolean;
    onRenderNotesChange: (enabled: boolean) => void;
    targetNotes: number;
    onTargetNotesChange: (value: number) => void;
    renderLinks: boolean;
    onRenderLinksChange: (enabled: boolean) => void;
    targetLinks: number;
    onTargetLinksChange: (value: number) => void;
    renderHighlights: boolean;
    onRenderHighlightsChange: (enabled: boolean) => void;
    targetHighlights: number;
    onTargetHighlightsChange: (value: number) => void;
    releaseGate: ReleaseGateProgress;
    engineMode: RendererBenchmarkEngineMode;
    interactionScenario: RendererInteractionScenario;
    interactionBurst: number;
    backendStatus: RendererBackendStatus;
    refreshRateBaseline: RendererRefreshRateBaseline | null;
}

/**
 * Renders deterministic injection controls and main/worker performance metrics.
 * @param props Renderer stress-harness controls and current measurements.
 * @returns Performance overlay UI.
 */
function PerfOverlay(props: PerfOverlayProps) {
    const {
        stats,
        workerStats,
        targetDocs,
        onTargetDocsChange,
        onRestart,
        isRunning,
        renderNotes,
        onRenderNotesChange,
        targetNotes,
        onTargetNotesChange,
        renderLinks,
        onRenderLinksChange,
        targetLinks,
        onTargetLinksChange,
        renderHighlights,
        onRenderHighlightsChange,
        targetHighlights,
        onTargetHighlightsChange,
        releaseGate,
        engineMode,
        interactionScenario,
        interactionBurst,
        backendStatus,
        refreshRateBaseline,
    } = props;
    const { t } = Locale.use();
    const pct = stats.total > 0 ? ((stats.injected / stats.total) * 100).toFixed(1) : '0.0';
    const maxNotesForDocs = Math.max(MIN_TOTAL_NOTES, Math.min(MAX_TOTAL_NOTES, targetDocs));
    const maxLinksForDocs = Math.max(MIN_TOTAL_LINKS, Math.min(MAX_TOTAL_LINKS, targetDocs));
    const releaseGateCriteria = releaseGate.result
        ? buildReleaseGateCriteria(releaseGate.result, t)
        : [];
    const releaseGatePassed =
        releaseGateCriteria.length > 0 &&
        releaseGateCriteria.every((criterion) => criterion.status !== "failed");
    const releaseGateActive =
        releaseGate.enabled &&
        releaseGate.phase !== "complete" &&
        releaseGate.phase !== "disabled";
    return (
        <div
            className={`${s.overlay} ${releaseGate.enabled ? s.gateOverlay : ""}`}
            data-release-gate-phase={releaseGate.phase}
            data-release-gate-result={
                releaseGate.result ? (releaseGatePassed ? "passed" : "failed") : "pending"
            }
            data-refresh-rate-status={
                refreshRateBaseline?.status ?? "collecting"
            }
        >
            <div className={s.badge}>
                {releaseGate.enabled
                    ? t('rendererTest.releaseGate')
                    : t('rendererTest.badge')}
            </div>
            {!releaseGate.enabled ? <div className={s.controls}>
                <div className={s.sliderRow}>
                    <label className={s.sliderLabel} htmlFor='renderer-doc-target'>{t('rendererTest.documents')}</label>
                    <select
                        id='renderer-doc-target'
                        className={s.numberInput}
                        disabled={isRunning}
                        value={targetDocs}
                        onChange={(event) => onTargetDocsChange(Number(event.target.value))}
                    >
                        {RENDERER_BENCHMARK_TARGETS.map((target) => (
                            <option key={target} value={target}>
                                {target.toLocaleString()}
                            </option>
                        ))}
                    </select>
                </div>
                <label className={s.checkRow} htmlFor='renderer-notes-toggle'>
                    <input
                        id='renderer-notes-toggle'
                        type='checkbox'
                        checked={renderNotes}
                        onChange={(e) => onRenderNotesChange(e.target.checked)}
                    />
                    {t('rendererTest.renderNotes')}
                </label>
                <div className={s.sliderRow}>
                    <label className={s.sliderLabel} htmlFor='renderer-note-slider'>{t('notes.title')}</label>
                    <input
                        id='renderer-note-number'
                        className={s.numberInput}
                        type='number'
                        min={MIN_TOTAL_NOTES}
                        max={maxNotesForDocs}
                        step={NOTE_STEP}
                        value={targetNotes}
                        disabled={!renderNotes}
                        onChange={(e) => {
                            const v = Math.max(
                                MIN_TOTAL_NOTES,
                                Math.min(maxNotesForDocs, Number(e.target.value) || MIN_TOTAL_NOTES),
                            );
                            onTargetNotesChange(v);
                        }}
                    />
                </div>
                <input
                    id='renderer-note-slider'
                    className={s.slider}
                    type='range'
                    min={MIN_TOTAL_NOTES}
                    max={maxNotesForDocs}
                    step={NOTE_STEP}
                    value={targetNotes}
                    disabled={!renderNotes}
                    onChange={(e) => onTargetNotesChange(Number(e.target.value))}
                />
                <label className={s.checkRow} htmlFor='renderer-links-toggle'>
                    <input
                        id='renderer-links-toggle'
                        type='checkbox'
                        checked={renderLinks}
                        onChange={(e) => onRenderLinksChange(e.target.checked)}
                    />
                    {t('rendererTest.renderLinks')}
                </label>
                <div className={s.sliderRow}>
                    <label className={s.sliderLabel} htmlFor='renderer-link-slider'>{t('rendererTest.links')}</label>
                    <input
                        id='renderer-link-number'
                        className={s.numberInput}
                        type='number'
                        min={MIN_TOTAL_LINKS}
                        max={maxLinksForDocs}
                        step={LINK_STEP}
                        value={targetLinks}
                        disabled={!renderLinks}
                        onChange={(e) => {
                            const v = Math.max(
                                MIN_TOTAL_LINKS,
                                Math.min(maxLinksForDocs, Number(e.target.value) || MIN_TOTAL_LINKS),
                            );
                            onTargetLinksChange(v);
                        }}
                    />
                </div>
                <input
                    id='renderer-link-slider'
                    className={s.slider}
                    type='range'
                    min={MIN_TOTAL_LINKS}
                    max={maxLinksForDocs}
                    step={LINK_STEP}
                    value={targetLinks}
                    disabled={!renderLinks}
                    onChange={(e) => onTargetLinksChange(Number(e.target.value))}
                />
                <label className={s.checkRow} htmlFor='renderer-highlights-toggle'>
                    <input
                        id='renderer-highlights-toggle'
                        type='checkbox'
                        checked={renderHighlights}
                        onChange={(e) => onRenderHighlightsChange(e.target.checked)}
                    />
                    {t('rendererTest.renderHighlights')}
                </label>
                <div className={s.sliderRow}>
                    <label className={s.sliderLabel} htmlFor='renderer-highlight-slider'>{t('rendererTest.highlights')}</label>
                    <input
                        id='renderer-highlight-number'
                        className={s.numberInput}
                        type='number'
                        min={MIN_TOTAL_HIGHLIGHTS}
                        max={MAX_TOTAL_HIGHLIGHTS}
                        step={HIGHLIGHT_STEP}
                        value={targetHighlights}
                        disabled={!renderHighlights}
                        onChange={(e) => {
                            const v = Math.max(MIN_TOTAL_HIGHLIGHTS, Math.min(MAX_TOTAL_HIGHLIGHTS, Number(e.target.value) || MIN_TOTAL_HIGHLIGHTS));
                            onTargetHighlightsChange(v);
                        }}
                    />
                </div>
                <input
                    id='renderer-highlight-slider'
                    className={s.slider}
                    type='range'
                    min={MIN_TOTAL_HIGHLIGHTS}
                    max={MAX_TOTAL_HIGHLIGHTS}
                    step={HIGHLIGHT_STEP}
                    value={targetHighlights}
                    disabled={!renderHighlights}
                    onChange={(e) => onTargetHighlightsChange(Number(e.target.value))}
                />
                <button className={s.restartButton} onClick={onRestart} type='button'>
                    {t('rendererTest.run')}
                </button>
            </div> : (
                <div className={s.controls}>
                    <div className={s.row}>
                        <span className={s.label}>{t('common.status')}</span>
                        <span className={s.value}>
                            {getReleaseGatePhaseLabel(releaseGate.phase, t)}
                        </span>
                    </div>
                    <div className={s.row}>
                        <span className={s.label}>{t('common.renderEngine')}</span>
                        <span className={s.value}>{engineMode}</span>
                    </div>
                    <div className={s.row}>
                        <span className={s.label}>{t('rendererTest.scenario')}</span>
                        <span className={s.value}>
                            {getRendererInteractionScenarioLabel(interactionScenario, t)}
                        </span>
                    </div>
                    <div className={s.row}>
                        <span className={s.label}>{t('rendererTest.burst')}</span>
                        <span className={s.value}>{interactionBurst}</span>
                    </div>
                    <div className={s.row}>
                        <span className={s.label}>{t('rendererTest.elapsed')}</span>
                        <span className={s.value}>
                            {(releaseGate.phaseElapsedMs / 1000).toFixed(1)}s
                        </span>
                    </div>
                    <button
                        className={s.restartButton}
                        disabled={isRunning || releaseGateActive || !releaseGate.supported}
                        onClick={onRestart}
                        type='button'
                    >
                        {t('rendererTest.run')}
                    </button>
                </div>
            )}
            <div className={s.row}>
                <span className={s.label}>{t('common.events')}</span>
                <span className={s.value}>{stats.injected.toLocaleString()} / {stats.total.toLocaleString()}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.progress')}</span>
                <span className={s.value}>{pct}%</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.fps')}</span>
                <span className={s.value}>{stats.fps}</span>
            </div>
            {releaseGate.enabled ? (
                <div className={s.row}>
                    <span className={s.label}>
                        {t('rendererTest.refreshRateBaseline')}
                    </span>
                    <span className={s.value}>
                        {refreshRateBaseline
                            ? formatRendererRefreshRateBaseline(
                                refreshRateBaseline,
                                t,
                            )
                            : t('rendererTest.refreshRateCollecting')}
                    </span>
                </div>
            ) : null}
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.elapsed')}</span>
                <span className={s.value}>{(stats.elapsed / 1000).toFixed(1)}s</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.requestedBackend')}</span>
                <span className={s.value}>{backendStatus.requestedBackend}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.activeBackend')}</span>
                <span className={s.value}>{backendStatus.activeBackend}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.fallbackReason')}</span>
                <span className={s.value}>
                    {backendStatus.fallbackReason ?? t('rendererTest.noFallback')}
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.frameIntervalP95')}</span>
                <span className={s.value}>
                    {workerStats.mainFrameIntervalP95Ms.toFixed(2)}ms ({t(
                        'rendererTest.samples',
                        { count: workerStats.mainFrameIntervalCount },
                    )})
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.frameIntervalP99')}</span>
                <span className={s.value}>
                    {workerStats.mainFrameIntervalP99Ms.toFixed(2)}ms
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.maximumFrameInterval')}</span>
                <span className={s.value}>
                    {workerStats.mainMaxFrameIntervalMs.toFixed(2)}ms
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.droppedFrames')}</span>
                <span className={s.value}>
                    {workerStats.mainDroppedFrameCount} ({
                        (workerStats.mainDroppedFrameRatio * 100).toFixed(2)
                    }%)
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.stallsOver50')}</span>
                <span className={s.value}>{workerStats.mainStallsOver50Ms}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.stallsOver100')}</span>
                <span className={s.value}>{workerStats.mainStallsOver100Ms}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.workerFrames')}</span>
                <span className={s.value}>{workerStats.frameCount}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.workerDrawP95')}</span>
                <span className={s.value}>{workerStats.drawP95Ms.toFixed(2)}ms</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.workerDrawAverage')}</span>
                <span className={s.value}>{workerStats.drawAverageMs.toFixed(2)}ms</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.localPointerLatencyP95')}</span>
                <span className={s.value}>
                    {workerStats.localPointerLatencyP95Ms.toFixed(2)}ms ({t(
                        'rendererTest.samples',
                        { count: workerStats.localPointerSampleCount },
                    )})
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.endToEndLatencyP95')}</span>
                <span className={s.value}>
                    {workerStats.endToEndInputLatencyP95Ms.toFixed(2)}ms ({t(
                        'rendererTest.samples',
                        { count: workerStats.inputLatencySampleCount },
                    )})
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.workerReceiveP95')}</span>
                <span className={s.value}>{workerStats.workerReceiveP95Ms.toFixed(2)}ms</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.workerWaitP95')}</span>
                <span className={s.value}>{workerStats.workerWaitP95Ms.toFixed(2)}ms</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.dataLatencyP95')}</span>
                <span className={s.value}>
                    {workerStats.dataLatencyP95Ms.toFixed(2)}ms ({t(
                        'rendererTest.samples',
                        { count: workerStats.dataLatencySampleCount },
                    )})
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.maxDataQueue')}</span>
                <span className={s.value}>{workerStats.maxQueueDepth}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.maxInFlightQueue')}</span>
                <span className={s.value}>{workerStats.maxInFlightQueueDepth}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.maxPendingQueue')}</span>
                <span className={s.value}>{workerStats.maxPendingQueueDepth}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.longTaskSupport')}</span>
                <span className={s.value}>
                    {workerStats.mainLongTaskSupported
                        ? t('rendererTest.supported')
                        : t('rendererTest.unsupported')}
                </span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.longTaskCount')}</span>
                <span className={s.value}>{workerStats.mainLongTaskCount}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.gateMainLongestTask')}</span>
                <span className={s.value}>{workerStats.mainLongestTaskMs.toFixed(2)}ms</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.gateResyncs')}</span>
                <span className={s.value}>{workerStats.resyncCount}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('requests.errors')}</span>
                <span className={s.value}>{workerStats.workerErrorCount}</span>
            </div>
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.gateFallbacks')}</span>
                <span className={s.value}>{workerStats.fallbackCount}</span>
            </div>
            {releaseGate.enabled && releaseGate.result ? (
                <div className={s.gateResults}>
                    <div className={releaseGatePassed ? s.gatePassed : s.gateFailed}>
                        {releaseGatePassed
                            ? t('rendererTest.gatePassed')
                            : t('rendererTest.gateFailed')}
                    </div>
                    {releaseGateCriteria.map((criterion) => (
                        <div
                            className={s.criterionRow}
                            data-criterion-status={criterion.status}
                            key={criterion.id}
                        >
                            <span className={s.label}>{criterion.label}</span>
                            <span
                                className={
                                    criterion.status === "passed"
                                        ? s.criterionPass
                                        : criterion.status === "failed"
                                            ? s.criterionFail
                                            : s.criterionUnsupported
                                }
                            >
                                {criterion.status === "passed"
                                    ? '✓'
                                    : criterion.status === "failed"
                                        ? '✕'
                                        : '—'} {criterion.measured}
                            </span>
                        </div>
                    ))}
                </div>
            ) : null}
            {stats.done && <div className={s.done}>{t('rendererTest.injectionComplete')}</div>}
        </div>
    );
}

/**
 * Isolates the measured Timeline from release-gate HUD state updates.
 * @returns Memoized production Timeline used by the stress harness.
 */
function RendererTestTimelineComponent() {
    return <Timeline disableSessionAutosave />;
}

const RendererTestTimeline = memo(RendererTestTimelineComponent);

// ─── main component ───────────────────────────────────────────────────────────

export namespace RendererTest {
    /**
     * Selects the existing EventStore benchmark or the QuerySocketSession replay.
     * @returns Renderer benchmark page.
     */
    export function Page() {
        const replayProfile = getRendererQueryReplayProfile(window.location.search);
        return replayProfile
            ? <RendererQueryReplayPage profile={replayProfile} />
            : <BenchmarkPage />;
    }

    /**
     * Renders the existing deterministic 10M, 20M, or 30M storage stress harness.
     * @returns Existing renderer benchmark page.
     */
    function BenchmarkPage() {
        const { setInfo, app, Info } = Application.use();
        const [releaseGateEnabled] = useState<boolean>(isRendererReleaseGateEnabled);
        const [releaseGateSupported] = useState<boolean>(supportsRendererReleaseGate);
        const [engineMode] = useState<RendererBenchmarkEngineMode>(
            getRendererBenchmarkEngineMode,
        );
        const [interactionScenario] = useState<RendererInteractionScenario>(() =>
            getRendererInteractionScenario(window.location.search),
        );
        const [interactionBurst] = useState<number>(() =>
            getRendererInteractionBurst(window.location.search),
        );
        const [timelineEnabled, setTimelineEnabled] = useState<boolean>(
            !releaseGateEnabled,
        );
        const [targetDocs, setTargetDocs] = useState<number>(() =>
            getRendererBenchmarkTarget(window.location.search),
        );
        const [renderNotes, setRenderNotes] = useState<boolean>(true);
        const [targetNotes, setTargetNotes] = useState<number>(DEFAULT_TOTAL_NOTES);
        const [renderLinks, setRenderLinks] = useState<boolean>(true);
        const [targetLinks, setTargetLinks] = useState<number>(DEFAULT_TOTAL_LINKS);
        const [renderHighlights, setRenderHighlights] = useState<boolean>(true);
        const [targetHighlights, setTargetHighlights] = useState<number>(DEFAULT_TOTAL_HIGHLIGHTS);
        const [runKey, setRunKey] = useState<number>(0);
        const [isRunning, setIsRunning] = useState<boolean>(false);
        const [workerStats, setWorkerStats] =
            useState<CanvasWorkerProfilerSnapshot>(() => CanvasProfiler.getWorkerSnapshot());
        const [backendStatus, setBackendStatus] = useState<RendererBackendStatus>(() =>
            CanvasProfiler.getRendererBackendStatus(),
        );
        const [refreshRateBaseline, setRefreshRateBaseline] =
            useState<RendererRefreshRateBaseline | null>(null);
        const [releaseGate, setReleaseGate] = useState<ReleaseGateProgress>({
            enabled: releaseGateEnabled,
            supported: releaseGateSupported,
            phase: releaseGateEnabled ? "preparing" : "disabled",
            phaseElapsedMs: 0,
            result: null,
        });

        const [benchmarkDiagnostics, setBenchmarkDiagnostics] =
            useState<RendererBenchmarkDiagnostics>(() =>
                createEmptyRendererBenchmarkDiagnostics(
                    getRendererBenchmarkTarget(window.location.search),
                ),
            );

        const [perfStats, setPerfStats] = useState<PerfStats>({
            injected: 0,
            total: getRendererBenchmarkTarget(window.location.search),
            fps: 0,
            elapsed: 0,
            done: false,
        });

        // FPS tracking
        const fpsFrameRef = useRef<number>(0);
        const fpsTsRef = useRef<number>(performance.now());
        const fpsCountRef = useRef<number>(0);
        const releaseGateFrameRef = useRef<number>(0);
        const releaseGateFixtureRef = useRef<ReleaseGateFixture | null>(null);
        const benchmarkStoreRef = useRef<RendererBenchmarkWorkerStore | null>(null);
        const benchmarkSourcesRef = useRef<Source.Type[]>([]);
        const benchmarkProjectionRevisionRef = useRef<number>(0);
        const benchmarkProjectionInFlightRef = useRef<Promise<boolean> | null>(null);
        const benchmarkProjectionErrorRef = useRef<unknown>(null);
        const benchmarkProjectionFailuresRef = useRef<number>(0);
        const benchmarkProjectionRetryAtRef = useRef<number>(0);
        const benchmarkFirstUnacceptedAtRef = useRef<number>(0);
        const benchmarkTeardownRef = useRef<Promise<void>>(Promise.resolve());
        const benchmarkProjectionKeyRef = useRef<string>('');
        const benchmarkLastProjectionAtRef = useRef<number>(0);
        const latestAppRef = useRef<App.Type>(app);
        latestAppRef.current = app;

        const tickFps = useCallback(() => {
            fpsCountRef.current++;
            const now = performance.now();
            const delta = now - fpsTsRef.current;
            if (delta >= 1000) {
                const fps = Math.round((fpsCountRef.current * 1000) / delta);
                fpsCountRef.current = 0;
                fpsTsRef.current = now;
                setPerfStats((prev) => ({ ...prev, fps }));
            }
            fpsFrameRef.current = requestAnimationFrame(tickFps);
        }, []);

        useEffect(() => {
            if (!releaseGateEnabled) return;

            let previousPreference: string | null = null;
            let rendererPreferenceChanged = false;
            try {
                previousPreference = window.localStorage.getItem("gulp.rendererBackend");
                window.localStorage.setItem("gulp.rendererBackend", "worker");
                rendererPreferenceChanged = true;
            } catch {
                // Capability probing still defaults to worker mode when storage is unavailable.
            }
            CanvasProfiler.resetWorkerMetrics();
            setWorkerStats(CanvasProfiler.getWorkerSnapshot());
            setRefreshRateBaseline(null);
            setTimelineEnabled(true);

            if (!releaseGateSupported) {
                const unsupportedStats = createEmptyWorkerSnapshot();
                const unsupportedDiagnostics =
                    createEmptyRendererBenchmarkDiagnostics(targetDocs);
                const unsupportedResult: ReleaseGateResult = {
                    durationMs: 0,
                    mainThreadFps: 0,
                    workerReady: false,
                    deltaSent: false,
                    deltaSynchronized: false,
                    workerStats: unsupportedStats,
                    bootstrapStats: unsupportedStats,
                    totalResyncCount: 0,
                    totalWorkerErrorCount: 0,
                    totalFallbackCount: 0,
                    maximumQueueDepth: 0,
                    maximumInFlightQueueDepth: 0,
                    maximumPendingQueueDepth: 0,
                    backendStatus: CanvasProfiler.getRendererBackendStatus(),
                    scenario: interactionScenario,
                    burst: interactionBurst,
                    refreshRateBaseline: createEmptyRendererRefreshRateBaseline(),
                    benchmarkDiagnostics: unsupportedDiagnostics,
                };
                publishRendererBenchmarkDiagnostics(
                    unsupportedDiagnostics,
                    true,
                );
                setRefreshRateBaseline(unsupportedResult.refreshRateBaseline);
                setReleaseGate({
                    enabled: true,
                    supported: false,
                    phase: "complete",
                    phaseElapsedMs: 0,
                    result: unsupportedResult,
                });
                publishRendererReleaseGateCompletion(unsupportedResult, false);
            }

            return () => {
                if (!rendererPreferenceChanged) return;
                try {
                    if (previousPreference === null) {
                        window.localStorage.removeItem("gulp.rendererBackend");
                    } else {
                        window.localStorage.setItem(
                            "gulp.rendererBackend",
                            previousPreference,
                        );
                    }
                } catch {
                    // The benchmark can still unmount when storage becomes unavailable.
                }
            };
        }, [
            interactionBurst,
            interactionScenario,
            releaseGateEnabled,
            releaseGateSupported,
        ]);

        useEffect(() => {
            if (
                !releaseGateEnabled ||
                !releaseGateSupported ||
                !timelineEnabled ||
                runKey !== 0
            ) {
                return;
            }
            const startFrame = requestAnimationFrame(() => {
                setRunKey(1);
            });
            return () => cancelAnimationFrame(startFrame);
        }, [releaseGateEnabled, releaseGateSupported, runKey, timelineEnabled]);

        // Start FPS ticker once.
        useEffect(() => {
            if (releaseGateEnabled) return;
            fpsFrameRef.current = requestAnimationFrame(tickFps);

            return () => {
                cancelAnimationFrame(fpsFrameRef.current);
            };
        }, [releaseGateEnabled, tickFps]);

        useEffect(() => {
            if (releaseGateEnabled) {
                return CanvasProfiler.beginCapture();
            }
            return CanvasProfiler.subscribeWorker(setWorkerStats);
        }, [releaseGateEnabled]);

        useEffect(
            () => CanvasProfiler.subscribeRendererBackendStatus(setBackendStatus),
            [],
        );

        /**
         * Projects the current viewport from IndexedDB and publishes it only through
         * the render-window bus. At most one request is active in this page adapter.
         * @param force Whether to bypass duplicate and streaming cadence checks.
         * @returns True when a mounted Canvas listener received the projection.
         */
        const publishBenchmarkRenderWindow = useCallback(async (
            force = false,
        ): Promise<boolean> => {
            const store = benchmarkStoreRef.current;
            if (!store) return false;
            const status = store.getDiagnostics().status;
            if (status === 'idle' || status === 'skipped' || status === 'failed') {
                return false;
            }

            const activeRequest = benchmarkProjectionInFlightRef.current;
            if (activeRequest) {
                if (!force) return false;
                try {
                    await activeRequest;
                } catch {
                    // A forced terminal request supersedes a transient projection error.
                }
                return publishBenchmarkRenderWindow(true);
            }

            const synchronization = CanvasProfiler.getWorkerSynchronizationSnapshot();
            if (!synchronization?.ready) return false;
            const currentApp = latestAppRef.current;
            const { frame, width, scale, scrollX } = synchronization.viewport;
            if (frame.max <= frame.min) return false;
            const visibleSourceIds = new Set(getCanvasViewportSourceIds(
                Source.Entity.rendered(currentApp),
                scrollStore.getY(),
                document.getElementById('canvas')?.clientHeight ?? 900,
            ));
            const sources = benchmarkSourcesRef.current.filter((source) =>
                visibleSourceIds.has(source.id),
            );
            if (sources.length === 0) return false;

            const operationEpoch = DataStore.getRenderOperationEpoch();
            const sourceSignature = sources
                .map((source) => `${source.id}:${DataStore.getSourceRevision(source.id)}`)
                .join('|');
            const projectionKey = [
                operationEpoch,
                synchronization.requestedViewportRevision,
                frame.min,
                frame.max,
                width,
                scale,
                scrollX,
                sourceSignature,
            ].join(':');
            const now = performance.now();
            if (!force && now < benchmarkProjectionRetryAtRef.current) return false;
            if (!force && projectionKey === benchmarkProjectionKeyRef.current) {
                return false;
            }
            if (
                !force &&
                status === 'writing' &&
                now - benchmarkLastProjectionAtRef.current <
                    STREAM_PROJECTION_INTERVAL_MS
            ) {
                return false;
            }

            benchmarkLastProjectionAtRef.current = now;
            const descriptor = createRendererBenchmarkViewportDescriptor({
                operationEpoch,
                projectionRevision: ++benchmarkProjectionRevisionRef.current,
                synchronization,
                sources,
            });
            const request = store.publishRenderWindow(descriptor);
            benchmarkProjectionInFlightRef.current = request;
            try {
                const published = await request;
                if (published) {
                    benchmarkProjectionKeyRef.current = projectionKey;
                    benchmarkFirstUnacceptedAtRef.current = 0;
                    benchmarkProjectionFailuresRef.current = 0;
                    benchmarkProjectionRetryAtRef.current = 0;
                    benchmarkProjectionErrorRef.current = null;
                } else if (
                    store.getDiagnostics().persistedEvents > 0 &&
                    CanvasProfiler.getWorkerSynchronizationSnapshot()?.requestedViewportRevision ===
                        descriptor.viewportRevision
                ) {
                    const rejectedAt = performance.now();
                    benchmarkFirstUnacceptedAtRef.current ||= rejectedAt;
                    if (rejectedAt - benchmarkFirstUnacceptedAtRef.current >= 5_000) {
                        throw new Error('Canvas did not accept a benchmark projection within five seconds.');
                    }
                } else {
                    benchmarkFirstUnacceptedAtRef.current = 0;
                }
                const diagnostics = store.getDiagnostics();
                publishRendererBenchmarkDiagnostics(diagnostics);
                return published;
            } catch (error) {
                benchmarkProjectionErrorRef.current = error;
                benchmarkProjectionFailuresRef.current++;
                benchmarkProjectionRetryAtRef.current = performance.now() +
                    Math.min(2_000, 100 * 2 ** Math.min(5, benchmarkProjectionFailuresRef.current - 1));
                throw error;
            } finally {
                if (benchmarkProjectionInFlightRef.current === request) {
                    benchmarkProjectionInFlightRef.current = null;
                }
            }
        }, []);

        useEffect(() => {
            if (runKey === 0 || !timelineEnabled) return;
            const interval = window.setInterval(() => {
                void publishBenchmarkRenderWindow().catch((error) => {
                    console.error('[RendererBenchmark] projection failed', error);
                });
            }, VIEWPORT_PROJECTION_POLL_MS);
            return () => window.clearInterval(interval);
        }, [publishBenchmarkRenderWindow, runKey, timelineEnabled]);

        // Initialize + inject documents for each run.
        useEffect(() => {
            if (runKey === 0) {
                return;
            }

            const totalDocs = targetDocs;
            CanvasProfiler.resetWorkerMetrics();
            setWorkerStats(CanvasProfiler.getWorkerSnapshot());
            setRefreshRateBaseline(null);
            cancelAnimationFrame(releaseGateFrameRef.current);
            if (releaseGateEnabled) {
                delete window.__rendererReleaseGateResult;
                document.documentElement.dataset.rendererReleaseGate = 'pending';
                document.documentElement.dataset.rendererReleaseGatePhase =
                    'injecting';
                document.documentElement.dataset.rendererReleaseGateResult =
                    'pending';
                setReleaseGate({
                    enabled: true,
                    supported: releaseGateSupported,
                    phase: "injecting",
                    phaseElapsedMs: 0,
                    result: null,
                });
            }
            const totalNotes = Math.min(targetNotes, targetDocs);
            const totalLinks = Math.min(targetLinks, targetDocs);
            const totalHighlights = targetHighlights;
            const cacheMode = new URLSearchParams(window.location.search).get('cache') === 'warm'
                ? 'warm'
                : 'cold';
            const nowTs = RENDERER_BENCHMARK_CLOCK_MS;
            const documentMinTs = nowTs - TIME_WINDOW_MS;
            const frameMinTs = releaseGateEnabled
                ? documentMinTs - RELEASE_GATE_DELTA_RESERVE_MS
                : documentMinTs;
            const frameMaxTs = nowTs + TIME_WINDOW_MS;
            const documentSpanMs = frameMaxTs - documentMinTs;
            releaseGateFixtureRef.current = releaseGateEnabled
                ? {
                    documentMinTs,
                    delta: generateReleaseGateDelta(documentMinTs),
                }
                : null;
            const frameCenterTs = documentMinTs + Math.floor(documentSpanMs / 2);
            const operation = buildMockOperation();
            const contexts = buildMockContexts();
            const sources = buildMockSources(
                frameMinTs,
                frameMaxTs,
                totalDocs,
                engineMode,
            );
            const startTs = performance.now();
            let cancelled = false;

            // Reset data stores for a fresh run.
            Doc.Entity.clearIndex();
            DataStore.resetRenderOperation(MOCK_OPERATION_ID);
            Note.Entity.invalidateCache();
            const namespace = createRendererBenchmarkNamespace(MOCK_OPERATION_ID);
            DataStore.openEventStoreNamespace(namespace);
            const benchmarkStore = createRendererBenchmarkWorkerStore(namespace, {
                publishSourceSummary: (summary) => {
                    if (cancelled) return;
                    DataStore.publishEventSourceSummary(summary, 'deferred');
                    if (summary.status === 'building' && summary.eventCount === 0) {
                        DataStore.markTransientEventSources([summary.sourceId]);
                    }
                },
                readMainCacheDocuments: () => DataStore.Events.cacheSize,
            });
            benchmarkStoreRef.current = benchmarkStore;
            benchmarkSourcesRef.current = sources;
            benchmarkProjectionRevisionRef.current = 0;
            benchmarkProjectionErrorRef.current = null;
            benchmarkProjectionFailuresRef.current = 0;
            benchmarkProjectionRetryAtRef.current = 0;
            benchmarkFirstUnacceptedAtRef.current = 0;
            benchmarkProjectionKeyRef.current = '';
            benchmarkLastProjectionAtRef.current = Number.NEGATIVE_INFINITY;
            const emptyDiagnostics = createEmptyRendererBenchmarkDiagnostics(totalDocs, cacheMode);
            setBenchmarkDiagnostics(emptyDiagnostics);
            publishRendererBenchmarkDiagnostics(emptyDiagnostics);

            // Center horizontal viewport on the generated event time range.
            const algorithm = new Algorhithm({
                frame: { min: frameMinTs, max: frameMaxTs },
                scroll: { x: 0, y: 0 },
                width: Math.max(1, Info.width),
                scale: Math.max(0.01, app.timeline.scale),
            });
            const centerScrollX = algorithm.center_scroll_from_timestamp(frameCenterTs);
            scrollStore.setScroll(centerScrollX, -26);

            setPerfStats({
                injected: 0,
                total: totalDocs,
                fps: 0,
                elapsed: 0,
                done: false,
            });
            setIsRunning(true);

            setInfo(((prev: App.Type) => ({
                ...createOfflineRendererTestApp(prev),
                target: {
                    ...prev.target,
                    operations: [operation],
                    contexts,
                    files: sources,
                    notes: [],
                    links: [],
                    highlights: [],
                },
                timeline: {
                    ...prev.timeline,
                    target: null,
                    frame: {
                        min: frameMinTs,
                        max: frameMaxTs,
                    },
                },
            })) as unknown as App.Type);

            /**
             * Builds note fixtures from O(1) source-percentile event lookups.
             * @returns Deterministic notes distributed across the benchmark timeline.
             */
            const buildSyntheticNotes = (): Note.Type[] => {
                if (!renderNotes) {
                    return [];
                }

                const noteCount = Math.max(
                    MIN_TOTAL_NOTES,
                    Math.min(MAX_TOTAL_NOTES, totalNotes),
                );

                const notes: Note.Type[] = [];
                if (totalDocs === 0) {
                    return [];
                }

                for (let i = 0; i < noteCount; i++) {
                    // Pick events by evenly spaced percentile over time so notes
                    // are scattered across the whole rendered timestamp range.
                    const percentile = noteCount <= 1 ? 0 : i / (noteCount - 1);
                    const selectedSource = sources[i % sources.length];
                    const event = getBenchmarkDocumentAtPercentile(
                        selectedSource.id,
                        percentile,
                        totalDocs,
                        documentMinTs,
                        documentSpanMs,
                    );
                    if (!event) continue;
                    const sourceId = event['gulp.source_id'] as Source.Id;
                    const source = sources.find((s) => s.id === sourceId);
                    const contextId = source?.context_id ?? MOCK_CONTEXT_A_ID;
                    const eventNs = BigInt(event.gulp_timestamp) * 1_000_000n;

                    // Build `doc` with GulpBasicDocument-compatible alias fields
                    // and UI-friendly normalized fields.
                    const noteDoc: Doc.Type & {
                        '@timestamp': string;
                        'gulp.timestamp': bigint;
                        'gulp.timestamp_invalid': boolean;
                        'gulp.operation_id': Operation.Id;
                        'gulp.context_id': Context.Id;
                    } = {
                        _id: event._id,
                        '@timestamp': new Date(event.gulp_timestamp).toISOString(),
                        'gulp.timestamp': eventNs,
                        'gulp.timestamp_invalid': false,
                        'gulp.operation_id': MOCK_OPERATION_ID,
                        'gulp.context_id': contextId,
                        'gulp.source_id': sourceId,
                        gulp_timestamp: event.gulp_timestamp,
                        'gulp.event_code': event['gulp.event_code'],
                        number_hash: event.number_hash,
                    };

                    notes.push({
                        id: `rnote-${i.toString(16).padStart(8, '0')}` as Note.Id,
                        type: 'note',
                        operation_id: MOCK_OPERATION_ID,
                        context_id: contextId,
                        source_id: sourceId,
                        owner_user_id: MOCK_USER_ID,
                        glyph_id: EMPTY_GLYPH_ID,
                        name: `Renderer Note ${i + 1}`,
                        text: `Synthetic note #${i + 1} on ${String(event._id).slice(0, 12)}`,
                        tags: ['renderer-test'],
                        color: '#f0b84b',
                        edits: [],
                        time_pin: 0,
                        doc: noteDoc,
                    } as Note.Type);
                }

                return notes;
            };

            /**
             * Builds cross-context link fixtures without flattening canonical events.
             * @returns Deterministic links spanning both benchmark contexts.
             */
            const buildSyntheticLinks = (): Link.Type[] => {
                if (!renderLinks) {
                    return [];
                }

                const linkCount = Math.max(
                    MIN_TOTAL_LINKS,
                    Math.min(MAX_TOTAL_LINKS, totalLinks),
                );

                if (linkCount === 0) {
                    return [];
                }

                const sourceIdsInA = sources
                    .filter((s) => s.context_id === MOCK_CONTEXT_A_ID)
                    .map((s) => s.id);
                const sourceIdsInB = sources
                    .filter((s) => s.context_id === MOCK_CONTEXT_B_ID)
                    .map((s) => s.id);

                if (sourceIdsInA.length === 0 || sourceIdsInB.length === 0) {
                    return [];
                }

                const links: Link.Type[] = [];
                for (let i = 0; i < linkCount; i++) {
                    // Spread endpoints across the whole range: source uses direct percentile,
                    // destination uses a shifted percentile to avoid clustering on the same side.
                    const sourcePercentile = linkCount <= 1 ? 0 : i / (linkCount - 1);
                    const destinationPercentile = (sourcePercentile + 0.38196601125) % 1;
                    const sourceEvent = getBenchmarkDocumentAtPercentile(
                        sourceIdsInA[i % sourceIdsInA.length],
                        sourcePercentile,
                        totalDocs,
                        documentMinTs,
                        documentSpanMs,
                    );
                    const destinationEvent = getBenchmarkDocumentAtPercentile(
                        sourceIdsInB[i % sourceIdsInB.length],
                        destinationPercentile,
                        totalDocs,
                        documentMinTs,
                        documentSpanMs,
                    );
                    if (!sourceEvent || !destinationEvent) continue;

                    const docFrom = sourceEvent._id;
                    const docTo = destinationEvent._id;

                    links.push({
                        id: `rlink-${i.toString(16).padStart(8, '0')}` as Link.Id,
                        type: 'link',
                        owner_user_id: 'renderer-test-user',
                        operation_id: MOCK_OPERATION_ID,
                        description: `Synthetic link #${i + 1}`,
                        tags: ['renderer-test'],
                        color: '#3399ff',
                        doc_id_from: docFrom,
                        doc_ids: [docTo],
                        glyph_id: EMPTY_GLYPH_ID,
                        name: `Renderer Link ${i + 1}`,
                    } as Link.Type);
                }

                return links;
            };

            /**
             * Builds deterministic highlight ranges from source edge timestamps.
             * @returns Timeline highlight fixtures for the active benchmark run.
             */
            const buildSyntheticHighlights = (): Highlight.Type[] => {
                if (!renderHighlights) {
                    return [];
                }

                const highlightCount = Math.max(
                    MIN_TOTAL_HIGHLIGHTS,
                    Math.min(MAX_TOTAL_HIGHLIGHTS, totalHighlights),
                );

                if (highlightCount === 0) {
                    return [];
                }

                const sourceEdges = sources
                    .flatMap((source) => [
                        getBenchmarkDocumentAtPercentile(
                            source.id,
                            0,
                            totalDocs,
                            documentMinTs,
                            documentSpanMs,
                        ),
                        getBenchmarkDocumentAtPercentile(
                            source.id,
                            1,
                            totalDocs,
                            documentMinTs,
                            documentSpanMs,
                        ),
                    ])
                    .filter((event): event is Doc.Type => event !== undefined);
                if (sourceEdges.length === 0) {
                    return [];
                }

                const docsMinTs = Math.min(
                    ...sourceEdges.map((event) => event.gulp_timestamp),
                );
                const docsMaxTs = Math.max(
                    ...sourceEdges.map((event) => event.gulp_timestamp),
                );
                const span = Math.max(1, docsMaxTs - docsMinTs);
                const colors = ['blue', 'green', 'red', 'teal', 'gray', 'pink'];
                const highlights: Highlight.Type[] = [];

                for (let i = 0; i < highlightCount; i++) {
                    const linear = highlightCount <= 1 ? 0 : i / (highlightCount - 1);
                    const golden = (i * 0.61803398875) % 1;
                    const centerPercentile = Math.max(0, Math.min(1, (linear * 0.7) + (golden * 0.3)));

                    const minDuration = Math.max(60_000, Math.floor(span / 200));
                    const maxDuration = Math.max(minDuration, Math.floor(span / 8));
                    const ratio = 0.05 + ((((i * 2654435761) >>> 0) % 1000) / 1000) * 0.35;
                    const ratioDuration = Math.floor(span * ratio);
                    const duration = Math.max(minDuration, Math.min(maxDuration, ratioDuration));

                    const center = docsMinTs + Math.floor(centerPercentile * span);
                    const start = Math.max(docsMinTs, Math.min(docsMaxTs - 1, center - Math.floor(duration / 2)));
                    const end = Math.min(docsMaxTs, start + duration);

                    highlights.push({
                        id: `rhl-${i.toString(16).padStart(8, '0')}` as Highlight.Id,
                        type: 'highlight',
                        owner_user_id: MOCK_USER_ID,
                        operation_id: MOCK_OPERATION_ID,
                        description: `Synthetic highlight #${i + 1}`,
                        tags: ['renderer-test'],
                        time_range: [start, Math.max(start + 1, end)],
                        glyph_id: EMPTY_GLYPH_ID,
                        color: colors[i % colors.length],
                        name: `Renderer Highlight ${i + 1}`,
                    } as Highlight.Type);
                }

                return highlights;
            };

            /**
             * Terminates a skipped workload without exposing a partial dataset.
             * @param diagnostics Concrete skip reason and resource measurements.
             * @returns Nothing.
             */
            const completeQuotaSkip = (
                diagnostics: RendererBenchmarkDiagnostics,
            ): void => {
                const elapsed = performance.now() - startTs;
                setBenchmarkDiagnostics(diagnostics);
                setPerfStats((previous) => ({
                    ...previous,
                    injected: diagnostics.persistedEvents,
                    elapsed,
                    done: true,
                }));
                setIsRunning(false);
                if (releaseGateEnabled) {
                    const emptyWorkerStats = createEmptyWorkerSnapshot();
                    const result: ReleaseGateResult = {
                        durationMs: elapsed,
                        mainThreadFps: 0,
                        workerReady: false,
                        deltaSent: false,
                        deltaSynchronized: false,
                        workerStats: emptyWorkerStats,
                        bootstrapStats: emptyWorkerStats,
                        totalResyncCount: 0,
                        totalWorkerErrorCount: 0,
                        totalFallbackCount: 0,
                        maximumQueueDepth: 0,
                        maximumInFlightQueueDepth: 0,
                        maximumPendingQueueDepth: 0,
                        backendStatus: CanvasProfiler.getRendererBackendStatus(),
                        scenario: interactionScenario,
                        burst: interactionBurst,
                        refreshRateBaseline:
                            createEmptyRendererRefreshRateBaseline(),
                        benchmarkDiagnostics: diagnostics,
                    };
                    window.__rendererReleaseGateResult = result;
                    setReleaseGate((previous) => ({
                        ...previous,
                        phase: 'complete',
                        phaseElapsedMs: elapsed,
                        result: null,
                    }));
                    document.documentElement.dataset.rendererReleaseGate = 'skipped';
                    document.documentElement.dataset.rendererReleaseGatePhase =
                        'complete';
                    document.documentElement.dataset.rendererReleaseGateResult =
                        'skipped';
                    requestAnimationFrame(() => {
                        document.dispatchEvent(
                            new CustomEvent<ReleaseGateResult>(
                                'renderer-release-gate-complete',
                                { detail: result },
                            ),
                        );
                    });
                }
                publishRendererBenchmarkDiagnostics(diagnostics, true);
                console.info('[RendererBenchmark] skipped', {
                    targetEvents: totalDocs,
                    reason: diagnostics.skipReason?.code,
                });
            };

            /**
             * Runs cold writes or validates a warm cache until a terminal result.
             * @returns Promise settled after completion, skip, cancellation, or failure.
             */
            const runBenchmark = async (): Promise<void> => {
                try {
                    await benchmarkTeardownRef.current;
                    if (cancelled) return;
                    let progress = await benchmarkStore.start({
                        targetEvents: totalDocs,
                        sourceIds: SOURCE_IDS,
                        generation: runKey,
                        documentMinTimestamp: documentMinTs,
                        frameSpanMs: documentSpanMs,
                        cacheMode,
                    });
                    setBenchmarkDiagnostics(benchmarkStore.getDiagnostics());
                    publishRendererBenchmarkDiagnostics(benchmarkStore.getDiagnostics());

                    while (!progress.done && !cancelled) {
                        progress = await benchmarkStore.appendNextPage();
                        if (cancelled) break;
                        if (benchmarkProjectionFailuresRef.current >= 6) {
                            throw benchmarkProjectionErrorRef.current;
                        }
                        setBenchmarkDiagnostics(benchmarkStore.getDiagnostics());
                        publishRendererBenchmarkDiagnostics(benchmarkStore.getDiagnostics());
                        setPerfStats((previous) => ({
                            ...previous,
                            injected: progress.persistedEvents,
                            elapsed: performance.now() - startTs,
                        }));
                    }

                    if (cancelled) {
                        await benchmarkStore.cancel();
                        return;
                    }
                    if (progress.status === 'skipped') {
                        const diagnostics = benchmarkStore.getDiagnostics();
                        if (diagnostics.processedEvents > 0) {
                            DataStore.resetRenderOperation(MOCK_OPERATION_ID);
                        }
                        completeQuotaSkip(diagnostics);
                        return;
                    }

                    await benchmarkStore.finalize();
                    const allSources = sources.map((source, sourceIndex) => {
                        const oldest = getBenchmarkDocumentAtPercentile(
                            source.id,
                            0,
                            totalDocs,
                            documentMinTs,
                            documentSpanMs,
                        );
                        const newest = getBenchmarkDocumentAtPercentile(
                            source.id,
                            1,
                            totalDocs,
                            documentMinTs,
                            documentSpanMs,
                        );
                        const timestamp = releaseGateEnabled || !oldest || !newest
                            ? { min: frameMinTs, max: frameMaxTs }
                            : {
                                min: Math.min(
                                    oldest.gulp_timestamp,
                                    newest.gulp_timestamp,
                                ),
                                max: Math.max(
                                    oldest.gulp_timestamp,
                                    newest.gulp_timestamp,
                                ),
                            };
                        return {
                            ...source,
                            total: getRendererBenchmarkSourceEventCount(
                                totalDocs,
                                sourceIndex,
                                SOURCE_COUNT,
                            ),
                            timestamp,
                            nanotimestamp: {
                                min: Internal.Transformator.toNanos(timestamp.min),
                                max: Internal.Transformator.toNanos(timestamp.max),
                            },
                        } as Source.Type;
                    });
                    benchmarkSourcesRef.current = allSources;

                    const generatedNotes = buildSyntheticNotes();
                    const generatedLinks = buildSyntheticLinks();
                    const generatedHighlights = buildSyntheticHighlights();
                    const linkFocusDoc =
                        renderLinks && generatedLinks.length > 0
                            ? await DataStore.Events.getById(
                                generatedLinks[0].doc_ids[0],
                            )
                            : null;
                    const nextScale =
                        linkFocusDoc && renderLinks
                            ? Math.max(app.timeline.scale, LINK_FOCUS_SCALE)
                            : app.timeline.scale;
                    DataStore.replaceNotes(generatedNotes, 'deferred');
                    DataStore.replaceLinks(generatedLinks, 'deferred');
                    DataStore.replaceHighlights(generatedHighlights, 'deferred');
                    Note.Entity.invalidateCache();

                    setInfo(((previous: App.Type) => ({
                        ...previous,
                        target: {
                            ...previous.target,
                            files: allSources,
                            notes: generatedNotes,
                            links: generatedLinks,
                            highlights: generatedHighlights,
                        },
                        timeline: {
                            ...previous.timeline,
                            scale: nextScale,
                            target: linkFocusDoc ?? previous.timeline.target,
                        },
                    })) as unknown as App.Type);

                    if (linkFocusDoc) {
                        requestAnimationFrame(() => {
                            requestAnimationFrame(() => {
                                const focusAlgorithm = new Algorhithm({
                                    frame: { min: frameMinTs, max: frameMaxTs },
                                    scroll: { x: 0, y: -26 },
                                    width: Math.max(1, Info.width),
                                    scale: nextScale,
                                });
                                const linkCenterX = focusAlgorithm.center_scroll_from_timestamp(
                                    linkFocusDoc.gulp_timestamp,
                                );
                                scrollStore.setScroll(linkCenterX, -26);
                            });
                        });
                    }

                    let terminalProjectionAccepted = false;
                    for (let attempt = 0; attempt < 50 && !cancelled; attempt++) {
                        terminalProjectionAccepted = await publishBenchmarkRenderWindow(true);
                        if (terminalProjectionAccepted) break;
                        await new Promise<void>((resolve) => window.setTimeout(resolve, 100));
                    }
                    if (!terminalProjectionAccepted) {
                        throw new Error('Canvas did not accept the terminal benchmark projection.');
                    }
                    const diagnostics = benchmarkStore.getDiagnostics();
                    setBenchmarkDiagnostics(diagnostics);
                    publishRendererBenchmarkDiagnostics(diagnostics, true);
                    setPerfStats((previous) => ({
                        ...previous,
                        done: true,
                        elapsed: performance.now() - startTs,
                        injected: diagnostics.persistedEvents,
                    }));
                    setIsRunning(false);
                    if (releaseGateEnabled) {
                        setReleaseGate((previous) => ({
                            ...previous,
                            phase: 'synchronizing',
                            phaseElapsedMs: 0,
                        }));
                    }
                } catch (error) {
                    if (cancelled) return;
                    await benchmarkStore.cancel();
                    const diagnostics: RendererBenchmarkDiagnostics = {
                        ...benchmarkStore.getDiagnostics(),
                        status: 'failed',
                        failureReason: error instanceof Error ? error.message : String(error),
                    };
                    setBenchmarkDiagnostics(diagnostics);
                    publishRendererBenchmarkDiagnostics(diagnostics, true);
                    setIsRunning(false);
                    if (releaseGateEnabled) {
                        setReleaseGate((previous) => ({
                            ...previous,
                            phase: 'complete',
                            phaseElapsedMs: performance.now() - startTs,
                        }));
                        document.documentElement.dataset.rendererReleaseGatePhase =
                            'complete';
                        document.documentElement.dataset.rendererReleaseGate = 'failed';
                        document.documentElement.dataset.rendererReleaseGateResult =
                            'failed';
                    }
                    console.error('[RendererBenchmark] failed', error);
                }
            };

            const runPromise = runBenchmark();
            return () => {
                cancelled = true;
                const pendingProjection = benchmarkProjectionInFlightRef.current;
                if (benchmarkStoreRef.current === benchmarkStore) {
                    benchmarkStoreRef.current = null;
                }
                benchmarkTeardownRef.current = runPromise
                    .then(async () => {
                        try {
                            await pendingProjection;
                        } catch {
                            // A stale projection is expected when its viewport is replaced.
                        }
                        await benchmarkStore.cancel();
                        benchmarkStore.close();
                    })
                    .catch((error) => {
                        benchmarkStore.close();
                        console.error('[RendererBenchmark] cleanup failed', error);
                    });
            };
            // eslint-disable-next-line
        }, [runKey]);

        useEffect(() => {
            if (
                !releaseGateEnabled ||
                !releaseGateSupported ||
                !perfStats.done ||
                benchmarkDiagnostics.status !== 'complete' ||
                runKey === 0
            ) {
                return;
            }

            let cancelled = false;
            const synchronizationStartedAt = performance.now();
            const synchronizationTarget: ReleaseGateSynchronizationTarget = {
                operationEpoch: DataStore.getRenderOperationEpoch(),
                globalRevision: DataStore.getRenderSequence(),
                sourceRevisions: new Map(
                    SOURCE_IDS.map((sourceId) => [
                        sourceId,
                        DataStore.getSourceRevision(sourceId),
                    ]),
                ),
            };

            /**
             * Reads the freshest adapter telemetry for terminal release-gate output.
             * @returns Current bounded-storage benchmark diagnostics.
             */
            const readBenchmarkDiagnostics = (): RendererBenchmarkDiagnostics =>
                benchmarkStoreRef.current?.getDiagnostics() ?? benchmarkDiagnostics;

            /**
             * Publishes the terminal machine-readable and visual gate result.
             * @param result Completed or precondition-failed gate result.
             * @returns Nothing.
             */
            const publishReleaseGateResult = (result: ReleaseGateResult): void => {
                if (cancelled) return;
                const passed = didRendererReleaseGatePass(result);
                setWorkerStats(result.workerStats);
                setRefreshRateBaseline(result.refreshRateBaseline);
                setReleaseGate({
                    enabled: true,
                    supported: true,
                    phase: "complete",
                    phaseElapsedMs: result.durationMs,
                    result,
                });
                publishRendererReleaseGateCompletion(result, passed);
                console.info("[RendererReleaseGate]", { passed, engineMode, result });
            };

            /**
             * Stops before measurement when no worker backend can produce frames.
             * @param snapshot Latest bootstrap profiler snapshot.
             * @returns Nothing.
             */
            const failWorkerReadiness = (
                snapshot: CanvasWorkerProfilerSnapshot,
            ): void => {
                publishReleaseGateResult({
                    durationMs: performance.now() - synchronizationStartedAt,
                    mainThreadFps: 0,
                    workerReady: false,
                    deltaSent: false,
                    deltaSynchronized: false,
                    workerStats: createEmptyWorkerSnapshot(),
                    bootstrapStats: snapshot,
                    totalResyncCount: snapshot.resyncCount,
                    totalWorkerErrorCount: snapshot.workerErrorCount,
                    totalFallbackCount: snapshot.fallbackCount,
                    maximumQueueDepth: snapshot.maxQueueDepth,
                    maximumInFlightQueueDepth: snapshot.maxInFlightQueueDepth,
                    maximumPendingQueueDepth: snapshot.maxPendingQueueDepth,
                    backendStatus: CanvasProfiler.getRendererBackendStatus(),
                    scenario: interactionScenario,
                    burst: interactionBurst,
                    refreshRateBaseline: createEmptyRendererRefreshRateBaseline(),
                    benchmarkDiagnostics: readBenchmarkDiagnostics(),
                });
            };

            /**
             * Stops before measurement when warm-up cadence does not represent a stable 60 Hz display.
             * @param snapshot Worker metrics captured through the warm-up boundary.
             * @param displayBaseline Evaluated warm-up refresh-rate baseline.
             * @returns Nothing.
             */
            const failRefreshRatePrecondition = (
                snapshot: CanvasWorkerProfilerSnapshot,
                displayBaseline: RendererRefreshRateBaseline,
            ): void => {
                const backend = CanvasProfiler.getRendererBackendStatus();
                publishReleaseGateResult({
                    durationMs: performance.now() - synchronizationStartedAt,
                    mainThreadFps: 0,
                    workerReady:
                        snapshot.frameCount > 0 &&
                        snapshot.fallbackCount === 0 &&
                        backend.activeBackend === "worker",
                    deltaSent: false,
                    deltaSynchronized: false,
                    workerStats: snapshot,
                    bootstrapStats: snapshot,
                    totalResyncCount: snapshot.resyncCount,
                    totalWorkerErrorCount: snapshot.workerErrorCount,
                    totalFallbackCount: snapshot.fallbackCount,
                    maximumQueueDepth: snapshot.maxQueueDepth,
                    maximumInFlightQueueDepth: snapshot.maxInFlightQueueDepth,
                    maximumPendingQueueDepth: snapshot.maxPendingQueueDepth,
                    backendStatus: backend,
                    scenario: interactionScenario,
                    burst: interactionBurst,
                    refreshRateBaseline: displayBaseline,
                    benchmarkDiagnostics: readBenchmarkDiagnostics(),
                });
            };

            /**
             * Runs the fixed 30-second interaction window and injects one normal delta.
             * @param bootstrapStats Metrics captured before the measured profiler reset.
             * @param displayBaseline Validated warm-up display cadence.
             * @returns Nothing.
             */
            const startMeasurement = (
                bootstrapStats: CanvasWorkerProfilerSnapshot,
                displayBaseline: RendererRefreshRateBaseline,
            ): void => {
                CanvasProfiler.resetWorkerMetrics();
                const measurementStartedAt = performance.now();
                const interactionState: ReleaseGateInteractionState = {
                    lastVerticalSlot: -1,
                    lastZoomSlot: -1,
                    geometry: null,
                };
                let measuredFrames = 0;
                let deltaSent = false;
                let deltaCompleted = false;
                let deltaFailed = false;
                let deltaSynchronizationTarget: ReleaseGateSynchronizationTarget | null =
                    null;
                const measurementOperationEpoch =
                    DataStore.getRenderOperationEpoch();

                setReleaseGate((previous) => ({
                    ...previous,
                    phase: "measuring",
                    phaseElapsedMs: 0,
                }));

                /**
                 * Drives one measured display tick and finalizes at 30 seconds.
                 * @param timestamp Current animation-frame timestamp.
                 * @returns Nothing.
                 */
                const measureFrame = (timestamp: number): void => {
                    if (cancelled) return;
                    const elapsedMs = timestamp - measurementStartedAt;
                    measuredFrames++;
                    dispatchReleaseGateInteraction(
                        elapsedMs,
                        interactionState,
                        interactionScenario,
                        interactionBurst,
                    );

                    if (!deltaSent && elapsedMs >= 10_000) {
                        const fixture = releaseGateFixtureRef.current;
                        const benchmarkStore = benchmarkStoreRef.current;
                        if (fixture && benchmarkStore) {
                            deltaSent = true;
                            void benchmarkStore.upsertDocuments(fixture.delta).then(
                                async (summaries) => {
                                if (
                                    cancelled ||
                                    DataStore.getRenderOperationEpoch() !==
                                        measurementOperationEpoch
                                ) {
                                    deltaFailed = true;
                                    return;
                                }
                                const summariesBySource = new Map(
                                    summaries.map((summary) => [
                                        summary.sourceId,
                                        summary,
                                    ]),
                                );
                                const updatedSources = benchmarkSourcesRef.current.map(
                                    (source) => {
                                        const summary = summariesBySource.get(source.id);
                                        if (!summary) return source;
                                        return {
                                            ...source,
                                            total: summary.eventCount,
                                            timestamp: {
                                                min: summary.minTimestamp,
                                                max: summary.maxTimestamp,
                                            },
                                            nanotimestamp: {
                                                min: Internal.Transformator.toNanos(
                                                    summary.minTimestamp,
                                                ),
                                                max: Internal.Transformator.toNanos(
                                                    summary.maxTimestamp,
                                                ),
                                            },
                                        } as Source.Type;
                                    },
                                );
                                benchmarkSourcesRef.current = updatedSources;
                                setInfo(((previous: App.Type) => ({
                                    ...previous,
                                    target: {
                                        ...previous.target,
                                        files: updatedSources,
                                    },
                                })) as unknown as App.Type);
                                await publishBenchmarkRenderWindow(true);
                                const diagnostics = benchmarkStore.getDiagnostics();
                                setBenchmarkDiagnostics(diagnostics);
                                publishRendererBenchmarkDiagnostics(diagnostics);
                                deltaSynchronizationTarget = {
                                    operationEpoch: measurementOperationEpoch,
                                    globalRevision: DataStore.getRenderSequence(),
                                    sourceRevisions: new Map(
                                        SOURCE_IDS.map((sourceId) => [
                                            sourceId,
                                            DataStore.getSourceRevision(sourceId),
                                        ]),
                                    ),
                                };
                                deltaCompleted = true;
                            }).catch(() => {
                                const diagnostics = benchmarkStore.getDiagnostics();
                                setBenchmarkDiagnostics(diagnostics);
                                publishRendererBenchmarkDiagnostics(diagnostics);
                                deltaFailed = true;
                            });
                        }
                    }

                    if (elapsedMs < RELEASE_GATE_MEASUREMENT_MS) {
                        releaseGateFrameRef.current = requestAnimationFrame(measureFrame);
                        return;
                    }

                    const measuredStats = CanvasProfiler.getWorkerSnapshot();
                    const deltaSynchronization =
                        CanvasProfiler.getWorkerSynchronizationSnapshot();
                    const deltaSynchronized =
                        deltaCompleted &&
                        !deltaFailed &&
                        deltaSynchronizationTarget !== null &&
                        isReleaseGateDataSynchronized(
                            deltaSynchronization,
                            deltaSynchronizationTarget,
                        );
                    const result: ReleaseGateResult = {
                        durationMs: elapsedMs,
                        mainThreadFps: measuredFrames * 1_000 / elapsedMs,
                        workerReady:
                            bootstrapStats.frameCount > 0 &&
                            (!rendererScenarioUsesViewport(interactionScenario) ||
                                measuredStats.frameCount > 0) &&
                            bootstrapStats.fallbackCount + measuredStats.fallbackCount === 0,
                        deltaSent,
                        deltaSynchronized,
                        workerStats: measuredStats,
                        bootstrapStats,
                        totalResyncCount:
                            bootstrapStats.resyncCount + measuredStats.resyncCount,
                        totalWorkerErrorCount:
                            bootstrapStats.workerErrorCount +
                            measuredStats.workerErrorCount,
                        totalFallbackCount:
                            bootstrapStats.fallbackCount + measuredStats.fallbackCount,
                        maximumQueueDepth: Math.max(
                            bootstrapStats.maxQueueDepth,
                            measuredStats.maxQueueDepth,
                        ),
                        maximumInFlightQueueDepth: Math.max(
                            bootstrapStats.maxInFlightQueueDepth,
                            measuredStats.maxInFlightQueueDepth,
                        ),
                        maximumPendingQueueDepth: Math.max(
                            bootstrapStats.maxPendingQueueDepth,
                            measuredStats.maxPendingQueueDepth,
                        ),
                        backendStatus: CanvasProfiler.getRendererBackendStatus(),
                        scenario: interactionScenario,
                        burst: interactionBurst,
                        refreshRateBaseline: displayBaseline,
                        benchmarkDiagnostics: readBenchmarkDiagnostics(),
                    };
                    publishReleaseGateResult(result);
                };

                releaseGateFrameRef.current = requestAnimationFrame(measureFrame);
            };

            /**
             * Warms renderer caches and assets with the same deterministic interaction mix.
             * @returns Nothing.
             */
            const startWarmUp = (): void => {
                const warmUpStartedAt = performance.now();
                const warmUpFrameIntervalsMs: number[] = [];
                let previousWarmUpFrameTimestamp: number | null = null;
                const interactionState: ReleaseGateInteractionState = {
                    lastVerticalSlot: -1,
                    lastZoomSlot: -1,
                    geometry: null,
                };
                setReleaseGate((previous) => ({
                    ...previous,
                    phase: "warming",
                    phaseElapsedMs: 0,
                }));

                /**
                 * Drives cache warm-up until the fixed interval has elapsed.
                 * @param timestamp Current animation-frame timestamp.
                 * @returns Nothing.
                 */
                const warmUpFrame = (timestamp: number): void => {
                    if (cancelled) return;
                    if (previousWarmUpFrameTimestamp !== null) {
                        warmUpFrameIntervalsMs.push(
                            timestamp - previousWarmUpFrameTimestamp,
                        );
                    }
                    previousWarmUpFrameTimestamp = timestamp;
                    const elapsedMs = timestamp - warmUpStartedAt;
                    dispatchReleaseGateInteraction(
                        elapsedMs,
                        interactionState,
                        interactionScenario,
                        interactionBurst,
                    );
                    if (elapsedMs < RELEASE_GATE_WARM_UP_MS) {
                        releaseGateFrameRef.current = requestAnimationFrame(warmUpFrame);
                        return;
                    }
                    const displayBaseline = evaluateRendererRefreshRateBaseline(
                        warmUpFrameIntervalsMs,
                    );
                    const bootstrapStats = CanvasProfiler.getWorkerSnapshot();
                    setRefreshRateBaseline(displayBaseline);
                    if (displayBaseline.status !== "passed") {
                        failRefreshRatePrecondition(
                            bootstrapStats,
                            displayBaseline,
                        );
                        return;
                    }
                    startMeasurement(bootstrapStats, displayBaseline);
                };

                releaseGateFrameRef.current = requestAnimationFrame(warmUpFrame);
            };

            /**
             * Waits for exact ACK-backed source revisions to become visible in a frame.
             * @param timestamp Current animation-frame timestamp.
             * @returns Nothing.
             */
            const waitForWorkerSynchronization = (timestamp: number): void => {
                if (cancelled) return;
                const synchronization =
                    CanvasProfiler.getWorkerSynchronizationSnapshot();
                const elapsedMs = timestamp - synchronizationStartedAt;
                const currentBackend = CanvasProfiler.getRendererBackendStatus();
                if (
                    currentBackend.activeBackend === "legacy" ||
                    currentBackend.fallbackReason !== null
                ) {
                    failWorkerReadiness(CanvasProfiler.getWorkerSnapshot());
                    return;
                }
                if (elapsedMs >= RELEASE_GATE_WORKER_SYNCHRONIZATION_TIMEOUT_MS) {
                    failWorkerReadiness(CanvasProfiler.getWorkerSnapshot());
                    return;
                }
                if (
                    isReleaseGateWorkerSynchronized(
                        synchronization,
                        synchronizationTarget,
                    )
                ) {
                    const synchronizedStats = CanvasProfiler.getWorkerSnapshot();
                    if (synchronizedStats.frameCount > 0) {
                        startWarmUp();
                        return;
                    }
                }
                releaseGateFrameRef.current = requestAnimationFrame(
                    waitForWorkerSynchronization,
                );
            };

            releaseGateFrameRef.current = requestAnimationFrame(
                waitForWorkerSynchronization,
            );
            return () => {
                cancelled = true;
                cancelAnimationFrame(releaseGateFrameRef.current);
            };
        }, [
            benchmarkDiagnostics.status,
            engineMode,
            interactionBurst,
            interactionScenario,
            perfStats.done,
            releaseGateEnabled,
            releaseGateSupported,
            runKey,
            setInfo,
        ]);

        /** Restarts document injection and cancels any active gate phase by generation. */
        const restartRun = () => {
            setRunKey((v) => v + 1);
        };

        /**
         * Applies a bounded interactive document target.
         * @param docs Requested synthetic document total.
         * @returns Nothing.
         */
        const handleTargetDocsChange = (docs: number) => {
            const target = RENDERER_BENCHMARK_TARGETS.reduce(
                (closest, candidate) =>
                    Math.abs(candidate - docs) < Math.abs(closest - docs)
                        ? candidate
                        : closest,
                DEFAULT_RENDERER_BENCHMARK_TARGET,
            );
            setTargetDocs(target);
            const diagnostics = createEmptyRendererBenchmarkDiagnostics(target);
            setBenchmarkDiagnostics(diagnostics);
            publishRendererBenchmarkDiagnostics(diagnostics);
            setTargetNotes((previous) =>
                Math.min(previous, Math.max(MIN_TOTAL_NOTES, target)),
            );
            setTargetLinks((previous) =>
                Math.min(previous, Math.max(MIN_TOTAL_LINKS, target)),
            );
        };

        /**
         * Applies a bounded interactive note target.
         * @param value Requested synthetic note total.
         * @returns Nothing.
         */
        const handleTargetNotesChange = (value: number) => {
            const notesUpperBound = Math.max(MIN_TOTAL_NOTES, Math.min(MAX_TOTAL_NOTES, targetDocs));
            const bounded = Math.max(MIN_TOTAL_NOTES, Math.min(notesUpperBound, value));
            setTargetNotes(bounded);
        };

        /**
         * Applies a bounded interactive link target.
         * @param value Requested synthetic link total.
         * @returns Nothing.
         */
        const handleTargetLinksChange = (value: number) => {
            const linksUpperBound = Math.max(MIN_TOTAL_LINKS, Math.min(MAX_TOTAL_LINKS, targetDocs));
            const bounded = Math.max(MIN_TOTAL_LINKS, Math.min(linksUpperBound, value));
            setTargetLinks(bounded);
        };

        return (
            <Stack gap={12} className={s.page} ai='stretch'>
                {timelineEnabled ? <RendererTestTimeline /> : null}
                <PerfOverlay
                    stats={perfStats}
                    workerStats={workerStats}
                    targetDocs={targetDocs}
                    onTargetDocsChange={handleTargetDocsChange}
                    onRestart={restartRun}
                    isRunning={isRunning}
                    renderNotes={renderNotes}
                    onRenderNotesChange={setRenderNotes}
                    targetNotes={targetNotes}
                    onTargetNotesChange={handleTargetNotesChange}
                    renderLinks={renderLinks}
                    onRenderLinksChange={setRenderLinks}
                    targetLinks={targetLinks}
                    onTargetLinksChange={handleTargetLinksChange}
                    renderHighlights={renderHighlights}
                    onRenderHighlightsChange={setRenderHighlights}
                    targetHighlights={targetHighlights}
                    onTargetHighlightsChange={setTargetHighlights}
                    releaseGate={releaseGate}
                    engineMode={engineMode}
                    interactionScenario={interactionScenario}
                    interactionBurst={interactionBurst}
                    backendStatus={backendStatus}
                    refreshRateBaseline={refreshRateBaseline}
                />
            </Stack>
        );
    }
}
