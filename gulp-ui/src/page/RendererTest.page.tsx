/**
 * RendererTest.page.tsx
 *
 * Renderer stress-test mode — triggered by navigating to /renderer-test.
 *
 * Bypasses authentication, creates a fully mock operation / context / source and
 * feeds millions of synthetic DOCUMENTS_CHUNK-equivalent events directly into the
 * DataStore so the Canvas / Timeline pipeline can be exercised without a running
 * backend server.
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

declare global {
    interface Window {
        /** Latest terminal renderer gate result retained for passive automation. */
        __rendererReleaseGateResult?: ReleaseGateResult;
    }
}

// ─── tuneable constants ──────────────────────────────────────────────────────

/** Total number of synthetic documents to inject. */
const DEFAULT_TOTAL_DOCS = 1_000_000;
const RELEASE_GATE_TOTAL_DOCS = 10_000_000;
const MIN_TOTAL_DOCS = 0;
const MAX_TOTAL_DOCS = 100_000_000;
const DOC_STEP = 100_000;

/** How many docs to inject per animation frame / tick. */
const CHUNK_SIZE = 10_000;

/** Simulated time window (ms) spread across all docs. */
const TIME_WINDOW_MS = 7 * 24 * 60 * 60 * 1000; // 1 week

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
    const perSource = Math.ceil(totalDocs / SOURCE_COUNT);
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
            settings,
            total: perSource,
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
 * Generates one globally unique, descending-timestamp document batch.
 * The ordering lets the canonical datastore append older benchmark chunks without
 * repeatedly sorting every previously generated document.
 * @param batchIndex Zero-based benchmark batch index.
 * @param batchSize Number of documents to generate in this batch.
 * @param documentMinTs Minimum timestamp before deterministic per-source jitter.
 * @param frameSpanMs Total benchmark timeline duration.
 * @param totalDocs Total document count across all batches and sources.
 * @returns Synthetic documents distributed deterministically across eight sources.
 */
function generateDocBatch(
    batchIndex: number,
    batchSize: number,
    documentMinTs: number,
    frameSpanMs: number,
    totalDocs: number,
): Doc.Type[] {
    const docs: Doc.Type[] = new Array(batchSize);
    const offset = batchIndex * CHUNK_SIZE;
    for (let i = 0; i < batchSize; i++) {
        const globalIdx = totalDocs - 1 - (offset + i);
        const sourceIdx = globalIdx % SOURCE_COUNT;
        const sourceId = SOURCE_IDS[sourceIdx];
        // A source-constant jitter preserves strict descending order across chunk
        // boundaries, keeping 10M bootstrap ingestion on the O(delta) edge path.
        const fraction = globalIdx / totalDocs;
        const jitter = -350 - (sourceIdx * 17);
        const ts = documentMinTs + Math.floor(fraction * frameSpanMs) + jitter;

        docs[i] = {
            _id: `rdoc-${globalIdx.toString(16).padStart(12, '0')}` as Doc.Id,
            gulp_timestamp: ts,
            'gulp.source_id': sourceId,
            'gulp.event_code': EVENT_CODES[globalIdx % EVENT_CODES.length],
            number_hash: (globalIdx % 2048) - 1024,
        } as Doc.Type;
    }
    return docs;
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
 * Selects one event by ascending-time percentile from a descending source array.
 * @param sourceId Source whose canonical benchmark events are inspected.
 * @param percentile Desired position from oldest zero to newest one.
 * @returns Matching canonical document, or undefined for an empty source.
 */
function getBenchmarkEventAtPercentile(
    sourceId: Source.Id,
    percentile: number,
): Doc.Type | undefined {
    const events = DataStore.events.get(sourceId) ?? [];
    if (events.length === 0) return undefined;
    const boundedPercentile = Math.max(0, Math.min(1, percentile));
    const ascendingIndex = Math.floor(boundedPercentile * (events.length - 1));
    return events[events.length - 1 - ascendingIndex];
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
                    <label className={s.sliderLabel} htmlFor='renderer-doc-slider'>{t('rendererTest.documents')}</label>
                    <input
                        id='renderer-doc-number'
                        className={s.numberInput}
                        type='number'
                        min={MIN_TOTAL_DOCS}
                        max={MAX_TOTAL_DOCS}
                        step={DOC_STEP}
                        value={targetDocs}
                        onChange={(e) => {
                            const v = Math.max(MIN_TOTAL_DOCS, Math.min(MAX_TOTAL_DOCS, Number(e.target.value) || MIN_TOTAL_DOCS));
                            onTargetDocsChange(v);
                        }}
                    />
                </div>
                <input
                    id='renderer-doc-slider'
                    className={s.slider}
                    type='range'
                    min={MIN_TOTAL_DOCS}
                    max={MAX_TOTAL_DOCS}
                    step={DOC_STEP}
                    value={targetDocs}
                    onChange={(e) => onTargetDocsChange(Number(e.target.value))}
                />
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
     * Renders the deterministic timeline stress harness and streams synthetic data.
     * @returns Renderer benchmark page.
     */
    export function Page() {
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
        const [targetDocs, setTargetDocs] = useState<number>(
            releaseGateEnabled ? RELEASE_GATE_TOTAL_DOCS : DEFAULT_TOTAL_DOCS,
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

        const [perfStats, setPerfStats] = useState<PerfStats>({
            injected: 0,
            total: releaseGateEnabled ? RELEASE_GATE_TOTAL_DOCS : DEFAULT_TOTAL_DOCS,
            fps: 0,
            elapsed: 0,
            done: false,
        });

        // FPS tracking
        const fpsFrameRef = useRef<number>(0);
        const fpsTsRef = useRef<number>(performance.now());
        const fpsCountRef = useRef<number>(0);
        const animFrameRef = useRef<number>(0);
        const releaseGateFrameRef = useRef<number>(0);
        const releaseGateFixtureRef = useRef<ReleaseGateFixture | null>(null);
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
                };
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
            const nowTs = Date.now();
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
            const batches = Math.ceil(totalDocs / CHUNK_SIZE);
            const startTs = performance.now();
            let batchIdx = 0;
            let cancelled = false;

            // Reset data stores for a fresh run.
            cancelAnimationFrame(animFrameRef.current);
            Doc.Entity.clearIndex();
            DataStore.resetRenderOperation(MOCK_OPERATION_ID);
            Note.Entity.invalidateCache();

            // Pre-allocate event buckets per source.
            sources.forEach((src) => {
                DataStore.replaceSourceEvents(src.id, [], 'deferred');
            });

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
                if (!sources.some((source) => (DataStore.events.get(source.id)?.length ?? 0) > 0)) {
                    return [];
                }

                for (let i = 0; i < noteCount; i++) {
                    // Pick events by evenly spaced percentile over time so notes
                    // are scattered across the whole rendered timestamp range.
                    const percentile = noteCount <= 1 ? 0 : i / (noteCount - 1);
                    const selectedSource = sources[i % sources.length];
                    const event = getBenchmarkEventAtPercentile(
                        selectedSource.id,
                        percentile,
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
                    const sourceEvent = getBenchmarkEventAtPercentile(
                        sourceIdsInA[i % sourceIdsInA.length],
                        sourcePercentile,
                    );
                    const destinationEvent = getBenchmarkEventAtPercentile(
                        sourceIdsInB[i % sourceIdsInB.length],
                        destinationPercentile,
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

                const nonEmptyEventArrays = sources
                    .map((source) => DataStore.events.get(source.id) ?? [])
                    .filter((events) => events.length > 0);
                if (nonEmptyEventArrays.length === 0) {
                    return [];
                }

                const docsMinTs = Math.min(
                    ...nonEmptyEventArrays.map(
                        (events) => events[events.length - 1].gulp_timestamp,
                    ),
                );
                const docsMaxTs = Math.max(
                    ...nonEmptyEventArrays.map((events) => events[0].gulp_timestamp),
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
             * Appends one descending document chunk and yields until the next frame.
             * @returns Nothing.
             */
            const injectNextBatch = (): void => {
                if (cancelled) {
                    setIsRunning(false);
                    return;
                }

                if (batchIdx >= batches) {
                    // All done — finalize source totals and frame.
                    const allSources = sources.map((src) => {
                        const events = DataStore.events.get(src.id) ?? [];
                        const ts = releaseGateEnabled
                            ? { min: frameMinTs, max: frameMaxTs }
                            : events.length > 0
                                ? Doc.Entity.range(events)
                                : { min: frameMinTs, max: frameMaxTs };
                        return {
                            ...src,
                            total: events.length,
                            timestamp: ts,
                            nanotimestamp: {
                                min: Internal.Transformator.toNanos(ts.min),
                                max: Internal.Transformator.toNanos(ts.max),
                            },
                        } as Source.Type;
                    });

                    const generatedNotes = buildSyntheticNotes();
                    const generatedLinks = buildSyntheticLinks();
                    const generatedHighlights = buildSyntheticHighlights();
                    const linkFocusDoc =
                        renderLinks && generatedLinks.length > 0
                            ? Doc.Entity.id(app, generatedLinks[0].doc_ids[0])
                            : null;
                    const nextScale =
                        linkFocusDoc && renderLinks
                            ? Math.max(app.timeline.scale, LINK_FOCUS_SCALE)
                            : app.timeline.scale;
                    DataStore.replaceNotes(generatedNotes, 'deferred');
                    DataStore.replaceLinks(generatedLinks, 'deferred');
                    DataStore.replaceHighlights(generatedHighlights, 'deferred');
                    Note.Entity.invalidateCache();

                    setInfo(((prev: App.Type) => ({
                        ...prev,
                        target: {
                            ...prev.target,
                            files: allSources,
                            notes: generatedNotes,
                            links: generatedLinks,
                            highlights: generatedHighlights,
                        },
                        timeline: {
                            ...prev.timeline,
                            scale: nextScale,
                            target: linkFocusDoc ?? prev.timeline.target,
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

                    setPerfStats((prev) => ({
                        ...prev,
                        done: true,
                        elapsed: performance.now() - startTs,
                        injected: totalDocs,
                    }));
                    setIsRunning(false);
                    if (releaseGateEnabled) {
                        setReleaseGate((previous) => ({
                            ...previous,
                            phase: "synchronizing",
                            phaseElapsedMs: 0,
                        }));
                    }
                    return;
                }

                const remaining = totalDocs - batchIdx * CHUNK_SIZE;
                const thisBatch = Math.min(CHUNK_SIZE, remaining);
                const docs = generateDocBatch(
                    batchIdx,
                    thisBatch,
                    documentMinTs,
                    documentSpanMs,
                    totalDocs,
                );

                // Use the same ingestion data path used in the rest of the UI.
                Doc.Entity.add(app, docs);
                batchIdx++;

                const injected = batchIdx * CHUNK_SIZE;
                const elapsed = performance.now() - startTs;

                if (!releaseGateEnabled) {
                    setPerfStats((prev) => ({
                        ...prev,
                        injected: Math.min(injected, totalDocs),
                        elapsed,
                    }));
                }

                // Yield to the browser so it can render, then continue.
                animFrameRef.current = requestAnimationFrame(injectNextBatch);
            };

            animFrameRef.current = requestAnimationFrame(injectNextBatch);

            return () => {
                cancelled = true;
                cancelAnimationFrame(animFrameRef.current);
            };
            // eslint-disable-next-line
        }, [runKey]);

        useEffect(() => {
            if (
                !releaseGateEnabled ||
                !releaseGateSupported ||
                !perfStats.done ||
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
                        if (fixture) {
                            deltaSent = true;
                            void Doc.Entity.addAsync(
                                latestAppRef.current,
                                fixture.delta,
                                measurementOperationEpoch,
                            ).then(() => {
                                if (
                                    cancelled ||
                                    DataStore.getRenderOperationEpoch() !==
                                        measurementOperationEpoch
                                ) {
                                    deltaFailed = true;
                                    return;
                                }
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
            const boundedDocs = Math.max(MIN_TOTAL_DOCS, Math.min(MAX_TOTAL_DOCS, docs));
            setTargetDocs(boundedDocs);
            setTargetNotes((prev) => Math.min(prev, Math.max(MIN_TOTAL_NOTES, boundedDocs)));
            setTargetLinks((prev) => Math.min(prev, Math.max(MIN_TOTAL_LINKS, boundedDocs)));
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
