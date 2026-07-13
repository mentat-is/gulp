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
import { DataStore } from '@/store/DataStore';
import { scrollStore } from '@/store/scroll.store';
import { Internal } from '@/entities/addon/Internal';
import { Locale } from '@/locales';
import s from './styles/RendererTest.module.css';

// ─── tuneable constants ──────────────────────────────────────────────────────

/** Total number of synthetic documents to inject. */
const DEFAULT_TOTAL_DOCS = 1_000_000;
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

// ─── mock entity IDs ─────────────────────────────────────────────────────────

const MOCK_OPERATION_ID = 'aaaaaaaa-0000-4000-8000-000000000001' as Operation.Id;
const MOCK_CONTEXT_A_ID = 'aaaaaaaa-0000-4000-8000-000000000002' as Context.Id;
const MOCK_CONTEXT_B_ID = 'aaaaaaaa-0000-4000-8000-000000000003' as Context.Id;

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
        glyph_id: null as any,
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
            owner_user_id: 'admin' as any,
            granted_user_ids: [],
            granted_user_group_ids: [],
            time_created: Date.now(),
            time_updated: Date.now(),
            glyph_id: null as any,
            selected: true,
        },
        {
            id: MOCK_CONTEXT_B_ID,
            operation_id: MOCK_OPERATION_ID,
            name: 'renderer-test-context-b',
            color: '#9b7ef3',
            type: 'context',
            owner_user_id: 'admin' as any,
            granted_user_ids: [],
            granted_user_group_ids: [],
            time_created: Date.now(),
            time_updated: Date.now(),
            glyph_id: null as any,
            selected: true,
        },
    ];
}

function buildMockSources(frameMinTs: number, frameMaxTs: number, totalDocs: number): Source.Type[] {
    const perSource = Math.ceil(totalDocs / SOURCE_COUNT);
    return SOURCE_IDS.map((id, i) => {
        const contextId = i < SOURCE_COUNT / 2 ? MOCK_CONTEXT_A_ID : MOCK_CONTEXT_B_ID;
        return {
            id,
            operation_id: MOCK_OPERATION_ID,
            context_id: contextId,
            name: SOURCE_NAMES[i] ?? `source_${i}.log`,
            plugin: 'win_evtx',
            type: 'source',
            owner_user_id: 'admin' as any,
            granted_user_ids: [],
            granted_user_group_ids: [],
            time_created: Date.now(),
            time_updated: Date.now(),
            glyph_id: null as any,
            selected: true,
            pinned: false,
            settings: Internal.Settings.default,
            total: perSource,
            timestamp: { min: frameMinTs, max: frameMaxTs },
            nanotimestamp: {
                min: Internal.Transformator.toNanos(frameMinTs),
                max: Internal.Transformator.toNanos(frameMaxTs),
            },
            _sampleDataCached: {
                frequency_sample: Internal.Settings.default.frequency_sample,
                min_timestamp: frameMinTs,
                max_timestampe: frameMaxTs,
                sample_data : null
                }
        } as Source.Type;
    });
}

/** Generate a batch of synthetic Doc.Type objects. */
function generateDocBatch(
    batchIndex: number,
    batchSize: number,
    frameMinTs: number,
    frameSpanMs: number,
    totalDocs: number,
    generationOffset: number,
): Doc.Type[] {
    const docs: Doc.Type[] = new Array(batchSize);
    const offset = batchIndex * batchSize;
    for (let i = 0; i < batchSize; i++) {
        const globalIdx = (offset + i + generationOffset) % totalDocs;
        const sourceIdx = globalIdx % SOURCE_COUNT;
        const sourceId = SOURCE_IDS[sourceIdx];
        // Spread timestamps evenly across the window, jittered slightly per source
        const fraction = globalIdx / totalDocs;
        const jitter = ((sourceIdx * 37 + i * 13) % 1000) - 500; // ±500 ms
        const ts = frameMinTs + Math.floor(fraction * frameSpanMs) + jitter;

        docs[i] = {
            _id: `rdoc-${globalIdx.toString(16).padStart(12, '0')}` as Doc.Id,
            gulp_timestamp: ts,
            'gulp.source_id': sourceId,
            'gulp.event_code': EVENT_CODES[globalIdx % EVENT_CODES.length],
        } as Doc.Type;
    }
    return docs;
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
}

function PerfOverlay({
    stats,
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
}: PerfOverlayProps) {
    const { t } = Locale.use();
    const pct = stats.total > 0 ? ((stats.injected / stats.total) * 100).toFixed(1) : '0.0';
    const maxNotesForDocs = Math.max(MIN_TOTAL_NOTES, Math.min(MAX_TOTAL_NOTES, targetDocs));
    const maxLinksForDocs = Math.max(MIN_TOTAL_LINKS, Math.min(MAX_TOTAL_LINKS, targetDocs));
    return (
        <div className={s.overlay}>
            <div className={s.badge}>{t('rendererTest.badge')}</div>
            <div className={s.controls}>
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
            </div>
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
            <div className={s.row}>
                <span className={s.label}>{t('rendererTest.elapsed')}</span>
                <span className={s.value}>{(stats.elapsed / 1000).toFixed(1)}s</span>
            </div>
            {stats.done && <div className={s.done}>{t('rendererTest.injectionComplete')}</div>}
        </div>
    );
}

// ─── main component ───────────────────────────────────────────────────────────

export namespace RendererTest {
    export function Page() {
        const { setInfo, app, Info } = Application.use();
        const [targetDocs, setTargetDocs] = useState<number>(DEFAULT_TOTAL_DOCS);
        const [renderNotes, setRenderNotes] = useState<boolean>(false);
        const [targetNotes, setTargetNotes] = useState<number>(DEFAULT_TOTAL_NOTES);
        const [renderLinks, setRenderLinks] = useState<boolean>(false);
        const [targetLinks, setTargetLinks] = useState<number>(DEFAULT_TOTAL_LINKS);
        const [renderHighlights, setRenderHighlights] = useState<boolean>(false);
        const [targetHighlights, setTargetHighlights] = useState<number>(DEFAULT_TOTAL_HIGHLIGHTS);
        const [runKey, setRunKey] = useState<number>(0);
        const [isRunning, setIsRunning] = useState<boolean>(false);

        const [perfStats, setPerfStats] = useState<PerfStats>({
            injected: 0,
            total: DEFAULT_TOTAL_DOCS,
            fps: 0,
            elapsed: 0,
            done: false,
        });

        // FPS tracking
        const fpsFrameRef = useRef<number>(0);
        const fpsTsRef = useRef<number>(performance.now());
        const fpsCountRef = useRef<number>(0);
        const animFrameRef = useRef<number>(0);

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

        // Start FPS ticker once.
        useEffect(() => {
            fpsFrameRef.current = requestAnimationFrame(tickFps);

            return () => {
                cancelAnimationFrame(fpsFrameRef.current);
            };
        }, [tickFps]);

        // Initialize + inject documents for each run.
        useEffect(() => {
            if (runKey === 0) {
                return;
            }

            const totalDocs = targetDocs;
            const totalNotes = Math.min(targetNotes, targetDocs);
            const totalLinks = Math.min(targetLinks, targetDocs);
            const totalHighlights = targetHighlights;
            const nowTs = Date.now();
            const frameMinTs = nowTs - TIME_WINDOW_MS;
            const frameMaxTs = nowTs + TIME_WINDOW_MS;
            const frameSpanMs = frameMaxTs - frameMinTs;
            const frameCenterTs = frameMinTs + Math.floor(frameSpanMs / 2);
            const operation = buildMockOperation();
            const contexts = buildMockContexts();
            const sources = buildMockSources(frameMinTs, frameMaxTs, totalDocs);
            const batches = Math.ceil(totalDocs / CHUNK_SIZE);
            const generationOffset = Math.floor(totalDocs / 2);
            const startTs = performance.now();
            let batchIdx = 0;
            let cancelled = false;

            // Reset data stores for a fresh run.
            cancelAnimationFrame(animFrameRef.current);
            Doc.Entity.clearIndex();
            DataStore.events.clear();
            DataStore.notes = [];
            DataStore.links = [];
            DataStore.highlights = [];
            Note.Entity.invalidateCache();

            // Pre-allocate event buckets per source.
            sources.forEach((src) => {
                DataStore.events.set(src.id, []);
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
                ...prev,
                general: {
                    ...prev.general,
                    skippedAuth: true,
                    user: {
                        id: 'renderer-test-user' as any,
                        name: 'Renderer Test',
                        email: 'test@renderer',
                        time_created: Date.now(),
                        time_updated: Date.now(),
                        permission: 0xFFFF,
                        glyph_id: null as any,
                    },
                },
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

            const buildSyntheticNotes = (): Note.Type[] => {
                if (!renderNotes) {
                    return [];
                }

                const noteCount = Math.max(
                    MIN_TOTAL_NOTES,
                    Math.min(MAX_TOTAL_NOTES, totalNotes),
                );

                const notes: Note.Type[] = [];
                const allEvents = sources
                    .flatMap((src) => DataStore.events.get(src.id) ?? [])
                    .slice()
                    .sort((a, b) => a.gulp_timestamp - b.gulp_timestamp);

                if (allEvents.length === 0) {
                    return [];
                }

                for (let i = 0; i < noteCount; i++) {
                    // Pick events by evenly spaced percentile over time so notes
                    // are scattered across the whole rendered timestamp range.
                    const percentile = noteCount <= 1 ? 0 : i / (noteCount - 1);
                    const eventIndex = Math.floor(percentile * (allEvents.length - 1));
                    const event = allEvents[eventIndex];
                    const sourceId = event['gulp.source_id'] as Source.Id;
                    const source = sources.find((s) => s.id === sourceId);
                    const contextId = source?.context_id ?? MOCK_CONTEXT_A_ID;
                    const eventNs = BigInt(event.gulp_timestamp) * 1_000_000n;

                    // Build `doc` with GulpBasicDocument-compatible alias fields
                    // and UI-friendly normalized fields.
                    const noteDoc = {
                        _id: event._id,
                        '@timestamp': new Date(event.gulp_timestamp).toISOString(),
                        'gulp.timestamp': eventNs,
                        'gulp.timestamp_invalid': false,
                        'gulp.operation_id': MOCK_OPERATION_ID,
                        'gulp.context_id': contextId,
                        'gulp.source_id': sourceId,
                        gulp_timestamp: event.gulp_timestamp,
                        'gulp.event_code': event['gulp.event_code'],
                    } as any;

                    notes.push({
                        id: (`rnote-${i.toString(16).padStart(8, '0')}` as unknown) as Note.Id,
                        type: 'note',
                        operation_id: MOCK_OPERATION_ID,
                        context_id: contextId,
                        source_id: sourceId,
                        owner_user_id: ('renderer-test-user' as unknown) as any,
                        glyph_id: null as any,
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

                const eventsInA = sourceIdsInA
                    .flatMap((sid) => DataStore.events.get(sid) ?? [])
                    .sort((a, b) => a.gulp_timestamp - b.gulp_timestamp);
                const eventsInB = sourceIdsInB
                    .flatMap((sid) => DataStore.events.get(sid) ?? [])
                    .sort((a, b) => a.gulp_timestamp - b.gulp_timestamp);

                if (eventsInA.length === 0 || eventsInB.length === 0) {
                    return [];
                }

                const links: Link.Type[] = [];
                for (let i = 0; i < linkCount; i++) {
                    // Spread endpoints across the whole range: source uses direct percentile,
                    // destination uses a shifted percentile to avoid clustering on the same side.
                    const sourcePercentile = linkCount <= 1 ? 0 : i / (linkCount - 1);
                    const destinationPercentile = (sourcePercentile + 0.38196601125) % 1;
                    const fromIdx = Math.floor(sourcePercentile * (eventsInA.length - 1));
                    const toIdx = Math.floor(destinationPercentile * (eventsInB.length - 1));

                    const docFrom = eventsInA[fromIdx]._id;
                    const docTo = eventsInB[toIdx]._id;

                    links.push({
                        id: (`rlink-${i.toString(16).padStart(8, '0')}` as unknown) as Link.Id,
                        type: 'link',
                        owner_user_id: 'renderer-test-user',
                        operation_id: MOCK_OPERATION_ID,
                        description: `Synthetic link #${i + 1}`,
                        tags: ['renderer-test'],
                        color: '#3399ff',
                        doc_id_from: docFrom,
                        doc_ids: [docTo],
                        glyph_id: null as any,
                        name: `Renderer Link ${i + 1}`,
                    } as Link.Type);
                }

                return links;
            };

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

                const allEvents = sources.flatMap((src) => DataStore.events.get(src.id) ?? []);
                if (allEvents.length === 0) {
                    return [];
                }

                const { min: docsMinTs, max: docsMaxTs } = Doc.Entity.range(allEvents);
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
                        id: (`rhl-${i.toString(16).padStart(8, '0')}` as unknown) as Highlight.Id,
                        type: 'highlight',
                        owner_user_id: 'renderer-test-user' as any,
                        operation_id: MOCK_OPERATION_ID,
                        description: `Synthetic highlight #${i + 1}`,
                        tags: ['renderer-test'],
                        time_range: [start, Math.max(start + 1, end)],
                        glyph_id: null as any,
                        color: colors[i % colors.length],
                        name: `Renderer Highlight ${i + 1}`,
                    } as Highlight.Type);
                }

                return highlights;
            };

            const injectNextBatch = () => {
                if (cancelled) {
                    setIsRunning(false);
                    return;
                }

                if (batchIdx >= batches) {
                    // All done — finalize source totals and frame.
                    const allSources = sources.map((src) => {
                        const events = DataStore.events.get(src.id) ?? [];
                        const ts = events.length > 0
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
                    DataStore.notes = generatedNotes;
                    DataStore.links = generatedLinks;
                    DataStore.highlights = generatedHighlights;
                    Note.Entity.invalidateCache();
                    DataStore.markDirty();

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
                    return;
                }

                const remaining = totalDocs - batchIdx * CHUNK_SIZE;
                const thisBatch = Math.min(CHUNK_SIZE, remaining);
                const docs = generateDocBatch(
                    batchIdx,
                    thisBatch,
                    frameMinTs,
                    frameSpanMs,
                    totalDocs,
                    generationOffset,
                );

                // Use the same ingestion data path used in the rest of the UI.
                Doc.Entity.add(app, docs);
                batchIdx++;

                const injected = batchIdx * CHUNK_SIZE;
                const elapsed = performance.now() - startTs;

                setPerfStats((prev) => ({
                    ...prev,
                    injected: Math.min(injected, totalDocs),
                    elapsed,
                }));

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

        const restartRun = () => {
            setRunKey((v) => v + 1);
        };

        const handleTargetDocsChange = (docs: number) => {
            const boundedDocs = Math.max(MIN_TOTAL_DOCS, Math.min(MAX_TOTAL_DOCS, docs));
            setTargetDocs(boundedDocs);
            setTargetNotes((prev) => Math.min(prev, Math.max(MIN_TOTAL_NOTES, boundedDocs)));
            setTargetLinks((prev) => Math.min(prev, Math.max(MIN_TOTAL_LINKS, boundedDocs)));
        };

        const handleTargetNotesChange = (value: number) => {
            const notesUpperBound = Math.max(MIN_TOTAL_NOTES, Math.min(MAX_TOTAL_NOTES, targetDocs));
            const bounded = Math.max(MIN_TOTAL_NOTES, Math.min(notesUpperBound, value));
            setTargetNotes(bounded);
        };

        const handleTargetLinksChange = (value: number) => {
            const linksUpperBound = Math.max(MIN_TOTAL_LINKS, Math.min(MAX_TOTAL_LINKS, targetDocs));
            const bounded = Math.max(MIN_TOTAL_LINKS, Math.min(linksUpperBound, value));
            setTargetLinks(bounded);
        };

        return (
            <Stack gap={12} style={{ height: '100vh', width: '100vw', position: 'relative' }} ai='stretch'>
                <Timeline />
                <PerfOverlay
                    stats={perfStats}
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
                />
            </Stack>
        );
    }
}
