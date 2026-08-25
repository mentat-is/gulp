import { Application } from "@/context/Application.context";
import {
	DataStore,
	type DataStoreRenderChange,
} from "@/store/DataStore";
import { requestStore } from "@/store/request.store";
import { scrollStore } from "@/store/scroll.store";
import { useTimestampTimeZone } from "@/store/timestampTimeZone.store";
import {
	getLimits,
	getTimestamp,
	stringToHexColor,
	throwableByTimestamp,
} from "@/ui/utils";
import {
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import s from "./styles/Canvas.module.css";
import {
	useMagnifier,
	type MagnifierFrameRequest,
} from "@/dto/useMagnifier";
import { Magnifier } from "@/ui/Magnifier";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { RenderEngine } from "@/class/RenderEngine";
import {
	CanvasIcon,
	createCanvasIconBitmap,
	createCanvasIconPlaceholderBitmap,
} from "@/ui/CanvasIcon";
import {
	DisplayGroupDialog,
	type DisplayGroupKind,
} from "@/dialogs/Group.dialog";
import { Logger, LoggerHandler } from "@/dto/Logger.class";
import { useDrugs } from "@/decorator/use";
import { ContextMenu, ContextMenuTrigger } from "@/ui/ContextMenu";
import { TargetMenu } from "./Target.menu";
import { cn } from "@/ui/utils";
import { Pointers } from "@/components/Pointers";
import {
	PointerPositionStore,
	projectLocalPointerTimestamp,
} from "@/components/PointerPosition.store";
import { XY } from "@/dto/XY.dto";
import { Highlights } from "@/overlays/Highlights";
import { Stack } from "@/ui/Stack";
import { Spinner } from "@/ui/Spinner";
import { Source } from "@/entities/Source";
import { Note } from "@/entities/Note";
import { Doc } from "@/entities/Doc";
import { Operation } from "@/entities/Operation";
import { Link } from "@/entities/Link";
import { App } from "@/entities/App";
import { useTheme } from "next-themes";
import { Button } from "@/ui/Button";
import { Color } from "@/entities/Color";
import { Locale } from "@/locales";
import { CanvasProfiler } from "./CanvasProfiler";
import { Context } from "@/entities/Context";
import { Request } from "@/entities/Request";
import { Glyph } from "@/entities/Glyph";
import { Default } from "@/dto/Dataset";
import { Icon } from "@/ui/Icon";
import {
	createRenderAssetIconKey,
	RenderProjectionAdapter,
	resolveRenderAssetIconName,
	type RenderSourceProjectionInput,
} from "@/render-worker/RenderProjection.adapter";
import { getWorkerGraphSampleSignature } from "@/render-worker/graphSampleSignature";
import {
	RenderProjectionScheduler,
	type RenderProjectionJobContext,
} from "@/render-worker/RenderProjection.scheduler";
import {
	MAX_RENDER_DATA_QUEUE_DEPTH,
	RenderWorkerClient,
	type RenderMagnifierCropRequest,
} from "@/render-worker/RenderWorker.client";
import {
	DirtyCanvas,
	type RenderDataPatch,
	type RenderLoadingKind,
	type RenderOverlaySnapshot,
	type RenderResyncRequiredMessage,
	type RenderSceneState,
	type RenderViewportState,
	type RenderWorkerFontSource,
} from "@/render-worker/RenderWorker.types";
import { getCanvasViewportSourceIds } from "./CanvasViewport";
import {
	CanvasInputCoordinator,
	CanvasScaleCommitGuard,
	type CanvasInputBatch,
} from "./CanvasInputCoordinator";
import {
	getCanvasRendererPreference,
	reloadWithLegacyEventPipeline,
	type CanvasRendererBackend,
	type CanvasRendererPreference,
	type RendererBackendStatus,
} from "./CanvasRendererBackend";
import {
	applyTimelineWheelViewport,
	shouldInvalidateCanvasGeometry,
} from "./CanvasWheelViewport";
import { shouldStreamWorkerSourceSnapshot } from "./WorkerSourceSnapshotPolicy";
import { renderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import { SmartSocket } from "@/class/SmartSocket";
import type { QueryRenderWindowDescriptor } from "@/event-stream";
import {
	getCanvasEventTimestampRange,
	isCanvasEventLookupSourceCurrent,
	isCanvasPixelWithinEventSourceRange,
	shouldPreferCanvasEventWhileMagnifying,
	type CanvasEventLookupGeometry,
} from "./CanvasEventLookup";
import {
	CANVAS_RENDER_WINDOW_RETRY_DELAY_MS,
	isCanvasRenderWindowRequestCurrent,
	isCanvasRenderWindowSourceCountMismatch,
	isCanvasRenderWindowSourceStale,
	shouldRetryCanvasRenderWindow,
	type CanvasRenderWindowRequestFence,
} from "./CanvasRenderWindowRetry";
import { CanvasTerminalRenderConvergence } from "./CanvasTerminalRenderConvergence";
import {
	EVENT_MAIN_CACHE_SIZE,
	EVENT_PIXEL_PAGE_SIZE,
	EVENT_RENDER_PROJECTION_LIMIT,
	StaleEventStoreCursorError,
	isEventStoreLodEnabled,
	type EventPageCursor,
	type EventPixelPage,
	type EventTimestampRange,
} from "@/event-store";

export namespace Canvas {
	export interface Props extends Stack.Props {
		timeline: React.RefObject<HTMLDivElement>;
		/** Sizes the render surface from the nearest stable CSS size container. */
		stableWidth?: boolean;
	}
}

/** Atomic viewport values captured while the DOM wrapper is being resized. */
interface WorkerViewportStateOverride {
	width: number;
	height: number;
	scale: number;
	scrollX: number;
}

const WORKER_EVENT_SNAPSHOT_CHUNK_SIZE = 131_072;
/** Incremental jobs retained before sustained source backlog becomes one snapshot. */
const WORKER_SOURCE_DELTA_BACKLOG_LIMIT = 4;
const RENDER_WORKER_FONTS: RenderWorkerFontSource[] = [
	{
		family: "GeistMono",
		url: new URL("../../font/GeistMono.ttf", import.meta.url).toString(),
	},
	{
		family: "Geist",
		url: new URL("../../font/GeistVF.ttf", import.meta.url).toString(),
	},
];

/**
 * Builds the stable row-membership signature used to detect visible source changes.
 * @param sources Ordered sources currently rendered as timeline rows.
 * @returns Pipe-delimited source identity signature.
 */
function getVisibleSourceSignature(sources: readonly Source.Type[]): string {
	return sources.map((source) => source.id).join("|");
}

/**
 * Converts a source request marker into the worker's compact loading decoration.
 * @param app Current canonical application state.
 * @param source Source whose request state is resolved.
 * @returns Worker loading kind.
 */
function getWorkerLoadingKind(
	app: App.Type,
	source: Source.Type,
): RenderLoadingKind {
	const requestType = Source.Entity.getRequestType(app, source);
	if (!requestType) return "none";
	return requestType === Request.Prefix.INGESTION ? "ingestion" : "loading";
}

/**
 * Resolves a namespaced render asset key to an icon registered on the main thread.
 * @param assetKey Worker-requested icon key.
 * @returns Registered icon name, or the event placeholder for malformed keys.
 */
function getWorkerAssetIconName(assetKey: string): Icon.Name {
	const iconName = resolveRenderAssetIconName(assetKey);
	if (
		iconName &&
		(Object.prototype.hasOwnProperty.call(Icon.icons, iconName) ||
			Glyph.Images.has(iconName as Icon.Name))
	) {
		return iconName as Icon.Name;
	}
	return Default.Icon.EVENT;
}

/**
 * Resolves icon colors to values that can be rasterized in detached SVG blobs.
 * @param color Worker-requested canvas icon color.
 * @returns CSS color safe for SVG-to-bitmap creation.
 */
function resolveWorkerAssetIconColor(color: string): string {
	const normalizedColor = color.trim();
	if (!normalizedColor || normalizedColor.includes("var(")) {
		return Color.Themer.theme.FONT_ACCENT;
	}
	if (
		typeof CSS !== "undefined" &&
		!CSS.supports("color", normalizedColor)
	) {
		return Color.Themer.theme.FONT_ACCENT;
	}
	return normalizedColor;
}

/**
 * Maps a render trigger to the worker-local dirty-mask reasons it affects.
 * @param reason Human-readable main-thread render trigger.
 * @param force Whether renderer caches must be invalidated unconditionally.
 * @returns Worker dirty-mask bits to coalesce with the latest frame state.
 */
function getWorkerDirtyMask(reason: string, force: boolean): DirtyCanvas {
	let dirtyMask = DirtyCanvas.Viewport;
	if (reason.includes("resize")) dirtyMask |= DirtyCanvas.Size;
	if (reason.includes("source") || reason.includes("scene")) {
		dirtyMask |= DirtyCanvas.Sources;
	}
	if (reason.includes("theme") || reason.includes("locale")) {
		dirtyMask |= DirtyCanvas.Theme;
	}
	if (reason.includes("hover")) dirtyMask |= DirtyCanvas.Hover;
	if (reason.includes("data")) dirtyMask |= DirtyCanvas.Events;
	if (force) dirtyMask |= DirtyCanvas.Force;
	return dirtyMask;
}

/**
 * Resolves the events opened by an interactive Canvas link badge.
 * @param link Link represented by the clicked badge.
 * @returns Connected events, or the root event for single-document links.
 */
async function getCanvasLinkDialogEvents(link: Link.Type): Promise<Doc.Type[]> {
	return DataStore.Events.getByIds(getUniqueCanvasLinkDocIds(link));
}

/**
 * Returns unique document IDs for the link dialog, including the root event.
 * @param link Link whose root and connected event IDs should be normalized.
 * @returns Unique document IDs in stored link order.
 */
function getUniqueCanvasLinkDocIds(link: Link.Type): Doc.Id[] {
	const uniqueDocIds: Doc.Id[] = [];
	const seenDocIds = new Set<Doc.Id>();

	for (const docId of [link.doc_id_from, ...link.doc_ids]) {
		if (seenDocIds.has(docId)) continue;
		seenDocIds.add(docId);
		uniqueDocIds.push(docId);
	}

	return uniqueDocIds;
}

interface CanvasEventStoreGroup {
	sourceId: Source.Id;
	range: EventTimestampRange;
	initialPage: EventPixelPage;
	operationEpoch: number;
}

export function Canvas({ timeline, stableWidth = false }: Canvas.Props) {
	const { theme } = useTheme();
	const { t, language } = Locale.use();
	const timestampTimeZone = useTimestampTimeZone();
	const canvas_ref = useRef<HTMLCanvasElement>(
		null as unknown as HTMLCanvasElement,
	);
	const overlay_ref = useRef<HTMLCanvasElement>(
		null as unknown as HTMLCanvasElement,
	);
	const wrapper_ref = useRef<HTMLDivElement>(null as unknown as HTMLDivElement);
	const render_surface_ref = useRef<HTMLDivElement>(
		null as unknown as HTMLDivElement,
	);
	const scrollbar = useRef<HTMLDivElement>(null);
	const isManualScroll = useRef(false);
	const isProgramScroll = useRef(false);
	const { app, banner, spawnDialog, Info, highlightsOverlay } =
		Application.use();
	const scrollX = scrollStore.getX();
	const scrollY = scrollStore.getY();
	const [rendererBackend, setRendererBackend] =
		useState<CanvasRendererBackend>("initializing");
	const [canvasElementKey, setCanvasElementKey] = useState<number>(0);
	const [, setFlagRenderRevision] = useState<number>(0);
	const [target, setTarget] = useState<Source.Type | null>(null);
	const [groupDialogEvents, setGroupDialogEvents] = useState<Doc.Type[]>([]);
	const [groupDialogEventStore, setGroupDialogEventStore] =
		useState<CanvasEventStoreGroup | null>(null);
	const [groupDialogAnchor, setGroupDialogAnchor] = useState<{
		x: number;
		y: number;
	} | null>(null);
	const [groupDialogKind, setGroupDialogKind] =
		useState<DisplayGroupKind>("events");
	const rendererBackendRef = useRef<CanvasRendererBackend>("initializing");
	const requestedRendererBackendRef = useRef<CanvasRendererPreference>("auto");
	const rendererFallbackReasonRef = useRef<string | null>(null);
	const forcedLegacyBackendRef = useRef<boolean>(false);
	const workerClientRef = useRef<RenderWorkerClient | null>(null);
	const workerViewportRevisionRef = useRef<number>(0);
	const workerLodProjectionRevisionRef = useRef<number>(0);
	const workerLodRetryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(
		null,
	);
	const workerSourcePatchRevisionRef = useRef<number>(0);
	const workerNotesProjectionRevisionRef = useRef<number>(0);
	const workerLinksProjectionRevisionRef = useRef<number>(0);
	const workerHighlightsProjectionRevisionRef = useRef<number>(0);
	const workerFlagsRevisionRef = useRef<number>(0);
	const workerFlagDocumentsRef = useRef<Doc.Type[]>([]);
	const workerFlagLookupSequenceRef = useRef<number>(0);
	const workerUnresolvedNoteDocumentIdsRef = useRef<Set<Doc.Id>>(new Set());
	const workerUnresolvedLinkDocumentIdsRef = useRef<Set<Doc.Id>>(new Set());
	const workerOverlayLookupSignatureRef = useRef<string>("");
	const workerOverlayLookupSequenceRef = useRef<number>(0);
	const workerThemeRevisionRef = useRef<number>(1);
	const workerAssetRevisionRef = useRef<number>(1);
	const workerKnownSourceIdsRef = useRef<Set<Source.Id>>(new Set());
	const workerGraphSignaturesRef = useRef<Map<string, string>>(new Map());
	const workerSourceMetadataSignaturesRef = useRef<Map<string, string>>(
		new Map(),
	);
	const workerSourceMetadataRevisionsRef = useRef<Map<string, number>>(
		new Map(),
	);
	const workerSourceMembershipRevisionsRef = useRef<Map<Source.Id, number>>(
		new Map(),
	);
	const workerOverrideSignaturesRef = useRef<Map<Source.Id, string>>(new Map());
	const workerOverrideRevisionsRef = useRef<Map<Source.Id, number>>(new Map());
	const workerProjectionGenerationRef = useRef<number>(0);
	const workerSnapshotIdRef = useRef<number>(0);
	const workerProjectionScheduler = useMemo(
		() => new RenderProjectionScheduler(),
		[],
	);
	const workerTerminalRenderConvergence = useMemo(
		() => new CanvasTerminalRenderConvergence(),
		[],
	);
	const workerCanvasSizeRef = useRef<{ width: number; height: number } | null>(
		null,
	);
	const workerViewportOverrideRef = useRef<WorkerViewportStateOverride | null>(
		null,
	);
	const workerSceneSignatureRef = useRef<string>("");
	const workerViewportSignatureRef = useRef<string>("");
	const [scaleCommitGuard] = useState(
		() => new CanvasScaleCommitGuard(app.timeline.scale),
	);
	const persistTimelineScaleRef = useRef<(gestureToken: number) => void>(
		() => undefined,
	);
	const flushCanvasInputRef = useRef<(batch: CanvasInputBatch) => void>(
		() => undefined,
	);
	const inputCoordinator = useMemo(
		() =>
			new CanvasInputCoordinator(
				(batch) => {
					flushCanvasInputRef.current(batch);
				},
				undefined,
				(gestureToken) => {
					persistTimelineScaleRef.current(gestureToken);
				},
			),
		[],
	);
	const syncWorkerSourcesRef = useRef<
		(client: RenderWorkerClient, streamMissingEvents: boolean) => void
	>(() => undefined);
	const requestEventStoreRenderWindowRef = useRef<() => void>(() => undefined);
	const scheduleTerminalRenderWatchdogRef = useRef<() => void>(() => undefined);
	const cancelEventStoreRenderWindowRetry = useCallback((): void => {
		if (workerLodRetryTimerRef.current === null) return;
		clearTimeout(workerLodRetryTimerRef.current);
		workerLodRetryTimerRef.current = null;
	}, []);
	const syncWorkerOverlaysRef = useRef<
		(client: RenderWorkerClient, includeFlags: boolean) => void
	>(() => undefined);
	const syncWorkerFlagsRef = useRef<(client: RenderWorkerClient) => void>(
		() => undefined,
	);
	const streamWorkerSourceSnapshotRef = useRef<
		(client: RenderWorkerClient, source: Source.Type) => void
	>(() => undefined);
	const handleWorkerRenderChangeRef = useRef<
		(change: DataStoreRenderChange) => void
	>(() => undefined);
	const handleWorkerResyncRef = useRef<
		(request: RenderResyncRequiredMessage["payload"]) => void
	>(() => undefined);
	const handleWorkerResizeRef = useRef<() => void>(() => undefined);
	const observedFlagSignatureRef = useRef<string>("");

	/**
	 * Publishes renderer selection changes to state, refs, and diagnostics together.
	 * @param status Requested backend, active backend, and optional fallback reason.
	 * @returns Nothing.
	 */
	const publishRendererBackendStatus = useCallback(
		(status: RendererBackendStatus): void => {
			requestedRendererBackendRef.current = status.requestedBackend;
			rendererBackendRef.current = status.activeBackend;
			rendererFallbackReasonRef.current = status.fallbackReason;
			setRendererBackend(status.activeBackend);
			CanvasProfiler.setRendererBackendStatus(status);
		},
		[],
	);

	/**
	 * Requests the current worker canvas crop for the DOM-owned magnifier surface.
	 * @param request Canvas-relative crop and destination dimensions.
	 * @returns Worker-produced bitmap, or null when the worker is unavailable.
	 */
	const requestWorkerMagnifierFrame = useCallback(
		(request: MagnifierFrameRequest): Promise<ImageBitmap | null> => {
			const client = workerClientRef.current;
			if (rendererBackendRef.current !== "worker" || !client) {
				return Promise.resolve(null);
			}

			const cropRequest: RenderMagnifierCropRequest = {
				x: request.x,
				y: request.y,
				cropWidth: request.sourceSize,
				cropHeight: request.sourceSize,
				outputWidth: request.destinationSize,
				outputHeight: request.destinationSize,
				viewportRevision: client.requestedViewportRevision,
			};
			return client.requestMagnifier(cropRequest);
		},
		[],
	);
	const { toggler, move, magnifier_ref, isAltPressed, mousePosition } =
		useMagnifier(
			canvas_ref,
			[
				app.target.files,
				scrollX,
				scrollY,
				app.timeline.frame,
				app.timeline.scale,
				app.timeline.target,
				app.timeline.filter,
				app.hidden,
				target,
			],
			100,
			2,
			rendererBackend === "worker"
				? requestWorkerMagnifierFrame
				: undefined,
		);
	/**
	 * Routes drag movement into the same rAF coordinator used by wheel and pointer input.
	 * @param deltaX Horizontal drag delta in CSS pixels.
	 * @param deltaY Vertical drag delta in CSS pixels.
	 * @param inputTimestamp Native input capture timestamp.
	 * @returns Nothing.
	 */
	const queueDragScroll = useCallback(
		(deltaX: number, deltaY: number, inputTimestamp: number): void => {
			inputCoordinator.queueScrollDelta(deltaX, deltaY, inputTimestamp);
		},
		[inputCoordinator],
	);
	const { resize, handleMouseDown, handleMouseMove, handleMouseUpOrLeave } =
		useDrugs(canvas_ref, queueDragScroll);
	const renderProjectionAdapter = useMemo(
		() =>
			new RenderProjectionAdapter({
				resolveDocument: (documentId) =>
					DataStore.Events.peekDocument(documentId) ??
					Doc.Entity.id(Info.app, documentId),
				resolveNoteIconKey: (note) =>
					createRenderAssetIconKey(
						Glyph.List.get(note.glyph_id) ?? Default.Icon.NOTE,
					),
				resolveLinkIconKey: (link) =>
					createRenderAssetIconKey(
						Glyph.List.get(link.glyph_id) ?? Default.Icon.LINK,
					),
			}),
		[Info],
	);
	const pointerPositionStore = useMemo(() => new PointerPositionStore(), []);
	const operation = Operation.Entity.selected(app);
	const flaggedEvents = Doc.Entity.flag.getList(operation?.id);
	const flaggedEventsSignature = Array.from(flaggedEvents).sort().join(":");
	observedFlagSignatureRef.current = flaggedEventsSignature;
	const pendingRenderReasonsRef = useRef<Set<string>>(new Set());
	const pendingRenderForceRef = useRef<boolean>(false);
	const didRenderInitialFrameRef = useRef<boolean>(false);
	const scrollXRef = useRef(scrollX);
	const scrollYRef = useRef(scrollY);
	const queuedScrollYRef = useRef(scrollY);
	const scaleRef = useRef(app.timeline.scale);
	const mouseXRef = useRef<number>(-1000);
	const mouseYRef = useRef<number>(-1000);
	const hoveredItemRef = useRef<string | null>(null);
	const wrapperRectRef = useRef<DOMRect | null>(null);
	const wrapperLineHeightRef = useRef<number>(16);
	const initialVisibleSources = Source.Entity.rendered(app);
	const visibleSourceSignatureRef = useRef<string>(
		getVisibleSourceSignature(initialVisibleSources),
	);
	const [visibleSourceRowCount, setVisibleSourceRowCount] = useState<number>(
		() => initialVisibleSources.length,
	);

	/**
	 * Reconciles source-row membership without subscribing Canvas to every document
	 * batch as a React render.
	 * @param currentApp Application state used to resolve selected timeline sources.
	 * @returns True when the visible source membership changed.
	 */
	const reconcileVisibleSourceMembership = useCallback(
		(currentApp: App.Type): boolean => {
			const visibleSources = Source.Entity.rendered(currentApp);
			const signature = getVisibleSourceSignature(visibleSources);
			if (signature === visibleSourceSignatureRef.current) {
				return false;
			}
			visibleSourceSignatureRef.current = signature;
			setVisibleSourceRowCount(visibleSources.length);
			return true;
		},
		[],
	);
	/**
	 * Persists the final transient wheel scale without replacing Application context.
	 * @param gestureToken Coordinator-issued identity for the settled wheel gesture.
	 * @returns Nothing.
	 */
	persistTimelineScaleRef.current = (gestureToken: number): void => {
		if (Info.app.timeline.scale === scaleRef.current) return;
		const scaleToPersist = scaleCommitGuard.prepareCommit(
			gestureToken,
			scaleRef.current,
			Info.app.timeline.scale,
		);
		if (scaleToPersist !== null) {
			const committedScale = Info.persistSettledTimelineScale(scaleToPersist);
			const reconciliation = scaleCommitGuard.reconcileCanonicalScale(
				committedScale,
			);
			if (reconciliation === "external") {
				inputCoordinator.cancelWheelGesture();
			}
		}
	};

	useLayoutEffect(() => {
		const reconciliation = scaleCommitGuard.reconcileCanonicalScale(
			app.timeline.scale,
		);
		if (reconciliation === "unchanged") return;
		scaleRef.current = app.timeline.scale;
		if (reconciliation === "external") {
			inputCoordinator.cancelWheelGesture();
		}
	}, [app.timeline.scale, inputCoordinator, scaleCommitGuard]);

	useEffect(() => {
		/**
		 * Cancels transient zoom immediately for explicit commands, including no-op resets.
		 * @param commandedScale Clamped scale requested by the external caller.
		 * @param origin Optional identity supplied by the command source.
		 * @returns Nothing.
		 */
		const reconcileTimelineScaleCommand = (
			commandedScale: number,
		): void => {
			scaleCommitGuard.cancelGesture();
			inputCoordinator.cancelWheelGesture();
			scaleRef.current = commandedScale;
			inputCoordinator.invalidate(DirtyCanvas.Viewport);
		};

		return Info.subscribeTimelineScaleCommands(reconcileTimelineScaleCommand);
	}, [Info, inputCoordinator, scaleCommitGuard]);

	useEffect(() => {
		/**
		 * Bridges external scroll-store mutations into refs and the Canvas coordinator.
		 * @returns Nothing.
		 */
		const synchronizeExternalScroll = (): void => {
			const nextScrollX = scrollStore.getX();
			const nextScrollY = scrollStore.getY();
			if (
				nextScrollX === scrollXRef.current &&
				nextScrollY === scrollYRef.current
			) {
				return;
			}
			scrollXRef.current = nextScrollX;
			scrollYRef.current = nextScrollY;
			queuedScrollYRef.current = nextScrollY;
			if (scrollbar.current) {
				isProgramScroll.current = true;
				scrollbar.current.scroll({
					behavior: "instant",
					top: nextScrollY + (wrapperRectRef.current?.height ?? 0),
				});
			}
			inputCoordinator.invalidate(DirtyCanvas.Viewport);
		};

		return scrollStore.subscribe(synchronizeExternalScroll);
	}, [inputCoordinator]);

	/**
	 * LAZY INDEXING: Instead of eagerly rebuilding the note-to-source index here
	 * (which ran O(n*m) on every WebSocket note update), we only invalidate.
	 * The actual rebuild happens lazily on first access in renderCanvas() via
	 * Note.Entity.ensureIndexing(). This eliminates a cascade where this effect
	 * and the render effect both triggered on the same `app.target.notes` change.
	 */
	useEffect(() => {
		Note.Entity.invalidateCache();
		if (rendererBackendRef.current === "legacy") {
			RenderEngine.reset("notes");
			RenderEngine.reset("flags");
		}
	}, [app.target.notes, app.target.files, rendererBackend]);

	/**
	 * Invalidates legacy event caches when the canonical datastore changes while fallback is active.
	 * @param change Revisioned render mutation published by the canonical datastore.
	 * @returns Nothing.
	 */
	const handleLegacyRenderChange = useCallback(
		(change: DataStoreRenderChange): void => {
			if (rendererBackendRef.current !== "legacy") return;
			if (change.type === "operation-reset") {
				RenderEngine.clearAllCaches();
				return;
			}
			if (change.type === "source-events") {
				RenderEngine.resetSource(change.sourceId);
			}
		},
		[],
	);

	useEffect(() => {
		return DataStore.subscribeRenderChanges(handleLegacyRenderChange);
	}, [handleLegacyRenderChange]);

	/**
	 * Renders the current Canvas frame and reports the Canvas-rendered source set.
	 * @param force Whether to force cache invalidation for overlay-related render data.
	 * @param ctx Optional canvas rendering context, defaulting to the main canvas.
	 * @param reason Profiler label describing the repaint trigger.
	 * @returns Nothing.
	 */
	const renderCanvas = (
		force = false,
		ctx?: CanvasRenderingContext2D | null,
		reason = "direct",
	) => {
		if (rendererBackendRef.current !== "legacy") {
			return;
		}
		ctx ??= canvas_ref.current?.getContext("2d");
		if (!render_surface_ref.current || !ctx || !canvas_ref.current) {
			return;
		}

		const app = Info.app; // Capture current state for this frame

		const oldWidth = canvas_ref.current.width;
		const oldHeight = canvas_ref.current.height;
		const newWidth = render_surface_ref.current.clientWidth;
		const newHeight = render_surface_ref.current.clientHeight;

		if (oldWidth !== newWidth || oldHeight !== newHeight) {
			canvas_ref.current.width = newWidth;
			canvas_ref.current.height = newHeight;

			if (oldWidth !== 0 && oldWidth !== newWidth) {
				const delta = oldWidth / newWidth;
				Info.setTimelineScale(app.timeline.scale * delta);
				scrollStore.setScrollX((s) => s - newWidth + oldWidth);
			}
			DataStore.markDirtySoon();
			return;
		}

		const profiler = CanvasProfiler.start(reason);
		ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
		profiler?.mark("clear");

		const currentScrollX = scrollXRef.current;
		const currentScrollY = scrollYRef.current;
		const limits = getLimits(app, Info, render_surface_ref, currentScrollX);
		const selectedSources = Source.Entity.rendered(app);

		// Create or reuse the singleton RenderEngine with current frame parameters.
		// The constructor uses the singleton pattern: if an instance exists, it updates
		// its properties and reuses existing sub-engine instances (via updateRenderer())
		// instead of allocating new ones every frame.
		const render = new RenderEngine({
			ctx,
			limits,
			info: Info,
			getPixelPosition,
			scrollX: currentScrollX,
			scrollY: currentScrollY,
			mouseX: mouseXRef.current,
			mouseY: mouseYRef.current,
			visibleSources: selectedSources,
		});
		profiler?.mark("visibleSources");

		const files = render.visibleSources;

		render.ruler.draw();
		profiler?.mark("ruler");

		// Y-AXIS VIEWPORT CULLING: Each source row is 48px tall. With more sources
		// and vertical scrolling, most rows are off-screen. By checking the Y position
		// against the canvas bounds (with 48px buffer for partial visibility), we skip
		// engine rendering, line drawing, local markers, and info labels for invisible rows.
		const canvasHeight = ctx.canvas.height;
		const viewportSources: Source.Type[] = [];

		files.forEach((file, i) => {
			const y = Source.Entity.getHeight(app, file, currentScrollY, i);
			if (y < -48 || y > canvasHeight + 48) return;
			viewportSources.push(file);

			if (
				!throwableByTimestamp(file.timestamp, limits, app, file.settings.offset)
			) {
				render[file.settings.render_engine].render(file, y - 24, force);
			}

			if (!i) render.primary(file, y);

			render.lines(file, y);
			render.target(file, y);
			render.locals(file, y);
			render.draw_info(file, y);
		});
		Info.setViewportSourceIds(viewportSources.map((file) => file.id));
		render.stats.renderedRows = viewportSources.length;
		profiler?.mark("rows");

		render.targetMarker();
		render.drawHighlights();

		if (force) {
			RenderEngine.reset("notes");
			RenderEngine.reset("flags");
		}

		render.highlightFlaggedDocuments();
		profiler?.mark("highlightsFlags");

		if (!app.hidden.notes) {
			// Lazy rebuild: only runs if cache was invalidated since last render
			Note.Entity.ensureIndexing(app);
			render.drawNotes(viewportSources);
		} else {
			render.drawNotes([]);
		}
		profiler?.mark("notes");

		if (!app.hidden.links) {
			render.drawLinks();
		}
		profiler?.mark("links");

		ctx.fillStyle = Color.Themer.theme.BORDER;
		ctx.fillRect(
			getPixelPosition(app.timeline.frame.min || app.timeline.frame?.min) - 2,
			0,
			3,
			render_surface_ref.current?.clientHeight || 0,
		);
		ctx.fillRect(
			getPixelPosition(app.timeline.frame.max || app.timeline.frame?.max) + 2,
			0,
			3,
			render_surface_ref.current?.clientHeight || 0,
		);

		render.ruler.sections();
		profiler?.mark("ruler");
		profiler?.setCounts(render.stats);
		profiler?.finish();
	};

	const renderCanvasRef = useRef(renderCanvas);
	renderCanvasRef.current = renderCanvas;

	/**
	 * Adds a render invalidation to the shared Canvas input display tick.
	 * @param reason Human-readable reason used by the dev profiler.
	 * @param force Whether the scheduled render should invalidate overlay-related caches.
	 * @returns Nothing.
	 */
	const requestCanvasRender = useMemo(
		() =>
			(reason: string, force = false): void => {
				if (rendererBackendRef.current === "initializing") return;
				pendingRenderForceRef.current = pendingRenderForceRef.current || force;
				pendingRenderReasonsRef.current.add(reason);
				inputCoordinator.invalidate(getWorkerDirtyMask(reason, force));
			},
		[inputCoordinator],
	);

	/**
	 * Draws the main-thread drag/range overlay using DOM wrapper dimensions.
	 * @returns Nothing.
	 */
	const renderOverlay = (): void => {
		if (!overlay_ref.current || !wrapper_ref.current) return;
		const overlayCtx = overlay_ref.current.getContext("2d");
		if (!overlayCtx) return;

		overlay_ref.current.height = wrapper_ref.current.clientHeight;
		overlay_ref.current.width = wrapper_ref.current.clientWidth;

		overlayCtx.clearRect(
			0,
			0,
			overlay_ref.current.width,
			overlay_ref.current.height,
		);

		overlayCtx.fillStyle = Color.Themer.theme.BORDER;
		const start = Math.round(resize.start);
		const end = Math.round(resize.end);

		overlayCtx.fillRect(start, 0, end - start, overlay_ref.current.height);
	};

	useEffect(() => {
		renderOverlay();
	}, [overlay_ref, canvas_ref, resize, theme]);

	/**
	 * Returns the CSS-pixel width shared by Canvas transforms and exact lookups.
	 * @returns Positive viewport width.
	 */
	const getEventLookupCanvasWidth = (): number =>
		Math.max(
			1,
			workerCanvasSizeRef.current?.width ??
				render_surface_ref.current?.clientWidth ??
				canvas_ref.current?.clientWidth ??
				1,
		);

	/**
	 * Captures the immutable transform used by one IndexedDB pixel lookup.
	 * @param file Source whose offset participates in the transform.
	 * @returns Geometry matching the renderer's rounded timestamp projection.
	 */
	const captureEventLookupGeometry = (
		file: Source.Type,
	): CanvasEventLookupGeometry => ({
		sourceId: file.id,
		frameMin: Info.app.timeline.frame.min,
		frameMax: Info.app.timeline.frame.max,
		canvasWidth: getEventLookupCanvasWidth(),
		scale: scaleRef.current,
		scrollX: scrollXRef.current,
		sourceOffset: file.settings.offset,
		renderEngine: file.settings.render_engine,
	});

	/**
	 * Returns whether viewport and operation state still match a pending click read.
	 * @param geometry Geometry captured before IndexedDB access.
	 * @param file Source whose offset must remain stable.
	 * @param operationEpoch Operation boundary captured with the click.
	 * @param viewportRevision Worker viewport revision captured with the click.
	 * @returns True only while the read can still address the clicked pixel.
	 */
	const isEventLookupCurrent = (
		geometry: CanvasEventLookupGeometry,
		file: Source.Type,
		operationEpoch: number,
		viewportRevision: number,
	): boolean => {
		const currentFile = Source.Entity.id(Info.app, file.id);
		return (
			DataStore.getRenderOperationEpoch() === operationEpoch &&
			(rendererBackendRef.current !== "worker" ||
				workerViewportRevisionRef.current === viewportRevision) &&
			Info.app.timeline.frame.min === geometry.frameMin &&
			Info.app.timeline.frame.max === geometry.frameMax &&
			getEventLookupCanvasWidth() === geometry.canvasWidth &&
			scaleRef.current === geometry.scale &&
			scrollXRef.current === geometry.scrollX &&
			isCanvasEventLookupSourceCurrent(
				geometry,
				currentFile
					? {
							sourceId: currentFile.id,
							sourceOffset: currentFile.settings.offset,
							renderEngine: currentFile.settings.render_engine,
						}
					: null,
			)
		);
	};

	/**
	 * Reads one exact Canvas hit area without materializing its source in RAM.
	 * @param x Rounded Canvas-relative hit pixel.
	 * @param file Source row selected by the click.
	 * @param geometry Immutable transform captured for this click.
	 * @param operationEpoch Operation boundary captured for this click.
	 * @param viewportRevision Worker viewport revision captured for this click.
	 * @returns Exact first page and timestamp range, null for an empty pixel, or undefined when stale.
	 */
	const queryEventPageByClickX = async (
		x: number,
		file: Source.Type,
		geometry: CanvasEventLookupGeometry,
		operationEpoch: number,
		viewportRevision: number,
	): Promise<
		{ page: EventPixelPage; range: EventTimestampRange } | null | undefined
	> => {
		const range = getCanvasEventTimestampRange(
			x,
			geometry,
			geometry.renderEngine === "graph" ? 16 : 0,
		);
		if (!range) return null;

		let page = await DataStore.Events.queryPixelBucket(
			file.id,
			range,
			EVENT_PIXEL_PAGE_SIZE,
		);
		if (!isEventLookupCurrent(geometry, file, operationEpoch, viewportRevision)) {
			return undefined;
		}

		const currentSourceRevision = DataStore.getSourceRevision(file.id);
		if (
			currentSourceRevision > 0 &&
			page.sourceRevision !== currentSourceRevision
		) {
			page = await DataStore.Events.queryPixelBucket(
				file.id,
				range,
				EVENT_PIXEL_PAGE_SIZE,
			);
			if (
				!isEventLookupCurrent(
					geometry,
					file,
					operationEpoch,
					viewportRevision,
				) ||
				(DataStore.getSourceRevision(file.id) > 0 &&
					page.sourceRevision !== DataStore.getSourceRevision(file.id))
			) {
				return undefined;
			}
		}

		return page.total > 0 ? { page, range } : null;
	};

	/**
	 * Resolves Canvas overlay and exact-event clicks using magnifier intent.
	 * @param event Native mouse-up click forwarded by the drag interaction bridge.
	 * @returns Promise settled after an ordered worker hit test and dialog dispatch.
	 */
	const handleClick = async (event: MouseEvent): Promise<void> => {
		if (event.button === 2) {
			event.preventDefault();
			return;
		}

		if (!canvas_ref.current) {
			return;
		}

		const rect = wrapper_ref.current?.getBoundingClientRect();
		const canvasRect = canvas_ref.current.getBoundingClientRect();
		if (!rect) return;
		if (
			event.clientX < rect.left ||
			event.clientX > rect.right ||
			event.clientY < rect.top ||
			event.clientY > rect.bottom
		) {
			return;
		}

		const { top, left } = canvasRect;
		const canvasHitX = Math.round(event.clientX - left);
		const canvasHitY = Math.round(event.clientY - top);
		const interactionOperationEpoch = DataStore.getRenderOperationEpoch();
		const preferExactEvent = shouldPreferCanvasEventWhileMagnifying(isAltPressed);

		/**
		 * Resolves and opens the topmost note or link badge at the clicked pixel.
		 * @returns True when the click was handled or became stale, otherwise false.
		 */
		const resolveOverlayClick = async (): Promise<boolean> => {
			if (rendererBackendRef.current === "worker") {
				const client = workerClientRef.current;
				const hitTestViewportRevision = client?.requestedViewportRevision ?? 0;
				const workerHit = client
					? await client.hitTest(
							canvasHitX,
							canvasHitY,
							hitTestViewportRevision,
						)
					: null;
				if (
					DataStore.getRenderOperationEpoch() !== interactionOperationEpoch ||
					!client ||
					client.requestedViewportRevision !== hitTestViewportRevision ||
					client.lastViewportRevision !== hitTestViewportRevision
				) {
					return true;
				}
				if (workerHit?.kind === "link") {
					const link = DataStore.links.find((item) => item.id === workerHit.id);
					if (link) {
						const docs = await getCanvasLinkDialogEvents(link);
						if (DataStore.getRenderOperationEpoch() !== interactionOperationEpoch) {
							return true;
						}
						if (docs.length === 1) {
							spawnDialog(<DisplayEventDialog event={docs[0]} />);
							return true;
						}
						if (docs.length > 1) {
							setGroupDialogEventStore(null);
							setGroupDialogEvents(docs);
							setGroupDialogKind("links");
							setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
							return true;
						}
					}
				}
				if (workerHit?.kind === "note") {
					const noteDocumentIds = workerHit.noteIds
						.map((noteId) => DataStore.notes.find((note) => note.id === noteId))
						.filter((note): note is Note.Type => Boolean(note))
						.map((note) => note.doc._id);
					const noteDocuments = await DataStore.Events.getByIds(noteDocumentIds);
					if (DataStore.getRenderOperationEpoch() !== interactionOperationEpoch) {
						return true;
					}
					if (noteDocuments.length === 1) {
						spawnDialog(<DisplayEventDialog event={noteDocuments[0]} />);
						return true;
					}
					if (noteDocuments.length > 1) {
						setGroupDialogEventStore(null);
						setGroupDialogEvents(noteDocuments);
						setGroupDialogKind("notes");
						setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
						return true;
					}
				}
				return false;
			}
			if (rendererBackendRef.current !== "legacy") return false;

			for (const item of RenderEngine.interactiveLinks) {
				if (
					canvasHitX >= item.rect.x &&
					canvasHitX <= item.rect.x + item.rect.w &&
					canvasHitY >= item.rect.y &&
					canvasHitY <= item.rect.y + item.rect.h
				) {
					const docs = await getCanvasLinkDialogEvents(item.link);
					if (DataStore.getRenderOperationEpoch() !== interactionOperationEpoch) {
						return true;
					}
					if (docs.length === 1) {
						spawnDialog(<DisplayEventDialog event={docs[0]} />);
					} else if (docs.length > 1) {
						setGroupDialogEventStore(null);
						setGroupDialogEvents(docs);
						setGroupDialogKind("links");
						setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
					}
					return true;
				}
			}

			for (const item of RenderEngine.interactiveNotes) {
				if (
					canvasHitX >= item.rect.x &&
					canvasHitX <= item.rect.x + item.rect.w &&
					canvasHitY >= item.rect.y &&
					canvasHitY <= item.rect.y + item.rect.h
				) {
					const noteDocuments = await DataStore.Events.getByIds(
						item.notes.map((note) => note.doc._id),
					);
					if (DataStore.getRenderOperationEpoch() !== interactionOperationEpoch) {
						return true;
					}
					if (noteDocuments.length === 1) {
						spawnDialog(<DisplayEventDialog event={noteDocuments[0]} />);
						return true;
					}

					setGroupDialogEventStore(null);
					setGroupDialogEvents(noteDocuments);
					setGroupDialogKind("notes");
					setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
					return true;
				}
			}
			return false;
		};

		if (!preferExactEvent && (await resolveOverlayClick())) return;

		const click: XY = {
			x: canvasHitX,
			y: Math.round(event.clientY - top + scrollYRef.current),
		};
		const clickScrollY = scrollYRef.current;
		const index = Math.floor(click.y / 48);

		const files = Source.Entity.rendered(Info.app);
		const file = files[index];

		if (!file) {
			if (preferExactEvent) await resolveOverlayClick();
			return;
		}
		const sourceSummary = DataStore.getEventSourceSummary(file.id);
		if (sourceSummary?.eventCount === 0) {
			if (preferExactEvent) await resolveOverlayClick();
			return;
		}

		const lookupGeometry = captureEventLookupGeometry(file);
		if (
			!isCanvasPixelWithinEventSourceRange(
				click.x,
				sourceSummary?.minTimestamp ?? file.timestamp.min,
				sourceSummary?.maxTimestamp ?? file.timestamp.max,
				lookupGeometry,
				lookupGeometry.renderEngine === "graph" ? 16 : 0,
			)
		) {
			if (preferExactEvent) await resolveOverlayClick();
			return;
		}

		const lookupViewportRevision = workerViewportRevisionRef.current;
		let lookup = await queryEventPageByClickX(
			click.x,
			file,
			lookupGeometry,
			interactionOperationEpoch,
			lookupViewportRevision,
		);
		if (lookup === undefined) return;
		if (lookup === null) {
			lookup = await queryEventPageByClickX(
				click.x - 1,
				file,
				lookupGeometry,
				interactionOperationEpoch,
				lookupViewportRevision,
			);
		}
		if (lookup === undefined) return;
		if (lookup === null) {
			lookup = await queryEventPageByClickX(
				click.x + 1,
				file,
				lookupGeometry,
				interactionOperationEpoch,
				lookupViewportRevision,
			);
		}
		if (
			lookup === undefined ||
			scrollYRef.current !== clickScrollY ||
			Source.Entity.rendered(Info.app)[index]?.id !== file.id
		) {
			return;
		}

		const events = lookup?.page.events ?? [];
		LoggerHandler.canvasClick(file, events, click.x);

		if (lookup && lookup.page.total > 0) {
			if (lookup.page.total > 1) {
				setGroupDialogEvents([]);
				setGroupDialogEventStore({
					sourceId: file.id,
					range: lookup.range,
					initialPage: lookup.page,
					operationEpoch: interactionOperationEpoch,
				});
				setGroupDialogKind("events");
				setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
				return;
			}

			if (events[0]) {
				spawnDialog(<DisplayEventDialog event={events[0]} />);
				return;
			}
		}

		if (preferExactEvent && (await resolveOverlayClick())) return;
		Info.setTimelineTarget(null);
	};

	useEffect(() => {
		const activeWindow = window as Window & {
			xxc?: (click: MouseEvent) => Promise<void>;
		};
		activeWindow.xxc = handleClick;
		return () => {
			if (activeWindow.xxc === handleClick) delete activeWindow.xxc;
		};
	}, [handleClick]);

	/**
	 * Refreshes cached Canvas geometry outside high-frequency pointer and wheel loops.
	 * @returns Current wrapper rectangle, or null before the wrapper mounts.
	 */
	const refreshCanvasGeometry = useCallback((): DOMRect | null => {
		const wrapper = wrapper_ref.current;
		if (!wrapper) return null;
		const rect = wrapper.getBoundingClientRect();
		wrapperRectRef.current = rect;
		const parsedLineHeight = Number.parseFloat(getComputedStyle(wrapper).lineHeight);
		wrapperLineHeightRef.current = Number.isFinite(parsedLineHeight)
			? parsedLineHeight
			: 16;
		return rect;
	}, []);

	/**
	 * Invalidates cached wrapper position after ancestor scrolling or window layout changes.
	 * @returns Nothing.
	 */
	const invalidateCanvasGeometry = useCallback((): void => {
		wrapperRectRef.current = null;
	}, []);

	/**
	 * Invalidates geometry only when scrolling outside the Canvas wrapper can move it.
	 * @param event Capture-phase scroll event from the document or an ancestor.
	 * @returns Nothing.
	 */
	const handleViewportScroll = useCallback(
		(event: Event): void => {
			if (!shouldInvalidateCanvasGeometry(event.target, wrapper_ref.current)) return;
			invalidateCanvasGeometry();
		},
		[invalidateCanvasGeometry],
	);

	useEffect(() => {
		window.addEventListener("scroll", handleViewportScroll, {
			capture: true,
			passive: true,
		});
		window.addEventListener("resize", invalidateCanvasGeometry, { passive: true });
		return () => {
			window.removeEventListener("scroll", handleViewportScroll, true);
			window.removeEventListener("resize", invalidateCanvasGeometry);
		};
	}, [handleViewportScroll, invalidateCanvasGeometry]);

	// Reset cached geometry after application changes that can move the viewport.
	useEffect(() => {
		invalidateCanvasGeometry();
	}, [
		app.target.files,
		banner,
		invalidateCanvasGeometry,
	]);

	/**
	 * Queues a native wheel event for pixel normalization and one display-tick flush.
	 * @param event Passive native wheel event containing browser-specific delta units.
	 * @returns Nothing.
	 */
	const handleWheel = useCallback(
		(event: WheelEvent): void => {
			const wrapper = wrapper_ref.current;
			if (!wrapper || banner) return;
			const cachedPageHeight =
				wrapperRectRef.current?.height ?? workerCanvasSizeRef.current?.height;
			const pageHeight =
				cachedPageHeight ?? refreshCanvasGeometry()?.height ?? 1;
			workerProjectionScheduler.markInteraction();
			inputCoordinator.queueWheel(
				{
					deltaX: event.deltaX,
					deltaY: event.deltaY,
					deltaMode: event.deltaMode,
					clientX: event.clientX,
					clientY: event.clientY,
					inputTimestamp: performance.now(),
				},
				wrapperLineHeightRef.current,
				Math.max(1, pageHeight),
			);
		},
		[
			banner,
			inputCoordinator,
			refreshCanvasGeometry,
			workerProjectionScheduler,
		],
	);

	useEffect(() => {
		CanvasIcon.onIconLoad = () => DataStore.markDirtySoon();
		return () => {
			CanvasIcon.onIconLoad = null;
		};
	}, []);

	// Track the clipping viewport without invalidating the stable render surface.
	useEffect(() => {
		const el = wrapper_ref.current;
		if (!el) return;
		refreshCanvasGeometry();
		const observer = new ResizeObserver(() => {
			refreshCanvasGeometry();
		});
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, [refreshCanvasGeometry]);

	// Only real render-surface changes resize the backing canvas and renderer.
	useEffect(() => {
		const el = render_surface_ref.current;
		if (!el) return;
		const observer = new ResizeObserver(() => {
			if (rendererBackendRef.current === "worker") {
				handleWorkerResizeRef.current();
				return;
			}
			DataStore.markDirtySoon();
		});
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);

	useEffect(() => {
		if (theme) {
			Color.Themer.setTheme();
			workerThemeRevisionRef.current++;
			workerAssetRevisionRef.current++;
		}

		const unsubscribe =
			rendererBackendRef.current === "legacy"
				? DataStore.subscribe(() => {
						requestCanvasRender("data-store");
					})
				: () => undefined;

		if (theme) {
			if (rendererBackendRef.current === "worker") {
				requestCanvasRender("theme");
			} else {
				DataStore.markDirtySoon();
			}
		}

		return () => {
			unsubscribe();
		};
	}, [language, theme, rendererBackend, requestCanvasRender]);

	useEffect(() => {
		requestCanvasRender("locale-time-zone");
	}, [requestCanvasRender, timestampTimeZone]);

	useEffect(() => {
		return requestStore.subscribe(() => {
			const client = workerClientRef.current;
			if (rendererBackendRef.current === "worker" && client) {
				syncWorkerSourcesRef.current(client, false);
					inputCoordinator.invalidate(DirtyCanvas.Sources);
				return;
			}
			requestCanvasRender("request-store");
		});
	}, [inputCoordinator, requestCanvasRender]);

	useEffect(() => {
		/**
		 * Synchronizes document visibility with the active render worker.
		 * @returns Nothing.
		 */
		const handleVisibilityChange = (): void => {
			const client = workerClientRef.current;
			if (!client) return;
			const visible = document.visibilityState !== "hidden";
			client.setVisibility(visible);
			if (!visible) {
				workerTerminalRenderConvergence.cancelWatchdog();
				return;
			}
			inputCoordinator.invalidate(DirtyCanvas.Force);
			if (workerTerminalRenderConvergence.hasTargets()) {
				requestEventStoreRenderWindowRef.current();
			}
		};

		document.addEventListener("visibilitychange", handleVisibilityChange);
		return () => {
			document.removeEventListener("visibilitychange", handleVisibilityChange);
		};
	}, [inputCoordinator, workerTerminalRenderConvergence]);

	useEffect(() => {
		/**
		 * Synchronizes local-storage flag changes with both render backends.
		 * @returns Nothing.
		 */
		const synchronizeFlagState = (): void => {
			const nextSignature = Array.from(
				Doc.Entity.flag.getList(operation?.id),
			)
				.sort()
				.join(":");
			if (nextSignature === observedFlagSignatureRef.current) return;
			observedFlagSignatureRef.current = nextSignature;
			setFlagRenderRevision((revision) => revision + 1);
			const client = workerClientRef.current;
			if (rendererBackendRef.current === "worker" && client) {
				syncWorkerFlagsRef.current(client);
					inputCoordinator.invalidate(DirtyCanvas.Overlays);
				return;
			}
			requestCanvasRender("flags", true);
		};
		/**
		 * Defers inspection until the clicked control has completed its mutation.
		 * @returns Nothing.
		 */
		const handleDocumentClick = (): void => {
			queueMicrotask(synchronizeFlagState);
		};
		/**
		 * Receives flag updates made by another same-origin browsing context.
		 * @param event Storage mutation broadcast by another browsing context.
		 * @returns Nothing.
		 */
		const handleStorage = (event: StorageEvent): void => {
			if (event.key !== Doc.Entity.flag.KEY) return;
			Doc.Entity.flag.invalidateCache();
			synchronizeFlagState();
		};

		document.addEventListener("click", handleDocumentClick);
		window.addEventListener("storage", handleStorage);
		return () => {
			document.removeEventListener("click", handleDocumentClick);
			window.removeEventListener("storage", handleStorage);
		};
	}, [inputCoordinator, operation?.id, requestCanvasRender]);

	useEffect(() => {
		const canvas = wrapper_ref.current;

		/**
		 * Stores the newest native pointer position in the shared input coordinator.
		 * @param event Passive native mouse movement over the timeline wrapper.
		 * @returns Nothing.
		 */
		const handleNativePointer = (event: MouseEvent): void => {
			inputCoordinator.queuePointer({
				clientX: event.clientX,
				clientY: event.clientY,
				inside: true,
				inputTimestamp: performance.now(),
			});
		};

		/**
		 * Clears main-thread and worker hover state when the pointer leaves the canvas.
		 * @returns Nothing.
		 */
		const resetHover = (): void => {
			inputCoordinator.queuePointer({
				clientX: -1000,
				clientY: -1000,
				inside: false,
				inputTimestamp: performance.now(),
			});
		};
		if (canvas) {
			canvas.addEventListener("wheel", handleWheel, { passive: true });
			canvas.addEventListener("contextmenu", handleContextMenu, {
				passive: true,
			});
			canvas.addEventListener("mousemove", handleNativePointer, {
				passive: true,
			});
			canvas.addEventListener("mouseenter", refreshCanvasGeometry, {
				passive: true,
			});
			canvas.addEventListener("mouseleave", resetHover);
		}
		return () => {
			if (canvas) {
				canvas.removeEventListener("wheel", handleWheel);
				canvas.removeEventListener("contextmenu", handleContextMenu);
				canvas.removeEventListener("mousemove", handleNativePointer);
				canvas.removeEventListener("mouseenter", refreshCanvasGeometry);
				canvas.removeEventListener("mouseleave", resetHover);
			}
		};
	}, [
		handleWheel,
		inputCoordinator,
		refreshCanvasGeometry,
	]);

	useEffect(() => {
		if (
			rendererBackendRef.current === "legacy" &&
			!didRenderInitialFrameRef.current
		) {
			didRenderInitialFrameRef.current = true;
			requestCanvasRender("react:initial");
			return;
		}

		requestCanvasRender("react:viewport");
	}, [
		app.timeline.frame,
		app.timeline.scale,
		rendererBackend,
		requestCanvasRender,
	]);

	useEffect(() => {
		requestCanvasRender("scene:deps");
	}, [
		app.target.files,
		app.timeline.target,
		app.timeline.filter,
		app.timeline.renderVersion,
		app.hidden,
		target,
		theme,
		language,
		rendererBackend,
		requestCanvasRender,
	]);

	useEffect(() => {
		reconcileVisibleSourceMembership(Info.app);
		const client = workerClientRef.current;
		if (rendererBackendRef.current !== "worker" || !client) return;
		syncWorkerSourcesRef.current(client, true);
		inputCoordinator.invalidate(DirtyCanvas.Sources);
	}, [
		Info,
		rendererBackend,
		app.target.files,
		app.target.contexts,
		app.timeline.filter,
		app.timeline.renderVersion,
		app.hidden.sourcesWithNoEvents,
		theme,
		language,
		inputCoordinator,
		reconcileVisibleSourceMembership,
	]);

	useEffect(() => {
		/**
		 * Resynchronizes row membership after document changes without rerendering
		 * Canvas for document batches that do not change visible source rows.
		 * @returns Nothing.
		 */
		const handleDocumentStoreChange = (): void => {
			if (!Info.app.hidden.sourcesWithNoEvents) return;
			if (!reconcileVisibleSourceMembership(Info.app)) return;

			const client = workerClientRef.current;
			if (rendererBackendRef.current === "worker" && client) {
				syncWorkerSourcesRef.current(client, true);
				syncWorkerOverlaysRef.current(client, false);
				inputCoordinator.invalidate(DirtyCanvas.Sources | DirtyCanvas.Overlays);
			}
		};

		return DataStore.subscribeDocuments(handleDocumentStoreChange);
	}, [
		Info,
		inputCoordinator,
		reconcileVisibleSourceMembership,
	]);

	useEffect(() => {
		const client = workerClientRef.current;
		if (rendererBackendRef.current !== "worker" || !client) return;
		syncWorkerFlagsRef.current(client);
		inputCoordinator.invalidate(DirtyCanvas.Overlays);
	}, [inputCoordinator, rendererBackend, flaggedEventsSignature]);

	useEffect(() => {
		const client = workerClientRef.current;
		if (rendererBackendRef.current !== "worker" || !client) return;
		workerAssetRevisionRef.current++;
		syncWorkerOverlaysRef.current(client, false);
		inputCoordinator.invalidate(
			DirtyCanvas.Assets | DirtyCanvas.Overlays,
		);
	}, [inputCoordinator, rendererBackend, app.target.glyphs]);

	useEffect(() => {
		return () => {
			inputCoordinator.flushNow();
			inputCoordinator.flushWheelSettled();
			inputCoordinator.cancelPending();
		};
	}, [inputCoordinator]);

	useEffect(() => {
		return () => workerProjectionScheduler.dispose();
	}, [workerProjectionScheduler]);

	useEffect(() => {
		return () => workerTerminalRenderConvergence.reset();
	}, [workerTerminalRenderConvergence]);

	/**
	 * Resolves source identities occupying the current vertically visible Canvas rows.
	 * @returns Current viewport source identities as a stable lookup set.
	 */
	const getCurrentCanvasViewportSourceIds = useCallback((): Set<Source.Id> => {
		const selectedSources = Source.Entity.rendered(Info.app);
		const height = Math.max(
			1,
			workerCanvasSizeRef.current?.height ??
				wrapperRectRef.current?.height ??
				1,
		);
		return new Set(
			getCanvasViewportSourceIds(
				selectedSources,
				scrollYRef.current,
				height,
			),
		);
	}, [Info]);

	useLayoutEffect(() => {
		const unsubscribe = renderWindowProjectionBus.subscribe((projection) => {
			const client = workerClientRef.current;
			const visibleSourceIds = getCurrentCanvasViewportSourceIds();
			if (
				rendererBackendRef.current !== "worker" ||
				!client ||
				projection.operationEpoch !== DataStore.getRenderOperationEpoch() ||
				projection.viewportRevision !== workerViewportRevisionRef.current ||
				(workerLodProjectionRevisionRef.current > 0 &&
					projection.projectionRevision !==
						workerLodProjectionRevisionRef.current) ||
				projection.sources.some((source) => {
					const summary = DataStore.getEventSourceSummary(
						source.sourceId as Source.Id,
					);
					return (
						isCanvasRenderWindowSourceStale(
							source.sourceRevision,
							summary?.revision,
						) ||
						isCanvasRenderWindowSourceCountMismatch(
							source.sourceRevision,
							source.loadedCount,
							summary,
						)
					);
				}) ||
				!workerTerminalRenderConvergence.acceptsProjection(
					projection.operationEpoch,
					visibleSourceIds,
					projection.sources,
				)
			) {
				return false;
			}
			client.queueDataPatch("render-window", {
				globalRevision: DataStore.getRenderSequence(),
				renderWindow: projection,
			});
			inputCoordinator.invalidate(DirtyCanvas.Events);
			return true;
		});
		return () => {
			unsubscribe();
			cancelEventStoreRenderWindowRetry();
		};
	}, [
		cancelEventStoreRenderWindowRetry,
		getCurrentCanvasViewportSourceIds,
		inputCoordinator,
		workerTerminalRenderConvergence,
	]);

	/**
	 * Converts a timeline timestamp into the current rounded canvas coordinate.
	 * @param timestamp Timeline timestamp to transform.
	 * @returns Canvas-relative horizontal pixel.
	 */
	const getPixelPosition = useCallback(
		(timestamp: number): number => {
			const canvasWidth = Math.max(
				1,
				workerCanvasSizeRef.current?.width ??
					render_surface_ref.current?.clientWidth ??
					1,
			);
			const frameRange =
				Info.app.timeline.frame.max - Info.app.timeline.frame.min;
			if (frameRange <= 0) return -scrollXRef.current;
			return (
				Math.round(
					((timestamp - Info.app.timeline.frame.min) / frameRange) *
						canvasWidth *
						scaleRef.current,
				) - scrollXRef.current
			);
		},
		[Info.app.timeline.frame],
	);

	/**
	 * Builds the frequently changing viewport half of worker render state.
	 * @param viewportRevision Monotonic viewport revision assigned to this state.
	 * @param inputSequence Display-tick input sequence represented by this state.
	 * @param inputTimestamp Absolute timestamp of the earliest input in the batch.
	 * @param viewportOverride Optional synchronous resize values captured before React commits.
	 * @returns Worker-safe viewport state.
	 */
	const createWorkerViewportState = useCallback(
		(
			viewportRevision: number,
			inputSequence: number,
			inputTimestamp: number,
			viewportOverride?: WorkerViewportStateOverride,
		): RenderViewportState => {
			const renderSurface = render_surface_ref.current;
			const cachedSize = workerCanvasSizeRef.current;
			return {
				width:
					viewportOverride?.width ??
					cachedSize?.width ??
					Math.max(1, renderSurface?.clientWidth ?? 1),
				height:
					viewportOverride?.height ??
					cachedSize?.height ??
					Math.max(1, renderSurface?.clientHeight ?? 1),
				frame: {
					min: Info.app.timeline.frame.min,
					max: Info.app.timeline.frame.max,
				},
				scale: viewportOverride?.scale ?? scaleRef.current,
				scrollX: viewportOverride?.scrollX ?? scrollXRef.current,
				scrollY: scrollYRef.current,
				viewportRevision,
				inputSequence,
				inputTimestamp,
			};
		},
		[Info],
	);

	/**
	 * Requests an operation-scoped, pixel-equivalent event projection from
	 * QueryWorker. Only vertically visible rows plus the standard two-row overscan
	 * are included, while horizontal overscan keeps normal pan motion populated.
	 * The unchanged latest request is retried once after a delivery failure.
	 * @returns Nothing; stale and superseded results are discarded by the protocol.
	 */
	const requestEventStoreRenderWindow = useCallback((): void => {
		cancelEventStoreRenderWindowRetry();
		const client = workerClientRef.current;
		const socket = SmartSocket.Class.instance;
		if (
			!isEventStoreLodEnabled() ||
			rendererBackendRef.current !== "worker" ||
			!client ||
			!socket
		) {
			return;
		}
		const operation = Operation.Entity.selected(Info.app);
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const viewportRevision = workerViewportRevisionRef.current;
		if (
			!operation ||
			DataStore.getRenderOperationId() !== operation.id ||
			viewportRevision <= 0
		) {
			return;
		}

		const width = Math.max(
			1,
			workerCanvasSizeRef.current?.width ??
				render_surface_ref.current?.clientWidth ??
				1,
		);
		const selectedSources = Source.Entity.rendered(Info.app);
		const visibleSourceIds = getCurrentCanvasViewportSourceIds();
		const visibleSources = selectedSources.filter((source) =>
			visibleSourceIds.has(source.id),
		);
		const descriptor: QueryRenderWindowDescriptor = {
			operationId: operation.id,
			operationEpoch,
			viewportRevision,
			projectionRevision: ++workerLodProjectionRevisionRef.current,
			frame: {
				min: Info.app.timeline.frame.min,
				max: Info.app.timeline.frame.max,
			},
			width,
			scale: scaleRef.current,
			scrollX: scrollXRef.current,
			horizontalOverscanPixels: width,
			maxRepresentatives: EVENT_RENDER_PROJECTION_LIMIT,
			evictedSourceIds: selectedSources
				.filter((source) => !visibleSourceIds.has(source.id))
				.map((source) => source.id),
			sources: visibleSources.map((source) => {
				const overrideHashes = Object.keys(
					source.settings.color_override?.lookup ?? {},
				)
					.map(Number)
					.filter(Number.isFinite)
					.sort((left, right) => left - right);
				const overrideSignature = overrideHashes.join(":");
				const previousSignature =
					workerOverrideSignaturesRef.current.get(source.id);
				let overrideRevision =
					workerOverrideRevisionsRef.current.get(source.id) ?? 0;
				if (previousSignature !== overrideSignature) {
					overrideRevision++;
					workerOverrideSignaturesRef.current.set(
						source.id,
						overrideSignature,
					);
					workerOverrideRevisionsRef.current.set(
						source.id,
						overrideRevision,
					);
				}
				return {
					sourceId: source.id,
					offset: source.settings.offset ?? 0,
					engine: source.settings.render_engine,
					frequency: source.settings.frequency_sample,
					overrideHashes,
					settingsRevision:
						workerSourceMetadataRevisionsRef.current.get(source.id) ?? 0,
					overrideRevision,
					};
				}),
		};
		const requestFence: CanvasRenderWindowRequestFence = {
			socket,
			operationEpoch,
			viewportRevision,
			projectionRevision: descriptor.projectionRevision,
		};
		const getCurrentFence = (): CanvasRenderWindowRequestFence => ({
			socket: SmartSocket.Class.instance,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			viewportRevision: workerViewportRevisionRef.current,
			projectionRevision: workerLodProjectionRevisionRef.current,
		});
		const isCurrent = (): boolean =>
			isCanvasRenderWindowRequestCurrent(requestFence, getCurrentFence()) &&
			workerClientRef.current === client &&
			rendererBackendRef.current === "worker" &&
			DataStore.getRenderOperationId() === operation.id;

		const handleAttemptFailure = (attempt: number, error: unknown): void => {
			const currentFence = getCurrentFence();
			if (
				shouldRetryCanvasRenderWindow(attempt, requestFence, currentFence) &&
				isCurrent()
			) {
				cancelEventStoreRenderWindowRetry();
				workerLodRetryTimerRef.current = setTimeout(() => {
					workerLodRetryTimerRef.current = null;
					if (isCurrent()) runAttempt(attempt + 1);
				}, CANVAS_RENDER_WINDOW_RETRY_DELAY_MS);
				return;
			}
			if (!isCurrent()) return;
			const message = error instanceof Error ? error.message : String(error);
			Logger.warn(
				`Unable to project the current EventStore viewport: ${message}`,
				"Canvas.EventStore",
			);
		};

		const runAttempt = (attempt: number): void => {
			if (!isCurrent()) return;
			void socket
				.requestRenderWindow(descriptor)
				.then(
					(projection) => {
						if (!isCurrent() || projection) return;
						handleAttemptFailure(
							attempt,
							new Error("QueryWorker returned no render-window projection."),
						);
					},
					(error: unknown) => handleAttemptFailure(attempt, error),
				)
				.finally(() => scheduleTerminalRenderWatchdogRef.current());
		};

		runAttempt(1);
	}, [
		Info,
		cancelEventStoreRenderWindowRetry,
		getCurrentCanvasViewportSourceIds,
	]);
	requestEventStoreRenderWindowRef.current = requestEventStoreRenderWindow;

	/**
	 * Arms one capped terminal-convergence check after a projection request settles.
	 * Offscreen targets remain dormant until normal viewport synchronization makes them
	 * visible again; a busy render queue is rechecked without adding another patch.
	 * @returns Nothing.
	 */
	const scheduleTerminalRenderWatchdog = useCallback((): void => {
		workerTerminalRenderConvergence.scheduleWatchdog(() => {
			const client = workerClientRef.current;
			if (
				rendererBackendRef.current !== "worker" ||
				!client ||
				document.visibilityState === "hidden" ||
				!isEventStoreLodEnabled()
			) {
				return;
			}
			const operationEpoch = DataStore.getRenderOperationEpoch();
			const visibleSourceIds = getCurrentCanvasViewportSourceIds();
			if (
				workerTerminalRenderConvergence.getVisibleTargets(
					operationEpoch,
					visibleSourceIds,
				).length === 0
			) {
				return;
			}
			if (client.dataQueueDepth.aggregate === 0) {
				requestEventStoreRenderWindowRef.current();
				return;
			}
			scheduleTerminalRenderWatchdogRef.current();
		});
	}, [getCurrentCanvasViewportSourceIds, workerTerminalRenderConvergence]);
	scheduleTerminalRenderWatchdogRef.current = scheduleTerminalRenderWatchdog;

	/**
	 * Builds infrequently changing source and appearance state for the worker.
	 * @returns Worker-safe scene state containing no viewport coordinates.
	 */
	const createWorkerSceneState = useCallback((): RenderSceneState => {
		const currentApp = Info.app;
		const currentTarget = currentApp.timeline.target;
		const currentTheme = Color.Themer.theme;
		return {
			sourceOrder: Source.Entity.rendered(currentApp).map((source) => source.id),
			hidden: {
				notes: currentApp.hidden.notes,
				links: currentApp.hidden.links,
			},
			target: currentTarget
				? {
						sourceId: currentTarget["gulp.source_id"],
						timestamp: currentTarget.gulp_timestamp,
					}
				: null,
			palette: {
				fontAccent: currentTheme.FONT_ACCENT,
				fontSecondary: currentTheme.FONT_SECOND,
				backgroundAccent: currentTheme.BACKGROUND_ACCENT,
				backgroundSecondary: currentTheme.BACKGROUND_SECOND,
				border: currentTheme.BORDER,
			},
			targetGuideColor: Color.Themer.getTargetGuideColor(),
			localized: {
				ingestingSuffix: t("renderEngine.ingestingSuffix"),
				loadingSuffix: t("renderEngine.loadingSuffix"),
				unknownContext: t("collab.unknownContext"),
				locale: language,
				timeZone: timestampTimeZone,
			},
			revisions: {
				settings: currentApp.timeline.renderVersion,
				sources: workerSourcePatchRevisionRef.current,
				overlays: Math.max(
					workerNotesProjectionRevisionRef.current,
					workerLinksProjectionRevisionRef.current,
					workerHighlightsProjectionRevisionRef.current,
					workerFlagsRevisionRef.current,
				),
				theme: workerThemeRevisionRef.current,
				assets: workerAssetRevisionRef.current,
			},
		};
	}, [Info, language, t, timestampTimeZone]);

	/**
	 * Creates a stable comparison key for viewport values that affect rendering.
	 * @param state Viewport state whose volatile revision and timing fields are ignored.
	 * @returns Compact viewport signature.
	 */
	const getWorkerViewportSignature = useCallback(
		(state: RenderViewportState): string =>
			[
				state.width,
				state.height,
				state.frame.min,
				state.frame.max,
				state.scale,
				state.scrollX,
				state.scrollY,
			].join(":"),
		[],
	);

	/**
	 * Creates a comparison key for scene state so unchanged static payloads are not cloned.
	 * @param state Scene state to compare with the last worker message.
	 * @returns Deterministic scene signature.
	 */
	const getWorkerSceneSignature = useCallback(
		(state: RenderSceneState): string => JSON.stringify(state),
		[],
	);

	/**
	 * Applies one coalesced input batch and immediately posts resulting worker controls.
	 * @param batch Wheel, drag, pointer, and invalidation work for one display tick.
	 * @returns Nothing.
	 */
	const flushCanvasInput = useCallback(
		(batch: CanvasInputBatch): void => {
			if (rendererBackendRef.current === "initializing") return;
			let dirtyMask = batch.dirtyMask;
			let nextScrollX = scrollXRef.current + batch.scrollDeltaX;
			let nextScrollY = scrollYRef.current + batch.scrollDeltaY;
			let nextScale = scaleRef.current;
			let scaleChanged = false;

			if (batch.wheel) {
				let zoomDeltaY = batch.wheel.zoomDeltaY;
				let cursorX = 0;
				let canvasWidth =
					workerCanvasSizeRef.current?.width ??
					render_surface_ref.current?.clientWidth ??
					1;
				if (zoomDeltaY !== 0 && batch.wheel.zoomPoint) {
					const rect = wrapperRectRef.current ?? refreshCanvasGeometry();
					if (rect) {
						cursorX = batch.wheel.zoomPoint.clientX - rect.left;
					} else {
						zoomDeltaY = 0;
					}
				}
				if (
					zoomDeltaY !== 0 &&
					batch.wheel.zoomGestureToken !== null
				) {
					scaleCommitGuard.beginGesture(
						batch.wheel.zoomGestureToken,
						Info.app.timeline.scale,
					);
				}
				const wheelViewport = applyTimelineWheelViewport({
					scale: nextScale,
					scrollX: nextScrollX,
					panDeltaX: batch.wheel.panDeltaX,
					zoomDeltaY,
					cursorX,
					timeRange:
						Info.app.timeline.frame.max - Info.app.timeline.frame.min,
					minimumVisibleDurationMilliseconds:
						Info.MIN_VISIBLE_TIMELINE_DURATION_MS,
					isScrollReversed: Info.app.timeline.isScrollReversed,
				});
				nextScrollX = wheelViewport.scrollX;
				nextScale = wheelViewport.scale;
				scaleChanged = wheelViewport.scaleChanged;
			}

			const scrollChanged =
				nextScrollX !== scrollXRef.current || nextScrollY !== scrollYRef.current;
			if (scrollChanged || scaleChanged) {
				scrollXRef.current = nextScrollX;
				scrollYRef.current = nextScrollY;
				scaleRef.current = nextScale;
				queuedScrollYRef.current = nextScrollY;
				dirtyMask |= DirtyCanvas.Viewport;
				scrollStore.setScroll(nextScrollX, nextScrollY);
			}

			if (batch.pointer) {
				const rect = wrapperRectRef.current ?? refreshCanvasGeometry();
				const pointerX = batch.pointer.inside && rect
					? batch.pointer.clientX - rect.left
					: -1000;
				const pointerY = batch.pointer.inside && rect
					? batch.pointer.clientY - rect.top
					: -1000;
				mouseXRef.current = pointerX;
				mouseYRef.current = pointerY;
				// Timestamp projection uses the stable render world, while label
				// orientation follows the currently visible clipping viewport.
				const canvasWidth = Math.max(
					1,
					workerCanvasSizeRef.current?.width ??
						render_surface_ref.current?.clientWidth ??
						1,
				);
				pointerPositionStore.setPosition(
					projectLocalPointerTimestamp(
						{
							x: pointerX,
							y: pointerY,
							timestamp: pointerPositionStore.getSnapshot().timestamp,
							isRightSide: pointerX > (rect?.width ?? canvasWidth) / 2,
						},
						{
							frame: Info.app.timeline.frame,
							viewportWidth: canvasWidth,
							scale: nextScale,
							scrollX: nextScrollX,
						},
					),
				);
				move({ x: pointerX, y: pointerY });
				CanvasProfiler.recordLocalPointerLatency(
					Math.max(0, performance.now() - batch.pointer.inputTimestamp),
				);

				if (rendererBackendRef.current === "legacy") {
					let nextHover: string | null = null;
					for (let index = 0; index < RenderEngine.interactiveLinks.length; index++) {
						const item = RenderEngine.interactiveLinks[index];
						if (
							pointerX >= item.rect.x &&
							pointerX <= item.rect.x + item.rect.w &&
							pointerY >= item.rect.y &&
							pointerY <= item.rect.y + item.rect.h
						) {
							nextHover = `link_${index}`;
							break;
						}
					}
					if (!nextHover) {
						for (let index = 0; index < RenderEngine.interactiveNotes.length; index++) {
							const item = RenderEngine.interactiveNotes[index];
							if (
								pointerX >= item.rect.x &&
								pointerX <= item.rect.x + item.rect.w &&
								pointerY >= item.rect.y &&
								pointerY <= item.rect.y + item.rect.h
							) {
								nextHover = `note_${index}`;
								break;
							}
						}
					}
					if (nextHover !== hoveredItemRef.current) {
						hoveredItemRef.current = nextHover;
						dirtyMask |= DirtyCanvas.Hover;
					}
				}
			} else if (
				scrollChanged ||
				scaleChanged ||
				(dirtyMask & (DirtyCanvas.Size | DirtyCanvas.Viewport)) !== 0
			) {
				const currentPointer = pointerPositionStore.getSnapshot();
				const canvasWidth = Math.max(
					1,
					workerCanvasSizeRef.current?.width ??
						render_surface_ref.current?.clientWidth ??
						1,
				);
				pointerPositionStore.setPosition(
					projectLocalPointerTimestamp(currentPointer, {
						frame: Info.app.timeline.frame,
						viewportWidth: canvasWidth,
						scale: nextScale,
						scrollX: nextScrollX,
					}),
				);
			}

			workerProjectionScheduler.markInteraction();
			if (rendererBackendRef.current === "legacy") {
				if (dirtyMask !== DirtyCanvas.None) {
					const renderReason =
						Array.from(pendingRenderReasonsRef.current).join(",") || "input";
					renderCanvasRef.current(
						pendingRenderForceRef.current,
						undefined,
						renderReason,
					);
					DataStore.isDirty = false;
				}
				pendingRenderReasonsRef.current.clear();
				pendingRenderForceRef.current = false;
				return;
			}

			const client = workerClientRef.current;
			if (!client) return;
			const forceState = (dirtyMask & DirtyCanvas.Force) !== 0;
			const sceneDirtyMask =
				DirtyCanvas.Sources |
				DirtyCanvas.Events |
				DirtyCanvas.Overlays |
				DirtyCanvas.Theme |
				DirtyCanvas.Assets |
				DirtyCanvas.Force;
			if ((dirtyMask & sceneDirtyMask) !== 0 || !workerSceneSignatureRef.current) {
				const sceneState = createWorkerSceneState();
				const sceneSignature = getWorkerSceneSignature(sceneState);
				if (forceState || sceneSignature !== workerSceneSignatureRef.current) {
					workerSceneSignatureRef.current = sceneSignature;
					client.sendSceneState(sceneState, dirtyMask & sceneDirtyMask);
				}
			}

			const mayNeedViewport =
				forceState ||
				!workerViewportSignatureRef.current ||
				(dirtyMask & (DirtyCanvas.Size | DirtyCanvas.Viewport)) !== 0;
			const workerPointer = batch.pointer
				? {
						x: mouseXRef.current,
						y: mouseYRef.current,
						inside: batch.pointer.inside,
					}
				: undefined;
			let pointerBundledWithViewport = false;
			if (mayNeedViewport) {
				const viewportOverride = workerViewportOverrideRef.current ?? undefined;
				workerViewportOverrideRef.current = null;
				const viewportRevision = workerViewportRevisionRef.current + 1;
				const absoluteInputTimestamp =
					performance.timeOrigin + batch.inputTimestamp;
				const viewportState = createWorkerViewportState(
					viewportRevision,
					batch.sequence,
					absoluteInputTimestamp,
					viewportOverride,
				);
				const viewportSignature = getWorkerViewportSignature(viewportState);
				if (
					forceState ||
					!workerViewportSignatureRef.current ||
					viewportSignature !== workerViewportSignatureRef.current
				) {
					workerViewportRevisionRef.current = viewportRevision;
					workerViewportSignatureRef.current = viewportSignature;
					CanvasProfiler.recordInputTimestamp(
						batch.sequence,
						absoluteInputTimestamp,
					);
					const selectedSources = Source.Entity.rendered(Info.app);
					const viewportSourceIds = getCanvasViewportSourceIds(
						selectedSources,
						viewportState.scrollY,
						viewportState.height,
					);
					Info.setViewportSourceIds(viewportSourceIds);
					workerProjectionScheduler.setViewportSourceIds(viewportSourceIds);
					client.sendViewportState(viewportState, dirtyMask, workerPointer);
					requestEventStoreRenderWindowRef.current();
					pointerBundledWithViewport = workerPointer !== undefined;
				}
			}

			if (workerPointer && !pointerBundledWithViewport) {
				client.sendPointerNow(
					workerPointer,
					client.requestedViewportRevision,
				);
			}
			pendingRenderReasonsRef.current.clear();
			pendingRenderForceRef.current = false;
		},
		[
			Info,
			createWorkerSceneState,
			createWorkerViewportState,
			getWorkerSceneSignature,
			getWorkerViewportSignature,
			move,
			pointerPositionStore,
			refreshCanvasGeometry,
			scaleCommitGuard,
			workerProjectionScheduler,
		],
	);
	flushCanvasInputRef.current = flushCanvasInput;

	/**
	 * Rebuilds the small dependency sets for overlays omitted until documents arrive.
	 * @returns Nothing.
	 */
	const refreshWorkerOverlayDependencies = useCallback((): void => {
		const unresolvedLinkIds = new Set<Doc.Id>();
		for (const link of DataStore.links) {
			for (const documentId of [link.doc_id_from, ...link.doc_ids]) {
				if (
					!DataStore.Events.peekDocument(documentId) &&
					!Doc.Entity.id(Info.app, documentId)
				) {
					unresolvedLinkIds.add(documentId);
				}
			}
		}
		workerUnresolvedNoteDocumentIdsRef.current = new Set();
		workerUnresolvedLinkDocumentIdsRef.current = unresolvedLinkIds;

		const missingIds = Array.from(unresolvedLinkIds).slice(
			0,
			EVENT_MAIN_CACHE_SIZE,
		);
		const lookupSignature = [...missingIds].sort().join("|");
		if (
			missingIds.length === 0 ||
			lookupSignature === workerOverlayLookupSignatureRef.current
		) {
			return;
		}
		workerOverlayLookupSignatureRef.current = lookupSignature;
		const lookupSequence = ++workerOverlayLookupSequenceRef.current;
		void DataStore.Events.getByIds(missingIds)
			.then(() => {
				if (lookupSequence !== workerOverlayLookupSequenceRef.current) return;
				const client = workerClientRef.current;
				if (rendererBackendRef.current !== "worker" || !client) return;
				syncWorkerOverlaysRef.current(client, false);
				inputCoordinator.invalidate(DirtyCanvas.Overlays);
			})
			.catch((error) => {
				Logger.warn(
					`Unable to resolve link endpoints: ${(error as Error).message}`,
					"Canvas.EventStore",
				);
			})
			.finally(() => {
				if (
					lookupSequence === workerOverlayLookupSequenceRef.current &&
					workerOverlayLookupSignatureRef.current === lookupSignature &&
					workerUnresolvedLinkDocumentIdsRef.current.size > 0
				) {
					// A terminal source summary must be able to retry unresolved IDs. This
					// also advances through dependency sets larger than the bounded batch.
					workerOverlayLookupSignatureRef.current = "";
				}
			});
	}, [Info, inputCoordinator]);

	/**
	 * Creates a fresh overlay projection revision so document-dependent endpoints can refresh.
	 * @param advanceFlagsRevision Whether local-storage flag membership also changed.
	 * @returns Complete overlay replacement with worker-local monotonic revisions.
	 */
	const createWorkerOverlaySnapshot = useCallback(
		(advanceFlagsRevision: boolean): RenderOverlaySnapshot => {
			refreshWorkerOverlayDependencies();
			workerNotesProjectionRevisionRef.current++;
			workerLinksProjectionRevisionRef.current++;
			workerHighlightsProjectionRevisionRef.current++;
			if (advanceFlagsRevision || workerFlagsRevisionRef.current === 0) {
				workerFlagsRevisionRef.current++;
			}
			return renderProjectionAdapter.createOverlaySnapshot({
				notes: DataStore.notes,
				notesRevision: workerNotesProjectionRevisionRef.current,
				links: DataStore.links,
				linksRevision: workerLinksProjectionRevisionRef.current,
				highlights: DataStore.highlights,
				highlightsRevision: workerHighlightsProjectionRevisionRef.current,
				flaggedDocuments: workerFlagDocumentsRef.current,
				flagsRevision: workerFlagsRevisionRef.current,
			});
		},
		[refreshWorkerOverlayDependencies, renderProjectionAdapter],
	);

	/**
	 * Waits for bounded data-channel capacity before allocating another large buffer set.
	 * @param client Worker client whose ACK-backed queue is inspected.
	 * @param projectionGeneration Operation generation that may cancel the wait.
	 * @returns True when a slot is available, or false after cancellation/recovery.
	 */
	const waitForWorkerProjectionCapacity = useCallback(
		async (
			client: RenderWorkerClient,
			projectionGeneration: number,
			context: RenderProjectionJobContext,
		): Promise<boolean> => {
			while (
				workerClientRef.current === client &&
				workerProjectionGenerationRef.current === projectionGeneration
			) {
				const depth = client.dataQueueDepth;
				if (depth.aggregate < MAX_RENDER_DATA_QUEUE_DEPTH) {
					return true;
				}
				if (context.isCancelled()) return false;
				await context.waitForVisibleWork();
				await context.yieldToMainThread();
			}
			return false;
		},
		[],
	);

	/**
	 * Streams a complete source view as a bounded, atomic snapshot transaction.
	 * @param client Worker client receiving the source replacement.
	 * @param source Canonical source whose current events are projected.
	 * @returns Nothing.
	 */
	const streamWorkerSourceSnapshot = useCallback(
		(client: RenderWorkerClient, source: Source.Type): void => {
			workerProjectionScheduler.enqueue({
				sourceId: source.id,
				replacesSource: true,
				run: async (context) => {
					const projectionGeneration = workerProjectionGenerationRef.current;
					const membershipRevision =
						workerSourceMembershipRevisionsRef.current.get(source.id) ?? 0;
					const isCurrent = (): boolean =>
						!context.isCancelled() &&
						workerClientRef.current === client &&
						workerProjectionGenerationRef.current === projectionGeneration &&
						workerKnownSourceIdsRef.current.has(source.id) &&
						(workerSourceMembershipRevisionsRef.current.get(source.id) ?? 0) ===
							membershipRevision;
					if (!isCurrent()) return;

					const events = DataStore.getSourceEvents(source.id);
					const eventCount = events.length;
					const sourceRevision = DataStore.getSourceRevision(source.id);
					const globalRevision = DataStore.getRenderSequence();
					const snapshotId = ++workerSnapshotIdRef.current;
					const graphSamples = renderProjectionAdapter.createGraphSamples(source);
					const sampleTimestamps = new Float64Array(graphSamples.length);
					const sampleCounts = new Float64Array(graphSamples.length);
					for (let index = 0; index < graphSamples.length; index++) {
						sampleTimestamps[index] = graphSamples[index].minTimestamp;
						sampleCounts[index] = graphSamples[index].count;
					}

					if (
						!(await waitForWorkerProjectionCapacity(
							client,
							projectionGeneration,
							context,
						)) ||
						!isCurrent()
					) {
						return;
					}
					client.queueDataPatch(`snapshot:${source.id}:${snapshotId}:begin`, {
						globalRevision,
						eventSnapshots: [
							{
								kind: "begin",
								snapshotId,
								sourceId: source.id,
								revision: sourceRevision,
								totalEvents: eventCount,
								sampleTimestamps,
								sampleCounts,
							},
						],
					});

					for (
						let offset = 0;
						offset < eventCount && isCurrent();
						offset += WORKER_EVENT_SNAPSHOT_CHUNK_SIZE
					) {
						await context.waitForVisibleWork();
						if (
							!(await waitForWorkerProjectionCapacity(
								client,
								projectionGeneration,
								context,
							)) ||
							!isCurrent()
						) {
							return;
						}
						const end = Math.min(
							eventCount,
							offset + WORKER_EVENT_SNAPSHOT_CHUNK_SIZE,
						);
						let projectionSliceStartedAt = performance.now();
						const batch = await renderProjectionAdapter.createEventBatchAsync(
							source.id,
							sourceRevision,
							0,
							"replace",
							events,
							undefined,
							context.getSliceBudgetMs(),
							async () => {
								await context.yieldToMainThread(projectionSliceStartedAt);
								projectionSliceStartedAt = performance.now();
							},
							offset,
							end,
						);
						if (!isCurrent()) return;
						client.queueDataPatch(
							`snapshot:${source.id}:${snapshotId}:chunk:${offset}`,
							{
								globalRevision,
								eventSnapshots: [
									{
										kind: "chunk",
										snapshotId,
										sourceId: source.id,
										offset,
										handles: batch.handles,
										timestamps: batch.timestamps,
										hashes: batch.hashes,
									},
								],
							},
						);
					}

					if (
						!(await waitForWorkerProjectionCapacity(
							client,
							projectionGeneration,
							context,
						)) ||
						!isCurrent()
					) {
						return;
					}
					client.queueDataPatch(`snapshot:${source.id}:${snapshotId}:commit`, {
						globalRevision,
						eventSnapshots: [
							{
								kind: "commit",
								snapshotId,
								sourceId: source.id,
								revision: sourceRevision,
							},
						],
					});
				},
			});
		},
		[renderProjectionAdapter, waitForWorkerProjectionCapacity, workerProjectionScheduler],
	);
	streamWorkerSourceSnapshotRef.current = streamWorkerSourceSnapshot;

	/**
	 * Replaces worker source metadata and streams newly visible or resampled sources.
	 * @param client Worker client receiving source projections.
	 * @param streamMissingEvents Whether new and graph-resampled sources need snapshots.
	 * @returns Nothing.
	 */
	const syncWorkerSources = useCallback(
		(client: RenderWorkerClient, streamMissingEvents: boolean): void => {
			const currentApp = Info.app;
			const selectedSources = Source.Entity.rendered(currentApp);
			const sourceInputs: RenderSourceProjectionInput[] = [];
			selectedSources.forEach((source, rowOrder) => {
					const context = Context.Entity.id(currentApp, source.context_id);
					const paletteId = source.settings.color_palette?.id;
					const contextLabel = context?.name ?? t("collab.unknownContext");
					const contextColor =
						context?.color?.trim() || stringToHexColor(source.context_id);
					const loadingKind = getWorkerLoadingKind(currentApp, source);
					const gradient =
						Color.GRADIENT[
							Color.normalizeGradient(source.settings.render_color_palette)
						];
					const customPaletteColors = paletteId
						? Color.Entity.getCustomPalette(paletteId)
						: undefined;
					const metadataSignature = JSON.stringify([
						source.name,
						source.total,
						source.timestamp.min,
						source.timestamp.max,
						source.settings.offset,
						source.settings.render_engine,
						source.settings.timeline_label,
						source.settings.frequency_sample,
						source.settings.render_color_palette,
						source.settings.color_override?.lookup ?? null,
						paletteId ?? null,
						customPaletteColors ?? null,
						contextLabel,
						contextColor,
						loadingKind,
						rowOrder,
					]);
					const previousSignature =
						workerSourceMetadataSignaturesRef.current.get(source.id);
					const previousRevision =
						workerSourceMetadataRevisionsRef.current.get(source.id) ?? 0;
					const metadataRevision =
						previousSignature === metadataSignature
							? previousRevision
							: previousRevision + 1;
					workerSourceMetadataSignaturesRef.current.set(
						source.id,
						metadataSignature,
					);
					workerSourceMetadataRevisionsRef.current.set(
						source.id,
						metadataRevision,
					);
					if (!streamMissingEvents && previousSignature === metadataSignature) {
						return;
					}
					sourceInputs.push({
						source,
						revision: metadataRevision,
						rowOrder,
						contextLabel,
						contextColor,
						contextTintAlpha: Color.Themer.contextTintAlpha(
							source.context_id,
						),
						loadingKind,
						gradient,
						customPaletteColors,
					});
				},
			);
			const previousSourceIds = workerKnownSourceIdsRef.current;
			const nextSourceIds = new Set(selectedSources.map((source) => source.id));
			const acknowledgedSourceRevisions =
				client.getSynchronizationState().acknowledgedSourceRevisions;
			const removedSourceIds = Array.from(
				previousSourceIds,
			).filter((sourceId) => !nextSourceIds.has(sourceId));
			workerTerminalRenderConvergence.removeSources(removedSourceIds);
			for (const removedSourceId of removedSourceIds) {
				const currentMembershipRevision =
					workerSourceMembershipRevisionsRef.current.get(removedSourceId) ?? 0;
				workerSourceMembershipRevisionsRef.current.set(
					removedSourceId,
					currentMembershipRevision + 1,
				);
			}
			workerKnownSourceIdsRef.current = nextSourceIds;

			if (sourceInputs.length > 0 || removedSourceIds.length > 0) {
				const nextSourceRevision = ++workerSourcePatchRevisionRef.current;
				const sourcePatch: RenderDataPatch = {
					globalRevision: DataStore.getRenderSequence(),
					sources: {
						revision: nextSourceRevision,
						records: renderProjectionAdapter.createSources(sourceInputs),
						removedSourceIds,
					},
				};
				client.queueDataPatch("sources", sourcePatch);
			}

			for (const source of selectedSources) {
				const graphSignature =
					source.settings.render_engine === "graph"
						? getWorkerGraphSampleSignature(source)
						: "non-graph";
				const previousGraphSignature =
					workerGraphSignaturesRef.current.get(source.id);
				const needsEventSnapshot =
					!isEventStoreLodEnabled() &&
					shouldStreamWorkerSourceSnapshot({
						streamMissingEvents,
						wasKnownSource: previousSourceIds.has(source.id),
						graphSignature,
						previousGraphSignature,
						sourceRevision: DataStore.getSourceRevision(source.id),
						acknowledgedSourceRevision:
							acknowledgedSourceRevisions.get(source.id) ?? 0,
					});
				workerGraphSignaturesRef.current.set(source.id, graphSignature);
				if (needsEventSnapshot) {
					streamWorkerSourceSnapshot(client, source);
				}
			}

			for (const removedSourceId of removedSourceIds) {
				workerProjectionScheduler.cancelSource(removedSourceId);
				workerGraphSignaturesRef.current.delete(removedSourceId);
				workerSourceMetadataSignaturesRef.current.delete(removedSourceId);
				workerSourceMetadataRevisionsRef.current.delete(removedSourceId);
			}
			requestEventStoreRenderWindowRef.current();
		},
		[
			Info,
			renderProjectionAdapter,
			streamWorkerSourceSnapshot,
			t,
			workerProjectionScheduler,
			workerTerminalRenderConvergence,
		],
	);
	syncWorkerSourcesRef.current = syncWorkerSources;

	/**
	 * Sends complete overlay collections, resolving flags and link endpoints on main.
	 * @param client Worker client receiving the overlay replacement.
	 * @param advanceFlagsRevision Whether the main-thread-only flag view changed.
	 * @returns Nothing.
	 */
	const syncWorkerOverlays = useCallback(
		(client: RenderWorkerClient, advanceFlagsRevision: boolean): void => {
			client.queueDataPatch("overlays:snapshot", {
				globalRevision: DataStore.getRenderSequence(),
				overlays: createWorkerOverlaySnapshot(advanceFlagsRevision),
			});
		},
		[createWorkerOverlaySnapshot],
	);
	syncWorkerOverlaysRef.current = syncWorkerOverlays;

	/**
	 * Reprojects only the main-thread-owned flag collection.
	 * @param client Worker client receiving the flag replacement.
	 * @returns Nothing.
	 */
	const syncWorkerFlags = useCallback(
		(client: RenderWorkerClient): void => {
			const selectedOperation = Operation.Entity.selected(Info.app);
			const documentIds = Array.from(
				Doc.Entity.flag.getList(selectedOperation?.id),
			);
			const lookupSequence = ++workerFlagLookupSequenceRef.current;
			void DataStore.Events.getByIds(documentIds)
				.then((documents) => {
					if (
						lookupSequence !== workerFlagLookupSequenceRef.current ||
						workerClientRef.current !== client ||
						rendererBackendRef.current !== "worker"
					) {
						return;
					}
					workerFlagDocumentsRef.current = documents;
					client.queueDataPatch("overlays:flags", {
						globalRevision: DataStore.getRenderSequence(),
						overlays: {
							flags: {
								revision: ++workerFlagsRevisionRef.current,
								records:
									renderProjectionAdapter.createFlagRecords(documents),
							},
						},
					});
				})
				.catch((error: unknown) => {
					Logger.warn(
						error instanceof Error ? error.message : String(error),
						"Canvas.RenderWorker.Flags",
					);
				});
		},
		[Info, renderProjectionAdapter],
	);
	syncWorkerFlagsRef.current = syncWorkerFlags;

	useEffect(() => {
		if (!isEventStoreLodEnabled()) return;
		let lastProjectionAt = 0;
		let pendingProjection: ReturnType<typeof setTimeout> | null = null;
		const requestProjection = (immediate: boolean): void => {
			if (immediate) {
				if (pendingProjection) clearTimeout(pendingProjection);
				pendingProjection = null;
				lastProjectionAt = performance.now();
				requestEventStoreRenderWindowRef.current();
				return;
			}
			const elapsed = performance.now() - lastProjectionAt;
			if (elapsed >= 250) {
				lastProjectionAt = performance.now();
				requestEventStoreRenderWindowRef.current();
				return;
			}
			if (pendingProjection) return;
			pendingProjection = setTimeout(() => {
				pendingProjection = null;
				lastProjectionAt = performance.now();
				requestEventStoreRenderWindowRef.current();
			}, 250 - elapsed);
		};

		const unsubscribe = DataStore.subscribeEventSourceSummaries((summary) => {
			const isTerminal = summary.status !== "building";
			if (isTerminal) {
				workerTerminalRenderConvergence.record({
					sourceId: summary.sourceId,
					operationEpoch: DataStore.getRenderOperationEpoch(),
					sourceRevision: summary.revision,
					eventCount: summary.eventCount,
				});
			}
			if (rendererBackendRef.current !== "worker") return;
			if (isTerminal) refreshWorkerOverlayDependencies();
			if (summary.status === "complete") {
				const client = workerClientRef.current;
				if (client) syncWorkerFlagsRef.current(client);
			}

			const viewportSourceIds = getCurrentCanvasViewportSourceIds();
			if (!viewportSourceIds.has(summary.sourceId)) return;
			requestProjection(isTerminal);
		});

		return () => {
			unsubscribe();
			if (pendingProjection) clearTimeout(pendingProjection);
		};
	}, [
		getCurrentCanvasViewportSourceIds,
		refreshWorkerOverlayDependencies,
		rendererBackend,
		workerTerminalRenderConvergence,
	]);

	/**
	 * Reprojects only overlay collections whose previously missing documents arrived.
	 * @param client Worker client receiving the dependency refresh.
	 * @param documents Newly available canonical documents.
	 * @param globalRevision Datastore sequence associated with the document mutation.
	 * @returns Nothing.
	 */
	const syncResolvedWorkerOverlayDependencies = useCallback(
		(
			client: RenderWorkerClient,
			documents: readonly Doc.Type[],
			globalRevision: number,
		): void => {
			if (documents.length === 0) return;
			const documentIds = new Set(documents.map((document) => document._id));
			const refreshNotes = Array.from(
				workerUnresolvedNoteDocumentIdsRef.current,
			).some((documentId) => documentIds.has(documentId));
			const refreshLinks = Array.from(
				workerUnresolvedLinkDocumentIdsRef.current,
			).some((documentId) => documentIds.has(documentId));
			if (!refreshNotes && !refreshLinks) return;

			const patch: RenderDataPatch = { globalRevision, overlays: {} };
			if (refreshNotes) {
				patch.overlays!.notes = {
					revision: ++workerNotesProjectionRevisionRef.current,
					records: renderProjectionAdapter.createNoteRecords(DataStore.notes),
				};
			}
			if (refreshLinks) {
				patch.overlays!.links = {
					revision: ++workerLinksProjectionRevisionRef.current,
					records: renderProjectionAdapter.createLinkRecords(DataStore.links),
				};
			}
			refreshWorkerOverlayDependencies();
			client.queueDataPatch("overlays:resolved-dependencies", patch);
		},
		[refreshWorkerOverlayDependencies, renderProjectionAdapter],
	);

	/**
	 * Converts one observable datastore mutation into an ordered worker projection.
	 * @param change Revisioned canonical datastore mutation.
	 * @returns Nothing.
	 */
	const handleWorkerRenderChange = useCallback(
		(change: DataStoreRenderChange): void => {
			if (change.type === "operation-reset") {
				cancelEventStoreRenderWindowRetry();
				workerTerminalRenderConvergence.reset();
			}
			const client = workerClientRef.current;
			if (rendererBackendRef.current !== "worker" || !client) return;

			if (change.type === "operation-reset") {
				workerProjectionGenerationRef.current++;
				workerProjectionScheduler.cancelAll();
				workerKnownSourceIdsRef.current.clear();
				workerGraphSignaturesRef.current.clear();
				workerSourceMetadataSignaturesRef.current.clear();
				workerSourceMetadataRevisionsRef.current.clear();
				workerSourceMembershipRevisionsRef.current.clear();
				workerOverrideSignaturesRef.current.clear();
				workerOverrideRevisionsRef.current.clear();
				workerSourcePatchRevisionRef.current = 0;
				workerLodProjectionRevisionRef.current = 0;
				workerNotesProjectionRevisionRef.current = 0;
				workerLinksProjectionRevisionRef.current = 0;
				workerHighlightsProjectionRevisionRef.current = 0;
				workerFlagsRevisionRef.current = 0;
				workerFlagDocumentsRef.current = [];
				workerFlagLookupSequenceRef.current++;
				workerUnresolvedNoteDocumentIdsRef.current.clear();
				workerUnresolvedLinkDocumentIdsRef.current.clear();
				workerOverlayLookupSignatureRef.current = "";
				workerOverlayLookupSequenceRef.current++;
				workerViewportRevisionRef.current = 0;
				workerViewportSignatureRef.current = "";
				workerSceneSignatureRef.current = "";
				client.resetOperation(change.operationEpoch);
				inputCoordinator.invalidate(DirtyCanvas.Force);
				return;
			}

			if (change.type === "source-events") {
				if (isEventStoreLodEnabled()) {
					requestEventStoreRenderWindowRef.current();
					return;
				}
				if (!workerKnownSourceIdsRef.current.has(change.sourceId)) return;
				const mode =
					change.mutation === "replace"
						? "replace"
						: change.mutation === "clear"
							? "clear"
							: "upsert";
				const changedEvents =
					mode === "replace"
						? change.events
						: mode === "clear"
							? []
							: change.changedEvents;
				syncResolvedWorkerOverlayDependencies(
					client,
					changedEvents,
					change.sequence,
				);
				const source = Source.Entity.id(Info.app, change.sourceId);
				const carriesGraphSamples =
					source?.settings.render_engine === "graph" &&
					(mode === "replace" ||
						(change.mutation === "touch" &&
							change.changedEvents === change.events));
				if (mode === "replace" || carriesGraphSamples) {
					if (source) streamWorkerSourceSnapshot(client, source);
					return;
				}
				if (
					mode === "upsert" &&
					source &&
					(workerProjectionScheduler.hasSourceReplacementWork(change.sourceId) ||
						workerProjectionScheduler.getSourceWorkDepth(change.sourceId) >=
							WORKER_SOURCE_DELTA_BACKLOG_LIMIT)
				) {
					streamWorkerSourceSnapshot(client, source);
					return;
				}

				const projectionGeneration = workerProjectionGenerationRef.current;
				workerProjectionScheduler.enqueue({
					sourceId: change.sourceId,
					replacesSource: mode === "clear",
					run: async (context) => {
						if (
							context.isCancelled() ||
							workerClientRef.current !== client ||
							workerProjectionGenerationRef.current !== projectionGeneration ||
							!workerKnownSourceIdsRef.current.has(change.sourceId)
						) {
							return;
						}
						if (
							!(await waitForWorkerProjectionCapacity(
								client,
								projectionGeneration,
								context,
							))
						) {
							return;
						}
						let projectionSliceStartedAt = performance.now();
						const eventBatch = await renderProjectionAdapter.createEventBatchAsync(
							change.sourceId,
							change.sourceRevision,
							Math.max(0, change.sourceRevision - 1),
							mode,
							changedEvents,
							undefined,
							context.getSliceBudgetMs(),
							async () => {
								await context.yieldToMainThread(projectionSliceStartedAt);
								projectionSliceStartedAt = performance.now();
							},
						);
						if (context.isCancelled()) return;
						client.queueDataPatch(`events:${change.sourceId}`, {
							globalRevision: change.sequence,
							events: [eventBatch],
						});
					},
				});
				return;
			}

			if (change.type === "notes") {
				workerNotesProjectionRevisionRef.current++;
				refreshWorkerOverlayDependencies();
				client.queueDataPatch("overlays:notes", {
					globalRevision: change.sequence,
					overlays: {
						notes: {
							revision: workerNotesProjectionRevisionRef.current,
							records: renderProjectionAdapter.createNoteRecords(change.notes),
						},
					},
				});
				return;
			}
			if (change.type === "links") {
				workerLinksProjectionRevisionRef.current++;
				refreshWorkerOverlayDependencies();
				client.queueDataPatch("overlays:links", {
					globalRevision: change.sequence,
					overlays: {
						links: {
							revision: workerLinksProjectionRevisionRef.current,
							records: renderProjectionAdapter.createLinkRecords(change.links),
						},
					},
				});
				return;
			}
			workerHighlightsProjectionRevisionRef.current++;
			client.queueDataPatch("overlays:highlights", {
				globalRevision: change.sequence,
				overlays: {
					highlights: {
						revision: workerHighlightsProjectionRevisionRef.current,
						records: renderProjectionAdapter.createHighlightRecords(
							change.highlights,
						),
					},
				},
			});
		},
		[
			Info,
			cancelEventStoreRenderWindowRetry,
			inputCoordinator,
			renderProjectionAdapter,
			refreshWorkerOverlayDependencies,
			streamWorkerSourceSnapshot,
			syncResolvedWorkerOverlayDependencies,
			waitForWorkerProjectionCapacity,
			workerProjectionScheduler,
			workerTerminalRenderConvergence,
		],
	);
	handleWorkerRenderChangeRef.current = handleWorkerRenderChange;

	/**
	 * Replays the requested source or collection after a worker revision gap.
	 * @param request Worker resynchronization scope and revision diagnostics.
	 * @returns Nothing.
	 */
	const handleWorkerResync = useCallback(
		(request: RenderResyncRequiredMessage["payload"]): void => {
			const client = workerClientRef.current;
			if (rendererBackendRef.current !== "worker" || !client) return;
			CanvasProfiler.recordWorkerResync();
			if (request.scope === "source" && request.id) {
				const sourceId = request.id as Source.Id;
				if (!workerKnownSourceIdsRef.current.has(sourceId)) return;
				if (isEventStoreLodEnabled()) {
					requestEventStoreRenderWindowRef.current();
					return;
				}
				const source = Source.Entity.id(
					Info.app,
					sourceId,
				);
				if (source) streamWorkerSourceSnapshot(client, source);
				return;
			}
			if (request.scope === "sources") {
				syncWorkerSourcesRef.current(client, true);
				return;
			}
			syncWorkerOverlaysRef.current(client, request.scope === "flags");
		},
		[Info, streamWorkerSourceSnapshot],
	);
	handleWorkerResyncRef.current = handleWorkerResync;

	/**
	 * Switches permanently to a newly mounted legacy canvas after worker failure.
	 * @param error Fatal initialization, protocol, or render error.
	 * @returns Nothing.
	 */
	const activateLegacyRenderer = useCallback((error: Error): void => {
		if (forcedLegacyBackendRef.current) return;
		workerTerminalRenderConvergence.reset();
		if (isEventStoreLodEnabled()) {
			Logger.error(
				`The render worker failed while EventStore LOD owned the documents: ${error.message}`,
				"Canvas.RenderWorker.LodRollback",
			);
			if (reloadWithLegacyEventPipeline()) return;
			publishRendererBackendStatus({
				requestedBackend: requestedRendererBackendRef.current,
				activeBackend: "initializing",
				fallbackReason:
					"Legacy fallback was blocked because the EventStore LOD rollback could not be persisted.",
			});
			return;
		}
		forcedLegacyBackendRef.current = true;
		workerProjectionGenerationRef.current++;
		workerProjectionScheduler.cancelAll();
		workerSourceMembershipRevisionsRef.current.clear();
		workerViewportSignatureRef.current = "";
		workerSceneSignatureRef.current = "";
		RenderEngine.clearAllCaches();
		workerClientRef.current?.dispose();
		workerClientRef.current = null;
		CanvasProfiler.recordWorkerFallback();
		Logger.error(error.message, "Canvas.RenderWorker");
		publishRendererBackendStatus({
			requestedBackend: requestedRendererBackendRef.current,
			activeBackend: "initializing",
			fallbackReason: error.message,
		});
		setCanvasElementKey((currentKey) => currentKey + 1);
	}, [
		publishRendererBackendStatus,
		workerProjectionScheduler,
		workerTerminalRenderConvergence,
	]);

	/**
	 * Reconciles render-surface dimensions before posting one atomic worker state.
	 * @returns Nothing.
	 */
	const handleWorkerResize = useCallback((): void => {
		const renderSurface = render_surface_ref.current;
		if (!renderSurface || rendererBackendRef.current !== "worker") return;
		const nextSize = {
			width: Math.max(1, renderSurface.clientWidth),
			height: Math.max(1, renderSurface.clientHeight),
		};
		const previousSize = workerCanvasSizeRef.current;
		if (
			previousSize?.width === nextSize.width &&
			previousSize.height === nextSize.height
		) {
			return;
		}
		workerCanvasSizeRef.current = nextSize;
		let nextScale = scaleRef.current;
		let nextScrollX = scrollXRef.current;

		if (previousSize && previousSize.width !== nextSize.width) {
			const requestedScale =
				nextScale * (previousSize.width / nextSize.width);
			const timeRange =
				Info.app.timeline.frame.max - Info.app.timeline.frame.min;
			const maxScale =
				timeRange > 0
					? timeRange / Info.MIN_VISIBLE_TIMELINE_DURATION_MS
					: 9_999_999;
			nextScale = Math.max(0.01, Math.min(maxScale, requestedScale));
			nextScrollX = nextScrollX - nextSize.width + previousSize.width;
			scrollXRef.current = nextScrollX;
			scaleRef.current = nextScale;
		}
		workerViewportOverrideRef.current = {
			...nextSize,
			scale: nextScale,
			scrollX: nextScrollX,
		};
		inputCoordinator.invalidate(DirtyCanvas.Size | DirtyCanvas.Viewport);

		if (previousSize && previousSize.width !== nextSize.width) {
			Info.setTimelineScale(nextScale);
			scrollStore.setScrollX(nextScrollX);
		}
	}, [Info, inputCoordinator]);
	handleWorkerResizeRef.current = handleWorkerResize;

	useLayoutEffect(() => {
		const canvas = canvas_ref.current;
		if (!canvas) return;
		const requestedPreference = forcedLegacyBackendRef.current
			? requestedRendererBackendRef.current
			: getCanvasRendererPreference();
		requestedRendererBackendRef.current = requestedPreference;
		const preference = forcedLegacyBackendRef.current
			? "legacy"
			: requestedPreference;
		const capabilities = RenderWorkerClient.probeCapabilities();
		if (preference === "legacy" || !capabilities.supported) {
			if (isEventStoreLodEnabled()) {
				if (reloadWithLegacyEventPipeline()) return;
				const lodFallbackReason =
					"Legacy rendering cannot start while EventStore LOD owns document storage.";
				Logger.error(lodFallbackReason, "Canvas.RenderWorker.LodRollback");
				publishRendererBackendStatus({
					requestedBackend: requestedPreference,
					activeBackend: "initializing",
					fallbackReason: lodFallbackReason,
				});
				return;
			}
			const fallbackReason = forcedLegacyBackendRef.current
				? rendererFallbackReasonRef.current
				: preference !== "legacy"
					? capabilities.reason ?? "Worker rendering is unavailable."
					: null;
			publishRendererBackendStatus({
				requestedBackend: requestedPreference,
				activeBackend: "legacy",
				fallbackReason,
			});
			if (preference !== "legacy" && capabilities.reason) {
				Logger.debug(capabilities.reason, "Canvas.RenderWorker");
			}
			return;
		}
		const initialCanvasSize = {
			width: Math.max(
				1,
				render_surface_ref.current?.clientWidth ?? canvas.clientWidth,
			),
			height: Math.max(
				1,
				render_surface_ref.current?.clientHeight ?? canvas.clientHeight,
			),
		};
		// The placeholder must start with the same one-to-one backing size that the
		// worker receives because its width and height cannot be reconciled on main
		// after canvas ownership has transferred.
		canvas.width = initialCanvasSize.width;
		canvas.height = initialCanvasSize.height;

		let client: RenderWorkerClient;
		let unsubscribeRenderChanges: (() => void) | null = null;
		let unregisterWorkerSynchronization: (() => void) | null = null;
		publishRendererBackendStatus({
			requestedBackend: requestedPreference,
			activeBackend: "initializing",
			fallbackReason: null,
		});
		try {
			client = RenderWorkerClient.start(
				canvas,
				DataStore.getRenderOperationEpoch(),
				{
					fonts: RENDER_WORKER_FONTS,
						onReady: () => {
							inputCoordinator.invalidate(DirtyCanvas.Force);
						},
						onDataQueueDepthChanged: (queueDepth) => {
							CanvasProfiler.recordWorkerQueueDepth(
								queueDepth.inFlight,
								queueDepth.pending,
							);
						},
						onFrameRendered: (statistics) => {
							const queueDepth = client.dataQueueDepth;
							CanvasProfiler.recordWorkerFrame({
								drawDurationMs: statistics.drawDuration,
								dataLatencyMs: statistics.dataLatency ?? undefined,
								queueDepth: queueDepth.aggregate,
								inFlightQueueDepth: queueDepth.inFlight,
								pendingQueueDepth: queueDepth.pending,
								dataRevision: statistics.dataRevision,
								viewportRevision: statistics.viewportRevision,
								inputSequence: statistics.inputSequence,
								workerReceiveDuration: statistics.workerReceiveDuration,
								workerWaitDuration: statistics.workerWaitDuration,
							});
							if (
								statistics.viewportRevision ===
								workerViewportRevisionRef.current
							) {
								const synchronization = client.getSynchronizationState();
								workerTerminalRenderConvergence.acknowledgeFrame(
									DataStore.getRenderOperationEpoch(),
									getCurrentCanvasViewportSourceIds(),
									synchronization.acknowledgedSourceRevisions,
								);
								if (workerTerminalRenderConvergence.hasTargets()) {
									scheduleTerminalRenderWatchdogRef.current();
								}
							}
						},
						onHoverChanged: (hit) => {
							hoveredItemRef.current = hit
								? `${hit.kind}:${hit.id}`
								: null;
						},
					onResyncRequired: (request) => {
						handleWorkerResyncRef.current(request);
					},
					onAssetRequest: async (request) => {
						const assetRevision = workerAssetRevisionRef.current;
						const iconName = getWorkerAssetIconName(request.assetKey);
						const iconColor = resolveWorkerAssetIconColor(request.color);
						try {
							return {
								bitmap: await createCanvasIconBitmap({
									name: iconName,
									color: iconColor,
									size: 32,
								}),
								revision: assetRevision,
							};
						} catch (error) {
							Logger.error(
								`Failed to prepare worker icon "${iconName}" from asset "${request.assetKey}" with color "${iconColor}": ${
									error instanceof Error ? error.message : String(error)
								}`,
								"Canvas.RenderWorker.Asset",
							);
							try {
								return {
									bitmap: await createCanvasIconBitmap({
										name: Default.Icon.EVENT,
										color: Color.Themer.theme.FONT_ACCENT,
										size: 32,
									}),
									revision: assetRevision,
								};
							} catch (fallbackError) {
								Logger.error(
									fallbackError instanceof Error
										? fallbackError.message
										: String(fallbackError),
									"Canvas.RenderWorker.AssetFallback",
								);
								return {
									bitmap: await createCanvasIconPlaceholderBitmap(),
									revision: assetRevision,
								};
							}
						}
					},
					onWorkerError: (error) => {
						CanvasProfiler.recordWorkerError();
						if (!error.fatal) {
							Logger.error(error.message, `Canvas.RenderWorker.${error.phase}`);
						}
					},
					onFatalError: activateLegacyRenderer,
				},
			);
		} catch (error) {
			activateLegacyRenderer(
				error instanceof Error ? error : new Error(String(error)),
			);
			return;
		}

		if (forcedLegacyBackendRef.current) {
			client.dispose();
			return;
		}
		workerClientRef.current = client;
		workerCanvasSizeRef.current = initialCanvasSize;
		publishRendererBackendStatus({
			requestedBackend: requestedPreference,
			activeBackend: "worker",
			fallbackReason: null,
		});
		unregisterWorkerSynchronization =
			CanvasProfiler.registerWorkerSynchronizationProvider(() => {
				const synchronization = client.getSynchronizationState();
				const projectionMetrics = workerProjectionScheduler.getMetrics();
				return {
					activeBackend: "worker",
					ready: synchronization.ready,
					operationEpoch: synchronization.operationEpoch,
					expectedGlobalRevision: DataStore.getRenderSequence(),
					acknowledgedGlobalRevision:
						synchronization.acknowledgedGlobalRevision,
					renderedGlobalRevision: synchronization.renderedGlobalRevision,
					inFlightDataMessages: synchronization.inFlightDataMessages,
					pendingDataMessages: synchronization.pendingDataMessages,
					pendingProjectionTasks:
						projectionMetrics.visibleQueueDepth +
						projectionMetrics.backgroundQueueDepth,
					pendingSourceResynchronizations: 0,
					visibleProjectionDepth: projectionMetrics.visibleQueueDepth,
					backgroundProjectionDepth: projectionMetrics.backgroundQueueDepth,
					oldestProjectionAgeMs: projectionMetrics.oldestQueuedAgeMs,
					projectionSliceDurationMs: projectionMetrics.lastSliceDurationMs,
					requestedViewportRevision:
						synchronization.requestedViewportRevision,
					renderedViewportRevision:
						synchronization.renderedViewportRevision,
					sources: Array.from(workerKnownSourceIdsRef.current).map(
						(sourceId) => ({
							sourceId,
							expectedRevision: DataStore.getSourceRevision(sourceId),
							acknowledgedRevision:
								synchronization.acknowledgedSourceRevisions.get(sourceId) ?? 0,
						}),
					),
				};
			});
		unsubscribeRenderChanges = DataStore.subscribeRenderChanges((change) => {
			handleWorkerRenderChangeRef.current(change);
		});
		syncWorkerSourcesRef.current(client, true);
		syncWorkerOverlaysRef.current(client, true);
		inputCoordinator.invalidate(
			DirtyCanvas.Size |
				DirtyCanvas.Sources |
				DirtyCanvas.Overlays |
				DirtyCanvas.Force,
		);
		client.setVisibility(document.visibilityState !== "hidden");
		void client.ready.catch(() => undefined);

		return () => {
			unsubscribeRenderChanges?.();
			unregisterWorkerSynchronization?.();
			if (workerClientRef.current === client) {
				workerClientRef.current = null;
			}
			client.dispose();
		};
	}, [
		activateLegacyRenderer,
		canvasElementKey,
		getCurrentCanvasViewportSourceIds,
		inputCoordinator,
		publishRendererBackendStatus,
		workerProjectionScheduler,
		workerTerminalRenderConvergence,
	]);

	/**
	 * Resolves the source row under a native context-menu event.
	 * @param event Native pointer event emitted by the primary canvas.
	 * @returns Nothing.
	 */
	const handleContextMenu = useCallback(
		(event: MouseEvent) => {
			if (!timeline.current) {
				return;
			}

			const index = Math.floor(
				(event.clientY +
					scrollYRef.current -
					timeline.current.getBoundingClientRect().top) /
					48,
			);

			const files = Source.Entity.rendered(Info.app);
			const file = files[index] ?? null;

			setTarget(file);
		},
		[
			setTarget,
			timeline,
			app.timeline.filter,
			app.target.files,
			app.hidden.sourcesWithNoEvents,
		],
	);

	const Menu = useCallback(() => {
		if (!target) {
			return null;
		}

		return <TargetMenu source={target} />;
	}, [target]);

	/**
	 * Closes the anchored group dialog and resets its source label.
	 *
	 * @returns Nothing.
	 */
	const closeGroupDialog = useCallback(() => {
		setGroupDialogAnchor(null);
		setGroupDialogEvents([]);
		setGroupDialogEventStore(null);
		setGroupDialogKind("events");
	}, []);

	/**
	 * Loads one bounded page for the open exact-event group.
	 * A stale cursor is converted into a fresh first page so the dialog can publish
	 * its non-blocking source-revision reset notice.
	 * @param cursor Optional EventStore cursor for the next page.
	 * @returns Exact page tied to the current operation epoch.
	 */
	const loadGroupDialogEventPage = useCallback(
		async (cursor?: EventPageCursor): Promise<EventPixelPage> => {
			const group = groupDialogEventStore;
			if (
				!group ||
				DataStore.getRenderOperationEpoch() !== group.operationEpoch
			) {
				throw new Error("The event group belongs to an inactive operation.");
			}

			let page: EventPixelPage;
			try {
				page = await DataStore.Events.queryPixelBucket(
					group.sourceId,
					group.range,
					EVENT_PIXEL_PAGE_SIZE,
					cursor,
				);
			} catch (error) {
				if (!cursor || !(error instanceof StaleEventStoreCursorError)) {
					throw error;
				}
				page = await DataStore.Events.queryPixelBucket(
					group.sourceId,
					group.range,
					EVENT_PIXEL_PAGE_SIZE,
				);
			}

			if (DataStore.getRenderOperationEpoch() !== group.operationEpoch) {
				throw new Error("The operation changed while loading the event group.");
			}
			return page;
		},
		[groupDialogEventStore],
	);

	const totalHeight = useMemo(() => {
		if (!canvas_ref.current) {
			return 1920;
		}

		return (
			(wrapper_ref.current?.clientHeight ?? 1920) * 2 +
			visibleSourceRowCount * 48 -
			80
		);
	}, [
		visibleSourceRowCount,
		canvas_ref,
	]);

	/**
	 * Applies vertical scrollbar movement optimistically before the store commit.
	 * @param event React scroll event from the hidden timeline scrollbar.
	 * @returns Nothing.
	 */
	const scrollBarEventHandler = useCallback(
		(event: React.UIEvent<HTMLDivElement>) => {
			if (isProgramScroll.current) {
				isProgramScroll.current = false;
				return;
			}

			const newScrollY =
					event.currentTarget.scrollTop -
					(wrapper_ref.current?.clientHeight ?? 0);
			const deltaY = newScrollY - queuedScrollYRef.current;
			queuedScrollYRef.current = newScrollY;
			inputCoordinator.queueScrollDelta(0, deltaY, performance.now());
			isManualScroll.current = true;
		},
		[inputCoordinator],
	);

	useLayoutEffect(() => {
		if (isManualScroll.current || !scrollbar.current || !canvas_ref.current) {
			isManualScroll.current = false;
			return;
		}

		const newScrollTop =
			scrollYRef.current + (wrapperRectRef.current?.height ?? 0);

		scrollbar.current.scroll({
			behavior: "instant",
			top: newScrollTop,
		});
		isProgramScroll.current = true;
	}, [canvasElementKey]);

	/**
	 * Applies row or page keyboard scrolling while preserving editable controls.
	 * @param event Keyboard event emitted by the focusable timeline wrapper.
	 * @returns Nothing.
	 */
	const handleKeyboardScroll = useCallback(
		(event: React.KeyboardEvent<HTMLElement>) => {
			const scrollYByKey: Record<string, number> = {
				ArrowDown: 48,
				ArrowUp: -48,
				PageDown: Math.max(48, Math.floor((wrapper_ref.current?.clientHeight ?? 0) * 0.9)),
				PageUp: -Math.max(48, Math.floor((wrapper_ref.current?.clientHeight ?? 0) * 0.9)),
			};
			const scrollXByKey: Record<string, number> = {
				ArrowLeft: -48,
				ArrowRight: 48,
			};
			const deltaY = scrollYByKey[event.key] ?? 0;
			const deltaX = scrollXByKey[event.key] ?? 0;
			if ((!deltaX && !deltaY) || banner) return;

			const target = event.target as HTMLElement | null;
			const tagName = target?.tagName;
			if (
				tagName === "INPUT" ||
				tagName === "TEXTAREA" ||
				tagName === "SELECT" ||
				tagName === "BUTTON" ||
				target?.isContentEditable
			) {
				return;
			}

			event.preventDefault();
			let appliedDeltaY = 0;
			if (deltaY) {
				const canvasHeight =
					wrapperRectRef.current?.height ?? workerCanvasSizeRef.current?.height ?? 0;
				const maxScrollY = Math.max(
					-26,
					(scrollbar.current?.scrollHeight ?? totalHeight) -
						(scrollbar.current?.clientHeight ?? canvasHeight) -
						canvasHeight,
				);
				const nextScrollY = Math.min(
					maxScrollY,
					Math.max(-26, queuedScrollYRef.current + deltaY),
				);
				appliedDeltaY = nextScrollY - queuedScrollYRef.current;
				queuedScrollYRef.current = nextScrollY;
			}
			inputCoordinator.queueScrollDelta(
				deltaX,
				appliedDeltaY,
				performance.now(),
			);
		},
		[banner, inputCoordinator, totalHeight],
	);

	return (
		<ContextMenu>
			<ContextMenuTrigger
				ref={wrapper_ref}
				className={cn(s.wrapper, isAltPressed && s.cursor, s.no_cursor)}
				onMouseLeave={handleMouseUpOrLeave as any}
				onMouseDown={handleMouseDown}
				onMouseMove={handleMouseMove}
				onMouseUp={handleMouseUpOrLeave as any}
				onKeyDown={(event) => {
					toggler(event as any);
					handleKeyboardScroll(event);
				}}
				tabIndex={0}
			>
				<div
					ref={render_surface_ref}
					className={cn(
						s.render_surface,
						stableWidth && s.stable_render_surface,
					)}
				>
					<Highlights.List.Overlay />
					<canvas
						key={canvasElementKey}
						className={s.canvas}
						ref={canvas_ref}
						id="canvas"
					/>
				</div>
				<Stack
					pos="absolute"
					className={s.island}
				>
					{flaggedEvents.size > 0 && operation && (
						<Button
							className={s.unflag}
							variant="glass"
							onClick={() => Doc.Entity.flag.reset(operation.id)}
							icon="FlagOff"
						>
							{t("doc.unflagAllDocuments", { count: flaggedEvents.size })}
						</Button>
					)}
				</Stack>
				<Spinner
					size={48}
					className={s.loading_background}
				/>
				<Pointers
					getPixelPosition={getPixelPosition}
					selfStore={pointerPositionStore}
				/>
				<canvas
					className={s.resize}
					ref={overlay_ref}
				/>
				<Magnifier
					self={magnifier_ref}
					mousePosition={mousePosition}
					isVisible={isAltPressed}
				/>
				<Stack
					ref={scrollbar}
					className={s.scrollbar}
					pos="absolute"
					ai="flex-start"
					jc="flex-start"
					onScroll={scrollBarEventHandler}
				>
					<Stack
						style={{ height: totalHeight }}
						pos="relative"
					/>
				</Stack>
				{highlightsOverlay}
			</ContextMenuTrigger>
			<Menu />
			{groupDialogAnchor &&
			groupDialogEventStore &&
			groupDialogEventStore.initialPage.total > 1 ? (
				<DisplayGroupDialog
					mode="event-store"
					initialPage={groupDialogEventStore.initialPage}
					loadPage={loadGroupDialogEventPage}
					anchor={groupDialogAnchor}
					groupKind="events"
					onClose={closeGroupDialog}
					preserveTimelineTarget={Boolean(app.timeline.target)}
				/>
			) : groupDialogAnchor && groupDialogEvents.length > 1 ? (
				<DisplayGroupDialog
					events={groupDialogEvents}
					anchor={groupDialogAnchor}
					groupKind={groupDialogKind}
					onClose={closeGroupDialog}
					preserveTimelineTarget={Boolean(app.timeline.target)}
				/>
			) : null}
		</ContextMenu>
	);
}
