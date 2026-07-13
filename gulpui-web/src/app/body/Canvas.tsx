import { Application } from "@/context/Application.context";
import { DataStore } from "@/store/DataStore";
import { useScroll, scrollStore } from "@/store/scroll.store";
import { getLimits, getTimestamp, throwableByTimestamp } from "@/ui/utils";
import {
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import s from "./styles/Canvas.module.css";
import { useMagnifier } from "@/dto/useMagnifier";
import { Magnifier } from "@/ui/Magnifier";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { RenderEngine } from "@/class/RenderEngine";
import { CanvasIcon } from "@/ui/CanvasIcon";
import {
	DisplayGroupDialog,
	type DisplayGroupKind,
} from "@/dialogs/Group.dialog";
import { LoggerHandler } from "@/dto/Logger.class";
import { debounce } from "lodash";
import { useDrugs } from "@/decorator/use";
import { ContextMenu, ContextMenuTrigger } from "@/ui/ContextMenu";
import { TargetMenu } from "./Target.menu";
import { cn } from "@/ui/utils";
import { Pointers } from "@/components/Pointers";
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

export namespace Canvas {
	export interface Props extends Stack.Props {
		timeline: React.RefObject<HTMLDivElement>;
	}
}

/**
 * Resolves the events opened by an interactive Canvas link badge.
 * @param app Current application state used for document lookup.
 * @param link Link represented by the clicked badge.
 * @returns Connected events, or the root event for single-document links.
 */
function getCanvasLinkDialogEvents(app: App.Type, link: Link.Type): Doc.Type[] {
	const docIds = getUniqueCanvasLinkDocIds(link);
	const docs: Doc.Type[] = [];

	for (const docId of docIds) {
		const doc = Doc.Entity.id(app, docId);
		if (doc) docs.push(doc);
	}

	return docs;
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

export function Canvas({ timeline }: Canvas.Props) {
	const { theme } = useTheme();
	const { t } = Locale.use();
	const canvas_ref = useRef<HTMLCanvasElement>(
		null as unknown as HTMLCanvasElement,
	);
	const overlay_ref = useRef<HTMLCanvasElement>(
		null as unknown as HTMLCanvasElement,
	);
	const wrapper_ref = useRef<HTMLDivElement>(null as unknown as HTMLDivElement);
	const { app, banner, spawnDialog, Info, dialog, highlightsOverlay } =
		Application.use();
	const { x: scrollX, y: scrollY } = useScroll();
	const [target, setTarget] = useState<Source.Type | null>(null);
	const [groupDialogEvents, setGroupDialogEvents] = useState<Doc.Type[]>([]);
	const [groupDialogAnchor, setGroupDialogAnchor] = useState<{
		x: number;
		y: number;
	} | null>(null);
	const [groupDialogKind, setGroupDialogKind] =
		useState<DisplayGroupKind>("events");
	const { toggler, move, magnifier_ref, isAltPressed, mousePosition } =
		useMagnifier(canvas_ref, [
			app.target.files,
			scrollX,
			scrollY,
			app.timeline.frame,
			app.timeline.scale,
			dialog,
			app.timeline.target,
			app.timeline.filter,
			app.timeline.dialogSize,
			app.hidden,
			target,
		]);
	const { resize, handleMouseDown, handleMouseMove, handleMouseUpOrLeave } =
		useDrugs(canvas_ref);
	const pendingFrame = useRef<number>(0);
	const pendingRenderReasonsRef = useRef<Set<string>>(new Set());
	const pendingRenderForceRef = useRef<boolean>(false);
	const pendingZoomFrame = useRef<number | null>(null);
	const pendingZoomScaleRef = useRef<number | null>(null);
	const pendingZoomScrollXRef = useRef<number | null>(null);
	const didRenderInitialFrameRef = useRef<boolean>(false);
	const scrollXRef = useRef(scrollX);
	const scrollYRef = useRef(scrollY);
	const mouseXRef = useRef<number>(-1000);
	const mouseYRef = useRef<number>(-1000);
	const hoveredItemRef = useRef<string | null>(null);

	useEffect(() => {
		scrollXRef.current = scrollX;
		scrollYRef.current = scrollY;
	}, [scrollX, scrollY]);

	/**
	 * Publishes the latest scroll position to React subscribers in one coalesced store update.
	 * @param x Horizontal canvas scroll position.
	 * @param y Vertical canvas scroll position.
	 * @returns Nothing.
	 */
	const syncScrollToContext = useMemo(
		() =>
			debounce((x: number, y: number) => {
				scrollStore.setScroll(x, y);
			}, 16), // 60fps circa
		[],
	);

	/**
	 * LAZY INDEXING: Instead of eagerly rebuilding the note-to-source index here
	 * (which ran O(n*m) on every WebSocket note update), we only invalidate.
	 * The actual rebuild happens lazily on first access in renderCanvas() via
	 * Note.Entity.ensureIndexing(). This eliminates a cascade where this effect
	 * and the render effect both triggered on the same `app.target.notes` change.
	 */
	useEffect(() => {
		Note.Entity.invalidateCache();
		RenderEngine.reset("notes");
		RenderEngine.reset("flags");
	}, [app.target.notes, app.target.files]);

	/**
	 * Renders the current Canvas frame and reports the Canvas-rendered source set.
	 * @param force Whether to force cache invalidation for overlay-related render data.
	 * @param ctx Optional canvas rendering context, defaulting to the main canvas.
	 * @returns Nothing.
	 */
	const renderCanvas = (
		force = false,
		ctx = canvas_ref.current?.getContext("2d"),
		reason = "direct",
	) => {
		if (!wrapper_ref.current || !ctx || !canvas_ref.current) {
			return;
		}

		const app = Info.app; // Capture current state for this frame

		const oldWidth = canvas_ref.current.width;
		const oldHeight = canvas_ref.current.height;
		const newWidth = wrapper_ref.current.clientWidth;
		const newHeight = wrapper_ref.current.clientHeight;

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
		const limits = getLimits(app, Info, timeline, currentScrollX);
		const selectedSources = Source.Entity.selected(app);

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

		// Publish the full Canvas-rendered source set, not the vertically visible
		// viewport subset. Detached NotesWindow uses this stable source list for its
		// "visible sources only" filter without re-filtering on every pan/scroll frame.
		Info.setVisibleSourceIds(files.map((file) => file.id));

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
			timeline.current?.clientHeight || 0,
		);
		ctx.fillRect(
			getPixelPosition(app.timeline.frame.max || app.timeline.frame?.max) + 2,
			0,
			3,
			timeline.current?.clientHeight || 0,
		);

		render.ruler.sections();
		profiler?.mark("ruler");
		profiler?.setCounts(render.stats);
		profiler?.finish();
	};

	const renderCanvasRef = useRef(renderCanvas);
	renderCanvasRef.current = renderCanvas;

	/**
	 * Schedules a Canvas repaint and coalesces multiple requests into one animation frame.
	 * @param reason Human-readable reason used by the dev profiler.
	 * @param force Whether the scheduled render should invalidate overlay-related caches.
	 * @returns Nothing.
	 */
	const requestCanvasRender = useMemo(
		() =>
			(reason: string, force = false): void => {
				pendingRenderForceRef.current = pendingRenderForceRef.current || force;
				pendingRenderReasonsRef.current.add(reason);

				if (pendingFrame.current) {
					return;
				}

				pendingFrame.current = requestAnimationFrame(() => {
					const renderReason =
						Array.from(pendingRenderReasonsRef.current).join(",") || "unknown";
					const renderForce = pendingRenderForceRef.current;
					pendingFrame.current = 0;
					pendingRenderReasonsRef.current.clear();
					pendingRenderForceRef.current = false;
					renderCanvasRef.current(renderForce, undefined, renderReason);
					DataStore.isDirty = false;
				});
			},
		[],
	);

	/**
	 * Commits the latest queued zoom scale and anchored scroll position.
	 * @returns Nothing.
	 */
	const commitPendingZoom = useCallback((): void => {
		pendingZoomFrame.current = null;
		const nextScale = pendingZoomScaleRef.current;
		const nextScrollX = pendingZoomScrollXRef.current;
		pendingZoomScaleRef.current = null;
		pendingZoomScrollXRef.current = null;

		if (typeof nextScale === "number") {
			Info.setTimelineScale(nextScale);
		}
		if (typeof nextScrollX === "number") {
			scrollStore.setScrollX(nextScrollX);
		}
		requestCanvasRender("wheel:zoom");
	}, [Info, requestCanvasRender]);

	/**
	 * Queues one zoom state commit for the next animation frame.
	 * @returns Nothing.
	 */
	const scheduleZoomCommit = useCallback((): void => {
		if (pendingZoomFrame.current !== null) {
			return;
		}

		pendingZoomFrame.current = requestAnimationFrame(commitPendingZoom);
	}, [commitPendingZoom]);

	const renderOverlay = () => {
		if (!overlay_ref.current || !canvas_ref.current) return;
		const overlayCtx = overlay_ref.current.getContext("2d");
		if (!overlayCtx) return;

		overlay_ref.current.height = canvas_ref.current.height;
		overlay_ref.current.width = canvas_ref.current.width;

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
	}, [overlay_ref, canvas_ref, resize]);

	// [ ] TODO: Move to Info.tsx;
	const getEventsListFromFileByClickX = (x: number, file: Source.Type) => {
		const events = Source.Entity.events(app, file);
		const result: Doc.Type[] = [];
		if (events.length === 0) return result;
		let left = 0;
		let right = events.length - 1;
		let foundIdx = -1;
		while (left <= right) {
			const mid = Math.floor((left + right) / 2);
			const pos = getPixelPosition(
				events[mid].gulp_timestamp + file.settings.offset,
			);

			const isGraph = file.settings.render_engine === "graph";
			const isHit = isGraph ? x >= pos - 16 && x <= pos : x === pos;

			if (isHit) {
				foundIdx = mid;
				break;
			} else if (pos < x) {
				right = mid - 1;
			} else {
				left = mid + 1;
			}
		}

		if (foundIdx !== -1) {
			let i = foundIdx;
			while (i >= 0) {
				const p = getPixelPosition(
					events[i].gulp_timestamp + file.settings.offset,
				);
				if (
					p === x ||
					(file.settings.render_engine === "graph" && x >= p - 16 && x <= p)
				) {
					result.push(events[i]);
					i--;
				} else break;
			}
			i = foundIdx + 1;
			while (i < events.length) {
				const p = getPixelPosition(
					events[i].gulp_timestamp + file.settings.offset,
				);
				if (
					p === x ||
					(file.settings.render_engine === "graph" && x >= p - 16 && x <= p)
				) {
					result.push(events[i]);
					i++;
				} else break;
			}
		}
		return result;
	};

	const handleClick = (event: MouseEvent) => {
		if (event.button === 2) {
			event.preventDefault();
			return;
		}

		if (!canvas_ref.current) {
			return;
		}

		const rect = canvas_ref.current.getBoundingClientRect();
		if (
			event.clientX < rect.left ||
			event.clientX > rect.right ||
			event.clientY < rect.top ||
			event.clientY > rect.bottom
		) {
			return;
		}

		const { top, left } = canvas_ref.current.getBoundingClientRect();

		if (RenderEngine.interactiveLinks) {
			const canvasHitX = Math.round(event.clientX - left);
			const canvasHitY = Math.round(event.clientY - top);
			for (const item of RenderEngine.interactiveLinks) {
				if (
					canvasHitX >= item.rect.x &&
					canvasHitX <= item.rect.x + item.rect.w &&
					canvasHitY >= item.rect.y &&
					canvasHitY <= item.rect.y + item.rect.h
				) {
					const link = item.link;
					const docs = getCanvasLinkDialogEvents(Info.app, link);
					if (docs.length === 1) {
						spawnDialog(<DisplayEventDialog event={docs[0]} />);
					} else if (docs.length > 1) {
						setGroupDialogEvents(docs);
						setGroupDialogKind("links");
						setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
					}
					return;
				}
			}
		}

		if (RenderEngine.interactiveNotes) {
			const canvasHitX = Math.round(event.clientX - left);
			const canvasHitY = Math.round(event.clientY - top);
			for (const item of RenderEngine.interactiveNotes) {
				if (
					canvasHitX >= item.rect.x &&
					canvasHitX <= item.rect.x + item.rect.w &&
					canvasHitY >= item.rect.y &&
					canvasHitY <= item.rect.y + item.rect.h
				) {
					if (item.notes.length === 1) {
						return spawnDialog(
							<DisplayEventDialog
								event={Doc.Entity.id(Info.app, item.notes[0].doc._id)}
							/>,
						);
					}

					setGroupDialogEvents(
						item.notes.map((note) => Doc.Entity.id(Info.app, note.doc._id)),
					);
					setGroupDialogKind("notes");
					setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
					return;
				}
			}
		}

		const click: XY = {
			x: Math.round(event.clientX - left),
			y: Math.round(event.clientY - top + scrollY),
		};
		const index = Math.floor(click.y / 48);

		const files = Source.Entity.selected(app);
		const file = files[index];

		if (!file) return;

		if (
			click.x < getPixelPosition(file.timestamp.min) ||
			getPixelPosition(file.timestamp.max) < click.x
		) {
			return;
		}

		let events = getEventsListFromFileByClickX(click.x, file);
		if (events.length === 0) {
			events = getEventsListFromFileByClickX(click.x - 1, file);
		}
		if (events.length === 0) {
			events = getEventsListFromFileByClickX(click.x + 1, file);
		}

		LoggerHandler.canvasClick(file, events, click.x);

		if (events.length > 0) {
			if (events.length > 1) {
				setGroupDialogEvents(events);
				setGroupDialogKind("events");
				setGroupDialogAnchor({ x: event.clientX, y: event.clientY });
				return;
			}

			spawnDialog(<DisplayEventDialog event={events[0]} />);
		} else {
			Info.setTimelineTarget(null);
		}
	};

	// @ts-ignore
	window.xxc = handleClick;

	const [bounding, setBounding] = useState<DOMRect | null>(null);

	// Reset cached bounding rect when files change (e.g. operation switch)
	useEffect(() => {
		setBounding(null);
	}, [app.target.files]);

	// STABLE REFS: Store scrollX and scale in refs so the handleWheel callback
	// doesn't need them as dependencies. Without this, every scroll/zoom tick would
	// recreate handleWheel → recreate debouncedHandleWheel → remove/add the DOM
	// event listener, causing unnecessary overhead on every single wheel event.

	const scaleRef = useRef(app.timeline.scale);
	scaleRef.current = app.timeline.scale;

	/**
	 * Handles mouse wheel events for both horizontal scrolling and zoom.
	 * - Horizontal scroll: deltaX > deltaY → pan the canvas horizontally.
	 * - Zoom: deltaY dominant → adjust timeline scale around cursor position.
	 *
	 * Uses `scrollXRef` and `scaleRef` instead of direct state to keep the callback
	 * stable across renders (deps: only wrapper_ref, banner, Info, bounding).
	 * This prevents the debounced listener from being re-attached on every frame.
	 */
	const handleWheel = useCallback(
		(event: WheelEvent) => {
			if (!wrapper_ref.current || banner) return;

			// Horizontal scroll — pan only, no zoom
			if (Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
				scrollXRef.current += event.deltaX;
				requestCanvasRender("wheel:pan");
				syncScrollToContext(scrollXRef.current, scrollYRef.current);
				return;
			}

			// Cache bounding rect to avoid repeated getBoundingClientRect() calls
			const rect = bounding || wrapper_ref.current.getBoundingClientRect();
			if (!bounding) setBounding(rect);

			// Read current values from refs (always up-to-date, no dependency needed)
			const oldScale = pendingZoomScaleRef.current ?? scaleRef.current;
			const cursorX = event.clientX - rect.left;
			const currentScrollX =
				pendingZoomScrollXRef.current ?? scrollXRef.current;
			const contentX = currentScrollX + cursorX;

			// Calculate new scale based on scroll direction and user preference
			const shouldDecrease = Info.app.timeline.isScrollReversed
				? event.deltaY < 0
				: event.deltaY > 0;
			let newScale = shouldDecrease
				? oldScale - oldScale / 8
				: oldScale + oldScale / 8;

			const timeRange =
				Info.app.timeline.frame.max - Info.app.timeline.frame.min;
			const canvasWidth = wrapper_ref.current.clientWidth || 1000;
			const maxScale =
				timeRange > 0
					? timeRange / (Info.MIN_MS_PER_PIXEL * canvasWidth)
					: 9999999;
			newScale = Math.max(0.01, Math.min(maxScale, newScale));

			if (newScale === oldScale) return;

			// Update scale and recompute scrollX to keep cursor position anchored
			const nextScrollX = Math.round(
				contentX * (newScale / oldScale) - cursorX,
			);
			pendingZoomScaleRef.current = newScale;
			pendingZoomScrollXRef.current = nextScrollX;
			scrollXRef.current = nextScrollX;
			scheduleZoomCommit();
		},
		[
			wrapper_ref,
			banner,
			Info,
			bounding,
			requestCanvasRender,
			scheduleZoomCommit,
		],
	);

	/** Debounced wheel handler — coalesces rapid scroll events (5ms window). */
	const debouncedHandleWheel = useMemo(
		() => debounce(handleWheel, 5),
		[handleWheel],
	);

	useEffect(() => {
		CanvasIcon.onIconLoad = () => DataStore.markDirtySoon();
		return () => {
			CanvasIcon.onIconLoad = null;
		};
	}, []);

	// Observe wrapper size changes (e.g. when dialog panel is docked/undocked)
	// and mark canvas dirty so renderCanvas() detects the size mismatch immediately.
	useEffect(() => {
		const el = wrapper_ref.current;
		if (!el) return;
		const observer = new ResizeObserver(() => {
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
		}

		const unsubscribe = DataStore.subscribe(() => {
			requestCanvasRender("data-store");
		});

		if (theme) {
			DataStore.markDirtySoon();
		}

		return () => {
			unsubscribe();
		};
	}, [theme, requestCanvasRender]);

	useEffect(() => {
		const canvas = wrapper_ref.current;

		const handleNativeHover = (e: MouseEvent) => {
			if (!canvas) return;
			const rect = canvas.getBoundingClientRect();
			const newX = e.clientX - rect.left;
			const newY = e.clientY - rect.top;

			mouseXRef.current = newX;
			mouseYRef.current = newY;

			let currentHoveredItemId: string | null = null;
			for (let i = 0; i < RenderEngine.interactiveLinks.length; i++) {
				const item = RenderEngine.interactiveLinks[i];
				if (
					newX >= item.rect.x &&
					newX <= item.rect.x + item.rect.w &&
					newY >= item.rect.y &&
					newY <= item.rect.y + item.rect.h
				) {
					currentHoveredItemId = `link_${i}`;
					break;
				}
			}
			if (!currentHoveredItemId) {
				for (let i = 0; i < RenderEngine.interactiveNotes.length; i++) {
					const item = RenderEngine.interactiveNotes[i];
					if (
						newX >= item.rect.x &&
						newX <= item.rect.x + item.rect.w &&
						newY >= item.rect.y &&
						newY <= item.rect.y + item.rect.h
					) {
						currentHoveredItemId = `note_${i}`;
						break;
					}
				}
			}

			if (hoveredItemRef.current !== currentHoveredItemId) {
				hoveredItemRef.current = currentHoveredItemId;
				requestCanvasRender("hover");
			}
		};

		const resetHover = () => {
			mouseXRef.current = -1000;
			mouseYRef.current = -1000;

			if (hoveredItemRef.current !== null) {
				hoveredItemRef.current = null;
				requestCanvasRender("hover:reset");
			}
		};

		if (canvas) {
			canvas.addEventListener(
				"wheel",
				debouncedHandleWheel as unknown as EventListener,
				{ passive: true },
			);
			canvas.addEventListener("mousemove", move as any, { passive: true });
			canvas.addEventListener("contextmenu", handleContextMenu, {
				passive: true,
			});
			canvas.addEventListener("mousemove", handleNativeHover, {
				passive: true,
			});
			canvas.addEventListener("mouseleave", resetHover);
		}
		return () => {
			if (canvas) {
				canvas.removeEventListener(
					"wheel",
					debouncedHandleWheel as unknown as EventListener,
				);
				canvas.removeEventListener("mousemove", move as any);
				canvas.removeEventListener("contextmenu", handleContextMenu);
				canvas.removeEventListener("mousemove", handleNativeHover);
				canvas.removeEventListener("mouseleave", resetHover);
			}
			debouncedHandleWheel.cancel();
		};
	}, [wrapper_ref, debouncedHandleWheel, requestCanvasRender]);

	useEffect(() => {
		if (!didRenderInitialFrameRef.current) {
			didRenderInitialFrameRef.current = true;
			renderCanvas(false, undefined, "react:initial");
			return;
		}

		requestCanvasRender("react:deps");
	}, [
		scrollX,
		scrollY,
		app.target.files,
		app.timeline.frame,
		app.timeline.scale,
		app.timeline.target,
		app.timeline.filter,
		app.timeline.dialogSize,
		app.timeline.renderVersion,
		app.hidden,
		target,
		theme,
	]);

	useEffect(() => {
		return () => {
			if (pendingFrame.current) {
				cancelAnimationFrame(pendingFrame.current);
				pendingFrame.current = 0;
			}
			if (pendingZoomFrame.current !== null) {
				cancelAnimationFrame(pendingZoomFrame.current);
				pendingZoomFrame.current = null;
			}
		};
	}, []);

	const getPixelPosition = useCallback(
		(timestamp: number) => {
			return (
				Math.round(
					((timestamp - Info.app.timeline.frame.min) /
						(Info.app.timeline.frame.max - Info.app.timeline.frame.min)) *
						Info.width,
				) - scrollXRef.current
			);
		},
		[scrollXRef.current, Info.width, Info.app.timeline.frame],
	);

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

			const files = Source.Entity.selected(Info.app);
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
		setGroupDialogKind("events");
	}, []);

	const totalHeight = useMemo(() => {
		if (!canvas_ref.current) {
			return 1920;
		}
		const amount = Source.Entity.selected(app).length;

		return canvas_ref.current.height * 2 + amount * 48 - 80;
	}, [app.target.files, app.timeline.filter, canvas_ref]);

	const scrollbar = useRef<HTMLDivElement>(null);
	const isManualScroll = useRef(false);
	const isProgramScroll = useRef(false);

	const scrollBarEventHandler = useCallback(
		(a: React.UIEvent<HTMLDivElement>) => {
			if (isProgramScroll.current) {
				isProgramScroll.current = false;
				return;
			}

			const newScrollY =
				a.currentTarget.scrollTop - (canvas_ref.current?.height ?? 0);
			scrollYRef.current = newScrollY;

			requestCanvasRender("scrollbar");

			syncScrollToContext(scrollXRef.current, newScrollY);
			isManualScroll.current = true;
		},
		[canvas_ref, syncScrollToContext, requestCanvasRender],
	);

	useLayoutEffect(() => {
		if (isManualScroll.current || !scrollbar.current || !canvas_ref.current) {
			isManualScroll.current = false;
			return;
		}

		const newScrollTop = scrollY + (canvas_ref.current.height ?? 0);

		scrollbar.current.scroll({
			behavior: "instant",
			top: newScrollTop,
		});
		isProgramScroll.current = true;
	}, [scrollY, canvas_ref]);

	const handleKeyboardScroll = useCallback(
		(event: React.KeyboardEvent<HTMLElement>) => {
			const scrollYByKey: Record<string, number> = {
				ArrowDown: 48,
				ArrowUp: -48,
				PageDown: Math.max(48, Math.floor((canvas_ref.current?.height ?? 0) * 0.9)),
				PageUp: -Math.max(48, Math.floor((canvas_ref.current?.height ?? 0) * 0.9)),
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
			if (deltaX) scrollStore.setScrollX((x) => x + deltaX);
			if (deltaY) {
				const canvasHeight = canvas_ref.current?.height ?? 0;
				const maxScrollY = Math.max(
					-26,
					(scrollbar.current?.scrollHeight ?? totalHeight) -
						(scrollbar.current?.clientHeight ?? canvasHeight) -
						canvasHeight,
				);
				scrollStore.setScrollY((y) =>
					Math.min(maxScrollY, Math.max(-26, y + deltaY)),
				);
			}
		},
		[banner, totalHeight],
	);

	const operation = Operation.Entity.selected(app);
	const flaggedEvents = Doc.Entity.flag.getList(operation?.id);

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
				<Highlights.List.Overlay />
				<canvas
					className={s.canvas}
					ref={canvas_ref}
					id="canvas"
				/>
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
					width={canvas_ref.current?.clientWidth || 1}
					self={mousePosition}
					timestamp={getTimestamp(scrollX + mousePosition.x, Info)}
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
			{groupDialogAnchor && groupDialogEvents.length > 1 ? (
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
