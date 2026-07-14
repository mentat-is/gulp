import { DisplayEventDialog, EventIndicator } from "./Event.dialog";
import { Application } from "@/context/Application.context";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
	type CSSProperties,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
	useSyncExternalStore,
} from "react";
import { createPortal } from "react-dom";
import { Button } from "@/ui/Button";
import { Dialog } from "@/ui/Dialog";
import { Doc } from "@/entities/Doc";
import { Icon } from "@/ui/Icon";
import { Note } from "@/entities/Note";
import { Stack } from "@/ui/Stack";
import { format } from "date-fns";
import { Markdown } from "@/ui/Markdown";

import s from "./styles/DisplayGroupDialog.module.css";
import { Internal } from "@/entities/addon/Internal";
import { Locale } from "@/locales";
import { DataStore } from "@/store/DataStore";

type DisplayGroupTitleKey = `group.title.${DisplayGroupKind}`;

export type DisplayGroupKind = "events" | "links" | "notes";

const GROUP_TITLE_KEYS: Record<DisplayGroupKind, DisplayGroupTitleKey> = {
	events: "group.title.events",
	links: "group.title.links",
	notes: "group.title.notes",
};

// Cache for hover preview event queries.
class PreviewEventCache {
	private cache = new Map<string, Doc.Type>();
	private maxSize = 100;

	/**
	 * Reads a cached preview event by document ID.
	 *
	 * @param id Document ID used as the cache key.
	 * @returns Cached event data when available.
	 */
	get(id: string): Doc.Type | undefined {
		return this.cache.get(id);
	}

	/**
	 * Stores preview event data and evicts the oldest item when full.
	 *
	 * @param id Document ID used as the cache key.
	 * @param event Full event data to cache.
	 * @returns Nothing.
	 */
	set(id: string, event: Doc.Type): void {
		if (this.cache.size >= this.maxSize && !this.cache.has(id)) {
			const firstKey = this.cache.keys().next().value as string | undefined;
			if (firstKey) {
				this.cache.delete(firstKey);
			}
		}
		this.cache.set(id, event);
	}

	/**
	 * Checks whether preview event data exists in cache.
	 *
	 * @param id Document ID used as the cache key.
	 * @returns Whether cached preview data is available.
	 */
	has(id: string): boolean {
		return this.cache.has(id);
	}
}

const previewEventCache = new PreviewEventCache();
const VIEWPORT_PADDING = 12;
const GROUP_LIST_WIDTH = 450;
const GROUP_PREVIEW_WIDTH = 520;
const GROUP_POPUP_HEIGHT = 250;
const DOCKED_DIALOG_WIDTH_RATIO = 0.5;
const FLOATING_GAP = 14;

interface EventListRow {
	key: string;
	event: Doc.Type;
	note: Note.Type | null;
}

/**
 * Restricts a coordinate to a visible viewport range.
 *
 * @param value Coordinate to clamp.
 * @param min Minimum visible coordinate.
 * @param max Maximum visible coordinate.
 * @returns Coordinate constrained between min and max.
 */
const clampCoordinate = (value: number, min: number, max: number): number => {
	if (max < min) return min;
	return Math.min(Math.max(value, min), max);
};

/**
 * Resolves a fixed-position box that remains visible in the current viewport.
 *
 * @param anchor Anchor coordinate where the floating element should start.
 * @param preferredSize Preferred floating element dimensions.
 * @param viewportSize Current viewport dimensions.
 * @returns CSS positioning and dimensions for the visible floating element.
 */
const resolveAnchoredBoxStyle = (
	anchor: { x: number; y: number },
	preferredSize: { width: number; height: number },
	viewportSize: { width: number; height: number },
): CSSProperties => {
	const width = Math.min(
		preferredSize.width,
		Math.max(0, viewportSize.width - VIEWPORT_PADDING * 2),
	);
	const height = Math.min(
		preferredSize.height,
		Math.max(0, viewportSize.height - VIEWPORT_PADDING * 2),
	);
	const canOpenRight =
		anchor.x + 14 + width <= viewportSize.width - VIEWPORT_PADDING;
	const preferredLeft = canOpenRight
		? anchor.x + 14
		: anchor.x - width - 14;
	const top = clampCoordinate(
		anchor.y,
		VIEWPORT_PADDING,
		viewportSize.height - height - VIEWPORT_PADDING,
	);
	const left = clampCoordinate(
		preferredLeft,
		VIEWPORT_PADDING,
		viewportSize.width - width - VIEWPORT_PADDING,
	);

	return {
		position: "fixed",
		left,
		top,
		width,
		height,
		maxHeight: viewportSize.height - top - VIEWPORT_PADDING,
		maxWidth: viewportSize.width - left - VIEWPORT_PADDING,
		zIndex: 1000,
	};
};

/**
 * Resolves a fixed preview box beside the group dialog without moving it.
 *
 * @param groupRect Current event group dialog rectangle.
 * @param preferredWidth Preferred preview width.
 * @param viewportSize Current viewport dimensions.
 * @returns CSS positioning and dimensions for the preview portal.
 */
const resolveFloatingPreviewStyle = (
	groupRect: DOMRect,
	preferredWidth: number,
	viewportSize: { width: number; height: number },
): CSSProperties => {
	const rightSpace =
		viewportSize.width - VIEWPORT_PADDING - groupRect.right - FLOATING_GAP;
	const leftSpace = groupRect.left - VIEWPORT_PADDING - FLOATING_GAP;
	const shouldOpenRight =
		rightSpace >= preferredWidth ||
		(rightSpace >= leftSpace && leftSpace < preferredWidth);
	const availableSideWidth = shouldOpenRight ? rightSpace : leftSpace;
	const width = Math.min(preferredWidth, Math.max(0, availableSideWidth));
	const height = Math.min(
		groupRect.height,
		Math.max(0, viewportSize.height - VIEWPORT_PADDING * 2),
	);
	const left = shouldOpenRight
		? groupRect.right + FLOATING_GAP
		: groupRect.left - width - FLOATING_GAP;

	return {
		position: "fixed",
		left: clampCoordinate(
			left,
			VIEWPORT_PADDING,
			viewportSize.width - width - VIEWPORT_PADDING,
		),
		top: clampCoordinate(
			groupRect.top,
			VIEWPORT_PADDING,
			viewportSize.height - height - VIEWPORT_PADDING,
		),
		width,
		height,
		maxWidth: width,
		maxHeight: height,
		zIndex: 1001,
	};
};

/**
 * Sorts events by timestamp and removes duplicate documents by ID.
 *
 * @param events - Events passed by canvas, note, or link callers.
 * @returns Timestamp-sorted events with each document represented once.
 */
function sortUniqueEventsByTimestamp(events: Doc.Type[]): Doc.Type[] {
	const seenEventIds = new Set<string>();
	const sortedEvents = events.toSorted(
		(a, b) => a.gulp_timestamp - b.gulp_timestamp,
	);
	const uniqueEvents: Doc.Type[] = [];

	for (const event of sortedEvents) {
		if (seenEventIds.has(event._id)) {
			continue;
		}

		seenEventIds.add(event._id);
		uniqueEvents.push(event);
	}

	return uniqueEvents;
}

/**
 * Expands events into virtualized display rows, duplicating noted events per note.
 *
 * @param events - Timestamp-sorted events to display in the group list.
 * @param notes - Notes available in the current application state.
 * @returns Event rows with one blank-note row for unnoted events.
 */
function buildEventRows(events: Doc.Type[], notes: Note.Type[]): EventListRow[] {
	const eventIds = new Set(events.map((event) => event._id));
	const notesByEventId = new Map<string, Note.Type[]>();

	for (const note of notes) {
		const eventId = note.doc?._id;
		if (!eventId || !eventIds.has(eventId)) {
			continue;
		}

		const eventNotes = notesByEventId.get(eventId);
		if (eventNotes) {
			eventNotes.push(note);
		} else {
			notesByEventId.set(eventId, [note]);
		}
	}

	const rows: EventListRow[] = [];
	for (const event of events) {
		const eventNotes = notesByEventId.get(event._id);
		if (!eventNotes?.length) {
			rows.push({ key: event._id, event, note: null });
			continue;
		}

		for (const note of eventNotes) {
			rows.push({
				key: `${event._id}:${note.id}`,
				event,
				note,
			});
		}
	}

	return rows;
}

interface DisplayGroupDialogProps {
	events: Doc.Type[];
	anchor?: { x: number; y: number } | null;
	groupKind?: DisplayGroupKind;
	onClose?: () => void;
	preserveTimelineTarget?: boolean;
}

/**
 * Displays a sorted group of events either as an anchored popup or full dialog.
 *
 * @param props - Event collection, popup anchor, group kind, close callback, and timeline target behavior.
 * @returns Virtualized event list with hover-preview controls.
 */
export function DisplayGroupDialog({
	events,
	anchor,
	groupKind = "events",
	onClose,
	preserveTimelineTarget = false,
}: DisplayGroupDialogProps) {
	const { spawnDialog, Info, app, currentDocument } = Application.use();
	const { t } = Locale.use();
	const dataStoreVersion = useSyncExternalStore(
		DataStore.subscribe,
		DataStore.getSnapshot,
	);
	const menuShellRef = useRef<HTMLDivElement>(null);
	const floatingPreviewRef = useRef<HTMLElement>(null);
	const hasExpandedDockedDialogRef = useRef(false);
	const previewRequestIdRef = useRef(0);
	const [floatingGroupRect, setFloatingGroupRect] = useState<DOMRect | null>(null);
	const [hoveredEventId, setHoveredEventId] = useState<string | null>(null);
	const [hoveredEventData, setHoveredEventData] = useState<Doc.Type | null>(null);
	const [isLoadingHover, setIsLoadingHover] = useState(false);
	const [openPreviewRowKey, setOpenPreviewRowKey] = useState<string | null>(null);
	const [showPreview, setShowPreview] = useState(() => app.general.user?.user_data?.show_preview ?? true);
	const groupTitle = t(GROUP_TITLE_KEYS[groupKind]);
	const sortedEvents = useMemo(
		() => sortUniqueEventsByTimestamp(events),
		[events],
	);
	const eventRows = useMemo(
		() => buildEventRows(sortedEvents, DataStore.notes),
		[dataStoreVersion, sortedEvents],
	);

	useEffect(() => {
		setShowPreview(app.general.user?.user_data?.show_preview ?? true);
	}, [app.general.user?.user_data?.show_preview]);

	// Close on click outside
	useEffect(() => {
		if (!anchor) return;

		const handleClickOutside = (e: MouseEvent) => {
			const target = e.target as Node | null;
			const isInsidePreview =
				target && floatingPreviewRef.current?.contains(target);
			if (
				target &&
				!isInsidePreview &&
				menuShellRef.current &&
				!menuShellRef.current.contains(target)
			) {
				onClose?.();
			}
		};

		currentDocument.addEventListener("mousedown", handleClickOutside);
		return () =>
			currentDocument.removeEventListener("mousedown", handleClickOutside);
	}, [anchor, currentDocument, onClose]);

	useEffect(() => {
		if (!preserveTimelineTarget) {
			Info.setTimelineTarget(null);
		}
	}, [Info, preserveTimelineTarget]);

	useEffect(() => {
		if (!anchor || !menuShellRef.current) return;

		const menuElement = menuShellRef.current;

		/**
		 * Stores the latest anchored group bounds used by the external preview.
		 */
		const updateFloatingGroupRect = () => {
			setFloatingGroupRect(menuElement.getBoundingClientRect());
		};

		updateFloatingGroupRect();
		const activeWindow = currentDocument.defaultView ?? window;
		const resizeObserver = new ResizeObserver(updateFloatingGroupRect);
		resizeObserver.observe(menuElement);
		activeWindow.addEventListener("resize", updateFloatingGroupRect);

		return () => {
			resizeObserver.disconnect();
			activeWindow.removeEventListener("resize", updateFloatingGroupRect);
		};
	}, [anchor, currentDocument]);

	const parentRef = useRef<HTMLDivElement>(null);

	const virtualizer = useVirtualizer({
		count: eventRows.length,
		getScrollElement: () => parentRef.current,
		estimateSize: () => 44,
		overscan: 5,
	});

	/**
	 * Opens the selected event and keeps the timeline crosshair focused on it.
	 *
	 * @param event Event row selected by the user.
	 * @returns Nothing.
	 */
	const handleSelectEvent = useCallback(
		(event: Doc.Type) => {
			Info.setTimelineTarget(event);
			onClose?.();
			spawnDialog(null);
			spawnDialog(<DisplayEventDialog event={event} />);
		},
		[Info, onClose, spawnDialog],
	);

	/**
	 * Loads full event data for the row preview and reuses cached responses.
	 *
	 * @param event Event row currently hovered by the user.
	 * @returns A promise that resolves once preview data has been stored.
	 */
	const handleEventPreviewOpen = useCallback(
		async (event: Doc.Type) => {
			const requestId = previewRequestIdRef.current + 1;
			previewRequestIdRef.current = requestId;
			setHoveredEventId(event._id);
			setHoveredEventData(null);

			// Check cache first
			if (previewEventCache.has(event._id)) {
				setHoveredEventData(previewEventCache.get(event._id) || null);
				setIsLoadingHover(false);
				return;
			}

			// Query gulp for the event
			setIsLoadingHover(true);
			try {
				const opId = Doc.Entity.operationId(app, event);
				if (!opId) {
					console.warn("No opId found for event", event._id);
					return;
				}
				const fetchedEvent = await Info.query_single_id(event._id, opId);
				if (fetchedEvent) {
					previewEventCache.set(fetchedEvent._id, fetchedEvent);
					if (previewRequestIdRef.current === requestId) {
						setHoveredEventData(fetchedEvent);
					}
				} else {
					console.warn("No event returned from query");
				}
			} catch (error) {
				console.error("Failed to fetch event on hover:", error);
			} finally {
				if (previewRequestIdRef.current === requestId) {
					setIsLoadingHover(false);
				}
			}
		},
		[Info, app],
	);

	const viewportSize = useMemo(() => {
		const activeWindow = currentDocument.defaultView ?? window;
		return {
			width: activeWindow.innerWidth,
			height: activeWindow.innerHeight,
		};
	}, [currentDocument]);

	useEffect(() => {
		if (anchor || hasExpandedDockedDialogRef.current) return;

		const targetWidth = viewportSize.width * DOCKED_DIALOG_WIDTH_RATIO;
		hasExpandedDockedDialogRef.current = true;
		if (app.timeline.dialogSize < targetWidth) {
			Info.setDialogSize(targetWidth);
		}
	}, [Info, anchor, app.timeline.dialogSize, viewportSize.width]);

	/**
	 * Updates the persisted hover-preview preference for the current user.
	 *
	 * @param checked - Whether hover previews should be enabled.
	 */
	const handleShowPreviewChange = useCallback(
		(checked: boolean) => {
			setShowPreview(checked);
			if (!checked) {
				setOpenPreviewRowKey(null);
			}
			if (app.general.user) {
				void Info.user_set_data("show_preview", checked);
			}
		},
		[Info, app.general.user],
	);

	/**
	 * Renders the preview toggle as a compact icon control shared by every list shell.
	 *
	 * @returns Icon button that toggles event hover previews.
	 */
	const renderPreviewToggle = useCallback(
		() => (
			<Button
				variant="secondary"
				size="sm"
				icon={showPreview ? "PreviewEye" : "EyeOff"}
				title={t("common.showPreview")}
				aria-label={t("common.showPreview")}
				aria-pressed={showPreview}
				onClick={() => handleShowPreviewChange(!showPreview)}
			/>
		),
		[handleShowPreviewChange, showPreview, t],
	);

	/**
	 * Closes the docked group dialog and clears timeline focus consistently.
	 *
	 * @returns Nothing.
	 */
	const handleCloseDialog = useCallback(() => {
		onClose?.();
		spawnDialog(null);
		Info.setTimelineTarget(null);
	}, [Info, onClose, spawnDialog]);

	/**
	 * Renders the note metadata cell for a virtualized event row.
	 *
	 * @param note - Note attached to the row, or null for unnoted events.
	 * @returns Note icon and title when present, otherwise an empty stable cell.
	 */
	const renderNoteInfo = useCallback((note: Note.Type | null) => {
		if (!note) {
			return (
				<div
					className={s.noteInfo}
					aria-hidden="true"
				/>
			);
		}

		return (
			<div
				className={s.noteInfo}
				title={note.name}
				style={{ "--note-color": Note.Entity.color(note) } as CSSProperties}
			>
				<Icon
					name={Note.Entity.icon(note)}
					size={16}
				/>
				<span>{note.name}</span>
			</div>
		);
	}, []);

	/**
	 * Renders the event preview panel for the selected row.
	 *
	 * @returns Preview panel content for the current hovered row.
	 */
	const renderEventPreview = useCallback(() => {
		const selectedEvent = hoveredEventId
			? sortedEvents.find((event) => event._id === hoveredEventId)
			: null;

		if (!showPreview) {
			return null;
		}

		if (isLoadingHover && selectedEvent) {
			const content = <div className={s.previewLoading}>{t("common.loading")}</div>;
			if (anchor && floatingGroupRect) {
				return createPortal(
					<aside
						ref={floatingPreviewRef}
						className={`${s.previewPanel} ${s.floatingPreviewPanel}`}
						style={resolveFloatingPreviewStyle(
							floatingGroupRect,
							GROUP_PREVIEW_WIDTH,
							viewportSize,
						)}
					>
						{content}
					</aside>,
					currentDocument.body,
				);
			}

			return <aside className={s.previewPanel}>{content}</aside>;
		}

		if (!hoveredEventData || !selectedEvent) {
			if (anchor) return null;

			return <aside className={s.previewPanel}>
				<div className={s.previewLoading}>{t("common.preview")}</div>
			</aside>;
		}

		const content = (
			<div className={s.previewContent}>
				<p className={s.previewTitle}>{selectedEvent._id}</p>
				<p className={s.previewCode}>
					{selectedEvent["gulp.event_code"] || "N/A"}
				</p>
				<div className={s.previewMarkdown}>
					<Markdown
						scrollable={false}
						value={`\`\`\`json\n${JSON.stringify(hoveredEventData, null, 2)}\n\`\`\``}
					/>
				</div>
			</div>
		);

		if (anchor && floatingGroupRect) {
			return createPortal(
				<aside
					ref={floatingPreviewRef}
					className={`${s.previewPanel} ${s.floatingPreviewPanel}`}
					style={resolveFloatingPreviewStyle(
						floatingGroupRect,
						GROUP_PREVIEW_WIDTH,
						viewportSize,
					)}
				>
					{content}
				</aside>,
				currentDocument.body,
			);
		}

		return <aside className={s.previewPanel}>{content}</aside>;
	}, [anchor, currentDocument.body, floatingGroupRect, hoveredEventData, hoveredEventId, isLoadingHover, showPreview, sortedEvents, t, viewportSize]);

	/**
	 * Renders a single event row and updates the stable preview on hover.
	 *
	 * @param row - Event and note metadata to show in the list.
	 * @returns Event row that can open the event dialog.
	 */
	const renderEvent = useCallback(
		(row: EventListRow) => {
			const { event, note } = row;
			if (!event) return null;

			return (
				<Stack
					className={s.event}
					data-active={openPreviewRowKey === row.key}
					onClick={() => handleSelectEvent(event)}
					onMouseEnter={() => {
						if (!showPreview) return;
						if (anchor && menuShellRef.current) {
							setFloatingGroupRect(menuShellRef.current.getBoundingClientRect());
						}
						handleEventPreviewOpen(event);
						setOpenPreviewRowKey(row.key);
					}}
				>
					<EventIndicator event={event} />
					<div className={s.info}>
						<p className={s.id}>
							{`${format(new Date(event.gulp_timestamp), "yyyy-MM-dd HH:mm:ss")}.${String(Internal.Transformator.toNanos(event.gulp_timestamp) % 1_000_000n).padStart(6, "0")}`}{" "}
							| {event["gulp.event_code"]}
						</p>
						<span className={s.description}>{event._id}</span>
					</div>
					{renderNoteInfo(note)}
				</Stack>
			);
		},
		[handleSelectEvent, handleEventPreviewOpen, openPreviewRowKey, renderNoteInfo, showPreview],
	);

	/**
	 * Renders the sorted events through the virtualizer.
	 *
	 * @param listClassName - CSS class applied to the scroll container for the current shell.
	 * @returns Virtualized sorted event list.
	 */
	const renderSortedEventsList = useCallback(
		(listClassName: string) => (
			<div
				ref={parentRef}
				className={listClassName}
			>
				<div
					className={s.virtualSpacer}
					style={{ height: `${virtualizer.getTotalSize()}px` }}
				>
					{virtualizer.getVirtualItems().map((virtualItem) => {
						const row = eventRows[virtualItem.index];

						return (
							<div
								key={row?.key ?? virtualItem.key}
								className={s.virtualRow}
								style={{
									height: `${virtualItem.size}px`,
									transform: `translateY(${virtualItem.start}px)`,
								}}
							>
								{row ? renderEvent(row) : null}
							</div>
						);
					})}
				</div>
			</div>
		),
		[eventRows, renderEvent, virtualizer],
	);

	/**
	 * Renders the localized group header and shared preview/close controls.
	 *
	 * @param closeHandler Callback invoked when the close control is clicked.
	 * @param compact Whether the header is rendered in an anchored popup.
	 * @returns Header element for the group list shell.
	 */
	const renderHeader = useCallback(
		(closeHandler: () => void, compact: boolean) => (
			<div className={compact ? s.menuHeader : s.header}>
				<div className={s.titleContainer}>
					<div className={s.iconWrapper}>
						<Icon
							name="List"
							size={compact ? 14 : 18}
						/>
					</div>
					<h2 className={compact ? s.menuTitle : s.title}>{groupTitle}</h2>
				</div>
				<div className={s.buttonGroup}>
					{renderPreviewToggle()}
					<Button
						variant="secondary"
						size={compact ? "sm" : "default"}
						icon="X"
						title={t("common.closeDialog")}
						aria-label={t("group.closeEventList")}
						onClick={closeHandler}
					/>
				</div>
			</div>
		),
		[groupTitle, renderPreviewToggle, t],
	);

	const popupStyle = useMemo(() => {
		if (!anchor) return undefined;

		return resolveAnchoredBoxStyle(
			anchor,
			{
				width: GROUP_LIST_WIDTH,
				height: GROUP_POPUP_HEIGHT,
			},
			viewportSize,
		);
	}, [anchor, viewportSize]);

	if (anchor) {
		return createPortal(
			<div
				className={s.menuShell}
				style={popupStyle}
				ref={menuShellRef}
			>
				<div className={s.groupContent}>
					<div className={s.listPane}>
						{renderHeader(() => onClose?.(), true)}
						{renderSortedEventsList(s.menuBody)}
					</div>
					{renderEventPreview()}
				</div>
			</div>,
			currentDocument.body,
		);
	}

	return (
		<Dialog
			callback={onClose}
			className={s.dialog}
		>
			<div className={s.groupContent}>
				<div className={s.listPane}>
					{renderHeader(handleCloseDialog, false)}
					{renderSortedEventsList(s.dialogEventList)}
				</div>
				{renderEventPreview()}
			</div>
		</Dialog>
	);
}
