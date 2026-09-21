import { cloneQueryForCache } from "@/entities/QueryCache";
import type { Query } from "@/entities/Query";
import { cn, getTimestamp } from "@/ui/utils";
import s from "./styles/Navigator.module.css";
import { Application } from "@/context/Application.context";
import { scrollStore } from "@/store/scroll.store";
import {
	ChangeEvent,
	KeyboardEvent as ReactKeyboardEvent,
	ReactNode,
	RefObject,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { Popover } from "@/ui/Popover";
import { Logger } from "@/dto/Logger.class";
import { Highlights } from "@/overlays/Highlights";
import { Switch } from "@/ui/Switch";
import { Label } from "@/ui/Label";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Context } from "@/entities/Context";
import { App } from "@/entities/App";
import { Extension } from "@/context/Extension.context";
import { Filter } from "@/entities/Filter";
import { Source } from "@/entities/Source";
import { Locale } from "@/locales";
import { DetachedWindow } from "@/context/DetachedWindow.context";
import { applyNavigatorZoomStep } from "./NavigatorZoom";

export namespace Navigator {
	export interface Props extends Stack.Props {
		timeline: RefObject<HTMLDivElement>;
	}
}

const BUILT_IN_ASSISTANT_KEY = "built-in-ai-assistant";
const EVENT_FILTER_ID = "navigator-event-filter" as Filter.Id;
const EVENT_ORIGINAL_FIELD = "event.original";

type EventSearchType = "text" | "regexp";

interface AssistantWindowTarget {
	key: string;
	title: string;
	pluginFilename?: string;
}

/**
 * Formats the Navigator input for the selected event-search strategy.
 * @param value Raw value entered by the user.
 * @param searchType Event-search strategy selected in the Navigator.
 * @returns A trimmed regexp or one wildcard-wrapped pattern per whitespace-delimited word.
 */
function formatEventSearchValue(
	value: string,
	searchType: EventSearchType,
): string {
	const trimmedValue = value.trim();
	if (!trimmedValue || searchType === "regexp") return trimmedValue;
	return trimmedValue
		.split(/\s+/)
		.map((word) => `*${word}*`)
		.join(" ");
}

/**
 * Renders timeline controls backed by an axis-specific horizontal scroll subscription.
 * @param props Timeline element reference and Stack presentation properties.
 * @returns Navigator control row.
 */
export function Navigator({
	timeline,
	className,
	...props
}: Navigator.Props) {
	const {
		Info,
		app,
		spawnDialog,
		spawnBanner,
		setHighlightsOverlay,
		dialogsDocked,
		setDialogsDocked,
		isDetachedWindow,
	} = Application.use();
	const { t } = Locale.use();
	const detachedWindows = DetachedWindow.use();
	const [timestampPopoverOpen, setTimestampPopoverOpen] = useState(false);
	const [viewportTimestamp, setViewportTimestamp] = useState(() =>
		getTimestamp(
			scrollStore.getX() + (timeline.current?.clientWidth || 0),
			Info,
		),
	);
	const [timestampDraft, setTimestampDraft] = useState<number | null>(null);
	const timestamp = timestampDraft ?? viewportTimestamp;
	const [timestampInputValid, setTimestampInputValid] = useState<boolean>(true);

	useEffect(() => {
		if (!timestampPopoverOpen) return;

		/**
		 * Updates the visible timestamp editor from the current imperative viewport.
		 * @returns Nothing.
		 */
		const synchronizeViewportTimestamp = (): void => {
			setViewportTimestamp(
				getTimestamp(
					scrollStore.getX() + (timeline.current?.clientWidth || 0),
					Info,
				),
			);
		};

		synchronizeViewportTimestamp();
		return scrollStore.subscribeX(synchronizeViewportTimestamp);
	}, [
		Info,
		app.timeline.frame.max,
		app.timeline.frame.min,
		app.timeline.scale,
		timeline,
		timestampPopoverOpen,
	]);

	const isDialogPanelDetached = isDetachedWindow || !dialogsDocked;

	const [filterMode, setFilterMode] = useState<"files" | "events">("events");
	const [eventSearchType, setEventSearchType] =
		useState<EventSearchType>("text");
	const [localFilterValue, setLocalFilterValue] = useState("");

	// Keep source/context filtering out of the timeline while event search is active.
	useEffect(() => {
		if (filterMode === "events") {
			if (app.timeline.filter) Info.setTimelineFilter("");
			return;
		}
		setLocalFilterValue(app.timeline.filter);
	}, [app.timeline.filter, filterMode, Info]);

	/**
	 * Clears an invalid timestamp draft while retaining the invalid state.
	 * @returns Nothing.
	 */
	const resetTimestamp = (): void => {
		setTimestampInputValid(false);
		setTimestampDraft(0);
	};

	/**
	 * Validates and stores an explicitly edited timestamp draft.
	 * @param ev Timestamp input change event.
	 * @returns Nothing.
	 */
	const handleTimestampChangeHandler = (
		ev: ChangeEvent<HTMLInputElement>,
	): void => {
		const { value } = ev.target;
		const limits = Context.Entity.frame(app);
		if (!value) {
			Logger.error(
				`Expected number, got ${value}`,
				"Navigator.handleTimestampChangeHandler",
			);
			resetTimestamp();
			return;
		}

		const num = parseInt(value);
		if (isNaN(num) || num < 0) {
			Logger.error(
				`Expected number, got ${num}`,
				"Navigator.handleTimestampChangeHandler",
			);
			resetTimestamp();
			return;
		}

		setTimestampInputValid(num < limits.max && num > limits.min);
		setTimestampDraft(num);
	};

	/**
	 * Restores the live timestamp derived from the current viewport.
	 * @returns Nothing.
	 */
	const resetTimestampToInitialValue = (): void => {
		setTimestampDraft(null);
		setTimestampInputValid(true);
	};

	/**
	 * Focuses the Canvas on the current draft or live viewport timestamp.
	 * @returns Nothing.
	 */
	const goToTimestamp = () => {
		// @ts-ignore
		return window.focusCanvasOnEvent(timestamp, true);
	};

	/**
	 * Opens or focuses the detached assistant window for the selected target.
	 * @param target Assistant target selected from the registry-backed options.
	 * @returns Nothing.
	 */
	const openChatWindow = useCallback(
		(target: AssistantWindowTarget) => {
			detachedWindows.openAssistantWindow(target);
		},
		[detachedWindows],
	);

	const size_plus = useRef<HTMLButtonElement>(null);
	const size_reset = useRef<HTMLButtonElement>(null);
	const size_minus = useRef<HTMLButtonElement>(null);

	/**
	 * Restores the default timeline scale and horizontal viewport origin.
	 * @returns Nothing.
	 */
	const resetScaleAndScroll = (): void => {
		Info.setTimelineScale(1);
		scrollStore.setScrollX(0);
	};

	/**
	 * Adjusts timeline scale around the current viewport center.
	 * @param out Whether to zoom out instead of zooming in.
	 * @returns Nothing.
	 */
	const zoom = (out = false): void => {
		const timelineWidth = timeline.current?.clientWidth || 1;
		const currentScale = Info.app.timeline.scale;
		const viewport = applyNavigatorZoomStep({
			currentScale,
			currentScrollX: scrollStore.getX(),
			viewportWidth: timelineWidth,
			zoomOut: out,
			clampScale: Info.clampTimelineScale,
		});

		Info.setTimelineScale(viewport.scale);
		scrollStore.setScrollX(viewport.scrollX);
	};

	/**
	 * Routes timeline zoom keyboard shortcuts while preserving editable controls.
	 * @param event Native keyboard event emitted by the window.
	 * @returns Nothing.
	 */
	const handleControllers = (event: KeyboardEvent): void => {
		const target = event.target as HTMLElement;
		const tag = target.tagName.toLowerCase();
		if (
			["input", "textarea", "select"].includes(tag) ||
			target.isContentEditable
		)
			return;

		switch (true) {
			case event.key === "-":
				size_plus.current?.click();
				break;

			case event.key === "=":
				resetScaleAndScroll();
				break;

			case event.key === "+":
				size_minus.current?.click();
				break;

			default:
				break;
		}
	};

	useEffect(() => {
		window.addEventListener("keypress", handleControllers);

		return () => {
			window.removeEventListener("keypress", handleControllers);
		};
	}, []);

	/**
	 * Toggles the interactive highlight-creation overlay.
	 * @returns Nothing.
	 */
	const createHighlightButtonClickHandler = (): void => {
		setHighlightsOverlay((prev) =>
			prev ? null : <Highlights.Create.Overlay />,
		);
	};

	/**
	 * Triggers the search based on the current mode and value.
	 * For 'files' mode, it updates the global timeline filter.
	 * For 'events' mode, it updates OpenSearch filters and triggers a refetch.
	 * @param value The search term.
	 * @param mode The current filter mode ('files' or 'events').
	 * @returns Nothing.
	 */
	const triggerSearch = useCallback(
		(value: string, mode: "files" | "events") => {
			if (mode === "files") {
				Info.setTimelineFilter(value);
				scrollStore.setScroll(0, -26);
			} else {
				let hasChanges = false;
				const queriesBySourceId: Partial<Record<Source.Id, Query.Type>> = {};
				const sources = Source.Entity.selected(app);
				if (sources.length === 0) return;
				const filterValue = formatEventSearchValue(value, eventSearchType);
				const filterType: Filter.Condition =
					eventSearchType === "text" ? "wildcard" : "regexp";

				sources.forEach((source: Source.Type) => {
					const query = cloneQueryForCache(Info.getQuery(source));
					const existingFilterIndex = query.filters.findIndex(
						(f) => f.id === EVENT_FILTER_ID,
					);

					if (filterValue === "") {
						if (existingFilterIndex !== -1) {
							query.filters.splice(existingFilterIndex, 1);
							queriesBySourceId[source.id] = query;
							hasChanges = true;
						}
					} else {
						const newFilter = Filter.Entity.createCondition(
							EVENT_FILTER_ID,
							{
								field: EVENT_ORIGINAL_FIELD,
								value: filterValue,
								type: filterType,
								case_insensitive: true,
							},
						);
						if (existingFilterIndex !== -1) {
							query.filters[existingFilterIndex] = newFilter;
						} else {
							query.filters.push(newFilter);
						}
						query.fieldTypeMap = {
							...query.fieldTypeMap,
							[EVENT_ORIGINAL_FIELD]: "wildcard",
						};
						queriesBySourceId[source.id] = query;
						hasChanges = true;
					}
				});

				if (hasChanges) {
					void Info.refetch({
						ids: sources.map((s: Source.Type) => s.id),
						queriesBySourceId,
					});
					scrollStore.setScroll(0, -26);
				}
			}
		},
		[app, Info, eventSearchType],
	);

	/**
	 * Handles filter input changes.
	 * Triggers instant auto-search in 'files' mode, but waits for manual trigger in 'events' mode.
	 * @param e Change event from the input.
	 * @returns Nothing.
	 */
	const handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
		const value = e.target.value;
		setLocalFilterValue(value);
		if (filterMode === "files") {
			Info.setTimelineFilter(value);
			scrollStore.setScroll(0, -26);
		}
	};

	/**
	 * Handles key down events on the search input.
	 * Triggers search when 'Enter' is pressed.
	 * @param e Keyboard event from the input.
	 * @returns Nothing.
	 */
	const handleKeyDown = (e: ReactKeyboardEvent<HTMLInputElement>): void => {
		if (e.key === "Enter") {
			triggerSearch(localFilterValue, filterMode);
		}
	};

	/**
	 * Stores a valid single event-search strategy selected from the dropdown.
	 * @param value Value emitted by the shared Select component.
	 * @returns Nothing.
	 */
	const handleEventSearchTypeChange = (value: string): void => {
		if (value === "text" || value === "regexp") {
			setEventSearchType(value);
		}
	};

	/**
	 * Toggles between 'files' and 'events' filter modes.
	 * When switching back to 'files', it clears any active event filters.
	 * @returns Nothing.
	 */
	const handleIconClick = useCallback(() => {
		const nextMode = filterMode === "files" ? "events" : "files";

		// Reset input value when switching modes to provide a clean state for the new mode
		setLocalFilterValue("");
		Info.setTimelineFilter("");

		if (filterMode === "events") {
			// Cleanup: remove event filters when switching back to file filtering
			let hasChanges = false;
			const queriesBySourceId: Partial<Record<Source.Id, Query.Type>> = {};
			const sources = Source.Entity.selected(app);
			sources.forEach((source: Source.Type) => {
				const query = cloneQueryForCache(Info.getQuery(source));
				const existingFilterIndex = query.filters.findIndex(
					(f) => f.id === EVENT_FILTER_ID,
				);
				if (existingFilterIndex !== -1) {
					query.filters.splice(existingFilterIndex, 1);
					queriesBySourceId[source.id] = query;
					hasChanges = true;
				}
			});

			if (hasChanges) {
				void Info.refetch({
					ids: sources.map((s: Source.Type) => s.id),
					queriesBySourceId,
				});
			}
		}

		setFilterMode(nextMode);
	}, [filterMode, app, Info]);

	const { extensions } = Extension.use();
	const assistantPlugins = useMemo(
		() => Extension.getBySlot(extensions, Extension.Slot.AIAssistantWindow),
		[extensions],
	);
	const assistantTargets = useMemo<AssistantWindowTarget[]>(
		() => [
			{
				key: BUILT_IN_ASSISTANT_KEY,
				title: t("aiAssistant.title"),
			},
			...assistantPlugins.map((plugin) => ({
				key: plugin.filename,
				title: plugin.display_name || plugin.filename,
				pluginFilename: plugin.filename,
			})),
		],
		[assistantPlugins, t],
	);
	const [selectionOpen, setSelectionOpen] = useState(false);

	/**
	 * Opens the assistant directly when only one target exists, otherwise opens the selector.
	 * @returns Nothing.
	 */
	const handleChatButtonClick = (): void => {
		if (assistantTargets.length > 1) {
			setSelectionOpen(true);
		} else {
			openChatWindow(assistantTargets[0]);
		}
	};

	/**
	 * Opens the selected assistant target and closes the selector.
	 * @param target Assistant target selected by the user.
	 * @returns Nothing.
	 */
	const selectChat = (target: AssistantWindowTarget): void => {
		openChatWindow(target);
		setSelectionOpen(false);
	};

	return (
		<Stack
			pos="relative"
			gap={5}
			flex={0}
			className={cn(className, s.navigator)}
			{...props}
		>
			<Button
				variant="tertiary"
				title={t("navigator.zoomIn")}
				ref={size_minus}
				onClick={() => zoom(false)}
				icon="ZoomIn"
				size="md"
			/>
			<Button
				variant="tertiary"
				title={t("navigator.zoomOut")}
				ref={size_plus}
				onClick={() => zoom(true)}
				icon="ZoomOut"
				size="md"
			/>
			<Button
				variant="tertiary"
				title={t("navigator.resetScale")}
				ref={size_reset}
				onClick={resetScaleAndScroll}
				icon="AlignHorizontalSpaceBetween"
				size="md"
			/>
			<Button
				variant="tertiary"
				title={t("navigator.createHighlight")}
				icon="ChartBarBig"
				onClick={createHighlightButtonClickHandler}
				size="md"
			/>
			<Input
				className={s.filter}
				variant="highlighted"
				value={localFilterValue}
				placeholder={
					filterMode === "files"
						? t("navigator.filterFilesPlaceholder")
						: t("navigator.filterEventsPlaceholder")
				}
				onChange={handleFilterChange}
				onKeyDown={handleKeyDown}
				icon={filterMode === "files" ? "Filter" : "Activity"}
				onIconClick={handleIconClick}
				iconTitle={
					filterMode === "files"
						? t("navigator.switchToEventFiltering")
						: t("navigator.switchToFileFiltering")
				}
				selector={
					filterMode === "events"
						? {
								values: [
									{
										key: "text",
										label: t("common.text"),
										icon: "ALargeSmall",
									},
									{
										key: "regexp",
										label: t("queryBuilder.condition.regexp"),
										icon: "Regex",
									},
								],
								selected: eventSearchType,
								onSelectedChange: handleEventSearchTypeChange,
							}
						: undefined
				}
			/>
			<Button
				variant="tertiary"
				title={t("common.search")}
				icon="Search"
				onClick={() => triggerSearch(localFilterValue, filterMode)}
				size="md"
			/>
			<Popover.Root
				open={timestampPopoverOpen}
				onOpenChange={setTimestampPopoverOpen}
			>
				<Popover.Trigger asChild>
					<Button
						size="md"
						variant="tertiary"
						title={t("navigator.visibilitySettings")}
						icon="Settings2"
					/>
				</Popover.Trigger>
				<Popover.Content>
					<Stack
						dir="column"
						gap={4}
						ai="stretch"
					>
						{(
							Object.keys(app.hidden) as unknown as Array<
								keyof App.Type["hidden"]
							>
						).map((key) => {
							return (
								<Stack
									key={key}
									jc="space-between"
								>
									<Label value={t(`navigator.visibility.${key}`)} />
									<Switch
										checked={!app.hidden[key]}
										onCheckedChange={() => {
											const willShow = app.hidden[key];
											Info.toggle_visibility(key);
											if (key === "sourcesWithNoEvents" && willShow) {
												void Info.sync({ includeEmptySources: true });
											}
										}}
									/>
								</Stack>
							);
						})}
					</Stack>
				</Popover.Content>
			</Popover.Root>
			<Popover.Root>
				<Popover.Trigger asChild>
					<Button
						size="md"
						variant="tertiary"
						title={t("navigator.goToTimestamp")}
						icon="Clock"
					/>
				</Popover.Trigger>
				<Popover.Content className={s.goto}>
					<Stack
						dir="column"
						ai="flex-start"
					>
						<p>{t("navigator.goToTimestamp")}:</p>
						<Stack>
							<Input
								variant="highlighted"
								icon="Clock"
								value={timestamp}
								valid={timestampInputValid}
								onChange={handleTimestampChangeHandler}
							/>
							<Button
								icon="Undo2"
								variant="tertiary"
								onClick={resetTimestampToInitialValue}
							/>
							<Button
								icon="Check"
								variant="glass"
								onClick={goToTimestamp}
							/>
						</Stack>
					</Stack>
				</Popover.Content>
			</Popover.Root>
			{assistantTargets.length > 1 ? (
				<Popover.Root
					open={selectionOpen}
					onOpenChange={setSelectionOpen}
				>
					<Popover.Trigger asChild>
						<Button
							variant={selectionOpen ? "default" : "tertiary"}
							title={t("navigator.selectChatVersion")}
							icon="Sparkles"
							onClick={handleChatButtonClick}
							size="md"
						/>
					</Popover.Trigger>
					<Popover.Content>
						<Stack
							dir="column"
							gap={3}
							ai="stretch"
							className={s.chat}
						>
							<Label
								value={t("navigator.selectChatVersion")}
								style={{ whiteSpace: "nowrap" }}
							/>
							<Stack>
								{assistantTargets.map((target, index) => (
									<Button
										key={target.key}
										variant={index === 0 ? "glass" : "default"}
										onClick={() => selectChat(target)}
										icon={target.pluginFilename ? "Sparkles" : "Sparkle"}
										title={target.title}
									>
										{target.title}
									</Button>
								))}
							</Stack>
						</Stack>
					</Popover.Content>
				</Popover.Root>
			) : (
				<Button
					variant="tertiary"
					title={t("navigator.openAiAssistantChat")}
					icon="Sparkle"
					onClick={handleChatButtonClick}
					size="md"
				/>
			)}
			<Button
				variant="tertiary"
				title={
					isDialogPanelDetached
						? t("navigator.dockDialogPanel")
						: t("navigator.undockDialogPanel")
				}
				icon={isDialogPanelDetached ? "PanelLeftOpen" : "PictureInPicture2"}
				onClick={() => setDialogsDocked((value) => !value)}
				size="md"
			/>

			{/* Event/Group use routed roots; arbitrary plugin dialogs use an opener portal. */}
		</Stack>
	);
}
