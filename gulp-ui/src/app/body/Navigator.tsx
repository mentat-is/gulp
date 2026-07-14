import { cn } from "@/ui/utils";
import s from "./styles/Navigator.module.css";
import { Application } from "@/context/Application.context";
import { useScroll, scrollStore } from "@/store/scroll.store";
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

export namespace Navigator {
	export interface Props extends Stack.Props {
		timeline: RefObject<HTMLDivElement>;
		timestamp: number;
	}
}

const BUILT_IN_ASSISTANT_KEY = "built-in-ai-assistant";

interface AssistantWindowTarget {
	key: string;
	title: string;
	pluginFilename?: string;
}

export function Navigator({
	timeline,
	className,
	timestamp: _timestamp,
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
	const { x: scrollX } = useScroll();
	const [timestamp, setTimestamp] = useState<number>(_timestamp);
	const [timestampInputValid, setTimestampInputValid] = useState<boolean>(true);

	const isDialogPanelDetached = isDetachedWindow || !dialogsDocked;

	const [filterMode, setFilterMode] = useState<"files" | "events">("files");
	const EVENT_FILTER_ID = "navigator-event-filter" as Filter.Id;

	const [localFilterValue, setLocalFilterValue] = useState(app.timeline.filter);

	// Sync local filter value with global app state
	useEffect(() => {
		setLocalFilterValue(app.timeline.filter);
	}, [app.timeline.filter]);

	useEffect(() => {
		setTimestamp(_timestamp);
	}, [_timestamp]);

	const resetTimestamp = () => {
		setTimestampInputValid(false);
		setTimestamp(0);
	};

	const handleTimestampChangeHandler = (ev: ChangeEvent<HTMLInputElement>) => {
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
		setTimestamp(num);
	};

	const resetTimestampToInitialValue = () => {
		setTimestamp(_timestamp);
		setTimestampInputValid(true);
	};

	const goToTimestamp = () => {
		// @ts-ignore
		return window.focusCanvasOnEvent(timestamp, true);
	};

	/**
	 * Opens or focuses the detached assistant window for the selected target.
	 * @param target Assistant target selected from the registry-backed options.
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

	const resetScaleAndScroll = () => {
		Info.setTimelineScale(1);
		scrollStore.setScrollX(0);
	};

	const zoom = (out = false) => {
		const timelineWidth = timeline.current?.clientWidth || 1;
		const currentScale = app.timeline.scale;

		const newScale = out
			? currentScale - currentScale / 4
			: currentScale + currentScale / 4;

		const clampedScale = Math.min(Math.max(newScale, 0.01), 9999999);

		const centerOffset = scrollX + timelineWidth / 2;
		const scaledOffset = (centerOffset * clampedScale) / currentScale;
		const left = scaledOffset - centerOffset;

		Info.setTimelineScale(clampedScale);
		scrollStore.setScrollX((x) => Math.round(x + left));
	};

	const handleControllers = (event: KeyboardEvent) => {
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

	const createHighlightButtonClickHandler = () => {
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
	 */
	const triggerSearch = useCallback(
		(value: string, mode: "files" | "events") => {
			if (mode === "files") {
				Info.setTimelineFilter(value);
				scrollStore.setScroll(0, -26);
			} else {
				let hasChanges = false;
				const sources = Source.Entity.selected(app);
				if (sources.length === 0) return;

				sources.forEach((source: Source.Type) => {
					const query = Info.getQuery(source);
					const existingFilterIndex = query.filters.findIndex(
						(f) => f.id === EVENT_FILTER_ID,
					);
					const trimmedValue = value.trim();

					if (trimmedValue === "") {
						if (existingFilterIndex !== -1) {
							query.filters.splice(existingFilterIndex, 1);
							Info.setQuery(source, query);
							hasChanges = true;
						}
					} else {
						const newFilter: Filter.Type = {
							id: EVENT_FILTER_ID,
							type: "wildcard" as any,
							operator: "must" as any,
							field: "event.original",
							value: trimmedValue,
							enabled: true,
							case_insensitive: true,
						};

						if (existingFilterIndex !== -1) {
							query.filters[existingFilterIndex] = newFilter;
						} else {
							query.filters.push(newFilter);
						}
						Info.setQuery(source, query);
						hasChanges = true;
					}
				});

				if (hasChanges) {
					Info.refetch({ ids: sources.map((s: Source.Type) => s.id) });
					scrollStore.setScroll(0, -26);
				}
			}
		},
		[app, Info, EVENT_FILTER_ID],
	);

	/**
	 * Handles filter input changes.
	 * Triggers instant auto-search in 'files' mode, but waits for manual trigger in 'events' mode.
	 * @param e Change event from the input.
	 */
	const handleFilterChange = (e: ChangeEvent<HTMLInputElement>) => {
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
	 */
	const handleKeyDown = (e: ReactKeyboardEvent<HTMLInputElement>) => {
		if (e.key === "Enter") {
			triggerSearch(localFilterValue, filterMode);
		}
	};

	/**
	 * Toggles between 'files' and 'events' filter modes.
	 * When switching back to 'files', it clears any active event filters.
	 */
	const handleIconClick = useCallback(() => {
		const nextMode = filterMode === "files" ? "events" : "files";

		// Reset input value when switching modes to provide a clean state for the new mode
		setLocalFilterValue("");
		Info.setTimelineFilter("");

		if (filterMode === "events") {
			// Cleanup: remove event filters when switching back to file filtering
			let hasChanges = false;
			const sources = Source.Entity.selected(app);
			sources.forEach((source: Source.Type) => {
				const query = Info.getQuery(source);
				const existingFilterIndex = query.filters.findIndex(
					(f) => f.id === EVENT_FILTER_ID,
				);
				if (existingFilterIndex !== -1) {
					query.filters.splice(existingFilterIndex, 1);
					Info.setQuery(source, query);
					hasChanges = true;
				}
			});

			if (hasChanges) {
				Info.refetch({ ids: sources.map((s: Source.Type) => s.id) });
			}
		}

		setFilterMode(nextMode);
	}, [filterMode, app, Info, EVENT_FILTER_ID]);

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
	 */
	const handleChatButtonClick = () => {
		if (assistantTargets.length > 1) {
			setSelectionOpen(true);
		} else {
			openChatWindow(assistantTargets[0]);
		}
	};

	/**
	 * Opens the selected assistant target and closes the selector.
	 * @param target Assistant target selected by the user.
	 */
	const selectChat = (target: AssistantWindowTarget) => {
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
			/>
			<Button
				variant="tertiary"
				title={t("common.search")}
				icon="Search"
				onClick={() => triggerSearch(localFilterValue, filterMode)}
				size="md"
			/>
			<Popover.Root>
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

			{/* Detached windows now use separate React roots via createRoot. */}
		</Stack>
	);
}
