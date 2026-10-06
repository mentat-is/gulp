import {
	type CSSProperties,
	type RefObject,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { createPortal } from "react-dom";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
	closestCenter,
	DndContext,
	DragOverlay,
	KeyboardSensor,
	PointerSensor,
	type Announcements,
	type DragEndEvent,
	type DragStartEvent,
	useSensor,
	useSensors,
} from "@dnd-kit/core";
import {
	sortableKeyboardCoordinates,
	SortableContext,
	useSortable,
	verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { Button } from "@/ui/Button";
import { Icon } from "@/ui/Icon";
import { Input } from "@/ui/Input";
import { Popover } from "@/ui/Popover";
import { cn } from "@/ui/utils";
import { Locale } from "@/locales";
import s from "./styles/FieldVisibilityMenu.module.css";

const ITEM_HEIGHT = 30;
const ITEM_OVERSCAN = 8;
const MENU_CHROME_HEIGHT = 160;
const DEFAULT_LIST_HEIGHT = 320;
const MENU_SIDE_OFFSET = 4;
const VIEWPORT_MARGIN = 8;

interface FrozenPopoverAnchor {
	/** Returns the trigger bounds captured when the popover opened. */
	getBoundingClientRect: () => DOMRect;
}

export namespace FieldVisibilityMenu {
	export type Terminology = "columns" | "fields";

	export interface Props {
		/** Complete canonical item order, including hidden items. */
		orderedItems: string[];
		/** Items currently excluded from the consumer view. */
		hiddenItems: ReadonlySet<string>;
		/** Toggles one item between the visible and hidden lists. */
		onToggleItem: (item: string) => void;
		/** Makes every item visible. */
		onSelectAll: () => void;
		/** Hides every item. */
		onDeselectAll: () => void;
		/** Moves one visible item relative to another. */
		onMoveItem: (activeItem: string, overItem: string) => void;
		/** Selects column-oriented or field-oriented localized copy. */
		terminology?: Terminology;
		/** Resolves a display label without changing the persisted item key. */
		getItemLabel?: (item: string) => string;
		/** Owner-document portal target, including detached windows. */
		portalContainer?: HTMLElement | null;
		/** Optional viewport whose height also limits the open menu. */
		heightBoundaryRef?: RefObject<HTMLElement | null>;
		/** Optional trigger styling supplied by the consumer. */
		triggerClassName?: string;
		/** Prevents opening while configuration data is unavailable. */
		disabled?: boolean;
		/** Notifies a consumer when the popover opens or closes. */
		onOpenChange?: (open: boolean) => void;
	}
}

interface ItemOptionContentProps {
	checked: boolean;
	label: string;
	onToggle: () => void;
}

/**
 * Renders the selectable content shared by sortable and static menu entries.
 * @param props Visibility state, label, and toggle callback.
 * @returns A checkbox-like item toggle.
 */
function ItemOptionContent({
	checked,
	label,
	onToggle,
}: ItemOptionContentProps) {
	return (
		<button
			type="button"
			className={s.toggle}
			role="checkbox"
			aria-checked={checked}
			onClick={onToggle}
		>
			<span className={s.indicator}>
				{checked ? <Icon name="Check" size={14} color="currentColor" /> : null}
			</span>
			<span className={s.optionLabel}>{label}</span>
		</button>
	);
}

interface StaticItemOptionProps extends ItemOptionContentProps {
	item: string;
	height: number;
	start: number;
}

/**
 * Renders one virtualized non-sortable menu entry.
 * @param props Item content and virtual row geometry.
 * @returns An absolutely positioned menu option.
 */
function StaticItemOption({
	item,
	height,
	start,
	...contentProps
}: StaticItemOptionProps) {
	return (
		<div
			className={s.virtualItem}
			style={{ height, transform: `translateY(${start}px)` }}
		>
			<div className={s.option} data-column-menu-option={item}>
				<ItemOptionContent {...contentProps} />
			</div>
		</div>
	);
}

interface SortableItemOptionProps extends StaticItemOptionProps {
	dragLabel: string;
	sortingDisabled: boolean;
}

/**
 * Renders one virtualized item with pointer and keyboard drag controls.
 * @param props Item content, virtual geometry, and sorting state.
 * @returns A sortable menu option.
 */
function SortableItemOption({
	item,
	height,
	start,
	dragLabel,
	sortingDisabled,
	...contentProps
}: SortableItemOptionProps) {
	const {
		attributes,
		listeners,
		setNodeRef,
		transform,
		transition,
		isDragging,
	} = useSortable({ id: item, disabled: sortingDisabled });

	return (
		<div
			className={s.virtualItem}
			style={{ height, transform: `translateY(${start}px)` }}
		>
			<div
				ref={setNodeRef}
				className={cn(s.option, isDragging && s.optionDragging)}
				data-column-menu-option={item}
				style={{
					transform: CSS.Transform.toString(transform) ?? "none",
					transition: transition ?? "none",
				}}
			>
				<ItemOptionContent {...contentProps} />
				<button
					type="button"
					className={s.dragHandle}
					disabled={sortingDisabled}
					aria-label={dragLabel}
					title={dragLabel}
					{...attributes}
					{...listeners}
				>
					<Icon name="GripVertical" size={14} color="currentColor" />
				</button>
			</div>
		</div>
	);
}

/**
 * Renders a reusable searchable visibility and ordering menu.
 * @param props Controlled item state, persistence callbacks, and portal context.
 * @returns A menu trigger and its portaled popover.
 */
export function FieldVisibilityMenu({
	orderedItems,
	hiddenItems,
	onToggleItem,
	onSelectAll,
	onDeselectAll,
	onMoveItem,
	terminology = "columns",
	getItemLabel = (item) => item,
	portalContainer,
	heightBoundaryRef,
	triggerClassName,
	disabled = false,
	onOpenChange,
}: FieldVisibilityMenu.Props) {
	const { t } = Locale.use();
	const [isOpen, setIsOpen] = useState(false);
	const [search, setSearch] = useState("");
	const [menuMaxHeight, setMenuMaxHeight] = useState<number>();
	const [activeDraggedItem, setActiveDraggedItem] = useState<string | null>(null);
	const triggerRef = useRef<HTMLButtonElement | null>(null);
	const anchorRef = useRef<FrozenPopoverAnchor>({
		getBoundingClientRect: () =>
			triggerRef.current?.getBoundingClientRect() ?? new DOMRect(),
	});
	const searchRef = useRef<HTMLInputElement | null>(null);
	const selectedListRef = useRef<HTMLDivElement | null>(null);
	const unselectedListRef = useRef<HTMLDivElement | null>(null);
	const sensors = useSensors(
		useSensor(PointerSensor, { activationConstraint: { distance: 4 } }),
		useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
	);

	const isFieldTerminology = terminology === "fields";
	const title = isFieldTerminology
		? t("eventDialog.fieldVisibility")
		: t("table.columnVisibility");
	const selectedLabel = isFieldTerminology
		? t("eventDialog.selectedFields")
		: t("table.selectedColumns");
	const unselectedLabel = isFieldTerminology
		? t("eventDialog.unselectedFields")
		: t("table.unselectedColumns");
	const emptyLabel = isFieldTerminology
		? t("eventDialog.noFields")
		: t("table.noColumns");
	const selectAllLabel = isFieldTerminology
		? t("eventDialog.selectAllFields")
		: t("table.selectAllColumns");
	const deselectAllLabel = isFieldTerminology
		? t("eventDialog.deselectAllFields")
		: t("table.deselectAllColumns");

	/** Captures stable trigger geometry and computes the menu height limit. */
	const handleOpenChange = useCallback(
		(open: boolean) => {
			if (open) {
				const trigger = triggerRef.current;
				const triggerRect = trigger?.getBoundingClientRect();
				if (triggerRect) {
					anchorRef.current = {
						getBoundingClientRect: () => triggerRect,
					};
				}

				setSearch("");
				const boundaryHeight = heightBoundaryRef?.current?.clientHeight;
				const ownerWindow = trigger?.ownerDocument.defaultView;
				const viewportHeight = ownerWindow?.innerHeight;
				const viewportAvailableHeight = triggerRect && viewportHeight
					? Math.max(
						0,
						Math.max(
							viewportHeight -
								triggerRect.bottom -
								MENU_SIDE_OFFSET -
								VIEWPORT_MARGIN,
							triggerRect.top - MENU_SIDE_OFFSET - VIEWPORT_MARGIN,
						),
					)
					: undefined;
				const availableHeights = [boundaryHeight, viewportAvailableHeight].filter(
					(height): height is number => !!height && height > 0,
				);
				setMenuMaxHeight(
					availableHeights.length > 0
						? Math.floor(Math.min(...availableHeights))
						: undefined,
				);
			}

			setIsOpen(open);
			if (!open) setActiveDraggedItem(null);
			onOpenChange?.(open);
		},
		[heightBoundaryRef, onOpenChange],
	);

	useEffect(() => {
		if (!isOpen) return;
		const ownerWindow = searchRef.current?.ownerDocument.defaultView;
		const timeoutId = ownerWindow?.setTimeout(
			() => searchRef.current?.focus({ preventScroll: true }),
			0,
		);
		return () => {
			if (timeoutId !== undefined) ownerWindow?.clearTimeout(timeoutId);
		};
	}, [isOpen]);

	const normalizedSearch = search.trim().toLocaleLowerCase();
	const matchesSearch = useCallback(
		(item: string) =>
			!normalizedSearch ||
			getItemLabel(item).toLocaleLowerCase().includes(normalizedSearch),
		[getItemLabel, normalizedSearch],
	);
	const selectedItems = useMemo(
		() =>
			orderedItems.filter(
				(item) => !hiddenItems.has(item) && matchesSearch(item),
			),
		[hiddenItems, matchesSearch, orderedItems],
	);
	const unselectedItems = useMemo(
		() =>
			orderedItems.filter(
				(item) => hiddenItems.has(item) && matchesSearch(item),
			),
		[hiddenItems, matchesSearch, orderedItems],
	);
	const selectedVirtualizer = useVirtualizer({
		count: selectedItems.length,
		getScrollElement: () => selectedListRef.current,
		estimateSize: () => ITEM_HEIGHT,
		overscan: ITEM_OVERSCAN,
		enabled: isOpen,
		getItemKey: (index) => selectedItems[index],
	});
	const unselectedVirtualizer = useVirtualizer({
		count: unselectedItems.length,
		getScrollElement: () => unselectedListRef.current,
		estimateSize: () => ITEM_HEIGHT,
		overscan: ITEM_OVERSCAN,
		enabled: isOpen,
		getItemKey: (index) => unselectedItems[index],
	});
	const selectedVirtualItems = isOpen
		? selectedVirtualizer.getVirtualItems()
		: [];
	const unselectedVirtualItems = isOpen
		? unselectedVirtualizer.getVirtualItems()
		: [];
	const listsMaxHeight = menuMaxHeight
		? Math.max(0, menuMaxHeight - MENU_CHROME_HEIGHT)
		: DEFAULT_LIST_HEIGHT;
	const menuStyle = menuMaxHeight
		? ({
			"--table-column-menu-max-height": `${menuMaxHeight}px`,
		} as CSSProperties)
		: undefined;

	useEffect(() => {
		if (!isOpen) return;
		selectedVirtualizer.scrollToOffset(0);
		unselectedVirtualizer.scrollToOffset(0);
	}, [search, isOpen, selectedVirtualizer, unselectedVirtualizer]);

	/** Stores the item whose drag overlay should be rendered. */
	const handleDragStart = useCallback((event: DragStartEvent) => {
		setActiveDraggedItem(String(event.active.id));
	}, []);

	/** Applies a completed visible-item reorder. */
	const handleDragEnd = useCallback(
		(event: DragEndEvent) => {
			const activeItem = String(event.active.id);
			const overItem = event.over ? String(event.over.id) : null;
			setActiveDraggedItem(null);
			if (overItem && activeItem !== overItem) {
				onMoveItem(activeItem, overItem);
			}
		},
		[onMoveItem],
	);

	const announcements = useMemo<Announcements>(() => {
		if (!isFieldTerminology) {
			return {
				onDragStart: ({ active }) =>
					t("table.columnDragStarted", {
						column: getItemLabel(String(active.id)),
					}),
				onDragOver: ({ active, over }) =>
					over
						? t("table.columnDragMoved", {
							column: getItemLabel(String(active.id)),
							over: getItemLabel(String(over.id)),
						})
						: undefined,
				onDragEnd: ({ active, over }) =>
					t("table.columnDragDropped", {
						column: getItemLabel(String(active.id)),
						over: getItemLabel(String(over?.id ?? active.id)),
					}),
				onDragCancel: ({ active }) =>
					t("table.columnDragCanceled", {
						column: getItemLabel(String(active.id)),
					}),
			};
		}

		return {
			onDragStart: ({ active }) =>
				t("eventDialog.fieldDragStarted", {
					field: getItemLabel(String(active.id)),
				}),
			onDragOver: ({ active, over }) =>
				over
					? t("eventDialog.fieldDragMoved", {
						field: getItemLabel(String(active.id)),
						over: getItemLabel(String(over.id)),
					})
					: undefined,
			onDragEnd: ({ active, over }) =>
				t("eventDialog.fieldDragDropped", {
					field: getItemLabel(String(active.id)),
					over: getItemLabel(String(over?.id ?? active.id)),
				}),
			onDragCancel: ({ active }) =>
				t("eventDialog.fieldDragCanceled", {
					field: getItemLabel(String(active.id)),
				}),
		};
	}, [getItemLabel, isFieldTerminology, t]);

	const areAllSelected = hiddenItems.size === 0;
	const areAllDeselected = orderedItems.length === hiddenItems.size;
	const dragInstructions = isFieldTerminology
		? t("eventDialog.fieldDragInstructions")
		: t("table.columnDragInstructions");

	return (
		<Popover.Root open={isOpen} onOpenChange={handleOpenChange}>
			<Popover.Anchor virtualRef={anchorRef} />
			<Popover.Trigger asChild>
				<Button
					ref={triggerRef}
					type="button"
					variant="tertiary"
					size="sm"
					icon="MenuAlt"
					className={cn(s.trigger, triggerClassName)}
					aria-label={title}
					title={title}
					disabled={disabled}
					data-column-visibility-trigger="true"
					data-field-visibility-trigger={
						isFieldTerminology ? "true" : undefined
					}
				/>
			</Popover.Trigger>
			{isOpen ? (
				<Popover.Content
					align="end"
					sideOffset={MENU_SIDE_OFFSET}
					className={s.menu}
					container={portalContainer ?? triggerRef.current?.ownerDocument.body}
					data-column-visibility-menu="true"
					data-field-visibility-menu={isFieldTerminology ? "true" : undefined}
					style={menuStyle}
				>
					<div className={s.label}>{title}</div>
					<div className={s.separator} />
					<div className={s.search}>
						<Input
							ref={searchRef}
							size="sm"
							value={search}
							placeholder={t("common.search")}
							aria-label={t("common.search")}
							onChange={(event) => setSearch(event.target.value)}
							onKeyDown={(event) => event.stopPropagation()}
						/>
					</div>
					<div className={s.separator} />
					<div className={s.actions}>
						<Button
							type="button"
							variant="secondary"
							size="sm"
							className={s.action}
							disabled={areAllDeselected}
							aria-disabled={areAllDeselected}
							data-column-visibility-action="deselect-all"
							onClick={() => {
								if (!areAllDeselected) onDeselectAll();
							}}
						>
							{deselectAllLabel}
						</Button>
						<Button
							type="button"
							variant="secondary"
							size="sm"
							className={s.action}
							disabled={areAllSelected}
							aria-disabled={areAllSelected}
							data-column-visibility-action="select-all"
							onClick={() => {
								if (!areAllSelected) onSelectAll();
							}}
						>
							{selectAllLabel}
						</Button>
					</div>
					<div className={s.separator} />
					<DndContext
						autoScroll
						sensors={sensors}
						collisionDetection={closestCenter}
						accessibility={{
							announcements,
							container: portalContainer ?? triggerRef.current?.ownerDocument.body,
							screenReaderInstructions: { draggable: dragInstructions },
						}}
						onDragStart={handleDragStart}
						onDragEnd={handleDragEnd}
						onDragCancel={() => setActiveDraggedItem(null)}
					>
						<div
							className={s.lists}
							data-column-menu-count={
								selectedItems.length + unselectedItems.length
							}
							style={{ height: listsMaxHeight }}
						>
							<section
								className={s.section}
								data-column-menu-list="selected"
								data-empty={selectedItems.length === 0 || undefined}
							>
								<div className={s.sectionLabel}>{selectedLabel}</div>
								<div ref={selectedListRef} className={s.options}>
									{selectedItems.length === 0 ? (
										<div className={s.empty}>{emptyLabel}</div>
									) : (
										<SortableContext
											items={selectedItems}
											strategy={verticalListSortingStrategy}
										>
											<div
												className={s.optionsSizer}
												style={{ height: selectedVirtualizer.getTotalSize() }}
											>
												{selectedVirtualItems.map((virtualItem) => {
													const item = selectedItems[virtualItem.index];
													const itemLabel = getItemLabel(item);
													return (
														<SortableItemOption
															key={item}
															item={item}
															label={itemLabel}
															checked
															height={virtualItem.size}
															start={virtualItem.start}
															sortingDisabled={normalizedSearch.length > 0}
															dragLabel={
																normalizedSearch
																	? isFieldTerminology
																		? t("eventDialog.fieldReorderSearchDisabled")
																		: t("table.columnReorderSearchDisabled")
																	: isFieldTerminology
																		? t("eventDialog.fieldDragHandle", {
																			field: itemLabel,
																		})
																		: t("table.columnDragHandle", {
																			column: itemLabel,
																		})
															}
															onToggle={() => onToggleItem(item)}
														/>
													);
												})}
											</div>
										</SortableContext>
									)}
								</div>
							</section>
							<section
								className={s.section}
								data-column-menu-list="unselected"
								data-empty={unselectedItems.length === 0 || undefined}
							>
								<div className={s.sectionLabel}>{unselectedLabel}</div>
								<div ref={unselectedListRef} className={s.options}>
									{unselectedItems.length === 0 ? (
										<div className={s.empty}>{emptyLabel}</div>
									) : (
										<div
											className={s.optionsSizer}
											style={{ height: unselectedVirtualizer.getTotalSize() }}
										>
											{unselectedVirtualItems.map((virtualItem) => {
												const item = unselectedItems[virtualItem.index];
												return (
													<StaticItemOption
														key={item}
														item={item}
														label={getItemLabel(item)}
														checked={false}
														height={virtualItem.size}
														start={virtualItem.start}
														onToggle={() => onToggleItem(item)}
													/>
												);
											})}
										</div>
									)}
								</div>
							</section>
						</div>
						{(portalContainer ?? triggerRef.current?.ownerDocument.body)
							? createPortal(
								<DragOverlay adjustScale={false} zIndex={1600}>
									{activeDraggedItem ? (
										<div className={s.dragOverlay} data-column-drag-overlay="true">
											<span className={s.optionLabel}>
												{getItemLabel(activeDraggedItem)}
											</span>
											<Icon name="GripVertical" size={14} color="currentColor" />
										</div>
									) : null}
								</DragOverlay>,
								(portalContainer ??
									triggerRef.current?.ownerDocument.body) as HTMLElement,
							)
							: null}
					</DndContext>
				</Popover.Content>
			) : null}
		</Popover.Root>
	);
}
