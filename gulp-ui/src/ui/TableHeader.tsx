import { Button } from "@/ui/Button";
import { Checkbox } from "@/ui/Checkbox";
import { Icon } from "@/ui/Icon";
import { Label } from "@/ui/Label";
import { Popover } from "@/ui/Popover";
import { Select } from "@/ui/Select";
import { TablePagination } from "@/ui/TablePagination";
import { Locale } from "@/locales";
import s from "./styles/TableHeader.module.css";

export namespace TableHeader {
	export interface Action {
		/** Stable action identifier. */
		id: string;
		/** Localized label rendered in the actions menu. */
		label: string;
		/** Optional icon displayed before the label. */
		icon?: Icon.Name;
		/** Prevents the individual action from being selected. */
		disabled?: boolean;
		/** Runs the view-owned action. */
		onSelect: () => void | Promise<void>;
	}

	export interface SortOption {
		/** Value passed back to the owning view. */
		value: string;
		/** Optional display value; defaults to `value`. */
		label?: string;
	}

	export interface Sorting {
		/** Active sort field. */
		value: string;
		/** Fields available to the owning view. */
		options: readonly SortOption[];
		/** Active sort direction. */
		direction: "asc" | "desc";
		/** Receives a newly selected field. */
		onValueChange: (value: string) => void;
		/** Toggles the active direction in the owning view. */
		onDirectionToggle: () => void;
	}

	export interface Props {
		/** Whether every entry represented by the selection label is selected. */
		allSelected: boolean;
		/** Localized description of the entries affected by select all. */
		selectionLabel: string;
		/** Disables selection changes when the owning view cannot select entries. */
		selectionDisabled?: boolean;
		/** Receives the requested select-all state. */
		onSelectionChange: (selected: boolean) => void;
		/** View-specific bulk actions. */
		actions: readonly Action[];
		/** Disables the complete actions menu, usually when nothing is selected. */
		actionsDisabled?: boolean;
		/** Optional backend sorting controls. */
		sorting?: Sorting;
		/** Pagination values and callbacks. */
		pagination: TablePagination.Props;
		/** Portal target for menus rendered from a detached view. */
		container?: HTMLElement | null;
		/** Optional layout class supplied by the consumer. */
		className?: string;
	}
}

/**
 * Renders the shared controls shown above paginated data tables.
 * @param props Selection, actions, optional sorting, and pagination configuration.
 * @returns A reusable responsive table header.
 */
export function TableHeader({
	allSelected,
	selectionLabel,
	selectionDisabled = false,
	onSelectionChange,
	actions,
	actionsDisabled = false,
	sorting,
	pagination,
	container,
	className,
}: TableHeader.Props) {
	const { t } = Locale.use();

	return (
		<div className={className ? `${s.root} ${className}` : s.root}>
			<div className={s.leftControls}>
				<div className={s.selectionControl}>
					<Checkbox
						checked={allSelected}
						disabled={selectionDisabled}
						onCheckedChange={(checked) =>
							onSelectionChange(checked === true)
						}
					/>
					<button
						type="button"
						className={s.selectionLabel}
						disabled={selectionDisabled}
						onClick={() => onSelectionChange(!allSelected)}
					>
						{selectionLabel}
					</button>
				</div>

				<Popover.Root>
					<Popover.Trigger asChild>
						<Button
							variant="secondary"
							icon="ChevronDown"
							disabled={actionsDisabled || actions.length === 0}
						>
							{t("common.action")}
						</Button>
					</Popover.Trigger>
					<Popover.Content
						align="start"
						sideOffset={4}
						className={s.popoverContent}
						container={container}
						role="menu"
					>
						{actions.map((action) => (
							<button
								type="button"
								role="menuitem"
								key={action.id}
								className={s.menuItem}
								disabled={action.disabled}
								onClick={() => void action.onSelect()}
							>
								{action.icon ? (
									<Icon
										name={action.icon}
										size={14}
									/>
								) : null}
								<span>{action.label}</span>
							</button>
						))}
					</Popover.Content>
				</Popover.Root>

				{sorting && sorting.options.length > 0 ? (
					<div className={s.sortingControl}>
						<Label value={t("tableView.sort")} />
						<Select.Root
							value={sorting.value}
							onValueChange={sorting.onValueChange}
						>
							<Select.Trigger
								data-no-icon
								className={s.sortSelect}
							>
								<Select.Value />
							</Select.Trigger>
							<Select.Content container={container}>
								{sorting.options.map((option) => (
									<Select.Item
										key={option.value}
										value={option.value}
									>
										{option.label ?? option.value}
									</Select.Item>
								))}
							</Select.Content>
						</Select.Root>
						<Button
							variant="secondary"
							icon={sorting.direction === "asc" ? "SortAsc" : "SortDesc"}
							onClick={sorting.onDirectionToggle}
							title={t("tableView.toggleSortDirection")}
							aria-label={t("tableView.toggleSortDirection")}
						/>
					</div>
				) : null}
			</div>

			<TablePagination
				{...pagination}
				container={pagination.container ?? container}
			/>
		</div>
	);
}
