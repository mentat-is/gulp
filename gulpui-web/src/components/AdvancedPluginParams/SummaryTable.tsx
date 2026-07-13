import { ReactNode } from "react";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import { cn } from "@/ui/utils";
import s from "../styles/Table.module.css";
import { Locale } from "@/locales";

/**
 * SummaryTableColumn defines the configuration for a single column in the SummaryTable.
 * It specifies the data key, the header label, and an optional custom render function.
 */
export interface SummaryTableColumn<T> {
	key: Extract<keyof T, string> | string;
	label: string;
	render?: (value: any, item: T) => ReactNode;
	width?: number | string;
}

/**
 * SummaryTableAction defines an icon-only action rendered for each table row.
 */
export interface SummaryTableAction<T> {
	icon: NonNullable<Button.Props["icon"]>;
	label: string;
	variant?: Button.Variant;
	onClick: (item: T, index: number) => void;
}

export interface SummaryTableProps<T> {
	columns: SummaryTableColumn<T>[];
	data: T[];
	onEdit?: (item: T, index: number) => void;
	onDelete?: (item: T, index: number) => void;
	actions?: SummaryTableAction<T>[];
	className?: string;
}

/**
 * SummaryTable is a generic, lightweight table component designed for displaying
 * collection summaries within the advanced configuration panels.
 * It supports double-click for editing and a dedicated delete action.
 */
export function SummaryTable<T extends Record<string, any>>({
	columns,
	data,
	onEdit,
	onDelete,
	actions = [],
	className,
}: SummaryTableProps<T>) {
	const { t } = Locale.use();
	const hasActions = actions.length > 0 || !!onDelete;

	return (
		<Stack
			ai="flex-start"
			jc="flex-start"
			dir="column"
			className={cn(s.wrapper, className)}
			style={{ width: "100%" }}
		>
			<table
				className={s.table}
				style={{ width: "100%", borderCollapse: "collapse" }}
			>
				<thead>
					<tr>
						{columns.map((c, i) => (
							<th
								key={`th-${c.key}-${i}`}
								style={c.width ? { width: c.width } : undefined}
							>
								{c.label}
							</th>
						))}
						{hasActions && (
							<th
								style={{ width: actions.length * 32 + (onDelete ? 40 : 0) }}
							></th>
						)}
					</tr>
				</thead>
				<tbody>
					{data.map((item, index) => (
						<tr
							key={`tr-${index}`}
							className={s.item}
							onDoubleClick={() => onEdit && onEdit(item, index)}
							style={{ cursor: onEdit ? "pointer" : "default" }}
						>
							{columns.map((c, i) => {
								const value = item[c.key as keyof T];

								// Content priority:
								// 1. Custom render function
								// 2. Stringified object (for arrays/nested values)
								// 3. String representation of primitive values
								const displayedValue = c.render
									? c.render(value, item)
									: typeof value === "object"
										? JSON.stringify(value)
										: String(value ?? "");

								return (
									<td
										key={`td-${c.key}-${i}`}
										className={s.value}
										style={c.width ? { width: c.width } : undefined}
									>
										{displayedValue}
									</td>
								);
							})}
							{hasActions && (
								<td
									className={s.value}
									style={{ textAlign: "center", padding: "0 4px" }}
								>
									<Stack
										gap={4}
										ai="center"
										jc="center"
									>
										{actions.map((action) => (
											<Button
												key={action.label}
												variant={action.variant || "tertiary"}
												icon={action.icon}
												shape="icon"
												aria-label={action.label}
												onClick={(e) => {
													e.stopPropagation();
													action.onClick(item, index);
												}}
												style={{ height: 24, width: 24, minHeight: 24 }}
											/>
										))}
										{onDelete && (
											<Button
												variant="tertiary"
												icon="Trash"
												shape="icon"
												aria-label="Delete row"
												onClick={(e) => {
													e.stopPropagation();
													onDelete(item, index);
												}}
												style={{ height: 24, width: 24, minHeight: 24 }}
											/>
										)}
									</Stack>
								</td>
							)}
						</tr>
					))}
					{data.length === 0 && (
						<tr>
							<td
								colSpan={columns.length + (hasActions ? 1 : 0)}
								className={cn(s.value, s.blank)}
								style={{ textAlign: "center", padding: 8 }}
							>
								{t("common.noRecords")}
							</td>
						</tr>
					)}
				</tbody>
			</table>
		</Stack>
	);
}
