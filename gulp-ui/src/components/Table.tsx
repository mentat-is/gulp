import React, {
	useCallback,
	useMemo,
	useRef,
	useState,
} from "react";
import { useVirtualizer } from "@tanstack/react-virtual";

import s from "./styles/Table.module.css";
import { cn } from "@/ui/utils";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { Checkbox } from "@/ui/Checkbox";
import { Glyph } from "@/entities/Glyph";
import { Button } from "@/ui/Button";
import {
	Tooltip,
	TooltipTrigger,
	TooltipContent,
	TooltipProvider,
} from "@/ui/Tooltip";
import { Color } from "@/entities/Color";
import {
	ContextMenu,
	ContextMenuTrigger,
	ContextMenuContent,
	ContextMenuCheckboxItem,
	ContextMenuLabel,
	ContextMenuSeparator,
} from "@/ui/ContextMenu";
import { useTableConfig } from "@/hooks/useTableConfig";
import { Locale } from "@/locales";

export type Object = Record<string, any>;

type RowRecord = Record<string, unknown>;

interface ColumnAccessor {
	/** Column key used by headers, visibility settings, and render callbacks. */
	key: string;
	/** Pre-split dotted path used when a literal row key is not present. */
	pathParts: string[] | null;
	/** Whether this column should resolve to the virtual row index. */
	isIndex: boolean;
}

interface FormatCellValueCache {
	/** Cached object previews keyed by object identity. */
	objectValues: WeakMap<object, string>;
}

interface SharedTooltipState {
	/** Text displayed by the shared cell tooltip. */
	text: string;
	/** Viewport-relative top coordinate. */
	top: number;
	/** Viewport-relative left coordinate. */
	left: number;
}

export namespace Table {
	/**
	 * Describes a single action button rendered per row.
	 * The parent component manages the callback logic.
	 */
	export interface Action<T extends Object> {
		/** Icon name from the icon library. */
		icon: Icon.Name;
		/** Accessible label, rendered as a tooltip on hover. */
		label: string;
		/** Callback executed when this action is triggered. */
		onClick: (
			value: T,
			index: number,
			event: React.MouseEvent<HTMLButtonElement>,
		) => void;
		/** Optional variant for the action button. Defaults to 'tertiary'. */
		variant?: "secondary" | "tertiary";
	}

	/**
	 * Custom render function signature for individual cells.
	 * Returns a React node to override the default stringified fallback.
	 */
	export type CellRenderer<T extends Object> = (
		value: unknown,
		row: T,
		columnKey: string,
		rowIndex: number,
	) => React.ReactNode;

	/** Map column keys to custom cell renderers. */
	export type RenderMap<T extends Object> = Record<string, CellRenderer<T>>;

	/**
	 * Describes the configuration of a single column inside the Table.
	 */
	export interface ColumnDefinition<T extends Object> {
		/** The unique key identifying the field in data rows. */
		key: string;
		/** Custom display label for the column header. */
		label?: string;
		/** Custom width for this column. Can be a number (pixels) or string (e.g. 'auto', '100%'). */
		width?: number | string;
		/** Custom render function specifically for cells in this column. */
		render?: CellRenderer<T>;
	}

	export interface Props<T extends Object> extends Stack.Props {
		/** The array of data objects to display in the table. */
		values: T[];
		/** Whether to include a virtual index column ('i'). Defaults to true. */
		includeIndex?: boolean;
		/** List of fields to explicitly hide from the table columns. */
		notshow?: string[];

		// --- Checkbox Selection ---
		/** Whether rows are selectable via checkboxes. */
		selectable?: boolean;
		/** Set of indices representing currently selected rows (checkbox-based). */
		selectedrows?: Set<number>;
		/** Callback triggered when a row's checkbox selection state changes. */
		onrowselect?: (index: number, selected: boolean) => void;
		/** Callback triggered when the select-all header checkbox is toggled. */
		onSelectAll?: (selected: boolean) => void;

		// --- Row Click Selection ---
		/** Callback fired when a row is clicked. Parent manages the active state. When provided, enables row hover effect. */
		onRowClick?: (value: T, index: number) => void;
		/** Index of the currently active/selected row. Receives distinct visual styling. */
		activeRowIndex?: number;

		// --- Actions ---
		/** Array of action configurations for per-row action buttons. */
		actions?: Action<T>[];

		/** @deprecated Use `actions` prop instead. */
		onrowaction?: (value: T, index: number) => void;
		/** @deprecated Use `actions` prop instead. */
		iconAction?: string;

		// --- Rendering ---
		/** Global custom cell renderer, applied to all columns without a specific renderMap entry. */
		renderCell?: CellRenderer<T>;
		/** Per-column custom renderers keyed by column name. Takes priority over renderCell. */
		renderMap?: RenderMap<T>;

		// --- Columns & Sorting ---
		/** Optional explicit list of columns to display. If omitted, columns are derived from data. */
		columns?: (string | ColumnDefinition<T>)[];
		/** The field currently being used for sorting. */
		sortField?: string;
		/** The current sorting direction. */
		sortDirection?: "asc" | "desc";
		/** Callback triggered when a column header is clicked to change sorting. */
		onSort?: (field: string) => void;

		// --- Highlighting ---
		/** The ID of the row to highlight (matches _id field). */
		highlightedId?: string;

		// --- Column Visibility (Phase 4) ---
		/** Enables the right-click context menu for toggling column visibility. */
		columnVisibility?: boolean;
		/** Unique ID for this table, used to persist column visibility config in IndexedDB. */
		persistId?: string;

		// --- Virtualization ---
		/** Enables viewport row rendering for large tables. Set to false to render all rows. */
		virtualization?:
			| false
			| {
					/** Row count above which virtualization is enabled. Defaults to 40. */
					threshold?: number;
					/** Number of extra rows rendered above and below the viewport. Defaults to 8. */
					overscan?: number;
					/** Estimated row height in pixels. Defaults to 28. */
					estimatedRowHeight?: number;
			  };
	}
}

const EMPTY_ACTIONS: Table.Action<Object>[] = [];

interface ColumnModel<T extends Object> {
	/** Source rows used by the table body. Explicit-column tables keep raw rows. */
	rows: T[];
	/** Visible column keys before user column-visibility filtering. */
	columns: string[];
	/** Precomputed value accessors keyed by column. */
	columnAccessors: Record<string, ColumnAccessor>;
	/** Renderers declared on column definitions. */
	renderMapFromColumns: Record<string, Table.CellRenderer<T>>;
	/** Header labels declared on column definitions. */
	columnLabels: Record<string, string>;
	/** Explicit widths declared on column definitions. */
	columnWidthsFromDefinitions: Record<string, number | string>;
	/** Whether the caller supplied a fixed column schema. */
	hasExplicitColumns: boolean;
}

interface VirtualizationConfig {
	/** Row count above which virtualization is enabled. */
	threshold: number;
	/** Number of additional rows rendered outside the viewport. */
	overscan: number;
	/** Estimated row height in pixels. */
	estimatedRowHeight: number;
}

const DEFAULT_VIRTUALIZATION_CONFIG: VirtualizationConfig = {
	threshold: 40,
	overscan: 8,
	estimatedRowHeight: 28,
};

const EMPTY_RENDER_MAP: Record<string, never> = {};

/**
 * Resolves row virtualization settings from the public Table prop.
 * @param virtualization Optional virtualization configuration or false.
 * @returns Concrete virtualization settings.
 */
function resolveVirtualizationConfig(
	virtualization: Table.Props<Object>["virtualization"],
): VirtualizationConfig {
	if (!virtualization) {
		return DEFAULT_VIRTUALIZATION_CONFIG;
	}

	return {
		threshold:
			virtualization.threshold ?? DEFAULT_VIRTUALIZATION_CONFIG.threshold,
		overscan: virtualization.overscan ?? DEFAULT_VIRTUALIZATION_CONFIG.overscan,
		estimatedRowHeight:
			virtualization.estimatedRowHeight ??
			DEFAULT_VIRTUALIZATION_CONFIG.estimatedRowHeight,
	};
}

/**
 * Checks whether a value is a plain object that should be flattened.
 * @param value Candidate value from a row.
 * @returns True when the value is a plain object.
 */
function isPlainObject(value: unknown): value is RowRecord {
	return Object.prototype.toString.call(value) === "[object Object]";
}

/**
 * Flattens a row object using dotted keys for nested plain objects.
 * @param value Source row or nested object.
 * @param parentKey Dotted prefix accumulated during recursion.
 * @returns Flattened row record.
 */
function flattenRow(
	value: RowRecord,
	parentKey = "",
): RowRecord {
	return Object.entries(value).reduce<RowRecord>(
		(acc, [key, entry]) => {
			const nextKey = parentKey ? `${parentKey}.${key}` : key;

			if (isPlainObject(entry)) {
				Object.assign(acc, flattenRow(entry, nextKey));
				return acc;
			}

			acc[nextKey] = entry;
			return acc;
		},
		{},
	);
}

/**
 * Creates a reusable accessor for a table column.
 * @param key Column key from the table schema.
 * @param includeIndex Whether the virtual index column is enabled.
 * @returns Column accessor metadata.
 */
function createColumnAccessor(
	key: string,
	includeIndex: boolean,
): ColumnAccessor {
	return {
		key,
		pathParts: key.includes(".") ? key.split(".") : null,
		isIndex: key === "i" && includeIndex,
	};
}

/**
 * Creates column accessors for the provided column keys.
 * @param columns Column keys available to the table.
 * @param includeIndex Whether the virtual index column is enabled.
 * @returns Accessors keyed by column name.
 */
function createColumnAccessors(
	columns: string[],
	includeIndex: boolean,
): Record<string, ColumnAccessor> {
	return columns.reduce<Record<string, ColumnAccessor>>((acc, column) => {
		acc[column] = createColumnAccessor(column, includeIndex);
		return acc;
	}, {});
}

/**
 * Reads a field from a row using precomputed dotted-path metadata.
 * @param row Source row object.
 * @param accessor Column accessor metadata.
 * @returns Field value when present.
 */
function readRowValue(row: RowRecord, accessor: ColumnAccessor): unknown {
	if (Object.prototype.hasOwnProperty.call(row, accessor.key)) {
		return row[accessor.key];
	}

	if (!accessor.pathParts) {
		return undefined;
	}

	return accessor.pathParts.reduce<unknown>((current, part) => {
		if (!current || typeof current !== "object") {
			return undefined;
		}

		return (current as Record<string, unknown>)[part];
	}, row);
}

/**
 * Determines whether a derived column key should be hidden.
 * @param key Flattened column key.
 * @param hiddenColumns Columns hidden by the caller.
 * @param hasNotshow Whether the caller provided the notshow prop.
 * @returns True when the key should not be part of the derived schema.
 */
function shouldHideDerivedColumn(
	key: string,
	hiddenColumns: Set<string> | undefined,
	hasNotshow: boolean,
): boolean {
	if (hiddenColumns?.has(key)) return true;
	return !hasNotshow && key === "event.original";
}

/**
 * Collects flattened leaf keys without allocating a flattened row object.
 * @param value Source row or nested object.
 * @param keys Accumulator for discovered column keys.
 * @param hiddenColumns Columns hidden by the caller.
 * @param hasNotshow Whether the caller provided the notshow prop.
 * @param parentKey Dotted prefix accumulated during recursion.
 */
function collectFlattenedRowKeys(
	value: RowRecord,
	keys: Set<string>,
	hiddenColumns: Set<string> | undefined,
	hasNotshow: boolean,
	parentKey = "",
): void {
	Object.entries(value).forEach(([key, entry]) => {
		const nextKey = parentKey ? `${parentKey}.${key}` : key;

		if (isPlainObject(entry)) {
			collectFlattenedRowKeys(
				entry,
				keys,
				hiddenColumns,
				hasNotshow,
				nextKey,
			);
			return;
		}

		if (!shouldHideDerivedColumn(nextKey, hiddenColumns, hasNotshow)) {
			keys.add(nextKey);
		}
	});
}

/**
 * Reads or creates a lazily flattened row for schema-less tables.
 * @param row Source row object.
 * @param cache Weak cache keyed by row identity.
 * @returns Flattened row record.
 */
function getFlattenedRow(
	row: RowRecord,
	cache: WeakMap<RowRecord, RowRecord>,
): RowRecord {
	const cached = cache.get(row);
	if (cached) {
		return cached;
	}

	const flattened = flattenRow(row);
	cache.set(row, flattened);
	return flattened;
}

/**
 * Builds the table model for explicit-column callers without projecting rows.
 * @param sourceRows Raw rows passed by the caller.
 * @param configuredColumns Explicit column list or definitions.
 * @param notshow Columns hidden by the caller.
 * @param includeIndex Whether to include the virtual index column.
 * @returns Column metadata and raw rows for lazy cell resolution.
 */
function buildExplicitColumnModel<T extends Object>(
	sourceRows: T[],
	configuredColumns: (string | Table.ColumnDefinition<T>)[],
	notshow: string[] | undefined,
	includeIndex: boolean,
): ColumnModel<T> {
	const renderMapFromColumns: Record<string, Table.CellRenderer<T>> = {};
	const columnLabels: Record<string, string> = {};
	const columnWidthsFromDefinitions: Record<string, number | string> = {};

	const keys = configuredColumns.map((column) => {
		if (typeof column === "string") {
			return column;
		}

		columnLabels[column.key] = column.label || column.key;
		if (column.render) {
			renderMapFromColumns[column.key] = column.render;
		}
		if (column.width !== undefined) {
			columnWidthsFromDefinitions[column.key] = column.width;
		}
		return column.key;
	});

	const columns = keys.filter((key) => !notshow?.includes(key));
	if (includeIndex && !columns.includes("i")) {
		columns.unshift("i");
	}

	return {
		rows: sourceRows,
		columns,
		columnAccessors: createColumnAccessors(columns, includeIndex),
		renderMapFromColumns,
		columnLabels,
		columnWidthsFromDefinitions,
		hasExplicitColumns: true,
	};
}

/**
 * Builds the table model for schema-less callers without flattening every row.
 * @param sourceRows Raw rows passed by the caller.
 * @param notshow Columns hidden by the caller.
 * @param includeIndex Whether to include the virtual index column.
 * @returns Column metadata and raw rows for lazy flattened reads.
 */
function buildDerivedColumnModel<T extends Object>(
	sourceRows: T[],
	notshow: string[] | undefined,
	includeIndex: boolean,
): ColumnModel<T> {
	const keys = new Set<string>();
	const hiddenColumns = notshow ? new Set(notshow) : undefined;
	const hasNotshow = notshow !== undefined;

	sourceRows.forEach((row) => {
		collectFlattenedRowKeys(row as RowRecord, keys, hiddenColumns, hasNotshow);
	});

	if (includeIndex) {
		keys.add("i");
	}

	const columns = Array.from(keys.values()).sort((a, b) => a.localeCompare(b));

	return {
		rows: sourceRows,
		columns,
		columnAccessors: createColumnAccessors(columns, includeIndex),
		renderMapFromColumns: EMPTY_RENDER_MAP as Record<
			string,
			Table.CellRenderer<T>
		>,
		columnLabels: {},
		columnWidthsFromDefinitions: {},
		hasExplicitColumns: false,
	};
}

/**
 * Reads a display value for the requested table cell.
 * @param row Source row for the current rendered line.
 * @param accessor Column accessor to read.
 * @param rowIndex Row index in the full table.
 * @param hasExplicitColumns Whether the caller supplied a column schema.
 * @returns Cell value or the blank placeholder when the field is absent.
 */
function readTableCellValue<T extends Object>(
	row: T,
	accessor: ColumnAccessor,
	rowIndex: number,
	hasExplicitColumns: boolean,
): unknown {
	if (accessor.isIndex) {
		return rowIndex;
	}

	const value = hasExplicitColumns
		? readRowValue(row as RowRecord, accessor)
		: (row as RowRecord)[accessor.key];

	return hasExplicitColumns
		? value ?? "<BLANK>"
		: value === undefined
			? "<BLANK>"
			: value;
}

/**
 * Resolves the row shape used by body rendering and public row callbacks.
 * @param row Source row from the table values.
 * @param hasExplicitColumns Whether the caller supplied a column schema.
 * @param flattenedRowCache Lazy flattened-row cache for schema-less tables.
 * @returns Raw row for explicit schemas, flattened row for derived schemas.
 */
function getRenderableRow<T extends Object>(
	row: T,
	hasExplicitColumns: boolean,
	flattenedRowCache: WeakMap<RowRecord, RowRecord>,
): T {
	if (hasExplicitColumns) {
		return row;
	}

	return getFlattenedRow(row as RowRecord, flattenedRowCache) as T;
}

/**
 * Determines whether a table column matches the active sort field.
 * @param column Column key from the table header.
 * @param sortField Currently sorted field.
 * @returns True when the column should display the sort indicator.
 */
function isTableColumnSorted(column: string, sortField: string | undefined): boolean {
	if (!sortField) return false;
	if (sortField === column) return true;
	if (column === "timestamp" && sortField === "@timestamp") return true;
	if (column === "@timestamp" && sortField === "timestamp") return true;
	return false;
}

/**
 * Creates a stable key for a rendered row.
 * @param row Row data used by the rendered table line.
 * @param index Row index in the full table.
 * @returns Stable key based on row identity and index fallback.
 */
function getRowKey<T extends Object>(row: T, index: number): string {
	const record = row as Record<string, unknown>;
	return `${String(record._id ?? record.id ?? "")}-${index}`;
}

/**
 * Creates a small object preview without deep stringifying large payloads.
 * @param value Object or array value to preview.
 * @returns Bounded human-readable preview.
 */
function previewObjectValue(value: unknown): string {
	if (Array.isArray(value)) {
		const items = value.slice(0, 6).map((item) =>
			item && typeof item === "object" ? "{...}" : String(item),
		);
		return `[${items.join(", ")}${value.length > 6 ? ", ..." : ""}]`;
	}

	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>).slice(0, 8);
		const preview = entries.map(([key, entry]) => {
			const display =
				entry && typeof entry === "object"
					? Array.isArray(entry)
						? `[${entry.length}]`
						: "{...}"
					: typeof entry === "bigint"
						? entry.toString()
						: String(entry);
			return `${key}: ${display}`;
		});
		const suffix =
			Object.keys(value as Record<string, unknown>).length > entries.length
				? ", ..."
				: "";
		return `{${preview.join(", ")}${suffix}}`;
	}

	return String(value);
}

/**
 * Converts a cell value to bounded display text.
 * @param value Cell value.
 * @returns String safe to compute during table render.
 */
function formatCellValue(value: unknown): string {
	if (typeof value === "bigint") {
		return value.toString();
	}

	if (value && typeof value === "object") {
		return previewObjectValue(value);
	}

	return String(value);
}

/**
 * Converts a cell value to display text using cached object previews.
 * @param value Cell value.
 * @param cache Formatter cache owned by the current table data set.
 * @returns String safe to compute during table render.
 */
function formatCellValueWithCache(
	value: unknown,
	cache: FormatCellValueCache,
): string {
	if (!value || typeof value !== "object") {
		return formatCellValue(value);
	}

	const cached = cache.objectValues.get(value);
	if (cached) {
		return cached;
	}

	const formatted = formatCellValue(value);
	cache.objectValues.set(value, formatted);
	return formatted;
}

/**
 * Resolves the final list of row actions, handling backward compatibility
 * with the deprecated `onrowaction` / `iconAction` props.
 */
function resolveActions<T extends Object>(
	actions: Table.Action<T>[] | undefined,
	onrowaction: ((value: T, index: number) => void) | undefined,
	iconAction: string | undefined,
	defaultActionLabel: string,
): Table.Action<T>[] {
	if (actions && actions.length > 0) {
		return actions;
	}

	if (onrowaction) {
		return [
			{
				icon: (iconAction || "Search") as Icon.Name,
				label: defaultActionLabel,
				onClick: onrowaction,
				variant: "tertiary",
			},
		];
	}

	return EMPTY_ACTIONS as Table.Action<T>[];
}

/**
 * Renders a generic data table with optional selection, actions, sorting, and virtualized rows.
 * @param props Table configuration, data rows, column definitions, and callbacks.
 * @returns Table wrapper element.
 */
export function Table<T extends Object>({
	values: _values = [],
	className,
	includeIndex = true,
	columns: _columns,
	notshow,
	selectable,
	selectedrows,
	onrowselect,
	onSelectAll,
	onRowClick,
	activeRowIndex,
	onrowaction,
	iconAction,
	actions: _actions,
	renderCell,
	renderMap,
	sortField,
	sortDirection,
	onSort,
	highlightedId,
	columnVisibility: _columnVisibility,
	persistId: _persistId,
	virtualization: virtualizationProp,
	...props
}: Table.Props<T>) {
	const { style: stackStyle, onScroll: stackOnScroll, ...stackProps } = props;
	const wrapperRef = useRef<HTMLDivElement | null>(null);
	const tableRef = useRef<HTMLTableElement | null>(null);
	const columnElementRefs = useRef<Record<string, HTMLTableColElement | null>>(
		{},
	);
	const { t } = Locale.use();
	const isResizingRef = useRef(false);
	const lastResolvedActionsRef = useRef<Table.Action<T>[]>([]);
	const [isColumnMenuOpen, setIsColumnMenuOpen] = useState(false);
	const [sharedTooltip, setSharedTooltip] =
		useState<SharedTooltipState | null>(null);

	const hasCustomVirtualizationConfig = typeof virtualizationProp === "object";
	const virtualizationThreshold =
		hasCustomVirtualizationConfig
			? virtualizationProp.threshold
			: undefined;
	const virtualizationOverscan =
		hasCustomVirtualizationConfig
			? virtualizationProp.overscan
			: undefined;
	const virtualizationEstimatedRowHeight =
		hasCustomVirtualizationConfig
			? virtualizationProp.estimatedRowHeight
			: undefined;
	const virtualizationConfig = useMemo(
		() => resolveVirtualizationConfig(virtualizationProp),
		[
			virtualizationProp === false,
			virtualizationThreshold,
			virtualizationOverscan,
			virtualizationEstimatedRowHeight,
		],
	);
	const formatCache = useMemo<FormatCellValueCache>(
		() => ({
			objectValues: new WeakMap(),
		}),
		[_values],
	);
	const flattenedRowCache = useMemo<WeakMap<RowRecord, RowRecord>>(
		() => new WeakMap(),
		[_values],
	);

	const handleSort = useCallback(
		(field: string) => {
			if (isResizingRef.current) return;
			onSort?.(field);
		},
		[onSort],
	);

	/** Resolve actions with backward compat for deprecated props. */
	const resolvedActions = useMemo(() => {
		const fresh = resolveActions(
			_actions,
			onrowaction,
			iconAction,
			t("common.action"),
		);
		const last = lastResolvedActionsRef.current;
		if (
			last.length === fresh.length &&
			last.every((act, idx) => {
				const fAct = fresh[idx];
				return (
					act.icon === fAct.icon &&
					act.label === fAct.label &&
					act.variant === fAct.variant &&
					act.onClick === fAct.onClick
				);
			})
		) {
			return last;
		}
		lastResolvedActionsRef.current = fresh;
		return fresh;
	}, [_actions, onrowaction, iconAction, t]);

	const columnModel = useMemo(
		() =>
			_columns
				? buildExplicitColumnModel([], _columns, notshow, includeIndex)
				: buildDerivedColumnModel(_values, notshow, includeIndex),
		[_columns, notshow, includeIndex, _columns ? null : _values],
	);

	const {
		columns,
		columnAccessors,
		renderMapFromColumns,
		columnLabels,
		columnWidthsFromDefinitions,
		hasExplicitColumns,
	} = columnModel;
	const rows = hasExplicitColumns ? _values : columnModel.rows;

	const isVirtualizationEnabled =
		virtualizationProp !== false && rows.length > virtualizationConfig.threshold;

	const rowVirtualizer = useVirtualizer({
		count: rows.length,
		getScrollElement: () => wrapperRef.current,
		estimateSize: () => virtualizationConfig.estimatedRowHeight,
		overscan: virtualizationConfig.overscan,
	});

	const virtualItems = isVirtualizationEnabled
		? rowVirtualizer.getVirtualItems()
		: [];
	const topSpacerHeight =
		isVirtualizationEnabled && virtualItems.length > 0
			? virtualItems[0].start
			: 0;
	const bottomSpacerHeight =
		isVirtualizationEnabled && virtualItems.length > 0
			? Math.max(
					0,
					rowVirtualizer.getTotalSize() -
						virtualItems[virtualItems.length - 1].end,
				)
			: 0;

	/** Combined custom cell renderers from column definition and renderMap prop. */
	const mergedRenderMap = useMemo<Table.RenderMap<T> | undefined>(() => {
		const hasColumnRenderers = Object.keys(renderMapFromColumns).length > 0;
		if (hasColumnRenderers && renderMap) {
			return { ...renderMapFromColumns, ...renderMap };
		}
		if (renderMap) {
			return renderMap;
		}
		if (hasColumnRenderers) {
			return renderMapFromColumns;
		}
		return undefined;
	}, [renderMapFromColumns, renderMap]);

	/**
	 * Determines whether all visible rows are currently selected via checkboxes.
	 * Used to drive the select-all header checkbox state.
	 */
	const isAllSelected = useMemo(() => {
		if (!selectable || !selectedrows || rows.length === 0) return false;
		return rows.length > 0 && selectedrows.size === rows.length;
	}, [selectable, selectedrows, rows.length]);

	/** Whether the table body has clickable rows (enables hover). */
	const isClickable = !!onRowClick;

	// ─── Column Visibility (Phase 4) ───────────────────────────────────
	const {
		hiddenColumns,
		toggleColumn,
		resetConfig,
		isLoaded: configLoaded,
	} = useTableConfig(_persistId, columns, !!_columnVisibility);

	/** Columns filtered by visibility (hidden columns removed). */
	const visibleColumns = useMemo(() => {
		if (!_columnVisibility || hiddenColumns.size === 0) return columns;
		return columns.filter((c) => !hiddenColumns.has(c));
	}, [columns, hiddenColumns, _columnVisibility]);

	/** Accessors for the columns currently rendered in the body. */
	const visibleColumnAccessors = useMemo(
		() => visibleColumns.map((column) => columnAccessors[column]),
		[visibleColumns, columnAccessors],
	);

	// ─── Column Resizing ───────────────────────────────────────────────
	const MIN_COL_WIDTH = 30;
	const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});

	/** Calculate default column widths for currently visible columns. */
	const defaultWidths = useMemo(
		() => {
			const widths: Record<string, number | string> = {};
			if (!configLoaded) {
				return widths;
			}
			const skipRowSampling =
				isVirtualizationEnabled || rows.length > virtualizationConfig.threshold;

			visibleColumnAccessors.forEach((accessor) => {
				const column = accessor.key;
				const configuredWidth = columnWidthsFromDefinitions[column];
				if (configuredWidth !== undefined) {
					widths[column] = configuredWidth;
					return;
				}

				if (column === "i") {
					widths[column] = 45;
					return;
				}

				let maxLen = (columnLabels[column] || column).length;

				if (!skipRowSampling) {
					const sampleCount = Math.min(rows.length, 100);
					for (let rowIndex = 0; rowIndex < sampleCount; rowIndex++) {
						const sampleRow = rows[rowIndex];
						const rowForRead = hasExplicitColumns
							? sampleRow
							: (getFlattenedRow(
									sampleRow as RowRecord,
									flattenedRowCache,
								) as T);
						const value = readTableCellValue(
							rowForRead,
							accessor,
							rowIndex,
							hasExplicitColumns,
						);
						if (value !== undefined && value !== null && value !== "<BLANK>") {
							const valueText = formatCellValueWithCache(value, formatCache);
							if (valueText.length > maxLen) {
								maxLen = valueText.length;
							}
						}
					}
				}

				widths[column] = Math.max(80, Math.min(350, maxLen * 7.5 + 24));
			});

			return widths;
		},
		[
			rows,
			visibleColumnAccessors,
			columnWidthsFromDefinitions,
			columnLabels,
			hasExplicitColumns,
			isVirtualizationEnabled,
			virtualizationConfig.threshold,
			flattenedRowCache,
			formatCache,
			configLoaded,
		],
	);

	/**
	 * Handles the start of a column resize drag operation.
	 * Attaches mousemove and mouseup listeners to the window for smooth tracking.
	 */
	const handleResizeStart = useCallback(
		(columnKey: string, startX: number, startWidth: number) => {
			isResizingRef.current = true;
			const localDoc = wrapperRef.current?.ownerDocument ?? document;
			const localWin = localDoc.defaultView ?? window;
			const initialTableWidth = tableRef.current?.getBoundingClientRect().width;
			let frame: number | null = null;
			let pendingWidth = startWidth;
			let committedWidth = startWidth;

			/**
			 * Applies the in-progress resize directly to table layout elements.
			 * @param width Next column width in pixels.
			 */
			const applyResizeWidth = (width: number) => {
				const columnElement = columnElementRefs.current[columnKey];
				if (columnElement) {
					columnElement.style.width = `${width}px`;
				}
				if (tableRef.current && initialTableWidth !== undefined) {
					const nextTableWidth = Math.max(
						MIN_COL_WIDTH,
						initialTableWidth + width - startWidth,
					);
					tableRef.current.style.width = `${nextTableWidth}px`;
				}
			};

			/**
			 * Queues visual resize updates without forcing React to render per event.
			 * @param e Mouse move event from the owner window.
			 */
			const onMouseMove = (e: MouseEvent) => {
				const delta = e.clientX - startX;
				pendingWidth = Math.max(MIN_COL_WIDTH, startWidth + delta);
				committedWidth = pendingWidth;
				if (frame !== null) {
					return;
				}
				frame = localWin.requestAnimationFrame(() => {
					frame = null;
					applyResizeWidth(pendingWidth);
				});
			};

			/**
			 * Commits the final resized width to React state and clears listeners.
			 */
			const onMouseUp = () => {
				if (frame !== null) {
					localWin.cancelAnimationFrame(frame);
					frame = null;
				}
				applyResizeWidth(committedWidth);
				localWin.removeEventListener("mousemove", onMouseMove);
				localWin.removeEventListener("mouseup", onMouseUp);
				localDoc.body.style.cursor = "";
				localDoc.body.style.userSelect = "";
				setColumnWidths((prev) => ({ ...prev, [columnKey]: committedWidth }));
				setTimeout(() => {
					isResizingRef.current = false;
				}, 100);
			};

			localDoc.body.style.cursor = "col-resize";
			localDoc.body.style.userSelect = "none";
			localWin.addEventListener("mousemove", onMouseMove);
			localWin.addEventListener("mouseup", onMouseUp);
		},
		[],
	);

	/** Calculates the total width of the table by summing all column widths. */
	const totalTableWidth = useMemo(() => {
		let width = 0;
		let hasFlexible = false;
		if (selectable) width += 40;
		visibleColumns.forEach((c) => {
			const w = columnWidths[c] ?? defaultWidths[c];
			if (typeof w === "number") {
				width += w;
			} else {
				hasFlexible = true;
			}
		});
		if (resolvedActions.length > 0) {
			width += resolvedActions.length * 32 + 16;
		}
		return hasFlexible ? "100%" : width;
	}, [
		selectable,
		visibleColumns,
		columnWidths,
		defaultWidths,
		resolvedActions,
	]);

	const bodyColumnCount = Math.max(
		1,
		visibleColumns.length +
			(selectable ? 1 : 0) +
			(resolvedActions.length > 0 ? 1 : 0),
	);

	/**
	 * Hides the shared overflow tooltip for default table cells.
	 * @returns Nothing.
	 */
	const hideSharedTooltip = useCallback(() => {
		setSharedTooltip(null);
	}, []);

	/**
	 * Shows the shared tooltip when a default cell span is visually truncated.
	 * @param event Mouse event delegated from the table element.
	 * @returns Nothing.
	 */
	const handleTableMouseOver = useCallback(
		(event: React.MouseEvent<HTMLTableElement>) => {
			const target =
				event.target instanceof Element ? event.target : undefined;
			const trigger = target?.closest<HTMLElement>(
				'[data-table-tooltip-trigger="true"]',
			);
			if (!trigger || !tableRef.current?.contains(trigger)) {
				return;
			}

			const isOverflowing = trigger.scrollWidth > trigger.clientWidth + 1;
			if (!isOverflowing) {
				hideSharedTooltip();
				return;
			}

			const tooltipText = trigger.dataset.tableTooltipText;
			if (!tooltipText) {
				hideSharedTooltip();
				return;
			}

			const rect = trigger.getBoundingClientRect();
			const ownerWindow = trigger.ownerDocument.defaultView ?? window;
			setSharedTooltip({
				text: tooltipText,
				top: Math.max(
					8,
					Math.min(rect.bottom + 6, ownerWindow.innerHeight - 320),
				),
				left: Math.max(8, Math.min(rect.left, ownerWindow.innerWidth - 420)),
			});
		},
		[hideSharedTooltip],
	);

	/**
	 * Hides the shared tooltip when the pointer leaves the active default cell.
	 * @param event Mouse event delegated from the table element.
	 * @returns Nothing.
	 */
	const handleTableMouseOut = useCallback(
		(event: React.MouseEvent<HTMLTableElement>) => {
			const target =
				event.target instanceof Element ? event.target : undefined;
			const trigger = target?.closest<HTMLElement>(
				'[data-table-tooltip-trigger="true"]',
			);
			if (!trigger) {
				return;
			}

			const relatedTarget =
				event.relatedTarget instanceof Node ? event.relatedTarget : null;
			if (relatedTarget && trigger.contains(relatedTarget)) {
				return;
			}

			hideSharedTooltip();
		},
		[hideSharedTooltip],
	);

	/**
	 * Preserves caller scroll handling while hiding stale cell tooltips.
	 * @param event Scroll event from the table wrapper.
	 * @returns Nothing.
	 */
	const handleWrapperScroll = useCallback(
		(event: React.UIEvent<HTMLDivElement>) => {
			hideSharedTooltip();
			stackOnScroll?.(event);
		},
		[hideSharedTooltip, stackOnScroll],
	);

	/**
	 * Tracks column visibility menu state so menu items render only while open.
	 * @param open Whether the context menu is open.
	 * @returns Nothing.
	 */
	const handleColumnMenuOpenChange = useCallback(
		(open: boolean) => {
			setIsColumnMenuOpen(open);
			if (open) {
				hideSharedTooltip();
			}
		},
		[hideSharedTooltip],
	);

	/**
	 * Renders a table body row from a virtual or sequential row index.
	 * @param rowIndex Row index produced by the render plan.
	 * @returns Rendered table item or null when the row no longer exists.
	 */
	const renderBodyRow = useCallback(
		(rowIndex: number) => {
			const row = rows[rowIndex];
			if (!row) {
				return null;
			}

			const record = getRenderableRow(
				row,
				hasExplicitColumns,
				flattenedRowCache,
			) as T & RowRecord;

			return (
				<Item
					columns={visibleColumnAccessors}
					key={getRowKey(record, rowIndex)}
					i={row}
					index={rowIndex}
					hasExplicitColumns={hasExplicitColumns}
					flattenedRowCache={flattenedRowCache}
					selectable={selectable}
					selected={selectedrows?.has(rowIndex)}
					onRowSelect={onrowselect}
					actions={resolvedActions}
					onRowClick={onRowClick}
					isActive={activeRowIndex === rowIndex}
					highlighted={!!(highlightedId && record._id === highlightedId)}
					renderCell={renderCell}
					renderMap={mergedRenderMap}
				/>
			);
		},
		[
			rows,
			visibleColumnAccessors,
			hasExplicitColumns,
			flattenedRowCache,
			selectable,
			selectedrows,
			onrowselect,
			resolvedActions,
			onRowClick,
			activeRowIndex,
			highlightedId,
			renderCell,
			mergedRenderMap,
		],
	);

	/** Renders the inner table element with colgroup, thead, and tbody. */
	const tableElement = configLoaded ? (
		<table
			ref={tableRef}
			className={s.table}
			style={{ width: totalTableWidth }}
			onMouseOver={handleTableMouseOver}
			onMouseOut={handleTableMouseOut}
			onMouseLeave={hideSharedTooltip}
			onClick={hideSharedTooltip}
		>
			<colgroup>
				{selectable && <col style={{ width: 40 }} />}
				{visibleColumns.map((c) => {
					const w: number | string = columnWidths[c] ?? defaultWidths[c];
					return (
						<col
							key={`col-${c}`}
							ref={(element) => {
								columnElementRefs.current[c] = element;
							}}
							style={String(w) === "auto" ? undefined : { width: w }}
						/>
					);
				})}
				{resolvedActions.length > 0 && (
					<col style={{ width: resolvedActions.length * 32 + 16 }} />
				)}
			</colgroup>
			<thead>
				<tr>
					{selectable && (
						<th className={s.selectAllCell}>
							{onSelectAll ? (
								<div className={s.centered}>
									<Checkbox
										checked={isAllSelected}
										onCheckedChange={(c) => onSelectAll(!!c)}
									/>
								</div>
							) : null}
						</th>
					)}
					{visibleColumns.map((c) => {
						const isSorted = isTableColumnSorted(c, sortField);
						return (
							<MemoCol
								c={c}
								label={columnLabels[c]}
								key={c}
								isSorted={isSorted}
								sortDirection={isSorted ? sortDirection : undefined}
								onSort={handleSort}
								onResizeStart={handleResizeStart}
							/>
						);
					})}
					{resolvedActions.length > 0 && <th className={s.actionCell} />}
				</tr>
			</thead>
			<tbody data-clickable={isClickable || undefined}>
				{isVirtualizationEnabled && topSpacerHeight > 0 ? (
					<tr
						className={s.virtualSpacerRow}
						aria-hidden="true"
					>
						<td
							className={s.virtualSpacerCell}
							colSpan={bodyColumnCount}
							style={{ height: topSpacerHeight }}
						/>
					</tr>
				) : null}
				{isVirtualizationEnabled
					? virtualItems.map((virtualItem) => renderBodyRow(virtualItem.index))
					: rows.map((_row, rowIndex) => renderBodyRow(rowIndex))}
				{isVirtualizationEnabled && bottomSpacerHeight > 0 ? (
					<tr
						className={s.virtualSpacerRow}
						aria-hidden="true"
					>
						<td
							className={s.virtualSpacerCell}
							colSpan={bodyColumnCount}
							style={{ height: bottomSpacerHeight }}
						/>
					</tr>
				) : null}
			</tbody>
		</table>
	) : null;
	const wrapperStyle: React.CSSProperties &
		Record<"--highlight-color", string> = {
			...stackStyle,
			"--highlight-color": Color.Themer.getTargetGuideColor(),
		};

	return (
		<TooltipProvider>
			<Stack
				ai="flex-start"
				jc="flex-start"
				dir="column"
				ref={wrapperRef}
				className={cn(s.wrapper, className)}
				onScroll={handleWrapperScroll}
				style={wrapperStyle}
				{...stackProps}
			>
				{!configLoaded ? null : _columnVisibility ? (
					<ContextMenu onOpenChange={handleColumnMenuOpenChange}>
						<ContextMenuTrigger asChild>{tableElement}</ContextMenuTrigger>
						{isColumnMenuOpen ? (
							<ContextMenuContent>
								<ContextMenuLabel>{t("table.columnVisibility")}</ContextMenuLabel>
								<ContextMenuSeparator />
								{columns.map((col) => (
									<ContextMenuCheckboxItem
										key={col}
										checked={!hiddenColumns.has(col)}
										onCheckedChange={() => toggleColumn(col)}
										onSelect={(e) => e.preventDefault()}
									>
										{columnLabels[col] || col}
									</ContextMenuCheckboxItem>
								))}
								{hiddenColumns.size > 0 && (
									<>
										<ContextMenuSeparator />
										<ContextMenuCheckboxItem
											checked={false}
											onCheckedChange={() => resetConfig()}
											onSelect={(e) => e.preventDefault()}
										>
											{t("table.showAllColumns")}
										</ContextMenuCheckboxItem>
									</>
								)}
							</ContextMenuContent>
						) : null}
					</ContextMenu>
				) : (
					tableElement
				)}
				{sharedTooltip ? (
					<div
						className={s.sharedTooltip}
						data-table-tooltip="true"
						style={{
							top: sharedTooltip.top,
							left: sharedTooltip.left,
						}}
					>
						{sharedTooltip.text}
					</div>
				) : null}
			</Stack>
		</TooltipProvider>
	);
}

namespace Col {
	export interface Props extends React.ThHTMLAttributes<HTMLTableCellElement> {
		/** The column field name. */
		c: string;
		/** Custom display label for the column header. */
		label?: string;
		/** Whether this column is currently sorted. */
		isSorted: boolean;
		/** The current sorting direction. */
		sortDirection?: "asc" | "desc";
		/** Callback to trigger a sort on this column. */
		onSort?: (field: string) => void;
		/** Callback to initiate column resize. */
		onResizeStart?: (
			columnKey: string,
			startX: number,
			startWidth: number,
		) => void;
	}
}

/**
 * Renders a table header cell with optional sorting interactivity and resize handle.
 *
 * @param props - Header column configurations and callbacks
 */
function Col({
	c,
	label,
	isSorted,
	sortDirection,
	onSort,
	onResizeStart,
	className,
	...props
}: Col.Props) {
	const thRef = useRef<HTMLTableCellElement>(null);

	/**
	 * Initiates column resize on mousedown on the resize handle.
	 * Reads the current rendered width as the starting value.
	 */
	const handleResizeMouseDown = useCallback(
		(e: React.MouseEvent) => {
			e.stopPropagation();
			e.preventDefault();
			if (!thRef.current || !onResizeStart) return;
			const startWidth = thRef.current.getBoundingClientRect().width;
			onResizeStart(c, e.clientX, startWidth);
		},
		[c, onResizeStart],
	);

	return (
		<th
			ref={thRef}
			{...props}
			onClick={() => onSort?.(c)}
			className={cn(s.headerCell, onSort && s.headerCellSortable, className)}
		>
			<Stack
				gap={4}
				ai="center"
				jc="flex-start"
				className={s.headerStack}
			>
				<span>{label ?? c}</span>
				{isSorted && onSort && (
					<Icon
						className={s.headerSortIcon}
						name={sortDirection === "asc" ? "SortAscending" : "SortDescending"}
						size={14}
					/>
				)}
			</Stack>
			{onResizeStart && (
				<div
					className={s.resizeHandle}
					onMouseDown={handleResizeMouseDown}
					onClick={(e) => {
						e.stopPropagation();
						e.preventDefault();
					}}
				/>
			)}
		</th>
	);
}

const MemoCol = React.memo(Col);

namespace Item {
	export interface Props<T extends Object>
		extends React.HTMLAttributes<HTMLTableRowElement> {
		/** The source data row. */
		i: T;
		/** Visible column accessors. */
		columns: ColumnAccessor[];
		/** Row index within the table. */
		index: number;
		/** Whether the table uses caller-provided columns and raw row reads. */
		hasExplicitColumns: boolean;
		/** Lazy flattened-row cache for schema-less tables. */
		flattenedRowCache: WeakMap<RowRecord, RowRecord>;
		/** Whether checkbox selection is enabled. */
		selectable?: boolean;
		/** Whether this row's checkbox is checked. */
		selected?: boolean;
		/** Callback when this row's checkbox changes. */
		onRowSelect?: (index: number, selected: boolean) => void;
		/** Resolved list of row-level action buttons. */
		actions: Table.Action<T>[];
		/** Callback when the row itself is clicked (click-selection). */
		onRowClick?: (value: T, index: number) => void;
		/** Whether this row is the active/click-selected row. */
		isActive: boolean;
		/** Whether this row is highlighted by external ID match. */
		highlighted: boolean;
		/** Global custom cell renderer. */
		renderCell?: Table.CellRenderer<T>;
		/** Per-column custom cell renderers. */
		renderMap?: Table.RenderMap<T>;
	}
}

/**
 * Renders a single table body row with optional checkbox, actions, and custom rendering.
 */
const Item = React.memo(
	function ItemInner<T extends Object>({
		i,
		columns,
		index,
		hasExplicitColumns,
		flattenedRowCache,
		selectable,
		selected,
		onRowSelect,
		actions,
		onRowClick,
		isActive,
		highlighted,
		renderCell,
		renderMap,
		...props
	}: Item.Props<T>) {
		const renderRow = useMemo(
			() => getRenderableRow(i, hasExplicitColumns, flattenedRowCache),
			[i, hasExplicitColumns, flattenedRowCache],
		);

		/**
		 * Handle row click for click-selection.
		 * Prevents triggering row click when clicking on interactive elements (checkbox, button).
		 */
		const handleRowClick = useCallback(
			(e: React.MouseEvent) => {
				if (!onRowClick) return;
				const target = e.target as HTMLElement;
				if (target.closest('button, input, [role="checkbox"]')) return;
				onRowClick(renderRow, index);
			},
			[onRowClick, renderRow, index],
		);

		return (
			<tr
				className={cn(
					s.item,
					isActive && s.activeRow,
					highlighted && s.highlighted,
				)}
				onClick={handleRowClick}
				{...props}
			>
				{selectable && (
					<td className={cn(s.value, s.actionCell)}>
						<div className={s.centered}>
							<Checkbox
								checked={!!selected}
								onCheckedChange={(c) => onRowSelect?.(index, !!c)}
							/>
						</div>
					</td>
				)}
				{columns.map((column) => {
					const c = column.key;
					const cellValue = readTableCellValue(
						renderRow,
						column,
						index,
						hasExplicitColumns,
					);

					// Priority: renderMap[column] > renderCell > default Value component
					const columnRenderer = renderMap?.[c];
					if (columnRenderer) {
						return (
							<td
								className={cn(s.value, cellValue === "<BLANK>" && s.blank)}
								key={c}
							>
								{columnRenderer(cellValue, renderRow, c, index)}
							</td>
						);
					}
					if (renderCell) {
						return (
							<td
								className={cn(s.value, cellValue === "<BLANK>" && s.blank)}
								key={c}
							>
								{renderCell(cellValue, renderRow, c, index)}
							</td>
						);
					}
					return (
						<Value
							k={c}
							v={cellValue}
							key={c}
						/>
					);
				})}
				{actions.length > 0 && (
					<td className={cn(s.value, s.actionsCell)}>
						<Stack
							gap={4}
							ai="center"
							jc="center"
						>
							{actions.map((action) => (
								<Tooltip
									key={action.label}
									delayDuration={300}
								>
									<TooltipTrigger asChild>
										<Button
											className={s.Button_Icon}
											icon={action.icon}
											variant={action.variant || "tertiary"}
											onClick={(event) =>
												action.onClick(renderRow, index, event)
											}
										/>
									</TooltipTrigger>
									<TooltipContent sideOffset={4}>{action.label}</TooltipContent>
								</Tooltip>
							))}
						</Stack>
					</td>
				)}
			</tr>
		);
	},
	(prevProps, nextProps) => {
		if (prevProps.index !== nextProps.index) return false;
		if (prevProps.hasExplicitColumns !== nextProps.hasExplicitColumns) return false;
		if (prevProps.flattenedRowCache !== nextProps.flattenedRowCache) return false;
		if (prevProps.selectable !== nextProps.selectable) return false;
		if (prevProps.selected !== nextProps.selected) return false;
		if (prevProps.isActive !== nextProps.isActive) return false;
		if (prevProps.highlighted !== nextProps.highlighted) return false;
		if (prevProps.i !== nextProps.i) return false;
		if (prevProps.columns.length !== nextProps.columns.length) return false;
		for (let i = 0; i < prevProps.columns.length; i++) {
			if (prevProps.columns[i] !== nextProps.columns[i]) return false;
		}
		if (prevProps.actions !== nextProps.actions) return false;
		if (prevProps.onRowClick !== nextProps.onRowClick) return false;
		if (prevProps.onRowSelect !== nextProps.onRowSelect) return false;
		if (prevProps.renderCell !== nextProps.renderCell) return false;
		if (prevProps.renderMap !== nextProps.renderMap) return false;
		return true;
	},
) as <T extends Object>(props: Item.Props<T>) => React.ReactElement | null;

namespace Value {
	export interface Props extends React.TdHTMLAttributes<HTMLTableCellElement> {
		/** Column key for cell-specific formatting. */
		k: string;
		/** Raw cell value to display. */
		v: unknown;
	}
}

/**
 * Renders a lightweight default table cell.
 * @param props Column key, raw value, and td attributes.
 * @returns Rendered table data cell.
 */
const Value = React.memo(
	function ValueInner({ k, v, style, ...props }: Value.Props) {
		const stringified = formatCellValue(v);
		const displayValue = stringified;
		const valueStyle =
			k === "color"
				? {
						...style,
						color: stringified,
					}
				: style;
		const glyph = k === "glyph_id" ? Glyph.List.get(v as Glyph.Id) : undefined;
		const icon =
			k === "glyph_id" && glyph ? (
				<Icon
					className={s.cellIcon}
					size={12}
					name={glyph}
				/>
			) : null;

		const content = (
			<>
				{icon}
				{displayValue}
			</>
		);

		return (
			<td
				className={cn(s.value, displayValue === "<BLANK>" && s.blank)}
				style={valueStyle}
				{...props}
			>
				<span
					className={s.triggerSpan}
					data-table-tooltip-trigger="true"
					data-table-tooltip-text={displayValue}
				>
					{content}
				</span>
			</td>
		);
	},
	(prevProps, nextProps) => {
		return prevProps.k === nextProps.k && prevProps.v === nextProps.v;
	},
);
