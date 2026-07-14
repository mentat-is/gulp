import {
	useState,
	useEffect,
	useCallback,
	useRef,
	useMemo,
	type SetStateAction,
} from "react";
import { Application } from "@/context/Application.context";
import { Input } from "@/ui/Input";
import { Stack } from "@/ui/Stack";
import { Checkbox } from "@/ui/Checkbox";
import { Select } from "@/ui/Select";
import { Button } from "@/ui/Button";
import { Popover } from "@/ui/Popover";
import { Label } from "@/ui/Label";
import { Table } from "@/components/Table";
import { Source } from "@/entities/Source";
import { Query } from "@/entities/Query";
import s from "./styles/TableViewWindow.module.css";
import { cn } from "@/ui/utils";
import { toast } from "sonner";
import { Toggle } from "@/ui/Toggle";
import { Internal } from "@/entities/addon/Internal";
import { format } from "date-fns";
import { Icon } from "@/ui/Icon";
import { Doc } from "@/entities/Doc";
import { NoteFunctionality } from "@/banners/Collab.functionality";
import { WindowBridge } from "@/lib/WindowBridge";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { OpenSearchQueryBuilder } from "@/components/QueryBuilder";
import { Separator } from "@/ui/Separator";
import { Filter } from "@/entities/Filter";
import { Locale } from "@/locales";

export namespace TableViewWindow {
	export interface Props {
		initialSourceId?: Source.Id;
		onClose?: () => void;
	}
}

type TableEventRow = Doc.Type & Record<string, unknown>;

type OpenSearchBoolQuery = Record<string, unknown> & {
	bool?: {
		must?: Record<string, unknown>[];
		should?: Record<string, unknown>[];
		must_not?: Record<string, unknown>[];
		filter?: Record<string, unknown>[];
	};
};

const DEFAULT_HIDDEN_FIELDS: string[] = [
	"_id",
	"gulp.source_id",
	"gulp.context_id",
	"gulp.operation_id",
	"gulp.event_code",
	"event.original",
	"gulp.timestamp",
	"gulp.unmapped",
	"gulp.enrich",
];

const DEFAULT_HIDDEN_FIELD_SET = new Set(DEFAULT_HIDDEN_FIELDS);

/**
 * Creates a stable signature for source-id selections.
 * @param sourceIds Selected source identifiers.
 * @returns Sorted comma-separated source-id signature.
 */
function getSourceIdsSignature(sourceIds: Source.Id[]): string {
	return [...sourceIds].sort((a, b) => a.localeCompare(b)).join(",");
}

/**
 * Derives the widest nanosecond time range covered by the selected sources.
 * @param sources Selected source entities.
 * @param fallback Timeline range used when sources do not expose nanotimestamps.
 * @returns Nanosecond min/max strings for table query scoping.
 */
function getSelectedSourcesTimeFrame(
	sources: Source.Type[],
	fallback: { min: string; max: string },
): { min: string; max: string } {
	if (sources.length === 0) {
		return fallback;
	}

	const minValues = sources
		.map((source) => source.nanotimestamp?.min)
		.filter((value): value is bigint => typeof value === "bigint");
	const maxValues = sources
		.map((source) => source.nanotimestamp?.max)
		.filter((value): value is bigint => typeof value === "bigint");

	if (minValues.length === 0 || maxValues.length === 0) {
		return fallback;
	}

	return {
		min: minValues
			.reduce((min, value) => (value < min ? value : min))
			.toString(),
		max: maxValues
			.reduce((max, value) => (value > max ? value : max))
			.toString(),
	};
}

/**
 * Adds a flagged-document id restriction to an OpenSearch bool query.
 * @param query OpenSearch query generated from the structured Query.Type.
 * @param flaggedDocIds Flagged document identifiers for the active operation.
 * @returns Query with a terms clause restricting results to flagged documents.
 */
function withFlaggedDocumentFilter(
	query: OpenSearchBoolQuery,
	flaggedDocIds: Doc.Id[],
): OpenSearchBoolQuery {
	const bool = query.bool ?? {};
	const must = bool.must ?? [];

	return {
		...query,
		bool: {
			...bool,
			must: [...must, { terms: { _id: flaggedDocIds } }],
		},
	};
}

/**
 * Helper component for date selection using datetime-local input.
 * Converts nanoseconds timestamps to/from browser-compatible date strings.
 */
function InputDateSelection({
	type,
	value,
	valid,
	onChange,
}: {
	type: "min" | "max";
	value: string;
	valid: boolean;
	onChange: (val: string) => void;
}) {
	const { t } = Locale.use();
	const inputRef = useRef<HTMLInputElement | null>(null);

	/**
	 * Formats a nanosecond timestamp string into a "yyyy-MM-dd'T'HH:mm" format.
	 */
	const getFormattedValue = (val: string) => {
		try {
			const ms = Number(BigInt(val) / 1000000n);
			return format(ms, "yyyy-MM-dd'T'HH:mm");
		} catch {
			return "";
		}
	};

	const [localValue, setLocalValue] = useState(getFormattedValue(value));

	useEffect(() => {
		setLocalValue(getFormattedValue(value));
	}, [value]);

	// Native showPicker support for better UX
	useEffect(() => {
		const input = inputRef.current;
		const icon = input?.parentElement?.querySelector("svg");
		const clickHandler = () => input?.showPicker?.();
		icon?.addEventListener("click", clickHandler);
		return () => icon?.removeEventListener("click", clickHandler);
	}, []);

	return (
		<Input
			ref={inputRef}
			label={type === "min" ? t("common.from") : t("common.to")}
			type="datetime-local"
			valid={valid}
			variant="highlighted"
			icon="Calendar"
			value={localValue}
			onChange={(e) => {
				setLocalValue(e.target.value);
				onChange(e.target.value);
			}}
		/>
	);
}

/**
 * Helper component for ISO string selection.
 * Handles manual entry of ISO 8601 strings and synchronizes with nanosecond state.
 */
function InputISOSelection({
	type,
	value,
	valid,
	onChange,
}: {
	type: "min" | "max";
	value: string;
	valid: boolean;
	onChange: (val: string) => void;
}) {
	const { t } = Locale.use();
	const [localValue, setLocalValue] = useState(
		Internal.Transformator.toISO(value),
	);

	useEffect(() => {
		// Only update local value if it's different from the formatted current value
		// to avoid interrupting user typing
		const currentISO = Internal.Transformator.toISO(value);
		if (
			Internal.Transformator.toNanos(localValue) !==
			Internal.Transformator.toNanos(currentISO)
		) {
			setLocalValue(currentISO);
		}
	}, [value]);

	return (
		<Input
			label={type === "min" ? t("common.from") : t("common.to")}
			type="text"
			icon="Calendar"
			placeholder={t("tableView.isoPlaceholder")}
			variant="highlighted"
			valid={valid}
			value={localValue}
			onChange={(e) => {
				setLocalValue(e.target.value);
				onChange(e.target.value);
			}}
		/>
	);
}

/**
 * TableViewWindow Component
 * Opens a paginated table view of raw events for one or more sources.
 */
export function TableViewWindow({
	initialSourceId,
	onClose,
}: TableViewWindow.Props) {
	// --- Context & Infrastructure ---
	const { Info, app, spawnBanner, banner } = Application.use();
	const { t } = Locale.use();
	const [container, setContainer] = useState<HTMLDivElement | null>(null);
	const tableRequestControllerRef = useRef<AbortController | null>(null);

	// --- Selection & Sync State ---
	const [isSynced, setIsSynced] = useState<boolean>(false);
	const [selectedSourceIds, setSelectedSourceIds] = useState<Source.Id[]>(
		initialSourceId ? [initialSourceId] : [],
	);

	const selectedSourceIdsSignature = useMemo(
		() => getSourceIdsSignature(selectedSourceIds),
		[selectedSourceIds],
	);

	const selectedSources = useMemo(
		() =>
			selectedSourceIds
				.map((sourceId) => Source.Entity.id(app, sourceId))
				.filter((source): source is Source.Type => !!source),
		[app, app.target.files, selectedSourceIdsSignature],
	);

	const selectedSourcesSignature = useMemo(
		() => getSourceIdsSignature(selectedSources.map((source) => source.id)),
		[selectedSources],
	);

	const primarySelectedSource = selectedSources[0] ?? null;
	const isMultiSourceSelection = selectedSources.length > 1;
	const isSyncActive = isSynced && !isMultiSourceSelection;
	const selectedOperationId = app.target.operations.find((o) => o.selected)?.id;
	const activeOperationId =
		selectedOperationId ?? primarySelectedSource?.operation_id ?? null;

	useEffect(() => {
		if (isMultiSourceSelection && isSynced) {
			setIsSynced(false);
		}
	}, [isMultiSourceSelection, isSynced]);

	// --- Filter Derived State ---
	const syncedSourceFilter =
		isSyncActive && primarySelectedSource
			? app.target.filters?.[primarySelectedSource.id]
			: null;

	const serializedFilters = useMemo(() => {
		return JSON.stringify(syncedSourceFilter?.filters || []);
	}, [syncedSourceFilter]);

	const syncedTextFilter = syncedSourceFilter?.text_filter || "";

	const [localFieldTypeMap, setLocalFieldTypeMap] = useState<Record<
		string,
		string
	> | null>(null);

	useEffect(() => {
		if (selectedSources.length === 0) {
			setLocalFieldTypeMap(null);
			return;
		}

		let cancelled = false;
		(async () => {
			const map: Record<string, string> = {};
			await Promise.all(
				selectedSources.map(async (source) => {
					const fileKeys = await Info.event_keys(source);
					Object.entries(fileKeys).forEach(([key, type]) => {
						map[key] = type as string;
					});
				}),
			);
			if (!cancelled) {
				setLocalFieldTypeMap(map);
			}
		})();

		return () => {
			cancelled = true;
		};
	}, [selectedSources, Info]);

	const columns = useMemo(() => {
		if (!localFieldTypeMap) return undefined;
		return Object.keys(localFieldTypeMap).sort((a, b) => a.localeCompare(b));
	}, [localFieldTypeMap]);

	// --- Pagination & Search State ---
	const [localSearchQuery, setLocalSearchQuery] = useState("");
	const [searchQuery, setSearchQuery] = useState("");
	const [pageSize, setPageSize] = useState<number>(50);
	const [currentPage, setCurrentPage] = useState<number>(1);
	const [sortField, setSortField] = useState<string>("timestamp");
	const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");

	/**
	 * Memoized list of fields that are allowed for sorting in OpenSearch.
	 *
	 * Exclusions:
	 * - Data types: 'text', 'flattened', 'unmapped', 'flat_object'
	 * - Field patterns: 'gulp.unmapped.*', 'gulp.enrich.*'
	 * - Specific hidden fields: DEFAULT_HIDDEN_FIELDS
	 *
	 * Note: '@timestamp' is preferred, but 'timestamp' is always added as a fallback
	 * if no timestamp field survived the filter.
	 */
	const sortableFields = useMemo(() => {
		const map = localFieldTypeMap;
		const fields: string[] = [];

		if (map) {
			Object.entries(map).forEach(([field, type]) => {
				const t = String(type).toLowerCase().trim();
				const f = String(field).toLowerCase().trim();

				const isDisallowedType = [
					"text",
					"flattened",
					"unmapped",
					"flat_object",
				].includes(t);
				const isInternalGulpField =
					f.startsWith("gulp.unmapped") || f.startsWith("gulp.enrich");
				const isHiddenField = DEFAULT_HIDDEN_FIELDS.includes(f);

				if (!isDisallowedType && !isInternalGulpField && !isHiddenField) {
					fields.push(field);
				}
			});
		}

		if (!fields.includes("timestamp") && !fields.includes("@timestamp")) {
			fields.push("timestamp");
		}

		return fields.sort((a, b) => a.localeCompare(b));
	}, [localFieldTypeMap]);

	/**
	 * Resets sort field if it's no longer valid for the selected source.
	 */
	useEffect(() => {
		// If current sortField is not in sortableFields, try to find a valid default
		if (!sortableFields.includes(sortField)) {
			if (sortField === "timestamp" && sortableFields.includes("@timestamp")) {
				setSortField("@timestamp");
			} else if (
				sortField === "@timestamp" &&
				sortableFields.includes("timestamp")
			) {
				setSortField("timestamp");
			} else if (sortField !== "timestamp") {
				setSortField("timestamp");
			}
		}
	}, [sortableFields, sortField]);

	// --- Data & Results State ---
	const [data, setData] = useState<TableEventRow[]>([]);
	const [totalHits, setTotalHits] = useState<number>(0);
	const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());
	const [loading, setLoading] = useState(false);

	// --- Display & Validation State ---
	const [manual, setManual] = useState(false);
	const [isMinValid, setIsMinValid] = useState(true);
	const [isMaxValid, setIsMaxValid] = useState(true);
	const timelineTimeFrame = useMemo(
		() => ({
			min: (app.timeline.frame.min * 1000000).toString(),
			max: (app.timeline.frame.max * 1000000).toString(),
		}),
		[app.timeline.frame.min, app.timeline.frame.max],
	);
	const [timeFrame, setTimeFrame] = useState<{ min: string; max: string }>({
		min: (app.timeline.frame.min * 1000000).toString(),
		max: (app.timeline.frame.max * 1000000).toString(),
	});

	// --- Filter Editing State ---
	const [isFiltersOpen, setIsFiltersOpen] = useState(false);
	const [localFilters, setLocalFilters] = useState<Filter.Item[]>([]);
	const [appliedFilters, setAppliedFilters] = useState<Filter.Item[]>([]);
	const [localFlaggedOnly, setLocalFlaggedOnly] = useState(false);
	const [appliedFlaggedOnly, setAppliedFlaggedOnly] = useState(false);

	// --- Performance Optimization: State Adjustment during Rendering ---
	/**
	 * Pattern: Adjusting state while rendering.
	 * This logic detects changes in global state (like filters or timeline) or selection
	 * and resets local pagination/search state within a single render pass.
	 * This prevents "useEffect cascades" that cause double-fetches.
	 */
	const [prevSync, setPrevSync] = useState({
		sourceIdsSignature: selectedSourcesSignature,
		operationId: selectedOperationId,
		isSynced: isSyncActive,
		filters: serializedFilters,
		textFilter: syncedTextFilter,
		timelineMin: app.timeline.frame.min,
		timelineMax: app.timeline.frame.max,
	});

	const hasLogicalChange =
		prevSync.sourceIdsSignature !== selectedSourcesSignature ||
		prevSync.operationId !== selectedOperationId ||
		prevSync.isSynced !== isSyncActive ||
		(isSyncActive &&
			(prevSync.filters !== serializedFilters ||
				prevSync.textFilter !== syncedTextFilter)) ||
		prevSync.timelineMin !== app.timeline.frame.min ||
		prevSync.timelineMax !== app.timeline.frame.max;

	if (hasLogicalChange) {
		const opChanged = prevSync.operationId !== selectedOperationId;
		const sourceChanged =
			prevSync.sourceIdsSignature !== selectedSourcesSignature;

		setPrevSync({
			sourceIdsSignature: selectedSourcesSignature,
			operationId: selectedOperationId,
			isSynced: isSyncActive,
			filters: serializedFilters,
			textFilter: syncedTextFilter,
			timelineMin: app.timeline.frame.min,
			timelineMax: app.timeline.frame.max,
		});

		// Reset pagination, search and field map
		setCurrentPage(1);
		setSearchQuery("");
		setLocalSearchQuery("");
		if (opChanged || sourceChanged) {
			setLocalFieldTypeMap(null);
			setData([]);
			setTotalHits(0);
			setSelectedRows(new Set());
		}

		if (opChanged) {
			setSelectedSourceIds((previousSourceIds) =>
				previousSourceIds.filter((sourceId) => {
					const source = Source.Entity.id(app, sourceId);
					if (source) {
						return source.operation_id === selectedOperationId;
					}

					return app.target.files.length === 0;
				}),
			);
		}

		// Sync timeframe
		if (isSyncActive) {
			setTimeFrame(timelineTimeFrame);
		} else if (sourceChanged && selectedSources.length > 0) {
			setTimeFrame(
				getSelectedSourcesTimeFrame(selectedSources, timelineTimeFrame),
			);
		}
	}

	/**
	 * Derived pagination info.
	 */
	const totalPages = Math.max(1, Math.ceil(totalHits / pageSize));

	/**
	 * Page overflow protection.
	 * If total results decrease, ensure we don't stay on a non-existent page.
	 */
	if (currentPage > totalPages) {
		setCurrentPage(1);
	}

	// --- Derived Query Parameters ---
	const activeMin = timeFrame.min;
	const activeMax = timeFrame.max;
	const activeFilters =
		isSyncActive && primarySelectedSource
			? app.target.filters?.[primarySelectedSource.id]
			: null;
	const activeSerializedFilters = isSyncActive
		? serializedFilters
		: JSON.stringify(appliedFilters);
	const activeTextFilter = isSyncActive ? syncedTextFilter : searchQuery;
	const activeFlaggedDocIdsSignature =
		appliedFlaggedOnly && activeOperationId
			? Array.from(Doc.Entity.flag.getList(activeOperationId)).join(",")
			: "";
	const renderableColumns = useMemo(
		() =>
			columns?.filter(
				(column) => !DEFAULT_HIDDEN_FIELD_SET.has(column.toLowerCase()),
			),
		[columns],
	);

	// --- Data Fetching ---

	/**
	 * Main function to fetch events from the backend.
	 * Uses query_paginate to retrieve a specific slice of data based on current state.
	 */
	const fetchTableData = useCallback(async () => {
		if (!primarySelectedSource || !activeOperationId) return;
		tableRequestControllerRef.current?.abort();
		const requestController = new AbortController();
		tableRequestControllerRef.current = requestController;
		setLoading(true);
		try {
			let queryObj: Query.Type;
			const sourceIds = selectedSources.map((source) => source.id);

			// Branch logic: Determine if we use synced global filters or local search query
			if (isSyncActive) {
				if (activeFilters) {
					queryObj = {
						...activeFilters,
						filters: [...(activeFilters.filters ?? [])],
						text_filter: syncedTextFilter,
						source_config: {
							...(activeFilters.source_config ?? {}),
							operation_id:
								activeFilters.source_config?.operation_id ?? activeOperationId,
							source_ids: sourceIds,
							range: { min: activeMin, max: activeMax },
						},
					};
				} else {
					queryObj = {
						string: "",
						filters: [],
						text_filter: "",
						source_config: {
							operation_id: activeOperationId,
							source_ids: sourceIds,
							range: { min: activeMin, max: activeMax },
						},
					};
				}
			} else {
				queryObj = {
					string: "",
					filters: appliedFilters,
					text_filter: searchQuery,
					source_config: {
						operation_id: activeOperationId,
						source_ids: sourceIds,
						range: { min: activeMin, max: activeMax },
					},
				};
			}

			if (appliedFlaggedOnly) {
				const flaggedDocIds = Array.from(
					Doc.Entity.flag.getList(activeOperationId),
				);

				if (flaggedDocIds.length === 0) {
					setData([]);
					setTotalHits(0);
					setSelectedRows(new Set());
					return;
				}

				queryObj = {
					...queryObj,
					isManual: true,
					raw: withFlaggedDocumentFilter(
						Filter.Entity.query(queryObj) as OpenSearchBoolQuery,
						flaggedDocIds,
					),
				};
			}

			const limit = pageSize;
			const offset = pageSize * (currentPage - 1);
			const sortOpt = {
				[sortField === "timestamp" ? "@timestamp" : sortField]: sortDirection,
			};

			const res = await Info.query_paginate(queryObj, {
				limit,
				offset,
				sort: sortOpt,
				signal: requestController.signal,
			});

			if (
				requestController.signal.aborted ||
				tableRequestControllerRef.current !== requestController
			) {
				return;
			}

			setData((res?.docs || []) as TableEventRow[]);
			setTotalHits(res?.total_hits || 0);
			setSelectedRows(new Set());
		} catch (e) {
			if (requestController.signal.aborted) {
				return;
			}
			toast.error(t("tableView.fetchFailed"));
		} finally {
			if (tableRequestControllerRef.current === requestController) {
				tableRequestControllerRef.current = null;
				setLoading(false);
			}
		}
		// We use stable primitives (IDs and serialized strings) for dependencies to avoid
		// redundant fetches when global object references change.
	}, [
		selectedSourcesSignature,
		activeOperationId,
		isSyncActive,
		activeMin,
		activeMax,
		activeSerializedFilters,
		activeTextFilter,
		appliedFlaggedOnly,
		activeFlaggedDocIdsSignature,
		pageSize,
		currentPage,
		sortField,
		sortDirection,
		Info,
	]);

	useEffect(() => {
		return () => tableRequestControllerRef.current?.abort();
	}, []);

	/**
	 * Effect to trigger fetch whenever logical query parameters change.
	 */
	useEffect(() => {
		fetchTableData();
	}, [fetchTableData]);

	// --- Event Handlers ---

	/**
	 * Commits current local filters, search query, and flagged-only state.
	 * @returns Nothing.
	 */
	const applyFilters = useCallback(() => {
		setAppliedFilters(localFilters);
		setSearchQuery(localSearchQuery);
		setAppliedFlaggedOnly(localFlaggedOnly);
		setCurrentPage(1);
	}, [localFilters, localSearchQuery, localFlaggedOnly]);

	/**
	 * Clears all local and applied filter states.
	 * @returns Nothing.
	 */
	const resetFilters = useCallback(() => {
		setLocalFilters([]);
		setAppliedFilters([]);
		setLocalSearchQuery("");
		setSearchQuery("");
		setLocalFlaggedOnly(false);
		setAppliedFlaggedOnly(false);
		setCurrentPage(1);
	}, []);

	/**
	 * Updates local search query state as user types.
	 */
	const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		setLocalSearchQuery(e.target.value);
	};

	/**
	 * Validates and updates the time range boundaries.
	 */
	const handleTimeChange = (type: "min" | "max", value: string | number) => {
		try {
			const nanos = Internal.Transformator.toNanos(value).toString();
			if (nanos === "0" && value !== "") {
				if (type === "min") setIsMinValid(false);
				else setIsMaxValid(false);
				return;
			}

			setTimeFrame((prev) => {
				const next = { ...prev, [type]: nanos };
				const min = BigInt(next.min);
				const max = BigInt(next.max);
				setIsMinValid(min < max);
				setIsMaxValid(max > min);
				return next;
			});
			setCurrentPage(1);
		} catch (e) {
			if (type === "min") setIsMinValid(false);
			else setIsMaxValid(false);
		}
	};

	/**
	 * Updates the sort field and resets to page 1.
	 */
	const handleSortFieldChange = useCallback((val: string) => {
		setSortField(val);
		setCurrentPage(1);
	}, []);

	/**
	 * Toggles the sort direction and resets to page 1.
	 */
	const toggleSortDirection = useCallback(() => {
		setSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
		setCurrentPage(1);
	}, []);

	/**
	 * Applies column-header sorting when the selected field can be sorted by the backend.
	 * @param field Column field requested by the generic Table header.
	 * @returns Nothing.
	 */
	const handleTableSort = useCallback(
		(field: string) => {
			if (!sortableFields.includes(field)) {
				return;
			}

			if (sortField === field) {
				toggleSortDirection();
				return;
			}

			handleSortFieldChange(field);
		},
		[sortableFields, sortField, toggleSortDirection, handleSortFieldChange],
	);

	/**
	 * Selects or deselects all visible rows.
	 * @param checked Whether all visible rows should be selected.
	 * @returns Nothing.
	 */
	const handleSelectAll = (checked: boolean) => {
		if (checked) {
			setSelectedRows(new Set(data.map((_, i) => i)));
		} else {
			setSelectedRows(new Set());
		}
	};

	/**
	 * Performs bulk operations (flagging or notes) on selected rows.
	 * @param action Bulk operation requested by the action menu.
	 * @returns Promise that resolves when the selected action has been handled.
	 */
	const handleBulkAction = async (action: "flagged" | "notes") => {
		if (!activeOperationId) return;
		if (selectedRows.size === 0) {
			toast.error(t("tableView.noItemsSelected"));
			return;
		}
		const selectedDocs = Array.from(selectedRows).map((i) => data[i]);

		if (action === "flagged") {
			for (const doc of selectedDocs) {
				Doc.Entity.flag.toggle(doc._id, activeOperationId);
			}
			const bridge = WindowBridge.create(WindowBridge.generateId(), () => {});
			bridge.send(WindowBridge.MessageType.FLAGS_CHANGED, {
				docId: selectedDocs[0]._id,
				operationId: activeOperationId,
			});
			bridge.destroy();
		} else if (action === "notes") {
			spawnBanner(
				<NoteFunctionality.Create.Banner
					events={selectedDocs}
					container={container}
				/>,
				"table",
			);
		}
	};

	/**
	 * Handles clicking an action on a specific row (usually to target an event in the main view).
	 * @param doc Table row document selected by the row action.
	 * @returns Nothing.
	 */
	const handleRowAction = useCallback(
		(doc: TableEventRow) => {
			if (!activeOperationId) return;
			const bridge = WindowBridge.create(WindowBridge.generateId(), () => {});
			bridge.send(WindowBridge.MessageType.TARGET_NOTE, {
				docId: doc._id,
				operationId: activeOperationId,
			});
			bridge.destroy();
		},
		[activeOperationId],
	);

	/**
	 * Updates the selected row index set for the generic Table checkbox column.
	 * @param index Row index in the current result page.
	 * @param selected Whether the row is now selected.
	 * @returns Nothing.
	 */
	const handleTableRowSelect = useCallback(
		(index: number, selected: boolean) => {
			setSelectedRows((prev) => {
				const next = new Set(prev);
				if (selected) next.add(index);
				else next.delete(index);
				return next;
			});
		},
		[],
	);

	const tableActions = useMemo<Table.Action<TableEventRow>[]>(
		() => [
			{
				icon: "Search",
				label: t("tableView.targetEvent"),
				onClick: handleRowAction,
			},
		],
		[handleRowAction, t],
	);

	/**
	 * Updates the selected source ids from the multi-source selector.
	 * @param action Next source ids or a React updater function.
	 * @returns Nothing.
	 */
	const handleSelectedSourcesChange = useCallback(
		(action: SetStateAction<Source.Id[]>) => {
			setSelectedSourceIds((previousSourceIds) => {
				const nextSourceIds =
					typeof action === "function" ? action(previousSourceIds) : action;

				return Array.from(new Set(nextSourceIds));
			});
		},
		[],
	);

	// --- Window Bridge Listener ---
	useEffect(() => {
		const bridgeId = WindowBridge.generateId();
		const bridge = WindowBridge.create(bridgeId, (message) => {
			if (message.type === WindowBridge.MessageType.TABLE_SELECT_SOURCE) {
				const payload =
					message.payload as WindowBridge.TableSelectSourcePayload;
				setSelectedSourceIds([payload.sourceId as Source.Id]);
			}
		});
		return () => bridge.destroy();
	}, []);

	// --- Constants ---

	const isAllSelected = data.length > 0 && selectedRows.size === data.length;
	const isInitialTableLoading =
		selectedSources.length > 0 &&
		(!localFieldTypeMap || (loading && data.length === 0));
	const tableTitle =
		selectedSources.length === 0
			? t("tableView.title")
			: selectedSources.length === 1
				? `${t("tableView.title")}: ${primarySelectedSource?.name}`
				: `${t("tableView.title")}: ${t("source.selectedSources", {
						count: selectedSources.length,
					})}`;

	return (
		<div
			className={s.main}
			ref={setContainer}
		>
			<div className={s.header}>
				<h2>{tableTitle}</h2>
				<TooltipProvider>
					<Tooltip>
						<TooltipTrigger asChild>
							<span>
								<Toggle
									checked={isSyncActive}
									onCheckedChange={setIsSynced}
									option={[t("tableView.detached"), t("tableView.synced")]}
									disabled={
										selectedSourceIds.length === 0 || isMultiSourceSelection
									}
								/>
							</span>
						</TooltipTrigger>
						<TooltipContent>
							<div style={{ maxWidth: 300 }}>
								{isSyncActive
									? t("tableView.syncedTooltip")
									: t("tableView.detachedTooltip")}
							</div>
						</TooltipContent>
					</Tooltip>
				</TooltipProvider>
			</div>

			<div className={s.row1}>
				<div className={s.sourceRow}>
					<Source.Select.Multi
						selected={selectedSourceIds}
						setSelected={handleSelectedSourcesChange}
						placeholder={t("source.selectSources")}
					/>
				</div>
				{!isSyncActive && (
					<TooltipProvider>
						<div className={s.filtersSection}>
							<Button
								variant="tertiary"
								onClick={() => setIsFiltersOpen(!isFiltersOpen)}
								icon={isFiltersOpen ? "ChevronUp" : "Filter"}
								className={s.filtersToggle}
							>
								{t("common.filters")}
							</Button>
							{isFiltersOpen && (
								<Stack
									dir="column"
									ai="stretch"
									className={s.filtersContent}
									gap={16}
								>
									<Stack
										ai="center"
										gap={12}
									>
										<Toggle
											checked={manual}
											onCheckedChange={setManual}
											option={[
												t("tableView.selectDates"),
												t("tableView.isoString"),
											]}
											disabled={selectedSourceIds.length === 0}
										/>
									</Stack>
									<div className={s.timeRow}>
										{manual ? (
											<>
												<InputISOSelection
													type="min"
													value={timeFrame.min}
													valid={isMinValid}
													onChange={(val) => handleTimeChange("min", val)}
												/>
												<InputISOSelection
													type="max"
													value={timeFrame.max}
													valid={isMaxValid}
													onChange={(val) => handleTimeChange("max", val)}
												/>
											</>
										) : (
											<>
												<InputDateSelection
													type="min"
													value={timeFrame.min}
													valid={isMinValid}
													onChange={(val) => handleTimeChange("min", val)}
												/>
												<InputDateSelection
													type="max"
													value={timeFrame.max}
													valid={isMaxValid}
													onChange={(val) => handleTimeChange("max", val)}
												/>
											</>
										)}
									</div>
									<OpenSearchQueryBuilder.Query.String
										textFilter={localSearchQuery}
										setTextFilter={setLocalSearchQuery}
										reset={() => setLocalSearchQuery("")}
									/>
									<Stack
										ai="center"
										gap={8}
									>
										<Checkbox
											id="tableViewFlaggedOnly"
											checked={localFlaggedOnly}
											onCheckedChange={(checked) =>
												setLocalFlaggedOnly(!!checked)
											}
										/>
										<Label
											htmlFor="tableViewFlaggedOnly"
											value={t("filterFile.flaggedOnly")}
											cursor="pointer"
										/>
										<Icon name="Flag" />
									</Stack>
									<OpenSearchQueryBuilder.Query.Add
										filters={localFilters}
										setFilters={setLocalFilters}
										container={container}
									/>
									<Separator />
									<OpenSearchQueryBuilder.Query.Filters
										filters={localFilters}
										setFilters={setLocalFilters}
										keys={columns || []}
										container={container}
									/>
									<Stack
										dir="row"
										jc="flex-end"
										gap={8}
									>
										<Button
											variant="secondary"
											onClick={resetFilters}
											icon="RotateCcw"
										>
											{t("common.reset")}
										</Button>
										<Button
											variant="secondary"
											onClick={applyFilters}
											icon="Check"
										>
											{t("common.apply")}
										</Button>
									</Stack>
								</Stack>
							)}
						</div>
					</TooltipProvider>
				)}
			</div>

			<div className={s.row2}>
				<div className={s.row2Left}>
					<Stack
						gap={8}
						ai="center"
					>
						<Checkbox
							checked={isAllSelected}
							onCheckedChange={(c) => handleSelectAll(!!c)}
						/>
						<span
							className={cn(s.label, s.pointer)}
							onClick={() => handleSelectAll(!isAllSelected)}
						>
							{t("tableView.selectAllVisible")}
						</span>
					</Stack>

					<Popover.Root>
						<Popover.Trigger asChild>
							<Button
								variant="secondary"
								icon="ChevronDown"
								disabled={selectedRows.size === 0}
							>
								{t("common.action")}
							</Button>
						</Popover.Trigger>
						<Popover.Content
							align="start"
							sideOffset={4}
							className={s.popoverContent}
							container={container}
						>
							<div
								className={s.menuItem}
								onClick={() => handleBulkAction("flagged")}
							>
								<Icon
									name="Flag"
									size={14}
								/>
								<span>{t("common.flag")}</span>
							</div>
							<div
								className={s.menuItem}
								onClick={() => handleBulkAction("notes")}
							>
								<Icon
									name="StickyNote"
									size={14}
								/>
								<span>{t("tableView.newNotes")}</span>
							</div>
						</Popover.Content>
					</Popover.Root>

					<Stack
						gap={8}
						ai="center"
					>
						<Label value={t("tableView.sort")} />
						<Select.Root
							onValueChange={handleSortFieldChange}
							value={sortField}
						>
							<Select.Trigger
								data-no-icon
								className={s.sortSelect}
							>
								<Select.Value />
							</Select.Trigger>
							<Select.Content container={container}>
								{sortableFields.map((f) => (
									<Select.Item
										key={f}
										value={f}
									>
										{f}
									</Select.Item>
								))}
							</Select.Content>
						</Select.Root>
						<Button
							variant="secondary"
							icon={
								sortDirection === "asc" ? "SortAscending" : "SortDescending"
							}
							onClick={toggleSortDirection}
							title={t("tableView.toggleSortDirection")}
						/>
					</Stack>
				</div>

				<div className={s.row2Right}>
					<span className={s.label}>
						{t("tableView.showingRange", {
							from: totalHits > 0 ? pageSize * (currentPage - 1) + 1 : 0,
							to: Math.min(pageSize * currentPage, totalHits),
							total: totalHits,
						})}
					</span>

					<Stack
						gap={8}
						ai="center"
					>
						<Select.Root
							onValueChange={(val) => setPageSize(Number(val))}
							value={pageSize.toString()}
						>
							<Select.Trigger className={s.pageSelect}>
								<Select.Value placeholder="50" />
							</Select.Trigger>
							<Select.Content container={container}>
								<Select.Item value="20">20</Select.Item>
								<Select.Item value="50">50</Select.Item>
								<Select.Item value="100">100</Select.Item>
							</Select.Content>
						</Select.Root>
						<span className={s.label}>{t("tableView.perPage")}</span>
					</Stack>

					<span className={s.label}>
						{t("tableView.pageOf", { page: currentPage, total: totalPages })}
					</span>

					<Stack gap={4}>
						<Button
							variant="secondary"
							icon="ChevronLeft"
							disabled={currentPage <= 1 || loading}
							onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
							title={t("tableView.previousPage")}
						/>
						<Button
							variant="secondary"
							icon="ChevronRight"
							disabled={currentPage >= totalPages || loading}
							onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
							title={t("tableView.nextPage")}
						/>
					</Stack>
				</div>
			</div>

			<div className={s.result}>
				{selectedSources.length === 0 ? (
					<div className={s.placeholder}>
						{t("tableView.selectSourceToViewEvents")}
					</div>
				) : isInitialTableLoading ? (
					<p className={s.label}>{t("tableView.loadingData")}</p>
				) : data.length > 0 ? (
					<Table
						persistId={`table-${selectedSourcesSignature}`}
						columnVisibility={true}
						values={data}
						columns={renderableColumns}
						includeIndex={false}
						notshow={DEFAULT_HIDDEN_FIELDS}
						selectable={true}
						selectedrows={selectedRows}
						onrowselect={handleTableRowSelect}
						onSelectAll={handleSelectAll}
						actions={tableActions}
						sortField={sortField}
						sortDirection={sortDirection}
						onSort={handleTableSort}
						highlightedId={app.timeline.target?._id}
					/>
				) : (
					<p className={s.label}>{t("tableView.noData")}</p>
				)}
			</div>
			{banner?.target === "table" && banner.node}
		</div>
	);
}
