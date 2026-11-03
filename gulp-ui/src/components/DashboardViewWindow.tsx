import {
	memo,
	startTransition,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import type { ChartData, ChartOptions } from "chart.js";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Application } from "@/context/Application.context";
import { Extension } from "@/context/Extension.context";
import { GulpDataset } from "@/class/Info";
import { Source } from "@/entities/Source";
import { Context } from "@/entities/Context";
import { Operation } from "@/entities/Operation";
import { Filter } from "@/entities/Filter";
import { Internal } from "@/entities/addon/Internal";
import { OpenSearchQueryBuilder } from "@/components/QueryBuilder";
import { Bar, Line } from "@/components/DashboardChartAdapter";
import { Banner } from "@/ui/Banner";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Popover } from "@/ui/Popover";
import { Select } from "@/ui/Select";
import { Stack } from "@/ui/Stack";
import { Table } from "@/components/Table";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { Locale } from "@/locales";
import { format } from "date-fns";
import s from "./styles/DashboardViewWindow.module.css";
import { Icon } from "@/ui/Icon";
import { stringToHexColor } from "@/ui/utils";
import { DataWorker } from "@/workers/DataWorker.class";
import type {
	DashboardAggregationChartBucket,
	DashboardAggregationTimeRow,
	DashboardAggregationValueRow,
	NormalizeDashboardAggregationResult,
} from "@/workers/DataWorker.types";
import {
	serializeSemanticValue,
	serializeSourceFieldMetadata,
	serializeSourceIdSet,
} from "./DetachedViewQuery.utils";

export namespace DashboardViewWindow {
	export interface Props {
		onClose?: () => void;
	}
}

type AggregationFunction = "top_values" | "top_values_by_time" | "rare_values";
type DashboardChartType = "line_chart" | "vertical_chart";
type DashboardSharedObjectType = "fixed" | "operation" | "custom";
type FixedDashboardSharedObjectType = Extract<
	DashboardSharedObjectType,
	"fixed"
>;
type DateHistogramIntervalType = "calendar_interval" | "fixed_interval";

interface DateHistogramInterval {
	type: DateHistogramIntervalType;
	value: string;
}

interface AnalyticsFilterState {
	sourceIds: Source.Id[];
	range: {
		min: string;
		max: string;
	};
	textFilter: string;
	filters: Filter.Item[];
}

interface DashboardDefinition extends Record<string, unknown> {
	id: string;
	name: string;
	type?: DashboardSharedObjectType;
	dashboard: DashboardChartType;
	required_ecs?: string[];
	labels?: Record<string, string>;
	opensearch_query?: {
		aggs?: Record<string, unknown>;
		query?: Record<string, unknown>;
	};
	visible?: boolean;
}

interface FieldInfoRow extends Record<string, string | number> {
	field: string;
	type: string;
	sources: number;
}

type AggregationTableRow = DashboardAggregationValueRow;
type TimeAggregationTableRow = DashboardAggregationTimeRow;
type ChartBucket = DashboardAggregationChartBucket;
type FieldAggregationResult = Extract<
	NormalizeDashboardAggregationResult,
	{ mode: "field_values" | "field_time" }
>;

interface ChartPalette {
	accent: string;
	border: string;
	text: string;
	grid: string;
	bars: string[];
}

const LAST_AGGREGATION_STORAGE_KEY = "dashboard.lastAggregationFunction";
const DEFAULT_TOP_VALUES_SIZE = 10;
const DEFAULT_TIME_BUCKET_SIZE = 5;
const DEFAULT_MAX_DOC_COUNT = 5;
const DEFAULT_AGGREGATION_INTERVAL = "15m";
const DASHBOARD_MAX_CHART_BUCKETS = 500;
const DASHBOARD_FIELD_NORMALIZATION_CHUNK_SIZE = 1000;
const EMPTY_VALUE_ROWS: AggregationTableRow[] = [];
const EMPTY_TIME_ROWS: TimeAggregationTableRow[] = [];
const DASHBOARD_FIELD_METADATA_CONCURRENCY = 4;
const DASHBOARD_CARD_AGGREGATION_CONCURRENCY = 3;
const FIELD_AGGREGATION_EXCLUDED_PREFIXES = ["gulp.unmapped", "gulp.enriched"];
const CALENDAR_INTERVAL_UNITS = new Set([
	"minute",
	"hour",
	"day",
	"week",
	"month",
	"quarter",
	"year",
]);
const CALENDAR_INTERVAL_VALUE_PATTERN = /^1\d*(?:M|q|y)$/;
const FIXED_INTERVAL_VALUE_PATTERN = /^\d+(?:ms|s|m|h|d)$/;
let fixedDashboardDefinitionsPromise: Promise<DashboardDefinition[]> | null = null;

/**
 * Runs async work across a list with a fixed concurrency limit.
 * @param items Items to process.
 * @param concurrency Maximum number of active tasks.
 * @param task Async task for one item.
 * @returns Ordered results matching the input list.
 */
async function runWithConcurrency<TItem, TResult>(
	items: TItem[],
	concurrency: number,
	task: (item: TItem, index: number) => Promise<TResult>,
): Promise<TResult[]> {
	const results: TResult[] = [];
	let nextIndex = 0;

	const workers = Array.from(
		{ length: Math.min(concurrency, items.length) },
		async () => {
			while (nextIndex < items.length) {
				const index = nextIndex;
				nextIndex += 1;
				results[index] = await task(items[index], index);
			}
		},
	);

	await Promise.all(workers);
	return results;
}

/**
 * Creates a FIFO promise scheduler for limiting dashboard request fanout.
 * @param concurrency Maximum number of active tasks.
 * @returns Function that schedules one async task.
 */
function createTaskQueue(concurrency: number) {
	let activeCount = 0;
	const queue: Array<() => void> = [];

	/**
	 * Starts queued tasks while capacity is available.
	 * @returns Nothing.
	 */
	const drainQueue = (): void => {
		while (activeCount < concurrency && queue.length > 0) {
			const run = queue.shift();
			run?.();
		}
	};

	return function scheduleTask<TResult>(
		task: () => Promise<TResult>,
		signal?: AbortSignal,
	): Promise<TResult> {
		return new Promise<TResult>((resolve, reject) => {
			/**
			 * Executes the scheduled task and releases capacity afterward.
			 * @returns Nothing.
			 */
			const run = (): void => {
				if (signal?.aborted) {
					reject(new DOMException("Aborted", "AbortError"));
					return;
				}

				activeCount += 1;
				task()
					.then(resolve)
					.catch(reject)
					.finally(() => {
						activeCount -= 1;
						drainQueue();
					});
			};

			queue.push(run);
			drainQueue();
		});
	};
}

const scheduleDashboardCardAggregation = createTaskQueue(
	DASHBOARD_CARD_AGGREGATION_CONCURRENCY,
);

/**
 * Parses a positive integer input value for OpenSearch aggregation options.
 *
 * @param value - Raw numeric input text entered by the analyst.
 * @param fallback - Safe value to use when the input is empty or invalid.
 * @returns Positive integer accepted by aggregation size/count options.
 */
function parsePositiveIntegerInput(value: string, fallback: number): number {
	const parsed = Number.parseInt(value, 10);

	if (!Number.isFinite(parsed) || parsed < 1) {
		return fallback;
	}

	return parsed;
}

/**
 * Checks whether a field can be selected for ad-hoc dashboard aggregations.
 *
 * @param field - Field name returned by the source event-key metadata endpoint.
 * @returns True when the field is safe to expose in the aggregation field list.
 */
function isFieldAggregationAllowed(field: string): boolean {
	return !FIELD_AGGREGATION_EXCLUDED_PREFIXES.some((prefix) =>
		field.startsWith(prefix),
	);
}

/**
 * Resolves whether a date histogram interval should use calendar or fixed mode.
 *
 * @param value - Raw interval text entered by the analyst.
 * @returns OpenSearch date_histogram interval key and sanitized value.
 */
function resolveDateHistogramInterval(value: string): DateHistogramInterval {
	const interval = value.trim();

	if (
		CALENDAR_INTERVAL_UNITS.has(interval) ||
		CALENDAR_INTERVAL_VALUE_PATTERN.test(interval)
	) {
		console.warn(`calendar_interval ${interval}`);
		return { type: "calendar_interval", value: interval };
	}

	if (FIXED_INTERVAL_VALUE_PATTERN.test(interval)) {
		console.warn(`fixed_interval ${interval}`);
		return { type: "fixed_interval", value: interval };
	}

	return { type: "fixed_interval", value: DEFAULT_AGGREGATION_INTERVAL };
}

/**
 * Converts a nanosecond timestamp string into a browser datetime-local value.
 *
 * @param nanos - Nanosecond timestamp accepted by the existing filter model.
 * @returns Date formatted for an input with type datetime-local.
 */
function formatDateTimeLocal(nanos: string): string {
	try {
		return format(
			Internal.Transformator.toTimestamp(nanos),
			"yyyy-MM-dd'T'HH:mm",
		);
	} catch {
		return "";
	}
}

/**
 * Resolves the initial dashboard time range from the currently available sources.
 *
 * @param sources - Sources participating in the current operation view.
 * @returns Min/max nanosecond range as strings.
 */
function resolveInitialRange(
	sources: Source.Type[],
): AnalyticsFilterState["range"] {
	const mins = sources
		.map((source) => source.nanotimestamp?.min)
		.filter(Boolean);
	const maxes = sources
		.map((source) => source.nanotimestamp?.max)
		.filter(Boolean);

	if (mins.length === 0 || maxes.length === 0) {
		const now = Internal.Transformator.toNanos(Date.now()).toString();
		return { min: now, max: now };
	}

	return {
		min: mins
			.reduce((min, value) => (value < min ? value : min), mins[0])
			.toString(),
		max: maxes
			.reduce((max, value) => (value > max ? value : max), maxes[0])
			.toString(),
	};
}

/**
 * Creates the initial filter state used by the Data Explorer.
 *
 * @param sources - Selected operation sources used to derive the default time range.
 * @returns Empty global filters scoped to the operation time range.
 */
function createInitialFilterState(
	sources: Source.Type[],
): AnalyticsFilterState {
	return {
		sourceIds: [],
		range: resolveInitialRange(sources),
		textFilter: "",
		filters: [],
	};
}

/**
 * Returns a stable string key for filter state changes that should refetch data.
 *
 * @param filterState - Current dashboard filter state.
 * @returns Serialized query-relevant filter values.
 */
function serializeFilterState(filterState: AnalyticsFilterState): string {
	return serializeSemanticValue({
		sourceIds: [...filterState.sourceIds].sort((left, right) =>
			left.localeCompare(right),
		),
		range: filterState.range,
		textFilter: filterState.textFilter,
		filters: filterState.filters,
	});
}

/**
 * Returns a stable string key for source selections that affect dashboard queries.
 *
 * @param sourceIds - Source identifiers selected in the operation view.
 * @returns Serialized source identifier list.
 */
function serializeSourceIds(sourceIds: Source.Id[]): string {
	return serializeSourceIdSet(sourceIds);
}

/**
 * Returns a stable string key for dashboard aggregation definitions.
 *
 * @param dashboard - Dashboard definition returned by shared objects.
 * @returns Serialized aggregation payload used by the fixed dashboard query.
 */
function serializeDashboardAggregations(
	dashboard: DashboardDefinition,
): string {
	return serializeSemanticValue(dashboard.opensearch_query?.aggs ?? {});
}

/**
 * Collects enabled leaf filter fields, including fields inside nested groups.
 *
 * @param filters - Filter items configured in the dashboard filter editor.
 * @returns Field names whose type metadata can affect query clause generation.
 */
function getEnabledFilterFieldNames(filters: Filter.Item[]): string[] {
	return filters.flatMap((item) => {
		if (!item.enabled) {
			return [];
		}

		if (Filter.isGroup(item)) {
			return getEnabledFilterFieldNames(item.children);
		}

		return item.field ? [item.field] : [];
	});
}

/**
 * Returns a stable key for field type entries used by enabled custom filters.
 *
 * @param filterState - Filter state whose enabled clauses may require field types.
 * @param fieldTypeMap - Field metadata used by the OpenSearch query builder.
 * @returns Serialized field type subset relevant to the current filters.
 */
function serializeRelevantFieldTypes(
	filterState: AnalyticsFilterState,
	fieldTypeMap: Record<string, string>,
): string {
	const fieldNames = Array.from(
		new Set(getEnabledFilterFieldNames(filterState.filters)),
	).sort((a, b) => a.localeCompare(b));

	return serializeSemanticValue(
		fieldNames.map((field) => [field, fieldTypeMap[field] ?? ""]),
	);
}

/**
 * Keeps an equivalent semantic value referentially stable across context snapshots.
 * @param value Latest value derived from application state.
 * @param signature Semantic signature for the value.
 * @returns Previous value while the signature remains unchanged.
 */
function useSemanticValue<T>(value: T, signature: string): T {
	const stableRef = useRef({ signature, value });
	if (stableRef.current.signature !== signature) {
		stableRef.current = { signature, value };
	}
	return stableRef.current.value;
}

/**
 * Checks whether two string/number records contain the same values.
 *
 * @param left - First record to compare.
 * @param right - Second record to compare.
 * @returns True when both records have identical keys and values.
 */
function areRecordsEqual<T extends string | number>(
	left: Record<string, T>,
	right: Record<string, T>,
): boolean {
	const leftKeys = Object.keys(left);
	const rightKeys = Object.keys(right);

	if (leftKeys.length !== rightKeys.length) {
		return false;
	}

	return leftKeys.every((key) => left[key] === right[key]);
}

/**
 * Resolves the static display color used for source-labelled dashboard buckets.
 *
 * @param source - Source represented by an aggregation bucket.
 * @param contextColorById - Context colors keyed by context ID.
 * @returns Source color, context color, or deterministic context fallback.
 */
function resolveDashboardSourceColor(
	source: Source.Type,
	contextColorById: Map<string, string>,
): string {
	return (
		source.color ||
		contextColorById.get(source.context_id) ||
		stringToHexColor(source.context_id)
	);
}

/**
 * Builds the OpenSearch bool query shared by dashboards and ad-hoc field aggregations.
 *
 * @param operationId - Selected operation identifier.
 * @param filterState - Global or dashboard-local filter state.
 * @param fieldTypeMap - Field type map used by the query builder conversion helpers.
 * @param defaultSourceIds - Source IDs selected in the main operation view.
 * @returns OpenSearch bool query object.
 */
function buildAnalyticsQuery(
	operationId: Operation.Id,
	filterState: AnalyticsFilterState,
	fieldTypeMap: Record<string, string>,
	defaultSourceIds: Source.Id[] = [],
): Record<string, unknown> {
	const sourceIds =
		filterState.sourceIds.length > 0 ? filterState.sourceIds : defaultSourceIds;
	const bool: Record<
		OpenSearchQueryBuilder.Operator,
		Record<string, unknown>[]
	> = {
		must: [{ term: { "gulp.operation_id": operationId } }],
		should: [],
		must_not: [],
		filter: [
			{
				range: {
					"gulp.timestamp": {
						gte: filterState.range.min,
						lte: filterState.range.max,
					},
				},
			},
		],
	};

	if (sourceIds.length > 0) {
		bool.must.push({ terms: { "gulp.source_id": sourceIds } });
	}

	if (filterState.textFilter.trim()) {
		bool.must.push({
			wildcard: {
				"event.original": {
					value: filterState.textFilter.trim(),
					case_insensitive: true,
				},
			},
		});
	}

	filterState.filters.forEach((item) => {
		if (!item.enabled) return;
		const clause = Filter.Entity.buildItemClause(item, fieldTypeMap);
		if (clause) {
			bool[item.operator].push(clause);
		}
	});

	Object.keys(bool).forEach((key) => {
		const operator = key as OpenSearchQueryBuilder.Operator;
		if (bool[operator].length === 0) {
			delete bool[operator];
		}
	});

	return { bool };
}

/**
 * Resolves the field name to use in terms-like aggregations.
 *
 * @param field - ECS/OpenSearch field selected by the analyst.
 * @param fieldTypeMap - Field type map returned by the backend.
 * @returns Aggregation-safe field name.
 */
function resolveAggregationField(
	field: string,
	fieldTypeMap: Record<string, string>,
): string {
	if (fieldTypeMap[field] === "text") {
		return `${field}.keyword`;
	}

	return field;
}

/**
 * Builds an aggregation body for the selected field and aggregation function.
 *
 * @param query - OpenSearch bool query built from the active filters.
 * @param field - Selected field to aggregate.
 * @param fieldTypeMap - Field type map returned by the backend.
 * @param aggregationFunction - Selected aggregation function.
 * @param options - Function-specific size, interval, and max-doc-count values.
 * @returns Query aggregation body accepted by /query_aggregation.
 */
function buildFieldAggregationBody(
	query: Record<string, unknown>,
	field: string,
	fieldTypeMap: Record<string, string>,
	aggregationFunction: AggregationFunction,
	options: {
		topValuesSize: number;
		timeBucketSize: number;
		interval: string;
		maxDocCount: number;
	},
): GulpDataset.QueryAggregation.Body {
	const aggregationField = resolveAggregationField(field, fieldTypeMap);

	if (aggregationFunction === "top_values_by_time") {
		const histogramInterval = resolveDateHistogramInterval(options.interval);

		return {
			query,
			aggs: {
				timeline_buckets: {
					date_histogram: {
						field: "@timestamp",
						[histogramInterval.type]: histogramInterval.value,
						min_doc_count: 1,
					},
					aggs: {
						top_values_in_bucket: {
							terms: {
								field: aggregationField,
								size: options.timeBucketSize,
							},
						},
					},
				},
			},
		};
	}

	if (aggregationFunction === "rare_values") {
		return {
			query,
			aggs: {
				rare_values: {
					rare_terms: {
						field: aggregationField,
						max_doc_count: options.maxDocCount,
					},
				},
			},
		};
	}

	return {
		query,
		aggs: {
			top_values: {
				terms: {
					field: aggregationField,
					size: options.topValuesSize,
					order: { _count: "desc" },
				},
			},
		},
	};
}

/**
 * Checks whether an unknown value can be safely inspected as an object record.
 *
 * @param value - Value to inspect before reading dynamic properties.
 * @returns True when the value is a non-array object record.
 */
function isRecordValue(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Checks whether a dashboard definition type belongs to the fixed section.
 *
 * @param type - Dashboard scope/category returned inside the shared object payload.
 * @returns True for fixed dashboard definitions.
 */
function isFixedDashboardType(
	type: unknown,
): type is FixedDashboardSharedObjectType {
	return type === "fixed";
}

/**
 * Checks whether a dashboard object declares a supported chart renderer.
 *
 * @param dashboard - Dashboard chart field returned inside the shared object obj.
 * @returns True when the chart kind can be rendered by this view.
 */
function isDashboardChartType(
	dashboard: unknown,
): dashboard is DashboardChartType {
	return dashboard === "line_chart" || dashboard === "vertical_chart";
}

/**
 * Checks whether a normalized dashboard contains an aggregation definition.
 *
 * @param dashboard - Dashboard candidate to validate.
 * @returns True when the dashboard has OpenSearch aggregations to execute.
 */
function hasDashboardAggregations(
	dashboard: Record<string, unknown>,
): dashboard is DashboardDefinition {
	const opensearchQuery = dashboard.opensearch_query;
	return isRecordValue(opensearchQuery) && isRecordValue(opensearchQuery.aggs);
}

/**
 * Normalizes fixed shared object payloads into dashboard definitions.
 *
 * @param sharedObjects - Shared objects returned by the backend.
 * @returns Visible fixed dashboard definitions returned by the backend.
 */
function normalizeDashboardDefinitions(
	sharedObjects: GulpDataset.SharedObject.Type<Record<string, unknown>>[],
): DashboardDefinition[] {
	return sharedObjects
		.map((sharedObject) => {
			const definition = sharedObject.obj;
			const id =
				typeof definition.id === "string"
					? definition.id
					: (sharedObject.obj_id ?? sharedObject.id);
			const name =
				typeof definition.name === "string"
					? definition.name
					: sharedObject.name;

			const normalizedDefinition: Record<string, unknown> = {
				...definition,
				id,
				name,
			};

			return normalizedDefinition;
		})
		.filter((definition): definition is DashboardDefinition => {
			return (
				isFixedDashboardType(definition.type) &&
				definition.visible !== false &&
				typeof definition.id === "string" &&
				typeof definition.name === "string" &&
				isDashboardChartType(definition.dashboard) &&
				hasDashboardAggregations(definition)
			);
		});
}

/**
 * Builds the aggregation body for a fixed dashboard definition.
 *
 * @param dashboard - Dashboard definition to render.
 * @param query - Active OpenSearch bool query.
 * @returns Aggregation body or null when the definition cannot be queried.
 */
function buildDashboardAggregationBody(
	dashboard: DashboardDefinition,
	query: Record<string, unknown>,
): GulpDataset.QueryAggregation.Body | null {
	if (dashboard.opensearch_query?.aggs) {
		return {
			query,
			aggs: dashboard.opensearch_query.aggs,
		};
	}

	return null;
}

/**
 * Throws when a chunked dashboard normalization run has been superseded.
 *
 * @param signal - Request signal shared by the network request and normalization work.
 * @returns Nothing.
 */
function throwIfDashboardNormalizationAborted(signal: AbortSignal): void {
	if (signal.aborted) {
		throw new DOMException("Aborted", "AbortError");
	}
}

/**
 * Yields back to the browser between dashboard normalization chunks.
 *
 * @param signal - Request signal checked before and after yielding.
 * @returns Promise resolved after the browser has a chance to handle input.
 */
async function yieldDashboardNormalizationChunk(
	signal: AbortSignal,
): Promise<void> {
	throwIfDashboardNormalizationAborted(signal);
	await new Promise<void>((resolve) => setTimeout(resolve, 0));
	throwIfDashboardNormalizationAborted(signal);
}

/**
 * Finds the first aggregation bucket list in a dashboard aggregation response.
 *
 * @param response - Aggregation response returned by the backend.
 * @returns First bucket list found in the response.
 */
function getDashboardAggregationBuckets(
	response: GulpDataset.QueryAggregation.Response,
): GulpDataset.QueryAggregation.Bucket[] {
	for (const aggregation of Object.values(response.aggregations ?? {})) {
		if (Array.isArray(aggregation.buckets)) {
			return aggregation.buckets;
		}
	}

	return [];
}

/**
 * Checks whether a dynamic aggregation property contains nested buckets.
 *
 * @param value - Dynamic property read from an aggregation bucket.
 * @returns True when the value is a nested bucket aggregation.
 */
function isDashboardNestedBucketAggregation(
	value: unknown,
): value is { buckets: GulpDataset.QueryAggregation.Bucket[] } {
	return (
		typeof value === "object" &&
		value !== null &&
		"buckets" in value &&
		Array.isArray((value as { buckets?: unknown }).buckets)
	);
}

/**
 * Normalizes value aggregation buckets without monopolizing the main thread.
 *
 * @param response - Aggregation response returned by the backend.
 * @param signal - Request signal used to cancel stale normalization.
 * @returns Table rows with value, count, and percentage columns.
 */
async function normalizeValueRowsInChunks(
	response: GulpDataset.QueryAggregation.Response,
	signal: AbortSignal,
): Promise<AggregationTableRow[]> {
	const buckets = getDashboardAggregationBuckets(response);
	const total = Math.max(response.total_hits, 1);
	const rows: AggregationTableRow[] = [];

	for (let index = 0; index < buckets.length; index += 1) {
		throwIfDashboardNormalizationAborted(signal);
		const bucket = buckets[index];
		rows.push({
			value: String(bucket.key_as_string ?? bucket.key),
			count: bucket.doc_count,
			percent: Number(((bucket.doc_count / total) * 100).toFixed(2)),
		});

		if (
			index > 0 &&
			index % DASHBOARD_FIELD_NORMALIZATION_CHUNK_SIZE === 0
		) {
			await yieldDashboardNormalizationChunk(signal);
		}
	}

	return rows;
}

/**
 * Normalizes time-bucketed aggregations without monopolizing the main thread.
 *
 * @param response - Aggregation response returned by the backend.
 * @param signal - Request signal used to cancel stale normalization.
 * @returns Flat table rows with time, value, and count columns.
 */
async function normalizeTimeRowsInChunks(
	response: GulpDataset.QueryAggregation.Response,
	signal: AbortSignal,
): Promise<TimeAggregationTableRow[]> {
	const buckets = getDashboardAggregationBuckets(response);
	const rows: TimeAggregationTableRow[] = [];
	let rowsSinceLastYield = 0;

	for (const bucket of buckets) {
		throwIfDashboardNormalizationAborted(signal);
		const time = String(bucket.key_as_string ?? bucket.key);
		const nested = Object.values(bucket).find(
			isDashboardNestedBucketAggregation,
		);

		if (!nested) {
			rows.push({
				time,
				value: "-",
				count: bucket.doc_count,
			});
			rowsSinceLastYield += 1;
		} else {
			for (const nestedBucket of nested.buckets) {
				throwIfDashboardNormalizationAborted(signal);
				rows.push({
					time,
					value: String(nestedBucket.key_as_string ?? nestedBucket.key),
					count: nestedBucket.doc_count,
				});
				rowsSinceLastYield += 1;

				if (rowsSinceLastYield >= DASHBOARD_FIELD_NORMALIZATION_CHUNK_SIZE) {
					rowsSinceLastYield = 0;
					await yieldDashboardNormalizationChunk(signal);
				}
			}
		}

		if (rowsSinceLastYield >= DASHBOARD_FIELD_NORMALIZATION_CHUNK_SIZE) {
			rowsSinceLastYield = 0;
			await yieldDashboardNormalizationChunk(signal);
		}
	}

	return rows;
}

/**
 * Normalizes the selected field aggregation into the active table result shape.
 *
 * @param response - Aggregation response returned by the backend.
 * @param mode - Target field aggregation display mode.
 * @param signal - Request signal used to cancel stale normalization.
 * @returns Render-ready field aggregation result.
 */
async function normalizeFieldAggregationResultInChunks(
	response: GulpDataset.QueryAggregation.Response,
	mode: FieldAggregationResult["mode"],
	signal: AbortSignal,
): Promise<FieldAggregationResult> {
	if (mode === "field_time") {
		return {
			mode,
			rows: await normalizeTimeRowsInChunks(response, signal),
		};
	}

	return {
		mode,
		rows: await normalizeValueRowsInChunks(response, signal),
	};
}

/**
 * Reads CSS custom properties into concrete colors that Canvas can render.
 *
 * @param documentRef - Document hosting the detached dashboard.
 * @returns Palette values for Chart.js datasets and axes.
 */
function readChartPalette(documentRef: Document): ChartPalette {
	const styles = documentRef.defaultView?.getComputedStyle(
		documentRef.documentElement,
	);
	const read = (name: string, fallback: string) =>
		styles?.getPropertyValue(name).trim() || fallback;

	return {
		accent: read("--accent", "#67e8f9"),
		border: read("--gray-400", "#3f3f46"),
		text: read("--gray-1000", "#e4e4e7"),
		grid: read("--gray-alpha-400", "rgba(128,128,128,0.28)"),
		bars: [
			read("--accent", "#67e8f9"),
			read("--second", "#fda4af"),
			"#a5b4fc",
			"#fcd34d",
			"#86efac",
			"#f0abfc",
		],
	};
}

/**
 * Renders global or card-local dashboard filter fields.
 *
 * @param props - Filter state, available field keys, and optional source selector data.
 */
function DashboardFilterEditor({
	filterState,
	setFilterState,
	fieldKeys,
	container,
	availableSources,
	inheritedSourceIds = [],
	showSourceSelector = true,
}: {
	filterState: AnalyticsFilterState;
	setFilterState: React.Dispatch<React.SetStateAction<AnalyticsFilterState>>;
	fieldKeys: string[];
	container: HTMLDivElement | null;
	availableSources?: Source.Type[];
	inheritedSourceIds?: Source.Id[];
	showSourceSelector?: boolean;
}) {
	const { t } = Locale.use();
	const selectedSourceIds =
		filterState.sourceIds.length > 0
			? filterState.sourceIds
			: inheritedSourceIds;

	const setSelectedSources = useCallback(
		(action: React.SetStateAction<Source.Id[]>) => {
			setFilterState((prev) => ({
				...prev,
				sourceIds:
					typeof action === "function" ? action(prev.sourceIds) : action,
			}));
		},
		[setFilterState],
	);

	const setTextFilter = useCallback(
		(textFilter: string) => {
			setFilterState((prev) => ({ ...prev, textFilter }));
		},
		[setFilterState],
	);

	const setFilters = useCallback(
		(action: Filter.Item[] | ((prev: Filter.Item[]) => Filter.Item[])) => {
			setFilterState((prev) => ({
				...prev,
				filters: typeof action === "function" ? action(prev.filters) : action,
			}));
		},
		[setFilterState],
	);

	const handleRangeChange = useCallback(
		(key: keyof AnalyticsFilterState["range"], value: string) => {
			setFilterState((prev) => ({
				...prev,
				range: {
					...prev.range,
					[key]: Internal.Transformator.toNanos(value).toString(),
				},
			}));
		},
		[setFilterState],
	);

	return (
		<Stack
			dir="column"
			ai="stretch"
			gap={12}
			className={s.filterEditor}
		>
			{showSourceSelector ? (
				<div className={s.filterControl}>
					<Source.Select.Multi
						sources={availableSources}
						selected={selectedSourceIds}
						setSelected={setSelectedSources}
						placeholder={t("dashboard.sourcesPlaceholder")}
					/>
				</div>
			) : null}
			<div className={s.timeGrid}>
				<Input
					label={t("common.from")}
					type="datetime-local"
					icon="Calendar"
					variant="highlighted"
					value={formatDateTimeLocal(filterState.range.min)}
					onChange={(event) => handleRangeChange("min", event.target.value)}
				/>
				<Input
					label={t("common.to")}
					type="datetime-local"
					icon="Calendar"
					variant="highlighted"
					value={formatDateTimeLocal(filterState.range.max)}
					onChange={(event) => handleRangeChange("max", event.target.value)}
				/>
			</div>
			<OpenSearchQueryBuilder.Query.String
				textFilter={filterState.textFilter}
				setTextFilter={setTextFilter}
				reset={() => setTextFilter("")}
			/>
			<OpenSearchQueryBuilder.Query.Add
				filters={filterState.filters}
				setFilters={setFilters}
				container={container}
			/>
			<OpenSearchQueryBuilder.Query.Filters
				filters={filterState.filters}
				setFilters={setFilters}
				keys={fieldKeys}
				container={container}
			/>
		</Stack>
	);
}

/**
 * Opens dashboard filters in a portal banner and applies changes on confirmation.
 *
 * @param props - Banner title, initial draft state, and apply callback.
 */
function DashboardFilterBanner({
	title,
	initialFilterState,
	onApply,
	fieldKeys,
	container,
	availableSources,
	inheritedSourceIds,
	showSourceSelector,
}: {
	title: string;
	initialFilterState: AnalyticsFilterState;
	onApply: (filterState: AnalyticsFilterState) => void;
	fieldKeys: string[];
	container: HTMLDivElement | null;
	availableSources?: Source.Type[];
	inheritedSourceIds?: Source.Id[];
	showSourceSelector: boolean;
}) {
	const { destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [draftFilterState, setDraftFilterState] =
		useState<AnalyticsFilterState>(initialFilterState);

	/**
	 * Applies the draft filters to the owning dashboard scope and closes the banner.
	 *
	 * @returns Nothing.
	 */
	const handleApplyFilters = useCallback(() => {
		onApply(draftFilterState);
		destroyBanner();
	}, [destroyBanner, draftFilterState, onApply]);

	return (
		<Banner
			title={title}
			container={container}
			className={s.filterBanner}
			done={
				<Button
					icon="Check"
					variant="glass"
					onClick={handleApplyFilters}
				>
					{t("common.apply")}
				</Button>
			}
		>
			<TooltipProvider>
				<DashboardFilterEditor
					filterState={draftFilterState}
					setFilterState={setDraftFilterState}
					fieldKeys={fieldKeys}
					container={container}
					availableSources={availableSources}
					inheritedSourceIds={inheritedSourceIds}
					showSourceSelector={showSourceSelector}
				/>
			</TooltipProvider>
		</Banner>
	);
}

/**
 * Renders the compact Data Explorer source selector and opens the full filters banner.
 *
 * @param props - Global filter state and data needed by the banner editor.
 */
function DashboardFilterControls({
	filterState,
	setFilterState,
	fieldKeys,
	container,
	availableSources,
	inheritedSourceIds = [],
}: {
	filterState: AnalyticsFilterState;
	setFilterState: React.Dispatch<React.SetStateAction<AnalyticsFilterState>>;
	fieldKeys: string[];
	container: HTMLDivElement | null;
	availableSources?: Source.Type[];
	inheritedSourceIds?: Source.Id[];
}) {
	const { spawnBanner } = Application.use();
	const { t } = Locale.use();
	const selectedSourceIds =
		filterState.sourceIds.length > 0
			? filterState.sourceIds
			: inheritedSourceIds;

	/**
	 * Updates the global source selection while preserving the rest of the filters.
	 *
	 * @param action - New source IDs or updater callback from the source selector.
	 * @returns Nothing.
	 */
	const setSelectedSources = useCallback(
		(action: React.SetStateAction<Source.Id[]>) => {
			setFilterState((prev) => ({
				...prev,
				sourceIds:
					typeof action === "function" ? action(prev.sourceIds) : action,
			}));
		},
		[setFilterState],
	);

	/**
	 * Opens the global filter editor while leaving source selection in the sidebar.
	 *
	 * @returns Nothing.
	 */
	const openGlobalFilterBanner = useCallback(() => {
		spawnBanner(
			<DashboardFilterBanner
				title={t("common.filters")}
				initialFilterState={filterState}
				onApply={(draftFilterState) =>
					setFilterState((prev) => ({
						...draftFilterState,
						sourceIds: prev.sourceIds,
					}))
				}
				fieldKeys={fieldKeys}
				container={container}
				availableSources={availableSources}
				inheritedSourceIds={inheritedSourceIds}
				showSourceSelector={false}
			/>,
			"table",
		);
	}, [
		availableSources,
		container,
		fieldKeys,
		filterState,
		inheritedSourceIds,
		setFilterState,
		spawnBanner,
		t,
	]);

	return (
		<Stack
			dir="column"
			ai="stretch"
			gap={8}
		>
			<div className={s.filterControl}>
				<Source.Select.Multi
					sources={availableSources}
					selected={selectedSourceIds}
					setSelected={setSelectedSources}
					placeholder={t("dashboard.sourcesPlaceholder")}
				/>
			</div>
			<Button
				className={s.filterButton}
				icon="Filter"
				variant="secondary"
				onClick={openGlobalFilterBanner}
			>
				{t("common.filters")}
			</Button>
		</Stack>
	);
}

interface DashboardFieldListProps {
	rows: FieldInfoRow[];
	selectedField: string | null;
	onSelectField: (field: string) => void;
}

interface DashboardFieldRowProps {
	row: FieldInfoRow;
	selected: boolean;
	fieldCountLabel: string;
	onSelectField: (field: string) => void;
}

/**
 * Renders one selectable field row in the virtualized available-fields list.
 *
 * @param props - Field row data and selection callback.
 */
const DashboardFieldRow = memo(function DashboardFieldRow({
	row,
	selected,
	fieldCountLabel,
	onSelectField,
}: DashboardFieldRowProps) {
	/**
	 * Selects this field for ad-hoc aggregation.
	 *
	 * @returns Nothing.
	 */
	const handleClick = useCallback(() => {
		onSelectField(row.field);
	}, [onSelectField, row.field]);

	return (
		<button
			type="button"
			className={selected ? s.fieldRowActive : s.fieldRow}
			onClick={handleClick}
		>
			<span>{row.field}</span>
			<small>{fieldCountLabel}</small>
		</button>
	);
});

/**
 * Renders available aggregation fields with viewport virtualization.
 *
 * @param props - Field rows, active field, and selection callback.
 */
function DashboardFieldList({
	rows,
	selectedField,
	onSelectField,
}: DashboardFieldListProps) {
	const { t } = Locale.use();
	const scrollContainerRef = useRef<HTMLDivElement | null>(null);
	const rowVirtualizer = useVirtualizer({
		count: rows.length,
		getScrollElement: () => scrollContainerRef.current,
		estimateSize: () => 38,
		overscan: 12,
	});

	return (
		<div
			className={s.fieldList}
			ref={scrollContainerRef}
		>
			<div
				className={s.fieldListContent}
				style={{ height: rowVirtualizer.getTotalSize() }}
			>
				{rowVirtualizer.getVirtualItems().map((virtualRow) => {
					const row = rows[virtualRow.index];
					if (!row) return null;

					return (
						<div
							key={row.field}
							className={s.fieldListVirtualRow}
							style={{ transform: `translateY(${virtualRow.start}px)` }}
						>
							<DashboardFieldRow
								row={row}
								selected={row.field === selectedField}
								fieldCountLabel={t("dashboard.fieldCount", {
									count: row.sources,
								})}
								onSelectField={onSelectField}
							/>
						</div>
					);
				})}
			</div>
		</div>
	);
}

interface DashboardTimeRowsTableProps {
	rows: TimeAggregationTableRow[];
}

/**
 * Renders time aggregation rows with div-based virtualization to avoid huge table layout.
 *
 * @param props - Time aggregation rows to display.
 */
function DashboardTimeRowsTable({ rows }: DashboardTimeRowsTableProps) {
	const { t } = Locale.use();
	const scrollContainerRef = useRef<HTMLDivElement | null>(null);
	const rowVirtualizer = useVirtualizer({
		count: rows.length,
		getScrollElement: () => scrollContainerRef.current,
		estimateSize: () => 28,
		overscan: 12,
	});

	return (
		<div
			className={s.timeResultTable}
			role="table"
		>
			<div
				className={s.timeResultHeader}
				role="row"
			>
				<div role="columnheader">{t("common.timestamp")}</div>
				<div role="columnheader">{t("dashboard.value")}</div>
				<div role="columnheader">{t("dashboard.count")}</div>
			</div>
			<div
				className={s.timeResultBody}
				ref={scrollContainerRef}
				role="rowgroup"
			>
				<div
					className={s.timeResultContent}
					style={{ height: rowVirtualizer.getTotalSize() }}
				>
					{rowVirtualizer.getVirtualItems().map((virtualRow) => {
						const row = rows[virtualRow.index];
						if (!row) return null;

						return (
							<div
								key={`${row.time}-${row.value}-${virtualRow.index}`}
								className={s.timeResultRow}
								role="row"
								style={{ transform: `translateY(${virtualRow.start}px)` }}
							>
								<div
									className={s.timeResultCell}
									role="cell"
									title={row.time}
								>
									{row.time}
								</div>
								<div
									className={s.timeResultCell}
									role="cell"
									title={row.value}
								>
									{row.value}
								</div>
								<div
									className={s.timeResultCell}
									role="cell"
									title={String(row.count)}
								>
									{row.count}
								</div>
							</div>
						);
					})}
				</div>
			</div>
		</div>
	);
}

interface DashboardCardProps {
	dashboard: DashboardDefinition;
	globalFilterState: AnalyticsFilterState;
	fieldTypeMap: Record<string, string>;
	fieldKeys: string[];
	container: HTMLDivElement | null;
	palette: ChartPalette;
	sourceNameById: Map<string, string>;
	sourceColorById: Map<string, string>;
	defaultSourceIds: Source.Id[];
	availableSources: Source.Type[];
}

/**
 * Renders a fixed dashboard card and handles its aggregation lifecycle.
 *
 * @param props - Dashboard definition and query dependencies.
 */
const DashboardCard = memo(function DashboardCard({
	dashboard,
	globalFilterState,
	fieldTypeMap,
	fieldKeys,
	container,
	palette,
	sourceNameById,
	sourceColorById,
	defaultSourceIds,
	availableSources,
}: DashboardCardProps) {
	const { Info, app, spawnBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState(false);
	const [error, setError] = useState<string | null>(null);
	const [chartBuckets, setChartBuckets] = useState<ChartBucket[]>([]);
	const [settingsPopoverOpen, setSettingsPopoverOpen] = useState(false);
	const [customFiltersEnabled, setCustomFiltersEnabled] = useState(false);
	const [localFilterState, setLocalFilterState] =
		useState<AnalyticsFilterState>(globalFilterState);
	const operation = Operation.Entity.selected(app);
	const activeFilterState = customFiltersEnabled
		? localFilterState
		: globalFilterState;
	const operationId = operation?.id;
	const activeFilterSignature = useMemo(
		() => serializeFilterState(activeFilterState),
		[activeFilterState],
	);
	const defaultSourceIdsSignature = useMemo(
		() => serializeSourceIds(defaultSourceIds),
		[defaultSourceIds],
	);
	const dashboardAggregationSignature = useMemo(
		() => serializeDashboardAggregations(dashboard),
		[dashboard],
	);
	const relevantFieldTypesSignature = useMemo(
		() => serializeRelevantFieldTypes(activeFilterState, fieldTypeMap),
		[activeFilterState, fieldTypeMap],
	);
	const loadFailedTextRef = useRef(t("dashboard.loadFailed"));

	useEffect(() => {
		loadFailedTextRef.current = t("dashboard.loadFailed");
	}, [t]);

	/**
	 * Opens the card-local filter editor and enables custom filters when applied.
	 *
	 * @returns Nothing.
	 */
	const openCustomFilterBanner = useCallback(() => {
		spawnBanner(
			<DashboardFilterBanner
				title={dashboard.name}
				initialFilterState={
					customFiltersEnabled ? localFilterState : globalFilterState
				}
				onApply={(draftFilterState) => {
					setLocalFilterState(draftFilterState);
					setCustomFiltersEnabled(true);
				}}
				fieldKeys={fieldKeys}
				container={container}
				availableSources={availableSources}
				inheritedSourceIds={defaultSourceIds}
				showSourceSelector
			/>,
			"table",
		);
	}, [
		availableSources,
		container,
		customFiltersEnabled,
		dashboard.name,
		defaultSourceIds,
		fieldKeys,
		globalFilterState,
		localFilterState,
		spawnBanner,
	]);

	/**
	 * Disables local dashboard filters and copies the current global filters as draft state.
	 *
	 * @returns Nothing.
	 */
	const resetCustomFiltersToGlobal = useCallback(() => {
		setLocalFilterState(globalFilterState);
		setCustomFiltersEnabled(false);
	}, [globalFilterState]);

	/**
	 * Closes the settings popover and opens the custom filter banner.
	 *
	 * @returns Nothing.
	 */
	const handleCustomFiltersAction = useCallback(() => {
		setSettingsPopoverOpen(false);
		openCustomFilterBanner();
	}, [openCustomFilterBanner]);

	/**
	 * Closes the settings popover and resets the dashboard to global filters.
	 *
	 * @returns Nothing.
	 */
	const handleResetCustomFiltersAction = useCallback(() => {
		setSettingsPopoverOpen(false);
		resetCustomFiltersToGlobal();
	}, [resetCustomFiltersToGlobal]);

	useEffect(() => {
		if (!operationId) return;
		const query = buildAnalyticsQuery(
			operationId,
			activeFilterState,
			fieldTypeMap,
			defaultSourceIds,
		);
		const body = buildDashboardAggregationBody(dashboard, query);
		if (!body) return;

		let cancelled = false;
		const requestController = new AbortController();
		setLoading(true);
		setError(null);
		scheduleDashboardCardAggregation(
			() =>
				Info.query_aggregation(operationId, body, {
					signal: requestController.signal,
				}),
			requestController.signal,
		)
			.then(async (nextResponse) => {
				if (cancelled) {
					return;
				}

				const nextChartResult =
					await DataWorker.normalizeDashboardAggregation({
						mode: "dashboard_chart",
						response: nextResponse,
						chartType: dashboard.dashboard,
						maxChartBuckets: DASHBOARD_MAX_CHART_BUCKETS,
					});

				if (!cancelled && nextChartResult.mode === "dashboard_chart") {
					startTransition(() => {
						setChartBuckets(nextChartResult.buckets);
					});
				}
			})
			.catch(() => {
				if (!cancelled) {
					setError(loadFailedTextRef.current);
					setChartBuckets([]);
				}
			})
			.finally(() => {
				if (!cancelled) {
					setLoading(false);
				}
			});

		return () => {
			cancelled = true;
			requestController.abort();
		};
	}, [
		Info,
		activeFilterSignature,
		dashboard.dashboard,
		dashboard.id,
		dashboardAggregationSignature,
		defaultSourceIdsSignature,
		operationId,
		relevantFieldTypesSignature,
	]);

	const chartRenderData = useMemo(() => {
		const labels = chartBuckets.map((bucket) =>
			bucket.sourceId
				? (sourceNameById.get(bucket.sourceId) ?? bucket.label)
				: bucket.label,
		);
		const values = chartBuckets.map((bucket) => bucket.value);
		const bucketColors = chartBuckets.map(
			(bucket, index) =>
				(bucket.sourceId ? sourceColorById.get(bucket.sourceId) : undefined) ??
				palette.bars[index % palette.bars.length],
		);
		const hasSourceBuckets = chartBuckets.some((bucket) => bucket.sourceId);

		return { labels, values, bucketColors, hasSourceBuckets };
	}, [chartBuckets, palette.bars, sourceColorById, sourceNameById]);
	const chartOptions = useMemo<ChartOptions<"line" | "bar">>(
		() => ({
			responsive: true,
			maintainAspectRatio: false,
			plugins: {
				legend: { display: false },
				tooltip: { enabled: true },
			},
			scales: {
				x: {
					ticks: { color: palette.text, maxRotation: 0, autoSkip: true },
					grid: { color: palette.grid },
				},
				y: {
					ticks: { color: palette.text },
					grid: { color: palette.grid },
					beginAtZero: true,
				},
			},
		}),
		[palette],
	);

	const lineData = useMemo<ChartData<"line">>(
		() => ({
			labels: chartRenderData.labels,
			datasets: [
				{
					label: dashboard.labels?.tooltip ?? dashboard.name,
					data: chartRenderData.values,
					borderColor: palette.accent,
					backgroundColor: `${palette.accent}33`,
					fill: true,
					tension: 0.25,
					pointRadius: 2,
					pointBackgroundColor: chartRenderData.hasSourceBuckets
						? chartRenderData.bucketColors
						: palette.accent,
					pointBorderColor: chartRenderData.hasSourceBuckets
						? chartRenderData.bucketColors
						: palette.accent,
				},
			],
		}),
		[chartRenderData, dashboard, palette],
	);

	const barData = useMemo<ChartData<"bar">>(
		() => ({
			labels: chartRenderData.labels,
			datasets: [
				{
					label: dashboard.labels?.value ?? dashboard.name,
					data: chartRenderData.values,
					backgroundColor: chartRenderData.bucketColors,
					borderColor: palette.border,
					borderWidth: 1,
				},
			],
		}),
		[chartRenderData, dashboard, palette.border],
	);

	return (
		<section className={s.dashboardCard}>
			<div className={s.cardHeader}>
				<h4
					className={s.cardTitle}
					title={dashboard.name}
				>
					{dashboard.name}
				</h4>
				<Popover.Root
					open={settingsPopoverOpen}
					onOpenChange={setSettingsPopoverOpen}
				>
					<Popover.Trigger asChild>
						<Button
							icon="Settings"
							variant="tertiary"
							title={t("dashboard.settings")}
						/>
					</Popover.Trigger>
					<Popover.Content
						align="end"
						container={container}
						className={s.cardSettingsPopover}
					>
						<Button
							className={s.cardSettingsOption}
							variant="secondary"
							icon="Filter"
							onClick={handleCustomFiltersAction}
						>
							{t("dashboard.useCustomFilters")}
						</Button>
						<Button
							className={s.cardSettingsOption}
							variant="tertiary"
							icon="RotateCcw"
							disabled={!customFiltersEnabled}
							onClick={handleResetCustomFiltersAction}
						>
							{t("dashboard.useGlobalFilters")}
						</Button>
					</Popover.Content>
				</Popover.Root>
			</div>
			<div className={s.chartArea}>
				{loading ? (
					<div className={s.placeholder}>{t("dashboard.loading")}</div>
				) : error ? (
					<div className={s.placeholder}>{error}</div>
				) : chartRenderData.values.length === 0 ? (
					<div className={s.placeholder}>{t("dashboard.noData")}</div>
				) : dashboard.dashboard === "line_chart" ? (
					<Line
						data={lineData}
						options={chartOptions as ChartOptions<"line">}
						datasetIdKey="label"
					/>
				) : (
					<Bar
						data={barData}
						options={chartOptions as ChartOptions<"bar">}
						datasetIdKey="label"
					/>
				)}
			</div>
		</section>
	);
});

/**
 * Detached analytics window for operation-level dashboards and field aggregations.
 *
 * @param props - Optional close callback supplied by the detached window owner.
 */
export function DashboardViewWindow({ onClose }: DashboardViewWindow.Props) {
	const { app, Info, banner, currentDocument } = Application.use();
	const { extensions } = Extension.use();
	const { t } = Locale.use();
	const [container, setContainer] = useState<HTMLDivElement | null>(null);
	const operation = Operation.Entity.selected(app);
	const operationSources = useMemo(
		() =>
			Source.Entity.pins(
				app.target.files.filter(
					(source) => source.selected,
				),
			),
		[app.target.files],
	);
	const [filterState, setFilterState] = useState<AnalyticsFilterState>(() =>
		createInitialFilterState(operationSources),
	);
	const previousOperationIdRef = useRef<Operation.Id | undefined>(operation?.id);
	const [fieldTypeMap, setFieldTypeMap] = useState<Record<string, string>>({});
	const [fieldSourceCounts, setFieldSourceCounts] = useState<
		Record<string, number>
	>({});
	const [dashboards, setDashboards] = useState<DashboardDefinition[]>([]);
	const [selectedField, setSelectedField] = useState<string | null>(null);
	const [aggregationFunction, setAggregationFunction] =
		useState<AggregationFunction>(() => {
			const stored = currentDocument.defaultView?.localStorage.getItem(
				LAST_AGGREGATION_STORAGE_KEY,
			);
			return stored === "top_values_by_time" || stored === "rare_values"
				? stored
				: "top_values";
		});
	const [topValuesSize, setTopValuesSize] = useState(
		String(DEFAULT_TOP_VALUES_SIZE),
	);
	const [timeBucketSize, setTimeBucketSize] = useState(
		String(DEFAULT_TIME_BUCKET_SIZE),
	);
	const [aggregationInterval, setAggregationInterval] = useState(
		DEFAULT_AGGREGATION_INTERVAL,
	);
	const [maxDocCount, setMaxDocCount] = useState(String(DEFAULT_MAX_DOC_COUNT));
	const [aggregationLoading, setAggregationLoading] = useState(false);
	const [fieldAggregationResult, setFieldAggregationResult] =
		useState<FieldAggregationResult | null>(null);
	const [aggregationError, setAggregationError] = useState<string | null>(null);
	const fieldAggregationControllerRef = useRef<AbortController | null>(null);
	const palette = useMemo(
		() => readChartPalette(currentDocument),
		[currentDocument, app.timeline.renderVersion],
	);
	const fieldKeys = useMemo(
		() => Object.keys(fieldTypeMap).sort((a, b) => a.localeCompare(b)),
		[fieldTypeMap],
	);
	const sourceNameById = useMemo(
		() => new Map(app.target.files.map((source) => [source.id, source.name])),
		[app.target.files],
	);
	const sourceColorById = useMemo(() => {
		const contextColorById = new Map(
			app.target.contexts.map((context: Context.Type) => [
				context.id,
				context.color,
			]),
		);

		return new Map(
			app.target.files.map((source) => [
				source.id,
				resolveDashboardSourceColor(source, contextColorById),
			]),
		);
	}, [app.target.contexts, app.target.files]);
	const operationSourceIds = useMemo(
		() => operationSources.map((source) => source.id),
		[operationSources],
	);
	const operationSourceIdsSignature = useMemo(
		() => serializeSourceIds(operationSourceIds),
		[operationSourceIds],
	);
	const fieldMetadataSourceIds = useMemo(
		() =>
			filterState.sourceIds.length > 0
				? filterState.sourceIds
				: operationSourceIds,
		[filterState.sourceIds, operationSourceIds],
	);
	const fieldMetadataSourceIdsSignature = useMemo(
		() => serializeSourceIds(fieldMetadataSourceIds),
		[fieldMetadataSourceIds],
	);
	const fieldMetadataSources = useMemo(
		() =>
			fieldMetadataSourceIds
				.map((id) => app.target.files.find((source) => source.id === id))
				.filter((source): source is Source.Type => !!source),
		[app.target.files, fieldMetadataSourceIdsSignature],
	);
	const fieldMetadataSignature =
		serializeSourceFieldMetadata(fieldMetadataSources);
	const stableFieldMetadataSources = useSemanticValue(
		fieldMetadataSources,
		fieldMetadataSignature,
	);
	const dashboardExtensions = useMemo(
		() => Extension.getBySlot(extensions, Extension.Slot.DashboardView),
		[extensions],
	);

	useEffect(() => {
		const previousOperationId = previousOperationIdRef.current;
		const isOperationChanged = previousOperationId !== operation?.id;
		previousOperationIdRef.current = operation?.id;

		setFilterState((prev) => {
			const sourceIds = prev.sourceIds.filter((id) =>
				operationSources.some((source) => source.id === id),
			);
			const range = isOperationChanged
				? resolveInitialRange(operationSources)
				: prev.range;
			const sourceIdsChanged =
				sourceIds.length !== prev.sourceIds.length ||
				sourceIds.some((id, index) => id !== prev.sourceIds[index]);

			if (!isOperationChanged && !sourceIdsChanged) {
				return prev;
			}

			return {
				...prev,
				range,
				sourceIds,
			};
		});
	}, [operation?.id, operationSourceIdsSignature]);

	useEffect(() => {
		currentDocument.defaultView?.localStorage.setItem(
			LAST_AGGREGATION_STORAGE_KEY,
			aggregationFunction,
		);
	}, [aggregationFunction, currentDocument]);

	useEffect(() => {
		let cancelled = false;
		if (!fixedDashboardDefinitionsPromise) {
			fixedDashboardDefinitionsPromise = Info.shared_object_list<
				Record<string, unknown>
			>({
				type: "shared_object",
				obj_type: "dashboard",
			})
				.then(normalizeDashboardDefinitions)
				.catch((error) => {
					fixedDashboardDefinitionsPromise = null;
					throw error;
				});
		}

		fixedDashboardDefinitionsPromise
			.then((sharedObjects) => {
				if (!cancelled) {
					setDashboards(sharedObjects);
				}
			})
			.catch(() => {
				if (!cancelled) {
					setDashboards([]);
				}
			});

		return () => {
			cancelled = true;
		};
	}, [Info]);

	useEffect(() => {
		let cancelled = false;
		runWithConcurrency(
			stableFieldMetadataSources,
			DASHBOARD_FIELD_METADATA_CONCURRENCY,
			(source) => Info.event_keys(source),
		)
			.then((fieldMaps) => {
				if (cancelled) return;
				const nextFieldTypeMap: Record<string, string> = {};
				const nextCounts: Record<string, number> = {};

				fieldMaps.forEach((fieldMap) => {
					Object.entries(fieldMap).forEach(([field, type]) => {
						nextFieldTypeMap[field] = String(type);
						nextCounts[field] = (nextCounts[field] ?? 0) + 1;
					});
				});

				setFieldTypeMap((prev) =>
					areRecordsEqual(prev, nextFieldTypeMap) ? prev : nextFieldTypeMap,
				);
				setFieldSourceCounts((prev) =>
					areRecordsEqual(prev, nextCounts) ? prev : nextCounts,
				);
			})
			.catch(() => {
				if (cancelled) return;
				setFieldTypeMap({});
				setFieldSourceCounts({});
			});

		return () => {
			cancelled = true;
		};
	}, [Info, stableFieldMetadataSources]);

	const fieldRows = useMemo<FieldInfoRow[]>(
		() =>
			fieldKeys.filter(isFieldAggregationAllowed).map((field) => ({
				field,
				type: fieldTypeMap[field] ?? "-",
				sources: fieldSourceCounts[field] ?? 0,
			})),
		[fieldKeys, fieldSourceCounts, fieldTypeMap],
	);

	/**
	 * Reads current aggregation controls and converts them into query-safe values.
	 *
	 * @returns Parsed aggregation option values with defaults for invalid inputs.
	 */
	const getFieldAggregationOptions = useCallback(
		() => ({
			topValuesSize: parsePositiveIntegerInput(
				topValuesSize,
				DEFAULT_TOP_VALUES_SIZE,
			),
			timeBucketSize: parsePositiveIntegerInput(
				timeBucketSize,
				DEFAULT_TIME_BUCKET_SIZE,
			),
			interval: aggregationInterval,
			maxDocCount: parsePositiveIntegerInput(
				maxDocCount,
				DEFAULT_MAX_DOC_COUNT,
			),
		}),
		[aggregationInterval, maxDocCount, timeBucketSize, topValuesSize],
	);

	/**
	 * Executes an aggregation request for a specific field and aggregation function.
	 *
	 * @param field - ECS/OpenSearch field selected from the available field list.
	 * @param nextAggregationFunction - Aggregation function to execute.
	 * @param options - Parsed function-specific aggregation options.
	 * @returns Nothing.
	 */
	const executeFieldAggregation = useCallback(
		(
			field: string,
			nextAggregationFunction: AggregationFunction,
			options: ReturnType<typeof getFieldAggregationOptions>,
		) => {
			if (!operation || !isFieldAggregationAllowed(field)) return;
			const query = buildAnalyticsQuery(
				operation.id,
				filterState,
				fieldTypeMap,
				operationSourceIds,
			);
			const body = buildFieldAggregationBody(
				query,
				field,
				fieldTypeMap,
				nextAggregationFunction,
				options,
			);

			fieldAggregationControllerRef.current?.abort();
			const requestController = new AbortController();
			const resultMode: FieldAggregationResult["mode"] =
				nextAggregationFunction === "top_values_by_time"
					? "field_time"
					: "field_values";
			fieldAggregationControllerRef.current = requestController;
			setAggregationLoading(true);
			setAggregationError(null);
			setFieldAggregationResult(null);
			Info.query_aggregation(operation.id, body, {
				signal: requestController.signal,
			})
				.then(async (nextResponse) => {
					if (
						requestController.signal.aborted ||
						fieldAggregationControllerRef.current !== requestController
					) {
						return;
					}

					const nextResult = await normalizeFieldAggregationResultInChunks(
						nextResponse,
						resultMode,
						requestController.signal,
					);

					if (
						requestController.signal.aborted ||
						fieldAggregationControllerRef.current !== requestController ||
						nextResult.mode !== resultMode
					) {
						return;
					}

					startTransition(() => {
						setFieldAggregationResult(nextResult);
					});
				})
				.catch(() => {
					if (requestController.signal.aborted) return;
					setFieldAggregationResult(null);
					setAggregationError(t("dashboard.loadFailed"));
				})
				.finally(() => {
					if (fieldAggregationControllerRef.current === requestController) {
						fieldAggregationControllerRef.current = null;
						setAggregationLoading(false);
					}
				});
		},
		[Info, fieldTypeMap, filterState, operation, operationSourceIds, t],
	);

	useEffect(() => {
		return () => fieldAggregationControllerRef.current?.abort();
	}, []);

	/**
	 * Runs the selected field aggregation with the current visible control values.
	 *
	 * @returns Nothing.
	 */
	const runFieldAggregation = useCallback(() => {
		if (!selectedField) return;
		executeFieldAggregation(
			selectedField,
			aggregationFunction,
			getFieldAggregationOptions(),
		);
	}, [
		aggregationFunction,
		executeFieldAggregation,
		getFieldAggregationOptions,
		selectedField,
	]);

	/**
	 * Changes the aggregation function and immediately reloads the selected field.
	 *
	 * @param value - Select value emitted by the aggregation function control.
	 * @returns Nothing.
	 */
	const handleAggregationFunctionChange = useCallback(
		(value: string) => {
			const nextAggregationFunction = value as AggregationFunction;
			setAggregationFunction(nextAggregationFunction);

			if (selectedField) {
				executeFieldAggregation(
					selectedField,
					nextAggregationFunction,
					getFieldAggregationOptions(),
				);
			}
		},
		[executeFieldAggregation, getFieldAggregationOptions, selectedField],
	);

	/**
	 * Closes the field aggregation overlay and keeps the last response in memory.
	 *
	 * @returns Nothing.
	 */
	const closeAggregationPanel = useCallback(() => {
		setSelectedField(null);
	}, []);

	const valueRows =
		fieldAggregationResult?.mode === "field_values"
			? fieldAggregationResult.rows
			: EMPTY_VALUE_ROWS;
	const timeRows =
		fieldAggregationResult?.mode === "field_time"
			? fieldAggregationResult.rows
			: EMPTY_TIME_ROWS;
	const chartValueRows = useMemo(
		() =>
			valueRows.length > DASHBOARD_MAX_CHART_BUCKETS
				? valueRows.slice(0, DASHBOARD_MAX_CHART_BUCKETS)
				: valueRows,
		[valueRows],
	);
	const aggregationChartData = useMemo<ChartData<"bar">>(
		() => ({
			labels: chartValueRows.map((row) => row.value),
			datasets: [
				{
					label: selectedField ?? "",
					data: chartValueRows.map((row) => row.count),
					backgroundColor: chartValueRows.map(
						(_, index) => palette.bars[index % palette.bars.length],
					),
					borderColor: palette.border,
					borderWidth: 1,
				},
			],
		}),
		[chartValueRows, palette, selectedField],
	);
	const aggregationChartOptions = useMemo<ChartOptions<"bar">>(
		() => ({
			responsive: true,
			maintainAspectRatio: false,
			plugins: {
				legend: { display: false },
			},
			scales: {
				x: {
					ticks: { color: palette.text, maxRotation: 0, autoSkip: true },
					grid: { color: palette.grid },
				},
				y: {
					ticks: { color: palette.text },
					grid: { color: palette.grid },
					beginAtZero: true,
				},
			},
		}),
		[palette],
	);
	const isAggregationPanelVisible = selectedField !== null;

	/**
	 * Selects a field and opens the aggregation panel in place of the dashboard section.
	 *
	 * @param field - ECS/OpenSearch field selected from the available field list.
	 * @returns Nothing.
	 */
	const openAggregationPanel = useCallback(
		(field: string) => {
			setSelectedField(field);
			executeFieldAggregation(
				field,
				aggregationFunction,
				getFieldAggregationOptions(),
			);
		},
		[aggregationFunction, executeFieldAggregation, getFieldAggregationOptions],
	);

	return (
		<div
			className={s.main}
			ref={setContainer}
		>
			<div className={s.body}>
				<aside className={`${s.column} ${s.explorerColumn}`}>
					<header className={s.columnHeader}>
						<h2>{t("dashboard.dataExplorer")}</h2>
					</header>
					<div className={`${s.columnBody} ${s.explorerBody}`}>
						<div className={s.panel}>
							<h4>{t("common.filters")}</h4>
							<DashboardFilterControls
								filterState={filterState}
								setFilterState={setFilterState}
								fieldKeys={fieldKeys}
								container={container}
								availableSources={operationSources}
								inheritedSourceIds={operationSourceIds}
							/>
						</div>
						<div className={s.panel}>
							<h4>{t("dashboard.availableFields")}</h4>
							{fieldRows.length > 0 ? (
								<DashboardFieldList
									rows={fieldRows}
									selectedField={selectedField}
									onSelectField={openAggregationPanel}
								/>
							) : (
								<div className={s.placeholder}>{t("dashboard.noFields")}</div>
							)}
						</div>
					</div>
				</aside>
				<main className={`${s.column} ${s.dashboardColumn}`}>
					<header className={s.columnHeader}>
						<h2>{t("dashboard.analyticsDashboard")}</h2>
					</header>
					<div className={`${s.columnBody} ${s.dashboardBody}`}>
						<section
							className={s.section}
							hidden={isAggregationPanelVisible}
						>
							<h3>{t("dashboard.fixedDashboards")}</h3>
							<div className={s.cardGrid}>
								{dashboards.length === 0 ? (
									<div className={s.placeholder}>{t("dashboard.noData")}</div>
								) : (
									dashboards.map((dashboard) => (
										<DashboardCard
											key={dashboard.id}
											dashboard={dashboard}
											globalFilterState={filterState}
											fieldTypeMap={fieldTypeMap}
											fieldKeys={fieldKeys}
											container={container}
											palette={palette}
											sourceNameById={sourceNameById}
											sourceColorById={sourceColorById}
											defaultSourceIds={operationSourceIds}
											availableSources={operationSources}
										/>
									))
								)}
								{dashboardExtensions.map((extension) => (
									<div
										key={extension.filename}
										className={s.dashboardCard}
									>
										<Extension.Component
											name={extension.filename}
											props={{
												filterState,
												fieldTypeMap,
												fieldKeys,
											}}
										/>
									</div>
								))}
							</div>
						</section>
						{selectedField ? (
							<section className={s.aggregationPanel}>
								<div className={s.aggregationTitleRow}>
									<h3
										className={s.aggregationTitle}
										title={selectedField}
									>
										{selectedField}
									</h3>
									<Button
										icon="X"
										variant="tertiary"
										title={t("common.closeDialog")}
										onClick={closeAggregationPanel}
									/>
								</div>
								<div className={s.aggregationControls}>
									<Select.Root
										value={aggregationFunction}
										onValueChange={handleAggregationFunctionChange}
									>
										<Select.Trigger>
											<Icon name="ChartGantt" />
											<Select.Value
												placeholder={t("dashboard.aggregationFunction")}
											/>
										</Select.Trigger>
										<Select.Content container={container}>
											<Select.Item value="top_values">
												{t("dashboard.topValues")}
											</Select.Item>
											<Select.Item value="top_values_by_time">
												{t("dashboard.topValuesByTime")}
											</Select.Item>
											<Select.Item value="rare_values">
												{t("dashboard.rareValues")}
											</Select.Item>
										</Select.Content>
									</Select.Root>
									{aggregationFunction === "rare_values" ? (
										<Input
											label={t("dashboard.maxDocCount")}
											type="number"
											min={1}
											value={maxDocCount}
											onChange={(event) => setMaxDocCount(event.target.value)}
										/>
									) : aggregationFunction === "top_values_by_time" ? (
										<Input
											label={t("dashboard.bucketSize")}
											type="number"
											min={1}
											value={timeBucketSize}
											onChange={(event) =>
												setTimeBucketSize(event.target.value)
											}
										/>
									) : (
										<Input
											label={t("dashboard.size")}
											type="number"
											min={1}
											value={topValuesSize}
											onChange={(event) => setTopValuesSize(event.target.value)}
										/>
									)}
									{aggregationFunction === "top_values_by_time" ? (
										<div className={s.intervalControl}>
											<div className={s.intervalLabelRow}>
												<span className={s.intervalLabel}>
													{t("dashboard.interval")}
												</span>
												<TooltipProvider>
													<Tooltip>
														<TooltipTrigger asChild>
															<button
																type="button"
																className={s.intervalHelpButton}
																aria-label={t("dashboard.intervalHelpLabel")}
															>
																<Icon name="CircleHelp" />
															</button>
														</TooltipTrigger>
														<TooltipContent container={currentDocument.body}>
															{t("dashboard.intervalTooltip")}
														</TooltipContent>
													</Tooltip>
												</TooltipProvider>
											</div>
											<Input
												aria-label={t("dashboard.interval")}
												value={aggregationInterval}
												onChange={(event) =>
													setAggregationInterval(event.target.value)
												}
											/>
										</div>
									) : null}
									<Button
										icon="Play"
										variant="secondary"
										loading={aggregationLoading}
										onClick={runFieldAggregation}
									>
										{t("dashboard.runAggregation")}
									</Button>
								</div>
								{aggregationLoading ? (
									<div className={s.placeholder}>{t("dashboard.loading")}</div>
								) : aggregationError ? (
									<div className={s.placeholder}>{aggregationError}</div>
								) : aggregationFunction === "top_values_by_time" ? (
									<div className={`${s.tablePanel} ${s.timeTablePanel}`}>
										<DashboardTimeRowsTable rows={timeRows} />
									</div>
								) : (
									<div className={s.aggregationResults}>
										<div className={s.smallChart}>
											{valueRows.length > 0 ? (
												<Bar
													data={aggregationChartData}
													options={aggregationChartOptions}
													datasetIdKey="label"
												/>
											) : (
												<div className={s.placeholder}>
													{t("dashboard.noData")}
												</div>
											)}
										</div>
										<div className={s.tablePanel}>
											<Table
												values={valueRows}
												includeIndex={false}
												columns={[
													{ key: "value", label: t("dashboard.value") },
													{ key: "count", label: t("dashboard.count") },
													{ key: "percent", label: t("dashboard.percent") },
												]}
											/>
										</div>
									</div>
								)}
							</section>
						) : null}
					</div>
				</main>
			</div>
			{banner?.target === "table" ? banner.node : null}
		</div>
	);
}
