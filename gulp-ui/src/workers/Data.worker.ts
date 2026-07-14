/// <reference lib="webworker" />

import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import type { GulpDataset } from "@/class/Info";
import type { HashFunctionName } from "@/ui/utils";
import type {
	DashboardAggregationChartBucket,
	DashboardAggregationTimeRow,
	DashboardAggregationValueRow,
	DataWorkerRequest,
	NormalizeQueryDocsPayload,
	NormalizeDashboardAggregationPayload,
	NormalizeDashboardAggregationResult,
	QueryRawDocument,
	TimestampedWorkerItem,
	TimestampInput,
} from "./DataWorker.types";

const HASH_MOD = 8000;
const DEFAULT_FIELD = "gulp.event_code";
const DEFAULT_HASH_FUNCTION: HashFunctionName = "fnv1a";

const hashAlgorithms: Record<HashFunctionName, (value: string) => number> = {
	fnv1a: (value: string): number => {
		let hash = 0x811c9dc5;
		for (let i = 0; i < value.length; i++) {
			hash ^= value.charCodeAt(i);
			hash = Math.imul(hash, 0x01000193);
		}
		return hash >>> 0;
	},
	djb2: (value: string): number => {
		let hash = 5381;
		for (let i = 0; i < value.length; i++) {
			hash = Math.imul(hash, 33) + value.charCodeAt(i);
		}
		return hash >>> 0;
	},
	sdbm: (value: string): number => {
		let hash = 0;
		for (let i = 0; i < value.length; i++) {
			hash = value.charCodeAt(i) + Math.imul(hash, 65599);
		}
		return hash >>> 0;
	},
};

/**
 * Converts timestamp inputs to nanoseconds with the same rules used on the main thread.
 * @param value Raw timestamp value from a document.
 * @returns Nanosecond timestamp, or zero when conversion fails.
 */
function toNanos(value: TimestampInput | undefined): bigint {
	try {
		if (typeof value === "bigint") return value;

		if (value instanceof Date) return BigInt(value.getTime()) * 1_000_000n;

		if (typeof value === "object" && value !== null && "source" in value) {
			return BigInt(value.source);
		}

		if (typeof value === "number") {
			const str = String(Math.floor(value));
			if (str.length === 19) return BigInt(str);
			if (str.length === 16) return BigInt(str) * 1_000n;
			if (str.length === 13) return BigInt(str) * 1_000_000n;
			if (str.length <= 10) return BigInt(str) * 1_000_000_000n;
			return BigInt(value);
		}

		if (typeof value === "string") {
			if (/^\d+$/.test(value)) return toNanos(Number(value));
			const parsed = Date.parse(value);
			if (!Number.isNaN(parsed)) return BigInt(parsed) * 1_000_000n;
			return 0n;
		}

		return 0n;
	} catch {
		return 0n;
	}
}

/**
 * Converts timestamp inputs to rounded millisecond timestamps.
 * @param timestamp Raw timestamp value from a document.
 * @returns Millisecond timestamp rounded like Internal.Transformator.toTimestamp.
 */
function toTimestamp(timestamp: TimestampInput | undefined): number {
	return new Date(Math.round(Number(toNanos(timestamp)) / 1_000_000)).valueOf();
}

/**
 * Hashes a string into the render-engine numeric range.
 * @param value String value to hash.
 * @param hashFunction Hash function selected in source settings.
 * @returns Stable numeric hash in the render range.
 */
function stringToNumber(
	value: string,
	hashFunction: HashFunctionName = DEFAULT_HASH_FUNCTION,
): number {
	const algorithm = hashAlgorithms[hashFunction] ?? hashAlgorithms.fnv1a;
	return algorithm(value) % HASH_MOD;
}

/**
 * Converts arbitrary field values into render-engine numbers.
 * @param value Raw field value extracted from the document.
 * @param hashFunction Hash function selected in source settings.
 * @returns Numeric representation used by color and height renderers.
 */
function anyToNumber(
	value: unknown,
	hashFunction: HashFunctionName = DEFAULT_HASH_FUNCTION,
): number {
	switch (typeof value) {
		case "string":
			return stringToNumber(value, hashFunction);
		case "number":
			return value;
		case "bigint":
			return toTimestamp(value);
		default: {
			const result = Number(value);
			if (Number.isNaN(result)) {
				return 0;
			}
			return result;
		}
	}
}

/**
 * Safely reads nested document fields with support for dotted raw keys.
 * @param obj Document-like object to inspect.
 * @param path Dot-separated or raw field path.
 * @returns Field value, or undefined when absent.
 */
function getFieldValue(obj: QueryRawDocument, path: string): unknown {
	if (!path || !obj) return undefined;
	if (obj[path] !== undefined) return obj[path];

	const parts = path.split(".");
	for (let i = parts.length - 1; i > 0; i--) {
		const head = parts.slice(0, i).join(".");
		const tail = parts.slice(i).join(".");
		const headValue = obj[head];
		if (headValue !== undefined && typeof headValue === "object" && headValue !== null) {
			return getFieldValue(headValue as QueryRawDocument, tail);
		}
	}

	let current: unknown = obj;
	for (const part of parts) {
		if (current === null || current === undefined || typeof current !== "object") {
			return undefined;
		}
		current = (current as Record<string, unknown>)[part];
	}
	return current;
}

/**
 * Returns the timestamp field used by worker-side sorting helpers.
 * @param item Item with either a note timestamp or document gulp timestamp.
 * @returns Sortable numeric timestamp.
 */
function getSortableTimestamp(item: TimestampedWorkerItem): number {
	return item.timestamp ?? item.gulp_timestamp ?? 0;
}

/**
 * Normalizes raw query documents, groups them by source, and sorts each group newest first.
 * @param payload Worker payload containing raw docs and source render settings.
 * @returns Prepared batches ready for main-thread insertion.
 */
function normalizeQueryDocs(
	payload: NormalizeQueryDocsPayload,
): Array<{ sourceId: Source.Id; docs: Doc.Type[] }> {
	const docsBySource = new Map<Source.Id, Doc.Type[]>();

	for (const raw of payload.docs) {
		const sourceId =
			(raw["gulp.source_id"] as Source.Id | undefined) ?? payload.fallbackSourceId;
		if (!sourceId) continue;

		const settings = payload.sourceSettingsById[sourceId] ?? {
			field: DEFAULT_FIELD,
			hash_function: DEFAULT_HASH_FUNCTION,
		};

		const normalized = {
			_id: raw._id,
			gulp_timestamp: toTimestamp(raw["gulp.timestamp"]),
			"gulp.source_id": sourceId,
			"gulp.event_code": raw["gulp.event_code"],
			number_hash: anyToNumber(
				getFieldValue(raw, settings.field),
				settings.hash_function,
			),
		} as Doc.Type;

		const group = docsBySource.get(sourceId);
		if (group) {
			group.push(normalized);
		} else {
			docsBySource.set(sourceId, [normalized]);
		}
	}

	return Array.from(docsBySource.entries()).map(([sourceId, docs]) => ({
		sourceId,
		docs: docs.sort((a, b) => b.gulp_timestamp - a.gulp_timestamp),
	}));
}

/**
 * Finds the first aggregation bucket list in a dashboard aggregation response.
 * @param response Aggregation response returned by the backend.
 * @returns First bucket list found in the response.
 */
function getFirstDashboardBucketAggregation(
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
 * Converts a backend bucket into a compact chart bucket.
 * @param bucket Aggregation bucket returned by OpenSearch.
 * @returns Chart-ready bucket with a display label and numeric value.
 */
function toDashboardChartBucket(
	bucket: GulpDataset.QueryAggregation.Bucket,
): DashboardAggregationChartBucket {
	return {
		label: String(bucket.key_as_string ?? bucket.key),
		value: bucket.doc_count,
		sourceId: typeof bucket.key === "string" ? bucket.key : undefined,
	};
}

/**
 * Downsamples line chart buckets evenly while preserving the first and last bucket.
 * @param buckets Full chart bucket list.
 * @param maxBuckets Maximum number of buckets to keep.
 * @returns Bounded bucket list for Chart.js rendering.
 */
function downsampleLineChartBuckets(
	buckets: DashboardAggregationChartBucket[],
	maxBuckets: number,
): DashboardAggregationChartBucket[] {
	if (buckets.length <= maxBuckets) {
		return buckets;
	}

	if (maxBuckets <= 0) {
		return [];
	}

	if (maxBuckets === 1) {
		return [buckets[0]];
	}

	const lastIndex = buckets.length - 1;
	const step = lastIndex / (maxBuckets - 1);

	return Array.from({ length: maxBuckets }, (_, index) => {
		const bucketIndex =
			index === maxBuckets - 1 ? lastIndex : Math.round(index * step);
		return buckets[bucketIndex];
	});
}

/**
 * Caps chart buckets according to the dashboard chart type.
 * @param buckets Full chart bucket list.
 * @param chartType Dashboard chart renderer type.
 * @param maxBuckets Maximum number of buckets to keep.
 * @returns Bounded bucket list for Chart.js rendering.
 */
function limitDashboardChartBuckets(
	buckets: DashboardAggregationChartBucket[],
	chartType: Extract<
		NormalizeDashboardAggregationPayload,
		{ mode: "dashboard_chart" }
	>["chartType"],
	maxBuckets: number,
): DashboardAggregationChartBucket[] {
	if (buckets.length <= maxBuckets) {
		return buckets;
	}

	if (chartType === "line_chart") {
		return downsampleLineChartBuckets(buckets, maxBuckets);
	}

	return buckets.slice(0, Math.max(0, maxBuckets));
}

/**
 * Normalizes fixed dashboard chart buckets away from the main React render path.
 * @param payload Dashboard chart normalization payload.
 * @returns Capped chart bucket result with metadata about the original response size.
 */
function normalizeDashboardChartAggregation(
	payload: Extract<NormalizeDashboardAggregationPayload, { mode: "dashboard_chart" }>,
): Extract<NormalizeDashboardAggregationResult, { mode: "dashboard_chart" }> {
	const buckets = getFirstDashboardBucketAggregation(payload.response).map(
		toDashboardChartBucket,
	);
	const limitedBuckets = limitDashboardChartBuckets(
		buckets,
		payload.chartType,
		payload.maxChartBuckets,
	);

	return {
		mode: "dashboard_chart",
		buckets: limitedBuckets,
		isChartLimited: limitedBuckets.length < buckets.length,
		originalBucketCount: buckets.length,
	};
}

/**
 * Converts top/rare aggregation buckets into table rows in the worker.
 * @param payload Field-value aggregation normalization payload.
 * @returns Table rows with value, count, and percentage columns.
 */
function normalizeDashboardValueAggregation(
	payload: Extract<NormalizeDashboardAggregationPayload, { mode: "field_values" }>,
): Extract<NormalizeDashboardAggregationResult, { mode: "field_values" }> {
	const buckets = getFirstDashboardBucketAggregation(payload.response);
	const total = Math.max(payload.response.total_hits, 1);

	return {
		mode: "field_values",
		rows: buckets.map<DashboardAggregationValueRow>((bucket) => ({
			value: String(bucket.key_as_string ?? bucket.key),
			count: bucket.doc_count,
			percent: Number(((bucket.doc_count / total) * 100).toFixed(2)),
		})),
	};
}

/**
 * Checks whether a dynamic bucket value contains a nested bucket list.
 * @param value Dynamic aggregation bucket property.
 * @returns True when the value is a nested aggregation with buckets.
 */
function isNestedBucketAggregation(
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
 * Converts time-bucketed top-value aggregations into flat table rows in the worker.
 * @param payload Time aggregation normalization payload.
 * @returns Table rows with time, value, and count columns.
 */
function normalizeDashboardTimeAggregation(
	payload: Extract<NormalizeDashboardAggregationPayload, { mode: "field_time" }>,
): Extract<NormalizeDashboardAggregationResult, { mode: "field_time" }> {
	const buckets = getFirstDashboardBucketAggregation(payload.response);
	const rows: DashboardAggregationTimeRow[] = [];

	buckets.forEach((bucket) => {
		const time = String(bucket.key_as_string ?? bucket.key);
		const nested = Object.values(bucket).find(isNestedBucketAggregation);

		if (!nested) {
			rows.push({
				time,
				value: "-",
				count: bucket.doc_count,
			});
			return;
		}

		nested.buckets.forEach((nestedBucket) => {
			rows.push({
				time,
				value: String(nestedBucket.key_as_string ?? nestedBucket.key),
				count: nestedBucket.doc_count,
			});
		});
	});

	return { mode: "field_time", rows };
}

/**
 * Normalizes dashboard aggregation responses into render-ready data.
 * @param payload Dashboard aggregation normalization payload.
 * @returns Render-ready chart buckets or table rows.
 */
function normalizeDashboardAggregation(
	payload: NormalizeDashboardAggregationPayload,
): NormalizeDashboardAggregationResult {
	switch (payload.mode) {
		case "dashboard_chart":
			return normalizeDashboardChartAggregation(payload);
		case "field_values":
			return normalizeDashboardValueAggregation(payload);
		case "field_time":
			return normalizeDashboardTimeAggregation(payload);
	}
}

self.onmessage = (event: MessageEvent<DataWorkerRequest>) => {
	const { type, payload, id } = event.data;

	try {
		switch (type) {
			case "SORT_EVENTS": {
				const sorted = payload.sort(
					(a, b) => getSortableTimestamp(b) - getSortableTimestamp(a),
				);
				self.postMessage({ id, result: sorted });
				break;
			}

			case "BINARY_SEARCH_DESC": {
				const { items, timestamp, findFirst } = payload;
				let left = 0;
				let right = items.length - 1;
				let result = -1;

				while (left <= right) {
					const mid = Math.floor((left + right) / 2);
					const noteTime = getSortableTimestamp(items[mid]);

					if (findFirst) {
						if (noteTime <= timestamp) {
							result = mid;
							right = mid - 1;
						} else {
							left = mid + 1;
						}
					} else if (noteTime >= timestamp) {
						result = mid;
						left = mid + 1;
					} else {
						right = mid - 1;
					}
				}

				self.postMessage({ id, result });
				break;
			}

			case "NORMALIZE_QUERY_DOCS": {
				self.postMessage({
					id,
					result: { batches: normalizeQueryDocs(payload) },
				});
				break;
			}

			case "NORMALIZE_DASHBOARD_AGGREGATION": {
				self.postMessage({
					id,
					result: normalizeDashboardAggregation(payload),
				});
				break;
			}

			default:
				self.postMessage({ id, error: "Unknown worker action" });
		}
	} catch (error: unknown) {
		const message = error instanceof Error ? error.message : String(error);
		self.postMessage({ id, error: message });
	}
};
