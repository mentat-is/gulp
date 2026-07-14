import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import type { GulpDataset } from "@/class/Info";
import type { HashFunctionName } from "@/ui/utils";

export type TimestampInput =
	| string
	| number
	| Date
	| bigint
	| { source: string; parsedValue: number };

export type QueryRawDocument = Record<string, unknown> &
	Partial<Pick<Doc.Type, "_id" | "gulp.source_id" | "gulp.event_code">> & {
		"gulp.timestamp"?: TimestampInput;
	};

export interface NormalizeQuerySourceSettings {
	field: string;
	hash_function: HashFunctionName;
}

export interface NormalizeQueryDocsPayload {
	docs: QueryRawDocument[];
	sourceSettingsById: Record<string, NormalizeQuerySourceSettings>;
	fallbackSourceId?: Source.Id;
}

export interface NormalizeQueryDocsResult {
	batches: Array<{
		sourceId: Source.Id;
		docs: Doc.Type[];
	}>;
}

export interface TimestampedWorkerItem {
	timestamp?: number;
	gulp_timestamp?: number;
}

export interface BinarySearchDescPayload {
	items: TimestampedWorkerItem[];
	timestamp: number;
	findFirst: boolean;
}

export type DashboardAggregationMode =
	| "dashboard_chart"
	| "field_values"
	| "field_time";

export type DashboardAggregationChartType = "line_chart" | "vertical_chart";

export interface DashboardAggregationChartBucket {
	label: string;
	value: number;
	sourceId?: string;
}

export interface DashboardAggregationValueRow
	extends Record<string, string | number> {
	value: string;
	count: number;
	percent: number;
}

export interface DashboardAggregationTimeRow
	extends Record<string, string | number> {
	time: string;
	value: string;
	count: number;
}

export type NormalizeDashboardAggregationPayload =
	| {
			mode: "dashboard_chart";
			response: GulpDataset.QueryAggregation.Response;
			chartType: DashboardAggregationChartType;
			maxChartBuckets: number;
	  }
	| {
			mode: "field_values";
			response: GulpDataset.QueryAggregation.Response;
	  }
	| {
			mode: "field_time";
			response: GulpDataset.QueryAggregation.Response;
	  };

export type NormalizeDashboardAggregationResult =
	| {
			mode: "dashboard_chart";
			buckets: DashboardAggregationChartBucket[];
			isChartLimited: boolean;
			originalBucketCount: number;
	  }
	| {
			mode: "field_values";
			rows: DashboardAggregationValueRow[];
	  }
	| {
			mode: "field_time";
			rows: DashboardAggregationTimeRow[];
	  };

export interface DataWorkerPayloadMap {
	SORT_EVENTS: TimestampedWorkerItem[];
	BINARY_SEARCH_DESC: BinarySearchDescPayload;
	NORMALIZE_QUERY_DOCS: NormalizeQueryDocsPayload;
	NORMALIZE_DASHBOARD_AGGREGATION: NormalizeDashboardAggregationPayload;
}

export interface DataWorkerResultMap {
	SORT_EVENTS: TimestampedWorkerItem[];
	BINARY_SEARCH_DESC: number;
	NORMALIZE_QUERY_DOCS: NormalizeQueryDocsResult;
	NORMALIZE_DASHBOARD_AGGREGATION: NormalizeDashboardAggregationResult;
}

export type DataWorkerAction = keyof DataWorkerPayloadMap;

export type DataWorkerRequest<TAction extends DataWorkerAction = DataWorkerAction> = {
	[Action in DataWorkerAction]: {
		type: Action;
		payload: DataWorkerPayloadMap[Action];
		id: string;
	};
}[TAction];

export interface DataWorkerResponse {
	id: string;
	result?: unknown;
	error?: string;
}
