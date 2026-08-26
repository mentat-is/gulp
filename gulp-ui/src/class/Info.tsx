import { debounce, Dictionary } from "lodash";
import { scrollStore } from "@/store/scroll.store";
import { Default } from "@/dto/Dataset";
import {
	generateUUID,
	NodeFile,
	Refractor,
	stringToHexColor,
} from "@/ui/utils";
import { Logger } from "@/dto/Logger.class";
import type {
	ResponseBase,
	ResponseErrorBody,
	ResponseHandler,
	SetState,
} from "./API";
import { Icon } from "@/ui/Icon";
import { toast } from "sonner";
import { Pointers } from "@/components/Pointers";
import { CustomParameters } from "@/components/CustomParameters";
import { Highlights } from "@/overlays/Highlights";
import { RenderEngine } from "./RenderEngine";
import { SmartSocket } from "./SmartSocket";
import { DataStore } from "@/store/DataStore";
import { Spinner } from "@/ui/Spinner";
import { Badge } from "@/ui/Badge";
import { Source } from "@/entities/Source";
import { Parser } from "@/entities/addon/Parser";
import { App } from "@/entities/App";
import { Doc } from "@/entities/Doc";
import { Glyph } from "@/entities/Glyph";
import { Operation } from "@/entities/Operation";
import { Context } from "@/entities/Context";
import { User } from "@/entities/User";
import { Request } from "@/entities/Request";
import { FileEntity } from "@/banners/Upload.banner";
import { Note } from "@/entities/Note";
import { Link } from "@/entities/Link";
import { CacheKey } from "./Engine.dto";
import { Filter } from "@/entities/Filter";
import { Query } from "@/entities/Query";
import { Highlight } from "@/entities/Highlight";
import { Mapping } from "@/entities/Mapping";
import { Internal } from "@/entities/addon/Internal";
import { ingestWorkerManager } from "@/workers/IngestWorker.manager";
import { translate } from "@/locales/core";
import { WindowBridge } from "@/lib/WindowBridge";
import { GulpIndexedDB } from "@/class/IndexedDB";
import { sourceFieldResolver } from "@/class/SourceFieldCache";
import { requestStore } from "@/store/request.store";
import { TimelineScaleStore } from "@/store/TimelineScale.store";
import { DataWorker } from "@/workers/DataWorker.class";
import type {
	NormalizeQuerySourceSettings,
	QueryRawDocument,
} from "@/workers/DataWorker.types";
import { ViewportQueryGate } from "@/class/ViewportQueryGate";
import {
	hasRawQueryViewportWork,
	isRawQuerySourceVisible,
	selectNextRawQueryQueueItem,
	type RawQueryQueuePriority,
	type RawQueryViewportState,
} from "@/class/RawQueryScheduler";
import {
	collectIngestionResyncLoadings,
	hasActiveIngestionResyncLoading,
	isIngestionRequest,
	type IngestionResyncLoadings,
} from "@/class/IngestionResync";
import {
	cloneWorkspaceSessionValue,
	WorkspaceSessionNameExistsError,
	WorkspaceSessionRepository,
	type StoredWorkspaceSession,
	type WorkspaceSessionSnapshot,
} from "@/class/WorkspaceSessionRepository";
import type {
	DocumentStreamDescriptor,
	DocumentStreamStateChange,
	DocumentStreamSourceSettings,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryStreamFailureReason,
} from "@/event-stream";
import {
	getEventStoreLodDecision,
	isEventStoreLodEnabled,
	reloadWithLegacyEventPipeline,
	type EventSourceSummary,
} from "@/event-store";

export namespace GulpDataset {
	export namespace GetAvailableLoginApi {
		export type Response = Method[];

		export interface Method {
			name: string;
			login: Struct;
			logout: Struct;
		}

		export interface Struct {
			method: string;
			url: string;
			params: Param[];
		}

		export interface Param {
			name: string;
			type: "str";
			location: "body";
			description: string;
			required?: boolean;
			default_value?: null;
		}
	}
	export namespace QueryGulp {
		export interface Options {
			preview?: boolean;
			id?: Source.Id | Source.Id[];
			refetchKeys?: Array<keyof Doc.Type>;
			addToHistory?: boolean;
			create_notes?: boolean;
			notes_color?: string;
			notes_tags?: string[];
			notes_glyph_id?: Glyph.Id;
			name?: string;
			offset?: number;
			limit?: number;
			sort?: Record<string, "desc" | "asc">;
			signal?: AbortSignal;
		}
	}
	export namespace QueryOperations {
		interface Operation {
			name: string;
			id: string;
			index?: string;
			glyph_id?: Glyph.Id;
			contexts: Context[];
			description?: string;
		}

		export interface Context {
			name: string;
			id: string;
			color?: string;
			glyph_id?: Glyph.Id;
			doc_count: number;
			plugins: Plugin[];
		}

		interface Plugin {
			name: string;
			sources: Source[];
		}

		export interface Source {
			name: string;
			id: Source.Id;
			glyph_id?: Glyph.Id;
			doc_count: number;
			time_updated?: number | string | null;
			"max_event.code": number;
			"min_event.code": number;
			"min_gulp.timestamp": bigint | { source: string; parsedValue: number };
			"max_gulp.timestamp": bigint | { source: string; parsedValue: number };
		}

		export type Summary = Operation[];
	}

	export namespace QueryAggregation {
		export interface Bucket {
			key: string | number;
			key_as_string?: string;
			doc_count: number;
			[name: string]: unknown;
		}

		export interface Aggregation {
			buckets?: Bucket[];
			doc_count_error_upper_bound?: number;
			sum_other_doc_count?: number;
			[name: string]: unknown;
		}

		export interface Body {
			query: Record<string, unknown>;
			aggs: Record<string, unknown>;
		}

		export interface Response {
			total_hits: number;
			aggregations: Record<string, Aggregation>;
		}
	}

	export namespace SharedObject {
		export type JsonPrimitive = string | number | boolean | null;
		export type JsonValue =
			| JsonPrimitive
			| JsonValue[]
			| { [key: string]: JsonValue };
		export type JsonObject = { [key: string]: JsonValue };

		export interface Type<TObject = Record<string, unknown>> {
			id?: string;
			obj_id?: string;
			type: string;
			operation_id?: string;
			tags?: string[];
			color?: string;
			owner_user_id?: string;
			granted_user_ids?: string[];
			granted_user_group_ids?: string[];
			time_created?: number;
			time_updated?: number;
			glyph_id?: Glyph.Id;
			name: string;
			description?: string;
			obj: TObject;
			obj_type: string;
		}

		export interface ListParams {
			type: "shared_object";
			obj_type: string;
		}

		export type ListResponse<TObject = Record<string, unknown>> =
			| Type<TObject>[]
			| { data: Type<TObject>[] };

		export interface UpdatePayload {
			name?: string;
			glyph_id?: Glyph.Id | string | null;
			description?: string;
			tags?: string[];
			obj?: JsonObject;
		}
	}

	export namespace PluginList {
		export type Type = "ingestion" | "enrichment" | "external" | "extension";

		export namespace SigmaSupport {
			export type Type = "backends" | "pipelines" | "output_formats";

			export interface Interface {
				name: string;
				description: string;
			}

			export type Summary = Record<SigmaSupport.Type, SigmaSupport.Interface>[];
		}

		export type DependsOn = "eml";

		export interface Interface {
			display_name: string;
			type: Type[];
			desc: string;
			path: string;
			data: {};
			filename: string;
			sigma_support: SigmaSupport.Summary;
			custom_parameters: CustomParameters.Interface[];
			depends_on: DependsOn[];
			tags: (string | "extension")[];
			version: string;
		}
	}
	export namespace QueryHistoryGet {
		export interface Interface {
			q: {
				query: {
					bool: Record<
						Filter.BoolOperator,
						Record<Filter.Condition, any>[]
					>;
				};
			};
			external: boolean;
			query_options: {
				loop: boolean;
				name: string;
				sort: {
					"@timestamp": "desc" | "asc";
				};
				limit: number;
				preview_mode: boolean;
				note_parameters: {
					note_tags: string[];
				};
				ensure_default_fields: boolean;
			};
			timestamp_msec: number;
		}
		export type Response = Interface[];
	}
	export interface SigmaFile {
		name: string;
		content: string;
	}

	export namespace OperationGetById {
		export interface Response {
			id: string;
			name: string;
			tags: string[];
			time_created: number;
			user_id: string;
			doc_count: number;
			granted_user_ids: string[];
			granted_user_group_ids: string[];
			description?: string;
		}
	}

	export namespace MappingFileUpload {
		export interface Payload {
			metadata: {
				plugin: string[];
			};
			mappings: Record<string, Record<string, unknown>>;
		}
	}
}

interface RefetchOptions {
	ids?: Arrayed<Source.Id>;
	refetchKeys?: Record<Source.Id, Array<keyof Doc.Type>>;
	addToHistory?: boolean;
	create_notes?: boolean;
	notes_color?: string;
	notes_tags?: string[];
	notes_glyph_id?: Glyph.Id;
	name?: string;
	frame?: MinMax;
}

const EVENT_STREAM_FAILURE_MESSAGE_KEYS: Record<
	Exclude<QueryStreamFailureReason, "superseded">,
	string
> = {
	backlog: "eventStore.streamFailed.backlog",
	quota: "eventStore.streamFailed.quota",
	storage: "eventStore.streamFailed.storage",
	normalization: "eventStore.streamFailed.normalization",
	disconnected: "eventStore.streamFailed.disconnected",
};

type ViewportQueryPriority = RawQueryQueuePriority;

type QueryFileOptions = GulpDataset.QueryGulp.Options & {
	operationId?: Operation.Id;
	queuePriority?: ViewportQueryPriority;
	eventStoreMode?: "replace" | "upsert";
	eventStreamKind?: "query" | "realtime";
};

interface QueryStreamState {
	requestId: Request.Id;
	sourceIds: Source.Id[];
	operationId: Operation.Id;
	operationEpoch: number;
	abortController?: AbortController;
	documentListenerId?: string;
	collabListenerId?: string;
	statsListenerId?: string;
	queuePriority?: ViewportQueryPriority;
	closed: boolean;
	completedByStats: boolean;
	flushChain: Promise<void>;
}

export interface EventDocumentStreamOptions {
	requestId: Request.Id;
	sourceIds: Source.Id[];
	operationId: Operation.Id;
	kind: "query" | "enrichment" | "ingestion" | "extension" | "realtime";
	mode: "replace" | "upsert";
	completionMode?: "last" | "explicit";
	deduplicateSequences?: boolean;
	defaultSourceSettings?: DocumentStreamSourceSettings;
	sourceSettingsById?: Record<string, DocumentStreamSourceSettings>;
	expectedEvents?: number;
}

interface QueryGulpRequestBody {
	flt: {
		doc_ids: Doc.Id[];
	};
	q_options: {
		limit: number;
		fields?: string[];
		preview_mode?: boolean;
	};
}

interface QueryGulpResponse {
	docs: Doc.Type[];
	total_hits: number;
}

interface RefetchQueueItem {
	sourceId: Source.Id;
	operationId: Operation.Id;
	query: Query.Type;
	options: QueryFileOptions;
	sequence: number;
}

interface InfoProps {
	app: App.Type;
	setInfo: React.Dispatch<React.SetStateAction<App.Type>>;
	timeline: React.RefObject<HTMLDivElement>;
}

export interface UserDeleteResult {
	/** Whether the backend confirmed the user deletion. */
	success: boolean;
	/** Backend error details returned for a declined delete request. */
	errorDescription?: string;
}

/**
 * Extracts a readable backend error message from a raw mutation response.
 *
 * @param response - Raw API response returned from a user mutation request.
 * @returns Backend error name/message text when the response exposes it.
 */
function getUserMutationErrorDescription(
	response: ResponseHandler<
		ResponseBase<undefined | ResponseErrorBody>
	> | null,
): string | undefined {
	const error = response?.data?.__error;
	if (typeof error !== "object" || error === null) {
		return undefined;
	}

	const parts = [error.name, error.msg].filter(
		(part): part is string => typeof part === "string" && part.length > 0,
	);
	return parts.length > 0 ? parts.join(": ") : undefined;
}

const glyphDB = new GulpIndexedDB("gulp_DB", "gulp_glyphs");
const GLYPH_PAGE_SIZE = 10000;
const ACTIVE_REQUEST_STATUSES = new Set<Request.Status>([
	Request.Status.PENDING,
	Request.Status.ONGOING,
]);
const MAX_QUERY_CONCURRENCY = 4;
const QUERY_DOC_WORKER_THRESHOLD = 1000;

export class Info implements InfoProps {
	private _app: App.Type;
	private readonly timelineScaleStore: TimelineScaleStore;
	setInfo: SetState<App.Type>;
	timeline: React.RefObject<HTMLDivElement>;

	public readonly MIN_VISIBLE_TIMELINE_DURATION_MS = 10;
	private static _latestInstance: Info | null = null;
	public ingestionProgress = new Map<Source.Id, number>();
	public activeUploads = new Map<
		Request.Id,
		{ filename: string; percent: number }
	>();
	private _resyncPollTimer: ReturnType<typeof setInterval> | null = null;
	private _resyncPollSequence = 0;
	private _resyncIngestionLoadings: IngestionResyncLoadings = new Map();
	private _ingestSourceDoneOperationId: Operation.Id | null = null;
	private _ingestSourceDoneCleanupListenerId: string | null = null;
	private _ingestStatsDoneListenerId: string | null = null;
	private _ingestSourceDoneFailureListenerId: string | null = null;
	private _notifiedIngestFailures = new Set<Request.Id>();
	private activeQueryStreams = new Map<Request.Id, QueryStreamState>();
	private activeQuerySourceRequests = new Map<Source.Id, Request.Id>();
	private refetchQueue: RefetchQueueItem[] = [];
	private refetchQueueSequence = 0;
	private activeQueuedQueryCount = 0;
	private viewportSourceIds = new Set<Source.Id>();
	private viewportSourceOrder = new Map<Source.Id, number>();
	private viewportSourceSignature = "";
	private readonly viewportQueryGate = new ViewportQueryGate();
	private viewportQueryPumpTimer: ReturnType<typeof setTimeout> | null = null;
	private viewportSourceListeners = new Set<() => void>();
	private timelineScaleCommandListeners = new Set<
		(scale: number, origin?: object) => void
	>();
	private notesReloadSequence = 0;
	private linksReloadSequence = 0;
	private highlightsReloadSequence = 0;
	private operationScopedSocketCleanups = new Set<() => void>();
	private activeEventDocumentStreams = new Set<Request.Id>();
	private backendDispatchedEventDocumentStreams = new Set<Request.Id>();
	private eventDocumentStreamOptions = new Map<
		Request.Id,
		EventDocumentStreamOptions
	>();
	private eventDocumentStreamCleanups = new Map<Request.Id, Set<() => void>>();
	private readonly workspaceSessionRepository: WorkspaceSessionRepository;
	private autosaveFailureNotified = false;
	private eventStoreFallbackNotified = false;

	constructor({ app, setInfo, timeline }: InfoProps) {
		this._app = app;
		this.timelineScaleStore = new TimelineScaleStore(app.timeline.scale);
		this.setInfo = setInfo;
		this.timeline = timeline;
		this.workspaceSessionRepository = new WorkspaceSessionRepository(
			async (userId, key, bucket) =>
				api<User.Type>("/user_update", {
					method: "PATCH",
					query: { user_id: userId },
					body: { user_data: { [key]: bucket } },
					showErrorToast: false,
				}),
		);
		Info._latestInstance = this;
	}

	/**
	 * Returns the application state currently owned by this Info instance.
	 * @returns Current application state.
	 */
	get app(): App.Type {
		return this._app;
	}

	/**
	 * Replaces the application state reference and tracks its canonical scale.
	 * Synchronization intentionally does not emit because providers assign during render.
	 * @param app Latest React application state.
	 */
	set app(app: App.Type) {
		const incomingScale = this.timelineScaleStore.reconcileReactSnapshot(
			app.timeline.scale,
		);
		if (incomingScale !== app.timeline.scale) {
			app.timeline = { ...app.timeline, scale: incomingScale };
		}
		this._app = app;
	}

	/**
	 * Stores the source IDs intersecting the current Canvas viewport and overscan.
	 * Viewport changes update render subscribers and reprioritize only already
	 * queued raw queries after the settle delay.
	 * @param sourceIds Source ids ordered by their Canvas row position.
	 * @returns Nothing.
	 */
	setViewportSourceIds = (sourceIds: Source.Id[]): void => {
		const signature = sourceIds.join("|");
		if (signature === this.viewportSourceSignature) return;

		this.viewportSourceSignature = signature;
		this.viewportSourceIds = new Set(sourceIds);
		this.viewportSourceOrder = new Map(
			sourceIds.map((sourceId, index) => [sourceId, index]),
		);
		this.viewportQueryGate.markViewportChanged();
		this.notifyViewportSourceListeners();
		if (this.refetchQueue.length > 0) {
			this.scheduleRefetchQueuePumpAfterViewportChange();
		}
	};

	/**
	 * Subscribes to viewport source changes.
	 * @param listener Callback invoked when the Canvas reports a new rendered source set.
	 * @returns Function that removes the listener.
	 */
	subscribeViewportSourceIds = (listener: () => void): (() => void) => {
		this.viewportSourceListeners.add(listener);
		return () => {
			this.viewportSourceListeners.delete(listener);
		};
	};

	/**
	 * Returns the stable signature for the current viewport source IDs.
	 * @returns Pipe-delimited source ID signature for use with external-store subscriptions.
	 */
	getViewportSourceSignature = (): string => this.viewportSourceSignature;

	/**
	 * Returns the current viewport source IDs.
	 * @returns New set containing the source IDs most recently reported by the Canvas.
	 */
	getViewportSourceIds = (): Set<Source.Id> => new Set(this.viewportSourceIds);

	/**
	 * Returns the current viewport state used by the raw query scheduler.
	 * @returns Latest viewport source ids and their Canvas row ordering.
	 */
	private getRawQueryViewportState = (): RawQueryViewportState => ({
		sourceIds: this.viewportSourceIds,
		sourceOrder: this.viewportSourceOrder,
	});

	/**
	 * Notifies subscribers that the Canvas viewport source set changed.
	 * @returns Nothing.
	 */
	private notifyViewportSourceListeners = (): void => {
		this.viewportSourceListeners.forEach((listener) => listener());
	};

	/**
	 * Cancels queued refetch work that has not started yet for the supplied sources.
	 * @param sourceIds Source ids whose pending queue entries should be removed.
	 * @returns Nothing.
	 */
	private removeQueuedRefetchesForSources = (sourceIds: Source.Id[]): void => {
		if (sourceIds.length === 0 || this.refetchQueue.length === 0) return;

		const sourceIdSet = new Set(sourceIds);
		this.refetchQueue = this.refetchQueue.filter(
			(item) => !sourceIdSet.has(item.sourceId),
		);
	};

	/**
	 * Cancels queued refetch work that has not started yet for one operation.
	 * @param operationId Operation whose queued timeline queries should be removed.
	 * @returns Nothing.
	 */
	private removeQueuedRefetchesForOperation = (
		operationId: Operation.Id,
	): void => {
		if (this.refetchQueue.length === 0) return;
		this.refetchQueue = this.refetchQueue.filter(
			(item) => item.operationId !== operationId,
		);
	};

	/**
	 * Applies the completed rebase to local source bounds and clears the UI offset.
	 * @param sourceId Source whose persisted documents were shifted by the backend.
	 * @param offsetMsec Offset that was applied to raw document timestamps.
	 * @returns Nothing.
	 */
	private applyCompletedSourceRebase = (
		sourceId: Source.Id,
		offsetMsec: number,
	): void => {
		const offsetNsec = this.offsetDurationToNanos(offsetMsec);
		const files = this.app.target.files.map((file) => {
			if (file.id !== sourceId) return file;

			const minNsec = this.toExactNanos(file.nanotimestamp.min);
			const maxNsec = this.toExactNanos(file.nanotimestamp.max);

			return {
				...file,
				timestamp: {
					min: file.timestamp.min + offsetMsec,
					max: file.timestamp.max + offsetMsec,
				},
				nanotimestamp: {
					min: minNsec + offsetNsec,
					max: maxNsec + offsetNsec,
				},
				settings: {
					...file.settings,
					offset: 0,
				},
				_sampleDataCached: {
					...file._sampleDataCached,
					sample_data: null,
				},
			};
		});
		const filters = this.shiftSourceFilterRangeByOffset(
			this.app.target.filters,
			sourceId,
			offsetNsec,
		);
		const cachedFilterData = new Map(this.app.timeline.cache.data);
		cachedFilterData.delete(sourceId);
		const cachedFilters = { ...this.app.timeline.cache.filters };
		delete cachedFilters[sourceId];

		RenderEngine.resetSource(sourceId);
		const nextApp = {
			...this.app,
			target: {
				...this.app.target,
				files,
				filters,
			},
			timeline: {
				...this.app.timeline,
				cache: {
					...this.app.timeline.cache,
					data: cachedFilterData,
					filters: cachedFilters,
				},
			},
		};
		this.app = nextApp;
		this.setInfo(() => nextApp);
	};

	/**
	 * Clears a saved source offset from all persisted user sessions.
	 * @param sourceId Source whose session offset should be cleared.
	 * @param offsetNsec Offset that was applied to raw document timestamps.
	 * @returns Promise resolving after user_data has been updated.
	 */
	private clearSourceOffsetInSessions = async (
		sourceId: Source.Id,
		offsetNsec: bigint,
	): Promise<void> => {
		const source = this.app.target.files.find((file) => file.id === sourceId);
		const operationId =
			source?.operation_id ?? Operation.Entity.selected(this.app)?.id;
		if (!this.app.general.user || !operationId) return;
		this.initializeWorkspaceSessionRepository();

		await this.workspaceSessionRepository.updateAll(
			operationId,
			(session) => {
				const savedSettings =
					session.snapshot.source_settings.source_settings[sourceId];
				const savedFilter = session.snapshot.filters[sourceId];
				let snapshot = session.snapshot;

				if (savedSettings && savedSettings.offset !== 0) {
					snapshot = {
						...snapshot,
						source_settings: {
							source_settings: {
								...snapshot.source_settings.source_settings,
								[sourceId]: {
									...savedSettings,
									offset: 0,
								},
							},
						},
					};
				}

				if (savedFilter?.source_config?.range) {
					snapshot = {
						...snapshot,
						filters: {
							...snapshot.filters,
							[sourceId]: {
								...savedFilter,
								source_config: {
									...savedFilter.source_config,
									range: this.shiftNanosRange(
										savedFilter.source_config.range,
										offsetNsec,
									),
								},
							},
						},
					};
				}

				return snapshot === session.snapshot
					? session
					: {
							...session,
							updated_at_msec: Date.now(),
							snapshot,
						};
			},
		);
	};

	/**
	 * Converts an offset duration in milliseconds to nanoseconds.
	 * @param offsetMsec Offset duration, not an epoch timestamp.
	 * @returns Offset duration in nanoseconds.
	 */
	private offsetDurationToNanos = (offsetMsec: number): bigint =>
		BigInt(Math.trunc(offsetMsec)) * 1_000_000n;

	/**
	 * Converts nanosecond-like values without losing precision through Number.
	 * @param value Timestamp value to normalize.
	 * @returns Nanosecond bigint.
	 */
	private toExactNanos = (value: string | number | bigint | Date): bigint => {
		if (typeof value === "bigint") return value;
		if (typeof value === "string" && /^-?\d+$/.test(value.trim())) {
			return BigInt(value.trim());
		}
		if (
			typeof value === "number" &&
			Number.isFinite(value) &&
			Math.abs(value) >= 1_000_000_000_000_000
		) {
			return BigInt(Math.trunc(value));
		}

		return Internal.Transformator.toNanos(value);
	};

	/**
	 * Shifts a raw nanosecond query range after backend rebase.
	 * @param range Existing query range.
	 * @param offsetNsec Rebase offset in nanoseconds.
	 * @returns Shifted query range as nanosecond strings.
	 */
	private shiftNanosRange = (
		range: { min: string | number; max: string | number },
		offsetNsec: bigint,
	): { min: string; max: string } => ({
		min: (this.toExactNanos(range.min) + offsetNsec).toString(),
		max: (this.toExactNanos(range.max) + offsetNsec).toString(),
	});

	/**
	 * Shifts the active stored filter range for a rebased source.
	 * @param filters Current source filters.
	 * @param sourceId Source whose documents were rebased.
	 * @param offsetNsec Rebase offset in nanoseconds.
	 * @returns Updated filter map.
	 */
	private shiftSourceFilterRangeByOffset = (
		filters: App.Type["target"]["filters"],
		sourceId: Source.Id,
		offsetNsec: bigint,
	): App.Type["target"]["filters"] => {
		const filter = filters[sourceId];
		if (!filter?.source_config?.range) return filters;

		return {
			...filters,
			[sourceId]: {
				...filter,
				source_config: {
					...filter.source_config,
					range: this.shiftNanosRange(filter.source_config.range, offsetNsec),
				},
			},
		};
	};

	/**
	 * Cancels active query streams for the supplied source ids and waits for any
	 * already-started DataStore flush to finish before callers reset source events.
	 * @param sourceIds Source ids whose active query work should be stopped.
	 * @param cancelBackend Whether to send a backend cancellation request.
	 * @returns Promise that resolves once local stream cleanup is complete.
	 */
	private cancelQueryStreamsForSources = async (
		sourceIds: Source.Id[],
		cancelBackend: boolean,
	): Promise<void> => {
		const requestIds = new Set<Request.Id>();

		sourceIds.forEach((sourceId) => {
			const activeRequestId = this.activeQuerySourceRequests.get(sourceId);
			if (activeRequestId) {
				requestIds.add(activeRequestId);
				return;
			}

			const loadingRequestId = requestStore.getRequestIdByFile(sourceId);
			if (
				loadingRequestId &&
				String(loadingRequestId).startsWith(Request.Prefix.QUERY)
			) {
				requestStore.deleteLoadingByFile(sourceId);
				this.app.general.loadings = requestStore.getLoadings();
				DataStore.markDirty();
				if (cancelBackend) {
					this.request_cancel(loadingRequestId);
				}
			}
		});

		await Promise.all(
			Array.from(requestIds).map((requestId) =>
				this.closeQueryStream(requestId, { cancelBackend }),
			),
		);
	};

	/**
	 * Adds one source query to the mutable refetch queue.
	 * @param item Source query and options to run when a concurrency slot is free.
	 * @returns Nothing.
	 */
	private enqueueRefetchQuery = (item: RefetchQueueItem): void => {
		this.removeQueuedRefetchesForSources([item.sourceId]);
		this.refetchQueue.push(item);
	};

	/**
	 * Starts queued refetches after the current viewport row set has stabilized,
	 * filling a shared four-request pool and selecting current viewport rows first.
	 * @returns Nothing.
	 */
	private pumpRefetchQueue = (): void => {
		if (this.deferQueryPumpForUnsettledViewport()) return;

		while (this.activeQueuedQueryCount < MAX_QUERY_CONCURRENCY) {
			const selection = selectNextRawQueryQueueItem(
				this.refetchQueue,
				this.getRawQueryViewportState(),
			);
			if (!selection) break;

			const [item] = this.refetchQueue.splice(selection.index, 1);
			this.startRefetchQueueItem(item, selection.priority);
		}
	};

	/**
	 * Defers queued retrieval briefly after viewport changes.
	 * Background work is also held during this short window so it cannot occupy a
	 * slot before explicitly scheduled viewport-priority work can start.
	 * @returns True when a trailing query-pump timer owns the next queue attempt.
	 */
	private deferQueryPumpForUnsettledViewport = (): boolean => {
		if (!this.hasVisibleQueuedSource()) {
			this.clearViewportQueryPumpTimer();
			return false;
		}

		const remainingDelay = this.viewportQueryGate.getRemainingDelay();
		if (remainingDelay <= 0) {
			this.clearViewportQueryPumpTimer();
			return false;
		}

		this.clearViewportQueryPumpTimer();
		this.viewportQueryPumpTimer = setTimeout(() => {
			this.viewportQueryPumpTimer = null;
			this.pumpRefetchQueue();
		}, Math.max(1, Math.ceil(remainingDelay)));
		return true;
	};

	/**
	 * Cancels the pending trailing viewport query-pump attempt, if one exists.
	 * @returns Nothing.
	 */
	private clearViewportQueryPumpTimer = (): void => {
		if (this.viewportQueryPumpTimer === null) return;
		clearTimeout(this.viewportQueryPumpTimer);
		this.viewportQueryPumpTimer = null;
	};

	/**
	 * Schedules queued raw query dispatch after a viewport update without blocking render.
	 * @returns Nothing.
	 */
	private scheduleRefetchQueuePumpAfterViewportChange = (): void => {
		const remainingDelay = this.hasVisibleQueuedSource()
			? this.viewportQueryGate.getRemainingDelay()
			: 0;

		this.clearViewportQueryPumpTimer();
		this.viewportQueryPumpTimer = setTimeout(() => {
			this.viewportQueryPumpTimer = null;
			this.pumpRefetchQueue();
		}, Math.max(1, Math.ceil(remainingDelay)));
	};

	/**
	 * Checks whether any currently visible source is waiting in the refetch queue.
	 * @returns True when current viewport-priority work is queued.
	 */
	private hasVisibleQueuedSource = (): boolean =>
		hasRawQueryViewportWork(this.refetchQueue, this.getRawQueryViewportState());

	/**
	 * Checks whether a source should be treated as visible right now.
	 * @param sourceId Source id to inspect.
	 * @returns True when the Canvas has not reported visibility yet or the source is visible.
	 */
	private isQueuedSourceVisible = (sourceId: Source.Id): boolean =>
		isRawQuerySourceVisible(sourceId, this.getRawQueryViewportState());

	/**
	 * Runs a queued source query and lets the stream lifecycle release concurrency.
	 * @param item Queue item to execute.
	 * @param queuePriority Priority bucket used for concurrency accounting.
	 * @returns Nothing.
	 */
	private startRefetchQueueItem = (
		item: RefetchQueueItem,
		queuePriority: ViewportQueryPriority,
	): void => {
		this.activeQueuedQueryCount++;

		this.query_file(item.query, {
			...item.options,
			queuePriority,
		}).catch((error) => {
			Logger.warn(
				`Queued query failed for source ${item.sourceId}: ${(error as Error).message}`,
				Info,
			);
		});
	};

	/**
	 * Releases one queue concurrency slot and schedules the next pending source.
	 * @param queuePriority Priority bucket to decrement.
	 * @returns Nothing.
	 */
	private finishQueuedQuery = (queuePriority?: ViewportQueryPriority): void => {
		if (queuePriority) {
			this.activeQueuedQueryCount = Math.max(
				0,
				this.activeQueuedQueryCount - 1,
			);
		}

		this.pumpRefetchQueue();
	};

	/**
	 * Registers a query stream before the HTTP request starts so early websocket
	 * chunks can be accepted without depending on request loading state.
	 * @param requestId Backend request id used by websocket chunks.
	 * @param sourceIds Source ids affected by this stream.
	 * @param operationId Operation that owns the backend request.
	 * @param operationEpoch Datastore generation captured for the request.
	 * @param queuePriority Optional queue priority used for concurrency release.
	 * @returns Mutable stream state stored until final chunk or cancellation.
	 */
	private registerQueryStream = (
		requestId: Request.Id,
		sourceIds: Source.Id[],
		operationId: Operation.Id,
		operationEpoch: number,
		abortController?: AbortController,
		queuePriority?: ViewportQueryPriority,
	): QueryStreamState => {
		const stream: QueryStreamState = {
			requestId,
			sourceIds,
			operationId,
			operationEpoch,
			abortController,
			queuePriority,
			closed: false,
			completedByStats: false,
			flushChain: Promise.resolve(),
		};

		this.activeQueryStreams.set(requestId, stream);
		requestStore.registerManagedQuery({
			requestId,
			operationId,
			sourceIds,
			abortController,
		});
		sourceIds.forEach((sourceId) => {
			this.activeQuerySourceRequests.set(sourceId, requestId);
		});

		return stream;
	};

	/**
	 * Checks whether queued work still belongs to the currently selected operation.
	 * @param operationEpoch Datastore generation captured before asynchronous work began.
	 * @param operationId Operation identifier captured with the request.
	 * @returns True only while both the generation and selected operation still match.
	 */
	private isOperationContextCurrent = (
		operationEpoch: number,
		operationId: Operation.Id,
	): boolean =>
		DataStore.getRenderOperationEpoch() === operationEpoch &&
		(DataStore.getRenderOperationId() ??
			Operation.Entity.selected(this.app)?.id) === operationId;

	/**
	 * Checks whether a query stream is still registered for the active operation.
	 * @param stream Stream state captured by an asynchronous callback.
	 * @returns True while the stream is open, registered, and operation-current.
	 */
	private isQueryStreamCurrent = (stream: QueryStreamState): boolean =>
		!stream.closed &&
		this.activeQueryStreams.get(stream.requestId) === stream &&
		this.isOperationContextCurrent(stream.operationEpoch, stream.operationId);

	/**
	 * Tracks a websocket cleanup that must run synchronously on operation switches.
	 * @param cleanup Function that unregisters every listener for one request.
	 * @returns Idempotent cleanup wrapper that also releases its registry entry.
	 */
	private trackOperationSocketCleanup = (cleanup: () => void): (() => void) => {
		let isReleased = false;
		const release = (): void => {
			if (isReleased) return;
			isReleased = true;
			this.operationScopedSocketCleanups.delete(release);
			cleanup();
		};
		this.operationScopedSocketCleanups.add(release);
		return release;
	};

	/**
	 * Invalidates queued queries and websocket listeners before changing operations.
	 * @returns Nothing.
	 */
	private invalidateOperationStreams = (): void => {
		this.stop_ingestion_resync_poll();
		this._clear_ingest_source_done_listeners();
		this.clearViewportQueryPumpTimer();
		this.refetchQueue = [];
		this.activeQueuedQueryCount = 0;

		Array.from(this.activeQueryStreams.values()).forEach((stream) => {
			stream.queuePriority = undefined;
			void this.closeQueryStream(stream.requestId, { cancelBackend: true });
		});
		this.activeQuerySourceRequests.clear();

		const cleanups = Array.from(this.operationScopedSocketCleanups);
		this.operationScopedSocketCleanups.clear();
		cleanups.forEach((cleanup) => cleanup());

		const socket = SmartSocket.Class.instance;
		const operationId = DataStore.getRenderOperationId();
		if (socket && operationId) {
			void socket.releaseOperation(operationId).catch((error: unknown) => {
				Logger.warn(
					`Unable to release EventStore operation ${operationId}: ${error instanceof Error ? error.message : String(error)}`,
					Info,
				);
			});
		}
		for (const requestId of this.activeEventDocumentStreams) {
			this.releaseEventDocumentStream(requestId);
		}
		this.activeEventDocumentStreams.clear();
		this.eventDocumentStreamOptions.clear();
	};

	/**
	 * Registers QueryWorker ownership before an HTTP request can emit document chunks.
	 * @param options Request identity, source scope, mutation mode, and completion policy.
	 * @returns True after the worker acknowledges registration.
	 */
	beginEventDocumentStream = async (
		options: EventDocumentStreamOptions,
	): Promise<boolean> => {
		const socket = SmartSocket.Class.instance;
		const operationEpoch = DataStore.getRenderOperationEpoch();
		if (
			!socket ||
			!this.isOperationContextCurrent(
				operationEpoch,
				options.operationId,
			)
		) {
			return false;
		}

		if (this.activeEventDocumentStreams.has(options.requestId)) {
			await this.cancelEventDocumentStream(options.requestId);
		}
		const lodDecision = getEventStoreLodDecision();
		if (
			!lodDecision.enabled &&
			!this.eventStoreFallbackNotified
		) {
			this.eventStoreFallbackNotified = true;
			toast.warning(translate("eventStore.legacyFallback"), {
				id: "event-store-legacy-fallback",
				icon: <Icon name="Warning" />,
				richColors: true,
			});
		}
		const descriptor: DocumentStreamDescriptor = {
			requestId: options.requestId,
			operationId: options.operationId,
			operationEpoch,
			owner: lodDecision.enabled ? "persist" : "passthrough",
			completionMode: options.completionMode ?? "last",
			deduplicateSequences: options.deduplicateSequences,
			kind:
				options.mode === "replace"
					? "replace"
					: options.kind === "enrichment"
						? "enrichment"
						: options.kind === "ingestion" || options.kind === "realtime"
							? "realtime-ingest"
							: "query-upsert",
			sourceIds: options.sourceIds,
			defaultSourceSettings: options.defaultSourceSettings,
			sourceSettingsById:
				options.sourceSettingsById ??
				(options.sourceIds.length > 0
					? this.getQuerySourceSettingsById(options.sourceIds)
					: undefined),
			expectedEvents:
				options.expectedEvents ??
				(options.mode === "replace"
					? options.sourceIds.reduce(
							(total, sourceId) =>
								total +
								(Source.Entity.id(this.app, sourceId)?.total ?? 0),
							0,
						)
					: undefined),
		};

		try {
			await socket.registerDocumentStream(descriptor);
			if (
				SmartSocket.Class.instance !== socket ||
				!this.isOperationContextCurrent(operationEpoch, options.operationId)
			) {
				await socket.cancelDocumentStream(options.requestId);
				return false;
			}
			this.activeEventDocumentStreams.add(options.requestId);
			this.eventDocumentStreamOptions.set(options.requestId, {
				...options,
				sourceIds: [...options.sourceIds],
				sourceSettingsById: options.sourceSettingsById
					? { ...options.sourceSettingsById }
					: undefined,
			});
			return true;
		} catch (error) {
			Logger.warn(
				`Unable to register event stream ${options.requestId}: ${(error as Error).message}`,
				Info,
			);
			return false;
		}
	};

	/**
	 * Records that a registered stream has crossed the backend dispatch boundary.
	 * @param requestId Registered request about to be sent or enqueued.
	 * @returns True when the request was active and its dispatch state was recorded.
	 */
	private markEventDocumentStreamDispatched = (
		requestId: Request.Id,
	): boolean => {
		if (!this.activeEventDocumentStreams.has(requestId)) return false;
		this.backendDispatchedEventDocumentStreams.add(requestId);
		return true;
	};

	/**
	 * Marks an explicitly completed request after its final storage commit.
	 * @param requestId Request whose QueryWorker stream can become terminal.
	 * @returns Promise settled after worker acknowledgement and local cleanup.
	 * @throws Error when the query transport is unavailable or rejects completion.
	 */
	completeEventDocumentStream = async (
		requestId: Request.Id,
	): Promise<void> => {
		const socket = SmartSocket.Class.instance;
		if (!socket) throw new Error("Query transport is unavailable.");
		await socket.completeDocumentStream(requestId);
		this.activeEventDocumentStreams.delete(requestId);
		this.eventDocumentStreamOptions.delete(requestId);
		this.releaseEventDocumentStream(requestId);
	};

	/**
	 * Invalidates one document stream before callers ask the backend to cancel work.
	 * @param requestId Request whose late chunks must be rejected.
	 * @returns Promise settled after QueryWorker installs the terminal guard.
	 */
	cancelEventDocumentStream = async (
		requestId: Request.Id,
	): Promise<void> => {
		try {
			await SmartSocket.Class.instance?.cancelDocumentStream(requestId);
		} finally {
			this.activeEventDocumentStreams.delete(requestId);
			this.eventDocumentStreamOptions.delete(requestId);
			this.releaseEventDocumentStream(requestId);
		}
	};

	/**
	 * Attaches request-scoped metadata cleanup without exposing document chunks.
	 * @param requestId Registered document request.
	 * @param cleanup Idempotent listener cleanup invoked at every terminal path.
	 * @returns False when the request is no longer active.
	 */
	attachEventDocumentStreamCleanup = (
		requestId: Request.Id,
		cleanup: () => void,
	): boolean => {
		if (!this.activeEventDocumentStreams.has(requestId)) return false;
		const cleanups = this.eventDocumentStreamCleanups.get(requestId) ?? new Set();
		cleanups.add(cleanup);
		this.eventDocumentStreamCleanups.set(requestId, cleanups);
		return true;
	};

	/**
	 * Runs and forgets every non-document listener attached to one request.
	 * @param requestId Terminal request whose resources must be released.
	 * @returns Nothing.
	 */
	private releaseEventDocumentStream = (requestId: Request.Id): void => {
		this.backendDispatchedEventDocumentStreams.delete(requestId);
		const cleanups = this.eventDocumentStreamCleanups.get(requestId);
		this.eventDocumentStreamCleanups.delete(requestId);
		for (const cleanup of cleanups ?? []) {
			try {
				cleanup();
			} catch (error) {
				Logger.warn(
					`Event stream cleanup failed for ${requestId}: ${(error as Error).message}`,
					Info,
				);
			}
		}
	};

	/**
	 * Reconciles application-owned query state after QueryWorker changes stream state.
	 * Document payloads remain worker-owned; this method only releases metadata and UI
	 * request state after the durable IndexedDB terminal transition.
	 * @param change Typed request lifecycle transition published by SmartSocket.
	 * @returns Nothing.
	 */
	handleEventDocumentStreamState = (
		change: DocumentStreamStateChange,
	): void => {
		const requestId = change.descriptor.requestId as Request.Id;
		// A correlated STREAM_FAILURE follows the failed state and carries the
		// actionable reason. Keep request metadata until that event is handled.
		if (change.state === "failed") return;
		if (
			change.state !== "completed" &&
			change.state !== "canceled"
		) {
			return;
		}

		this.activeEventDocumentStreams.delete(requestId);
		this.eventDocumentStreamOptions.delete(requestId);
		this.releaseEventDocumentStream(requestId);

		const stream = this.activeQueryStreams.get(requestId);
		if (!stream) return;
		const completed = change.state === "completed";
		if (completed && this.isQueryStreamCurrent(stream)) {
			this.reconcileQuerySourceTotals(stream.sourceIds);
		}
		void this.closeQueryStream(requestId, {
			managedStatus: completed ? "completed" : "canceled",
		}).then(() => {
			if (
				completed &&
				this.isOperationContextCurrent(
					change.descriptor.operationEpoch,
					change.descriptor.operationId as Operation.Id,
				)
			) {
				this.render();
			}
		});
	};

	/**
	 * Cancels backend work after QueryWorker rejects storage, quota, or backlog.
	 * @param event Terminal persistence failure containing no document payloads.
	 * @returns Nothing.
	 */
	handleEventDocumentStreamFailure = (
		event: QueryStreamFailureEvent,
	): void => {
		const requestId = event.requestId as Request.Id;
		const backendDispatched =
			this.backendDispatchedEventDocumentStreams.has(requestId);
		const options = this.eventDocumentStreamOptions.get(requestId);
		const recoverableSourceIds = Array.from(
			new Set(options?.sourceIds ?? []),
		).filter((sourceId) => Boolean(Source.Entity.id(this.app, sourceId)));
		if (backendDispatched) this.request_cancel(requestId);
		this.activeEventDocumentStreams.delete(requestId);
		this.eventDocumentStreamOptions.delete(requestId);
		this.releaseEventDocumentStream(requestId);
		console.error(
			`Event stream ${requestId} failed (${event.reason}): ${event.message}`,
			Info,
		);
		if (
			(event.reason === "storage" || event.reason === "quota") &&
			reloadWithLegacyEventPipeline()
		) {
			return;
		}
		if (event.reason !== "superseded") {
			toast.error(translate(EVENT_STREAM_FAILURE_MESSAGE_KEYS[event.reason]), {
				id: `event-store-stream-failure-${requestId}`,
				icon: <Icon name="Stop" />,
				richColors: true,
			});
		}
		if (
			event.reason === "disconnected" &&
			recoverableSourceIds.length > 0 &&
			this.isOperationContextCurrent(
				event.operationEpoch,
				event.operationId as Operation.Id,
			)
		) {
			void SmartSocket.Class.instance
				.awaitReady(30_000)
				.then(() => {
					if (
						this.isOperationContextCurrent(
							event.operationEpoch,
							event.operationId as Operation.Id,
						)
					) {
						return this.refetch({ ids: recoverableSourceIds });
					}
				})
				.catch(() => undefined);
		}

		if (this.activeQueryStreams.has(requestId)) {
			void this.closeQueryStream(requestId, {
				managedStatus: "canceled",
			});
			return;
		}
		for (const sourceId of options?.sourceIds ?? []) {
			this.delLoadingByFile(sourceId);
		}
		this.delLoading(requestId);
	};

	/**
	 * Records compact source authority and surfaces an in-place cache warning once.
	 * @param event Request-scoped summary without canonical document objects.
	 * @returns Nothing.
	 */
	handleEventSourceSummary = (event: QuerySourceSummaryEvent): void => {
		if (
			this.isOperationContextCurrent(
				event.operationEpoch,
				event.operationId as Operation.Id,
			)
		) {
			const options = this.eventDocumentStreamOptions.get(
				event.requestId as Request.Id,
			);
			if (options && !options.sourceIds.includes(event.summary.sourceId)) {
				options.sourceIds.push(event.summary.sourceId);
			}
			if (event.replacementMode === "in-place") {
				toast.warning(translate("source.cacheRebuildInPlace"), {
					id: `event-store-in-place-${event.requestId}`,
					icon: <Icon name="Warning" />,
					richColors: true,
				});
			}
			DataStore.publishEventSourceSummary(event.summary);
		}
	};

	/**
	 * Cancels active and queued timeline query work for one operation.
	 * @param operationId Operation whose render queries should be canceled.
	 * @param options Cancellation options.
	 * @returns Promise that resolves after local stream cleanup completes.
	 */
	cancelOperationQueries = async (
		operationId: Operation.Id,
		options: { cancelBackend?: boolean } = {},
	): Promise<void> => {
		const cancelBackend = options.cancelBackend ?? true;
		this.removeQueuedRefetchesForOperation(operationId);
		this.clearViewportQueryPumpTimer();

		const streams = Array.from(this.activeQueryStreams.values()).filter(
			(stream) => stream.operationId === operationId,
		);

		await Promise.all(
			streams.map((stream) =>
				this.closeQueryStream(stream.requestId, {
					cancelBackend,
					managedStatus: "canceled",
				}),
			),
		);

		const managedRequests = requestStore
			.getManagedQueriesForOperation(operationId)
			.filter((request) => request.status === "active");
		await Promise.all(
			managedRequests.map((request) =>
				this.cancelManagedRequest(request.requestId, { cancelBackend }),
			),
		);
	};

	/**
	 * Cancels one managed timeline query request locally and on the backend.
	 * @param requestId Backend request identifier.
	 * @param options Cancellation options.
	 * @returns Promise that resolves after local cleanup completes.
	 */
	cancelManagedRequest = async (
		requestId: Request.Id,
		options: { cancelBackend?: boolean } = {},
	): Promise<void> => {
		const cancelBackend = options.cancelBackend ?? true;

		if (this.activeQueryStreams.has(requestId)) {
			await this.closeQueryStream(requestId, {
				cancelBackend,
				managedStatus: "canceled",
			});
			return;
		}

		const managedQuery = requestStore.getManagedQuery(requestId);
		if (!managedQuery && !String(requestId).startsWith(Request.Prefix.QUERY)) {
			if (cancelBackend) {
				this.request_cancel(requestId);
			}
			return;
		}

		requestStore.cancelManagedQuery(requestId);
		managedQuery?.sourceIds.forEach((sourceId) => this.delLoadingByFile(sourceId));
		this.delLoading(requestId);

		if (cancelBackend) {
			this.request_cancel(requestId);
		}
	};

	/**
	 * Closes a query stream, removes websocket listeners, clears loading markers,
	 * and waits for any already-started event flush before resolving.
	 * @param requestId Backend request id whose stream should be closed.
	 * @param options Cleanup options.
	 * @returns Promise that resolves when local cleanup is complete.
	 */
	private closeQueryStream = async (
		requestId: Request.Id,
		options: {
			cancelBackend?: boolean;
			clearLoading?: boolean;
			managedStatus?: "completed" | "canceled";
		} = {},
	): Promise<void> => {
		const stream = this.activeQueryStreams.get(requestId);
		if (!stream) return;
		if (stream.closed) {
			await stream.flushChain.catch(() => undefined);
			return;
		}

		stream.closed = true;
		if (options.managedStatus === "canceled" || options.cancelBackend) {
			requestStore.cancelManagedQuery(requestId);
		} else {
			requestStore.completeManagedQuery(requestId);
		}

		if (stream.documentListenerId) {
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.DOCUMENTS_CHUNK,
				stream.documentListenerId,
			);
		}
		if (stream.collabListenerId) {
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.COLLAB_CREATE,
				stream.collabListenerId,
			);
		}
		if (stream.statsListenerId) {
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				stream.statsListenerId,
			);
		}

		this.activeQueryStreams.delete(requestId);
		stream.sourceIds.forEach((sourceId) => {
			if (this.activeQuerySourceRequests.get(sourceId) === requestId) {
				this.activeQuerySourceRequests.delete(sourceId);
			}
		});

		let documentCancellation: Promise<void> | null = null;
		if (
			(options.cancelBackend || options.managedStatus === "canceled") &&
			this.activeEventDocumentStreams.has(requestId)
		) {
			documentCancellation = this.cancelEventDocumentStream(requestId);
		}

		if (options.cancelBackend) {
			this.request_cancel(requestId);
		}
		await documentCancellation?.catch((error: unknown) => {
			Logger.warn(
				`QueryWorker cancellation guard failed for ${requestId}: ${error instanceof Error ? error.message : String(error)}`,
				Info,
			);
		});

		await stream.flushChain.catch((error) => {
			Logger.warn(
				`Query stream flush failed for ${requestId}: ${(error as Error).message}`,
				Info,
			);
		});

		if (options.clearLoading !== false) {
			if (stream.sourceIds.length > 0) {
				stream.sourceIds.forEach((sourceId) => this.delLoadingByFile(sourceId));
			} else {
				this.delLoading(requestId);
			}
		}

		this.finishQueuedQuery(stream.queuePriority);
	};

	/**
	 * Prepares websocket docs for insertion, using the worker only when the batch is large enough.
	 * @param docs Raw document payload received from the websocket.
	 * @param sourceIds Source ids associated with the query request.
	 * @returns Normalized, grouped, and sorted document batches.
	 */
	private prepareQueryDocBatches = async (
		docs: QueryRawDocument[],
		sourceIds: Source.Id[],
	): Promise<Doc.PreparedBatch[]> => {
		if (docs.length === 0) return [];

		if (docs.length < QUERY_DOC_WORKER_THRESHOLD) {
			return this.prepareQueryDocBatchesOnMainThread(docs, sourceIds);
		}

		try {
			const result = await DataWorker.normalizeQueryDocs({
				docs,
				sourceSettingsById: this.getQuerySourceSettingsById(sourceIds),
				fallbackSourceId: sourceIds.length === 1 ? sourceIds[0] : undefined,
			});
			return result.batches;
		} catch (error) {
			Logger.warn(
				`Worker normalization failed, falling back to main thread: ${
					(error as Error).message
				}`,
				Info,
			);
			return this.prepareQueryDocBatchesOnMainThread(docs, sourceIds);
		}
	};

	/**
	 * Builds source settings for worker-side query document normalization.
	 * @param sourceIds Source ids associated with the query request.
	 * @returns Source settings keyed by source id.
	 */
	private getQuerySourceSettingsById = (
		sourceIds: Source.Id[],
	): Record<string, NormalizeQuerySourceSettings> => {
		const sources =
			sourceIds.length > 0
				? sourceIds
						.map((sourceId) => Source.Entity.id(this.app, sourceId))
						.filter((source): source is Source.Type => Boolean(source))
				: this.app.target.files;

		return sources.reduce<Record<string, NormalizeQuerySourceSettings>>(
			(settingsById, source) => {
				settingsById[source.id] = {
					field: String(source.settings.field),
					hash_function: source.settings.hash_function,
				};
				return settingsById;
			},
			{},
		);
	};

	/**
	 * Normalizes websocket docs on the main thread for small batches.
	 * @param docs Raw document payload received from the websocket.
	 * @param sourceIds Source ids associated with the query request.
	 * @returns Normalized, grouped, and sorted document batches.
	 */
	private prepareQueryDocBatchesOnMainThread = (
		docs: QueryRawDocument[],
		sourceIds: Source.Id[],
	): Doc.PreparedBatch[] => {
		const fallbackSource =
			sourceIds.length === 1 ? Source.Entity.id(this.app, sourceIds[0]) : null;
		const docsBySource = new Map<Source.Id, QueryRawDocument[]>();

		docs.forEach((doc) => {
			const sourceId =
				(doc["gulp.source_id"] as Source.Id | undefined) ?? fallbackSource?.id;
			if (!sourceId) return;

			const group = docsBySource.get(sourceId) ?? [];
			group.push(doc);
			docsBySource.set(sourceId, group);
		});

		return Array.from(docsBySource.entries()).map(([sourceId, sourceDocs]) => {
			const source = Source.Entity.id(this.app, sourceId) ?? fallbackSource;
			const field = source?.settings.field ?? "gulp.event_code";
			const hashFunction = source?.settings.hash_function ?? "fnv1a";
			const normalizedDocs = Doc.Entity.normalize(
				sourceDocs as unknown as Doc.Type[],
				field,
				hashFunction,
			);
			normalizedDocs.forEach((doc) => {
				doc["gulp.source_id"] = doc["gulp.source_id"] ?? sourceId;
			});
			return {
				sourceId,
				docs: Doc.Entity.sort(normalizedDocs),
			};
		});
	};

	/**
	 * Checks whether a queued query stream belongs to the current Canvas viewport.
	 * @param stream Active stream to classify.
	 * @returns True when any stream source is currently visible.
	 */
	private isQueryStreamInViewport = (stream: QueryStreamState): boolean =>
		stream.sourceIds.some((sourceId) => this.isQueuedSourceVisible(sourceId));

	/**
	 * Checks whether another active queued query stream is viewport-priority work.
	 * @param excludedRequestId Request id that should not be considered.
	 * @returns True when a different active queued stream is currently visible.
	 */
	private hasActiveViewportQueryStream = (
		excludedRequestId: Request.Id,
	): boolean =>
		Array.from(this.activeQueryStreams.values()).some(
			(stream) =>
				stream.requestId !== excludedRequestId &&
				Boolean(stream.queuePriority) &&
				this.isQueryStreamInViewport(stream),
		);

	/**
	 * Lets offscreen stream insertion yield briefly while viewport rows are queued.
	 * @param stream Query stream that is about to process another batch.
	 * @returns Promise that resolves when viewport-priority insertion can proceed.
	 */
	private waitForVisibleQueueBeforeBackgroundBatch = async (
		stream: QueryStreamState,
	): Promise<void> => {
		if (!stream.queuePriority || this.isQueryStreamInViewport(stream)) return;

		while (
			!stream.closed &&
			!this.isQueryStreamInViewport(stream) &&
			this.hasVisibleQueuedSource() &&
			this.activeQueuedQueryCount < MAX_QUERY_CONCURRENCY
		) {
			this.pumpRefetchQueue();
			await new Promise((resolve) => setTimeout(resolve, 16));
		}
		if (
			!stream.closed &&
			!this.isQueryStreamInViewport(stream) &&
			this.hasActiveViewportQueryStream(stream.requestId)
		) {
			await new Promise((resolve) => setTimeout(resolve, 32));
		}
	};

	/**
	 * Reconciles source totals after a query stream has flushed all documents.
	 * @param sourceIds Source ids whose loaded counts should be checked.
	 * @returns Nothing.
	 */
	private reconcileQuerySourceTotals = (sourceIds: Source.Id[]): void => {
		let changed = false;

		sourceIds.forEach((sourceId) => {
			const file = Source.Entity.id(this.app, sourceId);
			const loadedCount =
				DataStore.getEventSourceSummary(sourceId)?.eventCount ?? 0;
			if (file && loadedCount > file.total) {
				file.total = loadedCount;
				changed = true;
			}
		});

		if (changed) {
			this.setInfoByKey([...this.app.target.files], "target", "files");
		}
	};

	/**
	 * Refetches documents and updates associated state for the selected source files.
	 *
	 * @param options - Configuration options for the refetch.
	 * @param options.ids - Optional list of source IDs to refetch (defaults to selected sources).
	 * @param options.refetchKeys - Optional mapping of source IDs to specific document fields to fetch.
	 * @param options.addToHistory - Whether to append this query to the server-side queries history.
	 * @param options.create_notes - Whether to automatically create notes for matched documents.
	 * @param options.notes_color - Color to apply to any auto-created notes.
	 * @param options.notes_tags - Tags to apply to any auto-created notes.
	 * @param options.notes_glyph_id - Icon glyph ID for any auto-created notes.
	 * @param options.name - Optional name label for this search action.
	 * @param options.frame - Optional rendered timeframe to persist before the
	 * current timeline frame is applied as every source query's base range.
	 * @returns A promise that resolves when all queries are queued.
	 */
	refetch = async ({
		ids: _ids = Source.Entity.selected(this.app).map((f) => f.id),
		refetchKeys,
		addToHistory,
		create_notes,
		notes_color,
		notes_tags,
		notes_glyph_id,
		name,
		frame,
	}: RefetchOptions = {}) => {
		const files: Source.Type[] = Parser.array(_ids).map((id) =>
			Source.Entity.id(this.app, id),
		).filter(Boolean);

		if (frame) {
			this.setTimelineFrame(frame);
		}
		if (files.length === 0) return;

		let effectiveFrame = frame;
		if (
			!effectiveFrame &&
			this.app.timeline.frame.min === 0 &&
			this.app.timeline.frame.max === 0
		) {
			effectiveFrame = Source.Entity.renderedFrame(files);
			this.setTimelineFrame(effectiveFrame);
		}
		effectiveFrame ??= this.app.timeline.frame;

		const sourceQueries = files.map((file) => ({
			file,
			query: Filter.Entity.scopeToSource(
				this.app,
				file,
				this.getQuery(file),
				effectiveFrame,
			),
		}));
		this.persistSourceQueries(sourceQueries);

		this.notes_reload();
		this.links_reload();
		this.highlights_reload();

		await this.cancelQueryStreamsForSources(
			files.map((file) => file.id),
			true,
		);
		this.removeQueuedRefetchesForSources(files.map((file) => file.id));

		sourceQueries.forEach(({ file, query }) => {
			this.events_reset_in_file(file);

			this.enqueueRefetchQuery({
				sourceId: file.id,
				operationId: file.operation_id,
				query,
				options: {
					id: file.id,
					operationId: file.operation_id,
					preview: false,
					eventStoreMode: "replace",
					refetchKeys: refetchKeys ? refetchKeys[file.id] : undefined,
					addToHistory,
					create_notes,
					notes_color,
					notes_tags,
					notes_glyph_id,
					name,
				},
				sequence: this.refetchQueueSequence++,
			});
		});

		this.pumpRefetchQueue();
	};

	private static _realtimeTimer: ReturnType<typeof setInterval> | null = null;

	setRealtime = (enabled: boolean, seconds: number) => {
		if (Info._realtimeTimer) {
			clearInterval(Info._realtimeTimer);
			Info._realtimeTimer = null;
		}

		this.setInfoByKey(enabled, "settings", "realtimeEnabled");
		this.setInfoByKey(seconds, "settings", "realtimeTimeoff");

		if (enabled && seconds >= 10) {
			Info._realtimeTimer = setInterval(() => {
				Info._latestInstance?.realtimePoll();
			}, seconds * 1000);
		}
	};

	realtimePoll = async () => {
		const files = Source.Entity.selected(this.app);
		if (files.length === 0) return;

		const now = Date.now();
		const nowNanos = Internal.Transformator.toNanos(now);
		let filesUpdated = false;
		const summaries = await Promise.all(
			files.map((file) => DataStore.Events.getSourceSummary(file.id)),
		);

		files.forEach((file, index) => {
			const summary = summaries[index];
			const fromNanos =
				summary && summary.eventCount > 0
					? Internal.Transformator.toNanos(summary.maxTimestamp) + 1n
					: file.nanotimestamp?.max ??
						Internal.Transformator.toNanos(file.timestamp.max);

			if (fromNanos >= nowNanos) return;

			// Extend source timestamp bounds so render engines can draw events beyond the original range
			if (file.timestamp.max < now) {
				file.timestamp.max = now;
				file.nanotimestamp.max = nowNanos;
				filesUpdated = true;
			}

			// Build a source_config-based query for real-time polling.
			// Uses the forward-shifted time range [fromNanos, nowNanos] to fetch
			// only events newer than the latest known event.
			const query: Query.Type = {
				string: "",
				source_config: {
					operation_id: file.operation_id,
					source_ids: [file.id],
					range: {
						min: fromNanos.toString(),
						max: nowNanos.toString(),
					},
				},
				filters: [],
			};

			this.query_file(query, {
				id: file.id,
				preview: false,
				eventStoreMode: "upsert",
				eventStreamKind: "realtime",
			});
		});

		if (filesUpdated) {
			this.setInfoByKey([...this.app.target.files], "target", "files");
		}

		this.realtimePollExtendFrame();
	};

	realtimePollExtendFrame = () => {
		const now = Date.now();
		if (this.app.timeline.frame.max < now) {
			this.setTimelineFrame({
				min: this.app.timeline.frame.min,
				max: now,
			});
		}
	};

	/**
	 * Starts a bulk enrichment request and displays API/websocket completion toasts.
	 *
	 * @param plugin - Enrichment plugin filename to execute.
	 * @param file - Source file whose documents should be enriched.
	 * @param range - Time range used to filter documents.
	 * @param custom_parameters - Plugin custom parameter values.
	 * @param isShowOnlyEnriched - Whether to clear the visible file events while enriched documents stream in.
	 * @param fields - Field values passed to the enrichment plugin.
	 * @returns True when the enrichment request is accepted by the API, false on request error.
	 */
	enrichment = async (
		plugin: string,
		file: Source.Type,
		range: MinMax,
		custom_parameters: Record<string, any>,
		isShowOnlyEnriched: boolean,
		fields: Record<string, string | null>,
	): Promise<boolean> => {
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const operationId = file.operation_id;
		const req_id = generateUUID<Request.Id>(Request.Prefix.ENRICHMENT);
		const registered = await this.beginEventDocumentStream({
			requestId: req_id,
			sourceIds: [file.id],
			operationId,
			kind: "enrichment",
			mode: isShowOnlyEnriched ? "replace" : "upsert",
			sourceSettingsById: this.getQuerySourceSettingsById([file.id]),
		});
		if (!registered) return false;

		if (isShowOnlyEnriched) {
			this.events_reset_in_file(file);
			this.setLoading(req_id, file.id);
		}
		let documentsComplete = false;
		let enrichmentComplete = false;
		let releaseSocketListeners = (): void => undefined;
		const releaseCompletedListeners = (): void => {
			if (documentsComplete && enrichmentComplete) {
				releaseSocketListeners();
			}
		};

		let documentListenerId: string | null = null;
		let unsubscribeStreamState = (): void => undefined;
		if (isEventStoreLodEnabled()) {
			unsubscribeStreamState =
				SmartSocket.Class.instance.subscribeDocumentStreamStates((change) => {
					if (change.descriptor.requestId !== req_id) return;
					if (
						change.state !== "completed" &&
						change.state !== "canceled" &&
						change.state !== "failed"
					) {
						return;
					}
					documentsComplete = true;
					if (change.state !== "completed") enrichmentComplete = true;
					if (this.isOperationContextCurrent(operationEpoch, operationId)) {
						this.delLoading(req_id);
					}
					releaseCompletedListeners();
				});
		} else {
			const bufferedEvents: Doc.Type[] = [];
			let lastFlushTime = 0;
			let flushChain: Promise<void> = Promise.resolve();
			const flushIntervalMs = 300;
			documentListenerId = SmartSocket.Class.instance.con(
				SmartSocket.Message.Type.DOCUMENTS_CHUNK,
				(message) =>
					message.req_id === req_id &&
					this.isOperationContextCurrent(operationEpoch, operationId),
				(message) => {
					if (!this.isOperationContextCurrent(operationEpoch, operationId)) return;
					bufferedEvents.push(
						...Doc.Entity.normalize(
							message.payload.docs ?? [],
							file.settings.field,
							file.settings.hash_function,
						),
					);
					if (
						(Date.now() - lastFlushTime >= flushIntervalMs ||
							message.payload.last) &&
						bufferedEvents.length > 0
					) {
						const toFlush = bufferedEvents.splice(0);
						lastFlushTime = Date.now();
						flushChain = flushChain.then(() =>
							this.events_add_async(toFlush, operationEpoch),
						);
					}
					if (message.payload.last) {
						void flushChain.then(() => {
							documentsComplete = true;
							this.delLoading(req_id);
							releaseCompletedListeners();
						});
					}
				},
			);
		}
		const enrichmentDoneListenerId = SmartSocket.Class.instance.conce(
			SmartSocket.Message.Type.ENRICH_DONE,
			(m) =>
				m.req_id === req_id &&
				this.isOperationContextCurrent(operationEpoch, operationId),
			(m) => {
				if (!this.isOperationContextCurrent(operationEpoch, operationId)) return;
				if (m.payload.obj.status !== "done") {
					const reason = m.payload.obj.data?.__error?.msg;
					toast.error(translate("enrichment.failed"), {
						description:
							typeof reason === "string"
								? translate("common.reason", { reason })
								: undefined,
						icon: <Icon name="Stop" />,
						richColors: true,
					});
				} else {
					toast.success(translate("info.enrichmentFinished"), {
						description: translate("info.totalProcessedDocuments", {
							count: m.payload.obj.data.total_hits ?? 0,
						}),
						icon: <Icon name="Check" />,
					});
				}
				enrichmentComplete = true;
				if (m.payload.obj.status !== "done") {
					void this.cancelEventDocumentStream(req_id);
				}
				releaseCompletedListeners();
			},
		);
		releaseSocketListeners = this.trackOperationSocketCleanup(() => {
			if (documentListenerId) {
				SmartSocket.Class.instance.coff(
					SmartSocket.Message.Type.DOCUMENTS_CHUNK,
					documentListenerId,
				);
			}
			unsubscribeStreamState();
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.ENRICH_DONE,
				enrichmentDoneListenerId,
			);
		});

		this.markEventDocumentStreamDispatched(req_id);
		const response = await api<undefined>("/enrich_documents", {
			method: "POST",
			query: {
				operation_id: file.operation_id,
				plugin,
				ws_id: this.app.general.ws_id,
				req_id,
			},
			raw: true,
			body: {
				flt: {
					source_ids: [file.id],
					time_range: [
						Internal.Transformator.toNanos(range.min).toString(),
						Internal.Transformator.toNanos(range.max).toString(),
					],
				},
				plugin_params: { custom_parameters },
				fields,
			},
			showErrorToast: false,
			toast: {
				onError: (errorResponse) =>
					toast.error(translate("enrichment.failed"), {
						description: translate("common.reason", {
							reason: errorResponse.data.__error.msg,
						}),
						icon: <Icon name="Stop" />,
						richColors: true,
					}),
			},
		});

		if (
			!response?.req_id ||
			response.req_id !== req_id ||
			!this.isOperationContextCurrent(operationEpoch, operationId)
		) {
			releaseSocketListeners();
			await this.cancelEventDocumentStream(req_id);
			return false;
		}

		return true;
	};

	/**
	 * Enriches a single document and displays API result toasts.
	 *
	 * @param plugin - Enrichment plugin filename to execute.
	 * @param event - Document event to enrich.
	 * @param custom_parameters - Plugin custom parameter values.
	 * @param fields - Field values passed to the enrichment plugin.
	 * @returns The enriched document when the API succeeds, otherwise undefined.
	 */
	enrich_single_id = (
		plugin: string,
		event: Doc.Type,
		custom_parameters: Record<string, any>,
		fields: Record<string, string | null>,
	): Promise<Doc.Type> | undefined =>
		api<Doc.Type>("/enrich_single_id", {
			method: "POST",
			query: {
				plugin,
				operation_id: Doc.Entity.operationId(this.app, event),
				ws_id: this.app.general.ws_id,
				doc_id: event._id,
			},
			body: {
				plugin_params: {
					custom_parameters,
				},
				fields,
			},
			showErrorToast: false,
			toast: {
				onSuccess: () =>
					toast.success(translate("doc.enriched"), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
				onError: (errorResponse) =>
					toast.error(translate("enrichment.failed"), {
						description: translate("common.reason", {
							reason: errorResponse.data.__error.msg,
						}),
						icon: <Icon name="Stop" />,
						richColors: true,
					}),
			},
		});

	download_storage_file = async (
		storage_id: string,
		operation_id: string,
	): Promise<void> => {
		const req_id = generateUUID<string>(Request.Prefix.QUERY);
		const query = new URLSearchParams({
			operation_id,
			storage_id,
			req_id,
		});

		const response = await fetch(
			`${Internal.Settings.server}/storage_get_file_by_id?${query}`,
			{
				method: "GET",
				headers: {
					token: Internal.Settings.token,
				},
			},
		);

		if (!response.ok) {
			toast.error(translate("source.downloadLogFailed"), { richColors: true });
			return;
		}

		const disposition = response.headers.get("Content-Disposition");
		let filename = `${storage_id}`;
		if (disposition) {
			const match = disposition.match(/filename="?(.+?)"?$/);
			if (match) {
				filename = match[1];
			}
		}

		const blob = await response.blob();
		const url = URL.createObjectURL(blob);
		const a = document.createElement("a");
		a.href = url;
		a.download = filename;
		document.body.appendChild(a);
		a.click();
		document.body.removeChild(a);
		URL.revokeObjectURL(url);
	};

	// Bridge Manager APIs
	list_bridges = (flt?: any, req_id?: string, options?: any) =>
		api<any>("/list_bridges", {
			method: "POST",
			query: { req_id },
			body: { flt },
			...options,
		});

	list_ingestion_tasks = (flt?: any, req_id?: string, options?: any) =>
		api<any>("/list_ingestion_tasks", {
			method: "POST",
			query: { req_id },
			body: flt,
			...options,
		});

	/**
	 * Registers current-operation ingestion completion listeners without duplicating websocket handlers.
	 * @param operationId Operation whose ingest websocket messages should be handled.
	 * @returns Nothing.
	 */
	private _register_ingest_source_done_listeners = (
		operationId: Operation.Id,
	): void => {
		const socket = SmartSocket.Class.instance;
		if (!socket) return;

		if (
			this._ingestSourceDoneOperationId === operationId &&
			this._ingestSourceDoneCleanupListenerId &&
			this._ingestStatsDoneListenerId &&
			this._ingestSourceDoneFailureListenerId
		) {
			return;
		}

		this._clear_ingest_source_done_listeners();
		this._ingestSourceDoneOperationId = operationId;

		this._ingestSourceDoneCleanupListenerId = socket.con(
			SmartSocket.Message.Type.INGEST_SOURCE_DONE,
			(m) => {
				const sourceId = this._ingest_source_done_source_id(m);
				return !!(
					sourceId &&
					requestStore.hasLoadingForFile(sourceId) &&
					this._is_ingest_message_for_operation(m, operationId)
				);
			},
			(m) => {
				this._cleanup_ingest_source_done_loading(m);
				this.sync().catch(() => {});
			},
		);

		this._ingestStatsDoneListenerId = socket.con(
			SmartSocket.Message.Type.STATS_UPDATE,
			(m) =>
				this._is_ingest_message_for_operation(m, operationId) &&
				m.payload?.obj?.req_type === "ingest" &&
				m.payload?.obj?.status === "done",
			(m) => {
				if (m.req_id) {
					this.delLoading(m.req_id);
				}
			},
		);

		this._ingestSourceDoneFailureListenerId = socket.con(
			SmartSocket.Message.Type.INGEST_SOURCE_DONE,
			(m) =>
				this._is_ingest_message_for_operation(m, operationId) &&
				m.payload?.status === "failed",
			this._handle_ingest_source_done_failure,
		);
	};

	/**
	 * Removes previously registered ingest completion listeners.
	 * @returns Nothing.
	 */
	private _clear_ingest_source_done_listeners = (): void => {
		const socket = SmartSocket.Class.instance;
		if (socket && this._ingestSourceDoneCleanupListenerId) {
			socket.coff(
				SmartSocket.Message.Type.INGEST_SOURCE_DONE,
				this._ingestSourceDoneCleanupListenerId,
			);
		}
		if (socket && this._ingestStatsDoneListenerId) {
			socket.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				this._ingestStatsDoneListenerId,
			);
		}
		if (socket && this._ingestSourceDoneFailureListenerId) {
			socket.coff(
				SmartSocket.Message.Type.INGEST_SOURCE_DONE,
				this._ingestSourceDoneFailureListenerId,
			);
		}

		this._ingestSourceDoneCleanupListenerId = null;
		this._ingestStatsDoneListenerId = null;
		this._ingestSourceDoneFailureListenerId = null;
		this._ingestSourceDoneOperationId = null;
	};

	/**
	 * Checks whether a websocket message belongs to the active operation.
	 * @param message Incoming websocket message.
	 * @param operationId Operation ID to match.
	 * @returns True when the message has no operation marker or matches the operation.
	 */
	private _is_ingest_message_for_operation = (
		message: SmartSocket.Message.Entity,
		operationId: Operation.Id,
	): boolean => {
		const messageOperationId = message["gulp.operation_id"];
		return !messageOperationId || messageOperationId === operationId;
	};

	/**
	 * Reads a source ID from an ingest_source_done websocket payload.
	 * @param message Incoming ingest completion message.
	 * @returns Source ID when present.
	 */
	private _ingest_source_done_source_id = (
		message: SmartSocket.Message.Entity,
	): Source.Id | null => {
		const sourceId =
			message.payload?.["gulp.source_id"] ??
			message.payload?.source_id ??
			message.payload?.gulp_source_id;

		return typeof sourceId === "string" && sourceId
			? (sourceId as Source.Id)
			: null;
	};

	/**
	 * Clears loading/progress state for a completed ingest source.
	 * @param message Incoming ingest completion message.
	 * @returns Nothing.
	 */
	private _cleanup_ingest_source_done_loading = (
		message: SmartSocket.Message.Entity,
	): void => {
		const sourceId = this._ingest_source_done_source_id(message);
		if (sourceId) {
			this.delLoadingByFile(sourceId);
			this.ingestionProgress.delete(sourceId);
			return;
		}

		if (message.req_id) {
			this.delLoading(message.req_id);
			this.ingestionProgress.delete(message.req_id as unknown as Source.Id);
		}
	};

	/**
	 * Handles failed ingestion completion by notifying the user and refreshing request/source state.
	 * @param message Incoming failed ingest completion message.
	 * @returns Nothing.
	 */
	private _handle_ingest_source_done_failure = (
		message: SmartSocket.Message.Entity,
	): void => {
		this._show_ingest_source_failure_toast(message);
		this._cleanup_ingest_source_done_loading(message);
		this.request_list()?.catch(() => {});
		this.sync().catch(() => {});
	};

	/**
	 * Shows a deduplicated failed-ingest toast with normalized record counters.
	 * @param message Incoming failed ingest completion message.
	 * @returns Nothing.
	 */
	private _show_ingest_source_failure_toast = (
		message: SmartSocket.Message.Entity,
	): void => {
		if (this._notifiedIngestFailures.has(message.req_id)) {
			return;
		}

		this._notifiedIngestFailures.add(message.req_id);
		const counts = Request.Entity.recordCounts(message.payload);

		toast.error(translate("source.ingestionFailedTitle"), {
			description: translate("requests.recordSummary", {
				ingested: counts.records_ingested,
				skipped: counts.records_skipped,
				failed: counts.records_failed,
			}),
			icon: <Icon name="Stop" />,
			richColors: true,
		});
	};

	/**
	 * Resynchronizes loading state for in-flight ingestion requests after navigation or reconnect.
	 * @param operationId Operation whose ingestion requests should be reconciled.
	 * @returns A promise that resolves when resync registration and initial reconciliation finish.
	 */
	resync_ingestion_state = async (
		operationId: Operation.Id,
	): Promise<void> => {
		this.stop_ingestion_resync_poll();
		const resyncSequence = this._resyncPollSequence;
		this._register_ingest_source_done_listeners(operationId);

		try {
			const requests = await this.request_list();
			if (resyncSequence !== this._resyncPollSequence) return;

			const recoveredLoadings = collectIngestionResyncLoadings(requests || []);
			const newlyRecoveredLoadings: IngestionResyncLoadings = new Map();
			recoveredLoadings.forEach((requestId, sourceId) => {
				if (!requestStore.hasLoadingForFile(sourceId)) {
					this.setLoading(requestId, sourceId);
					if (requestStore.getRequestIdByFile(sourceId) === requestId) {
						newlyRecoveredLoadings.set(sourceId, requestId);
					}
				}
			});

			this._resyncIngestionLoadings = newlyRecoveredLoadings;
			if (
				!hasActiveIngestionResyncLoading(
					this._resyncIngestionLoadings,
					requestStore.getLoadings(),
				)
			) {
				return;
			}

			// sync() fetches live source totals from OpenSearch — the same thing Reload does.
			this._resyncPollTimer = setInterval(async () => {
				if (
					!hasActiveIngestionResyncLoading(
						this._resyncIngestionLoadings,
						requestStore.getLoadings(),
					)
				) {
					this.stop_ingestion_resync_poll();
					return;
				}
				try {
					await this.sync();
				} catch {
					/* best-effort */
				}
			}, 5000);
		} catch (_) {
			// Best-effort — failure is non-fatal
		}
	};

	/**
	 * Stops source-total polling started by ingestion state recovery.
	 * @returns Nothing.
	 */
	stop_ingestion_resync_poll = (): void => {
		this._resyncPollSequence++;
		if (this._resyncPollTimer) {
			clearInterval(this._resyncPollTimer);
			this._resyncPollTimer = null;
		}
		this._resyncIngestionLoadings.clear();
	};

	create_start_ingestion = (
		bridge_id: string,
		operation_id: string,
		plugin_params: any,
		req_id?: string,
		options?: any,
	) =>
		api<any>("/create_start_ingestion", {
			method: "POST",
			query: { bridge_id, operation_id, req_id },
			body: plugin_params,
			...options,
		});

	stop_ingestion = (bridge_task_id: string, req_id?: string, options?: any) =>
		api<any>("/stop_ingestion", {
			method: "POST",
			query: { req_id },
			body: `"${bridge_task_id}"`,
			...options,
		});
	check_bridge_status = (bridge_id: string, req_id?: string, options?: any) =>
		api<any>("/get_bridge_status", {
			method: "GET",
			query: { req_id, bridge_id },
			...options,
		});

	delete_ingestion = (bridge_task_id: string, req_id?: string, options?: any) =>
		api<any>("/delete_ingestion", {
			method: "DELETE",
			query: { req_id, bridge_task_id },
			...options,
		});

	query_global = async ({
		filename,
		context,
		query,
		total,
		separately,
	}: {
		context?: string;
		filename?: string;
		query: Query.Type;
		total: number;
		separately?: boolean;
	}) => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}

		if (!filename || !context) {
			separately = true;
		}

		if (!separately) {
			this.query_file(query, {
				id: this.virtualize(filename!, total, context!),
				preview: false,
			});
		} else {
			this.query_file(query, {
				preview: false,
			});
		}
	};

	virtualize = (fileName: string, total: number, contextName: string) => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return null!;
		}

		const context_id = `temp-${contextName}` as Context.Id;

		const file: Source.Type = Source.Entity.virtualize(this.app, {
			name: fileName,
			context_id,
			operation_id: operation.id,
			total: 999999,
		});

		const context: Context.Type = {
			id: context_id,
			color: "",
			glyph_id: null as unknown as Glyph.Id,
			granted_user_group_ids: [],
			granted_user_ids: [],
			name: contextName,
			operation_id: operation.id,
			time_created: Date.now(),
			time_updated: Date.now(),
			type: "context",
			selected: true,
			owner_user_id: this.app.general.user?.id!,
		};

		this.setInfo((info) => ({
			...info,
			target: {
				...info.target,
				files: [...info.target.files, file],
				contexts: [...info.target.contexts, context],
			},
		}));

		return file.id;
	};

	/**
	 * Queries canonical documents by identifier and streams non-preview results safely.
	 * @param docIds Document identifiers requested from the selected operation.
	 * @param fields Optional source fields to include in the response.
	 * @param preview Whether the backend should return a preview response without ingestion.
	 * @returns Query response data, or an empty result when the backend returns no payload.
	 */
	query_gulp = async (
		docIds: Doc.Id[],
		fields: string[],
		preview: boolean,
	): Promise<QueryGulpResponse> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return {
				docs: [],
				total_hits: 0,
			};
		}
		const operationEpoch = DataStore.getRenderOperationEpoch();

		const body: QueryGulpRequestBody = {
			flt: {
				doc_ids: docIds,
			},
			q_options: {
				limit: 10000,
			},
		};

		if (fields.length > 0) {
			body.q_options.fields = fields;
		}

		if (preview) {
			body.q_options.preview_mode = true;
		}

		const requestId = generateUUID<Request.Id>(Request.Prefix.QUERY);
		const request_query: Record<string, string> = {
			ws_id: this.app.general.ws_id,
			operation_id: operation.id,
			req_id: requestId,
		};
		let releaseSocketListeners = (): void => undefined;
		if (!preview) {
			const registered = await this.beginEventDocumentStream({
				requestId,
				sourceIds: [],
				operationId: operation.id,
				kind: "query",
				mode: "upsert",
				sourceSettingsById: this.getQuerySourceSettingsById([]),
				defaultSourceSettings: {
					field: "gulp.event_code",
					hash_function: "fnv1a",
				},
			});
			if (!registered) return { docs: [], total_hits: 0 };

			let documentListenerId: string | null = null;
			let unsubscribeStreamState = (): void => undefined;
			if (isEventStoreLodEnabled()) {
				unsubscribeStreamState =
					SmartSocket.Class.instance.subscribeDocumentStreamStates((change) => {
						if (
							change.descriptor.requestId !== requestId ||
							(change.state !== "completed" &&
								change.state !== "canceled" &&
								change.state !== "failed")
						) {
							return;
						}
						this.delLoading(requestId);
						releaseSocketListeners();
					});
			} else {
				const bufferedEvents: Doc.Type[] = [];
				let lastFlushTime = 0;
				let flushChain: Promise<void> = Promise.resolve();
				documentListenerId = SmartSocket.Class.instance.con(
					SmartSocket.Message.Type.DOCUMENTS_CHUNK,
					(message) =>
						message.req_id === requestId &&
						this.isOperationContextCurrent(operationEpoch, operation.id),
					(message) => {
						bufferedEvents.push(
							...Doc.Entity.normalize(
								message.payload.docs ?? [],
								"gulp.event_code",
								"fnv1a",
							),
						);
						if (
							(Date.now() - lastFlushTime >= 300 || message.payload.last) &&
							bufferedEvents.length > 0
						) {
							const toFlush = bufferedEvents.splice(0);
							lastFlushTime = Date.now();
							flushChain = flushChain.then(() =>
								this.events_add_async(toFlush, operationEpoch),
							);
						}
						if (message.payload.last) {
							void flushChain.then(() => {
								this.delLoading(requestId);
								releaseSocketListeners();
							});
						}
					},
				);
			}
			releaseSocketListeners = this.trackOperationSocketCleanup(() => {
				if (documentListenerId) {
					SmartSocket.Class.instance.coff(
						SmartSocket.Message.Type.DOCUMENTS_CHUNK,
						documentListenerId,
					);
				}
				unsubscribeStreamState();
			});
		}

		if (!preview) this.markEventDocumentStreamDispatched(requestId);
		const resp = await api<QueryGulpResponse>("/query_gulp", {
			method: "POST",
			query: request_query,
			body,
			raw: true,
		});
		if (!preview && (!resp || resp.status !== "pending")) {
			releaseSocketListeners();
			await this.cancelEventDocumentStream(requestId);
		}

		if (preview) {
			if (!resp || (resp || {})?.data?.total_hits === 0) {
				toast.error(translate("filter.noResults"), {
					icon: <Icon name="FaceUnhappy" />,
					richColors: true,
				});
			} else {
				toast(translate("filter.totalHits", { count: resp.data?.total_hits }));
			}
		}

		return resp
			? resp.data
			: {
					docs: [],
					total_hits: 0,
				};
	};

	/**
	 * Executes a raw query and streams non-preview documents into DataStore.
	 * @param query Query definition converted to the backend raw query body.
	 * @param options Query options, including source id and optional queue priority.
	 * @returns Backend query response data, or an empty result on missing operation.
	 */
	query_file = async (query: Query.Type, options: QueryFileOptions) => {
		const {
			preview = false,
			id,
			refetchKeys,
			addToHistory,
			create_notes,
			notes_color,
			notes_tags,
			notes_glyph_id,
			name,
			operationId,
			queuePriority,
			eventStoreMode = "upsert",
			eventStreamKind = "query",
		} = options;
		const operation = operationId
			? Operation.Entity.id(this.app, operationId)
			: Operation.Entity.selected(this.app);
		if (!operation) {
			this.finishQueuedQuery(queuePriority);
			return {
				docs: [],
				total_hits: 0,
			};
		}
		const operationEpoch = DataStore.getRenderOperationEpoch();

		const sourceIds = id ? (Parser.array(id) as Source.Id[]) : [];

		const request_query: Record<string, string> = {
			ws_id: this.app.general.ws_id,
			operation_id: operation.id,
			req_id: generateUUID(Request.Prefix.QUERY),
		};
		const req_id = request_query.req_id as Request.Id;
		let stream: QueryStreamState | null = null;
		const abortController = preview ? undefined : new AbortController();

		try {
			if (sourceIds.length > 0) {
				await this.cancelQueryStreamsForSources(sourceIds, true);
			}

			const body = Filter.Entity.body(query);

			if (preview) {
				body.q_options.preview_mode = preview;
			}

			body.q_options.limit = 10000;

			if (sourceIds.length > 0) {
				const sourceFields = Array.from(
					new Set(
						sourceIds
							.map((sourceId) => Source.Entity.id(this.app, sourceId)?.settings.field)
							.filter((field): field is keyof Doc.Type => Boolean(field)),
					),
				);
				body.q_options.fields = refetchKeys ?? sourceFields;
			}

			if (addToHistory) {
				body.q_options.add_to_history = true;
			}
			if (create_notes && !preview) {
				body.q_options.create_notes = true;

				body.q_options.notes_color = notes_color ?? Default.Color.NOTE;
				body.q_options.notes_tags = notes_tags;
				body.q_options.notes_glyph_id = notes_glyph_id;
				body.q_options.name = name;
			}

			if (!this.isOperationContextCurrent(operationEpoch, operation.id)) {
				this.finishQueuedQuery(queuePriority);
				return {
					docs: [],
					total_hits: 0,
				};
			}

			if (!preview) {
				stream = this.registerQueryStream(
					req_id,
					sourceIds,
					operation.id,
					operationEpoch,
					abortController,
					queuePriority,
				);
				const registered = await this.beginEventDocumentStream({
					requestId: req_id,
					sourceIds,
					operationId: operation.id,
					kind: eventStreamKind,
					mode: eventStoreMode,
					sourceSettingsById: this.getQuerySourceSettingsById(sourceIds),
				});
				if (!registered) {
					await this.closeQueryStream(req_id, {
						managedStatus: "canceled",
					});
					return {
						docs: [],
						total_hits: 0,
					};
				}
			}

			if (sourceIds.length > 0) {
				sourceIds.forEach((sourceId) => this.setLoading(req_id, sourceId));
			}

			if (stream && !isEventStoreLodEnabled()) {
				const bufferedDocs: QueryRawDocument[] = [];
				let lastFlushTime = 0;
				const FLUSH_INTERVAL_MS = 300;

				const flushBufferedDocs = () => {
					if (
						!stream ||
						!this.isQueryStreamCurrent(stream) ||
						bufferedDocs.length === 0
					) {
						return;
					}
					const toFlush = bufferedDocs.splice(0);
					lastFlushTime = Date.now();
					const activeStream = stream;

					activeStream.flushChain = activeStream.flushChain.then(async () => {
						if (!this.isQueryStreamCurrent(activeStream)) {
							return;
						}

						await this.waitForVisibleQueueBeforeBackgroundBatch(activeStream);
						if (!this.isQueryStreamCurrent(activeStream)) {
							return;
						}

						const batches = await this.prepareQueryDocBatches(
							toFlush,
							activeStream.sourceIds,
						);
						if (!this.isQueryStreamCurrent(activeStream)) {
							return;
						}

						await this.events_add_prepared_async(
							batches,
							activeStream.operationEpoch,
						);
					});
				};

				stream.documentListenerId = SmartSocket.Class.instance.con(
					SmartSocket.Message.Type.DOCUMENTS_CHUNK,
					(m) =>
						m.req_id === req_id &&
						Boolean(stream && this.isQueryStreamCurrent(stream)),
					(m) => {
						if (!stream || !this.isQueryStreamCurrent(stream)) return;

						bufferedDocs.push(
							...((m.payload.docs ?? []) as unknown as QueryRawDocument[]),
						);

						if (
							Date.now() - lastFlushTime >= FLUSH_INTERVAL_MS ||
							m.payload.last
						) {
							flushBufferedDocs();
						}

						if (m.payload.last) {
							const finalStream = stream;
							finalStream.flushChain.then(async () => {
								if (!this.isQueryStreamCurrent(finalStream)) {
									return;
								}

								this.reconcileQuerySourceTotals(finalStream.sourceIds);
								await this.closeQueryStream(req_id);
								this.render();
							});
						}
					},
				);

			}

			if (stream) {
				stream.statsListenerId = SmartSocket.Class.instance.conce(
					SmartSocket.Message.Type.STATS_UPDATE,
					(m) =>
						m.req_id === req_id &&
						Boolean(stream && this.isQueryStreamCurrent(stream)),
					(m) => {
						if (stream) {
							stream.completedByStats = true;
							if (m.payload.obj?.status === Request.Status.DONE) {
								requestStore.completeManagedQuery(req_id);
							} else if (
								m.payload.obj?.status === Request.Status.FAILED ||
								m.payload.obj?.status === Request.Status.CANCELED
							) {
								requestStore.cancelManagedQuery(req_id);
							}
						}
					},
				);
			}

			if (stream && create_notes) {
				stream.collabListenerId = SmartSocket.Class.instance.con(
					SmartSocket.Message.Type.COLLAB_CREATE,
					(m) =>
						m.req_id === req_id &&
						Boolean(stream && this.isQueryStreamCurrent(stream)),
					(m) => {
						if (!stream || !this.isQueryStreamCurrent(stream)) return;
						if (Array.isArray(m.payload.obj) && m.payload.obj.length > 0) {
							const newItems = m.payload.obj.filter(
								(item: any) => item.type === "note",
							) as Note.Type[];
							if (newItems.length > 0) {
								this.AddNotesToDataStore(newItems);
								toast.success(
									translate("notes.fetchedCount", { count: newItems.length }),
									{
										richColors: true,
									},
								);
							}
						}
					},
				);
			}

			if (stream) this.markEventDocumentStreamDispatched(req_id);
			const resp = await api<any>("/query_raw", {
				method: "POST",
				query: request_query,
				body,
				raw: true,
				signal: abortController?.signal,
			});

			if (stream && (!resp || resp.status !== "pending")) {
				await this.closeQueryStream(req_id, {
					managedStatus: "canceled",
				});
			}

			if (preview) {
				if (!resp || (resp || {})?.data?.total_hits === 0) {
					toast.error(translate("filter.noResults"), {
						icon: <Icon name="FaceUnhappy" />,
						richColors: true,
					});
				} else {
					toast(translate("filter.totalHits", { count: resp.data?.total_hits }));
				}
			}

			return resp
				? resp.data
				: {
						docs: [],
						total_hits: 0,
					};
		} catch (error) {
			if (stream) {
				await this.closeQueryStream(req_id, {
					managedStatus: "canceled",
				});
			} else {
				this.finishQueuedQuery(queuePriority);
			}
			throw error;
		}
	};

	/**
	 * Fetches a paginated raw query result for table-style detached views.
	 * @param query Query body assembled from synced or local filters.
	 * @param options Pagination, sorting, and cancellation options.
	 * @returns Paginated documents and total hit count.
	 */
	query_paginate = async (
		query: Query.Type,
		{
			limit = 100,
			offset = 0,
			sort = { "@timestamp": "asc" },
			signal,
		}: GulpDataset.QueryGulp.Options,
	) => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}
		const body: Record<string, any> = {
			q: { query: Filter.Entity.query(query) },
			q_options: {
				limit: limit,
				offset: offset,
				sort,
			},
		};

		const request_query: Record<string, string> = {
			operation_id: operation.id,
			req_id: generateUUID(Request.Prefix.QUERY),
		};

		const resp = await api<any>("/query_raw_paginate", {
			method: "POST",
			body,
			query: request_query,
			raw: true,
			signal,
		});

		return resp
			? resp.data
			: {
					docs: [],
					total_hits: 0,
				};
	};

	/**
	 * Executes an OpenSearch aggregation query for the selected operation context.
	 *
	 * @param operationId - Operation identifier used by the backend to select the target index.
	 * @param body - OpenSearch query and aggregation body to execute.
	 * @param options - Optional cancellation settings.
	 * @returns Aggregation response containing total hits and named aggregation buckets.
	 */
	query_aggregation = async (
		operationId: Operation.Id,
		body: GulpDataset.QueryAggregation.Body,
		options: { signal?: AbortSignal } = {},
	) => {
		const resp = await api<GulpDataset.QueryAggregation.Response>(
			"/query_aggregation",
			{
				method: "POST",
				body,
				query: {
					operation_id: operationId,
					req_id: generateUUID(Request.Prefix.QUERY),
				},
				raw: true,
				signal: options.signal,
			},
		);

		return resp
			? resp.data
			: {
					total_hits: 0,
					aggregations: {},
				};
	};

	/**
	 * Retrieves shared objects matching a type filter from the backend.
	 *
	 * @param params - Shared object type and object subtype filters.
	 * @param options - Request display behavior for failures.
	 * @returns Matching shared objects or an empty list when the request yields no data.
	 */
	shared_object_list = async <TObject extends Record<string, unknown>>(
		params: GulpDataset.SharedObject.ListParams,
		options: { showErrorToast?: boolean } = {},
	): Promise<GulpDataset.SharedObject.Type<TObject>[]> => {
		const response = await api<GulpDataset.SharedObject.ListResponse<TObject>>(
			"/shared_object_list",
			{
				method: "POST",
				showErrorToast: options.showErrorToast,
				body: {
					type: params.type,
					obj_type: params.obj_type,
				},
			},
		);

		if (Array.isArray(response)) {
			return response;
		}

		return response?.data ?? [];
	};

	/**
	 * Retrieves one shared object by its backend object identifier.
	 *
	 * @param objId - Shared object identifier requested from the backend.
	 * @returns The matching shared object payload.
	 */
	shared_object_get_by_id = async <
		TObject extends Record<string, unknown> = Record<string, unknown>,
	>(
		objId: string,
	): Promise<GulpDataset.SharedObject.Type<TObject>> => {
		return api<GulpDataset.SharedObject.Type<TObject>>(
			"/shared_object_get_by_id",
			{
				method: "GET",
				query: { obj_id: objId },
			},
		);
	};

	/**
	 * Updates metadata and/or JSON payload stored inside a shared object.
	 *
	 * @param objId - Shared object identifier to update.
	 * @param payload - Shared object fields to update.
	 * @returns True when the backend confirms the update request.
	 */
	shared_object_update = async (
		objId: string,
		payload: GulpDataset.SharedObject.UpdatePayload,
	): Promise<boolean> => {
		const body: Record<string, unknown> = {};
		if (payload.obj !== undefined) {
			body.obj = payload.obj;
		}
		if (payload.description !== undefined) {
			body.description = payload.description;
		}
		if (payload.tags !== undefined) {
			body.tags = payload.tags;
		}

		const response = await api<undefined>("/shared_object_update", {
			method: "PATCH",
			query: {
				obj_id: objId,
				...(payload.name !== undefined ? { name: payload.name } : {}),
				...(payload.glyph_id !== undefined
					? { glyph_id: payload.glyph_id ?? "" }
					: {}),
			},
			body,
			raw: true,
		});

		return response?.status === "success";
	};

	/**
	 * Deletes a shared object from the active websocket workspace.
	 *
	 * @param objId - Shared object identifier to delete.
	 * @returns True when the backend confirms the deletion request.
	 */
	shared_object_delete = async (objId: string): Promise<boolean> => {
		const response = await api<undefined>("/shared_object_delete", {
			method: "DELETE",
			query: {
				obj_id: objId,
				ws_id: this.app.general.ws_id,
			},
			raw: true,
			showErrorToast: false,
		});

		return response?.status === "success";
	};

	/**
	 * Retrieves the list of previously applied queries from the server history.
	 *
	 * Reconstructs full Query.Type objects from the raw OpenSearch bool queries
	 * stored in history. Extracts source_config from term/terms/range clauses,
	 * text_filter from wildcard on event.original, and individual filter conditions.
	 * Generates a human-readable label via Filter.Entity.describe().
	 *
	 * @returns A promise resolving to an array of reconstructed Query.Type objects.
	 */
	getLastQueries = (): Promise<Query.Type[]> =>
		api<GulpDataset.QueryHistoryGet.Response>("/query_history_get").then(
			(list) => {
				const queries: Query.Type[] = [];

				/**
				 * Recursively walks an OpenSearch bool body `{ must: [...], should: [...], ... }`
				 * and returns the items belonging to it. Nested `bool` clauses become
				 * `Filter.Group` items; everything else becomes a leaf `Filter.Type`.
				 *
				 * `infra` is mutated to capture source_config / text_filter clauses found at the
				 * top level — those are extracted into `Query.Type` instead of becoming filters.
				 */
				const walkBool = (
					boolBody: Record<string, any[]>,
					infra: {
						topLevel: boolean;
						operationId: string;
						sourceIds: string[];
						rangeMin: string | number;
						rangeMax: string | number;
						textFilter: string;
					},
				): Filter.Item[] => {
					const items: Filter.Item[] = [];

					Object.entries(boolBody).forEach(([bucket, arr]) => {
						const operator = bucket as Filter.BoolOperator;
						if (!Array.isArray(arr)) return;

						arr.forEach((obj) => {
							Object.entries(obj).forEach(([type, v]: [string, any]) => {
								// Nested bool → Filter.Group (recurse)
								if (type === "bool" && v && typeof v === "object") {
									const groupChildren = walkBool(v, {
										...infra,
										topLevel: false,
									});
									items.push({
										id: generateUUID() as Filter.Id,
										type: "group",
										operator,
										children: groupChildren,
										enabled: true,
									});
									return;
								}

								// Skip legacy query_string — not reconstructable as a builder row
								if (type === "query_string") return;

								// Top-level infrastructure clauses → source_config / text_filter
								if (infra.topLevel) {
									if (type === "term" && v["gulp.operation_id"] !== undefined) {
										infra.operationId = v["gulp.operation_id"];
										return;
									}
									if (type === "terms" && v["gulp.source_id"] !== undefined) {
										infra.sourceIds = v["gulp.source_id"];
										return;
									}
									if (type === "range" && v["gulp.timestamp"] !== undefined) {
										infra.rangeMin = v["gulp.timestamp"].gte ?? "";
										infra.rangeMax = v["gulp.timestamp"].lte ?? "";
										return;
									}
									if (
										type === "wildcard" &&
										v["event.original"] !== undefined
									) {
										infra.textFilter = v["event.original"].value ?? "";
										return;
									}
								}

								// Leaf condition: clause shape is `{ [fieldName]: value | { value, ... } }`.
								// In practice there is exactly one field key per clause object.
								Object.keys(v).forEach((fieldKey) => {
									items.push({
										operator,
										type: type as Filter.Condition,
										id: generateUUID(),
										field: fieldKey,
										value:
											typeof v[fieldKey] === "object"
												? v[fieldKey].value
												: v[fieldKey],
										enabled: true,
									});
								});
							});
						});
					});

					return items;
				};

				list.forEach((payload) => {
					const root = payload.q.query;
					if (!root || !root.bool) return;

					const infra = {
						topLevel: true,
						operationId: "",
						sourceIds: [] as string[],
						rangeMin: "" as string | number,
						rangeMax: "" as string | number,
						textFilter: "",
					};

					const filters = walkBool(root.bool, infra);

					const source_config: Query.Type["source_config"] | undefined =
						infra.operationId && infra.sourceIds.length > 0
							? {
									operation_id: infra.operationId,
									source_ids: infra.sourceIds,
									range: { min: infra.rangeMin, max: infra.rangeMax },
								}
							: undefined;

					const queryObj: Query.Type = {
						string: "",
						text_filter: infra.textFilter,
						filters,
						source_config,
					};

					queryObj.string = Filter.Entity.describe(queryObj, this.app);
					queries.push(queryObj);
				});

				return queries;
			},
		);

	preview_file = (file: Source.Type, query = this.getQuery(file)) =>
		this.query_file(query, { preview: true, operationId: file.operation_id });

	preview_query = (query: Query.Type) =>
		this.query_file(query, { preview: true });

	/**
	 * Inserts or updates a request and applies active ingestion statistics to its sources.
	 * @param req Request stats object received from websocket or API.
	 * @returns Nothing.
	 */
	request_add = (req: Request.Type): void => {
		const activeIngestionSourceLinks =
			isIngestionRequest(req) && ACTIVE_REQUEST_STATUSES.has(req.status)
				? Request.Entity.sourceLinks(req)
				: [];

		// Request statistics are aggregate values. They are source-exact only
		// when the ingestion request contains one source. Apply progress before
		// publishing the request-store update so React sees one atomic change.
		if (activeIngestionSourceLinks.length === 1) {
			const typedSourceId = activeIngestionSourceLinks[0].sourceId as Source.Id;
			const progress = Request.Entity.recordCounts(req).records_ingested;
			this.ingestionProgress.set(
				typedSourceId,
				Math.max(this.ingestionProgress.get(typedSourceId) ?? 0, progress),
			);
		}

		requestStore.upsertRequest(req);
		this.app.general.requests = requestStore.getRequests();

		if (activeIngestionSourceLinks.length > 0) {
			activeIngestionSourceLinks.forEach(({ sourceId }) => {
				const typedSourceId = sourceId as Source.Id;
				if (requestStore.getRequestIdByFile(typedSourceId) !== req.id) {
					this.setLoading(req.id, typedSourceId);
				}
			});
		}

		if (ACTIVE_REQUEST_STATUSES.has(req.status)) return;

		if (req.status === Request.Status.DONE) {
			requestStore.completeManagedQuery(req.id);
		} else if (
			req.status === Request.Status.FAILED ||
			req.status === Request.Status.CANCELED
		) {
			requestStore.cancelManagedQuery(req.id);
		}

		const queryStream = this.activeQueryStreams.get(req.id);
		if (!queryStream) {
			this.delLoading(req.id);
			return;
		}

		queryStream.completedByStats = true;
		if (
			req.status === Request.Status.FAILED ||
			req.status === Request.Status.CANCELED
		) {
			this.closeQueryStream(req.id, { managedStatus: "canceled" });
		}
	};

	/**
	 * Loads backend requests for the selected operation into the request store.
	 * @returns Promise with the request list, or undefined when no operation is selected.
	 */
	request_list = () => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}

		return api<Request.Type[]>(
			"/request_list",
			{
				method: "GET",
				query: {
					operation_id: operation.id,
				},
			},
			(requests) => {
				requestStore.setRequests(requests);
				this.app.general.requests = requestStore.getRequests();
			},
		);
	};

	request_cancel = (req_id_to_cancel: Request.Id) =>
		api("/request_cancel", {
			method: "PATCH",
			query: { req_id_to_cancel },
		});

	/**
	 * Rebases one source by its current UI offset, then clears that offset when the
	 * backend confirms completion.
	 * @param source Source to rebase.
	 * @returns Promise resolving to true when the backend accepted the request.
	 */
	rebase_by_offset = async (source: Source.Type): Promise<boolean> => {
		const offsetMsec = source.settings.offset;
		if (!Number.isFinite(offsetMsec) || offsetMsec === 0) {
			toast.info(translate("source.rebaseNoOffset"), {
				icon: <Icon name="Info" />,
			});
			return false;
		}

		if (!Operation.Entity.id(this.app, source.operation_id)) return false;

		const operationEpoch = DataStore.getRenderOperationEpoch();
		const req_id = generateUUID<Request.Id>(Request.Prefix.REBASE);
		const toastId = `rebase-${req_id}`;
		let completed = false;
		let releaseSocketListeners = (): void => undefined;

		this.setLoading(req_id, source.id);
		toast.loading(translate("source.rebaseStarted", { name: source.name }), {
			id: toastId,
			icon: <Icon name="RefreshCw" />,
		});

		const finish = async (request: Request.Type | undefined): Promise<void> => {
			if (completed) return;
			completed = true;
			releaseSocketListeners();
			this.delLoading(req_id);

			const status = request?.status;
			const errorMessages = request ? Request.Entity.errorMessages(request) : [];

			if (status === Request.Status.CANCELED) {
				toast.warning(
					translate("source.rebaseCanceled", { name: source.name }),
					{
						id: toastId,
						icon: <Icon name="StopCircle" />,
					},
				);
				return;
			}

			if (!request || status !== Request.Status.DONE) {
				toast.error(translate("source.rebaseFailed", { name: source.name }), {
					id: toastId,
					description: errorMessages[0],
					icon: <Icon name="Stop" />,
					richColors: true,
				});
				return;
			}

			this.events_reset_in_file(source);
			this.applyCompletedSourceRebase(source.id, offsetMsec);
			await this.clearSourceOffsetInSessions(
				source.id,
				this.offsetDurationToNanos(offsetMsec),
			).catch((error) => {
				Logger.warn(
					`Failed to persist cleared source offset for ${source.id}: ${
						(error as Error).message
					}`,
					Info,
				);
			});

			toast.success(translate("source.rebaseFinished", { name: source.name }), {
				id: toastId,
				icon: <Icon name="Check" />,
			});

			this.refetch({ ids: source.id });
		};

		const terminalStatsListenerId = SmartSocket.Class.instance.con(
			SmartSocket.Message.Type.STATS_UPDATE,
			(m) =>
				m.req_id === req_id &&
				m.payload?.obj?.type === "request_stats" &&
				!ACTIVE_REQUEST_STATUSES.has(m.payload.obj.status) &&
				this.isOperationContextCurrent(operationEpoch, source.operation_id),
			(m) => {
				void finish(m.payload.obj);
			},
		);
		const rebaseDoneListenerId = SmartSocket.Class.instance.conce(
			SmartSocket.Message.Type.REBASE_DONE,
			(m) =>
				m.req_id === req_id &&
				this.isOperationContextCurrent(operationEpoch, source.operation_id),
			(m) => {
				void finish(m.payload.obj);
			},
		);
		releaseSocketListeners = this.trackOperationSocketCleanup(() => {
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				terminalStatsListenerId,
			);
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.REBASE_DONE,
				rebaseDoneListenerId,
			);
		});

		const response = await api<unknown>("/opensearch_rebase_by_query", {
			method: "POST",
			query: {
				operation_id: source.operation_id,
				ws_id: this.app.general.ws_id,
				offset_msec: offsetMsec,
				req_id,
			},
			body: {
				flt: {
					source_ids: [source.id],
				},
				fields: [],
			},
			raw: true,
			showErrorToast: false,
			toast: {
				onError: (errorResponse) =>
					toast.error(
						translate("source.rebaseFailed", { name: source.name }),
						{
							id: toastId,
							description: errorResponse.data.__error.msg,
							icon: <Icon name="Stop" />,
							richColors: true,
						},
					),
			},
		});

		if (!response || response.status !== "pending") {
			releaseSocketListeners();
			this.delLoading(req_id);
			toast.error(translate("source.rebaseFailed", { name: source.name }), {
				id: toastId,
				icon: <Icon name="Stop" />,
				richColors: true,
			});
			return false;
		}

		return true;
	};

	filters_cache = (files: Array<Source.Type | Source.Id>) => {
		files.forEach((file) => {
			const id = Parser.useUUID(file) as Source.Id;

			Logger.log(
				`Caching has been requested for files ${Source.Entity.id(this.app, file).name}`,
				Info,
			);

			// Keep only an undo marker. Exact events remain in IndexedDB and the
			// previous query is reconstructed from the cached filter definition.
			this.app.timeline.cache.data.set(id, []);
			this.app.timeline.cache.filters[id] = this.app.target.filters[id];
		});

		this.setInfoByKey(this.app.timeline.cache, "timeline", "cache");
		this.render();
	};

	/**
	 * Restores cached filters and refetches their exact source documents.
	 * @param files Sources whose cached filter result should be restored.
	 * @returns Nothing.
	 */
	filters_undo = (files: Array<Source.Type | Source.Id>): void => {
		const sourceIds: Source.Id[] = [];
		files.forEach((file) => {
			const id = Parser.useUUID(file) as Source.Id;
			sourceIds.push(id);

			this.app.target.filters = {
				...this.app.target.filters,
				[id]: this.app.timeline.cache.filters[id],
			};

			RenderEngine.resetSource(id);

			this.filters_delete_cache(file);
		});

		this.setInfoByKey(this.app.target.filters, "target", "filters");
		if (sourceIds.length > 0) {
			void this.refetch({ ids: sourceIds });
		}
	};

	filters_delete_cache = (file: Source.Type | Source.Id) => {
		const id = Parser.useUUID(file) as Source.Id;

		this.app.timeline.cache.data.delete(id);

		this.setInfoByKey(
			{
				data: this.app.timeline.cache.data,
				filters: { ...this.app.timeline.cache.filters, [id]: undefined },
			},
			"timeline",
			"cache",
		);
	};

	render = () => {
		Logger.log(`Render requested`, Info);
		this.setInfoByKey(
			this.app.timeline.renderVersion + 1,
			"timeline",
			"renderVersion",
		);
	};

	private _mappingFileListPromise: Promise<Mapping.Type.Plugin[]> | null = null;
	mapping_file_list = async (): Promise<Mapping.Type.Plugin[]> => {
		if (this._mappingFileListPromise) return this._mappingFileListPromise;

		this._mappingFileListPromise = (async () => {
			const shit = await api<Mapping.Raw[]>("/mapping_file_list");

			const parsed_shit = Mapping.Entity.parse(shit);

			const another_parsed_shit = await this.plugin_list().then((p) =>
				p.filter((p) => p.type.includes("ingestion")),
			);

			another_parsed_shit.forEach((shit) => {
				const found_shit = parsed_shit.find((ps) => ps.name === shit.filename);
				if (found_shit) {
					return;
				} else {
					parsed_shit.push({
						name: shit.filename,
						methods: [],
					});
				}
			});

			const sorted_parsed_shit = parsed_shit.sort((a, b) =>
				a.name.localeCompare(b.name),
			);

			this.setInfoByKey(sorted_parsed_shit, "target", "mappings");

			return sorted_parsed_shit;
		})();

		const result = await this._mappingFileListPromise;
		this._mappingFileListPromise = null;
		return result;
	};

	/**
	 * Uploads or updates a mapping JSON file on the backend.
	 *
	 * @param payload - Mapping file JSON body containing metadata.plugin and mappings.
	 * @param failIfExists - Whether the backend should reject an existing mapping file.
	 * @returns A promise that resolves after the backend upload and mapping list refresh.
	 */
	mapping_file_upload = async (
		payload: GulpDataset.MappingFileUpload.Payload,
		failIfExists = false,
	): Promise<void> => {
		const pluginName = payload.metadata.plugin[0] || "mapping";
		const mappingId = Object.keys(payload.mappings)[0] || "custom";
		const fileName = `${pluginName.replace(/\.[^.]+$/, "")}_${mappingId}.json`;
		const formData = new FormData();

		formData.append(
			"file",
			new Blob([JSON.stringify(payload, null, 2)], {
				type: "application/json",
			}),
			fileName,
		);

		const response = await api("/mapping_file_upload", {
			method: "POST",
			query: {
				fail_if_exists: failIfExists,
			},
			body: formData,
			deassign: true,
			raw: true,
		});

		if (!response || response.status === "error") {
			throw new Error("Mapping file upload failed");
		}

		await this.mapping_file_list();
	};

	/**
	 * Switches to a different operation and performs comprehensive memory cleanup.
	 *
	 * MEMORY MANAGEMENT: Clears all caches (render engines, notes, doc index, timeline,
	 * canvas icons) and resets event data before selecting the new operation.
	 * This prevents memory leaks when switching between operations with large datasets
	 * (e.g., 320k events = ~150MB of cached pixel maps, note groups, and doc references).
	 *
	 * PERFORMANCE: Uses batchUpdate to consolidate all state changes into a single
	 * React setState call, preventing 8+ intermediate re-renders.
	 * @param id Operation identifier to select.
	 * @returns Nothing.
	 */
	operations_select = (id: Operation.Id): void => {
		// External cache cleanup (not part of React state)
		this.invalidateOperationStreams();
		this.notesReloadSequence++;
		this.linksReloadSequence++;
		this.highlightsReloadSequence++;
		RenderEngine.clearAllCaches();
		Note.Entity.invalidateCache();
		Doc.Entity.clearIndex();
		DataStore.resetRenderOperation(id);
		if (isEventStoreLodEnabled()) {
			DataStore.openEventStoreNamespace({
				coordinatorId: WindowBridge.getOrCreateCoordinatorId(),
				operationId: id,
			});
		}

		// Reset viewport to default position (like first render)
		scrollStore.setScrollX(0);
		scrollStore.setScrollY(-26);
		this.synchronizeTimelineScaleFromState(1);

		// Single batched state update instead of 8 separate setInfoByKey calls
		this.batchUpdate((draft) => {
			// Timeline reset
			draft.timeline.scale = 1;
			draft.timeline.target = null;
			draft.timeline.cache.data.clear();
			draft.timeline.cache.filters = {};

			// Clear event data
			draft.target.events.clear();
			draft.target.events = new Map(draft.target.events);

			// Clear operation-scoped overlays.
			draft.target.notes = [];
			draft.target.links = [];
			draft.target.highlights = [];

			// Select operation, deselect contexts and files
			draft.target.operations = Operation.Entity.select(draft, id);
			draft.target.contexts = draft.target.contexts.map((context) => ({
				...context,
				selected: false,
			}));
			draft.target.files = draft.target.files.map((file) => ({
				...file,
				selected: false,
			}));
		});
	};

	operations_set = (operations: Operation.Type[]) =>
		this.setInfoByKey(
			Operation.Entity.reload(operations, this.app),
			"target",
			"operations",
		);

	/**
	 * Deletes one or more operations sequentially, shows a consolidated toast summary,
	 * and syncs the application state exactly once at the end.
	 *
	 * @param operations - A single operation object or an array of operation objects to delete.
	 * @param setLoading - State setter to control the loading indicator.
	 * @returns A promise that resolves to an array of successfully deleted operation IDs.
	 */
	deleteOperation = async (
		operations: Operation.Type | Operation.Type[],
		setLoading: SetState<boolean>,
	): Promise<Operation.Id[]> => {
		const list = Array.isArray(operations) ? operations : [operations];
		if (list.length === 0) return [];

		setLoading(true);
		const succeeded: Operation.Type[] = [];
		const failed: Operation.Type[] = [];

		for (const op of list) {
			const res = await api<any>("/operation_delete", {
				method: "DELETE",
				query: {
					operation_id: op.id,
				},
			});

			if (res !== undefined) {
				succeeded.push(op);
			} else {
				failed.push(op);
			}
		}

		setLoading(false);

		// Show single toast based on the consolidated outcomes
		if (succeeded.length > 0 && failed.length === 0) {
			if (succeeded.length === 1) {
				toast.success(
					translate("operation.deleted", { name: succeeded[0].name }),
					{
						icon: <Icon name="Check" />,
						richColors: true,
					},
				);
			} else {
				toast.success(
					translate("operation.deletedCount", { count: succeeded.length }),
					{
						icon: <Icon name="Check" />,
						richColors: true,
					},
				);
			}
		} else if (succeeded.length > 0 && failed.length > 0) {
			toast.warning(
				translate("operation.deletePartialFailed", {
					succeeded: succeeded.length,
					failed: failed.length,
				}),
				{
					description: translate("operation.deleteFailedList", {
						names: failed.map((f) => f.name).join(", "),
					}),
					icon: <Icon name="Warning" />,
					richColors: true,
				},
			);
		} else if (failed.length > 0) {
			if (failed.length === 1) {
				toast.error(
					translate("operation.deleteFailed", { name: failed[0].name }),
					{
						icon: <Icon name="Stop" />,
						richColors: true,
					},
				);
			} else {
				toast.error(
					translate("operation.deleteFailedCount", { count: failed.length }),
					{
						icon: <Icon name="Stop" />,
						richColors: true,
					},
				);
			}
		}

		// Perform state sync exactly once at the end of the batch
		await this.sync();

		return succeeded.map((op) => op.id);
	};

	/**
	 * Fetches detailed information about a specific operation by its ID.
	 *
	 * @param operation_id - The unique identifier of the operation to fetch.
	 * @returns A promise that resolves to the detailed operation information.
	 */
	operation_get_by_id = (
		operation_id: string,
	): Promise<GulpDataset.OperationGetById.Response> => {
		return api<GulpDataset.OperationGetById.Response>("/operation_get_by_id", {
			method: "GET",
			query: { operation_id },
		});
	};

	fetch_gulp_parameters = async () => {
		const res = await fetch("http://localhost:8080/openapi.json");
		const json = await res.json();
		const pluginParamsSchema = json.components?.schemas?.GulpPluginParameters;
		const example = pluginParamsSchema?.examples?.[0];
		return example;
	};

	file_delete = (source: Source.Type) => {
		return api(
			"/source_delete",
			{
				method: "DELETE",
				query: {
					source_id: source.id,
					ws_id: this.app.general.ws_id,
				},
				showErrorToast: false,
				toast: {
					onSuccess: () =>
						toast.success(translate("source.deleted", { name: source.name }), {
							icon: <Icon name="Check" />,
							richColors: true,
						}),
					onError: () =>
						toast.error(
							translate("source.deleteFailed", { name: source.name }),
							{
								icon: <Icon name="Stop" />,
								richColors: true,
							},
						),
				},
			},
			this.sync,
		);
	};

	/**
	 * Orchestrates ingestion lifecycle and shared worker callbacks.
	 * [Tech-Note] Centralizes upload state and error handling.
	 * @param id Ingestion request identifier.
	 * @param file Uploaded file tracked by the progress banner.
	 * @param context Context name associated with the upload.
	 * @param onProgress Optional upload progress callback.
	 * @param onError Optional terminal cleanup invoked when the upload worker fails.
	 * @returns Upload worker callbacks.
	 */
	private _ingest_orchestrator = (
		id: Request.Id,
		file: File,
		context: string,
		onProgress?: (num: number) => void,
		onError?: () => void,
	) => {
		this.activeUploads.set(id, { filename: file.name, percent: 0 });

		// SmartSocket.Class.instance.conce(
		// 	SmartSocket.Message.Type.COLLAB_CREATE,
		// 	(m) =>
		// 		m.payload.obj.type === "context" &&
		// 		(m.req_id === id || m.payload.obj.name === context),
		// 	(m) => console.log(m.payload, "Context verification complete", id),
		// );

		let isCompletedOrError = false;

		return {
			onProgress: (progress: number) => {
				if (isCompletedOrError) return;
				this.activeUploads.set(id, { filename: file.name, percent: progress });
				if (progress % 5 === 0 || progress === 100) this.render();
				if (onProgress) onProgress(progress);
			},
			onDone: () => {
				this.activeUploads.delete(id);
				this.render();
			},
			onError: (err: string) => {
				isCompletedOrError = true;
				onError?.();
				toast.error(
					translate("source.ingestionFailed", { name: file.name, error: err }),
				);
				this.activeUploads.delete(id);
				this.delLoading(id);
				this.render();
			},
		};
	};

	/**
	 * Registers listeners for incoming source entities generated by the server.
	 * Supports asynchronous source registration during ingest.
	 * @param id Ingestion request identifier.
	 * @param operationId Operation that owns the ingestion.
	 * @param operationEpoch Datastore generation captured when ingestion started.
	 * @param filename Optional uploaded filename used to match source messages.
	 * @param onSource Optional callback receiving each created source identifier.
	 * @returns Listener identifier used by the streaming cleanup.
	 */
	private _register_source_listeners = (
		id: Request.Id,
		operationId: Operation.Id,
		operationEpoch: number,
		filename?: string,
		onSource?: (sourceId: Source.Id) => void,
	): string => {
		return SmartSocket.Class.instance.con(
			SmartSocket.Message.Type.COLLAB_CREATE,
			(m) => {
				if (!this.isOperationContextCurrent(operationEpoch, operationId)) {
					return false;
				}
				if (m.payload.obj.type !== "source") return false;
				if (m.req_id === id) return true;
				if (filename) {
					const mName = m.payload.obj.name || "";
					if (mName === filename) return true;
					try {
						return decodeURIComponent(mName) === decodeURIComponent(filename);
					} catch (e) {
						return false;
					}
				}
				return false;
			},
			(m) => {
				if (this.isOperationContextCurrent(operationEpoch, operationId)) {
					const sourceId = m.payload.obj.id as Source.Id;
					const streamOptions = this.eventDocumentStreamOptions.get(id);
					if (streamOptions && !streamOptions.sourceIds.includes(sourceId)) {
						streamOptions.sourceIds.push(sourceId);
					}
					this.setLoading(m.req_id || id, sourceId);
					onSource?.(sourceId);
				}
			},
		);
	};

	/**
	 * Registers streaming event listeners for real-time document ingestion.
	 * [Tech-Note] Uses promise-chaining to ensure ordered event insertion.
	 * @param id Ingestion request identifier.
	 * @param sidSource Source-creation listener identifier.
	 * @param operationId Operation that owns the ingestion.
	 * @param operationEpoch Datastore generation captured when ingestion started.
	 * @returns Idempotent listener cleanup.
	 */
	private _register_streaming_listeners = (
		id: Request.Id,
		sidSource: string,
		operationId: Operation.Id,
		operationEpoch: number,
	): (() => void) => {
		const bufferedEvents: Doc.Type[] = [];
		let accumulatedCount = 0;
		let lastFlushTime = 0;
		let flushChain: Promise<void> = Promise.resolve();
		const FLUSH_INTERVAL_MS = 300;
		let releaseSocketListeners = (): void => undefined;

		const sid = SmartSocket.Class.instance.con(
			SmartSocket.Message.Type.DOCUMENTS_CHUNK,
			(m) =>
				m.req_id === id &&
				this.isOperationContextCurrent(operationEpoch, operationId),
			(m) => {
				if (!this.isOperationContextCurrent(operationEpoch, operationId)) return;
				const fileId = ((m.payload.docs as any)[0]?.["gulp.source_id"] ||
					(m.payload as any).source_id ||
					(m.payload as any).gulp_source_id) as Source.Id;
				if (!fileId) return;

				const events = Doc.Entity.normalize(
					m.payload.docs || [],
					"gulp.event_code",
					"fnv1a",
				);
				bufferedEvents.push(...events);
				accumulatedCount += events.length;

				this.ingestionProgress.set(
					fileId,
					(this.ingestionProgress.get(fileId) || 0) + events.length,
				);

				if (
					(Date.now() - lastFlushTime >= FLUSH_INTERVAL_MS || m.payload.last) &&
					bufferedEvents.length > 0
				) {
					const toFlush = bufferedEvents.splice(0);
					lastFlushTime = Date.now();
					flushChain = flushChain.then(async () => {
						if (!this.isOperationContextCurrent(operationEpoch, operationId)) {
							return;
						}
						await this.events_add_async(toFlush, operationEpoch);
					});
				}

				if (m.payload.last) {
					flushChain.then(() => {
						if (!this.isOperationContextCurrent(operationEpoch, operationId)) {
							return;
						}
						const finalFileId = fileId;
						const all = Source.Entity.events(this.app, finalFileId);

						this.ingestionProgress.delete(finalFileId);
						this.batchUpdate((draft) => {
							const files = Refractor.array(...draft.target.files);
							const exist = files.findIndex((f) => f.id === finalFileId);
							const file = files[exist];

							if (exist !== -1 && file) {
								const timestamp =
									all.length > 0
										? {
												min: all[all.length - 1].gulp_timestamp,
												max: all[0].gulp_timestamp,
											}
										: { min: Date.now(), max: Date.now() };

								files[exist] = Source.Entity.normalize(draft, {
									...file,
									timestamp,
									nanotimestamp: {
										min: Internal.Transformator.toNanos(timestamp.min),
										max: Internal.Transformator.toNanos(timestamp.max),
									},
									total: all.length,
									selected: true,
								});
								draft.target.files = files;

								draft.timeline.frame = Source.Entity.renderedFrame(files);
							}
						});
						this.delLoadingByFile(finalFileId);

						const finalFile = Source.Entity.id(this.app, finalFileId);
						if (finalFile) {
							toast.success(
								translate("source.ingested", { name: finalFile.name }),
								{
									description: translate("source.totalDocuments", {
										count: all.length,
									}),
									richColors: true,
									icon: <Icon name="Check" />,
								},
							);
						}
					});
				}
			},
		);

		const statsListenerId = SmartSocket.Class.instance.conce(
			SmartSocket.Message.Type.STATS_UPDATE,
			(m) =>
				m.req_id === id &&
				m.payload?.obj?.req_type === "ingest" &&
				m.payload?.obj?.status === "done" &&
				this.isOperationContextCurrent(operationEpoch, operationId),
			() => {
				if (this.isOperationContextCurrent(operationEpoch, operationId)) {
					this.delLoading(id);
				}
				void this.completeEventDocumentStream(id);
				releaseSocketListeners();
			},
		);
		releaseSocketListeners = this.trackOperationSocketCleanup(() => {
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.DOCUMENTS_CHUNK,
				sid,
			);
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.COLLAB_CREATE,
				sidSource,
			);
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				statsListenerId,
			);
		});
		return releaseSocketListeners;
	};

	/**
	 * Applies a durable ingestion summary to the source metadata without reading
	 * canonical document arrays into the main thread.
	 * @param sourceId Source whose committed manifest is authoritative.
	 * @param summary Exact IndexedDB count and timestamp range.
	 * @param toastedSources Sources that already emitted their success toast.
	 * @returns Nothing.
	 */
	private _finalize_persisted_ingestion_source = (
		sourceId: Source.Id,
		summary: EventSourceSummary,
		toastedSources: Set<Source.Id>,
	): void => {
		if (summary.status !== "complete") return;
		const now = Date.now();
		const timestamp =
			summary.eventCount > 0
				? { min: summary.minTimestamp, max: summary.maxTimestamp }
				: { min: now, max: now };
		let finalName: string | null = null;

		this.ingestionProgress.delete(sourceId);
		this.batchUpdate((draft) => {
			const files = Refractor.array(...draft.target.files);
			const index = files.findIndex((candidate) => candidate.id === sourceId);
			const file = files[index];
			if (index === -1 || !file) return;

			files[index] = Source.Entity.normalize(draft, {
				...file,
				timestamp,
				nanotimestamp: {
					min: Internal.Transformator.toNanos(timestamp.min),
					max: Internal.Transformator.toNanos(timestamp.max),
				},
				total: summary.eventCount,
				selected: true,
			});
			finalName = file.name;
			draft.target.files = files;
			draft.timeline.frame = Source.Entity.renderedFrame(files);
		});
		this.delLoadingByFile(sourceId);

		if (finalName && !toastedSources.has(sourceId)) {
			toastedSources.add(sourceId);
			toast.success(translate("source.ingested", { name: finalName }), {
				description: translate("source.totalDocuments", {
					count: summary.eventCount,
				}),
				richColors: true,
				icon: <Icon name="Check" />,
			});
		}
	};

	/**
	 * Tracks ingestion metadata while QueryWorker owns every `docs_chunk` payload.
	 * Source summaries update progress and exact source totals; terminal state and
	 * ingest stats may arrive in either order, so listeners remain until both do.
	 * @param id Ingestion request identifier.
	 * @param operationId Operation that owns the ingestion.
	 * @param operationEpoch Datastore generation captured before registration.
	 * @param filename Optional uploaded filename used to correlate source creation.
	 * @returns Idempotent listener cleanup.
	 */
	private _register_persisted_ingestion_listeners = (
		id: Request.Id,
		operationId: Operation.Id,
		operationEpoch: number,
		filename?: string,
	): (() => void) => {
		const sourceIds = new Set<Source.Id>();
		const summaries = new Map<Source.Id, EventSourceSummary>();
		const toastedSources = new Set<Source.Id>();
		let documentsComplete = false;
		let statsComplete = false;
		let terminalFailure = false;
		let releaseSocketListeners = (): void => undefined;

		const finalizeSource = (sourceId: Source.Id): void => {
			const summary = summaries.get(sourceId) ??
				DataStore.getEventSourceSummary(sourceId);
			if (summary) {
				this._finalize_persisted_ingestion_source(
					sourceId,
					summary,
					toastedSources,
				);
			}
		};
		const finalizeKnownSources = (): void => {
			for (const sourceId of sourceIds) finalizeSource(sourceId);
		};
		const releaseWhenTerminal = (): void => {
			if (terminalFailure || (documentsComplete && statsComplete)) {
				releaseSocketListeners();
			}
		};
		const clearFailedStreamState = (): void => {
			if (terminalFailure) return;
			terminalFailure = true;
			for (const sourceId of sourceIds) {
				this.ingestionProgress.delete(sourceId);
				this.delLoadingByFile(sourceId);
			}
			this.delLoading(id);
			releaseWhenTerminal();
		};
		const cancelFailedStream = (): void => {
			clearFailedStreamState();
			void this.cancelEventDocumentStream(id).catch((error: unknown) => {
				Logger.warn(
					`Unable to cancel ingestion stream ${id}: ${(error as Error).message}`,
					Info,
				);
			});
		};

		const sidSource = this._register_source_listeners(
			id,
			operationId,
			operationEpoch,
			filename,
			(sourceId) => {
				sourceIds.add(sourceId);
				if (documentsComplete) finalizeSource(sourceId);
			},
		);
		const unsubscribeSummaries =
			SmartSocket.Class.instance.subscribeSourceSummaries((event) => {
				if (
					event.requestId !== id ||
					event.operationId !== operationId ||
					event.operationEpoch !== operationEpoch
				) {
					return;
				}
				const sourceId = event.summary.sourceId;
				sourceIds.add(sourceId);
				summaries.set(sourceId, event.summary);
				this.ingestionProgress.set(sourceId, event.summary.eventCount);
				if (documentsComplete) finalizeSource(sourceId);
			});
		const unsubscribeStates =
			SmartSocket.Class.instance.subscribeDocumentStreamStates((change) => {
				if (change.descriptor.requestId !== id) return;
				if (change.state === "completed") {
					documentsComplete = true;
					if (
						this.isOperationContextCurrent(operationEpoch, operationId)
					) {
						finalizeKnownSources();
					}
					releaseWhenTerminal();
					return;
				}
				if (change.state === "canceled" || change.state === "failed") {
					clearFailedStreamState();
				}
			});
		const ingestTerminalListenerId = SmartSocket.Class.instance.conce(
			SmartSocket.Message.Type.INGEST_SOURCE_DONE,
			(message) =>
				message.req_id === id &&
				[
					Request.Status.FAILED,
					Request.Status.CANCELED,
				].includes(message.payload?.status) &&
				this.isOperationContextCurrent(operationEpoch, operationId),
			cancelFailedStream,
		);
		const statsListenerId = SmartSocket.Class.instance.conce(
			SmartSocket.Message.Type.STATS_UPDATE,
			(message) =>
				message.req_id === id &&
				message.payload?.obj?.req_type === "ingest" &&
				[
					Request.Status.DONE,
					Request.Status.FAILED,
					Request.Status.CANCELED,
				].includes(message.payload?.obj?.status) &&
				this.isOperationContextCurrent(operationEpoch, operationId),
			(message) => {
				if (message.payload.obj.status !== Request.Status.DONE) {
					cancelFailedStream();
					return;
				}
				statsComplete = true;
				this.delLoading(id);
				if (documentsComplete) finalizeKnownSources();
				void this.completeEventDocumentStream(id).catch((error: unknown) => {
					Logger.warn(
						`Unable to complete ingestion stream ${id}: ${(error as Error).message}`,
						Info,
					);
					cancelFailedStream();
				});
				releaseWhenTerminal();
			},
		);

		releaseSocketListeners = this.trackOperationSocketCleanup(() => {
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.COLLAB_CREATE,
				sidSource,
			);
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				statsListenerId,
			);
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.INGEST_SOURCE_DONE,
				ingestTerminalListenerId,
			);
			unsubscribeSummaries();
			unsubscribeStates();
		});
		return releaseSocketListeners;
	};

	/**
	 * Initiates standard data ingestion for a single file after QueryWorker has
	 * claimed its document stream, preventing an upload/socket registration race.
	 * @param options Upload, parser settings, progress handler, and optional frame.
	 * @returns Promise settled after the upload task has been enqueued.
	 */
	file_ingest = async ({
		context,
		file,
		settings,
		setProgress,
		frame,
	}: {
		context: FileEntity.IngestOptions["context"];
		file: File;
		settings: FileEntity.Settings;
		setProgress?: (num: number) => void;
		frame?: FileEntity.IngestOptions["frame"];
	}) => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) return;
		const operationEpoch = DataStore.getRenderOperationEpoch();

		this._register_ingest_source_done_listeners(operation.id);

		const id = generateUUID<Request.Id>(Request.Prefix.INGESTION);
		const registered = await this.beginEventDocumentStream({
			requestId: id,
			sourceIds: [],
			operationId: operation.id,
			kind: "ingestion",
			mode: "upsert",
			completionMode: "explicit",
			defaultSourceSettings: {
				field: String(settings.field ?? Internal.Settings.default.field),
				hash_function:
					settings.hash_function ?? Internal.Settings.default.hash_function,
			},
		});
		if (!registered) return;

		let releaseStreamingListeners = (): void => undefined;
		const callbacks = this._ingest_orchestrator(
			id,
			file,
			context as string,
			setProgress,
			() => {
				releaseStreamingListeners();
				void this.cancelEventDocumentStream(id);
			},
		);

		if (isEventStoreLodEnabled()) {
			releaseStreamingListeners =
				this._register_persisted_ingestion_listeners(
					id,
					operation.id,
					operationEpoch,
					file.name,
				);
		} else {
			const sidSource = this._register_source_listeners(
				id,
				operation.id,
				operationEpoch,
				file.name,
			);
			releaseStreamingListeners = this._register_streaming_listeners(
				id,
				sidSource,
				operation.id,
				operationEpoch,
			);
		}

		this.markEventDocumentStreamDispatched(id);
		ingestWorkerManager.enqueue({
			req_id: id,
			file,
			operation_id: operation.id,
			context_name: context as string,
			ws_id: this.app.general.ws_id,
			settings,
			server: Internal.Settings.server,
			token: Internal.Settings.token,
			frame,
			...callbacks,
		});
	};

	/**
	 * Uploads a file in preview mode and returns the parsed preview documents.
	 *
	 * @param options - File ingest options used to build the preview request payload.
	 * @returns A promise that resolves with preview documents, or an empty array when preview loading fails.
	 */
	file_ingest_preview = async (
		options: FileEntity.IngestOptions,
	): Promise<Doc.Type[]> => {
		const { file, settings } = options;
		const operation = Operation.Entity.selected(this.app);
		if (!operation) return [];

		const formData = new FormData();
		const ingestPayload: any = {
			original_file_path: file.name,
			offset: settings.offset ?? 0,
		};

		const pluginParams: any = {
			preview_mode: true,
		};
		if (settings.custom_parameters)
			pluginParams.custom_parameters = settings.custom_parameters;

		const mappingParameters: any = {};
		if (settings.method) mappingParameters.mapping_file = settings.method;
		if (settings.mapping) mappingParameters.mapping_id = settings.mapping;
		if (settings.additional_mapping_files)
			mappingParameters.additional_mapping_files =
				settings.additional_mapping_files;

		if (Object.keys(mappingParameters).length > 0) {
			pluginParams.mapping_parameters = mappingParameters;
		}

		if (settings.store_file !== undefined)
			pluginParams.store_file = settings.store_file;
		ingestPayload.plugin_params = pluginParams;

		formData.append(
			"payload",
			new Blob([JSON.stringify(ingestPayload)], { type: "application/json" }),
		);
		formData.append(
			"f",
			new File([file], file.name, { type: "application/octet-stream" }),
			file.name,
		);
		formData.append(
			"f",
			new File([file], file.name, { type: "application/octet-stream" }),
			file.name,
		);

		const query = {
			plugin: settings.plugin?.split(".")[0],
			operation_id: operation.id,
			context_name: "preview",
			ws_id: this.app.general.ws_id,
		};

		const response = await api<any>("/ingest_file", {
			method: "POST",
			body: formData,
			deassign: true,
			raw: true,
			query,
			headers: {
				size: file.size.toString(),
			},
		});

		if (!response?.data) {
			Logger.error(
				`Failed to load preview for ${file.name}: API returned no data`,
				"Info.file_ingest_preview",
			);
			return [];
		}

		return Array.isArray(response.data) ? (response.data as Doc.Type[]) : [];
	};

	file_set_settings = (
		id: Source.Id,
		settings: Partial<Source.Type["settings"]>,
	) => {
		const file = Source.Entity.id(this.app, id);
		const newSettings = {
			...file.settings,
			...settings,
		} satisfies Source.Type["settings"];
		const isHashInputChanged =
			newSettings.field !== file.settings.field ||
			newSettings.hash_function !== file.settings.hash_function;

		if (isHashInputChanged) {
			Doc.Entity.clearHashRange(id);
			RenderEngine.resetSource(id);
			// Exact pages intentionally retain only render-minimal columns. A changed
			// hash input therefore refetches from the authoritative backend instead of
			// scanning or rehydrating a source-wide object array on the main thread.
			file.settings = newSettings;
			this.refetch({ ids: id, refetchKeys: { [id]: [newSettings.field] } });
		}

		return this.setInfoByKey(
			this.app.target.files.map((file) =>
				id === file.id ? { ...file, settings: newSettings } : file,
			),
			"target",
			"files",
		);
	};

	file_set_total = (id: Source.Id, total = 0) =>
		this.setInfoByKey(
			this.app.target.files.map((file) =>
				file.id === id ? { ...file, total } : file,
			),
			"target",
			"files",
		);

	context_delete = (context: Context.Type, delete_data: boolean) =>
		api<any>(
			"/context_delete",
			{
				method: "DELETE",
				query: {
					context_id: context.id,
					delete_data,
					ws_id: this.app.general.ws_id,
				},
			},
			this.sync,
		);

	context_update = (id: Context.Id, color: string) => {
		return api<any>("/context_update", {
			method: "PATCH",
			query: {
				context_id: id,
				color,
				ws_id: this.app.general.ws_id,
			},
		}).then(() => {
			this.setInfoByKey(
				this.app.target.contexts.map((c) =>
					c.id === id ? { ...c, color } : c,
				),
				"target",
				"contexts",
			);
			this.render();
		});
	};

	/**
	 * Adds normalized events synchronously to the document store.
	 * @param newEvents Normalized documents to add.
	 * @returns Updated global events map.
	 */
	events_add = (newEvents: Doc.Type[]) => Doc.Entity.add(this.app, newEvents);

	/**
	 * Adds normalized events asynchronously to the document store.
	 * @param newEvents Normalized documents to add.
	 * @param expectedOperationEpoch Operation generation that owns the queued batch.
	 * @returns Promise that resolves when insertion completes.
	 */
	events_add_async = async (
		newEvents: Doc.Type[],
		expectedOperationEpoch = DataStore.getRenderOperationEpoch(),
	): Promise<void> => {
		await Doc.Entity.addAsync(this.app, newEvents, expectedOperationEpoch);
	};

	/**
	 * Adds prepared source batches asynchronously to the document store.
	 * @param batches Normalized documents grouped by source and sorted newest first.
	 * @param expectedOperationEpoch Operation generation that owns the queued batches.
	 * @returns Promise that resolves when insertion completes.
	 */
	events_add_prepared_async = async (
		batches: Doc.PreparedBatch[],
		expectedOperationEpoch = DataStore.getRenderOperationEpoch(),
	): Promise<void> => {
		await Doc.Entity.addPreparedAsync(
			this.app,
			batches,
			expectedOperationEpoch,
		);
	};

	/**
	 * Fetches source field metadata with persistent IndexedDB caching.
	 * @param file Source whose indexed fields should be returned.
	 * @returns Field metadata keyed by field name.
	 */
	event_keys = async (file: Source.Type): Promise<Filter.Options> => {
		if (!file) {
			return Internal.Transformator.toAsync({});
		}

		if (Source.Entity.isVirtual(file)) {
			const ids = file.id.split("-").slice(1) as Source.Id[];

			const filterOptionsStack = await Promise.all(
				ids.map((id) => this.event_keys(Source.Entity.id(this.app, id))),
			);

			const result = filterOptionsStack
				.flat()
				.reduce<Filter.Options>((acc, cur) => {
					Object.keys(cur).forEach((c) => {
						if (!acc[c]) {
							acc[c] = cur[c];
						}
					});

					return acc;
				}, {});

			return result;
		}

		return sourceFieldResolver.getFields(
			Internal.Settings.server,
			file,
			async (source) =>
				(await api<Filter.Options>("/query_fields_by_source", {
					query: {
						operation_id: source.operation_id,
						context_id: source.context_id,
						source_id: source.id,
						ws_id: this.app.general.ws_id,
					},
				})) ?? {},
		);
	};

	/**
	 * Clears loaded events and source-scoped render caches for a file.
	 * @param file Source whose events should be reset before a new query.
	 * @returns Nothing.
	 */
	events_reset_in_file = (file: Source.Type): void => {
		Doc.Entity.delete(this.app, file);
		if (file._sampleDataCached) {
			file._sampleDataCached.sample_data = null;
		}
		RenderEngine.resetSource(file.id);
		DataStore.markDirty();
	};

	setDialogSize = (number: number) => {
		this.setInfoByKey(number, "timeline", "dialogSize");
	};

	/**
	 * Fetches one filtered note page without changing the canonical DataStore.
	 * @param flt Existing GulpCollabFilter fields for filtering and pagination.
	 * @param signal Optional cancellation signal for stale detached-window requests.
	 * @returns Normalized notes for the requested page.
	 */
	notes_list = async (
		flt: Note.ListFilter,
		signal?: AbortSignal,
	): Promise<Note.Type[]> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) return [];

		const notes = await api<Note.Type[]>("/note_list", {
			method: "POST",
			query: {
				operation_id: operation.id,
			},
			body: flt,
			signal,
			showErrorToast: false,
		});
		if (signal?.aborted) return [];
		if (!notes) {
			throw new Error("Failed to fetch notes");
		}
		return Note.Entity.normalize(this.app, notes);
	};

	/**
	 * Counts notes matching an existing GulpCollabFilter without loading them.
	 * @param flt Filter fields shared with the corresponding note-list request.
	 * @param signal Optional cancellation signal for stale detached-window requests.
	 * @returns Number of notes matching the filter.
	 */
	notes_count = async (
		flt: Note.ListFilter,
		signal?: AbortSignal,
	): Promise<number> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) return 0;

		const result = await api<{ count: number }>("/object_count", {
			method: "POST",
			query: {
				obj_type: "note",
				operation_id: operation.id,
			},
			body: flt,
			signal,
			showErrorToast: false,
		});
		if (signal?.aborted) return 0;
		if (!result) {
			throw new Error("Failed to count notes");
		}
		return result.count;
	};

	/**
	 * Reloads notes for the currently selected sources and ignores stale responses.
	 * @returns Notes fetched for the selected sources, or undefined when no safe fetch can run.
	 */
	notes_reload = async (): Promise<Note.Type[] | undefined> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}

		const files = this.app.target.files
			.filter(
				(source) => source.selected && source.operation_id === operation.id,
			)
			.map((source) => source.id);
		const reloadSequence = ++this.notesReloadSequence;
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const isCurrentReload = (): boolean =>
			reloadSequence === this.notesReloadSequence &&
			operationEpoch === DataStore.getRenderOperationEpoch() &&
			Operation.Entity.selected(this.app)?.id === operation.id;
		if (files.length === 0) {
			Logger.warn(
				"Tried to fetch all notes from all operations. Ignoring",
				"Info.notes_reload",
			);
			return;
		}
		let notes: Note.Type[] = [];
		const fetch = async (offset = 0) => {
			const fetched = Note.Entity.normalize(
				this.app,
				await api<Note.Type[]>("/note_list", {
					method: "POST",
					query: {
						operation_id: operation.id,
					},
					body: {
						source_ids: files,
						offset,
						limit: 500,
					},
				}),
			);

			if (!isCurrentReload()) return notes;

			if (fetched.length) {
				notes = [...notes, ...fetched].sort(
					(a, b) => Note.Entity.timestamp(b) - Note.Entity.timestamp(a),
				);
				if (notes.length % 2500 === 0) {
					toast(translate("notes.fetchedCount", { count: notes.length }), {
						description: translate("notes.fetchContinuing"),
						icon: <Spinner />,
					});
				}

				DataStore.replaceNotes(notes, "deferred");
				Note.Entity.invalidateCache();
				RenderEngine.reset("notes");

				return new Promise((res) => {
					setTimeout(() => {
						res(fetch(offset + 500));
					});
				});
			} else {
				const message = translate("notes.fetchedInRounds", {
					count: notes.length,
					rounds: offset / 500,
				});
				Logger.log(message, Info);
				DataStore.replaceNotes(notes, "deferred");
				Note.Entity.invalidateCache();
				RenderEngine.reset("notes");
				if (notes.length >= 2500) {
					toast.success(message, {
						icon: <Icon name="Check" />,
						richColors: true,
					});
				}
			}
		};

		await fetch();

		if (!isCurrentReload()) return notes;

		Note.Entity[CacheKey].clear();
		DataStore.publishNotesSnapshotReady(operation.id);

		return notes;
	};

	/**
	 * Invalidates any in-flight note reload so stale responses cannot update the store.
	 * @returns Nothing.
	 */
	cancel_notes_reload = (): void => {
		this.notesReloadSequence++;
	};

	/**
	 *
	 * @param key Key of settings object
	 * @param value Value to save. Be carreful, it can save any shit
	 */
	setSettings = (key: string, value: any) =>
		this.setInfoByKey(value, "settings", key);

	/**
	 * Marks a source as loading without committing Application.Context.
	 * @param req_id Backend request identifier.
	 * @param file_id Source affected by the request.
	 * @returns Nothing.
	 */
	setLoading(req_id: Request.Id, file_id: Source.Id) {
		const req = requestStore.getRequests().find((r) => r.id === req_id);
		if (req && !ACTIVE_REQUEST_STATUSES.has(req.status)) return;

		requestStore.setLoading(req_id, file_id);
		this.app.general.loadings = requestStore.getLoadings();
		DataStore.markDirty();
	}

	/**
	 * Clears a loading marker by source without committing Application.Context.
	 * @param file_id Source identifier.
	 * @returns Nothing.
	 */
	delLoadingByFile(file_id: Source.Id) {
		requestStore.deleteLoadingByFile(file_id);
		this.app.general.loadings = requestStore.getLoadings();
		DataStore.markDirty();
	}

	/**
	 * Clears a loading marker by request without committing Application.Context.
	 * @param req_id Backend request identifier.
	 * @returns Nothing.
	 */
	delLoading(req_id: Request.Id) {
		requestStore.deleteLoadingByRequest(req_id);
		this.app.general.loadings = requestStore.getLoadings();
		DataStore.markDirty();
	}

	/**
	 * Deletes one note and synchronizes canonical and renderer-specific indexes.
	 * @param note Note to delete.
	 * @returns Request promise that resolves after local state is synchronized.
	 */
	note_delete = (note: Note.Type) =>
		api("/note_delete", {
			method: "DELETE",
			query: {
				obj_id: note.id,
				ws_id: this.app.general.ws_id,
			},
		}).then(() => {
			const deletedNote = DataStore.deleteNote(note.id, "deferred");
			if (deletedNote) {
				const sourceId = Note.Entity.sourceId(deletedNote);
				Note.Entity.removeIndexedNote(deletedNote);
				if (sourceId) RenderEngine.resetSourceNotes(sourceId);
			}
		});

	/**
	 * Deletes multiple notes in a single API call.
	 * Synchronizes the DataStore and invalidates caches upon success.
	 *
	 * @param ids Array of note identifiers to delete.
	 * @returns Request promise that resolves after local state is synchronized.
	 */
	notes_delete_bulk = (ids: Note.Id[]) =>
		api("/object_delete_bulk", {
			method: "DELETE",
			query: {
				obj_type: "note",
				operation_id: Operation.Entity.selected(this.app)?.id,
				ws_id: this.app.general.ws_id,
			},
			body: { ids },
		}).then(() => {
			const deletedNotes = DataStore.deleteNotes(ids, "deferred");
			const affectedSourceIds = new Set<Source.Id>();
			deletedNotes.forEach((deletedNote) => {
				const sourceId = Note.Entity.sourceId(deletedNote);
				if (sourceId) affectedSourceIds.add(sourceId);
			});
			Note.Entity.removeIndexedNotes(deletedNotes);
			this.resetNoteRenderCaches(affectedSourceIds);
		});

	/* GRANTED PERMISSIONS */
	add_granted_group = (obj_type: string, obj_id: string, group_id: string) => {
		return api("/object_add_granted_group", {
			method: "PATCH",
			query: {
				obj_type,
				obj_id,
				group_id,
			},
			showErrorToast: false,
			toast: {
				onSuccess: () =>
					toast.success(translate("permissions.groupAdded"), {
						icon: <Icon name="Check" />,
						richColors: true,
					}),
				onError: (response) =>
					toast.error(translate("permissions.groupAddFailed"), {
						description: translate("common.reason", {
							reason: response.data.__error.msg,
						}),
						icon: <Icon name="Stop" />,
						richColors: true,
					}),
			},
		});
	};

	add_granted_user = (obj_type: string, obj_id: string, user_id: string) => {
		return api("/object_add_granted_user", {
			method: "PATCH",
			query: {
				obj_type,
				obj_id,
				user_id,
			},
			showErrorToast: false,
			toast: {
				onSuccess: () =>
					toast.success(translate("permissions.userAdded"), {
						icon: <Icon name="Check" />,
						richColors: true,
					}),
				onError: (response) =>
					toast.error(translate("permissions.userAddFailed"), {
						description: translate("common.reason", {
							reason: response.data.__error.msg,
						}),
						icon: <Icon name="Stop" />,
						richColors: true,
					}),
			},
		});
	};

	remove_granted_group = (
		obj_type: string,
		obj_id: string,
		group_id: string,
	) => {
		return api("/object_remove_granted_group", {
			method: "PATCH",
			query: {
				obj_type,
				obj_id,
				group_id,
			},
			showErrorToast: false,
			toast: {
				onSuccess: () =>
					toast.success(translate("permissions.groupRemoved"), {
						icon: <Icon name="Check" />,
						richColors: true,
					}),
				onError: (response) =>
					toast.error(translate("permissions.groupRemoveFailed"), {
						description: translate("common.reason", {
							reason: response.data.__error.msg,
						}),
						icon: <Icon name="Stop" />,
						richColors: true,
					}),
			},
		});
	};

	remove_granted_user = (obj_type: string, obj_id: string, user_id: string) => {
		return api("/object_remove_granted_user", {
			method: "PATCH",
			query: {
				obj_type,
				obj_id,
				user_id,
			},
			showErrorToast: false,
			toast: {
				onSuccess: () =>
					toast.success(translate("permissions.userRemoved"), {
						icon: <Icon name="Check" />,
						richColors: true,
					}),
				onError: (response) =>
					toast.error(translate("permissions.userRemoveFailed"), {
						description: translate("common.reason", {
							reason: response.data.__error.msg,
						}),
						icon: <Icon name="Stop" />,
						richColors: true,
					}),
			},
		});
	};

	/**
	 * Creates a note and stores the response only while its operation remains active.
	 * @param options Note content, appearance, privacy, and source document.
	 * @returns Backend note request promise.
	 */
	note_create = ({
		name,
		text,
		color = Default.Color.NOTE,
		glyph_id = Glyph.List.entries().find(
			(e) => e[1] === Default.Icon.NOTE,
		)![0]!,
		event,
		isPrivate,
		tags,
	}: {
		name: string;
		text: string;
		color: string;
		event: Doc.Type;
		glyph_id: Glyph.Id;
		isPrivate: boolean;
		tags: string[];
	}) => {
		const operationId = Doc.Entity.operationId(this.app, event);
		const contextId = Doc.Entity.contextId(this.app, event);
		const operationEpoch = DataStore.getRenderOperationEpoch();
		return api<Note.Type>("/note_create", {
			method: "POST",
			query: {
				operation_id: operationId,
				context_id: contextId,
				source_id: event["gulp.source_id"],
				ws_id: this.app.general.ws_id,
				name,
				color,
				glyph_id,
				private: isPrivate,
			},

			toast: {
				onSuccess: () =>
					toast.success(translate("note.created", { name }), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
			},
			body: {
				text,
				tags,
				doc: Doc.Entity.toDoc(this.app, event),
			},
		}).then((note) => {
			if (
				operationId &&
				this.isOperationContextCurrent(operationEpoch, operationId)
			) {
				this.AddNoteToDataStore(note);
			}
		});
	};

	/**
	 * Updates a note and stores the response only while its operation remains active.
	 * @param options Updated note fields and source document.
	 * @returns Backend note update promise.
	 */
	note_edit = ({
		id: obj_id,
		name,
		text,
		color,
		glyph_id = Glyph.List.entries().find(
			(e) => e[1] === Default.Icon.NOTE,
		)![0]!,
		event,
		tags,
	}: {
		id: Note.Id;
		name: string;
		text: string;
		color: string;
		event: Doc.Type;
		glyph_id: Glyph.Id;
		tags: string[];
	}) => {
		const operationId = Doc.Entity.operationId(this.app, event);
		const operationEpoch = DataStore.getRenderOperationEpoch();
		return api<Note.Type>("/note_update", {
			method: "PATCH",
			query: {
				obj_id,
				ws_id: this.app.general.ws_id,
				name,
				glyph_id,
				color,
			},
			toast: {
				onSuccess: () =>
					toast.success(translate("note.updated", { name }), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
			},
			body: {
				text,
				tags,
				doc: Doc.Entity.toDoc(this.app, event),
			},
		}).then((note) => {
			if (
				operationId &&
				this.isOperationContextCurrent(operationEpoch, operationId)
			) {
				this.AddNoteToDataStore(note);
			}
		});
	};

	/**
	 * Reloads the selected operation links and replaces the canonical collection.
	 * @returns Link request result, or undefined when no operation is selected.
	 */
	links_reload = async () => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}

		const reloadSequence = ++this.linksReloadSequence;
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const isCurrentReload = (): boolean =>
			reloadSequence === this.linksReloadSequence &&
			operationEpoch === DataStore.getRenderOperationEpoch() &&
			Operation.Entity.selected(this.app)?.id === operation.id;

		return api<Link.Type[]>(
			"/link_list",
			{
				method: "POST",
				query: {
					operation_id: operation.id,
				},
				body: {
					source_ids: Source.Entity.selected(this.app).map((f) => f.id),
				},
			},
			(links) => {
				if (!isCurrentReload()) return;
				DataStore.replaceLinks(links, "immediate");
				RenderEngine.clearAllCaches();
				this.render();
			},
		);
	};

	link_delete = (link: Link.Type) =>
		api(
			"/link_delete",
			{
				method: "DELETE",
				query: {
					obj_id: link.id,
					ws_id: this.app.general.ws_id,
				},
			},
			this.links_reload,
		);

	link_create = ({
		name,
		event,
		doc_ids,
		glyph_id = Glyph.List.entries().find(
			(e) => e[1] === Default.Icon.LINK,
		)![0]!,
		color = Default.Color.LINK,
		description,
	}: {
		name: string;
		event: Doc.Type;
		doc_ids?: Doc.Type["_id"][];
		glyph_id: Glyph.Id;
		color: string;
		description: string;
	}) => {
		return api<Link.Type>("/link_create", {
			method: "POST",
			query: {
				doc_id_from: event._id,
				operation_id: Doc.Entity.operationId(this.app, event),
				ws_id: this.app.general.ws_id,
				name,
				glyph_id,
				color,
			},
			toast: {
				onSuccess: () =>
					toast.success(translate("link.created", { name }), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
			},
			body: {
				// FIXME: this creates a link without destination documents, which is wrong. the backend allows it
				// just to support this ui ....
				doc_ids: doc_ids,
				description: description,
			},
		}).then(this.links_reload);
	};

	link_edit = ({
		id: obj_id,
		name,
		color = Default.Color.LINK,
		glyph_id,
		events,
		description,
	}: {
		id: Link.Id;
		name: string;
		glyph_id: Glyph.Id;
		color: string;
		events: Doc.Type["_id"][];
		description: string;
	}) =>
		api("/link_update", {
			method: "PATCH",
			query: {
				obj_id,
				name,
				color,
				glyph_id,
				ws_id: this.app.general.ws_id,
			},
			toast: {
				onSuccess: () =>
					toast.success(translate("link.updated", { name }), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
			},
			body: {
				doc_ids: events,
				description: description,
			},
		}).then(this.links_reload);

	links_connect = (link: Link.Type, event: Doc.Type) =>
		api<Link.Type>("/link_update", {
			method: "PATCH",
			query: {
				obj_id: link.id,
				ws_id: this.app.general.ws_id,
			},
			toast: {
				onSuccess: () =>
					toast.success(
						translate("link.eventConnected", {
							event: event._id,
							link: link.name,
						}),
						{
							richColors: true,
							icon: <Icon name="Check" />,
						},
					),
			},
			body: {
				doc_ids: Refractor.array(...link.doc_ids, event._id),
			},
		}).then(this.links_reload);

	links_disconnect = (link: Link.Type, event: Doc.Type) =>
		api<Link.Type>("/link_update", {
			method: "PATCH",
			query: {
				obj_id: link.id,
				ws_id: this.app.general.ws_id,
			},
			toast: {
				onSuccess: () =>
					toast.success(
						translate("link.eventDisconnected", {
							event: event._id,
							link: link.name,
						}),
						{
							richColors: true,
							icon: <Icon name="Check" />,
						},
					),
			},
			body: {
				doc_ids: Refractor.array(
					...link.doc_ids.filter((id) => id !== event._id),
				),
			},
		}).then(this.links_reload);

	/**
	 * Reloads highlights for the selected operation after the debounce interval.
	 * @returns Debounced request result when an operation is selected.
	 */
	highlights_reload = debounce(() => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}
		const reloadSequence = ++this.highlightsReloadSequence;
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const isCurrentReload = (): boolean =>
			reloadSequence === this.highlightsReloadSequence &&
			operationEpoch === DataStore.getRenderOperationEpoch() &&
			Operation.Entity.selected(this.app)?.id === operation.id;

		return api<Highlight.Type[]>(
			"/highlight_list",
			{
				method: "POST",
				query: {
					operation_id: operation.id,
				},
			},
			(h) => {
				if (!isCurrentReload()) return;
				DataStore.replaceHighlights(h, "immediate");
				RenderEngine.clearAllCaches();
				this.render();
			},
		);
	}, 500);

	highlight_create = async ({
		time_range,
		name,
		icon: glyph_id = Glyph.List.entries().find(
			(e) => e[1] === Default.Icon.HIGHLIGHT,
		)![0]!,
		color = Default.Color.HIGHLIGHT,
		tags = [],
	}: {
		time_range: Range;
		name: string;
		icon: Glyph.Id | null;
		color: string;
		tags?: string[];
	}) => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}

		const files = Source.Entity.selected(this.app);
		if (!files.length) {
			return;
		}

		const source_id = files[0].id;

		return api("/highlight_create", {
			method: "POST",
			query: {
				operation_id: operation.id,
				ws_id: this.app.general.ws_id,
				source_id,
				name,
				color,
				glyph_id,
			},
			toast: {
				onSuccess: () =>
					toast.success(translate("highlights.created", { name }), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
			},
			body: {
				time_range,
				tags,
			},
		}).then(this.highlights_reload);
	};

	highlight_delete = (obj_id: Highlight.Id) =>
		api("highlight_delete", {
			method: "DELETE",
			query: {
				obj_id,
				ws_id: this.app.general.ws_id,
			},
		}).then(() => {
			Highlights.remove(obj_id);
			this.highlights_reload();
		});

	private _glyphsReloadPromise: Promise<void> | null = null;
	glyphs_reload = async () => {
		if (this._glyphsReloadPromise) return this._glyphsReloadPromise;

		this._glyphsReloadPromise = (async () => {
			const glyphs: Glyph.Type[] = [];
			let offset = 0;

			while (true) {
				const batch = await api<Glyph.Type[]>("/glyph_list", {
					method: "POST",
					body: { limit: GLYPH_PAGE_SIZE, offset },
				});

				if (!batch) {
					return Logger.error("Failed to sync glyphs", "Info.glyphs_reload");
				}

				glyphs.push(...batch);
				if (batch.length < GLYPH_PAGE_SIZE) break;
				offset += GLYPH_PAGE_SIZE;
			}

			const namedGlyphs = glyphs
				.filter((glyph) => glyph.name)
				.map((glyph) => ({ ...glyph, id: glyph.name as Glyph.Id }));

			Glyph.reset();
			namedGlyphs.forEach(Glyph.register);

			await glyphDB.UpdateConfigurations(
				namedGlyphs.map((glyph) => [glyph.name, glyph]),
			);

			this.setInfoByKey(true, "general", "glyphs_syncronized");
			Logger.log(`Glyphs has been syncronized with gulp-backend`, Info);
		})();

		try {
			await this._glyphsReloadPromise;
		} finally {
			this._glyphsReloadPromise = null;
		}
	};

	setPointers = (pointer: Pointers.Pointer) => {
		const pointers = this.app.timeline.pointers;

		const target = pointers.find((p) => p.id === pointer.id);

		if (target) {
			Object.assign(target, pointer);
		} else {
			pointers.push(pointer);
		}

		this.setInfoByKey(pointers, "timeline", "pointers");
	};

	/**
	 * Builds the per-source settings payload stored inside saved sessions.
	 * @returns Source settings keyed by source id for user_data session storage.
	 */
	private getSourceSettings = (): WorkspaceSessionSnapshot["source_settings"] => {
		const sourceSettings =
			this.app.target.files.reduce<
				WorkspaceSessionSnapshot["source_settings"]["source_settings"]
			>((settingsBySourceId, file) => {
				settingsBySourceId[file.id] = {
					...file.settings,
					pinned: file.pinned,
				};

				return settingsBySourceId;
			}, {} as WorkspaceSessionSnapshot["source_settings"]["source_settings"]);

		return { source_settings: sourceSettings };
	};

	/**
	 * Initializes the private workspace-session cache from the authenticated profile.
	 * @returns True when a user is available.
	 */
	private initializeWorkspaceSessionRepository = (): boolean => {
		const user = this.app.general.user;
		if (!user) return false;
		this.workspaceSessionRepository.initialize(user);
		return true;
	};

	/**
	 * Captures the current operation state as detached JSON-compatible session data.
	 * @param operationId Optional expected operation used to reject stale callbacks.
	 * @returns Current snapshot, or null when no operation is selected.
	 */
	session_snapshot = (
		operationId?: Operation.Id,
	): WorkspaceSessionSnapshot | null => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation || (operationId && operation.id !== operationId)) return null;

		return cloneWorkspaceSessionValue({
			selected: {
				files: Source.Entity.selected(this.app).map((file) => file.id),
				contexts: Context.Entity.selected(this.app).map((context) => context.id),
			},
			timeline: {
				scale: this.getTimelineScaleSnapshot(),
				frame: {
					min: this.app.timeline.frame.min,
					max: this.app.timeline.frame.max,
				},
				filter: this.app.timeline.filter,
				target: this.app.timeline.target,
				scroll: { x: scrollStore.getX(), y: scrollStore.getY() },
			},
			filters: this.app.target.filters,
			hidden: this.app.hidden,
			source_settings: this.getSourceSettings(),
		} satisfies WorkspaceSessionSnapshot);
	};

	/**
	 * Restores saved per-source settings onto a source collection loaded from a session.
	 * @param files Source collection selected for the session load.
	 * @param session Session payload containing optional source settings.
	 * @returns Source collection with matching saved settings merged onto each source.
	 */
	private applySessionSourceSettings = (
		files: Source.Type[],
		session: StoredWorkspaceSession,
	): Source.Type[] => {
		const sourceSettingsById =
			session.snapshot.source_settings?.source_settings;
		if (!sourceSettingsById) return files;

		return files.map((file) => {
			const savedSettings = sourceSettingsById[file.id];
			if (!savedSettings) return file;

			const restoredSettings: Partial<Source.Type["settings"]> = {};
			if (
				typeof savedSettings.offset === "number" &&
				Number.isFinite(savedSettings.offset)
			) {
				restoredSettings.offset = savedSettings.offset;
			}
			if (savedSettings.field) {
				restoredSettings.field = savedSettings.field;
			}
			if (savedSettings.hash_function) {
				restoredSettings.hash_function = savedSettings.hash_function;
			}
			if (savedSettings.render_color_palette) {
				restoredSettings.render_color_palette =
					savedSettings.render_color_palette;
			}
			if (savedSettings.render_engine) {
				restoredSettings.render_engine = savedSettings.render_engine;
			}
			if (
				savedSettings.timeline_label === "filename" ||
				savedSettings.timeline_label === "full_path"
			) {
				restoredSettings.timeline_label = savedSettings.timeline_label;
			}
			if (
				typeof savedSettings.frequency_sample === "number" &&
				Number.isFinite(savedSettings.frequency_sample)
			) {
				restoredSettings.frequency_sample = savedSettings.frequency_sample;
			}
			if (
				Object.prototype.hasOwnProperty.call(savedSettings, "color_palette")
			) {
				restoredSettings.color_palette =
					savedSettings.color_palette ?? undefined;
			}
			if (
				Object.prototype.hasOwnProperty.call(savedSettings, "color_override")
			) {
				restoredSettings.color_override =
					savedSettings.color_override ?? undefined;
			}

			return {
				...file,
				pinned:
					typeof savedSettings.pinned === "boolean"
						? savedSettings.pinned
						: file.pinned,
				settings: {
					...file.settings,
					...restoredSettings,
				},
			};
		});
	};

	/**
	 * Creates a named session for the selected operation with one bucket update.
	 * @param options Session metadata and optional viewport overrides.
	 * @returns The stored session, or null when validation or persistence fails.
	 */
	session_create = async ({
		name,
		icon = Default.Icon.SESSION,
		color = Default.Color.SESSION,
		scroll,
		scale,
	}: {
		name: string;
		icon: Icon.Name;
		color: string;
		scroll?: { x: number; y: number };
		scale?: number;
	}): Promise<StoredWorkspaceSession | null> => {
		const operation = Operation.Entity.selected(this.app);
		const snapshot = this.session_snapshot();
		if (!operation || !snapshot || !this.initializeWorkspaceSessionRepository()) {
			Logger.warn("Tried to create session before user has been defined");
			return null;
		}
		snapshot.timeline.scale = scale ?? snapshot.timeline.scale;
		snapshot.timeline.scroll = scroll ?? snapshot.timeline.scroll;

		try {
			const session = await this.workspaceSessionRepository.createNamed(
				operation.id,
				{ name, icon, color },
				snapshot,
			);
			if (!session) {
				toast.error(translate("api.requestFailed"), { richColors: true });
			}
			return session;
		} catch (error) {
			if (error instanceof WorkspaceSessionNameExistsError) {
				toast.error(translate("session.nameExists"), { richColors: true });
				return null;
			}
			throw error;
		}
	};

	/**
	 * Atomically replaces the selected operation's rolling autosave with one request.
	 * @param providedSnapshot Optional snapshot captured by the autosave coordinator.
	 * @param providedOperationId Optional operation owning the captured snapshot.
	 * @returns True when the operation bucket is persisted.
	 */
	session_autosave = async (
		providedSnapshot?: WorkspaceSessionSnapshot,
		providedOperationId?: Operation.Id,
	): Promise<boolean> => {
		const selectedOperation = Operation.Entity.selected(this.app);
		const operationId = providedOperationId ?? selectedOperation?.id;
		const snapshot =
			providedSnapshot ?? this.session_snapshot(providedOperationId);
		if (!operationId || !snapshot || !this.initializeWorkspaceSessionRepository()) {
			return false;
		}

		const success = await this.workspaceSessionRepository.upsertAutosave(
			operationId,
			snapshot,
		);
		if (!success && !this.autosaveFailureNotified) {
			this.autosaveFailureNotified = true;
			toast.error(translate("api.requestFailed"), {
				richColors: true,
			});
		}
		if (success) this.autosaveFailureNotified = false;
		return success;
	};

	/**
	 * Deletes selected operation sessions with one operation-bucket update.
	 * @param sessionIds Stable session identifiers.
	 * @returns True when deletion is persisted.
	 */
	sessions_delete = async (sessionIds: string[]): Promise<boolean> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation || !this.initializeWorkspaceSessionRepository()) return false;
		return this.workspaceSessionRepository.delete(operation.id, sessionIds);
	};

	/**
	 * Deletes one selected-operation session by stable identifier.
	 * @param sessionId Stable session identifier.
	 * @returns True when deletion is persisted.
	 */
	session_delete = async (sessionId: string): Promise<boolean> =>
		this.sessions_delete([sessionId]);

	/**
	 * Renames one named session in the selected operation.
	 * @param sessionId Stable session identifier.
	 * @param name Requested display name.
	 * @returns True when rename is persisted.
	 */
	session_rename = async (
		sessionId: string,
		name: string,
	): Promise<boolean> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation || !this.initializeWorkspaceSessionRepository()) return false;
		try {
			return await this.workspaceSessionRepository.rename(
				operation.id,
				sessionId,
				name,
			);
		} catch (error) {
			if (error instanceof WorkspaceSessionNameExistsError) {
				toast.error(translate("session.nameExists"), { richColors: true });
				return false;
			}
			throw error;
		}
	};

	/**
	 * Subscribes to cached session changes for one operation.
	 * @param operationId Operation to observe.
	 * @param listener Listener invoked after successful writes or tab synchronization.
	 * @returns Unsubscribe callback.
	 */
	session_subscribe = (
		operationId: Operation.Id,
		listener: () => void,
	): (() => void) => {
		this.initializeWorkspaceSessionRepository();
		return this.workspaceSessionRepository.subscribe(operationId, listener);
	};

	/**
	 * Loads a specific saved session, restoring source settings, selections, filters,
	 * hidden items, scroll positions, and timeline timeframe range.
	 *
	 * @param session - The session data object to restore.
	 * @returns A promise that resolves when the session load action has completed.
	 */
	session_load = async (session: StoredWorkspaceSession) => {
		const snapshot = session.snapshot;
		this.synchronizeTimelineScaleFromState(snapshot.timeline.scale);
		this.batchUpdate((draft) => {
			draft.timeline.target = snapshot.timeline.target;
			draft.timeline.frame = snapshot.timeline.frame;
			draft.timeline.filter = snapshot.timeline.filter;
			draft.timeline.scale = snapshot.timeline.scale;
			draft.target.operations = Operation.Entity.select(
				draft.target.operations,
				session.operation_id,
			);
			draft.target.contexts = Context.Entity.select(
				draft.target.contexts,
				snapshot.selected.contexts,
			);
			draft.target.files = this.applySessionSourceSettings(
				Source.Entity.select(draft, snapshot.selected.files),
				session,
			);
			draft.target.filters = snapshot.filters;

			if (snapshot.hidden && typeof snapshot.hidden === "object") {
				Object.keys(snapshot.hidden).forEach((k) => {
					const key = k as keyof App.Type["hidden"];
					draft.hidden[key] = snapshot.hidden[key];
				});
				draft.hidden.sourcesWithNoEvents =
					localStorage.getItem("settings.__sourcesWithNoEventsHidden") !==
					"false";
			}
		});

		snapshot.selected.files.forEach((sourceId) => {
			RenderEngine.resetSource(sourceId);
		});

		scrollStore.setScrollX(snapshot.timeline.scroll.x);
		scrollStore.setScrollY(snapshot.timeline.scroll.y);

		setTimeout(() => {
			this.refetch({
				ids: snapshot.selected.files,
				frame: snapshot.timeline.frame,
			});
		}, 0);
	};

	/**
	 * Adds or updates one note in DataStore and schedules a coalesced canvas repaint.
	 * @param note Raw note returned by an API or websocket message.
	 * @returns Normalized note stored in DataStore.
	 */
	private AddNoteToDataStore(note: Note.Type): Note.Type {
		return this.AddNotesToDataStore([note])[0];
	}

	/**
	 * Adds or updates collab notes received outside a direct API response.
	 * @param notes Raw notes returned by a websocket message.
	 * @returns Normalized notes stored in DataStore.
	 */
	public notes_upsert_from_collab(notes: Note.Type[]): Note.Type[] {
		return this.AddNotesToDataStore(notes);
	}

	/**
	 * Clears note render caches only for sources touched by an upsert batch.
	 * @param sourceIds Source identifiers that received new or updated notes.
	 * @returns Nothing.
	 */
	private resetNoteRenderCaches(sourceIds: Set<Source.Id>): void {
		sourceIds.forEach((sourceId) => {
			RenderEngine.resetSourceNotes(sourceId);
		});
	}

	/**
	 * Adds or updates multiple notes while sorting and invalidating affected source render caches once.
	 * @param notes Raw notes returned by an API or websocket message.
	 * @returns Normalized notes stored in DataStore.
	 */
	private AddNotesToDataStore(notes: Note.Type[]): Note.Type[] {
		if (notes.length === 0) return [];
		const selectedOperationId = Operation.Entity.selected(this.app)?.id;
		const currentOperationNotes = notes.filter(
			(note) => note.operation_id === selectedOperationId,
		);
		if (currentOperationNotes.length === 0) return [];

		const normalizedNotes = currentOperationNotes.map((note) =>
			Note.Entity.normalize_note(this.app, note),
		);
		const nextNotes = [...DataStore.notes];
		const noteIndexById = new Map<Note.Id, number>();
		const affectedSourceIds = new Set<Source.Id>();
		nextNotes.forEach((storedNote, index) => {
			noteIndexById.set(storedNote.id, index);
		});

		normalizedNotes.forEach((note) => {
			const sourceId = Note.Entity.sourceId(note);
			if (sourceId) affectedSourceIds.add(sourceId);
			const index = noteIndexById.get(note.id);
			if (typeof index === "number") {
				const previousNote = nextNotes[index];
				const previousSourceId = Note.Entity.sourceId(previousNote);
				if (previousSourceId) affectedSourceIds.add(previousSourceId);
				nextNotes[index] = note;
				Note.Entity.upsertIndexedNote(note, previousNote);
				return;
			}

			noteIndexById.set(note.id, nextNotes.length);
			nextNotes.push(note);
			Note.Entity.upsertIndexedNote(note);
		});

		nextNotes.sort(
			(a, b) => Note.Entity.timestamp(b) - Note.Entity.timestamp(a),
		);
		DataStore.replaceNotes(nextNotes, "deferred");
		this.resetNoteRenderCaches(affectedSourceIds);
		return normalizedNotes;
	}

	/**
	 * Lists selected-operation sessions from the private user_data cache.
	 * @param operationId Optional operation override.
	 * @returns Cached operation-scoped sessions without a backend request.
	 */
	async session_list(
		operationId = Operation.Entity.selected(this.app)?.id,
	): Promise<StoredWorkspaceSession[]> {
		if (
			!operationId ||
			!this.app.general.user ||
			!this.initializeWorkspaceSessionRepository()
		) {
			return [];
		}
		return this.workspaceSessionRepository.list(operationId);
	}

	/**
	 * Explicitly refreshes user_data and the private session cache from the backend.
	 * @returns Refreshed selected-operation sessions.
	 */
	async session_refresh(): Promise<StoredWorkspaceSession[]> {
		const user = this.app.general.user;
		const operationId = Operation.Entity.selected(this.app)?.id;
		if (!user || !operationId) return [];

		try {
			const refreshed = await this.user_get_by_id(user.id);
			if (!refreshed) return this.session_list(operationId);
			this.workspaceSessionRepository.refresh(refreshed);
			return this.workspaceSessionRepository.list(operationId);
		} catch (error) {
			toast.error(translate("session.loadFailed"), {
				description: translate("common.errorMessage", {
					message: JSON.stringify(error),
				}),
				icon: <Icon name="FaceSad" />,
			});
			return this.session_list(operationId);
		}
	}

	private _syncPromise: Promise<
		| {
				operations: Operation.Type[];
				contexts: Context.Type[];
				files: Source.Type[];
		  }
		| undefined
	> | null = null;

	/**
	 * Resolves a context color from backend data, local state, or a deterministic fallback.
	 * @param contextData Context summary returned by the backend.
	 * @param existingColor Previously known local color for the same context.
	 * @returns Context color to store in application state.
	 */
	private resolveSyncedContextColor = (
		contextData: GulpDataset.QueryOperations.Context,
		existingColor?: string,
	): string => {
		const backendColor = contextData.color?.trim();
		return (
			backendColor ||
			existingColor ||
			stringToHexColor(contextData.name ?? contextData.id)
		);
	};

	sync = async (
		options?:
			| {
					includeEmptySources?: boolean;
			  }
			| void,
	) => {
		if (this._syncPromise) return this._syncPromise;

		this._syncPromise = (async () => {
			await this.mapping_file_list();
			const syncOptions = options ?? {};

			const operationsData = await api<GulpDataset.QueryOperations.Summary>(
				"/query_operations",
				{
					query: {
						include_empty_sources:
							syncOptions.includeEmptySources ?? true,
					},
				},
			);
			if (!operationsData || !Array.isArray(operationsData)) return undefined;

			const operations: Operation.Type[] = [];
			const contexts: Context.Type[] = [];
			const files: Source.Type[] = [];

			operationsData.forEach((opData) => {
				const opId = opData.id as Operation.Id;
				const existOp = Operation.Entity.id(this.app, opId) ?? {};

				operations.push({
					id: opId,
					name: opData.name,
					index: opData.index ?? opData.id,
					glyph_id: opData.glyph_id ?? Default.Icon.OPERATION,
					description: opData.description,
					selected: existOp.selected ?? false,
				} as Operation.Type);

				opData.contexts?.forEach((ctxData) => {
					const ctxId = ctxData.id as Context.Id;
					const existCtx = Context.Entity.id(this.app, ctxId) ?? {};

					contexts.push({
						id: ctxId,
						name: ctxData.name,
						operation_id: opId,
						glyph_id:
							ctxData.glyph_id ?? (Default.Icon.CONTEXT as unknown as Glyph.Id),
						color: this.resolveSyncedContextColor(ctxData, existCtx.color),
						type: "context",
						selected: existCtx.selected ?? false,
						owner_user_id: this.app.general.user?.id!,
						time_created: Date.now(),
						time_updated: Date.now(),
						granted_user_group_ids: [],
						granted_user_ids: [],
					} as Context.Type);

					ctxData.plugins?.forEach((pluginData) => {
						pluginData.sources?.forEach((srcData) => {
							const srcId = (srcData as any).id as Source.Id;
							const existSrc = Source.Entity.id(this.app, srcId);
							const src = Source.Entity.normalize(
								this.app,
								{
									id: srcId,
									name: srcData.name || srcId,
									operation_id: opId,
									context_id: ctxId,
									plugin: pluginData.name,
									glyph_id: srcData.glyph_id ?? Default.Icon.SOURCE,
									selected: existSrc?.selected ?? existCtx.selected ?? false,
									type: "source",
									owner_user_id: this.app.general.user?.id!,
									time_created: Date.now(),
									time_updated:
										srcData.time_updated ?? existSrc?.time_updated ?? null,
								} as Source.Type,
								srcData,
							);
							if (src) files.push(src);
						});
					});
				});
			});

			Logger.log(
				`${operations.length} operations has been added to application data`,
				this.sync,
			);

			Logger.log(
				`${files.length} files has been added to application data`,
				this.sync,
			);

			RenderEngine.reset("range");

			this.setInfoByKey(operations, "target", "operations");
			this.setInfoByKey(contexts, "target", "contexts");
			this.setInfoByKey(files, "target", "files");

			return { operations, contexts, files };
		})();

		const result = await this._syncPromise;
		this._syncPromise = null;
		return result;
	};

	syncFile = (id: Source.Id) =>
		api<Source.Type>("/source_get_by_id", {
			query: { obj_id: id },
		}).then(async (file) => {
			const details = await this.getDetails().then((d) =>
				d.find((f) => f.id === id),
			);
			if (!details) {
				Logger.fatal("No detailed information for file has been provided");
			}

			const normalized = Source.Entity.normalize(this.app, file, details);

			const exist = this.app.target.files.findIndex((f) => f.id === file.id);
			if (exist >= 0) {
				this.app.target.files[exist] = normalized;
			} else {
				this.app.target.files = [...this.app.target.files, normalized];
			}

			this.setInfoByKey(this.app.target.files, "target", "files");
		});

	getDetails = () =>
		api<GulpDataset.QueryOperations.Summary>("/query_operations", {
			query: {
				include_empty_sources: true,
			},
		})
			.then((operations) => {
				if (!operations || !Array.isArray(operations)) return [];
				return operations
					.map(
						(operation) =>
							operation.contexts?.map(
								(context) =>
									context.plugins?.map((plugin) => plugin.sources ?? []) ?? [],
							) ?? [],
					)
					.flat(3);
			})
			.catch((err) => {
				Logger.error("Failed to fetch /query_operations", err);
				return [];
			});

	query_single_id = (
		doc_id: Doc.Type["_id"],
		operation_id: Operation.Id,
		signal?: AbortSignal,
	) => {
		return api<Doc.Type>("/query_single_id", {
			method: "POST",
			signal,
			query: {
				doc_id,
				operation_id,
			},
		});
	};

	private _pluginListPromise: Promise<
		GulpDataset.PluginList.Interface[]
	> | null = null;
	plugin_list = async (): Promise<GulpDataset.PluginList.Interface[]> => {
		const plugins = this.app.target.plugins;
		if (plugins.length) {
			return Internal.Transformator.toAsync(plugins);
		}

		if (this._pluginListPromise) return this._pluginListPromise;

		this._pluginListPromise = (async () => {
			Logger.warn("No plugins found in application data", "plugin_list");
			Logger.log("Fetching plugins...", "plugin_list");

			const list = await api<GulpDataset.PluginList.Interface[]>(
				"/plugin_list",
				(list) => list.sort((a, b) => a.filename.localeCompare(b.filename)),
			);
			if (!list) {
				return [];
			}

			this.setInfoByKey(list, "target", "plugins");

			Logger.log(
				`Fetched and sorted ${list.length} plugins. Names:`,
				"plugin_list",
			);
			Logger.log(
				list.map((l) => l.filename),
				"plugin_list",
			);

			return list;
		})();

		const result = await this._pluginListPromise;
		this._pluginListPromise = null;
		return result;
	};

	setTimelineFrame = (frame: MinMax) =>
		this.setInfoByKey(frame, "timeline", "frame");

	private _userGetByIdPromises = new Map<string, Promise<User.Type>>();

	/**
	 * Fetches all users available to the current session.
	 *
	 * @param options - Request behavior flags for permission-denied responses.
	 * @returns A promise resolving to the user list returned by the API.
	 */
	user_list = (
		options: {
			suppressMissingPermissionRedirect?: boolean;
			showErrorToast?: boolean;
		} = {},
	): Promise<User.Type[]> => {
		return api<User.Type[]>("/user_list", {
			method: "GET",
			suppressMissingPermissionRedirect:
				options.suppressMissingPermissionRedirect,
			showErrorToast: options.showErrorToast,
		});
	};

	/**
	 * Deletes a user by its unique identifier.
	 *
	 * @param userId - The unique identifier of the user to delete.
	 * @returns A promise resolving to the deletion status and optional error details.
	 */
	user_delete = async (userId: string): Promise<UserDeleteResult> => {
		const response = await api<undefined | ResponseErrorBody>("/user_delete", {
			method: "DELETE",
			query: { user_id: userId },
			raw: true,
			errorToastTitle: translate("home.users.deleteFailed", { userId }),
			suppressMissingPermissionRedirect: true,
		});

		return {
			success: response?.status === "success",
			errorDescription: getUserMutationErrorDescription(response),
		};
	};

	/**
	 * Fetches detailed information about a specific user by its ID.
	 *
	 * @param userId - The unique identifier of the user to fetch.
	 * @returns A promise resolving to the detailed user information.
	 */
	user_get_by_id = (userId: string): Promise<User.Type> => {
		if (this._userGetByIdPromises.has(userId)) {
			return this._userGetByIdPromises.get(userId)!;
		}
		const p = api<User.Type>("/user_get_by_id", {
			method: "GET",
			query: { user_id: userId },
		}).finally(() => {
			this._userGetByIdPromises.delete(userId);
		});
		this._userGetByIdPromises.set(userId, p);
		return p;
	};

	/**
	 * Authenticates a user, reloads user-scoped application data, and notifies
	 * detached windows that the main tab can provide context again.
	 *
	 * @param credentials - User identifier and password submitted by the login view.
	 * @returns The authenticated user, or null when login fails.
	 */
	login = async (
		credentials: Pick<User.Minified, "id" | "password">,
	): Promise<User.Type | null> => {
		const user = await api<User.Type>("/login", {
			method: "POST",
			query: {
				ws_id: this.app.general.ws_id,
			},
			toast: {
				onSuccess: () =>
					toast.success(translate("auth.accessGranted"), {
						richColors: true,
						icon: <Icon name="Check" />,
					}),
			},
			errorToastTitle: translate("auth.loginFailed"),
			authRedirect: false,
			body: {
				user_id: credentials.id,
				password: credentials.password,
			},
		});

		if (!user) {
			return null;
		}

		Internal.Settings.token = user.token;
		localStorage.setItem("__user_id", user.id);
		const fullUserProfile = await this.user_get_by_id(user.id).catch(
			() => null,
		);
		const authenticatedUser = {
			...credentials,
			...user,
			...(fullUserProfile ?? {}),
			token: user.token,
		};

		await this.plugin_list();
		await this.glyphs_reload();
		await this.sync();

		this.setInfoByKey(authenticatedUser, "general", "user");
		window.dispatchEvent(new CustomEvent("gulp-auth-restored"));

		return authenticatedUser;
	};

	user_set_data = async (
		key: string,
		value: any,
	): Promise<User.Type | null> => {
		if (!this.app.general.user) {
			Logger.warn(`Tried to set user data ${key} without a logged-in user`);
			return null;
		}

		const user = await api<User.Type>("/user_update", {
			method: "PATCH",
			query: {
				user_id: this.app.general.user.id,
			},
			body: {
				user_data: {
					[key]: value,
				},
			},
		});

		if (user) {
			this.setInfoByKey(user, "general", "user");
		}

		return user;
	};

	/**
	 * Logs out the current user session by calling the POST /logout API.
	 * Clears the stored session token and resets the user context.
	 *
	 * @returns Promise that resolves when logout cleanup is completed
	 */
	logout = async (): Promise<void> => {
		WindowBridge.persistMainStatus("auth_lost");
		if (window.location.pathname.startsWith("/detached/")) {
			WindowBridge.sendControlMessage(
				WindowBridge.MessageType.AUTH_LOST,
				{},
			);
		}
		window.dispatchEvent(new CustomEvent("gulp-auth-lost"));

		try {
			await api<undefined>("/logout", {
				method: "POST",
				query: {
					ws_id: this.app.general.ws_id,
				},
				authRedirect: false,
				showErrorToast: false,
			});
		} catch (error) {
			Logger.error(error, "Info.logout");
		}

		Internal.Settings.token = "";
		localStorage.removeItem("__user_id");
		this.workspaceSessionRepository.close();
		this.synchronizeTimelineScaleFromState(App.Base.timeline.scale);
		this.setInfo({
			...App.Base,
			general: {
				...App.Base.general,
				server: this.app.general.server,
				ws_id: this.app.general.ws_id,
			},
		});
	};

	/**
	 * Clamps a timeline scale against the current frame and minimum visible duration.
	 * @param scale Requested timeline scale.
	 * @returns Canonical scale within the supported viewport range.
	 */
	public clampTimelineScale = (scale: number): number => {
		const timeRange = this.app.timeline.frame.max - this.app.timeline.frame.min;
		const maxScale =
			timeRange > 0
				? timeRange / this.MIN_VISIBLE_TIMELINE_DURATION_MS
				: 9999999;
		return Math.max(0.01, Math.min(maxScale, scale));
	};

	/**
	 * Clamps and publishes an explicit scale command through normal React state.
	 * @param scale Requested timeline scale.
	 * @param origin Optional caller identity used to classify the command.
	 * @returns Nothing.
	 */
	setTimelineScale = (scale: number, origin?: object): void => {
		const clampedScale = this.clampTimelineScale(scale);
		this.timelineScaleCommandListeners.forEach((listener) =>
			listener(clampedScale, origin),
		);
		this.timelineScaleStore.prepareReactCommit(clampedScale);
		this.setInfoByKey(
			clampedScale,
			"timeline",
			"scale",
		);
	};

	/**
	 * Persists a settled Canvas scale without replacing Application context state.
	 * The existing timeline object is updated so Info and session serializers retain
	 * canonical reads while the instance-local store drives narrow subscribers.
	 * @param scale Settled transient Canvas scale.
	 * @returns Canonical committed scale after clamping.
	 */
	persistSettledTimelineScale = (scale: number): number => {
		const clampedScale = this.clampTimelineScale(scale);
		this._app.timeline.scale = clampedScale;
		this.timelineScaleStore.commit(clampedScale);
		return clampedScale;
	};

	/**
	 * Marks an intentional React-state scale replacement as authoritative.
	 * This prevents the pending renderless rebase from treating restores or resets
	 * that reuse the previous React value as unrelated stale state.
	 * @param scale Timeline scale carried by the imminent React state update.
	 * @returns Nothing.
	 */
	synchronizeTimelineScaleFromState = (scale: number): void => {
		this.timelineScaleStore.prepareReactCommit(scale, true);
		this._app = {
			...this._app,
			timeline: { ...this._app.timeline, scale },
		};
		this.timelineScaleStore.synchronize(scale);
	};

	/**
	 * Returns the instance-local canonical timeline scale snapshot.
	 * @returns Current canonical timeline scale.
	 */
	getTimelineScaleSnapshot = (): number =>
		this.timelineScaleStore.getSnapshot();

	/**
	 * Subscribes to renderless settled-scale commits for this Info instance.
	 * @param listener Callback invoked after the canonical snapshot changes.
	 * @returns Function that removes the listener.
	 */
	subscribeTimelineScale = (listener: () => void): (() => void) =>
		this.timelineScaleStore.subscribe(listener);

	/**
	 * Subscribes to explicit timeline-scale commands, including no-op reset values.
	 * @param listener Callback receiving the clamped scale and optional caller identity.
	 * @returns Function that removes the listener.
	 */
	subscribeTimelineScaleCommands = (
		listener: (scale: number, origin?: object) => void,
	): (() => void) => {
		this.timelineScaleCommandListeners.add(listener);
		return () => this.timelineScaleCommandListeners.delete(listener);
	};

	setTimelineTarget = (event?: Doc.Type | null): Doc.Type => {
		const previousTargetId = this.app.timeline.target?._id;

		if (typeof event !== "undefined") {
			this.setInfoByKey(event as Doc.Type, "timeline", "target");
			if ((event as Doc.Type | null | undefined)?._id !== previousTargetId) {
				DataStore.markDirty();
			}
		}

		return event as Doc.Type;
	};

	/**
	 * Moves the current timeline target through a bounded EventStore neighbor read.
	 * @param delta Descending-order offset: -1 selects newer and 1 selects older.
	 * @returns Selected neighboring event, or null at a source boundary.
	 */
	moveTimelineTarget = async (delta: -1 | 1): Promise<Doc.Type | null> => {
		const target = this.app.timeline.target;
		if (!target) return null;
		const operationEpoch = DataStore.getRenderOperationEpoch();
		for (let attempt = 0; attempt < 2; attempt++) {
			const window = await DataStore.Events.getNeighbors(
				target["gulp.source_id"],
				target._id,
				1,
				1,
			);
			if (
				DataStore.getRenderOperationEpoch() !== operationEpoch ||
				this.app.timeline.target?._id !== target._id
			) {
				return null;
			}
			const currentRevision = DataStore.getSourceRevision(
				target["gulp.source_id"],
			);
			if (
				attempt === 0 &&
				currentRevision > 0 &&
				currentRevision !== window.sourceRevision
			) {
				continue;
			}
			const next = window.events[window.currentIndex + delta] ?? null;
			if (next) this.setTimelineTarget(next);
			return next;
		}
		return null;
	};

	/**
	 * Resolves one source edge without loading or reversing its complete event list.
	 * @param sourceId Source whose edge is requested.
	 * @param newest Whether to return the newest rather than oldest timestamp.
	 * @returns One exact edge event, or null for an empty source.
	 */
	getTimelineSourceEdge = async (
		sourceId: Source.Id,
		newest: boolean,
	): Promise<Doc.Type | null> => {
		const operationEpoch = DataStore.getRenderOperationEpoch();
		for (let attempt = 0; attempt < 2; attempt++) {
			const summary =
				DataStore.getEventSourceSummary(sourceId) ??
				(await DataStore.Events.getSourceSummary(sourceId));
			if (!summary || summary.eventCount === 0) return null;
			const timestamp = newest ? summary.maxTimestamp : summary.minTimestamp;
			const page = await DataStore.Events.queryPixelBucket(
				sourceId,
				{ min: timestamp, max: timestamp },
				1,
			);
			if (DataStore.getRenderOperationEpoch() !== operationEpoch) return null;
			if (attempt === 0 && page.sourceRevision !== summary.revision) continue;
			return page.events[0] ?? null;
		}
		return null;
	};

	setTimelineFilter = (filter: string) =>
		this.setInfoByKey(filter, "timeline", "filter");

	increasedTimelineScale = (current: number = this.app.timeline.scale) =>
		current + current / 8;

	decreasedTimelineScale = () =>
		this.app.timeline.scale - this.app.timeline.scale / 8;

	query_external = async (
		plugin: string,
		plugin_params?: Record<string, any>,
		preview_mode = false,
		q?: string,
		q_options?: Record<string, any>,
	): Promise<{
		total_hits: number;
		docs: Doc.Type[];
	} | null> => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return { total_hits: 0, docs: [] };
		}
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const requestId = generateUUID<Request.Id>(Request.Prefix.INGESTION);
		let releaseStreamingListeners = (): void => undefined;
		let releaseDoneListener = (): void => undefined;
		let terminalPromise: Promise<null> | null = null;
		const persistDocuments = !preview_mode && isEventStoreLodEnabled();
		if (!preview_mode) {
			const registered = await this.beginEventDocumentStream({
				requestId,
				sourceIds: [],
				operationId: operation.id,
				kind: "ingestion",
				mode: "upsert",
				completionMode: "explicit",
				defaultSourceSettings: {
					field: "gulp.event_code",
					hash_function: "fnv1a",
				},
			});
			if (!registered) return null;

			if (persistDocuments) {
				releaseStreamingListeners =
					this._register_persisted_ingestion_listeners(
						requestId,
						operation.id,
						operationEpoch,
					);
			} else {
				const sidSource = this._register_source_listeners(
					requestId,
					operation.id,
					operationEpoch,
				);
				releaseStreamingListeners = this._register_streaming_listeners(
					requestId,
					sidSource,
					operation.id,
					operationEpoch,
				);
			}

			terminalPromise = new Promise<null>((resolve, reject) => {
				let isSettled = false;
				let sourceComplete = false;
				let documentsComplete = !persistDocuments;
				let unsubscribeStreamState = (): void => undefined;
				const settleResolved = (): void => {
					if (isSettled) return;
					isSettled = true;
					resolve(null);
				};
				const settleRejected = (error: Error): void => {
					if (isSettled) return;
					isSettled = true;
					reject(error);
				};
				const releaseIfSuccessful = (): void => {
					if (!sourceComplete || !documentsComplete) return;
					settleResolved();
					releaseDoneListener();
				};
				const finishUnsuccessful = (failed: boolean): void => {
					if (failed) {
						settleRejected(new Error("External query ingestion failed."));
					} else {
						settleResolved();
					}
					releaseDoneListener();
					releaseStreamingListeners();
					void this.cancelEventDocumentStream(requestId);
				};
				const listenerId = SmartSocket.Class.instance.conce(
					SmartSocket.Message.Type.INGEST_SOURCE_DONE,
					(message) =>
						message.req_id === requestId &&
						this.isOperationContextCurrent(operationEpoch, operation.id),
					(message) => {
						const status = message.payload.status;
						if (status === Request.Status.DONE) {
							sourceComplete = true;
							releaseIfSuccessful();
							return;
						}
						finishUnsuccessful(status === Request.Status.FAILED);
					},
				);
				if (persistDocuments) {
					unsubscribeStreamState =
						SmartSocket.Class.instance.subscribeDocumentStreamStates((change) => {
							if (change.descriptor.requestId !== requestId) return;
							if (change.state === "completed") {
								documentsComplete = true;
								releaseIfSuccessful();
								return;
							}
							if (change.state === "failed" || change.state === "canceled") {
								finishUnsuccessful(change.state === "failed");
							}
						});
				}
				releaseDoneListener = this.trackOperationSocketCleanup(() => {
					SmartSocket.Class.instance.coff(
						SmartSocket.Message.Type.INGEST_SOURCE_DONE,
						listenerId,
					);
					unsubscribeStreamState();
					settleResolved();
				});
			});
		}

		let response;
		try {
			if (!preview_mode) {
				this.markEventDocumentStreamDispatched(requestId);
			}
			response = await api<{
				total_hits: number;
				docs: Doc.Type[];
			}>("/query_external", {
				method: "POST",
				raw: true,
				query: {
					ws_id: this.app.general.ws_id,
					operation_id: operation.id,
					plugin,
					req_id: requestId,
				},
				body: {
					q: q,
					plugin_params,
					q_options: {
						...q_options,
						preview_mode,
					},
				},
			});
		} catch (error) {
			releaseDoneListener();
			releaseStreamingListeners();
			if (!preview_mode) await this.cancelEventDocumentStream(requestId);
			throw error;
		}
		if (response?.data) return response.data;
		if (!preview_mode && (!response || response.status !== "pending")) {
			releaseDoneListener();
			releaseStreamingListeners();
			await this.cancelEventDocumentStream(requestId);
			return null;
		}
		return terminalPromise;
	};

	/**
	 * Persists a Query.Type into the application filter cache for one or more source files.
	 *
	 * Before persisting, auto-generates the human-readable `string` label via
	 * `Filter.Entity.describe()` so it always reflects the actual applied state.
	 * Also preserves `fieldTypeMap` for correct clause generation on re-open.
	 *
	 * @param file - One or more source files to associate the query with.
	 * @param query - The structured query to persist.
	 */
	setQuery = (file: Arrayed<Source.Type>, query: Query.Type): void => {
		this.persistSourceQueries(
			Parser.array(file).map((source) => ({ file: source, query })),
		);
	};

	/**
	 * Persists distinct per-source queries in one state update. This is required
	 * for timeline frames because source offsets produce different raw ranges.
	 * @param entries Source/query pairs to store in the application filter cache.
	 * @returns Nothing.
	 */
	private persistSourceQueries = (
		entries: Array<{ file: Source.Type; query: Query.Type }>,
	): void => {
		const filters = { ...this.app.target.filters };

		entries.forEach(({ file, query }) => {
			filters[file.id] = {
				string: Filter.Entity.describe(query, this.app),
				text_filter: query.text_filter,
				source_config: query.source_config,
				// An empty array is a valid cleared state and must be persisted.
				filters: Array.isArray(query.filters) ? query.filters : [],
				raw: query.raw,
				isManual: query.isManual,
				fieldTypeMap: query.fieldTypeMap,
			};
		});

		this.app.target.filters = filters;
		this.setInfoByKey(Refractor.object(filters), "target", "filters");
	};

	getQuery = (file: Source.Type): Query.Type => {
		const query = this.app.target.filters[file.id];

		if (!query) {
			const q = Filter.Entity.default(this.app, file.id);

			this.setQuery(file, q);

			return q;
		}

		return query;
	};

	filters_remove = (file: Source.Type | Source.Id) => {
		const id = Parser.useUUID(file) as Source.Id;
		const filters = Refractor.object({
			...this.app.target.filters,
			[id]: Filter.Entity.default(this.app, file),
		});

		return this.setInfoByKey(filters, "target", "filters");
	};

	useReverseScroll = (bool: boolean) => {
		localStorage.setItem("settings.__isScrollReversed", String(bool));
		this.setInfoByKey(bool, "timeline", "isScrollReversed");
	};

	files_reorder_upper = (id: Source.Id) => {
		const files = this.app.target.files;
		const index = files.findIndex((file) => file.id === id);

		if (index === 0) return;

		const file = files[index];
		files[index] = files[index - 1];
		files[index - 1] = file;

		this.setInfoByKey(files, "target", "files");
		this.render();
	};

	files_reorder_lower = (id: Source.Id) => {
		const files = this.app.target.files;
		const index = files.findIndex((file) => file.id === id);

		if (index === files.length - 1) return;

		const file = files[index];
		files[index] = files[index + 1];
		files[index + 1] = file;

		this.setInfoByKey(files, "target", "files");
		this.render();
	};

	/**
	 * Executes Sigma rules and optionally stores generated notes for the active operation.
	 * @param src_ids Source identifiers included in the Sigma query.
	 * @param sigmas Sigma rule files to send to the backend.
	 * @param notes Whether matching documents should create notes.
	 * @returns Backend request result, or undefined after an operation change.
	 */
	query_sigma = async (
		src_ids: Source.Id[],
		sigmas: NodeFile[],
		notes: boolean,
	) => {
		const operation = Operation.Entity.selected(this.app);
		if (!operation) {
			return;
		}
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const sigmaContents = await Promise.all(sigmas.map((sigma) => sigma.text()));
		if (!this.isOperationContextCurrent(operationEpoch, operation.id)) return;
		const requestId = generateUUID<Request.Id>(Request.Prefix.QUERY);
		const registered = await this.beginEventDocumentStream({
			requestId,
			sourceIds: src_ids,
			operationId: operation.id,
			kind: "query",
			mode: "upsert",
			completionMode: "explicit",
			deduplicateSequences: false,
			sourceSettingsById: this.getQuerySourceSettingsById(src_ids),
		});
		if (!registered) return;

		let statsComplete = false;
		let documentsComplete = false;
		let notesComplete = !notes;
		let releaseSocketListeners = (): void => undefined;
		const releaseCompletedListeners = (): void => {
			if (statsComplete && documentsComplete && notesComplete) {
				releaseSocketListeners();
			}
		};
		const unsubscribeStreamState =
			SmartSocket.Class.instance.subscribeDocumentStreamStates((change) => {
				if (change.descriptor.requestId !== requestId) return;
				if (
					change.state !== "completed" &&
					change.state !== "canceled" &&
					change.state !== "failed"
				) {
					return;
				}
				documentsComplete = true;
				if (change.state !== "completed") {
					statsComplete = true;
					notesComplete = true;
				}
				releaseCompletedListeners();
			});
		const statsListenerId = SmartSocket.Class.instance.conce(
			SmartSocket.Message.Type.STATS_UPDATE,
			(message) =>
				message.req_id === requestId &&
				this.isOperationContextCurrent(operationEpoch, operation.id) &&
				[
					Request.Status.DONE,
					Request.Status.FAILED,
					Request.Status.CANCELED,
				].includes(message.payload.obj?.status),
			(message) => {
				if (!this.isOperationContextCurrent(operationEpoch, operation.id)) {
					return;
				}
				if (message.payload.obj.status !== Request.Status.DONE) {
					toast.error(translate("sigma.queryFailed"), {
						icon: <Icon name="Stop" />,
						richColors: true,
					});
					void this.cancelEventDocumentStream(requestId);
				} else {
					toast.success(
						translate("sigma.queryFinished", {
							name: message.payload.obj.name,
						}),
						{
							description: translate("sigma.totalMatches", {
								count: message.payload.obj.data.total_hits ?? 0,
							}),
							icon: <Icon name="Sigma" />,
						},
					);
					void this.completeEventDocumentStream(requestId);
				}
				statsComplete = true;
				// Terminal query stats are emitted after every note batch is queued;
				// this also closes zero-result Sigma requests that emit no collab packet.
				notesComplete = true;
				releaseCompletedListeners();
			},
		);
		const notesListenerId = notes
			? SmartSocket.Class.instance.con(
					SmartSocket.Message.Type.COLLAB_CREATE,
					(message) =>
						message.req_id === requestId &&
						this.isOperationContextCurrent(operationEpoch, operation.id),
					(message) => {
						if (
							!this.isOperationContextCurrent(operationEpoch, operation.id)
						) {
							return;
						}
						if (
							Array.isArray(message.payload.obj) &&
							message.payload.obj.length > 0
						) {
							const newItems = message.payload.obj.filter(
								(item) => item.type === "note",
							) as Note.Type[];
							const storedItems = this.AddNotesToDataStore(newItems);
							toast.success(
								translate("notes.fetchedCount", {
									count: storedItems.length,
								}),
								{ richColors: true },
							);
						}
						notesComplete = true;
						releaseCompletedListeners();
					},
				)
			: null;
		releaseSocketListeners = this.trackOperationSocketCleanup(() => {
			unsubscribeStreamState();
			SmartSocket.Class.instance.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				statsListenerId,
			);
			if (notesListenerId) {
				SmartSocket.Class.instance.coff(
					SmartSocket.Message.Type.COLLAB_CREATE,
					notesListenerId,
				);
			}
		});

		let response;
		try {
			this.markEventDocumentStreamDispatched(requestId);
			response = await api(
				"/query_sigma",
				{
					method: "POST",
					query: {
						ws_id: this.app.general.ws_id,
						operation_id: operation.id,
						req_id: requestId,
					},
					raw: true,
					body: {
						sigmas: sigmaContents,
						q_options: {
							create_notes: notes,
						},
						src_ids,
					},
					showErrorToast: false,
					toast: {
						onSuccess: () =>
							toast.success(translate("sigma.applied"), {
								richColors: true,
								icon: <Icon name="Check" />,
							}),
						onError: () =>
							toast.error(translate("sigma.notApplied"), {
								richColors: true,
								icon: <Icon name="Warning" />,
							}),
					},
				},
			);
		} catch (error) {
			releaseSocketListeners();
			await this.cancelEventDocumentStream(requestId);
			throw error;
		}
		if (!response || response.status !== "pending") {
			releaseSocketListeners();
			await this.cancelEventDocumentStream(requestId);
		}
		return response;
	};

	toggle_visibility = (key: keyof App.Type["hidden"]) => {
		const next = !this.app.hidden[key];
		if (key === "sourcesWithNoEvents") {
			localStorage.setItem("settings.__sourcesWithNoEventsHidden", String(next));
		}
		this.setInfoByKey(next, "hidden", key);
		this.render();
	};

	files_repin = (id: Source.Id) => {
		const files = this.app.target.files;
		const index = files.findIndex((file) => file.id === id);

		files[index].pinned = !files[index].pinned;

		this.setInfoByKey(files, "target", "files");
		this.render();
	};

	get width(): number {
		return (
			this.app.timeline.scale *
			(document.getElementById("canvas")?.clientWidth || 1)
		);
	}

	setInfoByKey = <K extends keyof App.Type, S extends keyof App.Type[K]>(
		value: App.Type[K][S] | ((prev: App.Type[K][S]) => App.Type[K][S]),
		section: K,
		key: S,
	) => {
		this.setInfo((_info) => {
			const resolvedValue =
				typeof value === "function"
					? (value as any)(_info[section][key])
					: value;
			this.app = {
				..._info,
				[section]: {
					..._info[section],
					[key]: resolvedValue,
				},
			};

			return this.app;
		});
	};

	/**
	 * Batches multiple state updates into a single React setState call.
	 * Use this instead of calling setInfoByKey N times in sequence,
	 * which would create N intermediate state objects and potentially N re-renders.
	 *
	 * The updater receives a shallow-cloned draft of the app state with all
	 * top-level sections pre-cloned. Modifications to the draft are applied
	 * as a single immutable update.
	 *
	 * @param updater - Function receiving a mutable draft of the current app state.
	 *                  Modify the draft's sections directly (e.g., draft.target.notes = []).
	 */
	batchUpdate = (updater: (draft: App.Type) => void) => {
		this.setInfo((prev) => {
			// Shallow-clone top-level sections so the updater can safely mutate them
			const next: App.Type = {
				...prev,
				target: { ...prev.target },
				timeline: { ...prev.timeline },
				general: { ...prev.general },
				settings: { ...prev.settings },
				hidden: { ...prev.hidden },
			};
			updater(next);
			this.app = next;
			return next;
		});
	};
}

export type Arrayed<K> = K | K[];

export type UUIDED<
	K extends Context.Type | Source.Type | Source.Type | Filter.Type,
> = K | K["id"];

export const Pattern = {
	Server: new RegExp(
		/https?:\/\/(?:[a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+(?:\.[a-zA-Z]{2,})?(:\d+)?(\/[^\s]*)?/,
	),
	Username: /^[\s\S]{3,48}$/,
	Password: /^[\s\S]{3,48}$/,
};

export interface MinMax<T extends number | bigint = number> {
	min: T;
	max: T;
}

export type Range = [number, number];

export const MinMaxBase = {
	min: 0,
	max: 0,
};
