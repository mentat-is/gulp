import type { Source } from "@/entities/Source";
import type { QueryCacheProvenance } from "@/entities/QueryCache";
import type {
	EventSourceSummary,
	EventSourceQueryCacheActivation,
	EventSourceQueryCacheReconciliation,
	DesiredQueryCacheLookup,
} from "@/event-store/EventStore.types";
import type { EventStoreViewportDescriptor } from "@/event-store/EventStore.viewport-projector";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

/** WebSocket message type carrying query, enrichment, and ingestion documents. */
export const DOCUMENT_CHUNK_MESSAGE_TYPE = "docs_chunk";

/** Client-to-server flow-control acknowledgement for one query document chunk. */
export const DOCUMENT_CHUNK_ACK_MESSAGE_TYPE = "docs_chunk_ack";

/** Maximum raw payload retained while a request is waiting for registration. */
export const UNKNOWN_STREAM_MAILBOX_MAX_BYTES = 64 * 1024 * 1024;

/** Maximum time an unregistered document chunk remains eligible for replay. */
export const UNKNOWN_STREAM_MAILBOX_TTL_MS = 30_000;

/** WebSocket connection states published by the query transport. */
export type QuerySocketState =
	| "connecting"
	| "authenticating"
	| "ready"
	| "disconnected";

/** Determines which realm owns documents for one registered stream. */
export type DocumentStreamOwner = "persist" | "ignore" | "passthrough";

/** Lifecycle states for one registered document stream. */
export type DocumentStreamState =
	| "registered"
	| "streaming"
	| "flushing"
	| "completed"
	| "canceled"
	| "failed";

/** Query mutation semantics retained with a stream registration. */
export type DocumentStreamKind =
	| "replace"
	| "ingestion-build"
	| "query-upsert"
	| "enrichment"
	| "realtime-ingest";

/** Serializable source normalization settings owned by QueryWorker. */
export interface DocumentStreamSourceSettings {
	field: string;
	hash_function: "fnv1a" | "djb2" | "sdbm";
}

/** Declares how one request's document chunks must be routed. */
export interface StreamDescriptor {
	requestId: string;
	operationId: string;
	operationEpoch: number;
	owner: DocumentStreamOwner;
	completionMode?: "last" | "explicit";
	/** Timeline replacement previews become visible only after persistence. */
	durableProjectionOnly?: boolean;
	/**
	 * Whether numeric chunk sequences are unique for the whole request.
	 * Multi-query endpoints may reset `chunk_num` for each child query and must
	 * disable this optimization; exact document IDs still make persistence idempotent.
	 */
	deduplicateSequences?: boolean;
	kind?: DocumentStreamKind;
	sourceIds?: readonly string[];
	defaultSourceSettings?: DocumentStreamSourceSettings;
	sourceSettingsById?: Readonly<Record<string, DocumentStreamSourceSettings>>;
	expectedEvents?: number;
	revision?: number;
	/** Query identity and source metadata persisted when a replace generation completes. */
	queryCache?: QueryCacheProvenance;
}

/** Explicit alias used at document-routing integration boundaries. */
export type DocumentStreamDescriptor = StreamDescriptor;

/** Terminal metadata supplied after an explicit stream's backend work completes. */
export interface DocumentStreamFinalization {
	/** Query-cache provenance keyed by a source discovered during the stream. */
	queryCachesBySourceId?: Readonly<Record<string, QueryCacheProvenance>>;
}

/** Loosely typed payload envelope accepted from the gULP WebSocket. */
export interface QuerySocketPayload {
	docs?: readonly unknown[];
	last?: boolean;
	/** True only for the last chunk of the complete request, including query batches. */
	request_last?: boolean;
	chunk_num?: unknown;
	chunk_number?: unknown;
	name?: unknown;
	[key: string]: unknown;
}

/** Minimum validated shape required to route a gULP WebSocket message. */
export interface QuerySocketMessage {
	type: string;
	req_id?: string;
	operation_id?: string;
	"gulp.operation_id"?: string;
	payload?: QuerySocketPayload;
	[key: string]: unknown;
}

/** Serializable details for a WebSocket close event. */
export interface QuerySocketCloseDetails {
	code: number;
	reason: string;
	wasClean: boolean;
}

/** Serializable details for a WebSocket or protocol error. */
export interface QuerySocketErrorDetails {
	message: string;
	raw?: string;
}

/** Internal payload passed from the router to the registered persistence owner. */
export interface DocumentChunkReady {
	descriptor: DocumentStreamDescriptor;
	message: QuerySocketMessage;
	sequence?: number;
	/** Conservative UTF-16 size of the original WebSocket frame. */
	rawByteLength: number;
}

/** Durable or accepted progress sent back to the query producer for flow control. */
export interface DocumentChunkAcknowledgement {
	requestId: string;
	queryName: string;
	sequence: number;
	stage: "accepted" | "durable";
}

/** Stream lifecycle update emitted by the router. */
export interface DocumentStreamStateChange {
	descriptor: DocumentStreamDescriptor;
	state: DocumentStreamState;
	error?: string;
}

/** Configuration sent once when the query worker is created. */
export interface QueryWorkerInitialization {
	socketUrl: string;
	/** HTTP API origin/path, independent from the WebSocket URL. */
	apiUrl?: string;
	token: string;
	wsId: string;
	/** Stable backend/user identity for IndexedDB, revision channels, and Web Locks. */
	cacheNamespaceId: string;
	legacyUnknownDocumentPassthrough?: boolean;
}

/** One source-scoped replacement prepared by the application coordinator. */
export interface TimelineQueryRequest {
	descriptor: DocumentStreamDescriptor;
	sourceId: Source.Id;
	desired: DesiredQueryCacheLookup;
	body: Record<string, unknown>;
	cachePolicy: "reuse" | "force";
}

/** Compact lifecycle delivery; event documents never cross this interface. */
export interface TimelineQueryUpdate {
	requestId: string;
	operationId: string;
	operationEpoch: number;
	sourceId: Source.Id;
	state: "started" | "cache-hit" | "failed" | "canceled";
	/** Previous-session history was reconciled before this decision. */
	initialized?: boolean;
	activation?: EventSourceQueryCacheReconciliation;
	error?: string;
	httpStatus?: number;
}

/** Operation-qualified viewport request executable inside QueryWorker. */
export interface QueryRenderWindowDescriptor
	extends EventStoreViewportDescriptor {
	operationId: string;
}

/** Compact source update emitted after an EventStore commit. */
export interface QuerySourceSummaryEvent {
	requestId: string;
	operationId: string;
	operationEpoch: number;
	/** Present on exactly one source update when rollback storage was reclaimed. */
	replacementMode?: "in-place";
	summary: EventSourceSummary;
}

/** Ingestion kinds whose accepted documents change source event totals. */
export type IngestionDocumentStreamKind = Extract<
	DocumentStreamKind,
	"ingestion-build" | "realtime-ingest"
>;

/** Compact source-exact progress emitted after one ingestion chunk is accepted. */
export interface QueryIngestionProgressEvent {
	requestId: string;
	operationId: string;
	operationEpoch: number;
	kind: IngestionDocumentStreamKind;
	/** Cumulative source totals after the accepted chunk, keyed by source ID. */
	sourceEventCounts: Readonly<Record<string, number>>;
}

/** Signals that worker-owned replacement residuals can produce a newer viewport. */
export interface QueryProjectionInvalidationEvent {
	requestId: string;
	operationId: string;
	operationEpoch: number;
	sourceIds: readonly Source.Id[];
}

/** Persistence failure reasons surfaced to the main-thread stream lifecycle. */
export type QueryStreamFailureReason =
	| "backlog"
	| "quota"
	| "storage"
	| "normalization"
	| "disconnected"
	| "superseded";

/** Terminal persistence failure emitted without forwarding document payloads. */
export interface QueryStreamFailureEvent {
	requestId: string;
	operationId: string;
	operationEpoch: number;
	reason: QueryStreamFailureReason;
	message: string;
}

/** On-demand persistence and projection telemetry retained inside QueryWorker. */
export interface QueryWorkerDiagnostics {
	projectionCache?: { exactBytes: number; lodBytes: number; heightBytes: number; refinementBytes: number };
	capturedAtMs: number;
	activeStreams: number;
	loadedOperations: number;
	rawBacklog: {
		currentBytes: number;
		maximumBytes: number;
		maximumStreamBytes: number;
	};
	normalizedBacklog: {
		currentDocuments: number;
		maximumDocuments: number;
	};
	commits: {
		active: number;
		maximumActive: number;
		completed: number;
		averageLatencyMs: number;
		maximumLatencyMs: number;
	};
	pages: {
		created: number;
		documents: number;
		capacity: number;
		fillRatio: number;
	};
	projections: {
		started: number;
		completed: number;
		superseded: number;
		retries: number;
	};
}

interface QueryWorkerControlCommand {
	controlId: string;
}

/** Starts the worker-owned WebSocket connection. */
export interface InitializeQueryWorkerCommand
	extends QueryWorkerControlCommand {
	type: "INITIALIZE";
	initialization: QueryWorkerInitialization;
}

/** Registers ownership before a backend request starts producing chunks. */
export interface RegisterDocumentStreamCommand
	extends QueryWorkerControlCommand {
	type: "REGISTER_STREAM";
	descriptor: DocumentStreamDescriptor;
}

/** Cancels one request and rejects its subsequent document chunks. */
export interface CancelDocumentStreamCommand
	extends QueryWorkerControlCommand {
	type: "CANCEL_STREAM";
	requestId: string;
}

/** Marks one stream complete after its final persistence commit or explicit owner action. */
export interface CompleteDocumentStreamCommand
	extends QueryWorkerControlCommand {
	type: "COMPLETE_STREAM";
	requestId: string;
	finalization?: DocumentStreamFinalization;
}

/** Cancels registered requests belonging to one operation epoch. */
export interface CancelOperationStreamsCommand
	extends QueryWorkerControlCommand {
	type: "CANCEL_OPERATION_STREAMS";
	operationId: string;
	operationEpoch?: number;
}

/** Releases one operation's transient repository and projection context. */
export interface ReleaseQueryOperationCommand extends QueryWorkerControlCommand {
	type: "RELEASE_OPERATION";
	operationId: string;
}

/** Requests one correlated IndexedDB-backed viewport projection. */
export interface RequestQueryRenderWindowCommand {
	type: "REQUEST_RENDER_WINDOW";
	projectionRequestId: string;
	descriptor: QueryRenderWindowDescriptor;
}

/** Requests a correlated snapshot of QueryWorker diagnostics. */
export interface GetQueryDiagnosticsCommand {
	type: "GET_QUERY_DIAGNOSTICS";
	diagnosticsRequestId: string;
}

/** Clears cumulative QueryWorker diagnostics while preserving current gauges. */
export interface ResetQueryDiagnosticsCommand extends QueryWorkerControlCommand {
	type: "RESET_QUERY_DIAGNOSTICS";
}

/** Selects one fresh startup cache by exact fingerprint or covered range. */
export interface ReconcileSourceQueryCacheCommand {
	type: "RECONCILE_SOURCE_QUERY_CACHE";
	cacheRequestId: string;
	operationId: string;
	sourceId: Source.Id;
	desired: DesiredQueryCacheLookup;
}

/** Moves one source-local query-cache history cursor without backend I/O. */
export interface NavigateSourceQueryCacheCommand {
	type: "NAVIGATE_SOURCE_QUERY_CACHE";
	cacheRequestId: string;
	operationId: string;
	sourceId: Source.Id;
	direction: "undo" | "redo";
}

/** Deletes one source cache pool before an authoritative rebuild such as rebase. */
export interface ClearSourceQueryCacheCommand extends QueryWorkerControlCommand {
	type: "CLEAR_SOURCE_QUERY_CACHE";
	operationId: string;
	sourceId: Source.Id;
}

/** Deletes one source's complete EventStore after the backend source is removed. */
export interface DeleteSourceEventStoreCommand extends QueryWorkerControlCommand {
	type: "DELETE_SOURCE_EVENT_STORE";
	operationId: string;
	sourceId: Source.Id;
}

/** Sends an application command through the authenticated WebSocket. */
export interface SendQuerySocketCommand {
	type: "SEND";
	data: object;
}

/** Requests an explicit WebSocket reconnect. */
export interface ReconnectQuerySocketCommand {
	type: "RECONNECT";
}

/** Closes the WebSocket without scheduling another connection. */
export interface CloseQuerySocketCommand {
	type: "CLOSE";
	code?: number;
	reason?: string;
}

/** Commands accepted by the query worker. */
export type QueryWorkerCommand =
	| { type: "START_TIMELINE_QUERIES"; controlId: string; requests: TimelineQueryRequest[] }
	| { type: "SET_TIMELINE_QUERY_PRIORITY"; sourceIds: Source.Id[] }
	| InitializeQueryWorkerCommand
	| RegisterDocumentStreamCommand
	| CancelDocumentStreamCommand
	| CompleteDocumentStreamCommand
	| CancelOperationStreamsCommand
	| ReleaseQueryOperationCommand
	| RequestQueryRenderWindowCommand
	| GetQueryDiagnosticsCommand
	| ResetQueryDiagnosticsCommand
	| ReconcileSourceQueryCacheCommand
	| NavigateSourceQueryCacheCommand
	| ClearSourceQueryCacheCommand
	| DeleteSourceEventStoreCommand
	| SendQuerySocketCommand
	| ReconnectQuerySocketCommand
	| CloseQuerySocketCommand;

/** Completes one acknowledged worker control command. */
export interface QueryWorkerControlResult {
	type: "CONTROL_RESULT";
	controlId: string;
	error?: string;
}

/** Publishes one WebSocket state transition. */
export interface QueryWorkerSocketStateEvent {
	type: "SOCKET_STATE";
	state: QuerySocketState;
}

/** Mirrors the WebSocket open lifecycle event without cloning a DOM Event. */
export interface QueryWorkerSocketOpenEvent {
	type: "SOCKET_OPEN";
}

/** Mirrors the WebSocket close lifecycle event. */
export interface QueryWorkerSocketCloseEvent {
	type: "SOCKET_CLOSE";
	details: QuerySocketCloseDetails;
}

/** Mirrors a WebSocket error lifecycle event. */
export interface QueryWorkerSocketErrorEvent {
	type: "SOCKET_ERROR";
	details: QuerySocketErrorDetails;
}

/** Forwards non-document messages and migration-owned document chunks. */
export interface QueryWorkerForwardMessageEvent {
	type: "FORWARD_MESSAGE";
	message: QuerySocketMessage;
}

/** Publishes one registered stream lifecycle transition. */
export interface QueryWorkerStreamStateEvent {
	type: "STREAM_STATE";
	change: DocumentStreamStateChange;
}

/** Publishes a compact source summary after a durable storage commit. */
export interface QueryWorkerSourceSummaryEvent {
	type: "SOURCE_SUMMARY";
	event: QuerySourceSummaryEvent;
}

/** Publishes source-exact ingestion totals without transferring documents. */
export interface QueryWorkerIngestionProgressEvent {
	type: "INGESTION_PROGRESS";
	event: QueryIngestionProgressEvent;
}

/** Requests a progressive projection after a partial replacement page changed. */
export interface QueryWorkerProjectionInvalidationEvent {
	type: "PROJECTION_INVALIDATED";
	event: QueryProjectionInvalidationEvent;
}

/** Publishes a persistence failure that requires backend cancellation. */
export interface QueryWorkerStreamFailureEvent {
	type: "STREAM_FAILURE";
	event: QueryStreamFailureEvent;
}

/** Completes one correlated viewport request or reports newest-wins supersession. */
export interface QueryWorkerRenderWindowResultEvent {
	type: "RENDER_WINDOW_RESULT";
	projectionRequestId: string;
	projection: RenderWindowProjection | null;
	error?: string;
}

/** Completes one correlated on-demand diagnostics request. */
export interface QueryWorkerDiagnosticsResultEvent {
	type: "QUERY_DIAGNOSTICS_RESULT";
	diagnosticsRequestId: string;
	diagnostics: QueryWorkerDiagnostics;
}

/** Completes one correlated startup reconciliation or history navigation. */
export interface QueryWorkerSourceQueryCacheResultEvent {
	type: "SOURCE_QUERY_CACHE_RESULT";
	cacheRequestId: string;
	result:
		| EventSourceQueryCacheActivation
		| EventSourceQueryCacheReconciliation
		| null;
	error?: string;
}

/** Reports malformed transport input without terminating the socket. */
export interface QueryWorkerProtocolErrorEvent {
	type: "PROTOCOL_ERROR";
	details: QuerySocketErrorDetails;
}

/** Events emitted by the query worker client protocol. */
export type QueryWorkerEvent =
	| { type: "TIMELINE_QUERY_UPDATE"; update: TimelineQueryUpdate }
	| QueryWorkerControlResult
	| QueryWorkerSocketStateEvent
	| QueryWorkerSocketOpenEvent
	| QueryWorkerSocketCloseEvent
	| QueryWorkerSocketErrorEvent
	| QueryWorkerForwardMessageEvent
	| QueryWorkerStreamStateEvent
	| QueryWorkerSourceSummaryEvent
	| QueryWorkerIngestionProgressEvent
	| QueryWorkerProjectionInvalidationEvent
	| QueryWorkerStreamFailureEvent
	| QueryWorkerRenderWindowResultEvent
	| QueryWorkerDiagnosticsResultEvent
	| QueryWorkerSourceQueryCacheResultEvent
	| QueryWorkerProtocolErrorEvent;

/**
 * Checks the minimum message shape required by the stream router.
 * @param value Parsed JSON value to validate.
 * @returns True when the value exposes a string message type.
 */
export function isQuerySocketMessage(value: unknown): value is QuerySocketMessage {
	return (
		typeof value === "object" &&
		value !== null &&
		"type" in value &&
		typeof value.type === "string"
	);
}

/**
 * Extracts the request ID used as a stream routing key.
 * @param message Validated WebSocket message.
 * @returns Non-empty request ID, or undefined when unavailable.
 */
export function getQuerySocketRequestId(
	message: QuerySocketMessage,
): string | undefined {
	return typeof message.req_id === "string" && message.req_id.length > 0
		? message.req_id
		: undefined;
}

/**
 * Extracts an operation ID from either supported WebSocket field name.
 * @param message Validated WebSocket message.
 * @returns Non-empty operation ID, or undefined when unavailable.
 */
export function getQuerySocketOperationId(
	message: QuerySocketMessage,
): string | undefined {
	if (
		typeof message.operation_id === "string" &&
		message.operation_id.length > 0
	) {
		return message.operation_id;
	}
	const aliasedOperationId = message["gulp.operation_id"];
	return typeof aliasedOperationId === "string" && aliasedOperationId.length > 0
		? aliasedOperationId
		: undefined;
}

/**
 * Reads the backend's real `chunk_num` field before its legacy `chunk_number` default.
 * @param message Document chunk whose sequence should be deduplicated.
 * @returns A non-negative integer sequence, or undefined when unavailable.
 */
export function getDocumentChunkSequence(
	message: QuerySocketMessage,
): number | undefined {
	const candidates = [
		message.payload?.chunk_num,
		message.payload?.chunk_number,
	];
	for (const candidate of candidates) {
		if (
			typeof candidate === "number" &&
			Number.isSafeInteger(candidate) &&
			candidate >= 0
		) {
			return candidate;
		}
	}
	return undefined;
}
