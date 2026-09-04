import type {
	DocumentStreamDescriptor,
	DocumentStreamFinalization,
	DocumentStreamStateChange,
	QuerySocketCloseDetails,
	QuerySocketErrorDetails,
	QuerySocketMessage,
	QuerySocketState,
	QueryRenderWindowDescriptor,
	QueryIngestionProgressEvent,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryWorkerDiagnostics,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type {
	EventSourceQueryCacheActivation,
	EventSourceQueryCacheReconciliation,
	DesiredQueryCacheLookup,
} from "@/event-store/EventStore.types";
import type { Source } from "@/entities/Source";

/** Application-facing callbacks shared by worker and direct fallback transports. */
export interface QueryTransportCallbacks {
	onSocketState: (state: QuerySocketState) => void;
	onSocketOpen: () => void;
	onSocketClose: (details: QuerySocketCloseDetails) => void;
	onSocketError: (details: QuerySocketErrorDetails) => void;
	onForwardMessage: (message: QuerySocketMessage) => void;
	onStreamStateChange: (change: DocumentStreamStateChange) => void;
	onSourceSummary: (event: QuerySourceSummaryEvent) => void;
	onIngestionProgress: (event: QueryIngestionProgressEvent) => void;
	onStreamFailure: (event: QueryStreamFailureEvent) => void;
	onProtocolError: (details: QuerySocketErrorDetails) => void;
}

/** Socket facade implemented by a QueryWorker client and its direct test fallback. */
export interface QueryTransport {
	/** Returns the latest worker-owned socket state. */
	readonly state: QuerySocketState;

	/** Resolves once WebSocket authentication reaches the ready state. */
	awaitReady(timeoutMs?: number): Promise<void>;

	/** Registers ownership before a backend request can publish document chunks. */
	registerDocumentStream(descriptor: DocumentStreamDescriptor): Promise<void>;

	/** Cancels one registered document stream. */
	cancelDocumentStream(requestId: string): Promise<void>;

	/** Completes one stream after its final durable owner action. */
	completeDocumentStream(
		requestId: string,
		finalization?: DocumentStreamFinalization,
	): Promise<void>;

	/** Cancels document streams belonging to one operation epoch. */
	cancelOperationStreams(
		operationId: string,
		operationEpoch?: number,
	): Promise<void>;

	/** Closes transient state for one operation without deleting IndexedDB data. */
	releaseOperation(operationId: string): Promise<void>;

	/** Builds the newest requested viewport window from exact IndexedDB data. */
	requestRenderWindow(
		descriptor: QueryRenderWindowDescriptor,
	): Promise<RenderWindowProjection | null>;

	/** Reads QueryWorker counters and gauges only when explicitly requested. */
	getQueryDiagnostics(): Promise<QueryWorkerDiagnostics>;

	/** Clears cumulative QueryWorker diagnostics without changing active work. */
	resetQueryDiagnostics(): Promise<void>;

	/** Chooses and prunes one source's startup query cache. */
	reconcileSourceQueryCache(input: {
		operationId: string;
		sourceId: Source.Id;
		desired: DesiredQueryCacheLookup;
	}): Promise<EventSourceQueryCacheReconciliation | null>;

	/** Moves one source-local query-cache cursor without contacting the backend. */
	navigateSourceQueryCache(input: {
		operationId: string;
		sourceId: Source.Id;
		direction: "undo" | "redo";
	}): Promise<EventSourceQueryCacheActivation | null>;

	/** Clears every persisted query generation for one source. */
	clearSourceQueryCache(operationId: string, sourceId: Source.Id): Promise<void>;

	/** Deletes the complete EventStore state for a removed backend source. */
	deleteSourceEventStore(operationId: string, sourceId: Source.Id): Promise<void>;

	/** Sends one application payload through the WebSocket. */
	send(data: object): void;

	/** Requests a fresh WebSocket connection. */
	reconnect(): void;

	/** Closes the socket without reconnecting. */
	close(code?: number, reason?: string): void;

	/** Permanently releases worker, socket, timers, and pending promises. */
	destroy(): void;
}
