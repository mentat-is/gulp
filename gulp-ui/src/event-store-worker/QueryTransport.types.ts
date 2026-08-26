import type {
	DocumentStreamDescriptor,
	DocumentStreamStateChange,
	QuerySocketCloseDetails,
	QuerySocketErrorDetails,
	QuerySocketMessage,
	QuerySocketState,
	QueryRenderWindowDescriptor,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryWorkerDiagnostics,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";

/** Application-facing callbacks shared by worker and direct fallback transports. */
export interface QueryTransportCallbacks {
	onSocketState: (state: QuerySocketState) => void;
	onSocketOpen: () => void;
	onSocketClose: (details: QuerySocketCloseDetails) => void;
	onSocketError: (details: QuerySocketErrorDetails) => void;
	onForwardMessage: (message: QuerySocketMessage) => void;
	onStreamStateChange: (change: DocumentStreamStateChange) => void;
	onSourceSummary: (event: QuerySourceSummaryEvent) => void;
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
	completeDocumentStream(requestId: string): Promise<void>;

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

	/** Sends one application payload through the WebSocket. */
	send(data: object): void;

	/** Requests a fresh WebSocket connection. */
	reconnect(): void;

	/** Closes the socket without reconnecting. */
	close(code?: number, reason?: string): void;

	/** Permanently releases worker, socket, timers, and pending promises. */
	destroy(): void;
}
