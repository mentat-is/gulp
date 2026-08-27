import type {
	DocumentStreamDescriptor,
	QueryRenderWindowDescriptor,
	QuerySocketState,
	QueryWorkerInitialization,
	QueryWorkerDiagnostics,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type {
	EventSourceQueryCacheActivation,
	EventSourceQueryCacheReconciliation,
	DesiredQueryCacheLookup,
} from "@/event-store/EventStore.types";
import type { Source } from "@/entities/Source";
import { QuerySocketSession } from "./QuerySocket.session";
import type { QueryPersistenceCoordinatorOptions } from "./QueryPersistenceCoordinator";
import type {
	QueryTransport,
	QueryTransportCallbacks,
} from "./QueryTransport.types";

const READY_TIMEOUT_MS = 10_000;

interface DirectReadyWaiter {
	resolve: () => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
}

/**
 * Main-thread transport used only by the explicit legacy rollback path when
 * browser workers are unavailable. It is not a scale-mode fallback.
 */
export class DirectQueryTransport implements QueryTransport {
	private readonly session: QuerySocketSession;
	private readonly readyWaiters = new Set<DirectReadyWaiter>();
	private socketState: QuerySocketState = "disconnected";
	private destroyed = false;

	/**
	 * Creates and immediately connects the direct WebSocket fallback.
	 * @param initialization WebSocket URL, credentials, and migration options.
	 * @param callbacks Application-facing transport callbacks.
	 * @param persistenceOptions Injectable persistence dependencies for tests.
	 */
	public constructor(
		initialization: QueryWorkerInitialization,
		callbacks: QueryTransportCallbacks,
		persistenceOptions: QueryPersistenceCoordinatorOptions = {},
	) {
		this.session = new QuerySocketSession(
			initialization,
			{
				...callbacks,
				onSocketState: (state) => {
					this.socketState = state;
					callbacks.onSocketState(state);
					if (state === "ready") this.resolveReadyWaiters();
				},
			},
			undefined,
			persistenceOptions,
		);
		this.session.connect();
	}

	/** Returns the latest state of the direct WebSocket fallback. */
	public get state(): QuerySocketState {
		return this.socketState;
	}

	/**
	 * Resolves after the authenticated `ws_connected` message is received.
	 * @param timeoutMs Maximum wait before rejecting, in milliseconds.
	 * @returns Promise resolved when the direct socket becomes ready.
	 */
	public awaitReady(timeoutMs = READY_TIMEOUT_MS): Promise<void> {
		if (this.socketState === "ready") return Promise.resolve();
		if (this.destroyed) {
			return Promise.reject(new Error("Direct query transport is destroyed."));
		}
		return new Promise<void>((resolve, reject) => {
			const waiter: DirectReadyWaiter = {
				resolve,
				reject,
				timer: setTimeout(() => {
					this.readyWaiters.delete(waiter);
					reject(new Error("Timeout waiting for query socket readiness."));
				}, Math.max(0, timeoutMs)),
			};
			this.readyWaiters.add(waiter);
		});
	}

	/**
	 * Registers one request and replays its early document chunks.
	 * @param descriptor Request identity and ownership policy.
	 * @returns Promise resolved once registration is installed.
	 */
	public registerDocumentStream(
		descriptor: DocumentStreamDescriptor,
	): Promise<void> {
		return this.session.registerDocumentStream(descriptor);
	}

	/**
	 * Cancels one request in the direct stream router.
	 * @param requestId Request identifier to cancel.
	 * @returns Already-resolved cancellation promise.
	 */
	public cancelDocumentStream(requestId: string): Promise<void> {
		return this.session.cancelDocumentStream(requestId);
	}

	/**
	 * Completes one stream after its final durable owner action succeeds.
	 * @param requestId Request identifier whose stream is complete.
	 * @returns Already-resolved completion promise.
	 */
	public completeDocumentStream(requestId: string): Promise<void> {
		return this.session.completeDocumentStream(requestId);
	}

	/**
	 * Cancels direct streams belonging to an operation or epoch.
	 * @param operationId Operation identifier to cancel.
	 * @param operationEpoch Optional epoch guard.
	 * @returns Already-resolved cancellation promise.
	 */
	public cancelOperationStreams(
		operationId: string,
		operationEpoch?: number,
	): Promise<void> {
		return this.session.cancelOperationStreams(operationId, operationEpoch);
	}

	/**
	 * Closes one direct operation context without deleting its IndexedDB database.
	 * @param operationId Operation context to release.
	 */
	public releaseOperation(operationId: string): Promise<void> {
		return this.session.releaseOperation(operationId);
	}

	/**
	 * Builds one viewport projection in the direct fallback realm.
	 * @param descriptor Operation-qualified viewport request.
	 * @returns Projection, or null when superseded in the pending slot.
	 */
	public requestRenderWindow(
		descriptor: QueryRenderWindowDescriptor,
	): Promise<RenderWindowProjection | null> {
		return this.session.requestRenderWindow(descriptor);
	}

	/** Returns an on-demand diagnostics snapshot from the direct session. */
	public getQueryDiagnostics(): Promise<QueryWorkerDiagnostics> {
		return Promise.resolve(this.session.getQueryDiagnostics());
	}

	/** Resets direct-session cumulative diagnostics without changing active work. */
	public resetQueryDiagnostics(): Promise<void> {
		this.session.resetQueryDiagnostics();
		return Promise.resolve();
	}

	/**
	 * Selects the safest exact, range-covered, or latest cache in direct mode.
	 * @param input Operation, source, and desired exact/shape/range lookup.
	 * @returns Reconciled cache activation, or null when no cache is reusable.
	 */
	public reconcileSourceQueryCache(input: {
		operationId: string;
		sourceId: Source.Id;
		desired: DesiredQueryCacheLookup;
	}): Promise<EventSourceQueryCacheReconciliation | null> {
		return this.session.reconcileSourceQueryCache(input);
	}

	/**
	 * Moves one source-local query-cache cursor in direct mode.
	 * @param input Operation, source, and Undo or Redo direction.
	 * @returns Activated cache, or null at the corresponding history boundary.
	 */
	public navigateSourceQueryCache(input: {
		operationId: string;
		sourceId: Source.Id;
		direction: "undo" | "redo";
	}): Promise<EventSourceQueryCacheActivation | null> {
		return this.session.navigateSourceQueryCache(input);
	}

	/**
	 * Clears every retained query cache for one source in direct mode.
	 * @param operationId Operation owning the source database.
	 * @param sourceId Source whose cache pool must be deleted.
	 * @returns Promise resolved after bounded cleanup finishes.
	 */
	public clearSourceQueryCache(
		operationId: string,
		sourceId: Source.Id,
	): Promise<void> {
		return this.session.clearSourceQueryCache(operationId, sourceId);
	}

	/**
	 * Deletes every EventStore record for a removed source in direct mode.
	 * @param operationId Operation owning the source database.
	 * @param sourceId Removed source whose complete local state must be deleted.
	 * @returns Promise resolved after bounded cleanup finishes.
	 */
	public deleteSourceEventStore(
		operationId: string,
		sourceId: Source.Id,
	): Promise<void> {
		return this.session.deleteSourceEventStore(operationId, sourceId);
	}

	/**
	 * Sends one application payload through the direct WebSocket.
	 * @param data Serializable payload to send.
	 */
	public send(data: object): void {
		this.session.send(data);
	}

	/** Requests an explicit reconnect from the direct WebSocket session. */
	public reconnect(): void {
		this.session.reconnect();
	}

	/**
	 * Closes the direct socket without scheduling another connection.
	 * @param code Optional WebSocket close code.
	 * @param reason Optional WebSocket close reason.
	 */
	public close(code?: number, reason?: string): void {
		this.rejectReadyWaiters("Query socket was closed.");
		this.session.close(code, reason);
	}

	/** Permanently releases the direct socket, router, timers, and waiters. */
	public destroy(): void {
		if (this.destroyed) return;
		this.destroyed = true;
		this.session.destroy();
		this.rejectReadyWaiters("Direct query transport was destroyed.");
	}

	/** Resolves and clears every pending socket readiness waiter. */
	private resolveReadyWaiters(): void {
		for (const waiter of this.readyWaiters) {
			clearTimeout(waiter.timer);
			waiter.resolve();
		}
		this.readyWaiters.clear();
	}

	/**
	 * Rejects and clears every pending socket readiness waiter.
	 * @param message Stable rejection reason.
	 */
	private rejectReadyWaiters(message: string): void {
		for (const waiter of this.readyWaiters) {
			clearTimeout(waiter.timer);
			waiter.reject(new Error(message));
		}
		this.readyWaiters.clear();
	}
}
