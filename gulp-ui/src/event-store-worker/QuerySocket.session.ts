import {
	DOCUMENT_CHUNK_ACK_MESSAGE_TYPE,
	QueryStreamRouter,
	getQuerySocketOperationId,
	isQuerySocketMessage,
	type DocumentChunkAcknowledgement,
	type DocumentStreamDescriptor,
	type DocumentStreamFinalization,
	type QueryRenderWindowDescriptor,
	type QuerySocketErrorDetails,
	type QuerySocketState,
	type QueryWorkerInitialization,
	type QueryWorkerDiagnostics,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type {
	EventSourceQueryCacheActivation,
	EventSourceQueryCacheReconciliation,
	DesiredQueryCacheLookup,
} from "@/event-store/EventStore.types";
import type { Source } from "@/entities/Source";
import type { QueryTransportCallbacks } from "./QueryTransport.types";
import {
	QueryPersistenceCoordinator,
	type QueryPersistenceCoordinatorOptions,
} from "./QueryPersistenceCoordinator";

const RECONNECT_BASE_DELAY_MS = 500;
const RECONNECT_MAX_DELAY_MS = 10_000;
const MALFORMED_MESSAGE_PREVIEW_LENGTH = 512;

type QuerySocketFactory = (url: string) => WebSocket;

interface RenderWindowRequest {
	descriptor: QueryRenderWindowDescriptor;
	resolve: (projection: RenderWindowProjection | null) => void;
	reject: (error: Error) => void;
	canceled: boolean;
	settled: Promise<void>;
	settle: () => void;
}

/**
 * Owns WebSocket authentication, reconnects, parsing, and request-aware routing.
 * It runs inside QueryWorker in browsers and on the main thread only as a fallback.
 */
export class QuerySocketSession {
	private readonly initialization: QueryWorkerInitialization;
	private readonly callbacks: QueryTransportCallbacks;
	private readonly socketFactory: QuerySocketFactory;
	private readonly router: QueryStreamRouter;
	private readonly persistence: QueryPersistenceCoordinator;
	private socket: WebSocket | null = null;
	private socketState: QuerySocketState = "disconnected";
	private reconnectAttempts = 0;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private mailboxSweepTimer: ReturnType<typeof setTimeout> | null = null;
	private manuallyClosed = false;
	private activeRenderWindowRequest: RenderWindowRequest | null = null;
	private pendingRenderWindowRequest: RenderWindowRequest | null = null;
	private readonly backgroundPersistenceCleanups = new Set<Promise<unknown>>();
	private projectionStartedCount = 0;
	private projectionCompletedCount = 0;
	private projectionSupersededCount = 0;
	private destroyed = false;

	/**
	 * Creates a transport session without opening its WebSocket yet.
	 * @param initialization URL, authentication, and migration routing settings.
	 * @param callbacks Serializable transport delivery callbacks.
	 * @param socketFactory Injectable WebSocket factory used by the direct fallback tests.
	 * @param persistenceOptions Injectable EventStore dependencies and limits.
	 */
	public constructor(
		initialization: QueryWorkerInitialization,
		callbacks: QueryTransportCallbacks,
		socketFactory: QuerySocketFactory = (url) => new WebSocket(url),
		persistenceOptions: QueryPersistenceCoordinatorOptions = {},
	) {
		this.initialization = initialization;
		this.callbacks = callbacks;
		this.socketFactory = socketFactory;
		this.router = new QueryStreamRouter({
			onForwardMessage: callbacks.onForwardMessage,
			onDocumentChunkReady: (chunk) =>
				this.persistence.handleDocumentChunk(chunk),
			onStreamStateChange: callbacks.onStreamStateChange,
			onUnknownDocumentStream: (requestId, message) => {
				if (!this.initialization.legacyUnknownDocumentPassthrough) return;
				this.router.register({
					requestId,
					operationId:
						getQuerySocketOperationId(message) ??
						`legacy:${this.initialization.wsId}`,
					operationEpoch: 0,
					owner: "passthrough",
					sourceSettingsById: {},
				});
			},
		});
		this.persistence = new QueryPersistenceCoordinator(
			initialization.cacheNamespaceId,
			{
				onSourceSummary: callbacks.onSourceSummary,
				onIngestionProgress: callbacks.onIngestionProgress,
				onProjectionInvalidation: callbacks.onProjectionInvalidation,
				onStreamFailure: (event) => {
					this.router.fail(event.requestId, event.message);
					callbacks.onStreamFailure(event);
				},
				onStreamFlushing: (requestId) => this.router.flush(requestId),
				onStreamCompleted: (requestId) => this.router.complete(requestId),
				onDocumentChunkAcknowledged: (acknowledgement) =>
					this.sendDocumentChunkAcknowledgement(acknowledgement),
			},
			persistenceOptions,
		);
	}

	/** Returns the latest WebSocket state published by this session. */
	public get state(): QuerySocketState {
		return this.socketState;
	}

	/** Opens the worker-owned WebSocket connection. */
	public connect(): void {
		this.manuallyClosed = false;
		this.clearReconnectTimer();
		this.openSocket();
	}

	/**
	 * Registers a document stream and synchronously replays its early mailbox.
	 * @param descriptor Request identity and ownership policy.
	 */
	public async registerDocumentStream(
		descriptor: DocumentStreamDescriptor,
	): Promise<void> {
		if (descriptor.owner === "persist") {
			await this.persistence.register(descriptor);
		}
		this.router.register(descriptor);
		this.scheduleMailboxSweep();
	}

	/**
	 * Cancels one stream and drops later document chunks for its request ID.
	 * @param requestId Request identifier to cancel.
	 */
	public cancelDocumentStream(requestId: string): Promise<void> {
		this.router.cancel(requestId);
		this.scheduleMailboxSweep();
		this.trackPersistenceCleanup(this.persistence.cancel(requestId));
		return Promise.resolve();
	}

	/**
	 * Completes one stream after its final persistence or owner action succeeds.
	 * @param requestId Request identifier whose stream is complete.
	 * @param finalization Optional terminal source-cache metadata.
	 */
	public async completeDocumentStream(
		requestId: string,
		finalization?: DocumentStreamFinalization,
	): Promise<void> {
		const handled = await this.persistence.complete(requestId, finalization);
		if (!handled) this.router.complete(requestId);
	}

	/**
	 * Cancels active streams belonging to an operation or one of its epochs.
	 * @param operationId Operation identifier to cancel.
	 * @param operationEpoch Optional epoch guard.
	 */
	public cancelOperationStreams(
		operationId: string,
		operationEpoch?: number,
	): Promise<void> {
		this.router.cancelOperation(operationId, operationEpoch);
		this.scheduleMailboxSweep();
		this.trackPersistenceCleanup(
			this.persistence.cancelOperation(operationId, operationEpoch),
		);
		return Promise.resolve();
	}

	/**
	 * Cancels operation streams, drains an active projection, and closes its IDB context.
	 * Stored IndexedDB data is preserved for a later operation revisit.
	 * @param operationId Operation whose transient worker context should be released.
	 */
	public async releaseOperation(operationId: string): Promise<void> {
		this.router.cancelOperation(operationId);
		this.scheduleMailboxSweep();
		const cancellation = this.persistence.cancelOperation(operationId);
		const activeRequest = this.activeRenderWindowRequest;
		const activeSettlement =
			activeRequest?.descriptor.operationId === operationId
				? activeRequest.settled
				: null;
		if (activeSettlement && activeRequest) activeRequest.canceled = true;
		if (this.pendingRenderWindowRequest?.descriptor.operationId === operationId) {
			this.cancelRenderWindowRequest(this.pendingRenderWindowRequest);
			this.pendingRenderWindowRequest = null;
		}
		await cancellation;
		if (activeSettlement) await activeSettlement;
		await this.persistence.releaseOperation(operationId);
	}

	/**
	 * Schedules one viewport projection with a single active and newest pending slot.
	 * @param descriptor Operation-qualified viewport transform.
	 * @returns Projection, or null when a newer pending request supersedes this one.
	 */
	public requestRenderWindow(
		descriptor: QueryRenderWindowDescriptor,
	): Promise<RenderWindowProjection | null> {
		if (this.destroyed) {
			return Promise.reject(new Error("Query socket session is destroyed."));
		}
		return new Promise<RenderWindowProjection | null>((resolve, reject) => {
			/** Releases operation-drain waiters after this request settles. */
			let settle = (): void => undefined;
			const settled = new Promise<void>((settledResolve) => {
				settle = settledResolve;
			});
			const request: RenderWindowRequest = {
				descriptor,
				resolve,
				reject,
				canceled: false,
				settled,
				settle,
			};
			if (!this.activeRenderWindowRequest) {
				this.startRenderWindowRequest(request);
				return;
			}
			if (this.pendingRenderWindowRequest) {
				this.cancelRenderWindowRequest(this.pendingRenderWindowRequest);
			}
			this.pendingRenderWindowRequest = request;
		});
	}

	/**
	 * Reads persistence and projection diagnostics only for an explicit caller.
	 * @returns Detached QueryWorker diagnostics snapshot.
	 */
	public getQueryDiagnostics(): QueryWorkerDiagnostics {
		return this.persistence.getDiagnostics({
			started: this.projectionStartedCount,
			completed: this.projectionCompletedCount,
			superseded: this.projectionSupersededCount,
			retries: 0,
		});
	}

	/** Clears cumulative diagnostics while preserving active session state. */
	public resetQueryDiagnostics(): void {
		this.projectionStartedCount = 0;
		this.projectionCompletedCount = 0;
		this.projectionSupersededCount = 0;
		this.persistence.resetDiagnostics();
	}

	/**
	 * Selects the safest exact, range-covered, or latest cache for a source.
	 * @param input Operation, source, and desired exact/shape/range lookup.
	 * @returns Reconciled cache activation, or null when no cache is reusable.
	 */
	public reconcileSourceQueryCache(input: {
		operationId: string;
		sourceId: Source.Id;
		desired: DesiredQueryCacheLookup;
	}): Promise<EventSourceQueryCacheReconciliation | null> {
		return this.persistence.reconcileSourceQueryCache(input);
	}

	/**
	 * Moves one source-local query-cache history cursor without backend I/O.
	 * @param input Operation, source, and Undo or Redo direction.
	 * @returns Activated cache, or null at the corresponding history boundary.
	 */
	public navigateSourceQueryCache(input: {
		operationId: string;
		sourceId: Source.Id;
		direction: "undo" | "redo";
	}): Promise<EventSourceQueryCacheActivation | null> {
		return this.persistence.navigateSourceQueryCache(input);
	}

	/**
	 * Deletes one source's complete query-cache pool before an authoritative rebuild.
	 * @param operationId Operation owning the persisted source.
	 * @param sourceId Source whose cache data must be removed.
	 * @returns Promise resolved after bounded cleanup finishes.
	 */
	public clearSourceQueryCache(
		operationId: string,
		sourceId: Source.Id,
	): Promise<void> {
		return this.persistence.clearSourceQueryCache(operationId, sourceId);
	}

	/**
	 * Deletes every EventStore record for a source removed from the backend.
	 * @param operationId Operation owning the source database.
	 * @param sourceId Removed source whose complete local state must be deleted.
	 * @returns Promise resolved after bounded cleanup finishes.
	 */
	public deleteSourceEventStore(
		operationId: string,
		sourceId: Source.Id,
	): Promise<void> {
		return this.persistence.deleteSourceEventStore(operationId, sourceId);
	}

	/**
	 * Sends one JSON application payload while the underlying socket is open.
	 * @param data Serializable payload to send.
	 * @returns True when the payload was written to the WebSocket.
	 */
	public send(data: object): boolean {
		if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return false;
		try {
			this.socket.send(JSON.stringify(data));
			return true;
		} catch (error) {
			this.reportProtocolError(error);
			return false;
		}
	}

	/**
	 * Sends one negotiated query flow-control acknowledgement to the backend.
	 * @param acknowledgement Request, child query, sequence, and progress stage.
	 * @returns Nothing; send failures use the existing protocol-error callback.
	 */
	private sendDocumentChunkAcknowledgement(
		acknowledgement: DocumentChunkAcknowledgement,
	): void {
		this.send({
			type: DOCUMENT_CHUNK_ACK_MESSAGE_TYPE,
			req_id: acknowledgement.requestId,
			name: acknowledgement.queryName,
			chunk_num: acknowledgement.sequence,
			stage: acknowledgement.stage,
		});
	}

	/** Closes the current socket and schedules a new connection. */
	public reconnect(): void {
		this.manuallyClosed = false;
		this.clearReconnectTimer();
		try {
			this.socket?.close();
		} catch (error) {
			this.reportProtocolError(error);
		}
		this.scheduleReconnect();
	}

	/**
	 * Closes the current socket without scheduling another connection.
	 * @param code Optional WebSocket close code.
	 * @param reason Optional WebSocket close reason.
	 */
	public close(code?: number, reason?: string): void {
		this.manuallyClosed = true;
		this.clearReconnectTimer();
		try {
			this.socket?.close(code, reason);
		} catch (error) {
			this.reportProtocolError(error);
		}
	}

	/** Permanently releases socket, reconnect timer, and retained stream data. */
	public destroy(): void {
		if (this.destroyed) return;
		this.destroyed = true;
		this.close();
		this.clearMailboxSweepTimer();
		if (this.activeRenderWindowRequest) {
			this.activeRenderWindowRequest.canceled = true;
			this.activeRenderWindowRequest.resolve(null);
		}
		if (this.pendingRenderWindowRequest) {
			this.cancelRenderWindowRequest(this.pendingRenderWindowRequest);
		}
		this.pendingRenderWindowRequest = null;
		this.persistence.destroy();
		this.router.clear();
		this.socket = null;
		this.setState("disconnected");
	}

	/**
	 * Executes one active projection and promotes the newest pending request afterward.
	 * @param request Request occupying the active projection slot.
	 */
	private startRenderWindowRequest(request: RenderWindowRequest): void {
		this.activeRenderWindowRequest = request;
		this.projectionStartedCount += 1;
		void this.persistence
			.requestRenderWindow(
				request.descriptor,
				() =>
					!this.destroyed &&
					!request.canceled &&
					this.activeRenderWindowRequest === request &&
					this.doesPendingRequestPreserveViewport(request),
			)
			.then(
				(projection) => {
					const accepted =
						!this.destroyed && !request.canceled && projection !== null;
					if (accepted) this.projectionCompletedCount += 1;
					else this.projectionSupersededCount += 1;
					request.resolve(accepted ? projection : null);
				},
				(error: unknown) =>
					request.reject(
						error instanceof Error ? error : new Error(String(error)),
					),
			)
			.finally(() => {
				request.settle();
				if (this.activeRenderWindowRequest === request) {
					this.activeRenderWindowRequest = null;
				}
				if (this.destroyed) return;
				const pending = this.pendingRenderWindowRequest;
				this.pendingRenderWindowRequest = null;
				if (pending) this.startRenderWindowRequest(pending);
			});
	}

	/**
	 * Keeps same-viewport projections valid while a trailing refresh waits, but
	 * invalidates active storage work immediately at a real viewport boundary.
	 * @param request Active projection request being guarded.
	 * @returns True when no pending request changes operation, epoch, or viewport.
	 */
	private doesPendingRequestPreserveViewport(
		request: RenderWindowRequest,
	): boolean {
		const pending = this.pendingRenderWindowRequest;
		if (!pending) return true;
		return (
			pending.descriptor.operationId === request.descriptor.operationId &&
			pending.descriptor.operationEpoch === request.descriptor.operationEpoch &&
			pending.descriptor.viewportRevision ===
				request.descriptor.viewportRevision
		);
	}

	/**
	 * Resolves a render request that must never start or publish its projection.
	 * @param request Pending or invalidated render request.
	 */
	private cancelRenderWindowRequest(request: RenderWindowRequest): void {
		if (request.canceled) return;
		request.canceled = true;
		this.projectionSupersededCount += 1;
		request.resolve(null);
		request.settle();
	}

	/**
	 * Keeps best-effort persistence cleanup alive after the control ACK is published.
	 * @param cleanup Cancellation cleanup that may outlive its main-thread command.
	 */
	private trackPersistenceCleanup(cleanup: Promise<unknown>): void {
		this.backgroundPersistenceCleanups.add(cleanup);
		void cleanup
			.catch((error: unknown) => this.reportProtocolError(error))
			.finally(() => this.backgroundPersistenceCleanups.delete(cleanup));
	}

	/** Creates one WebSocket and attaches generation-safe event handlers. */
	private openSocket(): void {
		this.setState("connecting");
		let socket: WebSocket;
		try {
			socket = this.socketFactory(this.initialization.socketUrl);
		} catch (error) {
			this.setState("disconnected");
			this.reportProtocolError(error);
			this.scheduleReconnect();
			return;
		}

		this.socket = socket;
		socket.onopen = () => this.handleSocketOpen(socket);
		socket.onclose = (event) => this.handleSocketClose(socket, event);
		socket.onerror = () => this.handleSocketError(socket);
		socket.onmessage = (event) => this.handleSocketMessage(socket, event.data);
	}

	/**
	 * Authenticates one current socket and publishes its open event.
	 * @param socket Socket whose open event fired.
	 */
	private handleSocketOpen(socket: WebSocket): void {
		if (socket !== this.socket) return;
		this.reconnectAttempts = 0;
		this.clearReconnectTimer();
		this.setState("authenticating");
		try {
				socket.send(
					JSON.stringify({
						token: this.initialization.token,
						ws_id: this.initialization.wsId,
						data: {
							query_chunk_ack_window: 2,
						},
					}),
			);
		} catch (error) {
			this.reportProtocolError(error);
		}
		this.callbacks.onSocketOpen();
	}

	/**
	 * Publishes one current socket close and schedules reconnect when appropriate.
	 * @param socket Socket whose close event fired.
	 * @param event Native close event to serialize.
	 */
	private handleSocketClose(socket: WebSocket, event: CloseEvent): void {
		if (socket !== this.socket) return;
		this.setState("disconnected");
		void this.persistence.handleDisconnect();
		this.callbacks.onSocketClose({
			code: event.code,
			reason: event.reason,
			wasClean: event.wasClean,
		});
		if (!this.manuallyClosed) this.scheduleReconnect();
	}

	/**
	 * Publishes one generic error for the current WebSocket.
	 * @param socket Socket whose error event fired.
	 */
	private handleSocketError(socket: WebSocket): void {
		if (socket !== this.socket) return;
		this.callbacks.onSocketError({ message: "WebSocket transport error." });
	}

	/**
	 * Parses and routes one text message entirely in the session's current realm.
	 * @param socket Socket that delivered the message.
	 * @param rawData Raw WebSocket message data.
	 */
	private handleSocketMessage(socket: WebSocket, rawData: unknown): void {
		if (socket !== this.socket) return;
		if (typeof rawData !== "string") {
			this.callbacks.onProtocolError({
				message: "Query WebSocket received a non-text message.",
			});
			return;
		}

		let parsed: unknown;
		try {
			parsed = JSON.parse(rawData) as unknown;
		} catch (error) {
			this.callbacks.onProtocolError({
				message: this.getErrorMessage(error),
				raw: rawData.slice(0, MALFORMED_MESSAGE_PREVIEW_LENGTH),
			});
			return;
		}
		if (!isQuerySocketMessage(parsed)) {
			this.callbacks.onProtocolError({
				message: "Query WebSocket message is missing a string type.",
				raw: rawData.slice(0, MALFORMED_MESSAGE_PREVIEW_LENGTH),
			});
			return;
		}

		try {
			this.router.route(parsed, rawData.length * 2);
			this.scheduleMailboxSweep();
			if (parsed.type === "ws_connected") this.setState("ready");
		} catch (error) {
			this.reportProtocolError(error);
		}
	}

	/** Schedules one bounded exponential reconnect attempt. */
	private scheduleReconnect(): void {
		if (this.manuallyClosed || this.reconnectTimer) return;
		const delay = Math.min(
			RECONNECT_BASE_DELAY_MS * 2 ** this.reconnectAttempts,
			RECONNECT_MAX_DELAY_MS,
		);
		this.reconnectAttempts += 1;
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;
			this.openSocket();
		}, delay);
	}

	/** Cancels the currently scheduled reconnect timer, if any. */
	private clearReconnectTimer(): void {
		if (!this.reconnectTimer) return;
		clearTimeout(this.reconnectTimer);
		this.reconnectTimer = null;
	}

	/** Schedules exact expiry of the oldest chunk in the unknown-stream mailbox. */
	private scheduleMailboxSweep(): void {
		this.clearMailboxSweepTimer();
		const nextExpiry = this.router.nextBufferedExpiryAt;
		if (nextExpiry === undefined) return;
		this.mailboxSweepTimer = setTimeout(() => {
			this.mailboxSweepTimer = null;
			this.router.sweepExpired();
			this.scheduleMailboxSweep();
		}, Math.max(0, nextExpiry - Date.now()));
	}

	/** Cancels the pending unknown-stream mailbox expiry timer, if any. */
	private clearMailboxSweepTimer(): void {
		if (!this.mailboxSweepTimer) return;
		clearTimeout(this.mailboxSweepTimer);
		this.mailboxSweepTimer = null;
	}

	/**
	 * Publishes a socket state only when it differs from the previous state.
	 * @param state New socket state.
	 */
	private setState(state: QuerySocketState): void {
		if (this.socketState === state) return;
		this.socketState = state;
		this.callbacks.onSocketState(state);
	}

	/**
	 * Converts an unknown thrown value into stable error details.
	 * @param error Thrown value to describe.
	 * @returns Human-readable message.
	 */
	private getErrorMessage(error: unknown): string {
		return error instanceof Error ? error.message : String(error);
	}

	/**
	 * Publishes an unknown thrown value as a protocol error.
	 * @param error Thrown value to report.
	 */
	private reportProtocolError(error: unknown): void {
		const details: QuerySocketErrorDetails = {
			message: this.getErrorMessage(error),
		};
		this.callbacks.onProtocolError(details);
	}
}
