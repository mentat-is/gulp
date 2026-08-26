import type {
	DocumentStreamDescriptor,
	QueryRenderWindowDescriptor,
	QuerySocketState,
	QueryWorkerCommand,
	QueryWorkerEvent,
	QueryWorkerInitialization,
	QueryWorkerDiagnostics,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type {
	QueryTransport,
	QueryTransportCallbacks,
} from "./QueryTransport.types";

const CONTROL_TIMEOUT_MS = 10_000;
const READY_TIMEOUT_MS = 10_000;

interface QueryWorkerLike {
	onmessage: ((event: MessageEvent<QueryWorkerEvent>) => void) | null;
	onerror: ((event: ErrorEvent) => void) | null;
	postMessage(message: QueryWorkerCommand): void;
	terminate(): void;
}

interface PendingControl {
	resolve: () => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout> | null;
}

interface ReadyWaiter {
	resolve: () => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
}

interface PendingRenderWindow {
	resolve: (projection: RenderWindowProjection | null) => void;
	reject: (error: Error) => void;
}

interface PendingDiagnostics {
	resolve: (diagnostics: QueryWorkerDiagnostics) => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
}

/** Main-thread facade for the browser QueryWorker protocol. */
export class QueryWorkerClient implements QueryTransport {
	private readonly worker: QueryWorkerLike;
	private readonly callbacks: QueryTransportCallbacks;
	private readonly pendingControls = new Map<string, PendingControl>();
	private readonly readyWaiters = new Set<ReadyWaiter>();
	private readonly pendingRenderWindows = new Map<string, PendingRenderWindow>();
	private readonly pendingDiagnostics = new Map<string, PendingDiagnostics>();
	private socketState: QuerySocketState = "disconnected";
	private controlCounter = 0;
	private projectionCounter = 0;
	private diagnosticsCounter = 0;
	private destroyed = false;

	/**
	 * Attaches a worker and starts its WebSocket session.
	 * @param worker QueryWorker-compatible worker instance.
	 * @param initialization WebSocket URL, credentials, and migration options.
	 * @param callbacks Application-facing transport callbacks.
	 */
	public constructor(
		worker: QueryWorkerLike,
		initialization: QueryWorkerInitialization,
		callbacks: QueryTransportCallbacks,
	) {
		this.worker = worker;
		this.callbacks = callbacks;
		this.worker.onmessage = (event) => this.handleWorkerEvent(event.data);
		this.worker.onerror = (event) => {
			this.callbacks.onProtocolError({
				message: event.message || "QueryWorker execution failed.",
			});
			this.rejectPendingControls("QueryWorker execution failed.");
			this.rejectPendingRenderWindows("QueryWorker execution failed.");
			this.rejectPendingDiagnostics("QueryWorker execution failed.");
			this.rejectReadyWaiters("QueryWorker execution failed.");
		};

		const command: QueryWorkerCommand = {
			type: "INITIALIZE",
			controlId: this.nextControlId(),
			initialization,
		};
		void this.sendControl(command).catch((error: unknown) => {
			if (this.destroyed) return;
			const message =
				error instanceof Error ? error.message : String(error);
			this.destroyed = true;
			this.worker.terminate();
			this.callbacks.onProtocolError({ message });
			this.rejectPendingControls(message);
			this.rejectPendingRenderWindows(message);
			this.rejectPendingDiagnostics(message);
			this.rejectReadyWaiters(message);
		});
	}

	/** Returns the latest socket state published by QueryWorker. */
	public get state(): QuerySocketState {
		return this.socketState;
	}

	/**
	 * Resolves after the worker receives the authenticated `ws_connected` message.
	 * @param timeoutMs Maximum wait before rejecting, in milliseconds.
	 * @returns Promise resolved when the socket becomes ready.
	 */
	public awaitReady(timeoutMs = READY_TIMEOUT_MS): Promise<void> {
		if (this.socketState === "ready") return Promise.resolve();
		if (this.destroyed) {
			return Promise.reject(new Error("QueryWorker transport is destroyed."));
		}
		return new Promise<void>((resolve, reject) => {
			const waiter: ReadyWaiter = {
				resolve,
				reject,
				timer: setTimeout(() => {
					this.readyWaiters.delete(waiter);
					reject(new Error("Timeout waiting for QueryWorker socket readiness."));
				}, Math.max(0, timeoutMs)),
			};
			this.readyWaiters.add(waiter);
		});
	}

	/**
	 * Registers one request before its backend call can publish document chunks.
	 * @param descriptor Request identity and document ownership policy.
	 * @returns Promise resolved after registration and early-chunk replay.
	 */
	public registerDocumentStream(
		descriptor: DocumentStreamDescriptor,
	): Promise<void> {
		return this.sendControl({
			type: "REGISTER_STREAM",
			controlId: this.nextControlId(),
			descriptor,
		});
	}

	/**
	 * Cancels one registered request in the QueryWorker router.
	 * @param requestId Request identifier to cancel.
	 * @returns Promise resolved when cancellation is installed.
	 */
	public cancelDocumentStream(requestId: string): Promise<void> {
		return this.sendControl({
			type: "CANCEL_STREAM",
			controlId: this.nextControlId(),
			requestId,
		});
	}

	/**
	 * Completes one stream after its final durable owner action succeeds.
	 * @param requestId Request identifier whose stream is complete.
	 * @returns Promise resolved after QueryWorker publishes the completed state.
	 */
	public completeDocumentStream(requestId: string): Promise<void> {
		return this.sendControl({
			type: "COMPLETE_STREAM",
			controlId: this.nextControlId(),
			requestId,
		});
	}

	/**
	 * Cancels document streams belonging to one operation or epoch.
	 * @param operationId Operation identifier to cancel.
	 * @param operationEpoch Optional epoch guard.
	 * @returns Promise resolved after matching streams are marked canceled.
	 */
	public cancelOperationStreams(
		operationId: string,
		operationEpoch?: number,
	): Promise<void> {
		return this.sendControl({
			type: "CANCEL_OPERATION_STREAMS",
			controlId: this.nextControlId(),
			operationId,
			operationEpoch,
		});
	}

	/**
	 * Releases one operation's transient worker repository after projections drain.
	 * @param operationId Operation context to close without deleting IndexedDB data.
	 */
	public releaseOperation(operationId: string): Promise<void> {
		return this.sendControl({
			type: "RELEASE_OPERATION",
			controlId: this.nextControlId(),
			operationId,
		});
	}

	/**
	 * Requests one correlated IndexedDB-backed viewport projection from QueryWorker.
	 * @param descriptor Operation-qualified viewport transform.
	 * @returns Projection, or null when superseded before execution.
	 */
	public requestRenderWindow(
		descriptor: QueryRenderWindowDescriptor,
	): Promise<RenderWindowProjection | null> {
		if (this.destroyed) {
			return Promise.reject(new Error("QueryWorker transport is destroyed."));
		}
		const projectionRequestId = this.nextProjectionRequestId();
		return new Promise<RenderWindowProjection | null>((resolve, reject) => {
			this.pendingRenderWindows.set(projectionRequestId, { resolve, reject });
			try {
				this.worker.postMessage({
					type: "REQUEST_RENDER_WINDOW",
					projectionRequestId,
					descriptor,
				});
			} catch (error) {
				this.pendingRenderWindows.delete(projectionRequestId);
				reject(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	/**
	 * Requests one correlated QueryWorker diagnostics snapshot.
	 * @returns Current persistence and projection counters and gauges.
	 */
	public getQueryDiagnostics(): Promise<QueryWorkerDiagnostics> {
		if (this.destroyed) {
			return Promise.reject(new Error("QueryWorker transport is destroyed."));
		}
		const diagnosticsRequestId = this.nextDiagnosticsRequestId();
		return new Promise<QueryWorkerDiagnostics>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.pendingDiagnostics.delete(diagnosticsRequestId);
				reject(new Error("QueryWorker diagnostics request timed out."));
			}, CONTROL_TIMEOUT_MS);
			this.pendingDiagnostics.set(diagnosticsRequestId, {
				resolve,
				reject,
				timer,
			});
			try {
				this.worker.postMessage({
					type: "GET_QUERY_DIAGNOSTICS",
					diagnosticsRequestId,
				});
			} catch (error) {
				clearTimeout(timer);
				this.pendingDiagnostics.delete(diagnosticsRequestId);
				reject(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	/** Clears cumulative QueryWorker diagnostics through an acknowledged command. */
	public resetQueryDiagnostics(): Promise<void> {
		return this.sendControl({
			type: "RESET_QUERY_DIAGNOSTICS",
			controlId: this.nextControlId(),
		});
	}

	/**
	 * Sends an application payload through the worker-owned WebSocket.
	 * @param data Serializable payload to send.
	 */
	public send(data: object): void {
		this.postCommand({ type: "SEND", data });
	}

	/** Requests an explicit reconnect from QueryWorker. */
	public reconnect(): void {
		this.postCommand({ type: "RECONNECT" });
	}

	/**
	 * Closes QueryWorker's WebSocket without terminating the reusable worker.
	 * @param code Optional WebSocket close code.
	 * @param reason Optional WebSocket close reason.
	 */
	public close(code?: number, reason?: string): void {
		this.rejectReadyWaiters("QueryWorker socket was closed.");
		this.postCommand({ type: "CLOSE", code, reason });
	}

	/** Permanently terminates QueryWorker and rejects unresolved operations. */
	public destroy(): void {
		if (this.destroyed) return;
		this.destroyed = true;
		try {
			this.worker.postMessage({ type: "CLOSE" });
		} catch {
			// Worker termination below is sufficient when it no longer accepts messages.
		}
		this.worker.terminate();
		this.worker.onmessage = null;
		this.worker.onerror = null;
		this.rejectReadyWaiters("QueryWorker transport was destroyed.");
		for (const pending of this.pendingControls.values()) {
			if (pending.timer) clearTimeout(pending.timer);
			pending.reject(new Error("QueryWorker transport was destroyed."));
		}
		this.pendingControls.clear();
		this.rejectPendingRenderWindows("QueryWorker transport was destroyed.");
		this.rejectPendingDiagnostics("QueryWorker transport was destroyed.");
	}

	/**
	 * Sends a command that must receive a correlated worker acknowledgement.
	 * @param command Control command carrying its correlation ID.
	 * @returns Promise resolved or rejected by the worker result.
	 */
	private sendControl(
		command: Extract<
			QueryWorkerCommand,
			{
				controlId: string;
			}
		>,
	): Promise<void> {
		if (this.destroyed) {
			return Promise.reject(new Error("QueryWorker transport is destroyed."));
		}
		return new Promise<void>((resolve, reject) => {
			const timer =
				command.type === "REGISTER_STREAM"
					? null
					: setTimeout(() => {
							this.pendingControls.delete(command.controlId);
							reject(
								new Error(`QueryWorker control ${command.type} timed out.`),
							);
						}, CONTROL_TIMEOUT_MS);
			this.pendingControls.set(command.controlId, { resolve, reject, timer });
			try {
				this.worker.postMessage(command);
			} catch (error) {
				if (timer) clearTimeout(timer);
				this.pendingControls.delete(command.controlId);
				reject(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	/**
	 * Posts a fire-and-forget command and reports structured-clone failures.
	 * @param command Command to post.
	 */
	private postCommand(command: QueryWorkerCommand): void {
		if (this.destroyed) return;
		try {
			this.worker.postMessage(command);
		} catch (error) {
			this.callbacks.onProtocolError({
				message: error instanceof Error ? error.message : String(error),
			});
		}
	}

	/**
	 * Dispatches one typed worker event to pending controls or application hooks.
	 * @param event Worker event to process.
	 */
	private handleWorkerEvent(event: QueryWorkerEvent): void {
		switch (event.type) {
			case "CONTROL_RESULT": {
				const pending = this.pendingControls.get(event.controlId);
				if (!pending) return;
				if (pending.timer) clearTimeout(pending.timer);
				this.pendingControls.delete(event.controlId);
				if (event.error) pending.reject(new Error(event.error));
				else pending.resolve();
				break;
			}
			case "SOCKET_STATE":
				this.socketState = event.state;
				this.callbacks.onSocketState(event.state);
				if (event.state === "ready") this.resolveReadyWaiters();
				break;
			case "SOCKET_OPEN":
				this.callbacks.onSocketOpen();
				break;
			case "SOCKET_CLOSE":
				this.callbacks.onSocketClose(event.details);
				break;
			case "SOCKET_ERROR":
				this.callbacks.onSocketError(event.details);
				break;
			case "FORWARD_MESSAGE":
				this.callbacks.onForwardMessage(event.message);
				break;
			case "STREAM_STATE":
				this.callbacks.onStreamStateChange(event.change);
				break;
			case "SOURCE_SUMMARY":
				this.callbacks.onSourceSummary(event.event);
				break;
			case "STREAM_FAILURE":
				this.callbacks.onStreamFailure(event.event);
				break;
			case "RENDER_WINDOW_RESULT": {
				const pending = this.pendingRenderWindows.get(
					event.projectionRequestId,
				);
				if (!pending) return;
				this.pendingRenderWindows.delete(event.projectionRequestId);
				if (event.error) pending.reject(new Error(event.error));
				else pending.resolve(event.projection);
				break;
			}
			case "QUERY_DIAGNOSTICS_RESULT": {
				const pending = this.pendingDiagnostics.get(
					event.diagnosticsRequestId,
				);
				if (!pending) return;
				clearTimeout(pending.timer);
				this.pendingDiagnostics.delete(event.diagnosticsRequestId);
				pending.resolve(event.diagnostics);
				break;
			}
			case "PROTOCOL_ERROR":
				this.callbacks.onProtocolError(event.details);
				break;
		}
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

	/**
	 * Rejects controls that cannot receive an acknowledgement after worker failure.
	 * @param message Stable rejection reason.
	 */
	private rejectPendingControls(message: string): void {
		for (const pending of this.pendingControls.values()) {
			if (pending.timer) clearTimeout(pending.timer);
			pending.reject(new Error(message));
		}
		this.pendingControls.clear();
	}

	/** Rejects correlated viewport requests after worker failure or destruction. */
	private rejectPendingRenderWindows(message: string): void {
		for (const pending of this.pendingRenderWindows.values()) {
			pending.reject(new Error(message));
		}
		this.pendingRenderWindows.clear();
	}

	/** Rejects correlated diagnostics requests after worker failure or destruction. */
	private rejectPendingDiagnostics(message: string): void {
		for (const pending of this.pendingDiagnostics.values()) {
			clearTimeout(pending.timer);
			pending.reject(new Error(message));
		}
		this.pendingDiagnostics.clear();
	}

	/** Returns a unique correlation identifier for the next control command. */
	private nextControlId(): string {
		this.controlCounter += 1;
		return `query_control_${this.controlCounter}`;
	}

	/** Returns a unique correlation identifier for the next viewport request. */
	private nextProjectionRequestId(): string {
		this.projectionCounter += 1;
		return `query_projection_${this.projectionCounter}`;
	}

	/** Returns a unique correlation identifier for one diagnostics request. */
	private nextDiagnosticsRequestId(): string {
		this.diagnosticsCounter += 1;
		return `query_diagnostics_${this.diagnosticsCounter}`;
	}
}
