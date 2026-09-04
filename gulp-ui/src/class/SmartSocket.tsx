import { Logger } from "@/dto/Logger.class";
import type {
	DocumentStreamDescriptor,
	DocumentStreamFinalization,
	DocumentStreamStateChange,
	QueryRenderWindowDescriptor,
	QueryIngestionProgressEvent,
	QuerySocketCloseDetails,
	QuerySocketErrorDetails,
	QuerySocketMessage,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryWorkerDiagnostics,
} from "@/event-stream";
import type { QueryTransport } from "@/event-store-worker/QueryTransport.types";
import { createQueryTransport } from "@/event-store-worker/QueryWorker.factory";
import type {
	EventSourceQueryCacheActivation,
	EventSourceQueryCacheReconciliation,
	DesiredQueryCacheLookup,
} from "@/event-store/EventStore.types";
import { renderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import { Internal } from "@/entities/addon/Internal";
import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import type { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import type { User } from "@/entities/User";
import { WindowBridge } from "@/lib/WindowBridge";
import { isEventStoreLodEnabled } from "@/event-store/EventStore.rollout";
import { resolveEventStoreCacheNamespaceId } from "@/event-store/EventStore.namespace";
import { Icon } from "@/ui/Icon";
import { EventEmitter } from "events";

export namespace SmartSocket {
	export type Condition<T = any> = (data: Message.Entity<T>) => boolean;
	export type Handler<T = any> = (data: Message.Entity<T>) => void;

	interface Conditional<T = any> {
		id: string;
		condition: Condition<T>;
		handler: Handler<T>;
		once?: boolean;
	}

	export namespace Message {
		export enum Type {
			WS_ERROR = "ws_error",
			WS_CONNECTED = "ws_connected",
			STATS_CREATE = "stats_create",
			STATS_UPDATE = "stats_update",
			COLLAB_CREATE = "collab_create",
			COLLAB_UPDATE = "collab_update",
			USER_LOGIN = "user_login",
			USER_LOGOUT = "user_logout",
			DOCUMENTS_CHUNK = "docs_chunk",
			COLLAB_DELETE = "collab_delete",
			INGEST_SOURCE_DONE = "ingest_source_done",
			ENRICH_DONE = "enrich_done",
			TAG_DONE = "tag_done",
			REBASE_DONE = "rebase_done",
			CLIENT_DATA = "client_data",
			SOURCE_FIELDS_CHUNK = "source_fields_chunk",
			AI_ASSISTANT_DONE = "ai_assistant_done",
			AI_ASSISTANT_STREAM = "ai_assistant_stream",
			AI_ASSISTANT_ERROR = "ai_assistant_error",
		}

		export interface Entity<T = any> {
			"@timestamp": number;
			type: Message.Type;
			private: boolean;
			payload: {
				obj: T;
				last?: boolean;
				docs?: Doc.Type[];
				[key: string]: any;
			};
			ws_id: string;
			user_id: User.Id;
			req_id: Request.Id;
			operation_id?: Operation.Id;
			"gulp.operation_id"?: Operation.Id;
		}
	}

	/** Compatibility facade over the worker-owned query WebSocket transport. */
	export class Class extends EventEmitter {
		public static instance: SmartSocket.Class;

		private readonly conditional = new Map<
			SmartSocket.Message.Type,
			Conditional[]
		>();
		private readonly transport!: QueryTransport;
		private readonly cacheNamespaceId!: string;
		private readonly sourceSummarySubscribers = new Set<
			(event: QuerySourceSummaryEvent) => void
		>();
		private readonly ingestionProgressSubscribers = new Set<
			(event: QueryIngestionProgressEvent) => void
		>();
		private readonly streamFailureSubscribers = new Set<
			(event: QueryStreamFailureEvent) => void
		>();
		private readonly streamStateSubscribers = new Set<
			(change: DocumentStreamStateChange) => void
		>();
		private counter = 0;

		/**
		 * Creates this browser realm's singleton socket facade.
		 * @param wsId Private WebSocket routing identity.
		 * @param authenticatedUserId Authoritative current user, when available.
		 * @param explicitCacheNamespaceId Namespace propagated by a main realm to a detached realm.
		 */
		public constructor(
			wsId: string,
			authenticatedUserId?: User.Id | null,
			explicitCacheNamespaceId?: string | null,
		) {
			super();

			const coordinatorId = WindowBridge.getOrCreateCoordinatorId();
			const cacheNamespaceId =
				explicitCacheNamespaceId ??
				resolveEventStoreCacheNamespaceId(
					authenticatedUserId,
					Internal.Settings.server,
					coordinatorId,
				);
			const existing = SmartSocket.Class.instance;
			if (existing) {
				if (existing.cacheNamespaceId === cacheNamespaceId) {
					return existing;
				}
				existing.destroy();
			}
			this.cacheNamespaceId = cacheNamespaceId;

			this.transport = createQueryTransport(
				{
					socketUrl: `${Internal.Settings.server}/ws`,
					token: Internal.Settings.token,
					wsId,
					cacheNamespaceId,
					// Rollback alone permits unregistered document chunks on the main thread.
					legacyUnknownDocumentPassthrough: !isEventStoreLodEnabled(),
				},
				{
					onSocketState: () => undefined,
					onSocketOpen: () => this.handleSocketOpen(),
					onSocketClose: (details) => this.handleSocketClose(details),
					onSocketError: (details) => this.handleSocketError(details),
					onForwardMessage: (message) =>
						this.handleForwardedMessage(message),
					onStreamStateChange: (change) =>
						this.publishStreamState(change),
					onSourceSummary: (event) => this.publishSourceSummary(event),
					onIngestionProgress: (event) =>
						this.publishIngestionProgress(event),
					onStreamFailure: (event) => this.publishStreamFailure(event),
					onProtocolError: (details) => this.handleProtocolError(details),
				},
			);

			SmartSocket.Class.instance = this;
		}

		/** Registers compatibility logging and preserves the legacy open event. */
		private handleSocketOpen(): void {
			this.once(SmartSocket.Message.Type.WS_CONNECTED, () =>
				Logger.log("WebSocket has been initialized", SmartSocket.Class, {
					icon: <Icon name="Rss" />,
				}),
			);
			this.emit("open", { type: "open" });
		}

		/**
		 * Preserves the legacy close event emitted by SmartSocket.
		 * @param details Serializable WebSocket close details.
		 */
		private handleSocketClose(details: QuerySocketCloseDetails): void {
			this.emit("close", details);
		}

		/**
		 * Preserves the legacy error event when a caller subscribed to it.
		 * @param details Serializable WebSocket error details.
		 */
		private handleSocketError(details: QuerySocketErrorDetails): void {
			if (this.listenerCount("error") > 0) {
				this.emit("error", details);
				return;
			}
			Logger.log(details.message, SmartSocket.Class);
		}

		/**
		 * Emits one parsed message before invoking conditional listeners, as before.
		 * @param rawMessage Validated WebSocket message from the selected transport.
		 */
		private handleForwardedMessage(rawMessage: QuerySocketMessage): void {
			const message = rawMessage as unknown as SmartSocket.Message.Entity;
			this.emit(message.type, message);
			this.handle(message.type, message);
		}

		/**
		 * Publishes stream lifecycle changes through the typed, non-legacy channel.
		 * @param change Request-scoped lifecycle transition.
		 */
		private publishStreamState(change: DocumentStreamStateChange): void {
			for (const subscriber of this.streamStateSubscribers) subscriber(change);
		}

		/**
		 * Publishes one compact committed source summary to dedicated subscribers.
		 * @param event Source summary without document payloads.
		 */
		private publishSourceSummary(event: QuerySourceSummaryEvent): void {
			for (const subscriber of this.sourceSummarySubscribers) subscriber(event);
		}

		/**
		 * Publishes source-exact ingestion totals without exposing document payloads.
		 * @param event Request-scoped cumulative counts produced by QueryWorker.
		 * @returns Nothing.
		 */
		private publishIngestionProgress(event: QueryIngestionProgressEvent): void {
			for (const subscriber of this.ingestionProgressSubscribers) {
				subscriber(event);
			}
		}

		/**
		 * Publishes one persistence failure so the application can cancel its backend request.
		 * @param event Terminal request failure without document payloads.
		 */
		private publishStreamFailure(event: QueryStreamFailureEvent): void {
			for (const subscriber of this.streamFailureSubscribers) subscriber(event);
		}

		/**
		 * Maps parsing and worker protocol errors onto the existing ws_error event.
		 * @param details Serializable error and optional raw message preview.
		 */
		private handleProtocolError(details: QuerySocketErrorDetails): void {
			this.emit(SmartSocket.Message.Type.WS_ERROR, {
				error: new Error(details.message),
				raw: details.raw,
			});
		}

		/**
		 * Evaluates registered conditional listeners for one forwarded message.
		 * @param event Message type whose conditions should run.
		 * @param data Message delivered to matching handlers.
		 */
		private handle(
			event: SmartSocket.Message.Type,
			data: SmartSocket.Message.Entity,
		): void {
			const listeners = this.conditional.get(event) ?? [];
			const remove: string[] = [];

			for (const listener of listeners) {
				try {
					if (listener.condition(data)) {
						listener.handler(data);
						if (listener.once) remove.push(listener.id);
					}
				} catch (error) {
					Logger.log(
						`Handler error for ${event}: ${(error as Error).message}`,
						SmartSocket.Class,
					);
				}
			}

			for (const id of remove) this.remove(event, id);
		}

		/**
		 * Removes one conditional listener from one message type.
		 * @param event Message type owning the listener.
		 * @param id Listener identifier to remove.
		 */
		private remove(event: SmartSocket.Message.Type, id: string): void {
			const listeners = this.conditional.get(event) ?? [];
			const filtered = listeners.filter((listener) => listener.id !== id);

			if (filtered.length === 0) this.conditional.delete(event);
			else this.conditional.set(event, filtered);
		}

		/**
		 * Adds a reusable conditional listener.
		 * @param event Message type to observe.
		 * @param condition Predicate selecting messages for the handler.
		 * @param handler Callback invoked for matching messages.
		 * @returns Stable listener identifier for later removal.
		 */
		public con<T = any>(
			event: SmartSocket.Message.Type,
			condition: Condition<T>,
			handler: Handler<T>,
		): string {
			const id = `listener_${++this.counter}`;
			const listeners = this.conditional.get(event) ?? [];
			listeners.push({ id, condition, handler, once: false });
			this.conditional.set(event, listeners);
			return id;
		}

		/**
		 * Adds a one-shot conditional listener.
		 * @param event Message type to observe.
		 * @param condition Predicate selecting the first matching message.
		 * @param handler Callback invoked for the first match.
		 * @returns Stable listener identifier for early removal.
		 */
		public conce<T = any>(
			event: SmartSocket.Message.Type,
			condition: Condition<T>,
			handler: Handler<T>,
		): string {
			const id = `listener_${++this.counter}`;
			const listeners = this.conditional.get(event) ?? [];
			listeners.push({ id, condition, handler, once: true });
			this.conditional.set(event, listeners);
			return id;
		}

		/**
		 * Removes one conditional listener by identifier.
		 * @param event Message type owning the listener.
		 * @param listenerId Listener identifier returned by con or conce.
		 */
		public coff(
			event: SmartSocket.Message.Type,
			listenerId: string,
		): void {
			this.remove(event, listenerId);
		}

		/**
		 * Removes all conditional listeners for one message type.
		 * @param event Message type to clear.
		 */
		public coffAll(event: SmartSocket.Message.Type): void {
			this.conditional.delete(event);
		}

		/**
		 * Removes conditional listeners that reference one predicate function.
		 * @param event Message type owning the listeners.
		 * @param condition Predicate reference to remove.
		 */
		public coffWhenCondition<T = any>(
			event: SmartSocket.Message.Type,
			condition: Condition<T>,
		): void {
			const listeners = this.conditional.get(event) ?? [];
			const filtered = listeners.filter(
				(listener) => listener.condition !== condition,
			);

			if (filtered.length === 0) this.conditional.delete(event);
			else this.conditional.set(event, filtered);
		}

		/**
		 * Waits for the first message satisfying one condition.
		 * @param event Message type to observe.
		 * @param condition Predicate selecting the desired message.
		 * @param timeout Optional rejection timeout in milliseconds.
		 * @returns Promise resolving with the matching message.
		 */
		public wait<T = any>(
			event: SmartSocket.Message.Type,
			condition: Condition<T>,
			timeout?: number,
		): Promise<Message.Entity<T>> {
			return new Promise((resolve, reject) => {
				let timeoutId: ReturnType<typeof setTimeout> | null = null;
				const listenerId = this.conce<T>(event, condition, (data) => {
					if (timeoutId) clearTimeout(timeoutId);
					resolve(data);
				});

				if (timeout) {
					timeoutId = setTimeout(() => {
						this.coff(event, listenerId);
						reject(new Error(`Timeout waiting for ${event} event`));
					}, timeout);
				}
			});
		}

		/**
		 * Registers document ownership before issuing the matching backend request.
		 * @param descriptor Request, operation epoch, and ownership policy.
		 * @returns Promise resolved after QueryWorker installs the registration.
		 */
		public registerDocumentStream(
			descriptor: DocumentStreamDescriptor,
		): Promise<void> {
			return this.transport.registerDocumentStream(descriptor);
		}

		/**
		 * Cancels one document request and rejects its late chunks.
		 * @param requestId Request identifier to cancel.
		 * @returns Promise resolved after QueryWorker installs the cancellation.
		 */
		public cancelDocumentStream(requestId: string): Promise<void> {
			return this.transport.cancelDocumentStream(requestId);
		}

		/**
		 * Completes one document stream after its final IndexedDB commit succeeds.
		 * @param requestId Request identifier whose stream is complete.
		 * @param finalization Optional terminal source-cache metadata.
		 * @returns Promise resolved after QueryWorker publishes the completed state.
		 */
		public completeDocumentStream(
			requestId: string,
			finalization?: DocumentStreamFinalization,
		): Promise<void> {
			return this.transport.completeDocumentStream(requestId, finalization);
		}

		/**
		 * Cancels document requests belonging to an operation or epoch.
		 * @param operationId Operation identifier to cancel.
		 * @param operationEpoch Optional epoch guard.
		 * @returns Promise resolved after matching requests are canceled.
		 */
		public cancelOperationStreams(
			operationId: string,
			operationEpoch?: number,
		): Promise<void> {
			return this.transport.cancelOperationStreams(
				operationId,
				operationEpoch,
			);
		}

		/**
		 * Releases one operation's transient QueryWorker context after safe drain.
		 * @param operationId Operation context to close without deleting cached events.
		 * @returns Promise resolved after repository and revision-bus closure.
		 */
		public releaseOperation(operationId: string): Promise<void> {
			return this.transport.releaseOperation(operationId);
		}

		/**
		 * Waits until WebSocket authentication is acknowledged.
		 * @param timeoutMs Maximum wait before rejection, in milliseconds.
		 * @returns Promise resolved when the worker reports ready.
		 */
		public awaitReady(timeoutMs?: number): Promise<void> {
			return this.transport.awaitReady(timeoutMs);
		}

		/**
		 * Builds and synchronously publishes one IndexedDB-backed render window.
		 * @param descriptor Operation-qualified viewport transform.
		 * @returns Projection, or null when a newer pending request superseded it.
		 * @throws Error when the active Canvas does not accept the projected window.
		 */
		public async requestRenderWindow(
			descriptor: QueryRenderWindowDescriptor,
		): Promise<RenderWindowProjection | null> {
			const projection = await this.transport.requestRenderWindow(descriptor);
			if (projection && !renderWindowProjectionBus.publish(projection)) {
				throw new Error(
					"Render-window projection was not accepted by the active Canvas.",
				);
			}
			return projection;
		}

		/**
		 * Reads QueryWorker diagnostics for benchmark and troubleshooting tools.
		 * @returns On-demand persistence and projection snapshot.
		 */
		public getQueryDiagnostics(): Promise<QueryWorkerDiagnostics> {
			return this.transport.getQueryDiagnostics();
		}

		/** Clears cumulative QueryWorker diagnostics without changing active work. */
		public resetQueryDiagnostics(): Promise<void> {
			return this.transport.resetQueryDiagnostics();
		}

		/**
		 * Selects the safest exact, range-covered, or latest cache for a source.
		 * @param input Operation, source, and desired exact/shape/range lookup.
		 * @returns Reconciled activation, or null when no cache is reusable.
		 */
		public reconcileSourceQueryCache(input: {
			operationId: string;
			sourceId: Source.Id;
			desired: DesiredQueryCacheLookup;
		}): Promise<EventSourceQueryCacheReconciliation | null> {
			return this.transport.reconcileSourceQueryCache(input);
		}

		/**
		 * Moves one source-local cache history cursor without backend I/O.
		 * @param input Operation, source, and Undo or Redo direction.
		 * @returns Activated cache, or null at the corresponding history boundary.
		 */
		public navigateSourceQueryCache(input: {
			operationId: string;
			sourceId: Source.Id;
			direction: "undo" | "redo";
		}): Promise<EventSourceQueryCacheActivation | null> {
			return this.transport.navigateSourceQueryCache(input);
		}

		/**
		 * Clears every persisted query cache for one source before a rebuild.
		 * @param operationId Operation owning the persisted source.
		 * @param sourceId Source whose cache pool must be removed.
		 * @returns Promise resolved after bounded cleanup completes.
		 */
		public clearSourceQueryCache(
			operationId: string,
			sourceId: Source.Id,
		): Promise<void> {
			return this.transport.clearSourceQueryCache(operationId, sourceId);
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
			return this.transport.deleteSourceEventStore(operationId, sourceId);
		}

		/**
		 * Subscribes to compact source summaries committed by QueryWorker.
		 * @param subscriber Summary handler that never receives document payloads.
		 * @returns Cleanup callback removing the subscription.
		 */
		public subscribeSourceSummaries(
			subscriber: (event: QuerySourceSummaryEvent) => void,
		): () => void {
			this.sourceSummarySubscribers.add(subscriber);
			return () => this.sourceSummarySubscribers.delete(subscriber);
		}

		/**
		 * Subscribes to source-exact progress from worker-owned ingestion streams.
		 * @param subscriber Handler receiving compact cumulative source counts.
		 * @returns Cleanup callback removing the subscription.
		 */
		public subscribeIngestionProgress(
			subscriber: (event: QueryIngestionProgressEvent) => void,
		): () => void {
			this.ingestionProgressSubscribers.add(subscriber);
			return () => this.ingestionProgressSubscribers.delete(subscriber);
		}

		/**
		 * Subscribes to persistence failures requiring backend request cancellation.
		 * @param subscriber Stream failure handler.
		 * @returns Cleanup callback removing the subscription.
		 */
		public subscribeDocumentStreamFailures(
			subscriber: (event: QueryStreamFailureEvent) => void,
		): () => void {
			this.streamFailureSubscribers.add(subscriber);
			return () => this.streamFailureSubscribers.delete(subscriber);
		}

		/**
		 * Subscribes to request-scoped document stream lifecycle transitions.
		 * @param subscriber Typed lifecycle handler.
		 * @returns Cleanup callback removing the subscription.
		 */
		public subscribeDocumentStreamStates(
			subscriber: (change: DocumentStreamStateChange) => void,
		): () => void {
			this.streamStateSubscribers.add(subscriber);
			return () => this.streamStateSubscribers.delete(subscriber);
		}

		/**
		 * Sends one payload through the selected WebSocket transport.
		 * @param data Serializable application payload.
		 */
		public send(data: object): void {
			if (
				this.transport.state !== "authenticating" &&
				this.transport.state !== "ready"
			) {
				Logger.log(
					"WebSocket not open, cannot send message",
					SmartSocket.Class,
				);
				return;
			}
			this.transport.send(data);
		}

		/**
		 * Closes the socket without automatic reconnect.
		 * @param code Optional WebSocket close code.
		 * @param reason Optional WebSocket close reason.
		 */
		public close(code?: number, reason?: string): void {
			this.transport.close(code, reason);
		}

		/** Permanently releases this realm's socket singleton and all listeners. */
		public destroy(): void {
			this.transport.destroy();
			this.conditional.clear();
			this.sourceSummarySubscribers.clear();
			this.ingestionProgressSubscribers.clear();
			this.streamFailureSubscribers.clear();
			this.streamStateSubscribers.clear();
			this.removeAllListeners();

			if (SmartSocket.Class.instance === this) {
				Reflect.deleteProperty(SmartSocket.Class, "instance");
			}
		}

		/** Requests a fresh WebSocket connection from the selected transport. */
		public reconnect(): void {
			this.transport.reconnect();
		}
	}
}
