/// <reference lib="webworker" />

import type {
	QueryWorkerCommand,
	QueryWorkerControlResult,
	QueryWorkerEvent,
} from "@/event-stream";
import { QuerySocketSession } from "./QuerySocket.session";
import { getRenderWindowProjectionTransferables } from "./QueryProjection.transfer";
import type { QueryTransportCallbacks } from "./QueryTransport.types";

const workerScope = self as DedicatedWorkerGlobalScope;
let session: QuerySocketSession | null = null;

/**
 * Posts one typed protocol event to the QueryWorker client.
 * @param event Serializable event to deliver.
 */
function postWorkerEvent(
	event: QueryWorkerEvent,
	transfer: Transferable[] = [],
): void {
	workerScope.postMessage(event, transfer);
}

const callbacks: QueryTransportCallbacks = {
	onSocketState: (state) => postWorkerEvent({ type: "SOCKET_STATE", state }),
	onSocketOpen: () => postWorkerEvent({ type: "SOCKET_OPEN" }),
	onSocketClose: (details) =>
		postWorkerEvent({ type: "SOCKET_CLOSE", details }),
	onSocketError: (details) =>
		postWorkerEvent({ type: "SOCKET_ERROR", details }),
	onForwardMessage: (message) =>
		postWorkerEvent({ type: "FORWARD_MESSAGE", message }),
	onStreamStateChange: (change) =>
		postWorkerEvent({ type: "STREAM_STATE", change }),
	onSourceSummary: (event) =>
		postWorkerEvent({ type: "SOURCE_SUMMARY", event }),
	onStreamFailure: (event) =>
		postWorkerEvent({ type: "STREAM_FAILURE", event }),
	onProtocolError: (details) =>
		postWorkerEvent({ type: "PROTOCOL_ERROR", details }),
};

/**
 * Replies to one acknowledged command with an optional failure reason.
 * @param controlId Command correlation identifier.
 * @param error Optional thrown value.
 */
function completeControl(controlId: string, error?: unknown): void {
	const result: QueryWorkerControlResult = {
		type: "CONTROL_RESULT",
		controlId,
		error:
			error === undefined
				? undefined
				: error instanceof Error
					? error.message
					: String(error),
	};
	postWorkerEvent(result);
}

/**
 * Returns the initialized socket session or throws a stable protocol error.
 * @returns Active QueryWorker socket session.
 */
function requireSession(): QuerySocketSession {
	if (!session) throw new Error("QueryWorker has not been initialized.");
	return session;
}

/**
 * Executes one command received from the main-thread QueryWorker client.
 * @param command Typed worker command.
 */
async function handleCommand(command: QueryWorkerCommand): Promise<void> {
	switch (command.type) {
		case "INITIALIZE":
			try {
				session?.destroy();
				session = new QuerySocketSession(command.initialization, callbacks);
				session.connect();
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "REGISTER_STREAM":
			try {
				await requireSession().registerDocumentStream(command.descriptor);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "CANCEL_STREAM":
			try {
				await requireSession().cancelDocumentStream(command.requestId);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "COMPLETE_STREAM":
			try {
				await requireSession().completeDocumentStream(
					command.requestId,
					command.finalization,
				);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "CANCEL_OPERATION_STREAMS":
			try {
				await requireSession().cancelOperationStreams(
					command.operationId,
					command.operationEpoch,
				);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "RELEASE_OPERATION":
			try {
				await requireSession().releaseOperation(command.operationId);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "REQUEST_RENDER_WINDOW":
			try {
				const projection = await requireSession().requestRenderWindow(
					command.descriptor,
				);
				postWorkerEvent(
					{
						type: "RENDER_WINDOW_RESULT",
						projectionRequestId: command.projectionRequestId,
						projection,
					},
					projection
						? getRenderWindowProjectionTransferables(projection)
						: [],
				);
			} catch (error) {
				postWorkerEvent({
					type: "RENDER_WINDOW_RESULT",
					projectionRequestId: command.projectionRequestId,
					projection: null,
					error: error instanceof Error ? error.message : String(error),
				});
			}
			break;
		case "GET_QUERY_DIAGNOSTICS":
			postWorkerEvent({
				type: "QUERY_DIAGNOSTICS_RESULT",
				diagnosticsRequestId: command.diagnosticsRequestId,
				diagnostics: requireSession().getQueryDiagnostics(),
			});
			break;
		case "RESET_QUERY_DIAGNOSTICS":
			try {
				requireSession().resetQueryDiagnostics();
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "RECONCILE_SOURCE_QUERY_CACHE":
			try {
				const result = await requireSession().reconcileSourceQueryCache({
					operationId: command.operationId,
					sourceId: command.sourceId,
					desired: command.desired,
				});
				postWorkerEvent({
					type: "SOURCE_QUERY_CACHE_RESULT",
					cacheRequestId: command.cacheRequestId,
					result,
				});
			} catch (error) {
				postWorkerEvent({
					type: "SOURCE_QUERY_CACHE_RESULT",
					cacheRequestId: command.cacheRequestId,
					result: null,
					error: error instanceof Error ? error.message : String(error),
				});
			}
			break;
		case "NAVIGATE_SOURCE_QUERY_CACHE":
			try {
				const result = await requireSession().navigateSourceQueryCache({
					operationId: command.operationId,
					sourceId: command.sourceId,
					direction: command.direction,
				});
				postWorkerEvent({
					type: "SOURCE_QUERY_CACHE_RESULT",
					cacheRequestId: command.cacheRequestId,
					result,
				});
			} catch (error) {
				postWorkerEvent({
					type: "SOURCE_QUERY_CACHE_RESULT",
					cacheRequestId: command.cacheRequestId,
					result: null,
					error: error instanceof Error ? error.message : String(error),
				});
			}
			break;
		case "CLEAR_SOURCE_QUERY_CACHE":
			try {
				await requireSession().clearSourceQueryCache(
					command.operationId,
					command.sourceId,
				);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "DELETE_SOURCE_EVENT_STORE":
			try {
				await requireSession().deleteSourceEventStore(
					command.operationId,
					command.sourceId,
				);
				completeControl(command.controlId);
			} catch (error) {
				completeControl(command.controlId, error);
			}
			break;
		case "SEND":
			requireSession().send(command.data);
			break;
		case "RECONNECT":
			requireSession().reconnect();
			break;
		case "CLOSE":
			requireSession().close(command.code, command.reason);
			break;
	}
}

workerScope.onmessage = (event: MessageEvent<QueryWorkerCommand>): void => {
	void handleCommand(event.data).catch((error: unknown) => {
		callbacks.onProtocolError({
			message: error instanceof Error ? error.message : String(error),
		});
	});
};

export {};
