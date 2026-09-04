/// <reference lib="webworker" />

import { QuerySocketSession } from "@/event-store-worker/QuerySocket.session";
import { getRenderWindowProjectionTransferables } from "@/event-store-worker/QueryProjection.transfer";
import type { QueryTransportCallbacks } from "@/event-store-worker/QueryTransport.types";
import { EVENT_PAGE_SIZE } from "@/event-store";
import type {
	QuerySocketMessage,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
} from "@/event-stream";
import type {
	RendererQueryReplayResult,
	RendererQueryReplayStartOptions,
	RendererQueryReplayWorkerCommand,
	RendererQueryReplayWorkerEvent,
} from "./RendererTest.query-replay";

const workerScope = self as DedicatedWorkerGlobalScope;
const REPLAY_REQUEST_ID = "renderer-query-replay";
const STEADY_CHUNKS_PER_PUMP = 2;
const DRAIN_POLL_MS = 0;

interface SyntheticSocketHandlers {
	onopen: ((event: Event) => void) | null;
	onclose: ((event: CloseEvent) => void) | null;
	onerror: ((event: Event) => void) | null;
	onmessage: ((event: MessageEvent<string>) => void) | null;
}

/** Minimal in-worker text socket used to exercise QuerySocketSession parsing. */
class SyntheticReplaySocket implements SyntheticSocketHandlers {
	public onopen: ((event: Event) => void) | null = null;
	public onclose: ((event: CloseEvent) => void) | null = null;
	public onerror: ((event: Event) => void) | null = null;
	public onmessage: ((event: MessageEvent<string>) => void) | null = null;
	public readyState: number = WebSocket.CONNECTING;
	private closed = false;

	/** Opens the socket on the next microtask after QuerySocketSession attaches hooks. */
	public open(): void {
		queueMicrotask(() => {
			if (this.closed) return;
			this.readyState = WebSocket.OPEN;
			this.onopen?.(new Event("open"));
		});
	}

	/**
	 * Accepts authentication and ignores client flow-control ACKs like a real server.
	 * @param data Serialized client-to-server payload.
	 * @returns Nothing after handling the supported synthetic protocol message.
	 */
	public send(data: string): void {
		if (this.readyState !== WebSocket.OPEN) {
			throw new Error("Synthetic replay socket is not open.");
		}
		const message = JSON.parse(data) as { token?: unknown; ws_id?: unknown };
		if (typeof message.token !== "string" || typeof message.ws_id !== "string") {
			return;
		}
		queueMicrotask(() => {
			if (this.closed) return;
			this.deliver(JSON.stringify({ type: "ws_connected", payload: {} }));
		});
	}

	/** Delivers one serialized server message through the normal socket callback. */
	public deliver(raw: string): void {
		if (this.closed) return;
		this.onmessage?.(new MessageEvent<string>("message", { data: raw }));
	}

	/** Closes the synthetic socket without reconnecting it. */
	public close(): void {
		if (this.closed) return;
		this.closed = true;
		this.readyState = WebSocket.CLOSED;
		this.onclose?.({
			code: 1000,
			reason: "renderer replay closed",
			wasClean: true,
		} as CloseEvent);
	}
}

let activeSession: QuerySocketSession | null = null;
let activeSocket: SyntheticReplaySocket | null = null;
let cancelled = false;

/** Posts one compact event to the renderer-test page. */
function postEvent(
	event: RendererQueryReplayWorkerEvent,
	transfer: Transferable[] = [],
): void {
	workerScope.postMessage(event, transfer);
}

/** Yields so QuerySocketSession normalization and IndexedDB transactions can advance. */
function yieldToWorker(): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, DRAIN_POLL_MS));
}

/**
 * Builds one deterministic JSON `docs_chunk` with no retained source-wide array.
 * @param options Replay source and timestamp distribution.
 * @param chunkIndex Zero-based backend chunk number.
 * @returns Raw socket text and exact document count.
 */
function buildChunk(
	options: RendererQueryReplayStartOptions,
	chunkIndex: number,
): { raw: string; documentCount: number; last: boolean } {
	const offset = chunkIndex * options.chunkEvents;
	const documentCount = Math.min(
		options.chunkEvents,
		Math.max(0, options.targetEvents - offset),
	);
	const documents: Array<Record<string, string | number>> = new Array(
		documentCount,
	);
	for (let localIndex = 0; localIndex < documentCount; localIndex++) {
		const globalIndex = offset + localIndex;
		const ratio =
			options.targetEvents <= 1
				? 0
				: globalIndex / (options.targetEvents - 1);
		documents[localIndex] = {
			_id: `renderer-query-${globalIndex.toString(16).padStart(8, "0")}`,
			"gulp.source_id": options.sourceId,
			"gulp.event_code": globalIndex % 8_000,
			"@timestamp":
				options.documentMinTimestamp + Math.floor(options.frameSpanMs * ratio),
		};
	}
	const last = offset + documentCount >= options.targetEvents;
	const message: QuerySocketMessage = {
		type: "docs_chunk",
		req_id: REPLAY_REQUEST_ID,
		operation_id: options.operationId,
		payload: {
			docs: documents,
			chunk_num: chunkIndex,
			last,
			request_last: last,
			name: "renderer-query-replay",
		},
	};
	return { raw: JSON.stringify(message), documentCount, last };
}

/**
 * Waits until a steady pump has committed every full page it made available.
 * A sub-page normalized remainder is expected to stay in the replace builder.
 * @param session Replay session exposing passive persistence diagnostics.
 */
async function waitForSteadyPump(session: QuerySocketSession): Promise<void> {
	while (!cancelled) {
		const diagnostics = session.getQueryDiagnostics();
		if (
			diagnostics.rawBacklog.currentBytes === 0 &&
			diagnostics.commits.active === 0 &&
			diagnostics.normalizedBacklog.currentDocuments < EVENT_PAGE_SIZE
		) {
			return;
		}
		await yieldToWorker();
	}
}

/**
 * Waits until every accepted raw frame has released its parsed JSON payload.
 * @param session Replay session exposing passive raw-backlog diagnostics.
 */
async function waitForRawDrain(session: QuerySocketSession): Promise<void> {
	while (!cancelled) {
		if (session.getQueryDiagnostics().rawBacklog.currentBytes === 0) return;
		await yieldToWorker();
	}
}

/**
 * Executes one real QuerySocketSession replay against IndexedDB.
 * @param options Steady or burst replay options.
 */
async function runReplay(options: RendererQueryReplayStartOptions): Promise<void> {
	cancelled = false;
	let latestSummary: QuerySourceSummaryEvent | null = null;
	let failure: QueryStreamFailureEvent | null = null;
	let queryDoneReceived = false;
	let streamCompleted = false;
	let emittedEvents = 0;
	let settleTerminal = (): void => undefined;
	const terminal = new Promise<void>((resolve) => {
		settleTerminal = resolve;
	});
	let settleReady = (): void => undefined;
	const ready = new Promise<void>((resolve) => {
		settleReady = resolve;
	});
	const callbacks: QueryTransportCallbacks = {
		onSocketState: (state) => {
			if (state === "ready") settleReady();
		},
		onSocketOpen: () => undefined,
		onSocketClose: () => undefined,
		onSocketError: () => undefined,
		onForwardMessage: (message) => {
			if (message.type === "query_done") queryDoneReceived = true;
		},
		onStreamStateChange: (change) => {
			if (change.descriptor.requestId !== REPLAY_REQUEST_ID) return;
			if (change.state === "completed") {
				streamCompleted = true;
				settleTerminal();
			} else if (change.state === "failed" || change.state === "canceled") {
				settleTerminal();
			}
		},
		onSourceSummary: (event) => {
			latestSummary = event;
			postEvent({ type: "SOURCE_SUMMARY", event });
		},
		onIngestionProgress: () => undefined,
		onStreamFailure: (event) => {
			failure = event;
			settleTerminal();
		},
		onProtocolError: (details) => {
			throw new Error(details.message);
		},
	};
	const session = new QuerySocketSession(
		{
			socketUrl: "ws://renderer-query-replay.invalid",
			token: "renderer-query-replay",
			wsId: "renderer-query-replay",
			cacheNamespaceId: options.cacheNamespaceId,
		},
		callbacks,
		() => {
			const socket = new SyntheticReplaySocket();
			activeSocket = socket;
			socket.open();
			return socket as unknown as WebSocket;
		},
		{
			rawGlobalByteLimit: options.rawGlobalByteLimit,
			rawStreamByteLimit: options.rawStreamByteLimit,
		},
	);
	activeSession = session;
	session.connect();
	await ready;
	await session.registerDocumentStream({
		requestId: REPLAY_REQUEST_ID,
		operationId: options.operationId,
		operationEpoch: options.operationEpoch,
		owner: "persist",
		kind: "replace",
		sourceIds: [options.sourceId],
		expectedEvents: options.targetEvents,
	});
	session.resetQueryDiagnostics();

	const chunkCount = Math.ceil(options.targetEvents / options.chunkEvents);
	let nextChunk = 0;
	while (nextChunk < chunkCount && !cancelled && !failure) {
		const pumpSize =
			options.profile === "steady"
				? STEADY_CHUNKS_PER_PUMP
				: chunkCount - nextChunk;
		const pumpEnd = Math.min(chunkCount, nextChunk + pumpSize);
		for (; nextChunk < pumpEnd && !failure; nextChunk++) {
			const chunk = buildChunk(options, nextChunk);
			activeSocket?.deliver(chunk.raw);
			emittedEvents += chunk.documentCount;
			if (chunk.last) {
				activeSocket?.deliver(
					JSON.stringify({
						type: "query_done",
						req_id: REPLAY_REQUEST_ID,
						operation_id: options.operationId,
						payload: {},
					}),
				);
			}
		}
		if (options.profile === "steady") await waitForSteadyPump(session);
		else await waitForRawDrain(session);
		postEvent({
			type: "PROGRESS",
			progress: {
				processedEvents: emittedEvents,
				totalEvents: options.targetEvents,
				diagnostics: session.getQueryDiagnostics(),
			},
		});
		if (options.profile === "steady") await yieldToWorker();
	}

	if (!cancelled) {
		await terminal;
		await waitForRawDrain(session);
	}
	const diagnostics = session.getQueryDiagnostics();
	const terminalFailure = failure as QueryStreamFailureEvent | null;
	const result: RendererQueryReplayResult = {
		status: cancelled
			? "cancelled"
			: terminalFailure?.reason === "backlog"
				? "backlog"
				: terminalFailure
					? "failed"
					: streamCompleted
						? "complete"
						: "failed",
		processedEvents: emittedEvents,
		queryDoneReceived,
		streamCompleted,
		latestSummary,
		failure: terminalFailure,
		diagnostics,
	};
	postEvent({ type: "COMPLETE", result });
}

/** Executes one replay-worker command. */
async function handleCommand(
	command: RendererQueryReplayWorkerCommand,
): Promise<void> {
	switch (command.type) {
		case "START":
			if (activeSession) throw new Error("Renderer query replay is already active.");
			await runReplay(command.options);
			break;
		case "REQUEST_PROJECTION": {
			if (!activeSession) throw new Error("Renderer query replay is not active.");
			try {
				const projection = await activeSession.requestRenderWindow(
					command.descriptor,
				);
				postEvent(
					{
						type: "PROJECTION_RESULT",
						projectionRequestId: command.projectionRequestId,
						projection,
					},
					projection ? getRenderWindowProjectionTransferables(projection) : [],
				);
			} catch (error) {
				postEvent({
					type: "PROJECTION_RESULT",
					projectionRequestId: command.projectionRequestId,
					projection: null,
					error: error instanceof Error ? error.message : String(error),
				});
			}
			break;
		}
		case "CANCEL":
			cancelled = true;
			activeSession?.destroy();
			activeSession = null;
			activeSocket = null;
			break;
	}
}

workerScope.onmessage = (
	event: MessageEvent<RendererQueryReplayWorkerCommand>,
): void => {
	void handleCommand(event.data).catch((error: unknown) => {
		postEvent({
			type: "ERROR",
			message: error instanceof Error ? error.message : String(error),
		});
	});
};

export {};
