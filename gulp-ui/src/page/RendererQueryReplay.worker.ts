import type { Operation } from "@/entities/Operation";
import type { EventStoreWriteOperation } from "@/event-store/EventStore.backend";
import type { EventStoreNamespace } from "@/event-store/EventStore.types";
import { getRendererQueryReplaySources } from "./RendererTest.query-replay";
import { IndexedDbEventStoreBackend } from "@/event-store/IndexedDbEventStore.backend";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import type { TimelineQueryRequest } from "@/event-stream";
import type { QueryCacheProvenance } from "@/entities/QueryCache";
import type { Doc } from "@/entities/Doc";
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

class DelayedIndexedDbEventStoreBackend extends IndexedDbEventStoreBackend {
	/**
	 * Creates a real browser backend with an optional test-only commit delay.
	 * @param namespace IndexedDB cache and operation identity.
	 * @param commitDelayMs Artificial delay before each transaction.
	 */
	public constructor(
		namespace: EventStoreNamespace,
		private readonly commitDelayMs: number,
	) {
		super(namespace);
	}

	/**
	 * Delays every transaction to make producer/consumer imbalance observable.
	 * @param operations Atomic EventStore write set.
	 * @returns Promise settled after the delayed transaction completes.
	 */
	public override async commit(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		if (this.commitDelayMs > 0) {
			await new Promise<void>((resolve) =>
				setTimeout(resolve, this.commitDelayMs),
			);
		}
		await super.commit(operations);
	}
}

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
let releaseVerification = (): void => undefined;

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
	sourceOffset = 0,
	requestId = REPLAY_REQUEST_ID,
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
			_id: `renderer-query-${(globalIndex + sourceOffset).toString(16).padStart(8, "0")}`,
			"gulp.source_id": options.sourceId,
			"gulp.event_code": globalIndex % 8_000,
			"@timestamp":
				options.documentMinTimestamp + Math.floor(options.frameSpanMs * ratio),
		};
	}
	const last = offset + documentCount >= options.targetEvents;
	const message: QuerySocketMessage = {
		type: "docs_chunk",
		req_id: requestId,
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
 * Replays concurrent file-ingestion streams without query acknowledgements.
 * @param options Ingestion workload, storage delay, and raw-limit configuration.
 * @returns Promise settled after terminal diagnostics are published.
 */
async function runIngestionReplay(
	options: RendererQueryReplayStartOptions,
): Promise<void> {
	cancelled = false;
	const sources = getRendererQueryReplaySources(options);
	const summaries = new Map<string, QuerySourceSummaryEvent>();
	const completed = new Set<string>();
	let failure: QueryStreamFailureEvent | null = null;
	let emittedEvents = 0;
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
		onForwardMessage: () => undefined,
		onStreamStateChange: (change) => {
			if (change.state === "completed") completed.add(change.descriptor.requestId);
		},
		onSourceSummary: (event) => {
			summaries.set(event.summary.sourceId, event);
			postEvent({ type: "SOURCE_SUMMARY", event });
		},
		onIngestionProgress: () => undefined,
		onStreamFailure: (event) => {
			failure ??= event;
		},
		onProtocolError: (details) => {
			throw new Error(details.message);
		},
	};
	const session = new QuerySocketSession(
		{
			socketUrl: "ws://renderer-ingestion-replay.invalid",
			token: "renderer-ingestion-replay",
			wsId: "renderer-ingestion-replay",
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
			rawGlobalByteLimit: options.rawGlobalByteLimit ?? 4 * 1024 ** 2,
			rawStreamByteLimit: options.rawStreamByteLimit ?? 1024 ** 2,
			createOperationStore: (namespace) => {
				const backend = new DelayedIndexedDbEventStoreBackend(
					namespace,
					options.commitDelayMs ?? 10,
				);
				return {
					backend,
					repository: new EventStoreRepository(backend),
				};
			},
			createRevisionBus: () => ({
				publish: () => undefined,
				close: () => undefined,
			}),
		},
	);
	activeSession = session;
	session.connect();
	await ready;
	for (const source of sources) {
		await session.registerDocumentStream({
			requestId: source.requestId,
			operationId: options.operationId,
			operationEpoch: options.operationEpoch,
			owner: "persist",
			kind: "ingestion-build",
			completionMode: "explicit",
			sourceIds: [],
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
	}
	session.resetQueryDiagnostics();

	const concurrency = Math.max(
		1,
		Math.min(sources.length, options.ingestionConcurrency ?? 5),
	);
	for (
		let batchStart = 0;
		batchStart < sources.length && !cancelled && !failure;
		batchStart += concurrency
	) {
		const batch = sources.slice(batchStart, batchStart + concurrency);
		const nextChunkByRequest = new Map(
			batch.map((source) => [source.requestId, 0]),
		);
		let pendingSources = [...batch];
		while (pendingSources.length > 0 && !cancelled && !failure) {
			const continuing: typeof sources = [];
			for (const source of pendingSources) {
				const chunkIndex = nextChunkByRequest.get(source.requestId) ?? 0;
				const chunk = buildChunk(
					{
						...options,
						sourceId: source.sourceId,
						targetEvents: source.count,
					},
					chunkIndex,
					source.offset,
					source.requestId,
				);
				activeSocket?.deliver(chunk.raw);
				emittedEvents += chunk.documentCount;
				nextChunkByRequest.set(source.requestId, chunkIndex + 1);
				if (!chunk.last) continuing.push(source);
			}
			pendingSources = continuing;
			postEvent({
				type: "PROGRESS",
				progress: {
					processedEvents: emittedEvents,
					totalEvents: options.targetEvents,
					diagnostics: session.getQueryDiagnostics(),
				},
			});
			await yieldToWorker();
		}
		if (!cancelled && !failure) {
			await Promise.all(
				batch.map((source) =>
					session.completeDocumentStream(source.requestId),
				),
			);
		}
	}
	if (!cancelled && !failure) await waitForRawDrain(session);
	const sourceSummaries = [...summaries.values()];
	const terminalFailure = failure as QueryStreamFailureEvent | null;
	postEvent({
		type: "COMPLETE",
		result: {
			status: cancelled
				? "cancelled"
				: terminalFailure
					? "failed"
					: completed.size === sources.length
						? "complete"
						: "failed",
			processedEvents: emittedEvents,
			targetEvents: options.targetEvents,
			queryDoneReceived: false,
			streamCompleted: completed.size === sources.length,
			sourceSummaries,
			expectedPages: sources.reduce(
				(total, source) => total + Math.ceil(source.count / EVENT_PAGE_SIZE),
				0,
			),
			latestSummary: sourceSummaries.at(-1) ?? null,
			failure: terminalFailure,
			diagnostics: session.getQueryDiagnostics(),
		},
	});
}

/**
 * Executes one real QuerySocketSession replay against IndexedDB.
 * @param options Steady, burst, or ingestion replay options.
 */
async function runReplay(options: RendererQueryReplayStartOptions): Promise<void> {
	if (options.profile === "steady") return runTimelineReplay(options);
	if (options.profile === "ingestion") return runIngestionReplay(options);
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
		const pumpSize = chunkCount - nextChunk;
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
		await waitForRawDrain(session);
		postEvent({
			type: "PROGRESS",
			progress: {
				processedEvents: emittedEvents,
				totalEvents: options.targetEvents,
				diagnostics: session.getQueryDiagnostics(),
			},
		});
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

/** Replays real source-local replacement scheduling with bounded synthetic HTTP/WS peers. */
async function runTimelineReplay(options: RendererQueryReplayStartOptions): Promise<void> {
	cancelled = false;
	const verificationReady = new Promise<void>((resolve) => { releaseVerification = resolve; });
	const sources = getRendererQueryReplaySources(options);
	const summaries = new Map<string, QuerySourceSummaryEvent>();
	const completed = new Set<string>();
	const server = new Map<string, { source: typeof sources[number]; chunk: number }>();
	let failure: QueryStreamFailureEvent | null = null;
	let emittedEvents = 0;
	let queryDoneCount = 0;
	let cacheHits = 0;
	let httpQueries = 0;
	let lastProgressAt = 0;
	let resolveReady = (): void => undefined;
	const ready = new Promise<void>((resolve) => { resolveReady = resolve; });
	const session = new QuerySocketSession({ socketUrl: "ws://renderer-query-replay.invalid/ws", apiUrl: "http://renderer-query-replay.invalid", token: "renderer-query-replay", wsId: "renderer-query-replay", cacheNamespaceId: options.cacheNamespaceId }, {
		onSocketState: (state) => { if (state === "ready") resolveReady(); },
		onSocketOpen: () => undefined, onSocketClose: () => undefined, onSocketError: () => undefined,
		onForwardMessage: (message) => { if (message.type === "query_done") queryDoneCount++; },
		onStreamStateChange: (change) => { if (change.state === "completed") completed.add(change.descriptor.requestId); },
		onSourceSummary: (event) => { summaries.set(event.summary.sourceId, event); postEvent({ type: "SOURCE_SUMMARY", event }); },
		onIngestionProgress: () => undefined,
		onStreamFailure: (event) => { failure = event; },
		onTimelineQueryUpdate: (update) => {
			if (update.state === "cache-hit") cacheHits++;
			if (update.state === "failed") failure = { ...update, reason: "storage", message: update.error ?? "Replay query failed" };
		},
		onProtocolError: (details) => { throw new Error(details.message); },
	}, () => {
		const socket = new SyntheticReplaySocket(); activeSocket = socket; socket.open(); return socket as unknown as WebSocket;
	}, { rawGlobalByteLimit: options.rawGlobalByteLimit, rawStreamByteLimit: options.rawStreamByteLimit }, async (input) => {
		const url = new URL(String(input));
		const id = url.searchParams.get("req_id")!;
		if (url.pathname === "/query_raw") {
			const source = sources.find((entry) => entry.requestId === id);
			if (!source) throw new Error(`Unexpected replay HTTP query ${id}`);
			server.set(id, { source, chunk: 0 }); httpQueries++;
		} else server.delete(id);
		return new Response(JSON.stringify({ status: "pending" }), { status: 200 });
	});
	activeSession = session; session.connect(); await ready;
	const range = { min: String(BigInt(options.documentMinTimestamp) * 1_000_000n), max: String(BigInt(options.documentMinTimestamp + options.frameSpanMs) * 1_000_000n) };
	const requests: TimelineQueryRequest[] = sources.map((source) => {
		const provenance: QueryCacheProvenance = {
			query: { string: "Replay query", filters: [], source_config: { operation_id: options.operationId, source_ids: [source.sourceId], range } },
			fingerprint: `${source.sourceId}:${source.count}:${source.offset}:${range.min}`, shapeFingerprint: `${source.sourceId}:${source.count}:${source.offset}`,
			normalization: { field: "gulp.event_code", hashFunction: "fnv1a", requestedFields: [] },
			serverSnapshot: { total: source.count, minNanoseconds: range.min, maxNanoseconds: range.max, timeUpdated: 1, millisecondPrecision: false },
		};
		return { sourceId: source.sourceId, cachePolicy: "force", body: { q: [{ q: { match_all: {} } }], q_options: {} }, desired: { fingerprint: provenance.fingerprint, shapeFingerprint: provenance.shapeFingerprint!, range, serverSnapshot: provenance.serverSnapshot }, descriptor: { requestId: source.requestId, operationId: options.operationId, operationEpoch: options.operationEpoch, owner: "persist", kind: "replace", sourceIds: [source.sourceId], expectedEvents: source.count, queryCache: provenance, durableProjectionOnly: true } };
	});
	session.resetQueryDiagnostics();
	await session.startTimelineQueries(requests);
	while (!cancelled && !failure && completed.size < sources.length) {
		// Round-robin active requests; JSON and normalized backlogs drain between pumps.
		for (const [id, entry] of [...server.entries()].slice(0, STEADY_CHUNKS_PER_PUMP)) {
			server.delete(id);
			const chunk = buildChunk({ ...options, sourceId: entry.source.sourceId, targetEvents: entry.source.count }, entry.chunk++, entry.source.offset, id);
			activeSocket?.deliver(chunk.raw); emittedEvents += chunk.documentCount;
			if (!chunk.last) server.set(id, entry);
			else activeSocket?.deliver(JSON.stringify({ type: "query_done", req_id: id, operation_id: options.operationId, payload: {} }));
		}
		await waitForRawDrain(session);
		while (!cancelled && !failure && session.getQueryDiagnostics().commits.active > 0) await yieldToWorker();
		if (performance.now() - lastProgressAt >= 100) {
			lastProgressAt = performance.now();
			postEvent({ type: "PROGRESS", progress: { processedEvents: emittedEvents, totalEvents: options.targetEvents, diagnostics: session.getQueryDiagnostics() } });
		}
		await yieldToWorker();
	}
	let sampledDocuments = 0;
	if (!cancelled && !failure) {
		// Exact sample reads decode many pages. They are test-only work and must
		// start after measuring the terminal frame, not compete with its projection.
		await verificationReady;
	}
	if (!cancelled && !failure) {
		const repository = new EventStoreRepository(new IndexedDbEventStoreBackend({ cacheNamespaceId: options.cacheNamespaceId, operationId: options.operationId as Operation.Id }));
		try {
			const samples = sources.flatMap((source) => [...new Set([0, Math.floor(source.count / 2), source.count - 1])].filter((index) => index >= 0 && index < source.count).map((index) => ({ source, index, id: `renderer-query-${(source.offset + index).toString(16).padStart(8, "0")}` as Doc.Id })));
			const documents = await repository.getByIds(samples.map((sample) => sample.id));
			const byId = new Map(documents.map((document) => [document._id, document]));
			for (const sample of samples) {
				const document = byId.get(sample.id);
				if (!document || document["gulp.source_id"] !== sample.source.sourceId || document.indicatorValue !== sample.index % 8000) throw new Error(`Persisted replay sample mismatch: ${sample.id}`);
				sampledDocuments++;
			}
			for (const source of sources) if (summaries.get(source.sourceId)?.summary.eventCount !== source.count) throw new Error(`Replay source count mismatch: ${source.sourceId}`);
		} finally { repository.close(); }
		const before = httpQueries;
		await session.startTimelineQueries(requests.map((request) => ({ ...request, cachePolicy: "reuse", descriptor: { ...request.descriptor, requestId: `${request.descriptor.requestId}-cached` } })));
		while (!cancelled && !failure && cacheHits < sources.length) await yieldToWorker();
		if (httpQueries !== before) throw new Error("Cache replay dispatched unexpected HTTP queries");
	}
	const sourceSummaries = [...summaries.values()];
	const terminalFailure = failure as QueryStreamFailureEvent | null;
	postEvent({ type: "COMPLETE", result: {
		status: cancelled ? "cancelled" : terminalFailure ? "failed" : "complete", processedEvents: emittedEvents, targetEvents: options.targetEvents,
		sourceSummaries, expectedPages: sources.reduce((sum, source) => sum + Math.ceil(source.count / EVENT_PAGE_SIZE), 0), cacheHits, sampledDocuments,
		queryDoneReceived: queryDoneCount === sources.length, streamCompleted: completed.size === sources.length,
		latestSummary: sourceSummaries[sourceSummaries.length - 1] ?? null, failure: terminalFailure, diagnostics: session.getQueryDiagnostics(),
	} });
}

/** Executes one replay-worker command. */
async function handleCommand(
	command: RendererQueryReplayWorkerCommand,
): Promise<void> {
	switch (command.type) {
		case "VERIFY_RESULTS":
			releaseVerification();
			break;
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
			releaseVerification();
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
