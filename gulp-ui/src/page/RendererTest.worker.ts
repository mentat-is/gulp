/// <reference lib="webworker" />

import {
	EventStoreRepository,
	EventStoreViewportProjector,
	IndexedDbEventStoreBackend,
} from "@/event-store";
import { getRenderWindowProjectionTransferables } from "@/event-store-worker/QueryProjection.transfer";
import {
	createEmptyRendererBenchmarkDiagnostics,
	RendererBenchmarkEventStore,
} from "./RendererTest.benchmark";
import type {
	RendererBenchmarkWorkerCommand,
	RendererBenchmarkWorkerEvent,
} from "./RendererTest.worker-protocol";

const workerScope = self as DedicatedWorkerGlobalScope;
const PROGRESS_INTERVAL_MS = 100;
let benchmarkStore: RendererBenchmarkEventStore | null = null;
let generationRunning = false;
let cancelled = false;

/** Publishes a compact worker result, transferring projection buffers when supplied. */
function postBenchmarkEvent(
	event: RendererBenchmarkWorkerEvent,
	transfer: Transferable[] = [],
): void {
	workerScope.postMessage(event, transfer);
}

/** Lets projection and cancellation messages run between durable page commits. */
function yieldBetweenPages(): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * Writes bounded pages in the worker and publishes coalesced progress.
 * @param command Start command carrying the namespace and deterministic fixture.
 * @returns Promise settled after completion, quota skip, or cancellation.
 */
async function runGeneration(
	command: Extract<RendererBenchmarkWorkerCommand, { type: "START" }>,
): Promise<void> {
	if (benchmarkStore) throw new Error("Renderer benchmark worker is already active.");
	cancelled = false;
	generationRunning = true;
	const backend = new IndexedDbEventStoreBackend(command.namespace);
	const store = new RendererBenchmarkEventStore({
		writer: new EventStoreRepository(backend),
		projector: new EventStoreViewportProjector(backend),
		publishSourceSummary: (summary) =>
			postBenchmarkEvent({ type: "SOURCE_SUMMARY", summary }),
	});
	benchmarkStore = store;
	try {
		let progress = await store.start(command.options);
		postBenchmarkEvent({ type: "STARTED", progress });
		let lastProgressAt = 0;
		while (!progress.done && !cancelled) {
			progress = await store.appendNextPage();
			const now = performance.now();
			if (
			progress.done ||
			progress.persistedEvents <= 16_384 ||
			now - lastProgressAt >= PROGRESS_INTERVAL_MS
			) {
				lastProgressAt = now;
				postBenchmarkEvent({ type: "PROGRESS", progress });
			}
			if (!progress.done) await yieldBetweenPages();
		}
		if (cancelled) {
			postBenchmarkEvent({ type: "CANCELLED", diagnostics: await store.cancel() });
			return;
		}
		if (progress.status === "writing") {
			const diagnostics = await store.finalize();
			progress = {
				...progress,
				status: diagnostics.status,
				persistedEvents: diagnostics.persistedEvents,
				done: true,
				diagnostics,
			};
		}
		postBenchmarkEvent({ type: "FINISHED", progress });
	} finally {
		generationRunning = false;
	}
}

/**
 * Handles one correlated projection or upsert while generation may still run.
 * @param command Worker command to execute.
 * @returns Promise settled after its result has been posted.
 */
async function handleCommand(command: RendererBenchmarkWorkerCommand): Promise<void> {
	switch (command.type) {
		case "START":
			await runGeneration(command);
			break;
		case "PROJECT": {
			try {
				const projection = await benchmarkStore?.projectRenderWindow(command.descriptor) ?? null;
				postBenchmarkEvent(
					{
						type: "PROJECTION",
						requestId: command.requestId,
						projection,
						diagnostics: benchmarkStore?.getDiagnostics() ??
							createEmptyRendererBenchmarkDiagnostics(),
					},
					projection ? getRenderWindowProjectionTransferables(projection) : [],
				);
			} catch (error) {
				postBenchmarkEvent({
					type: "PROJECTION",
					requestId: command.requestId,
					projection: null,
					diagnostics: benchmarkStore?.getDiagnostics() ??
						createEmptyRendererBenchmarkDiagnostics(),
					error: error instanceof Error ? error.message : String(error),
				});
			}
			break;
		}
		case "UPSERT": {
			try {
				if (!benchmarkStore) throw new Error("Renderer benchmark store is unavailable.");
				const summaries = await benchmarkStore.upsertDocuments(command.documents);
				postBenchmarkEvent({
					type: "UPSERTED",
					requestId: command.requestId,
					summaries,
					diagnostics: benchmarkStore.getDiagnostics(),
				});
			} catch (error) {
				postBenchmarkEvent({
					type: "UPSERTED",
					requestId: command.requestId,
					summaries: [],
					diagnostics: benchmarkStore?.getDiagnostics() ??
						createEmptyRendererBenchmarkDiagnostics(),
					error: error instanceof Error ? error.message : String(error),
				});
			}
			break;
		}
		case "CANCEL":
			cancelled = true;
			if (!generationRunning && benchmarkStore) {
				postBenchmarkEvent({
					type: "CANCELLED",
					diagnostics: await benchmarkStore.cancel(),
				});
			}
			break;
	}
}

workerScope.onmessage = (event: MessageEvent<RendererBenchmarkWorkerCommand>): void => {
	void handleCommand(event.data).catch((error: unknown) => {
		postBenchmarkEvent({
			type: "ERROR",
			message: error instanceof Error ? error.message : String(error),
		});
	});
};

export {};
