import type { QueryCacheProvenance } from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
import type {
	EventStoreName,
	EventStoreWriteOperation,
} from "@/event-store/EventStore.backend";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import type {
	EventSourceHeadManifest,
	EventStoreNamespace,
} from "@/event-store/EventStore.types";
import { EventStoreViewportProjector } from "@/event-store/EventStore.viewport-projector";
import { QueryPersistenceCoordinator } from "@/event-store-worker/QueryPersistenceCoordinator";
import type {
	DocumentChunkReady,
	DocumentStreamDescriptor,
	QueryIngestionProgressEvent,
} from "@/event-stream";
import { TextDecoder, TextEncoder } from "util";

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

const SOURCE_COUNT = 70;
const EVENT_COUNT = 140_000;
const CHUNK_SIZE = 10_000;
const EVENTS_PER_SOURCE = EVENT_COUNT / SOURCE_COUNT;

(jest as unknown as { setTimeout(milliseconds: number): void }).setTimeout(30_000);

/** Counts physical page reads and source-manifest transactions. */
class IngestionProjectionBackend extends MemoryEventStoreBackend {
	public exactPageReads = 0;
	public lodPageReads = 0;
	public sourceReadTransactions = 0;

	/** Counts one point read before preserving backend clone semantics. */
	public override async get<TValue>(
		store: EventStoreName,
		key: IDBValidKey,
	): Promise<TValue | undefined> {
		if (store === "pages") this.exactPageReads++;
		if (store === "page_lod" && Array.isArray(key) && key.at(-1) === "lod") {
			this.lodPageReads++;
		}
		return super.get<TValue>(store, key);
	}

	/** Counts a source snapshot regardless of the number of visible rows. */
	public override async getMany<TValue>(
		store: EventStoreName,
		keys: readonly IDBValidKey[],
	): Promise<Array<TValue | undefined>> {
		if (store === "sources") this.sourceReadTransactions++;
		return super.getMany<TValue>(store, keys);
	}

	/** Resets projection counters after ingestion persistence completes. */
	public resetProjectionReads(): void {
		this.exactPageReads = 0;
		this.lodPageReads = 0;
		this.sourceReadTransactions = 0;
	}
}

class DelayedIngestionJournalBackend extends MemoryEventStoreBackend {
	/**
	 * Adds deterministic latency only to ingestion-journal puts.
	 * @param operations Atomic EventStore write set.
	 * @returns Promise settled after the optional delay and commit.
	 */
	public override async commit(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		if (
			operations.some(
				(operation) =>
					operation.type === "put" && operation.store === "stream_journal",
			)
		) {
			await new Promise<void>((resolve) => setTimeout(resolve, 1));
		}
		await super.commit(operations);
	}
}

/** Creates source IDs matching the reported 70-source workload. */
function createSourceIds(): Source.Id[] {
	return Array.from(
		{ length: SOURCE_COUNT },
		(_, index) => `ingestion-source-${index}` as Source.Id,
	);
}

/** Creates one bounded parser-order chunk with timestamps distributed across the frame. */
function createIngestionChunk(
	descriptor: DocumentStreamDescriptor,
	sourceIds: readonly Source.Id[],
	chunkIndex: number,
): DocumentChunkReady {
	const start = chunkIndex * CHUNK_SIZE;
	const count = Math.min(CHUNK_SIZE, EVENT_COUNT - start);
	const documents = Array.from({ length: count }, (_, index) => {
		const globalIndex = start + index;
		const sourceId = sourceIds[globalIndex % sourceIds.length];
		const sourceIndex = Math.floor(globalIndex / sourceIds.length);
		const timestamp =
			1_700_000_000_000 + ((sourceIndex * 7919 + globalIndex * 17) % EVENT_COUNT);
		return {
			_id: `ingestion-event-${globalIndex}`,
			"gulp.source_id": sourceId,
			"gulp.timestamp": timestamp,
			"gulp.event_code": globalIndex % 31,
			"event.sequence": globalIndex,
		};
	});
	return {
		descriptor,
		sequence: chunkIndex,
		rawByteLength: 0,
		message: {
			type: "docs_chunk",
			req_id: descriptor.requestId,
			operation_id: descriptor.operationId,
			payload: {
				docs: documents,
				last: start + count === EVENT_COUNT,
				chunk_num: chunkIndex,
			},
		},
	};
}

/** Creates exact full-source provenance for one finalized ingestion generation. */
function createQueryCacheProvenance(sourceId: Source.Id): QueryCacheProvenance {
	return {
		query: {
			string: "",
			filters: [],
			source_config: {
				operation_id: "ingestion-operation",
				source_ids: [sourceId],
				range: { min: "0", max: "2000000000000000000" },
			},
		},
		fingerprint: `ingestion:${sourceId}`,
		shapeFingerprint: "ingestion:all",
		normalization: {
			field: "gulp.event_code",
			hashFunction: "fnv1a",
			requestedFields: ["gulp.event_code"],
		},
		serverSnapshot: {
			total: EVENTS_PER_SOURCE,
			minNanoseconds: "0",
			maxNanoseconds: "2000000000000000000",
			timeUpdated: null,
			millisecondPrecision: true,
		},
	};
}

/** Waits for QueryWorker-style progress without retaining document payloads. */
async function waitForProgress(
	progress: readonly QueryIngestionProgressEvent[],
	expected: number,
): Promise<void> {
	const deadline = Date.now() + 5_000;
	while (progress.length < expected) {
		if (Date.now() >= deadline) {
			throw new Error("Timed out waiting for ingestion progress.");
		}
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
}

describe("ingestion cache timeline integration", () => {
	it("drains journaled data for 200 concurrent ingestion streams", async () => {
		const sourceCount = 200;
		const eventsPerSource = 10;
		const backend = new DelayedIngestionJournalBackend();
		const repository = new EventStoreRepository(backend);
		const completed = new Set<string>();
		const failures: string[] = [];
		const coordinator = new QueryPersistenceCoordinator(
			"concurrent-ingestion-test",
			{
				onSourceSummary: () => undefined,
				onIngestionProgress: () => undefined,
				onStreamFailure: (event) => failures.push(event.reason),
				onStreamFlushing: () => undefined,
				onStreamCompleted: (requestId) => completed.add(requestId),
				onDocumentChunkAcknowledged: () => undefined,
			},
			{
				rawGlobalByteLimit: 1,
				rawStreamByteLimit: 1,
				createOperationStore: () => ({ backend, repository }),
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
			},
		);
		const descriptors = Array.from({ length: sourceCount }, (_, index) => ({
			requestId: `concurrent-ingestion-request-${index}`,
			operationId: "concurrent-ingestion-operation",
			operationEpoch: 1,
			owner: "persist" as const,
			kind: "ingestion-build" as const,
			completionMode: "explicit" as const,
			sourceIds: [],
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a" as const,
			},
		}));
		for (const descriptor of descriptors) await coordinator.register(descriptor);
		for (let sourceIndex = 0; sourceIndex < descriptors.length; sourceIndex++) {
			const descriptor = descriptors[sourceIndex];
			const sourceId = `concurrent-ingestion-source-${sourceIndex}` as Source.Id;
			const documents = Array.from({ length: eventsPerSource }, (_, eventIndex) => ({
				_id: `concurrent-ingestion-event-${sourceIndex}-${eventIndex}`,
				"gulp.source_id": sourceId,
				"gulp.timestamp": 1_700_000_000_000 + eventIndex,
				"gulp.event_code": eventIndex,
			}));
			coordinator.handleDocumentChunk({
				descriptor,
				sequence: 0,
				rawByteLength: 2,
				message: {
					type: "docs_chunk",
					req_id: descriptor.requestId,
					operation_id: descriptor.operationId,
					payload: { docs: documents, last: true, chunk_num: 0 },
				},
			});
		}
		await Promise.all(
			descriptors.map((descriptor) => coordinator.complete(descriptor.requestId)),
		);

		const diagnostics = coordinator.getDiagnostics({
			started: 0,
			completed: 0,
			superseded: 0,
			retries: 0,
		});
		expect(failures).toEqual([]);
		expect(completed.size).toBe(sourceCount);
		expect(diagnostics.activeStreams).toBe(0);
		expect(diagnostics.rawBacklog.currentBytes).toBe(0);
		expect(diagnostics.normalizedBacklog.currentDocuments).toBe(0);
		expect(diagnostics.journalWrites.active).toBe(0);
		expect(diagnostics.journalWrites.currentBytes).toBe(0);
		expect(diagnostics.journalWrites.maximumActive > 0).toBe(true);
		expect(diagnostics.journalWrites.maximumActive <= 2).toBe(true);
		expect(diagnostics.journalWrites.completed).toBe(sourceCount);
		expect(await backend.getAll("stream_journal")).toEqual([]);
		const manifests = await backend.getAll<EventSourceHeadManifest>("sources");
		expect(manifests).toHaveLength(sourceCount);
		expect(
			manifests.every(
				(manifest) =>
					manifest.status === "complete" &&
					manifest.eventCount === eventsPerSource,
			),
		).toBe(true);
		coordinator.destroy();
	});

	it("keeps 70-source 140k-event pans off the IndexedDB page path after warmup", async () => {
		const sourceIds = createSourceIds();
		const backend = new IngestionProjectionBackend();
		const repository = new EventStoreRepository(backend);
		const progress: QueryIngestionProgressEvent[] = [];
		const failures: string[] = [];
		const coordinator = new QueryPersistenceCoordinator(
			"ingestion-timeline-test",
			{
				onSourceSummary: () => undefined,
				onIngestionProgress: (event) => progress.push(event),
				onStreamFailure: (event) => failures.push(event.reason),
				onStreamFlushing: () => undefined,
				onStreamCompleted: () => undefined,
				onDocumentChunkAcknowledged: () => undefined,
			},
			{
				createOperationStore: (_namespace: EventStoreNamespace) => ({
					backend,
					repository,
				}),
				createRevisionBus: () => ({
					publish: () => undefined,
					close: () => undefined,
				}),
			},
		);
		const descriptor: DocumentStreamDescriptor = {
			requestId: "ingestion-request",
			operationId: "ingestion-operation",
			operationEpoch: 1,
			owner: "persist",
			kind: "ingestion-build",
			completionMode: "explicit",
			sourceIds: [],
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		};
		await coordinator.register(descriptor);
		const chunkCount = Math.ceil(EVENT_COUNT / CHUNK_SIZE);
		for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
			coordinator.handleDocumentChunk(
				createIngestionChunk(descriptor, sourceIds, chunkIndex),
			);
			await waitForProgress(progress, chunkIndex + 1);
		}
		await coordinator.complete(descriptor.requestId, {
			queryCachesBySourceId: Object.fromEntries(
				sourceIds.map((sourceId) => [
					sourceId,
					createQueryCacheProvenance(sourceId),
				]),
			),
		});

		const manifests = await backend.getMany<EventSourceHeadManifest>(
			"sources",
			sourceIds,
		);
		expect(failures).toEqual([]);
		expect(manifests).toHaveLength(SOURCE_COUNT);
		expect(
			manifests.every(
				(manifest) =>
					manifest?.status === "complete" &&
					manifest.eventCount === EVENTS_PER_SOURCE &&
					manifest.activeCacheGeneration === manifest.generation,
			),
		).toBe(true);
		expect(
			manifests.some((manifest) => (manifest?.pages.length ?? 0) > 1),
		).toBe(true);
		expect(
			manifests.some((manifest) =>
				manifest?.pages.some((page, index, pages) =>
					pages.slice(index + 1).some(
						(other) =>
							page.minTimestamp <= other.maxTimestamp &&
							other.minTimestamp <= page.maxTimestamp,
					),
				),
			),
		).toBe(true);

		const minimum = Math.min(
			...manifests.map((manifest) => manifest?.minTimestamp ?? Infinity),
		);
		const maximum = Math.max(
			...manifests.map((manifest) => manifest?.maxTimestamp ?? -Infinity),
		);
		const projector = new EventStoreViewportProjector(backend);
		const projectionDescriptor = {
			operationEpoch: 1,
			viewportRevision: 1,
			projectionRevision: 1,
			frame: { min: minimum, max: maximum },
			width: 256,
			scale: 4,
			scrollX: 0,
			horizontalOverscanPixels: 0,
			maxRepresentatives: 1_000_000,
			evictedSourceIds: [],
			sources: sourceIds.map((sourceId) => ({
				sourceId,
				offset: 0,
				engine: "default" as const,
				frequency: 0,
				overrideHashes: [],
				settingsRevision: 1,
				overrideRevision: 0,
			})),
		};
		backend.resetProjectionReads();
		const initial = await projector.requestRenderWindow(projectionDescriptor);
		const firstLodReads = backend.lodPageReads;
		const firstExactReads = backend.exactPageReads;
		expect(initial.sources).toHaveLength(SOURCE_COUNT);
		expect(firstLodReads > 0).toBe(true);
		expect(firstExactReads > 0).toBe(true);
		expect(backend.sourceReadTransactions).toBe(2);

		backend.resetProjectionReads();
		const panned = await projector.requestRenderWindow({
			...projectionDescriptor,
			viewportRevision: 2,
			projectionRevision: 2,
			scrollX: 64,
		});
		expect(panned.sources).toHaveLength(SOURCE_COUNT);
		expect(backend.lodPageReads).toBe(0);
		expect(backend.exactPageReads).toBe(0);
		expect(backend.sourceReadTransactions).toBe(2);
		const diagnostics = projector.getDiagnostics();
		expect(diagnostics.exactPageCacheBytes > 0).toBe(true);
		expect(diagnostics.exactPageCacheBytes <= 16 * 1024 * 1024).toBe(true);
		expect(diagnostics.lodPageCacheBytes > 0).toBe(true);
		expect(diagnostics.lodPageCacheBytes <= 4 * 1024 * 1024).toBe(true);
		coordinator.destroy();
	});
});
