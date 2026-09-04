import type { Doc } from "@/entities/Doc";
import type { QueryCacheProvenance } from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import {
	EVENT_PAGE_LOD_RECORD_KIND,
	EVENT_PAGE_SIZE,
	EVENT_PENDING_DOCUMENT_LIMIT,
	type EncodedEventPageLod,
	type EventSourceManifest,
	type EventSourceSummary,
	type EventStoreNamespace,
} from "@/event-store/EventStore.types";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import {
	QueryPersistenceCoordinator,
	type QueryPersistenceCoordinatorOptions,
} from "@/event-store-worker/QueryPersistenceCoordinator";
import type {
	DocumentChunkAcknowledgement,
	DocumentChunkReady,
	DocumentStreamDescriptor,
	QueryIngestionProgressEvent,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
} from "@/event-stream";
import { TextDecoder, TextEncoder } from "util";

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

const MEBIBYTE = 1024 ** 2;
const SOURCE_ONE = "source-one" as Source.Id;
const SOURCE_TWO = "source-two" as Source.Id;
const SOURCE_THREE = "source-three" as Source.Id;
const SOURCE_FOUR = "source-four" as Source.Id;

/** Creates one deterministic origin-wide lock manager shared by logical workers. */
function createCrossRealmLockManager(): Pick<LockManager, "request"> {
	const tails = new Map<string, Promise<void>>();
	const request = async <TResult>(
		name: string,
		options: LockOptions,
		callback: (lock: Lock | null) => TResult | PromiseLike<TResult>,
	): Promise<TResult> => {
		const previous = tails.get(name) ?? Promise.resolve();
		const result = previous.then(() =>
			callback({
				name,
				mode: options.mode ?? "exclusive",
			} as Lock),
		);
		const tail = result.then(
			() => undefined,
			() => undefined,
		);
		tails.set(name, tail);
		void tail.then(() => {
			if (tails.get(name) === tail) tails.delete(name);
		});
		return result;
	};
	return { request } as unknown as Pick<LockManager, "request">;
}

interface PersistenceHarness {
	coordinator: QueryPersistenceCoordinator;
	backends: Map<string, MemoryEventStoreBackend>;
	repositories: Map<string, EventStoreRepository>;
	summaries: QuerySourceSummaryEvent[];
	ingestionProgress: QueryIngestionProgressEvent[];
	failures: QueryStreamFailureEvent[];
	flushing: string[];
	completed: string[];
	broadcasts: EventSourceSummary[];
	acknowledgements: DocumentChunkAcknowledgement[];
}

/**
 * Creates one isolated coordinator backed by deterministic in-memory storage.
 * @param limits Optional raw backlog limits for boundary tests.
 * @returns Coordinator harness and captured lifecycle events.
 */
function createHarness(
	limits: Pick<
		QueryPersistenceCoordinatorOptions,
		"rawGlobalByteLimit" | "rawStreamByteLimit"
	> = {},
): PersistenceHarness {
	const backends = new Map<string, MemoryEventStoreBackend>();
	const repositories = new Map<string, EventStoreRepository>();
	const summaries: QuerySourceSummaryEvent[] = [];
	const ingestionProgress: QueryIngestionProgressEvent[] = [];
	const failures: QueryStreamFailureEvent[] = [];
	const flushing: string[] = [];
	const completed: string[] = [];
	const broadcasts: EventSourceSummary[] = [];
	const acknowledgements: DocumentChunkAcknowledgement[] = [];
	const coordinator = new QueryPersistenceCoordinator(
		"test-coordinator",
		{
			onSourceSummary: (event) => summaries.push(event),
			onIngestionProgress: (event) => ingestionProgress.push(event),
			onStreamFailure: (event) => failures.push(event),
			onStreamFlushing: (requestId) => flushing.push(requestId),
			onStreamCompleted: (requestId) => completed.push(requestId),
			onDocumentChunkAcknowledged: (acknowledgement) =>
				acknowledgements.push(acknowledgement),
		},
		{
			...limits,
			createOperationStore: (namespace: EventStoreNamespace) => {
				const operationId = namespace.operationId as string;
				const backend = new MemoryEventStoreBackend();
				const repository = new EventStoreRepository(backend);
				backends.set(operationId, backend);
				repositories.set(operationId, repository);
				return { backend, repository };
			},
			createRevisionBus: () => ({
				publish: (summary) => broadcasts.push(summary),
				close: () => undefined,
			}),
		},
	);
	return {
		coordinator,
		backends,
		repositories,
		summaries,
		ingestionProgress,
		failures,
		flushing,
		completed,
		broadcasts,
		acknowledgements,
	};
}

/** Creates one registered stream descriptor with test defaults. */
function createDescriptor(
	requestId: string,
	overrides: Partial<DocumentStreamDescriptor> = {},
): DocumentStreamDescriptor {
	return {
		requestId,
		operationId: "operation-one",
		operationEpoch: 1,
		owner: "persist",
		kind: "replace",
		sourceIds: [SOURCE_ONE],
		...overrides,
	};
}

/** Creates deterministic source-scoped query provenance for cache-pool tests. */
function createQueryCacheProvenance(
	fingerprint: string,
): QueryCacheProvenance {
	return {
		query: {
			string: fingerprint,
			filters: [],
			source_config: {
				operation_id: "operation-one",
				source_ids: [SOURCE_ONE],
				range: { min: 0, max: 100 },
			},
		},
		fingerprint,
		shapeFingerprint: `shape:${fingerprint}`,
		normalization: {
			field: "gulp.event_code",
			hashFunction: "fnv1a",
			requestedFields: [],
		},
		serverSnapshot: {
			total: 1,
			minNanoseconds: "0",
			maxNanoseconds: "100000000",
			timeUpdated: null,
			millisecondPrecision: true,
		},
	};
}

/** Creates the desired lookup corresponding to a provenance fixture. */
function createDesiredQueryCacheLookup(fingerprint: string) {
	const provenance = createQueryCacheProvenance(fingerprint);
	return {
		fingerprint: provenance.fingerprint,
		shapeFingerprint: provenance.shapeFingerprint!,
		range: { min: "0", max: "100" },
		serverSnapshot: provenance.serverSnapshot,
	};
}

/** Creates one raw backend document for worker-side normalization. */
function createRawDocument(
	id: string,
	sourceId: string = SOURCE_ONE,
	timestamp = 1_700_000_000_000,
	extra: Record<string, unknown> = {},
): Record<string, unknown> {
	return {
		_id: id,
		"gulp.source_id": sourceId,
		"gulp.timestamp": timestamp,
		"gulp.event_code": 7,
		...extra,
	};
}

/**
 * Creates one large deterministic raw chunk without sharing document identities.
 * @param prefix Identifier prefix unique to the chunk.
 * @param count Number of documents to create.
 * @param sourceId Source assigned to every document.
 * @param timestampBase First timestamp used by the chunk.
 * @returns Raw documents accepted by worker-side normalization.
 */
function createRawDocuments(
	prefix: string,
	count: number,
	sourceId: Source.Id,
	timestampBase: number,
): Record<string, unknown>[] {
	return Array.from({ length: count }, (_, index) =>
		createRawDocument(
			`${prefix}-${index}`,
			sourceId,
			timestampBase + index,
		),
	);
}

/** Creates one router-to-persistence chunk. */
function createChunk(
	descriptor: DocumentStreamDescriptor,
	documents: readonly unknown[],
	last: boolean,
	sequence = 0,
	rawByteLength = 0,
): DocumentChunkReady {
	return {
		descriptor,
		sequence,
		rawByteLength,
		message: {
			type: "docs_chunk",
			req_id: descriptor.requestId,
			operation_id: descriptor.operationId,
			payload: { docs: documents, last, chunk_num: sequence },
		},
	};
}

/** Waits until an asynchronous worker-side condition becomes true. */
async function waitFor(
	predicate: () => boolean,
	timeoutMs = 1_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("Timed out waiting for condition.");
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
}

/**
 * Persists and completes one replacement fixture before a capacity-sensitive test.
 * @param harness Coordinator harness that owns the operation store.
 * @param descriptor Replacement identity and declared source authority.
 * @param documents Raw documents forming the complete rollback generation.
 */
async function seedCompleteReplacement(
	harness: PersistenceHarness,
	descriptor: DocumentStreamDescriptor,
	documents: readonly unknown[],
): Promise<void> {
	await harness.coordinator.register(descriptor);
	harness.coordinator.handleDocumentChunk(
		createChunk(descriptor, documents, true),
	);
	await waitFor(() => harness.completed.includes(descriptor.requestId));
}

describe("QueryPersistenceCoordinator", () => {
	it("queues cache controls behind an active replacement source lease", async () => {
		const harness = createHarness();
		await seedCompleteReplacement(
			harness,
			createDescriptor("lease-cache-seed", {
				queryCache: createQueryCacheProvenance("lease-seed"),
			}),
			[createRawDocument("lease-seed")],
		);
		const replacement = createDescriptor("lease-cache-replacement", {
			queryCache: createQueryCacheProvenance("lease-replacement"),
		});
		await harness.coordinator.register(replacement);

		let reconciliationSettled = false;
		const reconciliation = harness.coordinator
			.reconcileSourceQueryCache({
				operationId: "operation-one",
				sourceId: SOURCE_ONE,
				desired: createDesiredQueryCacheLookup("lease-replacement"),
			})
			.finally(() => {
				reconciliationSettled = true;
			});
		await Promise.resolve();
		expect(reconciliationSettled).toBe(false);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				replacement,
				[createRawDocument("lease-replacement")],
				true,
			),
		);
		await waitFor(() => harness.completed.includes(replacement.requestId));
		expect((await reconciliation)?.cache.provenance.fingerprint).toBe(
			"lease-replacement",
		);
		harness.coordinator.destroy();
	});

	it("serializes cache reconciliation and source-local history navigation", async () => {
		const harness = createHarness();
		const first = createDescriptor("cache-first", {
			queryCache: createQueryCacheProvenance("fingerprint-one"),
		});
		await seedCompleteReplacement(
			harness,
			first,
			[createRawDocument("cache-one")],
		);
		const second = createDescriptor("cache-second", {
			queryCache: createQueryCacheProvenance("fingerprint-two"),
		});
		await seedCompleteReplacement(
			harness,
			second,
			[createRawDocument("cache-two")],
		);

		const repository = harness.repositories.get("operation-one");
		if (!repository) throw new Error("Expected operation repository.");
		const initialCaches = await repository.listQueryCaches(SOURCE_ONE);
		expect(initialCaches).toHaveLength(2);
		const secondAuthority = (
			await repository.getSourceSummary(SOURCE_ONE)
		)?.authorityRevision;

		const undo = await harness.coordinator.navigateSourceQueryCache({
			operationId: "operation-one",
			sourceId: SOURCE_ONE,
			direction: "undo",
		});
		expect(undo?.cache.provenance.fingerprint).toBe("fingerprint-one");
		expect(
			(undo?.summary.authorityRevision ?? -1) > (secondAuthority ?? -1),
		).toBe(true);
		expect(harness.broadcasts.at(-1)).toEqual(undo?.summary);

		const redo = await harness.coordinator.navigateSourceQueryCache({
			operationId: "operation-one",
			sourceId: SOURCE_ONE,
			direction: "redo",
		});
		expect(redo?.cache.provenance.fingerprint).toBe("fingerprint-two");

		const reconciliation =
			await harness.coordinator.reconcileSourceQueryCache({
				operationId: "operation-one",
				sourceId: SOURCE_ONE,
				desired: createDesiredQueryCacheLookup("fingerprint-one"),
			});
		expect(reconciliation).toMatchObject({
			match: "exact",
			cache: { provenance: { fingerprint: "fingerprint-one" } },
			summary: { cacheHistoryCursor: 0 },
		});
		if (!reconciliation) throw new Error("Expected reconciled query cache.");
		expect(reconciliation.summary.cacheHistory).toEqual([
			reconciliation.cache.generation,
		]);
		expect(await repository.listQueryCaches(SOURCE_ONE)).toHaveLength(1);

		await harness.coordinator.clearSourceQueryCache(
			"operation-one",
			SOURCE_ONE,
		);
		const resetSummary = await repository.getSourceSummary(SOURCE_ONE);
		expect(resetSummary).toMatchObject({
			status: "incomplete",
			eventCount: 0,
			activeCacheGeneration: null,
			cacheHistory: [],
			cacheHistoryCursor: -1,
		});
		expect(harness.broadcasts.at(-1)).toEqual(resetSummary);
		expect(await repository.listQueryCaches(SOURCE_ONE)).toHaveLength(0);

		const broadcastCount = harness.broadcasts.length;
		await harness.coordinator.deleteSourceEventStore(
			"operation-one",
			SOURCE_ONE,
		);
		expect(await repository.getSourceSummary(SOURCE_ONE)).toBeNull();
		expect(harness.broadcasts).toHaveLength(broadcastCount);
		harness.coordinator.destroy();
	});

	it("registers persistence without consulting quota telemetry", async () => {
		const storageDescriptor = Object.getOwnPropertyDescriptor(
			globalThis.navigator,
			"storage",
		);
		let estimateCalls = 0;
		Object.defineProperty(globalThis.navigator, "storage", {
			configurable: true,
			value: {
				estimate: async () => {
					estimateCalls += 1;
					throw new Error("estimate must not be called");
				},
			},
		});
		const harness = createHarness();
		const descriptor = createDescriptor("no-capacity-telemetry");

		try {
			await harness.coordinator.register(descriptor);
			expect(harness.coordinator.hasStream(descriptor.requestId)).toBe(true);
			expect(estimateCalls).toBe(0);
			expect(harness.failures).toHaveLength(0);
			await harness.coordinator.cancel(descriptor.requestId);
		} finally {
			harness.coordinator.destroy();
			if (storageDescriptor) {
				Object.defineProperty(
					globalThis.navigator,
					"storage",
					storageDescriptor,
				);
			} else {
				Reflect.deleteProperty(globalThis.navigator, "storage");
			}
		}
	});

	it("classifies a real IndexedDB quota rejection during commit", async () => {
		const harness = createHarness();
		type CommitSlotRunner = <TResult>(
			task: () => Promise<TResult>,
		) => Promise<TResult>;
		const runWithSlot = (
			harness.coordinator as unknown as { withCommitSlot: CommitSlotRunner }
		).withCommitSlot.bind(harness.coordinator);
		const quotaError = new Error("origin is full");
		quotaError.name = "QuotaExceededError";

		let rejection: unknown;
		try {
			await runWithSlot(async () => {
				throw quotaError;
			});
		} catch (error) {
			rejection = error;
		}
		expect(rejection).toBeInstanceOf(Error);
		expect((rejection as { reason?: string }).reason).toBe("quota");
		expect((rejection as Error).message).toBe(
			"IndexedDB quota was exhausted during commit.",
		);
		harness.coordinator.destroy();
	});

	it("commits replace chunks before completion and broadcasts only compact summaries", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("replace-request", {
			expectedEvents: 2,
		});
		await harness.coordinator.register(descriptor);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[
					createRawDocument("one"),
					createRawDocument("two", SOURCE_ONE, 1_700_000_000_001),
				],
				true,
			),
		);
		await waitFor(() => harness.completed.includes(descriptor.requestId));

		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		const summary = await repository.getSourceSummary(SOURCE_ONE);
		expect(summary).toMatchObject({
			status: "complete",
			eventCount: 2,
		});
		expect(await repository.getById("one" as Doc.Id, SOURCE_ONE)).toMatchObject({
			_id: "one",
			"gulp.source_id": SOURCE_ONE,
		});
		expect(harness.flushing).toEqual([descriptor.requestId]);
		expect(harness.failures).toHaveLength(0);
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		expect(harness.broadcasts.at(-1)).toEqual(summary);
		expect(harness.summaries.at(-1)?.summary).toEqual(summary);
		harness.coordinator.destroy();
	});

	it("publishes the first full replacement page while the stream is still building", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("first-building-page", {
			expectedEvents: EVENT_PAGE_SIZE * 2,
		});
		await harness.coordinator.register(descriptor);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				createRawDocuments(
					"first-building-page",
					EVENT_PAGE_SIZE,
					SOURCE_ONE,
					1_700_000_000_000,
				),
				false,
			),
		);

		await waitFor(
			() =>
				harness.summaries.some(
					(event) =>
						event.requestId === descriptor.requestId &&
						event.summary.status === "building" &&
						event.summary.eventCount === EVENT_PAGE_SIZE,
				),
			30_000,
		);

		expect(harness.completed.includes(descriptor.requestId)).toBe(false);
		expect(
			await harness.repositories
				.get(descriptor.operationId)
				?.getSourceSummary(SOURCE_ONE),
		).toMatchObject({
			status: "building",
			eventCount: EVENT_PAGE_SIZE,
		});

		await harness.coordinator.cancel(descriptor.requestId);
		harness.coordinator.destroy();
	});

	it("persists three queued 10k replacement chunks without treating them as backlog", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("thirty-thousand-replace", {
			expectedEvents: 30_000,
		});
		await harness.coordinator.register(descriptor);

		for (let chunkIndex = 0; chunkIndex < 3; chunkIndex++) {
			harness.coordinator.handleDocumentChunk(
				createChunk(
					descriptor,
					createRawDocuments(
						`large-${chunkIndex}`,
						10_000,
						SOURCE_ONE,
						1_700_000_000_000 + chunkIndex * 10_000,
					),
					chunkIndex === 2,
					chunkIndex,
				),
			);
		}

		await waitFor(
			() => harness.completed.includes(descriptor.requestId),
			60_000,
		);
		const repository = harness.repositories.get(descriptor.operationId);
		const backend = harness.backends.get(descriptor.operationId);
		const manifest = await backend?.get<EventSourceManifest>(
			"sources",
			SOURCE_ONE,
		);
		expect(await repository?.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 30_000,
		});
		expect(
			manifest?.pages.map((page) => page.count).sort((left, right) => right - left),
		).toEqual([EVENT_PAGE_SIZE, 30_000 - EVENT_PAGE_SIZE]);
		expect(Math.ceil(14_000_000 / EVENT_PAGE_SIZE)).toBe(855);
		expect(harness.failures).toHaveLength(0);
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		const diagnostics = harness.coordinator.getDiagnostics({
			started: 0,
			completed: 0,
			superseded: 0,
			retries: 0,
		});
		expect(diagnostics.pages).toEqual({
			created: 2,
			documents: 30_000,
			capacity: 2 * EVENT_PAGE_SIZE,
			fillRatio: 30_000 / (2 * EVENT_PAGE_SIZE),
		});
		expect(
			diagnostics.normalizedBacklog.maximumDocuments <=
				EVENT_PENDING_DOCUMENT_LIMIT,
		).toBe(true);
		expect(diagnostics.commits.completed > 0).toBe(true);
		expect(diagnostics.commits.maximumActive <= 2).toBe(true);
		harness.coordinator.resetDiagnostics();
		expect(
			harness.coordinator.getDiagnostics({
				started: 0,
				completed: 0,
				superseded: 0,
				retries: 0,
			}).pages.created,
		).toBe(0);
		harness.coordinator.destroy();
	});

	it("builds discovered ingestion sources in full pages and promotes their terminal cache", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("thirty-thousand-ingestion", {
			kind: "ingestion-build",
			sourceIds: [],
			completionMode: "explicit",
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		await harness.coordinator.register(descriptor);
		const repository = harness.repositories.get(descriptor.operationId);
		const backend = harness.backends.get(descriptor.operationId);
		if (!repository || !backend) throw new Error("Expected operation store.");
		const upsertDocuments = jest.spyOn(repository, "upsertDocuments");

		for (let chunkIndex = 0; chunkIndex < 3; chunkIndex++) {
			harness.coordinator.handleDocumentChunk(
				createChunk(
					descriptor,
					createRawDocuments(
						`ingestion-${chunkIndex}`,
						10_000,
						SOURCE_ONE,
						1_700_000_000_000 + chunkIndex * 10_000,
					),
					chunkIndex === 2,
					chunkIndex,
				),
			);
		}
		const provenance = createQueryCacheProvenance("ingestion-cache");
		await harness.coordinator.complete(descriptor.requestId, {
			queryCachesBySourceId: { [SOURCE_ONE]: provenance },
		});

		const manifest = await backend.get<EventSourceManifest>(
			"sources",
			SOURCE_ONE,
		);
		expect(upsertDocuments).not.toHaveBeenCalled();
		expect(
			manifest?.pages.map((page) => page.count).sort((left, right) => right - left),
		).toEqual([EVENT_PAGE_SIZE, 30_000 - EVENT_PAGE_SIZE]);
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 30_000,
			activeCacheGeneration: manifest?.generation,
		});
		const caches = await repository.listQueryCaches(SOURCE_ONE);
		expect(caches).toHaveLength(1);
		expect(caches[0].provenance).toMatchObject({
			fingerprint: "ingestion-cache",
			normalization: { field: "gulp.event_code" },
			serverSnapshot: {
				total: 30_000,
				millisecondPrecision: true,
			},
		});
		expect(harness.failures).toHaveLength(0);
		harness.coordinator.destroy();
	});

	it("publishes cumulative multi-source ingestion progress before partial pages flush", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("live-ingestion-progress", {
			kind: "ingestion-build",
			sourceIds: [],
			completionMode: "explicit",
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		await harness.coordinator.register(descriptor);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[
					createRawDocument("first-one", SOURCE_ONE),
					createRawDocument("first-two", SOURCE_ONE),
					createRawDocument("first-three", SOURCE_TWO),
				],
				false,
				0,
			),
		);
		await waitFor(() => harness.ingestionProgress.length === 1);

		expect(harness.ingestionProgress[0]).toEqual({
			requestId: descriptor.requestId,
			operationId: descriptor.operationId,
			operationEpoch: descriptor.operationEpoch,
			kind: "ingestion-build",
			sourceEventCounts: {
				[SOURCE_ONE]: 2,
				[SOURCE_TWO]: 1,
			},
		});
		expect(
			harness.summaries.some((event) => event.summary.eventCount > 0),
		).toBe(false);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[
					createRawDocument("second-one", SOURCE_ONE),
					createRawDocument("second-two", SOURCE_TWO),
					createRawDocument("second-three", SOURCE_TWO),
				],
				true,
				1,
			),
		);
		await waitFor(() => harness.ingestionProgress.length === 2);

		expect(harness.ingestionProgress[1].sourceEventCounts).toEqual({
			[SOURCE_ONE]: 3,
			[SOURCE_TWO]: 3,
		});
		expect(harness.completed.includes(descriptor.requestId)).toBe(false);

		await harness.coordinator.complete(descriptor.requestId);
		expect(
			await harness.repositories
				.get(descriptor.operationId)
				?.getSourceSummary(SOURCE_ONE),
		).toMatchObject({ status: "complete", eventCount: 3 });
		expect(
			await harness.repositories
				.get(descriptor.operationId)
				?.getSourceSummary(SOURCE_TWO),
		).toMatchObject({ status: "complete", eventCount: 3 });
		harness.coordinator.destroy();
	});

	it("publishes durable totals for realtime ingestion streams", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("realtime-ingestion-progress", {
			kind: "realtime-ingest",
			completionMode: "explicit",
		});
		await harness.coordinator.register(descriptor);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[
					createRawDocument("realtime-one", SOURCE_ONE),
					createRawDocument("realtime-two", SOURCE_ONE),
				],
				true,
			),
		);
		await waitFor(() => harness.ingestionProgress.length === 1);

		expect(harness.ingestionProgress[0]).toMatchObject({
			requestId: descriptor.requestId,
			kind: "realtime-ingest",
			sourceEventCounts: { [SOURCE_ONE]: 2 },
		});
		await harness.coordinator.cancel(descriptor.requestId);
		harness.coordinator.destroy();
	});

	for (const kind of ["replace", "query-upsert", "enrichment"] as const) {
		it(`does not publish ingestion progress for ${kind} streams`, async () => {
			const harness = createHarness();
			const descriptor = createDescriptor(`non-ingestion-${kind}`, { kind });
			await harness.coordinator.register(descriptor);
			harness.coordinator.handleDocumentChunk(
				createChunk(
					descriptor,
					[createRawDocument(`non-ingestion-${kind}`, SOURCE_ONE)],
					true,
				),
			);
			await waitFor(() => harness.completed.includes(descriptor.requestId));

			expect(harness.ingestionProgress).toHaveLength(0);
			harness.coordinator.destroy();
		});
	}

	it("persists ingestion timestamp ties in event.sequence order", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("sequence-ordered-ingestion", {
			kind: "ingestion-build",
			sourceIds: [],
			completionMode: "explicit",
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		await harness.coordinator.register(descriptor);
		const timestamp = 1_700_000_000_000;
		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[
					createRawDocument("sequence-40", SOURCE_ONE, timestamp, {
						"event.sequence": 40,
					}),
					createRawDocument("sequence-10", SOURCE_ONE, timestamp, {
						"event.sequence": 10,
					}),
				],
				false,
				0,
			),
		);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[
					createRawDocument("sequence-30", SOURCE_ONE, timestamp, {
						"event.sequence": 30,
					}),
					createRawDocument("sequence-20", SOURCE_ONE, timestamp, {
						"event.sequence": 20,
					}),
				],
				true,
				1,
			),
		);
		await harness.coordinator.complete(descriptor.requestId, {
			queryCachesBySourceId: {
				[SOURCE_ONE]: createQueryCacheProvenance("sequence-ingestion-cache"),
			},
		});

		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		const page = await repository.queryPixelBucket(
			SOURCE_ONE,
			{ min: timestamp, max: timestamp },
			10,
		);
		expect(page.events.map((event) => event["event.sequence"])).toEqual([
			40, 30, 20, 10,
		]);
		expect(harness.failures).toHaveLength(0);
		harness.coordinator.destroy();
	});

	it("releases raw chunks after normalization while an earlier page commit is blocked", async () => {
		const harness = createHarness({
			rawGlobalByteLimit: 90,
			rawStreamByteLimit: 90,
		});
		const descriptor = createDescriptor("flow-controlled-replace", {
			expectedEvents: 40_000,
		});
		await harness.coordinator.register(descriptor);
		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		const appendPages = repository.appendPages.bind(repository);
		let releaseFirstCommit = (): void => undefined;
		const firstCommitGate = new Promise<void>((resolve) => {
			releaseFirstCommit = resolve;
		});
		let appendCalls = 0;
		repository.appendPages = async (input) => {
			appendCalls += 1;
			if (appendCalls === 1) await firstCommitGate;
			return appendPages(input);
		};

		for (let chunkIndex = 0; chunkIndex < 3; chunkIndex++) {
			const chunk = createChunk(
				descriptor,
				createRawDocuments(
					`flow-${chunkIndex}`,
					10_000,
					SOURCE_ONE,
					1_700_000_000_000 + chunkIndex * 10_000,
				),
				false,
				chunkIndex,
				30,
			);
			if (chunk.message.payload) chunk.message.payload.name = "flow-query";
			harness.coordinator.handleDocumentChunk(chunk);
		}

		await waitFor(() => appendCalls === 1, 60_000);
		await waitFor(
			() => harness.coordinator.pendingRawChunkByteCount === 0,
			60_000,
		);
		expect(
			harness.acknowledgements.filter(({ stage }) => stage === "accepted"),
		).toHaveLength(3);

		const terminalChunk = createChunk(
			descriptor,
			createRawDocuments(
				"flow-3",
				10_000,
				SOURCE_ONE,
				1_700_000_030_000,
			),
			true,
			3,
			30,
		);
		if (terminalChunk.message.payload) {
			terminalChunk.message.payload.name = "flow-query";
			terminalChunk.message.payload.request_last = true;
		}
		harness.coordinator.handleDocumentChunk(terminalChunk);
		expect(harness.failures).toHaveLength(0);

		releaseFirstCommit();
		await waitFor(
			() => harness.completed.includes(descriptor.requestId),
			60_000,
		);
		expect(harness.failures).toHaveLength(0);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		expect(
			harness.acknowledgements.map(({ sequence, stage }) => ({ sequence, stage })),
		).toEqual([
			{ sequence: 0, stage: "accepted" },
			{ sequence: 1, stage: "accepted" },
			{ sequence: 2, stage: "accepted" },
			{ sequence: 3, stage: "accepted" },
			{ sequence: 3, stage: "durable" },
		]);
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 40_000,
		});
		harness.coordinator.destroy();
	});

	it("publishes terminal authority before bounded obsolete-generation cleanup", async () => {
		const harness = createHarness();
		const original = createDescriptor("cleanup-original");
		await seedCompleteReplacement(harness, original, [
			createRawDocument("cleanup-old"),
		]);
		const backend = harness.backends.get(original.operationId);
		const repository = harness.repositories.get(original.operationId);
		if (!backend || !repository) throw new Error("Expected operation store.");
		const originalManifest = await backend.get<EventSourceManifest>(
			"sources",
			SOURCE_ONE,
		);
		if (!originalManifest) throw new Error("Expected original manifest.");

		const replacement = createDescriptor("cleanup-replacement");
		await harness.coordinator.register(replacement);
		const cleanupObsoleteGenerations =
			repository.cleanupObsoleteGenerations.bind(repository);
		let releaseCleanup = (): void => undefined;
		let cleanupStarted = false;
		const cleanupBlocked = new Promise<void>((resolve) => {
			releaseCleanup = resolve;
		});
		repository.cleanupObsoleteGenerations = async (sourceId) => {
			cleanupStarted = true;
			await cleanupBlocked;
			await cleanupObsoleteGenerations(sourceId);
		};

		harness.coordinator.handleDocumentChunk(
			createChunk(replacement, [createRawDocument("cleanup-new")], true),
		);
		await waitFor(() => harness.completed.includes(replacement.requestId));

		expect(cleanupStarted).toBe(true);
		expect(harness.coordinator.hasStream(replacement.requestId)).toBe(true);
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 1,
		});
		expect(
			await backend.getAllByPrefix("pages", [
				SOURCE_ONE,
				originalManifest.generation,
			]),
		).toHaveLength(1);
		expect(harness.failures).toHaveLength(0);

		const cancelAfterTerminal = harness.coordinator.cancel(
			replacement.requestId,
		);
		releaseCleanup();
		expect(await cancelAfterTerminal).toBe(true);
		await waitFor(() => !harness.coordinator.hasStream(replacement.requestId));
		expect(
			await backend.getAllByPrefix("pages", [
				SOURCE_ONE,
				originalManifest.generation,
			]),
		).toHaveLength(0);
		expect(
			await repository.getById("cleanup-new" as Doc.Id, SOURCE_ONE),
		).not.toBeNull();
		expect(harness.failures).toHaveLength(0);
		harness.coordinator.destroy();
	});

	it(
		"commits nine 10k chunks and the final 4,391 events before terminal completion",
		async () => {
			const harness = createHarness();
			const expectedEvents = 94_391;
			const descriptor = createDescriptor("ninety-four-thousand-replace", {
				expectedEvents,
			});
			await harness.coordinator.register(descriptor);

			for (let chunkIndex = 0; chunkIndex < 10; chunkIndex++) {
				const eventCount = chunkIndex === 9 ? 4_391 : 10_000;
				harness.coordinator.handleDocumentChunk(
					createChunk(
						descriptor,
						createRawDocuments(
							`terminal-${chunkIndex}`,
							eventCount,
							SOURCE_ONE,
							1_700_000_000_000 + chunkIndex * 10_000,
						),
						chunkIndex === 9,
						chunkIndex,
					),
				);
			}

			await waitFor(
				() => harness.completed.includes(descriptor.requestId),
				120_000,
			);
			const repository = harness.repositories.get(descriptor.operationId);
			expect(await repository?.getSourceSummary(SOURCE_ONE)).toMatchObject({
				status: "complete",
				eventCount: expectedEvents,
			});
			expect(harness.summaries.at(-1)?.summary).toMatchObject({
				status: "complete",
				eventCount: expectedEvents,
			});
			expect(harness.completed).toEqual([descriptor.requestId]);
			expect(harness.failures).toHaveLength(0);
			expect(harness.coordinator.pendingDocumentCount).toBe(0);
			expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
			harness.coordinator.destroy();
		},
	);

	it("serializes four interleaved 10k streams through weighted normalized credit", async () => {
		const harness = createHarness();
		const rawBytesPerStream = 17 * MEBIBYTE;
		const descriptors = [
			createDescriptor("interleaved-one", { sourceIds: [SOURCE_ONE] }),
			createDescriptor("interleaved-two", { sourceIds: [SOURCE_TWO] }),
			createDescriptor("interleaved-three", { sourceIds: [SOURCE_THREE] }),
			createDescriptor("interleaved-four", { sourceIds: [SOURCE_FOUR] }),
		];
		for (const descriptor of descriptors) {
			await harness.coordinator.register(descriptor);
		}
		const repository = harness.repositories.get(descriptors[0].operationId);
		if (!repository) throw new Error("Expected operation repository.");
		const appendPages = repository.appendPages.bind(repository);
		let releaseWrites = (): void => undefined;
		const writesBlocked = new Promise<void>((resolve) => {
			releaseWrites = resolve;
		});
		repository.appendPages = async (...args) => {
			await writesBlocked;
			return appendPages(...args);
		};

		descriptors.forEach((descriptor, index) => {
			const sourceId = descriptor.sourceIds?.[0] as Source.Id;
			harness.coordinator.handleDocumentChunk(
				createChunk(
					descriptor,
					createRawDocuments(
						`interleaved-${index}`,
						10_000,
						sourceId,
						1_700_100_000_000 + index * 10_000,
					),
					false,
					0,
					rawBytesPerStream,
				),
			);
		});
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(
			rawBytesPerStream * descriptors.length,
		);
		expect(
			harness.coordinator.pendingRawChunkByteCount > 64 * MEBIBYTE,
		).toBe(true);
		await waitFor(
			() => harness.coordinator.pendingDocumentCount === 30_000,
			10_000,
		);
		expect(
			harness.coordinator.pendingDocumentCount <= EVENT_PENDING_DOCUMENT_LIMIT,
		).toBe(true);
		expect(harness.failures).toHaveLength(0);
		releaseWrites();
		for (const descriptor of descriptors) {
			harness.coordinator.handleDocumentChunk(
				createChunk(descriptor, [], true, 1),
			);
		}

		await waitFor(
			() => descriptors.every(({ requestId }) => harness.completed.includes(requestId)),
			60_000,
		);
		expect(harness.failures).toHaveLength(0);
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		for (const sourceId of [SOURCE_ONE, SOURCE_TWO, SOURCE_THREE, SOURCE_FOUR]) {
			expect(await repository?.getSourceSummary(sourceId)).toMatchObject({
				status: "complete",
				eventCount: 10_000,
			});
		}
		harness.coordinator.destroy();
	});

	it("accepts the exact raw stream limit and rejects one byte beyond it", async () => {
		const harness = createHarness({
			rawGlobalByteLimit: 200,
			rawStreamByteLimit: 100,
		});
		const accepted = createDescriptor("raw-exact-limit");
		await harness.coordinator.register(accepted);
		harness.coordinator.handleDocumentChunk(
			createChunk(accepted, [createRawDocument("raw-60")], false, 0, 60),
		);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				accepted,
				[createRawDocument("raw-40", SOURCE_ONE, 1_700_000_000_001)],
				true,
				1,
				40,
			),
		);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(100);
		expect(
			harness.coordinator.getStreamPendingRawChunkByteCount(accepted.requestId),
		).toBe(100);
		expect(
			harness.coordinator.getStreamMaximumPendingRawChunkByteCount(
				accepted.requestId,
			),
		).toBe(100);
		await waitFor(() => harness.completed.includes(accepted.requestId));
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		expect(harness.coordinator.maximumPendingRawChunkByteCount).toBe(100);

		const rejected = createDescriptor("raw-one-byte-over", {
			sourceIds: [SOURCE_TWO],
		});
		await harness.coordinator.register(rejected);
		const oversized = createChunk(
			rejected,
			[createRawDocument("raw-101", SOURCE_TWO)],
			true,
			0,
			101,
		);
		harness.coordinator.handleDocumentChunk(oversized);

		expect(oversized.message.payload?.docs).toEqual([]);
		expect(harness.failures.at(-1)).toMatchObject({
			requestId: rejected.requestId,
			reason: "backlog",
		});
		expect(harness.completed.includes(rejected.requestId)).toBe(false);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		await waitFor(() => !harness.coordinator.hasStream(rejected.requestId));
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		harness.coordinator.destroy();
	});

	it("spills ingestion overflow to its journal without failing or throttling the stream", async () => {
		const harness = createHarness({
			rawGlobalByteLimit: 100,
			rawStreamByteLimit: 100,
		});
		const descriptor = createDescriptor("journaled-ingestion", {
			kind: "ingestion-build",
			sourceIds: [],
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		await harness.coordinator.register(descriptor);
		const chunk = createChunk(
			descriptor,
			[createRawDocument("journaled-document")],
			true,
			0,
			101,
		);

		harness.coordinator.handleDocumentChunk(chunk);
		await waitFor(() => harness.completed.includes(descriptor.requestId));

		const repository = harness.repositories.get(descriptor.operationId);
		const backend = harness.backends.get(descriptor.operationId);
		expect(harness.failures).toHaveLength(0);
		expect(await repository?.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 1,
		});
		expect(await backend?.getAll("stream_journal")).toEqual([]);
		expect(chunk.message.payload?.docs).toEqual([]);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		harness.coordinator.destroy();
	});

	it("isolates a global raw backlog rejection from accepted concurrent streams", async () => {
		const harness = createHarness({
			rawGlobalByteLimit: 100,
			rawStreamByteLimit: 80,
		});
		const first = createDescriptor("raw-global-first", {
			sourceIds: [SOURCE_ONE],
		});
		const second = createDescriptor("raw-global-second", {
			sourceIds: [SOURCE_TWO],
		});
		const rejected = createDescriptor("raw-global-rejected", {
			sourceIds: [SOURCE_THREE],
		});
		for (const descriptor of [first, second, rejected]) {
			await harness.coordinator.register(descriptor);
		}
		harness.coordinator.handleDocumentChunk(
			createChunk(first, [createRawDocument("global-60")], true, 0, 60),
		);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				second,
				[createRawDocument("global-40", SOURCE_TWO)],
				true,
				0,
				40,
			),
		);
		const overflow = createChunk(
			rejected,
			[createRawDocument("global-overflow", SOURCE_THREE)],
			true,
			0,
			1,
		);
		harness.coordinator.handleDocumentChunk(overflow);

		expect(harness.coordinator.pendingRawChunkByteCount).toBe(100);
		expect(harness.coordinator.maximumPendingRawChunkByteCount).toBe(100);
		expect(overflow.message.payload?.docs).toEqual([]);
		expect(harness.failures.at(-1)).toMatchObject({
			requestId: rejected.requestId,
			reason: "backlog",
		});
		await waitFor(() =>
			[first, second].every(({ requestId }) =>
				harness.completed.includes(requestId),
			),
		);
		await waitFor(() => !harness.coordinator.hasStream(rejected.requestId));
		expect(harness.completed.includes(rejected.requestId)).toBe(false);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		for (const sourceId of [SOURCE_ONE, SOURCE_TWO]) {
			expect(
				await harness.repositories
					.get(first.operationId)
					?.getSourceSummary(sourceId),
			).toMatchObject({ status: "complete", eventCount: 1 });
		}
		harness.coordinator.destroy();
	});

	it("releases reserved raw bytes on cancel, disconnect, and destruction", async () => {
		const harness = createHarness({
			rawGlobalByteLimit: 100,
			rawStreamByteLimit: 100,
		});
		const canceled = createDescriptor("raw-canceled");
		await harness.coordinator.register(canceled);
		const canceledChunk = createChunk(
			canceled,
			[createRawDocument("raw-canceled")],
			false,
			0,
			50,
		);
		harness.coordinator.handleDocumentChunk(canceledChunk);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(50);
		expect(await harness.coordinator.cancel(canceled.requestId)).toBe(true);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		expect(canceledChunk.message.payload?.docs).toEqual([]);

		const disconnected = createDescriptor("raw-disconnected", {
			sourceIds: [SOURCE_TWO],
		});
		await harness.coordinator.register(disconnected);
		const disconnectedChunk = createChunk(
			disconnected,
			[createRawDocument("raw-disconnected", SOURCE_TWO)],
			false,
			0,
			60,
		);
		harness.coordinator.handleDocumentChunk(disconnectedChunk);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(60);
		await harness.coordinator.handleDisconnect();
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		expect(disconnectedChunk.message.payload?.docs).toEqual([]);

		const destroyed = createDescriptor("raw-destroyed", {
			sourceIds: [SOURCE_THREE],
		});
		await harness.coordinator.register(destroyed);
		const destroyedChunk = createChunk(
			destroyed,
			[createRawDocument("raw-destroyed", SOURCE_THREE)],
			false,
			0,
			70,
		);
		harness.coordinator.handleDocumentChunk(destroyedChunk);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(70);
		harness.coordinator.destroy();
		await waitFor(() => harness.coordinator.pendingRawChunkByteCount === 0);
		expect(destroyedChunk.message.payload?.docs).toEqual([]);
	});

	it("releases transferred builder credit when replacement storage fails", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("replace-storage-failure");
		await harness.coordinator.register(descriptor);
		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		repository.appendPages = async () => {
			throw new Error("deterministic append failure");
		};

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[createRawDocument("failed-write")],
				true,
				0,
				100,
			),
		);

		await waitFor(() => !harness.coordinator.hasStream(descriptor.requestId));
		expect(harness.failures).toHaveLength(1);
		expect(harness.failures[0]).toMatchObject({
			requestId: descriptor.requestId,
			reason: "storage",
		});
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
		harness.coordinator.destroy();
	});

	it("marks a failed ingestion build incomplete instead of exposing partial cache data", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("ingestion-storage-failure", {
			kind: "ingestion-build",
			sourceIds: [],
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		});
		await harness.coordinator.register(descriptor);
		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		repository.appendPages = async () => {
			throw new Error("deterministic ingestion append failure");
		};

		harness.coordinator.handleDocumentChunk(
			createChunk(descriptor, [createRawDocument("failed-ingestion")], true),
		);

		await waitFor(() => !harness.coordinator.hasStream(descriptor.requestId));
		expect(harness.failures[0]).toMatchObject({
			requestId: descriptor.requestId,
			reason: "storage",
		});
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "incomplete",
			activeCacheGeneration: null,
		});
		expect(await repository.listQueryCaches(SOURCE_ONE)).toHaveLength(0);
		harness.coordinator.destroy();
	});

	it("discovers query-upsert sources without treating hash settings as authority", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("dynamic-request", {
			kind: "realtime-ingest",
			sourceIds: undefined,
			sourceSettingsById: {
				"settings-only-source": { field: "ignored", hash_function: "fnv1a" },
			},
			defaultSourceSettings: { field: "category", hash_function: "djb2" },
		});
		await harness.coordinator.register(descriptor);
		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation store.");
		const upsertDocuments = jest.spyOn(repository, "upsertDocuments");
		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[createRawDocument("dynamic", "discovered-source", undefined, {
					category: "network",
				})],
				true,
			),
		);
		await waitFor(() => harness.completed.includes(descriptor.requestId));

		const backend = harness.backends.get(descriptor.operationId);
		if (!backend) throw new Error("Expected operation store.");
		expect(
			await backend.get("sources", "settings-only-source"),
		).toBeUndefined();
		const document = await repository.getById("dynamic" as Doc.Id);
		expect(document).toMatchObject({
			_id: "dynamic",
			"gulp.source_id": "discovered-source",
		});
		expect(document?.number_hash).not.toBe(0);
		expect(upsertDocuments).toHaveBeenCalledTimes(1);
		harness.coordinator.destroy();
	});

	it("keeps explicit streams open across multiple rule-level last chunks", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("explicit-request", {
			completionMode: "explicit",
		});
		await harness.coordinator.register(descriptor);
		harness.coordinator.handleDocumentChunk(
			createChunk(descriptor, [createRawDocument("rule-one")], true, 0),
		);
		await waitFor(() => harness.coordinator.pendingDocumentCount === 1);
		expect(harness.coordinator.hasStream(descriptor.requestId)).toBe(true);
		expect(
			harness.summaries.some(
				(event) =>
					event.requestId === descriptor.requestId &&
					event.summary.eventCount === 1,
			),
		).toBe(false);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[createRawDocument("rule-two", SOURCE_ONE, 1_700_000_000_001)],
				true,
				1,
			),
		);
		await harness.coordinator.complete(descriptor.requestId);

		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 2,
		});
		expect(harness.completed).toEqual([descriptor.requestId]);
		expect(harness.failures).toHaveLength(0);
		harness.coordinator.destroy();
	});

	it("defers explicit completion requested before the terminal chunk commit", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("early-explicit-completion", {
			completionMode: "explicit",
		});
		await harness.coordinator.register(descriptor);

		let completionSettled = false;
		const completion = harness.coordinator
			.complete(descriptor.requestId)
			.then((handled) => {
				completionSettled = true;
				return handled;
			});
		await Promise.resolve();
		expect(completionSettled).toBe(false);

		harness.coordinator.handleDocumentChunk(
			createChunk(descriptor, [createRawDocument("late-final")], true),
		);
		expect(await completion).toBe(true);

		const repository = harness.repositories.get(descriptor.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 1,
		});
		expect(harness.completed).toEqual([descriptor.requestId]);
		harness.coordinator.destroy();
	});

	it("releases an explicitly completed ingestion source for a later replacement", async () => {
		const harness = createHarness();
		const ingestion = createDescriptor("external-ingestion", {
			kind: "ingestion-build",
			sourceIds: [],
			completionMode: "explicit",
		});
		await harness.coordinator.register(ingestion);
		harness.coordinator.handleDocumentChunk(
			createChunk(ingestion, [createRawDocument("external-event")], true),
		);
		await harness.coordinator.complete(ingestion.requestId);

		expect(harness.completed.includes(ingestion.requestId)).toBe(true);
		const refetch = createDescriptor("external-refetch");
		await harness.coordinator.register(refetch);
		expect(harness.coordinator.hasStream(refetch.requestId)).toBe(true);

		await harness.coordinator.cancel(refetch.requestId);
		harness.coordinator.destroy();
	});

	it("prepares override LOD in source-leased slices before publishing", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("override-source-request");
		await harness.coordinator.register(descriptor);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[createRawDocument("override-event", SOURCE_ONE, undefined, { color: "red" })],
				true,
			),
		);
		await waitFor(() => harness.completed.includes(descriptor.requestId));

		const projection = await harness.coordinator.requestRenderWindow({
			operationId: descriptor.operationId,
			operationEpoch: descriptor.operationEpoch,
			viewportRevision: 1,
			projectionRevision: 1,
			frame: { min: 1_699_999_999_000, max: 1_700_000_001_000 },
			width: 100,
			scale: 1,
			scrollX: 0,
			horizontalOverscanPixels: 10,
			sources: [
				{
					sourceId: SOURCE_ONE,
					offset: 0,
					engine: "default",
					frequency: 0,
					overrideHashes: [7],
					settingsRevision: 1,
					overrideRevision: 3,
				},
			],
		});

		expect(projection?.sources[0].representativeHandles).toEqual(
			new Uint32Array([1]),
		);
		const backend = harness.backends.get(descriptor.operationId);
		if (!backend) throw new Error("Expected operation backend.");
		const manifest = await backend.get<EventSourceManifest>("sources", SOURCE_ONE);
		if (!manifest) throw new Error("Expected source manifest.");
		const lod = await backend.get<EncodedEventPageLod>("page_lod", [
			SOURCE_ONE,
			manifest.generation,
			manifest.pages[0].pageId,
			EVENT_PAGE_LOD_RECORD_KIND,
		]);
		expect(lod?.overrideRevision).toBe(3);
		harness.coordinator.destroy();
	});

	it("restores a complete generation when a replacement disconnects", async () => {
		const harness = createHarness();
		const original = createDescriptor("original-request");
		await harness.coordinator.register(original);
		harness.coordinator.handleDocumentChunk(
			createChunk(original, [createRawDocument("original")], true),
		);
		await waitFor(() => harness.completed.includes(original.requestId));

		const replacement = createDescriptor("replacement-request");
		await harness.coordinator.register(replacement);
		harness.coordinator.handleDocumentChunk(
			createChunk(replacement, [createRawDocument("partial")], false),
		);
		await harness.coordinator.handleDisconnect();

		const repository = harness.repositories.get(replacement.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		expect(await repository.getById("original" as Doc.Id, SOURCE_ONE)).not.toBeNull();
		expect(await repository.getById("partial" as Doc.Id, SOURCE_ONE)).toBeNull();
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 1,
		});
		expect(harness.failures.at(-1)).toMatchObject({
			requestId: replacement.requestId,
			reason: "disconnected",
		});
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		harness.coordinator.destroy();
	});

	it("retains a protected complete generation when replacement is canceled", async () => {
		const harness = createHarness();
		const original = createDescriptor("protected-original", {
			expectedEvents: 2,
		});
		await seedCompleteReplacement(harness, original, [
			createRawDocument("protected-one"),
			createRawDocument("protected-two", SOURCE_ONE, 1_700_000_000_001),
		]);
		const replacement = createDescriptor("protected-replacement", {
			expectedEvents: 5,
		});
		await harness.coordinator.register(replacement);
		expect(
			harness.summaries.some(
				(event) =>
					event.requestId === replacement.requestId &&
					event.replacementMode === "in-place",
			),
		).toBe(false);
		await harness.coordinator.cancel(replacement.requestId);

		const repository = harness.repositories.get(replacement.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 2,
		});
		expect(
			await repository.getById("protected-one" as Doc.Id, SOURCE_ONE),
		).not.toBeNull();
		harness.coordinator.destroy();
	});

	it("does not reject a replacement from its expected event count", async () => {
		const harness = createHarness();
		const original = createDescriptor("insufficient-original", {
			expectedEvents: 2,
		});
		await seedCompleteReplacement(harness, original, [
			createRawDocument("insufficient-one"),
			createRawDocument("insufficient-two", SOURCE_ONE, 1_700_000_000_001),
		]);

		const replacement = createDescriptor("insufficient-replacement", {
			expectedEvents: Number.MAX_SAFE_INTEGER,
		});
		await harness.coordinator.register(replacement);
		expect(harness.coordinator.hasStream(replacement.requestId)).toBe(true);
		expect(harness.failures).toHaveLength(0);
		await harness.coordinator.cancel(replacement.requestId);
		const repository = harness.repositories.get(replacement.operationId);
		if (!repository) throw new Error("Expected operation repository.");
		expect(await repository.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 2,
		});
		expect(
			await repository.getById("insufficient-one" as Doc.Id, SOURCE_ONE),
		).not.toBeNull();
		harness.coordinator.destroy();
	});

	it("supersedes an older replacement on the same source before ACK", async () => {
		const harness = createHarness();
		const older = createDescriptor("older-request");
		const newer = createDescriptor("newer-request");
		await harness.coordinator.register(older);
		await harness.coordinator.register(newer);

		expect(harness.coordinator.hasStream(older.requestId)).toBe(false);
		expect(harness.coordinator.hasStream(newer.requestId)).toBe(true);
		expect(
			harness.failures.some(
				(failure) =>
					failure.requestId === older.requestId &&
					failure.reason === "superseded",
			),
		).toBe(true);
		await harness.coordinator.cancel(newer.requestId);
		harness.coordinator.destroy();
	});

	it("persists concurrent chunks without quota admission", async () => {
		const harness = createHarness();
		const first = createDescriptor("capacity-first", {
			kind: "realtime-ingest",
			sourceIds: undefined,
		});
		const second = createDescriptor("capacity-second", {
			kind: "realtime-ingest",
			sourceIds: undefined,
		});
		await harness.coordinator.register(first);
		await harness.coordinator.register(second);

		harness.coordinator.handleDocumentChunk(
			createChunk(
				first,
				[createRawDocument("first-a", "first-source"), createRawDocument("first-b", "first-source")],
				true,
			),
		);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				second,
				[createRawDocument("second-a", "second-source"), createRawDocument("second-b", "second-source")],
				true,
			),
		);
		await waitFor(() => harness.completed.length >= 2);

		expect(harness.completed).toHaveLength(2);
		expect(harness.failures).toHaveLength(0);
		harness.coordinator.destroy();
	});

	it("retains dynamically discovered source leases until stream completion", async () => {
		const harness = createHarness();
		const first = createDescriptor("dynamic-first", {
			kind: "query-upsert",
			sourceIds: undefined,
		});
		const second = createDescriptor("dynamic-second", {
			kind: "query-upsert",
			sourceIds: undefined,
		});
		await harness.coordinator.register(first);
		await harness.coordinator.register(second);

		harness.coordinator.handleDocumentChunk(
			createChunk(first, [createRawDocument("dynamic-one")], false),
		);
		await waitFor(() => harness.summaries.some((event) => event.requestId === first.requestId));

		harness.coordinator.handleDocumentChunk(
			createChunk(
				second,
				[createRawDocument("dynamic-two", SOURCE_ONE, 1_700_000_000_001)],
				true,
			),
		);
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(harness.completed.includes(second.requestId)).toBe(false);

		harness.coordinator.handleDocumentChunk(createChunk(first, [], true, 1));
		await waitFor(
			() =>
				harness.completed.includes(first.requestId) &&
				harness.completed.includes(second.requestId),
		);
		const repository = harness.repositories.get(first.operationId);
		expect(await repository?.getSourceSummary(SOURCE_ONE)).toMatchObject({
			eventCount: 2,
			status: "complete",
		});
		harness.coordinator.destroy();
	});

	it("reacquires dynamic source authority when later chunks invert lock order", async () => {
		const harness = createHarness();
		const descriptor = createDescriptor("dynamic-order", {
			kind: "query-upsert",
			sourceIds: undefined,
		});
		await harness.coordinator.register(descriptor);
		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[createRawDocument("ordered-two", "source-two")],
				false,
			),
		);
		await waitFor(() => harness.summaries.some((event) => event.summary.sourceId === "source-two"));

		harness.coordinator.handleDocumentChunk(
			createChunk(
				descriptor,
				[createRawDocument("ordered-one", "source-one")],
				true,
				1,
			),
		);
		await waitFor(() => harness.completed.includes(descriptor.requestId));
		expect(harness.failures).toHaveLength(0);
		const repository = harness.repositories.get(descriptor.operationId);
		expect(await repository?.getSourceSummary("source-one" as Source.Id)).toMatchObject({
			eventCount: 1,
			status: "complete",
		});
		expect(await repository?.getSourceSummary("source-two" as Source.Id)).toMatchObject({
			eventCount: 1,
			status: "complete",
		});
		harness.coordinator.destroy();
	});

	it("enforces a FIFO non-barging ceiling for storage commits", async () => {
		const harness = createHarness();
		type CommitSlotRunner = <TResult>(
			task: () => Promise<TResult>,
		) => Promise<TResult>;
		const runWithSlot = (
			harness.coordinator as unknown as { withCommitSlot: CommitSlotRunner }
		).withCommitSlot.bind(harness.coordinator);
		const starts: number[] = [];
		const resolvers = new Map<number, () => void>();
		let active = 0;
		let maximumActive = 0;
		const run = (id: number): Promise<void> =>
			runWithSlot(async () => {
				starts.push(id);
				active += 1;
				maximumActive = Math.max(maximumActive, active);
				await new Promise<void>((resolve) => resolvers.set(id, resolve));
				active -= 1;
			});

		const tasks = [run(1), run(2), run(3), run(4)];
		await waitFor(() => starts.length === 2);
		expect(starts).toEqual([1, 2]);
		resolvers.get(1)?.();
		await waitFor(() => starts.length === 3);
		expect(starts).toEqual([1, 2, 3]);
		resolvers.get(2)?.();
		await waitFor(() => starts.length === 4);
		expect(starts).toEqual([1, 2, 3, 4]);
		resolvers.get(3)?.();
		resolvers.get(4)?.();
		await Promise.all(tasks);
		expect(maximumActive).toBe(2);
		expect(harness.coordinator.concurrentCommitCount).toBe(0);
		harness.coordinator.destroy();
	});

	it("releases operation repositories without deleting their stored data", async () => {
		const harness = createHarness();
		for (let index = 0; index < 3; index++) {
			const operationId = `operation-switch-${index}`;
			await harness.coordinator.requestRenderWindow({
				operationId,
				operationEpoch: index + 1,
				viewportRevision: 1,
				projectionRevision: 1,
				frame: { min: 0, max: 1_000 },
				width: 100,
				scale: 1,
				scrollX: 0,
				horizontalOverscanPixels: 10,
				sources: [],
			});
			expect(harness.coordinator.loadedOperationCount).toBe(1);
			const repository = harness.repositories.get(operationId);
			if (!repository) throw new Error("Expected operation repository.");
			const close = jest.spyOn(repository, "close");
			expect(await harness.coordinator.releaseOperation(operationId)).toBe(true);
			expect(close).toHaveBeenCalledTimes(1);
			expect(harness.coordinator.loadedOperationCount).toBe(0);
		}
		harness.coordinator.destroy();
	});

	it("cancels lock owners before waiting for in-flight operation registration", async () => {
		const harness = createHarness();
		const owner = createDescriptor("release-lock-owner");
		const waiter = createDescriptor("release-registration-waiter", {
			kind: "query-upsert",
		});
		await harness.coordinator.register(owner);
		let waiterRegistered = false;
		const registration = harness.coordinator.register(waiter).then(() => {
			waiterRegistered = true;
		});
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(waiterRegistered).toBe(false);

		const released = harness.coordinator.releaseOperation(owner.operationId);
		await Promise.all([registration, released]);
		expect(waiterRegistered).toBe(true);
		expect(await released).toBe(true);
		expect(harness.coordinator.loadedOperationCount).toBe(0);
		expect(harness.coordinator.hasStream(owner.requestId)).toBe(false);
		expect(harness.coordinator.hasStream(waiter.requestId)).toBe(false);
		harness.coordinator.destroy();
	});

	it("serializes same-source writers across logical QueryWorker coordinators", async () => {
		const backend = new MemoryEventStoreBackend();
		const lockManager = createCrossRealmLockManager();
		const completed: string[] = [];
		const createCoordinator = (): QueryPersistenceCoordinator =>
			new QueryPersistenceCoordinator(
				"shared-coordinator",
				{
					onSourceSummary: () => undefined,
					onIngestionProgress: () => undefined,
					onStreamFailure: () => undefined,
					onStreamFlushing: () => undefined,
					onStreamCompleted: (requestId) => completed.push(requestId),
					onDocumentChunkAcknowledged: () => undefined,
				},
				{
					createOperationStore: () => ({
						backend,
						repository: new EventStoreRepository(backend),
					}),
					createRevisionBus: () => ({
						publish: () => undefined,
						close: () => undefined,
					}),
					lockManager,
				},
			);
		const firstCoordinator = createCoordinator();
		const secondCoordinator = createCoordinator();
		const first = createDescriptor("cross-realm-first", {
			kind: "query-upsert",
		});
		const second = createDescriptor("cross-realm-second", {
			kind: "query-upsert",
		});
		await secondCoordinator.requestRenderWindow({
			operationId: second.operationId,
			operationEpoch: second.operationEpoch,
			viewportRevision: 1,
			projectionRevision: 1,
			frame: { min: 0, max: 1_000 },
			width: 100,
			scale: 1,
			scrollX: 0,
			horizontalOverscanPixels: 10,
			sources: [],
		});

		await firstCoordinator.register(first);
		let secondRegistered = false;
		const secondRegistration = secondCoordinator.register(second).then(() => {
			secondRegistered = true;
		});
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(secondRegistered).toBe(false);

		firstCoordinator.handleDocumentChunk(
			createChunk(first, [createRawDocument("cross-realm-one")], true),
		);
		await waitFor(() => completed.includes(first.requestId));
		await secondRegistration;
		expect(secondRegistered).toBe(true);

		secondCoordinator.handleDocumentChunk(
			createChunk(
				second,
				[
					createRawDocument(
						"cross-realm-two",
						SOURCE_ONE,
						1_700_000_000_001,
					),
				],
				true,
			),
		);
		await waitFor(() => completed.includes(second.requestId));

		const reader = new EventStoreRepository(backend);
		expect(await reader.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 2,
		});
		expect(
			await reader.getById("cross-realm-one" as Doc.Id, SOURCE_ONE),
		).not.toBeNull();
		expect(
			await reader.getById("cross-realm-two" as Doc.Id, SOURCE_ONE),
		).not.toBeNull();
		firstCoordinator.destroy();
		secondCoordinator.destroy();
	});
});
