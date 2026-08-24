import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import {
	EVENT_PAGE_LOD_RECORD_KIND,
	EVENT_PENDING_DOCUMENT_LIMIT,
	type EncodedEventPageLod,
	type EventSourceManifest,
	type EventSourceSummary,
	type EventStoreNamespace,
} from "@/event-store/EventStore.types";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import { QueryPersistenceCoordinator } from "@/event-store-worker/QueryPersistenceCoordinator";
import type {
	DocumentChunkReady,
	DocumentStreamDescriptor,
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
	failures: QueryStreamFailureEvent[];
	flushing: string[];
	completed: string[];
	broadcasts: EventSourceSummary[];
}

/**
 * Creates one isolated coordinator backed by deterministic in-memory storage.
 * @returns Coordinator harness and captured lifecycle events.
 */
function createHarness(): PersistenceHarness {
	const backends = new Map<string, MemoryEventStoreBackend>();
	const repositories = new Map<string, EventStoreRepository>();
	const summaries: QuerySourceSummaryEvent[] = [];
	const failures: QueryStreamFailureEvent[] = [];
	const flushing: string[] = [];
	const completed: string[] = [];
	const broadcasts: EventSourceSummary[] = [];
	const coordinator = new QueryPersistenceCoordinator(
		"test-coordinator",
		{
			onSourceSummary: (event) => summaries.push(event),
			onStreamFailure: (event) => failures.push(event),
			onStreamFlushing: (requestId) => flushing.push(requestId),
			onStreamCompleted: (requestId) => completed.push(requestId),
		},
		{
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
		failures,
		flushing,
		completed,
		broadcasts,
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
		expect(await repository?.getSourceSummary(SOURCE_ONE)).toMatchObject({
			status: "complete",
			eventCount: 30_000,
		});
		expect(harness.failures).toHaveLength(0);
		expect(harness.coordinator.pendingDocumentCount).toBe(0);
		expect(harness.coordinator.pendingRawChunkByteCount).toBe(0);
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
					true,
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
		const repository = harness.repositories.get(descriptor.operationId);
		if (!backend || !repository) throw new Error("Expected operation store.");
		expect(
			await backend.get("sources", "settings-only-source"),
		).toBeUndefined();
		const document = await repository.getById("dynamic" as Doc.Id);
		expect(document).toMatchObject({
			_id: "dynamic",
			"gulp.source_id": "discovered-source",
		});
		expect(document?.number_hash).not.toBe(0);
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
		await waitFor(() =>
			harness.summaries.some(
				(event) =>
					event.requestId === descriptor.requestId &&
					event.summary.eventCount === 1,
			),
		);

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
					onStreamFailure: () => undefined,
					onStreamFlushing: () => undefined,
					onStreamCompleted: (requestId) => completed.push(requestId),
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
