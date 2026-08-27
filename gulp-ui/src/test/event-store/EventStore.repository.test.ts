import type { Doc } from "@/entities/Doc";
import {
	fingerprintSourceQueryShape,
	type QueryCacheProvenance,
	type QueryCacheServerSnapshot,
} from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
import { encodeEventPage } from "@/event-store/EventPage.codec";
import type {
	EventStoreName,
	EventStoreWriteOperation,
} from "@/event-store/EventStore.backend";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import {
	type EncodedDocumentIndexSegment,
	EventStoreRepository,
	EventStoreSourceAuthorityError,
	StaleEventStoreCursorError,
	StaleEventStoreGenerationError,
} from "@/event-store/EventStore.repository";
import {
	EVENT_PAGE_HEIGHT_RECORD_KIND,
	EVENT_PAGE_LOD_RECORD_KIND,
	type EncodedEventPage,
	type EncodedEventPageHeight,
	type EncodedEventPageLod,
	type DesiredQueryCacheLookup,
	type EventSourceManifest,
	type EventSourceQueryCacheRecord,
} from "@/event-store/EventStore.types";
import { TextDecoder, TextEncoder } from "util";

const SOURCE_ID = "repository-source" as Source.Id;
const SECOND_SOURCE_ID = "repository-source-two" as Source.Id;

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

/** Creates one exact event-store fixture. */
function createDocument(
	id: string,
	timestamp: number,
	sourceId: Source.Id = SOURCE_ID,
): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": sourceId,
		"gulp.event_code": timestamp + 1,
		number_hash: timestamp + 2,
	};
}

/** Encodes one page with explicit source-local handles. */
function createPage(
	pageId: number,
	generation: number,
	revision: number,
	documents: readonly Doc.Type[],
	handles: readonly number[],
): EncodedEventPage {
	const sourceId = documents[0]?.["gulp.source_id"] ?? SOURCE_ID;
	return encodeEventPage({
		sourceId,
		generation,
		pageId,
		revision,
		documents,
		handles: new Uint32Array(handles),
	});
}

/** Creates serializable source-query provenance for cache-pool tests. */
function createQueryCacheProvenance(
	fingerprint: string,
	range: { min: number; max: number } = { min: 0, max: 100 },
	serverSnapshot: Partial<QueryCacheServerSnapshot> = {},
): QueryCacheProvenance {
	const provenance: QueryCacheProvenance = {
		query: {
			string: `Query ${fingerprint}`,
			filters: [],
			source_config: {
				operation_id: "operation",
				source_ids: [SOURCE_ID],
				range,
			},
		},
		fingerprint,
		normalization: {
			field: "number_hash",
			hashFunction: "fnv1a",
			requestedFields: ["gulp_timestamp", "number_hash"],
		},
		serverSnapshot: {
			total: 1,
			minNanoseconds: String(range.min),
			maxNanoseconds: String(range.max),
			timeUpdated: 1,
			millisecondPrecision: false,
			...serverSnapshot,
		},
	};
	provenance.shapeFingerprint = fingerprintSourceQueryShape({
		query: provenance.query,
		normalization: provenance.normalization,
	});
	return provenance;
}

/** Creates one desired startup lookup from source-scoped query provenance. */
function createDesiredQueryCacheLookup(
	fingerprint: string,
	range: { min: number; max: number } = { min: 0, max: 100 },
	serverSnapshot: Partial<QueryCacheServerSnapshot> = {},
): DesiredQueryCacheLookup {
	const provenance = createQueryCacheProvenance(
		fingerprint,
		range,
		serverSnapshot,
	);
	return {
		fingerprint,
		shapeFingerprint: provenance.shapeFingerprint!,
		range: { min: String(range.min), max: String(range.max) },
		serverSnapshot: provenance.serverSnapshot,
	};
}

/** Builds and finalizes one single-page query cache. */
async function appendQueryCache(
	repository: EventStoreRepository,
	generation: number | undefined,
	fingerprint: string,
	documentId: string,
	timestamp: number,
	options: {
		range?: { min: number; max: number };
		serverSnapshot?: Partial<QueryCacheServerSnapshot>;
	} = {},
): Promise<number> {
	const building = await repository.beginGeneration({
		sourceId: SOURCE_ID,
		...(generation === undefined ? {} : { generation }),
		queryCache: createQueryCacheProvenance(
			fingerprint,
			options.range,
			options.serverSnapshot,
		),
	});
	await repository.appendPages({
		sourceId: SOURCE_ID,
		generation: building.generation,
		pages: [
			createPage(
				0,
				building.generation,
				building.revision,
				[createDocument(documentId, timestamp)],
				[1],
			),
		],
	});
	await repository.finalizeGeneration(SOURCE_ID, building.generation);
	return building.generation;
}

/** Memory backend that exposes bounded index/page read counts. */
class CountingMemoryEventStoreBackend extends MemoryEventStoreBackend {
	public prefixReads = 0;
	public pageReads = 0;
	public indexReads = 0;
	public maximumIndexKeysPerRead = 0;
	public maximumPagePutsPerCommit = 0;
	public maximumDeletesPerCommit = 0;
	public maximumIndexBytesPerCommit = 0;
	public totalIndexEntriesWritten = 0;
	public failNextDeleteCommit = false;
	public failDeleteCommitNumber: number | null = null;
	public deleteCommitCount = 0;

	/** Counts exact-page reads while retaining normal clone semantics. */
	public override async get<TValue>(
		store: EventStoreName,
		key: IDBValidKey,
	): Promise<TValue | undefined> {
		if (store === "pages") this.pageReads++;
		if (store === "document_index_pages") this.indexReads++;
		return super.get<TValue>(store, key);
	}

	/** Counts one multi-key shard transaction independently of its segment count. */
	public override async getMany<TValue>(
		store: EventStoreName,
		keys: readonly IDBValidKey[],
	): Promise<Array<TValue | undefined>> {
		if (store === "document_index_pages") {
			this.indexReads++;
			this.maximumIndexKeysPerRead = Math.max(
				this.maximumIndexKeysPerRead,
				keys.length,
			);
		}
		return super.getMany<TValue>(store, keys);
	}

	/** Counts directly addressed index-shard reads. */
	public override async getAllByPrefix<TValue>(
		store: EventStoreName,
		keyPrefix: readonly IDBValidKey[],
		limit?: number,
	): Promise<TValue[]> {
		this.prefixReads++;
		return super.getAllByPrefix<TValue>(store, keyPrefix, limit);
	}

	/** Records bounded page and binary-index write amplification. */
	public override async commit(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		let pagePuts = 0;
		let deletes = 0;
		let indexBytes = 0;
		for (const operation of operations) {
			if (operation.type === "delete") {
				deletes++;
				continue;
			}
			if (operation.type !== "put") continue;
			if (operation.store === "pages") pagePuts++;
			if (operation.store !== "document_index_pages") continue;
			const segment = operation.value as EncodedDocumentIndexSegment;
			indexBytes += segment.encodedByteLength;
			this.totalIndexEntriesWritten += segment.count;
		}
		this.maximumPagePutsPerCommit = Math.max(
			this.maximumPagePutsPerCommit,
			pagePuts,
		);
		this.maximumDeletesPerCommit = Math.max(
			this.maximumDeletesPerCommit,
			deletes,
		);
		this.maximumIndexBytesPerCommit = Math.max(
			this.maximumIndexBytesPerCommit,
			indexBytes,
		);
		if (deletes > 0) this.deleteCommitCount++;
		if (this.failNextDeleteCommit && deletes > 0) {
			this.failNextDeleteCommit = false;
			throw new Error("Injected generation cleanup failure.");
		}
		if (
			deletes > 0 &&
			this.failDeleteCommitNumber === this.deleteCommitCount
		) {
			throw new Error("Injected generation cleanup interruption.");
		}
		await super.commit(operations);
	}

	/** Clears write metrics while preserving stored fixture data. */
	public resetWriteMetrics(): void {
		this.maximumPagePutsPerCommit = 0;
		this.maximumDeletesPerCommit = 0;
		this.maximumIndexBytesPerCommit = 0;
		this.totalIndexEntriesWritten = 0;
		this.deleteCommitCount = 0;
	}
}

/** Builds one finalized repository containing intentionally overlapping page ranges. */
async function createOverlappingRepository(): Promise<{
	backend: MemoryEventStoreBackend;
	repository: EventStoreRepository;
}> {
	const backend = new MemoryEventStoreBackend();
	const repository = new EventStoreRepository(backend);
	await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
	await repository.appendPages({
		sourceId: SOURCE_ID,
		generation: 1,
		pages: [
			createPage(
				1,
				1,
				1,
				[createDocument("event-100", 100), createDocument("event-80", 80)],
				[1, 3],
			),
			createPage(
				2,
				1,
				2,
				[createDocument("event-90", 90), createDocument("event-70", 70)],
				[2, 4],
			),
		],
	});
	await repository.finalizeGeneration(SOURCE_ID, 1);
	return { backend, repository };
}

describe("EventStoreRepository", () => {
	it("persists exact pages, LOD, ID segments, and source authority atomically", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		const summary = await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					1,
					1,
					1,
					[createDocument("first", 20), createDocument("second", 10)],
					[1, 2],
				),
			],
		});

		expect(summary.eventCount).toBe(2);
		expect(summary.status).toBe("building");
		expect(backend.getStoreSize("sources")).toBe(1);
		expect(backend.getStoreSize("pages")).toBe(1);
		// Both payloads share one store but retain independent clone boundaries.
		expect(backend.getStoreSize("page_lod")).toBe(2);
		const lod = await backend.get<EncodedEventPageLod>("page_lod", [
			SOURCE_ID,
			1,
			1,
			EVENT_PAGE_LOD_RECORD_KIND,
		]);
		const height = await backend.get<EncodedEventPageHeight>("page_lod", [
			SOURCE_ID,
			1,
			1,
			EVENT_PAGE_HEIGHT_RECORD_KIND,
		]);
		expect(Boolean(lod && "minuteTimestamps" in lod)).toBe(false);
		expect(Array.from(height?.minuteCounts ?? [])).toEqual([2]);
		expect(backend.getStoreSize("document_index_pages") > 0).toBe(true);
		expect((await repository.getByHandle(SOURCE_ID, 2))?._id).toBe("second");
	});

	it("rejects work from a superseded generation", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 2 });

		await expect(
			repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(1, 1, 1, [createDocument("late", 1)], [1]),
				],
			}),
		).rejects.toThrow("generation 1 is not building");
		await expect(repository.finalizeGeneration(SOURCE_ID, 1)).rejects.toThrow(
			"generation 1 is not building",
		);
	});

	it("allocates distinct source ordinals for concurrent source registration", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await Promise.all([
			repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 }),
			repository.beginGeneration({ sourceId: SECOND_SOURCE_ID, generation: 1 }),
		]);
		const manifests = await backend.getAll<EventSourceManifest>("sources");
		expect(new Set(manifests.map((manifest) => manifest.sourceOrdinal)).size).toBe(
			2,
		);
	});

	it("merges overlapping page ranges and paginates with revision cursors", async () => {
		const { repository } = await createOverlappingRepository();
		const firstPage = await repository.queryPixelBucket(
			SOURCE_ID,
			{ min: 70, max: 100 },
			2,
		);

		expect(firstPage.events.map((event) => event._id)).toEqual([
			"event-100",
			"event-90",
		]);
		expect(firstPage.total).toBe(4);
		expect(firstPage.nextCursor).not.toBeNull();

		const secondPage = await repository.queryPixelBucket(
			SOURCE_ID,
			{ min: 70, max: 100 },
			2,
			firstPage.nextCursor ?? undefined,
		);
		expect(secondPage.events.map((event) => event._id)).toEqual([
			"event-80",
			"event-70",
		]);
		expect(secondPage.total).toBe(4);
		expect(secondPage.nextCursor).toBeNull();
	});

	it("rejects a cursor after the source revision changes", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					1,
					1,
					1,
					[createDocument("first", 30), createDocument("second", 20)],
					[1, 2],
				),
			],
		});
		const page = await repository.queryPixelBucket(
			SOURCE_ID,
			{ min: 0, max: 100 },
			1,
		);
		expect(page.nextCursor).not.toBeNull();

		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(2, 1, 2, [createDocument("third", 10)], [3])],
		});
		const read = repository.queryPixelBucket(
			SOURCE_ID,
			{ min: 0, max: 100 },
			1,
			page.nextCursor ?? undefined,
		);
		await expect(read).rejects.toThrow(
			"cursor does not belong to the active source revision",
		);
		try {
			await read;
		} catch (error) {
			expect(error).toBeInstanceOf(StaleEventStoreCursorError);
		}
	});

	it("verifies exact IDs when binary hashes collide", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend(), {
			hashDocumentId: () => ({ high: 7, low: 11, prefix: 0 }),
		});
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					1,
					1,
					1,
					[createDocument("collision-a", 20), createDocument("collision-b", 10)],
					[1, 2],
				),
			],
		});

		expect((await repository.getById("collision-a" as Doc.Id))?._id).toBe(
			"collision-a",
		);
		expect((await repository.getById("collision-b" as Doc.Id))?._id).toBe(
			"collision-b",
		);
		expect(await repository.getById("collision-missing" as Doc.Id)).toBeNull();

		await repository.finalizeGeneration(SOURCE_ID, 1);
		await repository.upsertDocuments({
			documents: [{ ...createDocument("collision-b", 10), number_hash: 999 }],
		});
		expect((await repository.getById("collision-b" as Doc.Id))?.number_hash).toBe(
			999,
		);
		expect((await repository.getById("collision-a" as Doc.Id))?.number_hash).toBe(
			22,
		);
	});

	it("updates in place and retains a handle for same-source timestamp moves", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(1, 1, 1, [createDocument("mutable", 10)], [7])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);

		await repository.upsertDocuments({
			documents: [{ ...createDocument("mutable", 10), number_hash: 500 }],
		});
		expect((await repository.getByHandle(SOURCE_ID, 7))?.number_hash).toBe(500);

		await repository.upsertDocuments({
			documents: [createDocument("mutable", 50)],
		});
		const moved = await repository.getByHandle(SOURCE_ID, 7);
		expect(moved?._id).toBe("mutable");
		expect(moved?.gulp_timestamp).toBe(50);
		expect((await repository.getSourceSummary(SOURCE_ID))?.eventCount).toBe(1);
	});

	it("allocates a target-local handle when an event changes source", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(1, 1, 1, [createDocument("moving", 20)], [1])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);
		await repository.beginGeneration({
			sourceId: SECOND_SOURCE_ID,
			generation: 1,
		});
		await repository.appendPages({
			sourceId: SECOND_SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					1,
					1,
					1,
					[createDocument("target-existing", 30, SECOND_SOURCE_ID)],
					[1],
				),
			],
		});
		await repository.finalizeGeneration(SECOND_SOURCE_ID, 1);

		await repository.upsertDocuments({
			documents: [createDocument("moving", 40, SECOND_SOURCE_ID)],
			allowedSourceIds: [SOURCE_ID, SECOND_SOURCE_ID],
		});
		expect(await repository.getByHandle(SOURCE_ID, 1)).toBeNull();
		expect((await repository.getByHandle(SECOND_SOURCE_ID, 2))?._id).toBe(
			"moving",
		);
		expect((await repository.getSourceSummary(SOURCE_ID))?.eventCount).toBe(0);
		expect(
			(await repository.getSourceSummary(SECOND_SOURCE_ID))?.eventCount,
		).toBe(2);
	});

	it("rejects a cross-source move when the existing source lease is absent", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(1, 1, 1, [createDocument("guarded-move", 20)], [1])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);
		await repository.beginGeneration({
			sourceId: SECOND_SOURCE_ID,
			generation: 1,
		});
		await repository.finalizeGeneration(SECOND_SOURCE_ID, 1);

		let authorityError: unknown;
		try {
			await repository.upsertDocuments({
				documents: [createDocument("guarded-move", 40, SECOND_SOURCE_ID)],
				allowedSourceIds: [SECOND_SOURCE_ID],
			});
		} catch (error) {
			authorityError = error;
		}

		expect(authorityError).toBeInstanceOf(EventStoreSourceAuthorityError);
		expect((await repository.getByHandle(SOURCE_ID, 1))?._id).toBe(
			"guarded-move",
		);
		expect(await repository.getSourceSummary(SECOND_SOURCE_ID)).toMatchObject({
			eventCount: 0,
		});
	});

	it("compacts only a page whose tombstones exceed 25 percent", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					1,
					1,
					1,
					[
						createDocument("compact-a", 40),
						createDocument("compact-b", 30),
						createDocument("compact-c", 20),
						createDocument("compact-d", 10),
					],
					[1, 2, 3, 4],
				),
			],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);

		await repository.upsertDocuments({
			documents: [
				createDocument("compact-a", 400),
				createDocument("compact-b", 300),
			],
		});
		const compacted = await backend.get<EncodedEventPage>("pages", [
			SOURCE_ID,
			1,
			1,
		]);
		expect(compacted?.count).toBe(2);
		expect(compacted?.activeCount).toBe(2);
		expect(compacted?.tombstones).toEqual(new Uint8Array(2));
		expect((await repository.getByHandle(SOURCE_ID, 1))?.gulp_timestamp).toBe(
			400,
		);
		expect((await repository.getSourceSummary(SOURCE_ID))?.eventCount).toBe(4);
	});

	it("keeps ID-index reads bounded as page count grows", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend, {
			hashDocumentId: (documentId) => {
				const low = Number(documentId.split("-").at(-1)) + 1;
				return { high: 0xabc0_0000, low, prefix: 0xabc };
			},
		});
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		for (let pageId = 0; pageId < 80; pageId++) {
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(
						pageId,
						1,
						pageId + 1,
						[createDocument(`bounded-${pageId}`, 1_000 - pageId)],
						[pageId + 1],
					),
				],
			});
		}
		await repository.finalizeGeneration(SOURCE_ID, 1);
		const manifest = await backend.get<
			EventSourceManifest & {
				documentIndexSegments: Array<{ shard: number; segmentId: number }>;
			}
		>("sources", SOURCE_ID);
		const segments = await backend.getAllByPrefix<EncodedDocumentIndexSegment>(
			"document_index_pages",
			[SOURCE_ID, 1],
		);
		const orphan = segments[0];
		if (!manifest || !orphan) {
			throw new Error("Expected an active manifest and at least one index segment.");
		}
		await backend.commit([
			{
				type: "put",
				store: "document_index_pages",
				key: [SOURCE_ID, 1, orphan.shard, 0xffff_ffff],
				value: { ...orphan, segmentId: 0xffff_ffff },
			},
		]);
		backend.prefixReads = 0;
		backend.pageReads = 0;
		backend.indexReads = 0;
		backend.maximumIndexKeysPerRead = 0;

		expect((await repository.getById("bounded-47" as Doc.Id))?._id).toBe(
			"bounded-47",
		);
		expect(backend.prefixReads).toBe(0);
		expect(backend.indexReads).toBe(1);
		expect(backend.maximumIndexKeysPerRead).toBe(
			manifest.documentIndexSegments.filter(
				(entry) => entry.shard === orphan.shard,
			).length,
		);
		expect(backend.maximumIndexKeysPerRead <= 32).toBe(true);
		expect(backend.pageReads <= 2).toBe(true);
		expect(backend.getStoreSize("document_index_pages")).toBe(
			manifest.documentIndexSegments.length + 1,
		);
	});

	it("bounds scattered upsert page images and writes only revision-fenced index deltas", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend, {
			hashDocumentId: (documentId) => {
				const [, pageText, offsetText] = documentId.split("-");
				const prefix = Number(pageText) << 4;
				return {
					high: (prefix << 20) >>> 0,
					low: Number(offsetText) + 1,
					prefix,
				};
			},
		});
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		let nextHandle = 1;
		for (let pageId = 0; pageId < 128; pageId++) {
			const documents = Array.from({ length: 4 }, (_, offset) =>
				createDocument(
					`scattered-${pageId}-${offset}`,
					1_000_000 - pageId * 4 - offset,
				),
			);
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(
						pageId,
						1,
						pageId + 1,
						documents,
						documents.map(() => nextHandle++),
					),
				],
			});
		}
		await repository.finalizeGeneration(SOURCE_ID, 1);

		const metadataUpdates = Array.from({ length: 8 }, (_, pageId) => ({
			...createDocument(`scattered-${pageId}-0`, 1_000_000 - pageId * 4),
			number_hash: 50_000 + pageId,
		}));
		backend.resetWriteMetrics();
		await repository.upsertDocuments({ documents: metadataUpdates });
		expect(backend.maximumPagePutsPerCommit).toBe(8);
		expect(backend.totalIndexEntriesWritten).toBe(32);
		expect(backend.maximumIndexBytesPerCommit).toBe(32 * 24);

		backend.resetWriteMetrics();
		await repository.upsertDocuments({
			documents: metadataUpdates.map((document, index) => ({
				...document,
				gulp_timestamp: 2_000_000 + index,
			})),
		});
		expect(backend.maximumPagePutsPerCommit).toBe(9);
		expect(backend.totalIndexEntriesWritten).toBe(32);
		for (let index = 0; index < metadataUpdates.length; index++) {
			expect(
				(await repository.getById(metadataUpdates[index]._id))?.gulp_timestamp,
			).toBe(2_000_000 + index);
		}
		expect((await repository.getSourceSummary(SOURCE_ID))?.eventCount).toBe(512);
	});

	it("marks interrupted building generations incomplete", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		const summary = await repository.markGenerationIncomplete(SOURCE_ID, 1);
		expect(summary.status).toBe("incomplete");
		await expect(repository.finalizeGeneration(SOURCE_ID, 1)).rejects.toThrow(
			"generation 1 is not building",
		);
	});

	it("restarts revision-scoped handles for a replacement generation", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(1, 1, 1, [createDocument("old", 10)], [900])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);

		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 2 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 2,
			pages: [createPage(1, 2, 1, [createDocument("new", 20)], [1])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 2);

		expect((await repository.getByHandle(SOURCE_ID, 1))?._id).toBe("new");
		expect(await repository.getByHandle(SOURCE_ID, 900)).toBeNull();
	});

	it("keeps the last duplicate across building chunks without a source-wide ID set", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(1, 1, 1, [createDocument("duplicate", 10)], [1]),
			],
		});
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					2,
					1,
					2,
					[
						createDocument("duplicate", 30),
						createDocument("same-page", 20),
						{ ...createDocument("same-page", 40), number_hash: 999 },
					],
					[2, 3, 4],
				),
			],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);

		expect((await repository.getById("duplicate" as Doc.Id))?.gulp_timestamp).toBe(
			30,
		);
		expect((await repository.getById("same-page" as Doc.Id))?.number_hash).toBe(
			999,
		);
		expect((await repository.getSourceSummary(SOURCE_ID))?.eventCount).toBe(2);
		const all = await repository.queryPixelBucket(
			SOURCE_ID,
			{ min: 0, max: 100 },
			100,
		);
		expect(all.events.map((event) => event._id)).toEqual([
			"same-page",
			"duplicate",
		]);
	});

	it("bounds scattered building-duplicate page images to eight per commit", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend, {
			hashDocumentId: (documentId) => {
				const pageId = Number(documentId.split("-").at(-1));
				const prefix = pageId << 4;
				return {
					high: (prefix << 20) >>> 0,
					low: pageId + 1,
					prefix,
				};
			},
		});
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		for (let pageId = 0; pageId < 24; pageId++) {
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(
						pageId,
						1,
						pageId + 1,
						[createDocument(`building-${pageId}`, 1_000 - pageId)],
						[pageId + 1],
					),
				],
			});
		}

		const replacements = Array.from({ length: 16 }, (_, pageId) =>
			createDocument(`building-${pageId}`, 2_000 + pageId),
		);
		backend.resetWriteMetrics();
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [
				createPage(
					24,
					1,
					25,
					replacements,
					Array.from({ length: replacements.length }, (_, index) => 25 + index),
				),
			],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);

		expect(backend.maximumPagePutsPerCommit).toBe(8);
		expect(backend.totalIndexEntriesWritten).toBe(16);
		expect((await repository.getSourceSummary(SOURCE_ID))?.eventCount).toBe(24);
		for (let pageId = 0; pageId < replacements.length; pageId++) {
			expect(
				(await repository.getById(`building-${pageId}` as Doc.Id))
					?.gulp_timestamp,
			).toBe(2_000 + pageId);
		}
	});

	it("bounds exact reads for high-cardinality equal-timestamp pixel buckets", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		let handle = 1;
		for (let pageId = 0; pageId < 8; pageId++) {
			const documents = Array.from({ length: 128 }, (_, index) =>
				createDocument(`same-${pageId}-${index}`, 123_456),
			);
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(
						pageId,
						1,
						pageId + 1,
						documents,
						Array.from({ length: documents.length }, () => handle++),
					),
				],
			});
		}
		await repository.finalizeGeneration(SOURCE_ID, 1);
		backend.pageReads = 0;

		const result = await repository.queryPixelBucket(
			SOURCE_ID,
			{ min: 123_456, max: 123_456 },
			100,
		);
		expect(result.total).toBe(1_024);
		expect(result.events).toHaveLength(100);
		expect(result.nextCursor).not.toBeNull();
		expect(backend.pageReads <= 2).toBe(true);
	});

	it("restores the previous complete generation after cancel", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(1, 1, 1, [createDocument("stable", 10)], [1])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);

		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 2 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 2,
			pages: [createPage(1, 2, 1, [createDocument("replacement", 20)], [2])],
		});
		await repository.cancelGeneration(SOURCE_ID, 2);

		expect((await repository.getById("stable" as Doc.Id))?._id).toBe("stable");
		expect(await repository.getById("replacement" as Doc.Id)).toBeNull();
		expect((await repository.getSourceSummary(SOURCE_ID))?.status).toBe(
			"complete",
		);
		expect(backend.getStoreSize("pages")).toBe(1);
	});

	it("finalizes before bounded cleanup and resumes an interrupted marker on reopen", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		for (let pageId = 0; pageId < 90; pageId++) {
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(
						pageId,
						1,
						pageId + 1,
						[createDocument(`obsolete-${pageId}`, 1_000 - pageId)],
						[pageId + 1],
					),
				],
			});
		}
		await repository.finalizeGeneration(SOURCE_ID, 1);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 2 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 2,
			pages: [
				createPage(0, 2, 1, [createDocument("authoritative", 2_000)], [1]),
			],
		});

		backend.resetWriteMetrics();
		const complete = await repository.finalizeGeneration(SOURCE_ID, 2);
		const pending = await backend.get<EventSourceManifest>("sources", SOURCE_ID);
		expect(complete).toMatchObject({
			generation: 2,
			status: "complete",
			eventCount: 1,
		});
		expect(backend.maximumDeletesPerCommit).toBe(0);
		expect(pending?.obsoleteGenerations).toEqual([{ generation: 1 }]);
		expect(
			await backend.getAllByPrefix("pages", [SOURCE_ID, 1]),
		).toHaveLength(90);

		backend.failDeleteCommitNumber = 2;
		await expect(repository.cleanupObsoleteGenerations(SOURCE_ID)).rejects.toThrow(
			"Injected generation cleanup interruption",
		);
		expect(backend.maximumDeletesPerCommit <= 256).toBe(true);
		expect(
			(await backend.getAllByPrefix("pages", [SOURCE_ID, 1])).length < 90,
		).toBe(true);
		expect(
			(await backend.get<EventSourceManifest>("sources", SOURCE_ID))
				?.obsoleteGenerations,
		).toHaveLength(1);
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject({
			generation: 2,
			status: "complete",
			eventCount: 1,
		});

		backend.failDeleteCommitNumber = null;
		const reopened = new EventStoreRepository(backend);
		await reopened.beginGeneration({ sourceId: SOURCE_ID, generation: 3 });
		expect(
			await backend.getAllByPrefix("pages", [SOURCE_ID, 1]),
		).toHaveLength(0);
		expect(
			(await backend.get<EventSourceManifest>("sources", SOURCE_ID))
				?.obsoleteGenerations,
		).toBeUndefined();
		expect(await reopened.getSourceSummary(SOURCE_ID)).toMatchObject({
			generation: 3,
			status: "building",
		});
		expect(backend.maximumDeletesPerCommit <= 256).toBe(true);
	});

	it("discards rollback storage in bounded batches and cancels in place as incomplete", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		for (let pageId = 0; pageId < 90; pageId++) {
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: 1,
				pages: [
					createPage(
						pageId,
						1,
						pageId + 1,
						[createDocument(`rollback-${pageId}`, 1_000 - pageId)],
						[pageId + 1],
					),
				],
			});
		}
		await repository.finalizeGeneration(SOURCE_ID, 1);

		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 2 });
		const building = await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 2,
			pages: [
				createPage(
					0,
					2,
					1,
					[createDocument("in-place", 2_000)],
					[1],
				),
			],
		});
		backend.resetWriteMetrics();
		const discarded = await repository.discardGenerationRollback(SOURCE_ID, 2);
		const stored = await backend.get<{
			rollback?: unknown;
			inPlaceReplacement?: boolean;
		}>("sources", SOURCE_ID);

		expect(discarded.status).toBe("building");
		expect(discarded.revision).toBe(building.revision + 1);
		expect(discarded.eventCount).toBe(1);
		expect(stored?.rollback).toBeUndefined();
		expect(stored?.inPlaceReplacement).toBe(true);
		expect(backend.maximumDeletesPerCommit <= 256).toBe(true);
		expect(
			await backend.getAllByPrefix("pages", [SOURCE_ID, 1]),
		).toHaveLength(0);
		expect(
			await backend.getAllByPrefix("page_lod", [SOURCE_ID, 1]),
		).toHaveLength(0);
		expect(
			await backend.getAllByPrefix("document_index_pages", [SOURCE_ID, 1]),
		).toHaveLength(0);
		expect(
			await backend.getAllByPrefix("pages", [SOURCE_ID, 2]),
		).toHaveLength(1);
		expect(
			(await backend.getAllByPrefix("document_index_pages", [SOURCE_ID, 2]))
				.length > 0,
		).toBe(true);
		expect(await repository.getById("rollback-0" as Doc.Id)).toBeNull();
		expect((await repository.getById("in-place" as Doc.Id))?._id).toBe(
			"in-place",
		);

		await repository.cancelGeneration(SOURCE_ID, 2);
		const incomplete = await repository.getSourceSummary(SOURCE_ID);
		expect(incomplete?.status).toBe("incomplete");
		expect(incomplete?.eventCount).toBe(1);
		expect((await repository.getById("in-place" as Doc.Id))?._id).toBe(
			"in-place",
		);
	});

	it("keeps a failed rollback cleanup non-authoritative", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages: [createPage(0, 1, 1, [createDocument("old", 10)], [1])],
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 2 });
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 2,
			pages: [createPage(0, 2, 1, [createDocument("partial", 20)], [1])],
		});

		backend.failNextDeleteCommit = true;
		await expect(
			repository.discardGenerationRollback(SOURCE_ID, 2),
		).rejects.toThrow("Injected generation cleanup failure");
		const detached = await backend.get<{
			status: string;
			rollback?: unknown;
			inPlaceReplacement?: boolean;
		}>("sources", SOURCE_ID);
		expect(detached?.status).toBe("building");
		expect(detached?.rollback).toBeUndefined();
		expect(detached?.inPlaceReplacement).toBe(true);
		expect(
			await backend.getAllByPrefix("pages", [SOURCE_ID, 1]),
		).toHaveLength(1);
		expect(await repository.getById("old" as Doc.Id)).toBeNull();
		expect((await repository.getById("partial" as Doc.Id))?._id).toBe("partial");

		await repository.cancelGeneration(SOURCE_ID, 2);
		expect((await repository.getSourceSummary(SOURCE_ID))?.status).toBe(
			"incomplete",
		);
	});

	it("returns bounded canonical neighbors across pages", async () => {
		const { repository } = await createOverlappingRepository();
		const neighbors = await repository.getNeighbors(
			SOURCE_ID,
			2,
			1,
			2,
		);

		expect(neighbors.events.map((event) => event._id)).toEqual([
			"event-100",
			"event-90",
			"event-80",
			"event-70",
		]);
		expect(neighbors.currentIndex).toBe(1);
	});

	it("stops neighbor reads at adjacent non-overlapping pages", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await repository.beginGeneration({ sourceId: SOURCE_ID, generation: 1 });
		const pages = Array.from({ length: 40 }, (_, pageId) => {
			const documents = Array.from({ length: 4 }, (_, offset) =>
				createDocument(
					`ordered-${pageId}-${offset}`,
					10_000 - (pageId * 4 + offset),
				),
			);
			return createPage(
				pageId,
				1,
				1,
				documents,
				documents.map((_, offset) => pageId * 4 + offset + 1),
			);
		});
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: 1,
			pages,
		});
		await repository.finalizeGeneration(SOURCE_ID, 1);
		backend.pageReads = 0;

		const neighbors = await repository.getNeighbors(
			SOURCE_ID,
			81,
			5,
			5,
		);

		expect(neighbors.events.map((event) => event.gulp_timestamp)).toEqual(
			Array.from({ length: 11 }, (_, index) => 9_925 - index),
		);
		expect(neighbors.currentIndex).toBe(5);
		expect(backend.pageReads <= 10).toBe(true);
	});

	it("atomically retains completed query provenance and cache-head history", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend, { now: () => 1234 });
		const generation = await appendQueryCache(
			repository,
			undefined,
			"fingerprint-one",
			"cached-one",
			10,
		);

		expect(generation).toBe(1);
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject({
			generation: 1,
			authorityRevision: 1,
			activeCacheGeneration: 1,
			cacheHistory: [1],
			cacheHistoryCursor: 0,
			nextGeneration: 2,
		});
		const records = await backend.getAllByPrefix<EventSourceQueryCacheRecord>(
			"source_query_caches",
			[SOURCE_ID],
		);
		expect(records).toHaveLength(1);
		expect(records[0]).toMatchObject({
			sourceId: SOURCE_ID,
			generation: 1,
			state: "exact",
			createdAt: 1234,
			lastActivatedAt: 1234,
			provenance: {
				fingerprint: "fingerprint-one",
			},
			manifest: { status: "complete", eventCount: 1 },
		});
		expect(
			records[0].provenance.shapeFingerprint?.startsWith(
				"source-query-shape-v1:",
			),
		).toBe(true);
		expect("cacheHistory" in records[0].manifest).toBe(false);
		expect((await repository.listQueryCaches(SOURCE_ID))[0]).toMatchObject({
			generation: 1,
			state: "exact",
			eventCount: 1,
		});
	});

	it("does not synthesize a missing shape while finalizing a v4 cache", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const provenanceWithShape = createQueryCacheProvenance("legacy-v4");
		const { shapeFingerprint: _shapeFingerprint, ...legacyProvenance } =
			provenanceWithShape;
		const building = await repository.beginGeneration({
			sourceId: SOURCE_ID,
			queryCache: legacyProvenance,
		});
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: building.generation,
			pages: [
				createPage(
					0,
					building.generation,
					building.revision,
					[createDocument("legacy-v4", 10)],
					[1],
				),
			],
		});
		await repository.finalizeGeneration(SOURCE_ID, building.generation);

		const record = await backend.get<EventSourceQueryCacheRecord>(
			"source_query_caches",
			[SOURCE_ID, building.generation],
		);
		expect(record?.provenance.shapeFingerprint).toBeUndefined();
	});

	it("undoes and redoes older physical generations through monotonic authority", async () => {
		let now = 100;
		const repository = new EventStoreRepository(new MemoryEventStoreBackend(), {
			now: () => ++now,
		});
		await appendQueryCache(repository, undefined, "first", "old-event", 10);
		await appendQueryCache(repository, undefined, "second", "new-event", 20);
		const latest = await repository.getSourceSummary(SOURCE_ID);

		const undone = await repository.undoQueryCache(SOURCE_ID);
		expect(undone?.summary).toMatchObject({
			generation: 1,
			activeCacheGeneration: 1,
			cacheHistory: [1, 2],
			cacheHistoryCursor: 0,
		});
		expect(
			undone!.summary.authorityRevision > latest!.authorityRevision,
		).toBe(true);
		expect((await repository.getById("old-event" as Doc.Id))?._id).toBe(
			"old-event",
		);
		expect(await repository.getById("new-event" as Doc.Id)).toBeNull();

		const redone = await repository.redoQueryCache(SOURCE_ID);
		expect(redone?.summary).toMatchObject({
			generation: 2,
			activeCacheGeneration: 2,
			cacheHistoryCursor: 1,
			nextGeneration: 3,
		});
		expect(
			redone!.summary.authorityRevision > undone!.summary.authorityRevision,
		).toBe(true);
		expect(await repository.undoQueryCache(SOURCE_ID)).not.toBeNull();
		expect(await repository.undoQueryCache(SOURCE_ID)).toBeNull();
	});

	it("truncates redo and enforces an injectable cache-history limit", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend(), {
			queryCacheLimit: 3,
		});
		await appendQueryCache(repository, undefined, "one", "one", 10);
		await appendQueryCache(repository, undefined, "two", "two", 20);
		await appendQueryCache(repository, undefined, "three", "three", 30);
		await repository.undoQueryCache(SOURCE_ID);
		await repository.undoQueryCache(SOURCE_ID);

		await appendQueryCache(repository, undefined, "four", "four", 40);
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject({
			cacheHistory: [1, 4],
			cacheHistoryCursor: 1,
			nextGeneration: 5,
		});
		expect(
			(await repository.listQueryCaches(SOURCE_ID)).map(
				(cache) => cache.generation,
			),
		).toEqual([1, 4]);

		await appendQueryCache(repository, undefined, "five", "five", 50);
		await appendQueryCache(repository, undefined, "six", "six", 60);
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject({
			cacheHistory: [4, 5, 6],
			cacheHistoryCursor: 2,
			nextGeneration: 7,
		});
		expect(
			(await repository.listQueryCaches(SOURCE_ID)).map(
				(cache) => cache.generation,
			),
		).toEqual([4, 5, 6]);
	});

	it("reconciles an exact cache or latest reusable fallback and prunes alternatives", async () => {
		let now = 0;
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend, {
			now: () => ++now,
		});
		await appendQueryCache(repository, undefined, "one", "one", 10);
		await appendQueryCache(repository, undefined, "two", "two", 20);
		await appendQueryCache(repository, undefined, "three", "three", 30);
		backend.pageReads = 0;

		const exact = await repository.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup("two"),
		});
		expect(exact).toMatchObject({
			match: "exact",
			cache: { generation: 2, provenance: { fingerprint: "two" } },
			summary: { generation: 2, cacheHistory: [2], cacheHistoryCursor: 0 },
		});
		expect(
			(await repository.listQueryCaches(SOURCE_ID)).map(
				(cache) => cache.generation,
			),
		).toEqual([2]);
		expect(backend.pageReads).toBe(0);

		await appendQueryCache(repository, undefined, "latest", "latest", 40);
		const fallback = await repository.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup("missing", {
				min: 1_000,
				max: 2_000,
			}),
		});
		expect(fallback).toMatchObject({
			match: "latest",
			cache: { generation: 4, provenance: { fingerprint: "latest" } },
			summary: { cacheHistory: [4] },
		});
		expect(await repository.getById("latest" as Doc.Id)).not.toBeNull();
		expect(await repository.getById("two" as Doc.Id)).toBeNull();
	});

	it("ranks fresh exact, fresh covered, stale exact, then latest caches", async () => {
		const currentSnapshot = {
			total: 1,
			minNanoseconds: "0",
			maxNanoseconds: "100",
			timeUpdated: 1,
			millisecondPrecision: false,
		};
		const desiredRange = { min: 0, max: 200 };

		const exactRepository = new EventStoreRepository(
			new MemoryEventStoreBackend(),
		);
		await appendQueryCache(
			exactRepository,
			undefined,
			"desired",
			"exact",
			10,
			{ range: desiredRange, serverSnapshot: currentSnapshot },
		);
		await appendQueryCache(
			exactRepository,
			undefined,
			"covered-newer",
			"covered-newer",
			20,
			{
				range: { min: 0, max: 100 },
				serverSnapshot: currentSnapshot,
			},
		);
		const exact = await exactRepository.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup(
				"desired",
				desiredRange,
				currentSnapshot,
			),
		});
		expect(exact).toMatchObject({
			match: "exact",
			cache: { generation: 1 },
			summary: { cacheHistory: [1] },
		});

		const coveredRepository = new EventStoreRepository(
			new MemoryEventStoreBackend(),
		);
		await appendQueryCache(
			coveredRepository,
			undefined,
			"desired",
			"exact-stale",
			10,
			{
				range: desiredRange,
				serverSnapshot: { ...currentSnapshot, total: 2 },
			},
		);
		await appendQueryCache(
			coveredRepository,
			undefined,
			"covered",
			"covered",
			20,
			{
				range: { min: 0, max: 100 },
				serverSnapshot: currentSnapshot,
			},
		);
		await appendQueryCache(
			coveredRepository,
			undefined,
			"latest",
			"latest",
			30,
			{
				range: { min: 300, max: 400 },
				serverSnapshot: currentSnapshot,
			},
		);
		const covered = await coveredRepository.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup(
				"desired",
				desiredRange,
				currentSnapshot,
			),
		});
		expect(covered).toMatchObject({
			match: "covered",
			cache: { generation: 2 },
			summary: { cacheHistory: [2] },
		});
		expect(
			(await coveredRepository.listQueryCaches(SOURCE_ID)).map(
				(cache) => cache.generation,
			),
		).toEqual([2]);

		const staleExactRepository = new EventStoreRepository(
			new MemoryEventStoreBackend(),
		);
		await appendQueryCache(
			staleExactRepository,
			undefined,
			"desired",
			"exact-stale",
			10,
			{
				range: desiredRange,
				serverSnapshot: { ...currentSnapshot, total: 2 },
			},
		);
		await appendQueryCache(
			staleExactRepository,
			undefined,
			"latest",
			"latest",
			20,
			{
				range: { min: 300, max: 400 },
				serverSnapshot: currentSnapshot,
			},
		);
		const staleExact = await staleExactRepository.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup(
				"desired",
				desiredRange,
				currentSnapshot,
			),
		});
		expect(staleExact).toMatchObject({
			match: "exact",
			cache: { generation: 1 },
			summary: { cacheHistory: [1] },
		});
	});

	it("does not cover-match a v4 cache that has no persisted query shape", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const serverSnapshot = {
			total: 1,
			minNanoseconds: "0",
			maxNanoseconds: "100",
			timeUpdated: 1,
			millisecondPrecision: false,
		};
		await appendQueryCache(
			repository,
			undefined,
			"legacy-full-fingerprint",
			"legacy-covered",
			10,
			{
				range: { min: 0, max: 100 },
				serverSnapshot,
			},
		);
		const record = await backend.get<EventSourceQueryCacheRecord>(
			"source_query_caches",
			[SOURCE_ID, 1],
		);
		if (!record) throw new Error("Expected a persisted query cache record.");
		const { shapeFingerprint: _shapeFingerprint, ...legacyProvenance } =
			record.provenance;
		await backend.commit([
			{
				type: "put",
				store: "source_query_caches",
				key: [SOURCE_ID, 1],
				value: { ...record, provenance: legacyProvenance },
			},
		]);

		const reconciled = await repository.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup(
				"different-full-fingerprint",
				{ min: 0, max: 200 },
				serverSnapshot,
			),
		});

		expect(reconciled).toMatchObject({
			match: "latest",
			cache: { generation: 1 },
		});
		expect(reconciled?.cache.provenance.shapeFingerprint).toBeUndefined();
	});

	it("marks an upserted active cache mutated and refuses it at startup", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await appendQueryCache(repository, undefined, "original", "mutable", 10);
		const authorityBeforeMutation = (
			await repository.getSourceSummary(SOURCE_ID)
		)?.authorityRevision;
		await repository.upsertDocuments({
			documents: [{ ...createDocument("mutable", 10), number_hash: 999 }],
		});
		expect(await repository.listQueryCaches(SOURCE_ID)).toMatchObject([
			{ generation: 1, state: "mutated", eventCount: 1 },
		]);

			expect(
				await repository.reconcileQueryCache({
					sourceId: SOURCE_ID,
					desired: createDesiredQueryCacheLookup("original"),
			}),
		).toBeNull();
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject({
			status: "incomplete",
			activeCacheGeneration: null,
			cacheHistory: [],
			cacheHistoryCursor: -1,
			authorityRevision: (authorityBeforeMutation ?? 0) + 1,
		});
		expect(backend.getStoreSize("source_query_caches")).toBe(0);
		expect(backend.getStoreSize("pages")).toBe(0);
	});

	it("preserves pooled rollback on interruption and rejects destructive fallback", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		await appendQueryCache(repository, undefined, "stable", "stable", 10);
		const building = await repository.beginGeneration({
			sourceId: SOURCE_ID,
			queryCache: createQueryCacheProvenance("replacement"),
		});
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: building.generation,
			pages: [
				createPage(
					0,
					building.generation,
					building.revision,
					[createDocument("partial", 20)],
					[1],
				),
			],
		});

		await expect(
			repository.discardGenerationRollback(SOURCE_ID, building.generation),
		).rejects.toThrow("cannot discard its rollback cache");
		const restored = await repository.markGenerationIncomplete(
			SOURCE_ID,
			building.generation,
		);
		expect(restored).toMatchObject({
			generation: 1,
			status: "complete",
			activeCacheGeneration: 1,
			cacheHistory: [1],
		});
		expect((await repository.getById("stable" as Doc.Id))?._id).toBe("stable");
		expect(await repository.getById("partial" as Doc.Id)).toBeNull();
		expect(await repository.listQueryCaches(SOURCE_ID)).toHaveLength(1);
	});

	it("recovers an interrupted building cache before startup reconciliation", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await appendQueryCache(repository, undefined, "stable", "stable", 10);
		const interrupted = await repository.beginGeneration({
			sourceId: SOURCE_ID,
			queryCache: createQueryCacheProvenance("interrupted"),
		});
		await repository.appendPages({
			sourceId: SOURCE_ID,
			generation: interrupted.generation,
			pages: [
				createPage(
					0,
					interrupted.generation,
					interrupted.revision,
					[createDocument("uncommitted", 20)],
					[1],
				),
			],
		});

		const reopened = new EventStoreRepository(backend);
		const reconciled = await reopened.reconcileQueryCache({
			sourceId: SOURCE_ID,
			desired: createDesiredQueryCacheLookup("stable"),
		});
		expect(reconciled).toMatchObject({
			match: "exact",
			cache: { generation: 1 },
			summary: { status: "complete", cacheHistory: [1] },
		});
		expect((await reopened.getById("stable" as Doc.Id))?._id).toBe("stable");
		expect(await reopened.getById("uncommitted" as Doc.Id)).toBeNull();
	});

	it("prunes pooled generations through delete batches capped at 256 operations", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const building = await repository.beginGeneration({
			sourceId: SOURCE_ID,
			queryCache: createQueryCacheProvenance("large"),
		});
		for (let pageId = 0; pageId < 90; pageId++) {
			await repository.appendPages({
				sourceId: SOURCE_ID,
				generation: building.generation,
				pages: [
					createPage(
						pageId,
						building.generation,
						building.revision,
						[createDocument(`large-${pageId}`, 1_000 - pageId)],
						[pageId + 1],
					),
				],
			});
		}
		await repository.finalizeGeneration(SOURCE_ID, building.generation);
		await appendQueryCache(repository, undefined, "small", "small", 2_000);
		backend.resetWriteMetrics();

		await repository.pruneQueryCaches(SOURCE_ID, [2]);
		expect(backend.maximumDeletesPerCommit <= 256).toBe(true);
		expect(
			await backend.getAllByPrefix("pages", [SOURCE_ID, 1]),
		).toHaveLength(0);
		expect(
			(await repository.listQueryCaches(SOURCE_ID)).map(
				(cache) => cache.generation,
			),
		).toEqual([2]);
	});

	it("resumes an interrupted pooled cleanup from its persisted marker", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await appendQueryCache(repository, undefined, "old", "old", 10);
		await appendQueryCache(repository, undefined, "active", "active", 20);
		backend.failNextDeleteCommit = true;

		await expect(repository.pruneQueryCaches(SOURCE_ID, [2])).rejects.toThrow(
			"Injected generation cleanup failure",
		);
		const interrupted = await backend.get<{
			pendingCacheCleanup?: Array<{ generation: number }>;
		}>("sources", SOURCE_ID);
		expect(interrupted?.pendingCacheCleanup).toEqual([{ generation: 1 }]);
		expect(backend.getStoreSize("source_query_caches")).toBe(2);

		await repository.cleanupObsoleteGenerations(SOURCE_ID);
		expect(
			(await backend.get<{ pendingCacheCleanup?: unknown }>("sources", SOURCE_ID))
				?.pendingCacheCleanup,
		).toBeUndefined();
		expect(
			(await repository.listQueryCaches(SOURCE_ID)).map(
				(cache) => cache.generation,
			),
		).toEqual([2]);
	});

	it("resets a cache pool without moving authority or generation allocation backwards", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await appendQueryCache(repository, undefined, "first", "first", 10);
		await appendQueryCache(repository, undefined, "second", "second", 20);
		const undone = await repository.undoQueryCache(SOURCE_ID);
		if (!undone) throw new Error("Expected cache history before reset.");

		const reset = await repository.resetQueryCachePool(SOURCE_ID);
		expect(reset).toMatchObject({
			generation: 1,
			status: "incomplete",
			eventCount: 0,
			activeCacheGeneration: null,
			cacheHistory: [],
			cacheHistoryCursor: -1,
			nextGeneration: 3,
		});
		expect(reset!.authorityRevision > undone.summary.authorityRevision).toBe(true);
		expect(backend.getStoreSize("source_query_caches")).toBe(0);
		expect(backend.getStoreSize("pages")).toBe(0);
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject(reset!);

		const rebuiltGeneration = await appendQueryCache(
			repository,
			undefined,
			"rebuilt",
			"rebuilt",
			30,
		);
		expect(rebuiltGeneration).toBe(3);
		expect(await repository.getSourceSummary(SOURCE_ID)).toMatchObject({
			generation: 3,
			status: "complete",
			cacheHistory: [3],
			nextGeneration: 4,
		});
		expect((await repository.getById("rebuilt" as Doc.Id))?._id).toBe(
			"rebuilt",
		);
		expect(await repository.getById("first" as Doc.Id)).toBeNull();
		expect(await repository.getById("second" as Doc.Id)).toBeNull();
	});

	it("uses typed errors for stale generation operations", async () => {
		const repository = new EventStoreRepository(new MemoryEventStoreBackend());
		try {
			await repository.cancelGeneration(SOURCE_ID, 99);
		} catch (error) {
			expect(error).toBeInstanceOf(StaleEventStoreGenerationError);
		}
	});
});
