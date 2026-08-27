import type { Doc } from "@/entities/Doc";
import type { QueryCacheProvenance } from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
import { encodeEventPage } from "@/event-store/EventPage.codec";
import type { EventStoreName } from "@/event-store/EventStore.backend";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import {
	EVENT_HEIGHT_MAXIMUM_WORKING_BUCKET_LIMIT,
	StaleEventStoreProjectionError,
	EventStoreViewportProjector,
	type EventStoreViewportDescriptor,
} from "@/event-store/EventStore.viewport-projector";
import {
	EVENT_PAGE_HEIGHT_RECORD_KIND,
	EVENT_PAGE_LOD_RECORD_KIND,
	type EncodedEventPage,
	type EncodedEventPageLod,
} from "@/event-store/EventStore.types";
import { TextDecoder, TextEncoder } from "util";

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

const DEFAULT_SOURCE_ID = "projector-default" as Source.Id;

interface ExpectedEvent {
	handle: number;
	timestamp: number;
	hash: number;
}

/** Counts physical page-level reads without changing backend clone semantics. */
class CountingMemoryEventStoreBackend extends MemoryEventStoreBackend {
	public exactPageReads = 0;
	public pageLodReads = 0;
	public pageHeightReads = 0;

	/** Counts exact and LOD point reads. */
	public override async get<TValue>(
		store: EventStoreName,
		key: IDBValidKey,
	): Promise<TValue | undefined> {
		if (store === "pages") this.exactPageReads++;
		if (store === "page_lod") {
			if (Array.isArray(key) && key.at(-1) === EVENT_PAGE_HEIGHT_RECORD_KIND) {
				this.pageHeightReads++;
			} else {
				this.pageLodReads++;
			}
		}
		return super.get<TValue>(store, key);
	}

	/** Resets counters after fixture persistence. */
	public resetReads(): void {
		this.exactPageReads = 0;
		this.pageLodReads = 0;
		this.pageHeightReads = 0;
	}
}

/** Cancels one viewport guard immediately after its first persisted LOD read. */
class SupersedingMemoryEventStoreBackend extends CountingMemoryEventStoreBackend {
	public supersedeAfterFirstLodRead: (() => void) | null = null;
	public commitAfterInitialManifestRead: (() => Promise<void>) | null = null;

	/**
	 * Completes one real read, then simulates a newer worker request taking ownership.
	 * @param store EventStore object store being read.
	 * @param key Primary key being read.
	 * @returns Persisted value cloned by the memory backend.
	 */
	public override async get<TValue>(
		store: EventStoreName,
		key: IDBValidKey,
	): Promise<TValue | undefined> {
		const value = await super.get<TValue>(store, key);
		if (
			store === "page_lod" &&
			this.pageLodReads === 1 &&
			this.supersedeAfterFirstLodRead
		) {
			const supersede = this.supersedeAfterFirstLodRead;
			this.supersedeAfterFirstLodRead = null;
			supersede();
		}
		if (store === "sources" && this.commitAfterInitialManifestRead) {
			const commit = this.commitAfterInitialManifestRead;
			this.commitAfterInitialManifestRead = null;
			await commit();
		}
		return value;
	}
}

/** Creates one minimal exact event document. */
function createDocument(
	id: string,
	timestamp: number,
	hash: number,
	sourceId: Source.Id = DEFAULT_SOURCE_ID,
): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": sourceId,
		"gulp.event_code": hash + 1,
		number_hash: hash,
	};
}

/** Encodes one source page with explicit revision-scoped handles. */
function createPage(
	sourceId: Source.Id,
	pageId: number,
	documents: readonly Doc.Type[],
	handles: readonly number[],
): EncodedEventPage {
	return encodeEventPage({
		sourceId,
		generation: 1,
		pageId,
		revision: pageId + 1,
		documents,
		handles: Uint32Array.from(handles),
	});
}

/** Persists and finalizes one source fixture. */
async function persistSource(
	repository: EventStoreRepository,
	sourceId: Source.Id,
	pages: readonly EncodedEventPage[],
): Promise<number> {
	await repository.beginGeneration({ sourceId, generation: 1 });
	await repository.appendPages({ sourceId, generation: 1, pages });
	return (await repository.finalizeGeneration(sourceId, 1)).revision;
}

/** Creates a standard default-engine viewport descriptor. */
function createDescriptor(
	sourceId: Source.Id,
	overrides: readonly number[] = [],
): EventStoreViewportDescriptor {
	return {
		operationEpoch: 4,
		viewportRevision: 7,
		projectionRevision: 9,
		frame: { min: 0, max: 1_000 },
		width: 100,
		scale: 1,
		scrollX: 0,
		horizontalOverscanPixels: 0,
		sources: [
			{
				sourceId,
				offset: 0,
				engine: "default",
				frequency: 0,
				overrideHashes: overrides,
				settingsRevision: 1,
				overrideRevision: overrides.length > 0 ? 3 : 0,
			},
		],
	};
}

/** Asserts that a transferable column owns exactly its complete backing buffer. */
function expectExactBuffer(view: ArrayBufferView): void {
	expect(view.byteOffset).toBe(0);
	expect(view.byteLength).toBe(view.buffer.byteLength);
}

/** Creates minimal query provenance for authority-switch projection coverage. */
function createQueryProvenance(fingerprint: string): QueryCacheProvenance {
	return {
		query: {
			string: fingerprint,
			filters: [],
			source_config: {
				operation_id: "operation",
				source_ids: [DEFAULT_SOURCE_ID],
				range: { min: 0, max: 1_000 },
			},
		},
		fingerprint,
		normalization: {
			field: "number_hash",
			hashFunction: "fnv1a",
			requestedFields: [],
		},
		serverSnapshot: {
			total: 1,
			minNanoseconds: "0",
			maxNanoseconds: "1000000000",
			timeUpdated: 1,
			millisecondPrecision: false,
		},
	};
}

describe("EventStoreViewportProjector", () => {
	it("projects logical authority after undo activates an older generation", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const first = await repository.beginGeneration({
			sourceId: DEFAULT_SOURCE_ID,
			queryCache: createQueryProvenance("first"),
		});
		await repository.appendPages({
			sourceId: DEFAULT_SOURCE_ID,
			generation: first.generation,
			pages: [
				encodeEventPage({
					sourceId: DEFAULT_SOURCE_ID,
					generation: first.generation,
					pageId: 0,
					revision: first.revision,
					documents: [createDocument("first-cache", 100, 1)],
					handles: new Uint32Array([1]),
				}),
			],
		});
		await repository.finalizeGeneration(DEFAULT_SOURCE_ID, first.generation);
		const second = await repository.beginGeneration({
			sourceId: DEFAULT_SOURCE_ID,
			queryCache: createQueryProvenance("second"),
		});
		await repository.appendPages({
			sourceId: DEFAULT_SOURCE_ID,
			generation: second.generation,
			pages: [
				encodeEventPage({
					sourceId: DEFAULT_SOURCE_ID,
					generation: second.generation,
					pageId: 0,
					revision: second.revision,
					documents: [createDocument("second-cache", 200, 2)],
					handles: new Uint32Array([1]),
				}),
			],
		});
		await repository.finalizeGeneration(DEFAULT_SOURCE_ID, second.generation);
		const activation = await repository.undoQueryCache(DEFAULT_SOURCE_ID);
		if (!activation) throw new Error("Expected an undo activation.");

		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow(createDescriptor(DEFAULT_SOURCE_ID));
		expect(projection.sources[0]).toMatchObject({
			sourceGeneration: 1,
			sourceAuthorityRevision: activation.summary.authorityRevision,
			loadedCount: 1,
		});
		expect(Array.from(projection.sources[0].representativeHandles)).toEqual([1]);
	});

	it("matches an exact per-pixel oracle across overlapping random pages", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const expectedEvents: ExpectedEvent[] = [];
		const pages: EncodedEventPage[] = [];
		let seed = 0x1234_abcd;
		let nextHandle = 1;
		const random = (): number => {
			seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
			return seed / 0x1_0000_0000;
		};
		for (let pageId = 0; pageId < 4; pageId++) {
			const documents: Doc.Type[] = [];
			const handles: number[] = [];
			for (let index = 0; index < 300; index++) {
				const timestamp = Math.floor(random() * 10_000) / 10;
				const hash = Math.floor(random() * 37);
				const handle = nextHandle++;
				documents.push(
					createDocument(`random-${pageId}-${index}`, timestamp, hash),
				);
				handles.push(handle);
				expectedEvents.push({ handle, timestamp, hash });
			}
			pages.push(createPage(DEFAULT_SOURCE_ID, pageId, documents, handles));
		}
		await persistSource(repository, DEFAULT_SOURCE_ID, pages);

		const descriptor: EventStoreViewportDescriptor = {
			...createDescriptor(DEFAULT_SOURCE_ID),
			frame: { min: 0, max: 1_000 },
			width: 123,
			scale: 1.6,
			scrollX: 23,
			horizontalOverscanPixels: 7,
			sources: [
				{
					...createDescriptor(DEFAULT_SOURCE_ID).sources[0],
					offset: 13,
				},
			],
		};
		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow(descriptor);
		const source = projection.sources[0];

		const firstPixel = -Math.floor(descriptor.horizontalOverscanPixels);
		const lastPixel = Math.ceil(
			descriptor.width + descriptor.horizontalOverscanPixels,
		);
		const virtualWidth = descriptor.width * descriptor.scale;
		const winners = new Map<number, ExpectedEvent>();
		for (const event of expectedEvents) {
			if (
				event.timestamp + 13 < descriptor.frame.min ||
				event.timestamp + 13 > descriptor.frame.max
			) {
				continue;
			}
			const pixel =
				Math.round(
					((event.timestamp + 13 - descriptor.frame.min) /
						(descriptor.frame.max - descriptor.frame.min)) *
						virtualWidth,
				) - descriptor.scrollX;
			if (pixel < firstPixel || pixel > lastPixel) continue;
			const bucket = Math.floor(
				(pixel - firstPixel) / source.bucketPixelWidth,
			);
			const current = winners.get(bucket);
			if (
				!current ||
				event.timestamp > current.timestamp ||
				(event.timestamp === current.timestamp && event.handle < current.handle)
			) {
				winners.set(bucket, event);
			}
		}
		const oracle = [...winners.values()].sort(
			(left, right) =>
				right.timestamp - left.timestamp || left.handle - right.handle,
		);
		expect(Array.from(source.representativeHandles)).toEqual(
			oracle.map((event) => event.handle),
		);
		expect(Array.from(source.representativeTimestamps)).toEqual(
			oracle.map((event) => event.timestamp),
		);
		expect(Array.from(source.representativeHashes)).toEqual(
			oracle.map((event) => event.hash),
		);
	});

	it("uses page, microblock, and exact refinement only when each is required", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const pageSource = "projector-page" as Source.Id;
		const microSource = "projector-micro" as Source.Id;
		const exactSource = "projector-exact" as Source.Id;
		await persistSource(repository, pageSource, [
			createPage(
				pageSource,
				0,
				[
					createDocument("page-a", 100, 1, pageSource),
					createDocument("page-b", 100.1, 2, pageSource),
				],
				[1, 2],
			),
		]);
		const microDocuments = Array.from({ length: 512 }, (_, index) =>
			createDocument(
				`micro-${index}`,
				Math.floor(index / 2) * 10,
				index,
				microSource,
			),
		);
		await persistSource(repository, microSource, [
			createPage(
				microSource,
				0,
				microDocuments,
				microDocuments.map((_, index) => index + 1),
			),
		]);
		const exactDocuments = Array.from({ length: 512 }, (_, index) =>
			createDocument(`exact-${index}`, index, index, exactSource),
		);
		await persistSource(repository, exactSource, [
			createPage(
				exactSource,
				0,
				exactDocuments,
				exactDocuments.map((_, index) => index + 1),
			),
		]);

		const projector = new EventStoreViewportProjector(backend);
		backend.resetReads();
		await projector.requestRenderWindow(createDescriptor(pageSource));
		expect(backend.pageLodReads).toBe(0);
		expect(backend.pageHeightReads).toBe(0);
		expect(backend.exactPageReads).toBe(0);

		backend.resetReads();
		await projector.requestRenderWindow({
			...createDescriptor(microSource),
			frame: { min: -5, max: 2_555 },
			width: 256,
		});
		expect(backend.pageLodReads).toBe(1);
		expect(backend.pageHeightReads).toBe(0);
		expect(backend.exactPageReads).toBe(0);

		backend.resetReads();
		await projector.requestRenderWindow({
			...createDescriptor(exactSource),
			frame: { min: -1, max: 512 },
			width: 2_048,
		});
		expect(backend.pageLodReads).toBe(1);
		expect(backend.pageHeightReads).toBe(0);
		expect(backend.exactPageReads).toBe(1);
	});

	it("aborts a superseded page scan before exact reads and accepts the latest viewport", async () => {
		const backend = new SupersedingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-superseded" as Source.Id;
		let nextHandle = 1;
		const pages = Array.from({ length: 2 }, (_, pageId) => {
			const documents = Array.from({ length: 512 }, (_, index) =>
				createDocument(
					`superseded-${pageId}-${index}`,
					pageId * 512 + index,
					index,
					sourceId,
				),
			);
			const handles = documents.map(() => nextHandle++);
			return createPage(sourceId, pageId, documents, handles);
		});
		await persistSource(repository, sourceId, pages);
		const descriptor: EventStoreViewportDescriptor = {
			...createDescriptor(sourceId),
			frame: { min: -1, max: 1_024 },
			width: 4_096,
		};
		const projector = new EventStoreViewportProjector(backend);
		let isCurrent = true;
		backend.resetReads();
		backend.supersedeAfterFirstLodRead = () => {
			isCurrent = false;
		};

		let supersededError: unknown;
		try {
			await projector.requestRenderWindow(descriptor, () => isCurrent);
		} catch (error) {
			supersededError = error;
		}
		expect(supersededError instanceof StaleEventStoreProjectionError).toBe(true);
		expect(backend.pageLodReads).toBe(1);
		expect(backend.exactPageReads).toBe(0);

		isCurrent = true;
		backend.resetReads();
		const latest = await projector.requestRenderWindow(
			{ ...descriptor, viewportRevision: 8, projectionRevision: 10 },
			() => isCurrent,
		);
		expect(latest.viewportRevision).toBe(8);
		expect(latest.sources[0].representativeHandles.length > 0).toBe(true);
		expect(backend.pageLodReads).toBe(2);
		expect(backend.exactPageReads).toBe(2);
	});

	it("counts the complete source-adjusted frame and caches partial-page reads across scrolls", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-frame-count" as Source.Id;
		const partialPage = createPage(
			sourceId,
			0,
			[
				createDocument("partial-800", 800, 1, sourceId),
				createDocument("partial-tombstone", 700, 2, sourceId),
				createDocument("partial-600", 600, 3, sourceId),
			],
			[1, 2, 3],
		);
		const tombstoneIndex = Array.from(partialPage.timestamps).indexOf(700);
		partialPage.tombstones[tombstoneIndex] = 1;
		partialPage.activeCount--;
		const containedPage = createPage(
			sourceId,
			1,
			[
				createDocument("contained-700", 700, 4, sourceId),
				createDocument("contained-680", 680, 5, sourceId),
			],
			[4, 5],
		);
		await persistSource(repository, sourceId, [partialPage, containedPage]);
		const projector = new EventStoreViewportProjector(backend);
		const descriptor: EventStoreViewportDescriptor = {
			...createDescriptor(sourceId),
			frame: { min: 700, max: 900 },
			width: 100,
			sources: [
				{
					...createDescriptor(sourceId).sources[0],
					offset: 50,
					engine: "height",
				},
			],
		};

		backend.resetReads();
		const first = await projector.requestRenderWindow(descriptor);
		expect(first.sources[0]).toMatchObject({
			loadedCount: 4,
			frameEventCount: 3,
		});
		expect(backend.exactPageReads).toBe(1);

		backend.resetReads();
		const scrolled = await projector.requestRenderWindow({
			...descriptor,
			viewportRevision: descriptor.viewportRevision + 1,
			projectionRevision: descriptor.projectionRevision + 1,
			scrollX: 25,
		});
		expect(scrolled.sources[0].frameEventCount).toBe(3);
		expect(backend.exactPageReads).toBe(0);
	});

	it("publishes a coherent building snapshot after a newer same-generation commit", async () => {
		const backend = new SupersedingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-building-advance" as Source.Id;
		await repository.beginGeneration({ sourceId, generation: 1 });
		const firstSummary = await repository.appendPages({
			sourceId,
			generation: 1,
			pages: [
				createPage(
					sourceId,
					0,
					[createDocument("building-first", 100, 1, sourceId)],
					[1],
				),
			],
		});
		backend.resetReads();
		backend.commitAfterInitialManifestRead = async () => {
			await repository.appendPages({
				sourceId,
				generation: 1,
				pages: [
					createPage(
						sourceId,
						1,
						[createDocument("building-second", 200, 2, sourceId)],
						[2],
					),
				],
			});
		};

		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow({
			...createDescriptor(sourceId),
			width: 4_096,
		});
		const currentSummary = await repository.getSourceSummary(sourceId);
		expect(projection.sources[0]).toMatchObject({
			sourceGeneration: 1,
			sourceRevision: firstSummary.revision,
			sourceStatus: "building",
			loadedCount: 1,
		});
		expect((currentSummary?.revision ?? 0) > firstSummary.revision).toBe(true);
	});

	it("never projects overscan events outside the selected timeline frame", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await persistSource(repository, DEFAULT_SOURCE_ID, [
			createPage(
				DEFAULT_SOURCE_ID,
				0,
				[
					createDocument("before-frame", -1, 1),
					createDocument("inside-frame", 500, 2),
					createDocument("after-frame", 1_001, 3),
				],
				[1, 2, 3],
			),
		]);
		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow({
			...createDescriptor(DEFAULT_SOURCE_ID),
			horizontalOverscanPixels: 20,
		});
		expect(Array.from(projection.sources[0].representativeHandles)).toEqual([2]);
	});

	it("rejects an LOD page that does not match the manifest revision fence", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		await persistSource(repository, DEFAULT_SOURCE_ID, [
			createPage(
				DEFAULT_SOURCE_ID,
				0,
				[
					createDocument("fence-new", 900, 1),
					createDocument("fence-old", 100, 2),
				],
				[1, 2],
			),
		]);
		const key: IDBValidKey = [
			DEFAULT_SOURCE_ID,
			1,
			0,
			EVENT_PAGE_LOD_RECORD_KIND,
		];
		const lod = await backend.get<EncodedEventPageLod>("page_lod", key);
		if (!lod) throw new Error("Missing test LOD page.");
		lod.revision--;
		await backend.commit([{ type: "put", store: "page_lod", key, value: lod }]);

		await expect(
			new EventStoreViewportProjector(backend).requestRenderWindow(
				createDescriptor(DEFAULT_SOURCE_ID),
			),
		).rejects.toThrow("changed during projection");
	});

	it("prepares override LOD progressively before publishing its revision", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const pages = [
			createPage(
				DEFAULT_SOURCE_ID,
				0,
				[
					createDocument("newest", 109, 1),
					createDocument("newest-override", 108, 9),
				],
				[1, 2],
			),
			createPage(
				DEFAULT_SOURCE_ID,
				1,
				[createDocument("middle", 107, 1)],
				[3],
			),
			createPage(
				DEFAULT_SOURCE_ID,
				2,
				[createDocument("old-override", 106, 9)],
				[4],
			),
		];
		const sourceRevision = await persistSource(
			repository,
			DEFAULT_SOURCE_ID,
			pages,
		);
		const projector = new EventStoreViewportProjector(backend);
		const descriptor = createDescriptor(DEFAULT_SOURCE_ID, [9]);

		await expect(projector.requestRenderWindow(descriptor)).rejects.toThrow(
			"Override revision 3 is not prepared",
		);
		backend.resetReads();
		const first = await projector.prepareOverrideRevision({
			sourceId: DEFAULT_SOURCE_ID,
			sourceRevision,
			overrideRevision: 3,
			overrideHashes: [9],
			maximumPages: 2,
		});
		expect(first.complete).toBe(false);
		expect(first.processedPages).toBe(2);
		expect(backend.exactPageReads).toBe(2);
		await expect(projector.requestRenderWindow(descriptor)).rejects.toThrow(
			"Override revision 3 is not prepared",
		);

		const second = await projector.prepareOverrideRevision({
			sourceId: DEFAULT_SOURCE_ID,
			sourceRevision,
			overrideRevision: 3,
			overrideHashes: [9],
			startPageIndex: first.nextPageIndex,
			maximumPages: 2,
		});
		expect(second.complete).toBe(true);
		const projection = await projector.requestRenderWindow(descriptor);
		const source = projection.sources[0];
		expect(Array.from(source.representativeHandles)).toEqual([2]);
		expect(Array.from(source.representativeHashes)).toEqual([9]);
		for (const view of [
			source.representativeHandles,
			source.representativeTimestamps,
			source.representativeHashes,
			source.heightTimestamps,
			source.heightCounts,
			source.graphTimestamps,
			source.graphCounts,
		]) {
			expectExactBuffer(view);
		}
	});

	it("reads persistent bounded height counts and keeps graph samples ascending", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-aggregate" as Source.Id;
		const timestamps = [0, 1, 60_000, 60_001, 120_000];
		await persistSource(repository, sourceId, [
			createPage(
				sourceId,
				0,
				timestamps.map((timestamp, index) =>
					createDocument(`aggregate-${index}`, timestamp, index, sourceId),
				),
				timestamps.map((_, index) => index + 1),
			),
		]);
		const projector = new EventStoreViewportProjector(backend);
		const base = createDescriptor(sourceId);
		backend.resetReads();
		const height = await projector.requestRenderWindow({
			...base,
			frame: { min: -1_000, max: 121_000 },
			width: 122,
			sources: [{ ...base.sources[0], engine: "height" }],
		});
		expect(Array.from(height.sources[0].heightTimestamps)).toEqual([
			0,
			60_000,
			120_000,
		]);
		expect(Array.from(height.sources[0].heightCounts)).toEqual([2, 2, 1]);
		expect(backend.exactPageReads).toBe(0);
		expect(backend.pageLodReads).toBe(0);
		expect(backend.pageHeightReads).toBe(2);

		const graph = await projector.requestRenderWindow({
			...base,
			frame: { min: -1_000, max: 121_000 },
			width: 122,
			sources: [
				{ ...base.sources[0], engine: "graph", frequency: 60_000 },
			],
		});
		expect(Array.from(graph.sources[0].graphTimestamps)).toEqual([
			0,
			60_000,
			120_000,
		]);
		expect(Array.from(graph.sources[0].graphCounts)).toEqual([2, 2, 1]);
	});

	it("keeps height normalization tied to an outlier outside the viewport", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-height-global" as Source.Id;
		const documents = [
			createDocument("visible-a", 0, 1, sourceId),
			createDocument("visible-b", 1, 2, sourceId),
			...Array.from({ length: 9 }, (_, index) =>
				createDocument(`outlier-${index}`, 600_000 + index, 10 + index, sourceId),
			),
		];
		await persistSource(repository, sourceId, [
			createPage(
				sourceId,
				0,
				documents,
				documents.map((_, index) => index + 1),
			),
		]);
		const descriptor = createDescriptor(sourceId);
		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow({
			...descriptor,
			frame: { min: 0, max: 120_000 },
			width: 120,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});

		expect(Array.from(projection.sources[0].heightCounts)).toEqual([2]);
		expect(projection.sources[0].heightMaximumCount).toBe(9);
	});

	it("bounds global height memory when exact page ranges fully overlap", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-height-overlap" as Source.Id;
		const distantTimestamp =
			(EVENT_HEIGHT_MAXIMUM_WORKING_BUCKET_LIMIT + 10) * 60_000;
		await persistSource(repository, sourceId, [
			createPage(
				sourceId,
				0,
				[
					createDocument("overlap-a-min", 0, 1, sourceId),
					createDocument("overlap-a-max", distantTimestamp, 2, sourceId),
				],
				[1, 2],
			),
			createPage(
				sourceId,
				1,
				[
					createDocument("overlap-b-min", 0, 3, sourceId),
					createDocument("overlap-b-max", distantTimestamp, 4, sourceId),
				],
				[3, 4],
			),
		]);
		const descriptor = createDescriptor(sourceId);
		const projector = new EventStoreViewportProjector(backend);
		const projection = await projector.requestRenderWindow({
			...descriptor,
			frame: { min: 0, max: 120_000 },
			width: 120,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});

		expect(projection.sources[0].heightMaximumCount).toBe(2);
		expect(projector.getDiagnostics().maximumHeightWorkingBuckets).toBe(
			EVENT_HEIGHT_MAXIMUM_WORKING_BUCKET_LIMIT,
		);
	});

	it("avoids source-wide height rescans while a generation is building", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-height-building" as Source.Id;
		await repository.beginGeneration({ sourceId, generation: 1 });
		await repository.appendPages({
			sourceId,
			generation: 1,
			pages: [
				createPage(
					sourceId,
					0,
					[
						createDocument("building-visible-a", 0, 1, sourceId),
						createDocument("building-visible-b", 1, 2, sourceId),
					],
					[1, 2],
				),
				createPage(
					sourceId,
					1,
					Array.from({ length: 9 }, (_, index) =>
						createDocument(
							`building-outlier-${index}`,
							600_000 + index,
							10 + index,
							sourceId,
						),
					),
					Array.from({ length: 9 }, (_, index) => index + 3),
				),
			],
		});
		const descriptor = createDescriptor(sourceId);
		const projector = new EventStoreViewportProjector(backend);
		backend.resetReads();
		const buildingProjection = await projector.requestRenderWindow({
			...descriptor,
			frame: { min: 0, max: 120_000 },
			width: 120,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});
		expect(buildingProjection.sources[0].heightMaximumCount).toBe(2);
		expect(backend.pageHeightReads).toBe(1);

		await repository.finalizeGeneration(sourceId, 1);
		backend.resetReads();
		const completeProjection = await projector.requestRenderWindow({
			...descriptor,
			frame: { min: 0, max: 120_000 },
			width: 120,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});
		expect(completeProjection.sources[0].heightMaximumCount).toBe(9);
		expect(backend.pageHeightReads).toBe(1);
	});

	it("extends a cached height maximum from chronological append pages only", async () => {
		const backend = new CountingMemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-height-append" as Source.Id;
		await persistSource(repository, sourceId, [
			createPage(
				sourceId,
				0,
				[
					createDocument("append-old-a", 0, 1, sourceId),
					createDocument("append-old-b", 60_000, 2, sourceId),
				],
				[1, 2],
			),
		]);
		const descriptor = createDescriptor(sourceId);
		const projector = new EventStoreViewportProjector(backend);
		await projector.requestRenderWindow({
			...descriptor,
			frame: { min: 500_000, max: 600_000 },
			width: 100,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});
		await repository.upsertDocuments({
			documents: [
				createDocument("append-new-a", 120_000, 3, sourceId),
				createDocument("append-new-b", 180_000, 4, sourceId),
			],
			allowedSourceIds: [sourceId],
		});
		backend.resetReads();
		const projection = await projector.requestRenderWindow({
			...descriptor,
			frame: { min: 500_000, max: 600_000 },
			width: 100,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});

		expect(projection.sources[0].heightMaximumCount).toBe(1);
		expect(backend.pageHeightReads).toBe(1);
	});

	it("aggregates graph samples directly on non-aligned ten-pixel boundaries", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-graph-boundary" as Source.Id;
		const documents = [
			createDocument("anchor", 0, 1, sourceId),
			createDocument("left", 9, 2, sourceId),
			createDocument("right", 11, 3, sourceId),
		];
		await persistSource(repository, sourceId, [
			createPage(sourceId, 0, documents, [1, 2, 3]),
		]);
		const descriptor = createDescriptor(sourceId);
		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow({
			...descriptor,
			frame: { min: 0, max: 100 },
			width: 100,
			sources: [
				{ ...descriptor.sources[0], engine: "graph", frequency: 6 },
			],
		});

		expect(Array.from(projection.sources[0].graphTimestamps)).toEqual([0, 10]);
		expect(Array.from(projection.sources[0].graphCounts)).toEqual([2, 1]);
	});

	it("preserves the legacy source-minimum anchor for height buckets", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceId = "projector-height-anchor" as Source.Id;
		const timestamps = [1_000, 60_000, 61_000, 62_000];
		await persistSource(repository, sourceId, [
			createPage(
				sourceId,
				0,
				timestamps.map((timestamp, index) =>
					createDocument(`height-anchor-${index}`, timestamp, index, sourceId),
				),
				timestamps.map((_, index) => index + 1),
			),
		]);
		const descriptor = createDescriptor(sourceId);
		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow({
			...descriptor,
			frame: { min: 1_000, max: 63_000 },
			width: 124,
			sources: [{ ...descriptor.sources[0], engine: "height" }],
		});
		expect(Array.from(projection.sources[0].heightTimestamps)).toEqual([
			1_000,
			61_000,
		]);
		expect(Array.from(projection.sources[0].heightCounts)).toEqual([2, 2]);
	});

	it("enforces one global power-of-two record cap across all engines", async () => {
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const sourceIds = [
			"cap-default" as Source.Id,
			"cap-height" as Source.Id,
			"cap-graph" as Source.Id,
		];
		for (const sourceId of sourceIds) {
			const documents = Array.from({ length: 300 }, (_, index) =>
				createDocument(`cap-${sourceId}-${index}`, index * 2_000, index, sourceId),
			);
			await persistSource(repository, sourceId, [
				createPage(
					sourceId,
					0,
					documents,
					documents.map((_, index) => index + 1),
				),
			]);
		}
		const descriptor = createDescriptor(sourceIds[0]);
		const projection = await new EventStoreViewportProjector(
			backend,
		).requestRenderWindow({
			...descriptor,
			frame: { min: 0, max: 600_000 },
			width: 100,
			maxRepresentatives: 30,
			sources: [
				{ ...descriptor.sources[0], sourceId: sourceIds[0] },
				{
					...descriptor.sources[0],
					sourceId: sourceIds[1],
					engine: "height",
				},
				{
					...descriptor.sources[0],
					sourceId: sourceIds[2],
					engine: "graph",
					frequency: 1,
				},
			],
		});
		const totalRecords = projection.sources.reduce(
			(total, source) =>
				total +
				source.representativeHandles.length +
				source.heightTimestamps.length +
				source.graphTimestamps.length,
			0,
		);
		expect(totalRecords <= 30).toBe(true);
		for (const source of projection.sources) {
			expect(source.degradedLod).toBe(true);
			expect(source.bucketPixelWidth > 1).toBe(true);
			expect(
				Number.isInteger(Math.log2(source.bucketPixelWidth)),
			).toBe(true);
		}
	});
});
