import type { Source } from "@/entities/Source";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import type {
	EventSourceHeadManifest,
	EventSourceSummary,
	EventStoreNamespace,
} from "@/event-store/EventStore.types";
import { QueryPersistenceCoordinator } from "@/event-store-worker/QueryPersistenceCoordinator";
import { isCanvasRenderWindowSourceAcceptable } from "@/app/body/CanvasRenderWindowRetry";
import type {
	DocumentChunkReady,
	DocumentStreamDescriptor,
	QueryProjectionInvalidationEvent,
} from "@/event-stream";
import { TextDecoder, TextEncoder } from "util";

Object.defineProperties(globalThis, {
	TextEncoder: { configurable: true, value: TextEncoder },
	TextDecoder: { configurable: true, value: TextDecoder },
});

const SOURCE_COUNT = 70;
const TIMESTAMP_BASE = 1_700_000_000_000;

(jest as unknown as { setTimeout(milliseconds: number): void }).setTimeout(30_000);

/** Creates the multi-source operation shape reported by the progressive-render issue. */
function createSourceIds(): Source.Id[] {
	return Array.from(
		{ length: SOURCE_COUNT },
		(_, index) => `progressive-source-${index}` as Source.Id,
	);
}

/** Creates one parser-normalized replacement chunk with one event per source. */
function createChunk(
	descriptor: DocumentStreamDescriptor,
	sourceIds: readonly Source.Id[],
	sequence: number,
	last: boolean,
): DocumentChunkReady {
	return {
		descriptor,
		sequence,
		rawByteLength: 0,
		message: {
			type: "docs_chunk",
			req_id: descriptor.requestId,
			operation_id: descriptor.operationId,
			payload: {
				docs: sourceIds.map((sourceId, sourceIndex) => ({
					_id: `progressive-${sequence}-${sourceIndex}`,
					"gulp.source_id": sourceId,
					"gulp.timestamp":
						TIMESTAMP_BASE + sequence * SOURCE_COUNT + sourceIndex,
					"gulp.event_code": sourceIndex % 11,
					"event.sequence": sequence,
				})),
				last,
				chunk_num: sequence,
			},
		},
	};
}

/** Waits for a worker-side progressive invalidation without polling storage. */
async function waitForInvalidations(
	events: readonly QueryProjectionInvalidationEvent[],
	expected: number,
): Promise<void> {
	const deadline = Date.now() + 5_000;
	while (events.length < expected) {
		if (Date.now() >= deadline) {
			throw new Error("Timed out waiting for projection invalidation.");
		}
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
}

describe("progressive replacement rendering integration", () => {
	it("projects each partial chunk for seventy sources before dense final pages commit", async () => {
		const sourceIds = createSourceIds();
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const invalidations: QueryProjectionInvalidationEvent[] = [];
		const failures: string[] = [];
		const coordinator = new QueryPersistenceCoordinator(
			"progressive-rendering-test",
			{
				onSourceSummary: () => undefined,
				onIngestionProgress: () => undefined,
				onProjectionInvalidation: (event) => invalidations.push(event),
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
			requestId: "progressive-query",
			operationId: "progressive-operation",
			operationEpoch: 1,
			owner: "persist",
			kind: "replace",
			completionMode: "explicit",
			sourceIds,
			defaultSourceSettings: {
				field: "gulp.event_code",
				hash_function: "fnv1a",
			},
		};
		await coordinator.register(descriptor);

		coordinator.handleDocumentChunk(createChunk(descriptor, sourceIds, 0, false));
		await waitForInvalidations(invalidations, 1);
		expect(new Set(invalidations[0].sourceIds)).toEqual(new Set(sourceIds));
		const durableBeforeCompletion = await backend.getMany<EventSourceHeadManifest>(
			"sources",
			sourceIds,
		);
		expect(
			durableBeforeCompletion.every(
				(manifest) => manifest?.eventCount === 0 && manifest.pages.length === 0,
			),
		).toBe(true);

		/**
		 * Builds the full 70-row viewport used for each progressive projection.
		 * @param projectionRevision Monotonic render replacement revision.
		 * @returns Viewport descriptor covering all synthetic events.
		 */
		const createViewport = (projectionRevision: number) => ({
			operationId: descriptor.operationId,
			operationEpoch: descriptor.operationEpoch,
			viewportRevision: 1,
			projectionRevision,
			frame: {
				min: TIMESTAMP_BASE,
				max: TIMESTAMP_BASE + SOURCE_COUNT * 3,
			},
			width: 256,
			scale: 1,
			scrollX: 0,
			horizontalOverscanPixels: 0,
			sources: sourceIds.map((sourceId) => ({
				sourceId,
				offset: 0,
				engine: "default" as const,
				frequency: 0,
				overrideHashes: [],
				settingsRevision: 1,
				overrideRevision: 0,
			})),
		});
		const first = await coordinator.requestRenderWindow(createViewport(1));
		expect(first?.sources).toHaveLength(SOURCE_COUNT);
		expect(first?.sources.every((source) => source.loadedCount === 1)).toBe(true);
		expect(
			first?.sources.every(
				(source) => source.representativeHandles.length === 1,
			),
		).toBe(true);

		coordinator.handleDocumentChunk(createChunk(descriptor, sourceIds, 1, true));
		await waitForInvalidations(invalidations, 2);
		expect(new Set(invalidations[1].sourceIds)).toEqual(new Set(sourceIds));
		const second = await coordinator.requestRenderWindow(createViewport(2));
		expect(second?.sources.every((source) => source.loadedCount === 2)).toBe(true);

		await coordinator.complete(descriptor.requestId);
		const durableAfterCompletion = await backend.getMany<EventSourceHeadManifest>(
			"sources",
			sourceIds,
		);
		expect(failures).toEqual([]);
		expect(
			durableAfterCompletion.every(
				(manifest) =>
					manifest?.status === "complete" &&
					manifest.eventCount === 2 &&
					manifest.pages.length === 1 &&
					manifest.pages[0].count === 2,
			),
		).toBe(true);
		coordinator.destroy();
	});

	it("accepts a partial startup projection before the remaining source queries register", async () => {
		const sourceIds = createSourceIds();
		const backend = new MemoryEventStoreBackend();
		const repository = new EventStoreRepository(backend);
		const invalidations: QueryProjectionInvalidationEvent[] = [];
		const completed: string[] = [];
		const failures: string[] = [];
		const summaries = new Map<Source.Id, EventSourceSummary>();
		const coordinator = new QueryPersistenceCoordinator(
			"progressive-startup-test",
			{
				onSourceSummary: ({ summary }) =>
					summaries.set(summary.sourceId, summary),
				onIngestionProgress: () => undefined,
				onProjectionInvalidation: (event) => invalidations.push(event),
				onStreamFailure: (event) => failures.push(event.reason),
				onStreamFlushing: () => undefined,
				onStreamCompleted: (requestId) => completed.push(requestId),
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
		const descriptors = sourceIds.map<DocumentStreamDescriptor>(
			(sourceId, index) => ({
				requestId: `startup-query-${index}`,
				operationId: "progressive-startup-operation",
				operationEpoch: 1,
				owner: "persist",
				kind: "replace",
				completionMode: "last",
				sourceIds: [sourceId],
				defaultSourceSettings: {
					field: "gulp.event_code",
					hash_function: "fnv1a",
				},
			}),
		);
		await Promise.all(
			descriptors
				.slice(0, 4)
				.map((descriptor) => coordinator.register(descriptor)),
		);

		const appendPages = repository.appendPages.bind(repository);
		let releaseFirstCommit = (): void => undefined;
		const firstCommitGate = new Promise<void>((resolve) => {
			releaseFirstCommit = resolve;
		});
		let blockedSourceId: Source.Id | undefined;
		repository.appendPages = async (input) => {
			if (!blockedSourceId) {
				blockedSourceId = input.sourceId;
				await firstCommitGate;
			}
			return appendPages(input);
		};

		for (let index = 0; index < 4; index++) {
			const descriptor = descriptors[index];
			coordinator.handleDocumentChunk(
				createChunk(descriptor, [sourceIds[index]], 0, true),
			);
		}
		await waitForInvalidations(invalidations, 4);
		const appendDeadline = Date.now() + 5_000;
		while (!blockedSourceId) {
			if (Date.now() >= appendDeadline) {
				throw new Error("Timed out waiting for the first startup cache commit.");
			}
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
		}
		const blockedIndex = sourceIds.indexOf(blockedSourceId);
		const durableDuringCommit = await backend.get<EventSourceHeadManifest>(
			"sources",
			blockedSourceId,
		);
		expect(durableDuringCommit).toMatchObject({
			status: "building",
			eventCount: 0,
			pages: [],
		});

		let startupProjection:
			| Awaited<ReturnType<typeof coordinator.requestRenderWindow>>
			| undefined;
		const startupProjectionPromise = coordinator
			.requestRenderWindow({
				operationId: "progressive-startup-operation",
				operationEpoch: 1,
				viewportRevision: 1,
				projectionRevision: 1,
				frame: {
					min: TIMESTAMP_BASE,
					max: TIMESTAMP_BASE + SOURCE_COUNT,
				},
				width: 256,
				scale: 1,
				scrollX: 0,
				horizontalOverscanPixels: 0,
				sources: sourceIds.map((sourceId) => ({
					sourceId,
					offset: 0,
					engine: "default" as const,
					frequency: 0,
					overrideHashes: [],
					settingsRevision: 1,
					overrideRevision: 0,
				})),
			})
			.then((projection) => {
				startupProjection = projection;
				return projection;
			});
		const projectionDeadline = Date.now() + 5_000;
		while (startupProjection === undefined) {
			if (Date.now() >= projectionDeadline) {
				throw new Error(
					"Startup viewport rejected its partial registered-source projection.",
				);
			}
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
		}
		expect(startupProjection?.sources).toHaveLength(SOURCE_COUNT);
		expect(startupProjection?.sources[blockedIndex].loadedCount).toBe(1);
		expect(
			startupProjection?.sources.every((source) =>
				isCanvasRenderWindowSourceAcceptable(
					source,
					summaries.get(source.sourceId as Source.Id),
					undefined,
				),
			),
		).toBe(true);
		expect(failures).toEqual([]);

		releaseFirstCommit();
		await startupProjectionPromise;
		const activeRequestIds = new Set(
			descriptors.slice(0, 4).map((descriptor) => descriptor.requestId),
		);
		const completionDeadline = Date.now() + 5_000;
		while (
			completed.filter((requestId) => activeRequestIds.has(requestId)).length < 4
		) {
			if (Date.now() >= completionDeadline) {
				throw new Error(
					"Timed out waiting for startup source queries to complete.",
				);
			}
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
		}
		coordinator.destroy();
	});
});
