import { encodeEventPage } from "@/event-store/EventPage.codec";
import {
	EventStoreCapacityTelemetryUnavailableError,
	EventStoreCapacityTracker,
} from "@/event-store/EventStore.capacity";
import type { EventStoreBackend } from "@/event-store/EventStore.backend";
import { EventStoreRevisionBus } from "@/event-store/EventStore.revisions";
import {
	EventStoreOverrideProjectionPendingError,
	EventStoreViewportProjector,
	StaleEventStoreProjectionError,
	type EventStoreOverridePreparationDescriptor,
	type EventStoreOverridePreparationResult,
	type EventStoreViewportDescriptor,
} from "@/event-store/EventStore.viewport-projector";
import {
	EventStoreRepository,
	type AppendEventStorePagesInput,
	type BeginEventStoreGenerationInput,
} from "@/event-store/EventStore.repository";
import {
	EVENT_MAXIMUM_SOURCE_HANDLE,
	EVENT_PAGE_SIZE,
	EVENT_PENDING_DOCUMENT_LIMIT,
	type EventSourceManifest,
	type EventSourceSummary,
	type EventStoreCapacity,
	type EventStoreNamespace,
} from "@/event-store/EventStore.types";
import { IndexedDbEventStoreBackend } from "@/event-store/IndexedDbEventStore.backend";
import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type {
	DocumentChunkReady,
	DocumentStreamDescriptor,
	QueryCapacityEvent,
	QueryRenderWindowDescriptor,
	QuerySourceSummaryEvent,
	QueryStreamFailureEvent,
	QueryStreamFailureReason,
} from "@/event-stream";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import { normalizeQueryDocumentsCooperatively } from "./QueryDocument.normalizer";

interface UpsertDocumentsInput {
	documents: readonly Doc.Type[];
	allowedSourceIds: readonly Source.Id[];
}

interface QueryPersistenceRepository {
	beginGeneration(
		input: BeginEventStoreGenerationInput,
	): Promise<EventSourceSummary>;
	appendPages(input: AppendEventStorePagesInput): Promise<EventSourceSummary>;
	finalizeGeneration(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceSummary>;
	discardGenerationRollback(
		sourceId: Source.Id,
		generation: number,
	): Promise<EventSourceSummary>;
	cancelGeneration(sourceId: Source.Id, generation: number): Promise<void>;
	clearSource(sourceId: Source.Id): Promise<void>;
	upsertDocuments?: (
		input: UpsertDocumentsInput,
	) => Promise<EventSourceSummary[]>;
	markGenerationIncomplete?: (
		sourceId: Source.Id,
		generation: number,
	) => Promise<EventSourceSummary>;
	close(): void;
}

export interface QueryPersistenceOperationStore {
	backend: EventStoreBackend;
	repository: QueryPersistenceRepository;
}

interface QueryPersistenceRevisionBus {
	publish(summary: EventSourceSummary): void;
	close(): void;
}

interface QueryViewportProjector {
	requestRenderWindow(
		descriptor: EventStoreViewportDescriptor,
		isCurrent?: () => boolean,
	): Promise<RenderWindowProjection>;
	prepareOverrideRevision?(
		descriptor: EventStoreOverridePreparationDescriptor,
	): Promise<EventStoreOverridePreparationResult>;
}

export interface QueryPersistenceCallbacks {
	onSourceSummary: (event: QuerySourceSummaryEvent) => void;
	onCapacity: (event: QueryCapacityEvent) => void;
	onStreamFailure: (event: QueryStreamFailureEvent) => void;
	onStreamFlushing: (requestId: string) => void;
	onStreamCompleted: (requestId: string) => void;
}

export interface QueryPersistenceCoordinatorOptions {
	createOperationStore?: (
		namespace: EventStoreNamespace,
	) => QueryPersistenceOperationStore;
	createRevisionBus?: (
		namespace: EventStoreNamespace,
	) => QueryPersistenceRevisionBus;
	createViewportProjector?: (backend: EventStoreBackend) => QueryViewportProjector;
	capacityTracker?: EventStoreCapacityTracker;
	storageManager?: Pick<StorageManager, "estimate">;
	lockManager?: Pick<LockManager, "request">;
	pendingDocumentLimit?: number;
	maximumConcurrentCommits?: number;
	now?: () => number;
}

interface OperationPersistenceContext {
	operationId: string;
	backend: EventStoreBackend;
	repository: QueryPersistenceRepository;
	revisionBus: QueryPersistenceRevisionBus;
	projector: QueryViewportProjector;
	manifests: Map<Source.Id, StoredEventSourceManifest>;
}

interface StoredEventSourceManifest extends EventSourceManifest {
	rollback?: EventSourceManifest;
	inPlaceReplacement?: true;
}

interface ReplaceSourceContext {
	sourceId: Source.Id;
	generation: number;
	nextHandle: number;
	nextPageId: number;
	revision: number;
	pendingDocuments: Doc.Type[];
}

interface PersistenceStreamContext {
	descriptor: DocumentStreamDescriptor;
	operation: OperationPersistenceContext;
	sourceIds: Source.Id[];
	hasDeclaredSourceAuthority: boolean;
	replaceSources: Map<Source.Id, ReplaceSourceContext>;
	releaseSourceLeases: Array<() => void>;
	chain: Promise<void>;
	reservedEvents: number;
	unallocatedExpectedEvents: number;
	reclaimableEventsBySource: Map<Source.Id, number>;
	inPlaceReplacement: boolean;
	inPlaceNotificationPublished: boolean;
	accepting: boolean;
	canceling: boolean;
	explicitCompletionRequested: boolean;
	lastChunkCommitted: boolean;
	completionWaiters: Array<() => void>;
	failed: boolean;
	completed: boolean;
	released: boolean;
	cleanupPromise: Promise<void> | null;
}

interface PendingDocumentCreditWaiter {
	context: PersistenceStreamContext;
	documentCount: number;
	resolve: (acquired: boolean) => void;
}

class QueryPersistenceError extends Error {
	public readonly reason: QueryStreamFailureReason;

	/**
	 * Creates one typed persistence failure.
	 * @param reason Stable reason consumed by main-thread cancellation logic.
	 * @param message Human-readable failure detail.
	 */
	public constructor(reason: QueryStreamFailureReason, message: string) {
		super(message);
		this.name = "QueryPersistenceError";
		this.reason = reason;
	}
}

/**
 * Persists document streams without retaining source-wide object collections or
 * sending document payloads back to the main thread.
 */
export class QueryPersistenceCoordinator {
	private readonly coordinatorId: string;
	private readonly callbacks: QueryPersistenceCallbacks;
	private readonly createOperationStore: (
		namespace: EventStoreNamespace,
	) => QueryPersistenceOperationStore;
	private readonly createRevisionBus: (
		namespace: EventStoreNamespace,
	) => QueryPersistenceRevisionBus;
	private readonly createViewportProjector: (
		backend: EventStoreBackend,
	) => QueryViewportProjector;
	private readonly capacityTracker: EventStoreCapacityTracker;
	private readonly storageManager?: Pick<StorageManager, "estimate">;
	private readonly lockManager?: Pick<LockManager, "request">;
	private readonly pendingDocumentLimit: number;
	private readonly maximumConcurrentCommits: number;
	private readonly now: () => number;
	private readonly operationPromises = new Map<
		string,
		Promise<OperationPersistenceContext>
	>();
	private readonly operationReleasePromises = new Map<string, Promise<boolean>>();
	private readonly releasingOperations = new Set<string>();
	private readonly pendingOperationRegistrations = new Map<string, number>();
	private readonly operationRegistrationWaiters = new Map<
		string,
		Array<() => void>
	>();
	private readonly operations = new Map<string, OperationPersistenceContext>();
	private readonly streams = new Map<string, PersistenceStreamContext>();
	private readonly sourceLockTails = new Map<string, Promise<void>>();
	private readonly commitWaiters: Array<() => void> = [];
	private readonly pendingDocumentCreditWaiters: PendingDocumentCreditWaiter[] = [];
	private currentPendingDocuments = 0;
	private currentPageBuilderDocuments = 0;
	private currentPendingRawChunkBytes = 0;
	private currentCommitCount = 0;
	private latestCapacity: EventStoreCapacity | null = null;
	private capacityEstimatePromise: Promise<EventStoreCapacity> | null = null;
	private remainingCapacityEvents = 0;
	private totalReservedEvents = 0;
	private destroyed = false;

	/**
	 * Creates a coordinator for one cross-window EventStore namespace.
	 * @param coordinatorId Main/detached browsing-context coordinator identity.
	 * @param callbacks Summary, capacity, failure, and router lifecycle hooks.
	 * @param options Injectable storage and deterministic limits for tests.
	 */
	public constructor(
		coordinatorId: string,
		callbacks: QueryPersistenceCallbacks,
		options: QueryPersistenceCoordinatorOptions = {},
	) {
		if (coordinatorId.length === 0) {
			throw new TypeError("Query persistence coordinatorId cannot be empty.");
		}
		this.coordinatorId = coordinatorId;
		this.callbacks = callbacks;
		this.capacityTracker = options.capacityTracker ?? new EventStoreCapacityTracker();
		this.storageManager = options.storageManager;
		this.lockManager = options.lockManager ?? globalThis.navigator?.locks;
		this.pendingDocumentLimit =
			options.pendingDocumentLimit ?? EVENT_PENDING_DOCUMENT_LIMIT;
		this.maximumConcurrentCommits = options.maximumConcurrentCommits ?? 2;
		this.now = options.now ?? Date.now;
		this.createOperationStore =
			options.createOperationStore ??
			((namespace) => {
				const backend = new IndexedDbEventStoreBackend(namespace);
				return {
					backend,
					repository: new EventStoreRepository(
						backend,
					) as QueryPersistenceRepository,
				};
			});
		this.createRevisionBus =
			options.createRevisionBus ??
			((namespace) => new EventStoreRevisionBus(namespace, () => undefined));
		this.createViewportProjector =
			options.createViewportProjector ??
			((backend) => new EventStoreViewportProjector(backend));

		if (
			!Number.isSafeInteger(this.pendingDocumentLimit) ||
			this.pendingDocumentLimit <= 0
		) {
			throw new RangeError("Pending document limit must be a positive integer.");
		}
		if (
			!Number.isSafeInteger(this.maximumConcurrentCommits) ||
			this.maximumConcurrentCommits <= 0
		) {
			throw new RangeError("Concurrent commit limit must be a positive integer.");
		}
	}

	/** Returns the number of normalized documents retained by queued stream work. */
	public get pendingDocumentCount(): number {
		return this.currentPendingDocuments + this.currentPageBuilderDocuments;
	}

	/**
	 * Returns raw WebSocket bytes retained while chunks await normalization.
	 * @returns Current temporary raw frame byte count.
	 */
	public get pendingRawChunkByteCount(): number {
		return this.currentPendingRawChunkBytes;
	}

	/** Returns the number of storage commits currently executing. */
	public get concurrentCommitCount(): number {
		return this.currentCommitCount;
	}

	/** Returns the number of open operation repositories retained by this worker. */
	public get loadedOperationCount(): number {
		return this.operations.size;
	}

	/**
	 * Returns whether one request is currently owned by persistence.
	 * @param requestId Request identifier to inspect.
	 * @returns True for a registered active or draining persist stream.
	 */
	public hasStream(requestId: string): boolean {
		return this.streams.has(requestId);
	}

	/**
	 * Builds one viewport projection against the operation's shared exact store.
	 * @param descriptor Operation identity and viewport transform.
	 * @param isNewest Guard canceled as soon as a newer viewport request is pending.
	 * @returns Transfer-ready render projection.
	 */
	public async requestRenderWindow(
		descriptor: QueryRenderWindowDescriptor,
		isNewest: () => boolean = () => true,
	): Promise<RenderWindowProjection | null> {
		if (this.destroyed) throw new Error("Query persistence is destroyed.");
		const pendingRelease = this.operationReleasePromises.get(
			descriptor.operationId,
		);
		if (pendingRelease) await pendingRelease;
		const operation = await this.getOperation(descriptor.operationId);
		const viewportDescriptor: EventStoreViewportDescriptor = descriptor;
		while (isNewest()) {
			try {
				const projection =
					await operation.projector.requestRenderWindow(
						viewportDescriptor,
						isNewest,
					);
				return isNewest() ? projection : null;
			} catch (error) {
				if (error instanceof StaleEventStoreProjectionError) {
					await this.yieldProjectionSlice();
					continue;
				}
				if (!(error instanceof EventStoreOverrideProjectionPendingError)) {
					throw error;
				}
				const prepared = await this.prepareViewportOverrides(
					operation,
					viewportDescriptor,
					isNewest,
				);
				if (!prepared) return null;
			}
		}
		return null;
	}

	/**
	 * Prepares exact override representatives in cooperative sixteen-page slices.
	 * @param operation Operation store and projector.
	 * @param descriptor Viewport sources and requested override revisions.
	 * @param isNewest Newest-request guard checked between every storage slice.
	 * @returns False when projection work was superseded before preparation completed.
	 */
	private async prepareViewportOverrides(
		operation: OperationPersistenceContext,
		descriptor: EventStoreViewportDescriptor,
		isNewest: () => boolean,
	): Promise<boolean> {
		const prepare = operation.projector.prepareOverrideRevision;
		if (!prepare) {
			throw new Error("Viewport projector cannot prepare override revisions.");
		}
		for (const source of descriptor.sources) {
			if (source.overrideHashes.length === 0) continue;
			let sourceRevision = -1;
			let nextPageIndex = 0;
			while (isNewest()) {
					const release = await this.acquireSourceLeaseWhileCurrent(
						this.createSourceLeaseKey(operation.operationId, source.sourceId),
					isNewest,
				);
				if (!release) return false;
				let result: EventStoreOverridePreparationResult | null = null;
				try {
					const manifest =
						await operation.backend.get<StoredEventSourceManifest>(
							"sources",
							source.sourceId,
						);
					if (!manifest) break;
					if (manifest.revision !== sourceRevision) {
						sourceRevision = manifest.revision;
						nextPageIndex = 0;
					}
					result = await prepare.call(operation.projector, {
							sourceId: source.sourceId,
							sourceRevision,
							overrideRevision: source.overrideRevision,
							overrideHashes: source.overrideHashes,
							startPageIndex: nextPageIndex,
							maximumPages: 16,
						});
				} catch (error) {
					if (
						error instanceof StaleEventStoreProjectionError ||
						(error instanceof Error &&
							error.message === "Override preparation source revision is stale.")
					) {
						continue;
					}
					throw error;
				} finally {
					release();
				}
				if (!result) continue;
				nextPageIndex = result.nextPageIndex;
				if (result.complete) break;
				await this.yieldProjectionSlice();
			}
			if (!isNewest()) return false;
		}
		return isNewest();
	}

	/** Yields between override page slices so WebSocket parsing can continue. */
	private yieldProjectionSlice(): Promise<void> {
		return new Promise((resolve) => setTimeout(resolve, 0));
	}

	/**
	 * Admits one persist stream and prepares its storage state before worker ACK.
	 * @param descriptor Persist stream descriptor supplied before the backend call.
	 * @returns Promise resolved only after quota checks and source preparation commit.
	 */
	public async register(descriptor: DocumentStreamDescriptor): Promise<void> {
		if (this.destroyed) throw new Error("Query persistence is destroyed.");
		if (descriptor.owner !== "persist") return;
		if (this.streams.has(descriptor.requestId)) {
			throw new Error(`Document stream ${descriptor.requestId} is already registered.`);
		}
		this.validateDescriptor(descriptor);
		const pendingRelease = this.operationReleasePromises.get(
			descriptor.operationId,
		);
		if (pendingRelease) await pendingRelease;
		this.beginOperationRegistration(descriptor.operationId);
		try {
			const sourceIds = this.getDescriptorSourceIds(descriptor);
			if ((descriptor.kind ?? "query-upsert") === "replace") {
				await this.supersedeReplaceStreams(descriptor, sourceIds);
			}
			const releaseSourceLeases = await this.acquireSourceLeases(
				descriptor.operationId,
				sourceIds,
			);
			let context: PersistenceStreamContext | null = null;
			try {
				const operation = await this.getOperation(descriptor.operationId);
				await this.refreshDeclaredSourceManifests(operation, sourceIds);
				await this.estimateCapacity(
					operation.operationId,
					descriptor.requestId,
					descriptor.operationEpoch,
				);
				const expectedEvents = descriptor.expectedEvents ?? 0;
				const availableAfterReservations = Math.max(
					0,
					this.remainingCapacityEvents - this.totalReservedEvents,
				);
				const isReplace =
					(descriptor.kind ?? "query-upsert") === "replace";
				const reclaimableEventsBySource = isReplace
					? this.getReclaimableReplaceEvents(operation, sourceIds)
					: new Map<Source.Id, number>();
				const reclaimableEvents = this.getTotalReclaimableEvents(
					reclaimableEventsBySource,
				);
				const requiredReclaim = Math.max(
					0,
					expectedEvents - availableAfterReservations,
				);
				if (requiredReclaim > reclaimableEvents) {
					throw new QueryPersistenceError(
						"quota",
						`Query expects ${expectedEvents} events but protected and reclaimable capacity allow only ${
							availableAfterReservations + reclaimableEvents
						}.`,
					);
				}

				context = {
					descriptor: this.cloneDescriptor(descriptor),
					operation,
					sourceIds,
					hasDeclaredSourceAuthority: sourceIds.length > 0,
					replaceSources: new Map(),
					releaseSourceLeases,
					chain: Promise.resolve(),
					reservedEvents: expectedEvents,
					unallocatedExpectedEvents: expectedEvents,
					reclaimableEventsBySource,
					inPlaceReplacement: false,
					inPlaceNotificationPublished: false,
					accepting: true,
					canceling: false,
					explicitCompletionRequested: false,
					lastChunkCommitted: false,
					completionWaiters: [],
					failed: false,
					completed: false,
					released: false,
					cleanupPromise: null,
				};
				this.totalReservedEvents += expectedEvents;

				if (isReplace) {
					await this.prepareReplaceSources(context);
					await this.activateInPlaceReplacement(context, requiredReclaim);
				} else {
					await this.prepareUpsertSources(context);
				}
				this.streams.set(descriptor.requestId, context);
			} catch (error) {
				if (context) {
					await this.rollbackPreparedSources(context);
					this.releaseContext(context);
				} else {
					for (const release of releaseSourceLeases.reverse()) release();
				}
				const failure = this.toFailureEvent(descriptor, error);
				this.callbacks.onStreamFailure(failure);
				throw error;
			}
		} finally {
			this.endOperationRegistration(descriptor.operationId);
		}
	}

	/**
	 * Reloads declared source authority after acquiring its cross-realm leases.
	 * @param operation Operation cache shared by persistence and viewport requests.
	 * @param sourceIds Source manifests that may have changed in another worker realm.
	 */
	private async refreshDeclaredSourceManifests(
		operation: OperationPersistenceContext,
		sourceIds: readonly Source.Id[],
	): Promise<void> {
		if (sourceIds.length === 0) return;
		const manifests = await operation.backend.getMany<StoredEventSourceManifest>(
			"sources",
			sourceIds,
		);
		for (let index = 0; index < sourceIds.length; index++) {
			const manifest = manifests[index];
			if (manifest) operation.manifests.set(sourceIds[index], manifest);
			else operation.manifests.delete(sourceIds[index]);
		}
	}

	/**
	 * Accepts one deduplicated persist-owned chunk and queues only normalized events.
	 * @param chunk Router-owned document chunk and stream descriptor.
	 * @returns Nothing; persistence continues through the stream's ordered promise chain.
	 */
	public handleDocumentChunk(chunk: DocumentChunkReady): void {
		const context = this.streams.get(chunk.descriptor.requestId);
		if (!context || context.failed || context.completed || !context.accepting) return;
		let rawDocuments = chunk.message.payload?.docs ?? [];
		if (!Array.isArray(rawDocuments)) {
			this.beginFailure(
				context,
				new QueryPersistenceError(
					"normalization",
					"Document chunk payload.docs is not an array.",
				),
			);
			return;
		}
		const rawByteLength = this.normalizeRawChunkByteLength(chunk.rawByteLength);
		const queuedDocumentCount = rawDocuments.length;
		this.currentPendingRawChunkBytes += rawByteLength;
		const isLast = chunk.message.payload?.last === true;
		const completesOnLast =
			(context.descriptor.completionMode ?? "last") === "last";
		if (isLast && completesOnLast) context.accepting = false;

		let rawBytesRetained = true;
		let normalizedCredit = 0;
		const processing = context.chain.then(async () => {
			try {
				if (context.failed || context.completed) return;
				const acquired = await this.acquirePendingDocumentCredit(
					context,
					queuedDocumentCount,
				);
				if (!acquired || context.failed || context.completed) return;
				normalizedCredit = queuedDocumentCount;
				let grouped: Map<Source.Id, Doc.Type[]>;
				try {
					grouped = await normalizeQueryDocumentsCooperatively(
						rawDocuments,
						context.descriptor,
					);
				} catch (error) {
					throw new QueryPersistenceError(
						"normalization",
						error instanceof Error ? error.message : String(error),
					);
				} finally {
					this.releaseRawChunkBytes(rawByteLength);
					rawBytesRetained = false;
					if (chunk.message.payload) chunk.message.payload.docs = [];
					rawDocuments = [];
				}
				await this.ensureChunkCapacity(context, queuedDocumentCount);
				const sourceIds = [...grouped.keys()].sort();
				if (!context.hasDeclaredSourceAuthority) {
					await this.acquireDiscoveredSourceLeases(context, sourceIds);
					await this.prepareDiscoveredUpsertSources(context, sourceIds);
				}
				if ((context.descriptor.kind ?? "query-upsert") === "replace") {
					this.transferPendingDocumentsToReplaceBuilders(context, grouped);
					normalizedCredit = 0;
					await this.flushReplacePages(context);
				} else {
					await this.waitForAll(
						[...grouped.entries()].map(([sourceId, documents]) =>
							this.persistSourceDocuments(context, sourceId, documents),
						),
					);
				}
				if (!isLast) return;
				if ((context.descriptor.completionMode ?? "last") === "explicit") {
					context.lastChunkCommitted = true;
					if (!context.explicitCompletionRequested) return;
					context.accepting = false;
					await this.finishContext(context);
					return;
				}
				await this.finishContext(context);
			} finally {
				if (rawBytesRetained) {
					this.releaseRawChunkBytes(rawByteLength);
					rawBytesRetained = false;
				}
				if (normalizedCredit > 0) {
					this.releasePendingDocumentCredit(normalizedCredit);
					normalizedCredit = 0;
				}
			}
		});
		context.chain = processing
			.catch((error: unknown) => this.failContext(context, error));
	}

	/**
	 * Normalizes one raw WebSocket frame size for exact backlog accounting.
	 * @param byteLength Candidate raw frame byte length.
	 * @returns Safe non-negative integer byte length.
	 */
	private normalizeRawChunkByteLength(byteLength: number): number {
		return Number.isFinite(byteLength)
			? Math.max(0, Math.floor(byteLength))
			: 0;
	}

	/**
	 * Releases raw frame bytes as soon as worker-side normalization drops the payload.
	 * @param byteLength Previously reserved raw frame byte length.
	 * @returns Nothing.
	 */
	private releaseRawChunkBytes(byteLength: number): void {
		this.currentPendingRawChunkBytes = Math.max(
			0,
			this.currentPendingRawChunkBytes - byteLength,
		);
	}

	/**
	 * Acquires FIFO weighted credit before retaining normalized document objects.
	 * @param context Stream waiting to normalize one ordered chunk.
	 * @param documentCount Number of documents the chunk will normalize.
	 * @returns True after credit is reserved, or false when the stream became terminal.
	 */
	private acquirePendingDocumentCredit(
		context: PersistenceStreamContext,
		documentCount: number,
	): Promise<boolean> {
		if (documentCount === 0) return Promise.resolve(true);
		if (documentCount > this.pendingDocumentLimit) {
			return Promise.reject(
				new QueryPersistenceError(
					"backlog",
					`Document chunk contains ${documentCount} documents but the normalized limit is ${this.pendingDocumentLimit}.`,
				),
			);
		}
		if (
			context.failed ||
			context.completed ||
			context.canceling ||
			context.released
		) {
			return Promise.resolve(false);
		}
		if (
			this.pendingDocumentCreditWaiters.length === 0 &&
			this.pendingDocumentCount + documentCount <= this.pendingDocumentLimit
		) {
			this.currentPendingDocuments += documentCount;
			return Promise.resolve(true);
		}
		return new Promise<boolean>((resolve) => {
			this.pendingDocumentCreditWaiters.push({
				context,
				documentCount,
				resolve,
			});
			this.drainPendingDocumentCreditWaiters();
		});
	}

	/**
	 * Releases normalized FIFO credit after commit or before terminal cleanup.
	 * @param documentCount Number of normalized documents no longer retained.
	 * @returns Nothing.
	 */
	private releasePendingDocumentCredit(documentCount: number): void {
		this.currentPendingDocuments = Math.max(
			0,
			this.currentPendingDocuments - documentCount,
		);
		this.drainPendingDocumentCreditWaiters();
	}

	/**
	 * Grants normalized capacity to FIFO waiters without allowing smaller newcomers to barge.
	 * @returns Nothing.
	 */
	private drainPendingDocumentCreditWaiters(): void {
		while (this.pendingDocumentCreditWaiters.length > 0) {
			const waiter = this.pendingDocumentCreditWaiters[0];
			if (
				waiter.context.failed ||
				waiter.context.completed ||
				waiter.context.canceling ||
				waiter.context.released
			) {
				this.pendingDocumentCreditWaiters.shift();
				waiter.resolve(false);
				continue;
			}
			if (
				this.pendingDocumentCount + waiter.documentCount >
				this.pendingDocumentLimit
			) {
				return;
			}
			this.pendingDocumentCreditWaiters.shift();
			this.currentPendingDocuments += waiter.documentCount;
			waiter.resolve(true);
		}
	}

	/**
	 * Removes one terminal stream's queued credit requests and wakes later FIFO waiters.
	 * @param context Stream that can no longer consume normalized capacity.
	 * @returns Nothing.
	 */
	private cancelPendingDocumentCreditWaiters(
		context: PersistenceStreamContext,
	): void {
		const retained: PendingDocumentCreditWaiter[] = [];
		for (const waiter of this.pendingDocumentCreditWaiters) {
			if (waiter.context === context) waiter.resolve(false);
			else retained.push(waiter);
		}
		this.pendingDocumentCreditWaiters.splice(
			0,
			this.pendingDocumentCreditWaiters.length,
			...retained,
		);
		this.drainPendingDocumentCreditWaiters();
	}

	/**
	 * Flushes and completes one explicit-mode persistence stream.
	 * @param requestId Request identifier to complete after its durable commit.
	 * @returns True when persistence owned the request.
	 */
	public async complete(requestId: string): Promise<boolean> {
		const context = this.streams.get(requestId);
		if (!context) return false;
		const explicit =
			(context.descriptor.completionMode ?? "last") === "explicit";
		if (explicit) context.explicitCompletionRequested = true;
		await context.chain;
		if (context.failed || context.completed || context.canceling || context.released) {
			return true;
		}
		if (explicit && !context.lastChunkCommitted) {
			await new Promise<void>((resolve) => {
				context.completionWaiters.push(resolve);
			});
			return true;
		}
		context.accepting = false;
		try {
			await this.finishContext(context);
		} catch (error) {
			await this.failContext(context, error);
		}
		return true;
	}

	/**
	 * Cancels one persist stream and rolls back an unfinished replace generation.
	 * @param requestId Request identifier to cancel.
	 * @returns True when persistence owned the request.
	 */
	public async cancel(requestId: string): Promise<boolean> {
		const context = this.streams.get(requestId);
		if (!context) return false;
		if (context.cleanupPromise) {
			await context.cleanupPromise.catch(() => undefined);
			return true;
		}
		context.accepting = false;
		context.canceling = true;
		this.cancelPendingDocumentCreditWaiters(context);
		const pendingChain = context.chain.catch(() => undefined);
		const cleanup = pendingChain.then(async () => {
			if (
				!context.released &&
				!context.completed &&
				!context.failed
			) {
				await this.rollbackPreparedSources(context);
			}
			this.releaseContext(context);
		});
		context.cleanupPromise = cleanup;
		await cleanup;
		return true;
	}

	/**
	 * Cancels persist streams belonging to one operation or operation epoch.
	 * @param operationId Operation identifier to cancel.
	 * @param operationEpoch Optional epoch guard.
	 * @returns Number of persistence streams canceled.
	 */
	public async cancelOperation(
		operationId: string,
		operationEpoch?: number,
	): Promise<number> {
		const requestIds = [...this.streams.values()]
			.filter(
				(context) =>
					context.descriptor.operationId === operationId &&
					(operationEpoch === undefined ||
						context.descriptor.operationEpoch === operationEpoch),
			)
			.map((context) => context.descriptor.requestId);
		const results = await Promise.all(
			requestIds.map((requestId) => this.cancel(requestId)),
		);
		return results.filter(Boolean).length;
	}

	/**
	 * Closes one inactive operation context without deleting its reconstructible IndexedDB.
	 * New registrations wait until the old connection has fully closed.
	 * @param operationId Operation repository to release.
	 * @returns True when an open or opening operation context was closed.
	 */
	public releaseOperation(operationId: string): Promise<boolean> {
		const existing = this.operationReleasePromises.get(operationId);
		if (existing) return existing;
		this.releasingOperations.add(operationId);
		const release = (async (): Promise<boolean> => {
			await this.cancelOperation(operationId);
			await this.waitForOperationRegistrations(operationId);
			await this.cancelOperation(operationId);
			const operationPromise = this.operationPromises.get(operationId);
			if (!operationPromise) return false;
			let operation: OperationPersistenceContext;
			try {
				operation = await operationPromise;
			} catch {
				if (this.operationPromises.get(operationId) === operationPromise) {
					this.operationPromises.delete(operationId);
				}
				return false;
			}
			if (this.operationPromises.get(operationId) === operationPromise) {
				this.operationPromises.delete(operationId);
			}
			if (this.operations.get(operationId) === operation) {
				this.operations.delete(operationId);
			}
			operation.revisionBus.close();
			operation.repository.close();
			return true;
		})().finally(() => {
			this.releasingOperations.delete(operationId);
			this.operationReleasePromises.delete(operationId);
		});
		this.operationReleasePromises.set(operationId, release);
		return release;
	}

	/**
	 * Fails active persist streams on socket disconnect and leaves replace caches incomplete.
	 * @returns Promise resolved after best-effort incomplete markers and lock release.
	 */
	public async handleDisconnect(): Promise<void> {
		const contexts = [...this.streams.values()].filter(
			(context) => !context.completed,
		);
		for (const context of contexts) {
			if (context.failed || context.canceling) continue;
			this.signalFailure(
				context,
				new QueryPersistenceError(
					"disconnected",
					"WebSocket disconnected before persistence completed.",
				),
			);
		}
		await Promise.all(
			contexts.map(async (context) => {
				if (context.cleanupPromise) {
					await context.cleanupPromise.catch(() => undefined);
					return;
				}
				await context.chain.catch(() => undefined);
				if (!context.released) await this.cleanupFailedContext(context, true);
			}),
		);
	}

	/** Permanently closes operation repositories after failing active streams. */
	public destroy(): void {
		if (this.destroyed) return;
		this.destroyed = true;
		void this.handleDisconnect().finally(async () => {
			await Promise.allSettled(this.operationReleasePromises.values());
			const operations = await Promise.allSettled(this.operationPromises.values());
			for (const operation of operations) {
				if (operation.status !== "fulfilled") continue;
				operation.value.revisionBus.close();
				operation.value.repository.close();
			}
			this.operationPromises.clear();
			this.operations.clear();
			this.operationReleasePromises.clear();
			this.releasingOperations.clear();
		});
	}

	/**
	 * Validates fields required before persistence can reserve source mutations.
	 * @param descriptor Stream descriptor to validate.
	 */
	private validateDescriptor(descriptor: DocumentStreamDescriptor): void {
		const sourceIds = this.getDescriptorSourceIds(descriptor);
		if (
			(descriptor.kind ?? "query-upsert") === "replace" &&
			sourceIds.length === 0
		) {
			throw new TypeError(
				"Replace streams must declare at least one source before registration.",
			);
		}
		if (
			descriptor.expectedEvents !== undefined &&
			(!Number.isSafeInteger(descriptor.expectedEvents) ||
				descriptor.expectedEvents < 0)
		) {
			throw new TypeError("expectedEvents must be a non-negative integer.");
		}
	}

	/**
	 * Returns sorted unique source IDs that form explicit mutation authority.
	 * @param descriptor Stream descriptor carrying source authority.
	 * @returns Branded source IDs in deterministic lock order.
	 */
	private getDescriptorSourceIds(
		descriptor: DocumentStreamDescriptor,
	): Source.Id[] {
		return [...new Set(descriptor.sourceIds ?? [])]
			.filter((sourceId) => sourceId.length > 0)
			.sort()
			.map((sourceId) => sourceId as Source.Id);
	}

	/**
	 * Clones mutable descriptor collections before retaining stream authority.
	 * @param descriptor Caller-owned stream descriptor.
	 * @returns Serializable descriptor snapshot.
	 */
	private cloneDescriptor(
		descriptor: DocumentStreamDescriptor,
	): DocumentStreamDescriptor {
		return {
			...descriptor,
			sourceIds: descriptor.sourceIds ? [...descriptor.sourceIds] : undefined,
			defaultSourceSettings: descriptor.defaultSourceSettings
				? { ...descriptor.defaultSourceSettings }
				: undefined,
			sourceSettingsById: descriptor.sourceSettingsById
				? Object.fromEntries(
						Object.entries(descriptor.sourceSettingsById).map(
							([sourceId, settings]) => [sourceId, { ...settings }],
						),
					)
				: undefined,
		};
	}

	/**
	 * Opens one operation repository and loads compact source manifests once.
	 * @param operationId Operation database identity.
	 * @returns Shared operation persistence context.
	 */
	private async getOperation(
		operationId: string,
	): Promise<OperationPersistenceContext> {
		let operation = this.operationPromises.get(operationId);
		if (operation) return operation;
		operation = (async () => {
			const namespace: EventStoreNamespace = {
				coordinatorId: this.coordinatorId,
				operationId: operationId as Operation.Id,
			};
			const store = this.createOperationStore(namespace);
			const manifests =
				await store.backend.getAll<StoredEventSourceManifest>("sources");
			const context: OperationPersistenceContext = {
				operationId,
				backend: store.backend,
				repository: store.repository,
				revisionBus: this.createRevisionBus(namespace),
				projector: this.createViewportProjector(store.backend),
				manifests: new Map(
					manifests.map((manifest) => [manifest.sourceId, manifest]),
				),
			};
			this.operations.set(operationId, context);
			return context;
		})();
		this.operationPromises.set(operationId, operation);
		void operation.catch(() => this.operationPromises.delete(operationId));
		return operation;
	}

	/**
	 * Marks one operation registration as in-flight so release cannot close under it.
	 * @param operationId Operation receiving a new stream registration.
	 */
	private beginOperationRegistration(operationId: string): void {
		if (this.releasingOperations.has(operationId)) {
			throw new Error(`Operation ${operationId} is being released.`);
		}
		this.pendingOperationRegistrations.set(
			operationId,
			(this.pendingOperationRegistrations.get(operationId) ?? 0) + 1,
		);
	}

	/**
	 * Releases one registration reference and wakes operation-release waiters at zero.
	 * @param operationId Operation whose registration settled.
	 */
	private endOperationRegistration(operationId: string): void {
		const remaining = Math.max(
			0,
			(this.pendingOperationRegistrations.get(operationId) ?? 1) - 1,
		);
		if (remaining > 0) {
			this.pendingOperationRegistrations.set(operationId, remaining);
			return;
		}
		this.pendingOperationRegistrations.delete(operationId);
		for (const resolve of this.operationRegistrationWaiters.get(operationId) ?? []) {
			resolve();
		}
		this.operationRegistrationWaiters.delete(operationId);
	}

	/**
	 * Waits until every registration that started before operation release has settled.
	 * @param operationId Operation being closed.
	 */
	private async waitForOperationRegistrations(operationId: string): Promise<void> {
		if ((this.pendingOperationRegistrations.get(operationId) ?? 0) === 0) return;
		await new Promise<void>((resolve) => {
			const waiters = this.operationRegistrationWaiters.get(operationId) ?? [];
			waiters.push(resolve);
			this.operationRegistrationWaiters.set(operationId, waiters);
		});
	}

	/**
	 * Cancels an older replacement that owns any of the same declared sources.
	 * @param descriptor New replacement stream taking authority.
	 * @param sourceIds Sources that the new replacement will rebuild.
	 */
	private async supersedeReplaceStreams(
		descriptor: DocumentStreamDescriptor,
		sourceIds: readonly Source.Id[],
	): Promise<void> {
		const sourceSet = new Set(sourceIds);
		const superseded = [...this.streams.values()].filter(
			(context) =>
				context.descriptor.requestId !== descriptor.requestId &&
				context.descriptor.operationId === descriptor.operationId &&
				(context.descriptor.kind ?? "query-upsert") === "replace" &&
				context.sourceIds.some((sourceId) => sourceSet.has(sourceId)),
		);
		for (const context of superseded) {
			this.beginFailure(
				context,
				new QueryPersistenceError(
					"superseded",
					`Document stream ${context.descriptor.requestId} was superseded by ${descriptor.requestId}.`,
				),
			);
		}
		await Promise.all(
			superseded.map((context) => context.chain.catch(() => undefined)),
		);
	}

	/**
	 * Begins a building generation for every replace source before registration ACK.
	 * @param context Stream being prepared.
	 */
	private async prepareReplaceSources(
		context: PersistenceStreamContext,
	): Promise<void> {
		await this.waitForAll(
			context.sourceIds.map(async (sourceId) => {
				const current = context.operation.manifests.get(sourceId);
				const generation = Math.max(
					(current?.generation ?? 0) + 1,
					this.now(),
				);
				const summary = await this.withCommitSlot(() =>
					context.operation.repository.beginGeneration({
						sourceId,
						generation,
					}),
				);
				context.replaceSources.set(sourceId, {
					sourceId,
					generation,
					nextHandle: 1,
					nextPageId: 0,
					revision: summary.revision,
					pendingDocuments: [],
				});
				await this.afterCommit(context, summary, 0);
			}),
		);
	}

	/**
	 * Captures complete generations that a source-leased replacement may discard.
	 * @param operation Operation containing the source manifests read under lease.
	 * @param sourceIds Declared replacement sources in deterministic lock order.
	 * @returns Per-source event counts whose physical generations are reclaimable.
	 */
	private getReclaimableReplaceEvents(
		operation: OperationPersistenceContext,
		sourceIds: readonly Source.Id[],
	): Map<Source.Id, number> {
		const reclaimable = new Map<Source.Id, number>();
		for (const sourceId of sourceIds) {
			const manifest = operation.manifests.get(sourceId);
			const completeGeneration =
				manifest?.status === "complete"
					? manifest
					: manifest?.rollback?.status === "complete"
						? manifest.rollback
						: undefined;
			const eventCount = Math.max(
				0,
				Math.floor(completeGeneration?.eventCount ?? 0),
			);
			if (eventCount > 0) reclaimable.set(sourceId, eventCount);
		}
		return reclaimable;
	}

	/**
	 * Sums remaining replacement credit without exceeding safe integer arithmetic.
	 * @param reclaimableEventsBySource Per-source complete-generation event counts.
	 * @returns Saturated total events available by dropping retained rollbacks.
	 */
	private getTotalReclaimableEvents(
		reclaimableEventsBySource: ReadonlyMap<Source.Id, number>,
	): number {
		let total = 0;
		for (const eventCount of reclaimableEventsBySource.values()) {
			total = Math.min(Number.MAX_SAFE_INTEGER, total + eventCount);
		}
		return total;
	}

	/**
	 * Drops only as many retained complete generations as a replacement needs.
	 * The stream already holds every declared source lease, so the captured rollback
	 * counts cannot be consumed by another writer while storage is being reclaimed.
	 * @param context Source-leased replacement stream.
	 * @param requiredEvents Minimum additional event capacity to reclaim.
	 */
	private async activateInPlaceReplacement(
		context: PersistenceStreamContext,
		requiredEvents: number,
	): Promise<void> {
		if (requiredEvents <= 0) return;
		let reclaimedEvents = 0;
		for (const [sourceId, eventCount] of context.reclaimableEventsBySource) {
			const source = context.replaceSources.get(sourceId);
			if (!source) continue;
			const manifest =
				await context.operation.backend.get<StoredEventSourceManifest>(
					"sources",
					sourceId,
				);
			if (!manifest?.rollback || manifest.rollback.eventCount !== eventCount) {
				context.reclaimableEventsBySource.delete(sourceId);
				continue;
			}
			let summary: EventSourceSummary;
			try {
				summary = await this.withCommitSlot(() =>
					context.operation.repository.discardGenerationRollback(
						sourceId,
						source.generation,
					),
				);
			} catch (error) {
				try {
					const detached =
						await context.operation.backend.get<StoredEventSourceManifest>(
							"sources",
							sourceId,
						);
					if (detached?.inPlaceReplacement && !detached.rollback) {
						context.inPlaceReplacement = true;
					}
				} catch {
					// Preserve the primary storage failure; cleanup will remain conservative.
				}
				throw error;
			}
			context.reclaimableEventsBySource.delete(sourceId);
			context.inPlaceReplacement = true;
			reclaimedEvents = Math.min(
				Number.MAX_SAFE_INTEGER,
				reclaimedEvents + eventCount,
			);
			this.remainingCapacityEvents = Math.min(
				Number.MAX_SAFE_INTEGER,
				this.remainingCapacityEvents + eventCount,
			);
			await this.afterCommit(context, summary, 0);
			if (reclaimedEvents >= requiredEvents) return;
		}
		throw new QueryPersistenceError(
			"quota",
			`Replacement could reclaim ${reclaimedEvents} events but requires ${requiredEvents}.`,
		);
	}

	/**
	 * Ensures upsert sources expose complete manifests before registration ACK.
	 * @param context Stream being prepared.
	 */
	private async prepareUpsertSources(
		context: PersistenceStreamContext,
		sourceIds: readonly Source.Id[] = context.sourceIds,
	): Promise<void> {
		await this.waitForAll(
			sourceIds.map(async (sourceId) => {
				let current = context.operation.manifests.get(sourceId);
				if (current?.status === "complete") return;
				if (current) {
					await this.withCommitSlot(() =>
						context.operation.repository.clearSource(sourceId),
					);
					context.operation.manifests.delete(sourceId);
				}
				const generation = Math.max((current?.generation ?? 0) + 1, this.now());
				const building = await this.withCommitSlot(() =>
					context.operation.repository.beginGeneration({ sourceId, generation }),
				);
				await this.afterCommit(context, building, 0);
				const complete = await this.withCommitSlot(() =>
					context.operation.repository.finalizeGeneration(sourceId, generation),
				);
				await this.afterCommit(context, complete, 0);
			}),
		);
	}

	/**
	 * Prepares source-less query/ingest streams when their first documents reveal sources.
	 * Source settings configure hashing but deliberately do not grant source authority.
	 * @param context Dynamic non-replacement stream.
	 * @param sourceIds Source IDs discovered in the current normalized chunk.
	 */
	private async prepareDiscoveredUpsertSources(
		context: PersistenceStreamContext,
		sourceIds: readonly Source.Id[],
	): Promise<void> {
		if ((context.descriptor.kind ?? "query-upsert") === "replace") {
			throw new QueryPersistenceError(
				"normalization",
				"Replace streams cannot discover source authority from document payloads.",
			);
		}
		const known = new Set(context.sourceIds);
		const discovered = sourceIds.filter((sourceId) => !known.has(sourceId));
		if (discovered.length === 0) return;
		await this.prepareUpsertSources(context, discovered);
		context.sourceIds.push(...discovered);
		context.sourceIds.sort();
	}

	/**
	 * Acquires newly discovered source leases and retains them until stream terminal.
	 * When a later source sorts before an already-held lease, all leases are released
	 * after the prior chunk commit and reacquired together in deterministic order.
	 * @param context Source-less stream gaining mutation authority.
	 * @param sourceIds Sorted source IDs observed in the current chunk.
	 */
	private async acquireDiscoveredSourceLeases(
		context: PersistenceStreamContext,
		sourceIds: readonly Source.Id[],
	): Promise<void> {
		const known = new Set(context.sourceIds);
		const discovered = sourceIds
			.filter((sourceId) => !known.has(sourceId))
			.sort();
		if (discovered.length === 0) return;
		const highestHeldSourceId = context.sourceIds[context.sourceIds.length - 1];
		if (
			highestHeldSourceId !== undefined &&
			discovered[0] < highestHeldSourceId
		) {
			const completeAuthority = [...context.sourceIds, ...discovered].sort();
			for (const release of context.releaseSourceLeases.reverse()) release();
			context.releaseSourceLeases.length = 0;
			const reacquired = await this.acquireSourceLeases(
				context.descriptor.operationId,
				completeAuthority,
			);
			context.releaseSourceLeases.push(...reacquired);
			await this.refreshDeclaredSourceManifests(
				context.operation,
				completeAuthority,
			);
			return;
		}
		const releases = await this.acquireSourceLeases(
			context.descriptor.operationId,
			discovered,
		);
		context.releaseSourceLeases.push(...releases);
	}

	/**
	 * Atomically transfers normalized credit into replace page builders exactly once.
	 * Partial builders are flushed at the end of every chunk so FIFO waiters always progress.
	 * @param context Replacement stream owning the prepared sources.
	 * @param grouped Normalized documents grouped by their declared source.
	 * @returns Nothing.
	 */
	private transferPendingDocumentsToReplaceBuilders(
		context: PersistenceStreamContext,
		grouped: ReadonlyMap<Source.Id, readonly Doc.Type[]>,
	): void {
		const transfers: Array<{
			source: ReplaceSourceContext;
			documents: readonly Doc.Type[];
		}> = [];
		let transferredDocuments = 0;
		for (const [sourceId, documents] of grouped) {
			if (!context.sourceIds.includes(sourceId)) {
				throw new QueryPersistenceError(
					"normalization",
					`Source ${sourceId} was not registered for this stream.`,
				);
			}
			const source = context.replaceSources.get(sourceId);
			if (!source) {
				throw new QueryPersistenceError(
					"storage",
					`Replace source ${sourceId} is not prepared.`,
				);
			}
			transfers.push({ source, documents });
			transferredDocuments += documents.length;
		}
		for (const transfer of transfers) {
			transfer.source.pendingDocuments.push(...transfer.documents);
		}
		this.currentPendingDocuments = Math.max(
			0,
			this.currentPendingDocuments - transferredDocuments,
		);
		this.currentPageBuilderDocuments += transferredDocuments;
	}

	/**
	 * Persists one normalized upsert source group.
	 * @param context Registered persistence stream.
	 * @param sourceId Source owning the documents.
	 * @param documents Minimal normalized documents.
	 * @returns Promise settled after every bounded upsert commit.
	 */
	private async persistSourceDocuments(
		context: PersistenceStreamContext,
		sourceId: Source.Id,
		documents: readonly Doc.Type[],
	): Promise<void> {
		if (!context.sourceIds.includes(sourceId)) {
			throw new QueryPersistenceError(
				"normalization",
				`Source ${sourceId} was not registered for this stream.`,
			);
		}
		const upsert = context.operation.repository.upsertDocuments;
		if (!upsert) {
			throw new QueryPersistenceError(
				"storage",
				"EventStore upsertDocuments is unavailable.",
			);
		}
		for (let offset = 0; offset < documents.length; offset += EVENT_PAGE_SIZE) {
			const batch = documents.slice(offset, offset + EVENT_PAGE_SIZE);
			const summaries = await this.withCommitSlot(() =>
				upsert.call(context.operation.repository, {
					documents: batch,
					allowedSourceIds: [...context.sourceIds],
				}),
			);
			for (let index = 0; index < summaries.length; index++) {
				await this.afterCommit(
					context,
					summaries[index],
					index === 0 ? batch.length : 0,
				);
			}
		}
	}

	/**
	 * Encodes and appends one exact replace page with monotonic source handles.
	 * @param context Owning persistence stream.
	 * @param source Prepared replace source.
	 * @param documents Full page or final partial page.
	 * @returns Promise settled after page persistence and capacity publication.
	 */
	private async appendReplacePage(
		context: PersistenceStreamContext,
		source: ReplaceSourceContext,
		documents: readonly Doc.Type[],
	): Promise<void> {
		if (documents.length === 0) return;
		try {
			const finalHandle = source.nextHandle + documents.length - 1;
			if (finalHandle > EVENT_MAXIMUM_SOURCE_HANDLE) {
				throw new QueryPersistenceError(
					"storage",
					`Source ${source.sourceId} exhausted uint32 event handles.`,
				);
			}
			const handles = new Uint32Array(documents.length);
			for (let index = 0; index < handles.length; index++) {
				handles[index] = source.nextHandle + index;
			}
			const page = encodeEventPage({
				sourceId: source.sourceId,
				generation: source.generation,
				pageId: source.nextPageId,
				revision: source.revision + 1,
				documents,
				handles,
			});
			const summary = await this.withCommitSlot(() =>
				context.operation.repository.appendPages({
					sourceId: source.sourceId,
					generation: source.generation,
					pages: [page],
				}),
			);
			source.nextHandle = finalHandle + 1;
			source.nextPageId += 1;
			source.revision = summary.revision;
			await this.afterCommit(context, summary, documents.length);
		} finally {
			this.currentPageBuilderDocuments = Math.max(
				0,
				this.currentPageBuilderDocuments - documents.length,
			);
			this.drainPendingDocumentCreditWaiters();
		}
	}

	/**
	 * Flushes partial replace page builders without finalizing their generations.
	 * @param context Stream whose builders should commit.
	 * @returns Promise settled after every source builder is empty.
	 */
	private async flushReplacePages(
		context: PersistenceStreamContext,
	): Promise<void> {
		await this.waitForAll(
			[...context.replaceSources.values()].map(async (source) => {
				while (source.pendingDocuments.length > 0) {
					const documents = source.pendingDocuments.splice(0, EVENT_PAGE_SIZE);
					await this.appendReplacePage(context, source, documents);
				}
			}),
		);
	}

	/**
	 * Flushes, finalizes replace sources, and completes the router stream after commit.
	 * @param context Persistence stream reaching its durable terminal point.
	 */
	private async finishContext(context: PersistenceStreamContext): Promise<void> {
		if (context.completed || context.failed) return;
		this.callbacks.onStreamFlushing(context.descriptor.requestId);
		await this.flushReplacePages(context);
		if ((context.descriptor.kind ?? "query-upsert") === "replace") {
			await this.waitForAll(
				[...context.replaceSources.values()].map(async (source) => {
					const summary = await this.withCommitSlot(() =>
						context.operation.repository.finalizeGeneration(
							source.sourceId,
							source.generation,
						),
					);
					await this.afterCommit(context, summary, 0);
				}),
			);
		}
		context.completed = true;
		this.callbacks.onStreamCompleted(context.descriptor.requestId);
		this.releaseContext(context);
	}

	/**
	 * Emits and schedules one terminal stream failure without throwing on the socket path.
	 * @param context Stream to fail.
	 * @param error Typed or unknown failure.
	 */
	private beginFailure(
		context: PersistenceStreamContext,
		error: unknown,
	): void {
		if (!this.signalFailure(context, error)) return;
		const cleanup = context.chain
			.catch(() => undefined)
			.then(() => this.cleanupFailedContext(context, false));
		context.cleanupPromise = cleanup;
		context.chain = cleanup;
	}

	/**
	 * Handles a failure raised by queued persistence work.
	 * @param context Stream whose queued operation failed.
	 * @param error Failure raised by normalization, quota, or storage.
	 */
	private async failContext(
		context: PersistenceStreamContext,
		error: unknown,
	): Promise<void> {
		this.signalFailure(context, error);
		await this.cleanupFailedContext(context, false);
	}

	/**
	 * Publishes one failure once and stops accepting later chunks immediately.
	 * @param context Stream to fail.
	 * @param error Failure raised by the stream.
	 * @returns True when this call transitioned the stream to failed.
	 */
	private signalFailure(
		context: PersistenceStreamContext,
		error: unknown,
	): boolean {
		if (context.failed || context.completed || context.canceling) return false;
		context.failed = true;
		context.accepting = false;
		this.cancelPendingDocumentCreditWaiters(context);
		this.callbacks.onStreamFailure(
			this.toFailureEvent(context.descriptor, error),
		);
		return true;
	}

	/**
	 * Marks replace generations incomplete when possible, then releases the stream.
	 * @param context Failed persistence stream.
	 * @param disconnected Whether cleanup was caused by transport disconnect.
	 */
	private async cleanupFailedContext(
		context: PersistenceStreamContext,
		disconnected: boolean,
	): Promise<void> {
		if (context.released) return;
		if ((context.descriptor.kind ?? "query-upsert") === "replace") {
			await Promise.all(
				[...context.replaceSources.values()].map(async (source) => {
					try {
						const manifest =
							await context.operation.backend.get<StoredEventSourceManifest>(
								"sources",
								source.sourceId,
							);
						if (
							disconnected &&
							!manifest?.rollback &&
							context.operation.repository.markGenerationIncomplete
						) {
							const summary = await this.withCommitSlot(() =>
								context.operation.repository.markGenerationIncomplete!(
									source.sourceId,
									source.generation,
								),
							);
							await this.afterCommit(context, summary, 0);
							return;
						}
						await this.withCommitSlot(() =>
							context.operation.repository.cancelGeneration(
								source.sourceId,
								source.generation,
							),
						);
						await this.refreshSourceManifest(context, source.sourceId, true);
					} catch {
						// The source remains refetchable even if best-effort cleanup also fails.
					}
				}),
			);
		}
		this.releaseContext(context);
	}

	/**
	 * Rolls back replace generations prepared before cancellation or failed registration.
	 * @param context Stream whose building generations should be removed.
	 */
	private async rollbackPreparedSources(
		context: PersistenceStreamContext,
	): Promise<void> {
		if ((context.descriptor.kind ?? "query-upsert") !== "replace") return;
		await Promise.all(
			[...context.replaceSources.values()].map(async (source) => {
				try {
					await this.withCommitSlot(() =>
						context.operation.repository.cancelGeneration(
							source.sourceId,
							source.generation,
						),
					);
					await this.refreshSourceManifest(context, source.sourceId, true);
				} catch {
					// Registration failure already surfaces the primary cause to the caller.
				}
			}),
		);
	}

	/**
	 * Releases source leases and unused capacity reservation exactly once.
	 * @param context Terminal persistence stream.
	 */
	private releaseContext(context: PersistenceStreamContext): void {
		if (context.released) return;
		context.released = true;
		for (const source of context.replaceSources.values()) {
			this.currentPageBuilderDocuments = Math.max(
				0,
				this.currentPageBuilderDocuments - source.pendingDocuments.length,
			);
			source.pendingDocuments.length = 0;
		}
		this.cancelPendingDocumentCreditWaiters(context);
		this.drainPendingDocumentCreditWaiters();
		this.totalReservedEvents = Math.max(
			0,
			this.totalReservedEvents - context.reservedEvents,
		);
		context.reservedEvents = 0;
		context.unallocatedExpectedEvents = 0;
		for (const release of context.releaseSourceLeases.reverse()) release();
		context.releaseSourceLeases.length = 0;
		this.streams.delete(context.descriptor.requestId);
		for (const resolve of context.completionWaiters.splice(0)) resolve();
	}

	/**
	 * Checks current quota before accepting one queued normalized batch.
	 * @param context Stream consuming capacity.
	 * @param eventCount Conservative event count to reserve for this chunk.
	 */
	private async ensureChunkCapacity(
		context: PersistenceStreamContext,
		eventCount: number,
	): Promise<void> {
		if (this.capacityTracker.shouldReestimate() || !this.latestCapacity) {
			await this.estimateCapacity(
				context.operation.operationId,
				context.descriptor.requestId,
				context.descriptor.operationEpoch,
			);
		}
		const allocatedExpected = Math.min(
			context.unallocatedExpectedEvents,
			eventCount,
		);
		context.unallocatedExpectedEvents -= allocatedExpected;
		const additionalReservation = eventCount - allocatedExpected;
		const unreservedAvailable = Math.max(
			0,
			this.remainingCapacityEvents - this.totalReservedEvents,
		);
		if (additionalReservation > unreservedAvailable) {
			const requiredReclaim = additionalReservation - unreservedAvailable;
			if (
				(context.descriptor.kind ?? "query-upsert") !== "replace" ||
				requiredReclaim >
					this.getTotalReclaimableEvents(
						context.reclaimableEventsBySource,
					)
			) {
				throw new QueryPersistenceError(
					"quota",
					`Chunk requires ${additionalReservation} unreserved events but current protected and reclaimable capacity is exhausted.`,
				);
			}
			context.reservedEvents += additionalReservation;
			this.totalReservedEvents += additionalReservation;
			await this.activateInPlaceReplacement(context, requiredReclaim);
			return;
		}
		context.reservedEvents += additionalReservation;
		this.totalReservedEvents += additionalReservation;
	}

	/**
	 * Reloads source authority after a rollback and optionally publishes its summary.
	 * @param context Stream whose operation cache must be refreshed.
	 * @param sourceId Source restored or removed by cleanup.
	 * @param publish Whether a restored manifest should be sent to the main thread.
	 */
	private async refreshSourceManifest(
		context: PersistenceStreamContext,
		sourceId: Source.Id,
		publish: boolean,
	): Promise<void> {
		const manifest =
			await context.operation.backend.get<StoredEventSourceManifest>(
				"sources",
				sourceId,
			);
		if (!manifest) {
			context.operation.manifests.delete(sourceId);
			return;
		}
		context.operation.manifests.set(sourceId, manifest);
		if (!publish) return;
		const summary: EventSourceSummary = {
			sourceId: manifest.sourceId,
			revision: manifest.revision,
			status: manifest.status,
			eventCount: manifest.eventCount,
			minTimestamp: manifest.minTimestamp,
			maxTimestamp: manifest.maxTimestamp,
			hashMin: manifest.hashMin,
			hashMax: manifest.hashMax,
		};
		context.operation.revisionBus.publish(summary);
		this.publishSourceSummary(context, summary);
	}

	/**
	 * Updates manifests, write-cost samples, summary, and capacity telemetry after commit.
	 * @param context Stream owning the commit.
	 * @param summary Source summary returned by EventStore.
	 * @param committedEvents Conservative number of documents written.
	 */
	private async afterCommit(
		context: PersistenceStreamContext,
		summary: EventSourceSummary,
		committedEvents: number,
	): Promise<void> {
		const previous = context.operation.manifests.get(summary.sourceId);
		const manifest =
			await context.operation.backend.get<StoredEventSourceManifest>(
				"sources",
				summary.sourceId,
			);
		if (manifest) context.operation.manifests.set(summary.sourceId, manifest);
		else context.operation.manifests.delete(summary.sourceId);
		const encodedBytes = Math.max(
			0,
			(manifest?.encodedBytes ?? 0) - (previous?.encodedBytes ?? 0),
		);
		this.capacityTracker.recordCommittedWrite(encodedBytes, committedEvents);
		this.consumeCapacityReservation(context, committedEvents);
		context.operation.revisionBus.publish(summary);
		this.publishSourceSummary(context, summary);
		if (this.capacityTracker.shouldReestimate()) {
			await this.estimateCapacity(
				context.operation.operationId,
				context.descriptor.requestId,
				context.descriptor.operationEpoch,
			);
		} else if (this.latestCapacity) {
			this.callbacks.onCapacity({
				requestId: context.descriptor.requestId,
				operationId: context.descriptor.operationId,
				operationEpoch: context.descriptor.operationEpoch,
				capacity: this.latestCapacity,
			});
		}
	}

	/**
	 * Publishes one compact source update and attaches the in-place notice once.
	 * @param context Stream owning the durable source revision.
	 * @param summary Source authority to publish to the main thread.
	 */
	private publishSourceSummary(
		context: PersistenceStreamContext,
		summary: EventSourceSummary,
	): void {
		const publishReplacementMode =
			context.inPlaceReplacement && !context.inPlaceNotificationPublished;
		if (publishReplacementMode) context.inPlaceNotificationPublished = true;
		this.callbacks.onSourceSummary({
			requestId: context.descriptor.requestId,
			operationId: context.descriptor.operationId,
			operationEpoch: context.descriptor.operationEpoch,
			summary,
			...(publishReplacementMode
				? { replacementMode: "in-place" as const }
				: {}),
		});
	}

	/**
	 * Deducts committed events from both global capacity and this stream reservation.
	 * @param context Stream whose write committed.
	 * @param committedEvents Conservative committed document count.
	 */
	private consumeCapacityReservation(
		context: PersistenceStreamContext,
		committedEvents: number,
	): void {
		const safeEvents = Math.max(0, Math.floor(committedEvents));
		this.remainingCapacityEvents = Math.max(
			0,
			this.remainingCapacityEvents - safeEvents,
		);
		const reservedConsumption = Math.min(context.reservedEvents, safeEvents);
		context.reservedEvents -= reservedConsumption;
		this.totalReservedEvents = Math.max(
			0,
			this.totalReservedEvents - reservedConsumption,
		);
	}

	/**
	 * Measures origin quota and emits one operation-scoped capacity snapshot.
	 * @param operationId Operation associated with the telemetry request.
	 * @param requestId Optional request associated with the estimate.
	 * @param operationEpoch Optional epoch guarding a stream-scoped estimate.
	 * @returns Current conservative capacity.
	 */
	private async estimateCapacity(
		operationId: string,
		requestId?: string,
		operationEpoch?: number,
	): Promise<EventStoreCapacity> {
		if (!this.capacityEstimatePromise) {
			this.capacityEstimatePromise = this.capacityTracker
				.estimate(this.getCurrentEventCount(), this.storageManager)
				.then((capacity) => {
					this.latestCapacity = capacity;
					this.remainingCapacityEvents = capacity.maximumAdditionalEvents;
					return capacity;
				})
				.finally(() => {
					this.capacityEstimatePromise = null;
				});
		}
		const capacity = await this.capacityEstimatePromise;
		this.callbacks.onCapacity({
			requestId,
			operationId,
			operationEpoch,
			capacity,
		});
		return capacity;
	}

	/** Returns the logical event count represented by loaded source manifests. */
	private getCurrentEventCount(): number {
		let count = 0;
		for (const operation of this.operations.values()) {
			for (const manifest of operation.manifests.values()) {
				count += manifest.eventCount;
			}
		}
		return count;
	}

	/**
	 * Waits for independent source tasks to settle before surfacing the first failure.
	 * @param tasks Source-scoped asynchronous tasks.
	 */
	private async waitForAll(tasks: readonly Promise<unknown>[]): Promise<void> {
		const results = await Promise.allSettled(tasks);
		const failure = results.find(
			(result): result is PromiseRejectedResult => result.status === "rejected",
		);
		if (failure) throw failure.reason;
	}

	/**
	 * Runs one storage mutation under the global two-transaction ceiling.
	 * @param task Commit-producing storage task.
	 * @returns Task result after a commit slot becomes available.
	 */
	private async withCommitSlot<TResult>(
		task: () => Promise<TResult>,
	): Promise<TResult> {
		await this.acquireCommitSlot();
		try {
			return await task();
		} catch (error) {
			if (
				typeof error === "object" &&
				error !== null &&
				"name" in error &&
				error.name === "QuotaExceededError"
			) {
				throw new QueryPersistenceError(
					"quota",
					"IndexedDB quota was exhausted during commit.",
				);
			}
			throw error;
		} finally {
			this.releaseCommitSlot();
		}
	}

	/** Acquires one FIFO commit permit without allowing newcomers to bypass waiters. */
	private async acquireCommitSlot(): Promise<void> {
		if (
			this.currentCommitCount < this.maximumConcurrentCommits &&
			this.commitWaiters.length === 0
		) {
			this.currentCommitCount += 1;
			return;
		}
		await new Promise<void>((resolve) => this.commitWaiters.push(resolve));
	}

	/** Transfers a released commit permit to the oldest waiter or returns it to the pool. */
	private releaseCommitSlot(): void {
		const next = this.commitWaiters.shift();
		if (next) {
			next();
			return;
		}
		this.currentCommitCount = Math.max(0, this.currentCommitCount - 1);
	}

	/**
	 * Acquires deterministic whole-stream source leases to serialize mutations.
	 * @param operationId Operation owning the sources.
	 * @param sourceIds Sorted source identifiers.
	 * @returns Idempotent release callbacks for every acquired source.
	 */
	private async acquireSourceLeases(
		operationId: string,
		sourceIds: readonly Source.Id[],
	): Promise<Array<() => void>> {
		const releases: Array<() => void> = [];
		try {
			for (const sourceId of sourceIds) {
				releases.push(
					await this.acquireSourceLease(
						this.createSourceLeaseKey(operationId, sourceId),
					),
				);
			}
			return releases;
		} catch (error) {
			for (const release of releases.reverse()) release();
			throw error;
		}
	}

	/**
	 * Acquires one FIFO source mutation lease locally and, when supported, across
	 * every same-origin realm sharing this coordinator namespace.
	 * @param key Unambiguous operation/source lock key.
	 * @returns Idempotent callback releasing the lease.
	 */
	private async acquireSourceLease(key: string): Promise<() => void> {
		const previous = this.sourceLockTails.get(key) ?? Promise.resolve();
		let releaseGate = (): void => undefined;
		const gate = new Promise<void>((resolve) => {
			releaseGate = resolve;
		});
		const tail = previous.catch(() => undefined).then(() => gate);
		this.sourceLockTails.set(key, tail);
		await previous.catch(() => undefined);
		let releaseCrossRealmLease: () => void;
		try {
			releaseCrossRealmLease = await this.acquireCrossRealmSourceLease(key);
		} catch (error) {
			releaseGate();
			if (this.sourceLockTails.get(key) === tail) {
				void tail.then(() => this.sourceLockTails.delete(key));
			}
			throw error;
		}
		let released = false;
		/** Releases both the cross-realm lock and coordinator-local FIFO gate. */
		const release = (): void => {
			if (released) return;
			released = true;
			releaseCrossRealmLease();
			releaseGate();
			if (this.sourceLockTails.get(key) === tail) {
				void tail.then(() => this.sourceLockTails.delete(key));
			}
		};
		return release;
	}

	/**
	 * Holds one origin-wide exclusive Web Lock until the returned release callback runs.
	 * Browsers without Web Locks retain the existing coordinator-local FIFO behavior.
	 * @param key Unambiguous operation/source lock key.
	 * @returns Idempotent callback releasing the cross-realm lease.
	 */
	private async acquireCrossRealmSourceLease(key: string): Promise<() => void> {
		if (!this.lockManager) return () => undefined;
		const lockName = `gulp-event-store-writer:${encodeURIComponent(
			this.coordinatorId,
		)}:${encodeURIComponent(key)}`;
		let releaseGate = (): void => undefined;
		const gate = new Promise<void>((resolve) => {
			releaseGate = resolve;
		});
		let resolveAcquired = (_release: () => void): void => undefined;
		let rejectAcquired = (_error: Error): void => undefined;
		const acquired = new Promise<() => void>((resolve, reject) => {
			resolveAcquired = resolve;
			rejectAcquired = reject;
		});
		let callbackStarted = false;
		void Promise.resolve()
			.then(() =>
				this.lockManager!.request(
					lockName,
					{ mode: "exclusive" },
						async () => {
							callbackStarted = true;
							let released = false;
							/** Releases the Web Lock callback's pending gate once. */
							const release = (): void => {
								if (released) return;
								released = true;
								releaseGate();
							};
							resolveAcquired(release);
						await gate;
					},
				),
			)
			.catch((error: unknown) => {
				if (callbackStarted) return;
				rejectAcquired(error instanceof Error ? error : new Error(String(error)));
			});
		return acquired;
	}

	/**
	 * Creates an unambiguous local/cross-realm source lease identity.
	 * @param operationId Operation owning the source mutation.
	 * @param sourceId Source whose exact pages may be changed.
	 * @returns JSON tuple safe from delimiter collisions.
	 */
	private createSourceLeaseKey(
		operationId: string,
		sourceId: Source.Id,
	): string {
		return JSON.stringify([operationId, sourceId]);
	}

	/**
	 * Waits for one source lease while allowing a superseded viewport to leave the queue.
	 * A canceled waiter releases its eventual FIFO lease in the background.
	 * @param key Operation/source lock key.
	 * @param isCurrent Newest-request guard.
	 * @returns Lease release callback, or null after supersession.
	 */
	private async acquireSourceLeaseWhileCurrent(
		key: string,
		isCurrent: () => boolean,
	): Promise<(() => void) | null> {
		const ready = this.acquireSourceLease(key);
		let stopPolling = false;
		const canceled = (async (): Promise<null> => {
			while (!stopPolling && isCurrent()) {
				await new Promise<void>((resolve) => setTimeout(resolve, 4));
			}
			return null;
		})();
		const release = await Promise.race([ready, canceled]);
		stopPolling = true;
		if (release) return release;
		void ready.then((lateRelease) => lateRelease());
		return null;
	}

	/**
	 * Converts any failure into a stable main-thread cancellation event.
	 * @param descriptor Stream identity associated with the failure.
	 * @param error Typed or unknown thrown value.
	 * @returns Serializable failure event.
	 */
	private toFailureEvent(
		descriptor: DocumentStreamDescriptor,
		error: unknown,
	): QueryStreamFailureEvent {
		const reason =
			error instanceof QueryPersistenceError
				? error.reason
				: error instanceof EventStoreCapacityTelemetryUnavailableError
					? "capacity-unavailable"
					: "storage";
		return {
			requestId: descriptor.requestId,
			operationId: descriptor.operationId,
			operationEpoch: descriptor.operationEpoch,
			reason,
			message: error instanceof Error ? error.message : String(error),
		};
	}
}
