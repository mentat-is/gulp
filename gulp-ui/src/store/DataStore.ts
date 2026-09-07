import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import type { Highlight } from "@/entities/Highlight";
import type { Link } from "@/entities/Link";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { documentIdentityRegistry } from "@/store/DocumentIdentityRegistry";
import { EventStoreFacade } from "@/event-store/EventStore.facade";
import {
	EventStoreRepository,
	EventStoreRevisionBus,
	IndexedDbEventStoreBackend,
} from "@/event-store";
import type {
	EventSourceSummary,
	EventStoreNamespace,
} from "@/event-store/EventStore.types";

export type DataStoreNotificationMode = "immediate" | "deferred";
export type SourceEventsMutation = "replace" | "upsert" | "clear" | "touch";
export type RenderCollectionMutation = "replace" | "upsert" | "delete";

interface RenderChangeBase {
	operationEpoch: number;
	sequence: number;
}

export interface SourceEventsRenderChange extends RenderChangeBase {
	type: "source-events";
	mutation: SourceEventsMutation;
	sourceId: Source.Id;
	sourceRevision: number;
	events: readonly Doc.Type[];
	changedEvents: readonly Doc.Type[];
}

export interface SupplementalEventsRenderChange extends RenderChangeBase {
	type: "supplemental-events";
	mutation: "upsert" | "clear";
	collectionRevision: number;
	sourceId: Source.Id;
	events: readonly Doc.Type[];
	changedEvents: readonly Doc.Type[];
	removedEventIds: readonly Doc.Id[];
}

export interface NotesRenderChange extends RenderChangeBase {
	type: "notes";
	mutation: RenderCollectionMutation;
	collectionRevision: number;
	notes: readonly Note.Type[];
	changedNotes: readonly Note.Type[];
	removedNoteIds: readonly Note.Id[];
}

export interface NotesSnapshotReadyChange {
	type: "notes-sync";
	mutation: "snapshot-ready";
	operationId: Operation.Id;
}

export interface NotesUpsertSyncChange {
	type: "notes-sync";
	mutation: "create" | "update";
	operationId: Operation.Id;
	sequence: number;
	notes: readonly Note.Type[];
}

export interface NotesDeleteSyncChange {
	type: "notes-sync";
	mutation: "delete";
	operationId: Operation.Id;
	sequence: number;
	noteIds: readonly Note.Id[];
}

export type NotesSyncChange =
	| NotesSnapshotReadyChange
	| NotesUpsertSyncChange
	| NotesDeleteSyncChange;

export type NotesRealtimeSyncChange =
	| NotesUpsertSyncChange
	| NotesDeleteSyncChange;

export type NotesSyncChangeListener = (change: NotesSyncChange) => void;
export type EventSourceSummaryListener = (
	summary: EventSourceSummary,
) => void;

export interface LinksRenderChange extends RenderChangeBase {
	type: "links";
	mutation: RenderCollectionMutation;
	collectionRevision: number;
	links: readonly Link.Type[];
	changedLinks: readonly Link.Type[];
	removedLinkIds: readonly Link.Id[];
}

export interface HighlightsRenderChange extends RenderChangeBase {
	type: "highlights";
	mutation: "replace";
	collectionRevision: number;
	highlights: readonly Highlight.Type[];
}

export interface RenderOperationResetChange extends RenderChangeBase {
	type: "operation-reset";
}

export type DataStoreRenderChange =
	| SourceEventsRenderChange
	| SupplementalEventsRenderChange
	| NotesRenderChange
	| LinksRenderChange
	| HighlightsRenderChange
	| RenderOperationResetChange;

export type DataStoreRenderChangeListener = (
	change: DataStoreRenderChange,
) => void;

type RenderCollectionName = "notes" | "links" | "highlights";
type DataStoreMutationDomain = "documents" | "overlays";

/**
 * Stores canonical timeline entities and publishes both React-compatible version
 * notifications and render-specific, revisioned mutations.
 */
export class DataStore {
	/** Async bounded facade that replaces direct all-event access incrementally. */
	public static readonly Events = new EventStoreFacade({
		resolveById: (documentId) =>
			documentIdentityRegistry.getDocument(documentId),
		resolveSource: (sourceId) => DataStore.events.get(sourceId) ?? [],
		resolveSupplementalById: (documentId) =>
			DataStore.getSupplementalEventById(documentId),
		resolveSupplementalSource: (sourceId) =>
			DataStore.getSupplementalEvents(sourceId),
		resolveSourceRevision: (sourceId) => DataStore.getSourceRevision(sourceId),
	});

	/** Canonical note records used by React surfaces and the renderer projection. */
	public static notes: Note.Type[] = [];

	/** Canonical link records used by React surfaces and the renderer projection. */
	public static links: Link.Type[] = [];

	/** Canonical source documents, grouped for constant-time source lookup. */
	public static events: Map<Source.Id, Doc.Type[]> = new Map();

	/** Explicit note targets rendered outside the active IndexedDB query generation. */
	public static supplementalEvents: Map<Source.Id, Map<Doc.Id, Doc.Type>> =
		new Map();

	/** Canonical highlight records used by React surfaces and the renderer projection. */
	public static highlights: Highlight.Type[] = [];

	/** Canonical custom glyph records. */
	public static glyphs: Glyph.Type[] = [];

	/** Legacy dirty marker retained while the main-thread renderer remains available. */
	public static isDirty = false;

	private static readonly listeners = new Set<() => void>();
	private static readonly documentListeners = new Set<() => void>();
	private static readonly overlayListeners = new Set<() => void>();
	private static readonly renderChangeListeners =
		new Set<DataStoreRenderChangeListener>();
	private static readonly notesSyncChangeListeners =
		new Set<NotesSyncChangeListener>();
	private static readonly eventSourceSummaryListeners =
		new Set<EventSourceSummaryListener>();
	private static readonly sourceRevisions = new Map<Source.Id, number>();
	private static readonly eventSourceSummaries = new Map<
		Source.Id,
		EventSourceSummary
	>();
	private static readonly transientEventSourceIds = new Set<Source.Id>();
	private static readonly collectionRevisions: Record<RenderCollectionName, number> = {
		notes: 0,
		links: 0,
		highlights: 0,
	};
	private static version = 0;
	private static documentVersion = 0;
	private static overlayVersion = 0;
	private static renderSequence = 0;
	private static eventSourceVisibilitySequence = 0;
	private static notesSyncSequence = 0;
	private static supplementalEventsRevision = 0;
	private static operationEpoch = 1;
	private static renderOperationId: Operation.Id | null = null;
	private static eventStoreRepository: EventStoreRepository | null = null;
	private static eventStoreRevisionBus: EventStoreRevisionBus | null = null;
	private static eventStoreNamespaceKey: string | null = null;
	private static pendingDirtyFrame: number | null = null;
	private static pendingDirtyTimeout: ReturnType<typeof setTimeout> | null = null;
	private static pendingDocumentNotification = false;
	private static pendingOverlayNotification = false;

	/**
	 * Subscribes a listener to the versioned external store used by React.
	 * @param listener Callback invoked after canonical data changes.
	 * @returns Function that removes the listener.
	 */
	public static subscribe(listener: () => void): () => void {
		DataStore.listeners.add(listener);
		return () => {
			DataStore.listeners.delete(listener);
		};
	}

	/**
	 * Subscribes to canonical document mutations without receiving overlay changes.
	 * @param listener Callback invoked after a source document collection changes.
	 * @returns Function that removes the listener.
	 */
	public static subscribeDocuments(listener: () => void): () => void {
		DataStore.documentListeners.add(listener);
		return () => {
			DataStore.documentListeners.delete(listener);
		};
	}

	/**
	 * Subscribes to canonical note, link, or highlight mutations only.
	 * @param listener Callback invoked after an overlay collection changes.
	 * @returns Function that removes the listener.
	 */
	public static subscribeOverlays(listener: () => void): () => void {
		DataStore.overlayListeners.add(listener);
		return () => {
			DataStore.overlayListeners.delete(listener);
		};
	}

	/**
	 * Subscribes to compact, discriminated mutations consumed by render backends.
	 * @param listener Callback invoked synchronously for each render mutation.
	 * @returns Function that removes the listener.
	 */
	public static subscribeRenderChanges(
		listener: DataStoreRenderChangeListener,
	): () => void {
		DataStore.renderChangeListeners.add(listener);
		return () => {
			DataStore.renderChangeListeners.delete(listener);
		};
	}

	/**
	 * Subscribes to explicit note snapshot-completion and realtime collaboration changes.
	 * @param listener Callback invoked for semantic note synchronization events.
	 * @returns Function that removes the listener.
	 */
	public static subscribeNotesSyncChanges(
		listener: NotesSyncChangeListener,
	): () => void {
		DataStore.notesSyncChangeListeners.add(listener);
		return () => {
			DataStore.notesSyncChangeListeners.delete(listener);
		};
	}

	/**
	 * Subscribes to compact source commits published by QueryWorker or another realm.
	 * @param listener Callback receiving one source summary without document payloads.
	 * @returns Function that removes the listener.
	 */
	public static subscribeEventSourceSummaries(
		listener: EventSourceSummaryListener,
	): () => void {
		DataStore.eventSourceSummaryListeners.add(listener);
		return () => {
			DataStore.eventSourceSummaryListeners.delete(listener);
		};
	}

	/**
	 * Publishes completion of an authoritative note snapshot without marking it realtime.
	 * @param operationId Operation whose complete note snapshot is now available.
	 * @returns Published snapshot-ready change.
	 */
	public static publishNotesSnapshotReady(
		operationId: Operation.Id,
	): NotesSnapshotReadyChange {
		const change: NotesSnapshotReadyChange = {
			type: "notes-sync",
			mutation: "snapshot-ready",
			operationId,
		};
		DataStore.publishNotesSyncChange(change);
		return change;
	}

	/**
	 * Publishes one sequenced realtime create or update batch.
	 * @param operationId Operation affected by the collaboration notification.
	 * @param mutation Whether notes were created or updated.
	 * @param notes Complete note objects supplied by the websocket notification.
	 * @returns Published realtime change.
	 */
	public static publishNotesUpsertDelta(
		operationId: Operation.Id,
		mutation: "create" | "update",
		notes: readonly Note.Type[],
	): NotesUpsertSyncChange {
		const change: NotesUpsertSyncChange = {
			type: "notes-sync",
			mutation,
			operationId,
			sequence: ++DataStore.notesSyncSequence,
			notes,
		};
		DataStore.publishNotesSyncChange(change);
		return change;
	}

	/**
	 * Publishes one sequenced realtime delete batch.
	 * @param operationId Operation affected by the collaboration notification.
	 * @param noteIds Deleted note identifiers supplied by the websocket notification.
	 * @returns Published realtime change.
	 */
	public static publishNotesDeleteDelta(
		operationId: Operation.Id,
		noteIds: readonly Note.Id[],
	): NotesDeleteSyncChange {
		const change: NotesDeleteSyncChange = {
			type: "notes-sync",
			mutation: "delete",
			operationId,
			sequence: ++DataStore.notesSyncSequence,
			noteIds,
		};
		DataStore.publishNotesSyncChange(change);
		return change;
	}

	/**
	 * Relays a realtime note delta from another browser realm without resequencing it.
	 * @param change Realtime change received through WindowBridge.
	 * @returns Nothing.
	 */
	public static relayNotesRealtimeDelta(
		change: NotesRealtimeSyncChange,
	): void {
		DataStore.notesSyncSequence = Math.max(
			DataStore.notesSyncSequence,
			change.sequence,
		);
		DataStore.publishNotesSyncChange(change);
	}

	/**
	 * Returns the external-store version consumed by React's useSyncExternalStore.
	 * @returns Current monotonically increasing canonical-store version.
	 */
	public static getSnapshot(): number {
		return DataStore.version;
	}

	/**
	 * Returns the document-domain external-store version.
	 * @returns Current monotonically increasing document version.
	 */
	public static getDocumentsSnapshot(): number {
		return DataStore.documentVersion;
	}

	/**
	 * Returns the overlay-domain external-store version.
	 * @returns Current monotonically increasing overlay version.
	 */
	public static getOverlaysSnapshot(): number {
		return DataStore.overlayVersion;
	}

	/**
	 * Returns the epoch that distinguishes messages belonging to different operations.
	 * @returns Current render operation epoch.
	 */
	public static getRenderOperationEpoch(): number {
		return DataStore.operationEpoch;
	}

	/**
	 * Returns the operation identity synchronously associated with the render epoch.
	 * @returns Selected operation id, or null before an operation boundary is declared.
	 */
	public static getRenderOperationId(): Operation.Id | null {
		return DataStore.renderOperationId;
	}

	/**
	 * Returns the last sequence assigned to a render change.
	 * @returns Current global render-change sequence.
	 */
	public static getRenderSequence(): number {
		return DataStore.renderSequence;
	}

	/**
	 * Returns the version of worker-only source visibility hints.
	 * @returns Monotonic visibility sequence used by Canvas source memoization.
	 */
	public static getEventSourceVisibilitySequence(): number {
		return DataStore.eventSourceVisibilitySequence;
	}

	/**
	 * Returns the current event revision for one source.
	 * @param sourceId Source whose revision is requested.
	 * @returns Source-local revision, or zero when the source has not changed.
	 */
	public static getSourceRevision(sourceId: Source.Id): number {
		return DataStore.sourceRevisions.get(sourceId) ?? 0;
	}

	/**
	 * Returns the compact worker/IndexedDB authority for one source.
	 * @param sourceId Source whose loaded count and range are requested.
	 * @returns Latest summary, or undefined before the event-store pipeline publishes one.
	 */
	public static getEventSourceSummary(
		sourceId: Source.Id,
	): EventSourceSummary | undefined {
		return DataStore.eventSourceSummaries.get(sourceId);
	}

	/**
	 * Publishes a compact source commit without retaining canonical documents.
	 * @param summary Source state committed by QueryWorker and IndexedDB.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Stored source summary.
	 */
	public static publishEventSourceSummary(
		summary: EventSourceSummary,
		notificationMode: DataStoreNotificationMode = "deferred",
	): EventSourceSummary {
		const currentRevision = DataStore.getSourceRevision(summary.sourceId);
		const currentSummary = DataStore.eventSourceSummaries.get(summary.sourceId);
		if (
			currentSummary &&
			(summary.authorityRevision < currentSummary.authorityRevision ||
				(summary.authorityRevision === currentSummary.authorityRevision &&
					(summary.generation < currentSummary.generation ||
						(summary.generation === currentSummary.generation &&
							summary.revision < currentRevision))))
		) {
			return currentSummary ?? summary;
		}
		if (
			currentSummary &&
			currentSummary.authorityRevision === summary.authorityRevision &&
			currentSummary.generation === summary.generation &&
			currentSummary.revision === summary.revision &&
			currentSummary.status === summary.status &&
			currentSummary.eventCount === summary.eventCount &&
			currentSummary.minTimestamp === summary.minTimestamp &&
			currentSummary.maxTimestamp === summary.maxTimestamp &&
			currentSummary.hashMin === summary.hashMin &&
			currentSummary.hashMax === summary.hashMax &&
			currentSummary.activeCacheGeneration ===
				summary.activeCacheGeneration &&
			currentSummary.cacheHistoryCursor === summary.cacheHistoryCursor &&
			currentSummary.nextGeneration === summary.nextGeneration &&
			currentSummary.cacheHistory.length === summary.cacheHistory.length &&
			currentSummary.cacheHistory.every(
				(generation, index) => generation === summary.cacheHistory[index],
			)
		) {
			return currentSummary;
		}
		if (DataStore.transientEventSourceIds.delete(summary.sourceId)) {
			DataStore.eventSourceVisibilitySequence++;
		}
		DataStore.eventSourceSummaries.set(summary.sourceId, summary);
		DataStore.sourceRevisions.set(summary.sourceId, summary.revision);
		for (const listener of DataStore.eventSourceSummaryListeners) {
			listener(summary);
		}
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return summary;
	}

	/**
	 * Marks sources with a worker-only partial replacement page as renderable.
	 * @param sourceIds Sources whose current query residual contains events.
	 * @returns True when the transient visibility hint set changed.
	 */
	public static markTransientEventSources(
		sourceIds: readonly Source.Id[],
	): boolean {
		let changed = false;
		for (const sourceId of sourceIds) {
			if (DataStore.transientEventSourceIds.has(sourceId)) continue;
			DataStore.transientEventSourceIds.add(sourceId);
			changed = true;
		}
		if (!changed) return false;
		DataStore.eventSourceVisibilitySequence++;
		DataStore.notifyMutation("deferred", ["documents"]);
		return true;
	}

	/**
	 * Opens the operation-scoped IndexedDB reader shared with QueryWorker and detached realms.
	 * @param namespace Backend/user and operation identity for the exact event cache.
	 * @returns Nothing.
	 */
	public static openEventStoreNamespace(namespace: EventStoreNamespace): void {
		const namespaceKey = `${namespace.cacheNamespaceId}:${namespace.operationId}`;
		if (
			DataStore.eventStoreNamespaceKey === namespaceKey &&
			DataStore.eventStoreRepository
		) {
			return;
		}
		DataStore.closeEventStoreNamespace();
		const repository = new EventStoreRepository(
			new IndexedDbEventStoreBackend(namespace),
		);
		DataStore.eventStoreRepository = repository;
		DataStore.eventStoreNamespaceKey = namespaceKey;
		DataStore.Events.useReader(repository);
		DataStore.eventStoreRevisionBus = new EventStoreRevisionBus(
			namespace,
			(summary) => {
				if (DataStore.eventStoreNamespaceKey !== namespaceKey) return;
				DataStore.publishEventSourceSummary(summary);
			},
		);
	}

	/** Closes the realm-local EventStore connection and bounded document cache. */
	public static closeEventStoreNamespace(): void {
		DataStore.eventStoreRevisionBus?.close();
		DataStore.eventStoreRevisionBus = null;
		DataStore.eventStoreRepository?.close();
		DataStore.eventStoreRepository = null;
		DataStore.eventStoreNamespaceKey = null;
		DataStore.Events.reset();
	}

	/**
	 * Returns the current revision for a render overlay collection.
	 * @param collection Collection whose revision is requested.
	 * @returns Collection-local revision.
	 */
	public static getCollectionRevision(collection: RenderCollectionName): number {
		return DataStore.collectionRevisions[collection];
	}

	/**
	 * Gets a source event array, creating an empty canonical array when absent.
	 * Lazy creation does not emit a mutation because it does not change rendered data.
	 * Read-only render paths should use `peekSourceEvents` so an unqueried source
	 * is not promoted to a loaded-empty source.
	 * @param sourceId Source whose canonical events are requested.
	 * @returns Mutable canonical event array for the source.
	 */
	public static getSourceEvents(sourceId: Source.Id): Doc.Type[] {
		const existingEvents = DataStore.events.get(sourceId);
		if (existingEvents) {
			return existingEvents;
		}

		const emptyEvents: Doc.Type[] = [];
		DataStore.events.set(sourceId, emptyEvents);
		return emptyEvents;
	}

	/**
	 * Reads a source event array without creating an empty canonical collection.
	 * @param sourceId Source whose canonical events are requested.
	 * @returns Existing source events, or undefined when the source has not loaded.
	 */
	public static peekSourceEvents(sourceId: Source.Id): Doc.Type[] | undefined {
		return DataStore.events.get(sourceId);
	}

	/**
	 * Checks whether a source should be considered non-empty for timeline visibility.
	 * @param sourceId Source whose query and supplemental render events are inspected.
	 * @param fallbackTotal Backend-advertised total used before the source has a loaded event collection.
	 * @returns True when supplemental or query events exist, or when the backend fallback is positive before loading.
	 */
	public static hasRenderableSourceEvents(
		sourceId: Source.Id,
		fallbackTotal: number,
	): boolean {
		if (DataStore.hasSupplementalEvents(sourceId)) return true;
		if (DataStore.transientEventSourceIds.has(sourceId)) return true;
		const summary = DataStore.eventSourceSummaries.get(sourceId);
		if (summary) return summary.eventCount > 0;
		const loadedEvents = DataStore.events.get(sourceId);
		if (loadedEvents) {
			return loadedEvents.length > 0;
		}
		return fallbackTotal > 0;
	}

	/**
	 * Checks whether a source has an explicitly included note target.
	 * @param sourceId Source whose transient event collection is inspected.
	 * @returns True when at least one supplemental event is retained.
	 */
	public static hasSupplementalEvents(sourceId: Source.Id): boolean {
		return (DataStore.supplementalEvents.get(sourceId)?.size ?? 0) > 0;
	}

	/**
	 * Returns supplemental events for one source or the complete operation.
	 * @param sourceId Optional source filter.
	 * @returns Snapshot array of explicitly included note targets.
	 */
	public static getSupplementalEvents(sourceId?: Source.Id): Doc.Type[] {
		if (sourceId) {
			return Array.from(DataStore.supplementalEvents.get(sourceId)?.values() ?? []);
		}
		return Array.from(DataStore.supplementalEvents.values()).flatMap((events) =>
			Array.from(events.values()),
		);
	}

	/**
	 * Resolves one supplemental event without consulting IndexedDB.
	 * @param documentId Document identifier to resolve.
	 * @returns Matching transient event, or undefined.
	 */
	public static getSupplementalEventById(
		documentId: Doc.Id,
	): Doc.Type | undefined {
		for (const events of DataStore.supplementalEvents.values()) {
			const event = events.get(documentId);
			if (event) return event;
		}
		return undefined;
	}

	/**
	 * Inserts or replaces explicit note targets without changing query authority.
	 * @param events Normalized minimal documents to retain for this operation session.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Stored unique supplemental events in caller order.
	 */
	public static upsertSupplementalEvents(
		events: readonly Doc.Type[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Doc.Type[] {
		const uniqueEvents = Array.from(
			new Map(events.map((event) => [event._id, event])).values(),
		);
		const changedBySource = new Map<Source.Id, Doc.Type[]>();
		for (const event of uniqueEvents) {
			const sourceId = event["gulp.source_id"];
			const sourceEvents = DataStore.supplementalEvents.get(sourceId) ?? new Map();
			sourceEvents.set(event._id, event);
			DataStore.supplementalEvents.set(sourceId, sourceEvents);
			const changedEvents = changedBySource.get(sourceId) ?? [];
			changedEvents.push(event);
			changedBySource.set(sourceId, changedEvents);
		}
		for (const [sourceId, changedEvents] of changedBySource) {
			DataStore.publishSupplementalEventsChange(
				sourceId,
				"upsert",
				changedEvents,
				[],
			);
		}
		if (changedBySource.size > 0) {
			DataStore.notifyMutation(notificationMode, ["documents"]);
		}
		return uniqueEvents;
	}

	/**
	 * Clears explicit note targets for one source.
	 * @param sourceId Source whose transient events are released.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Removed supplemental events.
	 */
	public static clearSupplementalEvents(
		sourceId: Source.Id,
		notificationMode: DataStoreNotificationMode = "immediate",
	): Doc.Type[] {
		const sourceEvents = DataStore.supplementalEvents.get(sourceId);
		if (!sourceEvents || sourceEvents.size === 0) return [];
		const removedEvents = Array.from(sourceEvents.values());
		DataStore.supplementalEvents.delete(sourceId);
		DataStore.Events.forgetDocuments(removedEvents.map((event) => event._id));
		DataStore.publishSupplementalEventsChange(
			sourceId,
			"clear",
			[],
			removedEvents.map((event) => event._id),
		);
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return removedEvents;
	}

	/**
	 * Replaces every event for a source and publishes a full source revision.
	 * @param sourceId Source whose events are replaced.
	 * @param events Canonical, descending-timestamp event array.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Stored canonical event array.
	 */
	public static replaceSourceEvents(
		sourceId: Source.Id,
		events: Doc.Type[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Doc.Type[] {
		DataStore.events.set(sourceId, events);
		DataStore.publishSourceEventsChange(sourceId, "replace", events);
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return events;
	}

	/**
	 * Inserts or replaces source events by document ID while preserving descending
	 * timestamp order and publishing only the incoming documents as the delta.
	 * @param sourceId Source receiving the documents.
	 * @param changedEvents New or updated documents for the source.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Updated canonical source event array.
	 */
	public static upsertSourceEvents(
		sourceId: Source.Id,
		changedEvents: readonly Doc.Type[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Doc.Type[] {
		if (changedEvents.length === 0) {
			return DataStore.getSourceEvents(sourceId);
		}

		const sourceEvents = DataStore.getSourceEvents(sourceId);
		const incomingEventsById = new Map<Doc.Id, Doc.Type>();
		changedEvents.forEach((event) => incomingEventsById.set(event._id, event));

		for (let index = 0; index < sourceEvents.length; index++) {
			const existingEvent = sourceEvents[index];
			const changedEvent = incomingEventsById.get(existingEvent._id);
			if (!changedEvent) {
				continue;
			}

			sourceEvents[index] = changedEvent;
			incomingEventsById.delete(existingEvent._id);
		}

		incomingEventsById.forEach((event) => sourceEvents.push(event));
		sourceEvents.sort((first, second) =>
			second.gulp_timestamp - first.gulp_timestamp,
		);

		DataStore.publishSourceEventsChange(sourceId, "upsert", changedEvents);
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return sourceEvents;
	}

	/**
	 * Inserts documents that the caller has already proven are new to the canonical
	 * operation. Timestamp-disjoint batches use an O(delta) edge append/prepend;
	 * interleaved batches use one linear merge without an ID scan or full sort.
	 * @param sourceId Source receiving the new documents.
	 * @param newEvents Documents whose IDs are absent from the canonical operation.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Updated canonical source event array in descending timestamp order.
	 */
	public static insertNewSourceEvents(
		sourceId: Source.Id,
		newEvents: readonly Doc.Type[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Doc.Type[] {
		if (newEvents.length === 0) {
			return DataStore.getSourceEvents(sourceId);
		}

		const sourceEvents = DataStore.getSourceEvents(sourceId);
		const sortedIncoming = [...newEvents].sort(
			(left, right) => right.gulp_timestamp - left.gulp_timestamp,
		);
		let nextEvents = sourceEvents;
		if (sourceEvents.length === 0) {
			nextEvents = sortedIncoming;
			DataStore.events.set(sourceId, nextEvents);
		} else if (
			sortedIncoming[sortedIncoming.length - 1].gulp_timestamp >
			sourceEvents[0].gulp_timestamp
		) {
			nextEvents = sortedIncoming.concat(sourceEvents);
			DataStore.events.set(sourceId, nextEvents);
		} else if (
			sortedIncoming[0].gulp_timestamp <
			sourceEvents[sourceEvents.length - 1].gulp_timestamp
		) {
			for (const event of sortedIncoming) sourceEvents.push(event);
		} else {
			nextEvents = DataStore.mergeNewSourceEvents(sourceEvents, sortedIncoming);
			DataStore.events.set(sourceId, nextEvents);
		}

		DataStore.publishSourceEventsChange(sourceId, "upsert", newEvents);
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return nextEvents;
	}

	/**
	 * Clears every event for a source and advances its render revision.
	 * @param sourceId Source whose events are cleared.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns New empty canonical event array.
	 */
	public static clearSourceEvents(
		sourceId: Source.Id,
		notificationMode: DataStoreNotificationMode = "immediate",
	): Doc.Type[] {
		const emptyEvents: Doc.Type[] = [];
		DataStore.events.set(sourceId, emptyEvents);
		DataStore.publishSourceEventsChange(sourceId, "clear", emptyEvents);
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return emptyEvents;
	}

	/**
	 * Publishes an event revision after an optimized caller mutates canonical events
	 * in place. This keeps specialized ingestion paths observable without re-copying data.
	 * @param sourceId Source whose canonical events changed.
	 * @param changedEvents Documents changed by the caller; defaults to the full source.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Current canonical source event array.
	 */
	public static touchSourceEvents(
		sourceId: Source.Id,
		changedEvents: readonly Doc.Type[] = DataStore.getSourceEvents(sourceId),
		notificationMode: DataStoreNotificationMode = "deferred",
	): Doc.Type[] {
		const sourceEvents = DataStore.getSourceEvents(sourceId);
		DataStore.publishSourceEventsChange(sourceId, "touch", changedEvents);
		DataStore.notifyMutation(notificationMode, ["documents"]);
		return sourceEvents;
	}

	/**
	 * Linearly merges descending existing and new event arrays. Existing events win
	 * timestamp ties to preserve the stable ordering of the former append-and-sort path.
	 * @param existingEvents Canonical source events sorted newest first.
	 * @param newEvents New source events sorted newest first.
	 * @returns Newly allocated descending canonical array.
	 */
	private static mergeNewSourceEvents(
		existingEvents: readonly Doc.Type[],
		newEvents: readonly Doc.Type[],
	): Doc.Type[] {
		const mergedEvents = new Array<Doc.Type>(
			existingEvents.length + newEvents.length,
		);
		let existingIndex = 0;
		let newIndex = 0;
		for (let outputIndex = 0; outputIndex < mergedEvents.length; outputIndex++) {
			const existingEvent = existingEvents[existingIndex];
			const newEvent = newEvents[newIndex];
			if (
				newEvent &&
				(!existingEvent ||
					newEvent.gulp_timestamp > existingEvent.gulp_timestamp)
			) {
				mergedEvents[outputIndex] = newEvent;
				newIndex++;
			} else {
				mergedEvents[outputIndex] = existingEvent;
				existingIndex++;
			}
		}
		return mergedEvents;
	}

	/**
	 * Replaces the canonical notes collection and publishes its new revision.
	 * @param notes Complete canonical notes collection.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Stored notes collection.
	 */
	public static replaceNotes(
		notes: readonly Note.Type[],
		notificationMode: DataStoreNotificationMode = "deferred",
	): Note.Type[] {
		DataStore.notes = [...notes];
		DataStore.publishNotesChange("replace", DataStore.notes, []);
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return DataStore.notes;
	}

	/**
	 * Inserts or replaces one canonical note and publishes its collection revision.
	 * @param note Note to insert or replace by ID.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Previous note when replaced, otherwise undefined.
	 */
	public static upsertNote(
		note: Note.Type,
		notificationMode: DataStoreNotificationMode = "deferred",
	): Note.Type | undefined {
		const existingIndex = DataStore.notes.findIndex((item) => item.id === note.id);
		const previousNote =
			existingIndex === -1 ? undefined : DataStore.notes[existingIndex];

		if (existingIndex === -1) {
			DataStore.notes.push(note);
		} else {
			DataStore.notes[existingIndex] = note;
		}

		DataStore.publishNotesChange("upsert", [note], []);
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return previousNote;
	}

	/**
	 * Deletes one canonical note and publishes its collection revision when found.
	 * @param noteId Identifier of the note to delete.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Deleted note, or undefined when no note matched.
	 */
	public static deleteNote(
		noteId: Note.Id,
		notificationMode: DataStoreNotificationMode = "deferred",
	): Note.Type | undefined {
		return DataStore.deleteNotes([noteId], notificationMode)[0];
	}

	/**
	 * Deletes canonical notes and emits one coalesced collection mutation.
	 * @param noteIds Identifiers of notes to delete.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Notes removed from the canonical collection.
	 */
	public static deleteNotes(
		noteIds: readonly Note.Id[],
		notificationMode: DataStoreNotificationMode = "deferred",
	): Note.Type[] {
		if (noteIds.length === 0) {
			return [];
		}

		const idsToDelete = new Set(noteIds);
		const removedNotes = DataStore.notes.filter((note) => idsToDelete.has(note.id));
		if (removedNotes.length === 0) {
			return [];
		}

		DataStore.notes = DataStore.notes.filter((note) => !idsToDelete.has(note.id));
		DataStore.publishNotesChange(
			"delete",
			[],
			removedNotes.map((note) => note.id),
		);
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return removedNotes;
	}

	/**
	 * Replaces the canonical links collection and publishes its new revision.
	 * @param links Complete canonical links collection.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Stored links collection.
	 */
	public static replaceLinks(
		links: readonly Link.Type[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Link.Type[] {
		DataStore.links = [...links];
		DataStore.publishLinksChange("replace", DataStore.links, []);
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return DataStore.links;
	}

	/**
	 * Inserts or replaces one canonical link and publishes its collection revision.
	 * @param link Link to insert or replace by ID.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Previous link when replaced, otherwise undefined.
	 */
	public static upsertLink(
		link: Link.Type,
		notificationMode: DataStoreNotificationMode = "immediate",
	): Link.Type | undefined {
		const existingIndex = DataStore.links.findIndex((item) => item.id === link.id);
		const previousLink =
			existingIndex === -1 ? undefined : DataStore.links[existingIndex];

		if (existingIndex === -1) {
			DataStore.links.push(link);
		} else {
			DataStore.links[existingIndex] = link;
		}

		DataStore.publishLinksChange("upsert", [link], []);
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return previousLink;
	}

	/**
	 * Deletes one canonical link and publishes its collection revision when found.
	 * @param linkId Identifier of the link to delete.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Deleted link, or undefined when no link matched.
	 */
	public static deleteLink(
		linkId: Link.Id,
		notificationMode: DataStoreNotificationMode = "immediate",
	): Link.Type | undefined {
		return DataStore.deleteLinks([linkId], notificationMode)[0];
	}

	/**
	 * Deletes canonical links and emits one coalesced collection mutation.
	 * @param linkIds Identifiers of links to delete.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Links removed from the canonical collection.
	 */
	public static deleteLinks(
		linkIds: readonly Link.Id[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Link.Type[] {
		if (linkIds.length === 0) {
			return [];
		}

		const idsToDelete = new Set(linkIds);
		const removedLinks = DataStore.links.filter((link) =>
			idsToDelete.has(link.id),
		);
		if (removedLinks.length === 0) {
			return [];
		}

		DataStore.links = DataStore.links.filter(
			(link) => !idsToDelete.has(link.id),
		);
		DataStore.publishLinksChange(
			"delete",
			[],
			removedLinks.map((link) => link.id),
		);
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return removedLinks;
	}

	/**
	 * Replaces the canonical highlights collection and publishes its new revision.
	 * @param highlights Complete canonical highlights collection.
	 * @param notificationMode React subscription notification scheduling mode.
	 * @returns Stored highlights collection.
	 */
	public static replaceHighlights(
		highlights: readonly Highlight.Type[],
		notificationMode: DataStoreNotificationMode = "immediate",
	): Highlight.Type[] {
		DataStore.highlights = [...highlights];
		DataStore.collectionRevisions.highlights++;
		DataStore.publishRenderChange({
			type: "highlights",
			mutation: "replace",
			operationEpoch: DataStore.operationEpoch,
			sequence: DataStore.nextRenderSequence(),
			collectionRevision: DataStore.collectionRevisions.highlights,
			highlights: DataStore.highlights,
		});
		DataStore.notifyMutation(notificationMode, ["overlays"]);
		return DataStore.highlights;
	}

	/**
	 * Advances the operation epoch and clears canonical render collections,
	 * revisions, and document handle registries in one observable boundary.
	 * @param operationId Operation selected at this synchronous reset boundary.
	 * @returns New render operation epoch.
	 */
	public static resetRenderOperation(
		operationId: Operation.Id | null = null,
	): number {
		DataStore.closeEventStoreNamespace();
		DataStore.operationEpoch++;
		DataStore.renderOperationId = operationId;
		DataStore.events.clear();
		DataStore.supplementalEvents.clear();
		DataStore.notes = [];
		DataStore.links = [];
		DataStore.highlights = [];
		DataStore.sourceRevisions.clear();
		DataStore.eventSourceSummaries.clear();
		DataStore.transientEventSourceIds.clear();
		DataStore.collectionRevisions.notes = 0;
		DataStore.collectionRevisions.links = 0;
		DataStore.collectionRevisions.highlights = 0;
		DataStore.supplementalEventsRevision = 0;
		documentIdentityRegistry.reset();
		DataStore.publishRenderChange({
			type: "operation-reset",
			operationEpoch: DataStore.operationEpoch,
			sequence: DataStore.nextRenderSequence(),
		});
		DataStore.notifyMutation("immediate", ["documents", "overlays"]);
		return DataStore.operationEpoch;
	}

	/**
	 * Returns a stable, source-local uint32 handle for one document.
	 * @param sourceId Source that owns the document.
	 * @param documentId Canonical document identifier.
	 * @returns Stable non-zero uint32 handle for the current operation epoch.
	 */
	public static getRenderHandle(
		sourceId: Source.Id,
		documentId: Doc.Id,
	): number {
		return documentIdentityRegistry.getRenderHandle(sourceId, documentId);
	}

	/**
	 * Projects canonical document IDs into a transferable uint32 handle buffer.
	 * @param sourceId Source that owns every supplied document.
	 * @param documents Documents whose handles are requested.
	 * @param startIndex Inclusive document index to project.
	 * @param endIndex Exclusive document index to project.
	 * @returns Newly allocated handle buffer safe to transfer to a worker.
	 */
	public static createRenderHandleBuffer(
		sourceId: Source.Id,
		documents: readonly Doc.Type[],
		startIndex = 0,
		endIndex = documents.length,
	): Uint32Array {
		const safeStartIndex = Math.max(0, Math.min(documents.length, startIndex));
		const safeEndIndex = Math.max(
			safeStartIndex,
			Math.min(documents.length, endIndex),
		);
		const handles = new Uint32Array(safeEndIndex - safeStartIndex);
		DataStore.writeRenderHandleBuffer(
			sourceId,
			documents,
			handles,
			safeStartIndex,
			safeEndIndex,
		);
		return handles;
	}

	/**
	 * Writes stable handles for one document range into an existing typed buffer.
	 * The shared identity registry resolves the source once while cooperative
	 * projection retains bounded slice sizes.
	 * @param sourceId Source that owns every supplied document.
	 * @param documents Canonical documents containing stable IDs.
	 * @param target Destination handle buffer.
	 * @param startIndex Inclusive document index to project.
	 * @param endIndex Exclusive document index to project.
	 * @param targetOffset Destination index corresponding to startIndex.
	 * @returns Nothing.
	 */
	public static writeRenderHandleBuffer(
		sourceId: Source.Id,
		documents: readonly Doc.Type[],
		target: Uint32Array,
		startIndex: number,
		endIndex: number,
		targetOffset = 0,
	): void {
		documentIdentityRegistry.writeRenderHandles(
			sourceId,
			documents,
			target,
			startIndex,
			endIndex,
			targetOffset,
		);
	}

	/**
	 * Signals that canonical data changed without publishing a render-data mutation.
	 * Use a typed mutation API for changes that affect worker-owned render data.
	 * @returns Nothing.
	 */
	public static markDirty(): void {
		DataStore.cancelScheduledDirty();
		DataStore.notifyDirtyListeners();
	}

	/**
	 * Coalesces external-store notifications to the next animation frame.
	 * @returns Nothing.
	 */
	public static markDirtySoon(): void {
		DataStore.isDirty = true;
		if (
			DataStore.pendingDirtyFrame !== null ||
			DataStore.pendingDirtyTimeout !== null
		) {
			return;
		}

		const notify = (): void => {
			DataStore.pendingDirtyFrame = null;
			DataStore.pendingDirtyTimeout = null;
			DataStore.notifyDirtyListeners();
		};

		if (
			typeof window !== "undefined" &&
			typeof window.requestAnimationFrame === "function"
		) {
			DataStore.pendingDirtyFrame = window.requestAnimationFrame(notify);
			return;
		}

		DataStore.pendingDirtyTimeout = setTimeout(notify, 0);
	}

	/**
	 * Publishes a source mutation with a new source-local revision.
	 * @param sourceId Source whose events changed.
	 * @param mutation Kind of canonical event mutation.
	 * @param changedEvents Documents included in the mutation delta.
	 * @returns Nothing.
	 */
	private static publishSourceEventsChange(
		sourceId: Source.Id,
		mutation: SourceEventsMutation,
		changedEvents: readonly Doc.Type[],
	): void {
		const sourceRevision = DataStore.getSourceRevision(sourceId) + 1;
		DataStore.sourceRevisions.set(sourceId, sourceRevision);
		DataStore.publishRenderChange({
			type: "source-events",
			mutation,
			operationEpoch: DataStore.operationEpoch,
			sequence: DataStore.nextRenderSequence(),
			sourceId,
			sourceRevision,
			events: DataStore.getSourceEvents(sourceId),
			changedEvents,
		});
	}

	/**
	 * Publishes one source-scoped supplemental-event collection mutation.
	 * @param sourceId Source whose transient note targets changed.
	 * @param mutation Upsert or clear operation.
	 * @param changedEvents Events added or replaced by the mutation.
	 * @param removedEventIds Event IDs released by the mutation.
	 * @returns Nothing.
	 */
	private static publishSupplementalEventsChange(
		sourceId: Source.Id,
		mutation: "upsert" | "clear",
		changedEvents: readonly Doc.Type[],
		removedEventIds: readonly Doc.Id[],
	): void {
		DataStore.supplementalEventsRevision++;
		DataStore.publishRenderChange({
			type: "supplemental-events",
			mutation,
			operationEpoch: DataStore.operationEpoch,
			sequence: DataStore.nextRenderSequence(),
			collectionRevision: DataStore.supplementalEventsRevision,
			sourceId,
			events: DataStore.getSupplementalEvents(),
			changedEvents,
			removedEventIds,
		});
	}

	/**
	 * Publishes a notes mutation with a new collection revision.
	 * @param mutation Kind of canonical notes mutation.
	 * @param changedNotes Notes inserted, replaced, or included in a snapshot.
	 * @param removedNoteIds Note identifiers removed by the mutation.
	 * @returns Nothing.
	 */
	private static publishNotesChange(
		mutation: RenderCollectionMutation,
		changedNotes: readonly Note.Type[],
		removedNoteIds: readonly Note.Id[],
	): void {
		DataStore.collectionRevisions.notes++;
		DataStore.publishRenderChange({
			type: "notes",
			mutation,
			operationEpoch: DataStore.operationEpoch,
			sequence: DataStore.nextRenderSequence(),
			collectionRevision: DataStore.collectionRevisions.notes,
			notes: DataStore.notes,
			changedNotes,
			removedNoteIds,
		});
	}

	/**
	 * Publishes a links mutation with a new collection revision.
	 * @param mutation Kind of canonical links mutation.
	 * @param changedLinks Links inserted, replaced, or included in a snapshot.
	 * @param removedLinkIds Link identifiers removed by the mutation.
	 * @returns Nothing.
	 */
	private static publishLinksChange(
		mutation: RenderCollectionMutation,
		changedLinks: readonly Link.Type[],
		removedLinkIds: readonly Link.Id[],
	): void {
		DataStore.collectionRevisions.links++;
		DataStore.publishRenderChange({
			type: "links",
			mutation,
			operationEpoch: DataStore.operationEpoch,
			sequence: DataStore.nextRenderSequence(),
			collectionRevision: DataStore.collectionRevisions.links,
			links: DataStore.links,
			changedLinks,
			removedLinkIds,
		});
	}

	/**
	 * Notifies every render listener synchronously so transferable projections can
	 * be created before later canonical mutations alter the referenced arrays.
	 * @param change Revisioned discriminated render mutation.
	 * @returns Nothing.
	 */
	private static publishRenderChange(change: DataStoreRenderChange): void {
		DataStore.renderChangeListeners.forEach((listener) => listener(change));
	}

	/**
	 * Delivers one semantic note synchronization change without touching render state.
	 * @param change Snapshot-completion or realtime note delta.
	 * @returns Nothing.
	 */
	private static publishNotesSyncChange(change: NotesSyncChange): void {
		DataStore.notesSyncChangeListeners.forEach((listener) => listener(change));
	}

	/**
	 * Returns the next globally ordered render mutation sequence.
	 * @returns Incremented global render sequence.
	 */
	private static nextRenderSequence(): number {
		DataStore.renderSequence++;
		return DataStore.renderSequence;
	}

	/**
	 * Applies the requested React subscription notification policy.
	 * @param notificationMode Immediate or animation-frame-coalesced notification.
	 * @returns Nothing.
	 */
	private static notifyMutation(
		notificationMode: DataStoreNotificationMode,
		domains: readonly DataStoreMutationDomain[],
	): void {
		DataStore.pendingDocumentNotification ||= domains.includes("documents");
		DataStore.pendingOverlayNotification ||= domains.includes("overlays");
		if (notificationMode === "deferred") {
			DataStore.markDirtySoon();
			return;
		}

		DataStore.markDirty();
	}

	/**
	 * Cancels a deferred dirty notification before an immediate notification.
	 * @returns Nothing.
	 */
	private static cancelScheduledDirty(): void {
		if (DataStore.pendingDirtyFrame !== null) {
			if (
				typeof window !== "undefined" &&
				typeof window.cancelAnimationFrame === "function"
			) {
				window.cancelAnimationFrame(DataStore.pendingDirtyFrame);
			}
			DataStore.pendingDirtyFrame = null;
		}

		if (DataStore.pendingDirtyTimeout !== null) {
			clearTimeout(DataStore.pendingDirtyTimeout);
			DataStore.pendingDirtyTimeout = null;
		}
	}

	/**
	 * Increments the external-store version and notifies React subscribers.
	 * @returns Nothing.
	 */
	private static notifyDirtyListeners(): void {
		DataStore.isDirty = true;
		DataStore.version++;
		DataStore.listeners.forEach((listener) => listener());

		if (DataStore.pendingDocumentNotification) {
			DataStore.pendingDocumentNotification = false;
			DataStore.documentVersion++;
			DataStore.documentListeners.forEach((listener) => listener());
		}
		if (DataStore.pendingOverlayNotification) {
			DataStore.pendingOverlayNotification = false;
			DataStore.overlayVersion++;
			DataStore.overlayListeners.forEach((listener) => listener());
		}
	}
}
