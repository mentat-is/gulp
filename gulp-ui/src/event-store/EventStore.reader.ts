import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import type {
	EventPageCursor,
	EventPixelPage,
	EventSourceSummary,
} from "./EventStore.types";

/** Inclusive timestamp bounds used by exact event-store range reads. */
export interface EventTimestampRange {
	min: number;
	max: number;
}

/** One canonical source edge qualified by the source revision that selected it. */
export interface EventSourceEdge {
	event: Doc.Type | null;
	sourceRevision: number;
}

/** Bounded canonical neighbor window in descending timestamp order. */
export interface EventNeighborWindow {
	events: Doc.Type[];
	currentIndex: number;
	sourceRevision: number;
}

/** Bounded query events surrounding a timestamp that is not necessarily persisted. */
export interface EventTimestampNeighborWindow {
	newer: Doc.Type[];
	older: Doc.Type[];
	sourceRevision: number;
}

/** Read-only contract shared by QueryWorker, main, and detached realms. */
export interface EventStoreReader {
	getSourceSummary(sourceId: Source.Id): Promise<EventSourceSummary | null>;
	getSourceEdge(
		sourceId: Source.Id,
		newest: boolean,
	): Promise<EventSourceEdge>;
	getById(documentId: Doc.Id, sourceId?: Source.Id): Promise<Doc.Type | null>;
	getByIds(documentIds: readonly Doc.Id[]): Promise<Doc.Type[]>;
	getByHandle(sourceId: Source.Id, handle: number): Promise<Doc.Type | null>;
	getNeighbors(
		sourceId: Source.Id,
		documentIdOrHandle: Doc.Id | number,
		before: number,
		after: number,
	): Promise<EventNeighborWindow>;
	getTimestampNeighbors(
		sourceId: Source.Id,
		timestamp: number,
		before: number,
		after: number,
	): Promise<EventTimestampNeighborWindow>;
	queryPixelBucket(
		sourceId: Source.Id,
		range: EventTimestampRange,
		limit: number,
		cursor?: EventPageCursor,
	): Promise<EventPixelPage>;
}
