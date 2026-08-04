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

/** Bounded canonical neighbor window in descending timestamp order. */
export interface EventNeighborWindow {
	events: Doc.Type[];
	currentIndex: number;
	sourceRevision: number;
}

/** Read-only contract shared by QueryWorker, main, and detached realms. */
export interface EventStoreReader {
	getSourceSummary(sourceId: Source.Id): Promise<EventSourceSummary | null>;
	getById(documentId: Doc.Id, sourceId?: Source.Id): Promise<Doc.Type | null>;
	getByIds(documentIds: readonly Doc.Id[]): Promise<Doc.Type[]>;
	getByHandle(sourceId: Source.Id, handle: number): Promise<Doc.Type | null>;
	getNeighbors(
		sourceId: Source.Id,
		documentIdOrHandle: Doc.Id | number,
		before: number,
		after: number,
	): Promise<EventNeighborWindow>;
	queryPixelBucket(
		sourceId: Source.Id,
		range: EventTimestampRange,
		limit: number,
		cursor?: EventPageCursor,
	): Promise<EventPixelPage>;
}
