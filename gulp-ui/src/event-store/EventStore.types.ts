import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";

export const EVENT_PAGE_SIZE = 16_384;
export const EVENT_PAGE_LOD_BLOCKS = 256;
export const EVENT_PIXEL_PAGE_SIZE = 100;
export const EVENT_MAIN_CACHE_SIZE = 2_048;
export const EVENT_PENDING_DOCUMENT_LIMIT = 32_768;
export const EVENT_RENDER_REPRESENTATIVE_LIMIT = 2_000_000;
export const EVENT_RENDER_PROJECTION_LIMIT =
	EVENT_RENDER_REPRESENTATIVE_LIMIT / 2;
export const EVENT_DOCUMENT_HASH_PREFIX_BITS = 12;
export const EVENT_MAXIMUM_SOURCE_HANDLE = 0xffff_fffe;
export const EVENT_PAGE_LOD_RECORD_KIND = "lod";
export const EVENT_PAGE_HEIGHT_RECORD_KIND = "height";

/** Identifies one same-origin event database shared by related browser realms. */
export interface EventStoreNamespace {
	coordinatorId: string;
	operationId: Operation.Id;
}

/** Minimal input retained for one exact timeline event. */
export interface EventStoreDocument extends Doc.Type {}

/** Columnar exact-event page persisted as one IndexedDB value. */
export interface EncodedEventPage {
	sourceId: Source.Id;
	generation: number;
	pageId: number;
	revision: number;
	count: number;
	activeCount: number;
	minTimestamp: number;
	maxTimestamp: number;
	hashMin: number;
	hashMax: number;
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	eventCodes: Float64Array;
	idOffsets: Uint32Array;
	idBytes: Uint8Array;
	tombstones: Uint8Array;
	encodedByteLength: number;
}

/** One coarse page entry retained in a source manifest without loading page data. */
export interface EventPageCatalogEntry {
	pageId: number;
	generation: number;
	revision: number;
	count: number;
	activeCount: number;
	minTimestamp: number;
	maxTimestamp: number;
	hashMin: number;
	hashMax: number;
	minimumHandle: number;
	maximumHandle: number;
	representativeHandle: number;
	representativeTimestamp: number;
	representativeHash: number;
	encodedByteLength: number;
}

/** Fixed-size micro summaries used to refine viewport projections without decoding IDs. */
export interface EncodedEventPageLod {
	sourceId: Source.Id;
	generation: number;
	pageId: number;
	revision: number;
	blockCount: number;
	startOffsets: Uint32Array;
	endOffsets: Uint32Array;
	counts: Uint32Array;
	minTimestamps: Float64Array;
	maxTimestamps: Float64Array;
	representativeHandles: Uint32Array;
	representativeTimestamps: Float64Array;
	representativeHashes: Float64Array;
	overrideRevision: number;
	overrideSignature: string;
	pageOverrideHandle: number;
	pageOverrideTimestamp: number;
	pageOverrideHash: number;
	overrideBlockHandles: Uint32Array;
	overrideBlockTimestamps: Float64Array;
	overrideBlockHashes: Float64Array;
	encodedByteLength: number;
}

/** Page-local minute counts stored separately from default-render microblocks. */
export interface EncodedEventPageHeight {
	sourceId: Source.Id;
	generation: number;
	pageId: number;
	revision: number;
	minuteTimestamps: Float64Array;
	minuteCounts: Uint32Array;
	encodedByteLength: number;
}

/** Compact source authority stored independently from exact event pages. */
export interface EventSourceManifest {
	sourceId: Source.Id;
	sourceOrdinal: number;
	generation: number;
	revision: number;
	status: "building" | "complete" | "incomplete";
	eventCount: number;
	nextHandle: number;
	minTimestamp: number;
	maxTimestamp: number;
	hashMin: number;
	hashMax: number;
	encodedBytes: number;
	pages: EventPageCatalogEntry[];
}

/** Stable page cursor used by bounded pixel and navigation reads. */
export interface EventPageCursor {
	sourceId: Source.Id;
	sourceRevision: number;
	timestamp: number;
	documentId: Doc.Id;
}

/** Exact, bounded result for one Canvas pixel interval. */
export interface EventPixelPage {
	events: Doc.Type[];
	total: number;
	nextCursor: EventPageCursor | null;
	sourceRevision: number;
}

/** Serializable source-level update sent from QueryWorker to browser clients. */
export interface EventSourceSummary {
	sourceId: Source.Id;
	revision: number;
	status: EventSourceManifest["status"];
	eventCount: number;
	minTimestamp: number;
	maxTimestamp: number;
	hashMin: number;
	hashMax: number;
}
