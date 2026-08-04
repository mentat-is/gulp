import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import {
	EVENT_DOCUMENT_HASH_PREFIX_BITS,
	EVENT_PAGE_LOD_BLOCKS,
	EVENT_PAGE_SIZE,
	type EncodedEventPage,
	type EncodedEventPageHeight,
	type EncodedEventPageLod,
	type EventPageCatalogEntry,
} from "./EventStore.types";

export interface EncodeEventPageInput {
	sourceId: Source.Id;
	generation: number;
	pageId: number;
	revision: number;
	documents: readonly Doc.Type[];
	handles?: Uint32Array;
}

export interface DocumentHash64 {
	high: number;
	low: number;
	prefix: number;
}

export interface BuildEventPageLodOptions {
	overrideHashes?: ReadonlySet<number>;
	overrideRevision?: number;
	overrideSignature?: string;
}

/**
 * Calculates two independent 32-bit FNV-style hashes for compact ID directory lookup.
 * The exact ID stored in the event page must still be checked after a hash match.
 * @param documentId Canonical document identifier.
 * @returns Unsigned 64-bit pair plus its configured directory prefix.
 */
export function hashEventDocumentId(documentId: Doc.Id): DocumentHash64 {
	let high = 0x811c9dc5;
	let low = 0x9e3779b9;
	for (let index = 0; index < documentId.length; index++) {
		const code = documentId.charCodeAt(index);
		high ^= code;
		high = Math.imul(high, 0x01000193);
		low ^= code + index;
		low = Math.imul(low, 0x85ebca6b);
		low ^= low >>> 13;
	}
	high >>>= 0;
	low >>>= 0;
	return {
		high,
		low,
		prefix: high >>> (32 - EVENT_DOCUMENT_HASH_PREFIX_BITS),
	};
}

/**
 * Returns the exact byte footprint of typed arrays owned by one event page.
 * @param page Encoded page whose buffers are measured.
 * @returns Sum of ArrayBuffer byte lengths, excluding IndexedDB metadata overhead.
 */
export function getEncodedEventPageByteLength(page: {
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	eventCodes: Float64Array;
	idOffsets: Uint32Array;
	idBytes: Uint8Array;
	tombstones: Uint8Array;
}): number {
	return (
		page.handles.byteLength +
		page.timestamps.byteLength +
		page.hashes.byteLength +
		page.eventCodes.byteLength +
		page.idOffsets.byteLength +
		page.idBytes.byteLength +
		page.tombstones.byteLength
	);
}

/**
 * Encodes a bounded, descending-timestamp source page into transferable columns.
 * @param input Page identity, canonical documents, and optional preassigned handles.
 * @returns Exact event page ready for IndexedDB structured cloning.
 */
export function encodeEventPage(input: EncodeEventPageInput): EncodedEventPage {
	if (input.documents.length > EVENT_PAGE_SIZE) {
		throw new RangeError(`Event page exceeds ${EVENT_PAGE_SIZE} documents.`);
	}
	if (input.handles && input.handles.length !== input.documents.length) {
		throw new RangeError("Event page handle count does not match document count.");
	}

	const winningInputIndexes = new Map<Doc.Id, number>();
	for (let index = 0; index < input.documents.length; index++) {
		winningInputIndexes.set(input.documents[index]._id, index);
	}
	const entries = input.documents
		.flatMap((document, originalIndex) =>
			winningInputIndexes.get(document._id) === originalIndex
				? [
						{
							document,
							handle: input.handles?.[originalIndex] ?? originalIndex + 1,
						},
					]
				: [],
		)
		.sort(
			(left, right) =>
				right.document.gulp_timestamp - left.document.gulp_timestamp,
		);
	const count = entries.length;
	const handles = new Uint32Array(count);
	const timestamps = new Float64Array(count);
	const hashes = new Float64Array(count);
	const eventCodes = new Float64Array(count);
	const idOffsets = new Uint32Array(count + 1);
	const tombstones = new Uint8Array(count);
	const encodedIds = new Array<Uint8Array>(count);
	const encoder = new TextEncoder();
	let idByteLength = 0;
	let minTimestamp = Number.POSITIVE_INFINITY;
	let maxTimestamp = Number.NEGATIVE_INFINITY;
	let hashMin = Number.POSITIVE_INFINITY;
	let hashMax = Number.NEGATIVE_INFINITY;

	for (let index = 0; index < count; index++) {
		const { document, handle } = entries[index];
		if (document["gulp.source_id"] !== input.sourceId) {
			throw new Error("Event page cannot contain documents from another source.");
		}
		const encodedId = encoder.encode(document._id);
		encodedIds[index] = encodedId;
		idOffsets[index] = idByteLength;
		idByteLength += encodedId.byteLength;
		handles[index] = handle;
		timestamps[index] = document.gulp_timestamp;
		hashes[index] = document.number_hash;
		eventCodes[index] = document["gulp.event_code"];
		minTimestamp = Math.min(minTimestamp, document.gulp_timestamp);
		maxTimestamp = Math.max(maxTimestamp, document.gulp_timestamp);
		hashMin = Math.min(hashMin, document.number_hash);
		hashMax = Math.max(hashMax, document.number_hash);
	}
	idOffsets[count] = idByteLength;
	const idBytes = new Uint8Array(idByteLength);
	for (let index = 0; index < count; index++) {
		idBytes.set(encodedIds[index], idOffsets[index]);
	}

	const page: EncodedEventPage = {
		sourceId: input.sourceId,
		generation: input.generation,
		pageId: input.pageId,
		revision: input.revision,
		count,
		activeCount: count,
		minTimestamp: count === 0 ? 0 : minTimestamp,
		maxTimestamp: count === 0 ? 0 : maxTimestamp,
		hashMin: count === 0 ? 0 : hashMin,
		hashMax: count === 0 ? 0 : hashMax,
		handles,
		timestamps,
		hashes,
		eventCodes,
		idOffsets,
		idBytes,
		tombstones,
		encodedByteLength: 0,
	};
	page.encodedByteLength = getEncodedEventPageByteLength(page);
	return page;
}

/**
 * Decodes one canonical document from a page without materializing its siblings.
 * @param page Exact event page containing the requested offset.
 * @param offset Zero-based page offset.
 * @returns Canonical event, or undefined for an invalid/tombstoned offset.
 */
export function decodeEventAt(
	page: EncodedEventPage,
	offset: number,
): Doc.Type | undefined {
	if (
		!Number.isInteger(offset) ||
		offset < 0 ||
		offset >= page.count ||
		page.tombstones[offset] !== 0
	) {
		return undefined;
	}
	const idStart = page.idOffsets[offset];
	const idEnd = page.idOffsets[offset + 1];
	const documentId = new TextDecoder().decode(
		page.idBytes.subarray(idStart, idEnd),
	);
	return {
		_id: documentId as Doc.Id,
		gulp_timestamp: page.timestamps[offset],
		"gulp.source_id": page.sourceId,
		"gulp.event_code": page.eventCodes[offset],
		number_hash: page.hashes[offset],
	};
}

/**
 * Builds fixed micro summaries for viewport LOD refinement.
 * @param page Exact source page sorted by descending timestamp.
 * @returns Page-local LOD columns and their encoded size.
 */
export function buildEventPageLod(
	page: EncodedEventPage,
	options: BuildEventPageLodOptions = {},
): EncodedEventPageLod {
	const logicalBlockSize = Math.max(
		1,
		Math.ceil(page.count / EVENT_PAGE_LOD_BLOCKS),
	);
	const blockCount = Math.ceil(page.count / logicalBlockSize);
	const startOffsets = new Uint32Array(blockCount);
	const endOffsets = new Uint32Array(blockCount);
	const counts = new Uint32Array(blockCount);
	const minTimestamps = new Float64Array(blockCount);
	const maxTimestamps = new Float64Array(blockCount);
	const representativeHandles = new Uint32Array(blockCount);
	const representativeTimestamps = new Float64Array(blockCount);
	const representativeHashes = new Float64Array(blockCount);
	const overrideBlockHandles = new Uint32Array(blockCount);
	const overrideBlockTimestamps = new Float64Array(blockCount);
	const overrideBlockHashes = new Float64Array(blockCount);
	const overrideHashes = options.overrideHashes ?? new Set<number>();
	let pageOverrideHandle = 0;
	let pageOverrideTimestamp = 0;
	let pageOverrideHash = 0;

	for (let blockIndex = 0; blockIndex < blockCount; blockIndex++) {
		const startOffset = blockIndex * logicalBlockSize;
		const endOffset = Math.min(page.count, startOffset + logicalBlockSize);
		let activeCount = 0;
		let representativeOffset = -1;
		let overrideOffset = -1;
		let minimum = Number.POSITIVE_INFINITY;
		let maximum = Number.NEGATIVE_INFINITY;
		for (let offset = startOffset; offset < endOffset; offset++) {
			if (page.tombstones[offset] !== 0) continue;
			activeCount++;
			if (representativeOffset === -1) representativeOffset = offset;
			if (overrideOffset === -1 && overrideHashes.has(page.hashes[offset])) {
				overrideOffset = offset;
			}
			minimum = Math.min(minimum, page.timestamps[offset]);
			maximum = Math.max(maximum, page.timestamps[offset]);
		}
		startOffsets[blockIndex] = startOffset;
		endOffsets[blockIndex] = endOffset;
		counts[blockIndex] = activeCount;
		minTimestamps[blockIndex] = activeCount === 0 ? 0 : minimum;
		maxTimestamps[blockIndex] = activeCount === 0 ? 0 : maximum;
		if (representativeOffset !== -1) {
			representativeHandles[blockIndex] = page.handles[representativeOffset];
			representativeTimestamps[blockIndex] =
				page.timestamps[representativeOffset];
			representativeHashes[blockIndex] = page.hashes[representativeOffset];
		}
		if (overrideOffset !== -1) {
			overrideBlockHandles[blockIndex] = page.handles[overrideOffset];
			overrideBlockTimestamps[blockIndex] = page.timestamps[overrideOffset];
			overrideBlockHashes[blockIndex] = page.hashes[overrideOffset];
			if (
				pageOverrideHandle === 0 ||
				page.timestamps[overrideOffset] > pageOverrideTimestamp
			) {
				pageOverrideHandle = page.handles[overrideOffset];
				pageOverrideTimestamp = page.timestamps[overrideOffset];
				pageOverrideHash = page.hashes[overrideOffset];
			}
		}
	}
	const encodedByteLength =
		startOffsets.byteLength +
		endOffsets.byteLength +
		counts.byteLength +
		minTimestamps.byteLength +
		maxTimestamps.byteLength +
		representativeHandles.byteLength +
		representativeTimestamps.byteLength +
		representativeHashes.byteLength +
		overrideBlockHandles.byteLength +
		overrideBlockTimestamps.byteLength +
		overrideBlockHashes.byteLength;
	return {
		sourceId: page.sourceId,
		generation: page.generation,
		pageId: page.pageId,
		revision: page.revision,
		blockCount,
		startOffsets,
		endOffsets,
		counts,
		minTimestamps,
		maxTimestamps,
		representativeHandles,
		representativeTimestamps,
		representativeHashes,
		overrideRevision: options.overrideRevision ?? -1,
		overrideSignature: options.overrideSignature ?? "",
		pageOverrideHandle,
		pageOverrideTimestamp,
		pageOverrideHash,
		overrideBlockHandles,
		overrideBlockTimestamps,
		overrideBlockHashes,
		encodedByteLength,
	};
}

/**
 * Builds exact page-local UTC-minute counts independently from default-render LOD.
 * Keeping this record separate prevents default projections from cloning height-only data.
 * @param page Exact source page sorted by descending timestamp.
 * @returns Compact ascending minute-count columns for the height engine.
 */
export function buildEventPageHeight(
	page: EncodedEventPage,
): EncodedEventPageHeight {
	const minuteBuckets = new Map<number, number>();
	for (let offset = 0; offset < page.count; offset++) {
		if (page.tombstones[offset] !== 0) continue;
		const minuteTimestamp =
			Math.floor(page.timestamps[offset] / 60_000) * 60_000;
		minuteBuckets.set(
			minuteTimestamp,
			(minuteBuckets.get(minuteTimestamp) ?? 0) + 1,
		);
	}
	const minuteTimestamps = Float64Array.from(
		[...minuteBuckets.keys()].sort((left, right) => left - right),
	);
	const minuteCounts = Uint32Array.from(
		minuteTimestamps,
		(timestamp) => minuteBuckets.get(timestamp) ?? 0,
	);
	return {
		sourceId: page.sourceId,
		generation: page.generation,
		pageId: page.pageId,
		revision: page.revision,
		minuteTimestamps,
		minuteCounts,
		encodedByteLength: minuteTimestamps.byteLength + minuteCounts.byteLength,
	};
}

/**
 * Projects one exact page into its source-manifest catalog entry.
 * @param page Exact encoded page.
 * @returns Compact metadata safe to retain with the source manifest.
 */
export function createEventPageCatalogEntry(
	page: EncodedEventPage,
): EventPageCatalogEntry {
	let representativeOffset = 0;
	let minimumHandle = Number.POSITIVE_INFINITY;
	let maximumHandle = 0;
	for (let offset = 0; offset < page.count; offset++) {
		if (page.tombstones[offset] !== 0) continue;
		minimumHandle = Math.min(minimumHandle, page.handles[offset]);
		maximumHandle = Math.max(maximumHandle, page.handles[offset]);
	}
	while (
		representativeOffset < page.count &&
		page.tombstones[representativeOffset] !== 0
	) {
		representativeOffset++;
	}
	return {
		pageId: page.pageId,
		generation: page.generation,
		revision: page.revision,
		count: page.count,
		activeCount: page.activeCount,
		minTimestamp: page.minTimestamp,
		maxTimestamp: page.maxTimestamp,
		hashMin: page.hashMin,
		hashMax: page.hashMax,
		minimumHandle: page.activeCount === 0 ? 0 : minimumHandle,
		maximumHandle,
		representativeHandle:
			representativeOffset < page.count ? page.handles[representativeOffset] : 0,
		representativeTimestamp:
			representativeOffset < page.count
				? page.timestamps[representativeOffset]
				: 0,
		representativeHash:
			representativeOffset < page.count ? page.hashes[representativeOffset] : 0,
		encodedByteLength: page.encodedByteLength,
	};
}
