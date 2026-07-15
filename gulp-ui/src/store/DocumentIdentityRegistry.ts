import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";

const DEFAULT_PAGE_SIZE = 65_536;
const MAXIMUM_UINT32 = 0xffffffff;

interface SourceIdentity {
	readonly id: Source.Id;
	readonly ordinal: number;
	nextHandle: number;
}

interface IdentityPage {
	readonly handles: Uint32Array;
	readonly sourceOrdinals: Uint32Array;
}

interface DocumentCollision {
	activeOrdinal: number | null;
	readonly ordinalsBySource: Map<number, number>;
}

type DocumentLookup = number | DocumentCollision;

interface DocumentIdentityResolution {
	readonly collision: DocumentCollision | null;
	readonly ordinal: number;
}

export interface DocumentIdentityRegistryOptions {
	maximumRenderHandle?: number;
	pageSize?: number;
}

/**
 * Stores document lookup references and stable source-local render handles behind
 * one document-ID map. Numeric ordinals keep the common Map values compact while
 * typed sidecars avoid a second Map entry for every canonical document.
 */
export class DocumentIdentityRegistry {
	private readonly documentLookupById = new Map<Doc.Id, DocumentLookup>();
	private readonly maximumRenderHandle: number;
	private readonly pageSize: number;
	private readonly sourceIdentityById = new Map<Source.Id, SourceIdentity>();
	private documentPages: Array<Array<Doc.Type | undefined> | undefined> = [];
	private identityPages: IdentityPage[] = [];
	private nextDocumentOrdinal = 0;
	private nextSourceOrdinal = 1;

	/**
	 * Creates a document identity registry.
	 * @param options Optional page and capacity limits, primarily useful for focused tests.
	 */
	public constructor(options: DocumentIdentityRegistryOptions = {}) {
		const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
		const maximumRenderHandle =
			options.maximumRenderHandle ?? MAXIMUM_UINT32;
		if (!Number.isInteger(pageSize) || pageSize <= 0) {
			throw new RangeError("Document identity page size must be a positive integer.");
		}
		if (
			!Number.isInteger(maximumRenderHandle) ||
			maximumRenderHandle <= 0 ||
			maximumRenderHandle > MAXIMUM_UINT32
		) {
			throw new RangeError(
				"Maximum render handle must be a non-zero uint32 value.",
			);
		}

		this.pageSize = pageSize;
		this.maximumRenderHandle = maximumRenderHandle;
	}

	/**
	 * Indexes the latest canonical value for a document and makes it the global ID result.
	 * @param document Canonical document to retain for O(1) lookup.
	 * @returns Nothing. Render handles remain lazily assigned in projection order.
	 */
	public indexDocument(document: Doc.Type): void {
		const sourceIdentity = this.getOrCreateSourceIdentity(
			document["gulp.source_id"],
		);
		this.indexDocumentForSource(document, sourceIdentity);
	}

	/**
	 * Indexes canonical documents that share one source while resolving its identity once.
	 * @param sourceId Source that owns every supplied document.
	 * @param documents Canonical documents to retain for O(1) lookup.
	 * @returns Nothing. Render handles remain lazily assigned in projection order.
	 */
	public indexDocuments(
		sourceId: Source.Id,
		documents: readonly Doc.Type[],
	): void {
		if (documents.length === 0) return;
		const sourceIdentity = this.getOrCreateSourceIdentity(sourceId);
		for (const document of documents) {
			this.indexDocumentForSource(document, sourceIdentity);
		}
	}

	/**
	 * Reports whether an active canonical document is indexed for one global ID.
	 * Tombstoned render identities do not count as active documents.
	 * @param documentId Document identifier to inspect.
	 * @returns True when global document lookup resolves to a retained document.
	 */
	public hasDocument(documentId: Doc.Id): boolean {
		return this.getDocument(documentId) !== undefined;
	}

	/**
	 * Resolves the most recently indexed canonical document for one global ID.
	 * @param documentId Document identifier to resolve.
	 * @returns Indexed document, or undefined when absent or released.
	 */
	public getDocument(documentId: Doc.Id): Doc.Type | undefined {
		const lookup = this.documentLookupById.get(documentId);
		if (lookup === undefined) return undefined;
		const ordinal =
			typeof lookup === "number" ? lookup : lookup.activeOrdinal;
		return ordinal === null ? undefined : this.getDocumentAtOrdinal(ordinal);
	}

	/**
	 * Resolves multiple global document IDs without allocating an all-event array.
	 * @param documentIds Document identifiers to resolve in caller order.
	 * @returns Active documents for the supplied IDs, excluding missing entries.
	 */
	public getDocuments(documentIds: readonly Doc.Id[]): Doc.Type[] {
		const documents: Doc.Type[] = [];
		for (const documentId of documentIds) {
			const document = this.getDocument(documentId);
			if (document) documents.push(document);
		}
		return documents;
	}

	/**
	 * Releases one canonical document reference while retaining its stable render identity.
	 * @param sourceId Source that owns the document reference.
	 * @param documentId Document identifier to release.
	 * @returns True when a matching source-local identity was found.
	 */
	public removeDocument(sourceId: Source.Id, documentId: Doc.Id): boolean {
		const sourceIdentity = this.sourceIdentityById.get(sourceId);
		const lookup = this.documentLookupById.get(documentId);
		if (!sourceIdentity || lookup === undefined) return false;

		const ordinal = this.resolveDocumentOrdinal(lookup, sourceIdentity.ordinal);
		if (ordinal === undefined) return false;
		this.setDocumentAtOrdinal(ordinal, undefined);
		if (typeof lookup !== "number" && lookup.activeOrdinal === ordinal) {
			lookup.activeOrdinal = this.findRetainedCollisionOrdinal(
				lookup,
				ordinal,
			);
		}
		return true;
	}

	/**
	 * Releases every canonical document reference without invalidating render handles.
	 * @returns Nothing.
	 */
	public clearDocumentReferences(): void {
		this.documentPages = [];
	}

	/**
	 * Returns or allocates a stable non-zero source-local uint32 render handle.
	 * @param sourceId Source that owns the document identity.
	 * @param documentId Canonical document identifier.
	 * @returns Stable render handle for the current registry lifetime.
	 */
	public getRenderHandle(sourceId: Source.Id, documentId: Doc.Id): number {
		const sourceIdentity = this.getOrCreateSourceIdentity(sourceId);
		const { ordinal } = this.getOrCreateDocumentIdentity(
			documentId,
			sourceIdentity,
			true,
		);
		return this.getOrCreateHandleAtOrdinal(ordinal, sourceIdentity);
	}

	/**
	 * Writes stable render handles for a bounded canonical document range.
	 * @param sourceId Source that owns every supplied document.
	 * @param documents Canonical documents containing stable IDs.
	 * @param target Destination handle buffer.
	 * @param startIndex Inclusive document index to project.
	 * @param endIndex Exclusive document index to project.
	 * @param targetOffset Destination index corresponding to startIndex.
	 * @returns Nothing.
	 */
	public writeRenderHandles(
		sourceId: Source.Id,
		documents: readonly Doc.Type[],
		target: Uint32Array,
		startIndex: number,
		endIndex: number,
		targetOffset = 0,
	): void {
		const safeStartIndex = Math.max(0, Math.min(documents.length, startIndex));
		const safeEndIndex = Math.max(
			safeStartIndex,
			Math.min(documents.length, endIndex),
		);
		if (
			targetOffset < 0 ||
			targetOffset + safeEndIndex - safeStartIndex > target.length
		) {
			throw new RangeError("Render handle target range exceeds its buffer.");
		}

		const sourceIdentity = this.getOrCreateSourceIdentity(sourceId);
		for (
			let documentIndex = safeStartIndex;
			documentIndex < safeEndIndex;
			documentIndex++
		) {
			const { ordinal } = this.getOrCreateDocumentIdentity(
				documents[documentIndex]._id,
				sourceIdentity,
				true,
			);
			target[targetOffset + documentIndex - safeStartIndex] =
				this.getOrCreateHandleAtOrdinal(ordinal, sourceIdentity);
		}
	}

	/**
	 * Releases documents, identity pages, source counters, and ID mappings atomically.
	 * @returns Nothing.
	 */
	public reset(): void {
		this.documentLookupById.clear();
		this.sourceIdentityById.clear();
		this.documentPages = [];
		this.identityPages = [];
		this.nextDocumentOrdinal = 0;
		this.nextSourceOrdinal = 1;
	}

	/**
	 * Returns an existing source identity or creates its source-local handle sequence.
	 * @param sourceId Source identifier to resolve.
	 * @returns Compact source identity and its next handle counter.
	 */
	private getOrCreateSourceIdentity(sourceId: Source.Id): SourceIdentity {
		const existingIdentity = this.sourceIdentityById.get(sourceId);
		if (existingIdentity) return existingIdentity;
		if (this.nextSourceOrdinal > MAXIMUM_UINT32) {
			throw new RangeError("Document identity source capacity exceeded.");
		}

		const sourceIdentity: SourceIdentity = {
			id: sourceId,
			ordinal: this.nextSourceOrdinal,
			nextHandle: 1,
		};
		this.nextSourceOrdinal++;
		this.sourceIdentityById.set(sourceId, sourceIdentity);
		return sourceIdentity;
	}

	/**
	 * Indexes one document using an already resolved source identity.
	 * @param document Canonical document to retain.
	 * @param sourceIdentity Compact identity of the document's owning source.
	 * @returns Nothing.
	 */
	private indexDocumentForSource(
		document: Doc.Type,
		sourceIdentity: SourceIdentity,
	): void {
		const { ordinal, collision } = this.getOrCreateDocumentIdentity(
			document._id,
			sourceIdentity,
			false,
		);
		this.setDocumentAtOrdinal(ordinal, document);
		if (collision) collision.activeOrdinal = ordinal;
	}

	/**
	 * Resolves or allocates one source-specific identity for a document ID.
	 * @param documentId Canonical document identifier.
	 * @param sourceIdentity Compact identity of the owning source.
	 * @param requireRenderCapacity Whether a newly allocated identity immediately needs a handle.
	 * @returns Global ordinal and collision metadata for the source-local identity.
	 */
	private getOrCreateDocumentIdentity(
		documentId: Doc.Id,
		sourceIdentity: SourceIdentity,
		requireRenderCapacity: boolean,
	): DocumentIdentityResolution {
		const lookup = this.documentLookupById.get(documentId);
		if (lookup === undefined) {
			if (requireRenderCapacity) {
				this.assertRenderHandleCapacity(sourceIdentity);
			}
			const ordinal = this.allocateDocumentOrdinal(sourceIdentity);
			this.documentLookupById.set(documentId, ordinal);
			return { collision: null, ordinal };
		}

		const existingOrdinal = this.resolveDocumentOrdinal(
			lookup,
			sourceIdentity.ordinal,
		);
		if (existingOrdinal !== undefined) {
			return {
				collision: typeof lookup === "number" ? null : lookup,
				ordinal: existingOrdinal,
			};
		}

		if (requireRenderCapacity) {
			this.assertRenderHandleCapacity(sourceIdentity);
		}
		const ordinal = this.allocateDocumentOrdinal(sourceIdentity);
		if (typeof lookup === "number") {
			const existingSourceOrdinal = this.getSourceOrdinalAtOrdinal(lookup);
			const collision: DocumentCollision = {
				activeOrdinal:
					this.getDocumentAtOrdinal(lookup) === undefined ? null : lookup,
				ordinalsBySource: new Map([
					[existingSourceOrdinal, lookup],
					[sourceIdentity.ordinal, ordinal],
				]),
			};
			this.documentLookupById.set(documentId, collision);
			return { collision, ordinal };
		} else {
			lookup.ordinalsBySource.set(sourceIdentity.ordinal, ordinal);
			return { collision: lookup, ordinal };
		}
	}

	/**
	 * Finds another retained document after the active member of a collision is removed.
	 * @param collision Collision entry whose active document was released.
	 * @param removedOrdinal Ordinal that must not be selected again.
	 * @returns A retained fallback ordinal, or null when no canonical document remains.
	 */
	private findRetainedCollisionOrdinal(
		collision: DocumentCollision,
		removedOrdinal: number,
	): number | null {
		let fallbackOrdinal: number | null = null;
		for (const ordinal of collision.ordinalsBySource.values()) {
			if (
				ordinal !== removedOrdinal &&
				this.getDocumentAtOrdinal(ordinal) !== undefined
			) {
				fallbackOrdinal = ordinal;
			}
		}
		return fallbackOrdinal;
	}

	/**
	 * Finds the ordinal matching one source inside a normal or collision lookup.
	 * @param lookup ID lookup entry to inspect.
	 * @param sourceOrdinal Compact source ordinal to match.
	 * @returns Matching document ordinal, or undefined when the ID belongs to another source.
	 */
	private resolveDocumentOrdinal(
		lookup: DocumentLookup,
		sourceOrdinal: number,
	): number | undefined {
		if (typeof lookup !== "number") {
			return lookup.ordinalsBySource.get(sourceOrdinal);
		}
		return this.getSourceOrdinalAtOrdinal(lookup) === sourceOrdinal
			? lookup
			: undefined;
	}

	/**
	 * Allocates a sidecar ordinal for one source without consuming a render handle.
	 * @param sourceIdentity Source receiving the new document identity.
	 * @returns Newly allocated global document ordinal with a zero handle sentinel.
	 */
	private allocateDocumentOrdinal(sourceIdentity: SourceIdentity): number {
		if (this.nextDocumentOrdinal >= Number.MAX_SAFE_INTEGER) {
			throw new RangeError("Document identity ordinal capacity exceeded.");
		}

		const ordinal = this.nextDocumentOrdinal;
		this.nextDocumentOrdinal++;
		const { page, offset } = this.getOrCreateIdentitySlot(ordinal);
		page.sourceOrdinals[offset] = sourceIdentity.ordinal;
		return ordinal;
	}

	/**
	 * Returns the typed sidecar slot for an ordinal, allocating its page when needed.
	 * @param ordinal Global document ordinal.
	 * @returns Identity page and within-page offset.
	 */
	private getOrCreateIdentitySlot(ordinal: number): {
		page: IdentityPage;
		offset: number;
	} {
		const pageIndex = Math.floor(ordinal / this.pageSize);
		const offset = ordinal - pageIndex * this.pageSize;
		let page = this.identityPages[pageIndex];
		if (!page) {
			page = {
				handles: new Uint32Array(this.pageSize),
				sourceOrdinals: new Uint32Array(this.pageSize),
			};
			this.identityPages[pageIndex] = page;
		}
		return { page, offset };
	}

	/**
	 * Returns a typed identity slot that was allocated with its ordinal.
	 * @param ordinal Global document ordinal.
	 * @returns Existing identity page and within-page offset.
	 */
	private getIdentitySlot(ordinal: number): {
		page: IdentityPage;
		offset: number;
	} {
		const pageIndex = Math.floor(ordinal / this.pageSize);
		const offset = ordinal - pageIndex * this.pageSize;
		const page = this.identityPages[pageIndex];
		if (!page) {
			throw new Error("Document identity page is missing.");
		}
		return { page, offset };
	}

	/**
	 * Returns the stable handle at an ordinal or lazily assigns the next dense handle.
	 * @param ordinal Global document ordinal.
	 * @param sourceIdentity Source that owns the ordinal and handle sequence.
	 * @returns Non-zero source-local render handle.
	 */
	private getOrCreateHandleAtOrdinal(
		ordinal: number,
		sourceIdentity: SourceIdentity,
	): number {
		const { page, offset } = this.getIdentitySlot(ordinal);
		const existingHandle = page.handles[offset];
		if (existingHandle !== 0) return existingHandle;
		this.assertRenderHandleCapacity(sourceIdentity);

		page.handles[offset] = sourceIdentity.nextHandle;
		sourceIdentity.nextHandle++;
		return page.handles[offset];
	}

	/**
	 * Verifies that one source can assign another non-zero uint32 render handle.
	 * @param sourceIdentity Source whose next handle would be consumed.
	 * @returns Nothing.
	 */
	private assertRenderHandleCapacity(sourceIdentity: SourceIdentity): void {
		if (sourceIdentity.nextHandle <= this.maximumRenderHandle) return;
		throw new RangeError(
			`Render handle capacity exceeded for source ${sourceIdentity.id}`,
		);
	}

	/**
	 * Reads the compact source owner stored at one global ordinal.
	 * @param ordinal Global document ordinal.
	 * @returns Non-zero source ordinal.
	 */
	private getSourceOrdinalAtOrdinal(ordinal: number): number {
		const { page, offset } = this.getIdentitySlot(ordinal);
		return page.sourceOrdinals[offset];
	}

	/**
	 * Reads a canonical document reference from a lazily allocated reference page.
	 * @param ordinal Global document ordinal.
	 * @returns Indexed document, or undefined when released or never indexed.
	 */
	private getDocumentAtOrdinal(ordinal: number): Doc.Type | undefined {
		const pageIndex = Math.floor(ordinal / this.pageSize);
		const offset = ordinal - pageIndex * this.pageSize;
		return this.documentPages[pageIndex]?.[offset];
	}

	/**
	 * Writes or releases a canonical document reference at one global ordinal.
	 * @param ordinal Global document ordinal.
	 * @param document Canonical document to retain, or undefined to release it.
	 * @returns Nothing.
	 */
	private setDocumentAtOrdinal(
		ordinal: number,
		document: Doc.Type | undefined,
	): void {
		const pageIndex = Math.floor(ordinal / this.pageSize);
		const offset = ordinal - pageIndex * this.pageSize;
		let page = this.documentPages[pageIndex];
		if (!page) {
			if (document === undefined) return;
			page = new Array<Doc.Type | undefined>(this.pageSize);
			this.documentPages[pageIndex] = page;
		}
		page[offset] = document;
	}
}

/** Shared operation-scoped document identity registry. */
export const documentIdentityRegistry = new DocumentIdentityRegistry();
