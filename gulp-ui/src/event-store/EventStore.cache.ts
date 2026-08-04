import type { Doc } from "@/entities/Doc";
import { EVENT_MAIN_CACHE_SIZE } from "./EventStore.types";

/** Bounded least-recently-used cache for small main/detached event reads. */
export class EventDocumentCache {
	private readonly documents = new Map<Doc.Id, Doc.Type>();
	private readonly maximumSize: number;

	/**
	 * Creates an event cache with an explicit cardinality ceiling.
	 * @param maximumSize Maximum canonical documents retained by this realm.
	 */
	public constructor(maximumSize = EVENT_MAIN_CACHE_SIZE) {
		if (!Number.isInteger(maximumSize) || maximumSize <= 0) {
			throw new RangeError("Event document cache size must be a positive integer.");
		}
		this.maximumSize = maximumSize;
	}

	/** Returns the number of currently retained canonical documents. */
	public get size(): number {
		return this.documents.size;
	}

	/**
	 * Resolves and promotes one cached document.
	 * @param documentId Document identifier to find.
	 * @returns Cached document, or undefined when absent.
	 */
	public get(documentId: Doc.Id): Doc.Type | undefined {
		const document = this.documents.get(documentId);
		if (!document) return undefined;
		this.documents.delete(documentId);
		this.documents.set(documentId, document);
		return document;
	}

	/**
	 * Inserts or promotes one document and evicts the least recently used entry.
	 * @param document Canonical minimal document to retain.
	 * @returns The stored document.
	 */
	public set(document: Doc.Type): Doc.Type {
		this.documents.delete(document._id);
		this.documents.set(document._id, document);
		while (this.documents.size > this.maximumSize) {
			const oldestId = this.documents.keys().next().value as Doc.Id | undefined;
			if (!oldestId) break;
			this.documents.delete(oldestId);
		}
		return document;
	}

	/**
	 * Inserts a bounded document collection in caller order.
	 * @param documents Documents to retain, where later values become most recent.
	 * @returns Input documents as a mutable result array.
	 */
	public setMany(documents: readonly Doc.Type[]): Doc.Type[] {
		for (const document of documents) this.set(document);
		return [...documents];
	}

	/**
	 * Removes one cached document.
	 * @param documentId Document identifier to evict.
	 * @returns True when an entry was removed.
	 */
	public delete(documentId: Doc.Id): boolean {
		return this.documents.delete(documentId);
	}

	/** Releases every retained document. */
	public clear(): void {
		this.documents.clear();
	}
}
