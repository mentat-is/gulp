/** IndexedDB object stores owned by one operation-scoped event database. */
export const EVENT_STORE_NAMES = [
	"sources",
	"pages",
	"page_lod",
	"document_index_pages",
] as const;

export type EventStoreName = (typeof EVENT_STORE_NAMES)[number];

/** One atomic mutation accepted by an EventStore backend. */
export type EventStoreWriteOperation =
	| {
			type: "put";
			store: EventStoreName;
			key: IDBValidKey;
			value: unknown;
	  }
	| {
			type: "delete";
			store: EventStoreName;
			key: IDBValidKey;
	  }
	| {
			type: "clear";
			store: EventStoreName;
	  };

/**
 * Minimal persistence contract shared by the in-memory and IndexedDB repositories.
 * Write operations are prepared before `commit` so no unrelated asynchronous work
 * can let an IndexedDB transaction auto-close between requests.
 */
export interface EventStoreBackend {
	/**
	 * Reads one value by primary key.
	 * @param store Object store containing the value.
	 * @param key Exact primary key.
	 * @returns Stored value, or undefined when the key is absent.
	 */
	get<TValue>(store: EventStoreName, key: IDBValidKey): Promise<TValue | undefined>;

	/**
	 * Reads several exact primary keys in one readonly transaction.
	 * @param store Object store containing the values.
	 * @param keys Exact primary keys in caller order.
	 * @returns Stored values or undefined entries in the same order as `keys`.
	 */
	getMany<TValue>(
		store: EventStoreName,
		keys: readonly IDBValidKey[],
	): Promise<Array<TValue | undefined>>;

	/**
	 * Reads every value from one object store.
	 * @param store Object store to enumerate.
	 * @returns Stored values in backend key order.
	 */
	getAll<TValue>(store: EventStoreName): Promise<TValue[]>;

	/**
	 * Reads values whose compound primary key starts with an exact component prefix.
	 * @param store Object store to query.
	 * @param keyPrefix Leading compound-key components.
	 * @param limit Optional maximum number of records returned.
	 * @returns Matching values in backend key order.
	 */
	getAllByPrefix<TValue>(
		store: EventStoreName,
		keyPrefix: readonly IDBValidKey[],
		limit?: number,
	): Promise<TValue[]>;

	/**
	 * Applies a prepared mutation list in one atomic transaction.
	 * @param operations Ordered mutations spanning one or more event stores.
	 * @returns Promise resolved only after the transaction commits.
	 */
	commit(operations: readonly EventStoreWriteOperation[]): Promise<void>;

	/** Releases the backend connection and associated transient state. */
	close(): void;
}
