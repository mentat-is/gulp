import type { EventStoreNamespace } from "./EventStore.types";
import {
	EVENT_STORE_NAMES,
	type EventStoreBackend,
	type EventStoreName,
	type EventStoreWriteOperation,
} from "./EventStore.backend";

export const EVENT_STORE_DATABASE_VERSION = 4;
const EVENT_STORE_DATABASE_PREFIX = "gulp-event-store";

/**
 * Reports whether a persisted cache predates the current query-cache-pool schema.
 * Version zero is a new database and therefore has no incompatible data to rebuild.
 * @param oldVersion Version supplied by IndexedDB during upgrade.
 * @returns True when every cache store must be recreated.
 */
export function shouldRebuildEventStoreCache(oldVersion: number): boolean {
	return oldVersion > 0 && oldVersion < EVENT_STORE_DATABASE_VERSION;
}

/**
 * Creates the operation-isolated IndexedDB name shared by main and detached realms.
 * @param namespace Coordinator and operation identifiers owning the database.
 * @returns Stable same-origin database name.
 */
export function createEventStoreDatabaseName(
	namespace: EventStoreNamespace,
): string {
	return `${EVENT_STORE_DATABASE_PREFIX}:${encodeURIComponent(
		namespace.coordinatorId,
	)}:${encodeURIComponent(namespace.operationId)}`;
}

/**
 * Converts an IndexedDB error into an Error with a useful fallback message.
 * @param error Native IndexedDB error, when supplied by the browser.
 * @param fallback Human-readable operation description.
 * @returns Error safe to reject across repository boundaries.
 */
function toIndexedDbError(error: DOMException | null, fallback: string): Error {
	return error ?? new Error(fallback);
}

/** Persistent, worker-safe IndexedDB backend for one EventStore namespace. */
export class IndexedDbEventStoreBackend implements EventStoreBackend {
	private readonly databaseName: string;
	private readonly indexedDbFactory: IDBFactory | undefined;
	private readonly keyRangeFactory: typeof IDBKeyRange | undefined;
	private database: IDBDatabase | null = null;
	private databasePromise: Promise<IDBDatabase> | null = null;
	private closed = false;

	/**
	 * Creates a lazily opened EventStore database.
	 * @param namespace Coordinator and operation identifiers owning the data.
	 * @param indexedDbFactory Browser factory, injectable for integration tests.
	 * @param keyRangeFactory Key-range constructor, injectable with the factory.
	 */
	public constructor(
		namespace: EventStoreNamespace,
		indexedDbFactory: IDBFactory | undefined = globalThis.indexedDB,
		keyRangeFactory: typeof IDBKeyRange | undefined = globalThis.IDBKeyRange,
	) {
		this.databaseName = createEventStoreDatabaseName(namespace);
		this.indexedDbFactory = indexedDbFactory;
		this.keyRangeFactory = keyRangeFactory;
	}

	/**
	 * Reads one value and waits for its readonly transaction to complete.
	 * @param store Object store containing the value.
	 * @param key Exact primary key.
	 * @returns Stored value, or undefined when absent.
	 */
	public async get<TValue>(
		store: EventStoreName,
		key: IDBValidKey,
	): Promise<TValue | undefined> {
		const database = await this.openDatabase();
		return new Promise<TValue | undefined>((resolve, reject) => {
			const transaction = database.transaction(store, "readonly");
			const request = transaction.objectStore(store).get(key);
			let result: TValue | undefined;
			request.onsuccess = () => {
				result = request.result as TValue | undefined;
			};
			transaction.oncomplete = () => resolve(result);
			transaction.onerror = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB read failed for ${store}.`,
					),
				);
			transaction.onabort = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB read aborted for ${store}.`,
					),
				);
		});
	}

	/**
	 * Reads exact keys through one readonly transaction so a binary shard lookup does
	 * not open one IndexedDB transaction per active LSM segment.
	 * @param store Object store containing the values.
	 * @param keys Exact primary keys in caller order.
	 * @returns Stored values or undefined entries in caller order.
	 */
	public async getMany<TValue>(
		store: EventStoreName,
		keys: readonly IDBValidKey[],
	): Promise<Array<TValue | undefined>> {
		if (keys.length === 0) return [];
		const database = await this.openDatabase();
		return new Promise<Array<TValue | undefined>>((resolve, reject) => {
			const transaction = database.transaction(store, "readonly");
			const objectStore = transaction.objectStore(store);
			const results = new Array<TValue | undefined>(keys.length);
			keys.forEach((key, index) => {
				const request = objectStore.get(key);
				request.onsuccess = () => {
					results[index] = request.result as TValue | undefined;
				};
			});
			transaction.oncomplete = () => resolve(results);
			transaction.onerror = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB multi-key read failed for ${store}.`,
					),
				);
			transaction.onabort = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB multi-key read aborted for ${store}.`,
					),
				);
		});
	}

	/**
	 * Enumerates one object store through a completed readonly transaction.
	 * @param store Object store to enumerate.
	 * @returns Every stored value in IndexedDB key order.
	 */
	public async getAll<TValue>(store: EventStoreName): Promise<TValue[]> {
		const database = await this.openDatabase();
		return new Promise<TValue[]>((resolve, reject) => {
			const transaction = database.transaction(store, "readonly");
			const request = transaction.objectStore(store).getAll();
			let result: TValue[] = [];
			request.onsuccess = () => {
				result = request.result as TValue[];
			};
			transaction.oncomplete = () => resolve(result);
			transaction.onerror = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB enumeration failed for ${store}.`,
					),
				);
			transaction.onabort = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB enumeration aborted for ${store}.`,
					),
				);
		});
	}

	/**
	 * Reads a bounded compound-key prefix directly through an IndexedDB key range.
	 * @param store Object store to enumerate.
	 * @param keyPrefix Leading compound-key components.
	 * @param limit Optional maximum number of records.
	 * @returns Matching values in primary-key order.
	 */
	public async getAllByPrefix<TValue>(
		store: EventStoreName,
		keyPrefix: readonly IDBValidKey[],
		limit?: number,
	): Promise<TValue[]> {
		if (!this.keyRangeFactory) {
			throw new Error("IndexedDB key ranges are unavailable in this realm.");
		}
		const database = await this.openDatabase();
		const lower = [...keyPrefix];
		const upper = [...keyPrefix, [] as IDBValidKey[]];
		const keyRange = this.keyRangeFactory.bound(lower, upper, false, true);
		const safeLimit =
			limit === undefined
				? undefined
				: Math.max(0, Math.floor(limit));
		if (safeLimit === 0) return [];
		return new Promise<TValue[]>((resolve, reject) => {
			const transaction = database.transaction(store, "readonly");
			const request = transaction
				.objectStore(store)
				.getAll(keyRange, safeLimit);
			let result: TValue[] = [];
			request.onsuccess = () => {
				result = request.result as TValue[];
			};
			transaction.oncomplete = () => resolve(result);
			transaction.onerror = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB prefix read failed for ${store}.`,
					),
				);
			transaction.onabort = () =>
				reject(
					toIndexedDbError(
						transaction.error,
						`IndexedDB prefix read aborted for ${store}.`,
					),
				);
		});
	}

	/**
	 * Commits a prepared write set in one multi-store transaction.
	 * @param operations Ordered put, delete, and clear mutations.
	 * @returns Promise resolved only after IndexedDB reports transaction completion.
	 */
	public async commit(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		if (operations.length === 0) return;
		const database = await this.openDatabase();
		const storeNames = [
			...new Set<EventStoreName>(operations.map((operation) => operation.store)),
		];

		return new Promise<void>((resolve, reject) => {
			const transaction = database.transaction(storeNames, "readwrite");
			let settled = false;
			/** Rejects this transaction promise at most once. */
			const rejectOnce = (error: Error): void => {
				if (settled) return;
				settled = true;
				reject(error);
			};
			transaction.oncomplete = () => {
				if (settled) return;
				settled = true;
				resolve();
			};
			transaction.onerror = () =>
				rejectOnce(
					toIndexedDbError(
						transaction.error,
						"IndexedDB EventStore commit failed.",
					),
				);
			transaction.onabort = () =>
				rejectOnce(
					toIndexedDbError(
						transaction.error,
						"IndexedDB EventStore commit aborted.",
					),
				);

			try {
				for (const operation of operations) {
					const store = transaction.objectStore(operation.store);
					switch (operation.type) {
						case "put":
							store.put(operation.value, operation.key);
							break;
						case "delete":
							store.delete(operation.key);
							break;
						case "clear":
							store.clear();
							break;
					}
				}
			} catch (error) {
				try {
					transaction.abort();
				} catch {
					// The transaction may already be inactive after a synchronous request error.
				}
				rejectOnce(
					error instanceof Error
						? error
						: new Error("IndexedDB EventStore commit failed."),
				);
			}
		});
	}

	/** Closes the persistent connection and rejects future backend work. */
	public close(): void {
		this.closed = true;
		this.database?.close();
		this.database = null;
		const pendingDatabase = this.databasePromise;
		this.databasePromise = null;
		if (pendingDatabase) {
			void pendingDatabase.then((database) => database.close()).catch(() => undefined);
		}
	}

	/**
	 * Opens and upgrades the persistent database once per backend lifetime.
	 * @returns Shared connection promise.
	 */
	private openDatabase(): Promise<IDBDatabase> {
		if (this.closed) {
			return Promise.reject(new Error("EventStore backend is closed."));
		}
		if (this.database) return Promise.resolve(this.database);
		if (this.databasePromise) return this.databasePromise;
		if (!this.indexedDbFactory) {
			return Promise.reject(new Error("IndexedDB is unavailable in this realm."));
		}

		this.databasePromise = new Promise<IDBDatabase>((resolve, reject) => {
			const request = this.indexedDbFactory!.open(
				this.databaseName,
				EVENT_STORE_DATABASE_VERSION,
			);
			let settled = false;
			request.onupgradeneeded = (event) => {
				const database = request.result;
				const rebuildCache = shouldRebuildEventStoreCache(event.oldVersion);
				for (
					let index = database.objectStoreNames.length - 1;
					index >= 0;
					index--
				) {
					const storeName = database.objectStoreNames.item(index);
					if (
						storeName &&
						(rebuildCache ||
							!EVENT_STORE_NAMES.includes(storeName as EventStoreName))
					) {
						database.deleteObjectStore(storeName);
					}
				}
				for (const storeName of EVENT_STORE_NAMES) {
					if (!database.objectStoreNames.contains(storeName)) {
						database.createObjectStore(storeName);
					}
				}
			};
			request.onsuccess = () => {
				const database = request.result;
				if (settled) {
					database.close();
					return;
				}
				if (this.closed) {
					settled = true;
					database.close();
					this.databasePromise = null;
					reject(new Error("EventStore backend was closed while opening."));
					return;
				}
				settled = true;
				this.database = database;
				database.onversionchange = () => {
					database.close();
					if (this.database === database) {
						this.database = null;
						this.databasePromise = null;
					}
				};
				resolve(database);
			};
			request.onerror = () => {
				if (settled) return;
				settled = true;
				this.databasePromise = null;
				reject(
					toIndexedDbError(
						request.error,
						`Unable to open IndexedDB database ${this.databaseName}.`,
					),
				);
			};
			request.onblocked = () => {
				if (settled) return;
				settled = true;
				this.databasePromise = null;
				reject(
					new Error(
						`IndexedDB database ${this.databaseName} is blocked by another realm.`,
					),
				);
			};
		});
		return this.databasePromise;
	}
}
