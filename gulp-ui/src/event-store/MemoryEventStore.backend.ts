import {
	EVENT_STORE_NAMES,
	type EventStoreBackend,
	type EventStoreName,
	type EventStoreWriteOperation,
} from "./EventStore.backend";

interface MemoryEventStoreEntry {
	key: IDBValidKey;
	value: unknown;
}

/**
 * Creates a deterministic string representation for the primary-key shapes used by
 * the event repository. EventStore keys contain only strings, numbers, and arrays.
 * @param key IndexedDB-compatible key to serialize.
 * @returns Stable map key preserving scalar type and array nesting.
 */
function serializeMemoryKey(key: IDBValidKey): string {
	if (Array.isArray(key)) {
		const members = key.map((part) => serializeMemoryKey(part));
		return `array:${members.length}:${members
			.map((member) => `${member.length}:${member}`)
			.join("")}`;
	}
	if (key instanceof Date) return `date:${key.getTime()}`;
	if (key instanceof ArrayBuffer || ArrayBuffer.isView(key)) {
		const bytes =
			key instanceof ArrayBuffer
				? new Uint8Array(key)
				: new Uint8Array(key.buffer, key.byteOffset, key.byteLength);
		return `buffer:${bytes.byteLength}:${Array.from(bytes).join(".")}`;
	}
	if (typeof key === "string") return `string:${key.length}:${key}`;
	return `number:${String(key)}`;
}

/**
 * Compares an in-memory key against a leading compound-key prefix.
 * @param key Stored primary key.
 * @param prefix Expected leading components.
 * @returns True when every prefix component has the same serialized value.
 */
function memoryKeyHasPrefix(
	key: IDBValidKey,
	prefix: readonly IDBValidKey[],
): boolean {
	if (!Array.isArray(key) || key.length < prefix.length) return false;
	for (let index = 0; index < prefix.length; index++) {
		if (serializeMemoryKey(key[index]) !== serializeMemoryKey(prefix[index])) {
			return false;
		}
	}
	return true;
}

/**
 * Applies structured-clone semantics when the runtime exposes the native helper.
 * @param value Value crossing the in-memory persistence boundary.
 * @returns Detached object graph equivalent to an IndexedDB structured clone.
 */
function cloneMemoryValue<TValue>(value: TValue): TValue {
	return typeof globalThis.structuredClone === "function"
		? globalThis.structuredClone(value)
		: cloneMemoryValueFallback(value, new WeakMap<object, unknown>());
}

/**
 * Clones the structured data shapes used by EventStore when the test runtime does
 * not expose `structuredClone` (notably React Scripts' jsdom environment).
 * @param value Value or nested member being copied.
 * @param seen Previously cloned object identities used to preserve cycles.
 * @returns Independent clone with typed-array contents copied.
 */
function cloneMemoryValueFallback<TValue>(
	value: TValue,
	seen: WeakMap<object, unknown>,
): TValue {
	if (typeof value !== "object" || value === null) return value;
	const existing = seen.get(value);
	if (existing) return existing as TValue;
	if (value instanceof Date) return new Date(value.getTime()) as TValue;
	if (value instanceof ArrayBuffer) return value.slice(0) as TValue;
	if (ArrayBuffer.isView(value)) {
		const bytes = new Uint8Array(value.byteLength);
		bytes.set(
			new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
		);
		if (value instanceof DataView) return new DataView(bytes.buffer) as TValue;
		const typedValue = value as unknown as { readonly length: number };
		const TypedArray = value.constructor as new (
			buffer: ArrayBuffer,
			byteOffset: number,
			length: number,
		) => unknown;
		return new TypedArray(bytes.buffer, 0, typedValue.length) as TValue;
	}
	if (Array.isArray(value)) {
		const clone: unknown[] = [];
		seen.set(value, clone);
		for (const member of value) {
			clone.push(cloneMemoryValueFallback(member, seen));
		}
		return clone as TValue;
	}
	if (value instanceof Map) {
		const clone = new Map<unknown, unknown>();
		seen.set(value, clone);
		for (const [key, member] of value) {
			clone.set(
				cloneMemoryValueFallback(key, seen),
				cloneMemoryValueFallback(member, seen),
			);
		}
		return clone as TValue;
	}
	if (value instanceof Set) {
		const clone = new Set<unknown>();
		seen.set(value, clone);
		for (const member of value) {
			clone.add(cloneMemoryValueFallback(member, seen));
		}
		return clone as TValue;
	}

	const clone = Object.create(Object.getPrototypeOf(value)) as Record<
		PropertyKey,
		unknown
	>;
	seen.set(value, clone);
	for (const key of Reflect.ownKeys(value)) {
		clone[key] = cloneMemoryValueFallback(
			(value as Record<PropertyKey, unknown>)[key],
			seen,
		);
	}
	return clone as TValue;
}

/** Dependency-free EventStore backend used by repository unit tests. */
export class MemoryEventStoreBackend implements EventStoreBackend {
	private readonly stores = new Map<
		EventStoreName,
		Map<string, MemoryEventStoreEntry>
	>();

	/** Creates every required object store eagerly. */
	public constructor() {
		for (const storeName of EVENT_STORE_NAMES) {
			this.stores.set(storeName, new Map());
		}
	}

	/**
	 * Reads one structured-cloned value.
	 * @param store Object store containing the value.
	 * @param key Exact primary key.
	 * @returns Stored clone, or undefined when absent.
	 */
	public async get<TValue>(
		store: EventStoreName,
		key: IDBValidKey,
	): Promise<TValue | undefined> {
		const entry = this.getStore(store).get(serializeMemoryKey(key));
		return entry ? cloneMemoryValue(entry.value as TValue) : undefined;
	}

	/**
	 * Reads several exact keys while preserving IndexedDB-style clone boundaries.
	 * @param store Object store containing the values.
	 * @param keys Exact primary keys in caller order.
	 * @returns Stored clones or undefined entries in caller order.
	 */
	public async getMany<TValue>(
		store: EventStoreName,
		keys: readonly IDBValidKey[],
	): Promise<Array<TValue | undefined>> {
		const target = this.getStore(store);
		return keys.map((key) => {
			const entry = target.get(serializeMemoryKey(key));
			return entry ? cloneMemoryValue(entry.value as TValue) : undefined;
		});
	}

	/**
	 * Enumerates structured-cloned values in deterministic primary-key order.
	 * @param store Object store to enumerate.
	 * @returns Stored values ordered by their serialized keys.
	 */
	public async getAll<TValue>(store: EventStoreName): Promise<TValue[]> {
		return [...this.getStore(store).entries()]
			.sort(([left], [right]) => left.localeCompare(right))
			.map(([, entry]) => cloneMemoryValue(entry.value as TValue));
	}

	/**
	 * Enumerates records matching an exact compound-key prefix.
	 * @param store Object store to enumerate.
	 * @param keyPrefix Leading key components.
	 * @param limit Optional result cap.
	 * @returns Matching cloned values in deterministic key order.
	 */
	public async getAllByPrefix<TValue>(
		store: EventStoreName,
		keyPrefix: readonly IDBValidKey[],
		limit?: number,
	): Promise<TValue[]> {
		const safeLimit =
			limit === undefined
				? Number.POSITIVE_INFINITY
				: Math.max(0, Math.floor(limit));
		return [...this.getStore(store).entries()]
			.filter(([, entry]) => memoryKeyHasPrefix(entry.key, keyPrefix))
			.sort(([left], [right]) => left.localeCompare(right))
			.slice(0, safeLimit)
			.map(([, entry]) => cloneMemoryValue(entry.value as TValue));
	}

	/**
	 * Applies one atomic prepared write set.
	 * @param operations Ordered mutations to commit.
	 * @returns Promise resolved after every mutation is visible.
	 */
	public async commit(
		operations: readonly EventStoreWriteOperation[],
	): Promise<void> {
		const prepared = operations.map((operation): EventStoreWriteOperation => {
			this.getStore(operation.store);
			return operation.type === "put"
				? {
						type: "put",
						store: operation.store,
						key: cloneMemoryValue(operation.key),
						value: cloneMemoryValue(operation.value),
					}
				: operation;
		});

		for (const operation of prepared) {
			const store = this.getStore(operation.store);
			switch (operation.type) {
				case "put":
					store.set(serializeMemoryKey(operation.key), {
						key: operation.key,
						value: operation.value,
					});
					break;
				case "delete":
					store.delete(serializeMemoryKey(operation.key));
					break;
				case "clear":
					store.clear();
					break;
			}
		}
	}

	/**
	 * Returns one store's entry count for focused persistence assertions.
	 * @param store Object store to inspect.
	 * @returns Number of retained primary keys.
	 */
	public getStoreSize(store: EventStoreName): number {
		return this.getStore(store).size;
	}

	/** Releases no resources because the backend owns no external connection. */
	public close(): void {}

	/**
	 * Resolves one initialized object store.
	 * @param store Store identifier to resolve.
	 * @returns Mutable in-memory map backing the store.
	 */
	private getStore(store: EventStoreName): Map<string, MemoryEventStoreEntry> {
		const target = this.stores.get(store);
		if (!target) throw new Error(`Unknown EventStore object store: ${store}`);
		return target;
	}
}
