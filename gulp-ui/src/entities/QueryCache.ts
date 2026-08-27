import type { Query } from "./Query";

/** Freshness classification exposed by source cache state and render projections. */
export type QueryCacheFreshness =
	| "fresh"
	| "query-mismatch"
	| "source-changed"
	| "mutated"
	| "unknown";

/** Data-normalization inputs that make a persisted query result reusable. */
export interface QueryCacheNormalization {
	field: string;
	hashFunction: "fnv1a" | "djb2" | "sdbm";
	requestedFields: string[];
}

/** Backend source metadata captured independently from a filtered result count. */
export interface QueryCacheServerSnapshot {
	total: number;
	minNanoseconds: string;
	maxNanoseconds: string;
	timeUpdated: number | string | null;
	/** True when either range boundary was derived from millisecond-only metadata. */
	millisecondPrecision: boolean;
}

/** Serializable query provenance attached to one physical EventStore generation. */
export interface QueryCacheProvenance {
	query: Query.Type;
	fingerprint: string;
	requestedFrame: { min: number; max: number };
	normalization: QueryCacheNormalization;
	serverSnapshot: QueryCacheServerSnapshot;
}

/** Query-cache navigation state attached to one runtime source object. */
export interface QueryCacheState {
	activeGeneration: number | null;
	fingerprint: string | null;
	historyIndex: number;
	historyLength: number;
	canUndo: boolean;
	canRedo: boolean;
	freshness: QueryCacheFreshness;
	warningReason: QueryCacheFreshness | null;
	isActivating: boolean;
}

/** Input whose semantic values uniquely identify one source query result. */
export interface SourceQueryFingerprintInput {
	query: Query.Type;
	operationId: string;
	sourceId: string;
	normalization: QueryCacheNormalization;
	/** Final backend filter after field-type coercion; fieldTypeMap itself is excluded. */
	backendFilter?: unknown;
}

type CanonicalValue =
	| null
	| boolean
	| number
	| string
	| CanonicalValue[]
	| { [key: string]: CanonicalValue };

/** Creates an independent empty cache state for one source. */
export function createEmptyQueryCacheState(): QueryCacheState {
	return {
		activeGeneration: null,
		fingerprint: null,
		historyIndex: -1,
		historyLength: 0,
		canUndo: false,
		canRedo: false,
		freshness: "fresh",
		warningReason: null,
		isActivating: false,
	};
}

/**
 * Converts query-owned values into deterministic JSON-compatible data.
 * Object keys are sorted while arrays retain their semantic order.
 * @param value Value to canonicalize.
 * @returns JSON-compatible canonical value, or undefined for unsupported members.
 */
function canonicalizeValue(value: unknown): CanonicalValue | undefined {
	if (value === null) return null;
	if (typeof value === "string" || typeof value === "boolean") return value;
	if (typeof value === "number") {
		return Number.isFinite(value) ? value : String(value);
	}
	if (typeof value === "bigint") return value.toString();
	if (Array.isArray(value)) {
		return value.map((item) => canonicalizeValue(item) ?? null);
	}
	if (typeof value !== "object") return undefined;

	const record = value as Record<string, unknown>;
	const canonical: Record<string, CanonicalValue> = {};
	Object.keys(record)
		.sort()
		.forEach((key) => {
			const item = canonicalizeValue(record[key]);
			if (item !== undefined) canonical[key] = item;
		});
	return canonical;
}

/**
 * Produces the collision-free canonical fingerprint used for exact cache lookup.
 * Display-only query fields are deliberately omitted.
 * @param input Source-scoped query and normalization inputs.
 * @returns Versioned canonical JSON fingerprint.
 */
export function fingerprintSourceQuery(
	input: SourceQueryFingerprintInput,
): string {
	const range = input.query.source_config?.range;
	const semanticQuery = {
		operationId: input.operationId,
		sourceId: input.sourceId,
		range: range
			? { min: String(range.min), max: String(range.max) }
			: null,
		textFilter: input.query.text_filter ?? null,
		filters: input.query.filters ?? [],
		raw: input.query.raw ?? null,
		isManual: input.query.isManual === true,
		backendFilter: input.backendFilter ?? null,
		normalization: {
			field: input.normalization.field,
			hashFunction: input.normalization.hashFunction,
			requestedFields: [...input.normalization.requestedFields],
		},
	};
	return `source-query-v1:${JSON.stringify(canonicalizeValue(semanticQuery))}`;
}

/**
 * Clones a query into a structured-clone-safe snapshot for IndexedDB and workers.
 * @param query Query to detach from mutable React state.
 * @returns Independent query snapshot.
 */
export function cloneQueryForCache(query: Query.Type): Query.Type {
	const canonical = canonicalizeValue(query);
	if (!canonical || Array.isArray(canonical) || typeof canonical !== "object") {
		throw new Error("A source query must be a serializable object.");
	}
	return canonical as unknown as Query.Type;
}

/**
 * Compares source snapshots without confusing filtered result counts with totals.
 * The update marker participates only when both snapshots expose it because the
 * current `/query_operations` response may omit that collaboration metadata.
 * @param cached Metadata retained with the query result.
 * @param current Latest metadata returned for the complete source.
 * @returns True when count, range, and update marker still agree.
 */
export function sourceSnapshotsMatch(
	cached: QueryCacheServerSnapshot,
	current: QueryCacheServerSnapshot,
): boolean {
	const hasComparableTimeUpdated =
		cached.timeUpdated !== null && current.timeUpdated !== null;
	const timeUpdatedMatches =
		!hasComparableTimeUpdated ||
		String(cached.timeUpdated) === String(current.timeUpdated);
	if (cached.total !== current.total || !timeUpdatedMatches) {
		return false;
	}
	const tolerance =
		cached.millisecondPrecision || current.millisecondPrecision ? 1_000_000n : 0n;
	const difference = (left: string, right: string): bigint => {
		const value = BigInt(left) - BigInt(right);
		return value < 0n ? -value : value;
	};
	return (
		difference(cached.minNanoseconds, current.minNanoseconds) <= tolerance &&
		difference(cached.maxNanoseconds, current.maxNanoseconds) <= tolerance
	);
}
