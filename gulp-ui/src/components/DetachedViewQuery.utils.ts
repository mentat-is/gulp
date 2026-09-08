import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";

/**
 * Recursively normalizes a value before it is used as a query dependency key.
 * Object key order is intentionally ignored while array order is preserved.
 *
 * @param value Value whose query semantics should be serialized.
 * @returns JSON-compatible value with deterministically ordered object keys.
 */
function normalizeSemanticValue(value: unknown): unknown {
	if (typeof value === "bigint") {
		return { __gulp_bigint__: value.toString() };
	}

	if (typeof value === "number" && !Number.isFinite(value)) {
		return { __gulp_number__: String(value) };
	}

	if (Array.isArray(value)) {
		return value.map(normalizeSemanticValue);
	}

	if (value && typeof value === "object") {
		return Object.keys(value)
			.sort((left, right) => left.localeCompare(right))
			.reduce<Record<string, unknown>>((normalized, key) => {
				const child = (value as Record<string, unknown>)[key];
				if (child !== undefined) {
					normalized[key] = normalizeSemanticValue(child);
				}
				return normalized;
			}, {});
	}

	return value;
}

/**
 * Serializes a query-relevant value without depending on object property order.
 *
 * @param value Query value to serialize.
 * @returns Deterministic semantic signature.
 */
export function serializeSemanticValue(value: unknown): string {
	return JSON.stringify(normalizeSemanticValue(value));
}

/**
 * Returns a stable signature for a source set.
 * Terms queries do not attach meaning to source-id ordering.
 *
 * @param sourceIds Source identifiers participating in a query.
 * @returns Deterministically ordered source-id signature.
 */
export function serializeSourceIdSet(sourceIds: Source.Id[]): string {
	return serializeSemanticValue(
		Array.from(new Set(sourceIds)).sort((left, right) =>
			left.localeCompare(right),
		),
	);
}

/**
 * Mirrors the source-field cache invalidation semantics for React dependencies.
 * A backend update marker takes precedence; when absent, stable source metadata
 * is used as the fallback freshness fingerprint.
 *
 * @param sources Sources whose field metadata is requested.
 * @returns Deterministic source metadata dependency signature.
 */
export function serializeSourceFieldMetadata(
	sources: Source.Type[],
): string {
	const metadata = sources
		.map((source) => {
			const updatedAt =
				(typeof source.time_updated === "number" &&
					Number.isFinite(source.time_updated)) ||
				(typeof source.time_updated === "string" &&
					source.time_updated.trim())
					? source.time_updated
					: null;

			return {
				id: source.id,
				operation_id: source.operation_id,
				context_id: source.context_id,
				updated_at: updatedAt,
				fallback:
					updatedAt === null
						? {
								name: source.name ?? null,
								plugin: source.plugin ?? null,
							}
						: null,
			};
		})
		.sort((left, right) => left.id.localeCompare(right.id));

	return serializeSemanticValue(metadata);
}

export function getSemanticSyncedQuery(
	query: Query.Type | null | undefined,
): Omit<Query.Type, "string" | "source_config" | "fieldTypeMap"> | null {
	if (!query) return null;

	return {
		filters: query.filters ?? [],
		text_filter: query.text_filter ?? "",
		isManual: query.isManual,
		raw: query.raw,
	};
}
