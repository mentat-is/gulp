import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import type {
	DocumentStreamDescriptor,
	DocumentStreamSourceSettings,
} from "@/event-stream";

const HASH_MODULUS = 8_000;
const DEFAULT_FIELD = "gulp.event_code";
const DEFAULT_SETTINGS: DocumentStreamSourceSettings = {
	field: DEFAULT_FIELD,
	hash_function: "fnv1a",
};
export const QUERY_DOCUMENT_NORMALIZATION_SLICE_MS = 4;

type RawDocument = Record<string, unknown>;

interface QueryDocumentNormalizationContext {
	fallbackSourceId?: string;
	declaredSourceIds: ReadonlySet<string> | null;
}

/** Cooperative scheduler hooks used to keep worker lookups responsive. */
export interface QueryDocumentNormalizationOptions {
	maximumSliceMs?: number;
	now?: () => number;
	yieldToEventLoop?: () => Promise<void>;
}

/**
 * Reads nested values while preserving backend fields that contain literal dots.
 * @param document Raw document to inspect.
 * @param path Literal or nested field path.
 * @returns Matching value, or undefined when unavailable.
 */
function getFieldValue(document: RawDocument, path: string): unknown {
	if (document[path] !== undefined) return document[path];
	const parts = path.split(".");
	for (let boundary = parts.length - 1; boundary > 0; boundary--) {
		const head = parts.slice(0, boundary).join(".");
		const tail = parts.slice(boundary).join(".");
		const candidate = document[head];
		if (typeof candidate === "object" && candidate !== null) {
			const nested = getFieldValue(candidate as RawDocument, tail);
			if (nested !== undefined) return nested;
		}
	}

	let current: unknown = document;
	for (const part of parts) {
		if (typeof current !== "object" || current === null) return undefined;
		current = (current as RawDocument)[part];
	}
	return current;
}

/**
 * Converts supported timestamp inputs into nanoseconds with existing UI semantics.
 * @param value Raw timestamp value.
 * @returns Nanoseconds, or zero when conversion fails.
 */
function toNanoseconds(value: unknown): bigint {
	try {
		if (typeof value === "bigint") return value;
		if (value instanceof Date) return BigInt(value.getTime()) * 1_000_000n;
		if (
			typeof value === "object" &&
			value !== null &&
			"source" in value &&
			typeof value.source === "string"
		) {
			return BigInt(value.source);
		}
		if (typeof value === "number" && Number.isFinite(value)) {
			const normalized = Math.floor(value);
			const digits = String(normalized).replace(/^-/, "").length;
			if (digits === 19) return BigInt(normalized);
			if (digits === 16) return BigInt(normalized) * 1_000n;
			if (digits === 13) return BigInt(normalized) * 1_000_000n;
			if (digits <= 10) return BigInt(normalized) * 1_000_000_000n;
			return BigInt(normalized);
		}
		if (typeof value === "string") {
			if (/^-?\d+$/.test(value)) return toNanoseconds(Number(value));
			const parsed = Date.parse(value);
			return Number.isNaN(parsed) ? 0n : BigInt(parsed) * 1_000_000n;
		}
		return 0n;
	} catch {
		return 0n;
	}
}

/**
 * Converts backend timestamp units or ISO strings into timeline milliseconds.
 * @param value Raw `@timestamp`, `gulp.timestamp`, or already normalized timestamp.
 * @returns Rounded millisecond timestamp matching DataWorker normalization.
 */
function normalizeTimestamp(value: unknown): number {
	return Math.round(Number(toNanoseconds(value)) / 1_000_000);
}

/**
 * Hashes one string using the source-selected deterministic algorithm.
 * @param value String field value.
 * @param algorithm Configured source hash algorithm.
 * @returns Unsigned numeric hash reduced to the render range.
 */
function hashString(
	value: string,
	algorithm: DocumentStreamSourceSettings["hash_function"],
): number {
	let hash: number;
	switch (algorithm) {
		case "djb2":
			hash = 5_381;
			for (let index = 0; index < value.length; index++) {
				hash = Math.imul(hash, 33) + value.charCodeAt(index);
			}
			break;
		case "sdbm":
			hash = 0;
			for (let index = 0; index < value.length; index++) {
				hash = value.charCodeAt(index) + Math.imul(hash, 65_599);
			}
			break;
		case "fnv1a":
			hash = 0x811c9dc5;
			for (let index = 0; index < value.length; index++) {
				hash ^= value.charCodeAt(index);
				hash = Math.imul(hash, 0x01000193);
			}
			break;
	}
	return (hash >>> 0) % HASH_MODULUS;
}

/**
 * Converts one source render field into its numeric hash representation.
 * @param value Arbitrary source field value.
 * @param settings Source hash configuration.
 * @returns Stable render number.
 */
function normalizeNumberHash(
	value: unknown,
	settings: DocumentStreamSourceSettings,
): number {
	if (typeof value === "string") return hashString(value, settings.hash_function);
	if (typeof value === "number" && Number.isFinite(value)) return value;
	if (typeof value === "bigint") return normalizeTimestamp(value);
	const numeric = Number(value);
	return Number.isFinite(numeric) ? numeric : 0;
}

/**
 * Creates immutable authority helpers shared by every document in one chunk.
 * @param descriptor Registered stream settings and source authority.
 * @returns Batch-local normalization context.
 */
function createNormalizationContext(
	descriptor: DocumentStreamDescriptor,
): QueryDocumentNormalizationContext {
	const sourceIds = descriptor.sourceIds ?? [];
	return {
		fallbackSourceId: sourceIds.length === 1 ? sourceIds[0] : undefined,
		declaredSourceIds:
			sourceIds.length > 0 ? new Set<string>(sourceIds) : null,
	};
}

/**
 * Normalizes and appends one raw document without retaining its original object.
 * @param grouped Destination documents grouped by source encounter order.
 * @param value Raw document value.
 * @param descriptor Registered stream settings and source authority.
 * @param context Batch-local source authority helpers.
 */
function appendNormalizedDocument(
	grouped: Map<Source.Id, Doc.Type[]>,
	value: unknown,
	descriptor: DocumentStreamDescriptor,
	context: QueryDocumentNormalizationContext,
): void {
	if (typeof value !== "object" || value === null) {
		throw new TypeError("Document chunk contains a non-object value.");
	}
	const raw = value as RawDocument;
	const documentId = raw._id;
	const sourceValue = raw["gulp.source_id"] ?? context.fallbackSourceId;
	if (typeof documentId !== "string" || documentId.length === 0) {
		throw new TypeError("Document chunk contains an invalid _id.");
	}
	if (typeof sourceValue !== "string" || sourceValue.length === 0) {
		throw new TypeError("Document chunk contains an invalid gulp.source_id.");
	}
	if (
		context.declaredSourceIds &&
		!context.declaredSourceIds.has(sourceValue)
	) {
		throw new TypeError(
			`Document source ${sourceValue} is outside the registered stream.`,
		);
	}

	const settings =
		descriptor.sourceSettingsById?.[sourceValue] ??
		descriptor.defaultSourceSettings ??
		DEFAULT_SETTINGS;
	const rawTimestamp =
		raw["@timestamp"] ?? raw.gulp_timestamp ?? raw["gulp.timestamp"];
	const eventCode = Number(raw["gulp.event_code"] ?? 0);
	const document: Doc.Type = {
		_id: documentId as Doc.Id,
		gulp_timestamp: normalizeTimestamp(rawTimestamp),
		"gulp.source_id": sourceValue as Source.Id,
		"gulp.event_code": Number.isFinite(eventCode) ? eventCode : 0,
		number_hash: normalizeNumberHash(
			getFieldValue(raw, settings.field),
			settings,
		),
	};
	const sourceId = document["gulp.source_id"];
	const sourceDocuments = grouped.get(sourceId);
	if (sourceDocuments) sourceDocuments.push(document);
	else grouped.set(sourceId, [document]);
}

/**
 * Normalizes a document chunk into minimal canonical events grouped by source.
 * @param documents Parsed WebSocket document values.
 * @param descriptor Registered stream settings and source authority.
 * @returns Minimal events grouped by source in source encounter order.
 */
export function normalizeQueryDocuments(
	documents: readonly unknown[],
	descriptor: DocumentStreamDescriptor,
): Map<Source.Id, Doc.Type[]> {
	const grouped = new Map<Source.Id, Doc.Type[]>();
	const context = createNormalizationContext(descriptor);
	for (const value of documents) {
		appendNormalizedDocument(grouped, value, descriptor, context);
	}
	return grouped;
}

/**
 * Normalizes one chunk in worker tasks no longer than the configured time slice.
 * Raw objects remain bounded by the coordinator backlog and are released after the
 * promise settles; only minimal canonical documents survive into persistence.
 * @param documents Parsed WebSocket document values.
 * @param descriptor Registered stream settings and source authority.
 * @param options Injectable clock/yield hooks and maximum worker slice duration.
 * @returns Minimal events grouped by source in source encounter order.
 */
export async function normalizeQueryDocumentsCooperatively(
	documents: readonly unknown[],
	descriptor: DocumentStreamDescriptor,
	options: QueryDocumentNormalizationOptions = {},
): Promise<Map<Source.Id, Doc.Type[]>> {
	const grouped = new Map<Source.Id, Doc.Type[]>();
	const context = createNormalizationContext(descriptor);
	const now =
		options.now ??
		(() => globalThis.performance?.now?.() ?? Date.now());
	const yieldToEventLoop =
		options.yieldToEventLoop ??
		(() => new Promise<void>((resolve) => setTimeout(resolve, 0)));
	const maximumSliceMs = Math.max(
		0,
		options.maximumSliceMs ?? QUERY_DOCUMENT_NORMALIZATION_SLICE_MS,
	);
	let sliceStartedAt = now();
	for (let index = 0; index < documents.length; index++) {
		appendNormalizedDocument(
			grouped,
			documents[index],
			descriptor,
			context,
		);
		if (
			index + 1 < documents.length &&
			now() - sliceStartedAt >= maximumSliceMs
		) {
			await yieldToEventLoop();
			sliceStartedAt = now();
		}
	}
	return grouped;
}
