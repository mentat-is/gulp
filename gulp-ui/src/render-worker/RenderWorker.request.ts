import { EVENT_RENDER_PROJECTION_LIMIT } from "@/event-store/EventStore.types";
import {
	RENDER_PROTOCOL_VERSION,
	type RenderProtocolEnvelope,
	type RenderWindowProjection,
	type RenderWindowSourceProjection,
} from "./RenderWorker.types";

/** Data request shape narrowed to a patch that carries one complete render window. */
export type RenderWindowDataRequest = RenderProtocolEnvelope<
	"DATA_PATCH",
	{
		globalRevision: number;
		renderWindow: RenderWindowProjection;
	}
>;

/**
 * Returns whether a value is an object that can safely be inspected.
 * @param value Candidate protocol value.
 * @returns True when the value is a non-null record.
 */
function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

/**
 * Validates a non-negative protocol revision or event count.
 * @param value Candidate numeric value.
 * @returns True for non-negative safe integers.
 */
function isNonNegativeSafeInteger(value: unknown): value is number {
	return (
		typeof value === "number" &&
		Number.isSafeInteger(value) &&
		value >= 0
	);
}

/**
 * Validates a finite inclusive range without scanning projection buffers.
 * @param value Candidate range object.
 * @returns True when min and max are finite and ordered.
 */
function isFiniteRange(value: unknown): value is { min: number; max: number } {
	if (!isRecord(value)) return false;
	return (
		typeof value.min === "number" &&
		Number.isFinite(value.min) &&
		typeof value.max === "number" &&
		Number.isFinite(value.max) &&
		value.min <= value.max
	);
}

/**
 * Validates two correlated Float64 projection columns.
 * @param first Candidate timestamp column.
 * @param second Candidate aggregate-value column.
 * @returns True when both values are transferable Float64 arrays of equal length.
 */
function isFloat64Pair(first: unknown, second: unknown): boolean {
	return (
		first instanceof Float64Array &&
		first.buffer instanceof ArrayBuffer &&
		second instanceof Float64Array &&
		second.buffer instanceof ArrayBuffer &&
		first.length === second.length
	);
}

/**
 * Validates one source projection before worker hot-loop code dereferences its columns.
 * Validation is constant-time with respect to representative count and checks only shape.
 * @param value Candidate source projection.
 * @returns True when metadata and every correlated typed-array column are compatible.
 */
export function isRenderWindowSourceProjection(
	value: unknown,
): value is RenderWindowSourceProjection {
	if (!isRecord(value)) return false;
	if (
		typeof value.sourceId !== "string" ||
		!isNonNegativeSafeInteger(value.sourceRevision) ||
		!isFiniteRange(value.coveredRange) ||
		typeof value.bucketPixelWidth !== "number" ||
		!Number.isFinite(value.bucketPixelWidth) ||
		value.bucketPixelWidth <= 0 ||
		typeof value.degradedLod !== "boolean" ||
		!isNonNegativeSafeInteger(value.loadedCount) ||
		typeof value.heightMaximumCount !== "number" ||
		!Number.isFinite(value.heightMaximumCount) ||
		value.heightMaximumCount < 0 ||
		!isFiniteRange(value.hashRange)
	) {
		return false;
	}

	const handles = value.representativeHandles;
	const timestamps = value.representativeTimestamps;
	const hashes = value.representativeHashes;
	if (
		!(handles instanceof Uint32Array) ||
		!(handles.buffer instanceof ArrayBuffer) ||
		!(timestamps instanceof Float64Array) ||
		!(timestamps.buffer instanceof ArrayBuffer) ||
		!(hashes instanceof Float64Array) ||
		!(hashes.buffer instanceof ArrayBuffer) ||
		handles.length !== timestamps.length ||
		timestamps.length !== hashes.length ||
		handles.length > value.loadedCount
	) {
		return false;
	}

	return (
		isFloat64Pair(value.heightTimestamps, value.heightCounts) &&
		isFloat64Pair(value.graphTimestamps, value.graphCounts)
	);
}

/**
 * Validates an atomic multi-source render-window projection.
 * Duplicate and simultaneously evicted source identities are rejected.
 * @param value Candidate projection payload.
 * @returns True when the complete projection can be accepted as one replacement.
 */
export function isRenderWindowProjection(
	value: unknown,
): value is RenderWindowProjection {
	if (
		!isRecord(value) ||
		!isNonNegativeSafeInteger(value.operationEpoch) ||
		!isNonNegativeSafeInteger(value.viewportRevision) ||
		!isNonNegativeSafeInteger(value.projectionRevision) ||
		!Array.isArray(value.sources) ||
		!Array.isArray(value.evictedSourceIds) ||
		!value.evictedSourceIds.every(
			(sourceId) => typeof sourceId === "string",
		)
	) {
		return false;
	}

	const sourceIds = new Set<string>();
	let retainedRecords = 0;
	for (const source of value.sources) {
		if (
			!isRenderWindowSourceProjection(source) ||
			sourceIds.has(source.sourceId)
		) {
			return false;
		}
		sourceIds.add(source.sourceId);
		retainedRecords +=
			source.representativeHandles.length +
			source.heightTimestamps.length +
			source.graphTimestamps.length;
		if (retainedRecords > EVENT_RENDER_PROJECTION_LIMIT) return false;
	}

	const evictedSourceIds = new Set(value.evictedSourceIds);
	return (
		evictedSourceIds.size === value.evictedSourceIds.length &&
		value.evictedSourceIds.every((sourceId) => !sourceIds.has(sourceId))
	);
}

/**
 * Validates the protocol envelope and operation consistency for a render-window request.
 * Other optional patch sections may coexist but are intentionally validated by their owners.
 * @param value Candidate DATA_PATCH message.
 * @returns True when the message carries one valid atomic render-window projection.
 */
export function isRenderWindowDataRequest(
	value: unknown,
): value is RenderWindowDataRequest {
	if (
		!isRecord(value) ||
		value.protocolVersion !== RENDER_PROTOCOL_VERSION ||
		!isNonNegativeSafeInteger(value.operationEpoch) ||
		!isNonNegativeSafeInteger(value.sequence) ||
		value.type !== "DATA_PATCH" ||
		!isRecord(value.payload) ||
		!isNonNegativeSafeInteger(value.payload.globalRevision) ||
		!isRenderWindowProjection(value.payload.renderWindow)
	) {
		return false;
	}

	return value.payload.renderWindow.operationEpoch === value.operationEpoch;
}
