import type { Doc } from "@/entities/Doc";

/**
 * Converts a raw ECS event sequence into the finite numeric form stored by EventStore.
 * Missing or malformed values retain the historical neutral ordering value.
 * @param value Raw `event.sequence` value.
 * @returns Finite event sequence, or zero when unavailable.
 */
export function normalizeEventSequence(value: unknown): number {
	try {
		const sequence =
			typeof value === "bigint" ? Number(value) : Number(value ?? 0);
		return Number.isFinite(sequence) ? sequence : 0;
	} catch {
		return 0;
	}
}

/**
 * Compares canonical order keys: newest timestamp first, then highest sequence.
 * @param leftTimestamp First event timestamp in milliseconds.
 * @param leftSequence First event sequence.
 * @param rightTimestamp Second event timestamp in milliseconds.
 * @param rightSequence Second event sequence.
 * @returns Negative when the left event precedes the right event.
 */
export function compareEventOrderValues(
	leftTimestamp: number,
	leftSequence: number,
	rightTimestamp: number,
	rightSequence: number,
): number {
	return rightTimestamp - leftTimestamp || rightSequence - leftSequence;
}

/**
 * Compares canonical documents using the persisted EventStore ordering contract.
 * @param left First canonical document.
 * @param right Second canonical document.
 * @returns Negative when the left document precedes the right document.
 */
export function compareEventDocuments(left: Doc.Type, right: Doc.Type): number {
	return compareEventOrderValues(
		left.gulp_timestamp,
		normalizeEventSequence(left["event.sequence"]),
		right.gulp_timestamp,
		normalizeEventSequence(right["event.sequence"]),
	);
}
