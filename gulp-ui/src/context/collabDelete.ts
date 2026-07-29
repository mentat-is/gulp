/** Payload carried by singular and bulk collaboration delete events. */
export interface CollabDeletePayload {
	type: string;
	id?: unknown;
	ids?: unknown;
}

/** Operation fields supported by websocket delete envelopes. */
export interface CollabDeleteOperationEnvelope {
	operation_id?: unknown;
	"gulp.operation_id"?: unknown;
}

/**
 * Resolves the operation identifier from the websocket wire field or its alias.
 * @param message Delete envelope received from the websocket.
 * @returns Valid operation identifier, or undefined when neither field is usable.
 */
export function normalizeCollabDeleteOperationId(
	message: CollabDeleteOperationEnvelope,
): string | undefined {
	const candidates = [
		message.operation_id,
		message["gulp.operation_id"],
	];
	return candidates.find(
		(candidate): candidate is string =>
			typeof candidate === "string" && candidate.length > 0,
	);
}

/**
 * Normalizes singular and bulk collaboration delete payloads to unique IDs.
 * @param payload Websocket delete payload received from the backend.
 * @returns Valid non-empty IDs in their original order.
 */
export function normalizeCollabDeleteIds(
	payload: CollabDeletePayload,
): string[] {
	const candidates = Array.isArray(payload.ids)
		? payload.ids
		: [payload.id];
	const uniqueIds = new Set<string>();

	candidates.forEach((candidate) => {
		if (typeof candidate !== "string" || candidate.length === 0) return;
		uniqueIds.add(candidate);
	});

	return [...uniqueIds];
}
