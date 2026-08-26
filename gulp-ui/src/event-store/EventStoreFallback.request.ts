export type EventStoreFallbackReason =
	| "quota"
	| "storage"
	| "renderer-fatal"
	| "renderer-unavailable";

/** One explicit compatibility decision requested by a scalable-pipeline failure. */
export interface EventStoreFallbackRequest {
	reason: EventStoreFallbackReason;
	message: string;
	estimatedEventCount: number;
}

type EventStoreFallbackRequestListener = (
	request: EventStoreFallbackRequest | null,
) => void;

let activeRequest: EventStoreFallbackRequest | null = null;
const listeners = new Set<EventStoreFallbackRequestListener>();

/**
 * Returns the currently active compatibility decision request.
 * @returns Active request or null when no decision is pending.
 */
export function getEventStoreFallbackRequest(): EventStoreFallbackRequest | null {
	return activeRequest;
}

/**
 * Publishes one normalized request and deduplicates simultaneous failure signals.
 * @param request Failure reason, detail, and estimated dataset size.
 * @returns True only when this call created the active request.
 */
export function publishEventStoreFallbackRequest(
	request: EventStoreFallbackRequest,
): boolean {
	if (activeRequest) return false;
	activeRequest = {
		...request,
		estimatedEventCount: Number.isFinite(request.estimatedEventCount)
			? Math.max(0, Math.floor(request.estimatedEventCount))
			: 0,
	};
	for (const listener of listeners) listener(activeRequest);
	return true;
}

/** Clears the active compatibility request without changing rollout storage. */
export function clearEventStoreFallbackRequest(): void {
	if (!activeRequest) return;
	activeRequest = null;
	for (const listener of listeners) listener(null);
}

/**
 * Subscribes Application context to compatibility request changes.
 * @param listener Context state updater.
 * @returns Cleanup that removes the listener.
 */
export function subscribeEventStoreFallbackRequest(
	listener: EventStoreFallbackRequestListener,
): () => void {
	listeners.add(listener);
	return () => {
		listeners.delete(listener);
	};
}
