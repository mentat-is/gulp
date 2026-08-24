/** Maximum render-window delivery attempts for one unchanged Canvas request. */
export const CANVAS_RENDER_WINDOW_MAX_ATTEMPTS = 2;

/** Short delay allowing Canvas/worker lifecycle transitions to settle before retry. */
export const CANVAS_RENDER_WINDOW_RETRY_DELAY_MS = 50;

/** Identities and revisions that make one render-window request authoritative. */
export interface CanvasRenderWindowRequestFence {
	socket: unknown;
	operationEpoch: number;
	viewportRevision: number;
	projectionRevision: number;
}

/**
 * Checks whether an asynchronous render-window result still belongs to the latest
 * Canvas request.
 * @param expected Fence captured when the request was created.
 * @param current Current socket and revision authority.
 * @returns True only while every request identity remains current.
 */
export function isCanvasRenderWindowRequestCurrent(
	expected: CanvasRenderWindowRequestFence,
	current: CanvasRenderWindowRequestFence,
): boolean {
	return (
		expected.socket === current.socket &&
		expected.operationEpoch === current.operationEpoch &&
		expected.viewportRevision === current.viewportRevision &&
		expected.projectionRevision === current.projectionRevision
	);
}

/**
 * Applies the bounded retry policy to one failed current request.
 * @param attempt One-based attempt that just failed.
 * @param expected Fence captured when the request was created.
 * @param current Current socket and revision authority.
 * @returns True only when one further attempt is allowed for the latest request.
 */
export function shouldRetryCanvasRenderWindow(
	attempt: number,
	expected: CanvasRenderWindowRequestFence,
	current: CanvasRenderWindowRequestFence,
): boolean {
	return (
		attempt < CANVAS_RENDER_WINDOW_MAX_ATTEMPTS &&
		isCanvasRenderWindowRequestCurrent(expected, current)
	);
}

/**
 * Checks whether a projected source is older than the compact main-thread summary.
 * A newer IndexedDB revision is accepted because its summary may still be in transit.
 * @param projectedRevision Source revision carried by the render window.
 * @param currentRevision Latest DataStore summary revision, when known.
 * @returns True only for an observably obsolete projected source.
 */
export function isCanvasRenderWindowSourceStale(
	projectedRevision: number,
	currentRevision: number | undefined,
): boolean {
	return currentRevision !== undefined && projectedRevision < currentRevision;
}

/** Compact source summary fields used to validate one same-revision projection. */
export interface CanvasRenderWindowSourceSummary {
	revision: number;
	eventCount: number;
}

/**
 * Checks whether a projection disagrees with the committed count for its exact revision.
 * Newer projections remain valid because their compact summary may still be in transit.
 * @param projectedRevision Source revision carried by the render window.
 * @param loadedCount Event count encoded in that render-window source.
 * @param summary Latest compact source authority, when known.
 * @returns True only for a same-revision count mismatch.
 */
export function isCanvasRenderWindowSourceCountMismatch(
	projectedRevision: number,
	loadedCount: number,
	summary: CanvasRenderWindowSourceSummary | undefined,
): boolean {
	return (
		summary !== undefined &&
		projectedRevision === summary.revision &&
		loadedCount !== summary.eventCount
	);
}
