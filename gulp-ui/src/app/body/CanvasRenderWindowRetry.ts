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
	authorityRevision: number;
	generation: number;
	revision: number;
	status: "building" | "complete" | "incomplete";
	eventCount: number;
}

/** Projection authority fields used to reject obsolete source generations. */
export interface CanvasRenderWindowSourceProjectionState {
	sourceAuthorityRevision: number;
	sourceGeneration: number;
	sourceRevision: number;
	sourceStatus: "building" | "complete" | "incomplete";
	loadedCount: number;
}

/** Last logical authority, physical generation, and revision sent to RenderWorker. */
export interface CanvasAcceptedRenderWindowSourceState {
	authorityRevision: number;
	generation: number;
	revision: number;
}

/**
 * Validates a source projection against summary authority and the last accepted snapshot.
 * Building snapshots may trail a newer summary in the same generation when they still
 * advance Canvas. Before the first summary, the canonical empty placeholder represents
 * an uninitialized source row. Other terminal snapshots require exact authority.
 * @param projection Projected manifest identity and event count.
 * @param summary Latest compact source authority, when known.
 * @param accepted Last projection transferred for this source, when known.
 * @returns True when the source is coherent and monotonic for publication.
 */
export function isCanvasRenderWindowSourceAcceptable(
	projection: CanvasRenderWindowSourceProjectionState,
	summary: CanvasRenderWindowSourceSummary | undefined,
	accepted: CanvasAcceptedRenderWindowSourceState | undefined,
): boolean {
	if (
		accepted &&
		(projection.sourceAuthorityRevision < accepted.authorityRevision ||
			(projection.sourceAuthorityRevision === accepted.authorityRevision &&
				(projection.sourceGeneration < accepted.generation ||
					(projection.sourceGeneration === accepted.generation &&
						projection.sourceRevision < accepted.revision))))
	) {
		return false;
	}
	if (!summary) {
		const isUninitialized =
			projection.sourceAuthorityRevision === 0 &&
			projection.sourceGeneration === 0 &&
			projection.sourceRevision === 0 &&
			projection.sourceStatus === "incomplete" &&
			projection.loadedCount === 0;
		return projection.sourceStatus === "building" || isUninitialized;
	}
	if (projection.sourceStatus !== "building") {
		return (
			projection.sourceAuthorityRevision === summary.authorityRevision &&
			projection.sourceGeneration === summary.generation &&
			projection.sourceRevision === summary.revision &&
			projection.sourceStatus === summary.status &&
			projection.loadedCount === summary.eventCount
		);
	}
	if (projection.sourceAuthorityRevision < summary.authorityRevision) return false;
	if (projection.sourceAuthorityRevision > summary.authorityRevision) return true;
	if (projection.sourceGeneration < summary.generation) return false;
	if (
		projection.sourceGeneration === summary.generation &&
		projection.sourceRevision < summary.revision &&
		accepted?.generation === projection.sourceGeneration &&
		projection.sourceRevision <= accepted.revision
	) {
		return false;
	}
	return true;
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
