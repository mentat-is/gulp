/** Initial delay before checking an unconfirmed terminal render target. */
export const CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS = 500;

/** Maximum delay between terminal render convergence checks. */
export const CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS = 2_000;

/** Durable source state that must be acknowledged and included in a rendered frame. */
export interface CanvasTerminalRenderTarget {
	sourceId: string;
	operationEpoch: number;
	sourceRevision: number;
	eventCount: number;
}

/** Minimal render-window source fields needed to validate a terminal target. */
export interface CanvasTerminalRenderProjectionSource {
	sourceId: string;
	sourceRevision: number;
	loadedCount: number;
}

/**
 * Retains durable terminal source revisions until a worker frame proves visibility.
 * The controller owns only target and watchdog state; Canvas remains responsible for
 * deciding whether the current renderer, viewport, and queue can issue a projection.
 */
export class CanvasTerminalRenderConvergence {
	private readonly targets = new Map<string, CanvasTerminalRenderTarget>();
	private retryTimer: ReturnType<typeof setTimeout> | null = null;
	private nextRetryDelayMs = CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS;

	/**
	 * Records the newest terminal revision for one source.
	 * @param target Durable source revision and exact committed event count.
	 * @returns True when the retained target changed.
	 */
	public record(target: CanvasTerminalRenderTarget): boolean {
		const current = this.targets.get(target.sourceId);
		if (
			current &&
			(current.operationEpoch > target.operationEpoch ||
				(current.operationEpoch === target.operationEpoch &&
					(current.sourceRevision > target.sourceRevision ||
						(current.sourceRevision === target.sourceRevision &&
							current.eventCount === target.eventCount))))
		) {
			return false;
		}
		this.targets.set(target.sourceId, { ...target });
		this.cancelWatchdog(true);
		return true;
	}

	/**
	 * Returns pending targets that belong to the active operation and visible rows.
	 * @param operationEpoch Current render operation epoch.
	 * @param visibleSourceIds Source identities in the vertically rendered viewport.
	 * @returns Deterministically ordered terminal targets eligible for projection.
	 */
	public getVisibleTargets(
		operationEpoch: number,
		visibleSourceIds: ReadonlySet<string>,
	): CanvasTerminalRenderTarget[] {
		return Array.from(this.targets.values())
			.filter(
				(target) =>
					target.operationEpoch === operationEpoch &&
					visibleSourceIds.has(target.sourceId),
			)
			.sort((left, right) => left.sourceId.localeCompare(right.sourceId));
	}

	/**
	 * Checks that one complete render window contains every visible terminal target.
	 * A newer source revision is valid because its compact summary may still be in transit.
	 * @param operationEpoch Current render operation epoch.
	 * @param visibleSourceIds Source identities in the current vertical viewport.
	 * @param sources Complete source projections returned for that viewport.
	 * @returns True when no visible terminal source is missing or obsolete.
	 */
	public acceptsProjection(
		operationEpoch: number,
		visibleSourceIds: ReadonlySet<string>,
		sources: readonly CanvasTerminalRenderProjectionSource[],
	): boolean {
		const projections = new Map(sources.map((source) => [source.sourceId, source]));
		return this.getVisibleTargets(operationEpoch, visibleSourceIds).every(
			(target) => {
				const projection = projections.get(target.sourceId);
				if (!projection || projection.sourceRevision < target.sourceRevision) {
					return false;
				}
				return (
					projection.sourceRevision > target.sourceRevision ||
					projection.loadedCount === target.eventCount
				);
			},
		);
	}

	/**
	 * Clears visible targets whose ACK was observed before the completed frame callback.
	 * @param operationEpoch Operation epoch represented by the rendered frame.
	 * @param visibleSourceIds Source identities rendered in the current viewport.
	 * @param acknowledgedSourceRevisions Worker repository revisions applied before the frame.
	 * @returns Source IDs whose terminal target converged in this frame.
	 */
	public acknowledgeFrame(
		operationEpoch: number,
		visibleSourceIds: ReadonlySet<string>,
		acknowledgedSourceRevisions: ReadonlyMap<string, number>,
	): string[] {
		const completed: string[] = [];
		for (const target of this.getVisibleTargets(operationEpoch, visibleSourceIds)) {
			if (
				(acknowledgedSourceRevisions.get(target.sourceId) ?? 0) <
				target.sourceRevision
			) {
				continue;
			}
			this.targets.delete(target.sourceId);
			completed.push(target.sourceId);
		}
		if (this.targets.size === 0) this.cancelWatchdog(true);
		return completed;
	}

	/**
	 * Removes targets for sources that no longer belong to the render scene.
	 * @param sourceIds Removed source identities.
	 * @returns Nothing.
	 */
	public removeSources(sourceIds: Iterable<string>): void {
		for (const sourceId of sourceIds) this.targets.delete(sourceId);
		if (this.targets.size === 0) this.cancelWatchdog(true);
	}

	/**
	 * Reports whether any source still requires a terminal render confirmation.
	 * @returns True while at least one target remains pending.
	 */
	public hasTargets(): boolean {
		return this.targets.size > 0;
	}

	/**
	 * Schedules the sole convergence watchdog using capped exponential backoff.
	 * @param callback Canvas-owned eligibility check and reprojection action.
	 * @returns Nothing.
	 */
	public scheduleWatchdog(callback: () => void): void {
		if (this.retryTimer !== null || this.targets.size === 0) return;
		const delay = this.nextRetryDelayMs;
		this.retryTimer = setTimeout(() => {
			this.retryTimer = null;
			this.nextRetryDelayMs = Math.min(
				CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS,
				delay * 2,
			);
			callback();
		}, delay);
	}

	/**
	 * Cancels the current watchdog and optionally restores its initial delay.
	 * @param resetDelay Whether the next check should use the initial delay.
	 * @returns Nothing.
	 */
	public cancelWatchdog(resetDelay = false): void {
		if (this.retryTimer !== null) clearTimeout(this.retryTimer);
		this.retryTimer = null;
		if (resetDelay) {
			this.nextRetryDelayMs =
				CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS;
		}
	}

	/**
	 * Clears every source target and resets watchdog state at an operation boundary.
	 * @returns Nothing.
	 */
	public reset(): void {
		this.targets.clear();
		this.cancelWatchdog(true);
	}
}
