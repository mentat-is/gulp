/** Initial delay before checking an unconfirmed terminal render target. */
export const CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS = 500;

/** Maximum delay between terminal render convergence checks. */
export const CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS = 2_000;

/** Durable source state that must be acknowledged and included in a rendered frame. */
export interface CanvasTerminalRenderTarget {
	sourceId: string;
	operationEpoch: number;
	sourceAuthorityRevision: number;
	sourceGeneration: number;
	sourceRevision: number;
	sourceStatus: "complete" | "incomplete";
	eventCount: number;
}

/** Minimal render-window source fields needed to validate a terminal target. */
export interface CanvasTerminalRenderProjectionSource {
	sourceId: string;
	sourceAuthorityRevision: number;
	sourceGeneration: number;
	sourceRevision: number;
	sourceStatus: "building" | "complete" | "incomplete";
	loadedCount: number;
}

/** ACK-backed source revision and the data message that made it authoritative. */
export interface CanvasAcknowledgedSourceState {
	revision: number;
	dataSequence: number;
}

/**
 * Retains durable terminal source revisions until a worker frame proves visibility.
 * The controller owns only target and watchdog state; Canvas remains responsible for
 * deciding whether the current renderer, viewport, and queue can issue a projection.
 */
export class CanvasTerminalRenderConvergence {
	private readonly targets = new Map<
		string,
		CanvasTerminalRenderTarget & { recordedAtMs: number }
	>();
	private retryTimer: ReturnType<typeof setTimeout> | null = null;
	private nextRetryDelayMs = CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS;

	/**
	 * Creates terminal convergence state with an injectable monotonic clock.
	 * @param now Monotonic timestamp provider used by on-demand diagnostics.
	 */
	public constructor(private readonly now: () => number = () => performance.now()) {}

	/**
	 * Records the newest terminal revision for one source.
	 * @param target Durable source revision and exact committed event count.
	 * @returns True when the retained target changed.
	 */
	public record(target: CanvasTerminalRenderTarget): boolean {
		const current = this.targets.get(target.sourceId);
		if (current) {
			const sameOperation = current.operationEpoch === target.operationEpoch;
			const sameAuthority =
				current.sourceAuthorityRevision === target.sourceAuthorityRevision;
			const sameGeneration =
				current.sourceGeneration === target.sourceGeneration;
			const sameRevision = current.sourceRevision === target.sourceRevision;
			const isDuplicate =
				sameOperation &&
				sameAuthority &&
				sameGeneration &&
				sameRevision &&
				current.sourceStatus === target.sourceStatus &&
				current.eventCount === target.eventCount;
			const isObsolete =
				current.operationEpoch > target.operationEpoch ||
				(sameOperation &&
					(current.sourceAuthorityRevision > target.sourceAuthorityRevision ||
						(sameAuthority &&
							(current.sourceGeneration > target.sourceGeneration ||
								(sameGeneration &&
									current.sourceRevision > target.sourceRevision)))));
			if (isDuplicate || isObsolete) return false;
		}
		this.targets.set(target.sourceId, {
			...target,
			recordedAtMs: this.now(),
		});
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
				return (
					projection !== undefined &&
					projection.sourceAuthorityRevision ===
						target.sourceAuthorityRevision &&
					projection.sourceGeneration === target.sourceGeneration &&
					projection.sourceRevision === target.sourceRevision &&
					projection.sourceStatus === target.sourceStatus &&
					projection.loadedCount === target.eventCount
				);
			},
		);
	}

	/**
	 * Clears visible targets only when the completed frame includes their applying patch.
	 * @param operationEpoch Operation epoch represented by the rendered frame.
	 * @param visibleSourceIds Source identities rendered in the current viewport.
	 * @param acknowledgedSourceStates Worker revisions and the sequences that applied them.
	 * @param renderedDataSequence Highest committed DATA_PATCH included in the frame.
	 * @returns Source IDs whose terminal target converged in this frame.
	 */
	public acknowledgeFrame(
		operationEpoch: number,
		visibleSourceIds: ReadonlySet<string>,
		acknowledgedSourceStates: ReadonlyMap<
			string,
			CanvasAcknowledgedSourceState
		>,
		renderedDataSequence: number,
	): string[] {
		const completed: string[] = [];
		for (const target of this.getVisibleTargets(operationEpoch, visibleSourceIds)) {
			const acknowledged = acknowledgedSourceStates.get(target.sourceId);
			if (
				!acknowledged ||
				acknowledged.revision < target.sourceRevision ||
				renderedDataSequence < acknowledged.dataSequence
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
	 * Returns the age of the oldest terminal revision still awaiting a frame.
	 * @returns Oldest target age in milliseconds, or zero without pending targets.
	 */
	public getOldestTargetAgeMs(): number {
		let oldest = Number.POSITIVE_INFINITY;
		for (const target of this.targets.values()) {
			oldest = Math.min(oldest, target.recordedAtMs);
		}
		return Number.isFinite(oldest) ? Math.max(0, this.now() - oldest) : 0;
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
