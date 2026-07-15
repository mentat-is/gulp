export const VIEWPORT_QUERY_SETTLE_DELAY_MS = 180;

/**
 * Tracks the trailing stabilization window used before viewport-priority queries run.
 */
export class ViewportQueryGate {
	private latestViewportChangeAt = Number.NEGATIVE_INFINITY;

	/**
	 * Creates a gate with an overridable delay for deterministic tests.
	 * @param settleDelayMs Required quiet time after the latest viewport row change.
	 */
	constructor(
		private readonly settleDelayMs = VIEWPORT_QUERY_SETTLE_DELAY_MS,
	) {}

	/**
	 * Restarts the stabilization window after the visible source row set changes.
	 * @param timestamp Timestamp of the viewport change in the performance clock domain.
	 * @returns Nothing.
	 */
	public markViewportChanged(timestamp = performance.now()): void {
		this.latestViewportChangeAt = timestamp;
	}

	/**
	 * Returns how long query dispatch must remain paused for the current viewport.
	 * @param timestamp Current timestamp in the performance clock domain.
	 * @returns Remaining delay in milliseconds, or zero when dispatch may continue.
	 */
	public getRemainingDelay(timestamp = performance.now()): number {
		return Math.max(
			0,
			this.settleDelayMs - (timestamp - this.latestViewportChangeAt),
		);
	}
}
