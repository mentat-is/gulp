/** Instance-owned external store for the canonical timeline scale. */
export class TimelineScaleStore {
	private snapshot: number;
	private lastObservedReactSnapshot: number;
	private hasRenderlessCommit = false;
	private pendingReactCommit: number | null = null;
	private readonly listeners = new Set<() => void>();

	/**
	 * Creates a scale store initialized from its owning Info instance.
	 * @param initialScale Initial canonical timeline scale.
	 */
	public constructor(initialScale: number) {
		this.snapshot = initialScale;
		this.lastObservedReactSnapshot = initialScale;
	}

	/**
	 * Returns the current canonical scale snapshot.
	 * @returns Current timeline scale.
	 */
	public getSnapshot = (): number => this.snapshot;

	/**
	 * Tracks a React-provided canonical scale without emitting during render.
	 * @param scale Scale carried by the latest application state object.
	 * @returns Nothing.
	 */
	public synchronize = (scale: number): void => {
		this.snapshot = scale;
	};

	/**
	 * Marks an intentional React-state scale write before it reaches the provider.
	 * @param scale Scale expected in the queued React state update.
	 * @param synchronizeSnapshot Whether the intended value is already authoritative.
	 * @returns Nothing.
	 */
	public prepareReactCommit = (
		scale: number,
		synchronizeSnapshot = false,
	): void => {
		this.pendingReactCommit = scale;
		if (synchronizeSnapshot) {
			this.snapshot = scale;
			this.hasRenderlessCommit = false;
		}
	};

	/**
	 * Reconciles provider state while rebasing already-computed stale snapshots.
	 * @param incomingScale Scale carried by the provider's latest state object.
	 * @returns Effective canonical scale that the provider must expose.
	 */
	public reconcileReactSnapshot = (incomingScale: number): number => {
		let effectiveScale = incomingScale;
		if (
			this.pendingReactCommit !== null &&
			incomingScale === this.pendingReactCommit
		) {
			this.pendingReactCommit = null;
			this.hasRenderlessCommit = false;
			this.snapshot = incomingScale;
		} else if (this.pendingReactCommit !== null) {
			effectiveScale = this.snapshot;
		} else if (this.hasRenderlessCommit) {
			if (incomingScale === this.snapshot) {
				this.hasRenderlessCommit = false;
			} else if (incomingScale === this.lastObservedReactSnapshot) {
				effectiveScale = this.snapshot;
			} else {
				this.hasRenderlessCommit = false;
				this.snapshot = incomingScale;
			}
		} else {
			this.hasRenderlessCommit = false;
			this.snapshot = incomingScale;
		}
		this.lastObservedReactSnapshot = incomingScale;
		return effectiveScale;
	};

	/**
	 * Commits a renderless scale and notifies subscribed external-store consumers.
	 * @param scale Settled canonical timeline scale.
	 * @returns True when the snapshot changed.
	 */
	public commit = (scale: number): boolean => {
		this.hasRenderlessCommit = true;
		if (this.snapshot === scale) return false;
		this.snapshot = scale;
		[...this.listeners].forEach((listener) => listener());
		return true;
	};

	/**
	 * Subscribes to renderless canonical scale commits.
	 * @param listener Callback invoked after the scale snapshot changes.
	 * @returns Function that removes the listener.
	 */
	public subscribe = (listener: () => void): (() => void) => {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	};
}
