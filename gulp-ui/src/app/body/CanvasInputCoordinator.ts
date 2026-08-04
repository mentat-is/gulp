import { DirtyCanvas } from "@/render-worker/RenderWorker.types";
import { normalizeWheelDelta } from "./CanvasWheelViewport";

/** Minimal native wheel values retained until the next display tick. */
export interface CanvasWheelInput {
	deltaX: number;
	deltaY: number;
	deltaMode: number;
	clientX: number;
	clientY: number;
	inputTimestamp: number;
}

/** Minimal native pointer values retained until the next display tick. */
export interface CanvasPointerInput {
	clientX: number;
	clientY: number;
	inside: boolean;
	inputTimestamp: number;
}

/** Coalesced high-frequency Canvas input consumed once per display tick. */
export interface CanvasInputBatch {
	sequence: number;
	inputTimestamp: number;
	wheel: {
		panDeltaX: number;
		zoomDeltaY: number;
		zoomPoint: { clientX: number; clientY: number } | null;
		zoomGestureToken: number | null;
	} | null;
	pointer: CanvasPointerInput | null;
	scrollDeltaX: number;
	scrollDeltaY: number;
	dirtyMask: DirtyCanvas;
}

/** Classification returned when canonical scale state changes. */
export type CanvasScaleReconciliation =
	| "unchanged"
	| "self-commit"
	| "external";

/**
 * Guards transient wheel scale commits against newer external scale commands.
 */
export class CanvasScaleCommitGuard {
	private canonicalScale: number;
	private activeGesture: {
		token: number;
		originCanonicalScale: number;
	} | null = null;
	private pendingCommit: { token: number; scale: number } | null = null;

	/**
	 * Creates a guard synchronized with the initial canonical timeline scale.
	 * @param initialCanonicalScale Canonical scale at Canvas mount time.
	 */
	public constructor(initialCanonicalScale: number) {
		this.canonicalScale = initialCanonicalScale;
	}

	/**
	 * Records the canonical origin of a newly observed wheel gesture.
	 * @param gestureToken Coordinator-issued identity for the active gesture.
	 * @param originCanonicalScale Canonical scale when its first batch was applied.
	 * @returns Nothing.
	 */
	public beginGesture(
		gestureToken: number,
		originCanonicalScale: number,
	): void {
		if (this.activeGesture?.token === gestureToken) return;
		this.activeGesture = {
			token: gestureToken,
			originCanonicalScale,
		};
		this.pendingCommit = null;
	}

	/**
	 * Marks a transient scale as safe to persist when its canonical origin is current.
	 * @param gestureToken Gesture identity delivered by the settle callback.
	 * @param transientScale Latest live scale rendered during the gesture.
	 * @param currentCanonicalScale Canonical scale visible when persistence is attempted.
	 * @returns Scale to persist, or null when the gesture became stale.
	 */
	public prepareCommit(
		gestureToken: number,
		transientScale: number,
		currentCanonicalScale: number,
	): number | null {
		if (
			this.activeGesture?.token !== gestureToken ||
			this.activeGesture.originCanonicalScale !== this.canonicalScale ||
			currentCanonicalScale !== this.canonicalScale
		) {
			return null;
		}
		this.pendingCommit = { token: gestureToken, scale: transientScale };
		return transientScale;
	}

	/**
	 * Discards an active gesture when an explicit external scale command wins.
	 * @returns Nothing.
	 */
	public cancelGesture(): void {
		this.activeGesture = null;
		this.pendingCommit = null;
	}

	/**
	 * Reconciles a React-provided canonical scale with any prepared wheel commit.
	 * @param nextCanonicalScale Latest canonical application scale.
	 * @returns Whether the value is unchanged, this gesture's commit, or external.
	 */
	public reconcileCanonicalScale(
		nextCanonicalScale: number,
	): CanvasScaleReconciliation {
		if (nextCanonicalScale === this.canonicalScale) return "unchanged";
		const isSelfCommit =
			this.pendingCommit !== null &&
			this.activeGesture?.token === this.pendingCommit.token &&
			this.pendingCommit.scale === nextCanonicalScale;
		this.canonicalScale = nextCanonicalScale;
		this.activeGesture = null;
		this.pendingCommit = null;
		return isSelfCommit ? "self-commit" : "external";
	}
}

/** Display-frame scheduling primitives injectable by deterministic tests. */
export interface CanvasInputFrameScheduler {
	request(callback: FrameRequestCallback): number;
	cancel(handle: number): void;
}

/** Timer primitives used to detect the end of a wheel gesture. */
export interface CanvasInputSettleScheduler {
	schedule(callback: () => void, delayMs: number): ReturnType<typeof setTimeout>;
	cancel(handle: ReturnType<typeof setTimeout>): void;
}

const browserFrameScheduler: CanvasInputFrameScheduler = {
	/**
	 * Requests a browser display callback.
	 * @param callback Callback invoked before the next paint.
	 * @returns Browser animation-frame handle.
	 */
	request: (callback) => requestAnimationFrame(callback),
	/**
	 * Cancels a browser display callback.
	 * @param handle Animation-frame handle to cancel.
	 * @returns Nothing.
	 */
	cancel: (handle) => cancelAnimationFrame(handle),
};

const browserSettleScheduler: CanvasInputSettleScheduler = {
	/**
	 * Schedules a wheel-settle callback.
	 * @param callback Callback invoked after the quiet interval.
	 * @param delayMs Quiet interval in milliseconds.
	 * @returns Browser timeout handle.
	 */
	schedule: (callback, delayMs) => setTimeout(callback, delayMs),
	/**
	 * Cancels a wheel-settle callback.
	 * @param handle Timeout handle to cancel.
	 * @returns Nothing.
	 */
	cancel: (handle) => clearTimeout(handle),
};

/**
 * Coalesces wheel, drag, pointer, and invalidation work behind one main-thread rAF.
 */
export class CanvasInputCoordinator {
	private frameHandle: number | null = null;
	private nextSequence = 0;
	private earliestInputTimestamp: number | null = null;
	private wheelPanDeltaX = 0;
	private wheelZoomDeltaY = 0;
	private latestZoomPoint: Pick<CanvasWheelInput, "clientX" | "clientY"> | null = null;
	private hasWheelInput = false;
	private latestPointer: CanvasPointerInput | null = null;
	private scrollDeltaX = 0;
	private scrollDeltaY = 0;
	private dirtyMask = DirtyCanvas.None;
	private disposed = false;
	private wheelSettleHandle: ReturnType<typeof setTimeout> | null = null;
	private nextWheelGestureToken = 0;
	private activeWheelGestureToken: number | null = null;
	private wheelSettleGeneration = 0;
	private wheelSettleDeadline = Number.NEGATIVE_INFINITY;

	/**
	 * Creates a coordinator for one mounted Canvas.
	 * @param flush Consumer invoked once with the latest accumulated input batch.
	 * @param scheduler Display-frame scheduler, injectable for deterministic tests.
	 * @param onWheelSettled Consumer that persists transient wheel state after inactivity.
	 * @param settleScheduler Timer scheduler, injectable for deterministic tests.
	 * @param wheelSettleDelayMs Quiet interval that identifies the end of a wheel gesture.
	 * @param now Reads the monotonic clock used by the trailing settle deadline.
	 */
	public constructor(
		private readonly flush: (batch: CanvasInputBatch) => void,
		private readonly scheduler: CanvasInputFrameScheduler = browserFrameScheduler,
		private readonly onWheelSettled?: (gestureToken: number) => void,
		private readonly settleScheduler: CanvasInputSettleScheduler =
			browserSettleScheduler,
		private readonly wheelSettleDelayMs = 100,
		private readonly now: () => number = () => performance.now(),
	) {}

	/**
	 * Accumulates one normalized wheel event without cancelling native propagation.
	 * @param input Native wheel values captured by the passive listener.
	 * @param lineHeightPx Cached CSS line height used for line-mode deltas.
	 * @param pageHeightPx Cached viewport height used for page-mode deltas.
	 * @returns Nothing.
	 */
	public queueWheel(
		input: CanvasWheelInput,
		lineHeightPx: number,
		pageHeightPx: number,
	): void {
		if (this.disposed) return;
		const normalized = normalizeWheelDelta(input, lineHeightPx, pageHeightPx);
		if (!Number.isFinite(normalized.deltaX) || !Number.isFinite(normalized.deltaY)) {
			return;
		}
		if (normalized.deltaX === 0 && normalized.deltaY === 0) return;
		this.hasWheelInput = true;
		if (Math.abs(normalized.deltaX) > Math.abs(normalized.deltaY)) {
			this.wheelPanDeltaX += normalized.deltaX;
		} else if (normalized.deltaY !== 0) {
			this.wheelZoomDeltaY += normalized.deltaY;
			this.latestZoomPoint = {
				clientX: input.clientX,
				clientY: input.clientY,
			};
			if (this.activeWheelGestureToken === null) {
				this.activeWheelGestureToken = ++this.nextWheelGestureToken;
			}
			this.wheelSettleDeadline = this.now() + this.wheelSettleDelayMs;
			this.scheduleWheelSettle();
		}
		this.recordInputTimestamp(input.inputTimestamp);
		this.dirtyMask |= DirtyCanvas.Viewport;
		this.schedule();
	}

	/**
	 * Reports whether a wheel gesture still has a pending persistence callback.
	 * @returns True while waiting for the wheel quiet interval.
	 */
	public get isWheelGesturePending(): boolean {
		return this.activeWheelGestureToken !== null;
	}

	/**
	 * Replaces the pending pointer sample with the latest native position.
	 * @param pointer Native pointer coordinates and inside state.
	 * @returns Nothing.
	 */
	public queuePointer(pointer: CanvasPointerInput): void {
		if (this.disposed) return;
		this.latestPointer = pointer;
		this.recordInputTimestamp(pointer.inputTimestamp);
		this.schedule();
	}

	/**
	 * Adds drag or scrollbar movement to the next atomic viewport update.
	 * @param deltaX Horizontal scroll delta in CSS pixels.
	 * @param deltaY Vertical scroll delta in CSS pixels.
	 * @param inputTimestamp Time at which the originating native input arrived.
	 * @returns Nothing.
	 */
	public queueScrollDelta(
		deltaX: number,
		deltaY: number,
		inputTimestamp = performance.now(),
	): void {
		if (this.disposed) return;
		this.scrollDeltaX += deltaX;
		this.scrollDeltaY += deltaY;
		this.recordInputTimestamp(inputTimestamp);
		this.dirtyMask |= DirtyCanvas.Viewport;
		this.schedule();
	}

	/**
	 * Adds render invalidation reasons to the next display-tick update.
	 * @param dirtyMask Worker or legacy Canvas invalidation reasons.
	 * @param inputTimestamp Optional initiating timestamp for end-to-end diagnostics.
	 * @returns Nothing.
	 */
	public invalidate(
		dirtyMask: DirtyCanvas,
		inputTimestamp = performance.now(),
	): void {
		if (this.disposed || dirtyMask === DirtyCanvas.None) return;
		this.dirtyMask |= dirtyMask;
		this.recordInputTimestamp(inputTimestamp);
		this.schedule();
	}

	/**
	 * Flushes pending work synchronously, cancelling a scheduled display callback.
	 * @returns Nothing.
	 */
	public flushNow(): void {
		if (this.disposed || !this.hasPendingWork()) return;
		if (this.frameHandle !== null) {
			this.scheduler.cancel(this.frameHandle);
			this.frameHandle = null;
		}
		this.flushPendingBatch();
	}

	/**
	 * Cancels pending work while leaving the coordinator reusable after effect replay.
	 * @returns Nothing.
	 */
	public cancelPending(): void {
		if (this.disposed) return;
		if (this.frameHandle !== null) {
			this.scheduler.cancel(this.frameHandle);
			this.frameHandle = null;
		}
		this.resetPendingState();
	}

	/**
	 * Immediately invokes a pending wheel-settle callback, if any.
	 * @returns Nothing.
	 */
	public flushWheelSettled(): void {
		const gestureToken = this.activeWheelGestureToken;
		if (gestureToken === null) return;
		this.flushNow();
		if (this.wheelSettleHandle !== null) {
			this.settleScheduler.cancel(this.wheelSettleHandle);
		}
		this.wheelSettleHandle = null;
		this.activeWheelGestureToken = null;
		this.wheelSettleGeneration++;
		this.wheelSettleDeadline = Number.NEGATIVE_INFINITY;
		this.onWheelSettled?.(gestureToken);
	}

	/**
	 * Cancels an active wheel gesture without persisting its transient state.
	 * A generation bump also rejects a timer callback that was already queued.
	 * @returns Nothing.
	 */
	public cancelWheelGesture(): void {
		if (this.wheelSettleHandle !== null) {
			this.settleScheduler.cancel(this.wheelSettleHandle);
		}
		this.wheelSettleHandle = null;
		this.activeWheelGestureToken = null;
		this.wheelSettleGeneration++;
		this.wheelSettleDeadline = Number.NEGATIVE_INFINITY;
		this.wheelZoomDeltaY = 0;
		this.latestZoomPoint = null;
		if (this.wheelPanDeltaX === 0) this.hasWheelInput = false;
	}

	/**
	 * Cancels pending work and permanently disables this coordinator.
	 * @returns Nothing.
	 */
	public dispose(): void {
		if (this.disposed) return;
		this.flushWheelSettled();
		this.cancelPending();
		this.disposed = true;
	}

	/**
	 * Arms one trailing deadline timer used to persist transient wheel state once.
	 * Later wheel events only move the deadline; the existing timer checks that
	 * deadline before settling, avoiding cancel/recreate churn for native bursts.
	 * @returns Nothing.
	 */
	private scheduleWheelSettle(): void {
		const gestureToken = this.activeWheelGestureToken;
		if (!this.onWheelSettled || gestureToken === null) return;
		if (this.wheelSettleHandle !== null) return;
		const settleGeneration = this.wheelSettleGeneration;

		/**
		 * Settles at the latest deadline without flushing unrelated rAF work.
		 * @returns Nothing.
		 */
		const settleAtDeadline = (): void => {
			if (
				this.wheelSettleGeneration !== settleGeneration ||
				this.activeWheelGestureToken !== gestureToken
			) {
				return;
			}
			const remainingDelayMs = this.wheelSettleDeadline - this.now();
			if (remainingDelayMs > 0) {
				this.wheelSettleHandle = this.settleScheduler.schedule(
					settleAtDeadline,
					remainingDelayMs,
				);
				return;
			}
			this.wheelSettleHandle = null;
			this.activeWheelGestureToken = null;
			this.wheelSettleDeadline = Number.NEGATIVE_INFINITY;
			this.onWheelSettled?.(gestureToken);
		};

		this.wheelSettleHandle = this.settleScheduler.schedule(
			settleAtDeadline,
			Math.max(0, this.wheelSettleDeadline - this.now()),
		);
	}

	/**
	 * Retains the earliest valid input time represented by the pending batch.
	 * @param inputTimestamp Native input timestamp candidate.
	 * @returns Nothing.
	 */
	private recordInputTimestamp(inputTimestamp: number): void {
		const timestamp = Number.isFinite(inputTimestamp)
			? inputTimestamp
			: performance.now();
		this.earliestInputTimestamp = this.earliestInputTimestamp === null
			? timestamp
			: Math.min(this.earliestInputTimestamp, timestamp);
	}

	/**
	 * Requests the sole display callback used for pending Canvas input.
	 * @returns Nothing.
	 */
	private schedule(): void {
		if (this.frameHandle !== null || this.disposed) return;
		this.frameHandle = this.scheduler.request(() => {
			this.frameHandle = null;
			this.flushPendingBatch();
		});
	}

	/**
	 * Creates one immutable batch, clears pending values, and invokes the consumer.
	 * @returns Nothing.
	 */
	private flushPendingBatch(): void {
		if (!this.hasPendingWork()) return;
		const wheel = this.hasWheelInput
			? {
					panDeltaX: this.wheelPanDeltaX,
					zoomDeltaY: this.wheelZoomDeltaY,
					zoomPoint: this.latestZoomPoint
						? { ...this.latestZoomPoint }
						: null,
					zoomGestureToken: this.latestZoomPoint
						? this.activeWheelGestureToken
						: null,
				}
			: null;
		const batch: CanvasInputBatch = {
			sequence: ++this.nextSequence,
			inputTimestamp: this.earliestInputTimestamp ?? performance.now(),
			wheel,
			pointer: this.latestPointer,
			scrollDeltaX: this.scrollDeltaX,
			scrollDeltaY: this.scrollDeltaY,
			dirtyMask: this.dirtyMask,
		};
		this.resetPendingState();
		this.flush(batch);
	}

	/**
	 * Reports whether the coordinator has data worth flushing.
	 * @returns True when any input or render invalidation is pending.
	 */
	private hasPendingWork(): boolean {
		return Boolean(
			this.hasWheelInput ||
			this.latestPointer ||
			this.scrollDeltaX ||
			this.scrollDeltaY ||
			this.dirtyMask !== DirtyCanvas.None,
		);
	}

	/**
	 * Clears mutable values after a flush or disposal.
	 * @returns Nothing.
	 */
	private resetPendingState(): void {
		this.earliestInputTimestamp = null;
		this.wheelPanDeltaX = 0;
		this.wheelZoomDeltaY = 0;
		this.latestZoomPoint = null;
		this.hasWheelInput = false;
		this.latestPointer = null;
		this.scrollDeltaX = 0;
		this.scrollDeltaY = 0;
		this.dirtyMask = DirtyCanvas.None;
	}
}
