import type { Source } from "@/entities/Source";

const INTERACTION_WINDOW_MS = 120;
const INTERACTIVE_SLICE_BUDGET_MS = 1;
const IDLE_SLICE_BUDGET_MS = 4;

export interface RenderProjectionSchedulerMetrics {
	activeSourceId: Source.Id | null;
	visibleQueueDepth: number;
	backgroundQueueDepth: number;
	oldestQueuedAgeMs: number;
	lastSliceDurationMs: number;
}

export interface RenderProjectionJobContext {
	isCancelled: () => boolean;
	getSliceBudgetMs: () => number;
	yieldToMainThread: (sliceStartedAt?: number) => Promise<void>;
	waitForVisibleWork: () => Promise<void>;
}

export interface RenderProjectionJob {
	sourceId: Source.Id;
	replacesSource: boolean;
	run: (context: RenderProjectionJobContext) => Promise<void>;
}

interface QueuedProjectionJob extends RenderProjectionJob {
	sequence: number;
	queuedAt: number;
	generation: number;
}

/**
 * Schedules one cooperative render projection at a time with viewport-first priority.
 * Pending source deltas retain their immutable order. A newer authoritative replacement
 * advances the source generation so queued work is discarded and active cooperative work
 * exits at its next cancellation boundary.
 */
export class RenderProjectionScheduler {
	private readonly pendingBySource = new Map<
		Source.Id,
		QueuedProjectionJob[]
	>();
	private readonly sourceGenerations = new Map<Source.Id, number>();
	private readonly metricsListeners = new Set<
		(metrics: RenderProjectionSchedulerMetrics) => void
	>();
	private viewportSourceIds = new Set<Source.Id>();
	private activeJob: QueuedProjectionJob | null = null;
	private sequence = 0;
	private lastInteractionAt = Number.NEGATIVE_INFINITY;
	private lastSliceDurationMs = 0;
	private disposed = false;

	/**
	 * Adds one source projection and starts the cooperative queue pump.
	 * @param job Source-local projection work and replacement semantics.
	 * @returns Nothing.
	 */
	public enqueue(job: RenderProjectionJob): void {
		if (this.disposed) return;
		const previousGeneration = this.sourceGenerations.get(job.sourceId) ?? 0;
		const generation = job.replacesSource
			? previousGeneration + 1
			: previousGeneration;
		if (job.replacesSource) {
			this.sourceGenerations.set(job.sourceId, generation);
		}
		const queue = job.replacesSource
			? []
			: (this.pendingBySource.get(job.sourceId) ?? []);
		queue.push({
			...job,
			sequence: ++this.sequence,
			queuedAt: performance.now(),
			generation,
		});
		this.pendingBySource.set(job.sourceId, queue);
		this.publishMetrics();
		void this.pump();
	}

	/**
	 * Reports whether a source already owns active or queued projection work.
	 * Callers use this to collapse sustained mutation bursts into one authoritative
	 * replacement without changing the low-latency single-delta path.
	 * @param sourceId Source whose scheduler occupancy is requested.
	 * @returns True when the source is active or has at least one queued job.
	 */
	public hasSourceWork(sourceId: Source.Id): boolean {
		return this.getSourceWorkDepth(sourceId) > 0;
	}

	/**
	 * Returns active plus queued projection jobs for one source.
	 * @param sourceId Source whose bounded backlog is requested.
	 * @returns Count used by callers to preserve small incremental bursts and collapse sustained ones.
	 */
	public getSourceWorkDepth(sourceId: Source.Id): number {
		return (
			(this.activeJob?.sourceId === sourceId ? 1 : 0) +
			(this.pendingBySource.get(sourceId)?.length ?? 0)
		);
	}

	/**
	 * Reports whether authoritative replacement work is active or queued for a source.
	 * @param sourceId Source whose replacement state is requested.
	 * @returns True while later deltas should refresh the replacement instead of queueing behind it.
	 */
	public hasSourceReplacementWork(sourceId: Source.Id): boolean {
		return (
			(this.activeJob?.sourceId === sourceId && this.activeJob.replacesSource) ||
			(this.pendingBySource.get(sourceId)?.some((job) => job.replacesSource) ??
				false)
		);
	}

	/**
	 * Reprioritizes pending sources without cancelling active projections.
	 * @param sourceIds Ordered source IDs intersecting the Canvas viewport.
	 * @returns Nothing.
	 */
	public setViewportSourceIds(sourceIds: readonly Source.Id[]): void {
		this.viewportSourceIds = new Set(sourceIds);
		this.publishMetrics();
	}

	/**
	 * Marks recent pointer or scroll activity so projection slices use a 1 ms budget.
	 * @returns Nothing.
	 */
	public markInteraction(): void {
		this.lastInteractionAt = performance.now();
	}

	/**
	 * Cancels queued work for one removed source while allowing active code to exit safely.
	 * @param sourceId Source whose queued projection work is obsolete.
	 * @returns Nothing.
	 */
	public cancelSource(sourceId: Source.Id): void {
		this.pendingBySource.delete(sourceId);
		this.sourceGenerations.set(
			sourceId,
			(this.sourceGenerations.get(sourceId) ?? 0) + 1,
		);
		this.publishMetrics();
	}

	/**
	 * Cancels every queued or cooperatively active projection for an operation reset.
	 * @returns Nothing.
	 */
	public cancelAll(): void {
		for (const sourceId of this.pendingBySource.keys()) {
			this.cancelSource(sourceId);
		}
		if (this.activeJob) this.cancelSource(this.activeJob.sourceId);
		this.pendingBySource.clear();
		this.publishMetrics();
	}

	/**
	 * Returns current queue and slice diagnostics for profiler reporting.
	 * @returns Immutable scheduler metrics snapshot.
	 */
	public getMetrics(): RenderProjectionSchedulerMetrics {
		let visibleQueueDepth = 0;
		let backgroundQueueDepth = 0;
		let oldestQueuedAt = Number.POSITIVE_INFINITY;
		for (const [sourceId, jobs] of this.pendingBySource) {
			if (this.isVisible(sourceId)) visibleQueueDepth += jobs.length;
			else backgroundQueueDepth += jobs.length;
			for (const job of jobs) oldestQueuedAt = Math.min(oldestQueuedAt, job.queuedAt);
		}
		return {
			activeSourceId: this.activeJob?.sourceId ?? null,
			visibleQueueDepth,
			backgroundQueueDepth,
			oldestQueuedAgeMs: Number.isFinite(oldestQueuedAt)
				? Math.max(0, performance.now() - oldestQueuedAt)
				: 0,
			lastSliceDurationMs: this.lastSliceDurationMs,
		};
	}

	/**
	 * Subscribes to projection queue and slice metric changes.
	 * @param listener Callback receiving the newest immutable metrics snapshot.
	 * @returns Function that removes the listener.
	 */
	public subscribeMetrics(
		listener: (metrics: RenderProjectionSchedulerMetrics) => void,
	): () => void {
		this.metricsListeners.add(listener);
		return () => {
			this.metricsListeners.delete(listener);
		};
	}

	/**
	 * Cancels pending work and prevents future projections.
	 * @returns Nothing.
	 */
	public dispose(): void {
		this.disposed = true;
		this.cancelAll();
		this.metricsListeners.clear();
	}

	/**
	 * Runs queued jobs serially and always selects a viewport source first.
	 * @returns Promise settled when the current queue drains.
	 */
	private async pump(): Promise<void> {
		if (this.activeJob || this.disposed) return;
		let nextJob = this.takeNextJob();
		while (nextJob && !this.disposed) {
			await this.executeJob(nextJob);
			nextJob = this.takeNextJob();
		}
	}

	/**
	 * Executes one queued job and restores a paused background job after nested priority work.
	 * @param job Projection job selected by the priority queue.
	 * @returns Promise settled after the cooperative job finishes or is cancelled.
	 */
	private async executeJob(job: QueuedProjectionJob): Promise<void> {
		const pausedJob = this.activeJob;
		this.activeJob = job;
		this.publishMetrics();
		const isCancelled = (): boolean =>
			this.disposed ||
			(this.sourceGenerations.get(job.sourceId) ?? 0) !== job.generation;
		try {
			await job.run({
				isCancelled,
				getSliceBudgetMs: () => this.getSliceBudgetMs(),
				yieldToMainThread: (sliceStartedAt) =>
					this.yieldToMainThread(sliceStartedAt),
				waitForVisibleWork: () =>
					this.waitForVisibleWork(job.sourceId, isCancelled),
			});
		} finally {
			this.activeJob = pausedJob;
			this.publishMetrics();
		}
	}

	/**
	 * Removes the oldest visible job, falling back to the oldest background job.
	 * @returns Next queued job or null when the scheduler is idle.
	 */
	private takeNextJob(): QueuedProjectionJob | null {
		let selected: QueuedProjectionJob | null = null;
		let selectedVisible = false;
		for (const [sourceId, jobs] of this.pendingBySource) {
			const candidate = jobs[0];
			if (!candidate) continue;
			const candidateVisible = this.isVisible(sourceId);
			if (
				!selected ||
				(candidateVisible && !selectedVisible) ||
				(candidateVisible === selectedVisible &&
					candidate.sequence < selected.sequence)
			) {
				selected = candidate;
				selectedVisible = candidateVisible;
			}
		}
		if (!selected) return null;
		const queue = this.pendingBySource.get(selected.sourceId);
		queue?.shift();
		if (!queue || queue.length === 0) this.pendingBySource.delete(selected.sourceId);
		return selected;
	}

	/**
	 * Returns whether a source is foreground work for the current viewport.
	 * @param sourceId Source to classify.
	 * @returns True before the first viewport report or when the source is visible.
	 */
	private isVisible(sourceId: Source.Id): boolean {
		return this.viewportSourceIds.size === 0 || this.viewportSourceIds.has(sourceId);
	}

	/**
	 * Selects the current cooperative slice budget from recent interaction activity.
	 * @returns One millisecond while interactive, otherwise four milliseconds.
	 */
	private getSliceBudgetMs(): number {
		return performance.now() - this.lastInteractionAt <= INTERACTION_WINDOW_MS
			? INTERACTIVE_SLICE_BUDGET_MS
			: IDLE_SLICE_BUDGET_MS;
	}

	/**
	 * Lets active background work pause while a viewport source is queued.
	 * @param sourceId Active source whose priority may have changed.
	 * @param isCancelled Cancellation check for the active job.
	 * @returns Promise settled when foreground work is no longer waiting.
	 */
	private async waitForVisibleWork(
		sourceId: Source.Id,
		isCancelled: () => boolean,
	): Promise<void> {
		while (
			!isCancelled() &&
			!this.isVisible(sourceId) &&
			this.hasQueuedVisibleWork()
		) {
			const foregroundJob = this.takeNextJob();
			if (!foregroundJob || !this.isVisible(foregroundJob.sourceId)) return;
			await this.executeJob(foregroundJob);
		}
	}

	/**
	 * Checks whether any currently visible source has pending projection work.
	 * @returns True when foreground projection is waiting.
	 */
	private hasQueuedVisibleWork(): boolean {
		for (const sourceId of this.pendingBySource.keys()) {
			if (this.isVisible(sourceId)) return true;
		}
		return false;
	}

	/**
	 * Yields through requestIdleCallback with an animation-frame-aware fallback.
	 * @param sliceStartedAt Optional timestamp used to publish the completed slice duration.
	 * @returns Promise resolved after browser input and paint work gets an opportunity to run.
	 */
	private yieldToMainThread(sliceStartedAt?: number): Promise<void> {
		if (sliceStartedAt !== undefined) {
			this.lastSliceDurationMs = Math.max(0, performance.now() - sliceStartedAt);
			this.publishMetrics();
		}
		return new Promise<void>((resolve) => {
			if (typeof window === "undefined") {
				globalThis.setTimeout(resolve, 0);
				return;
			}
			const afterFrame = (): void => {
				window.setTimeout(resolve, 0);
			};
			if (typeof window.requestIdleCallback === "function") {
				window.requestIdleCallback(afterFrame, { timeout: 16 });
				return;
			}
			window.requestAnimationFrame(afterFrame);
		});
	}

	/**
	 * Publishes the latest scheduler metrics to profiler subscribers.
	 * @returns Nothing.
	 */
	private publishMetrics(): void {
		if (this.metricsListeners.size === 0) return;
		const metrics = this.getMetrics();
		this.metricsListeners.forEach((listener) => listener(metrics));
	}
}
