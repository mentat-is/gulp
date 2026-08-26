export type CanvasRenderWindowPriority = "normal" | "terminal";

type CanvasRenderWindowTask = (
	priority: CanvasRenderWindowPriority,
) => Promise<void>;

interface ScheduledRenderWindowTask {
	viewportKey: string;
	priority: CanvasRenderWindowPriority;
	task: CanvasRenderWindowTask;
}

/**
 * Coalesces same-viewport projection refreshes into one active request and one
 * trailing trigger. A viewport boundary starts immediately so QueryWorker can
 * invalidate obsolete storage reads through its pending request guard.
 */
export class CanvasRenderWindowScheduler {
	private active: (ScheduledRenderWindowTask & { token: object }) | null = null;
	private trailing: ScheduledRenderWindowTask | null = null;
	private disposed = false;

	/**
	 * Schedules projection work without allocating a new projection revision for
	 * repeated triggers while the same viewport request is active.
	 * @param viewportKey Operation, epoch, and viewport identity.
	 * @param priority Terminal refreshes retained ahead of normal trailing work.
	 * @param task Asynchronous request factory invoked only when a slot starts.
	 * @returns Nothing.
	 */
	public schedule(
		viewportKey: string,
		priority: CanvasRenderWindowPriority,
		task: CanvasRenderWindowTask,
	): void {
		if (this.disposed) return;
		const scheduled = { viewportKey, priority, task };
		if (!this.active) {
			this.start(scheduled);
			return;
		}
		if (this.active.viewportKey !== viewportKey) {
			this.trailing = null;
			this.start(scheduled);
			return;
		}
		this.trailing = {
			...scheduled,
			priority:
				priority === "terminal" || this.trailing?.priority === "terminal"
					? "terminal"
					: "normal",
		};
	}

	/**
	 * Clears queued state at an operation boundary while allowing old promises to drain.
	 * @returns Nothing.
	 */
	public reset(): void {
		this.active = null;
		this.trailing = null;
	}

	/**
	 * Prevents future work and detaches all active and trailing scheduler state.
	 * @returns Nothing.
	 */
	public dispose(): void {
		this.disposed = true;
		this.reset();
	}

	/**
	 * Starts one request slot and promotes its single trailing trigger on settlement.
	 * @param scheduled Viewport-qualified task taking ownership of the active slot.
	 * @returns Nothing.
	 */
	private start(scheduled: ScheduledRenderWindowTask): void {
		const token = {};
		this.active = { ...scheduled, token };
		void scheduled
			.task(scheduled.priority)
			.catch(() => undefined)
			.finally(() => {
				if (this.disposed || this.active?.token !== token) return;
				this.active = null;
				const trailing = this.trailing;
				this.trailing = null;
				if (trailing) this.start(trailing);
			});
	}
}
