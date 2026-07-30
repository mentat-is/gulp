/** Callable subset of a debounced Timeline autosave task. */
export interface TimelineAutosaveTask {
	(): void;
	cancel(): void;
	flush(): void;
}

/**
 * Schedules Timeline autosave when enabled and cancels pending work when disabled.
 * @param task Debounced autosave task.
 * @param disabled Whether this Timeline instance forbids session autosave.
 * @returns Nothing.
 */
export function scheduleTimelineAutosave(
	task: TimelineAutosaveTask,
	disabled: boolean,
): void {
	if (disabled) {
		task.cancel();
		return;
	}

	task();
}

/**
 * Finalizes Timeline autosave without flushing disabled or empty sessions.
 * @param task Debounced autosave task.
 * @param disabled Whether this Timeline instance forbids session autosave.
 * @param hasSessionData Whether the current session contains persistable data.
 * @returns Nothing.
 */
export function cleanupTimelineAutosave(
	task: TimelineAutosaveTask,
	disabled: boolean,
	hasSessionData: boolean,
): void {
	if (disabled || !hasSessionData) {
		task.cancel();
		return;
	}

	task.flush();
}

/**
 * Runs snapshot work when the browser is idle, with a bounded fallback.
 * @param task Low-priority persistence work.
 * @returns Nothing.
 */
export function runTimelineAutosaveWhenIdle(task: () => void): void {
	if (typeof window.requestIdleCallback === "function") {
		window.requestIdleCallback(task, { timeout: 1000 });
		return;
	}
	setTimeout(task, 0);
}
