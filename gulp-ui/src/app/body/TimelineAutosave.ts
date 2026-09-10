/** Callable subset of a debounced Timeline autosave task. */
export interface TimelineAutosaveTask {
	(): void;
	cancel(): void;
	flush(): void;
}

/** Quiet period required before autosave may serialize timeline state after input. */
export const TIMELINE_AUTOSAVE_INTERACTION_QUIET_MS = 1_000;
const TIMELINE_AUTOSAVE_IDLE_RETRY_MS = 250;

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
 * Runs snapshot work when the browser is idle and retries while interaction is active.
 * @param task Low-priority persistence work.
 * @param shouldDefer Optional guard that protects active timeline interaction.
 * @returns Cancellation callback for pending idle or retry work.
 */
export function runTimelineAutosaveWhenIdle(
	task: () => void,
	shouldDefer: () => boolean = () => false,
): () => void {
	let canceled = false;
	let idleHandle: number | null = null;
	let retryHandle: ReturnType<typeof setTimeout> | null = null;

	const run = (): void => {
		idleHandle = null;
		retryHandle = null;
		if (canceled) return;
		if (shouldDefer()) {
			retryHandle = setTimeout(schedule, TIMELINE_AUTOSAVE_IDLE_RETRY_MS);
			return;
		}
		task();
	};

	const schedule = (): void => {
		if (canceled) return;
		if (typeof window.requestIdleCallback === "function") {
			idleHandle = window.requestIdleCallback(() => run(), {
				timeout: 1000,
			});
			return;
		}
		retryHandle = setTimeout(run, 0);
	};

	schedule();
	return () => {
		canceled = true;
		if (idleHandle !== null && typeof window.cancelIdleCallback === "function") {
			window.cancelIdleCallback(idleHandle);
		}
		if (retryHandle !== null) clearTimeout(retryHandle);
	};
}
