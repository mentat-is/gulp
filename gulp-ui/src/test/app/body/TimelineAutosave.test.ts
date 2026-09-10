import {
	cleanupTimelineAutosave,
	runTimelineAutosaveWhenIdle,
	scheduleTimelineAutosave,
	type TimelineAutosaveTask,
} from "@/app/body/TimelineAutosave";

interface AutosaveTaskHarness {
	task: TimelineAutosaveTask;
	getScheduleCount: () => number;
	getCancelCount: () => number;
	getFlushCount: () => number;
}

/**
 * Creates a callable autosave task with observable scheduling lifecycle counts.
 * @returns Autosave task and count readers.
 */
function createAutosaveTaskHarness(): AutosaveTaskHarness {
	let scheduleCount = 0;
	let cancelCount = 0;
	let flushCount = 0;
	const task = Object.assign(
		() => {
			scheduleCount++;
		},
		{
			cancel: () => {
				cancelCount++;
			},
			flush: () => {
				flushCount++;
			},
		},
	);

	return {
		task,
		getScheduleCount: () => scheduleCount,
		getCancelCount: () => cancelCount,
		getFlushCount: () => flushCount,
	};
}

describe("Timeline autosave control", () => {
	it("cancels instead of scheduling or flushing when disabled", () => {
		const harness = createAutosaveTaskHarness();

		scheduleTimelineAutosave(harness.task, true);
		cleanupTimelineAutosave(harness.task, true, true);

		expect(harness.getScheduleCount()).toBe(0);
		expect(harness.getFlushCount()).toBe(0);
		expect(harness.getCancelCount()).toBe(2);
	});

	it("preserves normal scheduling and populated-session flushing", () => {
		const harness = createAutosaveTaskHarness();

		scheduleTimelineAutosave(harness.task, false);
		cleanupTimelineAutosave(harness.task, false, true);

		expect(harness.getScheduleCount()).toBe(1);
		expect(harness.getFlushCount()).toBe(1);
		expect(harness.getCancelCount()).toBe(0);
	});

	it("cancels an enabled but empty session during cleanup", () => {
		const harness = createAutosaveTaskHarness();

		cleanupTimelineAutosave(harness.task, false, false);

		expect(harness.getFlushCount()).toBe(0);
		expect(harness.getCancelCount()).toBe(1);
	});

	it("defers idle preparation while timeline interaction remains active", () => {
		jest.useFakeTimers();
		const originalRequestIdleCallback = window.requestIdleCallback;
		const originalCancelIdleCallback = window.cancelIdleCallback;
		Object.defineProperty(window, "requestIdleCallback", {
			configurable: true,
			value: (callback: IdleRequestCallback): number => {
				callback({
					didTimeout: false,
					timeRemaining: () => 10,
				});
				return 1;
			},
		});
		Object.defineProperty(window, "cancelIdleCallback", {
			configurable: true,
			value: jest.fn(),
		});
		const task = jest.fn();
		let interacting = true;

		try {
			runTimelineAutosaveWhenIdle(task, () => interacting);
			expect(task).not.toHaveBeenCalled();

			interacting = false;
			jest.advanceTimersByTime(250);
			expect(task).toHaveBeenCalledTimes(1);

			interacting = true;
			const canceledTask = jest.fn();
			const cancel = runTimelineAutosaveWhenIdle(
				canceledTask,
				() => interacting,
			);
			cancel();
			interacting = false;
			jest.advanceTimersByTime(250);
			expect(canceledTask).not.toHaveBeenCalled();
		} finally {
			jest.useRealTimers();
			Object.defineProperty(window, "requestIdleCallback", {
				configurable: true,
				value: originalRequestIdleCallback,
			});
			Object.defineProperty(window, "cancelIdleCallback", {
				configurable: true,
				value: originalCancelIdleCallback,
			});
		}
	});
});
