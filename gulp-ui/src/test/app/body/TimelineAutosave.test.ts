import {
	cleanupTimelineAutosave,
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
});
