import {
	CanvasRenderWindowScheduler,
	type CanvasRenderWindowPriority,
} from "@/app/body/CanvasRenderWindowScheduler";

/** Creates one externally settled projection task. */
function createDeferredTask(
	starts: CanvasRenderWindowPriority[],
): { task: (priority: CanvasRenderWindowPriority) => Promise<void>; resolve: () => void } {
	let resolve = (): void => undefined;
	const promise = new Promise<void>((promiseResolve) => {
		resolve = promiseResolve;
	});
	return {
		task: async (priority) => {
			starts.push(priority);
			await promise;
		},
		resolve,
	};
}

describe("CanvasRenderWindowScheduler", () => {
	it("coalesces twenty building summaries into one active and one trailing request", async () => {
		const scheduler = new CanvasRenderWindowScheduler();
		const starts: CanvasRenderWindowPriority[] = [];
		const active = createDeferredTask(starts);
		const trailing = jest.fn(async () => {
			starts.push("normal");
		});
		scheduler.schedule("operation:1:viewport:1", "normal", active.task);
		for (let index = 0; index < 20; index++) {
			scheduler.schedule("operation:1:viewport:1", "normal", trailing);
		}

		expect(starts).toEqual(["normal"]);
		active.resolve();
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(trailing).toHaveBeenCalledTimes(1);
		expect(starts).toEqual(["normal", "normal"]);
		scheduler.dispose();
	});

	it("retains terminal priority in the sole trailing slot", async () => {
		const scheduler = new CanvasRenderWindowScheduler();
		const starts: CanvasRenderWindowPriority[] = [];
		const active = createDeferredTask(starts);
		const trailing = jest.fn(async (priority: CanvasRenderWindowPriority) => {
			starts.push(priority);
		});
		scheduler.schedule("operation:1:viewport:1", "normal", active.task);
		scheduler.schedule("operation:1:viewport:1", "terminal", trailing);
		scheduler.schedule("operation:1:viewport:1", "normal", trailing);

		active.resolve();
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(starts).toEqual(["normal", "terminal"]);
		scheduler.dispose();
	});

	it("starts a changed viewport immediately and ignores settlement of obsolete work", async () => {
		const scheduler = new CanvasRenderWindowScheduler();
		const starts: string[] = [];
		let resolveOld = (): void => undefined;
		const oldPromise = new Promise<void>((resolve) => {
			resolveOld = resolve;
		});
		scheduler.schedule("operation:1:viewport:1", "normal", async () => {
			starts.push("old");
			await oldPromise;
		});
		scheduler.schedule("operation:1:viewport:2", "normal", async () => {
			starts.push("new");
		});

		expect(starts).toEqual(["old", "new"]);
		resolveOld();
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(starts).toEqual(["old", "new"]);
		scheduler.dispose();
	});
});
