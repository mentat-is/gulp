import {
	CanvasInputCoordinator,
	CanvasScaleCommitGuard,
	type CanvasInputBatch,
	type CanvasInputFrameScheduler,
	type CanvasInputSettleScheduler,
} from "@/app/body/CanvasInputCoordinator";
import { DirtyCanvas } from "@/render-worker/RenderWorker.types";
import { normalizeWheelDelta } from "@/app/body/CanvasWheelViewport";

/** Deterministic display-frame scheduler used by coordinator tests. */
class TestFrameScheduler implements CanvasInputFrameScheduler {
	private nextHandle = 0;
	private readonly callbacks = new Map<number, FrameRequestCallback>();

	/**
	 * Stores one pending display callback.
	 * @param callback Callback awaiting a test-controlled frame.
	 * @returns Deterministic callback handle.
	 */
	public request(callback: FrameRequestCallback): number {
		const handle = ++this.nextHandle;
		this.callbacks.set(handle, callback);
		return handle;
	}

	/**
	 * Removes one pending display callback.
	 * @param handle Callback handle to remove.
	 * @returns Nothing.
	 */
	public cancel(handle: number): void {
		this.callbacks.delete(handle);
	}

	/**
	 * Runs every currently pending display callback once.
	 * @returns Nothing.
	 */
	public flush(): void {
		const callbacks = Array.from(this.callbacks.values());
		this.callbacks.clear();
		callbacks.forEach((callback) => callback(100));
	}

	/**
	 * Reports the number of scheduled callbacks.
	 * @returns Pending callback count.
	 */
	public get size(): number {
		return this.callbacks.size;
	}
}

/** Deterministic quiet-period scheduler used by wheel persistence tests. */
class TestSettleScheduler implements CanvasInputSettleScheduler {
	private nextHandle = 0;
	private readonly callbacks = new Map<number, () => void>();
	private scheduledCallbackCount = 0;

	/**
	 * Creates a scheduler that can model a callback already queued during cancellation.
	 * @param honorCancellation Whether cancel removes the stored callback.
	 */
	public constructor(private readonly honorCancellation = true) {}

	/**
	 * Stores one quiet-period callback.
	 * @param callback Callback awaiting a test-controlled timeout.
	 * @param _delayMs Requested delay, unused by the deterministic scheduler.
	 * @returns Deterministic timeout handle.
	 */
	public schedule(
		callback: () => void,
		_delayMs: number,
	): ReturnType<typeof setTimeout> {
		const handle = ++this.nextHandle;
		this.scheduledCallbackCount++;
		this.callbacks.set(handle, callback);
		return handle as unknown as ReturnType<typeof setTimeout>;
	}

	/**
	 * Removes one pending quiet-period callback.
	 * @param handle Callback handle to remove.
	 * @returns Nothing.
	 */
	public cancel(handle: ReturnType<typeof setTimeout>): void {
		if (this.honorCancellation) {
			this.callbacks.delete(handle as unknown as number);
		}
	}

	/**
	 * Runs every currently pending quiet-period callback once.
	 * @returns Nothing.
	 */
	public flush(): void {
		const callbacks = Array.from(this.callbacks.values());
		this.callbacks.clear();
		callbacks.forEach((callback) => callback());
	}

	/**
	 * Reports the number of scheduled callbacks.
	 * @returns Pending callback count.
	 */
	public get size(): number {
		return this.callbacks.size;
	}

	/**
	 * Reports how many native timer registrations the coordinator requested.
	 * @returns Total scheduled callback count.
	 */
	public get scheduleCount(): number {
		return this.scheduledCallbackCount;
	}
}

describe("wheel delta normalization", () => {
	it("converts pixel, line, and page units to CSS pixels", () => {
		expect(normalizeWheelDelta({ deltaX: 2, deltaY: -3, deltaMode: 0 }, 20, 600))
			.toEqual({ deltaX: 2, deltaY: -3 });
		expect(normalizeWheelDelta({ deltaX: 2, deltaY: -3, deltaMode: 1 }, 20, 600))
			.toEqual({ deltaX: 40, deltaY: -60 });
		expect(normalizeWheelDelta({ deltaX: 2, deltaY: -3, deltaMode: 2 }, 20, 600))
			.toEqual({ deltaX: 1_200, deltaY: -1_800 });
	});

	it("uses safe fallbacks for invalid cached dimensions", () => {
		expect(
			normalizeWheelDelta(
				{ deltaX: 1, deltaY: 1, deltaMode: 1 },
				Number.NaN,
				0,
			),
		).toEqual({ deltaX: 16, deltaY: 16 });
	});
});

describe("CanvasInputCoordinator", () => {
	it("coalesces wheel, pointer, scroll, and dirty state into one display tick", () => {
		const scheduler = new TestFrameScheduler();
		const batches: CanvasInputBatch[] = [];
		const coordinator = new CanvasInputCoordinator(
			(batch) => batches.push(batch),
			scheduler,
		);

		coordinator.queueWheel(
			{
				deltaX: 1,
				deltaY: 2,
				deltaMode: 0,
				clientX: 10,
				clientY: 20,
				inputTimestamp: 20,
			},
			16,
			500,
		);
		coordinator.queueWheel(
			{
				deltaX: 2,
				deltaY: -1,
				deltaMode: 1,
				clientX: 30,
				clientY: 40,
				inputTimestamp: 15,
			},
			16,
			500,
		);
		coordinator.queuePointer({
			clientX: 50,
			clientY: 60,
			inside: true,
			inputTimestamp: 18,
		});
		coordinator.queueScrollDelta(4, 5, 17);
		coordinator.invalidate(DirtyCanvas.Theme, 19);

		expect(scheduler.size).toBe(1);
		scheduler.flush();

		expect(batches).toHaveLength(1);
		expect(batches[0]).toMatchObject({
			sequence: 1,
			inputTimestamp: 15,
			wheel: {
				panDeltaX: 32,
				zoomDeltaY: 2,
				zoomPoint: { clientX: 10, clientY: 20 },
				zoomGestureToken: 1,
			},
			pointer: { clientX: 50, clientY: 60, inside: true },
			scrollDeltaX: 4,
			scrollDeltaY: 5,
			dirtyMask: DirtyCanvas.Viewport | DirtyCanvas.Theme,
		});
	});

	it("preserves same-frame horizontal pan and vertical zoom contributions", () => {
		const scheduler = new TestFrameScheduler();
		const batches: CanvasInputBatch[] = [];
		const coordinator = new CanvasInputCoordinator(
			(batch) => batches.push(batch),
			scheduler,
		);

		coordinator.queueWheel(
			{
				deltaX: 24,
				deltaY: 2,
				deltaMode: 0,
				clientX: 10,
				clientY: 20,
				inputTimestamp: 1,
			},
			16,
			500,
		);
		coordinator.queueWheel(
			{
				deltaX: 1,
				deltaY: -40,
				deltaMode: 0,
				clientX: 30,
				clientY: 40,
				inputTimestamp: 2,
			},
			16,
			500,
		);
		scheduler.flush();

		expect(batches[0].wheel).toEqual({
			panDeltaX: 24,
			zoomDeltaY: -40,
			zoomPoint: { clientX: 30, clientY: 40 },
			zoomGestureToken: 1,
		});
	});

	it("flushes synchronously and advances sequence once per batch", () => {
		const scheduler = new TestFrameScheduler();
		const batches: CanvasInputBatch[] = [];
		const coordinator = new CanvasInputCoordinator(
			(batch) => batches.push(batch),
			scheduler,
		);

		coordinator.invalidate(DirtyCanvas.Force, 1);
		coordinator.flushNow();
		coordinator.queueScrollDelta(1, 0, 2);
		scheduler.flush();

		expect(scheduler.size).toBe(0);
		expect(batches.map((batch) => batch.sequence)).toEqual([1, 2]);
	});

	it("requests one viewport post per scroll tick and none for pointer-only input", () => {
		const scheduler = new TestFrameScheduler();
		let viewportPosts = 0;
		const coordinator = new CanvasInputCoordinator((batch) => {
			if ((batch.dirtyMask & DirtyCanvas.Viewport) !== 0) viewportPosts++;
		}, scheduler);

		coordinator.queueScrollDelta(1, 0, 1);
		coordinator.queueScrollDelta(2, 0, 2);
		coordinator.queueScrollDelta(3, 0, 3);
		scheduler.flush();
		expect(viewportPosts).toBe(1);

		coordinator.queuePointer({
			clientX: 10,
			clientY: 20,
			inside: true,
			inputTimestamp: 4,
		});
		scheduler.flush();
		expect(viewportPosts).toBe(1);
	});

	it("persists repeated wheel frames once after the gesture settles", () => {
		const frameScheduler = new TestFrameScheduler();
		const settleScheduler = new TestSettleScheduler();
		const persist = jest.fn();
		let now = 0;
		const coordinator = new CanvasInputCoordinator(
			() => undefined,
			frameScheduler,
			persist,
			settleScheduler,
			100,
			() => now,
		);
		const firstWheel = {
			deltaX: 0,
			deltaY: 10,
			deltaMode: 0,
			clientX: 20,
			clientY: 20,
			inputTimestamp: 1,
		};

		coordinator.queueWheel(firstWheel, 16, 500);
		frameScheduler.flush();
		now = 20;
		coordinator.queueWheel({ ...firstWheel, inputTimestamp: 2 }, 16, 500);
		frameScheduler.flush();

		expect(persist).toHaveBeenCalledTimes(0);
		expect(settleScheduler.size).toBe(1);
		expect(settleScheduler.scheduleCount).toBe(1);
		now = 100;
		settleScheduler.flush();
		expect(persist).toHaveBeenCalledTimes(0);
		expect(settleScheduler.scheduleCount).toBe(2);
		now = 120;
		settleScheduler.flush();
		expect(persist).toHaveBeenCalledTimes(1);
		expect(coordinator.isWheelGesturePending).toBe(false);
	});

	it("settles without flushing unrelated work outside the display tick", () => {
		const frameScheduler = new TestFrameScheduler();
		const settleScheduler = new TestSettleScheduler();
		const batches: CanvasInputBatch[] = [];
		const persist = jest.fn();
		let now = 0;
		const coordinator = new CanvasInputCoordinator(
			(batch) => batches.push(batch),
			frameScheduler,
			persist,
			settleScheduler,
			100,
			() => now,
		);

		coordinator.queueWheel(
			{
				deltaX: 0,
				deltaY: 10,
				deltaMode: 0,
				clientX: 20,
				clientY: 20,
				inputTimestamp: 1,
			},
			16,
			500,
		);
		frameScheduler.flush();
		coordinator.queueScrollDelta(12, 0, 2);

		now = 100;
		settleScheduler.flush();

		expect(persist).toHaveBeenCalledTimes(1);
		expect(batches).toHaveLength(1);
		expect(frameScheduler.size).toBe(1);
		frameScheduler.flush();
		expect(batches).toHaveLength(2);
	});

	it("rejects a cancelled stale settle callback after an external scale command", () => {
		const frameScheduler = new TestFrameScheduler();
		const settleScheduler = new TestSettleScheduler(false);
		let now = 0;
		const guard = new CanvasScaleCommitGuard(1);
		let transientScale = 1;
		let currentCanonicalScale = 1;
		const persist = jest.fn<void, [number]>();
		const coordinator = new CanvasInputCoordinator(
			(batch) => {
				const token = batch.wheel?.zoomGestureToken;
				if (token === null || token === undefined) return;
				guard.beginGesture(token, currentCanonicalScale);
				transientScale = 2;
			},
			frameScheduler,
			(gestureToken) => {
				const scaleToPersist = guard.prepareCommit(
					gestureToken,
					transientScale,
					currentCanonicalScale,
				);
				if (scaleToPersist !== null) persist(scaleToPersist);
			},
			settleScheduler,
			100,
			() => now,
		);

		coordinator.queueWheel(
			{
				deltaX: 0,
				deltaY: 10,
				deltaMode: 0,
				clientX: 20,
				clientY: 20,
				inputTimestamp: 1,
			},
			16,
			500,
		);
		frameScheduler.flush();
		// An explicit reset-to-1 does not change React's canonical value, so the
		// command signal must cancel the gesture even though no scale effect runs.
		guard.cancelGesture();
		transientScale = 1;
		coordinator.cancelWheelGesture();
		now = 100;
		settleScheduler.flush();

		expect(persist).toHaveBeenCalledTimes(0);
		expect(currentCanonicalScale).toBe(1);
		expect(transientScale).toBe(1);
		expect(coordinator.isWheelGesturePending).toBe(false);
	});

	it("cancels pending work when disposed", () => {
		const scheduler = new TestFrameScheduler();
		const flush = jest.fn();
		const coordinator = new CanvasInputCoordinator(flush, scheduler);
		coordinator.invalidate(DirtyCanvas.Viewport);

		coordinator.dispose();
		scheduler.flush();
		coordinator.queueScrollDelta(1, 1);

		expect(flush).toHaveBeenCalledTimes(0);
		expect(scheduler.size).toBe(0);
	});
});
