import {
	CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS,
	CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS,
	CanvasTerminalRenderConvergence,
} from "@/app/body/CanvasTerminalRenderConvergence";

const SOURCE_ONE = "source-one";
const SOURCE_TWO = "source-two";
const OPERATION_EPOCH = 4;

/** Records the 94,391-event terminal source used by the regression scenarios. */
function recordTerminalTarget(
	controller: CanvasTerminalRenderConvergence,
): void {
	controller.record({
		sourceId: SOURCE_ONE,
		operationEpoch: OPERATION_EPOCH,
		sourceRevision: 12,
		eventCount: 94_391,
	});
}

describe("Canvas terminal render convergence", () => {
	afterEach(() => {
		jest.useRealTimers();
	});

	it("retains the terminal target through an intermediate 90k projection and frame", () => {
		const controller = new CanvasTerminalRenderConvergence();
		recordTerminalTarget(controller);
		const visible = new Set([SOURCE_ONE]);

		expect(
			controller.acceptsProjection(OPERATION_EPOCH, visible, [
				{
					sourceId: SOURCE_ONE,
					sourceRevision: 11,
					loadedCount: 90_000,
				},
			]),
		).toBe(false);
		expect(
			controller.acknowledgeFrame(
				OPERATION_EPOCH,
				visible,
				new Map([[SOURCE_ONE, 11]]),
			),
		).toEqual([]);
		expect(controller.hasTargets()).toBe(true);
	});

	for (const engine of ["default", "height", "graph"]) {
		it(`accepts the exact final window for the ${engine} engine only after its rendered ACK`, () => {
			const controller = new CanvasTerminalRenderConvergence();
			recordTerminalTarget(controller);
			const visible = new Set([SOURCE_ONE]);

			expect(
				controller.acceptsProjection(OPERATION_EPOCH, visible, [
					{
						sourceId: SOURCE_ONE,
						sourceRevision: 12,
						loadedCount: 94_391,
					},
				]),
			).toBe(true);
			expect(controller.hasTargets()).toBe(true);
			expect(
				controller.acknowledgeFrame(
					OPERATION_EPOCH,
					visible,
					new Map([[SOURCE_ONE, 12]]),
				),
			).toEqual([SOURCE_ONE]);
			expect(controller.hasTargets()).toBe(false);
		});
	}

	it("rejects a same-revision count mismatch and requires all visible targets", () => {
		const controller = new CanvasTerminalRenderConvergence();
		recordTerminalTarget(controller);
		controller.record({
			sourceId: SOURCE_TWO,
			operationEpoch: OPERATION_EPOCH,
			sourceRevision: 7,
			eventCount: 10,
		});
		const visible = new Set([SOURCE_ONE, SOURCE_TWO]);

		expect(
			controller.acceptsProjection(OPERATION_EPOCH, visible, [
				{
					sourceId: SOURCE_ONE,
					sourceRevision: 12,
					loadedCount: 90_000,
				},
			]),
		).toBe(false);
		expect(
			controller.acceptsProjection(OPERATION_EPOCH, visible, [
				{
					sourceId: SOURCE_ONE,
					sourceRevision: 13,
					loadedCount: 95_000,
				},
				{
					sourceId: SOURCE_TWO,
					sourceRevision: 7,
					loadedCount: 10,
				},
			]),
		).toBe(true);
	});

	it("keeps offscreen targets dormant and discards them at lifecycle boundaries", () => {
		const controller = new CanvasTerminalRenderConvergence();
		recordTerminalTarget(controller);

		expect(
			controller.getVisibleTargets(OPERATION_EPOCH, new Set([SOURCE_TWO])),
		).toEqual([]);
		expect(
			controller.acknowledgeFrame(
				OPERATION_EPOCH,
				new Set([SOURCE_TWO]),
				new Map([[SOURCE_ONE, 12]]),
			),
		).toEqual([]);
		expect(controller.hasTargets()).toBe(true);

		controller.removeSources([SOURCE_ONE]);
		expect(controller.hasTargets()).toBe(false);
		recordTerminalTarget(controller);
		controller.reset();
		expect(controller.hasTargets()).toBe(false);
	});

	it("uses one watchdog with capped exponential backoff", () => {
		jest.useFakeTimers();
		const controller = new CanvasTerminalRenderConvergence();
		recordTerminalTarget(controller);
		const callback = jest.fn();

		controller.scheduleWatchdog(callback);
		controller.scheduleWatchdog(callback);
		jest.advanceTimersByTime(
			CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS - 1,
		);
		expect(callback).not.toHaveBeenCalled();
		jest.advanceTimersByTime(1);
		expect(callback).toHaveBeenCalledTimes(1);

		controller.scheduleWatchdog(callback);
		jest.advanceTimersByTime(999);
		expect(callback).toHaveBeenCalledTimes(1);
		jest.advanceTimersByTime(1);
		expect(callback).toHaveBeenCalledTimes(2);

		controller.scheduleWatchdog(callback);
		jest.advanceTimersByTime(CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS);
		expect(callback).toHaveBeenCalledTimes(3);
		controller.scheduleWatchdog(callback);
		jest.advanceTimersByTime(CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS);
		expect(callback).toHaveBeenCalledTimes(4);
	});
});
