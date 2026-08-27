import {
	CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS,
	CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS,
	CanvasTerminalRenderConvergence,
} from "@/app/body/CanvasTerminalRenderConvergence";

const SOURCE_ONE = "source-one";
const SOURCE_TWO = "source-two";
const OPERATION_EPOCH = 4;
const AUTHORITY_REVISION = 6;

/** Records the 94,391-event terminal source used by the regression scenarios. */
function recordTerminalTarget(
	controller: CanvasTerminalRenderConvergence,
): void {
	controller.record({
		sourceId: SOURCE_ONE,
		operationEpoch: OPERATION_EPOCH,
		sourceAuthorityRevision: AUTHORITY_REVISION,
		sourceGeneration: 3,
		sourceRevision: 12,
		sourceStatus: "complete",
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
					sourceAuthorityRevision: AUTHORITY_REVISION,
					sourceGeneration: 3,
					sourceRevision: 11,
					sourceStatus: "building",
					loadedCount: 90_000,
				},
			]),
		).toBe(false);
		expect(
			controller.acknowledgeFrame(
				OPERATION_EPOCH,
				visible,
				new Map([
					[SOURCE_ONE, { revision: 11, dataSequence: 20 }],
				]),
				20,
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
						sourceAuthorityRevision: AUTHORITY_REVISION,
						sourceGeneration: 3,
						sourceRevision: 12,
						sourceStatus: "complete",
						loadedCount: 94_391,
					},
				]),
			).toBe(true);
			expect(controller.hasTargets()).toBe(true);
			expect(
				controller.acknowledgeFrame(
					OPERATION_EPOCH,
					visible,
					new Map([
						[SOURCE_ONE, { revision: 12, dataSequence: 21 }],
					]),
					21,
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
			sourceAuthorityRevision: AUTHORITY_REVISION,
			sourceGeneration: 5,
			sourceRevision: 7,
			sourceStatus: "incomplete",
			eventCount: 10,
		});
		const visible = new Set([SOURCE_ONE, SOURCE_TWO]);

		expect(
			controller.acceptsProjection(OPERATION_EPOCH, visible, [
				{
					sourceId: SOURCE_ONE,
					sourceAuthorityRevision: AUTHORITY_REVISION,
					sourceGeneration: 3,
					sourceRevision: 12,
					sourceStatus: "complete",
					loadedCount: 90_000,
				},
			]),
		).toBe(false);
		expect(
			controller.acceptsProjection(OPERATION_EPOCH, visible, [
				{
					sourceId: SOURCE_ONE,
					sourceAuthorityRevision: AUTHORITY_REVISION,
					sourceGeneration: 3,
					sourceRevision: 12,
					sourceStatus: "complete",
					loadedCount: 94_391,
				},
				{
					sourceId: SOURCE_TWO,
					sourceAuthorityRevision: AUTHORITY_REVISION,
					sourceGeneration: 5,
					sourceRevision: 7,
					sourceStatus: "incomplete",
					loadedCount: 10,
				},
			]),
		).toBe(true);
	});

	it("replaces a newer physical generation with an authoritative Undo target", () => {
		const controller = new CanvasTerminalRenderConvergence();
		controller.record({
			sourceId: SOURCE_ONE,
			operationEpoch: OPERATION_EPOCH,
			sourceAuthorityRevision: 10,
			sourceGeneration: 5,
			sourceRevision: 30,
			sourceStatus: "complete",
			eventCount: 50,
		});
		const undoTarget = {
			sourceId: SOURCE_ONE,
			operationEpoch: OPERATION_EPOCH,
			sourceAuthorityRevision: 11,
			sourceGeneration: 1,
			sourceRevision: 12,
			sourceStatus: "complete" as const,
			eventCount: 10,
		};

		expect(controller.record(undoTarget)).toBe(true);
		expect(
			controller.getVisibleTargets(
				OPERATION_EPOCH,
				new Set([SOURCE_ONE]),
			)[0],
		).toMatchObject(undoTarget);
		expect(
			controller.record({
				...undoTarget,
				sourceAuthorityRevision: 10,
				sourceGeneration: 9,
				sourceRevision: 99,
			}),
		).toBe(false);
	});

	it("retains the terminal target when a pre-terminal frame arrives after the final ACK", () => {
		jest.useFakeTimers();
		const controller = new CanvasTerminalRenderConvergence();
		recordTerminalTarget(controller);
		const visible = new Set([SOURCE_ONE]);
		const retry = jest.fn();
		const acknowledged = new Map([
			[SOURCE_ONE, { revision: 12, dataSequence: 42 }],
		]);

		expect(
			controller.acknowledgeFrame(
				OPERATION_EPOCH,
				visible,
				acknowledged,
				41,
			),
		).toEqual([]);
		expect(controller.hasTargets()).toBe(true);
		controller.scheduleWatchdog(retry);
		jest.advanceTimersByTime(
			CANVAS_TERMINAL_RENDER_INITIAL_RETRY_DELAY_MS,
		);
		expect(retry).toHaveBeenCalledTimes(1);
		expect(
			controller.acknowledgeFrame(
				OPERATION_EPOCH,
				visible,
				acknowledged,
				42,
			),
		).toEqual([SOURCE_ONE]);
		controller.scheduleWatchdog(retry);
		jest.advanceTimersByTime(CANVAS_TERMINAL_RENDER_MAX_RETRY_DELAY_MS);
		expect(retry).toHaveBeenCalledTimes(1);
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
				new Map([
					[SOURCE_ONE, { revision: 12, dataSequence: 20 }],
				]),
				20,
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

	it("reports the oldest pending terminal target age on demand", () => {
		let now = 1_000;
		const controller = new CanvasTerminalRenderConvergence(() => now);
		recordTerminalTarget(controller);
		now = 1_275;
		expect(controller.getOldestTargetAgeMs()).toBe(275);
		controller.removeSources([SOURCE_ONE]);
		expect(controller.getOldestTargetAgeMs()).toBe(0);
	});
});
