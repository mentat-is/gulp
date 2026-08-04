import {
	CANVAS_RENDER_WINDOW_MAX_ATTEMPTS,
	CANVAS_RENDER_WINDOW_RETRY_DELAY_MS,
	isCanvasRenderWindowRequestCurrent,
	isCanvasRenderWindowSourceStale,
	shouldRetryCanvasRenderWindow,
	type CanvasRenderWindowRequestFence,
} from "@/app/body/CanvasRenderWindowRetry";

const socket = {};
const fence: CanvasRenderWindowRequestFence = {
	socket,
	operationEpoch: 4,
	viewportRevision: 8,
	projectionRevision: 12,
};

describe("Canvas render-window retry policy", () => {
	it("allows one short retry and then stops", () => {
		expect(CANVAS_RENDER_WINDOW_MAX_ATTEMPTS).toBe(2);
		expect(CANVAS_RENDER_WINDOW_RETRY_DELAY_MS > 0).toBe(true);
		expect(shouldRetryCanvasRenderWindow(1, fence, fence)).toBe(true);
		expect(shouldRetryCanvasRenderWindow(2, fence, fence)).toBe(false);
	});

	const changedFences: Array<[string, CanvasRenderWindowRequestFence]> = [
		["socket", { ...fence, socket: {} }],
		["operation epoch", { ...fence, operationEpoch: 5 }],
		["viewport revision", { ...fence, viewportRevision: 9 }],
		["projection revision", { ...fence, projectionRevision: 13 }],
	];
	for (const [label, current] of changedFences) {
		it(`rejects retry after a ${label} change`, () => {
			expect(isCanvasRenderWindowRequestCurrent(fence, current)).toBe(false);
			expect(shouldRetryCanvasRenderWindow(1, fence, current)).toBe(false);
		});
	}

	it("rejects only source projections older than the DataStore summary", () => {
		expect(isCanvasRenderWindowSourceStale(11, 12)).toBe(true);
		expect(isCanvasRenderWindowSourceStale(12, 12)).toBe(false);
		expect(isCanvasRenderWindowSourceStale(13, 12)).toBe(false);
		expect(isCanvasRenderWindowSourceStale(13, undefined)).toBe(false);
	});
});
