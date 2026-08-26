import {
	CANVAS_RENDER_WINDOW_MAX_ATTEMPTS,
	CANVAS_RENDER_WINDOW_RETRY_DELAY_MS,
	isCanvasRenderWindowRequestCurrent,
	isCanvasRenderWindowSourceAcceptable,
	isCanvasRenderWindowSourceCountMismatch,
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

	it("rejects count mismatches only for the exact committed revision", () => {
		const summary = {
			generation: 2,
			revision: 12,
			status: "complete" as const,
			eventCount: 94_391,
		};
		expect(
			isCanvasRenderWindowSourceCountMismatch(12, 90_000, summary),
		).toBe(true);
		expect(
			isCanvasRenderWindowSourceCountMismatch(12, 94_391, summary),
		).toBe(false);
		expect(
			isCanvasRenderWindowSourceCountMismatch(13, 95_000, summary),
		).toBe(false);
		expect(
			isCanvasRenderWindowSourceCountMismatch(12, 90_000, undefined),
		).toBe(false);
	});

	it("accepts a coherent building snapshot that advances within the current generation", () => {
		const summary = {
			generation: 3,
			revision: 12,
			status: "building" as const,
			eventCount: 120_000,
		};
		const projection = {
			sourceGeneration: 3,
			sourceRevision: 11,
			sourceStatus: "building" as const,
			loadedCount: 110_000,
		};
		expect(
			isCanvasRenderWindowSourceAcceptable(projection, summary, {
				generation: 3,
				revision: 10,
			}),
		).toBe(true);
		expect(
			isCanvasRenderWindowSourceAcceptable(projection, summary, {
				generation: 3,
				revision: 11,
			}),
		).toBe(false);
	});

	it("rejects obsolete generations and requires exact terminal authority", () => {
		const summary = {
			generation: 4,
			revision: 20,
			status: "complete" as const,
			eventCount: 200_000,
		};
		expect(
			isCanvasRenderWindowSourceAcceptable(
				{
					sourceGeneration: 3,
					sourceRevision: 20,
					sourceStatus: "building",
					loadedCount: 200_000,
				},
				summary,
				undefined,
			),
		).toBe(false);
		expect(
			isCanvasRenderWindowSourceAcceptable(
				{
					sourceGeneration: 4,
					sourceRevision: 20,
					sourceStatus: "complete",
					loadedCount: 200_000,
				},
				summary,
				undefined,
			),
		).toBe(true);
	});
});
