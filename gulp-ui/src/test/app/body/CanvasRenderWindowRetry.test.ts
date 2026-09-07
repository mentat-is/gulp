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
			authorityRevision: 2,
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
			authorityRevision: 7,
			generation: 3,
			revision: 12,
			status: "building" as const,
			eventCount: 120_000,
		};
		const projection = {
			sourceAuthorityRevision: 7,
			sourceGeneration: 3,
			sourceRevision: 11,
			sourceStatus: "building" as const,
			loadedCount: 110_000,
		};
		expect(
			isCanvasRenderWindowSourceAcceptable(projection, summary, {
				authorityRevision: 7,
				generation: 3,
				revision: 10,
			}),
		).toBe(true);
		expect(
			isCanvasRenderWindowSourceAcceptable(projection, summary, {
				authorityRevision: 7,
				generation: 3,
				revision: 11,
			}),
		).toBe(false);
	});

	it("accepts only the canonical empty placeholder before a source has its first summary", () => {
		expect(
			isCanvasRenderWindowSourceAcceptable(
				{
					sourceAuthorityRevision: 0,
					sourceGeneration: 0,
					sourceRevision: 0,
					sourceStatus: "incomplete",
					loadedCount: 0,
				},
				undefined,
				undefined,
			),
		).toBe(true);
		expect(
			isCanvasRenderWindowSourceAcceptable(
				{
					sourceAuthorityRevision: 1,
					sourceGeneration: 1,
					sourceRevision: 1,
					sourceStatus: "complete",
					loadedCount: 1,
				},
				undefined,
				undefined,
			),
		).toBe(false);
	});

	it("rejects obsolete generations and requires exact terminal authority", () => {
		const summary = {
			authorityRevision: 8,
			generation: 4,
			revision: 20,
			status: "complete" as const,
			eventCount: 200_000,
		};
		expect(
			isCanvasRenderWindowSourceAcceptable(
				{
					sourceAuthorityRevision: 8,
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
					sourceAuthorityRevision: 8,
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

	it("accepts Undo to an older physical generation when logical authority advances", () => {
		const summary = {
			authorityRevision: 11,
			generation: 1,
			revision: 12,
			status: "complete" as const,
			eventCount: 10,
		};
		const projection = {
			sourceAuthorityRevision: 11,
			sourceGeneration: 1,
			sourceRevision: 12,
			sourceStatus: "complete" as const,
			loadedCount: 10,
		};

		expect(
			isCanvasRenderWindowSourceAcceptable(projection, summary, {
				authorityRevision: 10,
				generation: 5,
				revision: 30,
			}),
		).toBe(true);
		expect(
			isCanvasRenderWindowSourceAcceptable(
				{
					...projection,
					sourceAuthorityRevision: 10,
					sourceGeneration: 9,
					sourceRevision: 99,
					sourceStatus: "building",
				},
				summary,
				undefined,
			),
		).toBe(false);
	});
});
