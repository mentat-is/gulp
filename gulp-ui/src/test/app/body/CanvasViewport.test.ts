import {
	getCanvasViewportSourceIds,
	getCanvasViewportSourceRange,
} from "@/app/body/CanvasViewport";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";

/**
 * Creates the minimum source fixture required by the viewport ID selector.
 * @param id Source fixture identifier.
 * @returns Strictly typed source fixture.
 */
function createSource(id: string): Source.Type {
	return {
		id,
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
	} as Source.Type;
}

describe("Canvas viewport source range", () => {
	it("includes two 48-pixel rows of overscan above and below", () => {
		expect(getCanvasViewportSourceRange(20, 96, 96)).toEqual({
			firstIndex: 0,
			lastIndex: 5,
		});
	});

	it("clamps the range at the first and final source rows", () => {
		expect(getCanvasViewportSourceRange(3, 0, 500)).toEqual({
			firstIndex: 0,
			lastIndex: 2,
		});
	});

	it("returns ordered source IDs for the calculated range", () => {
		const sources = Array.from({ length: 8 }, (_, index) =>
			createSource(`source-${index}`),
		);

		expect(getCanvasViewportSourceIds(sources, 96, 96)).toEqual([
			"source-0",
			"source-1",
			"source-2",
			"source-3",
			"source-4",
			"source-5",
		]);
	});
});
