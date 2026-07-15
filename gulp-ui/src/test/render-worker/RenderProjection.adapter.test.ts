import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { RenderProjectionAdapter } from "@/render-worker/RenderProjection.adapter";
import { DataStore } from "@/store/DataStore";

const SOURCE_ID = "source-a" as Source.Id;

/**
 * Creates one canonical projection fixture.
 * @param index Stable identifier and numeric payload seed.
 * @returns Document owned by the test source.
 */
function createDocument(index: number): Doc.Type {
	return {
		_id: `document-${index}` as Doc.Id,
		gulp_timestamp: 10_000 - index,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": 1,
		number_hash: index * 2,
	};
}

describe("RenderProjectionAdapter cooperative ranges", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation();
	});

	it("projects a source range in bounded blocks without slicing canonical data", async () => {
		const documents = Array.from({ length: 5_000 }, (_, index) =>
			createDocument(index),
		);
		const yieldCallback = jest.fn(async () => undefined);
		const adapter = new RenderProjectionAdapter({
			resolveDocument: () => undefined,
		});

		const batch = await adapter.createEventBatchAsync(
			SOURCE_ID,
			1,
			0,
			"replace",
			documents,
			undefined,
			0,
			yieldCallback,
			1_000,
			4_100,
		);

		expect(batch.handles).toHaveLength(3_100);
		expect(batch.timestamps[0]).toBe(documents[1_000].gulp_timestamp);
		expect(batch.timestamps[3_099]).toBe(documents[4_099].gulp_timestamp);
		expect(batch.hashes[3_099]).toBe(documents[4_099].number_hash);
		expect(yieldCallback).toHaveBeenCalledTimes(1);
	});
});
