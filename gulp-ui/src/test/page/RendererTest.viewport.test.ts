import type { Source } from "@/entities/Source";
import { createRendererBenchmarkViewportDescriptor } from "@/page/RendererTest.viewport";

const SOURCE_ID = "aaaaaaaa-0000-4000-8000-00000000000a" as Source.Id;
const SOURCE = {
	id: SOURCE_ID,
	settings: {
		offset: 0,
		render_engine: "default",
		frequency_sample: 1_000,
		color_override: { lookup: {} },
	},
} as Source.Type;

describe("RendererTest physical viewport descriptor", () => {
	it("keeps width physical at zoom 1 and 2.5 while projecting only passed rows", () => {
		for (const scale of [1, 2.5]) {
			const descriptor = createRendererBenchmarkViewportDescriptor({
				operationEpoch: 1,
				projectionRevision: 2,
				synchronization: {
					requestedViewportRevision: 3,
					viewport: {
						frame: { min: 1, max: 10 },
						width: 800,
						scale,
						scrollX: 25,
					},
				},
				sources: [SOURCE],
			});

			expect(descriptor.width).toBe(800);
			expect(descriptor.horizontalOverscanPixels).toBe(800);
			expect(descriptor.scale).toBe(scale);
			expect(descriptor.scrollX).toBe(25);
			expect(descriptor.sources.map((source) => source.sourceId)).toEqual([SOURCE_ID]);
		}
	});
});
