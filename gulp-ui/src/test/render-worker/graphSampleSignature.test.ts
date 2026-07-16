import type { Source } from "@/entities/Source";
import { getWorkerGraphSampleSignature } from "@/render-worker/graphSampleSignature";

/**
 * Creates a minimal source fixture for graph sample signature tests.
 * @param overrides Source fields to override.
 * @returns Source fixture.
 */
function createSource(overrides: Partial<Source.Type> = {}): Source.Type {
	return {
		id: "graph-source" as Source.Id,
		settings: {
			frequency_sample: 1000,
		},
		timestamp: {
			min: 10,
			max: 20,
		},
		...overrides,
	} as Source.Type;
}

describe("getWorkerGraphSampleSignature", () => {
	it("treats sources without cached graph samples as empty current-range samples", () => {
		const source = createSource();

		expect(getWorkerGraphSampleSignature(source)).toBe("1000:10:20:0:0:0");
	});

	it("ignores stale cached graph samples", () => {
		const source = createSource({
			_sampleDataCached: {
				frequency_sample: 500,
				min_timestamp: 1,
				max_timestampe: 2,
				sample_data: [{ min_timestamp: 1, max_timestamp: 2, sample: 9 }],
			},
		});

		expect(getWorkerGraphSampleSignature(source)).toBe("1000:10:20:0:0:0");
	});

	it("includes loaded graph samples in the signature", () => {
		const source = createSource({
			_sampleDataCached: {
				frequency_sample: 1000,
				min_timestamp: 10,
				max_timestampe: 20,
				sample_data: [
					{ min_timestamp: 10, max_timestamp: 11, sample: 3 },
					{ min_timestamp: 11, max_timestamp: 12, sample: 5 },
				],
			},
		});

		expect(getWorkerGraphSampleSignature(source)).toBe("1000:10:20:2:322:99");
	});
});
