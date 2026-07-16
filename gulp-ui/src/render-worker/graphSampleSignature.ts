import type { Source } from "@/entities/Source";

/**
 * Computes a compact signature for server-provided graph aggregation samples.
 * @param source Source whose graph sample cache should be compared.
 * @returns Signature that changes for same-length sample value updates.
 */
export function getWorkerGraphSampleSignature(source: Source.Type): string {
	const cachedSamples = source._sampleDataCached;
	if (
		!cachedSamples ||
		cachedSamples.frequency_sample !== source.settings.frequency_sample ||
		cachedSamples.min_timestamp !== source.timestamp.min ||
		cachedSamples.max_timestampe !== source.timestamp.max
	) {
		return [
			source.settings.frequency_sample,
			source.timestamp.min,
			source.timestamp.max,
			0,
			0,
			0,
		].join(":");
	}
	const samples = cachedSamples.sample_data ?? [];
	let timestampChecksum = 0;
	let countChecksum = 0;
	for (let index = 0; index < samples.length; index++) {
		const sample = samples[index];
		timestampChecksum =
			(timestampChecksum * 31 + sample.min_timestamp + index) % 2_147_483_647;
		countChecksum =
			(countChecksum * 31 + sample.sample + index) % 2_147_483_647;
	}
	return [
		cachedSamples.frequency_sample,
		cachedSamples.min_timestamp,
		cachedSamples.max_timestampe,
		samples.length,
		timestampChecksum,
		countChecksum,
	].join(":");
}
