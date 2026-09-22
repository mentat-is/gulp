import type { Source } from "@/entities/Source";
import type { EventStoreViewportDescriptor } from "@/event-store";
import type { CanvasWorkerSynchronizationSnapshot } from "@/app/body/CanvasProfiler";
import { RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT } from "./RendererTest.benchmark";

/** Current Canvas authority and rows required by the direct benchmark projector. */
export interface RendererBenchmarkViewportInput {
	operationEpoch: number;
	projectionRevision: number;
	synchronization: Pick<
		CanvasWorkerSynchronizationSnapshot,
		"viewport" | "requestedViewportRevision"
	>;
	sources: readonly Source.Type[];
}

/**
 * Creates one bounded descriptor from the physical Canvas width and live zoom.
 * @param input Current Canvas synchronization and visible benchmark sources.
 * @returns Projection descriptor containing no exact document objects.
 */
export function createRendererBenchmarkViewportDescriptor(
	input: RendererBenchmarkViewportInput,
): EventStoreViewportDescriptor {
	const { frame, width, scale, scrollX } = input.synchronization.viewport;
	return {
		operationEpoch: input.operationEpoch,
		viewportRevision: input.synchronization.requestedViewportRevision,
		projectionRevision: input.projectionRevision,
		frame,
		width: Math.max(1, width),
		scale: Math.max(0.01, scale),
		scrollX,
		horizontalOverscanPixels: Math.max(1, width),
		maxRepresentatives: RENDERER_BENCHMARK_REPRESENTATIVE_LIMIT,
		sources: input.sources.map((source) => ({
			sourceId: source.id,
			offset: source.settings.offset,
			engine: source.settings.render_engine,
			frequency: source.settings.frequency_sample,
			overrideHashes: Object.keys(
				source.settings.color_override?.lookup ?? {},
			)
				.map(Number)
				.filter(Number.isFinite),
			settingsRevision: 0,
			overrideRevision: 0,
		})),
	};
}
