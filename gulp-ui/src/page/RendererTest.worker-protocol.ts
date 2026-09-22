import type { Doc } from "@/entities/Doc";
import type { EventSourceSummary, EventStoreNamespace, EventStoreViewportDescriptor } from "@/event-store";
import type { RenderWindowProjection } from "@/render-worker/RenderWorker.types";
import type {
	RendererBenchmarkDiagnostics,
	RendererBenchmarkStartOptions,
	RendererBenchmarkWriteProgress,
} from "./RendererTest.benchmark";

/** Commands sent from the benchmark page to its dedicated storage worker. */
export type RendererBenchmarkWorkerCommand =
	| {
			type: "START";
			namespace: EventStoreNamespace;
			options: RendererBenchmarkStartOptions;
	  }
	| {
			type: "PROJECT";
			requestId: number;
			descriptor: EventStoreViewportDescriptor;
	  }
	| { type: "UPSERT"; requestId: number; documents: Doc.Type[] }
	| { type: "CANCEL" };

/** Bounded progress, source authority, and transferable viewport replies. */
export type RendererBenchmarkWorkerEvent =
	| { type: "STARTED"; progress: RendererBenchmarkWriteProgress }
	| { type: "PROGRESS"; progress: RendererBenchmarkWriteProgress }
	| { type: "FINISHED"; progress: RendererBenchmarkWriteProgress }
	| { type: "CANCELLED"; diagnostics: RendererBenchmarkDiagnostics }
	| { type: "SOURCE_SUMMARY"; summary: EventSourceSummary }
	| {
			type: "PROJECTION";
			requestId: number;
			projection: RenderWindowProjection | null;
			diagnostics: RendererBenchmarkDiagnostics;
			error?: string;
	  }
	| {
			type: "UPSERTED";
			requestId: number;
			summaries: EventSourceSummary[];
			diagnostics: RendererBenchmarkDiagnostics;
			error?: string;
	  }
	| { type: "ERROR"; message: string };
