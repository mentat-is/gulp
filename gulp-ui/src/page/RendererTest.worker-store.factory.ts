import type { EventSourceSummary, EventStoreNamespace } from "@/event-store";
import { RendererBenchmarkWorkerStore } from "./RendererTest.worker-store";

/** Creates the production benchmark worker with typed main-realm callbacks. */
export function createRendererBenchmarkWorkerStore(
	namespace: EventStoreNamespace,
	callbacks: {
		publishSourceSummary: (summary: EventSourceSummary) => void;
		readMainCacheDocuments: () => number;
	},
): RendererBenchmarkWorkerStore {
	const worker = new Worker(new URL("./RendererTest.worker", import.meta.url), {
		type: "module",
	});
	return new RendererBenchmarkWorkerStore(
		worker,
		namespace,
		callbacks.publishSourceSummary,
		callbacks.readMainCacheDocuments,
	);
}
