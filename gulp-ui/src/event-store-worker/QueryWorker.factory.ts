import type { QueryWorkerInitialization } from "@/event-stream";
import { DirectQueryTransport } from "./DirectQueryTransport";
import type {
	QueryTransport,
	QueryTransportCallbacks,
} from "./QueryTransport.types";
import { QueryWorkerClient } from "./QueryWorker.client";

/**
 * Creates the dedicated query worker when the current browser realm supports it.
 * @returns Worker instance, or null when construction is unavailable or blocked.
 */
export function createQueryWorker(): Worker | null {
	if (typeof Worker === "undefined") return null;
	try {
		return new Worker(new URL("./Query.worker", import.meta.url), {
			type: "module",
		});
	} catch {
		return null;
	}
}

/**
 * Selects QueryWorker for the EventStore pipeline. Direct transport is permitted
 * only by the explicit legacy rollback path because it parses documents on main.
 * @param initialization WebSocket URL, credentials, and migration routing settings.
 * @param callbacks Application-facing socket and stream callbacks.
 * @returns Connected query transport facade.
 */
export function createQueryTransport(
	initialization: QueryWorkerInitialization,
	callbacks: QueryTransportCallbacks,
): QueryTransport {
	const worker = createQueryWorker();
	if (worker) {
		try {
			return new QueryWorkerClient(worker, initialization, callbacks);
		} catch {
			// QueryWorkerClient terminates a worker that fails during initialization.
		}
	}
	if (initialization.legacyUnknownDocumentPassthrough) {
		return new DirectQueryTransport(initialization, callbacks);
	}
	throw new Error(
		"QueryWorker is required while EventStore LOD is enabled. Use the explicit legacy rollback only for recovery.",
	);
}
