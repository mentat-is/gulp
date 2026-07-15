/// <reference lib="webworker" />

import { RenderWorkerController } from "./RenderWorkerController";
import {
	RENDER_PROTOCOL_VERSION,
	type RenderWorkerBootstrapMessage,
	type RenderWorkerResponse,
} from "./RenderWorker.types";

const workerScope = self as DedicatedWorkerGlobalScope;
let controller: RenderWorkerController | null = null;
let bootstrapErrorSequence = 0;

/**
 * Checks the global INIT message before transferred resources are dereferenced.
 * @param value Untrusted worker-global message payload.
 * @returns True when the minimum bootstrap shape and protocol version are valid.
 */
function isBootstrapMessage(
	value: unknown,
): value is RenderWorkerBootstrapMessage {
	if (!value || typeof value !== "object") return false;
	const record = value as Record<string, unknown>;
	if (
		record.protocolVersion !== RENDER_PROTOCOL_VERSION ||
		typeof record.operationEpoch !== "number" ||
		typeof record.sequence !== "number" ||
		record.type !== "INIT" ||
		!record.payload ||
		typeof record.payload !== "object"
	) {
		return false;
	}
	const payload = record.payload as Record<string, unknown>;
	return (
		typeof OffscreenCanvas !== "undefined" &&
		payload.canvas instanceof OffscreenCanvas &&
		payload.controlPort instanceof MessagePort &&
		payload.dataPort instanceof MessagePort &&
		Array.isArray(payload.fonts)
	);
}

/**
 * Publishes an initialization error on the global worker channel before ports attach.
 * @param error Unknown bootstrap exception.
 * @param operationEpoch Epoch from the rejected bootstrap when available.
 * @returns Nothing.
 */
function publishBootstrapError(error: unknown, operationEpoch = 0): void {
	const response: Extract<RenderWorkerResponse, { type: "ERROR" }> = {
		protocolVersion: RENDER_PROTOCOL_VERSION,
		operationEpoch,
		sequence: ++bootstrapErrorSequence,
		type: "ERROR",
		payload: {
			phase: "initialize",
			message: error instanceof Error ? error.message : String(error),
			fatal: true,
			stack: error instanceof Error ? error.stack : undefined,
		},
	};
	workerScope.postMessage(response);
}

workerScope.onmessage = (event: MessageEvent<unknown>): void => {
	if (!isBootstrapMessage(event.data)) {
		publishBootstrapError("Malformed render-worker INIT message");
		return;
	}
	if (controller) {
		publishBootstrapError(
			"Render worker has already been initialized",
			event.data.operationEpoch,
		);
		return;
	}
	try {
		controller = new RenderWorkerController(event.data);
	} catch (error) {
		publishBootstrapError(error, event.data.operationEpoch);
	}
};

workerScope.onmessageerror = (): void => {
	publishBootstrapError("Unable to deserialize render-worker INIT message");
};
