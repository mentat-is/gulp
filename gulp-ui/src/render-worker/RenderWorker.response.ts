import {
	RENDER_PROTOCOL_VERSION,
	type RenderAcknowledgedSourceState,
	type RenderHitTestResult,
	type RenderWorkerResponse,
} from "./RenderWorker.types";

type RenderAcknowledgement = Extract<
	RenderWorkerResponse,
	{ type: "ACK" }
>["payload"];

/**
 * Returns whether a value is an object that can safely be inspected as a payload.
 * @param value Candidate protocol value.
 * @returns True when the value is a non-null record.
 */
function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

/**
 * Checks that every named property is a finite number.
 * @param record Record containing protocol numeric fields.
 * @param keys Required numeric property names.
 * @returns True when every property is finite.
 */
function hasFiniteNumbers(
	record: Record<string, unknown>,
	keys: readonly string[],
): boolean {
	return keys.every(
		(key) => typeof record[key] === "number" && Number.isFinite(record[key]),
	);
}

/**
 * Validates a worker hit-test identity without trusting its discriminant.
 * @param value Candidate hit-test result.
 * @returns True for null or a fully shaped interactive-overlay identity.
 */
function isRenderHitTestResult(value: unknown): value is RenderHitTestResult | null {
	if (value === null) return true;
	if (!isRecord(value) || typeof value.id !== "string") return false;
	if (
		value.kind === "link" ||
		value.kind === "cache-warning" ||
		value.kind === "supplemental-event"
	)
		return true;
	return (
		value.kind === "note" &&
		Array.isArray(value.noteIds) &&
		value.noteIds.every((noteId) => typeof noteId === "string")
	);
}

/**
 * Validates a transferable bitmap in browser and test runtimes.
 * @param value Candidate magnifier bitmap.
 * @returns True when the value is an ImageBitmap or exposes its required close method.
 */
function isImageBitmapPayload(value: unknown): value is ImageBitmap {
	return (
		(typeof ImageBitmap !== "undefined" && value instanceof ImageBitmap) ||
		(isRecord(value) && typeof value.close === "function")
	);
}

/**
 * Validates one response payload according to its known discriminant.
 * @param type Candidate response type.
 * @param payload Candidate response payload.
 * @returns True only for a complete supported payload.
 */
function isRenderWorkerResponsePayload(type: string, payload: unknown): boolean {
	if (!isRecord(payload)) return false;
	switch (type) {
		case "CAPABILITIES":
			return [
				"offscreenCanvas",
				"workerAnimationFrame",
				"workerFontFace",
				"imageBitmap",
			].every((key) => typeof payload[key] === "boolean");
		case "READY":
			return (
				typeof payload.fontsLoaded === "boolean" &&
				(payload.fontFallbackReason === undefined ||
					typeof payload.fontFallbackReason === "string")
			);
		case "ACK":
			return (
				hasFiniteNumbers(payload, ["acknowledgedSequence", "globalRevision"]) &&
				Array.isArray(payload.sourceRevisions) &&
				payload.sourceRevisions.every(
					(sourceRevision) =>
						isRecord(sourceRevision) &&
						typeof sourceRevision.sourceId === "string" &&
						typeof sourceRevision.revision === "number" &&
						Number.isSafeInteger(sourceRevision.revision) &&
						sourceRevision.revision >= 0 &&
						typeof sourceRevision.dataSequence === "number" &&
						Number.isSafeInteger(sourceRevision.dataSequence) &&
						sourceRevision.dataSequence >= 0,
				) &&
				Array.isArray(payload.rejectedSourceIds) &&
				payload.rejectedSourceIds.every(
					(sourceId) => typeof sourceId === "string",
				)
			);
		case "FRAME_RENDERED":
			return (
				hasFiniteNumbers(payload, [
					"frameTimestamp",
					"drawDuration",
					"workerReceiveDuration",
					"workerWaitDuration",
					"dirtyMask",
					"viewportRevision",
					"inputSequence",
					"dataRevision",
					"renderedDataSequence",
					"visibleSources",
					"renderedRows",
					"renderedEventPixels",
					"graphSamples",
					"graphBuckets",
					"visibleNotes",
					"visibleLinks",
					"interactiveRegions",
				]) &&
				(payload.dataLatency === null ||
					(typeof payload.dataLatency === "number" &&
						Number.isFinite(payload.dataLatency)))
			);
		case "HIT_TEST_RESULT":
			return (
				hasFiniteNumbers(payload, ["requestId", "viewportRevision"]) &&
				isRenderHitTestResult(payload.hit)
			);
		case "HOVER_CHANGED":
			return (
				hasFiniteNumbers(payload, ["viewportRevision"]) &&
				isRenderHitTestResult(payload.hit)
			);
		case "MAGNIFIER_FRAME":
			return (
				hasFiniteNumbers(payload, ["requestId", "viewportRevision"]) &&
				isImageBitmapPayload(payload.bitmap)
			);
		case "ASSET_REQUEST":
			return (
				typeof payload.assetKey === "string" &&
				typeof payload.color === "string"
			);
		case "RESYNC_REQUIRED": {
			const validScope = [
				"source",
				"sources",
				"notes",
				"links",
				"highlights",
				"flags",
			].includes(String(payload.scope));
			return (
				validScope &&
				hasFiniteNumbers(payload, ["expectedRevision", "receivedRevision"]) &&
				(payload.scope !== "source" || typeof payload.id === "string") &&
				(payload.id === undefined || typeof payload.id === "string")
			);
		}
		case "ERROR":
			return (
				[
					"protocol",
					"initialize",
					"data",
					"render",
					"asset",
					"magnifier",
				].includes(String(payload.phase)) &&
				typeof payload.message === "string" &&
				typeof payload.fatal === "boolean" &&
				(payload.stack === undefined || typeof payload.stack === "string")
			);
		default:
			return false;
	}
}

/**
 * Validates complete protocol metadata and discriminated payload before consumption.
 * @param value Candidate response received from the worker.
 * @returns True when the response is a fully shaped supported response.
 */
export function isRenderWorkerResponse(value: unknown): value is RenderWorkerResponse {
	return (
		isRecord(value) &&
		value.protocolVersion === RENDER_PROTOCOL_VERSION &&
		typeof value.operationEpoch === "number" &&
		Number.isSafeInteger(value.operationEpoch) &&
		typeof value.sequence === "number" &&
		Number.isSafeInteger(value.sequence) &&
		typeof value.type === "string" &&
		isRenderWorkerResponsePayload(value.type, value.payload)
	);
}

/**
 * Applies worker-authored source revision and applying-sequence telemetry.
 * Rejected identities are removed after accepted revisions so response ordering with
 * RESYNC_REQUIRED cannot reinstate an optimistic revision.
 * @param acknowledgedSourceStates Client synchronization map to update.
 * @param acknowledgement Authoritative ACK payload emitted by the worker repository.
 * @returns Nothing.
 */
export function applyAcknowledgedSourceRevisions(
	acknowledgedSourceStates: Map<string, RenderAcknowledgedSourceState>,
	acknowledgement: RenderAcknowledgement,
): void {
	for (const sourceRevision of acknowledgement.sourceRevisions) {
		acknowledgedSourceStates.set(sourceRevision.sourceId, {
			revision: sourceRevision.revision,
			dataSequence: sourceRevision.dataSequence,
		});
	}
	for (const rejectedSourceId of acknowledgement.rejectedSourceIds) {
		acknowledgedSourceStates.delete(rejectedSourceId);
	}
}
