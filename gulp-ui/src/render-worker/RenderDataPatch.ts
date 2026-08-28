import type {
	RenderDataPatch,
	RenderEventSnapshotRecord,
	RenderSourcePatch,
	RenderWindowProjection,
} from "./RenderWorker.types";

/**
 * Checks whether an operation epoch advances the current client session.
 * @param currentEpoch Epoch currently owned by the render client.
 * @param candidateEpoch Epoch proposed by an operation reset.
 * @returns True only for a finite integer greater than the current epoch.
 */
export function isOperationEpochAdvance(
	currentEpoch: number,
	candidateEpoch: number,
): boolean {
	return (
		Number.isSafeInteger(candidateEpoch) && candidateEpoch > currentEpoch
	);
}

/**
 * Collects transferable buffers from a render data patch without including canonical data.
 * @param value Patch value or nested member to inspect.
 * @param transferables Destination set used to avoid duplicate transfer-list entries.
 * @param visited Objects already traversed while collecting nested values.
 * @returns Nothing.
 */
function collectTransferables(
	value: unknown,
	transferables: Set<Transferable>,
	visited: Set<object>,
): void {
	if (value === null || typeof value !== "object") return;
	if (visited.has(value)) return;
	visited.add(value);

	if (value instanceof ArrayBuffer) {
		transferables.add(value);
		return;
	}

	if (ArrayBuffer.isView(value)) {
		if (value.buffer instanceof ArrayBuffer) {
			transferables.add(value.buffer);
		}
		return;
	}

	if (Array.isArray(value)) {
		for (const item of value) {
			collectTransferables(item, transferables, visited);
		}
		return;
	}

	for (const nestedValue of Object.values(value)) {
		collectTransferables(nestedValue, transferables, visited);
	}
}

/**
 * Creates a transfer list containing every newly allocated event or render-window buffer.
 * @param patch Render data patch whose buffers will be detached after posting.
 * @returns Unique transferables owned by the patch.
 */
export function getDataPatchTransferables(
	patch: RenderDataPatch,
): Transferable[] {
	const transferables = new Set<Transferable>();
	collectTransferables(patch, transferables, new Set<object>());
	return Array.from(transferables);
}

/**
 * Coalesces partial metadata records by source identity while preserving removals
 * unless the newer patch explicitly reintroduces that source. New records win for
 * repeated IDs and combined removals are applied to all retained records.
 * @param previous Source metadata already retained by the pending envelope.
 * @param next Newer source metadata entering the pending envelope.
 * @returns Newer source records with all still-applicable removals retained.
 */
function mergeSourcePatches(
	previous: RenderSourcePatch | undefined,
	next: RenderSourcePatch | undefined,
): RenderSourcePatch | undefined {
	if (!next) return previous;
	if (!previous) return next;

	const recordsBySourceId = new Map(
		previous.records.map((source) => [source.id, source]),
	);
	for (const source of next.records) {
		recordsBySourceId.set(source.id, source);
	}
	const removedSourceIds = new Set([
		...previous.removedSourceIds,
		...next.removedSourceIds,
	]);
	for (const source of next.records) {
		removedSourceIds.delete(source.id);
	}
	for (const removedSourceId of removedSourceIds) {
		recordsBySourceId.delete(removedSourceId);
	}

	return {
		...next,
		records: Array.from(recordsBySourceId.values()),
		removedSourceIds: Array.from(removedSourceIds),
	};
}

/**
 * Coalesces unsent snapshot transactions while allowing a newer begin record to
 * discard every older unsent transaction fragment for the same source.
 * @param previous Snapshot records already retained by the bounded pending envelope.
 * @param next New snapshot records entering the pending envelope.
 * @returns Ordered records required to reconstruct only current source transactions.
 */
function mergeEventSnapshotRecords(
	previous: readonly RenderEventSnapshotRecord[],
	next: readonly RenderEventSnapshotRecord[],
): RenderEventSnapshotRecord[] {
	const merged = [...previous];
	for (const record of next) {
		if (record.kind === "begin") {
			for (let index = merged.length - 1; index >= 0; index--) {
				if (merged[index].sourceId === record.sourceId) merged.splice(index, 1);
			}
		}
		merged.push(record);
	}
	return merged;
}

/**
 * Selects one complete render window without combining source arrays or evictions.
 * Operation and viewport freshness take precedence over the producer-local projection revision.
 * @param previous Projection already retained by the pending envelope.
 * @param next New projection entering the pending envelope.
 * @returns The newest complete projection, preserving its original transferable buffers.
 */
export function selectNewestRenderWindowProjection(
	previous: RenderWindowProjection | undefined,
	next: RenderWindowProjection | undefined,
): RenderWindowProjection | undefined {
	if (!next) return previous;
	if (!previous) return next;
	if (next.operationEpoch !== previous.operationEpoch) {
		return next.operationEpoch > previous.operationEpoch ? next : previous;
	}
	if (next.viewportRevision !== previous.viewportRevision) {
		return next.viewportRevision > previous.viewportRevision ? next : previous;
	}
	if (next.projectionRevision !== previous.projectionRevision) {
		return next.projectionRevision > previous.projectionRevision ? next : previous;
	}
	return next;
}

/**
 * Coalesces two unsent data patches without mutating transferable buffers.
 * Incremental sections preserve their ordering while a render window remains atomic.
 * @param previous Patch already retained by the bounded pending envelope.
 * @param next Newer patch entering the pending envelope.
 * @returns Patch representing both queued updates.
 */
export function mergeRenderDataPatches(
	previous: RenderDataPatch,
	next: RenderDataPatch,
): RenderDataPatch {
	const replacementSnapshotSourceIds = new Set(
		(next.eventSnapshots ?? [])
			.filter((record) => record.kind === "begin")
			.map((record) => record.sourceId),
	);
	const eventBatches = new Map<
		string,
		NonNullable<RenderDataPatch["events"]>
	>();

	for (const batch of previous.events ?? []) {
		if (replacementSnapshotSourceIds.has(batch.sourceId)) continue;
		const sourceBatches = eventBatches.get(batch.sourceId) ?? [];
		sourceBatches.push(batch);
		eventBatches.set(batch.sourceId, sourceBatches);
	}
	for (const batch of next.events ?? []) {
		const sourceBatches = eventBatches.get(batch.sourceId) ?? [];
		const previousBatch = sourceBatches[sourceBatches.length - 1];
		if (batch.mode === "replace" || batch.mode === "clear") {
			eventBatches.set(batch.sourceId, [batch]);
		} else {
			// Preserve immutable transferred batches. The worker applies them serially,
			// avoiding repeated typed-array-to-JavaScript-array merges under backlog.
			sourceBatches.push(batch);
			eventBatches.set(batch.sourceId, sourceBatches);
		}
	}

	const overlays =
		previous.overlays || next.overlays
			? {
					notes: next.overlays?.notes ?? previous.overlays?.notes,
					links: next.overlays?.links ?? previous.overlays?.links,
					highlights:
						next.overlays?.highlights ?? previous.overlays?.highlights,
					flags: next.overlays?.flags ?? previous.overlays?.flags,
					supplementalEvents:
						next.overlays?.supplementalEvents ??
						previous.overlays?.supplementalEvents,
				}
			: undefined;

	return {
		globalRevision: Math.max(previous.globalRevision, next.globalRevision),
		sources: mergeSourcePatches(previous.sources, next.sources),
		events:
			eventBatches.size > 0
				? Array.from(eventBatches.values()).flat()
				: undefined,
		eventSnapshots:
			previous.eventSnapshots || next.eventSnapshots
				? mergeEventSnapshotRecords(
						previous.eventSnapshots ?? [],
						next.eventSnapshots ?? [],
					)
				: undefined,
		overlays,
		renderWindow: selectNewestRenderWindowProjection(
			previous.renderWindow,
			next.renderWindow,
		),
	};
}
