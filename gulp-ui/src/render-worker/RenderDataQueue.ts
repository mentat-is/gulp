import { mergeRenderDataPatches } from "./RenderDataPatch";
import type { RenderDataPatch } from "./RenderWorker.types";

const MAX_DATA_MESSAGES_IN_FLIGHT = 1;

/** Hard upper bound for sent plus unsent render-data envelopes. */
export const MAX_RENDER_DATA_QUEUE_DEPTH = 2;

/** Current data-channel occupancy exposed after every client transition. */
export interface RenderWorkerDataQueueDepth {
	inFlight: number;
	pending: number;
	aggregate: number;
}

/**
 * Owns the strict one-in-flight plus one-coalesced-pending data queue.
 * Typed-array patches stay in the pending envelope until dispatch transfers them.
 */
export class RenderDataQueue {
	private readonly inFlightSequences = new Set<number>();
	private pendingPatch: RenderDataPatch | null = null;

	/**
	 * Returns immutable queue occupancy for synchronization and profiling.
	 * @returns Current in-flight, pending, and aggregate envelope counts.
	 */
	public get depth(): RenderWorkerDataQueueDepth {
		const inFlight = this.inFlightSequences.size;
		const pending = this.pendingPatch ? 1 : 0;
		return { inFlight, pending, aggregate: inFlight + pending };
	}

	/**
	 * Merges a patch into the sole pending envelope without increasing queue depth.
	 * @param patch Revisioned render data entering the bounded queue.
	 * @returns Nothing.
	 */
	public enqueue(patch: RenderDataPatch): void {
		this.pendingPatch = this.pendingPatch
			? mergeRenderDataPatches(this.pendingPatch, patch)
			: patch;
	}

	/**
	 * Moves the pending envelope into the single in-flight slot.
	 * @param sequence Protocol sequence assigned to this attempted dispatch.
	 * @returns Patch to post, or null while the slot is occupied or no patch is pending.
	 */
	public dispatch(sequence: number): RenderDataPatch | null {
		if (
			this.inFlightSequences.size >= MAX_DATA_MESSAGES_IN_FLIGHT ||
			!this.pendingPatch
		) {
			return null;
		}
		const patch = this.pendingPatch;
		this.pendingPatch = null;
		this.inFlightSequences.add(sequence);
		return patch;
	}

	/**
	 * Releases one known in-flight sequence after ACK or a fatal post failure.
	 * @param sequence Protocol sequence to release.
	 * @returns True when the sequence occupied the in-flight slot.
	 */
	public acknowledge(sequence: number): boolean {
		return this.inFlightSequences.delete(sequence);
	}

	/**
	 * Clears pending and in-flight state at reset or disposal.
	 * @returns Nothing.
	 */
	public clear(): void {
		this.pendingPatch = null;
		this.inFlightSequences.clear();
	}
}
