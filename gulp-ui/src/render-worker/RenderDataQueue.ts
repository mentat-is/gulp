import {
	mergeRenderDataPatches,
	selectNewestRenderWindowProjection,
} from "./RenderDataPatch";
import { isRenderWindowProjection } from "./RenderWorker.request";
import type {
	RenderDataPatch,
	RenderWindowProjection,
} from "./RenderWorker.types";

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
	private latestRenderWindow: RenderWindowProjection | null = null;

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
	 * A render window older than the newest queued or dispatched window is discarded.
	 * @param patch Revisioned render data entering the bounded queue.
	 * @returns Nothing.
	 * @throws Error when an optional render-window projection is malformed.
	 */
	public enqueue(patch: RenderDataPatch): void {
		if (
			patch.renderWindow !== undefined &&
			!isRenderWindowProjection(patch.renderWindow)
		) {
			throw new Error("Malformed render-window projection");
		}
		if (patch.renderWindow !== undefined) {
			const selected = selectNewestRenderWindowProjection(
				this.latestRenderWindow ?? undefined,
				patch.renderWindow,
			);
			if (selected !== patch.renderWindow) {
				patch = { ...patch, renderWindow: undefined };
			} else {
				this.latestRenderWindow = patch.renderWindow;
			}
		}
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
	 * Clears pending, in-flight, and render-window freshness state at reset or disposal.
	 * @returns Nothing.
	 */
	public clear(): void {
		this.pendingPatch = null;
		this.inFlightSequences.clear();
		this.latestRenderWindow = null;
	}
}
