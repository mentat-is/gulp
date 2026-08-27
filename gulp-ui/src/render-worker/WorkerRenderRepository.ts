import {
	type RenderDataPatch,
	type RenderEventBatch,
	type RenderEventSnapshotRecord,
	type RenderFlagRecord,
	type RenderFrameState,
	type RenderHighlightRecord,
	type RenderHitTestResult,
	type RenderLinkRecord,
	type RenderNoteRecord,
	type RenderOverlayPatch,
	type RenderPointerState,
	type RenderSceneState,
	type RenderSource,
	type RenderTimeRange,
	type RenderViewportState,
	type RenderWindowProjection,
	type RenderWindowSourceProjection,
} from "./RenderWorker.types";
import { isRenderWindowProjection } from "./RenderWorker.request";

const HEIGHT_BUCKET_DURATION = 60_000;
const INTERACTIVE_STAGING_SLICE_BUDGET_MS = 1;
const IDLE_STAGING_SLICE_BUDGET_MS = 4;
const STAGING_INTERACTION_WINDOW_MS = 120;
const MINIMUM_SPARE_CAPACITY = 16;
const STAGING_CLOCK_CHECK_MASK = 2_047;

/** Precomputed event state atomically exposed to the renderer after staging. */
export interface WorkerRenderEventView {
	revision: number;
	loadedCount: number;
	/** Exact frame-local count for EventStore projections; absent for direct snapshots. */
	frameEventCount?: number;
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	hashMin: number;
	hashMax: number;
	hashMinCount: number;
	hashMaxCount: number;
	handleIndexes: Uint32Array;
	overrideIndexes: Uint32Array;
	colorSourceRevision: number;
	heightTimestamps: Float64Array;
	heightCounts: Uint32Array | Float64Array;
	heightSuffixCounts: Float64Array;
	maxHeightCount: number;
	sampleTimestamps: Float64Array;
	sampleCounts: Float64Array;
	sampleSuffixCounts: Float64Array;
	samplesAreDerived: boolean;
	operationEpoch?: number;
	viewportRevision?: number;
	projectionRevision?: number;
	coveredRange?: RenderTimeRange;
	bucketPixelWidth?: number;
	degradedLod?: boolean;
}

/** Canvas rectangle associated with a compact overlay identity. */
export interface WorkerHitRegion {
	kind: "note" | "link";
	id: string;
	noteIds?: string[];
	x: number;
	y: number;
	width: number;
	height: number;
}

/** Revision gap detected while applying a non-replacement data change. */
export interface WorkerResyncRequest {
	scope: "source" | "sources" | "notes" | "links" | "highlights" | "flags";
	id?: string;
	expectedRevision: number;
	receivedRevision: number;
}

/** Result of applying one data-channel patch. */
export interface WorkerRepositoryApplyResult {
	committedEvents: boolean;
	committedSources: boolean;
	committedOverlays: boolean;
	resync: WorkerResyncRequest[];
}

interface PreparedEventArrays {
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	sampleTimestamps: Float64Array;
	sampleCounts: Float64Array;
	samplesAreDerived: boolean;
}

/** Mutable worker-owned storage for an in-progress source snapshot transaction. */
interface WorkerEventSnapshotStage {
	snapshotId: number;
	sourceId: string;
	revision: number;
	totalEvents: number;
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	sampleTimestamps?: Float64Array;
	sampleCounts?: Float64Array;
	receivedEvents: number;
}

/** Snapshot receive buffers that become the committed event backing allocation. */
interface AdoptedEventArrays {
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
}

interface IncrementalBucketView<TCounts extends Uint32Array | Float64Array> {
	timestamps: Float64Array;
	counts: TCounts;
	maximum: number;
}

/** Spare-capacity storage hidden behind immutable logical typed-array views. */
interface WorkerEventBackingStore {
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	eventLength: number;
	handleIndexes: Uint32Array;
	handleIndexLength: number;
	overrideIndexes: Uint32Array;
	overrideLength: number;
	heightTimestamps: Float64Array;
	heightCounts: Uint32Array;
	heightSuffixCounts: Float64Array;
	heightStart: number;
	heightLength: number;
	sampleTimestamps: Float64Array;
	sampleCounts: Float64Array;
	sampleSuffixCounts: Float64Array;
	sampleStart: number;
	sampleLength: number;
}

type StageCancellationCheck = () => boolean;

/**
 * Reusable scheduler that yields staging work through a worker MessageChannel.
 * This lets control-port traffic run between large event-processing slices.
 */
class WorkerYieldScheduler {
	private readonly channel: MessageChannel;
	private readonly resolvers: Array<() => void> = [];

	/** Creates the internal channel used to enqueue cooperative continuations. */
	public constructor() {
		this.channel = new MessageChannel();
		this.channel.port1.onmessage = () => {
			this.resolvers.shift()?.();
		};
		this.channel.port1.start();
		this.channel.port2.start();
	}

	/**
	 * Yields until a message task can resume the current staging operation.
	 * @returns Promise resolved by the next internal channel task.
	 */
	public yield(): Promise<void> {
		return new Promise<void>((resolve) => {
			this.resolvers.push(resolve);
			this.channel.port2.postMessage(null);
		});
	}

	/**
	 * Closes both channel ports and resolves pending continuations during disposal.
	 * @returns Nothing.
	 */
	public dispose(): void {
		this.channel.port1.close();
		this.channel.port2.close();
		while (this.resolvers.length > 0) {
			this.resolvers.shift()?.();
		}
	}
}

/**
 * Worker-local, render-only state repository.
 * Canonical entities remain on the main thread and only compact projections live here.
 */
export class WorkerRenderRepository {
	private readonly sources = new Map<string, RenderSource>();
	private readonly events = new Map<string, WorkerRenderEventView>();
	private readonly eventStageGeneration = new Map<string, number>();
	private readonly eventSnapshotStages = new Map<
		string,
		WorkerEventSnapshotStage
	>();
	private readonly removedSourceIds = new Set<string>();
	private readonly eventBackingStores = new WeakMap<
		WorkerRenderEventView,
		WorkerEventBackingStore
	>();
	private readonly assets = new Map<string, ImageBitmap>();
	private readonly yieldScheduler = new WorkerYieldScheduler();
	private notes: RenderNoteRecord[] = [];
	private links: RenderLinkRecord[] = [];
	private highlights: RenderHighlightRecord[] = [];
	private flags: RenderFlagRecord[] = [];
	private hitRegions: WorkerHitRegion[] = [];
	private sourceRevision = 0;
	private notesRevision = 0;
	private linksRevision = 0;
	private highlightsRevision = 0;
	private flagsRevision = 0;
	private globalRevision = 0;
	private acknowledgedGlobalRevision = 0;
	private renderWindowOperationEpoch = -1;
	private renderWindowViewportRevision = -1;
	private renderWindowProjectionRevision = -1;
	private resetGeneration = 0;
	private disposed = false;
	private lastInteractionAt = Number.NEGATIVE_INFINITY;
	private viewportState: RenderViewportState | null = null;
	private sceneState: RenderSceneState | null = null;
	private pointerState: RenderPointerState = { x: -1000, y: -1000, inside: false };
	private frameState: RenderFrameState | null = null;

	/**
	 * Returns the latest complete worker frame state.
	 * @returns Frame state, or null before both split state messages arrive.
	 */
	public getFrameState(): RenderFrameState | null {
		return this.frameState;
	}

	/**
	 * Replaces the frequently changing viewport half of worker render state.
	 * @param state Latest dimensions, transform, and input revision state.
	 * @returns Nothing.
	 */
	public setViewportState(state: RenderViewportState): void {
		this.viewportState = state;
		this.rebuildFrameState();
	}

	/**
	 * Replaces the infrequently changing scene half of worker render state.
	 * @param state Latest source order, appearance, target, and revision state.
	 * @returns Nothing.
	 */
	public setSceneState(state: RenderSceneState): void {
		if (
			this.sceneState &&
			this.sceneState.revisions.assets !== state.revisions.assets
		) {
			for (const bitmap of this.assets.values()) bitmap.close();
			this.assets.clear();
		}
		this.sceneState = state;
		this.rebuildFrameState();
	}

	/**
	 * Replaces pointer coordinates without rebuilding viewport or scene payloads.
	 * @param pointer Latest pointer coordinates and inside state.
	 * @returns Nothing.
	 */
	public setPointerState(pointer: RenderPointerState): void {
		this.pointerState = pointer;
		if (this.frameState) {
			this.frameState = { ...this.frameState, pointer };
		}
	}

	/**
	 * Installs a complete internal frame fixture while preserving split wire-state storage.
	 * @param state Complete worker-internal frame state.
	 * @returns Nothing.
	 */
	public setFrameState(state: RenderFrameState): void {
		const { pointer, ...splitState } = state;
		this.setViewportState({
			width: splitState.width,
			height: splitState.height,
			frame: splitState.frame,
			scale: splitState.scale,
			scrollX: splitState.scrollX,
			scrollY: splitState.scrollY,
			viewportRevision: splitState.viewportRevision,
			inputSequence: splitState.inputSequence,
			inputTimestamp: splitState.inputTimestamp,
		});
		this.setSceneState({
			sourceOrder: splitState.sourceOrder,
			hidden: splitState.hidden,
			target: splitState.target,
			palette: splitState.palette,
			targetGuideColor: splitState.targetGuideColor,
			localized: splitState.localized,
			revisions: splitState.revisions,
		});
		this.setPointerState(pointer);
	}

	/**
	 * Reassembles the internal frame view when both protocol state halves exist.
	 * @returns Nothing.
	 */
	private rebuildFrameState(): void {
		if (!this.viewportState || !this.sceneState) {
			this.frameState = null;
			return;
		}
		this.frameState = {
			...this.viewportState,
			...this.sceneState,
			pointer: this.pointerState,
		};
	}

	/**
	 * Marks recent high-priority control activity for one-millisecond staging slices.
	 * @returns Nothing.
	 */
	public markInteraction(): void {
		this.lastInteractionAt = performance.now();
	}

	/**
	 * Returns the current worker preprocessing budget from recent control activity.
	 * @returns One millisecond while interactive, otherwise four milliseconds.
	 */
	private getStagingSliceBudgetMs(): number {
		return performance.now() - this.lastInteractionAt <=
			STAGING_INTERACTION_WINDOW_MS
			? INTERACTIVE_STAGING_SLICE_BUDGET_MS
			: IDLE_STAGING_SLICE_BUDGET_MS;
	}

	/**
	 * Returns selected sources in the exact order provided by the current frame state.
	 * @returns Ordered render-source list.
	 */
	public getOrderedSources(): RenderSource[] {
		const order = this.frameState?.sourceOrder;
		if (!order) {
			return Array.from(this.sources.values()).sort(
				(left, right) => left.rowOrder - right.rowOrder,
			);
		}
		if (order.length === 0) return [];

		const ordered: RenderSource[] = [];
		for (const sourceId of order) {
			const source = this.sources.get(sourceId);
			if (source) ordered.push(source);
		}
		return ordered;
	}

	/**
	 * Resolves one source by its stable string identifier.
	 * @param sourceId Source identifier to resolve.
	 * @returns Render source, or undefined when unavailable.
	 */
	public getSource(sourceId: string): RenderSource | undefined {
		return this.sources.get(sourceId);
	}

	/**
	 * Resolves the latest committed event projection for one source.
	 * @param sourceId Source identifier to resolve.
	 * @returns Committed event view, or undefined before data synchronization.
	 */
	public getEvents(sourceId: string): WorkerRenderEventView | undefined {
		return this.events.get(sourceId);
	}

	/**
	 * Returns the current compact note collection.
	 * @returns Read-only note records used by drawing and hit testing.
	 */
	public getNotes(): readonly RenderNoteRecord[] {
		return this.notes;
	}

	/**
	 * Returns the current compact link collection.
	 * @returns Read-only link records with resolved endpoints.
	 */
	public getLinks(): readonly RenderLinkRecord[] {
		return this.links;
	}

	/**
	 * Returns the current compact highlight collection.
	 * @returns Read-only timeline highlight records.
	 */
	public getHighlights(): readonly RenderHighlightRecord[] {
		return this.highlights;
	}

	/**
	 * Returns the current compact flag collection.
	 * @returns Read-only flagged-event records.
	 */
	public getFlags(): readonly RenderFlagRecord[] {
		return this.flags;
	}

	/**
	 * Returns the latest globally acknowledged data revision.
	 * @returns Highest committed global data revision.
	 */
	public getGlobalRevision(): number {
		return this.globalRevision;
	}

	/**
	 * Returns the highest fully processed data-message revision for ACK telemetry.
	 * @returns Highest revision accepted even when a snapshot chunk did not repaint.
	 */
	public getAcknowledgedGlobalRevision(): number {
		return this.acknowledgedGlobalRevision;
	}

	/**
	 * Returns the event revision currently committed for one source.
	 * @param sourceId Source whose authoritative worker revision is requested.
	 * @returns Committed event revision, or zero before a source snapshot exists.
	 */
	public getEventRevision(sourceId: string): number {
		return this.events.get(sourceId)?.revision ?? 0;
	}

	/**
	 * Applies source metadata, overlays, and staged event views from one data message.
	 * @param patch Revisioned render-only data patch.
	 * @param operationEpoch Envelope epoch used to reject cross-operation windows.
	 * @returns Commit flags plus any source-level resynchronization requests.
	 */
	public async applyDataPatch(
		patch: RenderDataPatch,
		operationEpoch?: number,
	): Promise<WorkerRepositoryApplyResult> {
		const result: WorkerRepositoryApplyResult = {
			committedEvents: false,
			committedSources: false,
			committedOverlays: false,
			resync: [],
		};
		if (this.disposed) return result;
		if (
			patch.renderWindow !== undefined &&
			!isRenderWindowProjection(patch.renderWindow)
		) {
			throw new Error("Malformed render-window projection");
		}
		for (const batch of patch.events ?? []) {
			this.validateEventBatch(batch);
		}
		const applyResetGeneration = this.resetGeneration;

		const changedSourceIds = new Set<string>();
		const previousSources = new Map<string, RenderSource | undefined>();
		const metadataAdvancedSourceIds = new Set<string>();
		if (patch.sources && patch.sources.revision > this.sourceRevision) {
			for (const source of patch.sources.records) {
				const current = this.sources.get(source.id);
				if (!current || source.revision >= current.revision) {
					previousSources.set(source.id, current);
					this.removedSourceIds.delete(source.id);
					this.sources.set(source.id, source);
					if (!current || source.revision > current.revision) {
						changedSourceIds.add(source.id);
					}
				}
			}
			for (const sourceId of patch.sources.removedSourceIds) {
				this.removedSourceIds.add(sourceId);
				this.eventStageGeneration.set(
					sourceId,
					(this.eventStageGeneration.get(sourceId) ?? 0) + 1,
				);
				this.sources.delete(sourceId);
				this.events.delete(sourceId);
				this.eventSnapshotStages.delete(sourceId);
			}
			this.sourceRevision = patch.sources.revision;
			result.committedSources = true;
			for (const sourceId of changedSourceIds) {
				const previousSource = previousSources.get(sourceId);
				const nextSource = this.sources.get(sourceId);
				if (
					previousSource &&
					nextSource &&
					this.advanceCompatibleSourceMetadata(previousSource, nextSource)
				) {
					metadataAdvancedSourceIds.add(sourceId);
					result.committedEvents = true;
				}
			}
		}

		if (patch.overlays) {
			result.committedOverlays = this.applyOverlayPatch(patch.overlays);
		}

		const snapshotSourceIds = new Set<string>();
		for (const snapshotRecord of patch.eventSnapshots ?? []) {
			const snapshotResult = await this.applyEventSnapshotRecord(snapshotRecord);
			snapshotSourceIds.add(snapshotRecord.sourceId);
			result.committedEvents =
				result.committedEvents || snapshotResult.committed;
			if (snapshotResult.resync) result.resync.push(snapshotResult.resync);
		}

		if (patch.events) {
			const eventSourceIds = new Set(
				patch.events.map((batch) => batch.sourceId),
			);
			const batchesBySource = new Map<string, RenderEventBatch[]>();
			for (const batch of patch.events) {
				const sourceBatches = batchesBySource.get(batch.sourceId) ?? [];
				sourceBatches.push(batch);
				batchesBySource.set(batch.sourceId, sourceBatches);
			}
			const eventResults = (
				await Promise.all(
					Array.from(batchesBySource.values()).map(async (sourceBatches) => {
						const sourceResults: Array<{
							committed: boolean;
							resync?: WorkerResyncRequest;
						}> = [];
						for (const batch of sourceBatches) {
							sourceResults.push(await this.stageEventBatch(batch));
						}
						return sourceResults;
					}),
				)
			).flat();
			for (const eventResult of eventResults) {
				result.committedEvents =
					result.committedEvents || eventResult.committed;
				if (eventResult.resync) result.resync.push(eventResult.resync);
			}
			for (const sourceId of changedSourceIds) {
				if (
					!eventSourceIds.has(sourceId) &&
					!snapshotSourceIds.has(sourceId) &&
					!metadataAdvancedSourceIds.has(sourceId)
				) {
					result.committedEvents =
						(await this.restageSourceColorMetadata(sourceId)) ||
						result.committedEvents;
				}
			}
		} else if (!patch.eventSnapshots) {
			for (const sourceId of changedSourceIds) {
				if (metadataAdvancedSourceIds.has(sourceId)) continue;
				result.committedEvents =
					(await this.restageSourceColorMetadata(sourceId)) ||
					result.committedEvents;
			}
		}

		if (patch.renderWindow) {
			result.committedEvents =
				(await this.applyRenderWindowProjection(
					patch.renderWindow,
					operationEpoch,
				)) || result.committedEvents;
		}

		if (
			this.disposed ||
			applyResetGeneration !== this.resetGeneration
		) {
			return {
				committedEvents: false,
				committedSources: false,
				committedOverlays: false,
				resync: [],
			};
		}
		this.acknowledgedGlobalRevision = Math.max(
			this.acknowledgedGlobalRevision,
			patch.globalRevision,
		);
		if (
			result.committedEvents ||
			result.committedSources ||
			result.committedOverlays
		) {
			this.globalRevision = Math.max(this.globalRevision, patch.globalRevision);
		}
		return result;
	}

	/**
	 * Stages every source in one viewport projection before publishing any of them.
	 * The transferred projection columns become the committed render view directly;
	 * only representative override indexes and aggregate suffix sums are allocated.
	 * @param projection Complete render-window replacement from the query worker.
	 * @param expectedOperationEpoch Data-envelope epoch, when supplied by the controller.
	 * @returns True when the complete projection and its explicit evictions committed.
	 */
	private async applyRenderWindowProjection(
		projection: RenderWindowProjection,
		expectedOperationEpoch?: number,
	): Promise<boolean> {
		if (
			!this.isRenderWindowProjectionFresh(
				projection,
				expectedOperationEpoch,
			)
		) {
			return false;
		}

		for (const sourceProjection of projection.sources) {
			if (!this.hasExactRenderWindowBuffers(sourceProjection)) {
				throw new Error(
					`Render window retains spare buffers for source ${sourceProjection.sourceId}`,
				);
			}
			if (
				this.removedSourceIds.has(sourceProjection.sourceId) ||
				!this.sources.has(sourceProjection.sourceId)
			) {
				throw new Error(
					`Render window references unknown source ${sourceProjection.sourceId}`,
				);
			}
			const current = this.events.get(sourceProjection.sourceId);
			if (
				current &&
				sourceProjection.sourceRevision < current.revision
			) {
				return false;
			}
		}

		const resetGeneration = this.resetGeneration;
		const isCancelled = (): boolean =>
			this.disposed ||
			resetGeneration !== this.resetGeneration ||
			!this.isRenderWindowProjectionFresh(
				projection,
				expectedOperationEpoch,
			);
		const stagedViews = new Map<string, WorkerRenderEventView>();
		for (const sourceProjection of projection.sources) {
			const view = await this.createRenderWindowEventView(
				sourceProjection,
				projection,
				isCancelled,
			);
			if (!view) return false;
			stagedViews.set(sourceProjection.sourceId, view);
		}
		if (isCancelled()) return false;

		for (const sourceId of projection.evictedSourceIds) {
			this.eventStageGeneration.set(
				sourceId,
				(this.eventStageGeneration.get(sourceId) ?? 0) + 1,
			);
			this.events.delete(sourceId);
			this.eventSnapshotStages.delete(sourceId);
		}
		for (const [sourceId, view] of stagedViews) {
			this.eventStageGeneration.set(
				sourceId,
				(this.eventStageGeneration.get(sourceId) ?? 0) + 1,
			);
			this.eventSnapshotStages.delete(sourceId);
			this.events.set(sourceId, view);
		}
		this.renderWindowOperationEpoch = projection.operationEpoch;
		this.renderWindowViewportRevision = projection.viewportRevision;
		this.renderWindowProjectionRevision = projection.projectionRevision;
		return true;
	}

	/**
	 * Checks that every adopted projection view spans its complete backing buffer.
	 * This preserves zero-copy ownership without retaining hidden sender capacity.
	 * @param sourceProjection Source projection whose columns will be retained.
	 * @returns True when every typed array has zero byte offset and exact capacity.
	 */
	private hasExactRenderWindowBuffers(
		sourceProjection: RenderWindowSourceProjection,
	): boolean {
		const columns: Array<Uint32Array | Float64Array> = [
			sourceProjection.representativeHandles,
			sourceProjection.representativeTimestamps,
			sourceProjection.representativeHashes,
			sourceProjection.heightTimestamps,
			sourceProjection.heightCounts,
			sourceProjection.graphTimestamps,
			sourceProjection.graphCounts,
		];
		return columns.every(
			(column) =>
				column.byteOffset === 0 &&
				column.byteLength === column.buffer.byteLength,
		);
	}

	/**
	 * Determines whether a render-window replacement is newer than committed state
	 * and still targets the currently known viewport.
	 * @param projection Candidate complete viewport projection.
	 * @param expectedOperationEpoch Data-envelope epoch, when available.
	 * @returns True when staging and committing the projection is still useful.
	 */
	private isRenderWindowProjectionFresh(
		projection: RenderWindowProjection,
		expectedOperationEpoch?: number,
	): boolean {
		if (
			expectedOperationEpoch !== undefined &&
			projection.operationEpoch !== expectedOperationEpoch
		) {
			return false;
		}
		if (
			this.viewportState &&
			projection.viewportRevision < this.viewportState.viewportRevision
		) {
			return false;
		}
		if (projection.operationEpoch !== this.renderWindowOperationEpoch) {
			return projection.operationEpoch > this.renderWindowOperationEpoch;
		}
		if (projection.viewportRevision !== this.renderWindowViewportRevision) {
			return projection.viewportRevision > this.renderWindowViewportRevision;
		}
		return (
			projection.projectionRevision > this.renderWindowProjectionRevision
		);
	}

	/**
	 * Creates a render-only LOD view while retaining the transferred data columns.
	 * No spare-capacity buffers or dense handle-to-position index are constructed.
	 * @param sourceProjection One source's viewport-resolution projection.
	 * @param projection Parent projection carrying operation and viewport revisions.
	 * @param isCancelled Callback that aborts stale cooperative preprocessing.
	 * @returns Prepared immutable event view, or null when superseded.
	 */
	private async createRenderWindowEventView(
		sourceProjection: RenderWindowSourceProjection,
		projection: RenderWindowProjection,
		isCancelled: StageCancellationCheck,
	): Promise<WorkerRenderEventView | null> {
		const source = this.sources.get(sourceProjection.sourceId);
		if (!source || isCancelled()) return null;
		const overrideHashes = new Set(
			source.overrides.map((override) => override.hash),
		);
		let overrideCount = 0;
		let sliceStartedAt = performance.now();
		for (
			let index = 0;
			index < sourceProjection.representativeHashes.length;
			index++
		) {
			if (overrideHashes.has(sourceProjection.representativeHashes[index])) {
				overrideCount++;
			}
			if (
				(index & STAGING_CLOCK_CHECK_MASK) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		const overrideIndexes = new Uint32Array(overrideCount);
		let overrideIndex = 0;
		sliceStartedAt = performance.now();
		for (
			let index = 0;
			index < sourceProjection.representativeHashes.length;
			index++
		) {
			if (overrideHashes.has(sourceProjection.representativeHashes[index])) {
				overrideIndexes[overrideIndex++] = index;
			}
			if (
				(index & STAGING_CLOCK_CHECK_MASK) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		const heightSuffixCounts = new Float64Array(
			sourceProjection.heightCounts.length,
		);
		if (
			!(await this.buildSuffixCounts(
				heightSuffixCounts,
				sourceProjection.heightCounts,
				0,
				sourceProjection.heightCounts.length,
				isCancelled,
			))
		) {
			return null;
		}
		const sampleSuffixCounts = new Float64Array(
			sourceProjection.graphCounts.length,
		);
		if (
			!(await this.buildSuffixCounts(
				sampleSuffixCounts,
				sourceProjection.graphCounts,
				0,
				sourceProjection.graphCounts.length,
				isCancelled,
			))
		) {
			return null;
		}
		if (isCancelled()) return null;

		return {
			revision: sourceProjection.sourceRevision,
			loadedCount: sourceProjection.loadedCount,
			frameEventCount: sourceProjection.frameEventCount,
			handles: sourceProjection.representativeHandles,
			timestamps: sourceProjection.representativeTimestamps,
			hashes: sourceProjection.representativeHashes,
			hashMin: sourceProjection.hashRange.min,
			hashMax: sourceProjection.hashRange.max,
			hashMinCount: 0,
			hashMaxCount: 0,
			handleIndexes: new Uint32Array(0),
			overrideIndexes,
			colorSourceRevision: source.revision,
			heightTimestamps: sourceProjection.heightTimestamps,
			heightCounts: sourceProjection.heightCounts,
			heightSuffixCounts,
			maxHeightCount: sourceProjection.heightMaximumCount,
			sampleTimestamps: sourceProjection.graphTimestamps,
			sampleCounts: sourceProjection.graphCounts,
			sampleSuffixCounts,
			samplesAreDerived: false,
			operationEpoch: projection.operationEpoch,
			viewportRevision: projection.viewportRevision,
			projectionRevision: projection.projectionRevision,
			coveredRange: sourceProjection.coveredRange,
			bucketPixelWidth: sourceProjection.bucketPixelWidth,
			degradedLod: sourceProjection.degradedLod,
		};
	}

	/**
	 * Applies one begin, chunk, or commit record for a source snapshot transaction.
	 * @param record Snapshot transaction record received on the serial data channel.
	 * @returns Commit status and optional source resynchronization request.
	 */
	private async applyEventSnapshotRecord(
		record: RenderEventSnapshotRecord,
	): Promise<{ committed: boolean; resync?: WorkerResyncRequest }> {
		if (record.kind === "begin") {
			if (!Number.isSafeInteger(record.totalEvents) || record.totalEvents < 0) {
				throw new Error(`Malformed snapshot size for source ${record.sourceId}`);
			}
			if (
				(record.sampleTimestamps && !record.sampleCounts) ||
				(!record.sampleTimestamps && record.sampleCounts) ||
				record.sampleTimestamps?.length !== record.sampleCounts?.length
			) {
				throw new Error(`Malformed snapshot samples for source ${record.sourceId}`);
			}
			const eventCapacity =
				record.totalEvents + this.getSpareCapacity(record.totalEvents);
			this.eventSnapshotStages.set(record.sourceId, {
				snapshotId: record.snapshotId,
				sourceId: record.sourceId,
				revision: record.revision,
				totalEvents: record.totalEvents,
				handles: new Uint32Array(eventCapacity),
				timestamps: new Float64Array(eventCapacity),
				hashes: new Float64Array(eventCapacity),
				sampleTimestamps: record.sampleTimestamps,
				sampleCounts: record.sampleCounts,
				receivedEvents: 0,
			});
			return { committed: false };
		}

		const stage = this.eventSnapshotStages.get(record.sourceId);
		if (!stage || stage.snapshotId !== record.snapshotId) {
			return {
				committed: false,
				resync: {
					scope: "source",
					id: record.sourceId,
					expectedRevision: this.events.get(record.sourceId)?.revision ?? 0,
					receivedRevision:
						record.kind === "commit" ? record.revision : 0,
				},
			};
		}

		if (record.kind === "chunk") {
			if (
				record.offset !== stage.receivedEvents ||
				record.handles.length !== record.timestamps.length ||
				record.timestamps.length !== record.hashes.length ||
				record.offset + record.handles.length > stage.totalEvents
			) {
				this.eventSnapshotStages.delete(record.sourceId);
				throw new Error(`Out-of-order snapshot chunk for source ${record.sourceId}`);
			}
			stage.handles.set(record.handles, record.offset);
			stage.timestamps.set(record.timestamps, record.offset);
			stage.hashes.set(record.hashes, record.offset);
			stage.receivedEvents += record.handles.length;
			return { committed: false };
		}

		this.eventSnapshotStages.delete(record.sourceId);
		if (
			record.revision !== stage.revision ||
			stage.receivedEvents !== stage.totalEvents
		) {
			return {
				committed: false,
				resync: {
					scope: "source",
					id: record.sourceId,
					expectedRevision: stage.revision,
					receivedRevision: record.revision,
				},
			};
		}

		return this.stageEventBatch(
			{
				sourceId: stage.sourceId,
				baseRevision: 0,
				revision: stage.revision,
				mode: "replace",
				handles: stage.handles.subarray(0, stage.totalEvents),
				timestamps: stage.timestamps.subarray(0, stage.totalEvents),
				hashes: stage.hashes.subarray(0, stage.totalEvents),
				sampleTimestamps: stage.sampleTimestamps,
				sampleCounts: stage.sampleCounts,
			},
			{
				handles: stage.handles,
				timestamps: stage.timestamps,
				hashes: stage.hashes,
			},
		);
	}

	/**
	 * Advances cache-neutral source metadata without rescanning committed events.
	 * Outward bounds are safe when their bucket origin remains aligned; palette,
	 * labels, totals, offsets, and loading changes are consumed directly by RenderCore.
	 * @param previousSource Previously committed source metadata.
	 * @param nextSource Newer source metadata.
	 * @returns True when the event view revision advanced without restaging.
	 */
	private advanceCompatibleSourceMetadata(
		previousSource: RenderSource,
		nextSource: RenderSource,
	): boolean {
		const current = this.events.get(nextSource.id);
		if (!current) return false;
		if (
			previousSource.engine !== nextSource.engine ||
			previousSource.frequency !== nextSource.frequency ||
			!this.haveEqualOverrideHashes(
				previousSource.overrides,
				nextSource.overrides,
			)
		) {
			return false;
		}
		const previousMinimum = Math.min(
			previousSource.timestamp.min,
			previousSource.timestamp.max,
		);
		const previousMaximum = Math.max(
			previousSource.timestamp.min,
			previousSource.timestamp.max,
		);
		const nextMinimum = Math.min(
			nextSource.timestamp.min,
			nextSource.timestamp.max,
		);
		const nextMaximum = Math.max(
			nextSource.timestamp.min,
			nextSource.timestamp.max,
		);
		if (nextMinimum > previousMinimum || nextMaximum < previousMaximum) {
			return false;
		}
		const minimumShift = previousMinimum - nextMinimum;
		if (
			minimumShift !== 0 &&
			(!this.isDurationAligned(minimumShift, HEIGHT_BUCKET_DURATION) ||
				(current.samplesAreDerived &&
					nextSource.engine === "graph" &&
					!this.isDurationAligned(minimumShift, nextSource.frequency)))
		) {
			return false;
		}

		const nextView: WorkerRenderEventView = {
			...current,
			colorSourceRevision: nextSource.revision,
		};
		const backingStore = this.eventBackingStores.get(current);
		if (backingStore) this.eventBackingStores.set(nextView, backingStore);
		this.events.set(nextSource.id, nextView);
		return true;
	}

	/**
	 * Compares override hash membership independently from display colors.
	 * @param previousOverrides Previous source overrides.
	 * @param nextOverrides New source overrides.
	 * @returns True when both revisions select the same event hashes.
	 */
	private haveEqualOverrideHashes(
		previousOverrides: readonly { hash: number }[],
		nextOverrides: readonly { hash: number }[],
	): boolean {
		if (previousOverrides.length !== nextOverrides.length) return false;
		const previousHashes = new Set(
			previousOverrides.map((override) => override.hash),
		);
		return nextOverrides.every((override) => previousHashes.has(override.hash));
	}

	/**
	 * Checks floating-point duration alignment for stable bucket origins.
	 * @param duration Non-negative origin shift.
	 * @param frequency Positive bucket duration.
	 * @returns True when the shift is an integer number of buckets.
	 */
	private isDurationAligned(duration: number, frequency: number): boolean {
		if (!Number.isFinite(frequency) || frequency <= 0) return false;
		const bucketCount = duration / frequency;
		return Math.abs(bucketCount - Math.round(bucketCount)) <= 1e-9;
	}

	/**
	 * Replaces only overlay collections whose revisions are newer than committed state.
	 * @param patch Independently revisioned overlay collections.
	 * @returns True when at least one overlay collection changed.
	 */
	private applyOverlayPatch(patch: RenderOverlayPatch): boolean {
		let changed = false;
		if (patch.notes && patch.notes.revision >= this.notesRevision) {
			this.notes = patch.notes.records;
			this.notesRevision = patch.notes.revision;
			changed = true;
		}
		if (patch.links && patch.links.revision >= this.linksRevision) {
			this.links = patch.links.records;
			this.linksRevision = patch.links.revision;
			changed = true;
		}
		if (
			patch.highlights &&
			patch.highlights.revision >= this.highlightsRevision
		) {
			this.highlights = patch.highlights.records;
			this.highlightsRevision = patch.highlights.revision;
			changed = true;
		}
		if (patch.flags && patch.flags.revision >= this.flagsRevision) {
			this.flags = patch.flags.records;
			this.flagsRevision = patch.flags.revision;
			changed = true;
		}
		return changed;
	}

	/**
	 * Stages one source mutation, precomputes render metadata, and atomically commits it.
	 * @param batch Transferable source-event mutation.
	 * @param adoptedEventArrays Optional snapshot receive allocations with bounded spare capacity.
	 * @returns Commit status and an optional revision-gap request.
	 */
	private async stageEventBatch(
		batch: RenderEventBatch,
		adoptedEventArrays?: AdoptedEventArrays,
	): Promise<{
		committed: boolean;
		resync?: WorkerResyncRequest;
	}> {
		if (
			this.removedSourceIds.has(batch.sourceId) ||
			!this.sources.has(batch.sourceId)
		) {
			return { committed: false };
		}
		const current = this.events.get(batch.sourceId);
		const currentRevision = current?.revision ?? 0;
		if (batch.mode === "upsert" && current?.operationEpoch !== undefined) {
			return {
				committed: false,
				resync: {
					scope: "source",
					id: batch.sourceId,
					expectedRevision: currentRevision,
					receivedRevision: batch.baseRevision,
				},
			};
		}
		if (
			current &&
			(batch.revision < currentRevision ||
				(batch.revision === currentRevision && batch.mode !== "replace"))
		) {
			return { committed: false };
		}
		if (batch.mode === "upsert" && batch.baseRevision !== currentRevision) {
			return {
				committed: false,
				resync: {
					scope: "source",
					id: batch.sourceId,
					expectedRevision: currentRevision,
					receivedRevision: batch.baseRevision,
				},
			};
		}

		this.validateEventBatch(batch);
		const stageGeneration =
			(this.eventStageGeneration.get(batch.sourceId) ?? 0) + 1;
		this.eventStageGeneration.set(batch.sourceId, stageGeneration);
		const resetGeneration = this.resetGeneration;
		const isCancelled = (): boolean =>
			this.isEventStageCancelled(
				batch.sourceId,
				stageGeneration,
				resetGeneration,
			);
		if (batch.mode === "upsert" && current) {
			const fastView = this.applyStableHashUpsert(batch, current);
			if (fastView) {
				this.events.set(batch.sourceId, fastView);
				return { committed: true };
			}
			const appendedView = await this.applyOlderEdgeUpsert(
				batch,
				current,
				isCancelled,
			);
			if (appendedView) {
				this.events.set(batch.sourceId, appendedView);
				return { committed: true };
			}
		}
		const arrays = await this.prepareEventArrays(
			batch,
			current,
			isCancelled,
		);
		if (!arrays) return { committed: false };
		let view = await this.precomputeEventView(
			batch.sourceId,
			batch.revision,
			arrays,
			undefined,
			isCancelled,
			undefined,
			adoptedEventArrays,
		);
		if (!view) return { committed: false };
		while (
			view.colorSourceRevision !==
			(this.sources.get(batch.sourceId)?.revision ?? 0)
		) {
			view = await this.precomputeEventView(
				batch.sourceId,
				batch.revision,
				arrays,
				undefined,
				isCancelled,
				undefined,
				adoptedEventArrays,
			);
			if (!view) return { committed: false };
		}

		if (
			resetGeneration !== this.resetGeneration ||
			stageGeneration !== this.eventStageGeneration.get(batch.sourceId)
		) {
			return { committed: false };
		}

		this.events.set(batch.sourceId, view);
		return { committed: true };
	}

	/**
	 * Checks whether reset, disposal, or a newer same-source stage superseded work.
	 * @param sourceId Source whose staged view is being prepared.
	 * @param stageGeneration Source-local generation captured by the stage.
	 * @param resetGeneration Operation generation captured by the stage.
	 * @returns True when preprocessing must stop without committing or reporting an error.
	 */
	private isEventStageCancelled(
		sourceId: string,
		stageGeneration: number,
		resetGeneration: number,
	): boolean {
		return (
			this.disposed ||
			this.removedSourceIds.has(sourceId) ||
			!this.sources.has(sourceId) ||
			resetGeneration !== this.resetGeneration ||
			stageGeneration !== this.eventStageGeneration.get(sourceId)
		);
	}

	/**
	 * Applies a small hash-only update in one atomic task using stable handle indexes.
	 * Timestamp-changing or range-invalidating batches fall back to cooperative merging.
	 * @param batch Stable-handle upsert batch.
	 * @param current Current complete source event view.
	 * @returns Updated view when the fast path is safe, otherwise null.
	 */
	private applyStableHashUpsert(
		batch: RenderEventBatch,
		current: WorkerRenderEventView,
	): WorkerRenderEventView | null {
		const source = this.sources.get(batch.sourceId);
		if (
			batch.sampleTimestamps !== undefined ||
			current.handleIndexes.length === 0 ||
			current.colorSourceRevision !== (source?.revision ?? 0)
		) {
			return null;
		}

		const eventIndexes = new Uint32Array(batch.handles.length);
		const seenHandles = new Set<number>();
		let removedMinimumCount = 0;
		let addedMinimumCount = 0;
		let removedMaximumCount = 0;
		let addedMaximumCount = 0;
		let lowerMinimum = current.hashMin;
		let lowerMinimumCount = 0;
		let upperMaximum = current.hashMax;
		let upperMaximumCount = 0;

		for (let index = 0; index < batch.handles.length; index++) {
			const handle = batch.handles[index];
			if (seenHandles.has(handle) || handle >= current.handleIndexes.length) {
				return null;
			}
			seenHandles.add(handle);
			const mappedIndex = current.handleIndexes[handle];
			if (mappedIndex === 0) return null;
			const eventIndex = mappedIndex - 1;
			if (current.timestamps[eventIndex] !== batch.timestamps[index]) return null;
			eventIndexes[index] = eventIndex;
			const previousHash = current.hashes[eventIndex];
			const nextHash = batch.hashes[index];
			if (previousHash === current.hashMin && nextHash !== current.hashMin) {
				removedMinimumCount++;
			}
			if (previousHash !== current.hashMin && nextHash === current.hashMin) {
				addedMinimumCount++;
			}
			if (previousHash === current.hashMax && nextHash !== current.hashMax) {
				removedMaximumCount++;
			}
			if (previousHash !== current.hashMax && nextHash === current.hashMax) {
				addedMaximumCount++;
			}
			if (nextHash < lowerMinimum) {
				lowerMinimum = nextHash;
				lowerMinimumCount = 1;
			} else if (nextHash === lowerMinimum && nextHash < current.hashMin) {
				lowerMinimumCount++;
			}
			if (nextHash > upperMaximum) {
				upperMaximum = nextHash;
				upperMaximumCount = 1;
			} else if (nextHash === upperMaximum && nextHash > current.hashMax) {
				upperMaximumCount++;
			}
		}

		const retainedMinimumCount =
			current.hashMinCount - removedMinimumCount + addedMinimumCount;
		const retainedMaximumCount =
			current.hashMaxCount - removedMaximumCount + addedMaximumCount;
		if (
			(lowerMinimum === current.hashMin && retainedMinimumCount <= 0) ||
			(upperMaximum === current.hashMax && retainedMaximumCount <= 0)
		) {
			return null;
		}

		const overrideHashes = new Set(
			(source?.overrides ?? []).map((override) => override.hash),
		);
		const overrideIndexes = new Set<number>(current.overrideIndexes);
		for (let index = 0; index < eventIndexes.length; index++) {
			const eventIndex = eventIndexes[index];
			const hash = batch.hashes[index];
			current.hashes[eventIndex] = hash;
			if (overrideHashes.has(hash)) overrideIndexes.add(eventIndex);
			else overrideIndexes.delete(eventIndex);
		}
		const sortedOverrideIndexes = Array.from(overrideIndexes).sort(
			(left, right) => left - right,
		);

		const view: WorkerRenderEventView = {
			...current,
			revision: batch.revision,
			hashMin: lowerMinimum,
			hashMax: upperMaximum,
			hashMinCount:
				lowerMinimum < current.hashMin
					? lowerMinimumCount
					: retainedMinimumCount,
			hashMaxCount:
				upperMaximum > current.hashMax
					? upperMaximumCount
					: retainedMaximumCount,
			overrideIndexes: Uint32Array.from(sortedOverrideIndexes),
			sampleTimestamps: current.sampleTimestamps,
			sampleCounts: current.sampleCounts,
			sampleSuffixCounts: current.sampleSuffixCounts,
			samplesAreDerived: current.samplesAreDerived,
		};
		const currentStore = this.eventBackingStores.get(current);
		if (currentStore) {
			this.eventBackingStores.set(view, {
				...currentStore,
				overrideIndexes: view.overrideIndexes,
				overrideLength: view.overrideIndexes.length,
			});
		}
		return view;
	}

	/**
	 * Appends a proven-new, strictly older page without rescanning or preprocessing
	 * the complete committed source. This is the normal descending-query pagination
	 * path used by large snapshots and benchmark bootstrap.
	 * @param batch Source-local upsert containing a timestamp-disjoint older page.
	 * @param current Current committed event view.
	 * @param isCancelled Callback that aborts superseded staging work.
	 * @returns Incrementally updated view, or null when the safe fast path cannot apply.
	 */
	private async applyOlderEdgeUpsert(
		batch: RenderEventBatch,
		current: WorkerRenderEventView,
		isCancelled: StageCancellationCheck,
	): Promise<WorkerRenderEventView | null> {
		const source = this.sources.get(batch.sourceId);
		const currentStore = this.eventBackingStores.get(current);
		if (
			!source ||
			!currentStore ||
			batch.sampleTimestamps !== undefined ||
			batch.handles.length === 0 ||
			current.handles.length === 0 ||
			current.handleIndexes.length === 0 ||
			current.colorSourceRevision !== source.revision
		) {
			return null;
		}

		const incomingIndexes = await this.createDescendingEventIndexes(
			batch,
			isCancelled,
		);
		if (!incomingIndexes) return null;
		const seenHandles = new Set<number>();
		let maximumHandle = current.handleIndexes.length - 1;
		let sliceStartedAt = performance.now();
		for (let offset = 0; offset < incomingIndexes.length; offset++) {
			const incomingIndex = incomingIndexes[offset];
			const handle = batch.handles[incomingIndex];
			if (
				seenHandles.has(handle) ||
				(handle < current.handleIndexes.length &&
					current.handleIndexes[handle] !== 0)
			) {
				return null;
			}
			seenHandles.add(handle);
			if (handle > maximumHandle) maximumHandle = handle;
			if (
				(offset & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		if (isCancelled()) return null;
		const newestIncomingTimestamp = batch.timestamps[incomingIndexes[0]];
		const oldestCurrentTimestamp =
			current.timestamps[current.timestamps.length - 1];
		if (
			!Number.isFinite(newestIncomingTimestamp) ||
			newestIncomingTimestamp >= oldestCurrentTimestamp
		) {
			return null;
		}

		const nextLength = current.handles.length + incomingIndexes.length;
		const maximumDenseHandle = nextLength * 2 + 1_024;
		if (maximumHandle > maximumDenseHandle) return null;

		let deltaHashMin = Infinity;
		let deltaHashMax = -Infinity;
		let deltaHashMinCount = 0;
		let deltaHashMaxCount = 0;
		const overrideHashes = new Set(
			(source.overrides ?? []).map((override) => override.hash),
		);
		const appendedOverrideIndexes: number[] = [];
		sliceStartedAt = performance.now();
		for (let offset = 0; offset < incomingIndexes.length; offset++) {
			const incomingIndex = incomingIndexes[offset];
			const outputIndex = current.handles.length + offset;
			const hash = batch.hashes[incomingIndex];
			if (hash < deltaHashMin) {
				deltaHashMin = hash;
				deltaHashMinCount = 1;
			} else if (hash === deltaHashMin) {
				deltaHashMinCount++;
			}
			if (hash > deltaHashMax) {
				deltaHashMax = hash;
				deltaHashMaxCount = 1;
			} else if (hash === deltaHashMax) {
				deltaHashMaxCount++;
			}
			if (overrideHashes.has(hash)) appendedOverrideIndexes.push(outputIndex);
			if (
				(offset & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		const sourceMinimum = Math.min(source.timestamp.min, source.timestamp.max);
		const sourceMaximum = Math.max(source.timestamp.min, source.timestamp.max);
		const heightDelta = await this.createIncrementalBuckets(
			Array.from(incomingIndexes, (index) => batch.timestamps[index]),
			sourceMinimum,
			sourceMaximum,
			sourceMinimum,
			HEIGHT_BUCKET_DURATION,
			isCancelled,
		);
		if (!heightDelta) return null;
		let graphDelta: IncrementalBucketView<Float64Array> | null = null;
		if (current.samplesAreDerived && source.engine === "graph") {
			const graphFrequency = Math.max(source.frequency, 0.001);
			if (Number.isFinite(graphFrequency)) {
				graphDelta = await this.createIncrementalBuckets(
					Array.from(
						incomingIndexes,
						(index) => batch.timestamps[index],
					),
					sourceMinimum,
					sourceMaximum,
					sourceMinimum,
					graphFrequency,
					isCancelled,
				);
				if (!graphDelta) return null;
			}
		}

		const heightSharesBoundary = this.sharesOldestBucketBoundary(
			heightDelta,
			current.heightTimestamps,
		);
		const graphSharesBoundary = graphDelta
			? this.sharesOldestBucketBoundary(graphDelta, current.sampleTimestamps)
			: false;
		const requiredHeightHeadroom =
			heightDelta.timestamps.length - (heightSharesBoundary ? 1 : 0);
		const requiredGraphHeadroom = graphDelta
			? graphDelta.timestamps.length - (graphSharesBoundary ? 1 : 0)
			: 0;
		if (
			nextLength > currentStore.handles.length ||
			maximumHandle >= currentStore.handleIndexes.length ||
			currentStore.overrideLength + appendedOverrideIndexes.length >
				currentStore.overrideIndexes.length ||
			requiredHeightHeadroom > currentStore.heightStart ||
			requiredGraphHeadroom > currentStore.sampleStart
		) {
			return null;
		}

		const nextStore: WorkerEventBackingStore = { ...currentStore };
		for (let offset = 0; offset < incomingIndexes.length; offset++) {
			const incomingIndex = incomingIndexes[offset];
			const outputIndex = currentStore.eventLength + offset;
			const handle = batch.handles[incomingIndex];
			nextStore.handles[outputIndex] = handle;
			nextStore.timestamps[outputIndex] = batch.timestamps[incomingIndex];
			nextStore.hashes[outputIndex] = batch.hashes[incomingIndex];
			nextStore.handleIndexes[handle] = outputIndex + 1;
		}
		nextStore.eventLength = nextLength;
		nextStore.handleIndexLength = Math.max(
			currentStore.handleIndexLength,
			maximumHandle + 1,
		);
		for (const eventIndex of appendedOverrideIndexes) {
			nextStore.overrideIndexes[nextStore.overrideLength++] = eventIndex;
		}

		const maxHeightCount = this.prependHeightBucketsInPlace(
			nextStore,
			heightDelta,
			current.maxHeightCount,
			heightSharesBoundary,
		);
		if (graphDelta) {
			this.prependSampleBucketsInPlace(
				nextStore,
				graphDelta,
				graphSharesBoundary,
			);
		}

		const hashMin = Math.min(current.hashMin, deltaHashMin);
		const hashMax = Math.max(current.hashMax, deltaHashMax);
		const view: WorkerRenderEventView = {
			revision: batch.revision,
			loadedCount: nextLength,
			handles: nextStore.handles.subarray(0, nextStore.eventLength),
			timestamps: nextStore.timestamps.subarray(0, nextStore.eventLength),
			hashes: nextStore.hashes.subarray(0, nextStore.eventLength),
			hashMin,
			hashMax,
			hashMinCount:
				deltaHashMin < current.hashMin
					? deltaHashMinCount
					: current.hashMinCount +
						(deltaHashMin === current.hashMin ? deltaHashMinCount : 0),
			hashMaxCount:
				deltaHashMax > current.hashMax
					? deltaHashMaxCount
					: current.hashMaxCount +
						(deltaHashMax === current.hashMax ? deltaHashMaxCount : 0),
			handleIndexes: nextStore.handleIndexes.subarray(
				0,
				nextStore.handleIndexLength,
			),
			overrideIndexes: nextStore.overrideIndexes.subarray(
				0,
				nextStore.overrideLength,
			),
			colorSourceRevision: source.revision,
			heightTimestamps: nextStore.heightTimestamps.subarray(
				nextStore.heightStart,
				nextStore.heightStart + nextStore.heightLength,
			),
			heightCounts: nextStore.heightCounts.subarray(
				nextStore.heightStart,
				nextStore.heightStart + nextStore.heightLength,
			),
			heightSuffixCounts: nextStore.heightSuffixCounts.subarray(
				nextStore.heightStart,
				nextStore.heightStart + nextStore.heightLength,
			),
			maxHeightCount,
			sampleTimestamps: nextStore.sampleTimestamps.subarray(
				nextStore.sampleStart,
				nextStore.sampleStart + nextStore.sampleLength,
			),
			sampleCounts: nextStore.sampleCounts.subarray(
				nextStore.sampleStart,
				nextStore.sampleStart + nextStore.sampleLength,
			),
			sampleSuffixCounts: nextStore.sampleSuffixCounts.subarray(
				nextStore.sampleStart,
				nextStore.sampleStart + nextStore.sampleLength,
			),
			samplesAreDerived: current.samplesAreDerived,
		};
		this.eventBackingStores.set(view, nextStore);
		return view;
	}

	/**
	 * Cooperatively orders a small incoming batch by descending timestamp and handle.
	 * @param batch Source-local event delta to order.
	 * @param isCancelled Callback that aborts superseded staging work.
	 * @returns Ordered input indexes, or null when staging was cancelled.
	 */
	private async createDescendingEventIndexes(
		batch: RenderEventBatch,
		isCancelled: StageCancellationCheck,
	): Promise<Uint32Array | null> {
		let indexes = Uint32Array.from(
			{ length: batch.handles.length },
			(_, index) => index,
		);
		let scratch = new Uint32Array(indexes.length);
		let sliceStartedAt = performance.now();
		for (let width = 1; width < indexes.length; width *= 2) {
			for (let runStart = 0; runStart < indexes.length; runStart += width * 2) {
				const leftEnd = Math.min(runStart + width, indexes.length);
				const rightEnd = Math.min(runStart + width * 2, indexes.length);
				let left = runStart;
				let right = leftEnd;
				let output = runStart;
				while (left < leftEnd || right < rightEnd) {
					const takeLeft =
						right >= rightEnd ||
						(left < leftEnd &&
							this.isEventIndexBefore(indexes[left], indexes[right], batch));
					scratch[output++] = takeLeft ? indexes[left++] : indexes[right++];
					if (
						(output & 4_095) === 0 &&
						performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
					) {
						await this.yieldScheduler.yield();
						if (isCancelled()) return null;
						sliceStartedAt = performance.now();
					}
				}
			}
			[indexes, scratch] = [scratch, indexes];
		}
		return isCancelled() ? null : indexes;
	}

	/**
	 * Compares two batch indexes using the repository's canonical event ordering.
	 * @param left Left batch index.
	 * @param right Right batch index.
	 * @param batch Batch containing timestamps and stable handles.
	 * @returns True when the left record precedes the right record.
	 */
	private isEventIndexBefore(
		left: number,
		right: number,
		batch: RenderEventBatch,
	): boolean {
		const timestampDifference = batch.timestamps[left] - batch.timestamps[right];
		return timestampDifference !== 0
			? timestampDifference > 0
			: batch.handles[left] <= batch.handles[right];
	}

	/**
	 * Cooperatively normalizes authoritative graph samples into ascending time order.
	 * @param timestamps Incoming sample timestamps.
	 * @param counts Counts aligned with incoming timestamps.
	 * @param isCancelled Callback that aborts superseded staging work.
	 * @returns Ascending sample arrays, or null when staging was cancelled.
	 */
	private async ensureAscendingSamples(
		timestamps: Float64Array,
		counts: Float64Array,
		isCancelled: StageCancellationCheck,
	): Promise<{ timestamps: Float64Array; counts: Float64Array } | null> {
		let sliceStartedAt = performance.now();
		let isAscending = true;
		for (let index = 1; index < timestamps.length; index++) {
			if (timestamps[index] < timestamps[index - 1]) isAscending = false;
			if (
				(index & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		if (isAscending) return { timestamps, counts };

		let indexes = Uint32Array.from(
			{ length: timestamps.length },
			(_, index) => index,
		);
		let scratch = new Uint32Array(indexes.length);
		for (let width = 1; width < indexes.length; width *= 2) {
			for (let runStart = 0; runStart < indexes.length; runStart += width * 2) {
				const leftEnd = Math.min(runStart + width, indexes.length);
				const rightEnd = Math.min(runStart + width * 2, indexes.length);
				let left = runStart;
				let right = leftEnd;
				let output = runStart;
				while (left < leftEnd || right < rightEnd) {
					const takeLeft =
						right >= rightEnd ||
						(left < leftEnd &&
							(timestamps[indexes[left]] < timestamps[indexes[right]] ||
								(timestamps[indexes[left]] === timestamps[indexes[right]] &&
									indexes[left] <= indexes[right])));
					scratch[output++] = takeLeft ? indexes[left++] : indexes[right++];
					if (
						(output & 4_095) === 0 &&
						performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
					) {
						await this.yieldScheduler.yield();
						if (isCancelled()) return null;
						sliceStartedAt = performance.now();
					}
				}
			}
			[indexes, scratch] = [scratch, indexes];
		}
		const sortedTimestamps = new Float64Array(timestamps.length);
		const sortedCounts = new Float64Array(counts.length);
		for (let index = 0; index < indexes.length; index++) {
			sortedTimestamps[index] = timestamps[indexes[index]];
			sortedCounts[index] = counts[indexes[index]];
			if (
				(index & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		return { timestamps: sortedTimestamps, counts: sortedCounts };
	}

	/**
	 * Detects the one bucket shared by an older delta and the committed bucket view.
	 * @param delta Ascending older bucket delta.
	 * @param currentTimestamps Ascending committed bucket timestamps.
	 * @returns True when the delta's newest bucket equals the committed oldest bucket.
	 */
	private sharesOldestBucketBoundary(
		delta: IncrementalBucketView<Float64Array>,
		currentTimestamps: Float64Array,
	): boolean {
		return (
			delta.timestamps.length > 0 &&
			currentTimestamps.length > 0 &&
			delta.timestamps[delta.timestamps.length - 1] === currentTimestamps[0]
		);
	}

	/**
	 * Prepends height buckets inside reserved headroom and updates suffix sums in O(delta).
	 * @param store Spare-capacity storage cloned for the next logical event view.
	 * @param delta Ascending older height buckets.
	 * @param currentMaximum Maximum committed height count.
	 * @param sharesBoundary Whether the newest delta bucket overlaps the oldest committed bucket.
	 * @returns Updated maximum bucket count.
	 */
	private prependHeightBucketsInPlace(
		store: WorkerEventBackingStore,
		delta: IncrementalBucketView<Float64Array>,
		currentMaximum: number,
		sharesBoundary: boolean,
	): number {
		let writeLength = delta.timestamps.length;
		let maximum = Math.max(currentMaximum, delta.maximum);
		if (sharesBoundary) {
			writeLength--;
			const boundaryDelta = delta.counts[delta.counts.length - 1];
			store.heightCounts[store.heightStart] += boundaryDelta;
			store.heightSuffixCounts[store.heightStart] += boundaryDelta;
			maximum = Math.max(maximum, store.heightCounts[store.heightStart]);
		}
		const nextStart = store.heightStart - writeLength;
		let suffix =
			store.heightLength > 0
				? store.heightSuffixCounts[store.heightStart]
				: 0;
		for (let index = writeLength - 1; index >= 0; index--) {
			const outputIndex = nextStart + index;
			const count = delta.counts[index];
			store.heightTimestamps[outputIndex] = delta.timestamps[index];
			store.heightCounts[outputIndex] = count;
			suffix += count;
			store.heightSuffixCounts[outputIndex] = suffix;
		}
		store.heightStart = nextStart;
		store.heightLength += writeLength;
		return maximum;
	}

	/**
	 * Prepends graph samples inside reserved headroom and updates suffix sums in O(delta).
	 * @param store Spare-capacity storage cloned for the next logical event view.
	 * @param delta Ascending older graph buckets.
	 * @param sharesBoundary Whether the newest delta bucket overlaps the oldest committed bucket.
	 * @returns Nothing.
	 */
	private prependSampleBucketsInPlace(
		store: WorkerEventBackingStore,
		delta: IncrementalBucketView<Float64Array>,
		sharesBoundary: boolean,
	): void {
		let writeLength = delta.timestamps.length;
		if (sharesBoundary) {
			writeLength--;
			const boundaryDelta = delta.counts[delta.counts.length - 1];
			store.sampleCounts[store.sampleStart] += boundaryDelta;
			store.sampleSuffixCounts[store.sampleStart] += boundaryDelta;
		}
		const nextStart = store.sampleStart - writeLength;
		let suffix =
			store.sampleLength > 0
				? store.sampleSuffixCounts[store.sampleStart]
				: 0;
		for (let index = writeLength - 1; index >= 0; index--) {
			const outputIndex = nextStart + index;
			const count = delta.counts[index];
			store.sampleTimestamps[outputIndex] = delta.timestamps[index];
			store.sampleCounts[outputIndex] = count;
			suffix += count;
			store.sampleSuffixCounts[outputIndex] = suffix;
		}
		store.sampleStart = nextStart;
		store.sampleLength += writeLength;
	}

	/**
	 * Aggregates one small timestamp delta into sorted source-relative buckets.
	 * @param timestamps Incoming raw event timestamps in descending order.
	 * @param sourceMinimum Inclusive source lower bound.
	 * @param sourceMaximum Inclusive source upper bound.
	 * @param bucketOrigin Timestamp used as bucket zero.
	 * @param bucketDuration Positive bucket width.
	 * @param isCancelled Callback that aborts superseded staging work.
	 * @returns Sorted bucket timestamps and counts, or null after cancellation.
	 */
	private async createIncrementalBuckets(
		timestamps: readonly number[],
		sourceMinimum: number,
		sourceMaximum: number,
		bucketOrigin: number,
		bucketDuration: number,
		isCancelled: StageCancellationCheck,
	): Promise<IncrementalBucketView<Float64Array> | null> {
		const buckets = new Map<number, number>();
		let maximum = 0;
		let sliceStartedAt = performance.now();
		for (let index = 0; index < timestamps.length; index++) {
			const timestamp = timestamps[index];
			if (timestamp >= sourceMinimum && timestamp <= sourceMaximum) {
				const bucketTimestamp =
					bucketOrigin +
					Math.floor((timestamp - bucketOrigin) / bucketDuration) *
						bucketDuration;
				const count = (buckets.get(bucketTimestamp) ?? 0) + 1;
				buckets.set(bucketTimestamp, count);
				if (count > maximum) maximum = count;
			}
			if (
				(index & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		const bucketTimestamps = new Float64Array(buckets.size);
		const bucketCounts = new Float64Array(buckets.size);
		let outputIndex = buckets.size - 1;
		for (const [timestamp, count] of buckets) {
			bucketTimestamps[outputIndex] = timestamp;
			bucketCounts[outputIndex] = count;
			outputIndex--;
			if (
				(outputIndex & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		return { timestamps: bucketTimestamps, counts: bucketCounts, maximum };
	}

	/**
	 * Prepends older height buckets and combines the single possible boundary bucket.
	 * @param currentTimestamps Existing ascending height-bucket timestamps.
	 * @param currentCounts Existing height-bucket counts.
	 * @param currentMaximum Existing maximum bucket count.
	 * @param delta Older ascending delta buckets.
	 * @returns Updated Uint32 height bucket view.
	 */
	private prependUint32Buckets(
		currentTimestamps: Float64Array,
		currentCounts: Uint32Array,
		currentMaximum: number,
		delta: IncrementalBucketView<Float64Array>,
	): IncrementalBucketView<Uint32Array> {
		const merged = this.prependFloat64Buckets(
			currentTimestamps,
			Float64Array.from(currentCounts),
			delta,
		);
		const counts = Uint32Array.from(merged.counts);
		return {
			timestamps: merged.timestamps,
			counts,
			maximum: Math.max(currentMaximum, merged.maximum),
		};
	}

	/**
	 * Prepends older numeric buckets and combines the single possible boundary bucket.
	 * @param currentTimestamps Existing ascending bucket timestamps.
	 * @param currentCounts Existing bucket counts.
	 * @param delta Older ascending delta buckets.
	 * @returns Updated Float64 bucket view.
	 */
	private prependFloat64Buckets(
		currentTimestamps: Float64Array,
		currentCounts: Float64Array,
		delta: IncrementalBucketView<Float64Array>,
	): IncrementalBucketView<Float64Array> {
		if (delta.timestamps.length === 0) {
			return {
				timestamps: currentTimestamps,
				counts: currentCounts,
				maximum: this.getMaximumBucketCount(currentCounts),
			};
		}
		if (currentTimestamps.length === 0) return delta;
		const sharesBoundary =
			delta.timestamps[delta.timestamps.length - 1] === currentTimestamps[0];
		const nextLength =
			currentTimestamps.length + delta.timestamps.length -
			(sharesBoundary ? 1 : 0);
		const timestamps = new Float64Array(nextLength);
		const counts = new Float64Array(nextLength);
		if (!sharesBoundary) {
			timestamps.set(delta.timestamps);
			counts.set(delta.counts);
			timestamps.set(currentTimestamps, delta.timestamps.length);
			counts.set(currentCounts, delta.counts.length);
		} else {
			timestamps.set(delta.timestamps);
			counts.set(delta.counts);
			const boundaryIndex = delta.timestamps.length - 1;
			counts[boundaryIndex] += currentCounts[0];
			timestamps.set(currentTimestamps.subarray(1), delta.timestamps.length);
			counts.set(currentCounts.subarray(1), delta.counts.length);
		}
		return {
			timestamps,
			counts,
			maximum: this.getMaximumBucketCount(counts),
		};
	}

	/**
	 * Finds the maximum count in a compact bucket array.
	 * @param counts Bucket counts to inspect.
	 * @returns Largest finite count, or zero for an empty array.
	 */
	private getMaximumBucketCount(counts: Float64Array): number {
		let maximum = 0;
		for (let index = 0; index < counts.length; index++) {
			if (counts[index] > maximum) maximum = counts[index];
		}
		return maximum;
	}

	/**
	 * Calculates geometric spare capacity for append-heavy worker-local buffers.
	 * @param logicalLength Number of currently committed elements.
	 * @returns Capacity that amortizes repeated small edge upserts.
	 */
	private getSpareCapacity(logicalLength: number): number {
		return Math.max(
			MINIMUM_SPARE_CAPACITY,
			Math.ceil(logicalLength / 8),
		);
	}

	/**
	 * Copies one typed array into reserved storage while yielding every four milliseconds.
	 * @param target Destination typed array.
	 * @param source Logical source values.
	 * @param targetOffset Destination element offset.
	 * @param isCancelled Callback that aborts superseded staging.
	 * @returns True after a complete copy, or false after cancellation.
	 */
	private async copyTypedArrayCooperatively(
		target: Uint32Array | Float64Array,
		source: Uint32Array | Float64Array,
		targetOffset: number,
		isCancelled: StageCancellationCheck,
	): Promise<boolean> {
		const chunkLength = 65_536;
		let sliceStartedAt = performance.now();
		for (let start = 0; start < source.length; start += chunkLength) {
			const end = Math.min(source.length, start + chunkLength);
			target.set(source.subarray(start, end), targetOffset + start);
			if (performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return false;
				sliceStartedAt = performance.now();
			}
		}
		return !isCancelled();
	}

	/**
	 * Creates spare-capacity storage and suffix-sum indexes before atomic commit.
	 * @param arrays Complete event buffers.
	 * @param handleIndexes Dense stable-handle lookup.
	 * @param overrideIndexes Sorted color-override event indexes.
	 * @param heightTimestamps Ascending height-bucket timestamps.
	 * @param heightCounts Height-bucket counts.
	 * @param sampleTimestamps Ascending graph sample timestamps.
	 * @param sampleCounts Graph sample counts.
	 * @param isCancelled Callback that aborts superseded staging.
	 * @param reusableStore Optional existing event-array storage for metadata-only restaging.
	 * @param adoptedEventArrays Optional snapshot receive allocations to commit without recopying.
	 * @returns Prepared backing storage, or null after cancellation.
	 */
	private async createEventBackingStore(
		arrays: PreparedEventArrays,
		handleIndexes: Uint32Array,
		overrideIndexes: Uint32Array,
		heightTimestamps: Float64Array,
		heightCounts: Uint32Array,
		sampleTimestamps: Float64Array,
		sampleCounts: Float64Array,
		isCancelled: StageCancellationCheck,
		reusableStore?: WorkerEventBackingStore,
		adoptedEventArrays?: AdoptedEventArrays,
	): Promise<WorkerEventBackingStore | null> {
		let handles: Uint32Array;
		let timestamps: Float64Array;
		let hashes: Float64Array;
		let handleIndexStore: Uint32Array;
		if (reusableStore) {
			handles = reusableStore.handles;
			timestamps = reusableStore.timestamps;
			hashes = reusableStore.hashes;
			handleIndexStore = reusableStore.handleIndexes;
		} else {
			if (adoptedEventArrays) {
				handles = adoptedEventArrays.handles;
				timestamps = adoptedEventArrays.timestamps;
				hashes = adoptedEventArrays.hashes;
			} else {
				const eventCapacity =
					arrays.handles.length + this.getSpareCapacity(arrays.handles.length);
				handles = new Uint32Array(eventCapacity);
				timestamps = new Float64Array(eventCapacity);
				hashes = new Float64Array(eventCapacity);
				if (
					!(await this.copyTypedArrayCooperatively(
						handles,
						arrays.handles,
						0,
						isCancelled,
					)) ||
					!(await this.copyTypedArrayCooperatively(
						timestamps,
						arrays.timestamps,
						0,
						isCancelled,
					)) ||
					!(await this.copyTypedArrayCooperatively(
						hashes,
						arrays.hashes,
						0,
						isCancelled,
					))
				) {
					return null;
				}
			}

			const handleIndexCapacity =
				handleIndexes.length === 0
					? 0
					: handleIndexes.length + this.getSpareCapacity(handleIndexes.length);
			handleIndexStore = new Uint32Array(handleIndexCapacity);
			if (
				!(await this.copyTypedArrayCooperatively(
					handleIndexStore,
					handleIndexes,
					0,
					isCancelled,
				))
			) {
				return null;
			}
		}
		const overrideCapacity =
			overrideIndexes.length + this.getSpareCapacity(overrideIndexes.length);
		const overrideStore = new Uint32Array(overrideCapacity);
		if (
			!(await this.copyTypedArrayCooperatively(
				overrideStore,
				overrideIndexes,
				0,
				isCancelled,
			))
		) {
			return null;
		}

		const heightHeadroom = this.getSpareCapacity(heightTimestamps.length);
		const heightCapacity = heightHeadroom + heightTimestamps.length;
		const heightTimestampStore = new Float64Array(heightCapacity);
		const heightCountStore = new Uint32Array(heightCapacity);
		const heightSuffixStore = new Float64Array(heightCapacity);
		if (
			!(await this.copyTypedArrayCooperatively(
				heightTimestampStore,
				heightTimestamps,
				heightHeadroom,
				isCancelled,
			)) ||
			!(await this.copyTypedArrayCooperatively(
				heightCountStore,
				heightCounts,
				heightHeadroom,
				isCancelled,
			))
		) {
			return null;
		}
		if (
			!(await this.buildSuffixCounts(
				heightSuffixStore,
				heightCountStore,
				heightHeadroom,
				heightTimestamps.length,
				isCancelled,
			))
		) {
			return null;
		}

		const sampleHeadroom = this.getSpareCapacity(sampleTimestamps.length);
		const sampleCapacity = sampleHeadroom + sampleTimestamps.length;
		const sampleTimestampStore = new Float64Array(sampleCapacity);
		const sampleCountStore = new Float64Array(sampleCapacity);
		const sampleSuffixStore = new Float64Array(sampleCapacity);
		if (
			!(await this.copyTypedArrayCooperatively(
				sampleTimestampStore,
				sampleTimestamps,
				sampleHeadroom,
				isCancelled,
			)) ||
			!(await this.copyTypedArrayCooperatively(
				sampleCountStore,
				sampleCounts,
				sampleHeadroom,
				isCancelled,
			)) ||
			!(await this.buildSuffixCounts(
				sampleSuffixStore,
				sampleCountStore,
				sampleHeadroom,
				sampleTimestamps.length,
				isCancelled,
			))
		) {
			return null;
		}

		return {
			handles,
			timestamps,
			hashes,
			eventLength: arrays.handles.length,
			handleIndexes: handleIndexStore,
			handleIndexLength: handleIndexes.length,
			overrideIndexes: overrideStore,
			overrideLength: overrideIndexes.length,
			heightTimestamps: heightTimestampStore,
			heightCounts: heightCountStore,
			heightSuffixCounts: heightSuffixStore,
			heightStart: heightHeadroom,
			heightLength: heightTimestamps.length,
			sampleTimestamps: sampleTimestampStore,
			sampleCounts: sampleCountStore,
			sampleSuffixCounts: sampleSuffixStore,
			sampleStart: sampleHeadroom,
			sampleLength: sampleTimestamps.length,
		};
	}

	/**
	 * Builds suffix sums aligned with a logical typed-array range cooperatively.
	 * @param suffix Destination suffix-count buffer.
	 * @param counts Count buffer sharing the logical range.
	 * @param start Logical range start in both buffers.
	 * @param length Logical range length.
	 * @param isCancelled Callback that aborts superseded staging.
	 * @returns True after completion, or false after cancellation.
	 */
	private async buildSuffixCounts(
		suffix: Float64Array,
		counts: Uint32Array | Float64Array,
		start: number,
		length: number,
		isCancelled: StageCancellationCheck,
	): Promise<boolean> {
		let runningCount = 0;
		let sliceStartedAt = performance.now();
		for (let index = length - 1; index >= 0; index--) {
			runningCount += counts[start + index];
			suffix[start + index] = runningCount;
			if (
				(index & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return false;
				sliceStartedAt = performance.now();
			}
		}
		return !isCancelled();
	}

	/**
	 * Ensures all correlated typed arrays have compatible lengths.
	 * @param batch Event batch to validate before worker hot-loop access.
	 * @returns Nothing.
	 * @throws Error when event or sample buffers are malformed.
	 */
	private validateEventBatch(batch: RenderEventBatch): void {
		if (
			!(batch.handles instanceof Uint32Array) ||
			!(batch.timestamps instanceof Float64Array) ||
			!(batch.hashes instanceof Float64Array) ||
			batch.handles.length !== batch.timestamps.length ||
			batch.timestamps.length !== batch.hashes.length
		) {
			throw new Error(`Malformed event buffers for source ${batch.sourceId}`);
		}
		if (
			(batch.sampleTimestamps !== undefined &&
				!(batch.sampleTimestamps instanceof Float64Array)) ||
			(batch.sampleCounts !== undefined &&
				!(batch.sampleCounts instanceof Float64Array)) ||
			(batch.sampleTimestamps && !batch.sampleCounts) ||
			(!batch.sampleTimestamps && batch.sampleCounts) ||
			batch.sampleTimestamps?.length !== batch.sampleCounts?.length
		) {
			throw new Error(`Malformed sample buffers for source ${batch.sourceId}`);
		}
	}

	/**
	 * Resolves replacement, clear, or stable-handle upsert buffers for staging.
	 * @param batch Incoming event batch.
	 * @param current Previously committed source view.
	 * @param isCancelled Callback that detects intentionally superseded staging work.
	 * @returns Complete next-source arrays, or null when staging was cancelled.
	 */
	private async prepareEventArrays(
		batch: RenderEventBatch,
		current: WorkerRenderEventView | undefined,
		isCancelled: StageCancellationCheck,
	): Promise<PreparedEventArrays | null> {
		if (isCancelled()) return null;
		if (batch.mode === "clear") {
			return {
				handles: new Uint32Array(0),
				timestamps: new Float64Array(0),
				hashes: new Float64Array(0),
				sampleTimestamps: new Float64Array(0),
				sampleCounts: new Float64Array(0),
				samplesAreDerived: true,
			};
		}

		if (batch.mode === "replace" || !current) {
			return {
				handles: batch.handles,
				timestamps: batch.timestamps,
				hashes: batch.hashes,
				sampleTimestamps:
					batch.sampleTimestamps ?? new Float64Array(0),
				sampleCounts: batch.sampleCounts ?? new Float64Array(0),
				samplesAreDerived: (batch.sampleTimestamps?.length ?? 0) === 0,
			};
		}

		return this.mergeEventUpsert(current, batch, isCancelled);
	}

	/**
	 * Merges a small stable-handle update into a descending timestamp event view.
	 * @param current Current complete source view.
	 * @param batch Incoming changed or newly inserted events.
	 * @param isCancelled Callback that detects intentionally superseded staging work.
	 * @returns Descending merged arrays, or null when staging was cancelled.
	 */
	private async mergeEventUpsert(
		current: WorkerRenderEventView,
		batch: RenderEventBatch,
		isCancelled: StageCancellationCheck,
	): Promise<PreparedEventArrays | null> {
		const updates = new Map<number, { timestamp: number; hash: number }>();
		let sliceStartedAt = performance.now();
		for (let index = 0; index < batch.handles.length; index++) {
			updates.set(batch.handles[index], {
				timestamp: batch.timestamps[index],
				hash: batch.hashes[index],
			});
			if (
				(index & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		let retainedCount = 0;
		sliceStartedAt = performance.now();
		for (let index = 0; index < current.handles.length; index++) {
			if (!updates.has(current.handles[index])) retainedCount++;
			if (performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}
		if (isCancelled()) return null;

		const incomingRecords: Array<{
			handle: number;
			timestamp: number;
			hash: number;
		}> = [];
		for (const [handle, event] of updates) {
			incomingRecords.push({ handle, ...event });
		}
		incomingRecords.sort((left, right) => {
			const timestampDifference = right.timestamp - left.timestamp;
			return timestampDifference || left.handle - right.handle;
		});
		if (isCancelled()) return null;

		const mergedLength = retainedCount + incomingRecords.length;
		const handles = new Uint32Array(mergedLength);
		const timestamps = new Float64Array(mergedLength);
		const hashes = new Float64Array(mergedLength);
		let currentIndex = 0;
		let incomingIndex = 0;
		let outputIndex = 0;
		sliceStartedAt = performance.now();
		while (outputIndex < mergedLength) {
			while (
				currentIndex < current.handles.length &&
				updates.has(current.handles[currentIndex])
			) {
				currentIndex++;
			}
			const incoming = incomingRecords[incomingIndex];
			const hasCurrent = currentIndex < current.handles.length;
			const takeIncoming =
				Boolean(incoming) &&
				(!hasCurrent ||
					incoming.timestamp > current.timestamps[currentIndex] ||
					(incoming.timestamp === current.timestamps[currentIndex] &&
						incoming.handle < current.handles[currentIndex]));

			if (takeIncoming && incoming) {
				handles[outputIndex] = incoming.handle;
				timestamps[outputIndex] = incoming.timestamp;
				hashes[outputIndex] = incoming.hash;
				incomingIndex++;
			} else if (hasCurrent) {
				handles[outputIndex] = current.handles[currentIndex];
				timestamps[outputIndex] = current.timestamps[currentIndex];
				hashes[outputIndex] = current.hashes[currentIndex];
				currentIndex++;
			}
			outputIndex++;
			if (
				(outputIndex & 4_095) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		return {
			handles,
			timestamps,
			hashes,
			sampleTimestamps:
				batch.sampleTimestamps ?? current.sampleTimestamps,
			sampleCounts: batch.sampleCounts ?? current.sampleCounts,
			samplesAreDerived:
				batch.sampleTimestamps === undefined
					? current.samplesAreDerived
					: batch.sampleTimestamps.length === 0,
		};
	}

	/**
	 * Precomputes hash ranges, height buckets, and graph fallback samples in
	 * cooperative time slices.
	 * @param sourceId Source whose settings define bucket anchoring and overrides.
	 * @param revision Source-local revision assigned to the resulting view.
	 * @param arrays Complete event and sample arrays to preprocess.
	 * @param existingHandleIndexes Stable handle index reused for settings-only restaging.
	 * @param isCancelled Callback that detects intentionally superseded staging work.
	 * @param reusableStore Existing event-array storage reused during metadata-only restaging.
	 * @param adoptedEventArrays Optional snapshot receive allocations to commit directly.
	 * @returns Immutable view metadata, or null when staging was cancelled.
	 */
	private async precomputeEventView(
		sourceId: string,
		revision: number,
		arrays: PreparedEventArrays,
		existingHandleIndexes?: Uint32Array,
		isCancelled: StageCancellationCheck = () => false,
		reusableStore?: WorkerEventBackingStore,
		adoptedEventArrays?: AdoptedEventArrays,
	): Promise<WorkerRenderEventView | null> {
		if (isCancelled()) return null;
		let hashMin = Infinity;
		let hashMax = -Infinity;
		let hashMinCount = 0;
		let hashMaxCount = 0;
		let maximumHandle = 0;
		let maxHeightCount = 0;
		const heightBuckets = new Map<number, number>();
		const source = this.sources.get(sourceId);
		const sourceStart = source?.timestamp.min ?? 0;
		const graphFrequency = Math.max(source?.frequency ?? 0, 0.001);
		const deriveGraphSamples =
			arrays.samplesAreDerived &&
			source?.engine === "graph" &&
			Number.isFinite(graphFrequency);
		const graphBuckets = new Map<number, number>();
		const sourceMinimum = Math.min(
			source?.timestamp.min ?? sourceStart,
			source?.timestamp.max ?? sourceStart,
		);
		const sourceMaximum = Math.max(
			source?.timestamp.min ?? sourceStart,
			source?.timestamp.max ?? sourceStart,
		);
		const overrideHashes = new Set(
			(source?.overrides ?? []).map((override) => override.hash),
		);
		const overrideIndexes: number[] = [];
		let sliceStartedAt = performance.now();

		for (let index = 0; index < arrays.timestamps.length; index++) {
			const shouldCheckClock = (index & STAGING_CLOCK_CHECK_MASK) === 0;
			const hash = arrays.hashes[index];
			if (hash < hashMin) {
				hashMin = hash;
				hashMinCount = 1;
			} else if (hash === hashMin) {
				hashMinCount++;
			}
			if (hash > hashMax) {
				hashMax = hash;
				hashMaxCount = 1;
			} else if (hash === hashMax) {
				hashMaxCount++;
			}
			if (arrays.handles[index] > maximumHandle) {
				maximumHandle = arrays.handles[index];
			}
			if (overrideHashes.has(hash)) overrideIndexes.push(index);
			const timestamp = arrays.timestamps[index];
			if (timestamp < sourceMinimum || timestamp > sourceMaximum) {
				if (
					shouldCheckClock &&
					performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
				) {
					await this.yieldScheduler.yield();
					if (isCancelled()) return null;
					sliceStartedAt = performance.now();
				}
				continue;
			}
			const bucketTimestamp =
				sourceStart +
				Math.floor((timestamp - sourceStart) / HEIGHT_BUCKET_DURATION) *
					HEIGHT_BUCKET_DURATION;
			const count = (heightBuckets.get(bucketTimestamp) ?? 0) + 1;
			heightBuckets.set(bucketTimestamp, count);
			if (count > maxHeightCount) maxHeightCount = count;
			if (deriveGraphSamples) {
				const graphTimestamp =
					sourceStart +
					Math.floor((timestamp - sourceStart) / graphFrequency) *
						graphFrequency;
				graphBuckets.set(
					graphTimestamp,
					(graphBuckets.get(graphTimestamp) ?? 0) + 1,
				);
			}

			if (
				shouldCheckClock &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		if (arrays.timestamps.length === 0) {
			hashMin = 0;
			hashMax = 0;
			hashMinCount = 0;
			hashMaxCount = 0;
		}
		if (isCancelled()) return null;

		const maximumDenseHandle = arrays.handles.length * 2 + 1_024;
		const handleIndexes = existingHandleIndexes ??
			(maximumHandle <= maximumDenseHandle
				? new Uint32Array(maximumHandle + 1)
				: new Uint32Array(0));
		if (!existingHandleIndexes && handleIndexes.length > 0) {
			sliceStartedAt = performance.now();
			for (let index = 0; index < arrays.handles.length; index++) {
				handleIndexes[arrays.handles[index]] = index + 1;
				if (
					(index & STAGING_CLOCK_CHECK_MASK) === 0 &&
					performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
				) {
					await this.yieldScheduler.yield();
					if (isCancelled()) return null;
					sliceStartedAt = performance.now();
				}
			}
		}

		const overrideIndexBuffer = new Uint32Array(overrideIndexes.length);
		sliceStartedAt = performance.now();
		for (let index = 0; index < overrideIndexes.length; index++) {
			overrideIndexBuffer[index] = overrideIndexes[index];
			if (
				(index & STAGING_CLOCK_CHECK_MASK) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		const heightTimestamps = new Float64Array(heightBuckets.size);
		const heightCounts = new Uint32Array(heightBuckets.size);
		let heightIndex = heightBuckets.size - 1;
		sliceStartedAt = performance.now();
		let processedHeightBuckets = 0;
		for (const [timestamp, count] of heightBuckets) {
			heightTimestamps[heightIndex] = timestamp;
			heightCounts[heightIndex] = count;
			heightIndex--;
			processedHeightBuckets++;
			if (
				(processedHeightBuckets & STAGING_CLOCK_CHECK_MASK) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (isCancelled()) return null;
				sliceStartedAt = performance.now();
			}
		}

		const sampleTimestamps = deriveGraphSamples
			? new Float64Array(graphBuckets.size)
			: arrays.sampleTimestamps;
		const sampleCounts = deriveGraphSamples
			? new Float64Array(graphBuckets.size)
			: arrays.sampleCounts;
		if (deriveGraphSamples) {
			let sampleIndex = graphBuckets.size - 1;
			let processedGraphBuckets = 0;
			sliceStartedAt = performance.now();
			for (const [timestamp, count] of graphBuckets) {
				sampleTimestamps[sampleIndex] = timestamp;
				sampleCounts[sampleIndex] = count;
				sampleIndex--;
				processedGraphBuckets++;
				if (
					(processedGraphBuckets & STAGING_CLOCK_CHECK_MASK) === 0 &&
					performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
				) {
					await this.yieldScheduler.yield();
					if (isCancelled()) return null;
					sliceStartedAt = performance.now();
				}
			}
		}
		const orderedSamples = await this.ensureAscendingSamples(
			sampleTimestamps,
			sampleCounts,
			isCancelled,
		);
		if (!orderedSamples) return null;

		const backingStore = await this.createEventBackingStore(
			arrays,
			handleIndexes,
			overrideIndexBuffer,
			heightTimestamps,
				heightCounts,
			orderedSamples.timestamps,
			orderedSamples.counts,
			isCancelled,
			reusableStore,
			adoptedEventArrays,
		);
		if (!backingStore) return null;
		const view: WorkerRenderEventView = {
			revision,
			loadedCount: arrays.handles.length,
			handles: backingStore.handles.subarray(
				0,
				backingStore.eventLength,
			),
			timestamps: backingStore.timestamps.subarray(
				0,
				backingStore.eventLength,
			),
			hashes: backingStore.hashes.subarray(
				0,
				backingStore.eventLength,
			),
			hashMin,
			hashMax,
			hashMinCount,
			hashMaxCount,
			handleIndexes: backingStore.handleIndexes.subarray(
				0,
				backingStore.handleIndexLength,
			),
			overrideIndexes: backingStore.overrideIndexes.subarray(
				0,
				backingStore.overrideLength,
			),
			colorSourceRevision: source?.revision ?? 0,
			heightTimestamps: backingStore.heightTimestamps.subarray(
				backingStore.heightStart,
				backingStore.heightStart + backingStore.heightLength,
			),
			heightCounts: backingStore.heightCounts.subarray(
				backingStore.heightStart,
				backingStore.heightStart + backingStore.heightLength,
			),
			heightSuffixCounts: backingStore.heightSuffixCounts.subarray(
				backingStore.heightStart,
				backingStore.heightStart + backingStore.heightLength,
			),
			maxHeightCount,
			sampleTimestamps: backingStore.sampleTimestamps.subarray(
				backingStore.sampleStart,
				backingStore.sampleStart + backingStore.sampleLength,
			),
			sampleCounts: backingStore.sampleCounts.subarray(
				backingStore.sampleStart,
				backingStore.sampleStart + backingStore.sampleLength,
			),
			sampleSuffixCounts: backingStore.sampleSuffixCounts.subarray(
				backingStore.sampleStart,
				backingStore.sampleStart + backingStore.sampleLength,
			),
			samplesAreDerived: arrays.samplesAreDerived,
		};
		this.eventBackingStores.set(view, backingStore);
		return view;
	}

	/**
	 * Rebuilds source-dependent metadata when settings change without retransferring buffers.
	 * @param sourceId Source whose color settings were replaced.
	 * @returns True when refreshed metadata was committed.
	 */
	private async restageSourceColorMetadata(sourceId: string): Promise<boolean> {
		const current = this.events.get(sourceId);
		const source = this.sources.get(sourceId);
		if (!current || !source || current.colorSourceRevision >= source.revision) {
			return false;
		}
		if (current.operationEpoch !== undefined) {
			return this.restageRenderWindowColorMetadata(
				sourceId,
				current,
				source,
			);
		}

		const stageGeneration = this.eventStageGeneration.get(sourceId) ?? 0;
		const resetGeneration = this.resetGeneration;
		const isCancelled = (): boolean =>
			this.isEventStageCancelled(
				sourceId,
				stageGeneration,
				resetGeneration,
			);
		const arrays: PreparedEventArrays = {
			handles: current.handles,
			timestamps: current.timestamps,
			hashes: current.hashes,
			sampleTimestamps: current.sampleTimestamps,
			sampleCounts: current.sampleCounts,
			samplesAreDerived: current.samplesAreDerived,
		};
		const reusableStore = this.eventBackingStores.get(current);
		let refreshed = await this.precomputeEventView(
			sourceId,
			current.revision,
			arrays,
			current.handleIndexes,
			isCancelled,
			reusableStore,
		);
		if (!refreshed) return false;
		while (
			refreshed.colorSourceRevision !==
			(this.sources.get(sourceId)?.revision ?? 0)
		) {
			refreshed = await this.precomputeEventView(
				sourceId,
				current.revision,
				arrays,
					current.handleIndexes,
					isCancelled,
					reusableStore,
			);
			if (!refreshed) return false;
		}

		if (
			resetGeneration !== this.resetGeneration ||
			stageGeneration !== (this.eventStageGeneration.get(sourceId) ?? 0) ||
			this.events.get(sourceId) !== current
		) {
			return false;
		}

		const refreshedStore = this.eventBackingStores.get(refreshed);
		const identityPreservingView: WorkerRenderEventView = {
			...refreshed,
			handles: current.handles,
			timestamps: current.timestamps,
			hashes: current.hashes,
			handleIndexes: current.handleIndexes,
		};
		if (refreshedStore) {
			this.eventBackingStores.set(identityPreservingView, refreshedStore);
		}
		this.events.set(sourceId, identityPreservingView);
		return true;
	}

	/**
	 * Refreshes override membership for an LOD view without rebuilding or copying
	 * its transferred representative and aggregate columns.
	 * @param sourceId Source whose override hashes changed.
	 * @param current Current LOD event view retained by the renderer.
	 * @param source Latest source metadata used to resolve override membership.
	 * @returns True when the refreshed representative override index committed.
	 */
	private async restageRenderWindowColorMetadata(
		sourceId: string,
		current: WorkerRenderEventView,
		source: RenderSource,
	): Promise<boolean> {
		const stageGeneration = this.eventStageGeneration.get(sourceId) ?? 0;
		const resetGeneration = this.resetGeneration;
		const overrideHashes = new Set(
			source.overrides.map((override) => override.hash),
		);
		let overrideCount = 0;
		let sliceStartedAt = performance.now();
		for (let index = 0; index < current.hashes.length; index++) {
			if (overrideHashes.has(current.hashes[index])) overrideCount++;
			if (
				(index & STAGING_CLOCK_CHECK_MASK) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (
					this.isEventStageCancelled(
						sourceId,
						stageGeneration,
						resetGeneration,
					) ||
					this.events.get(sourceId) !== current
				) {
					return false;
				}
				sliceStartedAt = performance.now();
			}
		}
		const overrideIndexes = new Uint32Array(overrideCount);
		let overrideIndex = 0;
		sliceStartedAt = performance.now();
		for (let index = 0; index < current.hashes.length; index++) {
			if (overrideHashes.has(current.hashes[index])) {
				overrideIndexes[overrideIndex++] = index;
			}
			if (
				(index & STAGING_CLOCK_CHECK_MASK) === 0 &&
				performance.now() - sliceStartedAt >= this.getStagingSliceBudgetMs()
			) {
				await this.yieldScheduler.yield();
				if (
					this.isEventStageCancelled(
						sourceId,
						stageGeneration,
						resetGeneration,
					) ||
					this.events.get(sourceId) !== current
				) {
					return false;
				}
				sliceStartedAt = performance.now();
			}
		}
		if (
			this.isEventStageCancelled(
				sourceId,
				stageGeneration,
				resetGeneration,
			) ||
			this.events.get(sourceId) !== current ||
			this.sources.get(sourceId) !== source
		) {
			return false;
		}

		this.events.set(sourceId, {
			...current,
			overrideIndexes,
			colorSourceRevision: source.revision,
		});
		return true;
	}

	/**
	 * Replaces the frame's hit-test index after all overlay drawing completes.
	 * @param regions Interactive overlay rectangles in paint order.
	 * @returns Nothing.
	 */
	public setHitRegions(regions: WorkerHitRegion[]): void {
		this.hitRegions = regions;
	}

	/**
	 * Finds the topmost interactive overlay at a canvas coordinate.
	 * @param x Horizontal canvas coordinate.
	 * @param y Vertical canvas coordinate.
	 * @returns Compact note/link identity, or null when no region contains the point.
	 */
	public hitTest(x: number, y: number): RenderHitTestResult | null {
		for (let index = this.hitRegions.length - 1; index >= 0; index--) {
			const region = this.hitRegions[index];
			if (
				x >= region.x &&
				x <= region.x + region.width &&
				y >= region.y &&
				y <= region.y + region.height
			) {
				return region.kind === "note"
					? {
							kind: "note",
							id: region.id,
							noteIds: region.noteIds ?? [region.id],
						}
					: { kind: "link", id: region.id };
			}
		}
		return null;
	}

	/**
	 * Resolves a worker-owned bitmap asset by key.
	 * @param assetKey Stable icon asset key.
	 * @param color Concrete icon color.
	 * @returns ImageBitmap, or undefined while the asset request is pending.
	 */
	public getAsset(assetKey: string, color: string): ImageBitmap | undefined {
		return this.assets.get(this.getAssetCacheKey(assetKey, color));
	}

	/**
	 * Stores a transferred asset and closes the superseded bitmap for the same key.
	 * @param assetKey Stable icon asset key.
	 * @param color Concrete icon color.
	 * @param bitmap Newly transferred bitmap owned by the worker.
	 * @returns Nothing.
	 */
	public setAsset(assetKey: string, color: string, bitmap: ImageBitmap): void {
		const cacheKey = this.getAssetCacheKey(assetKey, color);
		const previous = this.assets.get(cacheKey);
		if (previous && previous !== bitmap) previous.close();
		this.assets.set(cacheKey, bitmap);
	}

	/**
	 * Builds a collision-safe bitmap cache key for a glyph and concrete color.
	 * @param assetKey Stable glyph identifier.
	 * @param color Concrete glyph color.
	 * @returns Worker-local asset cache key.
	 */
	private getAssetCacheKey(assetKey: string, color: string): string {
		return `${assetKey}\u0000${color}`;
	}

	/**
	 * Clears operation-scoped render data while preserving the canvas and ports.
	 * @returns Nothing.
	 */
	public resetOperation(): void {
		this.resetGeneration++;
		this.sources.clear();
		this.events.clear();
		this.eventStageGeneration.clear();
		this.eventSnapshotStages.clear();
		this.removedSourceIds.clear();
		this.notes = [];
		this.links = [];
		this.highlights = [];
		this.flags = [];
		this.hitRegions = [];
		this.sourceRevision = 0;
		this.notesRevision = 0;
		this.linksRevision = 0;
		this.highlightsRevision = 0;
		this.flagsRevision = 0;
		this.globalRevision = 0;
		this.acknowledgedGlobalRevision = 0;
		this.renderWindowOperationEpoch = -1;
		this.renderWindowViewportRevision = -1;
		this.renderWindowProjectionRevision = -1;
		this.viewportState = null;
		this.sceneState = null;
		this.pointerState = { x: -1000, y: -1000, inside: false };
		this.frameState = null;
		for (const bitmap of this.assets.values()) bitmap.close();
		this.assets.clear();
	}

	/**
	 * Releases every worker-owned resource before worker termination.
	 * @returns Nothing.
	 */
	public dispose(): void {
		if (this.disposed) return;
		this.disposed = true;
		this.resetOperation();
		this.yieldScheduler.dispose();
	}
}
