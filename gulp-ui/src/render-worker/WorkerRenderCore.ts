import {
	DirtyCanvas,
	RENDER_GRAPH_BUCKET_PIXEL_WIDTH,
	type RenderFrameState,
	type RenderFrameStatistics,
	type RenderHighlightRecord,
	type RenderLinkRecord,
	type RenderNoteRecord,
	type RenderSource,
} from "./RenderWorker.types";
import {
	type WorkerHitRegion,
	type WorkerRenderEventView,
	WorkerRenderRepository,
} from "./WorkerRenderRepository";
import { formatTimestampInTimeZone } from "@/timeZone";

const ROW_HEIGHT = 48;
const ROW_CENTER_OFFSET = 24;
const NOTE_SIZE = 32;
const NOTE_OFFSET = -NOTE_SIZE / 2;
const HIGHLIGHT_ALPHA = "29";
const GRAPH_DRAWABLE_HEIGHT = 47;
const GRAPH_SEMI_LOG_BASE = 10;
const RULER_HEIGHT = 25;
const MAX_DENSE_COLOR_RANGE = 8_192;
const MAX_SPARSE_COLOR_CACHE_SIZE = 4_096;

/** Context surface shared by the worker and the staged main-thread render adapter. */
export type RenderCanvasContext =
	| CanvasRenderingContext2D
	| OffscreenCanvasRenderingContext2D;

/** Callbacks for resources that remain owned by the main thread. */
export interface WorkerRenderCoreCallbacks {
	requestAsset: (assetKey: string, color: string) => void;
}

interface RenderDot {
	x: number;
	y: number;
	color: string;
}

interface RulerSection {
	timestamp: number;
	position: number;
	step: number;
	unit: string;
	value: number;
	even: boolean;
}

interface GraphBucketCache {
	eventRevision: number;
	sourceRevision: number;
	frequency: number;
	sampleLength: number;
	startBucket: number;
	endBucket: number;
	eventViewToken: number;
	buckets: Map<number, number>;
	maxHeight: number;
	sampleCount: number;
	totalCount: number;
}

/** Parsed color retained across frames for canvas and raster render paths. */
interface CachedRenderColor {
	css: string;
	rgb: number | null;
}

/** Revision-scoped color metadata and bounded resolved-value cache. */
interface SourceColorCache {
	source: RenderSource;
	overrides: Map<number, CachedRenderColor>;
	customPalette: CachedRenderColor[];
	contextColor: CachedRenderColor;
	gradientColors: CachedRenderColor[];
	gradientNumbers: Float64Array;
	rasterCompatible: boolean;
	rangeMinimum: number;
	rangeMaximum: number;
	denseMinimum: number | null;
	denseColors: Array<CachedRenderColor | undefined> | null;
	sparseColors: Map<number, CachedRenderColor>;
}

/** Immutable highlight paint layout derived from one overlay revision. */
interface HighlightLayoutCache {
	revision: number;
	records: readonly RenderHighlightRecord[];
	depths: readonly number[];
	colors: readonly string[];
}

/** Timestamp-sorted note index for one source. */
interface NoteSourceLayout {
	records: readonly RenderNoteRecord[];
	timestamps: Float64Array;
}

/** Source-partitioned note index derived from one overlay revision. */
interface NoteLayoutCache {
	revision: number;
	records: readonly RenderNoteRecord[];
	bySource: Map<string, NoteSourceLayout>;
}

/** Link collection retained for one immutable overlay revision. */
interface LinkLayoutCache {
	revision: number;
	records: readonly RenderLinkRecord[];
}

/** Reusable transparent pixel buffer uploaded as one event-row draw. */
interface RowRasterSurface {
	canvas: OffscreenCanvas;
	context: OffscreenCanvasRenderingContext2D;
	imageData: ImageData;
}

/**
 * JSX-free render core that consumes only worker DTOs and a canvas-like 2D context.
 */
export class WorkerRenderCore {
	private readonly graphBucketCache = new Map<string, GraphBucketCache>();
	private readonly sourceColorCache = new Map<string, SourceColorCache>();
	private readonly pendingAssetRequests = new Set<string>();
	private readonly rowRasterSurfaces = new Map<number, RowRasterSurface>();
	private readonly linkDotScratch: RenderDot[] = [];
	private eventViewTokens = new WeakMap<WorkerRenderEventView, number>();
	private nextEventViewToken = 1;
	private magnifierCanvas: OffscreenCanvas | null = null;
	private highlightLayoutCache: HighlightLayoutCache | null = null;
	private noteLayoutCache: NoteLayoutCache | null = null;
	private linkLayoutCache: LinkLayoutCache | null = null;
	private lastRenderedViewportRevision = -1;

	/**
	 * Creates a renderer around one context and worker-local repository.
	 * @param context Canvas context that receives all drawing commands.
	 * @param repository Worker-local render projections and assets.
	 * @param callbacks Main-thread resource callbacks.
	 */
	public constructor(
		private readonly context: RenderCanvasContext,
		private readonly repository: WorkerRenderRepository,
		private readonly callbacks: WorkerRenderCoreCallbacks,
	) {
		this.context.imageSmoothingEnabled = false;
		this.context.imageSmoothingQuality = "high";
		this.context.font = '10px "GeistMono", monospace';
	}

	/**
	 * Draws the latest committed repository view and rebuilds overlay hit regions.
	 * @param frameTimestamp Worker animation-frame timestamp.
	 * @param dirtyMask Coalesced invalidation reasons captured for this frame.
	 * @returns Detailed frame statistics for the profiler.
	 */
	public render(
		frameTimestamp: number,
		dirtyMask: DirtyCanvas,
	): RenderFrameStatistics | null {
		const state = this.repository.getFrameState();
		if (!state || state.width <= 0 || state.height <= 0) return null;

		const startedAt = performance.now();
		this.resizeCanvas(state);
		this.context.clearRect(0, 0, state.width, state.height);
		const statistics = this.createFrameStatistics(
			frameTimestamp,
			dirtyMask,
			state,
		);
		const sources = this.repository.getOrderedSources();
		statistics.visibleSources = sources.length;
		const sections = this.drawRulerWalls(state);
		const visibleSources: RenderSource[] = [];
		const sourceIndexes = new Map<string, number>();

		for (let index = 0; index < sources.length; index++) {
			const source = sources[index];
			sourceIndexes.set(source.id, index);
			const centerY = this.getSourceCenterY(index, state);
			if (centerY < -ROW_HEIGHT || centerY > state.height + ROW_HEIGHT) {
				continue;
			}
			visibleSources.push(source);
			statistics.renderedRows++;
			if (this.isSourceInTimeline(source, state)) {
				this.drawSourceEvents(source, centerY - ROW_CENTER_OFFSET, state, statistics);
			}
			if (index === 0) this.drawPrimaryRowEdge(centerY, state);
			this.drawSourceRow(source, centerY, state);
			this.drawTargetGuide(source, centerY, state);
			this.drawSourceLocals(source, centerY, state);
			this.drawSourceInfo(source, centerY, state);
		}

		this.drawTargetMarker(state);
		this.drawHighlights(state);
		this.drawFlags(sourceIndexes, state);
		const hitRegions: WorkerHitRegion[] = [];
		if (!state.hidden.notes) {
			this.drawNotes(
				visibleSources,
				sourceIndexes,
				state,
				hitRegions,
				statistics,
			);
		}
		if (!state.hidden.links) {
			this.drawLinks(sourceIndexes, state, hitRegions, statistics);
		}
		this.repository.setHitRegions(hitRegions);
		statistics.interactiveRegions = hitRegions.length;
		this.drawFrameEdges(state);
		this.drawRulerSections(sections, state);
		this.lastRenderedViewportRevision = state.viewportRevision;
		statistics.drawDuration = performance.now() - startedAt;
		return statistics;
	}

	/**
	 * Returns the viewport revision represented by the latest completed frame.
	 * @returns Last rendered viewport revision, or -1 before the first frame.
	 */
	public getRenderedViewportRevision(): number {
		return this.lastRenderedViewportRevision;
	}

	/**
	 * Produces a magnified crop without reacquiring the transferred source context.
	 * @param request Crop center, source size, and output dimensions.
	 * @returns Transferable bitmap containing the requested crop.
	 */
	public createMagnifierBitmap(request: {
		x: number;
		y: number;
		cropWidth: number;
		cropHeight: number;
		outputWidth: number;
		outputHeight: number;
	}): ImageBitmap {
		const outputWidth = Math.max(1, Math.round(request.outputWidth));
		const outputHeight = Math.max(1, Math.round(request.outputHeight));
		if (
			!this.magnifierCanvas ||
			this.magnifierCanvas.width !== outputWidth ||
			this.magnifierCanvas.height !== outputHeight
		) {
			this.magnifierCanvas = new OffscreenCanvas(outputWidth, outputHeight);
		}
		const magnifierContext = this.magnifierCanvas.getContext("2d");
		if (!magnifierContext) {
			throw new Error("Unable to create magnifier 2D context");
		}

		const sourceWidth = Math.max(1, request.cropWidth);
		const sourceHeight = Math.max(1, request.cropHeight);
		const sourceX = Math.max(0, request.x - sourceWidth / 2);
		const sourceY = Math.max(0, request.y - sourceHeight / 2);
		magnifierContext.clearRect(0, 0, outputWidth, outputHeight);
		magnifierContext.imageSmoothingEnabled = false;
		magnifierContext.drawImage(
			this.context.canvas,
			sourceX,
			sourceY,
			sourceWidth,
			sourceHeight,
			0,
			0,
			outputWidth,
			outputHeight,
		);
		return this.magnifierCanvas.transferToImageBitmap();
	}

	/**
	 * Releases a pending marker once an icon response has been installed.
	 * @param assetKey Stable glyph identifier.
	 * @param color Concrete glyph color.
	 * @returns Nothing.
	 */
	public notifyAssetAvailable(assetKey: string, color: string): void {
		this.pendingAssetRequests.delete(this.getAssetRequestKey(assetKey, color));
	}

	/**
	 * Releases a failed asset request so a later render invalidation may retry it.
	 * No frame is scheduled here, preventing a persistently unavailable asset from
	 * creating a request/repaint loop.
	 * @param assetKey Stable glyph identifier.
	 * @param color Concrete glyph color.
	 * @returns Nothing.
	 */
	public notifyAssetUnavailable(assetKey: string, color: string): void {
		this.pendingAssetRequests.delete(this.getAssetRequestKey(assetKey, color));
	}

	/**
	 * Removes source-scoped graph and override caches after metadata removal.
	 * @param sourceIds Source identifiers removed from the render repository.
	 * @returns Nothing.
	 */
	public invalidateSources(sourceIds: readonly string[]): void {
		for (const sourceId of sourceIds) {
			this.graphBucketCache.delete(sourceId);
			this.sourceColorCache.delete(sourceId);
		}
	}

	/**
	 * Invalidates graph aggregation for sources whose committed event view changed.
	 * The cache deliberately stores only a numeric view token, never a strong reference
	 * to the prior typed-array view.
	 * @param sourceIds Source identifiers with newly committed event projections.
	 * @returns Nothing.
	 */
	public invalidateEventSources(sourceIds: readonly string[]): void {
		for (const sourceId of sourceIds) {
			this.graphBucketCache.delete(sourceId);
		}
	}

	/**
	 * Clears pending asset-request identities after an asset collection revision changes.
	 * @returns Nothing.
	 */
	public invalidateAssets(): void {
		this.pendingAssetRequests.clear();
	}

	/**
	 * Clears operation-scoped renderer caches and request ordering state.
	 * @returns Nothing.
	 */
	public reset(): void {
		this.graphBucketCache.clear();
		this.sourceColorCache.clear();
		this.pendingAssetRequests.clear();
		this.rowRasterSurfaces.clear();
		this.linkDotScratch.length = 0;
		this.eventViewTokens = new WeakMap<WorkerRenderEventView, number>();
		this.nextEventViewToken = 1;
		this.lastRenderedViewportRevision = -1;
		this.magnifierCanvas = null;
		this.highlightLayoutCache = null;
		this.noteLayoutCache = null;
		this.linkLayoutCache = null;
	}

	/**
	 * Releases renderer-owned scratch state before worker disposal.
	 * @returns Nothing.
	 */
	public dispose(): void {
		this.reset();
	}

	/**
	 * Resizes the backing surface while preserving the fixed one-CSS-pixel ratio.
	 * @param state Frame dimensions measured by the main thread.
	 * @returns Nothing.
	 */
	private resizeCanvas(state: RenderFrameState): void {
		if (this.context.canvas.width !== state.width) {
			this.context.canvas.width = state.width;
		}
		if (this.context.canvas.height !== state.height) {
			this.context.canvas.height = state.height;
		}
		this.context.imageSmoothingEnabled = false;
	}

	/**
	 * Creates zeroed counters for one render pass.
	 * @param frameTimestamp Worker animation-frame timestamp.
	 * @param dirtyMask Captured invalidation mask.
	 * @param state Current frame state.
	 * @returns Mutable frame-statistics record.
	 */
	private createFrameStatistics(
		frameTimestamp: number,
		dirtyMask: DirtyCanvas,
		state: RenderFrameState,
	): RenderFrameStatistics {
			return {
				frameTimestamp,
				drawDuration: 0,
				workerReceiveDuration: 0,
				workerWaitDuration: 0,
				dirtyMask,
				viewportRevision: state.viewportRevision,
				inputSequence: state.inputSequence,
			dataRevision: this.repository.getGlobalRevision(),
			dataLatency: null,
			visibleSources: 0,
			renderedRows: 0,
			renderedEventPixels: 0,
			graphSamples: 0,
			graphBuckets: 0,
			visibleNotes: 0,
			visibleLinks: 0,
			interactiveRegions: 0,
		};
	}

	/**
	 * Converts a timestamp to the nearest virtual-timeline canvas pixel.
	 * @param timestamp Timestamp to transform.
	 * @param state Current frame and viewport state.
	 * @returns Canvas x coordinate.
	 */
	private getPixelPosition(timestamp: number, state: RenderFrameState): number {
		const range = state.frame.max - state.frame.min;
		if (!Number.isFinite(range) || range === 0) return -state.scrollX;
		return (
			Math.round(
				((timestamp - state.frame.min) / range) * state.width * state.scale,
			) - state.scrollX
		);
	}

	/**
	 * Converts a canvas x coordinate to a timeline timestamp.
	 * @param x Canvas x coordinate.
	 * @param state Current frame and viewport state.
	 * @returns Timeline timestamp.
	 */
	private getTimestamp(x: number, state: RenderFrameState): number {
		const virtualWidth = state.width * state.scale;
		if (!Number.isFinite(virtualWidth) || virtualWidth === 0) {
			return state.frame.min;
		}
		return (
			state.frame.min +
			((x + state.scrollX) / virtualWidth) *
				(state.frame.max - state.frame.min)
		);
	}

	/**
	 * Computes a source row's vertical center.
	 * @param rowIndex Ordered source row index.
	 * @param state Current vertical scroll state.
	 * @returns Canvas y coordinate for the row center.
	 */
	private getSourceCenterY(rowIndex: number, state: RenderFrameState): number {
		return rowIndex * ROW_HEIGHT - state.scrollY + ROW_CENTER_OFFSET;
	}

	/**
	 * Checks whether a source overlaps both the selected frame and visible x limits.
	 * @param source Source timestamp bounds and offset.
	 * @param state Current timeline viewport.
	 * @returns True when source content can affect the frame.
	 */
	private isSourceInTimeline(
		source: RenderSource,
		state: RenderFrameState,
	): boolean {
		const visibleMin = this.getTimestamp(0, state);
		const visibleMax = this.getTimestamp(state.width, state);
		const sourceMin = source.timestamp.min + source.offset;
		const sourceMax = source.timestamp.max + source.offset;
		return !(
			sourceMax < visibleMin ||
			sourceMin > visibleMax ||
			sourceMax < state.frame.min ||
			sourceMin > state.frame.max
		);
	}

	/**
	 * Selects and runs one worker-safe source rendering algorithm.
	 * @param source Source metadata and engine selection.
	 * @param rowTop Top edge of the 48-pixel source row.
	 * @param state Current viewport state.
	 * @param statistics Mutable profiling counters.
	 * @returns Nothing.
	 */
	private drawSourceEvents(
		source: RenderSource,
		rowTop: number,
		state: RenderFrameState,
		statistics: RenderFrameStatistics,
	): void {
		const events = this.repository.getEvents(source.id);
		if (!events) return;
		switch (source.engine) {
			case "height":
				this.drawHeightEvents(source, events, rowTop, state);
				break;
			case "graph":
				this.drawGraphEvents(source, events, rowTop, state, statistics);
				break;
			default:
				this.drawDefaultEvents(source, events, rowTop, state, statistics);
				break;
		}
	}

	/**
	 * Draws at most one default-engine event line per visible canvas pixel bucket.
	 * @param source Source color and offset settings.
	 * @param events Descending timestamp event view.
	 * @param rowTop Top edge of the source row.
	 * @param state Current timeline viewport.
	 * @param statistics Mutable profiling counters.
	 * @returns Nothing.
	 */
	private drawDefaultEvents(
		source: RenderSource,
		events: WorkerRenderEventView,
		rowTop: number,
		state: RenderFrameState,
		statistics: RenderFrameStatistics,
	): void {
		const minimumVisible = this.getTimestamp(-50, state) - source.offset;
		const maximumVisible =
			this.getTimestamp(state.width + 50, state) - source.offset;
		const startIndex = this.findFirstDescendingAtMost(
			events.timestamps,
			maximumVisible,
		);
		if (startIndex === events.timestamps.length) return;
		const endIndex = this.findLastDescendingAtLeast(
			events.timestamps,
			minimumVisible,
			startIndex,
		);
		if (endIndex < startIndex) return;

		const virtualWidth = state.width * state.scale;
		const frameRange = state.frame.max - state.frame.min;
		const colorCache = this.getSourceColorCache(source);
		const rasterSurface = colorCache.rasterCompatible
			? this.getRowRasterSurface(state.width, 1)
			: null;
		if (rasterSurface) rasterSurface.imageData.data.fill(0);
		let eventIndex = startIndex;
		while (eventIndex <= endIndex) {
			const adjustedTimestamp =
				events.timestamps[eventIndex] + source.offset;
			if (
				adjustedTimestamp < state.frame.min ||
				adjustedTimestamp > state.frame.max
			) {
				eventIndex++;
				continue;
			}

			const x = this.getPixelPosition(adjustedTimestamp, state);
			const threshold =
				state.frame.min +
				((x - 0.5 + state.scrollX) / virtualWidth) * frameRange -
				source.offset;
			const nextIndex = this.findFirstBelowThreshold(
				events.timestamps,
				eventIndex + 1,
				endIndex,
				threshold,
			);
			const overrideIndex = this.findOverrideInBucket(
				events.overrideIndexes,
				eventIndex,
				nextIndex,
			);
			const colorIndex = overrideIndex === -1 ? eventIndex : overrideIndex;
			const color = this.resolveColorValue(
				source,
				events.hashes[colorIndex],
				events.hashMin,
				events.hashMax,
			);
			if (rasterSurface && color.rgb !== null) {
				this.writeOpaqueRasterPixel(
					rasterSurface.imageData.data,
					state.width,
					x,
					0,
					color.rgb,
				);
			} else {
				this.context.fillStyle = color.css;
				this.context.fillRect(x, rowTop, 1, ROW_HEIGHT - 1);
			}
			statistics.renderedEventPixels++;
			eventIndex = nextIndex;
		}
		if (rasterSurface) {
			this.paintRasterSurface(rasterSurface, rowTop, ROW_HEIGHT - 1);
		}
	}

	/**
	 * Finds the first descending value less than or equal to a maximum.
	 * @param values Descending timestamps.
	 * @param maximum Inclusive maximum timestamp.
	 * @returns Matching index, or values.length when absent.
	 */
	private findFirstDescendingAtMost(
		values: Float64Array,
		maximum: number,
	): number {
		let left = 0;
		let right = values.length - 1;
		let result = values.length;
		while (left <= right) {
			const middle = (left + right) >>> 1;
			if (values[middle] <= maximum) {
				result = middle;
				right = middle - 1;
			} else {
				left = middle + 1;
			}
		}
		return result;
	}

	/**
	 * Finds the final descending value greater than or equal to a minimum.
	 * @param values Descending timestamps.
	 * @param minimum Inclusive minimum timestamp.
	 * @param startIndex First candidate index.
	 * @returns Matching index, or -1 when absent.
	 */
	private findLastDescendingAtLeast(
		values: Float64Array,
		minimum: number,
		startIndex: number,
	): number {
		let left = startIndex;
		let right = values.length - 1;
		let result = -1;
		while (left <= right) {
			const middle = (left + right) >>> 1;
			if (values[middle] >= minimum) {
				result = middle;
				left = middle + 1;
			} else {
				right = middle - 1;
			}
		}
		return result;
	}

	/**
	 * Finds the first event that maps below the current pixel threshold.
	 * @param timestamps Descending event timestamps.
	 * @param startIndex First candidate index.
	 * @param endIndex Last visible index.
	 * @param threshold Raw timestamp threshold.
	 * @returns First next-pixel event index, or one past endIndex.
	 */
	private findFirstBelowThreshold(
		timestamps: Float64Array,
		startIndex: number,
		endIndex: number,
		threshold: number,
	): number {
		let left = startIndex;
		let right = endIndex;
		let result = endIndex + 1;
		while (left <= right) {
			const middle = (left + right) >>> 1;
			if (timestamps[middle] < threshold) {
				result = middle;
				right = middle - 1;
			} else {
				left = middle + 1;
			}
		}
		return result;
	}

	/**
	 * Finds the newest override-colored event inside a collapsed pixel bucket.
	 * @param overrideIndexes Sorted event indexes with explicit colors.
	 * @param bucketStart Inclusive event index.
	 * @param bucketEnd Exclusive event index.
	 * @returns Override event index, or -1 when absent.
	 */
	private findOverrideInBucket(
		overrideIndexes: Uint32Array,
		bucketStart: number,
		bucketEnd: number,
	): number {
		let left = 0;
		let right = overrideIndexes.length - 1;
		let candidate = overrideIndexes.length;
		while (left <= right) {
			const middle = (left + right) >>> 1;
			if (overrideIndexes[middle] >= bucketStart) {
				candidate = middle;
				right = middle - 1;
			} else {
				left = middle + 1;
			}
		}
		if (candidate === overrideIndexes.length) return -1;
		const eventIndex = overrideIndexes[candidate];
		return eventIndex < bucketEnd ? eventIndex : -1;
	}

	/**
	 * Draws fixed one-minute event-count buckets as normalized vertical bars.
	 * @param source Source color and offset settings.
	 * @param events Precomputed height buckets.
	 * @param rowTop Top edge of the source row.
	 * @param state Current timeline viewport.
	 * @returns Nothing.
	 */
	private drawHeightEvents(
		source: RenderSource,
		events: WorkerRenderEventView,
		rowTop: number,
		state: RenderFrameState,
	): void {
		if (events.maxHeightCount <= 0) return;
		const minimum = this.getTimestamp(-2, state) - source.offset;
		const maximum = this.getTimestamp(state.width + 2, state) - source.offset;
		const colorCache = this.getSourceColorCache(source);
		const rasterSurface = colorCache.rasterCompatible
			? this.getRowRasterSurface(state.width, GRAPH_DRAWABLE_HEIGHT)
			: null;
		if (rasterSurface) rasterSurface.imageData.data.fill(0);
		let index = this.lowerBoundAscending(events.heightTimestamps, minimum);
		while (
			index < events.heightTimestamps.length &&
			events.heightTimestamps[index] <= maximum
		) {
			const count = events.heightCounts[index];
			if (count > 0) {
				const color = this.resolveColorValue(
					source,
					count,
					0,
					events.maxHeightCount,
				);
				const barHeight = 1 + 46 * (count / events.maxHeightCount);
				const x = this.getPixelPosition(
					events.heightTimestamps[index] + source.offset,
					state,
				);
				if (rasterSurface && color.rgb !== null) {
					this.writeHeightRasterBar(
						rasterSurface.imageData.data,
						state.width,
						x,
						barHeight,
						color.rgb,
					);
				} else {
					this.context.fillStyle = color.css;
					this.context.fillRect(x, rowTop + 47, 1, -barHeight);
				}
			}
			index++;
		}
		if (rasterSurface) {
			this.paintRasterSurface(
				rasterSurface,
				rowTop,
				GRAPH_DRAWABLE_HEIGHT,
			);
		}
	}

	/**
	 * Returns the first ascending value greater than or equal to a target.
	 * @param values Ascending numeric typed array.
	 * @param target Inclusive target value.
	 * @returns First matching index.
	 */
	private lowerBoundAscending(values: Float64Array, target: number): number {
		let left = 0;
		let right = values.length;
		while (left < right) {
			const middle = (left + right) >>> 1;
			if (values[middle] < target) left = middle + 1;
			else right = middle;
		}
		return left;
	}

	/**
	 * Draws dynamically aggregated graph samples using a semi-log density scale.
	 * @param source Source graph and color settings.
	 * @param events Event view containing stable graph samples.
	 * @param rowTop Top edge of the source row.
	 * @param state Current timeline viewport.
	 * @param statistics Mutable profiling counters.
	 * @returns Nothing.
	 */
	private drawGraphEvents(
		source: RenderSource,
		events: WorkerRenderEventView,
		rowTop: number,
		state: RenderFrameState,
		statistics: RenderFrameStatistics,
	): void {
		const sampleTimestamps =
			events.sampleTimestamps.length > 0
				? events.sampleTimestamps
				: events.heightTimestamps;
		const sampleSuffixCounts =
			events.sampleCounts.length > 0
				? events.sampleSuffixCounts
				: events.heightSuffixCounts;
		if (sampleTimestamps.length === 0) return;

		const timePerTenPixels =
			(RENDER_GRAPH_BUCKET_PIXEL_WIDTH * (state.frame.max - state.frame.min)) /
			(state.width * state.scale);
		const dynamicFrequency = Math.max(source.frequency, timePerTenPixels);
		if (!Number.isFinite(dynamicFrequency) || dynamicFrequency <= 0) return;
		const visibleStart = this.getTimestamp(0, state) - source.offset;
		const visibleEnd = this.getTimestamp(state.width, state) - source.offset;
		const sourceStart = source.timestamp.min;
		const startBucket = Math.max(
			0,
			Math.floor((visibleStart - sourceStart) / dynamicFrequency) - 1,
		);
		const finalSourceBucket = Math.max(
			0,
			Math.ceil(
				(source.timestamp.max - sourceStart) / dynamicFrequency,
			) - 1,
		);
		const endBucket = Math.min(
			finalSourceBucket,
			Math.floor((visibleEnd - sourceStart) / dynamicFrequency) + 1,
		);
		const bucketCache = this.getGraphBuckets(
			source,
			events,
			sampleTimestamps,
			sampleSuffixCounts,
			dynamicFrequency,
			startBucket,
			endBucket,
		);
		statistics.graphSamples += bucketCache.sampleCount;
		statistics.graphBuckets += bucketCache.buckets.size;
		if (bucketCache.totalCount <= 0) return;

		const maximumCeiling =
			bucketCache.maxHeight <= 1
				? 1
				: GRAPH_SEMI_LOG_BASE ** Math.ceil(Math.log10(bucketCache.maxHeight));
		const semiLogMaximum = this.getSemiLogValue(maximumCeiling);
		let hasPrevious = false;
		let previousX = 0;
		let previousY = 0;
		let previousColor = "";

		for (let bucketIndex = startBucket; bucketIndex <= endBucket; bucketIndex++) {
			const boundaryTimestamp = sourceStart + bucketIndex * dynamicFrequency;
			const boundaryX = this.getPixelPosition(
				boundaryTimestamp + source.offset,
				state,
			);
			this.context.beginPath();
			this.context.moveTo(boundaryX, rowTop);
			this.context.lineTo(boundaryX, rowTop + ROW_HEIGHT);
			this.context.strokeStyle = state.palette.border;
			this.context.stroke();

			const centerRaw = Math.min(
				source.timestamp.max,
				Math.max(
					source.timestamp.min,
					sourceStart +
						bucketIndex * dynamicFrequency +
						dynamicFrequency / 2,
				),
			);
			const x = this.getPixelPosition(centerRaw + source.offset, state);
			if (x < -32 || x > state.width + 32) continue;
			const count = bucketCache.buckets.get(bucketIndex) ?? 0;
			const ratio =
				semiLogMaximum > 0
					? this.getSemiLogValue(count) / semiLogMaximum
					: 0;
			const y =
				rowTop +
				GRAPH_DRAWABLE_HEIGHT -
				Math.floor(Math.min(ratio, 1) * GRAPH_DRAWABLE_HEIGHT);
			const color = this.resolveColor(
				source,
				this.getSemiLogValue(count),
				0,
				semiLogMaximum,
			);
			this.context.font = "8px Arial";
			this.context.fillStyle = state.palette.fontAccent;
			this.context.textAlign = "left";
			this.context.fillText(count.toString(), x - 3.5, y - 8);
			this.drawDotCoordinates(x, y, color, state);
			if (hasPrevious) {
				this.drawConnectionSegment(
					previousX,
					previousY,
					previousColor,
					x,
					y,
					color,
				);
			}
			hasPrevious = true;
			previousX = x;
			previousY = y;
			previousColor = color;
		}
	}

	/**
	 * Returns cached dynamic graph aggregation for a source and zoom frequency.
	 * @param source Source timeline metadata.
	 * @param events Committed event view and revision.
	 * @param sampleTimestamps Stable sample timestamps.
	 * @param sampleSuffixCounts Precomputed suffix sums aligned with samples.
	 * @param frequency Dynamic bucket duration.
	 * @param startBucket First visible bucket including the paint buffer.
	 * @param endBucket Last visible bucket including the paint buffer.
	 * @returns Cached or newly aggregated graph buckets.
	 */
	private getGraphBuckets(
		source: RenderSource,
		events: WorkerRenderEventView,
		sampleTimestamps: Float64Array,
		sampleSuffixCounts: Float64Array,
		frequency: number,
		startBucket: number,
		endBucket: number,
	): GraphBucketCache {
		const normalizedFrequency = Math.max(
			0.001,
			Math.round(frequency * 1000) / 1000,
		);
		const eventViewToken = this.getEventViewToken(events);
		const cached = this.graphBucketCache.get(source.id);
		if (
			cached?.eventRevision === events.revision &&
			cached.sourceRevision === source.revision &&
			cached.frequency === normalizedFrequency &&
			cached.sampleLength === sampleTimestamps.length &&
			cached.startBucket === startBucket &&
			cached.endBucket === endBucket &&
			cached.eventViewToken === eventViewToken
		) {
			return cached;
		}

		const buckets = new Map<number, number>();
		let maxHeight = 0;
		for (let bucketIndex = startBucket; bucketIndex <= endBucket; bucketIndex++) {
			const bucketStart =
				source.timestamp.min + bucketIndex * normalizedFrequency;
			const bucketEnd = bucketStart + normalizedFrequency;
			const startIndex = this.lowerBoundAscending(sampleTimestamps, bucketStart);
			const endIndex = this.lowerBoundAscending(sampleTimestamps, bucketEnd);
			const count = this.getSuffixRangeCount(
				sampleSuffixCounts,
				startIndex,
				endIndex,
			);
			if (count > 0) buckets.set(bucketIndex, count);
			if (count > maxHeight) maxHeight = count;
		}
		const result = {
			eventRevision: events.revision,
			sourceRevision: source.revision,
			frequency: normalizedFrequency,
			sampleLength: sampleTimestamps.length,
			startBucket,
			endBucket,
			eventViewToken,
			buckets,
			maxHeight,
			sampleCount: sampleTimestamps.length,
			totalCount:
				sampleSuffixCounts.length > 0
					? sampleSuffixCounts[0]
					: 0,
		};
		this.graphBucketCache.set(source.id, result);
		return result;
	}

	/**
	 * Resolves an exact half-open range sum from a precomputed suffix index.
	 * @param suffixCounts Suffix sums aligned with ascending samples.
	 * @param start Inclusive sample index.
	 * @param end Exclusive sample index.
	 * @returns Sum of sample counts inside the requested range.
	 */
	private getSuffixRangeCount(
		suffixCounts: Float64Array,
		start: number,
		end: number,
	): number {
		if (start >= end || start >= suffixCounts.length) return 0;
		return (
			suffixCounts[start] -
			(end < suffixCounts.length ? suffixCounts[end] : 0)
		);
	}

	/**
	 * Resolves a weakly associated token that changes for same-revision view replacements.
	 * @param eventView Committed source event view used by graph aggregation.
	 * @returns Renderer-local identity token without retaining superseded event buffers.
	 */
	private getEventViewToken(eventView: WorkerRenderEventView): number {
		const existingToken = this.eventViewTokens.get(eventView);
		if (existingToken !== undefined) return existingToken;
		const token = this.nextEventViewToken++;
		this.eventViewTokens.set(eventView, token);
		return token;
	}

	/**
	 * Maps a graph count into the base-10 semi-log domain.
	 * @param count Bucket count.
	 * @returns Semi-log plotting value.
	 */
	private getSemiLogValue(count: number): number {
		return count <= 0 ? 0 : Math.log10(count + 1);
	}

	/**
	 * Resolves explicit overrides, custom palette buckets, or an interpolated gradient.
	 * @param source Source color configuration.
	 * @param value Numeric hash or density value.
	 * @param minimum Minimum value represented by the gradient.
	 * @param maximum Maximum value represented by the gradient.
	 * @returns Concrete CSS color.
	 */
	private resolveColor(
		source: RenderSource,
		value: number,
		minimum: number,
		maximum: number,
	): string {
		return this.resolveColorValue(source, value, minimum, maximum).css;
	}

	/**
	 * Resolves one color together with an optional packed RGB representation.
	 * @param source Source color configuration.
	 * @param value Numeric hash or density value.
	 * @param minimum Minimum value represented by the gradient.
	 * @param maximum Maximum value represented by the gradient.
	 * @returns Cached CSS color and an opaque RGB value when raster-safe.
	 */
	private resolveColorValue(
		source: RenderSource,
		value: number,
		minimum: number,
		maximum: number,
	): CachedRenderColor {
		const sourceCache = this.getSourceColorCache(source);
		const override = sourceCache.overrides.get(value);
		if (override?.css) return override;
		if (sourceCache.customPalette.length > 0) {
			const paletteIndex =
				Math.abs(Math.trunc(value)) % sourceCache.customPalette.length;
			return sourceCache.customPalette[paletteIndex];
		}

		if (sourceCache.gradientColors.length === 0) {
			return sourceCache.contextColor;
		}
		if (!Number.isFinite(maximum - minimum) || maximum <= minimum) {
			return sourceCache.gradientColors[0];
		}
		this.prepareResolvedColorRange(sourceCache, minimum, maximum);
		const denseIndex =
			sourceCache.denseMinimum === null
				? -1
				: value - sourceCache.denseMinimum;
		if (
			sourceCache.denseColors &&
			Number.isInteger(denseIndex) &&
			denseIndex >= 0 &&
			denseIndex < sourceCache.denseColors.length
		) {
			const cached = sourceCache.denseColors[denseIndex];
			if (cached) return cached;
			const resolved = this.interpolateCachedGradient(
				sourceCache,
				value,
				minimum,
				maximum,
			);
			sourceCache.denseColors[denseIndex] = resolved;
			return resolved;
		}

		const sparseCached = sourceCache.sparseColors.get(value);
		if (sparseCached) return sparseCached;
		const resolved = this.interpolateCachedGradient(
			sourceCache,
			value,
			minimum,
			maximum,
		);
		if (sourceCache.sparseColors.size >= MAX_SPARSE_COLOR_CACHE_SIZE) {
			sourceCache.sparseColors.clear();
		}
		sourceCache.sparseColors.set(value, resolved);
		return resolved;
	}

	/**
	 * Returns immutable parsed source colors until the source DTO identity changes.
	 * @param source Source whose palette metadata should be resolved.
	 * @returns Revision-scoped source color cache.
	 */
	private getSourceColorCache(source: RenderSource): SourceColorCache {
		const cached = this.sourceColorCache.get(source.id);
		if (cached?.source === source) return cached;

		const overrides = new Map<number, CachedRenderColor>();
		for (const override of source.overrides) {
			overrides.set(
				override.hash,
				this.createCachedRenderColor(override.color),
			);
		}
		const customPalette = source.customPaletteColors.map((color) =>
			this.createCachedRenderColor(color),
		);
		const gradientColors = source.gradient.map((color) =>
			this.createCachedRenderColor(this.normalizeHexColor(color)),
		);
		const gradientNumbers = Float64Array.from(
			gradientColors,
			(color) => Number.parseInt(color.css.slice(1), 16),
		);
		const contextColor = this.createCachedRenderColor(source.contextColor);
		const activeBaseColors =
			customPalette.length > 0
				? customPalette
				: gradientColors.length > 0
					? gradientColors
					: [contextColor];
		const result: SourceColorCache = {
			source,
			overrides,
			customPalette,
			contextColor,
			gradientColors,
			gradientNumbers,
			rasterCompatible:
				activeBaseColors.every((color) => color.rgb !== null) &&
				Array.from(overrides.values()).every((color) => color.rgb !== null),
			rangeMinimum: Number.NaN,
			rangeMaximum: Number.NaN,
			denseMinimum: null,
			denseColors: null,
			sparseColors: new Map<number, CachedRenderColor>(),
		};
		this.sourceColorCache.set(source.id, result);
		return result;
	}

	/**
	 * Resets the bounded value cache when a rendering engine changes its color range.
	 * @param cache Source color cache to prepare.
	 * @param minimum Current gradient minimum.
	 * @param maximum Current gradient maximum.
	 * @returns Nothing.
	 */
	private prepareResolvedColorRange(
		cache: SourceColorCache,
		minimum: number,
		maximum: number,
	): void {
		if (cache.rangeMinimum === minimum && cache.rangeMaximum === maximum) return;
		cache.rangeMinimum = minimum;
		cache.rangeMaximum = maximum;
		cache.sparseColors.clear();
		const range = maximum - minimum;
		if (
			Number.isInteger(minimum) &&
			Number.isInteger(maximum) &&
			range >= 0 &&
			range <= MAX_DENSE_COLOR_RANGE
		) {
			cache.denseMinimum = minimum;
			cache.denseColors = new Array<CachedRenderColor | undefined>(range + 1);
			return;
		}
		cache.denseMinimum = null;
		cache.denseColors = null;
	}

	/**
	 * Interpolates parsed gradient stops without reparsing CSS strings per event pixel.
	 * @param cache Parsed source gradient cache.
	 * @param value Current numeric color value.
	 * @param minimum Gradient minimum.
	 * @param maximum Gradient maximum.
	 * @returns Concrete cached render color.
	 */
	private interpolateCachedGradient(
		cache: SourceColorCache,
		value: number,
		minimum: number,
		maximum: number,
	): CachedRenderColor {
		const ratio = Math.min(1, Math.max(0, (value - minimum) / (maximum - minimum)));
		const scaledIndex = ratio * (cache.gradientColors.length - 1);
		const lowerIndex = Math.floor(scaledIndex);
		const upperIndex = Math.min(
			lowerIndex + 1,
			cache.gradientColors.length - 1,
		);
		const factor = scaledIndex - lowerIndex;
		const firstNumber = cache.gradientNumbers[lowerIndex];
		const secondNumber = cache.gradientNumbers[upperIndex];
		if (!Number.isFinite(firstNumber) || !Number.isFinite(secondNumber)) {
			return cache.gradientColors[lowerIndex];
		}
		const red = Math.trunc(
			((firstNumber >> 16) & 0xff) +
				(((secondNumber >> 16) & 0xff) - ((firstNumber >> 16) & 0xff)) *
					factor,
		);
		const green = Math.trunc(
			((firstNumber >> 8) & 0xff) +
				(((secondNumber >> 8) & 0xff) - ((firstNumber >> 8) & 0xff)) *
					factor,
		);
		const blue = Math.trunc(
			(firstNumber & 0xff) +
				((secondNumber & 0xff) - (firstNumber & 0xff)) * factor,
		);
		const rgb = (red << 16) | (green << 8) | blue;
		return {
			css: `#${rgb.toString(16).padStart(6, "0")}`,
			rgb,
		};
	}

	/**
	 * Parses an opaque hexadecimal CSS color once while preserving its display string.
	 * @param css Concrete CSS color used by the canvas path.
	 * @returns Cached color with packed RGB when direct rasterization is safe.
	 */
	private createCachedRenderColor(css: string): CachedRenderColor {
		const stripped = css.trim().replace(/^#/, "");
		const expanded = /^[0-9a-f]{3}$/i.test(stripped)
			? stripped
					.split("")
					.map((character) => `${character}${character}`)
					.join("")
			: stripped;
		return {
			css,
			rgb: /^[0-9a-f]{6}$/i.test(expanded)
				? Number.parseInt(expanded, 16)
				: null,
		};
	}

	/**
	 * Normalizes worker-projected gradient colors to six-digit hexadecimal form.
	 * @param color Color with or without a leading hash.
	 * @returns Six-digit hexadecimal color, or the original CSS value when unsupported.
	 */
	private normalizeHexColor(color: string): string {
		const stripped = color.trim().replace(/^#/, "");
		if (/^[0-9a-f]{3}$/i.test(stripped)) {
			return `#${stripped
				.split("")
				.map((character) => `${character}${character}`)
				.join("")}`;
		}
		return /^[0-9a-f]{6}$/i.test(stripped) ? `#${stripped}` : color;
	}

	/**
	 * Returns a reusable worker-owned raster surface for one event-row height.
	 * @param width Current canvas width in pixels.
	 * @param height Raster buffer height in pixels.
	 * @returns Reusable raster surface, or null when OffscreenCanvas is unavailable.
	 */
	private getRowRasterSurface(
		width: number,
		height: number,
	): RowRasterSurface | null {
		if (typeof OffscreenCanvas === "undefined") return null;
		const existing = this.rowRasterSurfaces.get(height);
		if (existing?.canvas.width === width) return existing;

		const canvas = new OffscreenCanvas(width, height);
		const context = canvas.getContext("2d");
		if (!context) return null;
		context.imageSmoothingEnabled = false;
		const surface = {
			canvas,
			context,
			imageData: context.createImageData(width, height),
		};
		this.rowRasterSurfaces.set(height, surface);
		return surface;
	}

	/**
	 * Uploads one prepared pixel buffer and composites it onto the primary canvas once.
	 * @param surface Prepared worker-owned raster surface.
	 * @param rowTop Destination row top in canvas coordinates.
	 * @param destinationHeight Destination height matching legacy event geometry.
	 * @returns Nothing.
	 */
	private paintRasterSurface(
		surface: RowRasterSurface,
		rowTop: number,
		destinationHeight: number,
	): void {
		surface.context.putImageData(surface.imageData, 0, 0);
		this.context.drawImage(
			surface.canvas,
			0,
			0,
			surface.canvas.width,
			surface.canvas.height,
			0,
			rowTop,
			surface.canvas.width,
			destinationHeight,
		);
	}

	/**
	 * Writes one fully opaque pixel into a reusable raster buffer.
	 * @param pixels Target RGBA pixel buffer.
	 * @param width Raster width in pixels.
	 * @param x Horizontal pixel coordinate.
	 * @param y Vertical pixel coordinate.
	 * @param rgb Packed red, green, and blue channels.
	 * @returns Nothing.
	 */
	private writeOpaqueRasterPixel(
		pixels: Uint8ClampedArray,
		width: number,
		x: number,
		y: number,
		rgb: number,
	): void {
		if (x < 0 || x >= width || y < 0) return;
		const offset = (y * width + x) * 4;
		if (offset < 0 || offset + 3 >= pixels.length) return;
		pixels[offset] = (rgb >> 16) & 0xff;
		pixels[offset + 1] = (rgb >> 8) & 0xff;
		pixels[offset + 2] = rgb & 0xff;
		pixels[offset + 3] = 0xff;
	}

	/**
	 * Rasterizes one height-engine bar in timestamp order, retaining same-x overlays.
	 * @param pixels Target 47-pixel-high RGBA buffer.
	 * @param width Raster width in pixels.
	 * @param x Horizontal pixel coordinate.
	 * @param barHeight Legacy bar height in pixels.
	 * @param rgb Packed opaque bar color.
	 * @returns Nothing.
	 */
	private writeHeightRasterBar(
		pixels: Uint8ClampedArray,
		width: number,
		x: number,
		barHeight: number,
		rgb: number,
	): void {
		if (x < 0 || x >= width) return;
		const top = GRAPH_DRAWABLE_HEIGHT - Math.min(GRAPH_DRAWABLE_HEIGHT, barHeight);
		const firstFullRow = Math.ceil(top);
		const partialCoverage = firstFullRow - top;
		if (partialCoverage > 0 && firstFullRow > 0) {
			this.blendRasterPixel(
				pixels,
				width,
				x,
				firstFullRow - 1,
				rgb,
				partialCoverage,
			);
		}
		for (
			let y = Math.max(0, firstFullRow);
			y < GRAPH_DRAWABLE_HEIGHT;
			y++
		) {
			this.writeOpaqueRasterPixel(pixels, width, x, y, rgb);
		}
	}

	/**
	 * Source-over composites a partially covered opaque pixel into the raster buffer.
	 * @param pixels Target RGBA pixel buffer.
	 * @param width Raster width in pixels.
	 * @param x Horizontal pixel coordinate.
	 * @param y Vertical pixel coordinate.
	 * @param rgb Packed opaque source color.
	 * @param coverage Fractional pixel coverage in the inclusive 0..1 range.
	 * @returns Nothing.
	 */
	private blendRasterPixel(
		pixels: Uint8ClampedArray,
		width: number,
		x: number,
		y: number,
		rgb: number,
		coverage: number,
	): void {
		const offset = (y * width + x) * 4;
		const destinationAlpha = pixels[offset + 3] / 255;
		const sourceAlpha = Math.min(1, Math.max(0, coverage));
		const outputAlpha = sourceAlpha + destinationAlpha * (1 - sourceAlpha);
		if (outputAlpha <= 0) return;
		const sourceRed = (rgb >> 16) & 0xff;
		const sourceGreen = (rgb >> 8) & 0xff;
		const sourceBlue = rgb & 0xff;
		const retainedDestinationAlpha = destinationAlpha * (1 - sourceAlpha);
		pixels[offset] = Math.round(
			(sourceRed * sourceAlpha +
				pixels[offset] * retainedDestinationAlpha) /
				outputAlpha,
		);
		pixels[offset + 1] = Math.round(
			(sourceGreen * sourceAlpha +
				pixels[offset + 1] * retainedDestinationAlpha) /
				outputAlpha,
		);
		pixels[offset + 2] = Math.round(
			(sourceBlue * sourceAlpha +
				pixels[offset + 2] * retainedDestinationAlpha) /
				outputAlpha,
		);
		pixels[offset + 3] = Math.round(outputAlpha * 255);
	}

	/**
	 * Draws source separator and context tint behind row content.
	 * @param source Source context appearance.
	 * @param centerY Row center coordinate.
	 * @param state Current canvas dimensions.
	 * @returns Nothing.
	 */
	private drawSourceRow(
		source: RenderSource,
		centerY: number,
		state: RenderFrameState,
	): void {
		const previousAlpha = this.context.globalAlpha;
		this.context.globalAlpha = Math.min(source.contextTintAlpha * 0.4, 0.18);
		this.context.fillStyle = source.contextColor;
		this.context.fillRect(
			0,
			centerY - ROW_CENTER_OFFSET,
			state.width,
			ROW_HEIGHT,
		);
		this.context.globalAlpha = previousAlpha;
		this.context.fillStyle = source.contextColor;
		this.context.fillRect(0, centerY + ROW_CENTER_OFFSET, state.width, 1);
	}

	/**
	 * Draws the subtle top edge used to identify the primary source row.
	 * @param centerY Primary row center coordinate.
	 * @param state Current canvas dimensions and palette.
	 * @returns Nothing.
	 */
	private drawPrimaryRowEdge(
		centerY: number,
		state: RenderFrameState,
	): void {
		const previousAlpha = this.context.globalAlpha;
		this.context.globalAlpha = 0.35;
		this.context.fillStyle = state.palette.border;
		this.context.fillRect(0, centerY - 25, state.width, 1);
		this.context.globalAlpha = previousAlpha;
	}

	/**
	 * Draws source bounds, totals, dates, counts, and loading decoration.
	 * @param source Source metadata.
	 * @param centerY Row center coordinate.
	 * @param state Current viewport and palette.
	 * @returns Nothing.
	 */
	private drawSourceLocals(
		source: RenderSource,
		centerY: number,
		state: RenderFrameState,
	): void {
		const maximumX = this.getPixelPosition(
			source.timestamp.max + source.offset,
			state,
		);
		const minimumX = this.getPixelPosition(
			source.timestamp.min + source.offset,
			state,
		);
		this.context.fillStyle = state.palette.fontAccent;
		this.context.fillRect(maximumX + 1, centerY - 24, 1, 47);
		this.context.fillRect(minimumX - 1, centerY - 24, 1, 47);

		const eventCount = (
			this.repository.getEvents(source.id)?.loadedCount ?? 0
		).toString();
		const lineY = [centerY - 6, centerY + 4, centerY + 14];
		this.context.font = '10px "GeistMono", monospace';
		this.context.textAlign = "left";
		this.drawLocalLabels(
			maximumX + 12,
			lineY,
			source.total,
			source.timestamp.max + source.offset,
			eventCount,
			state,
		);
		this.context.textAlign = "right";
		this.drawLocalLabels(
			minimumX - 12,
			lineY,
			source.total,
			source.timestamp.min + source.offset,
			eventCount,
			state,
		);
		if (source.loadingKind !== "none") {
			this.drawLoadingLine(centerY, state);
		}
	}

	/**
	 * Draws one side of a source's local boundary labels.
	 * @param x Shared label x coordinate.
	 * @param lineY Three label baselines.
	 * @param total Canonical source total.
	 * @param timestamp Boundary timestamp.
	 * @param eventCount Loaded event count.
	 * @param state Current palette and locale.
	 * @returns Nothing.
	 */
	private drawLocalLabels(
		x: number,
		lineY: number[],
		total: number,
		timestamp: number,
		eventCount: string,
		state: RenderFrameState,
	): void {
		this.context.fillStyle = state.palette.fontAccent;
		this.context.fillText(total.toString(), x, lineY[0]);
		this.context.fillStyle = state.palette.fontSecondary;
		this.context.fillText(
			this.formatShortDate(timestamp, state.localized.timeZone),
			x,
			lineY[1],
		);
		this.context.fillText(eventCount, x, lineY[2]);
	}

	/**
	 * Formats a timestamp using the existing day-month-year source label layout.
	 * @param timestamp Timestamp in milliseconds.
	 * @param timeZone IANA time zone selected for timestamp presentation.
	 * @returns Date label in the selected time zone.
	 */
	private formatShortDate(timestamp: number, timeZone: string): string {
		return formatTimestampInTimeZone(timestamp, "dd.MM.yyyy", timeZone);
	}

	/**
	 * Draws the dashed loading line across one source row.
	 * @param centerY Source row center.
	 * @param state Current canvas dimensions and palette.
	 * @returns Nothing.
	 */
	private drawLoadingLine(centerY: number, state: RenderFrameState): void {
		this.context.save();
		this.context.beginPath();
		this.context.strokeStyle = state.palette.fontAccent;
		this.context.setLineDash([5, 5]);
		this.context.moveTo(0, centerY);
		this.context.lineTo(state.width, centerY);
		this.context.stroke();
		this.context.restore();
	}

	/**
	 * Draws source name, loaded count, total, context, and localized loading suffixes.
	 * @param source Source metadata.
	 * @param centerY Source row center.
	 * @param state Current palette and localized strings.
	 * @returns Nothing.
	 */
	private drawSourceInfo(
		source: RenderSource,
		centerY: number,
		state: RenderFrameState,
	): void {
		const suffix =
			source.loadingKind === "ingestion"
				? state.localized.ingestingSuffix
				: source.loadingKind === "loading"
					? state.localized.loadingSuffix
					: "";
		const eventCount =
			this.repository.getEvents(source.id)?.loadedCount ?? 0;
		const contextLabel = source.contextLabel || state.localized.unknownContext;
		const lines = [
			{ text: `${source.name}${suffix}`, y: centerY + 4, color: state.palette.fontAccent },
			{ text: eventCount.toString(), y: centerY + 18, color: state.palette.fontAccent },
			{
				text: `${source.total} | ${contextLabel}${suffix}`,
				y: centerY - 10,
				color: state.palette.fontSecondary,
			},
		];
		this.context.font = '12px "Geist", sans-serif';
		this.context.textAlign = "left";
		for (const line of lines) {
			this.context.fillStyle = line.color;
			this.context.fillText(line.text, 10, line.y);
		}
	}

	/**
	 * Draws the horizontal target guide within its source row.
	 * @param source Candidate target source.
	 * @param centerY Source row center.
	 * @param state Current target and guide appearance.
	 * @returns Nothing.
	 */
	private drawTargetGuide(
		source: RenderSource,
		centerY: number,
		state: RenderFrameState,
	): void {
		if (state.target?.sourceId !== source.id) return;
		this.context.save();
		this.context.globalAlpha = 0.7;
		this.context.strokeStyle = state.targetGuideColor;
		this.context.lineWidth = 1;
		this.context.lineCap = "round";
		this.context.setLineDash([1, 3]);
		this.context.beginPath();
		this.context.moveTo(0, centerY - 0.5);
		this.context.lineTo(state.width, centerY - 0.5);
		this.context.stroke();
		this.context.restore();
	}

	/**
	 * Draws the vertical target marker across the full canvas.
	 * @param state Current target and timeline state.
	 * @returns Nothing.
	 */
	private drawTargetMarker(state: RenderFrameState): void {
		if (!state.target) return;
		const source = this.repository.getSource(state.target.sourceId);
		if (!source) return;
		const x = this.getPixelPosition(
			state.target.timestamp + source.offset,
			state,
		);
		this.context.save();
		this.context.globalAlpha = 0.7;
		this.context.strokeStyle = state.targetGuideColor;
		this.context.lineWidth = 1;
		this.context.lineCap = "round";
		this.context.setLineDash([1, 3]);
		this.context.beginPath();
		this.context.moveTo(x + 0.5, 0);
		this.context.lineTo(x + 0.5, state.height);
		this.context.stroke();
		this.context.restore();
	}

	/**
	 * Draws nested translucent highlight ranges with deterministic overlap depths.
	 * @param state Current timeline transform and canvas dimensions.
	 * @returns Nothing.
	 */
	private drawHighlights(state: RenderFrameState): void {
		const layout = this.getHighlightLayout(state.revisions.overlays);
		const highlights = layout.records;
		for (let index = 0; index < highlights.length; index++) {
			const highlight = highlights[index];
			const startX = this.getPixelPosition(highlight.start, state);
			const endX = this.getPixelPosition(highlight.end, state);
			const depth = layout.depths[index];
			this.context.fillStyle = layout.colors[index];
			this.context.fillRect(
				startX,
				32 * (depth + 1),
				endX - startX,
				state.height - 40 * (depth + 1),
			);
		}
	}

	/**
	 * Returns revision-scoped highlight depths and composed alpha colors.
	 * @param revision Current aggregate overlay revision.
	 * @returns Immutable highlight paint layout.
	 */
	private getHighlightLayout(revision: number): HighlightLayoutCache {
		const records = this.repository.getHighlights();
		if (
			this.highlightLayoutCache?.revision === revision &&
			this.highlightLayoutCache.records === records
		) {
			return this.highlightLayoutCache;
		}
		const layout = {
			revision,
			records,
			depths: this.computeHighlightDepths(records),
			colors: records.map((highlight) =>
				highlight.color.startsWith("#") && highlight.color.length === 7
					? `${highlight.color}${HIGHLIGHT_ALPHA}`
					: highlight.color,
			),
		};
		this.highlightLayoutCache = layout;
		return layout;
	}

	/**
	 * Computes the lowest non-conflicting vertical depth for every highlight.
	 * @param highlights Highlight records in paint order.
	 * @returns Depth index for each highlight.
	 */
	private computeHighlightDepths(
		highlights: readonly RenderHighlightRecord[],
	): number[] {
		const depths: number[] = [];
		for (let index = 0; index < highlights.length; index++) {
			const used = new Set<number>();
			for (let priorIndex = 0; priorIndex < index; priorIndex++) {
				const overlaps = !(
					highlights[index].end <= highlights[priorIndex].start ||
					highlights[index].start >= highlights[priorIndex].end
				);
				if (overlaps) used.add(depths[priorIndex]);
			}
			let depth = 0;
			while (used.has(depth)) depth++;
			depths.push(depth);
		}
		return depths;
	}

	/**
	 * Draws emphasized vertical markers for flagged events in visible source rows.
	 * @param sourceIndexes Ordered source-row lookup shared by all overlays.
	 * @param state Current viewport and palette.
	 * @returns Nothing.
	 */
	private drawFlags(
		sourceIndexes: ReadonlyMap<string, number>,
		state: RenderFrameState,
	): void {
		for (const flag of this.repository.getFlags()) {
			const rowIndex = sourceIndexes.get(flag.sourceId);
			const source = this.repository.getSource(flag.sourceId);
			if (rowIndex === undefined || !source) continue;
			const x = this.getPixelPosition(flag.timestamp + source.offset, state);
			if (x < -10 || x > state.width + 10) continue;
			const rowTop = this.getSourceCenterY(rowIndex, state) - 24;
			this.context.fillStyle = state.palette.fontAccent;
			this.context.fillRect(x - 1, rowTop - 2, 1, 51);
			this.context.fillRect(x, rowTop - 4, 1, 55);
			this.context.fillRect(x + 1, rowTop - 2, 1, 51);
		}
	}

	/**
	 * Groups and draws visible note badges for vertically visible sources.
	 * @param visibleSources Sources inside the y viewport.
	 * @param sourceIndexes Ordered source-row lookup shared by all overlays.
	 * @param state Current viewport, pointer, and palette state.
	 * @param hitRegions Mutable overlay hit index.
	 * @param statistics Mutable profiling counters.
	 * @returns Nothing.
	 */
	private drawNotes(
		visibleSources: RenderSource[],
		sourceIndexes: ReadonlyMap<string, number>,
		state: RenderFrameState,
		hitRegions: WorkerHitRegion[],
		statistics: RenderFrameStatistics,
	): void {
		const noteLayout = this.getNoteLayout(state.revisions.overlays);
		for (const source of visibleSources) {
			const sourceLayout = noteLayout.bySource.get(source.id);
			const rowIndex = sourceIndexes.get(source.id);
			if (!sourceLayout || rowIndex === undefined) continue;
			const minimum = this.getTimestamp(-128, state) - source.offset;
			const maximum =
				this.getTimestamp(state.width + 128, state) - source.offset;
			const startVisibleIndex = this.findFirstDescendingAtMost(
				sourceLayout.timestamps,
				maximum,
			);
			if (startVisibleIndex === sourceLayout.records.length) continue;
			const endVisibleIndex = this.findLastDescendingAtLeast(
				sourceLayout.timestamps,
				minimum,
				startVisibleIndex,
			);
			for (
				let startIndex = startVisibleIndex;
				startIndex <= endVisibleIndex;
			) {
				const startX = this.getPixelPosition(
					sourceLayout.records[startIndex].timestamp + source.offset,
					state,
				);
				let endIndex = startIndex;
				let endX = startX;
				while (
					endIndex + 1 <= endVisibleIndex
				) {
					const nextX = this.getPixelPosition(
						sourceLayout.records[endIndex + 1].timestamp + source.offset,
						state,
					);
					if (Math.abs(startX - nextX) > NOTE_SIZE) break;
					endIndex++;
					endX = nextX;
				}
				const group = sourceLayout.records.slice(startIndex, endIndex + 1);
				const centerX = (startX + endX) / 2;
				this.drawNoteBadge(
					group[0],
					group,
					group.length === 1 ? group[0].name : group.length.toString(),
					centerX,
					this.getSourceCenterY(rowIndex, state),
					state,
					hitRegions,
				);
				statistics.visibleNotes += group.length;
				startIndex = endIndex + 1;
			}
		}
	}

	/**
	 * Returns a revision-scoped source partition and descending timestamp index for notes.
	 * @param revision Current aggregate overlay revision.
	 * @returns Immutable note layout used for viewport range queries.
	 */
	private getNoteLayout(revision: number): NoteLayoutCache {
		const records = this.repository.getNotes();
		if (
			this.noteLayoutCache?.revision === revision &&
			this.noteLayoutCache.records === records
		) {
			return this.noteLayoutCache;
		}

		const mutableBySource = new Map<string, RenderNoteRecord[]>();
		for (const note of records) {
			const sourceNotes = mutableBySource.get(note.sourceId);
			if (sourceNotes) sourceNotes.push(note);
			else mutableBySource.set(note.sourceId, [note]);
		}
		const bySource = new Map<string, NoteSourceLayout>();
		for (const [sourceId, sourceNotes] of mutableBySource) {
			sourceNotes.sort((left, right) => right.timestamp - left.timestamp);
			bySource.set(sourceId, {
				records: sourceNotes,
				timestamps: Float64Array.from(
					sourceNotes,
					(note) => note.timestamp,
				),
			});
		}
		const layout = { revision, records, bySource };
		this.noteLayoutCache = layout;
		return layout;
	}

	/**
	 * Draws one note or grouped-note badge and registers its interaction rectangle.
	 * @param note Representative note for appearance and identity.
	 * @param group All notes represented by the badge.
	 * @param label Text displayed below the icon.
	 * @param centerX Badge center x coordinate.
	 * @param centerY Source row center y coordinate.
	 * @param state Current pointer and palette state.
	 * @param hitRegions Mutable overlay hit index.
	 * @returns Nothing.
	 */
	private drawNoteBadge(
		note: RenderNoteRecord,
		group: RenderNoteRecord[],
		label: string,
		centerX: number,
		centerY: number,
		state: RenderFrameState,
		hitRegions: WorkerHitRegion[],
	): void {
		const x = centerX + NOTE_OFFSET;
		const y = centerY + NOTE_OFFSET;
		const hovered = this.isPointInside(
			state.pointer.x,
			state.pointer.y,
			x,
			y,
			NOTE_SIZE,
			NOTE_SIZE,
			state.pointer.inside,
		);
		const accent =
			group.length > 1
				? state.palette.fontAccent
				: note.color || state.palette.fontAccent;
		this.context.save();
		if (hovered) {
			this.context.shadowColor = state.palette.border;
			this.context.shadowBlur = 12;
			this.context.lineWidth = 2;
			this.context.strokeStyle = state.palette.border;
		}
		this.drawRoundedRect(
			x,
			y,
			NOTE_SIZE,
			NOTE_SIZE,
			5,
			state.palette.border,
			state.palette.backgroundSecondary,
		);
		this.drawRoundedRect(
			x,
			y + NOTE_SIZE - 4,
			NOTE_SIZE,
			4,
			4,
			state.palette.border,
			accent,
		);
		if (hovered) this.context.stroke();
		this.drawIcon(
			group.length > 1 ? (note.groupIconKey ?? note.iconKey) : note.iconKey,
			state.palette.fontAccent,
			x + (NOTE_SIZE - 16) / 2,
			y + (NOTE_SIZE - 16) / 2 - 1,
			16,
		);
		this.drawTextLabel(
			label,
			x + NOTE_SIZE / 2,
			y + 26,
			120,
			state.palette.border,
			state,
		);
		this.context.restore();
		hitRegions.push({
			kind: "note",
			id: note.id,
			noteIds: group.map((groupNote) => groupNote.id),
			x,
			y,
			width: NOTE_SIZE,
			height: NOTE_SIZE,
		});
	}

	/**
	 * Draws visible link connections, endpoint dots, and interactive badges.
	 * @param sourceIndexes Ordered source-row lookup shared by all overlays.
	 * @param state Current viewport, pointer, and palette state.
	 * @param hitRegions Mutable overlay hit index.
	 * @param statistics Mutable profiling counters.
	 * @returns Nothing.
	 */
	private drawLinks(
		sourceIndexes: ReadonlyMap<string, number>,
		state: RenderFrameState,
		hitRegions: WorkerHitRegion[],
		statistics: RenderFrameStatistics,
	): void {
		const linkLayout = this.getLinkLayout(state.revisions.overlays);
		for (const link of linkLayout.records) {
			const endpoints = this.getRenderedLinkEndpoints(link);
			if (endpoints.length === 0) continue;
			const dots = this.linkDotScratch;
			dots.length = 0;
			let allOffLeft = true;
			let allOffRight = true;
			let missingSource = false;
			for (const endpoint of endpoints) {
				const source = this.repository.getSource(endpoint.sourceId);
				const rowIndex = sourceIndexes.get(endpoint.sourceId);
				if (!source || rowIndex === undefined) {
					missingSource = true;
					break;
				}
				const x = this.getPixelPosition(
					endpoint.timestamp + source.offset,
					state,
				);
				allOffLeft = allOffLeft && x < -50;
				allOffRight = allOffRight && x > state.width + 50;
				dots.push({
					x,
					y: rowIndex * ROW_HEIGHT + 20 - state.scrollY,
					color: link.color || state.palette.fontAccent,
				});
			}
			if (missingSource) continue;
			if (dots.length === 0) continue;
			if (allOffLeft || allOffRight) continue;
			statistics.visibleLinks++;
			this.drawConnection(dots);
			for (const dot of dots) this.drawDot(dot, state);
			if (dots.length === 1) {
				this.drawLinkBadge(link, dots[0].x, dots[0].y, state, hitRegions);
			} else {
				for (let index = 0; index < dots.length - 1; index++) {
					this.drawLinkBadge(
						link,
						(dots[index].x + dots[index + 1].x) / 2,
						(dots[index].y + dots[index + 1].y) / 2,
						state,
						hitRegions,
					);
				}
			}
		}
	}

	/**
	 * Returns link records for an immutable link overlay revision.
	 * @param revision Current aggregate overlay revision.
	 * @returns Revision-scoped link layout.
	 */
	private getLinkLayout(revision: number): LinkLayoutCache {
		const records = this.repository.getLinks();
		if (
			this.linkLayoutCache?.revision === revision &&
			this.linkLayoutCache.records === records
		) {
			return this.linkLayoutCache;
		}
		const layout = { revision, records };
		this.linkLayoutCache = layout;
		return layout;
	}

	/**
	 * Orders link endpoints by rendered timestamp using current source offsets.
	 * @param link Link whose endpoints should be connected.
	 * @returns Endpoints sorted in rendered timeline order.
	 */
	private getRenderedLinkEndpoints(
		link: RenderLinkRecord,
	): ReadonlyArray<RenderLinkRecord["endpoints"][number]> {
		if (link.endpoints.length <= 1) return link.endpoints;
		return [...link.endpoints].sort((left, right) => {
			const leftSource = this.repository.getSource(left.sourceId);
			const rightSource = this.repository.getSource(right.sourceId);
			const timestampDifference =
				left.timestamp +
				(leftSource?.offset ?? 0) -
				(right.timestamp + (rightSource?.offset ?? 0));
			return (
				timestampDifference ||
				left.sortKey.localeCompare(right.sortKey)
			);
		});
	}

	/**
	 * Draws one link badge and registers its hit-test rectangle.
	 * @param link Link represented by this badge.
	 * @param centerX Badge center x coordinate.
	 * @param centerY Badge center y coordinate.
	 * @param state Current pointer and palette state.
	 * @param hitRegions Mutable overlay hit index.
	 * @returns Nothing.
	 */
	private drawLinkBadge(
		link: RenderLinkRecord,
		centerX: number,
		centerY: number,
		state: RenderFrameState,
		hitRegions: WorkerHitRegion[],
	): void {
		const size = 24;
		const x = centerX - size / 2;
		const y = centerY - size / 2;
		const color = link.color || state.palette.fontAccent;
		const hovered = this.isPointInside(
			state.pointer.x,
			state.pointer.y,
			x,
			y,
			size,
			size,
			state.pointer.inside,
		);
		this.context.save();
		if (hovered) {
			this.context.shadowColor = color;
			this.context.shadowBlur = 12;
			this.context.lineWidth = 2;
			this.context.strokeStyle = state.palette.fontAccent;
		}
		this.drawRoundedRect(
			x,
			y,
			size,
			size,
			4,
			color,
			state.palette.backgroundSecondary,
		);
		if (hovered) this.context.stroke();
		this.drawIcon(link.iconKey, color, centerX - 8, centerY - 8, 16);
		this.context.restore();
		hitRegions.push({
			kind: "link",
			id: link.id,
			x,
			y,
			width: size,
			height: size,
		});
	}

	/**
	 * Draws an icon bitmap or issues one deduplicated asset request.
	 * @param assetKey Stable glyph identifier.
	 * @param color Concrete glyph color.
	 * @param x Destination x coordinate.
	 * @param y Destination y coordinate.
	 * @param size Destination width and height.
	 * @returns Nothing.
	 */
	private drawIcon(
		assetKey: string,
		color: string,
		x: number,
		y: number,
		size: number,
	): void {
		const bitmap = this.repository.getAsset(assetKey, color);
		if (bitmap) {
			this.context.drawImage(bitmap, x, y, size, size);
			return;
		}
		this.drawIconPlaceholder(color, x, y, size);
		const requestKey = this.getAssetRequestKey(assetKey, color);
		if (!this.pendingAssetRequests.has(requestKey)) {
			this.pendingAssetRequests.add(requestKey);
			this.callbacks.requestAsset(assetKey, color);
		}
	}

	/**
	 * Paints a lightweight canvas fallback while an icon bitmap is loading or unavailable.
	 * @param color Fallback stroke color.
	 * @param x Destination x coordinate.
	 * @param y Destination y coordinate.
	 * @param size Destination width and height.
	 * @returns Nothing.
	 */
	private drawIconPlaceholder(
		color: string,
		x: number,
		y: number,
		size: number,
	): void {
		const inset = Math.max(2, size * 0.16);
		const corner = Math.max(3, size * 0.22);
		this.context.save();
		this.context.strokeStyle = color;
		this.context.lineWidth = Math.max(1.5, size * 0.12);
		this.context.lineCap = "round";
		this.context.lineJoin = "round";
		this.context.beginPath();
		this.context.moveTo(x + inset, y + inset);
		this.context.lineTo(x + size - corner, y + inset);
		this.context.lineTo(x + size - inset, y + corner);
		this.context.lineTo(x + size - inset, y + size - inset);
		this.context.lineTo(x + inset, y + size - inset);
		this.context.closePath();
		this.context.stroke();
		this.context.beginPath();
		this.context.moveTo(x + size - corner, y + inset);
		this.context.lineTo(x + size - corner, y + corner);
		this.context.lineTo(x + size - inset, y + corner);
		this.context.stroke();
		this.context.restore();
	}

	/**
	 * Builds a stable pending-request key for a glyph and color.
	 * @param assetKey Stable glyph identifier.
	 * @param color Concrete glyph color.
	 * @returns Request-deduplication key.
	 */
	private getAssetRequestKey(assetKey: string, color: string): string {
		return `${assetKey}\u0000${color}`;
	}

	/**
	 * Draws a rounded badge label with width-aware ellipsis.
	 * @param text Full label text.
	 * @param centerX Label center x coordinate.
	 * @param y Anchor y coordinate matching the legacy renderer.
	 * @param maxWidth Maximum label width.
	 * @param accent Border color.
	 * @param state Current palette.
	 * @returns Nothing.
	 */
	private drawTextLabel(
		text: string,
		centerX: number,
		y: number,
		maxWidth: number,
		accent: string,
		state: RenderFrameState,
	): void {
		this.context.font = '10px "GeistMono", sans-serif';
		const padding = 5;
		const measuredWidth = this.context.measureText(text).width;
		const labelWidth = Math.min(
			Math.max(measuredWidth + padding * 2, NOTE_SIZE),
			maxWidth,
		);
		const labelHeight = 20;
		this.drawRoundedRect(
			centerX - labelWidth / 2,
			y + NOTE_SIZE - labelHeight - 2,
			labelWidth,
			labelHeight,
			5,
			accent,
			state.palette.backgroundSecondary,
		);
		let displayText = text;
		while (
			displayText.length > 0 &&
			this.context.measureText(`${displayText}...`).width > maxWidth - padding * 2
		) {
			displayText = displayText.slice(0, -1);
		}
		if (displayText !== text) displayText += "...";
		this.context.fillStyle = state.palette.fontAccent;
		this.context.textAlign = "center";
		this.context.textBaseline = "middle";
		this.context.fillText(
			displayText,
			centerX,
			y + NOTE_SIZE - labelHeight / 2 - 2,
		);
	}

	/**
	 * Draws a filled and stroked rounded rectangle with a rectangular fallback.
	 * @param x Rectangle x coordinate.
	 * @param y Rectangle y coordinate.
	 * @param width Rectangle width.
	 * @param height Rectangle height.
	 * @param radius Corner radius.
	 * @param accent Stroke color.
	 * @param fill Fill color.
	 * @returns Nothing.
	 */
	private drawRoundedRect(
		x: number,
		y: number,
		width: number,
		height: number,
		radius: number,
		accent: string,
		fill: string,
	): void {
		this.context.fillStyle = fill;
		this.context.strokeStyle = accent;
		this.context.lineWidth = 1;
		this.context.beginPath();
		if (typeof this.context.roundRect === "function") {
			this.context.roundRect(x, y, width, height, radius);
		} else {
			this.context.rect(x, y, width, height);
		}
		this.context.fill();
		this.context.stroke();
	}

	/**
	 * Draws a bordered endpoint dot.
	 * @param dot Dot coordinates and inner color.
	 * @param state Current palette.
	 * @returns Nothing.
	 */
	private drawDot(dot: RenderDot, state: RenderFrameState): void {
		this.drawDotCoordinates(dot.x, dot.y, dot.color, state);
	}

	/**
	 * Draws a bordered endpoint dot without allocating an intermediate record.
	 * @param x Dot horizontal coordinate.
	 * @param y Dot vertical coordinate.
	 * @param color Dot inner color.
	 * @param state Current palette.
	 * @returns Nothing.
	 */
	private drawDotCoordinates(
		x: number,
		y: number,
		color: string,
		state: RenderFrameState,
	): void {
		this.context.fillStyle = state.palette.fontAccent;
		this.context.beginPath();
		this.context.arc(x, y, 4, 0, Math.PI * 2);
		this.context.fill();
		this.context.fillStyle = color;
		this.context.beginPath();
		this.context.arc(x, y, 3, 0, Math.PI * 2);
		this.context.fill();
	}

	/**
	 * Connects adjacent dots using per-segment linear gradients.
	 * @param dots Ordered connection points.
	 * @returns Nothing.
	 */
	private drawConnection(dots: RenderDot[]): void {
		if (dots.length < 2) return;
		for (let index = 0; index < dots.length - 1; index++) {
			const start = dots[index];
			const end = dots[index + 1];
			this.drawConnectionSegment(
				start.x,
				start.y,
				start.color,
				end.x,
				end.y,
				end.color,
			);
		}
	}

	/**
	 * Draws one gradient connection segment from scalar coordinates.
	 * @param startX Start horizontal coordinate.
	 * @param startY Start vertical coordinate.
	 * @param startColor Start color.
	 * @param endX End horizontal coordinate.
	 * @param endY End vertical coordinate.
	 * @param endColor End color.
	 * @returns Nothing.
	 */
	private drawConnectionSegment(
		startX: number,
		startY: number,
		startColor: string,
		endX: number,
		endY: number,
		endColor: string,
	): void {
		const gradient = this.context.createLinearGradient(
			startX,
			startY,
			endX,
			endY,
		);
		gradient.addColorStop(0, startColor);
		gradient.addColorStop(1, endColor);
		this.context.lineWidth = 2;
		this.context.strokeStyle = gradient;
		this.context.beginPath();
		this.context.moveTo(startX, startY);
		this.context.lineTo(endX, endY);
		this.context.stroke();
	}

	/**
	 * Tests whether a pointer lies inside one interaction rectangle.
	 * @param pointerX Pointer x coordinate.
	 * @param pointerY Pointer y coordinate.
	 * @param x Rectangle x coordinate.
	 * @param y Rectangle y coordinate.
	 * @param width Rectangle width.
	 * @param height Rectangle height.
	 * @param pointerInside Whether the pointer is currently over the canvas.
	 * @returns True when the pointer is inside the rectangle.
	 */
	private isPointInside(
		pointerX: number,
		pointerY: number,
		x: number,
		y: number,
		width: number,
		height: number,
		pointerInside: boolean,
	): boolean {
		return (
			pointerInside &&
			pointerX >= x &&
			pointerX <= x + width &&
			pointerY >= y &&
			pointerY <= y + height
		);
	}

	/**
	 * Draws alternating ruler backgrounds and returns tick metadata for the header.
	 * @param state Current timeline viewport and palette.
	 * @returns Visible ruler sections.
	 */
	private drawRulerWalls(state: RenderFrameState): RulerSection[] {
		const visibleStart = this.getTimestamp(0, state);
		const visibleEnd = this.getTimestamp(state.width, state);
		const [step, unit, value] = this.getRulerStep(visibleEnd - visibleStart);
		if (step <= 0 || !Number.isFinite(step)) return [];
		const roundedStart = Math.floor(visibleStart / step) * step;
		const roundedEnd = Math.ceil(visibleEnd / step) * step;
		const sections: RulerSection[] = [];
		let timestamp = roundedStart;
		let even = Math.floor(timestamp / step) % 2 === 0;
		while (timestamp <= roundedEnd && sections.length < 256) {
			sections.push({
				timestamp,
				position: this.getPixelPosition(timestamp, state),
				step,
				unit,
				value,
				even,
			});
			timestamp += step;
			even = !even;
		}
		for (let index = 0; index < sections.length; index++) {
			const section = sections[index];
			const nextPosition = sections[index + 1]?.position ?? state.width;
			this.context.fillStyle = section.even
				? state.palette.backgroundAccent
				: state.palette.backgroundSecondary;
			this.context.fillRect(
				section.position,
				RULER_HEIGHT,
				nextPosition - section.position,
				state.height - RULER_HEIGHT,
			);
			this.context.beginPath();
			this.context.moveTo(section.position, 0);
			this.context.lineTo(section.position, state.height);
			this.context.strokeStyle = state.palette.border;
			this.context.stroke();
		}
		return sections;
	}

	/**
	 * Selects the same 4..40-section ruler interval used by the legacy drawer.
	 * @param totalMilliseconds Visible timeline duration.
	 * @returns Millisecond step, display unit, and unit value.
	 */
	private getRulerStep(totalMilliseconds: number): [number, string, number] {
		const intervals: Array<{
			unit: string;
			values: number[];
			multiplier: number;
		}> = [
			{ unit: "nanoseconds", values: [1, 10, 100, 500], multiplier: 0.001 },
			{ unit: "milliseconds", values: [1, 10, 100, 250, 500], multiplier: 1 },
			{ unit: "seconds", values: [1, 2, 5, 10, 15, 30], multiplier: 1_000 },
			{ unit: "minutes", values: [1, 2, 5, 10, 15, 30], multiplier: 60_000 },
			{ unit: "hours", values: [1, 3, 6, 12], multiplier: 3_600_000 },
			{ unit: "days", values: [1, 2, 3, 7, 14], multiplier: 86_400_000 },
			{ unit: "months", values: [1, 3, 6], multiplier: 2_592_000_000 },
			{
				unit: "years",
				values: [1, 2, 3, 5, 10, 25, 50, 100],
				multiplier: 31_536_000_000,
			},
		];
		let result: [number, string, number] = [0, "nanoseconds", 0];
		let bestCount = Infinity;
		for (const interval of intervals) {
			for (const value of interval.values) {
				const step = interval.multiplier * value;
				const count = totalMilliseconds / step;
				if (count >= 4 && count <= 40 && count < bestCount) {
					bestCount = count;
					result = [step, interval.unit, value];
				}
			}
		}
		return result;
	}

	/**
	 * Draws ruler header backgrounds, timestamps, interval labels, and separator.
	 * @param sections Tick metadata created before row rendering.
	 * @param state Current canvas palette and locale.
	 * @returns Nothing.
	 */
	private drawRulerSections(
		sections: RulerSection[],
		state: RenderFrameState,
	): void {
		this.context.font = "12px Arial";
		for (let index = 0; index < sections.length; index++) {
			const section = sections[index];
			const nextPosition = sections[index + 1]?.position ?? state.width;
			this.context.fillStyle = section.even
				? state.palette.backgroundAccent
				: state.palette.backgroundSecondary;
			this.context.fillRect(
				section.position,
				0,
				nextPosition - section.position,
				RULER_HEIGHT,
			);
			this.context.fillStyle = state.palette.fontAccent;
			this.context.textAlign = "left";
			this.context.textBaseline = "alphabetic";
			this.context.fillText(
				this.formatRulerTimestamp(section.timestamp, section.step, state),
				section.position + 6,
				16,
			);
			this.context.fillStyle = state.palette.fontSecondary;
			this.context.textAlign = "right";
			this.context.fillText(
				`${section.value} ${section.unit}`,
				nextPosition - 6,
				16,
			);
			this.context.beginPath();
			this.context.moveTo(section.position, 0);
			this.context.lineTo(section.position, RULER_HEIGHT + 1);
			this.context.strokeStyle = state.palette.border;
			this.context.stroke();
		}
		this.context.beginPath();
		this.context.moveTo(0, RULER_HEIGHT);
		this.context.lineTo(state.width, RULER_HEIGHT);
		this.context.strokeStyle = state.palette.border;
		this.context.stroke();
	}

	/**
	 * Formats one ruler timestamp according to the active interval size.
	 * @param timestamp Timestamp in milliseconds.
	 * @param step Ruler interval duration.
	 * @param state Current locale and time-zone payload.
	 * @returns Legacy-compatible ruler label.
	 */
	private formatRulerTimestamp(
		timestamp: number,
		step: number,
		state: RenderFrameState,
	): string {
		const day = 86_400_000;
		const hour = 3_600_000;
		const minute = 60_000;
		let pattern = "HH:mm:ss dd.MM.yyyy";
		if (step >= day) pattern = "dd.MM.yyyy";
		else if (step >= hour) pattern = "HH:mm dd.MM";
		else if (step >= minute) pattern = "HH:mm:ss dd.MM";
		return formatTimestampInTimeZone(
			timestamp,
			pattern,
			state.localized.timeZone,
		);
	}

	/**
	 * Draws the selected frame's hard left and right boundaries.
	 * @param state Current frame transform and palette.
	 * @returns Nothing.
	 */
	private drawFrameEdges(state: RenderFrameState): void {
		this.context.fillStyle = state.palette.border;
		this.context.fillRect(
			this.getPixelPosition(state.frame.min, state) - 2,
			0,
			3,
			state.height,
		);
		this.context.fillRect(
			this.getPixelPosition(state.frame.max, state) + 2,
			0,
			3,
			state.height,
		);
	}
}
