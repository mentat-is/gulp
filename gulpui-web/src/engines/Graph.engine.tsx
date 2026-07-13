import { Source } from "@/entities/Source";
import { Engine, Hardcode } from "../class/Engine.dto";
import { Dot, RenderEngine } from "../class/RenderEngine";
import { Color } from "@/entities/Color";

const GRAPH_DRAWABLE_HEIGHT = 47;
const SEMI_LOG_BASE = 10;

interface GraphBucketCache {
	key: string;
	buckets: Map<number, number>;
	maxHeight: number;
	sampleCount: number;
	bucketCount: number;
}

/**
 * Graph render engine: draws connected line graphs for event density over time.
 * Caches per-source graph points (Map<timestamp, height>) with scale/range metadata.
 * Singleton pattern — reused across frames.
 */
export class GraphEngine implements Engine.Interface<
	typeof GraphEngine.target
> {
	/** Reference to the parent RenderEngine. */
	private renderer!: RenderEngine;
	/** Singleton instance, accessible for cache clearing in clearAllCaches(). */
	public static instance: GraphEngine | null = null;
	/** Type definition for the per-source graph map with metadata symbols. */
	private static target: Map<number, number> & {
		[Hardcode.Scale]: number;
		[Hardcode.MaxHeight]: number;
		[Hardcode.Start]: number;
		[Hardcode.End]: number;
	};
	/** Per-source cache of graph data. Key: source ID, Value: graph point map. */
	map = new Map<Source.Id, typeof GraphEngine.target>();
	private dynamicBucketCache = new Map<Source.Id, GraphBucketCache>();

	constructor(renderer: Engine.Constructor) {
		if (GraphEngine.instance) {
			GraphEngine.instance.renderer = renderer;
			return GraphEngine.instance;
		}
		this.renderer = renderer;
		GraphEngine.instance = this;
	}

	/** Updates the renderer reference without constructor overhead. Called per frame. */
	updateRenderer(renderer: Engine.Constructor) {
		this.renderer = renderer;
	}

	/**
	 * Renders the source density graph by aggregating visible samples into dynamic
	 * buckets and drawing one semi-log-scaled dot per bucket.
	 *
	 * @param file The source to render.
	 * @param y The top Y coordinate of the source row.
	 * @returns Nothing; draws directly on the canvas context.
	 */
	render(file: Source.Type, y: number) {
		if (y < -48 || y > this.renderer.ctx.canvas.height + 48) return;

		const sampleData =
			Source.Entity.samples(this.renderer.info.app, file) ?? [];
		if (sampleData.length === 0) return;

		const freq = file.settings.frequency_sample;
		const frame = this.renderer.info.app.timeline.frame;
		const timePer10Px =
			(10 * (frame.max - frame.min)) / this.renderer.info.width;

		const dynamicFrequency = Math.max(freq, timePer10Px);
		if (!Number.isFinite(dynamicFrequency) || dynamicFrequency <= 0) return;
		const offset = file.timestamp.min;
		const bucketCache = this.getDynamicBuckets(
			file,
			sampleData,
			dynamicFrequency,
			offset,
		);
		const dynamicBuckets = bucketCache.buckets;
		const maxHeight = bucketCache.maxHeight;
		this.renderer.stats.graphSamples += bucketCache.sampleCount;
		this.renderer.stats.graphBuckets += bucketCache.bucketCount;

		if (maxHeight === 0) return;

		const semiLogMaxHeight = this.getSemiLogCeiling(maxHeight);
		const semiLogMaxValue = this.getSemiLogValue(semiLogMaxHeight);
		const minLimit = this.renderer.limits.min;
		const maxLimit = this.renderer.limits.max;

		const startBucketIdx =
			Math.floor((minLimit - offset) / dynamicFrequency) - 1;
		const endBucketIdx = Math.floor((maxLimit - offset) / dynamicFrequency) + 1;

		// Strict bounds to file's timeline
		const fileStartBucketIdx = 0; // Relative to offset, the file starts at bucket 0
		// Use Math.ceil(x) - 1 to find the precise ending bucket index and avoid an extra trailing zero-bucket if max lands perfectly on a boundary
		const fileEndBucketIdx = Math.max(
			0,
			Math.ceil((file.timestamp.max - offset) / dynamicFrequency) - 1,
		);

		const actualStartBucketIdx = Math.max(startBucketIdx, fileStartBucketIdx);
		const actualEndBucketIdx = Math.min(endBucketIdx, fileEndBucketIdx);

		let prevDot: Dot | null = null;

		for (let i = actualStartBucketIdx; i <= actualEndBucketIdx; i++) {
			let centerTimestamp =
				offset + i * dynamicFrequency + dynamicFrequency / 2;

			// Strictly clamp the rendered dot to the file's absolute boundaries so it never spills over the file's start/end
			if (centerTimestamp < file.timestamp.min)
				centerTimestamp = file.timestamp.min;
			if (centerTimestamp > file.timestamp.max)
				centerTimestamp = file.timestamp.max;

			const x = this.renderer.getPixelPosition(centerTimestamp);

			if (x < -32 || x > this.renderer.ctx.canvas.width + 32)
				continue;

			const count = dynamicBuckets.get(i) || 0;

			this.renderer.ctx.strokeStyle = Color.Themer.theme.BORDER;
			const startLine = this.renderer.getPixelPosition(
				offset + i * dynamicFrequency,
			);
			this.renderer.ctx.beginPath();
			this.renderer.ctx.moveTo(startLine, y);
			this.renderer.ctx.lineTo(startLine, y + 48);
			this.renderer.ctx.stroke();

			const currentDot = this.drawDot(y, count, semiLogMaxValue, file, x);
			if (prevDot) {
				this.renderer.connection([prevDot, currentDot]);
			}
			prevDot = currentDot;
		}
	}

	/**
	 * Returns cached dynamic buckets for a graph source at the current zoom frequency.
	 * @param file Source being rendered.
	 * @param sampleData Source sample data used to build graph buckets.
	 * @param dynamicFrequency Current zoom-derived bucket frequency.
	 * @param offset Source minimum timestamp used as relative bucket origin.
	 * @returns Cached or newly built bucket data.
	 */
	private getDynamicBuckets(
		file: Source.Type,
		sampleData: Array<{ min_timestamp: number; sample: number }>,
		dynamicFrequency: number,
		offset: number,
	): GraphBucketCache {
		const normalizedFrequency = Math.max(
			0.001,
			Math.round(dynamicFrequency * 1000) / 1000,
		);
		const key = [
			this.getSampleDataSignature(sampleData),
			normalizedFrequency,
			offset,
			file.timestamp.max,
		].join(":");
		const cached = this.dynamicBucketCache.get(file.id);
		if (cached?.key === key) {
			return cached;
		}

		const dynamicBuckets = new Map<number, number>();
		let maxHeight = 0;

		for (let i = 0; i < sampleData.length; i++) {
			const relativeTs = sampleData[i].min_timestamp - offset;
			const bucketIndex = Math.floor(relativeTs / normalizedFrequency);
			const newCount =
				(dynamicBuckets.get(bucketIndex) || 0) + sampleData[i].sample;
			dynamicBuckets.set(bucketIndex, newCount);
			if (newCount > maxHeight) maxHeight = newCount;
		}

		const nextCache: GraphBucketCache = {
			key,
			buckets: dynamicBuckets,
			maxHeight,
			sampleCount: sampleData.length,
			bucketCount: dynamicBuckets.size,
		};
		this.dynamicBucketCache.set(file.id, nextCache);
		return nextCache;
	}

	/**
	 * Builds a stable signature for the sample data used by the graph renderer.
	 * @param sampleData Source sample data.
	 * @returns Signature that changes when the sampled timeline changes.
	 */
	private getSampleDataSignature(
		sampleData: Array<{ min_timestamp: number; sample: number }>,
	): string {
		const first = sampleData[0];
		const last = sampleData[sampleData.length - 1];
		return [
			sampleData.length,
			first?.min_timestamp ?? 0,
			first?.sample ?? 0,
			last?.min_timestamp ?? 0,
			last?.sample ?? 0,
		].join(":");
	}

	/**
	 * Returns the next power-of-10 ceiling used as the maximum on the semi-log scale.
	 *
	 * @param maxHeight The highest bucket count in the current render pass.
	 * @returns A power of 10 greater than or equal to maxHeight.
	 */
	private getSemiLogCeiling(maxHeight: number): number {
		if (maxHeight <= 1) return 1;

		return SEMI_LOG_BASE ** Math.ceil(Math.log10(maxHeight));
	}

	/**
	 * Converts a bucket count into the semi-log plotting domain.
	 *
	 * @param count The bucket count or semi-log ceiling to convert.
	 * @returns The base-10 log value used for graph normalization.
	 */
	private getSemiLogValue(count: number): number {
		if (count <= 0) return 0;

		return Math.log10(count + 1);
	}

	/**
	 * Draws a graph point using semi-log normalization for both vertical position
	 * and color intensity so dense buckets remain distinguishable at large scale.
	 *
	 * @param y The top Y coordinate of the source row.
	 * @param count The aggregated sample count for the current bucket.
	 * @param semiLogMaxValue The maximum base-10 log value derived from maxHeight.
	 * @param file The source being rendered.
	 * @param x The canvas X coordinate for the current bucket center.
	 * @returns The rendered dot coordinates and color used for line connections.
	 */
	private drawDot(
		y: number,
		count: number,
		semiLogMaxValue: number,
		file: Source.Type,
		x: number,
	) {
		const bottomY = y + GRAPH_DRAWABLE_HEIGHT;
		const semiLogCount = this.getSemiLogValue(count);
		const heightRatio =
			semiLogMaxValue > 0 ? semiLogCount / semiLogMaxValue : count;
		const dotY =
			bottomY - Math.floor(Math.min(heightRatio, 1) * GRAPH_DRAWABLE_HEIGHT);
		const color = Source.Entity.resolveColor(
			file,
			semiLogCount,
			{
				min: 0,
				max: semiLogMaxValue,
			},
		);

		const currentDot = { x, y: dotY, color };

		this.renderer.ctx.font = "8px Arial";
		this.renderer.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
		this.renderer.ctx.fillText(count.toString(), x - 3.5, dotY - 8);
		this.renderer.dot(currentDot);

		return currentDot;
	}

	get(file: Source.Type): typeof GraphEngine.target {
		const heightData = this.renderer.height.get(file);
		const result = new Map() as typeof GraphEngine.target;

		const entries = Array.from(heightData.entries());
		let lastRenderedX = -Infinity;
		let lastKey: number | undefined = undefined;

		for (let i = 0; i < entries.length; i++) {
			const [timestamp, height] = entries[i];
			const x = this.renderer.getPixelPosition(timestamp);

			if (x - lastRenderedX < 8) {
				if (lastKey !== undefined) {
					const prevHeight = result.get(lastKey) || 0;
					result.set(lastKey, prevHeight + height);
				}
				continue;
			}

			const prevEntry = i > 0 ? entries[i - 1] : null;

			if (prevEntry) {
				const [prevTimestamp] = prevEntry;
				const prevX = this.renderer.getPixelPosition(prevTimestamp);
				const gap = Math.abs(x - prevX);

				if (gap > 16) {
					const steps = Math.floor(gap / 16);
					const timestampStep = (timestamp - prevTimestamp) / (steps + 1);

					for (let step = 1; step <= steps; step++) {
						const fillTimestamp = prevTimestamp + timestampStep * step;
						const fillX = this.renderer.getPixelPosition(fillTimestamp);

						if (fillX - lastRenderedX >= 8) {
							result.set(fillTimestamp, 0);
							lastKey = fillTimestamp;
							lastRenderedX = fillX;
						}
					}
				}
			}

			result.set(timestamp, height);
			lastKey = timestamp;
			lastRenderedX = x;
		}

		const heights = Array.from(result.values());
		const timestamps = Array.from(result.keys());

		result[Hardcode.Scale] = this.renderer.info.app.timeline.scale;
		result[Hardcode.MaxHeight] = Math.max(...heights, 0);
		result[Hardcode.Start] = timestamps[0] ?? 0;
		result[Hardcode.End] = timestamps[timestamps.length - 1] ?? 0;

		this.map.set(file.id, result);

		return result;
	}

	is(file: Source.Type): boolean {
		return this.map.has(file.id);
	}

	/**
	 * Clears cached graph aggregation for a specific source.
	 * @param sourceId Source identifier to clear.
	 * @returns Nothing.
	 */
	clearSourceCache(sourceId: Source.Id): void {
		this.map.delete(sourceId);
		this.dynamicBucketCache.delete(sourceId);
	}

	/**
	 * Clears all cached graph aggregations.
	 * @returns Nothing.
	 */
	clearCache(): void {
		this.map.clear();
		this.dynamicBucketCache.clear();
	}
}
