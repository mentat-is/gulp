import { Source } from "@/entities/Source";
import { Engine, Hardcode } from "../class/Engine.dto";
import { RenderEngine } from "../class/RenderEngine";
import { throwableByTimestamp } from "@/ui/utils";
import { Doc } from "@/entities/Doc";

const SAMPLE_SIZE = 60000;

/**
 * Height-based render engine: draws vertical bars proportional to event density.
 * Caches per-source height maps (Map<timestamp, eventCount>).
 * Singleton pattern — reused across frames.
 */
export class HeightEngine implements Engine.Interface<
	typeof HeightEngine.target
> {
	/** Singleton instance, accessible for cache clearing in clearAllCaches(). */
	public static instance: HeightEngine | null = null;
	static target: Map<number, number> & {
		[Hardcode.MaxHeight]: number;
	};
	/** Reference to the parent RenderEngine. */
	private renderer!: RenderEngine;
	/** Per-source cache of height data. Key: source ID, Value: height map. */
	map = new Map<Source.Id, typeof HeightEngine.target>();

	constructor(renderer: Engine.Constructor) {
		if (HeightEngine.instance) {
			HeightEngine.instance.renderer = renderer;
			return HeightEngine.instance;
		}
		this.renderer = renderer;
		HeightEngine.instance = this;
	}

	/** Updates the renderer reference without constructor overhead. Called per frame. */
	updateRenderer(renderer: Engine.Constructor) {
		this.renderer = renderer;
	}

	render(file: Source.Type, y: number) {
		const samples = this.get(file);
		const height = samples[Hardcode.MaxHeight];

		const { min: visibleStart, max: visibleEnd } = this.renderer.limits;

		for (const [timestamp, amount] of samples) {
			const adjustedTime = timestamp + file.settings.offset;

			if (
				adjustedTime < visibleStart ||
				adjustedTime > visibleEnd ||
				throwableByTimestamp(
					adjustedTime,
					this.renderer.limits,
					this.renderer.info.app,
				) ||
				amount <= 0
			)
				continue;

			this.renderer.ctx.fillStyle = Source.Entity.resolveColor(
				file,
				amount,
				{ min: 0, max: height },
			);

			this.renderer.ctx.fillRect(
				this.renderer.getPixelPosition(adjustedTime),
				y + 47,
				1,
				-(1 + 46 * (amount / height)),
			);
		}
	}

	/** Per-source cache key strings, cleared via clearAllCaches() on operation switch. */
	public cacheKeys = new Map<Source.Id, string>();

	get(file: Source.Type) {
		const events = Source.Entity.events(this.renderer.info.app, file);
		const [minTime, maxTime] = [
			Math.min(file.timestamp.min, file.timestamp.max),
			Math.max(file.timestamp.min, file.timestamp.max),
		];

		// Check cache validity — recompute only when events or bounds changed
		const cacheKey = `${events.length}:${minTime}:${maxTime}`;
		const existingKey = this.cacheKeys.get(file.id);
		if (existingKey === cacheKey && this.map.has(file.id)) {
			return this.map.get(file.id)!;
		}

		const bucketCount = Math.ceil((maxTime - minTime) / SAMPLE_SIZE);
		const sampledData = new Map() as typeof HeightEngine.target;

		for (let i = 0; i <= bucketCount; i++) {
			const timestamp = minTime + i * SAMPLE_SIZE;
			if (timestamp <= maxTime) {
				sampledData.set(timestamp, 0);
			}
		}

		let maxHeight = 0;
		events.forEach((event) => {
			const eventTime = event.gulp_timestamp;
			if (eventTime < minTime || eventTime > maxTime) return;

			const bucketStart =
				minTime + Math.floor((eventTime - minTime) / SAMPLE_SIZE) * SAMPLE_SIZE;
			const newCount = (sampledData.get(bucketStart) || 0) + 1;
			sampledData.set(bucketStart, newCount);

			if (newCount > maxHeight) maxHeight = newCount;
		});

		sampledData[Hardcode.MaxHeight] = maxHeight;

		this.map.set(file.id, sampledData);
		this.cacheKeys.set(file.id, cacheKey);

		return sampledData;
	}

	is = () => Boolean();
}
