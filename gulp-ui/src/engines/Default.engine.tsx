import { Engine, Hardcode, CacheKey } from "../class/Engine.dto";
import { RenderEngine } from "../class/RenderEngine";
import { MinMax } from "@/class/Info";
import { Logger } from "@/dto/Logger.class";
import { Source } from "@/entities/Source";
import { Doc } from "@/entities/Doc";

interface OverrideIndexCache {
	eventsRef: Doc.Type[];
	eventCount: number;
	overrideLookupRef: Source.ColorOverride["lookup"];
	field: Source.Type["settings"]["field"];
	hashFunction: Source.Type["settings"]["hash_function"];
	indexes: number[];
}

export class DefaultEngine implements Engine.Interface<any> {
	static instance: DefaultEngine | null = null;
	private renderer!: RenderEngine;
	private overrideIndexCache = new Map<Source.Id, OverrideIndexCache>();

	map = new Map<Source.Id, any>();

	constructor(renderer: Engine.Constructor) {
		if (DefaultEngine.instance) {
			DefaultEngine.instance.renderer = renderer;
			return DefaultEngine.instance;
		}
		this.renderer = renderer;
		DefaultEngine.instance = this;
	}

	get: (file: Source.Type) => any = () => {};

	updateRenderer(renderer: Engine.Constructor) {
		this.renderer = renderer;
	}

	/**
	 * Draws one vertical event line for each visible canvas pixel bucket.
	 *
	 * @param file Source whose events should be rendered.
	 * @param y Top Y coordinate for the source event row.
	 * @param force Reserved render invalidation flag kept for engine interface compatibility.
	 * @returns Nothing.
	 */
	render(file: Source.Type, y: number, force?: boolean): void {
		//console.log(`render: ${file.name}`);
		//console.trace()
		const events = Source.Entity.events(this.renderer.info.app, file);
		if (!events || events.length === 0) return;

		const range = this.getRanges(file);
		const overrideEventIndexes = this.getOverrideEventIndexes(file, events);

		// Precompute hot-loop constants — avoids repeated property-chain traversal
		const ctx = this.renderer.ctx;
		const offset = file.settings.offset;
		const scrollX = this.renderer.scrollX;
		// Use ctx.canvas.width * scale (same denominator as getTimestamp), not Info.width
		// which queries the DOM and may differ if the canvas element ID lookup fails.
		const visibleWidth =
			ctx.canvas.width * this.renderer.info.app.timeline.scale;
		const frame = this.renderer.info.app.timeline.frame;
		const frameMin = frame.min;
		const frameRange = frame.max - frameMin;

		const minTimestampVisible = this.renderer.getTimestamp(-50) - offset;
		const maxTimestampVisible =
			this.renderer.getTimestamp(ctx.canvas.width + 50) - offset;

		// Events are stored in DESCENDING timestamp order (newest first).
		// Binary searches must account for this.

		// Find startIdx: smallest index where events[mid].timestamp <= maxTimestampVisible
		// (first event in the visible window, i.e. highest timestamp that is still on-screen)
		let startIdx = events.length; // default: nothing visible
		let left = 0,
			right = events.length - 1;
		while (left <= right) {
			const mid = (left + right) >>> 1;
			if (events[mid].gulp_timestamp <= maxTimestampVisible) {
				startIdx = mid;
				right = mid - 1; // try smaller index (higher timestamps live at lower indices)
			} else {
				left = mid + 1;
			}
		}
		if (startIdx === events.length) return;

		// Find endIdx: largest index where events[mid].timestamp >= minTimestampVisible
		// (last event in the visible window, i.e. lowest timestamp still on-screen)
		let endIdx = -1;
		left = startIdx;
		right = events.length - 1;
		while (left <= right) {
			const mid = (left + right) >>> 1;
			if (events[mid].gulp_timestamp >= minTimestampVisible) {
				endIdx = mid;
				left = mid + 1; // try larger index (lower timestamps live at higher indices)
			} else {
				right = mid - 1;
			}
		}
		if (endIdx === -1) return;

		let i = startIdx;

		while (i <= endIdx) {
			const timestamp = events[i].gulp_timestamp + offset;

			// Safety guard: skip events outside the selection frame
			if (timestamp > frame.max || timestamp < frame.min) {
				i++;
				continue;
			}

			const x = this.renderer.getPixelPosition(timestamp);

			// SKIP-AHEAD: events are descending in timestamp, so as i increases, x decreases.
			// After drawing pixel x, skip to the first event that maps to a pixel strictly
			// below x. Events at pixel x satisfy:
			//   timestamp >= frameMin + (x - 0.5 + scrollX) / visibleWidth * frameRange - offset
			// Events at pixel < x have timestamp below that threshold.
			// This reduces iterations from O(N_visible) to O(canvasWidth * log N).
			const nextThreshold =
				frameMin + ((x - 0.5 + scrollX) / visibleWidth) * frameRange - offset;
			const next = this.findNextEventIndexBelowPixel(
				events,
				i,
				endIdx,
				nextThreshold,
			);
			const overrideEventIndex = this.findFirstOverrideEventIndexInBucket(
				overrideEventIndexes,
				i,
				next,
			);
			const colorEventIndex =
				overrideEventIndex === -1 ? i : overrideEventIndex;
			const numberHash = events[colorEventIndex].number_hash;
			this.renderer.ctx.fillStyle = Source.Entity.resolveColor(
				file,
				numberHash,
				range,
			);
			this.renderer.ctx.fillRect(x, y, 1, 47);
			this.renderer.stats.renderedEventPixels++;
			i = next;
		}
	}

	/**
	 * Returns cached event indexes whose colors are explicitly overridden for one source.
	 *
	 * @param file Source whose override settings should be inspected.
	 * @param events Timestamp-sorted event array for the source.
	 * @returns Ascending event indexes that have override colors.
	 */
	private getOverrideEventIndexes(
		file: Source.Type,
		events: Doc.Type[],
	): number[] {
		const overrideLookup = file.settings.color_override?.lookup;
		if (!this.hasOverrideLookupValues(overrideLookup)) {
			this.overrideIndexCache.delete(file.id);
			return [];
		}

		const cached = this.overrideIndexCache.get(file.id);
		if (
			cached &&
			cached.eventsRef === events &&
			cached.eventCount === events.length &&
			cached.overrideLookupRef === overrideLookup &&
			cached.field === file.settings.field &&
			cached.hashFunction === file.settings.hash_function
		) {
			return cached.indexes;
		}

		const indexes: number[] = [];
		for (let i = 0; i < events.length; i++) {
			if (Source.Entity.resolveOverrideColor(file, events[i].number_hash)) {
				indexes.push(i);
			}
		}

		this.overrideIndexCache.set(file.id, {
			eventsRef: events,
			eventCount: events.length,
			overrideLookupRef: overrideLookup,
			field: file.settings.field,
			hashFunction: file.settings.hash_function,
			indexes,
		});

		return indexes;
	}

	/**
	 * Checks whether a color override lookup contains at least one usable color.
	 *
	 * @param overrideLookup Override lookup table from source settings.
	 * @returns True when the lookup has at least one configured color.
	 */
	private hasOverrideLookupValues(
		overrideLookup: Source.ColorOverride["lookup"] | undefined,
	): overrideLookup is Source.ColorOverride["lookup"] {
		if (!overrideLookup) {
			return false;
		}

		for (const key in overrideLookup) {
			if (overrideLookup[key]) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Finds the first event index below the current canvas pixel bucket.
	 *
	 * @param events Descending timestamp events for the source.
	 * @param startIndex Current visible event index.
	 * @param endIndex Last visible event index.
	 * @param nextThreshold Timestamp threshold for events that fall below the current pixel.
	 * @returns First index for the next pixel bucket, or one past endIndex.
	 */
	private findNextEventIndexBelowPixel(
		events: Doc.Type[],
		startIndex: number,
		endIndex: number,
		nextThreshold: number,
	): number {
		let lo = startIndex + 1;
		let hi = endIndex;
		let next = endIndex + 1;
		while (lo <= hi) {
			const mid = (lo + hi) >>> 1;
			if (events[mid].gulp_timestamp < nextThreshold) {
				next = mid;
				hi = mid - 1; // try smaller index to find the first qualifying event
			} else {
				lo = mid + 1;
			}
		}

		return next;
	}

	/**
	 * Finds the newest override-colored event within a collapsed pixel bucket.
	 *
	 * @param overrideEventIndexes Ascending event indexes with override colors.
	 * @param bucketStartIndex Inclusive start index for the current pixel bucket.
	 * @param bucketEndIndex Exclusive end index for the current pixel bucket.
	 * @returns Event index for the newest override in the bucket, or -1 when absent.
	 */
	private findFirstOverrideEventIndexInBucket(
		overrideEventIndexes: number[],
		bucketStartIndex: number,
		bucketEndIndex: number,
	): number {
		if (overrideEventIndexes.length === 0) {
			return -1;
		}

		let lo = 0;
		let hi = overrideEventIndexes.length - 1;
		let candidate = overrideEventIndexes.length;
		while (lo <= hi) {
			const mid = (lo + hi) >>> 1;
			if (overrideEventIndexes[mid] >= bucketStartIndex) {
				candidate = mid;
				hi = mid - 1;
			} else {
				lo = mid + 1;
			}
		}

		if (candidate === overrideEventIndexes.length) {
			return -1;
		}

		const eventIndex = overrideEventIndexes[candidate];
		return eventIndex < bucketEndIndex ? eventIndex : -1;
	}

	getRanges(file: Source.Type): MinMax {
		const events = Source.Entity.events(this.renderer.info.app, file);
		const cache = RenderEngine[CacheKey].range.get(file.id);
		const cachedHashRange = Doc.Entity.hashRange(file.id);

		if (cachedHashRange && cachedHashRange.length === events.length) {
			if (
				cache &&
				cache.field === file.settings.field &&
				cache[Hardcode.Length] === events.length &&
				cache.min === cachedHashRange.min &&
				cache.max === cachedHashRange.max
			) {
				return cache;
			}

			const range = {
				min: cachedHashRange.min,
				max: cachedHashRange.max,
				field: file.settings.field,
				[Hardcode.Length]: cachedHashRange.length,
			};
			RenderEngine[CacheKey].range.set(file.id, range);
			return range;
		}

		if (cache && cache.field === file.settings.field) {
			if (cache[Hardcode.Length] === events.length) {
				return cache;
			} else {
				this.computeRanges(file, cache[Hardcode.Length]);
			}
		}

		this.computeRanges(file);
		return this.getRanges(file);
	}

	computeRanges(file: Source.Type, skip: number = 0) {
		const events = Source.Entity.events(this.renderer.info.app, file);
		const cache = RenderEngine[CacheKey].range.get(file.id);

		const range =
			skip > 0 && cache
				? cache
				: {
						min: Infinity,
						max: -Infinity,
						field: file.settings.field,
					};

		for (let i = skip; i < events.length; i++) {
			const value = events[i].number_hash;
			if (value > range.max) range.max = value;
			if (value < range.min) range.min = value;
		}

		RenderEngine[CacheKey].range.set(file.id, {
			...range,
			[Hardcode.Length]: events.length,
		});

		Logger.log(
			`RenderEngine cache ranges for file ${file.id} has been recalculated`,
			DefaultEngine.name,
		);
	}

	is = (file: Source.Type) => false;
}
