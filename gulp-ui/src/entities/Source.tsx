import { Default, Selectable } from "@/dto/Dataset";
import s from "./styles/Source.module.css";
type UUID = string;
import { Context } from "./Context";
import { Operation } from "./Operation";
import { Doc } from "./Doc";
import { Arrayed, GulpDataset, MinMax } from "@/class/Info";
import { App } from "./App";
import { Parser } from "./addon/Parser";
import { Note } from "./Note";
import { User } from "./User";
import { Glyph } from "./Glyph";
import { Engine } from "@/class/Engine.dto";
import {
	generateUUID,
	HASH_FUNCTIONS,
	type HashFunctionName,
	Refractor,
} from "@/ui/utils";
import { Request } from "./Request";
import { Application } from "@/context/Application.context";
import { Button } from "@/ui/Button";
import { useMemo, useState, Fragment, ChangeEvent, useEffect, useRef } from "react";
import { Banner as UIBanner } from "@/ui/Banner";
import { Toggle } from "@/ui/Toggle";
import { Internal } from "./addon/Internal";
import { Color } from "./Color";
import { Select as UISelect } from "@/ui/Select";
import { SetState } from "@/class/API";
import { Icon } from "@/ui/Icon";
import { Badge } from "@/ui/Badge";
import { Checkbox } from "@/ui/Checkbox";
import { FilterFileBanner } from "@/banners/FilterFile.banner";
import { Skeleton } from "@/ui/Skeleton";
import { enginesBase } from "@/dto/Engine.dto";
import {
	ColorPicker,
	ColorPickerTrigger,
	ColorPickerPopover,
} from "@/ui/Color";
import { Input } from "@/ui/Input";
import { Stack } from "@/ui/Stack";
import {
	Tooltip,
	TooltipContent,
	TooltipTrigger,
	TooltipProvider,
} from "@/ui/Tooltip";
import { Separator } from "@radix-ui/react-select";
import { formatDuration, intervalToDuration } from "date-fns";
import { Label } from "@/ui/Label";
import { toast } from "sonner";
import { Locale } from "@/locales";
import { requestStore } from "@/store/request.store";
import { DataStore } from "@/store/DataStore";
import type { Query } from "./Query";
import {
	createEmptyQueryCacheState,
	type QueryCacheState,
} from "./QueryCache";

export namespace Source {
	export const name = "Source";
	const _ = Symbol(Source.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	type SampleData = {
		min_timestamp: number;
		max_timestamp: number;
		sample: number;
	};

	export interface Type extends Selectable {
		operation_id: Operation.Id;
		context_id: Context.Id;
		color?: string;
		plugin: string;
		id: Source.Id;
		type: "source";
		owner_user_id: User.Id;
		granted_user_ids: string[];
		granted_user_group_ids: User.Id[];
		time_created: number;
		time_updated: number | string | null;
		glyph_id: Glyph.Id;
		name: string;
		// Client-only params
		settings: {
			offset: number;
			field: keyof Doc.Type;
			hash_function: HashFunctionName;
			render_color_palette: Color.Gradient;
			render_engine: Engine.List;
			timeline_label: "filename" | "full_path";
			frequency_sample: number; //value in millisecondi
			color_override?: Source.ColorOverride;
			color_palette?: Source.ColorPalette;
		};
		pinned: boolean; // (false)
		// Enriched  using /query_operation
		timestamp: MinMax;
		nanotimestamp: MinMax<bigint>;
		total: number;
		/** Query that produced the EventStore generation currently shown for this source. */
		appliedQuery: Query.Type | null;
		/** Browser-local IndexedDB query-cache navigation and freshness state. */
		queryCache: QueryCacheState;
		_sampleDataCached: {
			frequency_sample: number;
			min_timestamp: number;
			max_timestampe: number;
			sample_data: SampleData[] | null;
		};
	}

	export interface ColorOverride {
		field: keyof Doc.Type | string;
		values: Record<string, string>;
		lookup: Record<number, string>;
	}

	export interface ColorPalette {
		id: string;
		name: string;
		interval: number;
	}

	export class Entity {
		// @ts-ignore
		public static icon = Internal.IconExtractor.activate<Source.Type | null>(
			Default.Icon.SOURCE,
		);

		/** Memoization cache for the Canvas-rendered source row set. */
		private static _renderedCache: {
			ref: Source.Type[] | null;
			loadings: ReturnType<typeof requestStore.getLoadings> | null;
			filterText: string;
			sourcesWithNoEvents: boolean;
			notesHidden: boolean;
			renderVersion: number;
			renderDataSequence: number;
			eventSourceVisibilitySequence: number;
			documentsVersion: number;
			result: Source.Type[];
		} = {
			ref: null,
			loadings: null,
			filterText: "",
			sourcesWithNoEvents: true,
			notesHidden: false,
			renderVersion: 0,
			renderDataSequence: 0,
			eventSourceVisibilitySequence: 0,
			documentsVersion: 0,
			result: [],
		};

		/**
		 * Checks whether a source should stay visible while empty-source rows are hidden.
		 * @param source Source whose query, supplemental events, and notes are inspected.
		 * @param notesHidden Whether note-only rows are currently suppressed.
		 * @returns True while loading, when events exist, or when a visible note targets the source.
		 */
		private static hasVisibleEvents = (
			source: Source.Type,
			notesHidden: boolean,
		): boolean => {
			if (requestStore.getRequestIdByFile(source.id)) return true;
			if (DataStore.hasRenderableSourceEvents(source.id, source.total)) {
				return true;
			}
			return (
				!notesHidden &&
				DataStore.notes.some(
					(note) => Note.Entity.sourceId(note) === source.id,
				)
			);
		};

		/**
		 * Returns the list of sources selected by the user, sorted by pin status.
		 * Canvas-only visibility controls are not applied here.
		 * @param app Current application state.
		 * @returns User-selected source rows.
		 */
		public static selected = (app: App.Type): Source.Type[] => {
			return Source.Entity.pins(
				app.target.files.filter((source) => source.selected),
			);
		};

		/**
		 * Serializes the applied queries owned by source objects into persisted maps.
		 * @param sources Sources whose active query snapshots should be exported.
		 * @returns Query snapshots keyed by source id.
		 */
		public static appliedQueries = (
			sources: readonly Source.Type[],
		): Record<Source.Id, Query.Type> =>
			Object.fromEntries(
				sources
					.filter(
						(source): source is Source.Type & { appliedQuery: Query.Type } =>
							Boolean(source.appliedQuery),
					)
					.map((source) => [source.id, source.appliedQuery]),
			) as Record<Source.Id, Query.Type>;

		/**
		 * Returns the source rows rendered on the Canvas timeline.
		 * Applies user selection, pin ordering, timeline text filtering, and the
		 * Canvas-only hidden-empty-source setting.
		 * @param app Current application state.
		 * @returns Source rows that should be rendered on the Canvas timeline.
		 */
		public static rendered = (app: App.Type): Source.Type[] => {
			const currentFilter = (app.timeline.filter || "").toLowerCase();
			const hiddenSourcesWithNoEvents = app.hidden.sourcesWithNoEvents;
			const notesHidden = app.hidden.notes;
			const renderVersion = app.timeline.renderVersion;
			const renderDataSequence = DataStore.getRenderSequence();
			const eventSourceVisibilitySequence =
				DataStore.getEventSourceVisibilitySequence();
			const documentsVersion = DataStore.getDocumentsSnapshot();
			const loadings = requestStore.getLoadings();

			if (
				app.target.files === Source.Entity._renderedCache.ref &&
				loadings === Source.Entity._renderedCache.loadings &&
				currentFilter === Source.Entity._renderedCache.filterText &&
				hiddenSourcesWithNoEvents ===
					Source.Entity._renderedCache.sourcesWithNoEvents &&
				notesHidden === Source.Entity._renderedCache.notesHidden &&
				renderVersion === Source.Entity._renderedCache.renderVersion &&
				renderDataSequence === Source.Entity._renderedCache.renderDataSequence &&
				eventSourceVisibilitySequence ===
					Source.Entity._renderedCache.eventSourceVisibilitySequence &&
				documentsVersion === Source.Entity._renderedCache.documentsVersion
			) {
				return Source.Entity._renderedCache.result;
			}
			const pins = Source.Entity.selected(app).filter((source) =>
				hiddenSourcesWithNoEvents
					? Source.Entity.hasVisibleEvents(source, notesHidden)
					: true,
			);

			const result = pins.filter(
				(s) =>
					s.name?.toLowerCase().includes(currentFilter) ||
					Context.Entity.id(app, s.context_id)
						.name?.toLowerCase()
						.includes(currentFilter),
			);

			Source.Entity._renderedCache = {
				ref: app.target.files,
				loadings,
				filterText: currentFilter,
				sourcesWithNoEvents: hiddenSourcesWithNoEvents,
				notesHidden,
				renderVersion,
				renderDataSequence,
				eventSourceVisibilitySequence,
				documentsVersion,
				result,
			};

			return result;
		};

		/**
		 * Returns one source's rendered timeline bounds after applying its source offset.
		 * @param source Source whose stored bounds should be shifted into render time.
		 * @returns Shifted minimum and maximum timestamps.
		 */
		public static renderedTimestamp = (source: Source.Type): MinMax => ({
			min: source.timestamp.min + source.settings.offset,
			max: source.timestamp.max + source.settings.offset,
		});

		/**
		 * Returns the rendered timeline frame covering all provided sources.
		 * @param sources Sources whose shifted bounds should be merged.
		 * @returns Shifted source frame, or an empty frame when no sources are supplied.
		 */
		public static renderedFrame = (sources: Source.Type[]): MinMax => {
			if (sources.length === 0) {
				return { min: 0, max: 0 };
			}
			return {
				min: Math.min(
					...sources.map(
						(source) => Source.Entity.renderedTimestamp(source).min,
					),
				),
				max: Math.max(
					...sources.map(
						(source) => Source.Entity.renderedTimestamp(source).max,
					),
				),
			};
		};

		public static select = (
			app: App.Type,
			selected: Source.Type[] | Source.Id[],
		): Source.Type[] =>
			app.target.files.map((f) =>
				selected.map((s) => Parser.useUUID(s)).find((id) => id === f.id)
					? Source.Entity._select(f)
					: f,
			);

		public static pins = (use: App.Type | Source.Type[]) =>
			Parser.use(use, "files").sort((a, b) =>
				a.pinned === b.pinned ? 0 : a.pinned ? -1 : 1,
			);

		public static pin = (file: Source.Type): Source.Type => ({
			...file,
			pinned: true,
		});
		public static unpin = (file: Source.Type): Source.Type => ({
			...file,
			pinned: false,
		});
		public static togglePin = (file: Source.Type): Source.Type =>
			file.pinned ? Source.Entity.unpin(file) : Source.Entity.pin(file);

		public static isEventKeyFetched = (
			app: App.Type,
			id: Source.Type | Source.Id,
			keys: Array<keyof Doc.Type> = [],
		) => {
			const file = Source.Entity.id(app, id);
			return Source.Entity.events(app, file)
				.slice(0, 100)
				.every((e) =>
					[...keys, file.settings.field].every(
						(k) => typeof Refractor.get(e, k) !== "undefined",
					),
				);
		};

		public static context = (app: App.Type, file: Source.Type) =>
			Context.Entity.id(app, file.context_id);

		public static id = (
			use: App.Type | Source.Type[],
			file: Source.Type | Source.Id,
		) =>
			typeof file === "string"
				? (Parser.use(use, "files").find(
						(s) => s.id === Parser.useUUID(file),
					) as Source.Type)
				: file;

		public static getRequestType = (
			app: App.Type,
			file: Source.Type | Source.Id,
		): Request.Prefix | null | undefined => {
			const id = Parser.useUUID(file) as Source.Id;

			const request = requestStore.getRequestIdByFile(id);
			if (!request) {
				// Source.Entity is not requesting
				return null;
			}

			const parts = request.split("-");
			if (parts.length === 1) {
				// Type not defined
				return void 0;
			}

			const type = parts[0];
			if (Object.values(Request.Prefix).includes(type as Request.Prefix)) {
				return type as Request.Prefix;
			}

			// Type defined, but unknown
			return null;
		};

		public static unselect = (
			app: App.Type,
			unselected: Source.Type[],
		): Source.Type[] =>
			app.target.files.map((f) =>
				unselected.find((u) => u.id === f.id) ? Source.Entity._unselect(f) : f,
			);

		public static check = (
			use: App.Type | Source.Type[],
			selected: Arrayed<Source.Type | string>,
			check: boolean,
		): Source.Type[] =>
			Parser.use(use, "files").map((s) =>
				Parser.array(selected).find((f) => s.id === Parser.useUUID(f) && check)
					? Source.Entity._select(s)
					: Source.Entity._unselect(s),
			);

		public static isVirtual = (file: Source.Type) => file.id.startsWith("temp");

		public static virtualize = (
			app: App.Type,
			{
				name,
				total,
				context_id,
				operation_id,
			}: {
				name: string;
				total: number;
				context_id: Context.Id;
				operation_id: Operation.Id;
			},
		): Source.Type => ({
			name,
			id: generateUUID(),
			timestamp: app.timeline.frame,
			nanotimestamp: {
				min: BigInt(Math.round(app.timeline.frame.min)),
				max: BigInt(Math.round(app.timeline.frame.max)),
			},
			settings: Internal.Settings.default,
			selected: true,
			operation_id,
			context_id,
			total,
			type: "source",
			glyph_id: null as unknown as Glyph.Id,
			granted_user_group_ids: [],
			granted_user_ids: [],
			time_created: Date.now(),
			time_updated: Date.now(),
			plugin: "",
			owner_user_id: app.general.user?.id!,
			pinned: false,
			appliedQuery: null,
			queryCache: createEmptyQueryCacheState(),
			_sampleDataCached: {
				frequency_sample: Internal.Settings.default.frequency_sample,
				min_timestamp: 0,
				max_timestampe: 0,
				sample_data: null,
			},
		});

		public static devirtualize = (
			app: App.Type,
			file: Source.Type,
		): Source.Type[] =>
			file.id
				.split("-")
				.slice(1)
				.map((id) => Source.Entity.id(app, id as Source.Id))
				.filter((f) => f);

		/**
		 * Normalizes backend source metadata while preserving client-owned state.
		 * @param app Application containing any existing source instance.
		 * @param file Backend source object to normalize.
		 * @param details Optional aggregate count and nanosecond range metadata.
		 * @returns Canonical runtime source with query provenance and cache state intact.
		 */
		public static normalize = (
			app: App.Type,
			file: Source.Type,
			details?: GulpDataset.QueryOperations.Source,
		): Source.Type => {
			// @ts-ignore
			delete file.mapping_parameters;

			const exist = Source.Entity.id(app, file.id) ?? {};
			const min =
				details?.["min_gulp.timestamp"] ??
				Internal.Transformator.toNanos(Date.now() - 1);
			const max =
				details?.["max_gulp.timestamp"] ??
				Internal.Transformator.toNanos(Date.now());
			const settings = {
				...Internal.Settings.default,
				...(exist.settings ?? {}),
				...(file.settings ?? {}),
			};
			const nanotimestamp = {
				...file.nanotimestamp,
				min: file.nanotimestamp?.min ?? min,
				max: file.nanotimestamp?.max ?? max,
			};
			const timestamp = {
				min: Internal.Transformator.toTimestamp(
					nanotimestamp.min,
					"floor",
				),
				max: Internal.Transformator.toTimestamp(
					nanotimestamp.max,
					"ceil",
				),
			};

			return Object.assign(file, {
				color: file.color ?? exist.color,
				selected: file.selected ?? exist.selected ?? false,
				pinned: file.pinned ?? exist.pinned ?? false,
				settings,
				total: file.total ?? details?.doc_count ?? 0,
				appliedQuery: file.appliedQuery ?? exist.appliedQuery ?? null,
				queryCache:
					file.queryCache ?? exist.queryCache ?? createEmptyQueryCacheState(),
				// @ts-ignore
				nanotimestamp,
				timestamp,
				_sampleDataCached:
					file._sampleDataCached ??
					exist._sampleDataCached ?? {
						frequency_sample: settings.frequency_sample,
						min_timestamp: timestamp.min,
						max_timestampe: timestamp.max,
						sample_data: null,
					},
			});
		};

		public static events = (
			app: App.Type,
			file: Source.Type | Source.Id,
		): Doc.Type[] =>
			DataStore.peekSourceEvents(Parser.useUUID(file) as Source.Id) ?? [];

		/**
		 * Hashes a color override value with the exact same hash calculation used by Doc.Entity.normalize().
		 *
		 * @param value Field value configured by a render rule.
		 * @param hashFunction Source hash function used for Doc.number_hash.
		 * @returns Numeric hash compatible with event.number_hash.
		 */
		public static hashColorValue = (
			value: string,
			hashFunction: HashFunctionName,
		): number => Refractor.any.toNumber(value, hashFunction);

		/**
		 * Builds a render-ready color override lookup object for O(1) canvas reads.
		 *
		 * @param field Source field that Doc.number_hash will be calculated from.
		 * @param values Mapping of raw field values to CSS colors.
		 * @param hashFunction Source hash function used for the selected field.
		 * @returns Color override settings with a precomputed numeric lookup table.
		 */
		public static buildColorOverride = (
			field: keyof Doc.Type | string,
			values: Record<string, string>,
			hashFunction: HashFunctionName,
		): Source.ColorOverride => {
			const lookup: Record<number, string> = {};

			Object.entries(values).forEach(([value, color]) => {
				if (!color) return;
				lookup[Source.Entity.hashColorValue(value, hashFunction)] = color;

				const numericValue = Number(value);
				if (value.trim() !== "" && Number.isFinite(numericValue)) {
					lookup[Refractor.any.toNumber(numericValue, hashFunction)] = color;
				}
			});

			return { field, values, lookup };
		};

		/**
		 * Resolves only the explicit override color for a numeric event value.
		 *
		 * @param file Source whose render settings own the optional color override table.
		 * @param value Numeric hash used for override lookup.
		 * @returns Override color when configured for the value, otherwise undefined.
		 */
		public static resolveOverrideColor = (
			file: Source.Type,
			value: number,
		): string | undefined => file.settings.color_override?.lookup?.[value];

		/**
		 * Resolves the render color for a numeric event value with override priority.
		 *
		 * @param file Source whose render settings own the optional color override table.
		 * @param value Numeric hash or bucket amount used for color resolution.
		 * @param range Min/max range for gradient fallback.
		 * @returns Override color, custom palette bucket, or standard gradient color.
		 */
		public static resolveColor = (
			file: Source.Type,
			value: number,
			range: MinMax,
		): string => {
			const overrideColor = Source.Entity.resolveOverrideColor(file, value);
			if (overrideColor) {
				return overrideColor;
			}

			const paletteId = file.settings.color_palette?.id;
			if (paletteId) {
				const paletteColor = Color.Entity.customPaletteColor(paletteId, value);
				if (paletteColor) {
					return paletteColor;
				}
			}

			return Color.Entity.gradient(
				file.settings.render_color_palette,
				value,
				range,
			);
		};

		public static notes = (
			app: App.Type,
			files: Arrayed<Source.Type>,
		): Note.Type[] =>
			Parser.array(files)
				.map((s) => Note.Entity.findByFile(app, s))
				.flat();

		public static index = (app: App.Type, file: Source.Type | Source.Id) =>
			Source.Entity.rendered(app).findIndex(
				(s) => s.id === Parser.useUUID(file),
			);

		public static getHeight = (
			app: App.Type,
			file: Source.Type | Source.Id,
			scrollY: number,
			index?: number,
		) =>
			48 * (typeof index === "number" ? index : this.index(app, file)) -
			scrollY +
			24;

		private static _select = (p: Source.Type): Source.Type => ({
			...p,
			selected: true,
		});

		private static _unselect = (p: Source.Type): Source.Type => ({
			...p,
			selected: false,
		});

		/**
		 * Finds the index of the first event that falls within the specified time range using binary search.
		 * Auto-detects whether the events array is sorted in ascending or descending order.
		 *
		 * @param events List of events to search.
		 * @param startTimestamp The start of the time range (inclusive).
		 * @param endTimestamp The end of the time range (inclusive).
		 * @returns The index of the first event found, or -1 if no events fall within the range.
		 */
		public static findFirstEventIndexInTimeRange = (
			events: Doc.Type[],
			startTimestamp: number,
			endTimestamp: number,
		): number => {
			if (!events || events.length === 0 || startTimestamp > endTimestamp) {
				return -1;
			}

			const isDescending =
				events[0].gulp_timestamp > events[events.length - 1].gulp_timestamp;

			if (isDescending) {
				// For descending order (highest/newest first, lowest/oldest last):
				// The first matching event is the leftmost element <= endTimestamp.
				let low = 0;
				let high = events.length - 1;
				let first = events.length;
				while (low <= high) {
					const mid = (low + high) >> 1;
					if (events[mid].gulp_timestamp <= endTimestamp) {
						first = mid;
						high = mid - 1;
					} else {
						low = mid + 1;
					}
				}

				if (
					first < events.length &&
					events[first].gulp_timestamp >= startTimestamp
				) {
					return first;
				}
				return -1;
			} else {
				// For ascending order (lowest/oldest first, highest/newest last):
				// The first matching event is the leftmost element >= startTimestamp.
				let low = 0;
				let high = events.length - 1;
				let first = events.length;
				while (low <= high) {
					const mid = (low + high) >> 1;
					if (events[mid].gulp_timestamp >= startTimestamp) {
						first = mid;
						high = mid - 1;
					} else {
						low = mid + 1;
					}
				}

				if (
					first < events.length &&
					events[first].gulp_timestamp <= endTimestamp
				) {
					return first;
				}
				return -1;
			}
		};

		/**
		 * Counts the total number of events within the specified millisecond time range using binary search.
		 * Auto-detects whether the events array is sorted in ascending or descending order.
		 * Performs in O(log N) time, completely avoiding any sequential loop traversals even with identical timestamps.
		 *
		 * @param events List of events to count.
		 * @param minTimestamp The start of the time range in milliseconds (inclusive).
		 * @param maxTimestamp The end of the time range in milliseconds (inclusive).
		 * @returns The total number of events inside the time range.
		 */
		public static countEventsInTimeRange = (
			events: Doc.Type[],
			minTimestamp: number,
			maxTimestamp: number,
		): number => {
			if (
				!events ||
				events.length === 0 ||
				minTimestamp > maxTimestamp
			) {
				return 0;
			}

			const getEventTimestamp = (event: Doc.Type): number => {
				return event.gulp_timestamp;
			};

			const firstTimestamp = getEventTimestamp(events[0]);
			const lastTimestamp = getEventTimestamp(events[events.length - 1]);
			const isDescending = firstTimestamp > lastTimestamp;

			if (isDescending) {
				// Find the first index where the event timestamp is <= maxTimestamp.
				let low = 0;
				let high = events.length - 1;
				let firstIdx = -1;
				while (low <= high) {
					const mid = (low + high) >> 1;
					const eventTimestamp = getEventTimestamp(events[mid]);
					if (eventTimestamp <= maxTimestamp) {
						firstIdx = mid;
						high = mid - 1;
					} else {
						low = mid + 1;
					}
				}

				// Find the last index where the event timestamp is >= minTimestamp.
				low = 0;
				high = events.length - 1;
				let lastIdx = -1;
				while (low <= high) {
					const mid = (low + high) >> 1;
					const eventTimestamp = getEventTimestamp(events[mid]);
					if (eventTimestamp >= minTimestamp) {
						lastIdx = mid;
						low = mid + 1;
					} else {
						high = mid - 1;
					}
				}

				if (firstIdx === -1 || lastIdx === -1 || firstIdx > lastIdx) {
					return 0;
				}

				return lastIdx - firstIdx + 1;
			} else {
				// Find the first index where the event timestamp is >= minTimestamp.
				let low = 0;
				let high = events.length - 1;
				let firstIdx = -1;
				while (low <= high) {
					const mid = (low + high) >> 1;
					const eventTimestamp = getEventTimestamp(events[mid]);
					if (eventTimestamp >= minTimestamp) {
						firstIdx = mid;
						high = mid - 1;
					} else {
						low = mid + 1;
					}
				}

				// Find the last index where the event timestamp is <= maxTimestamp.
				low = 0;
				high = events.length - 1;
				let lastIdx = -1;
				while (low <= high) {
					const mid = (low + high) >> 1;
					const eventTimestamp = getEventTimestamp(events[mid]);
					if (eventTimestamp <= maxTimestamp) {
						lastIdx = mid;
						low = mid + 1;
					} else {
						high = mid - 1;
					}
				}

				if (firstIdx === -1 || lastIdx === -1 || firstIdx > lastIdx) {
					return 0;
				}

				return lastIdx - firstIdx + 1;
			}
		};

		public static samples = (app: App.Type, file: Source.Type) => {
			const events = Source.Entity.events(app, file);
			if (!events || events.length === 0) return;

			if (
				file._sampleDataCached &&
				file._sampleDataCached.sample_data &&
				file._sampleDataCached.frequency_sample ==
					file.settings.frequency_sample &&
				file._sampleDataCached.min_timestamp == file.timestamp.min &&
				file._sampleDataCached.max_timestampe == file.timestamp.max
			) {
				return file._sampleDataCached.sample_data;
			}

			const [minTime, maxTime] = [
				Math.min(file.timestamp.min, file.timestamp.max),
				Math.max(file.timestamp.min, file.timestamp.max),
			];
			const freq = file.settings.frequency_sample;

			const bucketMap = new Map<number, number>();

			for (let i = 0; i < events.length; i++) {
				const timestamp = events[i].gulp_timestamp;
				if (timestamp < minTime || timestamp >= maxTime) continue;

				const bucketIndex = Math.floor((timestamp - minTime) / freq);
				bucketMap.set(bucketIndex, (bucketMap.get(bucketIndex) || 0) + 1);
			}

			const sortedKeys = Array.from(bucketMap.keys()).sort((a, b) => a - b);
			const results: SampleData[] = [];

			for (let i = 0; i < sortedKeys.length; i++) {
				const index = sortedKeys[i];
				results.push({
					min_timestamp: minTime + index * freq,
					max_timestamp: minTime + (index + 1) * freq,
					sample: bucketMap.get(index)!,
				});
			}

			file._sampleDataCached = {
				frequency_sample: file.settings.frequency_sample,
				min_timestamp: file.timestamp.min,
				max_timestampe: file.timestamp.max,
				sample_data: results,
			};
			return results;
		};
	}

	export namespace Delete {
		export namespace Banner {
			export interface Props extends UIBanner.Props {
				source: Source.Type;
			}
		}

		/**
		 * Banner component for confirming deletion of a source file.
		 *
		 * @param props - Component props.
		 * @param props.source - The source object to delete.
		 * @returns The React element for the confirmation banner.
		 */
		export function Banner({ source, ...props }: Source.Delete.Banner.Props) {
			const { Info, destroyBanner } = Application.use();
			const { t } = Locale.use();
			const [loading, setLoading] = useState<boolean>(false);
			const [isSubmitted, setIsSubmitted] = useState<boolean>(false);

			/**
			 * Deletes the source file and triggers the back callback or destroys the banner.
			 */
			const deleteFile = async () => {
				setLoading(true);
				await Info.file_delete(source);
				setLoading(false);
				if (props.back) {
					props.back();
				} else {
					destroyBanner();
				}
			};

			return (
				<UIBanner
					title={t("source.deleteTitle")}
					done={
						<Button
							loading={loading}
							icon="Trash2"
							variant="secondary"
							onClick={deleteFile}
							disabled={!isSubmitted}
						/>
					}
					{...props}
				>
					<p>
						{t("source.deleteConfirmBefore")} <code>{source.name}</code>.{" "}
						{t("source.deleteConfirmAfter")}
					</p>
					<Toggle
						option={[t("common.noDontDelete"), t("common.yesImSure")]}
						checked={isSubmitted}
						onCheckedChange={setIsSubmitted}
					/>
				</UIBanner>
			);
		}
	}

	export namespace Select {
		export namespace Multi {
			export interface Props {
				sources?: Source.Type[];
				selected: Source.Id[];
				setSelected: SetState<Source.Id[]>;
				placeholder?: string;
			}
		}

		interface ContextGroupProps {
			context: Context.Type;
			toggleContext: (contextSources: Source.Type[]) => void;
			isAllContextSelected: boolean;
			isIndeterminate: boolean;
			contextSources: Source.Type[];
		}

		/**
		 * Renders one context section inside the multi source selector.
		 *
		 * @param props Context grouping and selection state.
		 * @returns Source selector group for a single context.
		 */
		function ContextGroup({
			context,
			toggleContext,
			isAllContextSelected,
			isIndeterminate,
			contextSources,
		}: ContextGroupProps) {
			const { app } = Application.use();
			const [isCollapsed, setIsCollapsed] = useState(false);

			return (
				<UISelect.Group>
					<UISelect.Label
						style={{
							display: "flex",
							justifyContent: "flex-start",
							textAlign: "left",
							gap: 8,
							paddingLeft: 25,
						}}
					>
						<Icon
							name={isCollapsed ? "ChevronRight" : "ChevronDown"}
							onClick={(e) => {
								e.preventDefault();
								e.stopPropagation();
								setIsCollapsed(!isCollapsed);
							}}
							style={{
								cursor: "pointer",
								width: 16,
								height: 16,
								position: "absolute",
								left: 4,
							}}
						/>
						<Checkbox
							style={{
								width: "calc(100% - 20px)",
								opacity: 0,
								position: "absolute",
								left: 20,
							}}
							checked={
								isAllContextSelected
									? true
									: isIndeterminate
										? "indeterminate"
										: false
							}
							onCheckedChange={() =>
								toggleContext(contextSources)
							}
						/>
						{context.name}
					</UISelect.Label>
					{!isCollapsed &&
						contextSources.map((source) => (
							<UISelect.Item
								key={source.id}
								value={source.id}
								style={{ display: "flex", alignItems: "center", gap: 6 }}
							>
								<Icon
									name={Source.Entity.icon(source) || "File"}
									onClick={(e) => {
										e.preventDefault();
										e.stopPropagation();
										const updated = Source.Entity.togglePin(source);
										app.target.files = app.target.files.map((f) =>
											f.id === updated.id ? updated : f,
										);
									}}
									style={{ cursor: "pointer" }}
								/>
								{source.name}
								{source.pinned && (
									<Icon
										name="Pin"
										style={{ color: "#f5a623" }}
									/>
								)}
							</UISelect.Item>
						))}
				</UISelect.Group>
			);
		}

		export function Multi({
			sources,
			selected,
			setSelected,
			placeholder,
		}: Select.Multi.Props) {
			const { app } = Application.use();
			const { t } = Locale.use();
			const appSources = app.target.files;

			const all = useMemo(
				() =>
					sources ??
					Source.Entity.pins(appSources.filter((source) => source.selected)),
				[sources, appSources],
			);
			const selectedSet = useMemo(() => new Set(selected), [selected]);
			const selectedOperationId = useMemo(
				() => app.target.operations.find((operation) => operation.selected)?.id,
				[app.target.operations],
			);
			const availableContexts = useMemo(() => {
				const contextIds = new Set(all.map((s) => s.context_id));
				return app.target.contexts.filter(
					(context) =>
						context.selected &&
						context.operation_id === selectedOperationId &&
						contextIds.has(context.id),
				);
			}, [all, app.target.contexts, selectedOperationId]);

			const isAllSelected = useMemo(
				() => all.length > 0 && all.every((source) => selectedSet.has(source.id)),
				[all, selectedSet],
			);
			const sourcesByContext = useMemo(() => {
				const groupedSources = new Map<Context.Id, Source.Type[]>();
				all.forEach((source) => {
					const contextSources = groupedSources.get(source.context_id) ?? [];
					contextSources.push(source);
					groupedSources.set(source.context_id, contextSources);
				});
				return groupedSources;
			}, [all]);

			/**
			 * Toggles every source in one context using constant-time membership checks.
			 * @param contextSources Sources belonging to the context being toggled.
			 * @returns Nothing.
			 */
			const toggleContext = (contextSources: Source.Type[]): void => {
				const nextSelected = new Set(selectedSet);
				const isAllContextSelected = contextSources.every((source) =>
					selectedSet.has(source.id),
				);
				contextSources.forEach((source) => {
					if (isAllContextSelected) nextSelected.delete(source.id);
					else nextSelected.add(source.id);
				});
				setSelected(Array.from(nextSelected));
			};

			const [isOpen, setIsOpen] = useState(false);

			/**
			 * Resolves the text shown by the multi-select trigger.
			 * @param value Selected count or the single selected source id.
			 * @returns Localized selection count, resolved source name, or loading text while detached state hydrates.
			 */
			const getSelectedSourceLabel = (value: number | string): string => {
				if (typeof value === "number") {
					return t("source.selectedSources", { count: value });
				}

				const source = Source.Entity.id(app, value as Source.Id);
				return source?.name ?? t("common.loading");
			};

			return (
				<UISelect.Multi.Root
					value={selected}
					onValueChange={(selected) => setSelected(selected as Source.Id[])}
					onOpenChange={setIsOpen}
				>
					<UISelect.Trigger>
						<UISelect.Multi.Value
							icon={["File", "Files"]}
							placeholder={placeholder ?? t("source.selectSources")}
							text={getSelectedSourceLabel}
						/>
					</UISelect.Trigger>
					<UISelect.Content>
						{isOpen && (
							<>
								<UISelect.Multi.ToggleAll
									label={
										isAllSelected
											? t("common.deselectAll")
											: t("common.selectAll")
									}
									checked={isAllSelected}
									onToggle={(val) =>
										setSelected(val ? all.map((s) => s.id) : [])
									}
								/>
								{availableContexts.map((context, index) => {
									const contextSources =
										sourcesByContext.get(context.id) ?? [];
									const selectedContextSourceCount = contextSources.reduce(
										(count, source) =>
											count + (selectedSet.has(source.id) ? 1 : 0),
										0,
									);
									const isAllContextSelected =
										selectedContextSourceCount === contextSources.length;
									const isIndeterminate =
										selectedContextSourceCount > 0 && !isAllContextSelected;

									return (
										<Fragment key={context.id}>
											<ContextGroup
												context={context}
												toggleContext={toggleContext}
												isAllContextSelected={isAllContextSelected}
												isIndeterminate={isIndeterminate}
												contextSources={contextSources}
											/>
											{index < availableContexts.length - 1 && (
												<UISelect.Separator />
											)}
										</Fragment>
									);
								})}
							</>
						)}
					</UISelect.Content>
				</UISelect.Multi.Root>
			);
		}
	}

	export namespace Settings {
		export namespace Banner {
			export interface Props {
				source: Source.Type;
			}
		}

		/**
		 * Settings Banner component to manage configuration for a specific data Source.
		 * Allows adjusting time offset, sampling frequency, render engine, color palette,
		 * target field for color scheme, and context color.
		 *
		 * @param props.source The data source object to configure.
		 */
		export function Banner({ source }: Settings.Banner.Props) {
			const { Info, app, spawnBanner, destroyBanner } = Application.use();
			const { t } = Locale.use();
			const [render_color_palette, setRenderColorPalette] =
				useState<Color.Gradient>(
					Color.normalizeGradient(source.settings.render_color_palette),
				);
			const [offset, setOffset] = useState<number>(source.settings.offset);
			const [render_engine, setEngine] = useState<Engine.List>(
				source.settings.render_engine,
			);
			const [timeline_label, setTimelineLabel] = useState<
				Source.Type["settings"]["timeline_label"]
			>(source.settings.timeline_label);
			const context = useMemo(
				() => Context.Entity.id(app, source.context_id),
				[app.target.contexts, source.context_id],
			);
			const [contextColor, setContextColor] = useState<string>(
				context?.color ?? "",
			);
			const initialContextColorRef = useRef<string>(context?.color ?? "");
			const [frequency_sample, setFrequencySample] = useState<number>(
				source.settings.frequency_sample,
			);
			const [hash_function, setHashFunction] = useState<HashFunctionName>(
				source.settings.hash_function,
			);
			const [colorOverride, setColorOverride] = useState<
				Source.ColorOverride | undefined
			>(source.settings.color_override);
			const colorOverrideEntries = useMemo(
				() => Object.entries(colorOverride?.values ?? {}),
				[colorOverride],
			);
			const [colorPalette, setColorPalette] = useState<
				Source.ColorPalette | undefined
			>(source.settings.color_palette);
			const colorPaletteColors = colorPalette
				? Color.Entity.getCustomPalette(colorPalette.id)
				: undefined;

			/**
			 * Selects a standard render palette and marks any active custom palette for removal.
			 *
			 * @param color Selected palette id, or a React state updater from the color picker.
			 * @returns Nothing.
			 */
			const selectRenderColorPalette = (
				color: string | ((previousColor: string) => string),
			) => {
				const nextColor =
					typeof color === "function" ? color(render_color_palette) : color;
				setRenderColorPalette(nextColor as Color.Gradient);
				setColorPalette(undefined);
			};

			const [eventKeys, setEventKeys] = useState<string[] | null>(null);

			useEffect(() => {
				Info.event_keys(source)
					.then(Object.keys)
					.then((keys) => keys.sort((a, b) => a.localeCompare(b)))
					.then(setEventKeys);
			}, []);

			const [field, setField] = useState<keyof Doc.Type>(source.settings.field);

			/**
			 * Normalizes a context color for semantic comparison without changing the saved value.
			 *
			 * @param color Context color value to normalize.
			 * @returns Trimmed color value, with hex colors compared case-insensitively.
			 */
			const normalizeContextColorForComparison = (color: string): string => {
				const normalizedColor = color.trim();
				return /^#[\da-f]{3,8}$/i.test(normalizedColor)
					? normalizedColor.toLowerCase()
					: normalizedColor;
			};

			/**
			 * Checks whether the user changed the context color in this settings banner.
			 *
			 * @returns True when a context exists and its color differs from the banner's initial color.
			 */
			const hasContextColorChanged = (): boolean =>
				Boolean(context) &&
				normalizeContextColorForComparison(contextColor) !==
					normalizeContextColorForComparison(initialContextColorRef.current);

			/**
			 * Saves source settings and persists the context color only when that color changed.
			 * Persists custom palette state unless the user selected a standard render palette.
			 *
			 * @returns A promise that resolves when saving is completed.
			 */
			const save = async () => {
				Info.file_set_settings(source.id, {
					render_color_palette,
					render_engine,
					timeline_label,
					offset,
					field,
					hash_function,
					frequency_sample,
					color_override: colorOverride,
					color_palette: colorPalette,
				});

				if (context && hasContextColorChanged()) {
					await Info.context_update(context.id, contextColor);
				}

				destroyBanner();
			};

			/**
			 * Event handler for changes to the source offset input.
			 * Parses the input value as a number and updates the local offset state.
			 *
			 * @param event The change event from the offset input.
			 */
			const handleOffsetChange = (event: ChangeEvent<HTMLInputElement>) =>
				setOffset(event.target.valueAsNumber || 0);

			/**
			 * Event handler for changes to the frequency sample input.
			 * Parses the input value as a number (falling back to a default value)
			 * and updates the local frequency sample state.
			 *
			 * @param event The change event from the frequency sample input.
			 */
			const handleFrequencyChange = (event: ChangeEvent<HTMLInputElement>) =>
				setFrequencySample(event.target.valueAsNumber || 1000);

			/**
			 * Removes the active color override from the source and refreshes rendering immediately.
			 */
			const clearColorOverride = () => {
				setColorOverride(undefined);
				Info.file_set_settings(source.id, { color_override: undefined });
				toast.success(t("source.colorOverrideRemoved"));
			};

			const done = (
				<Button
					variant="glass"
					onClick={save}
					icon="Check"
				/>
			);

			const option = (
				<Button
					onClick={() => spawnBanner(<FilterFileBanner sources={[source]} />)}
					variant="tertiary"
					icon="Filter"
				/>
			);

			const EventFieldsSelection = useMemo(() => {
				if (!eventKeys) {
					return <Skeleton width="full" />;
				}

				return (
					<UISelect.Root
						onValueChange={(field: keyof Doc.Type) => setField(field)}
						defaultValue={field.toString()}
					>
						<UISelect.Trigger
							data-no-icon
							value={field}
						>
							{field}
						</UISelect.Trigger>
						<UISelect.Content>
							{eventKeys.map((field) => (
								<UISelect.Item
									key={field}
									value={field}
								>
									{field}
								</UISelect.Item>
							))}
						</UISelect.Content>
					</UISelect.Root>
				);
			}, [eventKeys, field]);

			return (
				<UIBanner
					title={t("source.settingsTitle")}
					done={done}
					option={option}
				>
					<TooltipProvider>
						<h4>
							{t("source.nameInContext", {
								source: source.name,
								context: Context.Entity.id(app, source.context_id)?.name ?? "",
							})}
						</h4>
						<Input
							variant="highlighted"
							icon="AlarmClockPlus"
							type="number"
							label={t("source.offsetLabel", {
								duration: formatDuration(
									intervalToDuration({ start: 0, end: offset }),
									{
										format: [
											"years",
											"months",
											"days",
											"hours",
											"minutes",
											"seconds",
										],
										zero: false,
									},
								),
								ms: parseInt(offset.toString().slice(-3)),
							})}
							value={offset}
							placeholder={t("source.offsetPlaceholder")}
							onChange={handleOffsetChange}
						/>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
							style={{ width: "100%" }}
						>
							<Stack
								dir="row"
								gap={4}
								ai="center"
							>
								<Label
									value={t("source.minFrequencySample", {
										value: `${frequency_sample}ms`,
									})}
								/>
								<Tooltip>
									<TooltipTrigger asChild>
										<span
											style={{
												cursor: "help",
												display: "inline-flex",
												alignItems: "center",
											}}
										>
											<Icon
												name="Information"
												size={14}
												style={{ opacity: 0.6 }}
											/>
										</span>
									</TooltipTrigger>
									<TooltipContent style={{ whiteSpace: "normal" }}>
										{t("source.frequencyTooltip")}
									</TooltipContent>
								</Tooltip>
							</Stack>
							<Input
								variant="highlighted"
								icon="AlarmClockPlus"
								type="number"
								value={frequency_sample}
								placeholder={t("source.frequencyPlaceholder")}
								onChange={handleFrequencyChange}
							/>
						</Stack>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
						>
							<Label value={t("common.renderEngine")} />
							<UISelect.Root
								onValueChange={(v: Engine.List) => setEngine(v)}
								value={render_engine}
							>
								<UISelect.Trigger>
									<Icon
										name={
											enginesBase.find((e) => e.plugin === render_engine)
												?.img ?? "CircleDashed"
										}
									/>
									{enginesBase.find((e) => e.plugin === render_engine)?.title ??
										render_engine}
								</UISelect.Trigger>
								<UISelect.Content>
									{enginesBase.map((i) => (
										<UISelect.Item
											value={i.plugin}
											key={i.plugin}
										>
											<Icon name={i.img} />
											{i.title}
										</UISelect.Item>
									))}
								</UISelect.Content>
							</UISelect.Root>
						</Stack>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
						>
							<Label value="Timeline label" />
							<UISelect.Root
								onValueChange={(value: Source.Type["settings"]["timeline_label"]) =>
									setTimelineLabel(value)
								}
								value={timeline_label}
							>
								<UISelect.Trigger data-no-icon>
									{timeline_label === "full_path" ? "Full path" : "Filename only"}
								</UISelect.Trigger>
								<UISelect.Content>
									<UISelect.Item value="filename">Filename only</UISelect.Item>
									<UISelect.Item value="full_path">Full path</UISelect.Item>
								</UISelect.Content>
							</UISelect.Root>
						</Stack>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
						>
							<Label value={t("source.hashFunction")} />
							<UISelect.Root
								onValueChange={(value: HashFunctionName) =>
									setHashFunction(value)
								}
								value={hash_function}
							>
								<UISelect.Trigger data-no-icon>
									{hash_function}
								</UISelect.Trigger>
								<UISelect.Content>
									{HASH_FUNCTIONS.map((value) => (
										<UISelect.Item
											key={value}
											value={value}
										>
											{value}
										</UISelect.Item>
									))}
								</UISelect.Content>
							</UISelect.Root>
						</Stack>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
						>
							<Label value={t("source.colorScheme")} />
							{colorPalette && (
								<div className={s.custom_palette_notice}>
									{!!colorPaletteColors?.length && (
										<span
											className={s.custom_palette_preview}
											style={{
												background: `linear-gradient(to right, ${colorPaletteColors.join(", ")})`,
											}}
										/>
									)}
									<span>
										{t("source.customPaletteApplied", {
											name: colorPalette.name,
										})}
									</span>
								</div>
							)}
							<ColorPicker
								color={render_color_palette}
								setColor={selectRenderColorPalette}
							>
								<ColorPickerTrigger />
								<ColorPickerPopover
									gradients={Color.GRADIENT}
									solids={[]}
								/>
							</ColorPicker>
						</Stack>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
						>
							<Label value={t("source.colorSchemeField")} />
							{EventFieldsSelection}
						</Stack>
						{colorOverrideEntries.length > 0 && (
							<Stack
								dir="column"
								gap={6}
								ai="stretch"
							>
								<Stack
									dir="row"
									ai="center"
									jc="space-between"
									className={s.override_header}
								>
									<Label value={t("source.overrides")} />
									<Button
										variant="secondary"
										icon="Trash2"
										onClick={clearColorOverride}
									/>
								</Stack>
								<div className={s.override_list}>
									{colorOverrideEntries.map(([value, color]) => (
										<div
											key={value}
											className={s.override_row}
										>
											<span>{value}</span>
											<span className={s.override_color_value}>
												<span
													className={s.override_swatch}
													style={{ background: color }}
												/>
												{color}
											</span>
										</div>
									))}
								</div>
							</Stack>
						)}
						<Separator style={{ margin: "8px 0" }} />
						<h4>{Context.Entity.id(app, source.context_id)?.name}</h4>
						<Stack
							dir="column"
							gap={6}
							ai="flex-start"
						>
							<Label value={t("source.changeContextColor")} />
							<ColorPicker
								color={contextColor}
								setColor={setContextColor}
							>
								<ColorPickerTrigger />
								<ColorPickerPopover solids={Color.GEIST_STRINGS} />
							</ColorPicker>
						</Stack>
					</TooltipProvider>
				</UIBanner>
			);
		}

		export namespace RenderRules {
			export namespace Banner {
				export interface Props extends UIBanner.Props {}
			}

			export function Banner({ ...props }: Banner.Props) {
				const { t } = Locale.use();
				return (
					<UIBanner
						title={t("source.manageRenderRules")}
						{...props}
					></UIBanner>
				);
			}
		}
	}
}
