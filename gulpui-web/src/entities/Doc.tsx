type UUID = string;
import { Source } from "./Source";
import { App } from "./App";
import { Arrayed } from "@/class/Info";
import { Parser } from "./addon/Parser";
import { Note } from "./Note";
import { Operation } from "./Operation";
import { Context } from "./Context";
import { Internal } from "./addon/Internal";
import { toast } from "sonner";
import { Icon } from "@/ui/Icon";
import { Logger } from "@/dto/Logger.class";
import { DataStore } from "@/store/DataStore";
import { Refractor } from "@/ui/utils";
import { translate } from "@/locales";
import type { Link } from "./Link";

export namespace Doc {
	export const name = "Doc";
	const _ = Symbol(Doc.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export interface Type {
		_id: Doc.Id;
		gulp_timestamp: number;
		"gulp.source_id": Source.Id;
		"gulp.event_code": number;
		number_hash: number;
	}

	export interface PreparedBatch {
		sourceId: Source.Id;
		docs: Doc.Type[];
	}

	export interface HashRange {
		min: number;
		max: number;
		length: number;
	}

	interface MergeResult {
		events: Doc.Type[];
		hashRange: Doc.HashRange | undefined;
	}

	export type Minified = Pick<Doc.Type, "_id" | "gulp.source_id">;

	interface Flag {
		KEY: string;
		getList: (operationId?: Operation.Id) => Set<Doc.Id>;
		getDocIds: (app: App.Type, operationId?: Operation.Id) => Doc.Id[];
		getDocs: (app: App.Type, operationId?: Operation.Id) => Doc.Type[];
		isLimitReached: (ids?: Set<Doc.Id>) => boolean;
		toggle: (id: Doc.Id, operationId?: Operation.Id) => boolean;
		reset: (operationId?: Operation.Id) => void;
		isFlagged: (id: Doc.Id, operationId?: Operation.Id) => boolean;
	}

	export class Entity {
		/**
		 * Global event lookup index: maps Doc.Id → Doc.Type for O(1) access.
		 *
		 * ARCHITECTURAL DECISION: Previously, `Doc.Entity.id()` used
		 * `Array.from(app.target.events.values()).flat().find(...)` which created
		 * a new flat array of ALL events (up to 320k) on every single call.
		 * With links calling `id()` per doc_id per link, this caused ~6.4M array
		 * operations per render frame. The Map index reduces this to O(1).
		 *
		 * The index is maintained by `add()` and `delete()`, and must be cleared
		 * via `clearIndex()` when switching operations to prevent memory leaks.
		 */
		private static _index = new Map<Doc.Id, Doc.Type>();
		private static _hashRanges = new Map<Source.Id, Doc.HashRange>();
		private static readonly PREPARED_INSERT_YIELD_CHUNK_SIZE = 25000;

		/**
		 * Clears cached document lookup and hash range data during operation switch.
		 * @returns Nothing.
		 */
		public static clearIndex = (): void => {
			Doc.Entity._index.clear();
			Doc.Entity._hashRanges.clear();
		};

		/**
		 * Returns the cached number_hash range for one source.
		 * @param sourceId Source identifier.
		 * @returns Cached hash range, or undefined when it must be recomputed.
		 */
		public static hashRange = (
			sourceId: Source.Id,
		): Doc.HashRange | undefined => Doc.Entity._hashRanges.get(sourceId);

		/**
		 * Clears cached number_hash range data for one source.
		 * @param sourceId Source identifier.
		 * @returns Nothing.
		 */
		public static clearHashRange = (sourceId: Source.Id): void => {
			Doc.Entity._hashRanges.delete(sourceId);
		};

		/**
		 * Recomputes number_hash values for one source after render field settings change.
		 * @param app Current application state.
		 * @param sourceId Source identifier.
		 * @param field Field used to read the hash input.
		 * @param hashFunction Hash function used for string values.
		 * @returns Nothing.
		 */
		public static recalculateNumberHashes = (
			app: App.Type,
			sourceId: Source.Id,
			field: string,
			hashFunction: Source.Type["settings"]["hash_function"],
		): void => {
			const events = Doc.Entity.get(app, sourceId);
			events.forEach((event) => {
				event.number_hash = Refractor.any.toNumber(
					Refractor.get(event, field),
					hashFunction,
				);
			});
			Doc.Entity.updateHashRange(sourceId, events);
		};

		/**
		 * Yields control back to the browser so rendering and input can run between large chunks.
		 * @returns Promise that resolves on the next macrotask.
		 */
		private static yieldToMainThread = (): Promise<void> =>
			new Promise((resolve) => setTimeout(resolve, 0));

		/** Extracts a minimal Doc payload from a full event, keeping only essential fields. */
		public static toDoc = (app: App.Type, event: Doc.Type) => {
			const raw = event as any;
			return {
				_id: event._id,
				"@timestamp":
					raw["@timestamp"] ||
					Internal.Transformator.toISO((event.gulp_timestamp || 0) * 1_000_000),
				"gulp.operation_id":
					raw["gulp.operation_id"] || this.operationId(app, event),
				"gulp.context_id": raw["gulp.context_id"] || this.contextId(app, event),
				"gulp.source_id": event["gulp.source_id"],
				"gulp.timestamp":
					raw["gulp.timestamp"] ||
					Internal.Transformator.toNanos(event.gulp_timestamp),
			};
		};

		/** Returns the Operation.Id for a doc by looking it up through its source. */
		public static operationId = (
			app: App.Type,
			doc: Doc.Type,
		): Operation.Id | undefined =>
			Source.Entity.id(app, doc["gulp.source_id"])?.operation_id;

		/** Returns the Context.Id for a doc by looking it up through its source. */
		public static contextId = (
			app: App.Type,
			doc: Doc.Type,
		): Context.Id | undefined =>
			Source.Entity.id(app, doc["gulp.source_id"])?.context_id;

		/**
		 * Removes all events for the given source files.
		 * Also removes corresponding entries from the `_index` to prevent stale references.
		 */
		public static delete = (app: App.Type, files: Arrayed<Source.Type>) => {
			files = Parser.array(files);

			files.forEach((file) => {
				// Remove from index before deleting
				const events = DataStore.events.get(file.id);
				if (events) {
					events.forEach((e) => Doc.Entity._index.delete(e._id));
				}
				DataStore.events.delete(file.id);
				DataStore.events.set(file.id, []);
				Doc.Entity.clearHashRange(file.id);
			});

			return DataStore.events;
		};

		/** Returns the time range (min/max) of a sorted event array. Assumes descending sort order. */
		public static range = (events: Doc.Type[]) => ({
			max: events[0].gulp_timestamp, //new Date(events[0]["@timestamp"]).valueOf(),
			min: events[events.length - 1].gulp_timestamp, //new Date(events[events.length - 1]["@timestamp"]).valueOf(),
		});

		/** Finds a single event by its ID using the O(1) `_index` Map. */
		public static id = (_app: App.Type, event: Doc.Type["_id"]): Doc.Type =>
			Doc.Entity._index.get(event) as Doc.Type;

		/** Retrieves all events for a given source ID. Auto-initializes an empty array if none exist. */
		public static get = (_app: App.Type, id: Source.Id): Doc.Type[] =>
			DataStore.events.get(id) ||
			(DataStore.events.set(id, []).get(id) as Doc.Type[]);

		/** Sorts events in descending timestamp order (newest first). Mutates the array in place. */
		public static sort = (events: Doc.Type[]) =>
			events.sort((a, b) => b.gulp_timestamp - a.gulp_timestamp);

		/** Returns all events from currently selected sources, flattened into a single array. */
		public static selected = (app: App.Type): Doc.Type[] =>
			Source.Entity.selected(app)
				.map((s) => Doc.Entity.get(app, s.id))
				.flat();

		/**
		 * Adds or updates events into the global event store.
		 * Groups incoming events by source for batched processing.
		 * Updates existing events in place, appends new ones, and maintains
		 * the `_index` Map for O(1) lookups. Re-sorts only if changes were made.
		 */
		public static add = (app: App.Type, events: Doc.Type[]) => {
			const sources = new Set<Source.Id>();

			// Group events by source to optimize processing
			const eventsBySource = new Map<Source.Id, Map<Doc.Id, Doc.Type>>();
			events.forEach((e) => {
				const sourceId = e["gulp.source_id"];
				sources.add(sourceId);
				if (!eventsBySource.has(sourceId)) {
					eventsBySource.set(sourceId, new Map());
				}
				eventsBySource.get(sourceId)!.set(e._id, e);
			});

			// Process each source: Update existing and Add new events
			sources.forEach((id) => {
				const existingEvents = Doc.Entity.get(app, id);
				const newEventsMap = eventsBySource.get(id)!;
				let hasChanges = false;

				// Updating existing events in place
				for (let i = 0; i < existingEvents.length; i++) {
					const evt = existingEvents[i];
					if (newEventsMap.has(evt._id)) {
						const updated = newEventsMap.get(evt._id)!;
						existingEvents[i] = updated;
						Doc.Entity._index.set(updated._id, updated);
						newEventsMap.delete(evt._id);
						hasChanges = true;
					}
				}

				// Add remaining new events
				if (newEventsMap.size > 0) {
					for (const evt of newEventsMap.values()) {
						Doc.Entity._index.set(evt._id, evt);
					}
					// Same fix as addAsync: avoid push(...spread) to prevent RangeError with 1M+ events
					for (const evt of newEventsMap.values()) {
						existingEvents.push(evt);
					}
					hasChanges = true;
				}

				// Sort if we modified the list, optimize performance
				if (hasChanges) {
					Doc.Entity.sort(existingEvents);
					Doc.Entity.updateHashRange(id, existingEvents);
					DataStore.markDirty();
				}
			});

			return DataStore.events;
		};

		/**
		 * Asynchronous version of `add` that prevents main thread freeze.
		 * Use in-place pushing with Event Loop Yielding (chunking) and native Array.prototype.sort().
		 */
		public static addAsync = async (app: App.Type, events: Doc.Type[]) => {
			const sources = new Set<Source.Id>();
			const eventsBySource = new Map<Source.Id, Map<Doc.Id, Doc.Type>>();

			// Group events by source and ID for O(1) deduplication
			events.forEach((e) => {
				const sourceId = e["gulp.source_id"];
				sources.add(sourceId);
				if (!eventsBySource.has(sourceId)) {
					eventsBySource.set(sourceId, new Map());
				}
				eventsBySource.get(sourceId)!.set(e._id, e);
			});

			for (const id of sources) {
				const existingEvents = Doc.Entity.get(app, id);
				const incomingMap = eventsBySource.get(id)!;
				let hasChanges = false;

				// 1. Update existing events in chunks to keep main thread responsive
				const updateChunkSize = 25000;
				for (let i = 0; i < existingEvents.length; i += updateChunkSize) {
					const limit = Math.min(i + updateChunkSize, existingEvents.length);
					for (let j = i; j < limit; j++) {
						const evt = existingEvents[j];
						if (incomingMap.has(evt._id)) {
							const updated = incomingMap.get(evt._id)!;
							existingEvents[j] = updated;
							Doc.Entity._index.set(updated._id, updated);
							incomingMap.delete(evt._id);
							hasChanges = true;
						}
					}
					if (incomingMap.size === 0 && !hasChanges) break; // Optimization
					await new Promise((resolve) => setTimeout(resolve, 0));
				}

				// 2. Add remaining (truly new) events in chunks
				if (incomingMap.size > 0) {
					const trulyNew = Array.from(incomingMap.values());
					const addChunkSize = 10000;
					for (let i = 0; i < trulyNew.length; i += addChunkSize) {
						const chunk = trulyNew.slice(i, i + addChunkSize);
						for (const evt of chunk) {
							Doc.Entity._index.set(evt._id, evt);
							existingEvents.push(evt);
						}
						await new Promise((resolve) => setTimeout(resolve, 0));
					}
					hasChanges = true;
				}

				// Fast in-place native sort if changes were made
				if (hasChanges) {
					Doc.Entity.sort(existingEvents);
					Doc.Entity.updateHashRange(id, existingEvents);
					DataStore.markDirtySoon();
				}
			}
			return DataStore.events;
		};

		/**
		 * Adds grouped and timestamp-sorted documents without re-sorting entire source arrays.
		 * @param app Current application state.
		 * @param batches Normalized documents grouped by source and sorted newest first.
		 * @returns Updated global events map.
		 */
		public static addPreparedAsync = async (
			app: App.Type,
			batches: Doc.PreparedBatch[],
		) => {
			for (const batch of batches) {
				if (batch.docs.length === 0) continue;

				const incomingEvents = Doc.Entity.deduplicateSortedBatch(batch.docs);
				const existingEvents = Doc.Entity.get(app, batch.sourceId);
				const retainedEvents = await Doc.Entity.collectRetainedEventsAsync(
					existingEvents,
					incomingEvents,
				);

				incomingEvents.forEach((event) => {
					Doc.Entity._index.set(event._id, event);
				});

				const mergeResult = await Doc.Entity.mergeSortedEventsAsync(
					retainedEvents,
					incomingEvents,
				);
				DataStore.events.set(batch.sourceId, mergeResult.events);
				Doc.Entity.setHashRange(batch.sourceId, mergeResult.hashRange);
				DataStore.markDirtySoon();
			}

			return DataStore.events;
		};

		/**
		 * Removes duplicate document IDs from an incoming sorted batch, keeping the latest duplicate value.
		 * @param docs Incoming documents sorted by descending timestamp.
		 * @returns Unique documents sorted by descending timestamp.
		 */
		private static deduplicateSortedBatch = (docs: Doc.Type[]): Doc.Type[] => {
			const docsById = new Map<Doc.Id, Doc.Type>();
			docs.forEach((doc) => docsById.set(doc._id, doc));

			if (docsById.size === docs.length) {
				return docs;
			}

			return Doc.Entity.sort(Array.from(docsById.values()));
		};

		/**
		 * Collects existing source events that are not replaced by the incoming batch.
		 * @param existingEvents Current sorted source event array.
		 * @param incomingEvents Incoming unique events for the same source.
		 * @returns Existing events that should be retained before merge.
		 */
		private static collectRetainedEventsAsync = async (
			existingEvents: Doc.Type[],
			incomingEvents: Doc.Type[],
		): Promise<Doc.Type[]> => {
			const incomingIds = new Set(incomingEvents.map((event) => event._id));
			const retainedEvents: Doc.Type[] = [];
			const chunkSize = Doc.Entity.PREPARED_INSERT_YIELD_CHUNK_SIZE;

			for (let i = 0; i < existingEvents.length; i += chunkSize) {
				const limit = Math.min(i + chunkSize, existingEvents.length);
				for (let j = i; j < limit; j++) {
					const event = existingEvents[j];
					if (event && !incomingIds.has(event._id)) {
						retainedEvents.push(event);
					}
				}
				await Doc.Entity.yieldToMainThread();
			}

			return retainedEvents;
		};

		/**
		 * Merges two descending timestamp arrays into one descending timestamp array.
		 * @param existingEvents Retained existing source events.
		 * @param incomingEvents Incoming source events sorted newest first.
		 * @returns Merged source events sorted newest first and their number_hash range.
		 */
		private static mergeSortedEventsAsync = async (
			existingEvents: Doc.Type[],
			incomingEvents: Doc.Type[],
		): Promise<MergeResult> => {
			const mergedEvents = new Array<Doc.Type>(
				existingEvents.length + incomingEvents.length,
			);
			const chunkSize = Doc.Entity.PREPARED_INSERT_YIELD_CHUNK_SIZE;
			let existingIndex = 0;
			let incomingIndex = 0;
			let mergedIndex = 0;
			let min = Infinity;
			let max = -Infinity;

			const trackHashRange = (event: Doc.Type): void => {
				const value = event.number_hash;
				if (typeof value !== "number") return;
				if (value > max) max = value;
				if (value < min) min = value;
			};

			while (mergedIndex < mergedEvents.length) {
				const limit = Math.min(mergedIndex + chunkSize, mergedEvents.length);
				while (mergedIndex < limit) {
					const existingEvent = existingEvents[existingIndex];
					const incomingEvent = incomingEvents[incomingIndex];

					if (!existingEvent) {
						trackHashRange(incomingEvent);
						mergedEvents[mergedIndex++] = incomingEvent;
						incomingIndex++;
						continue;
					}

					if (!incomingEvent) {
						trackHashRange(existingEvent);
						mergedEvents[mergedIndex++] = existingEvent;
						existingIndex++;
						continue;
					}

					if (incomingEvent.gulp_timestamp > existingEvent.gulp_timestamp) {
						trackHashRange(incomingEvent);
						mergedEvents[mergedIndex++] = incomingEvent;
						incomingIndex++;
					} else {
						trackHashRange(existingEvent);
						mergedEvents[mergedIndex++] = existingEvent;
						existingIndex++;
					}
				}

				if (mergedIndex < mergedEvents.length) {
					await Doc.Entity.yieldToMainThread();
				}
			}

			return {
				events: mergedEvents,
				hashRange:
					Number.isFinite(min) && Number.isFinite(max)
						? {
								min,
								max,
								length: mergedEvents.length,
							}
						: undefined,
			};
		};

		/**
		 * Stores or clears the cached number_hash range for a source.
		 * @param sourceId Source identifier.
		 * @param hashRange Range to store, or undefined to clear it.
		 * @returns Nothing.
		 */
		private static setHashRange = (
			sourceId: Source.Id,
			hashRange: Doc.HashRange | undefined,
		): void => {
			if (!hashRange) {
				Doc.Entity._hashRanges.delete(sourceId);
				return;
			}

			Doc.Entity._hashRanges.set(sourceId, hashRange);
		};

		/**
		 * Updates the cached number_hash range for a source.
		 * @param sourceId Source identifier.
		 * @param events Current source events.
		 * @returns Nothing.
		 */
		private static updateHashRange = (
			sourceId: Source.Id,
			events: Doc.Type[],
		): void => {
			if (events.length === 0) {
				Doc.Entity._hashRanges.delete(sourceId);
				return;
			}

			let min = Infinity;
			let max = -Infinity;

			for (let i = 0; i < events.length; i++) {
				const value = events[i]?.number_hash;
				if (typeof value !== "number") continue;
				if (value > max) max = value;
				if (value < min) min = value;
			}

			if (!Number.isFinite(min) || !Number.isFinite(max)) {
				Doc.Entity._hashRanges.delete(sourceId);
				return;
			}

			Doc.Entity._hashRanges.set(sourceId, {
				min,
				max,
				length: events.length,
			});
		};
		/** Finds multiple events by their IDs using the O(1) `_index` Map. Filters out missing entries. */
		public static ids = (_app: App.Type, ids: Doc.Type["_id"][]) =>
			ids.map((id) => Doc.Entity._index.get(id)).filter(Boolean) as Doc.Type[];

		public static notes = (app: App.Type, event: Doc.Type) =>
			Note.Entity.findByFile(app, event["gulp.source_id"]).filter(
				(n) => n.doc._id === event._id,
			);

		/**
		 * Finds every link related to an event, whether the event is the root
		 * `doc_id_from` or one of the connected destination `doc_ids`.
		 * @param _app Current application state, kept for entity API consistency.
		 * @param event Event whose related links should be returned.
		 * @returns Links that reference the event as root or destination.
		 */
		public static links = (_app: App.Type, event: Doc.Type): Link.Type[] =>
			DataStore.links.filter(
				(link) =>
					link.doc_id_from === event._id ||
					link.doc_ids.some((docId) => docId === event._id),
			);

		public static normalize = (
			docs: Doc.Type[],
			field: string,
			hashFunction: Source.Type["settings"]["hash_function"] = "fnv1a",
		): Doc.Type[] => {
			for (let i = 0; i < docs.length; i++) {
				const raw = docs[i] as any;
				docs[i] = {
					_id: raw._id,
					gulp_timestamp: Internal.Transformator.toTimestamp(
						raw["gulp.timestamp"],
						"round",
					),
					"gulp.source_id": raw["gulp.source_id"],
					"gulp.event_code": raw["gulp.event_code"],
					number_hash: Refractor.any.toNumber(
						Refractor.get(raw, field),
						hashFunction,
					),
				} as Doc.Type;
			}
			return docs;
		};
		/**
		 * Helper to get all flagged data from localStorage
		 * @returns Record<Operation.Id, Doc.Id[]>
		 */
		private static _flaggedCache: Record<string, string[]> | null = null;
		private static getFlaggedData = (): Record<string, string[]> => {
			if (Doc.Entity._flaggedCache) {
				return Doc.Entity._flaggedCache;
			}

			const raw = localStorage.getItem(Doc.Entity.flag.KEY);
			if (!raw) {
				return {};
			}

			try {
				const parsed = JSON.parse(raw);
				// Handle migration from old format (array) to new format (object)
				if (Array.isArray(parsed)) {
					Doc.Entity._flaggedCache = {};
					return Doc.Entity._flaggedCache;
				}
				Doc.Entity._flaggedCache = parsed as Record<string, string[]>;
				return Doc.Entity._flaggedCache;
			} catch (_) {
				Doc.Entity._flaggedCache = {};
				return Doc.Entity._flaggedCache;
			}
		};

		/**
		 * Helper to save flagged data to localStorage
		 */
		private static saveFlaggedData = (data: Record<string, string[]>) => {
			const cleaned = Object.fromEntries(
				Object.entries(data).filter(([_, ids]) => ids.length > 0),
			);
			Doc.Entity._flaggedCache = cleaned;
			localStorage.setItem(Doc.Entity.flag.KEY, JSON.stringify(cleaned));
		};

		public static flag: Flag = {
			KEY: "flagged-events",

			/**
			 * Method to get all flagged events from local storage
			 * @param operationId - Optional operation ID to filter by
			 * @returns Set of Doc.Id for the specified operation (or all if not specified)
			 */
			getList: (operationId?: Operation.Id): Set<Doc.Id> => {
				const ids: Set<Doc.Id> = new Set();
				const data = Doc.Entity.getFlaggedData();

				if (operationId) {
					// Return only IDs for the specified operation
					const operationIds = data[operationId] || [];
					operationIds.forEach((id) => ids.add(id as Doc.Id));
				} else {
					// Return all IDs across all operations
					Object.values(data)
						.flat()
						.forEach((id) => {
							if (typeof id === "string") {
								ids.add(id as Doc.Id);
							}
						});
				}

				return ids;
			},

			/**
			 * Method to get all flagged event IDs for the specified operation
			 * @param app - App state
			 * @param operationId - Optional operation ID to filter by
			 * @returns Array of Doc.Id
			 */
			getDocIds: (app: App.Type, operationId?: Operation.Id): Doc.Id[] => {
				const ids = Doc.Entity.flag.getList(operationId);
				if (!ids.size) return [];
				const result: Doc.Id[] = [];

				for (const events of DataStore.events.values()) {
					for (const event of events) {
						if (ids.has(event._id)) {
							result.push(event._id);
						}
					}
				}
				return result;
			},

			/**
			 * Method to get all flagged documents for the specified operation
			 * @param app - App state
			 * @param operationId - Optional operation ID to filter by
			 * @returns Array of Doc.Type
			 */
			getDocs: (app: App.Type, operationId?: Operation.Id): Doc.Type[] => {
				const ids = Doc.Entity.flag.getList(operationId);

				if (!ids.size) return [];

				const result: Doc.Type[] = [];

				for (const events of DataStore.events.values()) {
					for (const event of events) {
						if (ids.has(event._id)) {
							result.push(event);
						}
					}
				}

				return result;
			},

			isLimitReached: (ids = Doc.Entity.flag.getList()) => ids.size >= 10,

			/**
			 * Toggle flag state for a document within a specific operation
			 * @param id Doc.Id
			 * @param operationId Operation.Id to associate the flag with
			 * @returns New document flagged state
			 */
			toggle: (id: Doc.Id, operationId?: Operation.Id) => {
				if (typeof id !== "string") {
					return false;
				}

				if (!operationId) {
					toast.error(translate("doc.cannotFlag"), {
						description: translate("doc.noOperationSelected"),
						richColors: true,
						icon: <Icon name="X" />,
					});
					return false;
				}

				const data = Doc.Entity.getFlaggedData();
				const operationIds = data[operationId] || [];
				const isFlagged = operationIds.includes(id);

				// Check limit for the specific operation
				if (!isFlagged && operationIds.length >= 10) {
					toast.error(translate("doc.limitReached"), {
						description: translate("doc.flagLimitDescription"),
						richColors: true,
						icon: <Icon name="X" />,
					});
					return isFlagged;
				}

				if (isFlagged) {
					// Remove the id
					data[operationId] = operationIds.filter((docId) => docId !== id);
				} else {
					// Add the id
					data[operationId] = [...operationIds, id];
				}

				toast.info(translate(isFlagged ? "doc.unflagged" : "doc.flagged"));

				Doc.Entity.saveFlaggedData(data);
				return !isFlagged;
			},

			/**
			 * Resets flagged events for a specific operation
			 * @param operationId - If provided, only resets flags for that operation. Otherwise resets all.
			 */
			reset: (operationId?: Operation.Id) => {
				if (operationId) {
					const data = Doc.Entity.getFlaggedData();
					delete data[operationId];
					Doc.Entity.saveFlaggedData(data);
				} else {
					localStorage.setItem(Doc.Entity.flag.KEY, JSON.stringify({}));
				}
			},

			/**
			 * Checks if document is flagged
			 * @param id Doc.Id
			 * @param operationId Optional operation ID to check within
			 * @returns Whether the document is flagged
			 */
			isFlagged: (id: Doc.Id, operationId?: Operation.Id) => {
				if (typeof id !== "string") {
					return false;
				}

				const ids = Doc.Entity.flag.getList(operationId);
				return ids.has(id);
			},
		};
	}
}
