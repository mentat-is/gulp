import type { DataStoreRenderChange } from "@/store/DataStore";
import { DataStore } from "@/store/DataStore";
import type { Doc } from "@/entities/Doc";
import type { Highlight } from "@/entities/Highlight";
import type { Link } from "@/entities/Link";
import type { Note } from "@/entities/Note";
import type { Source } from "@/entities/Source";
import type {
	RenderDataPatch,
	RenderEventBatch,
	RenderEventBatchMode,
	RenderFlagRecord,
	RenderHighlightRecord,
	RenderLinkRecord,
	RenderLoadingKind,
	RenderNoteRecord,
	RenderOverlaySnapshot,
	RenderSource,
	RenderSupplementalEventRecord,
} from "./RenderWorker.types";
import { getRenderSourceName } from "./RenderSourceLabel";

const DEFAULT_PROJECTION_SLICE_MS = 4;
const PROJECTION_RECORD_BLOCK_SIZE = 2_048;
const RENDER_ASSET_ICON_PREFIX = "icon:";

export interface RenderProjectionAdapterOptions {
	resolveDocument: (documentId: Doc.Id) => Doc.Type | undefined;
	resolveNoteIconKey?: (note: Note.Type) => string;
	resolveLinkIconKey?: (link: Link.Type) => string;
}

export interface RenderSourceProjectionInput {
	source: Source.Type;
	revision: number;
	rowOrder: number;
	contextLabel: string;
	contextColor: string;
	contextTintAlpha?: number;
	loadingKind: RenderLoadingKind;
	cacheWarning: boolean;
	gradient: string[];
	customPaletteColors?: string[];
}

export interface RenderGraphSampleInput {
	minTimestamp: number;
	count: number;
}

export interface RenderOverlayProjectionInput {
	notes: readonly Note.Type[];
	notesRevision: number;
	links: readonly Link.Type[];
	linksRevision: number;
	highlights: readonly Highlight.Type[];
	highlightsRevision: number;
	flaggedDocuments: readonly Doc.Type[];
	flagsRevision: number;
	supplementalEvents: readonly Doc.Type[];
	supplementalEventsRevision: number;
}

export type RenderChangeProjection =
	| {
		kind: "data";
		queueKey: string;
		patch: RenderDataPatch;
	}
	| {
		kind: "reset";
		operationEpoch: number;
	};

/**
 * Encodes a CanvasIcon-compatible icon name as a namespaced worker asset key.
 * @param iconName Built-in or registered custom glyph name.
 * @returns Stable asset key safe to use in the render protocol.
 */
export function createRenderAssetIconKey(iconName: string): string {
	return `${RENDER_ASSET_ICON_PREFIX}${encodeURIComponent(iconName)}`;
}

/**
 * Resolves a worker asset key back to the icon name accepted by CanvasIcon.
 * @param assetKey Namespaced asset key received through ASSET_REQUEST.
 * @returns Decoded icon name, or null when the key does not represent an icon.
 */
export function resolveRenderAssetIconName(assetKey: string): string | null {
	if (!assetKey.startsWith(RENDER_ASSET_ICON_PREFIX)) return null;
	try {
		const iconName = decodeURIComponent(
			assetKey.slice(RENDER_ASSET_ICON_PREFIX.length),
		);
		return iconName || null;
	} catch {
		return null;
	}
}

/**
 * Creates the semantic source-metadata signature used to advance worker revisions.
 * @param input Source projection values that can affect a rendered row.
 * @returns Stable serialized signature, including the derived cache-warning flag.
 */
export function createRenderSourceMetadataSignature(
	input: RenderSourceProjectionInput,
): string {
	const { source } = input;
	return JSON.stringify([
		source.name,
		source.total,
		source.timestamp.min,
		source.timestamp.max,
		source.settings.offset,
		source.settings.render_engine,
		source.settings.timeline_label,
		source.settings.frequency_sample,
		source.settings.render_color_palette,
		source.settings.color_override?.lookup ?? null,
		source.settings.color_palette?.id ?? null,
		input.customPaletteColors ?? null,
		input.contextLabel,
		input.contextColor,
		input.contextTintAlpha ?? 0.08,
		input.loadingKind,
		input.cacheWarning,
		input.rowOrder,
	]);
}

/**
 * Yields to the browser between projection slices so large snapshots do not form one long task.
 * @returns Promise resolved in a later macrotask.
 */
function yieldToMainThread(): Promise<void> {
	return new Promise((resolve) => globalThis.setTimeout(resolve, 0));
}

/**
 * Reads a monotonic clock where available for time-bounded projection slices.
 * @returns Current high-resolution or wall-clock timestamp in milliseconds.
 */
function now(): number {
	return typeof performance === "undefined" ? Date.now() : performance.now();
}

/**
 * Converts an event mutation name into the worker repository's batch semantics.
 * @param mutation Datastore source-event mutation.
 * @returns Worker event batch mode.
 */
function getEventBatchMode(
	mutation: Extract<DataStoreRenderChange, { type: "source-events" }>["mutation"],
): RenderEventBatchMode {
	if (mutation === "replace") return "replace";
	if (mutation === "clear") return "clear";
	return "upsert";
}

/**
 * Converts a source color-override lookup into deterministic worker records.
 * @param lookup Numeric hash-to-color lookup from source settings.
 * @returns Sorted render override records.
 */
function projectColorOverrides(
	lookup: Record<number, string> | undefined,
): RenderSource["overrides"] {
	if (!lookup) return [];
	return Object.entries(lookup)
		.map(([hash, color]) => ({ hash: Number(hash), color }))
		.filter((record) => Number.isFinite(record.hash))
		.sort((left, right) => left.hash - right.hash);
}

/**
 * Builds compact, transferable worker projections from canonical main-thread entities.
 */
export class RenderProjectionAdapter {
	private readonly options: RenderProjectionAdapterOptions;

	/**
	 * Creates a projection adapter with explicit document, theme, and asset resolvers.
	 * @param options Main-thread resolvers unavailable inside a dedicated worker.
	 */
	public constructor(options: RenderProjectionAdapterOptions) {
		this.options = options;
	}

	/**
	 * Projects canonical source metadata into a worker-safe render-only DTO.
	 * @param input Source plus already-resolved context, loading, and palette fields.
	 * @returns Worker-safe source metadata without entity methods or DOM dependencies.
	 */
	public createSource(input: RenderSourceProjectionInput): RenderSource {
		const { source } = input;
		return {
			id: source.id,
			revision: input.revision,
			rowOrder: input.rowOrder,
			name: getRenderSourceName(source),
			contextLabel: input.contextLabel,
			contextColor: input.contextColor,
			contextTintAlpha: input.contextTintAlpha ?? 0.08,
			timestamp: {
				min: source.timestamp.min,
				max: source.timestamp.max,
			},
			total: source.total,
			loadingKind: input.loadingKind,
			cacheWarning: input.cacheWarning,
			offset: source.settings.offset,
			engine: source.settings.render_engine,
			frequency: source.settings.frequency_sample,
			gradient: [...input.gradient],
			overrides: projectColorOverrides(
				source.settings.color_override?.lookup,
			),
			customPaletteColors: [...(input.customPaletteColors ?? [])],
		};
	}

	/**
	 * Projects multiple source DTOs while preserving caller-provided row order.
	 * @param inputs Source projection inputs in visible source order.
	 * @returns Worker-safe source metadata records.
	 */
	public createSources(inputs: readonly RenderSourceProjectionInput[]): RenderSource[] {
		return inputs.map((input) => this.createSource(input));
	}

	/**
	 * Converts a source's cached server aggregation into graph sample DTOs.
	 * @param source Canonical source containing optional sampled aggregation data.
	 * @returns Worker-safe graph samples, or an empty array when no sample is loaded.
	 */
	public createGraphSamples(source: Source.Type): RenderGraphSampleInput[] {
		const cachedSamples = source._sampleDataCached;
		if (
			!cachedSamples ||
			cachedSamples.frequency_sample !== source.settings.frequency_sample ||
			cachedSamples.min_timestamp !== source.timestamp.min ||
			cachedSamples.max_timestampe !== source.timestamp.max
		) {
			return [];
		}
		return (cachedSamples.sample_data ?? []).map((sample) => ({
			minTimestamp: sample.min_timestamp,
			count: sample.sample,
		}));
	}

	/**
	 * Builds one transferable event batch synchronously for small snapshots and deltas.
	 * @param sourceId Source that owns all supplied documents.
	 * @param revision Resulting source-local revision.
	 * @param baseRevision Worker revision against which an upsert delta applies.
	 * @param mode Replacement, upsert, or clear semantics.
	 * @param documents Canonical documents to project.
	 * @param graphSamples Optional server-side graph samples for replacement snapshots.
	 * @returns Newly allocated typed arrays safe to detach through postMessage.
	 */
	public createEventBatch(
		sourceId: Source.Id,
		revision: number,
		baseRevision: number,
		mode: RenderEventBatchMode,
		documents: readonly Doc.Type[],
		graphSamples?: readonly RenderGraphSampleInput[],
	): RenderEventBatch {
		const handles = new Uint32Array(documents.length);
		const timestamps = new Float64Array(documents.length);
		const hashes = new Float64Array(documents.length);
		const sampleTimestamps = graphSamples
			? new Float64Array(graphSamples.length)
			: undefined;
		const sampleCounts = graphSamples
			? new Float64Array(graphSamples.length)
			: undefined;

		for (let index = 0; index < documents.length; index++) {
			const document = documents[index];
			handles[index] = DataStore.getRenderHandle(sourceId, document._id);
			timestamps[index] = document.gulp_timestamp;
			hashes[index] = document.number_hash;
		}
		for (let index = 0; index < (graphSamples?.length ?? 0); index++) {
			sampleTimestamps![index] = graphSamples![index].minTimestamp;
			sampleCounts![index] = graphSamples![index].count;
		}

		return {
			sourceId,
			revision,
			baseRevision,
			mode,
			handles,
			timestamps,
			hashes,
			sampleTimestamps,
			sampleCounts,
		};
	}

	/**
	 * Builds one transferable event batch in time-bounded slices for large snapshots.
	 * @param sourceId Source that owns all supplied documents.
	 * @param revision Resulting source-local revision.
	 * @param baseRevision Worker revision against which an upsert delta applies.
	 * @param mode Replacement, upsert, or clear semantics.
	 * @param documents Canonical documents to project.
	 * @param graphSamples Optional server-side graph samples for replacement snapshots.
	 * @param maximumSliceMs Maximum uninterrupted main-thread projection time.
	 * @param yieldCallback Optional scheduler-aware continuation used between slices.
	 * @param documentStartIndex Inclusive source-array index to project.
	 * @param documentEndIndex Exclusive source-array index to project.
	 * @returns Promise resolving to newly allocated transferable typed arrays.
	 */
	public async createEventBatchAsync(
		sourceId: Source.Id,
		revision: number,
		baseRevision: number,
		mode: RenderEventBatchMode,
		documents: readonly Doc.Type[],
		graphSamples?: readonly RenderGraphSampleInput[],
		maximumSliceMs = DEFAULT_PROJECTION_SLICE_MS,
		yieldCallback: () => Promise<void> = yieldToMainThread,
		documentStartIndex = 0,
		documentEndIndex = documents.length,
	): Promise<RenderEventBatch> {
		const operationEpoch = DataStore.getRenderOperationEpoch();
		this.assertOperationEpoch(operationEpoch);
		const safeDocumentStartIndex = Math.max(
			0,
			Math.min(documents.length, documentStartIndex),
		);
		const safeDocumentEndIndex = Math.max(
			safeDocumentStartIndex,
			Math.min(documents.length, documentEndIndex),
		);
		const documentCount = safeDocumentEndIndex - safeDocumentStartIndex;
		const graphSampleCount = graphSamples?.length ?? 0;
		const handles = new Uint32Array(documentCount);
		const timestamps = new Float64Array(documentCount);
		const hashes = new Float64Array(documentCount);
		const sampleTimestamps = graphSamples
			? new Float64Array(graphSampleCount)
			: undefined;
		const sampleCounts = graphSamples
			? new Float64Array(graphSampleCount)
			: undefined;
		let sliceStartedAt = now();

		for (
			let blockStartIndex = safeDocumentStartIndex;
			blockStartIndex < safeDocumentEndIndex;
			blockStartIndex += PROJECTION_RECORD_BLOCK_SIZE
		) {
			const blockEndIndex = Math.min(
				safeDocumentEndIndex,
				blockStartIndex + PROJECTION_RECORD_BLOCK_SIZE,
			);
			const outputStartIndex = blockStartIndex - safeDocumentStartIndex;
			DataStore.writeRenderHandleBuffer(
				sourceId,
				documents,
				handles,
				blockStartIndex,
				blockEndIndex,
				outputStartIndex,
			);
			for (
				let documentIndex = blockStartIndex;
				documentIndex < blockEndIndex;
				documentIndex++
			) {
				const outputIndex = documentIndex - safeDocumentStartIndex;
				const document = documents[documentIndex];
				timestamps[outputIndex] = document.gulp_timestamp;
				hashes[outputIndex] = document.number_hash;
			}

			if (
				blockEndIndex < safeDocumentEndIndex &&
				now() - sliceStartedAt >= maximumSliceMs
			) {
				await yieldCallback();
				this.assertOperationEpoch(operationEpoch);
				sliceStartedAt = now();
			}
		}
		for (
			let blockStartIndex = 0;
			blockStartIndex < graphSampleCount;
			blockStartIndex += PROJECTION_RECORD_BLOCK_SIZE
		) {
			const blockEndIndex = Math.min(
				graphSampleCount,
				blockStartIndex + PROJECTION_RECORD_BLOCK_SIZE,
			);
			for (let index = blockStartIndex; index < blockEndIndex; index++) {
				sampleTimestamps![index] = graphSamples![index].minTimestamp;
				sampleCounts![index] = graphSamples![index].count;
			}
			if (
				blockEndIndex < graphSampleCount &&
				now() - sliceStartedAt >= maximumSliceMs
			) {
				await yieldCallback();
				this.assertOperationEpoch(operationEpoch);
				sliceStartedAt = now();
			}
		}

		return {
			sourceId,
			revision,
			baseRevision,
			mode,
			handles,
			timestamps,
			hashes,
			sampleTimestamps,
			sampleCounts,
		};
	}

	/**
	 * Stops an asynchronous projection before it can repopulate handles after reset.
	 * @param expectedEpoch Operation epoch captured when projection work started.
	 * @returns Nothing.
	 * @throws Error when an operation reset occurred during a cooperative yield.
	 */
	private assertOperationEpoch(expectedEpoch: number): void {
		if (DataStore.getRenderOperationEpoch() !== expectedEpoch) {
			throw new Error("Render projection cancelled after operation reset.");
		}
	}

	/**
	 * Converts one observable datastore mutation into a queue-ready worker update.
	 * Collection mutations intentionally send compact full replacements because the
	 * overlay protocol and hit-region cache are collection-revision atomic.
	 * @param change Revisioned canonical datastore change.
	 * @returns Data update or operation-reset command for the client.
	 */
	public createDataStoreChangeProjection(
		change: DataStoreRenderChange,
	): RenderChangeProjection {
		switch (change.type) {
			case "operation-reset":
				return { kind: "reset", operationEpoch: change.operationEpoch };
			case "source-events": {
				const mode = getEventBatchMode(change.mutation);
				const documents =
					mode === "replace"
						? change.events
						: mode === "clear"
							? []
							: change.changedEvents;
				return {
					kind: "data",
					queueKey: `events:${change.sourceId}`,
					patch: {
						globalRevision: change.sequence,
						events: [
							this.createEventBatch(
								change.sourceId,
								change.sourceRevision,
								Math.max(0, change.sourceRevision - 1),
								mode,
								documents,
							),
						],
					},
				};
			}
			case "supplemental-events":
				return {
					kind: "data",
					queueKey: "overlays:supplemental-events",
					patch: {
						globalRevision: change.sequence,
						overlays: {
							supplementalEvents: {
								revision: change.collectionRevision,
								records: this.createSupplementalEventRecords(
									change.events,
								),
							},
						},
					},
				};
			case "notes":
				return {
					kind: "data",
					queueKey: "overlays:notes",
					patch: {
						globalRevision: change.sequence,
						overlays: {
							notes: {
								revision: change.collectionRevision,
								records: this.createNoteRecords(change.notes),
							},
						},
					},
				};
			case "links":
				return {
					kind: "data",
					queueKey: "overlays:links",
					patch: {
						globalRevision: change.sequence,
						overlays: {
							links: {
								revision: change.collectionRevision,
								records: this.createLinkRecords(change.links),
							},
						},
					},
				};
			case "highlights":
				return {
					kind: "data",
					queueKey: "overlays:highlights",
					patch: {
						globalRevision: change.sequence,
						overlays: {
							highlights: {
								revision: change.collectionRevision,
								records: this.createHighlightRecords(change.highlights),
							},
						},
					},
				};
		}
	}

	/**
	 * Creates an initial full replacement for one source's canonical documents.
	 * @param sourceId Source whose current datastore events are projected.
	 * @param globalRevision Global datastore revision attached to the snapshot.
	 * @param graphSamples Server-side samples used by the graph engine.
	 * @returns Queue-ready full source replacement patch.
	 */
	public createSourceSnapshotProjection(
		sourceId: Source.Id,
		globalRevision: number,
		graphSamples: readonly RenderGraphSampleInput[] = [],
	): RenderChangeProjection {
		return {
			kind: "data",
			queueKey: `events:${sourceId}`,
			patch: {
				globalRevision,
				events: [
					this.createEventBatch(
						sourceId,
						DataStore.getSourceRevision(sourceId),
						0,
						"replace",
						DataStore.getSourceEvents(sourceId),
						graphSamples,
					),
				],
			},
		};
	}

	/**
	 * Projects one full source snapshot in time-bounded main-thread slices.
	 * @param sourceId Source whose current datastore events are projected.
	 * @param globalRevision Global datastore revision attached to the snapshot.
	 * @param graphSamples Server-side samples used by the graph engine.
	 * @param maximumSliceMs Maximum uninterrupted projection time before yielding.
	 * @param yieldCallback Optional scheduler-aware continuation used between slices.
	 * @returns Promise resolving to a queue-ready full source replacement patch.
	 */
	public async createSourceSnapshotProjectionAsync(
		sourceId: Source.Id,
		globalRevision: number,
		graphSamples: readonly RenderGraphSampleInput[] = [],
		maximumSliceMs = DEFAULT_PROJECTION_SLICE_MS,
		yieldCallback: () => Promise<void> = yieldToMainThread,
	): Promise<RenderChangeProjection> {
		const batch = await this.createEventBatchAsync(
			sourceId,
			DataStore.getSourceRevision(sourceId),
			0,
			"replace",
			DataStore.getSourceEvents(sourceId),
			graphSamples,
			maximumSliceMs,
			yieldCallback,
		);
		return {
			kind: "data",
			queueKey: `events:${sourceId}`,
			patch: {
				globalRevision,
				events: [batch],
			},
		};
	}

	/**
	 * Projects canonical notes into compact render and grouped-hit-test records.
	 * @param notes Canonical notes to project.
	 * @returns Worker-safe note records.
	 */
	public createNoteRecords(notes: readonly Note.Type[]): RenderNoteRecord[] {
		const records: RenderNoteRecord[] = [];
		for (const note of notes) {
			if (!note.doc) continue;
			records.push({
				id: note.id,
				sourceId: note.doc["gulp.source_id"],
				timestamp: note.doc.gulp_timestamp,
				name: note.name,
				color: note.color?.trim() || "",
				iconKey:
					this.options.resolveNoteIconKey?.(note) ??
					createRenderAssetIconKey(String(note.glyph_id ?? "note")),
				groupIconKey: createRenderAssetIconKey("Status"),
			});
		}
		return records;
	}

	/**
	 * Projects links with main-thread-resolved, de-duplicated event endpoints.
	 * @param links Canonical links to project.
	 * @returns Worker-safe link records with stable endpoint tie-break keys.
	 */
	public createLinkRecords(links: readonly Link.Type[]): RenderLinkRecord[] {
		const records: RenderLinkRecord[] = [];
		for (const link of links) {
			const endpoints: RenderLinkRecord["endpoints"] = [];
			const seenDocumentIds = new Set<Doc.Id>();
			let hasMissingDocument = false;
			for (const documentId of [link.doc_id_from, ...link.doc_ids]) {
				if (seenDocumentIds.has(documentId)) continue;
				seenDocumentIds.add(documentId);
				const document = this.options.resolveDocument(documentId);
				if (!document) {
					hasMissingDocument = true;
					break;
				}
				endpoints.push({
					sourceId: document["gulp.source_id"],
					timestamp: document.gulp_timestamp,
					sortKey: String(document._id),
				});
			}
			if (hasMissingDocument) continue;

			records.push({
				id: link.id,
				color: link.color?.trim() || "",
				iconKey:
					this.options.resolveLinkIconKey?.(link) ??
					createRenderAssetIconKey(String(link.glyph_id ?? "link")),
				endpoints,
			});
		}
		return records;
	}

	/**
	 * Projects timeline highlights into compact range records.
	 * @param highlights Canonical highlights to project.
	 * @returns Worker-safe highlight records.
	 */
	public createHighlightRecords(
		highlights: readonly Highlight.Type[],
	): RenderHighlightRecord[] {
		return highlights.map((highlight) => ({
			id: highlight.id,
			start: highlight.time_range[0],
			end: highlight.time_range[1],
			color: highlight.color,
		}));
	}

	/**
	 * Projects flagged canonical documents using the same source-local handles as events.
	 * @param documents Flagged documents resolved on the main thread.
	 * @returns Worker-safe flagged-event records.
	 */
	public createFlagRecords(documents: readonly Doc.Type[]): RenderFlagRecord[] {
		return documents.map((document) => ({
			handle: DataStore.getRenderHandle(
				document["gulp.source_id"],
				document._id,
			),
			sourceId: document["gulp.source_id"],
			timestamp: document.gulp_timestamp,
		}));
	}

	/**
	 * Projects explicitly included note targets into compact render-only records.
	 * @param documents Normalized supplemental event documents.
	 * @returns Worker-safe event marker records.
	 */
	public createSupplementalEventRecords(
		documents: readonly Doc.Type[],
	): RenderSupplementalEventRecord[] {
		return documents.map((document) => ({
			id: document._id,
			sourceId: document["gulp.source_id"],
			timestamp: document.gulp_timestamp,
			numberHash: document.number_hash,
		}));
	}

	/**
	 * Creates the complete revisioned overlay snapshot used during startup and replay.
	 * @param input Canonical overlay collections and their current revisions.
	 * @returns Compact overlay snapshot safe to post to the worker.
	 */
	public createOverlaySnapshot(
		input: RenderOverlayProjectionInput,
	): RenderOverlaySnapshot {
		return {
			notes: {
				revision: input.notesRevision,
				records: this.createNoteRecords(input.notes),
			},
			links: {
				revision: input.linksRevision,
				records: this.createLinkRecords(input.links),
			},
			highlights: {
				revision: input.highlightsRevision,
				records: this.createHighlightRecords(input.highlights),
			},
			flags: {
				revision: input.flagsRevision,
				records: this.createFlagRecords(input.flaggedDocuments),
			},
			supplementalEvents: {
				revision: input.supplementalEventsRevision,
				records: this.createSupplementalEventRecords(
					input.supplementalEvents,
				),
			},
		};
	}
}
