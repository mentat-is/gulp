/** Current wire-protocol version shared by the main thread and render worker. */
export const RENDER_PROTOCOL_VERSION = 9 as const;
/** Horizontal graph density interval preserved by both QueryWorker and RenderCore. */
export const RENDER_GRAPH_BUCKET_PIXEL_WIDTH = 10;

/** Render invalidation reasons that can be coalesced into one worker frame. */
export enum DirtyCanvas {
	None = 0,
	Size = 1 << 0,
	Viewport = 1 << 1,
	Sources = 1 << 2,
	Events = 1 << 3,
	Overlays = 1 << 4,
	Theme = 1 << 5,
	Assets = 1 << 6,
	Hover = 1 << 7,
	Force = 1 << 8,
}

/** Drawing algorithms supported by the worker-safe render core. */
export type RenderEngineKind = "default" | "height" | "graph";

/** Inclusive time range used by timeline transforms and overlays. */
export interface RenderTimeRange {
	min: number;
	max: number;
}

/** Concrete colors resolved from CSS variables on the main thread. */
export interface RenderThemePalette {
	fontAccent: string;
	fontSecondary: string;
	backgroundAccent: string;
	backgroundSecondary: string;
	border: string;
}

/** Localized text needed by the renderer without importing localization state. */
export interface RenderLocalizedStrings {
	ingestingSuffix: string;
	loadingSuffix: string;
	unknownContext: string;
	locale?: string;
	timeZone: string;
}

/** Pointer coordinates expressed in primary-canvas pixels. */
export interface RenderPointerState {
	x: number;
	y: number;
	inside: boolean;
}

/** Render-only target event marker. */
export interface RenderTargetMarker {
	sourceId: string;
	timestamp: number;
}

/** Revisions that determine whether renderer caches remain valid. */
export interface RenderFrameRevisions {
	settings: number;
	sources: number;
	overlays: number;
	theme: number;
	assets: number;
}

/** Frequently changing viewport values sent independently from scene configuration. */
export interface RenderViewportState {
	width: number;
	height: number;
	frame: RenderTimeRange;
	scale: number;
	scrollX: number;
	scrollY: number;
	viewportRevision: number;
	inputSequence: number;
	inputTimestamp: number;
}

/** Infrequently changing appearance and source configuration owned by the scene. */
export interface RenderSceneState {
	sourceOrder: string[];
	hidden: {
		notes: boolean;
		links: boolean;
	};
	target: RenderTargetMarker | null;
	palette: RenderThemePalette;
	targetGuideColor: string;
	localized: RenderLocalizedStrings;
	revisions: RenderFrameRevisions;
}

/** Complete worker-internal render state assembled from split control messages. */
export interface RenderFrameState extends RenderViewportState, RenderSceneState {
	pointer: RenderPointerState;
}

/** Optional loading decoration associated with one source. */
export type RenderLoadingKind = "none" | "ingestion" | "loading";

/** Explicit hash-to-color override resolved on the main thread. */
export interface RenderColorOverride {
	hash: number;
	color: string;
}

/** Render-only source metadata; canonical source entities never cross realms. */
export interface RenderSource {
	id: string;
	revision: number;
	rowOrder: number;
	name: string;
	contextLabel: string;
	contextColor: string;
	contextTintAlpha: number;
	timestamp: RenderTimeRange;
	total: number;
	loadingKind: RenderLoadingKind;
	offset: number;
	engine: RenderEngineKind;
	frequency: number;
	gradient: string[];
	overrides: RenderColorOverride[];
	customPaletteColors: string[];
}

/** Mutation semantics for a compact source-event batch. */
export type RenderEventBatchMode = "replace" | "upsert" | "clear";

/**
 * Transferable render projection for one source's events and graph samples.
 * Complete replacement buffers and merged upsert results use descending timestamp order.
 */
export interface RenderEventBatch {
	sourceId: string;
	baseRevision: number;
	revision: number;
	mode: RenderEventBatchMode;
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
	sampleTimestamps?: Float64Array;
	sampleCounts?: Float64Array;
}

/** Begins a bounded full-source snapshot assembled in worker-owned memory. */
export interface RenderEventSnapshotBegin {
	kind: "begin";
	snapshotId: number;
	sourceId: string;
	revision: number;
	totalEvents: number;
	sampleTimestamps?: Float64Array;
	sampleCounts?: Float64Array;
}

/** Transfers one ordered segment of a full-source snapshot. */
export interface RenderEventSnapshotChunk {
	kind: "chunk";
	snapshotId: number;
	sourceId: string;
	offset: number;
	handles: Uint32Array;
	timestamps: Float64Array;
	hashes: Float64Array;
}

/** Atomically stages and publishes a completely received source snapshot. */
export interface RenderEventSnapshotCommit {
	kind: "commit";
	snapshotId: number;
	sourceId: string;
	revision: number;
}

/** Transaction record used to stream large replacements without one main-thread allocation. */
export type RenderEventSnapshotRecord =
	| RenderEventSnapshotBegin
	| RenderEventSnapshotChunk
	| RenderEventSnapshotCommit;

/** Compact note record used for drawing and grouped hit testing. */
export interface RenderNoteRecord {
	id: string;
	sourceId: string;
	timestamp: number;
	name: string;
	color: string;
	iconKey: string;
	groupIconKey?: string;
}

/** Main-thread-resolved link endpoint. */
export interface RenderLinkEndpoint {
	sourceId: string;
	timestamp: number;
	sortKey: string;
}

/** Compact link record with resolved endpoint coordinates. */
export interface RenderLinkRecord {
	id: string;
	color: string;
	iconKey: string;
	endpoints: RenderLinkEndpoint[];
}

/** Compact timeline highlight record. */
export interface RenderHighlightRecord {
	id: string;
	start: number;
	end: number;
	color: string;
}

/** Compact flagged-event record. */
export interface RenderFlagRecord {
	handle: number;
	sourceId: string;
	timestamp: number;
}

/** Revisioned replacement for one overlay collection. */
export interface RenderCollectionPatch<TRecord> {
	revision: number;
	records: TRecord[];
}

/** Independently revisioned overlay changes included in a data patch. */
export interface RenderOverlayPatch {
	notes?: RenderCollectionPatch<RenderNoteRecord>;
	links?: RenderCollectionPatch<RenderLinkRecord>;
	highlights?: RenderCollectionPatch<RenderHighlightRecord>;
	flags?: RenderCollectionPatch<RenderFlagRecord>;
}

/** Complete overlay view used by initial snapshots and recovery replay. */
export interface RenderOverlaySnapshot {
	notes: RenderCollectionPatch<RenderNoteRecord>;
	links: RenderCollectionPatch<RenderLinkRecord>;
	highlights: RenderCollectionPatch<RenderHighlightRecord>;
	flags: RenderCollectionPatch<RenderFlagRecord>;
}

/** Revisioned replacement of source metadata. */
export interface RenderSourcePatch {
	revision: number;
	records: RenderSource[];
	removedSourceIds: string[];
}

/** Finite numeric range retained independently from exact event representatives. */
export interface RenderNumericRange {
	min: number;
	max: number;
}

/**
 * Atomic viewport-resolution projection for one source.
 * Every correlated typed array is replaced together and never merged with another window.
 */
export interface RenderWindowSourceProjection {
	sourceId: string;
	/** Monotonic logical authority independent from the physical page generation. */
	sourceAuthorityRevision: number;
	sourceGeneration: number;
	sourceRevision: number;
	sourceStatus: "building" | "complete" | "incomplete";
	coveredRange: RenderTimeRange;
	bucketPixelWidth: number;
	degradedLod: boolean;
	/** Exact active event count inside the complete source-adjusted frame. */
	frameEventCount: number;
	/** Complete active-generation count retained for authority convergence. */
	loadedCount: number;
	hashRange: RenderNumericRange;
	representativeHandles: Uint32Array;
	representativeTimestamps: Float64Array;
	representativeHashes: Float64Array;
	heightTimestamps: Float64Array;
	heightCounts: Float64Array;
	heightMaximumCount: number;
	graphTimestamps: Float64Array;
	graphCounts: Float64Array;
}

/**
 * Complete multi-source viewport projection produced from one operation snapshot.
 * A newer projection replaces this value in full, including source evictions.
 */
export interface RenderWindowProjection {
	operationEpoch: number;
	viewportRevision: number;
	projectionRevision: number;
	sources: RenderWindowSourceProjection[];
	evictedSourceIds: string[];
}

/** Data-channel payload containing only changed render projections. */
export interface RenderDataPatch {
	globalRevision: number;
	sources?: RenderSourcePatch;
	events?: RenderEventBatch[];
	eventSnapshots?: RenderEventSnapshotRecord[];
	overlays?: RenderOverlayPatch;
	renderWindow?: RenderWindowProjection;
}

/** Font descriptor loadable through FontFace inside the worker. */
export interface RenderWorkerFontSource {
	family: string;
	url: string;
	weight?: string;
	style?: string;
}

/** Common metadata carried by every protocol envelope. */
export interface RenderProtocolEnvelope<TType extends string, TPayload> {
	protocolVersion: typeof RENDER_PROTOCOL_VERSION;
	operationEpoch: number;
	sequence: number;
	type: TType;
	payload: TPayload;
}

/** Initial global-scope message that transfers canvas and both message ports. */
export type RenderWorkerBootstrapMessage = RenderProtocolEnvelope<
	"INIT",
	{
		canvas: OffscreenCanvas;
		controlPort: MessagePort;
		dataPort: MessagePort;
		fonts: RenderWorkerFontSource[];
	}
>;

/** Main-to-worker viewport replacement with an optional pointer from the same input batch. */
export type RenderViewportStateMessage = RenderProtocolEnvelope<
	"VIEWPORT_STATE",
	{
		state: RenderViewportState;
		dirty: DirtyCanvas;
		pointer?: RenderPointerState;
	}
>;

/** Main-to-worker scene replacement posted only when scene configuration changes. */
export type RenderSceneStateMessage = RenderProtocolEnvelope<
	"SCENE_STATE",
	{
		state: RenderSceneState;
		dirty: DirtyCanvas;
	}
>;

/** Standalone main-to-worker pointer update used when no viewport state accompanies it. */
export type RenderPointerMessage = RenderProtocolEnvelope<
	"POINTER",
	{
		pointer: RenderPointerState;
		viewportRevision: number;
	}
>;

/** Main-to-worker request for an overlay hit at a rendered viewport revision. */
export type RenderHitTestMessage = RenderProtocolEnvelope<
	"HIT_TEST",
	{
		requestId: number;
		x: number;
		y: number;
		viewportRevision: number;
	}
>;

/** Main-to-worker request for a magnifier crop of the rendered canvas. */
export type RenderMagnifierRequestMessage = RenderProtocolEnvelope<
	"MAGNIFIER_REQUEST",
	{
		requestId: number;
		x: number;
		y: number;
		cropWidth: number;
		cropHeight: number;
		outputWidth: number;
		outputHeight: number;
		viewportRevision: number;
	}
>;

/** Main-to-worker bitmap response for an icon requested during rendering. */
export type RenderAssetResponseMessage = RenderProtocolEnvelope<
	"ASSET_RESPONSE",
	{
		assetKey: string;
		color: string;
		assetRevision: number;
		bitmap: ImageBitmap;
	}
>;

/** Main-to-worker notification that an icon load failed and may be retried later. */
export type RenderAssetUnavailableMessage = RenderProtocolEnvelope<
	"ASSET_UNAVAILABLE",
	{
		assetKey: string;
		color: string;
	}
>;

/** Main-to-worker page visibility update. */
export type RenderVisibilityMessage = RenderProtocolEnvelope<
	"VISIBILITY",
	{
		visible: boolean;
	}
>;

/** Main-to-worker operation boundary that invalidates all worker-owned state. */
export type RenderResetOperationMessage = RenderProtocolEnvelope<
	"RESET_OPERATION",
	{
		reason: string;
	}
>;

/** Main-to-worker lifecycle termination request. */
export type RenderDisposeMessage = RenderProtocolEnvelope<
	"DISPOSE",
	Record<string, never>
>;

/** Messages carried by the high-priority control port. */
export type RenderControlMessage =
	| RenderViewportStateMessage
	| RenderSceneStateMessage
	| RenderPointerMessage
	| RenderHitTestMessage
	| RenderMagnifierRequestMessage
	| RenderAssetResponseMessage
	| RenderAssetUnavailableMessage
	| RenderVisibilityMessage
	| RenderResetOperationMessage
	| RenderDisposeMessage;

/** Data-channel message containing source/event/overlay changes. */
export type RenderDataMessage = RenderProtocolEnvelope<
	"DATA_PATCH",
	RenderDataPatch
>;

/** Hit-test identity returned without canonical entity objects. */
export type RenderHitTestResult =
	| { kind: "note"; id: string; noteIds: string[] }
	| { kind: "link"; id: string };

/** Per-frame counters reported for profiling and release gates. */
export interface RenderFrameStatistics {
	frameTimestamp: number;
	drawDuration: number;
	workerReceiveDuration: number;
	workerWaitDuration: number;
	dirtyMask: DirtyCanvas;
	viewportRevision: number;
	inputSequence: number;
	dataRevision: number;
	dataLatency: number | null;
	visibleSources: number;
	renderedRows: number;
	renderedEventPixels: number;
	graphSamples: number;
	graphBuckets: number;
	visibleNotes: number;
	visibleLinks: number;
	interactiveRegions: number;
}

/** Frame statistics extended with the newest committed data message included in the draw. */
export interface RenderFrameRenderedStatistics extends RenderFrameStatistics {
	renderedDataSequence: number;
}

/** Source revision accepted by the worker and the data message that applied it. */
export interface RenderAcknowledgedSourceState {
	revision: number;
	dataSequence: number;
}

/** Worker capability response emitted once both ports are attached. */
export type RenderCapabilitiesMessage = RenderProtocolEnvelope<
	"CAPABILITIES",
	{
		offscreenCanvas: boolean;
		workerAnimationFrame: boolean;
		workerFontFace: boolean;
		imageBitmap: boolean;
	}
>;

/** Worker readiness response emitted after fonts resolve or explicitly fall back. */
export type RenderReadyMessage = RenderProtocolEnvelope<
	"READY",
	{
		fontsLoaded: boolean;
		fontFallbackReason?: string;
	}
>;

/** Data acknowledgement used by the client to enforce bounded backpressure. */
export type RenderAckMessage = RenderProtocolEnvelope<
	"ACK",
	{
		acknowledgedSequence: number;
		globalRevision: number;
		sourceRevisions: Array<
			RenderAcknowledgedSourceState & { sourceId: string }
		>;
		rejectedSourceIds: string[];
	}
>;

/** Frame completion response used for profiling and request ordering. */
export type RenderFrameRenderedMessage = RenderProtocolEnvelope<
	"FRAME_RENDERED",
	RenderFrameRenderedStatistics
>;

/** Worker response for an overlay hit-test request. */
export type RenderHitTestResultMessage = RenderProtocolEnvelope<
	"HIT_TEST_RESULT",
	{
		requestId: number;
		viewportRevision: number;
		hit: RenderHitTestResult | null;
	}
>;

/** Worker notification emitted only when the pointer's hovered identity changes. */
export type RenderHoverChangedMessage = RenderProtocolEnvelope<
	"HOVER_CHANGED",
	{
		viewportRevision: number;
		hit: RenderHitTestResult | null;
	}
>;

/** Worker response containing a transferable magnifier bitmap. */
export type RenderMagnifierFrameMessage = RenderProtocolEnvelope<
	"MAGNIFIER_FRAME",
	{
		requestId: number;
		viewportRevision: number;
		bitmap: ImageBitmap;
	}
>;

/** Worker request for one missing icon bitmap. */
export type RenderAssetRequestMessage = RenderProtocolEnvelope<
	"ASSET_REQUEST",
	{
		assetKey: string;
		color: string;
	}
>;

/** Worker request for a revisioned source or collection replacement. */
export type RenderResyncRequiredMessage = RenderProtocolEnvelope<
	"RESYNC_REQUIRED",
	{
		scope: "source" | "sources" | "notes" | "links" | "highlights" | "flags";
		id?: string;
		expectedRevision: number;
		receivedRevision: number;
	}
>;

/** Serializable worker error response used for diagnostics and fallback. */
export type RenderErrorMessage = RenderProtocolEnvelope<
	"ERROR",
	{
		phase: "protocol" | "initialize" | "data" | "render" | "asset" | "magnifier";
		message: string;
		fatal: boolean;
		stack?: string;
	}
>;

/** All responses that may be emitted through a worker-owned message port. */
export type RenderWorkerResponse =
	| RenderCapabilitiesMessage
	| RenderReadyMessage
	| RenderAckMessage
	| RenderFrameRenderedMessage
	| RenderHitTestResultMessage
	| RenderHoverChangedMessage
	| RenderMagnifierFrameMessage
	| RenderAssetRequestMessage
	| RenderResyncRequiredMessage
	| RenderErrorMessage;
