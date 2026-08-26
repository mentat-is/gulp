import {
	DirtyCanvas,
	RENDER_PROTOCOL_VERSION,
	type RenderAcknowledgedSourceState,
	type RenderAssetRequestMessage,
	type RenderCapabilitiesMessage,
	type RenderControlMessage,
	type RenderDataMessage,
	type RenderDataPatch,
	type RenderErrorMessage,
	type RenderFrameRenderedStatistics,
	type RenderHitTestResult,
	type RenderPointerState,
	type RenderReadyMessage,
	type RenderResyncRequiredMessage,
	type RenderSceneState,
	type RenderViewportState,
	type RenderWorkerBootstrapMessage,
	type RenderWorkerFontSource,
	type RenderWorkerResponse,
} from "./RenderWorker.types";
import {
	getDataPatchTransferables,
	isOperationEpochAdvance,
} from "./RenderDataPatch";
import {
	applyAcknowledgedSourceRevisions,
	isRenderWorkerResponse,
} from "./RenderWorker.response";
import { createDefaultRenderWorker } from "./RenderWorker.factory";
import {
	RenderDataQueue,
	type RenderWorkerDataQueueDepth,
} from "./RenderDataQueue";

export { mergeRenderDataPatches } from "./RenderDataPatch";
export { MAX_RENDER_DATA_QUEUE_DEPTH } from "./RenderDataQueue";
export type { RenderWorkerDataQueueDepth } from "./RenderDataQueue";

const DEFAULT_INTERACTION_TIMEOUT_MS = 5_000;

let workerRenderingDisabledForSession = false;

type RenderCapabilities = RenderCapabilitiesMessage["payload"];
type RenderReadyState = RenderReadyMessage["payload"];
type RenderAssetRequest = RenderAssetRequestMessage["payload"];
type RenderResyncRequest = RenderResyncRequiredMessage["payload"];
type RenderWorkerError = RenderErrorMessage["payload"];
type RenderAcknowledgement = Extract<
	RenderWorkerResponse,
	{ type: "ACK" }
>["payload"];

export interface RenderWorkerCapabilityProbe {
	supported: boolean;
	reason?: string;
}

export interface RenderWorkerAsset {
	bitmap: ImageBitmap;
	revision: number;
}

export interface RenderWorkerClientSynchronizationState {
	ready: boolean;
	operationEpoch: number;
	inFlightDataMessages: number;
	pendingDataMessages: number;
	acknowledgedGlobalRevision: number;
	acknowledgedDataSequence: number;
	acknowledgedDataAtMs: number;
	acknowledgedSourceRevisions: ReadonlyMap<string, number>;
	acknowledgedSourceStates: ReadonlyMap<string, RenderAcknowledgedSourceState>;
	acknowledgedSourceAtMs: ReadonlyMap<string, number>;
	renderedGlobalRevision: number;
	renderedDataSequence: number;
	renderedDataAtMs: number;
	requestedViewportRevision: number;
	renderedViewportRevision: number;
}

export interface RenderWorkerClientOptions {
	fonts?: RenderWorkerFontSource[];
	interactionTimeoutMs?: number;
	initializationTimeoutMs?: number;
	workerFactory?: () => Worker;
	onCapabilities?: (capabilities: RenderCapabilities) => void;
	onReady?: (state: RenderReadyState) => void;
	onFrameRendered?: (statistics: RenderFrameRenderedStatistics) => void;
	onDataQueueDepthChanged?: (depth: RenderWorkerDataQueueDepth) => void;
	onFatalError?: (error: Error) => void;
	onWorkerError?: (error: RenderWorkerError) => void;
	onHoverChanged?: (
		hit: RenderHitTestResult | null,
		viewportRevision: number,
	) => void;
	onResyncRequired?: (request: RenderResyncRequest) => void;
	onAssetRequest?: (
		request: RenderAssetRequest,
	) => RenderWorkerAsset | null | Promise<RenderWorkerAsset | null> | void;
}

export interface RenderMagnifierCropRequest {
	x: number;
	y: number;
	cropWidth: number;
	cropHeight: number;
	outputWidth: number;
	outputHeight: number;
	viewportRevision: number;
}

interface PendingInteraction<TResult> {
	resolve: (result: TResult) => void;
	reject: (error: Error) => void;
	timeout: ReturnType<typeof setTimeout>;
	requestedViewportRevision: number;
}

/** Latest viewport control retained while one earlier viewport awaits a rendered frame. */
interface PendingViewportControl {
	state: RenderViewportState;
	dirty: DirtyCanvas;
	pointer?: RenderPointerState;
}

/**
 * Returns whether a value is an object that can safely be inspected as a protocol payload.
 * @param value Candidate message value.
 * @returns True when the value is a non-null record.
 */
function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

/**
 * Owns one transferred timeline canvas and the two priority-separated worker channels.
 */
export class RenderWorkerClient {
	private readonly options: RenderWorkerClientOptions;
	private readonly worker: Worker;
	private readonly controlPort: MessagePort;
	private readonly dataPort: MessagePort;
	private readonly interactionTimeoutMs: number;
	private readonly readyResolve: (state: RenderReadyState) => void;
	private readonly readyReject: (error: Error) => void;
	private readonly dataQueue = new RenderDataQueue();
	private readonly acknowledgedSourceStates = new Map<
		string,
		RenderAcknowledgedSourceState
	>();
	private readonly acknowledgedSourceAtMs = new Map<string, number>();
	private readonly pendingHitTests = new Map<
		number,
		PendingInteraction<RenderHitTestResult | null>
	>();
	private readonly pendingMagnifiers = new Map<
		number,
		PendingInteraction<ImageBitmap | null>
	>();
	private sequence = 0;
	private requestId = 0;
	private operationEpoch: number;
	private readyState = false;
	private disposed = false;
	private fatal = false;
	private initializationTimeout: ReturnType<typeof setTimeout> | null = null;
	private lastRenderedViewportRevision = 0;
	private latestFrameViewportRevision = 0;
	private postedViewportRevisionAwaitingFrame: number | null = null;
	private pendingViewportControl: PendingViewportControl | null = null;
	private acknowledgedGlobalRevision = 0;
	private lastAcknowledgedDataSequence = 0;
	private lastAcknowledgedDataAtMs = 0;
	private lastRenderedGlobalRevision = 0;
	private lastRenderedDataSequence = 0;
	private lastRenderedDataAtMs = 0;

	public readonly ready: Promise<RenderReadyState>;

	/**
	 * Creates and initializes one render worker without ever acquiring the canvas context.
	 * @param canvas HTML canvas whose ownership will be transferred permanently.
	 * @param operationEpoch Current datastore operation epoch.
	 * @param options Worker callbacks, fonts, and optional test factory.
	 */
	private constructor(
		canvas: HTMLCanvasElement,
		operationEpoch: number,
		options: RenderWorkerClientOptions,
	) {
		this.options = options;
		this.operationEpoch = operationEpoch;
		this.interactionTimeoutMs =
			options.interactionTimeoutMs ?? DEFAULT_INTERACTION_TIMEOUT_MS;

		let resolveReady!: (state: RenderReadyState) => void;
		let rejectReady!: (error: Error) => void;
		this.ready = new Promise<RenderReadyState>((resolve, reject) => {
			resolveReady = resolve;
			rejectReady = reject;
		});
		// Bootstrap can fail synchronously before Canvas receives the client. Attach a
		// handler immediately while leaving the public promise rejected for callers.
		void this.ready.catch(() => undefined);
		this.readyResolve = resolveReady;
		this.readyReject = rejectReady;

		this.worker = options.workerFactory?.() ?? createDefaultRenderWorker();
		const controlChannel = new MessageChannel();
		const dataChannel = new MessageChannel();
		this.controlPort = controlChannel.port1;
		this.dataPort = dataChannel.port1;

		this.worker.addEventListener("message", this.handleWorkerMessage);
		this.worker.addEventListener("error", this.handleWorkerRuntimeError);
		this.worker.addEventListener("messageerror", this.handleMessageError);
		this.controlPort.addEventListener("message", this.handleWorkerMessage);
		this.controlPort.addEventListener("messageerror", this.handleMessageError);
		this.dataPort.addEventListener("message", this.handleWorkerMessage);
		this.dataPort.addEventListener("messageerror", this.handleMessageError);
		this.controlPort.start();
		this.dataPort.start();

		try {
			const offscreenCanvas = canvas.transferControlToOffscreen();
			const bootstrap: RenderWorkerBootstrapMessage = {
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: this.operationEpoch,
				sequence: this.nextSequence(),
				type: "INIT",
				payload: {
					canvas: offscreenCanvas,
					controlPort: controlChannel.port2,
					dataPort: dataChannel.port2,
					fonts: options.fonts ?? [],
				},
			};
			this.initializationTimeout = setTimeout(() => {
				this.failFatal(new Error("Render worker initialization timed out."));
			}, options.initializationTimeoutMs ?? this.interactionTimeoutMs);
			this.worker.postMessage(bootstrap, [
				offscreenCanvas,
				controlChannel.port2,
				dataChannel.port2,
			]);
		} catch (error) {
			this.failFatal(this.toError(error, "Failed to transfer the render canvas"));
		}
	}

	/**
	 * Checks synchronous main-thread capabilities required before canvas transfer.
	 * @returns Capability result with a diagnostic reason when worker mode is unavailable.
	 */
	public static probeCapabilities(): RenderWorkerCapabilityProbe {
		if (workerRenderingDisabledForSession) {
			return {
				supported: false,
				reason: "Worker rendering was disabled after a fatal session error.",
			};
		}
		if (typeof Worker === "undefined") {
			return { supported: false, reason: "Dedicated workers are unavailable." };
		}
		if (typeof MessageChannel === "undefined") {
			return { supported: false, reason: "Message channels are unavailable." };
		}
		if (typeof OffscreenCanvas === "undefined") {
			return { supported: false, reason: "OffscreenCanvas is unavailable." };
		}
		if (typeof createImageBitmap !== "function") {
			return { supported: false, reason: "ImageBitmap creation is unavailable." };
		}
		if (
			typeof HTMLCanvasElement === "undefined" ||
			typeof HTMLCanvasElement.prototype.transferControlToOffscreen !== "function"
		) {
			return {
				supported: false,
				reason: "Canvas ownership transfer is unavailable.",
			};
		}
		return { supported: true };
	}

	/**
	 * Starts a dedicated render worker and transfers the supplied canvas immediately.
	 * @param canvas Unused HTML canvas that has never acquired a rendering context.
	 * @param operationEpoch Current datastore operation epoch.
	 * @param options Lifecycle callbacks and initialization options.
	 * @returns Client that owns the transferred-canvas session.
	 */
	public static start(
		canvas: HTMLCanvasElement,
		operationEpoch: number,
		options: RenderWorkerClientOptions = {},
	): RenderWorkerClient {
		const capabilities = RenderWorkerClient.probeCapabilities();
		if (!capabilities.supported) {
			throw new Error(capabilities.reason ?? "Worker rendering is unavailable.");
		}
		return new RenderWorkerClient(canvas, operationEpoch, options);
	}

	/**
	 * Reports whether the worker has completed initialization and font handling.
	 * @returns True after READY and before disposal or fatal failure.
	 */
	public get isReady(): boolean {
		return this.readyState && !this.disposed && !this.fatal;
	}

	/**
	 * Returns the newest viewport revision confirmed by a rendered worker frame.
	 * @returns Last rendered viewport revision.
	 */
	public get lastViewportRevision(): number {
		return this.lastRenderedViewportRevision;
	}

	/**
	 * Returns the newest viewport revision queued by the main thread.
	 * @returns Latest requested frame-state viewport revision.
	 */
	public get requestedViewportRevision(): number {
		return this.latestFrameViewportRevision;
	}

	/**
	 * Returns the current bounded data-channel occupancy for profiler diagnostics.
	 * @returns Counts of sent and pending data messages.
	 */
	public get dataQueueDepth(): RenderWorkerDataQueueDepth {
		return this.dataQueue.depth;
	}

	/**
	 * Returns an immutable snapshot of ACK-backed data and rendered-frame progress.
	 * @returns Current worker readiness, queue occupancy, and synchronized revisions.
	 */
	public getSynchronizationState(): RenderWorkerClientSynchronizationState {
		const acknowledgedSourceStates = new Map(this.acknowledgedSourceStates);
		return {
			ready: this.isReady,
			operationEpoch: this.operationEpoch,
			inFlightDataMessages: this.dataQueue.depth.inFlight,
			pendingDataMessages: this.dataQueue.depth.pending,
			acknowledgedGlobalRevision: this.acknowledgedGlobalRevision,
			acknowledgedDataSequence: this.lastAcknowledgedDataSequence,
			acknowledgedDataAtMs: this.lastAcknowledgedDataAtMs,
			acknowledgedSourceRevisions: new Map(
				Array.from(acknowledgedSourceStates, ([sourceId, state]) => [
					sourceId,
					state.revision,
				]),
			),
			acknowledgedSourceStates,
			acknowledgedSourceAtMs: new Map(this.acknowledgedSourceAtMs),
			renderedGlobalRevision: this.lastRenderedGlobalRevision,
			renderedDataSequence: this.lastRenderedDataSequence,
			renderedDataAtMs: this.lastRenderedDataAtMs,
			requestedViewportRevision: this.latestFrameViewportRevision,
			renderedViewportRevision: this.lastRenderedViewportRevision,
		};
	}

	/**
	 * Sends or coalesces a viewport state from the Canvas display-tick coordinator.
	 * @param state Latest dimensions and timeline viewport state.
	 * @param dirty Dirty reasons associated with the viewport replacement.
	 * @param pointer Optional pointer captured in the same atomic input batch.
	 * @returns Nothing.
	 */
	public sendViewportState(
		state: RenderViewportState,
		dirty: DirtyCanvas = DirtyCanvas.Viewport,
		pointer?: RenderPointerState,
	): void {
		if (!this.canSend()) return;
		this.latestFrameViewportRevision = Math.max(
			this.latestFrameViewportRevision,
			state.viewportRevision,
		);
		if (this.postedViewportRevisionAwaitingFrame === null) {
			this.postViewportControl({ state, dirty, pointer });
			return;
		}
		this.pendingViewportControl = {
			state,
			dirty: (this.pendingViewportControl?.dirty ?? DirtyCanvas.None) | dirty,
			pointer: pointer ?? this.pendingViewportControl?.pointer,
		};
	}

	/**
	 * Sends changed scene configuration without cloning it on every viewport tick.
	 * @param state Latest source order, visibility, target, palette, and revisions.
	 * @param dirty Dirty reasons associated with the scene replacement.
	 * @returns Nothing.
	 */
	public sendSceneState(
		state: RenderSceneState,
		dirty: DirtyCanvas,
	): void {
		if (!this.canSend()) return;
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "SCENE_STATE",
			payload: { state, dirty },
		});
	}

	/**
	 * Sends pointer motion or folds it into a matching unsent viewport control.
	 * @param pointer Latest pointer coordinates and inside state.
	 * @param viewportRevision Viewport revision in which coordinates were captured.
	 * @returns Nothing.
	 */
	public queuePointer(
		pointer: RenderPointerState,
		viewportRevision: number,
	): void {
		this.sendPointerNow(pointer, viewportRevision);
	}

	/**
	 * Sends the latest pointer or attaches it to a matching pending viewport atomically.
	 * @param pointer Latest pointer coordinates and inside state.
	 * @param viewportRevision Viewport revision in which coordinates were captured.
	 * @returns Nothing.
	 */
	public sendPointerNow(
		pointer: RenderPointerState,
		viewportRevision: number,
	): void {
		if (!this.canSend()) return;
		if (
			this.pendingViewportControl?.state.viewportRevision === viewportRevision
		) {
			this.pendingViewportControl = {
				...this.pendingViewportControl,
				pointer,
			};
			return;
		}
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "POINTER",
			payload: { pointer, viewportRevision },
		});
	}

	/**
	 * Enqueues one data patch into the single bounded unsent envelope.
	 * Independent logical keys are safely merged because patch sections carry their own
	 * revisions and source event batches retain source-local ordering.
	 * Newly allocated typed arrays are transferred and detached only when the message is sent.
	 * @param _key Stable producer identity retained for API compatibility while revisions govern coalescing.
	 * @param patch Revisioned render data patch.
	 * @returns Nothing.
	 */
	public queueDataPatch(_key: string, patch: RenderDataPatch): void {
		if (!this.canSend()) return;
		this.dataQueue.enqueue(patch);
		this.notifyDataQueueDepth();
		this.flushDataQueue();
	}

	/**
	 * Requests worker-side overlay hit testing at a specific rendered viewport revision.
	 * @param x Canvas-relative horizontal coordinate.
	 * @param y Canvas-relative vertical coordinate.
	 * @param viewportRevision Exact rendered viewport revision required by the result.
	 * @returns Compact overlay identity, or null when no overlay was hit.
	 */
	public hitTest(
		x: number,
		y: number,
		viewportRevision: number,
	): Promise<RenderHitTestResult | null> {
		if (!this.canSend()) return Promise.resolve(null);
		const requestId = this.nextRequestId();
		const promise = this.createInteractionPromise(
			this.pendingHitTests,
			requestId,
			viewportRevision,
			null,
		);
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "HIT_TEST",
			payload: { requestId, x, y, viewportRevision },
		});
		return promise;
	}

	/**
	 * Requests a bitmap crop from the worker-owned primary canvas for the DOM magnifier.
	 * Superseded requests resolve to null and stale returned bitmaps are closed.
	 * @param request Crop geometry and required viewport revision.
	 * @returns Transferable bitmap, or null when superseded or unavailable.
	 */
	public requestMagnifier(
		request: RenderMagnifierCropRequest,
	): Promise<ImageBitmap | null> {
		if (!this.canSend()) return Promise.resolve(null);
		this.resolvePendingMagnifiers(null);
		const requestId = this.nextRequestId();
		const promise = this.createInteractionPromise(
			this.pendingMagnifiers,
			requestId,
			request.viewportRevision,
			null,
		);
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "MAGNIFIER_REQUEST",
			payload: { requestId, ...request },
		});
		return promise;
	}

	/**
	 * Transfers a decoded icon bitmap requested by the worker.
	 * @param assetKey Stable icon identifier from ASSET_REQUEST.
	 * @param color Concrete color used to rasterize the icon.
	 * @param assetRevision Current main-thread asset revision.
	 * @param bitmap Newly allocated bitmap whose ownership moves to the worker.
	 * @returns Nothing.
	 */
	public respondWithAsset(
		assetKey: string,
		color: string,
		assetRevision: number,
		bitmap: ImageBitmap,
	): void {
		if (!this.canSend()) {
			bitmap.close();
			return;
		}
		this.postControl(
			{
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: this.operationEpoch,
				sequence: this.nextSequence(),
				type: "ASSET_RESPONSE",
				payload: { assetKey, color, assetRevision, bitmap },
			},
			[bitmap],
		);
	}

	/**
	 * Sends the latest document visibility state on the high-priority control channel.
	 * @param visible Whether the canvas-owning document is visible.
	 * @returns Nothing.
	 */
	public setVisibility(visible: boolean): void {
		if (!this.canSend()) return;
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "VISIBILITY",
			payload: { visible },
		});
	}

	/**
	 * Advances the operation epoch, clears pending state, and invalidates worker repositories.
	 * @param nextOperationEpoch New authoritative datastore operation epoch.
	 * @param reason Technical reset reason used in diagnostics.
	 * @returns Nothing.
	 */
	public resetOperation(
		nextOperationEpoch: number,
		reason = "operation-changed",
	): void {
		if (
			!this.canSend() ||
			!isOperationEpochAdvance(this.operationEpoch, nextOperationEpoch)
		) {
			return;
		}
		this.operationEpoch = nextOperationEpoch;
		this.dataQueue.clear();
		this.notifyDataQueueDepth();
		this.acknowledgedSourceStates.clear();
		this.acknowledgedSourceAtMs.clear();
		this.acknowledgedGlobalRevision = 0;
		this.lastAcknowledgedDataSequence = 0;
		this.lastAcknowledgedDataAtMs = 0;
		this.lastRenderedGlobalRevision = 0;
		this.lastRenderedDataSequence = 0;
		this.lastRenderedDataAtMs = 0;
		this.lastRenderedViewportRevision = 0;
		this.latestFrameViewportRevision = 0;
		this.clearViewportControls();
		this.resolvePendingHitTests(null);
		this.resolvePendingMagnifiers(null);
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "RESET_OPERATION",
			payload: { reason },
		});
	}

	/**
	 * Disposes ports, rejects readiness, resolves interactions, and terminates the worker.
	 * @returns Nothing.
	 */
	public dispose(): void {
		if (this.disposed) return;
		if (!this.fatal) {
			try {
				this.postControl({
					protocolVersion: RENDER_PROTOCOL_VERSION,
					operationEpoch: this.operationEpoch,
					sequence: this.nextSequence(),
					type: "DISPOSE",
					payload: {},
				});
			} catch {
				// Termination below is authoritative when the worker channel is already gone.
			}
		}
		this.disposed = true;
		this.readyState = false;
		this.resolvePendingHitTests(null);
		this.resolvePendingMagnifiers(null);
		this.readyReject(new Error("Render worker disposed before becoming ready."));
		this.releaseWorkerResources();
	}

	/**
	 * Allocates the next monotonically increasing protocol sequence.
	 * @returns Sequence number unique within this client session.
	 */
	private nextSequence(): number {
		this.sequence += 1;
		return this.sequence;
	}

	/**
	 * Posts one viewport control and marks its revision as awaiting a rendered frame.
	 * Protocol sequence allocation happens here so replaced pending states create no gaps.
	 * @param control Viewport state, merged dirty reasons, and optional atomic pointer.
	 * @returns Nothing.
	 */
	private postViewportControl(control: PendingViewportControl): void {
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "VIEWPORT_STATE",
			payload: control,
		});
		this.postedViewportRevisionAwaitingFrame = control.state.viewportRevision;
	}

	/**
	 * Posts the latest retained viewport after the prior sent revision becomes visible.
	 * @returns Nothing.
	 */
	private flushPendingViewportControl(): void {
		if (
			!this.canSend() ||
			this.postedViewportRevisionAwaitingFrame !== null ||
			this.pendingViewportControl === null
		) {
			return;
		}
		const control = this.pendingViewportControl;
		this.pendingViewportControl = null;
		this.postViewportControl(control);
	}

	/**
	 * Releases a sent viewport only after a worker frame reaches its revision.
	 * @param renderedViewportRevision Viewport revision reported by FRAME_RENDERED.
	 * @returns Nothing.
	 */
	private acknowledgeRenderedViewport(renderedViewportRevision: number): void {
		if (
			this.postedViewportRevisionAwaitingFrame === null ||
			renderedViewportRevision < this.postedViewportRevisionAwaitingFrame
		) {
			return;
		}
		this.postedViewportRevisionAwaitingFrame = null;
		this.flushPendingViewportControl();
	}

	/**
	 * Discards sent and pending viewport-control backpressure state at a lifecycle boundary.
	 * @returns Nothing.
	 */
	private clearViewportControls(): void {
		this.postedViewportRevisionAwaitingFrame = null;
		this.pendingViewportControl = null;
	}

	/**
	 * Allocates the next request identity used by hit-test and magnifier promises.
	 * @returns Positive request identifier unique within this client session.
	 */
	private nextRequestId(): number {
		this.requestId += 1;
		return this.requestId;
	}

	/**
	 * Reports whether messages can still be accepted for this worker session.
	 * @returns True while the client is neither disposed nor fatally failed.
	 */
	private canSend(): boolean {
		return !this.disposed && !this.fatal;
	}

	/**
	 * Posts a typed message through the high-priority control port.
	 * @param message Control protocol envelope.
	 * @param transferables Optional values whose ownership moves to the worker.
	 * @returns Nothing.
	 */
	private postControl(
		message: RenderControlMessage,
		transferables: Transferable[] = [],
	): void {
		this.controlPort.postMessage(message, transferables);
	}

	/**
	 * Sends the single pending envelope while respecting the global in-flight bound.
	 * @returns Nothing.
	 */
	private flushDataQueue(): void {
		if (!this.canSend()) return;
		const depth = this.dataQueue.depth;
		if (depth.inFlight > 0 || depth.pending === 0) return;
		const sequence = this.nextSequence();
		const patch = this.dataQueue.dispatch(sequence);
		if (!patch) return;
		const message: RenderDataMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence,
			type: "DATA_PATCH",
			payload: patch,
		};
		this.notifyDataQueueDepth();
		try {
			this.dataPort.postMessage(
				message,
				getDataPatchTransferables(patch),
			);
		} catch (error) {
			this.dataQueue.acknowledge(sequence);
			this.notifyDataQueueDepth();
			this.failFatal(this.toError(error, "Failed to post render data"));
		}
	}

	/**
	 * Receives responses emitted through the worker global scope or either message port.
	 * @param event Browser message event containing an unknown protocol value.
	 * @returns Nothing.
	 */
	private handleWorkerMessage = (event: MessageEvent<unknown>): void => {
		if (!isRenderWorkerResponse(event.data)) {
			const message =
				isRecord(event.data) &&
				"protocolVersion" in event.data &&
				event.data.protocolVersion !== RENDER_PROTOCOL_VERSION
					? "Render worker protocol version mismatch."
					: "Render worker returned an unknown or malformed protocol response.";
			this.failFatal(new Error(message));
			return;
		}

		try {
			this.routeWorkerResponse(event.data);
		} catch (error) {
			this.failFatal(
				this.toError(error, "Failed to route render worker response"),
			);
		}
	};

	/**
	 * Routes one strictly validated response to its typed lifecycle handler.
	 * @param message Fully validated worker response.
	 * @returns Nothing.
	 */
	private routeWorkerResponse(message: RenderWorkerResponse): void {
		if (
			message.type === "ERROR" &&
			message.payload.fatal &&
			message.payload.phase === "initialize"
		) {
			this.options.onWorkerError?.(message.payload);
			this.failFatal(new Error(message.payload.message));
			return;
		}
		if (message.operationEpoch !== this.operationEpoch) {
			if (message.type === "MAGNIFIER_FRAME") message.payload.bitmap.close();
			return;
		}

		switch (message.type) {
			case "CAPABILITIES":
				this.handleCapabilities(message.payload);
				break;
			case "READY":
				this.clearInitializationTimeout();
				this.readyState = true;
				this.readyResolve(message.payload);
				this.options.onReady?.(message.payload);
				this.flushDataQueue();
				break;
			case "ACK":
				this.handleAcknowledgement(message.payload);
				break;
			case "FRAME_RENDERED":
				this.lastRenderedViewportRevision = Math.max(
					this.lastRenderedViewportRevision,
					message.payload.viewportRevision,
				);
				this.lastRenderedGlobalRevision = Math.max(
					this.lastRenderedGlobalRevision,
					message.payload.dataRevision,
				);
				this.lastRenderedDataSequence = Math.max(
					this.lastRenderedDataSequence,
					message.payload.renderedDataSequence,
				);
				this.lastRenderedDataAtMs = performance.now();
				this.acknowledgeRenderedViewport(message.payload.viewportRevision);
				this.options.onFrameRendered?.(message.payload);
				break;
			case "HIT_TEST_RESULT": {
				const pendingHitTest = this.pendingHitTests.get(
					message.payload.requestId,
				);
				this.resolveInteraction(
					this.pendingHitTests,
					message.payload.requestId,
					pendingHitTest?.requestedViewportRevision ===
						message.payload.viewportRevision
						? message.payload.hit
						: null,
				);
				break;
			}
			case "MAGNIFIER_FRAME": {
				const pendingMagnifier = this.pendingMagnifiers.get(
					message.payload.requestId,
				);
				if (
					!pendingMagnifier ||
					pendingMagnifier.requestedViewportRevision !==
						message.payload.viewportRevision
				) {
					message.payload.bitmap.close();
					if (pendingMagnifier) {
						this.resolveInteraction(
							this.pendingMagnifiers,
							message.payload.requestId,
							null,
						);
					}
					break;
				}
				this.resolveInteraction(
					this.pendingMagnifiers,
					message.payload.requestId,
					message.payload.bitmap,
				);
				break;
			}
			case "ASSET_REQUEST":
				this.handleAssetRequest(message.payload);
				break;
			case "RESYNC_REQUIRED":
				this.invalidateAcknowledgedSynchronization(message.payload);
				this.options.onResyncRequired?.(message.payload);
				break;
			case "HOVER_CHANGED":
				this.options.onHoverChanged?.(
					message.payload.hit,
					message.payload.viewportRevision,
				);
				break;
			case "ERROR":
				this.options.onWorkerError?.(message.payload);
				if (message.payload.fatal) {
					this.failFatal(new Error(message.payload.message));
				}
				break;
		}
	}

	/**
	 * Validates worker-only capabilities that cannot be reliably probed on the main thread.
	 * @param capabilities Capability flags reported after worker initialization.
	 * @returns Nothing.
	 */
	private handleCapabilities(capabilities: RenderCapabilities): void {
		this.options.onCapabilities?.(capabilities);
		if (
			!capabilities.offscreenCanvas ||
			!capabilities.workerAnimationFrame ||
			!capabilities.imageBitmap
		) {
			this.failFatal(
				new Error(
					"The render worker lacks required canvas animation or bitmap support.",
				),
			);
		}
	}

	/**
	 * Releases one in-flight data slot and sends the next eligible pending patch.
	 * @param acknowledgement DATA_PATCH sequence and committed repository revision.
	 * @returns Nothing.
	 */
	private handleAcknowledgement(acknowledgement: RenderAcknowledgement): void {
		if (!this.dataQueue.acknowledge(acknowledgement.acknowledgedSequence)) return;
		this.notifyDataQueueDepth();
		this.acknowledgedGlobalRevision = Math.max(
			this.acknowledgedGlobalRevision,
			acknowledgement.globalRevision,
		);
		this.lastAcknowledgedDataSequence = Math.max(
			this.lastAcknowledgedDataSequence,
			acknowledgement.acknowledgedSequence,
		);
		const acknowledgedAtMs = performance.now();
		this.lastAcknowledgedDataAtMs = acknowledgedAtMs;
		applyAcknowledgedSourceRevisions(
			this.acknowledgedSourceStates,
			acknowledgement,
		);
		for (const source of acknowledgement.sourceRevisions) {
			this.acknowledgedSourceAtMs.set(source.sourceId, acknowledgedAtMs);
		}
		for (const sourceId of acknowledgement.rejectedSourceIds) {
			this.acknowledgedSourceAtMs.delete(sourceId);
		}
		this.flushDataQueue();
	}

	/**
	 * Invalidates optimistic ACK telemetry when the worker rejects a revision gap.
	 * @param request Worker scope whose authoritative snapshot must be replayed.
	 * @returns Nothing.
	 */
	private invalidateAcknowledgedSynchronization(
		request: RenderResyncRequest,
	): void {
		if (request.scope === "source" && request.id) {
			this.acknowledgedSourceStates.delete(request.id);
			this.acknowledgedSourceAtMs.delete(request.id);
			return;
		}
		if (request.scope === "sources") {
			this.acknowledgedSourceStates.clear();
			this.acknowledgedSourceAtMs.clear();
		}
	}

	/**
	 * Invokes the optional asset loader and transfers its asynchronous result safely.
	 * @param request Missing icon identity and concrete render color.
	 * @returns Nothing.
	 */
	private handleAssetRequest(request: RenderAssetRequest): void {
		const requestEpoch = this.operationEpoch;
		const loader = this.options.onAssetRequest;
		if (!loader) {
			this.reportAssetUnavailable(request, requestEpoch);
			return;
		}
		void Promise.resolve()
			.then(() => loader(request))
			.then((asset) => {
				if (!asset) {
					this.reportAssetUnavailable(request, requestEpoch);
					return;
				}
				if (
					this.disposed ||
					this.fatal ||
					requestEpoch !== this.operationEpoch
				) {
					asset.bitmap.close();
					return;
				}
				this.respondWithAsset(
					request.assetKey,
					request.color,
					asset.revision,
					asset.bitmap,
				);
			})
			.catch((error: unknown) => {
				this.reportAssetUnavailable(request, requestEpoch);
				this.options.onWorkerError?.({
					phase: "asset",
					message: this.toError(error, "Failed to prepare a render asset").message,
					fatal: false,
				});
			});
	}

	/**
	 * Notifies the worker that a failed icon request may be retried on a later frame.
	 * @param request Missing icon identity originally requested by the worker.
	 * @param requestEpoch Operation epoch captured when loading began.
	 * @returns Nothing.
	 */
	private reportAssetUnavailable(
		request: RenderAssetRequest,
		requestEpoch: number,
	): void {
		if (!this.canSend() || requestEpoch !== this.operationEpoch) return;
		this.postControl({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: this.nextSequence(),
			type: "ASSET_UNAVAILABLE",
			payload: {
				assetKey: request.assetKey,
				color: request.color,
			},
		});
	}

	/**
	 * Creates one interaction promise with deterministic timeout cleanup.
	 * @param requests Target pending-request map.
	 * @param requestId Protocol request identifier.
	 * @param requestedViewportRevision Exact viewport revision required by the response.
	 * @param timeoutResult Value returned when the worker does not answer in time.
	 * @returns Promise resolved by the matching worker response.
	 */
	private createInteractionPromise<TResult>(
		requests: Map<number, PendingInteraction<TResult>>,
		requestId: number,
		requestedViewportRevision: number,
		timeoutResult: TResult,
	): Promise<TResult> {
		return new Promise<TResult>((resolve, reject) => {
			const timeout = setTimeout(() => {
				requests.delete(requestId);
				resolve(timeoutResult);
			}, this.interactionTimeoutMs);
			requests.set(requestId, {
				resolve,
				reject,
				timeout,
				requestedViewportRevision,
			});
		});
	}

	/**
	 * Resolves and removes one pending interaction request.
	 * @param requests Pending-request map containing the request.
	 * @param requestId Protocol request identifier.
	 * @param result Result returned by the worker.
	 * @returns True when a live request consumed the result.
	 */
	private resolveInteraction<TResult>(
		requests: Map<number, PendingInteraction<TResult>>,
		requestId: number,
		result: TResult,
	): boolean {
		const request = requests.get(requestId);
		if (!request) return false;
		clearTimeout(request.timeout);
		requests.delete(requestId);
		request.resolve(result);
		return true;
	}

	/**
	 * Resolves all outstanding hit tests during reset, disposal, or failure.
	 * @param result Result supplied to every pending hit test.
	 * @returns Nothing.
	 */
	private resolvePendingHitTests(result: RenderHitTestResult | null): void {
		for (const [requestId] of this.pendingHitTests) {
			this.resolveInteraction(this.pendingHitTests, requestId, result);
		}
	}

	/**
	 * Resolves all outstanding magnifier requests during supersession or lifecycle changes.
	 * @param result Result supplied to every pending magnifier request.
	 * @returns Nothing.
	 */
	private resolvePendingMagnifiers(result: ImageBitmap | null): void {
		for (const [requestId] of this.pendingMagnifiers) {
			this.resolveInteraction(this.pendingMagnifiers, requestId, result);
		}
	}

	/**
	 * Handles uncaught worker exceptions as fatal transferred-canvas failures.
	 * @param event Worker error event.
	 * @returns Nothing.
	 */
	private handleWorkerRuntimeError = (event: ErrorEvent): void => {
		this.failFatal(new Error(event.message || "Render worker crashed."));
	};

	/**
	 * Handles structured-clone failures as fatal protocol-channel failures.
	 * @param _event Browser message-error event.
	 * @returns Nothing.
	 */
	private handleMessageError = (_event: MessageEvent<unknown>): void => {
		this.failFatal(new Error("Render worker message deserialization failed."));
	};

	/**
	 * Disables worker mode for the page session and signals that Canvas must remount.
	 * @param error Fatal initialization, protocol, or runtime error.
	 * @returns Nothing.
	 */
	private failFatal(error: Error): void {
		if (this.fatal || this.disposed) return;
		this.fatal = true;
		this.readyState = false;
		this.clearInitializationTimeout();
		workerRenderingDisabledForSession = true;
		this.resolvePendingHitTests(null);
		this.resolvePendingMagnifiers(null);
		this.readyReject(error);
		this.releaseWorkerResources();
		this.options.onFatalError?.(error);
	}

	/**
	 * Removes listeners, closes message ports, and terminates the dedicated worker.
	 * @returns Nothing.
	 */
	private releaseWorkerResources(): void {
		this.clearInitializationTimeout();
		this.worker.removeEventListener("message", this.handleWorkerMessage);
		this.worker.removeEventListener("error", this.handleWorkerRuntimeError);
		this.worker.removeEventListener("messageerror", this.handleMessageError);
		this.controlPort.removeEventListener("message", this.handleWorkerMessage);
		this.controlPort.removeEventListener("messageerror", this.handleMessageError);
		this.dataPort.removeEventListener("message", this.handleWorkerMessage);
		this.dataPort.removeEventListener("messageerror", this.handleMessageError);
		this.controlPort.close();
		this.dataPort.close();
		this.worker.terminate();
		this.dataQueue.clear();
		this.notifyDataQueueDepth();
		this.acknowledgedSourceStates.clear();
		this.acknowledgedSourceAtMs.clear();
		this.acknowledgedGlobalRevision = 0;
		this.lastAcknowledgedDataSequence = 0;
		this.lastAcknowledgedDataAtMs = 0;
		this.lastRenderedGlobalRevision = 0;
		this.lastRenderedDataSequence = 0;
		this.lastRenderedDataAtMs = 0;
		this.clearViewportControls();
	}

	/**
	 * Publishes current data-channel occupancy after a pending or in-flight transition.
	 * @returns Nothing.
	 */
	private notifyDataQueueDepth(): void {
		this.options.onDataQueueDepthChanged?.(this.dataQueueDepth);
	}

	/**
	 * Cancels the bounded worker initialization watchdog after readiness or shutdown.
	 * @returns Nothing.
	 */
	private clearInitializationTimeout(): void {
		if (this.initializationTimeout === null) return;
		clearTimeout(this.initializationTimeout);
		this.initializationTimeout = null;
	}

	/**
	 * Converts unknown thrown values into diagnostic Error objects.
	 * @param value Thrown browser or application value.
	 * @param fallbackMessage Message used when the thrown value has no text.
	 * @returns Normalized Error instance.
	 */
	private toError(value: unknown, fallbackMessage: string): Error {
		if (value instanceof Error) return value;
		if (typeof value === "string") return new Error(value);
		return new Error(fallbackMessage);
	}
}
