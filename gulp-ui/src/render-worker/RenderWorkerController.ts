import {
	DirtyCanvas,
	RENDER_PROTOCOL_VERSION,
	type RenderAssetResponseMessage,
	type RenderControlMessage,
	type RenderDataMessage,
	type RenderHitTestMessage,
	type RenderHitTestResult,
	type RenderMagnifierRequestMessage,
	type RenderProtocolEnvelope,
	type RenderReadyMessage,
	type RenderWorkerBootstrapMessage,
	type RenderWorkerFontSource,
	type RenderWorkerResponse,
} from "./RenderWorker.types";
import { WorkerRenderCore } from "./WorkerRenderCore";
import { WorkerRenderRepository } from "./WorkerRenderRepository";

const DATA_REDRAW_QUIET_WINDOW_MS = 8;
export const DATA_REDRAW_MAXIMUM_WAIT_MS = 100;

interface WorkerFontScope {
	fonts?: FontFaceSet;
}

/**
 * Owns protocol validation, lifecycle, render scheduling, and request ordering for one canvas.
 */
export class RenderWorkerController {
	private readonly repository = new WorkerRenderRepository();
	private readonly renderer: WorkerRenderCore;
	private readonly controlPort: MessagePort;
	private readonly dataPort: MessagePort;
	private operationEpoch: number;
	private outgoingSequence = 0;
	private lastControlSequence = -1;
	private lastDataSequence = -1;
	private lastContiguousIncomingSequence: number;
	private readonly outOfOrderIncomingSequences = new Set<number>();
	private readonly activeDataPatchSequences = new Set<number>();
	private readonly dataSequencesAwaitingRender = new Set<number>();
	private readonly acknowledgedSourceStates = new Map<
		string,
		{ revision: number; dataSequence: number }
	>();
	private readonly controlSequencesAwaitingRender = new Set<number>();
	private highestCommittedDataSequence = 0;
	private dataProcessingChain: Promise<void> = Promise.resolve();
	private dirtyMask = DirtyCanvas.None;
	private pendingDataDirtyMask = DirtyCanvas.None;
	private dataRedrawTimeout: ReturnType<typeof setTimeout> | null = null;
	private dataRedrawMaximumTimeout: ReturnType<typeof setTimeout> | null = null;
	private frameRequest: number | null = null;
	private ready = false;
	private visible = true;
	private disposed = false;
	private lastHoverKey: string | null = null;
	private pendingHitTests: RenderHitTestMessage[] = [];
	private pendingMagnifier: RenderMagnifierRequestMessage | null = null;
	private readonly dataReceivedAtByRevision = new Map<number, number>();
	private readonly viewportReceivedAtByRevision = new Map<number, number>();
	private readyPayload: RenderReadyMessage["payload"] | null = null;

	/**
	 * Attaches transferred ports and creates the sole rendering context for a canvas.
	 * @param bootstrap Validated INIT envelope containing canvas, ports, and fonts.
	 */
	public constructor(bootstrap: RenderWorkerBootstrapMessage) {
		this.operationEpoch = bootstrap.operationEpoch;
		this.lastContiguousIncomingSequence = bootstrap.sequence;
		this.controlPort = bootstrap.payload.controlPort;
		this.dataPort = bootstrap.payload.dataPort;
		const context = bootstrap.payload.canvas.getContext("2d");
		if (!context) throw new Error("Unable to acquire OffscreenCanvas 2D context");
		this.renderer = new WorkerRenderCore(context, this.repository, {
			requestAsset: (assetKey, color) => {
				this.postControl("ASSET_REQUEST", { assetKey, color });
			},
		});
		this.attachPorts();
		this.postCapabilities();
		void this.initializeFonts(bootstrap.payload.fonts);
	}

	/**
	 * Starts both ports and installs typed message and messageerror handlers.
	 * @returns Nothing.
	 */
	private attachPorts(): void {
		this.controlPort.onmessage = (event: MessageEvent<unknown>) => {
			this.handleControlMessage(event.data);
		};
		this.controlPort.onmessageerror = () => {
			this.publishError(
				"protocol",
				"Unable to deserialize a render control message",
				true,
			);
		};
		this.dataPort.onmessage = (event: MessageEvent<unknown>) => {
			this.enqueueDataMessage(event.data);
		};
		this.dataPort.onmessageerror = () => {
			this.publishError(
				"protocol",
				"Unable to deserialize a render data message",
				true,
			);
		};
		this.controlPort.start();
		this.dataPort.start();
	}

	/**
	 * Validates a data message at receipt time and appends it to the serial staging queue.
	 * @param value Untrusted structured-clone payload from the worker data port.
	 * @returns Nothing.
	 */
	private enqueueDataMessage(value: unknown): void {
		try {
			if (!this.isEnvelope(value)) {
				throw new Error("Malformed render data envelope");
			}
			const message = value as RenderDataMessage;
			if (message.type !== "DATA_PATCH") {
				throw new Error(`Unexpected data message type: ${message.type}`);
			}
			if (!this.acceptDataEnvelope(message)) return;
			const receivedAt = performance.now();
			this.suspendPendingDataRedraw();
			this.activeDataPatchSequences.add(message.sequence);
			this.dataProcessingChain = this.dataProcessingChain
				.catch(() => undefined)
				.then(() => this.handleDataMessage(message, receivedAt));
		} catch (error) {
			this.publishCaughtError("data", error, false);
		}
	}

	/**
	 * Reports browser primitives required by the worker backend.
	 * @returns Nothing.
	 */
	private postCapabilities(): void {
		this.postControl("CAPABILITIES", {
			offscreenCanvas: typeof OffscreenCanvas !== "undefined",
			workerAnimationFrame: typeof requestAnimationFrame === "function",
			workerFontFace: typeof FontFace !== "undefined",
			imageBitmap: typeof ImageBitmap !== "undefined",
		});
	}

	/**
	 * Loads bundled fonts into the worker font set before declaring parity readiness.
	 * @param fontSources FontFace descriptors resolved by the main bundle.
	 * @returns Promise settled after all fonts load or an explicit fallback is published.
	 */
	private async initializeFonts(
		fontSources: RenderWorkerFontSource[],
	): Promise<void> {
		let fontsLoaded = true;
		let fontFallbackReason: string | undefined;
		try {
			if (fontSources.length > 0 && typeof FontFace === "undefined") {
				throw new Error("FontFace is unavailable in this dedicated worker");
			}
			const workerFonts = (globalThis as unknown as WorkerFontScope).fonts;
			if (fontSources.length > 0 && !workerFonts) {
				throw new Error("The worker FontFaceSet is unavailable");
			}
			for (const fontSource of fontSources) {
				const font = new FontFace(
					fontSource.family,
					`url(${JSON.stringify(fontSource.url)})`,
					{
						style: fontSource.style ?? "normal",
						weight: fontSource.weight ?? "normal",
					},
				);
				const loaded = await font.load();
				workerFonts?.add(loaded);
			}
		} catch (error) {
			fontsLoaded = false;
			fontFallbackReason = this.getErrorMessage(error);
		}

		if (this.disposed) return;
		this.readyPayload = { fontsLoaded, fontFallbackReason };
		this.ready = true;
		this.postControl("READY", this.readyPayload);
		if (this.dirtyMask !== DirtyCanvas.None) this.scheduleFrame();
	}

	/**
	 * Validates and routes one high-priority control-port message.
	 * @param value Untrusted structured-clone payload.
	 * @returns Nothing.
	 */
	private handleControlMessage(value: unknown): void {
		try {
			if (!this.isEnvelope(value)) {
				throw new Error("Malformed render control envelope");
			}
			const message = value as RenderControlMessage;
			if (!this.acceptControlEnvelope(message)) {
				if (message.type === "ASSET_RESPONSE") {
					message.payload.bitmap.close();
				}
				this.flushOrderedRequests();
				return;
			}
			switch (message.type) {
				case "VIEWPORT_STATE": {
					const isInteractionViewport =
						(message.payload.dirty & DirtyCanvas.Viewport) !== 0;
					if (isInteractionViewport || message.payload.pointer !== undefined) {
						this.repository.markInteraction();
					}
					this.viewportReceivedAtByRevision.set(
						message.payload.state.viewportRevision,
						performance.timeOrigin + performance.now(),
					);
					this.repository.setViewportState(message.payload.state);
					if (message.payload.pointer !== undefined) {
						this.repository.setPointerState(message.payload.pointer);
					}
					this.markDirty(
						message.payload.dirty || DirtyCanvas.Viewport,
						message.sequence,
						isInteractionViewport,
					);
					break;
				}
				case "SCENE_STATE": {
					const previousAssetRevision =
						this.repository.getFrameState()?.revisions.assets;
					if (
						previousAssetRevision !== undefined &&
						previousAssetRevision !==
							message.payload.state.revisions.assets
					) {
						this.renderer.invalidateAssets();
					}
					this.repository.setSceneState(message.payload.state);
					this.markDirty(
						message.payload.dirty || DirtyCanvas.Sources,
						message.sequence,
					);
					break;
				}
				case "POINTER":
					this.handlePointer(message);
					break;
				case "HIT_TEST":
					this.handleHitTest(message);
					break;
				case "MAGNIFIER_REQUEST":
					this.handleMagnifierRequest(message);
					break;
				case "ASSET_RESPONSE":
					this.handleAssetResponse(message);
					break;
				case "ASSET_UNAVAILABLE":
					this.renderer.notifyAssetUnavailable(
						message.payload.assetKey,
						message.payload.color,
					);
					break;
				case "VISIBILITY":
					this.visible = message.payload.visible;
					if (this.visible) {
						this.markDirty(DirtyCanvas.Force, message.sequence);
					}
					break;
				case "RESET_OPERATION":
					this.resetOperation(message.operationEpoch);
					break;
				case "DISPOSE":
					this.dispose();
					break;
			}
			this.flushOrderedRequests();
		} catch (error) {
			this.publishCaughtError("protocol", error, true);
		}
	}

	/**
	 * Applies one data-port patch without blocking control traffic during staging yields.
	 * @param message Validated data message awaiting serial repository staging.
	 * @param receivedAt Worker timestamp captured when the data message arrived.
	 * @returns Promise settled after staging, commit, ACK, and invalidation.
	 */
	private async handleDataMessage(
		message: RenderDataMessage,
		receivedAt: number,
	): Promise<void> {
		try {
			const result = await this.repository.applyDataPatch(
				message.payload,
				message.operationEpoch,
			);
			if (this.disposed || message.operationEpoch !== this.operationEpoch) return;
			this.renderer.invalidateSources(
				Array.from(
					new Set([
						...(message.payload.sources?.removedSourceIds ?? []),
						...(message.payload.renderWindow?.evictedSourceIds ?? []),
					]),
				),
			);
			if (result.committedEvents) {
				const eventSourceIds = [
					...(message.payload.events ?? []).map((batch) => batch.sourceId),
					...(message.payload.eventSnapshots ?? [])
						.filter((record) => record.kind === "commit")
						.map((record) => record.sourceId),
					...(message.payload.renderWindow?.sources ?? []).map(
						(source) => source.sourceId,
					),
					...(message.payload.renderWindow?.evictedSourceIds ?? []),
				];
				this.renderer.invalidateEventSources(
					Array.from(new Set(eventSourceIds)),
				);
			}
			for (const request of result.resync) {
				this.postControl("RESYNC_REQUIRED", request);
			}
			const rejectedSourceIds = new Set<string>();
			for (const request of result.resync) {
				if (request.scope === "source" && request.id) {
					rejectedSourceIds.add(request.id);
				} else if (request.scope === "sources") {
					this.getDataPatchSourceIds(message).forEach((sourceId) =>
						rejectedSourceIds.add(sourceId),
					);
				}
			}
			let dirty = DirtyCanvas.None;
			if (result.committedSources) dirty |= DirtyCanvas.Sources;
			if (result.committedEvents) dirty |= DirtyCanvas.Events;
			if (result.committedOverlays) dirty |= DirtyCanvas.Overlays;
			if (dirty !== DirtyCanvas.None) {
				this.dataSequencesAwaitingRender.add(message.sequence);
				this.highestCommittedDataSequence = Math.max(
					this.highestCommittedDataSequence,
					message.sequence,
				);
				this.dataReceivedAtByRevision.set(
					message.payload.globalRevision,
					receivedAt,
				);
				this.markDataDirty(dirty);
			}
			this.postData(
				"ACK",
				this.createDataAcknowledgement(message, rejectedSourceIds),
			);
		} catch (error) {
			if (
				message.operationEpoch === this.operationEpoch &&
				!this.disposed
			) {
				this.postData(
					"ACK",
					this.createDataAcknowledgement(
						message,
						new Set(this.getDataPatchSourceIds(message)),
					),
				);
				this.requestPatchResynchronization(message.payload);
			}
			this.publishCaughtError("data", error, false);
		} finally {
			this.activeDataPatchSequences.delete(message.sequence);
			if (this.activeDataPatchSequences.size === 0) {
				this.schedulePendingDataRedraw();
			}
			this.flushOrderedRequests();
		}
	}

	/**
	 * Requests authoritative replacements for every scope touched by a failed patch.
	 * This repairs any independently committed source or overlay data before a later
	 * staging failure interrupted the remainder of the message.
	 * @param patch Accepted patch whose application did not complete.
	 * @returns Nothing.
	 */
	private requestPatchResynchronization(
		patch: RenderDataMessage["payload"],
	): void {
		if (patch.sources) {
			this.postControl("RESYNC_REQUIRED", {
				scope: "sources",
				expectedRevision: 0,
				receivedRevision: patch.sources.revision,
			});
		}
		const sourceIds = new Set(
			[
				...(patch.events ?? []).map((batch) => batch.sourceId),
				...(patch.eventSnapshots ?? []).map((record) => record.sourceId),
				...(patch.renderWindow?.sources ?? []).map(
					(source) => source.sourceId,
				),
				...(patch.renderWindow?.evictedSourceIds ?? []),
			],
		);
		for (const sourceId of sourceIds) {
			const receivedRevision = Math.max(
				0,
				...(patch.events ?? [])
					.filter((batch) => batch.sourceId === sourceId)
					.map((batch) => batch.revision),
				...(patch.eventSnapshots ?? [])
					.filter((record) => record.sourceId === sourceId)
					.map((record) =>
						record.kind === "chunk" ? 0 : record.revision,
					),
				...(patch.renderWindow?.sources ?? [])
					.filter((source) => source.sourceId === sourceId)
					.map((source) => source.sourceRevision),
			);
			this.postControl("RESYNC_REQUIRED", {
				scope: "source",
				id: sourceId,
				expectedRevision: 0,
				receivedRevision,
			});
		}
		const overlayScopes = [
			["notes", patch.overlays?.notes?.revision],
			["links", patch.overlays?.links?.revision],
			["highlights", patch.overlays?.highlights?.revision],
			["flags", patch.overlays?.flags?.revision],
		] as const;
		for (const [scope, revision] of overlayScopes) {
			if (revision === undefined) continue;
			this.postControl("RESYNC_REQUIRED", {
				scope,
				expectedRevision: 0,
				receivedRevision: revision,
			});
		}
	}

	/**
	 * Validates protocol version, epoch, and per-port sequence for a control message.
	 * @param message Parsed control envelope.
	 * @returns True when the message may mutate worker state.
	 */
	private acceptControlEnvelope(message: RenderControlMessage): boolean {
		if (message.protocolVersion !== RENDER_PROTOCOL_VERSION) {
			throw new Error(`Unsupported render protocol ${message.protocolVersion}`);
		}
		this.recordIncomingSequence(message.sequence);
		if (message.sequence <= this.lastControlSequence) return false;
		this.lastControlSequence = message.sequence;
		if (message.type === "RESET_OPERATION") {
			return message.operationEpoch >= this.operationEpoch;
		}
		return message.operationEpoch === this.operationEpoch;
	}

	/**
	 * Validates protocol version, epoch, and per-port sequence for a data message.
	 * @param message Parsed data envelope.
	 * @returns True when the message may enter repository staging.
	 */
	private acceptDataEnvelope(message: RenderDataMessage): boolean {
		if (message.protocolVersion !== RENDER_PROTOCOL_VERSION) {
			throw new Error(`Unsupported render protocol ${message.protocolVersion}`);
		}
		this.recordIncomingSequence(message.sequence);
		if (message.sequence <= this.lastDataSequence) return false;
		this.lastDataSequence = message.sequence;
		if (message.operationEpoch < this.operationEpoch) return false;
		if (message.operationEpoch > this.operationEpoch) {
			this.resetOperation(message.operationEpoch);
		}
		return true;
	}

	/**
	 * Advances the cross-port input watermark once every earlier global sequence arrived.
	 * @param sequence Globally monotonic main-thread message sequence.
	 * @returns Nothing.
	 */
	private recordIncomingSequence(sequence: number): void {
		if (sequence <= this.lastContiguousIncomingSequence) return;
		this.outOfOrderIncomingSequences.add(sequence);
		while (
			this.outOfOrderIncomingSequences.delete(
				this.lastContiguousIncomingSequence + 1,
			)
		) {
			this.lastContiguousIncomingSequence++;
		}
	}

	/**
	 * Stores pointer state and invalidates only when hovered overlay identity changes.
	 * @param message Pointer control message.
	 * @returns Nothing.
	 */
	private handlePointer(
		message: Extract<RenderControlMessage, { type: "POINTER" }>,
	): void {
		this.repository.markInteraction();
		const state = this.repository.getFrameState();
		if (!state || message.payload.viewportRevision < state.viewportRevision) return;
		this.repository.setPointerState(message.payload.pointer);
		if (
			message.payload.viewportRevision >
			this.renderer.getRenderedViewportRevision()
		) {
			return;
		}
		const nextHit = message.payload.pointer.inside
			? this.repository.hitTest(
					message.payload.pointer.x,
					message.payload.pointer.y,
				)
			: null;
		if (this.getHitKey(nextHit) !== this.lastHoverKey) {
			this.markDirty(DirtyCanvas.Hover, message.sequence);
		}
	}

	/**
	 * Responds immediately to ordered hit tests or queues them behind a newer viewport.
	 * @param message Overlay hit-test request.
	 * @returns Nothing.
	 */
	private handleHitTest(message: RenderHitTestMessage): void {
		if (!this.isOrderedRequestReady(message)) {
			this.pendingHitTests.push(message);
			return;
		}
		this.publishHitTest(message);
	}

	/**
	 * Responds to a hit-test using the latest rendered hit-region index.
	 * @param message Hit-test request to resolve.
	 * @returns Nothing.
	 */
	private publishHitTest(message: RenderHitTestMessage): void {
		this.postControl("HIT_TEST_RESULT", {
			requestId: message.payload.requestId,
			viewportRevision: this.renderer.getRenderedViewportRevision(),
			hit: this.repository.hitTest(message.payload.x, message.payload.y),
		});
	}

	/**
	 * Keeps only the latest magnifier request until its viewport has rendered.
	 * @param message Magnifier crop request.
	 * @returns Nothing.
	 */
	private handleMagnifierRequest(
		message: RenderMagnifierRequestMessage,
	): void {
		if (!this.isOrderedRequestReady(message)) {
			this.pendingMagnifier = message;
			return;
		}
		this.publishMagnifier(message);
	}

	/**
	 * Creates and transfers one scratch-canvas magnifier bitmap.
	 * @param message Magnifier crop request to resolve.
	 * @returns Nothing.
	 */
	private publishMagnifier(message: RenderMagnifierRequestMessage): void {
		try {
			const bitmap = this.renderer.createMagnifierBitmap(message.payload);
			this.postControl(
				"MAGNIFIER_FRAME",
				{
					requestId: message.payload.requestId,
					viewportRevision: this.renderer.getRenderedViewportRevision(),
					bitmap,
				},
				[bitmap],
			);
		} catch (error) {
			this.publishCaughtError("magnifier", error, false);
		}
	}

	/**
	 * Installs a transferred bitmap, clears its pending marker, and requests repaint.
	 * @param message Asset response message.
	 * @returns Nothing.
	 */
	private handleAssetResponse(message: RenderAssetResponseMessage): void {
		const state = this.repository.getFrameState();
		if (
			state &&
			message.payload.assetRevision !== state.revisions.assets
		) {
			message.payload.bitmap.close();
			this.renderer.notifyAssetUnavailable(
				message.payload.assetKey,
				message.payload.color,
			);
			return;
		}
		this.repository.setAsset(
			message.payload.assetKey,
			message.payload.color,
			message.payload.bitmap,
		);
		this.renderer.notifyAssetAvailable(
			message.payload.assetKey,
			message.payload.color,
		);
		this.markDirty(DirtyCanvas.Assets, message.sequence);
	}

	/**
	 * Adds a dirty reason and schedules at most one worker animation frame.
	 * @param reason DirtyCanvas bit or bitmask.
	 * @param controlSequence Optional control message that must become visible in a frame.
	 * @param renderImmediately Whether interaction work should bypass the worker animation-frame wait.
	 * @returns Nothing.
	 */
	private markDirty(
		reason: DirtyCanvas,
		controlSequence?: number,
		renderImmediately = false,
	): void {
		this.dirtyMask |= reason | this.takePendingDataDirty();
		if (controlSequence !== undefined) {
			this.controlSequencesAwaitingRender.add(controlSequence);
		}
		if (renderImmediately) {
			this.renderInteractionFrameNow();
			return;
		}
		if (!this.ready || !this.visible || this.frameRequest !== null) return;
		this.scheduleFrame();
	}

	/**
	 * Draws a main-thread-coalesced viewport update in the current worker task.
	 * Cancelling a passive frame preserves accumulated scene and committed-data dirtiness
	 * while removing a redundant second display-frame wait from interactive input.
	 * @returns Nothing.
	 */
	private renderInteractionFrameNow(): void {
		if (
			this.disposed ||
			!this.ready ||
			!this.visible ||
			!this.repository.getFrameState()
		) {
			return;
		}
		if (this.frameRequest !== null) {
			cancelAnimationFrame(this.frameRequest);
			this.frameRequest = null;
		}
		this.renderFrame(performance.now());
	}

	/**
	 * Accumulates data-only invalidation until the serial data queue becomes quiet.
	 * Control-driven interaction frames bypass the delay through markDirty and include
	 * every committed data reason available at that moment.
	 * @param reason Data revision dirty bit or bitmask.
	 * @returns Nothing.
	 */
	private markDataDirty(reason: DirtyCanvas): void {
		const startsDirtyWindow = this.pendingDataDirtyMask === DirtyCanvas.None;
		this.pendingDataDirtyMask |= reason;
		if (startsDirtyWindow) this.scheduleMaximumDataRedraw();
	}

	/**
	 * Starts the non-renewable deadline for committed data awaiting a visible frame.
	 * Additional envelopes may reset the quiet timer but never this maximum wait.
	 * @returns Nothing.
	 */
	private scheduleMaximumDataRedraw(): void {
		if (this.dataRedrawMaximumTimeout !== null || this.disposed) return;
		this.dataRedrawMaximumTimeout = setTimeout(() => {
			this.dataRedrawMaximumTimeout = null;
			if (
				this.pendingDataDirtyMask === DirtyCanvas.None ||
				!this.ready ||
				!this.visible ||
				this.disposed
			) {
				return;
			}
			const reason = this.pendingDataDirtyMask;
			this.pendingDataDirtyMask = DirtyCanvas.None;
			this.suspendPendingDataRedraw();
			this.markDirty(reason);
		}, DATA_REDRAW_MAXIMUM_WAIT_MS);
	}

	/**
	 * Cancels the quiet-window timer when another data envelope reaches the worker.
	 * The accumulated reason remains intact for either an interaction frame or final drain.
	 * @returns Nothing.
	 */
	private suspendPendingDataRedraw(): void {
		if (this.dataRedrawTimeout === null) return;
		clearTimeout(this.dataRedrawTimeout);
		this.dataRedrawTimeout = null;
	}

	/**
	 * Schedules one guaranteed final redraw after the received data queue drains.
	 * @returns Nothing.
	 */
	private schedulePendingDataRedraw(): void {
		if (
			this.pendingDataDirtyMask === DirtyCanvas.None ||
			this.dataRedrawTimeout !== null ||
			this.disposed
		) {
			return;
		}
		this.dataRedrawTimeout = setTimeout(() => {
			this.dataRedrawTimeout = null;
			const reason = this.pendingDataDirtyMask;
			this.pendingDataDirtyMask = DirtyCanvas.None;
			if (reason !== DirtyCanvas.None) this.markDirty(reason);
		}, DATA_REDRAW_QUIET_WINDOW_MS);
	}

	/**
	 * Consumes deferred data invalidation when an immediate control frame is requested.
	 * @returns Accumulated data dirty bits.
	 */
	private takePendingDataDirty(): DirtyCanvas {
		this.suspendPendingDataRedraw();
		this.cancelMaximumDataRedraw();
		const reason = this.pendingDataDirtyMask;
		this.pendingDataDirtyMask = DirtyCanvas.None;
		return reason;
	}

	/**
	 * Cancels the maximum data-redraw deadline after its dirty state is consumed.
	 * @returns Nothing.
	 */
	private cancelMaximumDataRedraw(): void {
		if (this.dataRedrawMaximumTimeout === null) return;
		clearTimeout(this.dataRedrawMaximumTimeout);
		this.dataRedrawMaximumTimeout = null;
	}

	/**
	 * Requests the worker animation frame used by the on-demand render loop.
	 * @returns Nothing.
	 */
	private scheduleFrame(): void {
		if (
			this.disposed ||
			this.frameRequest !== null ||
			!this.repository.getFrameState() ||
			typeof requestAnimationFrame !== "function"
		) {
			return;
		}
		this.frameRequest = requestAnimationFrame((timestamp) => {
			this.renderFrame(timestamp);
		});
	}

	/**
	 * Captures and clears dirty state before drawing so callbacks cannot lose invalidation.
	 * @param timestamp Worker animation-frame timestamp.
	 * @returns Nothing.
	 */
	private renderFrame(timestamp: number): void {
		this.frameRequest = null;
		const reasons = this.dirtyMask;
		const renderedDataSequence = this.highestCommittedDataSequence;
		const dataSequencesInFrame = new Set(
			Array.from(this.dataSequencesAwaitingRender).filter(
				(sequence) => sequence <= renderedDataSequence,
			),
		);
		const controlSequencesInFrame = new Set(
			this.controlSequencesAwaitingRender,
		);
		this.dirtyMask = DirtyCanvas.None;
		try {
			const state = this.repository.getFrameState();
			const drawStartedAt = performance.timeOrigin + performance.now();
			const statistics = this.renderer.render(timestamp, reasons);
			if (statistics) {
				const receivedAt = this.viewportReceivedAtByRevision.get(
					statistics.viewportRevision,
				);
				if (receivedAt !== undefined) {
					statistics.workerReceiveDuration = Math.max(
						0,
						receivedAt - (state?.inputTimestamp ?? receivedAt),
					);
					statistics.workerWaitDuration = Math.max(
						0,
						drawStartedAt - receivedAt,
					);
				}
				statistics.dataLatency = this.consumeDataLatency(
					statistics.dataRevision,
				);
				this.postControl("FRAME_RENDERED", {
					...statistics,
					renderedDataSequence,
				});
				for (const sequence of dataSequencesInFrame) {
					this.dataSequencesAwaitingRender.delete(sequence);
				}
				for (const sequence of controlSequencesInFrame) {
					this.controlSequencesAwaitingRender.delete(sequence);
				}
				this.publishHoverIfChanged();
				this.flushOrderedRequests();
				for (const revision of this.viewportReceivedAtByRevision.keys()) {
					if (revision <= statistics.viewportRevision) {
						this.viewportReceivedAtByRevision.delete(revision);
					}
				}
			}
		} catch (error) {
			this.publishCaughtError("render", error, true);
			return;
		}
		if (this.dirtyMask !== DirtyCanvas.None) this.scheduleFrame();
	}

	/**
	 * Resolves worker-receipt-to-visible latency for revisions represented by a frame.
	 * @param renderedRevision Highest committed data revision visible in the frame.
	 * @returns Worst resolved latency in milliseconds, or null without new data.
	 */
	private consumeDataLatency(renderedRevision: number): number | null {
		let latency: number | null = null;
		const now = performance.now();
		for (const [revision, receivedAt] of this.dataReceivedAtByRevision) {
			if (revision > renderedRevision) continue;
			latency = Math.max(latency ?? 0, now - receivedAt);
			this.dataReceivedAtByRevision.delete(revision);
		}
		return latency;
	}

	/**
	 * Emits HOVER_CHANGED only when the compact note/link identity changes.
	 * @returns Nothing.
	 */
	private publishHoverIfChanged(): void {
		const state = this.repository.getFrameState();
		if (!state) return;
		const hit = state.pointer.inside
			? this.repository.hitTest(state.pointer.x, state.pointer.y)
			: null;
		const key = this.getHitKey(hit);
		if (key === this.lastHoverKey) return;
		this.lastHoverKey = key;
		this.postControl("HOVER_CHANGED", {
			viewportRevision: this.renderer.getRenderedViewportRevision(),
			hit,
		});
	}

	/**
	 * Builds a deterministic hover identity key without serializing full entities.
	 * @param hit Compact hit-test identity.
	 * @returns Comparable key, or null for empty space.
	 */
	private getHitKey(hit: RenderHitTestResult | null): string | null {
		if (!hit) return null;
		return hit.kind === "note"
			? `note:${hit.noteIds.join(",")}`
			: `link:${hit.id}`;
	}

	/**
	 * Resolves queued click and latest magnifier requests after their viewport renders.
	 * @returns Nothing.
	 */
	private flushOrderedRequests(): void {
		if (this.disposed) return;
		const remaining: RenderHitTestMessage[] = [];
		for (const request of this.pendingHitTests) {
			if (this.isOrderedRequestReady(request)) {
				this.publishHitTest(request);
			} else {
				remaining.push(request);
			}
		}
		this.pendingHitTests = remaining;
		if (
			this.pendingMagnifier &&
			this.isOrderedRequestReady(this.pendingMagnifier)
		) {
			const request = this.pendingMagnifier;
			this.pendingMagnifier = null;
			this.publishMagnifier(request);
		}
	}

	/**
	 * Determines whether an interaction can observe hit regions from all preceding
	 * cross-port updates, control-state repaints, and the latest requested viewport.
	 * @param request Hit-test or magnifier request awaiting an ordered render state.
	 * @returns True when no earlier input, data staging, or repaint remains pending.
	 */
	private isOrderedRequestReady(
		request: RenderHitTestMessage | RenderMagnifierRequestMessage,
	): boolean {
		const hasEarlierData = (sequence: number): boolean => {
			for (const dataSequence of this.activeDataPatchSequences) {
				if (dataSequence < sequence) return true;
			}
			for (const dataSequence of this.dataSequencesAwaitingRender) {
				if (dataSequence < sequence) return true;
			}
			return false;
		};
		const hasEarlierDirtyControl = (sequence: number): boolean => {
			for (const controlSequence of this.controlSequencesAwaitingRender) {
				if (controlSequence < sequence) return true;
			}
			return false;
		};
		return (
			this.lastContiguousIncomingSequence >= request.sequence &&
			!hasEarlierData(request.sequence) &&
			!hasEarlierDirtyControl(request.sequence) &&
			this.renderer.getRenderedViewportRevision() >=
				request.payload.viewportRevision
		);
	}

	/**
	 * Clears all operation-scoped work and advances the accepted epoch.
	 * @param nextEpoch New operation epoch from the reset envelope.
	 * @returns Nothing.
	 */
	private resetOperation(nextEpoch: number): void {
		if (nextEpoch <= this.operationEpoch) return;
		if (this.frameRequest !== null) {
			cancelAnimationFrame(this.frameRequest);
			this.frameRequest = null;
		}
		this.operationEpoch = nextEpoch;
		this.dirtyMask = DirtyCanvas.None;
		this.takePendingDataDirty();
		this.pendingHitTests = [];
		this.pendingMagnifier = null;
		this.lastHoverKey = null;
		this.dataReceivedAtByRevision.clear();
		this.viewportReceivedAtByRevision.clear();
		this.activeDataPatchSequences.clear();
		this.dataSequencesAwaitingRender.clear();
		this.acknowledgedSourceStates.clear();
		this.controlSequencesAwaitingRender.clear();
		this.highestCommittedDataSequence = 0;
		this.repository.resetOperation();
		this.renderer.reset();
		this.replayInitializationState();
	}

	/**
	 * Re-emits initialization state in the new epoch so cross-direction message
	 * ordering cannot orphan the client's one-time capability or readiness checks.
	 * @returns Nothing.
	 */
	private replayInitializationState(): void {
		this.postCapabilities();
		if (this.readyPayload) {
			this.postControl("READY", this.readyPayload);
		}
	}

	/**
	 * Releases ports, staged data, bitmaps, caches, and scheduled work.
	 * @returns Nothing.
	 */
	public dispose(): void {
		if (this.disposed) return;
		this.disposed = true;
		if (this.frameRequest !== null) cancelAnimationFrame(this.frameRequest);
		this.frameRequest = null;
		this.takePendingDataDirty();
		this.dataReceivedAtByRevision.clear();
		this.viewportReceivedAtByRevision.clear();
		this.activeDataPatchSequences.clear();
		this.dataSequencesAwaitingRender.clear();
		this.acknowledgedSourceStates.clear();
		this.controlSequencesAwaitingRender.clear();
		this.highestCommittedDataSequence = 0;
		this.renderer.dispose();
		this.repository.dispose();
		this.controlPort.close();
		this.dataPort.close();
	}

	/**
	 * Performs the minimal runtime shape check shared by both port handlers.
	 * @param value Untrusted structured-clone value.
	 * @returns True when common envelope fields are present.
	 */
	private isEnvelope(
		value: unknown,
	): value is RenderProtocolEnvelope<string, unknown> {
		if (!value || typeof value !== "object") return false;
		const record = value as Record<string, unknown>;
		return (
			typeof record.protocolVersion === "number" &&
			typeof record.operationEpoch === "number" &&
			typeof record.sequence === "number" &&
			typeof record.type === "string" &&
			"payload" in record
		);
	}

	/**
	 * Posts a typed response through the control port with a new worker sequence.
	 * @param type Response type.
	 * @param payload Response payload.
	 * @param transfer Optional transferable ownership list.
	 * @returns Nothing.
	 */
	private postControl<TType extends RenderWorkerResponse["type"]>(
		type: TType,
		payload: Extract<RenderWorkerResponse, { type: TType }>["payload"],
		transfer: Transferable[] = [],
	): void {
		if (this.disposed) return;
		const response = this.createResponse(type, payload);
		this.controlPort.postMessage(response, transfer);
	}

	/**
	 * Posts a typed acknowledgement through the low-priority data port.
	 * @param type ACK response type.
	 * @param payload ACK response payload.
	 * @returns Nothing.
	 */
	private postData(
		type: "ACK",
		payload: Extract<RenderWorkerResponse, { type: "ACK" }>["payload"],
	): void {
		if (this.disposed) return;
		this.dataPort.postMessage(this.createResponse(type, payload));
	}

	/**
	 * Collects source identities whose event state was represented by one data patch.
	 * @param message Data message being acknowledged.
	 * @returns Unique legacy-event, snapshot, projection, and eviction identities.
	 */
	private getDataPatchSourceIds(message: RenderDataMessage): string[] {
		return Array.from(
			new Set([
				...(message.payload.events ?? []).map((batch) => batch.sourceId),
				...(message.payload.eventSnapshots ?? []).map(
					(record) => record.sourceId,
				),
				...(message.payload.renderWindow?.sources ?? []).map(
					(source) => source.sourceId,
				),
				...(message.payload.renderWindow?.evictedSourceIds ?? []),
			]),
		);
	}

	/**
	 * Builds authoritative source-revision telemetry from committed repository state.
	 * @param message Data message whose backpressure slot is being released.
	 * @param rejectedSourceIds Sources that require an authoritative resynchronization.
	 * @returns ACK payload that never infers acceptance from the submitted patch.
	 */
	private createDataAcknowledgement(
		message: RenderDataMessage,
		rejectedSourceIds: ReadonlySet<string>,
	): Extract<RenderWorkerResponse, { type: "ACK" }>["payload"] {
		const sourceRevisions = this.getDataPatchSourceIds(message)
			.filter((sourceId) => !rejectedSourceIds.has(sourceId))
			.map((sourceId) => {
				const revision = this.repository.getEventRevision(sourceId);
				const previous = this.acknowledgedSourceStates.get(sourceId);
				const state =
					previous?.revision === revision
						? previous
						: { revision, dataSequence: message.sequence };
				this.acknowledgedSourceStates.set(sourceId, state);
				return { sourceId, ...state };
			});
		for (const sourceId of rejectedSourceIds) {
			this.acknowledgedSourceStates.delete(sourceId);
		}
		return {
			acknowledgedSequence: message.sequence,
			globalRevision: this.repository.getAcknowledgedGlobalRevision(),
			sourceRevisions,
			rejectedSourceIds: Array.from(rejectedSourceIds),
		};
	}

	/**
	 * Creates one correctly versioned worker response envelope.
	 * @param type Response discriminant.
	 * @param payload Response payload.
	 * @returns Typed protocol envelope.
	 */
	private createResponse(
		type: RenderWorkerResponse["type"],
		payload: RenderWorkerResponse["payload"],
	): RenderProtocolEnvelope<RenderWorkerResponse["type"], RenderWorkerResponse["payload"]> {
		return {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: this.operationEpoch,
			sequence: ++this.outgoingSequence,
			type,
			payload,
		};
	}

	/**
	 * Publishes a serializable caught exception through the control port.
	 * @param phase Worker phase that failed.
	 * @param error Unknown caught value.
	 * @param fatal Whether the client must activate fallback.
	 * @returns Nothing.
	 */
	private publishCaughtError(
		phase: Extract<RenderWorkerResponse, { type: "ERROR" }>["payload"]["phase"],
		error: unknown,
		fatal: boolean,
	): void {
		this.publishError(
			phase,
			this.getErrorMessage(error),
			fatal,
			error instanceof Error ? error.stack : undefined,
		);
	}

	/**
	 * Publishes one explicit worker error response.
	 * @param phase Worker phase that failed.
	 * @param message Human-readable diagnostic message.
	 * @param fatal Whether the client must activate fallback.
	 * @param stack Optional serialized stack trace.
	 * @returns Nothing.
	 */
	private publishError(
		phase: Extract<RenderWorkerResponse, { type: "ERROR" }>["payload"]["phase"],
		message: string,
		fatal: boolean,
		stack?: string,
	): void {
		this.postControl("ERROR", { phase, message, fatal, stack });
	}

	/**
	 * Converts any thrown value into a readable diagnostic string.
	 * @param error Unknown caught value.
	 * @returns Error message or stringified fallback.
	 */
	private getErrorMessage(error: unknown): string {
		return error instanceof Error ? error.message : String(error);
	}
}
