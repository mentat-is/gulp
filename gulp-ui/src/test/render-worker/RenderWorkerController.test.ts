import { MessageChannel as NodeMessageChannel } from "worker_threads";
import {
	DirtyCanvas,
	RENDER_PROTOCOL_VERSION,
	type RenderAssetResponseMessage,
	type RenderFrameState,
	type RenderDataMessage,
	type RenderHitTestMessage,
	type RenderMagnifierRequestMessage,
	type RenderPointerMessage,
	type RenderResetOperationMessage,
	type RenderSceneState,
	type RenderSceneStateMessage,
	type RenderSource,
	type RenderViewportState,
	type RenderViewportStateMessage,
	type RenderVisibilityMessage,
	type RenderWorkerBootstrapMessage,
	type RenderWorkerResponse,
} from "@/render-worker/RenderWorker.types";
import {
	DATA_REDRAW_MAXIMUM_WAIT_MS,
	RenderWorkerController,
} from "@/render-worker/RenderWorkerController";

const originalMessageChannel = globalThis.MessageChannel;
const originalRequestAnimationFrame = globalThis.requestAnimationFrame;
const originalCancelAnimationFrame = globalThis.cancelAnimationFrame;

interface ControllerHarness {
	controller: RenderWorkerController;
	controlPort: MessagePort;
	dataPort: MessagePort;
	responses: RenderWorkerResponse[];
}

/**
 * Creates a no-op OffscreenCanvas-compatible context for controller scheduling tests.
 * @returns Fake offscreen canvas whose context supports the worker render core.
 */
function createOffscreenCanvas(): OffscreenCanvas {
	const canvasRecord: {
		width: number;
		height: number;
		getContext?: () => OffscreenCanvasRenderingContext2D;
		transferToImageBitmap?: () => ImageBitmap;
	} = { width: 1, height: 1 };
	const context = {
		canvas: canvasRecord,
		globalAlpha: 1,
		fillStyle: "#000000",
		strokeStyle: "#000000",
		font: "10px monospace",
		lineCap: "butt",
		lineWidth: 1,
		shadowBlur: 0,
		shadowColor: "transparent",
		textAlign: "start",
		textBaseline: "alphabetic",
		imageSmoothingEnabled: false,
		imageSmoothingQuality: "low",
		arc: jest.fn(),
		beginPath: jest.fn(),
		closePath: jest.fn(),
		clearRect: jest.fn(),
		createLinearGradient: jest.fn(
			() =>
				({ addColorStop: jest.fn() }) as unknown as CanvasGradient,
		),
		drawImage: jest.fn(),
		fill: jest.fn(),
		fillRect: jest.fn(),
		fillText: jest.fn(),
		lineTo: jest.fn(),
		measureText: jest.fn(
			(text: string) => ({ width: text.length * 6 }) as TextMetrics,
		),
		moveTo: jest.fn(),
		rect: jest.fn(),
		restore: jest.fn(),
		roundRect: jest.fn(),
		save: jest.fn(),
		setLineDash: jest.fn(),
		stroke: jest.fn(),
	} as unknown as OffscreenCanvasRenderingContext2D;
	canvasRecord.getContext = () => context;
	/**
	 * Creates the transferable result returned by the magnifier scratch surface.
	 * @returns Empty transferable bitmap fixture.
	 */
	canvasRecord.transferToImageBitmap = () =>
		new ArrayBuffer(0) as unknown as ImageBitmap;
	return canvasRecord as unknown as OffscreenCanvas;
}

/**
 * Creates a constructible magnifier scratch-canvas fixture.
 * @param width Requested output width.
 * @param height Requested output height.
 * @returns Offscreen canvas fixture with a transferable bitmap result.
 */
function createMagnifierOffscreenCanvas(
	width: number,
	height: number,
): OffscreenCanvas {
	const canvas = createOffscreenCanvas();
	canvas.width = width;
	canvas.height = height;
	return canvas;
}

/**
 * Creates a frame state that renders without source or overlay fixtures.
 * @param viewportRevision Revision represented by the frame.
 * @returns Complete frame state for controller tests.
 */
function createFrameState(viewportRevision: number): RenderFrameState {
	return {
		width: 500,
		height: 100,
		frame: { min: 0, max: 1_000 },
		scale: 1,
		scrollX: 0,
		scrollY: 0,
		viewportRevision,
		inputSequence: viewportRevision,
		inputTimestamp: performance.timeOrigin,
		sourceOrder: [],
		hidden: { notes: false, links: false },
		target: null,
		pointer: { x: 0, y: 0, inside: false },
		palette: {
			fontAccent: "#ffffff",
			fontSecondary: "#cccccc",
			backgroundAccent: "#222222",
			backgroundSecondary: "#111111",
			border: "#555555",
		},
		targetGuideColor: "#ff0000",
		localized: {
			ingestingSuffix: " ingesting",
			loadingSuffix: " loading",
			unknownContext: "Unknown",
			locale: "en-US",
			timeZone: "UTC",
		},
		revisions: {
			settings: 1,
			sources: 1,
			overlays: 1,
			theme: 1,
			assets: 1,
		},
	};
}

/**
 * Selects the viewport half of a complete controller frame fixture.
 * @param state Complete internal frame state.
 * @returns Viewport protocol payload.
 */
function createViewportState(state: RenderFrameState): RenderViewportState {
	return {
		width: state.width,
		height: state.height,
		frame: state.frame,
		scale: state.scale,
		scrollX: state.scrollX,
		scrollY: state.scrollY,
		viewportRevision: state.viewportRevision,
		inputSequence: state.inputSequence,
		inputTimestamp: state.inputTimestamp,
	};
}

/**
 * Selects the scene half of a complete controller frame fixture.
 * @param state Complete internal frame state.
 * @returns Scene protocol payload.
 */
function createSceneState(state: RenderFrameState): RenderSceneState {
	return {
		sourceOrder: state.sourceOrder,
		hidden: state.hidden,
		target: state.target,
		palette: state.palette,
		targetGuideColor: state.targetGuideColor,
		localized: state.localized,
		revisions: state.revisions,
	};
}

/**
 * Creates one deterministic source record for cross-port epoch ordering tests.
 * @returns Worker-safe source fixture.
 */
function createRenderSource(): RenderSource {
	return {
		id: "source-a",
		revision: 1,
		rowOrder: 0,
		name: "Source A",
		contextLabel: "Context",
		contextColor: "#334455",
		contextTintAlpha: 0.1,
		timestamp: { min: 0, max: 1_000 },
		total: 0,
		loadingKind: "none",
		offset: 0,
		engine: "default",
		frequency: 100,
		gradient: ["#000000", "#ffffff"],
		overrides: [],
		customPaletteColors: [],
	};
}

/**
 * Creates a controller and retains the main-thread side of its two ports.
 * @param operationEpoch Initial operation epoch.
 * @returns Controller, ports, and captured response list.
 */
function createControllerHarness(operationEpoch: number): ControllerHarness {
	const controlChannel = new MessageChannel();
	const dataChannel = new MessageChannel();
	const responses: RenderWorkerResponse[] = [];
	controlChannel.port2.onmessage = (event: MessageEvent<unknown>) => {
		responses.push(event.data as RenderWorkerResponse);
	};
	controlChannel.port2.start();
	dataChannel.port2.start();
	dataChannel.port2.onmessage = (event: MessageEvent<unknown>) => {
		responses.push(event.data as RenderWorkerResponse);
	};
	const bootstrap: RenderWorkerBootstrapMessage = {
		protocolVersion: RENDER_PROTOCOL_VERSION,
		operationEpoch,
		sequence: 0,
		type: "INIT",
		payload: {
			canvas: createOffscreenCanvas(),
			controlPort: controlChannel.port1,
			dataPort: dataChannel.port1,
			fonts: [],
		},
	};
	return {
		controller: new RenderWorkerController(bootstrap),
		controlPort: controlChannel.port2,
		dataPort: dataChannel.port2,
		responses,
	};
}

/**
 * Waits until an asynchronous MessagePort condition becomes true.
 * @param predicate Condition checked after each task turn.
 * @returns Promise rejected if the condition remains false after bounded retries.
 */
async function waitForCondition(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 30; attempt++) {
		if (predicate()) return;
		await new Promise<void>((resolve) => {
			setTimeout(resolve, 0);
		});
	}
	throw new Error("Timed out waiting for worker controller message");
}

describe("RenderWorkerController", () => {
	const animationFrames = new Map<number, FrameRequestCallback>();
	let nextAnimationFrameId = 1;
	const harnesses: ControllerHarness[] = [];

	beforeAll(() => {
		globalThis.MessageChannel =
			NodeMessageChannel as unknown as typeof MessageChannel;
		globalThis.requestAnimationFrame = (callback: FrameRequestCallback): number => {
			const identifier = nextAnimationFrameId++;
			animationFrames.set(identifier, callback);
			return identifier;
		};
		globalThis.cancelAnimationFrame = (identifier: number): void => {
			animationFrames.delete(identifier);
		};
	});

	afterAll(() => {
		globalThis.MessageChannel = originalMessageChannel;
		globalThis.requestAnimationFrame = originalRequestAnimationFrame;
		globalThis.cancelAnimationFrame = originalCancelAnimationFrame;
	});

	beforeEach(() => {
		animationFrames.clear();
	});

	afterEach(() => {
		while (harnesses.length > 0) {
			const harness = harnesses.pop();
			harness?.controller.dispose();
			harness?.controlPort.close();
			harness?.dataPort.close();
		}
	});

	it("renders an interaction viewport immediately with preceding control dirtiness", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const initialState = createFrameState(1);
		const sceneMessage: RenderSceneStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(initialState), dirty: DirtyCanvas.Sources },
		};
		const sizeMessage: RenderViewportStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(initialState), dirty: DirtyCanvas.Size },
		};
		const viewportMessage: RenderViewportStateMessage = {
			...sizeMessage,
			sequence: 3,
			payload: {
				state: createViewportState(createFrameState(2)),
				dirty: DirtyCanvas.Viewport,
			},
		};

		harness.controlPort.postMessage(sceneMessage);
		harness.controlPort.postMessage(sizeMessage);
		harness.controlPort.postMessage(viewportMessage);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "FRAME_RENDERED"),
		);
		const rendered = harness.responses.find(
			(response) => response.type === "FRAME_RENDERED",
		);

		expect(rendered?.payload).toMatchObject({
			dirtyMask:
				DirtyCanvas.Sources | DirtyCanvas.Size | DirtyCanvas.Viewport,
			viewportRevision: 2,
		});
		expect(animationFrames.size).toBe(0);
	});

	it("applies an atomic viewport pointer before the immediate hover frame", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const source = createRenderSource();
		const state = createFrameState(1);
		state.sourceOrder = [source.id];
		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				sources: {
					revision: 1,
					records: [source],
					removedSourceIds: [],
				},
				overlays: {
					notes: {
						revision: 1,
						records: [
							{
								id: "note-a",
								sourceId: source.id,
								timestamp: 500,
								name: "A",
								color: "#ffaa00",
								iconKey: "note",
							},
						],
					},
				},
			},
		} satisfies RenderDataMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 1,
			),
		);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Sources },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "VIEWPORT_STATE",
			payload: {
				state: createViewportState(state),
				dirty: DirtyCanvas.Viewport,
				pointer: { x: 250, y: 24, inside: true },
			},
		} satisfies RenderViewportStateMessage);

		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "HOVER_CHANGED" &&
					response.payload.hit?.kind === "note",
			),
		);
		const hoverResponse = harness.responses.find(
			(response) => response.type === "HOVER_CHANGED",
		);
		expect(hoverResponse?.payload).toMatchObject({
			viewportRevision: 1,
			hit: { kind: "note", id: "note-a" },
		});
		expect(animationFrames.size).toBe(0);
	});

	it("renders one guaranteed final data frame after consecutive patches drain", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const source = createRenderSource();
		const state = createFrameState(1);
		state.sourceOrder = [source.id];
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 1,
		);

		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				sources: {
					revision: 1,
					records: [source],
					removedSourceIds: [],
				},
			},
		} satisfies RenderDataMessage);
		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 2,
				sources: {
					revision: 2,
					records: [{ ...source, revision: 2, name: "Source A updated" }],
					removedSourceIds: [],
				},
			},
		} satisfies RenderDataMessage);
		await waitForCondition(
			() =>
				harness.responses.filter((response) => response.type === "ACK")
					.length === 2,
		);
		await waitForCondition(() => animationFrames.size === 1);

		const finalFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(finalFrame[0]);
		finalFrame[1](200);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 2,
		);
		const renderedFrames = harness.responses.filter(
			(response) => response.type === "FRAME_RENDERED",
		);

		expect(renderedFrames[1].payload).toMatchObject({
			dataRevision: 2,
			renderedDataSequence: 4,
			dirtyMask: DirtyCanvas.Sources,
		});
		expect(animationFrames.size).toBe(0);
	});

	it("forces a committed data frame within 100 ms during continuous traffic", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const source = createRenderSource();
		const state = createFrameState(1);
		state.sourceOrder = [source.id];
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 1,
		);

		let nextPatch = 0;
		const sentSequences = new Set<number>();
		const interval = setInterval(() => {
			nextPatch++;
			const sequence = nextPatch + 2;
			sentSequences.add(sequence);
			harness.dataPort.postMessage({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence,
				type: "DATA_PATCH",
				payload: {
					globalRevision: nextPatch,
					sources: {
						revision: nextPatch,
						records: [
							{
								...source,
								revision: nextPatch,
								name: `Source ${nextPatch}`,
							},
						],
						removedSourceIds: [],
					},
				},
			} satisfies RenderDataMessage);
		}, 2);
		await new Promise<void>((resolve) =>
			setTimeout(resolve, DATA_REDRAW_MAXIMUM_WAIT_MS + 25),
		);
		clearInterval(interval);

		expect(nextPatch > 0).toBe(true);
		expect(animationFrames.size).toBe(1);
		const forcedFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(forcedFrame[0]);
		forcedFrame[1](200);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 2,
		);
		const frame = harness.responses.filter(
			(response) => response.type === "FRAME_RENDERED",
		)[1];
		expect(frame.type).toBe("FRAME_RENDERED");
		if (frame.type === "FRAME_RENDERED") {
			expect(sentSequences.has(frame.payload.renderedDataSequence)).toBe(true);
			expect(frame.payload.dirtyMask & DirtyCanvas.Sources).toBe(
				DirtyCanvas.Sources,
			);
		}
	});

	it("defers hidden data frames and cancels redraw deadlines on dispose", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const source = createRenderSource();
		const state = createFrameState(1);
		state.sourceOrder = [source.id];
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "FRAME_RENDERED"),
		);

		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "VISIBILITY",
			payload: { visible: false },
		} satisfies RenderVisibilityMessage);
		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				sources: {
					revision: 1,
					records: [source],
					removedSourceIds: [],
				},
			},
		} satisfies RenderDataMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 4,
			),
		);
		await new Promise<void>((resolve) =>
			setTimeout(resolve, DATA_REDRAW_MAXIMUM_WAIT_MS + 10),
		);
		expect(animationFrames.size).toBe(0);

		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 5,
			type: "VISIBILITY",
			payload: { visible: true },
		} satisfies RenderVisibilityMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const visibleFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(visibleFrame[0]);
		visibleFrame[1](200);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 2,
		);

		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 6,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 2,
				sources: {
					revision: 2,
					records: [{ ...source, revision: 2 }],
					removedSourceIds: [],
				},
			},
		} satisfies RenderDataMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 6,
			),
		);
		harness.controller.dispose();
		await new Promise<void>((resolve) =>
			setTimeout(resolve, DATA_REDRAW_MAXIMUM_WAIT_MS + 10),
		);
		expect(animationFrames.size).toBe(0);
	});

	it("reports zero viewport timing for a scene-only redraw of an old viewport", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const state = createFrameState(1);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const firstFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(firstFrame[0]);
		firstFrame[1](100);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 1,
		);

		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "SCENE_STATE",
			payload: {
				state: { ...createSceneState(state), hidden: { notes: true, links: false } },
				dirty: DirtyCanvas.Theme,
			},
		} satisfies RenderSceneStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const secondFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(secondFrame[0]);
		secondFrame[1](200);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 2,
		);
		const renderedFrames = harness.responses.filter(
			(response) => response.type === "FRAME_RENDERED",
		);

		expect(renderedFrames[1]?.payload).toMatchObject({
			viewportRevision: 1,
			workerReceiveDuration: 0,
			workerWaitDuration: 0,
		});
	});

	it("waits for a preceding scene repaint before hit testing the same viewport", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const state = createFrameState(1);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) => response.type === "FRAME_RENDERED",
				).length === 1,
		);

		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "SCENE_STATE",
			payload: {
				state: {
					...createSceneState(state),
					hidden: { notes: true, links: false },
				},
				dirty: DirtyCanvas.Overlays,
			},
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "HIT_TEST",
			payload: { requestId: 101, x: 10, y: 10, viewportRevision: 1 },
		} satisfies RenderHitTestMessage);
		await waitForCondition(() => animationFrames.size === 1);
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 101,
			),
		).toBe(false);

		const sceneFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(sceneFrame[0]);
		sceneFrame[1](200);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 101,
			),
		);
	});

	it("waits for a preceding scene repaint before capturing a magnifier frame", async () => {
		const originalDescriptor = Object.getOwnPropertyDescriptor(
			globalThis,
			"OffscreenCanvas",
		);
		Object.defineProperty(globalThis, "OffscreenCanvas", {
			configurable: true,
			writable: true,
			value: createMagnifierOffscreenCanvas,
		});
		try {
			const harness = createControllerHarness(1);
			harnesses.push(harness);
			await waitForCondition(() =>
				harness.responses.some((response) => response.type === "READY"),
			);
			const state = createFrameState(1);
			harness.controlPort.postMessage({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 1,
				type: "SCENE_STATE",
				payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
			} satisfies RenderSceneStateMessage);
			harness.controlPort.postMessage({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 2,
				type: "VIEWPORT_STATE",
				payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
			} satisfies RenderViewportStateMessage);
			await waitForCondition(() => animationFrames.size === 1);
			const initialFrame = Array.from(animationFrames.entries())[0];
			animationFrames.delete(initialFrame[0]);
			initialFrame[1](100);
			await waitForCondition(
				() =>
					harness.responses.filter(
						(response) => response.type === "FRAME_RENDERED",
					).length === 1,
			);

			harness.controlPort.postMessage({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 3,
				type: "SCENE_STATE",
				payload: {
					state: {
						...createSceneState(state),
						hidden: { notes: false, links: true },
					},
					dirty: DirtyCanvas.Overlays,
				},
			} satisfies RenderSceneStateMessage);
			harness.controlPort.postMessage({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 4,
				type: "MAGNIFIER_REQUEST",
				payload: {
					requestId: 102,
					x: 10,
					y: 10,
					cropWidth: 20,
					cropHeight: 20,
					outputWidth: 40,
					outputHeight: 40,
					viewportRevision: 1,
				},
			} satisfies RenderMagnifierRequestMessage);
			await waitForCondition(() => animationFrames.size === 1);
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			expect(
				harness.responses.some(
					(response) =>
						response.type === "MAGNIFIER_FRAME" &&
						response.payload.requestId === 102,
				),
			).toBe(false);

			const sceneFrame = Array.from(animationFrames.entries())[0];
			animationFrames.delete(sceneFrame[0]);
			sceneFrame[1](200);
			await waitForCondition(() =>
				harness.responses.some(
					(response) =>
						response.type === "MAGNIFIER_FRAME" &&
						response.payload.requestId === 102,
				),
			);
		} finally {
			if (originalDescriptor) {
				Object.defineProperty(
					globalThis,
					"OffscreenCanvas",
					originalDescriptor,
				);
			} else {
				Reflect.deleteProperty(globalThis, "OffscreenCanvas");
			}
		}
	});

	it("waits for a preceding asset installation before serving a hit test", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const state = createFrameState(1);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "FRAME_RENDERED"),
		);

		const bitmapChannel = new MessageChannel();
		const assetMessage: RenderAssetResponseMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "ASSET_RESPONSE",
			payload: {
				assetKey: "note",
				color: "#ffffff",
				assetRevision: 1,
				bitmap: bitmapChannel.port1 as unknown as ImageBitmap,
			},
		};
		harness.controlPort.postMessage(assetMessage, [bitmapChannel.port1]);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "HIT_TEST",
			payload: { requestId: 103, x: 10, y: 10, viewportRevision: 1 },
		} satisfies RenderHitTestMessage);
		await waitForCondition(() => animationFrames.size === 1);
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 103,
			),
		).toBe(false);

		const assetFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(assetFrame[0]);
		assetFrame[1](200);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 103,
			),
		);
		bitmapChannel.port2.close();
	});

	it("waits for a preceding hover repaint before serving a hit test", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const source = createRenderSource();
		const state = createFrameState(1);
		state.sourceOrder = [source.id];
		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				sources: {
					revision: 1,
					records: [source],
					removedSourceIds: [],
				},
				overlays: {
					notes: {
						revision: 1,
						records: [
							{
								id: "note-a",
								sourceId: source.id,
								timestamp: 500,
								name: "A",
								color: "#ffaa00",
								iconKey: "note",
							},
						],
					},
				},
			},
		} satisfies RenderDataMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 1,
			),
		);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "FRAME_RENDERED"),
		);

		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "POINTER",
			payload: {
				pointer: { x: 250, y: 24, inside: true },
				viewportRevision: 1,
			},
		} satisfies RenderPointerMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 5,
			type: "HIT_TEST",
			payload: { requestId: 104, x: 250, y: 24, viewportRevision: 1 },
		} satisfies RenderHitTestMessage);
		await waitForCondition(() => animationFrames.size === 1);
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 104,
			),
		).toBe(false);

		const hoverFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(hoverFrame[0]);
		hoverFrame[1](200);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 104,
			),
		);
	});

	it("waits for preceding cross-port overlay data to render before hit testing the same viewport", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const initialState = createFrameState(1);
		const sceneMessage: RenderSceneStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: {
				state: createSceneState(initialState),
				dirty: DirtyCanvas.Force,
			},
		};
		const frameMessage: RenderViewportStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: {
				state: createViewportState(initialState),
				dirty: DirtyCanvas.Force,
			},
		};
		harness.controlPort.postMessage(sceneMessage);
		harness.controlPort.postMessage(frameMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "FRAME_RENDERED" &&
					response.payload.viewportRevision === 1,
			),
		);

		const hitTestMessage: RenderHitTestMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "HIT_TEST",
			payload: { requestId: 42, x: 10, y: 10, viewportRevision: 1 },
		};
		harness.controlPort.postMessage(hitTestMessage);
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 42,
			),
		).toBe(false);

		const overlayPatch: RenderDataMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				overlays: { notes: { revision: 1, records: [] } },
			},
		};
		harness.dataPort.postMessage(overlayPatch);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 3,
			),
		);
		await waitForCondition(() => animationFrames.size === 1);
		expect(
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 42,
			),
		).toBe(false);

		const overlayFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(overlayFrame[0]);
		overlayFrame[1](200);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 42,
			),
		);
	});

	it("does not hold an earlier hit test behind a later data repaint", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const initialState = createFrameState(1);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: { state: createSceneState(initialState), dirty: DirtyCanvas.Force },
		} satisfies RenderSceneStateMessage);
		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(initialState), dirty: DirtyCanvas.Force },
		} satisfies RenderViewportStateMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const initialFrame = Array.from(animationFrames.entries())[0];
		animationFrames.delete(initialFrame[0]);
		initialFrame[1](100);

		harness.dataPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 4,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				overlays: { notes: { revision: 1, records: [] } },
			},
		} satisfies RenderDataMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 4,
			),
		);
		await waitForCondition(() => animationFrames.size === 1);

		harness.controlPort.postMessage({
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 3,
			type: "HIT_TEST",
			payload: { requestId: 77, x: 10, y: 10, viewportRevision: 1 },
		} satisfies RenderHitTestMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "HIT_TEST_RESULT" &&
					response.payload.requestId === 77,
			),
		);

		expect(animationFrames.size).toBe(1);
	});

	it("cancels reset work and rejects stale operation epochs", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const initialState = createFrameState(1);
		const firstScene: RenderSceneStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "SCENE_STATE",
			payload: {
				state: createSceneState(initialState),
				dirty: DirtyCanvas.Force,
			},
		};
		const firstFrame: RenderViewportStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 2,
			type: "VIEWPORT_STATE",
			payload: {
				state: createViewportState(initialState),
				dirty: DirtyCanvas.Force,
			},
		};
		harness.controlPort.postMessage(firstScene);
		harness.controlPort.postMessage(firstFrame);
		await waitForCondition(() => animationFrames.size === 1);
		const resetMessage: RenderResetOperationMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 2,
			sequence: 3,
			type: "RESET_OPERATION",
			payload: { reason: "test" },
		};
		harness.controlPort.postMessage(resetMessage);
		await waitForCondition(() => animationFrames.size === 0);

		harness.controlPort.postMessage({
			...firstFrame,
			sequence: 4,
			payload: {
				state: createViewportState(createFrameState(99)),
				dirty: DirtyCanvas.Force,
			},
		});
		await new Promise<void>((resolve) => {
			setTimeout(resolve, 0);
		});
		expect(animationFrames.size).toBe(0);

		const nextState = createFrameState(2);
		harness.controlPort.postMessage({
			...firstScene,
			operationEpoch: 2,
			sequence: 5,
			payload: {
				state: createSceneState(nextState),
				dirty: DirtyCanvas.Force,
			},
		});
		harness.controlPort.postMessage({
			...firstFrame,
			operationEpoch: 2,
			sequence: 6,
			payload: {
				state: createViewportState(nextState),
				dirty: DirtyCanvas.Force,
			},
		});
		await waitForCondition(() => animationFrames.size === 1);
		const scheduled = Array.from(animationFrames.entries())[0];
		animationFrames.delete(scheduled[0]);
		scheduled[1](200);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "FRAME_RENDERED" &&
					response.operationEpoch === 2,
			),
		);
		const rendered = harness.responses.find(
			(response) =>
				response.type === "FRAME_RENDERED" && response.operationEpoch === 2,
		);

		expect(rendered?.payload).toMatchObject({ viewportRevision: 2 });
	});

	it("replays capabilities and readiness after an operation epoch reset", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);

		const resetMessage: RenderResetOperationMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 2,
			sequence: 1,
			type: "RESET_OPERATION",
			payload: { reason: "initialization-response-race" },
		};
		harness.controlPort.postMessage(resetMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "READY" && response.operationEpoch === 2,
			),
		);

		expect(
			harness.responses.some(
				(response) =>
					response.type === "CAPABILITIES" && response.operationEpoch === 2,
			),
		).toBe(true);
		expect(
			harness.responses.some(
				(response) =>
					response.type === "READY" && response.operationEpoch === 2,
			),
		).toBe(true);
	});

	it("accepts new-epoch data before RESET without clearing it on the late control message", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const source = createRenderSource();
		const dataMessage: RenderDataMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 2,
			sequence: 1,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				sources: {
					revision: 1,
					records: [source],
					removedSourceIds: [],
				},
			},
		};
		harness.dataPort.postMessage(dataMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" && response.operationEpoch === 2,
			),
		);
		for (const [identifier, callback] of animationFrames) {
			animationFrames.delete(identifier);
			callback(250);
		}

		const lateReset: RenderResetOperationMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 2,
			sequence: 1,
			type: "RESET_OPERATION",
			payload: { reason: "cross-port-order" },
		};
		harness.controlPort.postMessage(lateReset);
		const state = createFrameState(1);
		state.sourceOrder = [source.id];
		const sceneMessage: RenderSceneStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 2,
			sequence: 2,
			type: "SCENE_STATE",
			payload: { state: createSceneState(state), dirty: DirtyCanvas.Force },
		};
		const frameMessage: RenderViewportStateMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 2,
			sequence: 3,
			type: "VIEWPORT_STATE",
			payload: { state: createViewportState(state), dirty: DirtyCanvas.Force },
		};
		harness.controlPort.postMessage(sceneMessage);
		harness.controlPort.postMessage(frameMessage);
		await waitForCondition(() => animationFrames.size === 1);
		const scheduled = Array.from(animationFrames.entries())[0];
		animationFrames.delete(scheduled[0]);
		scheduled[1](300);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "FRAME_RENDERED" &&
					response.operationEpoch === 2 &&
					response.payload.viewportRevision === 1,
			),
		);
		const rendered = harness.responses.find(
			(response) =>
				response.type === "FRAME_RENDERED" &&
				response.operationEpoch === 2 &&
				response.payload.viewportRevision === 1,
		);

		expect(rendered?.payload).toMatchObject({ visibleSources: 1 });
	});

	it("includes render-window sources and evictions in invalidation and ACK telemetry", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const sourceA = createRenderSource();
		const sourceB = { ...createRenderSource(), id: "source-b", rowOrder: 1 };
		const renderer = (
			harness.controller as unknown as {
				renderer: {
					invalidateSources: (sourceIds: readonly string[]) => void;
					invalidateEventSources: (sourceIds: readonly string[]) => void;
				};
		}
		).renderer;
		const invalidateSources = jest.spyOn(renderer, "invalidateSources");
		const invalidateEventSources = jest.spyOn(
			renderer,
			"invalidateEventSources",
		);

		const renderWindowMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				sources: {
					revision: 1,
					records: [sourceA, sourceB],
					removedSourceIds: [],
				},
				renderWindow: {
					operationEpoch: 1,
					viewportRevision: 1,
					projectionRevision: 1,
					sources: [
						{
							sourceId: sourceA.id,
							sourceGeneration: 3,
							sourceRevision: 7,
							sourceStatus: "complete",
							coveredRange: { min: 0, max: 1_000 },
							bucketPixelWidth: 1,
							degradedLod: false,
							loadedCount: 12_000_000,
							hashRange: { min: 1, max: 1 },
							representativeHandles: Uint32Array.of(1),
							representativeTimestamps: Float64Array.of(500),
							representativeHashes: Float64Array.of(1),
							heightTimestamps: Float64Array.of(0),
							heightCounts: Float64Array.of(12_000_000),
							heightMaximumCount: 12_000_000,
							graphTimestamps: new Float64Array(0),
							graphCounts: new Float64Array(0),
						},
					],
					evictedSourceIds: [sourceB.id],
				},
			},
		} satisfies RenderDataMessage;
		// worker_threads clones typed arrays into Node's realm while this suite runs
		// in jsdom; exercise the post-validation controller path without that artifact.
		await (
			harness.controller as unknown as {
				handleDataMessage: (
					message: RenderDataMessage,
					receivedAt: number,
				) => Promise<void>;
			}
		).handleDataMessage(renderWindowMessage, performance.now());
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "ACK" &&
					response.payload.acknowledgedSequence === 1,
			),
		);

		const acknowledgement = harness.responses.find(
			(response) => response.type === "ACK",
		);
		expect(acknowledgement?.payload).toMatchObject({
			sourceRevisions: [
				{ sourceId: "source-a", revision: 7, dataSequence: 1 },
				{ sourceId: "source-b", revision: 0, dataSequence: 1 },
			],
			rejectedSourceIds: [],
		});
		expect(invalidateSources.mock.calls).toEqual([[["source-b"]]]);
		expect(invalidateEventSources.mock.calls).toEqual([
			[["source-a", "source-b"]],
		]);
	});

	it("resynchronizes every source identity in a malformed render window", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const malformedMessage: RenderDataMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				renderWindow: {
					operationEpoch: 1,
					viewportRevision: 1,
					projectionRevision: 1,
					sources: [
						{
							sourceId: "source-a",
							sourceGeneration: 4,
							sourceRevision: 9,
							sourceStatus: "complete",
							coveredRange: { min: 0, max: 1_000 },
							bucketPixelWidth: 1,
							degradedLod: false,
							loadedCount: 2,
							hashRange: { min: 1, max: 2 },
							representativeHandles: Uint32Array.of(1, 2),
							representativeTimestamps: Float64Array.of(500),
							representativeHashes: Float64Array.of(1, 2),
							heightTimestamps: new Float64Array(0),
							heightCounts: new Float64Array(0),
							heightMaximumCount: 0,
							graphTimestamps: new Float64Array(0),
							graphCounts: new Float64Array(0),
						},
					],
					evictedSourceIds: ["source-b"],
				},
			},
		};

		harness.dataPort.postMessage(malformedMessage);
		await waitForCondition(
			() =>
				harness.responses.filter(
					(response) =>
						response.type === "RESYNC_REQUIRED" &&
						response.payload.scope === "source",
				).length === 2,
		);

		const sourceResyncs = harness.responses
			.filter(
				(response) =>
					response.type === "RESYNC_REQUIRED" &&
					response.payload.scope === "source",
			)
			.map((response) => response.payload);
		const acknowledgement = harness.responses.find(
			(response) => response.type === "ACK",
		);
		expect(sourceResyncs).toEqual([
			{
				scope: "source",
				id: "source-a",
				expectedRevision: 0,
				receivedRevision: 9,
			},
			{
				scope: "source",
				id: "source-b",
				expectedRevision: 0,
				receivedRevision: 0,
			},
		]);
		expect(acknowledgement?.payload).toMatchObject({
			sourceRevisions: [],
			rejectedSourceIds: ["source-a", "source-b"],
		});
	});

	it("acknowledges failed data and requests authoritative source recovery", async () => {
		const harness = createControllerHarness(1);
		harnesses.push(harness);
		await waitForCondition(() =>
			harness.responses.some((response) => response.type === "READY"),
		);
		const malformedMessage: RenderDataMessage = {
			protocolVersion: RENDER_PROTOCOL_VERSION,
			operationEpoch: 1,
			sequence: 1,
			type: "DATA_PATCH",
			payload: {
				globalRevision: 1,
				events: [
					{
						sourceId: "source-a",
						baseRevision: 0,
						revision: 1,
						mode: "replace",
						handles: Uint32Array.of(1, 2),
						timestamps: Float64Array.of(10),
						hashes: Float64Array.of(5),
					},
				],
			},
		};

		harness.dataPort.postMessage(malformedMessage);
		await waitForCondition(() =>
			harness.responses.some(
				(response) =>
					response.type === "RESYNC_REQUIRED" &&
					response.payload.scope === "source",
			),
		);

		const acknowledgement = harness.responses.find(
			(response) => response.type === "ACK",
		);
		expect(acknowledgement?.payload).toMatchObject({
			sourceRevisions: [],
			rejectedSourceIds: ["source-a"],
		});
		expect(
			harness.responses.some(
				(response) =>
					response.type === "ERROR" && response.payload.fatal === false,
			),
		).toBe(true);
	});
});
