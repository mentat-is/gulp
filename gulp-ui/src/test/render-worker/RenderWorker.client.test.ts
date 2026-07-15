import { MessageChannel as NodeMessageChannel } from "worker_threads";
import {
	applyAcknowledgedSourceRevisions,
	isRenderWorkerResponse,
} from "@/render-worker/RenderWorker.response";
import { RenderWorkerClient } from "@/render-worker/RenderWorker.client";
import {
	DirtyCanvas,
	RENDER_PROTOCOL_VERSION,
	type RenderAckMessage,
	type RenderControlMessage,
	type RenderFrameRenderedMessage,
	type RenderHitTestMessage,
	type RenderHitTestResult,
	type RenderMagnifierRequestMessage,
	type RenderPointerState,
	type RenderViewportState,
	type RenderViewportStateMessage,
	type RenderWorkerBootstrapMessage,
	type RenderWorkerResponse,
} from "@/render-worker/RenderWorker.types";

jest.mock("@/render-worker/RenderWorker.factory", () => ({
	createDefaultRenderWorker: jest.fn(),
}));

/** In-memory worker endpoint that retains the transferred bootstrap ports. */
class ClientTestWorker extends EventTarget {
	public bootstrap: RenderWorkerBootstrapMessage | null = null;
	public terminated = false;

	/**
	 * Captures the client bootstrap without transferring its test-owned ports.
	 * @param message Worker bootstrap envelope.
	 * @returns Nothing.
	 */
	public postMessage(message: unknown): void {
		this.bootstrap = message as RenderWorkerBootstrapMessage;
	}

	/**
	 * Records authoritative worker termination during client cleanup.
	 * @returns Nothing.
	 */
	public terminate(): void {
		this.terminated = true;
	}
}

interface RenderWorkerClientHarness {
	client: RenderWorkerClient;
	worker: ClientTestWorker;
	workerControlPort: MessagePort;
	workerDataPort: MessagePort;
	controlMessages: RenderControlMessage[];
	dispose(): void;
}

/**
 * Waits for an asynchronous MessagePort condition with bounded retries.
 * @param predicate Condition checked after each task turn.
 * @returns Promise resolved when the condition becomes true.
 */
async function waitForCondition(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 30; attempt++) {
		if (predicate()) return;
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
	throw new Error("Timed out waiting for render worker client message");
}

/**
 * Creates a deterministic worker-safe viewport fixture.
 * @param viewportRevision Monotonic viewport revision.
 * @returns Viewport state suitable for client control messages.
 */
function createViewportState(viewportRevision: number): RenderViewportState {
	return {
		width: 500,
		height: 300,
		frame: { min: 0, max: 1_000 },
		scale: 1,
		scrollX: viewportRevision * 10,
		scrollY: 0,
		viewportRevision,
		inputSequence: viewportRevision,
		inputTimestamp: performance.timeOrigin + performance.now(),
	};
}

/**
 * Creates a complete validated frame response for viewport backpressure tests.
 * @param operationEpoch Worker operation epoch represented by the frame.
 * @param viewportRevision Rendered viewport revision.
 * @param responseSequence Worker-to-client response sequence.
 * @returns FRAME_RENDERED response envelope.
 */
function createFrameRenderedResponse(
	operationEpoch: number,
	viewportRevision: number,
	responseSequence: number,
): RenderFrameRenderedMessage {
	return {
		protocolVersion: RENDER_PROTOCOL_VERSION,
		operationEpoch,
		sequence: responseSequence,
		type: "FRAME_RENDERED",
		payload: {
			frameTimestamp: performance.now(),
			drawDuration: 1,
			workerReceiveDuration: 1,
			workerWaitDuration: 0,
			dirtyMask: DirtyCanvas.Viewport,
			viewportRevision,
			inputSequence: viewportRevision,
			dataRevision: 0,
			dataLatency: null,
			visibleSources: 0,
			renderedRows: 0,
			renderedEventPixels: 0,
			graphSamples: 0,
			graphBuckets: 0,
			visibleNotes: 0,
			visibleLinks: 0,
			interactiveRegions: 0,
		},
	};
}

/**
 * Starts a real RenderWorkerClient around Node MessagePorts and a captured worker endpoint.
 * @param operationEpoch Initial operation epoch.
 * @returns Ready client, worker-side ports, and captured main-to-worker controls.
 */
async function createClientHarness(
	operationEpoch = 1,
): Promise<RenderWorkerClientHarness> {
	const worker = new ClientTestWorker();
	const offscreenCanvas = {} as OffscreenCanvas;
	const canvas = {
		transferControlToOffscreen: () => offscreenCanvas,
	} as HTMLCanvasElement;
	const client = RenderWorkerClient.start(canvas, operationEpoch, {
		workerFactory: () => worker as unknown as Worker,
		initializationTimeoutMs: 1_000,
	});
	const bootstrap = worker.bootstrap;
	if (!bootstrap) throw new Error("Render worker bootstrap was not captured");
	const controlMessages: RenderControlMessage[] = [];
	bootstrap.payload.controlPort.addEventListener("message", (event) => {
		controlMessages.push(event.data as RenderControlMessage);
	});
	bootstrap.payload.controlPort.start();
	bootstrap.payload.dataPort.start();
	bootstrap.payload.controlPort.postMessage({
		protocolVersion: RENDER_PROTOCOL_VERSION,
		operationEpoch,
		sequence: 1,
		type: "READY",
		payload: { fontsLoaded: true },
	} satisfies RenderWorkerResponse);
	await client.ready;

	return {
		client,
		worker,
		workerControlPort: bootstrap.payload.controlPort,
		workerDataPort: bootstrap.payload.dataPort,
		controlMessages,
		/** Releases both client-side and worker-side test resources. */
		dispose(): void {
			client.dispose();
			bootstrap.payload.controlPort.close();
			bootstrap.payload.dataPort.close();
		},
	};
}

const rejectedAcknowledgement: RenderAckMessage["payload"] = {
	acknowledgedSequence: 7,
	globalRevision: 10,
	sourceRevisions: [],
	rejectedSourceIds: ["source-a"],
};

describe("RenderWorkerClient acknowledgement telemetry", () => {
	it("rejects unknown response discriminants before routing", () => {
		expect(
			isRenderWorkerResponse({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 1,
				type: "UNKNOWN_RESPONSE",
				payload: {},
			}),
		).toBe(false);
	});

	it("rejects malformed payloads for known response discriminants", () => {
		expect(
			isRenderWorkerResponse({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 1,
				type: "ACK",
				payload: {
					acknowledgedSequence: 1,
					globalRevision: 1,
					sourceRevisions: "not-an-array",
					rejectedSourceIds: [],
				},
			}),
		).toBe(false);
	});

	it("does not reinstate a rejected revision when RESYNC arrives before ACK", () => {
		const revisions = new Map<string, number>([["source-a", 1]]);
		revisions.delete("source-a");
		applyAcknowledgedSourceRevisions(revisions, rejectedAcknowledgement);

		expect(revisions.has("source-a")).toBe(false);
	});

	it("does not reinstate a rejected revision when ACK arrives before RESYNC", () => {
		const revisions = new Map<string, number>([["source-a", 1]]);
		applyAcknowledgedSourceRevisions(revisions, rejectedAcknowledgement);
		revisions.delete("source-a");

		expect(revisions.has("source-a")).toBe(false);
	});

	it("uses the worker's accepted revision instead of an optimistic patch revision", () => {
		const revisions = new Map<string, number>([["source-a", 1]]);
		applyAcknowledgedSourceRevisions(revisions, {
			acknowledgedSequence: 8,
			globalRevision: 11,
			sourceRevisions: [{ sourceId: "source-a", revision: 2 }],
			rejectedSourceIds: [],
		});

		expect(revisions.get("source-a")).toBe(2);
	});
});

describe("RenderWorkerClient viewport control backpressure", () => {
	const originalMessageChannelDescriptor = Object.getOwnPropertyDescriptor(
		globalThis,
		"MessageChannel",
	);
	const originalWorkerDescriptor = Object.getOwnPropertyDescriptor(
		globalThis,
		"Worker",
	);
	const originalOffscreenCanvasDescriptor = Object.getOwnPropertyDescriptor(
		globalThis,
		"OffscreenCanvas",
	);
	const originalCreateImageBitmapDescriptor = Object.getOwnPropertyDescriptor(
		globalThis,
		"createImageBitmap",
	);
	const originalTransferDescriptor = Object.getOwnPropertyDescriptor(
		HTMLCanvasElement.prototype,
		"transferControlToOffscreen",
	);

	beforeAll(() => {
		Object.defineProperty(globalThis, "MessageChannel", {
			configurable: true,
			writable: true,
			value: NodeMessageChannel,
		});
		Object.defineProperty(globalThis, "Worker", {
			configurable: true,
			writable: true,
			value: ClientTestWorker,
		});
		Object.defineProperty(globalThis, "OffscreenCanvas", {
			configurable: true,
			writable: true,
			value: class TestOffscreenCanvas {},
		});
		Object.defineProperty(globalThis, "createImageBitmap", {
			configurable: true,
			writable: true,
			value: jest.fn(),
		});
		Object.defineProperty(
			HTMLCanvasElement.prototype,
			"transferControlToOffscreen",
			{
				configurable: true,
				writable: true,
				value: jest.fn(),
			},
		);
	});

	afterAll(() => {
		/**
		 * Restores one capability property after the client harness completes.
		 * @param target Object that originally owned the property.
		 * @param property Property identity to restore.
		 * @param descriptor Original descriptor, or undefined when originally absent.
		 * @returns Nothing.
		 */
		const restoreProperty = (
			target: object,
			property: PropertyKey,
			descriptor: PropertyDescriptor | undefined,
		): void => {
			if (descriptor) {
				Object.defineProperty(target, property, descriptor);
			} else {
				Reflect.deleteProperty(target, property);
			}
		};
		restoreProperty(
			globalThis,
			"MessageChannel",
			originalMessageChannelDescriptor,
		);
		restoreProperty(globalThis, "Worker", originalWorkerDescriptor);
		restoreProperty(
			globalThis,
			"OffscreenCanvas",
			originalOffscreenCanvasDescriptor,
		);
		restoreProperty(
			globalThis,
			"createImageBitmap",
			originalCreateImageBitmapDescriptor,
		);
		restoreProperty(
			HTMLCanvasElement.prototype,
			"transferControlToOffscreen",
			originalTransferDescriptor,
		);
	});

	it("posts one viewport and coalesces rapid revisions until its frame arrives", async () => {
		const harness = await createClientHarness();
		try {
			const firstPointer: RenderPointerState = { x: 10, y: 20, inside: true };
			const pendingPointer: RenderPointerState = { x: 30, y: 40, inside: true };
			harness.client.sendViewportState(
				createViewportState(1),
				DirtyCanvas.Viewport,
				firstPointer,
			);
			harness.client.sendViewportState(
				createViewportState(2),
				DirtyCanvas.Size,
			);
			harness.client.sendPointerNow(pendingPointer, 2);
			harness.client.sendViewportState(
				createViewportState(3),
				DirtyCanvas.Theme,
			);

			await waitForCondition(
				() =>
					harness.controlMessages.filter(
						(message) => message.type === "VIEWPORT_STATE",
					).length === 1,
			);
			/**
			 * Returns captured viewport controls in post order.
			 * @returns Posted viewport messages.
			 */
			const getViewportMessages = (): RenderViewportStateMessage[] =>
				harness.controlMessages.filter(
					(message): message is RenderViewportStateMessage =>
						message.type === "VIEWPORT_STATE",
				);
			expect(harness.client.requestedViewportRevision).toBe(3);
			expect(getViewportMessages()[0]).toMatchObject({
				sequence: 2,
				payload: {
					state: { viewportRevision: 1 },
					dirty: DirtyCanvas.Viewport,
					pointer: firstPointer,
				},
			});
			expect(
				harness.controlMessages.some((message) => message.type === "POINTER"),
			).toBe(false);

			harness.workerControlPort.postMessage(
				createFrameRenderedResponse(1, 0, 2),
			);
			harness.workerControlPort.postMessage(
				createFrameRenderedResponse(2, 99, 3),
			);
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			expect(getViewportMessages()).toHaveLength(1);
			expect(harness.client.lastViewportRevision).toBe(0);

			harness.workerControlPort.postMessage(
				createFrameRenderedResponse(1, 1, 4),
			);
			await waitForCondition(() => getViewportMessages().length === 2);
			expect(getViewportMessages()[1]).toMatchObject({
				sequence: 3,
				payload: {
					state: { viewportRevision: 3 },
					dirty: DirtyCanvas.Size | DirtyCanvas.Theme,
					pointer: pendingPointer,
				},
			});

			harness.client.sendViewportState(
				createViewportState(4),
				DirtyCanvas.Assets,
			);
			const olderDataFrame = createFrameRenderedResponse(1, 1, 5);
			olderDataFrame.payload.dirtyMask = DirtyCanvas.Events;
			harness.workerControlPort.postMessage(olderDataFrame);
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			expect(getViewportMessages()).toHaveLength(2);
			harness.client.resetOperation(2, "viewport-test-reset");
			harness.client.sendViewportState(
				createViewportState(1),
				DirtyCanvas.Viewport,
				{ x: 5, y: 6, inside: true },
			);
			await waitForCondition(() => harness.controlMessages.length === 4);
			expect(harness.controlMessages.map((message) => message.type)).toEqual([
				"VIEWPORT_STATE",
				"VIEWPORT_STATE",
				"RESET_OPERATION",
				"VIEWPORT_STATE",
			]);
			expect(harness.controlMessages.map((message) => message.sequence)).toEqual([
				2, 3, 4, 5,
			]);
			expect(getViewportMessages()[2]).toMatchObject({
				operationEpoch: 2,
				sequence: 5,
				payload: { state: { viewportRevision: 1 } },
			});
		} finally {
			harness.dispose();
		}
	});

	it("rejects interaction results rendered for a different viewport revision", async () => {
		const harness = await createClientHarness();
		try {
			harness.client.sendViewportState(
				createViewportState(1),
				DirtyCanvas.Viewport,
			);
			harness.client.sendViewportState(
				createViewportState(2),
				DirtyCanvas.Viewport,
			);
			const hitPromise = harness.client.hitTest(10, 20, 2);
			await waitForCondition(() =>
				harness.controlMessages.some((message) => message.type === "HIT_TEST"),
			);
			const hitRequest = harness.controlMessages.find(
				(message): message is RenderHitTestMessage => message.type === "HIT_TEST",
			);
			if (!hitRequest) throw new Error("Hit-test request was not captured");
			const mismatchedHit: RenderHitTestResult = { kind: "link", id: "link-a" };
			harness.workerControlPort.postMessage({
				protocolVersion: RENDER_PROTOCOL_VERSION,
				operationEpoch: 1,
				sequence: 2,
				type: "HIT_TEST_RESULT",
				payload: {
					requestId: hitRequest.payload.requestId,
					viewportRevision: 3,
					hit: mismatchedHit,
				},
			} satisfies RenderWorkerResponse);
			expect(await hitPromise).toBeNull();

			const magnifierPromise = harness.client.requestMagnifier({
				x: 10,
				y: 20,
				cropWidth: 20,
				cropHeight: 20,
				outputWidth: 40,
				outputHeight: 40,
				viewportRevision: 2,
			});
			await waitForCondition(() =>
				harness.controlMessages.some(
					(message) => message.type === "MAGNIFIER_REQUEST",
				),
			);
			const magnifierRequest = harness.controlMessages.find(
				(message): message is RenderMagnifierRequestMessage =>
					message.type === "MAGNIFIER_REQUEST",
			);
			if (!magnifierRequest) {
				throw new Error("Magnifier request was not captured");
			}
			const closeBitmap = jest.fn();
			const bitmap = { close: closeBitmap } as unknown as ImageBitmap;
			harness.worker.dispatchEvent(
				new MessageEvent("message", {
					data: {
						protocolVersion: RENDER_PROTOCOL_VERSION,
						operationEpoch: 1,
						sequence: 3,
						type: "MAGNIFIER_FRAME",
						payload: {
							requestId: magnifierRequest.payload.requestId,
							viewportRevision: 3,
							bitmap,
						},
					} satisfies RenderWorkerResponse,
				}),
			);
			expect(await magnifierPromise).toBeNull();
			expect(closeBitmap).toHaveBeenCalledTimes(1);
		} finally {
			harness.dispose();
		}
	});
});
