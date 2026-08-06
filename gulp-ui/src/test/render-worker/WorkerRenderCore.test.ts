import { MessageChannel as NodeMessageChannel } from "worker_threads";
import {
	DirtyCanvas,
	type RenderFrameState,
	type RenderSource,
} from "@/render-worker/RenderWorker.types";
import { WorkerRenderCore } from "@/render-worker/WorkerRenderCore";
import { WorkerRenderRepository } from "@/render-worker/WorkerRenderRepository";

const originalMessageChannel = globalThis.MessageChannel;
const recordedRasterUploads: Uint8ClampedArray[] = [];

/** Minimal OffscreenCanvas implementation that records worker raster uploads. */
class RecordingRasterOffscreenCanvas {
	/** Raster backing width. */
	public readonly width: number;
	/** Raster backing height. */
	public readonly height: number;
	private readonly context: OffscreenCanvasRenderingContext2D;

	/**
	 * Creates a deterministic RGBA raster surface.
	 * @param width Requested raster width.
	 * @param height Requested raster height.
	 */
	public constructor(width: number, height: number) {
		this.width = width;
		this.height = height;
		this.context = {
			canvas: this,
			imageSmoothingEnabled: true,
			createImageData: jest.fn(
				(imageWidth: number, imageHeight: number) =>
					({
						data: new Uint8ClampedArray(imageWidth * imageHeight * 4),
						width: imageWidth,
						height: imageHeight,
						colorSpace: "srgb",
					}) as ImageData,
			),
			putImageData: jest.fn((imageData: ImageData) => {
				recordedRasterUploads.push(new Uint8ClampedArray(imageData.data));
			}),
		} as unknown as OffscreenCanvasRenderingContext2D;
	}

	/**
	 * Returns the recording context for 2D requests.
	 * @param contextId Requested rendering context kind.
	 * @returns Recording context, or null for unsupported context kinds.
	 */
	public getContext(
		contextId: string,
	): OffscreenCanvasRenderingContext2D | null {
		return contextId === "2d" ? this.context : null;
	}
}

/**
 * Installs the raster-recording OffscreenCanvas fixture for one test.
 * @returns Cleanup callback that restores the previous global constructor.
 */
function installRecordingRasterOffscreenCanvas(): () => void {
	const originalDescriptor = Object.getOwnPropertyDescriptor(
		globalThis,
		"OffscreenCanvas",
	);
	recordedRasterUploads.length = 0;
	Object.defineProperty(globalThis, "OffscreenCanvas", {
		configurable: true,
		writable: true,
		value: RecordingRasterOffscreenCanvas,
	});
	return () => {
		if (originalDescriptor) {
			Object.defineProperty(globalThis, "OffscreenCanvas", originalDescriptor);
		} else {
			Reflect.deleteProperty(globalThis, "OffscreenCanvas");
		}
	};
}

/**
 * Creates a no-op canvas context that records the drawing calls relevant to tests.
 * @param width Initial canvas backing width.
 * @param height Initial canvas backing height.
 * @param recordedText Optional sink for canvas text labels.
 * @returns Canvas-like 2D context accepted by the worker-safe render core.
 */
function createRecordingContext(
	width = 1,
	height = 1,
	recordedText?: string[],
): CanvasRenderingContext2D {
	const canvas = { width, height } as HTMLCanvasElement;
	const gradient = {
		addColorStop: jest.fn<void, [number, string]>(),
	} as unknown as CanvasGradient;
	return {
		canvas,
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
		createLinearGradient: jest.fn(() => gradient),
		drawImage: jest.fn(),
		fill: jest.fn(),
		fillRect: jest.fn(),
		fillText: jest.fn((label: string) => {
			recordedText?.push(label);
		}),
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
	} as unknown as CanvasRenderingContext2D;
}

/**
 * Creates one row of deterministic render-only source metadata.
 * @param id Stable source ID.
 * @param rowOrder Source order used by vertical culling.
 * @returns Complete source fixture.
 */
function createRenderSource(id: string, rowOrder: number): RenderSource {
	return {
		id,
		revision: 1,
		rowOrder,
		name: id,
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
 * Creates a complete frame state with a predictable timeline transform.
 * @param sourceOrder Ordered source IDs represented by the frame.
 * @param overrides Frame fields to replace in the default fixture.
 * @returns Complete render frame fixture.
 */
function createFrameState(
	sourceOrder: string[],
	overrides: Partial<RenderFrameState> = {},
): RenderFrameState {
	return {
		width: 1_000,
		height: 96,
		frame: { min: 0, max: 1_000 },
		scale: 1,
		scrollX: 0,
		scrollY: 0,
		viewportRevision: 1,
		inputSequence: 1,
		inputTimestamp: 0,
		sourceOrder,
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
		},
		revisions: {
			settings: 1,
			sources: 1,
			overlays: 1,
			theme: 1,
			assets: 1,
		},
		...overrides,
	};
}

describe("WorkerRenderCore", () => {
	let repository: WorkerRenderRepository;

	beforeAll(() => {
		globalThis.MessageChannel =
			NodeMessageChannel as unknown as typeof MessageChannel;
	});

	afterAll(() => {
		globalThis.MessageChannel = originalMessageChannel;
	});

	beforeEach(() => {
		repository = new WorkerRenderRepository();
	});

	afterEach(() => {
		repository.dispose();
	});

	it("preserves the 48-pixel vertical culling buffer", async () => {
		const sources = Array.from({ length: 6 }, (_, index) =>
			createRenderSource(`source-${index}`, index),
		);
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: sources, removedSourceIds: [] },
		});
		repository.setFrameState(
			createFrameState(
				sources.map((source) => source.id),
				{ height: 48 },
			),
		);
		const renderCore = new WorkerRenderCore(
			createRecordingContext(),
			repository,
			{ requestAsset: jest.fn() },
		);

		const statistics = renderCore.render(100, DirtyCanvas.Viewport);

		expect(statistics?.visibleSources).toBe(6);
		expect(statistics?.renderedRows).toBe(2);
		expect(statistics?.dirtyMask).toBe(DirtyCanvas.Viewport);
	});

	it("treats an explicitly empty source order as authoritative", async () => {
		const source = createRenderSource("source-retained", 0);
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
		});
		repository.setFrameState(createFrameState([]));
		const renderCore = new WorkerRenderCore(
			createRecordingContext(),
			repository,
			{ requestAsset: jest.fn() },
		);

		const statistics = renderCore.render(100, DirtyCanvas.Sources);

		expect(statistics?.visibleSources).toBe(0);
		expect(statistics?.renderedRows).toBe(0);
	});

	it("renders the projected source label", async () => {
		const source = {
			...createRenderSource("source-path", 0),
			name: "events.json",
		};
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
		});
		repository.setFrameState(createFrameState([source.id]));
		const recordedText: string[] = [];
		const renderCore = new WorkerRenderCore(
			createRecordingContext(1, 1, recordedText),
			repository,
			{ requestAsset: jest.fn() },
		);

		renderCore.render(100, DirtyCanvas.Sources);

		expect(recordedText.includes("events.json")).toBe(true);
		expect(recordedText.includes(source.name)).toBe(true);
	});

	it("renders default-engine events shifted into the visible frame by source offset", async () => {
		const rawEventTimestamp = 1_743_025_550_367;
		const offset = -124_900_000_000;
		const shiftedTimestamp = rawEventTimestamp + offset;
		const source = {
			...createRenderSource("source-offset", 0),
			timestamp: {
				min: rawEventTimestamp - 1_000,
				max: rawEventTimestamp + 1_000,
			},
			offset,
			total: 1,
		};
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1),
					timestamps: Float64Array.of(rawEventTimestamp),
					hashes: Float64Array.of(1),
				},
			],
		});
		repository.setFrameState(
			createFrameState([source.id], {
				frame: {
					min: shiftedTimestamp - 10_000,
					max: shiftedTimestamp + 10_000,
				},
				width: 400,
			}),
		);
		const renderCore = new WorkerRenderCore(
			createRecordingContext(400, 96),
			repository,
			{ requestAsset: jest.fn() },
		);

		const statistics = renderCore.render(100, DirtyCanvas.Events);

		expect(statistics?.renderedRows).toBe(1);
		expect(statistics?.renderedEventPixels).toBe(1);
	});

	it("renders graph-engine samples shifted into the visible frame by source offset", async () => {
		const rawEventTimestamp = 1_743_025_550_367;
		const offset = -124_900_000_000;
		const shiftedTimestamp = rawEventTimestamp + offset;
		const source = {
			...createRenderSource("source-graph-offset", 0),
			engine: "graph" as const,
			frequency: 1_000,
			timestamp: {
				min: rawEventTimestamp - 1_000,
				max: rawEventTimestamp + 1_000,
			},
			offset,
			total: 1,
		};
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1),
					timestamps: Float64Array.of(rawEventTimestamp),
					hashes: Float64Array.of(1),
					sampleTimestamps: Float64Array.of(rawEventTimestamp),
					sampleCounts: Float64Array.of(1),
				},
			],
		});
		repository.setFrameState(
			createFrameState([source.id], {
				frame: {
					min: shiftedTimestamp - 10_000,
					max: shiftedTimestamp + 10_000,
				},
				width: 400,
			}),
		);
		const renderCore = new WorkerRenderCore(
			createRecordingContext(400, 96),
			repository,
			{ requestAsset: jest.fn() },
		);

		const statistics = renderCore.render(100, DirtyCanvas.Events);

		expect(statistics?.renderedRows).toBe(1);
		expect(statistics?.graphBuckets).toBe(1);
	});

	it("renders and hit-tests notes shifted into the visible frame by source offset", async () => {
		const rawEventTimestamp = 1_743_025_550_367;
		const offset = -124_900_000_000;
		const shiftedTimestamp = rawEventTimestamp + offset;
		const source = {
			...createRenderSource("source-note-offset", 0),
			timestamp: {
				min: rawEventTimestamp - 1_000,
				max: rawEventTimestamp + 1_000,
			},
			offset,
		};
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			overlays: {
				notes: {
					revision: 1,
					records: [
						{
							id: "note-offset",
							sourceId: source.id,
							timestamp: rawEventTimestamp,
							name: "N",
							color: "#ffaa00",
							iconKey: "note",
						},
					],
				},
			},
		});
		repository.setFrameState(
			createFrameState([source.id], {
				frame: {
					min: shiftedTimestamp - 10_000,
					max: shiftedTimestamp + 10_000,
				},
				width: 400,
			}),
		);
		const renderCore = new WorkerRenderCore(
			createRecordingContext(400, 96),
			repository,
			{ requestAsset: jest.fn() },
		);

		const statistics = renderCore.render(100, DirtyCanvas.Overlays);
		const hit = repository.hitTest(200, 24);

		expect(statistics?.visibleNotes).toBe(1);
		expect(hit).toEqual({
			kind: "note",
			id: "note-offset",
			noteIds: ["note-offset"],
		});
	});

	it("renders and hit-tests links shifted into the visible frame by source offset", async () => {
		const rawEventTimestamp = 1_743_025_550_367;
		const offset = -124_900_000_000;
		const shiftedTimestamp = rawEventTimestamp + offset;
		const source = {
			...createRenderSource("source-link-offset", 0),
			timestamp: {
				min: rawEventTimestamp - 1_000,
				max: rawEventTimestamp + 1_000,
			},
			offset,
		};
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			overlays: {
				links: {
					revision: 1,
					records: [
						{
							id: "link-offset",
							color: "#00aaff",
							iconKey: "link",
							endpoints: [
								{
									sourceId: source.id,
									timestamp: rawEventTimestamp,
									sortKey: "doc-offset",
								},
							],
						},
					],
				},
			},
		});
		repository.setFrameState(
			createFrameState([source.id], {
				frame: {
					min: shiftedTimestamp - 10_000,
					max: shiftedTimestamp + 10_000,
				},
				width: 400,
			}),
		);
		const renderCore = new WorkerRenderCore(
			createRecordingContext(400, 96),
			repository,
			{ requestAsset: jest.fn() },
		);

		const statistics = renderCore.render(100, DirtyCanvas.Overlays);
		const hit = repository.hitTest(200, 20);

		expect(statistics?.visibleLinks).toBe(1);
		expect(hit).toEqual({ kind: "link", id: "link-offset" });
	});

	it("rebuilds graph buckets after a same-revision event-view replacement", async () => {
		const source = {
			...createRenderSource("source-graph", 0),
			engine: "graph" as const,
			frequency: 100,
		};
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1, 2),
					timestamps: Float64Array.of(150, 50),
					hashes: Float64Array.of(1, 2),
					sampleTimestamps: Float64Array.of(0, 100),
					sampleCounts: Float64Array.of(1, 1),
				},
			],
		});
		repository.setFrameState(createFrameState([source.id]));
		const renderCore = new WorkerRenderCore(
			createRecordingContext(),
			repository,
			{ requestAsset: jest.fn() },
		);

		const initialStatistics = renderCore.render(100, DirtyCanvas.Events);
		await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1, 2),
					timestamps: Float64Array.of(150, 50),
					hashes: Float64Array.of(1, 2),
					sampleTimestamps: Float64Array.of(0, 0),
					sampleCounts: Float64Array.of(5, 5),
				},
			],
		});
		const replacedStatistics = renderCore.render(101, DirtyCanvas.Events);

		expect(initialStatistics?.graphBuckets).toBe(2);
		expect(replacedStatistics?.graphBuckets).toBe(1);
	});

	it("aggregates exact graph counts only for visible buckets and invalidates suffix caches", async () => {
		const sampleCount = 1_000;
		const source = {
			...createRenderSource("source-graph-range", 0),
			engine: "graph" as const,
			frequency: 100,
			timestamp: { min: 0, max: 10_000 },
		};
		const sampleTimestamps = Float64Array.from(
			{ length: sampleCount },
			(_, index) => index * 10,
		);
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1),
					timestamps: Float64Array.of(1),
					hashes: Float64Array.of(1),
					sampleTimestamps,
					sampleCounts: Float64Array.from(
						{ length: sampleCount },
						(_, index) => index + 1,
					),
				},
			],
		});
		repository.setFrameState(
			createFrameState([source.id], {
				frame: { min: 0, max: 10_000 },
				scale: 10,
			}),
		);
		const recordedText: string[] = [];
		const context = createRecordingContext(1, 1, recordedText);
		const renderCore = new WorkerRenderCore(context, repository, {
			requestAsset: jest.fn(),
		});

		const initialStatistics = renderCore.render(100, DirtyCanvas.Events);
		expect(initialStatistics?.graphSamples).toBe(sampleCount);
		expect(initialStatistics?.graphBuckets).toBe(12);
		expect(recordedText.includes("55")).toBe(true);

		await repository.applyDataPatch({
			globalRevision: 2,
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1),
					timestamps: Float64Array.of(1),
					hashes: Float64Array.of(1),
					sampleTimestamps: Float64Array.from(sampleTimestamps),
					sampleCounts: new Float64Array(sampleCount).fill(2),
				},
			],
		});
		recordedText.length = 0;
		const replacedStatistics = renderCore.render(101, DirtyCanvas.Events);

		expect(replacedStatistics?.graphBuckets).toBe(12);
		expect(recordedText.includes("20")).toBe(true);
		expect(recordedText.includes("55")).toBe(false);
	});

	it("maps zoomed timeline timestamps to hit-test pixels and groups nearby notes", async () => {
		const source = createRenderSource("source-a", 0);
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
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
						{
							id: "note-b",
							sourceId: source.id,
							timestamp: 505,
							name: "B",
							color: "#ffaa00",
							iconKey: "note",
						},
					],
				},
			},
		});
		repository.setFrameState(
			createFrameState([source.id], {
				scale: 2,
				scrollX: 500,
				viewportRevision: 7,
			}),
		);
		const requestAsset = jest.fn<void, [string, string]>();
		const renderCore = new WorkerRenderCore(
			createRecordingContext(),
			repository,
			{ requestAsset },
		);
		const dirtyMask = DirtyCanvas.Viewport | DirtyCanvas.Overlays;

		const statistics = renderCore.render(200, dirtyMask);
		const hit = repository.hitTest(505, 24);

		expect(statistics?.visibleNotes).toBe(2);
		expect(statistics?.interactiveRegions).toBe(1);
		expect(statistics?.dirtyMask).toBe(dirtyMask);
		expect(renderCore.getRenderedViewportRevision()).toBe(7);
		expect(hit).toEqual({
			kind: "note",
			id: "note-b",
			noteIds: ["note-b", "note-a"],
		});
		expect(requestAsset).toHaveBeenCalledTimes(1);
		renderCore.render(201, DirtyCanvas.Force);
		expect(requestAsset).toHaveBeenCalledTimes(1);

		renderCore.notifyAssetUnavailable("note", "#ffffff");
		renderCore.render(202, DirtyCanvas.Force);
		expect(requestAsset).toHaveBeenCalledTimes(2);
	});

	it("reuses parsed gradient values across unchanged frames", async () => {
		const source = createRenderSource("source-color-cache", 0);
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			events: [
				{
					sourceId: source.id,
					baseRevision: 0,
					revision: 1,
					mode: "replace",
					handles: Uint32Array.of(1, 2, 3),
					timestamps: Float64Array.of(900, 500, 100),
					hashes: Float64Array.of(2, 1, 0),
				},
			],
		});
		repository.setFrameState(createFrameState([source.id], { width: 20 }));
		const renderCore = new WorkerRenderCore(
			createRecordingContext(),
			repository,
			{ requestAsset: jest.fn() },
		);
		const originalParseInteger = Number.parseInt;
		let parseCount = 0;
		Number.parseInt = (value: string, radix?: number): number => {
			parseCount++;
			return originalParseInteger(value, radix);
		};
		try {
			renderCore.render(100, DirtyCanvas.Events);
			const parsesAfterFirstFrame = parseCount;

			renderCore.render(101, DirtyCanvas.Viewport);

			expect(parsesAfterFirstFrame === 0).toBe(false);
			expect(parseCount).toBe(parsesAfterFirstFrame);
		} finally {
			Number.parseInt = originalParseInteger;
		}
	});

	it("uploads one height raster while preserving same-pixel bucket layering", async () => {
		const restoreOffscreenCanvas = installRecordingRasterOffscreenCanvas();
		try {
			const source = {
				...createRenderSource("source-height-raster", 0),
				engine: "height" as const,
				timestamp: { min: 0, max: 180_000 },
				total: 4,
			};
			await repository.applyDataPatch({
				globalRevision: 1,
				sources: { revision: 1, records: [source], removedSourceIds: [] },
				events: [
					{
						sourceId: source.id,
						baseRevision: 0,
						revision: 1,
						mode: "replace",
						handles: Uint32Array.of(4, 3, 2, 1),
						timestamps: Float64Array.of(60_001, 3, 2, 1),
						hashes: Float64Array.of(0, 0, 0, 0),
					},
				],
			});
			repository.setFrameState(
				createFrameState([source.id], {
					width: 1,
					frame: { min: 0, max: 180_000 },
				}),
			);
			const context = createRecordingContext();
			const renderCore = new WorkerRenderCore(context, repository, {
				requestAsset: jest.fn(),
			});

			renderCore.render(100, DirtyCanvas.Events);

			const fillRectCalls = (
				context.fillRect as unknown as {
					mock: { calls: Array<[number, number, number, number]> };
				}
			).mock.calls;
			const negativeHeightDraws = fillRectCalls.filter(
				([, , , height]) => height < 0,
			);
			expect(negativeHeightDraws).toHaveLength(0);
			const drawImageCalls = (
				context.drawImage as unknown as { mock: { calls: unknown[][] } }
			).mock.calls;
			expect(drawImageCalls).toHaveLength(1);
			expect(recordedRasterUploads).toHaveLength(1);
			const pixels = recordedRasterUploads[0];
			expect(Array.from(pixels.slice(0, 4))).toEqual([255, 255, 255, 255]);
			const bottomOffset = 46 * 4;
			expect(Array.from(pixels.slice(bottomOffset, bottomOffset + 4))).toEqual([
				85,
				85,
				85,
				255,
			]);
		} finally {
			restoreOffscreenCanvas();
		}
	});

	it("recomputes highlight depth only when its overlay revision changes", async () => {
		const source = createRenderSource("source-highlight-cache", 0);
		const highlights = [
			{ id: "highlight-a", start: 100, end: 600, color: "#ff0000" },
			{ id: "highlight-b", start: 300, end: 800, color: "#00ff00" },
		];
		await repository.applyDataPatch({
			globalRevision: 1,
			sources: { revision: 1, records: [source], removedSourceIds: [] },
			overlays: { highlights: { revision: 1, records: highlights } },
		});
		repository.setFrameState(createFrameState([source.id]));
		const renderCore = new WorkerRenderCore(
			createRecordingContext(),
			repository,
			{ requestAsset: jest.fn() },
		);
		const renderCoreMethods = renderCore as unknown as {
				computeHighlightDepths: (
					records: typeof highlights,
				) => number[];
			};
		const originalComputeDepths =
			renderCoreMethods.computeHighlightDepths.bind(renderCore);
		let computeDepthCount = 0;
		renderCoreMethods.computeHighlightDepths = (records): number[] => {
			computeDepthCount++;
			return originalComputeDepths(records);
		};

		renderCore.render(100, DirtyCanvas.Overlays);
		renderCore.render(101, DirtyCanvas.Viewport);
		expect(computeDepthCount).toBe(1);

		await repository.applyDataPatch({
			globalRevision: 2,
			overlays: {
				highlights: {
					revision: 2,
					records: [...highlights, { id: "highlight-c", start: 900, end: 950, color: "blue" }],
				},
			},
		});
		repository.setFrameState(
			createFrameState([source.id], {
				revisions: {
					settings: 1,
					sources: 1,
					overlays: 2,
					theme: 1,
					assets: 1,
				},
			}),
		);
		renderCore.render(102, DirtyCanvas.Overlays);

		expect(computeDepthCount).toBe(2);
	});

	it("preserves the legacy magnifier crop at the right and bottom edges", () => {
		const sourceContext = createRecordingContext(100, 80);
		const outputBitmap = {} as ImageBitmap;
		const drawImage = jest.fn<
			void,
			[
				CanvasImageSource,
				number,
				number,
				number,
				number,
				number,
				number,
				number,
				number,
			]
		>();
		const magnifierContext = {
			clearRect: jest.fn(),
			drawImage,
			imageSmoothingEnabled: true,
		} as unknown as OffscreenCanvasRenderingContext2D;
		const originalDescriptor = Object.getOwnPropertyDescriptor(
			globalThis,
			"OffscreenCanvas",
		);

		class RecordingOffscreenCanvas {
			/** Output backing width. */
			public readonly width: number;
			/** Output backing height. */
			public readonly height: number;

			/**
			 * Creates the deterministic magnifier scratch surface.
			 * @param width Requested output width.
			 * @param height Requested output height.
			 */
			public constructor(width: number, height: number) {
				this.width = width;
				this.height = height;
			}

			/**
			 * Returns the recording 2D context.
			 * @param contextId Requested rendering context kind.
			 * @returns Recording context for 2D requests, otherwise null.
			 */
			public getContext(
				contextId: string,
			): OffscreenCanvasRenderingContext2D | null {
				return contextId === "2d" ? magnifierContext : null;
			}

			/**
			 * Returns the deterministic transferable frame fixture.
			 * @returns Magnifier bitmap fixture.
			 */
			public transferToImageBitmap(): ImageBitmap {
				return outputBitmap;
			}
		}

		Object.defineProperty(globalThis, "OffscreenCanvas", {
			configurable: true,
			writable: true,
			value: RecordingOffscreenCanvas,
		});
		try {
			const renderCore = new WorkerRenderCore(sourceContext, repository, {
				requestAsset: jest.fn(),
			});

			const bitmap = renderCore.createMagnifierBitmap({
				x: 95,
				y: 75,
				cropWidth: 20,
				cropHeight: 20,
				outputWidth: 200,
				outputHeight: 200,
			});

			expect(bitmap).toBe(outputBitmap);
			expect(drawImage.mock.calls[0].slice(1, 5)).toEqual([85, 65, 20, 20]);
			expect(drawImage.mock.calls[0].slice(5)).toEqual([0, 0, 200, 200]);
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
});
