import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { RenderEngine } from "@/class/RenderEngine";

const mockWarningBitmap = {
	id: "warning-bitmap",
} as unknown as HTMLCanvasElement;

jest.mock("@/class/Info", () => ({ Info: class {} }));
jest.mock("@/class/Ruler.drawer", () => ({
	RulerDrawer: class {
		public draw = jest.fn();
		public sections = jest.fn();
	},
}));
jest.mock("@/engines/Default.engine", () => ({
	DefaultEngine: class {
		public updateRenderer = jest.fn();
	},
}));
jest.mock("@/engines/Height.engine", () => ({
	HeightEngine: class {
		public updateRenderer = jest.fn();
	},
}));
jest.mock("@/engines/Graph.engine", () => ({
	GraphEngine: class {
		public updateRenderer = jest.fn();
	},
}));
jest.mock("@/ui/CanvasIcon", () => ({
	getCanvasIcon: (): HTMLCanvasElement => mockWarningBitmap,
	CanvasIcon: jest.fn(),
}));
jest.mock("@/dto/Logger.class", () => ({ Logger: { error: jest.fn() } }));
jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			getHeight: (_app: unknown, _source: unknown, scrollY: number, index: number) =>
				index * 48 + 24 - scrollY,
			getRequestType: () => "query",
			context: () => ({ name: "Context", color: "#000000" }),
		},
	},
}));
jest.mock("@/entities/Doc", () => ({ Doc: { Entity: {} } }));
jest.mock("@/entities/Link", () => ({ Link: { Entity: {} } }));
jest.mock("@/entities/Note", () => ({ Note: { Entity: {} } }));
jest.mock("@/entities/Glyph", () => ({ Glyph: {} }));
jest.mock("@/entities/Request", () => ({
	Request: { Prefix: { INGESTION: "ingestion", QUERY: "query" } },
}));
jest.mock("@/entities/addon/Internal", () => ({ Internal: { Settings: {} } }));
jest.mock("@/entities/Color", () => ({
	Color: {
		Themer: {
			theme: {
				BORDER: "#444444",
				FONT_ACCENT: "#ffffff",
				FONT_SECOND: "#cccccc",
			},
		},
	},
}));
jest.mock("@/entities/Operation", () => ({ Operation: { Entity: {} } }));
jest.mock("@/store/DataStore", () => ({ DataStore: { links: [] } }));
jest.mock("@/entities/Highlight", () => ({ Highlight: { Entity: {} } }));
jest.mock("@/entities/Context", () => ({ Context: { Entity: {} } }));
jest.mock("@/ui/utils", () => ({ stringToHexColor: () => "#000000" }));
jest.mock("@/locales/core", () => ({
	translate: (key: string): string =>
		key === "renderEngine.loadingSuffix" ? " loading" : " ingesting",
}));
jest.mock("@/store/request.store", () => ({
	requestStore: { hasLoadingForFile: () => false },
}));
jest.mock("@/timeZone", () => ({ formatTimestampInTimeZone: () => "" }));

/** Creates the recording context required by the legacy warning pass. */
function createContext(): CanvasRenderingContext2D {
	return {
		canvas: { width: 1_000, height: 96 } as HTMLCanvasElement,
		font: "10px monospace",
		drawImage: jest.fn(),
		measureText: jest.fn(
			(text: string) => ({ width: text.length * 6 }) as TextMetrics,
		),
	} as unknown as CanvasRenderingContext2D;
}

describe("RenderEngine cache warnings", () => {
	it("uses the shared label geometry and registers a 14-pixel legacy hit region", () => {
		const source = {
			id: "source-a",
			name: "cache.json",
			context_id: "context-a",
			settings: { timeline_label: "filename" },
			appliedQuery: {
				string: "cached",
				filters: [],
				source_config: {
					operation_id: "operation-a",
					source_ids: ["source-a"],
					range: { min: 0, max: 1 },
				},
			},
			queryCache: {
				...createEmptyQueryCacheState(),
				activeGeneration: 1,
				freshness: "source-changed",
				warningReason: "source-changed",
			},
		} as unknown as Source.Type;
		const context = createContext();
		const renderer = new RenderEngine({
			ctx: context,
			limits: { min: 0, max: 1 },
			info: {
				app: { timeline: { scale: 1, frame: { min: 0, max: 1 } } },
				width: 1_000,
			} as never,
			scrollX: 0,
			scrollY: 0,
			getPixelPosition: () => 0,
			visibleSources: [source],
		});

		renderer.drawCacheWarnings([source]);

		expect(context.drawImage).toHaveBeenCalledWith(
			mockWarningBitmap,
			123,
			14,
			14,
			14,
		);
		expect(RenderEngine.interactiveCacheWarnings).toEqual([
			{
				sourceId: "source-a",
				rect: { x: 123, y: 14, w: 14, h: 14 },
			},
		]);
	});
});
