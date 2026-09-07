import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import { Source } from "@/entities/Source";
import type { EventSourceSummary } from "@/event-store/EventStore.types";
import { DataStore } from "@/store/DataStore";

jest.mock("@/entities/addon/Parser", () => ({
	Parser: {
		use: (value: App.Type | Source.Type[]): Source.Type[] =>
			Array.isArray(value) ? value : value.target.files,
		useUUID: (value: Source.Type | string): string =>
			typeof value === "string" ? value : value.id,
	},
}));
jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		IconExtractor: { activate: () => (): null => null },
	},
}));
jest.mock("@/entities/Context", () => ({
	Context: {
		Entity: {
			id: (app: App.Type, contextId: Context.Id): Context.Type | undefined =>
				app.target.contexts.find((context) => context.id === contextId),
		},
	},
}));
jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {
			sourceId: (note: { source_id?: Source.Id }): Source.Id | undefined =>
				note.source_id,
		},
	},
}));
jest.mock("@/context/Application.context", () => ({
	Application: { use: jest.fn() },
}));
jest.mock("@/banners/FilterFile.banner", () => ({
	FilterFileBanner: (): null => null,
}));
jest.mock("@/ui/utils", () => ({
	generateUUID: jest.fn(),
	HASH_FUNCTIONS: [],
	Refractor: { any: { toNumber: (): number => 0 } },
}));
jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);

const OPERATION_ID = "source-visibility-operation" as Operation.Id;
const CONTEXT_ID = "source-visibility-context" as Context.Id;
const SOURCE_ID = "source-visibility-source" as Source.Id;

/** Creates the source fields consumed by timeline membership selection. */
function createSource(): Source.Type {
	return {
		id: SOURCE_ID,
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		name: "Progressive LOD source",
		selected: true,
		pinned: false,
		total: 32_768,
	} as Source.Type;
}

/** Creates a stable application snapshot for source-membership assertions. */
function createApp(source: Source.Type): App.Type {
	return {
		target: {
			operations: [],
			files: [source],
			contexts: [],
			events: new Map(),
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		general: {
			server: "",
			ws_id: "",
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: {
				byRequestId: new Map(),
				byFileId: new Map(),
			},
			tableViewSource: null,
		},
		timeline: {
			scale: 1,
			frame: { min: 0, max: 1 },
			target: null,
			filter: "",
			isScrollReversed: false,
			dialogSize: 320,
			pointers: [],
			renderVersion: 0,
		},
		settings: {},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: true,
			toasts: false,
		},
	};
}

/** Creates one compact building summary for the active source generation. */
function createBuildingSummary(
	overrides: Partial<EventSourceSummary> = {},
): EventSourceSummary {
	return {
		sourceId: SOURCE_ID,
		generation: 1,
		revision: 1,
		authorityRevision: 1,
		status: "building",
		eventCount: 0,
		minTimestamp: 0,
		maxTimestamp: 0,
		hashMin: 0,
		hashMax: 0,
		activeCacheGeneration: null,
		cacheHistory: [],
		cacheHistoryCursor: -1,
		nextGeneration: 2,
		...overrides,
	};
}

describe("Source progressive LOD visibility", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation(OPERATION_ID);
	});

	it("invalidates hidden-source membership on the first positive building summary", () => {
		const source = createSource();
		const app = createApp(source);
		const filesReference = app.target.files;
		const renderVersion = app.timeline.renderVersion;

		DataStore.publishEventSourceSummary(createBuildingSummary(), "immediate");
		expect(Source.Entity.rendered(app)).toEqual([]);

		DataStore.publishEventSourceSummary(
			createBuildingSummary({
				revision: 2,
				eventCount: 16_384,
				minTimestamp: 1_700_000_000_000,
				maxTimestamp: 1_700_000_001_000,
			}),
			"immediate",
		);

		expect(app.target.files).toBe(filesReference);
		expect(app.timeline.renderVersion).toBe(renderVersion);
		expect(Source.Entity.rendered(app).map(({ id }) => id)).toEqual([SOURCE_ID]);
	});

	it("shows a source as soon as its transient replacement page is available", () => {
		const source = createSource();
		const app = createApp(source);

		DataStore.publishEventSourceSummary(createBuildingSummary(), "immediate");
		expect(Source.Entity.rendered(app)).toEqual([]);

		DataStore.markTransientEventSources([SOURCE_ID]);

		expect(Source.Entity.rendered(app).map(({ id }) => id)).toEqual([SOURCE_ID]);

		DataStore.publishEventSourceSummary(
			createBuildingSummary({ revision: 2, status: "complete" }),
			"immediate",
		);
		expect(Source.Entity.rendered(app)).toEqual([]);
	});
});
