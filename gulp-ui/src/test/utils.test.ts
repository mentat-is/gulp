import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import {
	getCanvasRenderedSourceIds,
	getCanvasRenderedSourceIdSet,
} from "@/utils";

const OPERATION_ID = "operation" as Operation.Id;
const CONTEXT_ID = "context" as Context.Id;
let mockSourceSelectedImplementation = (app: App.Type): Source.Type[] =>
	app.target.files.filter((source: Source.Type) => source.selected);
let mockSourceRenderedImplementation = (_app: App.Type): Source.Type[] => [];

jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: { get: jest.fn() } },
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			selected: (app: App.Type) => mockSourceSelectedImplementation(app),
			rendered: (app: App.Type) => mockSourceRenderedImplementation(app),
		},
	},
}));

/**
 * Creates one source fixture with the fields used by Source.Entity.selected.
 * @param id Stable source ID.
 * @param name Source name used by timeline text filtering.
 * @param total Backend-advertised event total.
 * @returns Source entity fixture.
 */
function createSource(id: string, name: string, total = 10): Source.Type {
	return {
		id: id as Source.Id,
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		name,
		selected: true,
		pinned: false,
		total,
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
	} as Source.Type;
}

/**
 * Creates a minimal app fixture for canvas source-set assertions.
 * @param sources Source rows available to the timeline.
 * @param appOverrides App fields that should differ from the base fixture.
 * @returns Application fixture.
 */
function createApp(
	sources: Source.Type[],
	appOverrides: {
		filter?: string;
		sourcesWithNoEvents?: boolean;
		contextName?: string;
	} = {},
): App.Type {
	return {
		target: {
			operations: [],
			contexts: [
				{
					id: CONTEXT_ID,
					name: appOverrides.contextName ?? "Matching Context",
				} as Context.Type,
			],
			files: sources,
			events: new Map(),
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		timeline: {
			scale: 1,
			frame: { min: 0, max: 100 },
			target: null,
			filter: appOverrides.filter ?? "",
			isScrollReversed: false,
			dialogSize: 320,
			pointers: [],
			renderVersion: 0,
		},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: appOverrides.sourcesWithNoEvents ?? false,
			toasts: false,
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
		settings: {},
	} as App.Type;
}

describe("getCanvasRenderedSourceIdSet", () => {
	beforeEach(() => {
		mockSourceSelectedImplementation = (app: App.Type) =>
			app.target.files.filter((source: Source.Type) => source.selected);
		mockSourceRenderedImplementation = (app: App.Type) => {
			const contextById = new Map<Context.Id, string>(
				app.target.contexts.map((context) => [context.id, context.name]),
			);
			const filter = app.timeline.filter.toLowerCase();
			return app.target.files.filter((source: Source.Type) => {
				if (!source.selected) return false;
				if (app.hidden.sourcesWithNoEvents && source.total <= 0) return false;
				if (!filter) return true;
				const sourceName = source.name.toLowerCase();
				const contextName = (contextById.get(source.context_id) ?? "").toLowerCase();
				return sourceName.includes(filter) || contextName.includes(filter);
			});
		};
	});

	it("returns selected source IDs from the canvas source set", () => {
		const firstSource = createSource("first", "First Source");
		const secondSource = createSource("second", "Second Source");
		const app = createApp([firstSource, secondSource]);

		expect([...getCanvasRenderedSourceIdSet(app)]).toEqual([
			firstSource.id,
			secondSource.id,
		]);
	});

	it("excludes sources filtered out by the timeline source filter", () => {
		const matchingSource = createSource("matching", "Endpoint Logs");
		const filteredSource = createSource("filtered", "Network Logs");
		const app = createApp([matchingSource, filteredSource], {
			filter: "endpoint",
		});

		expect([...getCanvasRenderedSourceIdSet(app)]).toEqual([
			matchingSource.id,
		]);
	});

	it("excludes hidden empty sources after their empty render collection is known", () => {
		const populatedSource = createSource("populated", "Populated Source");
		const emptySource = createSource("empty", "Empty Source", 0);
		const app = createApp([populatedSource, emptySource], {
			sourcesWithNoEvents: true,
		});

		expect([...getCanvasRenderedSourceIdSet(app)]).toEqual([
			populatedSource.id,
		]);
	});

	it("returns ordered rendered source IDs for detached snapshots", () => {
		const firstSource = createSource("first", "First Source");
		const secondSource = createSource("second", "Second Source");
		const app = createApp([firstSource, secondSource]);

		expect(getCanvasRenderedSourceIds(app)).toEqual([
			firstSource.id,
			secondSource.id,
		]);
	});

	it("prefers bridged rendered source IDs in detached snapshots", () => {
		const firstSource = createSource("first", "First Source");
		const secondSource = createSource("second", "Second Source");
		const app = createApp([firstSource, secondSource]);
		app.timeline.renderedSourceIds = [secondSource.id];

		expect([...getCanvasRenderedSourceIdSet(app)]).toEqual([secondSource.id]);
	});
});
