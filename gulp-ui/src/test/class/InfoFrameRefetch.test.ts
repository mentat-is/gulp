import { Info } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import { requestStore } from "@/store/request.store";

jest.mock("@/class/IndexedDB", () => ({
	GulpIndexedDB: class MockGulpIndexedDB {},
}));

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {},
}));

jest.mock("@/dto/Logger.class", () => ({
	Logger: {
		log: jest.fn(),
		warn: jest.fn(),
		error: jest.fn(),
		fatal: jest.fn(),
		assert: jest.fn(),
	},
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: jest.fn(),
	NodeFile: class MockNodeFile {},
	Refractor: {
		array: (...values: unknown[]): unknown[] => values,
		object: <T extends object>(value: T): T => ({ ...value }),
	},
	stringToHexColor: (): string => "#000000",
}));

jest.mock("@/overlays/Highlights", () => ({ Highlights: {} }));
jest.mock("@/components/Pointers", () => ({ Pointers: {} }));
jest.mock("@/components/CustomParameters", () => ({ CustomParameters: {} }));

jest.mock("@/class/RenderEngine", () => ({
	RenderEngine: {
		clearAllCaches: jest.fn(),
		reset: jest.fn(),
		resetSource: jest.fn(),
	},
}));

jest.mock("@/class/SmartSocket", () => ({
	SmartSocket: {
		Class: {
			instance: null,
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			/** Resolves source fixtures from the application state. */
			id: (app: App.Type, source: Source.Type | Source.Id): Source.Type =>
				typeof source === "object"
					? source
					: app.target.files.find((file) => file.id === source)!,
			/** Returns selected source fixtures. */
			selected: (app: App.Type): Source.Type[] =>
				app.target.files.filter((file) => file.selected),
			/** Returns a simple union frame for the supplied fixtures. */
			renderedFrame: (files: Source.Type[]) => ({
				min: Math.min(...files.map((file) => file.timestamp.min)),
				max: Math.max(...files.map((file) => file.timestamp.max)),
			}),
		},
	},
}));

jest.mock("@/entities/App", () => ({ App: {} }));
jest.mock("@/entities/Doc", () => ({ Doc: { Entity: { delete: jest.fn() } } }));
jest.mock("@/entities/Note", () => ({ Note: { Entity: {} } }));
jest.mock("@/entities/Link", () => ({ Link: { Entity: {} } }));
jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);
jest.mock("@/workers/IngestWorker.manager", () => ({
	ingestWorkerManager: {},
}));
jest.mock("@/workers/DataWorker.class", () => ({ DataWorker: {} }));

const sourceId = "frame-refetch-source" as Source.Id;
const sourceTimestamp = {
	min: 1_700_000_000_000,
	max: 1_800_000_000_000,
};

/**
 * Creates a selected source with a two-second positive render offset.
 * @returns Source fixture for the refetch test.
 */
function createSource(): Source.Type {
	return {
		id: sourceId,
		name: "frame-refetch.csv",
		selected: true,
		operation_id: "frame-refetch-operation",
		settings: { offset: 2_000 },
		timestamp: sourceTimestamp,
		nanotimestamp: {
			min: BigInt(sourceTimestamp.min) * 1_000_000n,
			max: BigInt(sourceTimestamp.max) * 1_000_000n,
		},
	} as Source.Type;
}

/**
 * Creates application state with a persisted structured filter over the full source.
 * @param source Source stored in target.files.
 * @returns Application fixture owned by Info.
 */
function createApp(source: Source.Type): App.Type {
	const query: Query.Type = {
		string: "existing filter",
		text_filter: "*authentication*",
		filters: [
			{
				id: "outcome-filter",
				type: "eq",
				operator: "must",
				field: "event.outcome",
				value: "failure",
				enabled: true,
			} as Query.Type["filters"][number],
		],
		source_config: {
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: {
				min: source.nanotimestamp.min.toString(),
				max: source.nanotimestamp.max.toString(),
			},
		},
	};

	return {
		target: {
			files: [source],
			filters: { [source.id]: query },
			events: new Map(),
			operations: [],
			contexts: [],
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		timeline: {
			scale: 1,
			frame: sourceTimestamp,
			filter: "",
			cache: { data: new Map(), filters: {} },
			target: null,
			isScrollReversed: false,
			dialogSize: 0,
			pointers: [],
			renderVersion: 0,
		},
		general: {
			server: "http://localhost:8080",
			ws_id: "frame-refetch-workspace",
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: requestStore.getLoadings(),
			tableViewSource: null,
		},
		settings: {},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: false,
			toasts: false,
		},
	} as App.Type;
}

describe("Info timeline-frame refetch", () => {
	beforeEach(() => requestStore.reset());
	afterEach(() => requestStore.reset());

	it("persists the frame range and reapplies it to subsequent filtered refetches", async () => {
		const source = createSource();
		let currentApp = createApp(source);
		const info = new Info({
			app: currentApp,
			setInfo: (action) => {
				currentApp =
					typeof action === "function" ? action(currentApp) : action;
			},
			timeline: { current: document.createElement("div") },
		});
		const queryFile = jest.fn<Promise<undefined>, [Query.Type, object]>(
			async () => undefined,
		);
		info.query_file = queryFile as typeof info.query_file;
		info.notes_reload = jest.fn(async () => undefined);
		info.links_reload = jest.fn(async () => undefined);
		info.events_reset_in_file = jest.fn();
		(
			info as unknown as { highlights_reload: () => void }
		).highlights_reload = jest.fn();

		const frame = {
			min: 1_742_169_600_000,
			max: 1_743_033_600_000,
		};
		await info.refetch({ ids: source.id, frame });

		expect(queryFile).toHaveBeenCalled();
		const dispatched = queryFile.mock.calls[0][0];
		expect(dispatched.source_config).toEqual({
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: {
				min: (
					BigInt(frame.min - source.settings.offset) * 1_000_000n
				).toString(),
				max: (
					BigInt(frame.max - source.settings.offset) * 1_000_000n
				).toString(),
			},
		});
		expect(dispatched.text_filter).toBe("*authentication*");
		expect(dispatched.filters).toHaveLength(1);
		expect(currentApp.timeline.frame).toEqual(frame);
		expect(currentApp.target.filters[source.id].source_config).toEqual(
			dispatched.source_config,
		);
		expect(currentApp.target.filters[source.id].filters).toEqual(
			dispatched.filters,
		);

		info.setQuery(source, {
			...currentApp.target.filters[source.id],
			text_filter: "*subsequent filter*",
			source_config: {
				operation_id: source.operation_id,
				source_ids: [source.id],
				range: {
					min: source.nanotimestamp.min.toString(),
					max: source.nanotimestamp.max.toString(),
				},
			},
		});
		await info.refetch({ ids: source.id });

		const filteredRefetch = queryFile.mock.calls[1][0];
		expect(filteredRefetch.source_config?.range).toEqual(
			dispatched.source_config?.range,
		);
		expect(filteredRefetch.text_filter).toBe("*subsequent filter*");
	});
});
