import { Info, type GulpDataset } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Mapping } from "@/entities/Mapping";

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
		assert: (value: unknown): unknown => value,
	},
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: (): string => "mapping-files-request",
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
jest.mock("@/class/RenderEngine", () => ({ RenderEngine: {} }));
jest.mock("@/class/SmartSocket", () => ({
	SmartSocket: { Class: { instance: null }, Message: { Type: {} } },
}));
jest.mock("@/workers/IngestWorker.manager", () => ({
	ingestWorkerManager: {},
}));
jest.mock("@/workers/DataWorker.class", () => ({ DataWorker: {} }));
jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);

/** Creates the minimal application state used by mapping refreshes. */
function createApp(mappings: Mapping.Type.Plugin[] = []): App.Type {
	return {
		target: {
			operations: [],
			contexts: [],
			files: [],
			events: new Map(),
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings,
			plugins: [],
		},
		general: {
			server: "",
			ws_id: "",
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: { byRequestId: new Map(), byFileId: new Map() },
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

/** Creates an Info instance whose state updates are applied synchronously. */
function createInfo(mappings: Mapping.Type.Plugin[] = []): Info {
	let app = createApp(mappings);
	let info: Info;
	info = new Info({
		app,
		setInfo: (update) => {
			app = typeof update === "function" ? update(app) : update;
		},
		timeline: { current: null } as unknown as React.RefObject<HTMLDivElement>,
	});
	return info;
}

/** Creates one complete plugin-list fixture. */
function createPlugin(
	filename: string,
	type: GulpDataset.PluginList.Type[],
): GulpDataset.PluginList.Interface {
	return {
		display_name: filename,
		type,
		desc: "",
		path: "",
		data: {},
		filename,
		sigma_support: [],
		custom_parameters: [],
		depends_on: [],
		tags: [],
		version: "1.0.0",
	};
}

describe("Info.mapping_file_list", () => {
	const originalApi = globalThis.api;
	let errorSpy: JestSpy & { mockRestore(): void };

	beforeEach(() => {
		errorSpy = jest.spyOn(console, "error") as JestSpy & {
			mockRestore(): void;
		};
		errorSpy.mockReturnValue(undefined);
	});

	afterEach(() => {
		globalThis.api = originalApi;
		errorSpy.mockRestore();
	});

	it("keeps valid mappings and reports the malformed plugin filename", async () => {
		const info = createInfo();
		globalThis.api = (async () => [
			{
				metadata: { plugin: ["csv.py"] },
				filename: "csv.json",
				mapping_ids: ["default"],
			},
		]) as unknown as typeof globalThis.api;
		info.plugin_list = async () => [
			createPlugin("standalone.py", ["ingestion"]),
			{
				...createPlugin("broken.py", ["ingestion"]),
				type: undefined,
			} as unknown as GulpDataset.PluginList.Interface,
		];

		const mappings = await info.mapping_file_list();

		expect(mappings).toEqual([
			{
				name: "csv.py",
				methods: [{ name: "csv.json", mappings: ["default"] }],
			},
			{ name: "standalone.py", methods: [] },
		]);
		expect(info.app.target.mappings).toEqual(mappings);
		const pluginError = errorSpy.mock.calls.find(
			(call) =>
				call[0] ===
				'[Info.mapping_file_list] Failed to process plugin file "broken.py".',
		);
		expect(pluginError?.[0]).toBe(
			'[Info.mapping_file_list] Failed to process plugin file "broken.py".',
		);
		expect(pluginError?.[1]).toBeInstanceOf(TypeError);
	});

	it("retains existing mappings after failure and allows a later retry", async () => {
		const previousMappings: Mapping.Type.Plugin[] = [
			{ name: "existing.py", methods: [] },
		];
		const info = createInfo(previousMappings);
		let requestCount = 0;
		globalThis.api = (async () => {
			requestCount += 1;
			if (requestCount === 1) throw new Error("mapping endpoint unavailable");
			return [
				{
					metadata: { plugin: ["json.py"] },
					filename: "json.json",
					mapping_ids: ["default"],
				},
			];
		}) as unknown as typeof globalThis.api;
		info.plugin_list = async () => [];

		expect(await info.mapping_file_list()).toEqual(previousMappings);
		expect(await info.mapping_file_list()).toEqual([
			{
				name: "json.py",
				methods: [{ name: "json.json", mappings: ["default"] }],
			},
		]);
		expect(requestCount).toBe(2);
	});
});
