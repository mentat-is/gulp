import { act } from "react";
import type {
	ButtonHTMLAttributes,
	ReactElement,
	ReactNode,
} from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { DashboardViewWindow } from "@/components/DashboardViewWindow";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "dashboard-query-operation" as Operation.Id;
const CONTEXT_ID = "dashboard-query-context" as Context.Id;
const SOURCE_ID = "dashboard-query-source" as Source.Id;

/**
 * Creates the source fixture used by the isolated Dashboard view.
 * @param overrides Source fields to replace.
 * @returns Selected operation source.
 */
function createSource(overrides: Partial<Source.Type> = {}): Source.Type {
	return {
		id: SOURCE_ID,
		type: "source",
		name: "Dashboard source",
		selected: true,
		pinned: false,
		color: "#336699",
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		plugin: "test-plugin",
		time_updated: 10,
		settings: { offset: 0 },
		nanotimestamp: { min: BigInt(1), max: BigInt(100) },
		...overrides,
	} as Source.Type;
}

/**
 * Creates the minimal application snapshot required by DashboardViewWindow.
 * @param source Source included in the selected operation.
 * @returns Application fixture.
 */
function createApp(source: Source.Type): App.Type {
	return {
		target: {
			files: [source],
			operations: [{ id: OPERATION_ID, selected: true }],
			contexts: [{ id: CONTEXT_ID, color: "#112233" }],
			filters: {},
		},
		timeline: {
			frame: { min: 1, max: 100 },
			renderVersion: 0,
		},
	} as unknown as App.Type;
}

let mockApp = createApp(createSource());
let mockEventKeysCalls = 0;
let mockAggregationCalls = 0;
let mockSpawnedBanner: ReactElement | null = null;
const mockInfo = {
	event_keys: async (): Promise<Record<string, string>> => {
		mockEventKeysCalls += 1;
		return { "event.code": "keyword" };
	},
	shared_object_list: async (): Promise<unknown[]> => [
		{
			id: "dashboard-shared-object",
			obj_id: "dashboard-fixed-card",
			name: "Fixed dashboard",
			obj: {
				id: "dashboard-fixed-card",
				name: "Fixed dashboard",
				type: "fixed",
				dashboard: "vertical_chart",
				visible: true,
				opensearch_query: {
					aggs: {
						top_values: {
							terms: { field: "event.code" },
						},
					},
				},
			},
		},
	],
	query_aggregation: async (): Promise<Record<string, unknown>> => {
		mockAggregationCalls += 1;
		return {};
	},
};
const mockApplicationContext = {
	get app(): App.Type {
		return mockApp;
	},
	Info: mockInfo,
	spawnBanner: (banner: ReactElement): void => {
		mockSpawnedBanner = banner;
	},
	banner: null,
	currentDocument: document,
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => mockApplicationContext,
	},
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Slot: { DashboardView: "dashboard-view" },
		use: (): { extensions: unknown[] } => ({ extensions: [] }),
		getBySlot: (): unknown[] => [],
		Component: (): null => null,
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

jest.mock("@/entities/Operation", () => ({
	Operation: {
		Entity: {
			selected: (app: App.Type): { id: Operation.Id } | undefined =>
				app.target.operations.find((operation) => operation.selected),
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			pins: (sources: Source.Type[]): Source.Type[] => sources,
		},
		Select: { Multi: (): null => null },
	},
}));

jest.mock("@/entities/Filter", () => ({
	Filter: {
		isGroup: (): boolean => false,
		Entity: {
			buildItemClause: (): null => null,
		},
	},
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			toNanos: (value: number | string): bigint =>
				BigInt(Math.round(Number(value) * 1_000_000)),
			toTimestamp: (value: string): number => Number(value),
		},
	},
}));

jest.mock("@/workers/DataWorker.class", () => ({
	DataWorker: {
		normalizeDashboardAggregation: async (): Promise<{
			mode: "dashboard_chart";
			buckets: unknown[];
		}> => ({
			mode: "dashboard_chart",
			buckets: [],
		}),
	},
}));

jest.mock("@/components/DashboardChartAdapter", () => ({
	Bar: (): null => null,
	Line: (): null => null,
}));
jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {
		Query: {
			String: (): null => null,
			Add: (): null => null,
			Filters: (): null => null,
		},
	},
}));
jest.mock("@/components/Table", () => ({
	Table: (): null => null,
}));

jest.mock("@/ui/Banner", () => ({ Banner: (): null => null }));
jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		icon,
		variant,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & {
		icon?: string;
		variant?: string;
	}) => <button {...props}>{children ?? icon}</button>,
}));
jest.mock("@/ui/Input", () => ({ Input: (): null => null }));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
jest.mock("@/ui/Popover", () => ({
	Popover: {
		Root: (): null => null,
		Trigger: (): null => null,
		Content: (): null => null,
	},
}));
jest.mock("@/ui/Select", () => ({
	Select: {
		Root: (): null => null,
		Trigger: (): null => null,
		Value: (): null => null,
		Content: (): null => null,
		Item: (): null => null,
	},
}));
jest.mock("@/ui/Tooltip", () => ({
	Tooltip: (): null => null,
	TooltipContent: (): null => null,
	TooltipProvider: (): null => null,
	TooltipTrigger: (): null => null,
}));
jest.mock("@/ui/utils", () => ({
	stringToHexColor: (): string => "#445566",
}));

/**
 * Lets dashboard definitions, metadata, and aggregation promises settle.
 * @returns Promise resolved after the current microtask queue.
 */
async function flushRequests(): Promise<void> {
	await act(async () => {
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
	});
}

describe("DashboardViewWindow query stability", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockApp = createApp(createSource());
		mockEventKeysCalls = 0;
		mockAggregationCalls = 0;
		mockSpawnedBanner = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("does not reload metadata or aggregations for cloned source/context data", async () => {
		act(() => {
			root.render(<DashboardViewWindow />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(1);
		expect(mockAggregationCalls).toBe(1);

		mockApp = createApp(createSource());
		act(() => {
			root.render(<DashboardViewWindow />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(1);
		expect(mockAggregationCalls).toBe(1);

		mockApp = createApp(createSource({ name: "Renamed dashboard source" }));
		act(() => {
			root.render(<DashboardViewWindow />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(1);
		expect(mockAggregationCalls).toBe(1);

		mockApp = createApp(createSource({ time_updated: 11 }));
		act(() => {
			root.render(<DashboardViewWindow />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(2);
		expect(mockAggregationCalls).toBe(1);
	});

	it("passes loaded field metadata to the generic filter editor", async () => {
		act(() => {
			root.render(<DashboardViewWindow />);
		});
		await flushRequests();

		const filterButton = [...container.querySelectorAll("button")].find(
			(button) => button.textContent === "common.filters",
		);
		act(() => filterButton?.click());

		expect(
			(
				mockSpawnedBanner as ReactElement<{
					fieldOptions: Record<string, string>;
				}>
			)?.props.fieldOptions,
		).toEqual({ "event.code": "keyword" });
	});
});
