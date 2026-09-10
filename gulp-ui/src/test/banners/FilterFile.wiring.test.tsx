import { act } from "react";
import type { ButtonHTMLAttributes, ReactNode } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Filter } from "@/entities/Filter";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { FilterFileBanner } from "@/banners/FilterFile.banner";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "filter-operation" as Operation.Id;
const CONTEXT_ID = "filter-context" as Context.Id;
const SOURCE_ID = "filter-source" as Source.Id;

const mockSource = {
	id: SOURCE_ID,
	type: "source",
	name: "Filter source",
	selected: true,
	pinned: false,
	operation_id: OPERATION_ID,
	context_id: CONTEXT_ID,
	plugin: "test-plugin",
	time_updated: 10,
	settings: { offset: 0 },
	nanotimestamp: { min: BigInt(1), max: BigInt(100) },
	appliedQuery: null,
	queryCache: createEmptyQueryCacheState(),
} as Source.Type;

const mockApp = {
	target: {
		files: [mockSource],
		operations: [{ id: OPERATION_ID, selected: true }],
		contexts: [],
	},
	timeline: {
		frame: { min: 1, max: 100 },
		renderVersion: 0,
	},
} as unknown as App.Type;

let mockCapturedOptions: Record<string, string> | undefined;
let mockCapturedFilters: Filter.Item[] | undefined;
let mockSerializedQuery: Query.Type | undefined;
let mockPersistedQuery: Query.Type | null = null;
let mockEventKeys = async (): Promise<Record<string, string>> => ({
	"event.sequence": "long",
	message: "text",
});
const mockInfo = {
	refetch: jest.fn((_options: { ids: Source.Id[]; addToHistory: boolean }): void => undefined),
	render: jest.fn(),
	getQuery: (): Query.Type | null => mockPersistedQuery,
	event_keys: (): Promise<Record<string, string>> => mockEventKeys(),
	getLastQueries: async (): Promise<unknown[]> => [],
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => ({
			app: mockApp,
			Info: mockInfo,
			spawnBanner: (): void => undefined,
			destroyBanner: (): void => undefined,
		}),
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: (): Source.Type => mockSource,
		},
		Select: { Multi: (): null => null },
	},
}));

jest.mock("@/entities/Filter", () => ({
	Filter: {
		EMPTY_OPTIONS: {},
		Entity: {
			query: (query: Query.Type): { bool: { must: unknown[] } } => {
				mockSerializedQuery = query;
				return { bool: { must: [] } };
			},
			describe: (): string => "filter",
		},
	},
}));

jest.mock("@/entities/Operation", () => ({
	Operation: { Entity: { selected: (): undefined => undefined } },
}));

jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: { flag: { getDocIds: (): string[] => [] } } },
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			toNanos: (value: number): bigint => BigInt(value) * 1_000_000n,
		},
	},
}));

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {
		Preview: (): null => null,
		Query: {
			String: (): null => null,
			Add: (): null => null,
			Filters: ({
				filters,
				options,
			}: {
				filters: Filter.Item[];
				options: Record<string, string>;
			}): null => {
				mockCapturedFilters = filters;
				mockCapturedOptions = options;
				return null;
			},
		},
	},
}));

jest.mock("@/components/QueriesHistory", () => ({
	QueriesHistory: (): null => null,
}));
jest.mock("@/banners/Preview.banner", () => ({
	Preview: { Banner: (): null => null },
}));
jest.mock("@/ui/Banner", () => ({
	Banner: ({ children, done }: { children?: ReactNode; done?: ReactNode }) => <div>{done}{children}</div>,
}));
jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		icon,
		variant,
		loading,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & {
		icon?: string;
		variant?: string;
		loading?: boolean;
	}) => <button {...props}>{children ?? icon}</button>,
}));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
jest.mock("@/ui/Separator", () => ({ Separator: (): null => null }));
jest.mock("@/ui/Checkbox", () => ({ Checkbox: (): null => null }));
jest.mock("@/ui/Label", () => ({ Label: (): null => null }));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Textarea", () => ({ Textarea: (): null => null }));
jest.mock("@/ui/Popover", () => ({ Popover: {} }));
jest.mock("@/ui/Tooltip", () => ({
	Tooltip: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipContent: (): null => null,
	TooltipProvider: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipTrigger: ({ children }: { children?: ReactNode }) => children ?? null,
}));
jest.mock("@/ui/Tabs", () => ({
	Tabs: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
	TabsContent: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
	TabsList: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
	TabsTrigger: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
jest.mock("@/ui/utils", () => ({ fws: {} }));
jest.mock("sonner", () => ({
	toast: {
		error: (): void => undefined,
		info: (): void => undefined,
		success: (): void => undefined,
	},
}));

/** Lets field metadata requests and their React state updates settle. */
async function flushRequests(): Promise<void> {
	await act(async () => {
		await Promise.resolve();
		await Promise.resolve();
	});
}

describe("FilterFileBanner field catalog wiring", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockCapturedOptions = undefined;
		mockCapturedFilters = undefined;
		mockSerializedQuery = undefined;
		mockPersistedQuery = null;
		mockEventKeys = async (): Promise<Record<string, string>> => ({
			"event.sequence": "long",
			message: "text",
		});
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("passes backend field types directly to the generic query builder", async () => {
		await act(async () => {
			root.render(<FilterFileBanner sources={[mockSource]} />);
		});
		await flushRequests();

		expect(mockCapturedOptions).toEqual({
			"event.sequence": "long",
			message: "text",
		});
	});

	it("submits through the common batch entry without a redundant global render", async () => {
		await act(async () => { root.render(<FilterFileBanner sources={[mockSource]} />); });
		await flushRequests();
		const submit = Array.from(container.querySelectorAll("button")).find((button) => button.textContent === "Check")!;
		await act(async () => { submit.click(); });
		expect(mockInfo.refetch).toHaveBeenCalledTimes(1);
		expect(mockInfo.refetch.mock.calls[0][0]).toMatchObject({ ids: [SOURCE_ID], addToHistory: true });
		expect(mockInfo.render).not.toHaveBeenCalled();
	});

	it("restores persisted field types together with Navigator filters", async () => {
		mockPersistedQuery = {
			string: "event.original:*failed*",
			filters: [
				{
					id: "navigator-event-filter" as Filter.Id,
					type: "wildcard",
					operator: "must",
					field: "event.original",
					value: "*failed*",
					enabled: true,
					case_insensitive: true,
				},
			],
			fieldTypeMap: { "event.original": "wildcard" },
		};
		mockEventKeys = () => new Promise<Record<string, string>>(() => undefined);

		await act(async () => {
			root.render(<FilterFileBanner sources={[mockSource]} />);
		});
		await flushRequests();

		expect(mockCapturedOptions).toEqual({ "event.original": "wildcard" });
		expect(mockCapturedFilters?.[0]).toMatchObject({
			field: "event.original",
			type: "wildcard",
		});
		expect(mockSerializedQuery?.fieldTypeMap).toEqual({
			"event.original": "wildcard",
		});
	});
});
