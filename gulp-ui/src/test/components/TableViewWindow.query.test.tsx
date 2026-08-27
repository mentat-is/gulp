import { act } from "react";
import type { ButtonHTMLAttributes, ReactNode } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { TableViewWindow } from "@/components/TableViewWindow";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "table-query-operation" as Operation.Id;
const CONTEXT_ID = "table-query-context" as Context.Id;
const SOURCE_ID = "table-query-source" as Source.Id;

/**
 * Creates the source fixture consumed by the isolated Table view.
 * @param overrides Source fields to replace.
 * @returns Queryable source fixture.
 */
function createSource(overrides: Partial<Source.Type> = {}): Source.Type {
	return {
		id: SOURCE_ID,
		type: "source",
		name: "Table source",
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
		...overrides,
	} as Source.Type;
}

/**
 * Creates the minimal application snapshot required by TableViewWindow.
 * @param source Source included in the snapshot.
 * @returns Application fixture.
 */
function createApp(source: Source.Type): App.Type {
	return {
		target: {
			files: [source],
			operations: [{ id: OPERATION_ID, selected: true }],
			contexts: [],
		},
		timeline: {
			frame: { min: 1, max: 100 },
			renderVersion: 0,
		},
	} as unknown as App.Type;
}

let mockApp = createApp(createSource());
let mockEventKeysCalls = 0;
let mockQueryPaginateCalls = 0;
let mockSyncChange: ((checked: boolean) => void) | null = null;
let mockTableFilterOptions: Record<string, string> | undefined;
const mockInfo = {
	event_keys: async (): Promise<Record<string, string>> => {
		mockEventKeysCalls += 1;
		return { timestamp: "date" };
	},
	query_paginate: async (): Promise<{ docs: unknown[]; total_hits: number }> => {
		mockQueryPaginateCalls += 1;
		return { docs: [], total_hits: 0 };
	},
};
const mockApplicationContext = {
	get app(): App.Type {
		return mockApp;
	},
	Info: mockInfo,
	spawnBanner: (): void => undefined,
	banner: null,
	currentDocument: document,
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => mockApplicationContext,
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
			id: (app: App.Type, id: Source.Id): Source.Type | undefined =>
				app.target.files.find((source) => source.id === id),
		},
		Select: { Multi: (): null => null },
	},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			flag: {
				getList: (): Set<string> => new Set(),
				toggle: (): void => undefined,
			},
		},
	},
}));

jest.mock("@/entities/Filter", () => ({
	Filter: {
		Entity: { query: (query: unknown): unknown => query },
	},
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			toNanos: (value: number | string): bigint =>
				BigInt(Math.round(Number(value) * 1_000_000)),
			toISO: (value: string): string => value,
		},
	},
}));

jest.mock("@/lib/WindowBridge", () => ({
	WindowBridge: {
		MessageType: {
			NAVIGATE_TO_EVENT: "NAVIGATE_TO_EVENT",
		},
		sendControlMessage: (): void => undefined,
	},
}));

jest.mock("@/banners/Collab.functionality", () => ({
	NoteFunctionality: { Create: { Banner: (): null => null } },
}));

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {
		Query: {
			String: (): null => null,
			Add: (): null => null,
			Filters: ({
				options,
			}: {
				options: Record<string, string>;
			}): null => {
				mockTableFilterOptions = options;
				return null;
			},
		},
	},
}));

jest.mock("@/components/Table", () => ({
	Table: (): null => null,
}));

jest.mock("@/ui/Input", () => ({ Input: (): null => null }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
jest.mock("@/ui/Checkbox", () => ({ Checkbox: (): null => null }));
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
jest.mock("@/ui/Label", () => ({ Label: (): null => null }));
jest.mock("@/ui/Toggle", () => ({
	Toggle: ({
		onCheckedChange,
	}: {
		onCheckedChange: (checked: boolean) => void;
	}): null => {
		mockSyncChange = onCheckedChange;
		return null;
	},
}));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Separator", () => ({ Separator: (): null => null }));
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
	Tooltip: ({ children }: { children?: React.ReactNode }): React.ReactNode =>
		children ?? null,
	TooltipContent: (): null => null,
	TooltipProvider: ({
		children,
	}: {
		children?: React.ReactNode;
	}): React.ReactNode => children ?? null,
	TooltipTrigger: ({
		children,
	}: {
		children?: React.ReactNode;
	}): React.ReactNode => children ?? null,
}));
jest.mock("@/ui/utils", () => ({
	cn: (...classNames: unknown[]): string => classNames.filter(Boolean).join(" "),
}));
jest.mock("sonner", () => ({
	toast: { error: (): void => undefined },
}));

/**
 * Lets resolved request promises and their React state updates settle.
 * @returns Promise resolved after the current microtask queue.
 */
async function flushRequests(): Promise<void> {
	await act(async () => {
		await Promise.resolve();
		await Promise.resolve();
	});
}

describe("TableViewWindow query stability", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockApp = createApp(createSource());
		mockEventKeysCalls = 0;
		mockQueryPaginateCalls = 0;
		mockSyncChange = null;
		mockTableFilterOptions = undefined;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		jest.useRealTimers();
	});

	it("does not reload metadata or events for a cloned equivalent app snapshot", async () => {
		act(() => {
			root.render(<TableViewWindow initialSourceId={SOURCE_ID} />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(1);
		expect(mockQueryPaginateCalls).toBe(1);

		mockApp = createApp(createSource());
		act(() => {
			root.render(<TableViewWindow initialSourceId={SOURCE_ID} />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(1);
		expect(mockQueryPaginateCalls).toBe(1);

		mockApp = createApp(createSource({ name: "Renamed table source" }));
		act(() => {
			root.render(<TableViewWindow initialSourceId={SOURCE_ID} />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(1);
		expect(mockQueryPaginateCalls).toBe(1);

		mockApp = createApp(createSource({ time_updated: 11 }));
		act(() => {
			root.render(<TableViewWindow initialSourceId={SOURCE_ID} />);
		});
		await flushRequests();

		expect(mockEventKeysCalls).toBe(2);
		expect(mockQueryPaginateCalls).toBe(1);
	});

	it("debounces rapid synchronized frame changes while remaining reactive", async () => {
		jest.useFakeTimers();
		act(() => {
			root.render(<TableViewWindow initialSourceId={SOURCE_ID} />);
		});
		await flushRequests();

		expect(mockQueryPaginateCalls).toBe(1);
		if (!mockSyncChange) {
			throw new Error("The synchronized Table toggle was not rendered");
		}

		act(() => mockSyncChange?.(true));
		act(() => jest.advanceTimersByTime(149));
		await flushRequests();
		expect(mockQueryPaginateCalls).toBe(1);

		act(() => jest.advanceTimersByTime(1));
		await flushRequests();
		expect(mockQueryPaginateCalls).toBe(2);

		mockApp = {
			...mockApp,
			timeline: {
				...mockApp.timeline,
				frame: { min: 2, max: 99 },
			},
		};
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));
		mockApp = {
			...mockApp,
			timeline: {
				...mockApp.timeline,
				frame: { min: 3, max: 98 },
			},
		};
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));

		act(() => jest.advanceTimersByTime(149));
		await flushRequests();
		expect(mockQueryPaginateCalls).toBe(2);

		act(() => jest.advanceTimersByTime(1));
		await flushRequests();
		expect(mockQueryPaginateCalls).toBe(3);
	});

	it("passes loaded field metadata to the generic query builder", async () => {
		act(() => {
			root.render(<TableViewWindow initialSourceId={SOURCE_ID} />);
		});
		await flushRequests();

		const filterButton = [...container.querySelectorAll("button")].find(
			(button) => button.textContent === "common.filters",
		);
		act(() => filterButton?.click());

		expect(mockTableFilterOptions).toEqual({ timestamp: "date" });
	});
});
