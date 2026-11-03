import { act } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type { Query } from "@/entities/Query";
import type { GulpDataset } from "@/class/Info";
import type { Doc } from "@/entities/Doc";
import { TableViewWindow } from "@/components/TableViewWindow";
import type { QueryRawDocument } from "@/workers/DataWorker.types";

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
		...overrides,
	} as Source.Type;
}

/**
 * Creates a minimal raw event returned by a paginated table request.
 * @param id Stable document identifier used by stale-response assertions.
 * @returns Table-compatible document fixture.
 */
function createDocument(id: string): Doc.Type & QueryRawDocument {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: 1,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": 1,
		number_hash: 1,
	} as Doc.Type & QueryRawDocument;
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
let mockQueryPaginateCalls = 0;
let mockQueryPaginateRequests: Array<{
	query: Query.Type;
	options: GulpDataset.QueryRawPaginate.Options;
}> = [];
let mockQueryPaginateImplementation: (
	query: Query.Type,
	options: GulpDataset.QueryRawPaginate.Options,
) => Promise<GulpDataset.QueryRawPaginate.Response | undefined>;
let mockQueryPaginateCloseRequests: Array<{
	pitId: string;
	operationId: Operation.Id;
}> = [];
let mockSyncChange: ((checked: boolean) => void) | null = null;
let mockTableFilterOptions: Record<string, string> | undefined;
let mockTablePagination:
	| {
			onPageChange: (page: number) => void;
			onPageSizeChange: (pageSize: number) => void;
	  }
	| undefined;
let mockToastErrorCalls: unknown[][] = [];
let mockRenderedTableValues: Doc.Type[] | undefined;
const mockInfo = {
	event_keys: async (): Promise<Record<string, string>> => {
		mockEventKeysCalls += 1;
		return { timestamp: "date" };
	},
	query_paginate: async (
		query: Query.Type,
		options: GulpDataset.QueryRawPaginate.Options,
	): Promise<GulpDataset.QueryRawPaginate.Response | undefined> => {
		mockQueryPaginateCalls += 1;
		mockQueryPaginateRequests.push({ query, options });
		return mockQueryPaginateImplementation(query, options);
	},
	query_raw_paginate_close: async (
		pitId: string,
		operationId: Operation.Id,
	): Promise<GulpDataset.QueryRawPaginate.CloseResponse> => {
		mockQueryPaginateCloseRequests.push({ pitId, operationId });
		return { closed: true };
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
			Filters: (): null => null,
		},
	},
}));

jest.mock("@/components/Table", () => ({
	Table: ({ values }: { values: Doc.Type[] }): null => {
		mockRenderedTableValues = values;
		return null;
	},
}));


jest.mock("@/ui/Input", () => ({ Input: (): null => null }));
jest.mock("@/ui/Stack", () => ({ Stack: (): null => null }));
jest.mock("@/ui/Checkbox", () => ({ Checkbox: (): null => null }));
jest.mock("@/ui/Button", () => ({ Button: (): null => null }));
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
	toast: {
		error: (...arguments_: unknown[]): void => {
			mockToastErrorCalls.push(arguments_);
		},
	},
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
		mockQueryPaginateRequests = [];
		mockQueryPaginateCloseRequests = [];
		mockQueryPaginateImplementation = async () => ({
			docs: [],
			total_hits: 0,
		});
		mockSyncChange = null;
		mockTableFilterOptions = undefined;
		mockTablePagination = undefined;
		mockToastErrorCalls = [];
		mockRenderedTableValues = undefined;
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

	it("opens a PIT session and reuses the cached cursor for the next page", async () => {
		mockQueryPaginateImplementation = async () => ({
			docs: [],
			total_hits: 200,
			pit_id: "table-pit",
			search_after: ["page-2", 49],
		});
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));
		await flushRequests();

		expect(mockQueryPaginateRequests[0].options).toMatchObject({
			limit: 50,
			offset: 0,
			paginationMode: "pit",
		});
		expect(mockQueryPaginateRequests[0].options.pitId).toBeUndefined();
		expect(mockQueryPaginateRequests[0].options.searchAfter).toBeUndefined();

		act(() => mockTablePagination?.onPageChange(2));
		await flushRequests();

		expect(mockQueryPaginateRequests[1].options).toMatchObject({
			limit: 50,
			offset: 50,
			paginationMode: "pit",
			pitId: "table-pit",
			searchAfter: ["page-2", 49],
		});
	});

	it("uses the logical offset for an unvisited first-to-last jump beyond 100,000 events", async () => {
		mockQueryPaginateImplementation = async () => ({
			docs: [],
			total_hits: 250_000,
			pit_id: "large-table-pit",
			search_after: ["next-page"],
		});
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));
		await flushRequests();

		act(() => mockTablePagination?.onPageChange(5_000));
		await flushRequests();

		expect(mockQueryPaginateRequests[1].options).toMatchObject({
			offset: 249_950,
			pitId: "large-table-pit",
			paginationMode: "pit",
		});
		expect(mockQueryPaginateRequests[1].options.searchAfter).toBeUndefined();
	});

	it("closes and resets the PIT when page size changes", async () => {
		let requestCount = 0;
		mockQueryPaginateImplementation = async () => {
			requestCount += 1;
			return {
				docs: [],
				total_hits: 500,
				pit_id: `table-pit-${requestCount}`,
				search_after: [requestCount],
			};
		};
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));
		await flushRequests();

		act(() => mockTablePagination?.onPageSizeChange(100));
		await flushRequests();

		expect(
			mockQueryPaginateCloseRequests.some(
				(request) =>
					request.pitId === "table-pit-1" &&
					request.operationId === OPERATION_ID,
			),
		).toBe(true);
		expect(mockQueryPaginateRequests[1].options).toMatchObject({
			limit: 100,
			offset: 0,
			paginationMode: "pit",
		});
		expect(mockQueryPaginateRequests[1].options.pitId).toBeUndefined();
		expect(mockQueryPaginateRequests[1].options.searchAfter).toBeUndefined();
	});

	it("recovers once from an expired snapshot by returning to page 1", async () => {
		let requestCount = 0;
		mockQueryPaginateImplementation = async () => {
			requestCount += 1;
			if (requestCount === 2) {
				const error = new Error("snapshot expired");
				error.name = "PaginationSnapshotExpired";
				throw error;
			}
			return {
				docs: [],
				total_hits: 200,
				pit_id: requestCount === 1 ? "expired-pit" : "replacement-pit",
				search_after: [requestCount],
			};
		};
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));
		await flushRequests();

		act(() => mockTablePagination?.onPageChange(2));
		await flushRequests();
		await flushRequests();

		expect(mockQueryPaginateRequests).toHaveLength(3);
		expect(mockQueryPaginateRequests[2].options).toMatchObject({
			offset: 0,
			paginationMode: "pit",
		});
		expect(mockQueryPaginateRequests[2].options.pitId).toBeUndefined();
		expect(mockQueryPaginateRequests[2].options.searchAfter).toBeUndefined();
		expect(
			mockToastErrorCalls.some(
				(arguments_) =>
					arguments_[0] === "tableView.paginationSnapshotExpired",
			),
		).toBe(true);
	});

	it("ignores an aborted stale page response after a newer direct jump", async () => {
		let requestCount = 0;
		let resolveStaleRequest:
			| ((response: GulpDataset.QueryRawPaginate.Response) => void)
			| undefined;
		mockQueryPaginateImplementation = async () => {
			requestCount += 1;
			if (requestCount === 2) {
				return new Promise((resolve) => {
					resolveStaleRequest = resolve;
				});
			}

			const page = requestCount === 1 ? 1 : 3;
			return {
				docs: [createDocument(`page-${page}`)],
				total_hits: 150,
				pit_id: "stable-pit",
				search_after: page === 1 ? ["page-2"] : null,
			};
		};
		act(() => root.render(<TableViewWindow initialSourceId={SOURCE_ID} />));
		await flushRequests();

		act(() => mockTablePagination?.onPageChange(2));
		await flushRequests();
		act(() => mockTablePagination?.onPageChange(3));
		await flushRequests();

		expect(mockQueryPaginateRequests[1].options.signal?.aborted).toBe(true);
		expect(mockQueryPaginateRequests[2].options.pitId).toBeUndefined();
		expect(mockQueryPaginateRequests[2].options.searchAfter).toBeUndefined();
		expect(
			mockQueryPaginateCloseRequests.some(
				(request) => request.pitId === "stable-pit",
			),
		).toBe(true);
		expect(mockRenderedTableValues?.map((document) => document._id)).toEqual([
			"page-3",
		]);

		await act(async () => {
			resolveStaleRequest?.({
				docs: [createDocument("stale-page-2")],
				total_hits: 150,
				pit_id: "stable-pit",
				search_after: ["page-3"],
			});
			await Promise.resolve();
		});

		expect(mockRenderedTableValues?.map((document) => document._id)).toEqual([
			"page-3",
		]);
	});
});
