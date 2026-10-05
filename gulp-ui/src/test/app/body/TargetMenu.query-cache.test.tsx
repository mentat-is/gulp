import { act, type ButtonHTMLAttributes, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import { Filter } from "@/entities/Filter";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { TargetMenu } from "@/app/body/Target.menu";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SOURCE_ID = "target-menu-query-cache" as Source.Id;
const mockNavigateSourceQueryCache = jest.fn();
const mockLoadings = { byFileId: new Map<Source.Id, string>() };

const mockInfo = {
	refetch: jest.fn(),
	filters_remove: jest.fn(),
	navigateSourceQueryCache: mockNavigateSourceQueryCache,
	setInfoByKey: jest.fn(),
	file_set_settings: jest.fn(),
	rebase_by_offset: jest.fn(),
	files_repin: jest.fn(),
	files_reorder_upper: jest.fn(),
	files_reorder_lower: jest.fn(),
	getTimelineSourceEdge: jest.fn(),
	cancelManagedRequest: jest.fn(),
	request_cancel: jest.fn(),
};

let mockSource: Source.Type;
const mockApp = {
	target: { files: [] as Source.Type[] },
} as unknown as App.Type;
const sourceLabelCases: ReadonlyArray<{
	sourceName: string;
	timelineLabel: Source.Type["settings"]["timeline_label"];
	expectedLabel: string;
}> = [
	{
		sourceName: "C:\\evidence\\windows.evtx",
		timelineLabel: "filename",
		expectedLabel: "windows.evtx",
	},
	{
		sourceName: "/var/log/security/linux.json",
		timelineLabel: "filename",
		expectedLabel: "linux.json",
	},
	{
		sourceName: "C:\\evidence\\windows.evtx",
		timelineLabel: "full_path",
		expectedLabel: "C:\\evidence\\windows.evtx",
	},
];
const filterSummaryCases: ReadonlyArray<{
	name: string;
	query: Partial<Query.Type>;
	description: string;
}> = [
	{
		name: "text",
		query: { text_filter: "needle", filters: [] },
		description: 'Text: "needle"',
	},
	{
		name: "structured",
		query: {
			text_filter: "",
			filters: [
				{
					id: "target-menu-filter",
					type: "eq",
					operator: "must",
					field: "event.code",
					value: 4624,
					enabled: true,
				},
			] as Query.Type["filters"],
		},
		description: "must eq event.code: 4624",
	},
	{
		name: "manual",
		query: { text_filter: "", filters: [], isManual: true, raw: {} },
		description: "filter.manualQuery",
	},
];

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => ({
			Info: mockInfo,
			app: mockApp,
			spawnBanner: jest.fn(),
			spawnDialog: jest.fn(),
		}),
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string, values?: { query?: string }) => string } => ({
			t: (key: string, values?: { query?: string }): string =>
				values?.query ? `${key}: ${values.query}` : key,
		}),
	},
}));

jest.mock("@/store/request.store", () => ({
	requestStore: {
		isManagedQueryRequest: (): boolean => false,
	},
	useRequestLoadings: (): typeof mockLoadings => mockLoadings,
}));

jest.mock("@/ui/ContextMenu", () => ({
	ContextMenuContent: ({ children }: { children?: ReactNode }) => (
		<div>{children}</div>
	),
	ContextMenuGroup: ({ children }: { children?: ReactNode }) => (
		<section>{children}</section>
	),
	ContextMenuItem: ({
		children,
		icon,
		revert: _revert,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & {
		icon?: string;
		revert?: boolean;
	}) => (
		<button data-icon={icon} {...props}>
			{children}
		</button>
	),
	ContextMenuLabel: ({
		children,
		title,
		icon,
	}: {
		children?: ReactNode;
		title?: string;
		icon?: string;
	}) => (
		<div data-context-menu-label="true" data-icon={icon} title={title}>
			{children}
		</div>
	),
	ContextMenuSeparator: (): null => null,
	ContextMenuSub: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
	ContextMenuSubContent: ({ children }: { children?: ReactNode }) => (
		<div>{children}</div>
	),
	ContextMenuSubTrigger: ({ children }: { children?: ReactNode }) => (
		<div>{children}</div>
	),
}));

jest.mock("@/ui/Tooltip", () => ({
	Tooltip: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipContent: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipProvider: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipTrigger: ({ children }: { children?: ReactNode }) => children ?? null,
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Settings: { Banner: (): null => null },
		Delete: { Banner: (): null => null },
	},
}));

jest.mock("@/entities/Filter", () => ({
	Filter: {
		Entity: {
			describe: jest.fn(),
		},
	},
}));

const mockDescribe = Filter.Entity.describe as typeof Filter.Entity.describe & {
	mockImplementation: (
		implementation: typeof Filter.Entity.describe,
	) => void;
};
let describedQuery: Query.Type | null = null;

/** Describes the user-controlled portion of a mocked applied query. */
function describeAppliedQuery(query: Query.Type): string {
	describedQuery = query;
	if (query.text_filter?.trim()) return `Text: "${query.text_filter}"`;
	if (query.filters.some((filter) => filter.enabled)) {
		return "must eq event.code: 4624";
	}
	if (query.isManual) return "filter.manualQuery";
	return "filter.noFiltersApplied";
}

jest.mock("@/dto/Engine.dto", () => ({ enginesBase: [] }));
jest.mock("@/banners/FilterFile.banner", () => ({
	FilterFileBanner: (): null => null,
}));
jest.mock("@/banners/Enrichment.banner", () => ({
	Enrichment: { Banner: (): null => null },
}));
jest.mock("@/banners/Sigma", () => ({ Sigma: { Banner: (): null => null } }));
jest.mock("@/dialogs/Event.dialog", () => ({
	DisplayEventDialog: (): null => null,
}));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
jest.mock("@/ui/utils", () => ({
	Refractor: { array: (...values: unknown[]): unknown[] => values },
}));
jest.mock("@/entities/Request", () => ({
	Request: { Prefix: { QUERY: "query" } },
}));
jest.mock("sonner", () => ({ toast: { error: jest.fn() } }));

/** Creates a complete source fixture with configurable local cache navigation state. */
function createSource(): Source.Type {
	const appliedQuery: Query.Type = {
		string: "failed-login cache",
		text_filter: "*failed login*",
		filters: [],
		source_config: {
			operation_id: "target-menu-operation",
			source_ids: [SOURCE_ID],
			range: { min: "1", max: "2" },
		},
	};
	return {
		id: SOURCE_ID,
		name: "target-menu-source.csv",
		operation_id: "target-menu-operation",
		selected: true,
		pinned: false,
		settings: { offset: 0, timeline_label: "filename" },
		timestamp: { min: 1, max: 2 },
		nanotimestamp: { min: 1n, max: 2n },
		appliedQuery,
		queryCache: {
			...createEmptyQueryCacheState(),
			activeGeneration: 2,
			canUndo: true,
			canRedo: false,
			freshness: "query-mismatch",
			warningReason: "query-mismatch",
		},
	} as unknown as Source.Type;
}

/** Finds one mocked context-menu item by its icon identifier. */
function getMenuButton(container: HTMLElement, icon: string): HTMLButtonElement {
	const button = container.querySelector<HTMLButtonElement>(
		`button[data-icon="${icon}"]`,
	);
	if (!button) throw new Error(`Missing menu button with icon ${icon}.`);
	return button;
}

/** Finds one mocked context-menu label by its optional icon identifier. */
function getMenuLabel(
	container: HTMLElement,
	icon?: string,
): HTMLDivElement | null {
	const selector = icon
		? `[data-context-menu-label="true"][data-icon="${icon}"]`
		: '[data-context-menu-label="true"]';
	return container.querySelector<HTMLDivElement>(selector);
}

describe("TargetMenu query-cache controls", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockDescribe.mockImplementation(describeAppliedQuery);
		describedQuery = null;
		mockLoadings.byFileId.clear();
		mockSource = createSource();
		mockApp.target.files = [mockSource];
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("shows the applied query and warning while wiring source-local Undo/Redo", () => {
		act(() => root.render(<TargetMenu source={mockSource} />));

		const undo = getMenuButton(container, "Undo");
		const redo = getMenuButton(container, "Redo");
		const appliedQueryLabel = getMenuLabel(container, "Search");
		expect(appliedQueryLabel?.textContent).toBe(
			'targetMenu.appliedQuery: Text: "*failed login*"',
		);
		expect(appliedQueryLabel?.title).toBe(
			'targetMenu.appliedQuery: Text: "*failed login*"',
		);
		expect(describedQuery?.source_config).toBe(undefined);
		expect(
			container.textContent?.includes("source.cacheWarning.queryMismatch"),
		).toBe(true);
		expect(undo.disabled).toBe(false);
		expect(redo.disabled).toBe(true);

		act(() => undo.click());
		expect(mockNavigateSourceQueryCache).toHaveBeenCalledWith(
			mockSource,
			"undo",
		);
	});

	for (const { sourceName, timelineLabel, expectedLabel } of sourceLabelCases) {
		it(`shows ${sourceName} using the ${timelineLabel} Canvas label mode`, () => {
			mockSource = {
				...mockSource,
				name: sourceName,
				settings: {
					...mockSource.settings,
					timeline_label: timelineLabel,
				},
			};

			act(() => root.render(<TargetMenu source={mockSource} />));

			expect(getMenuLabel(container)?.textContent).toBe(expectedLabel);
		});
	}

	it("omits only the applied-query row for a source-scoped query", () => {
		mockSource = {
			...mockSource,
			appliedQuery: {
				...mockSource.appliedQuery!,
				string: "Sources: target-menu-source.csv",
				text_filter: "",
				filters: [],
			},
		};

		act(() => root.render(<TargetMenu source={mockSource} />));

		expect(getMenuLabel(container, "Search")).toBeNull();
		expect(container.textContent).toContain("common.filters");
		expect(getMenuButton(container, "Filter").disabled).toBe(false);
		expect(getMenuButton(container, "X").disabled).toBe(false);
		expect(mockDescribe).not.toHaveBeenCalled();
	});

	for (const { name, query, description } of filterSummaryCases) {
		it(`shows the source-free ${name} filter summary`, () => {
			mockSource = {
				...mockSource,
				appliedQuery: {
					...mockSource.appliedQuery!,
					...query,
					string: "persisted source-inclusive label",
				},
			};

			act(() => root.render(<TargetMenu source={mockSource} />));

			const appliedQueryLabel = getMenuLabel(container, "Search");
			const expectedLabel = `targetMenu.appliedQuery: ${description}`;
			expect(appliedQueryLabel?.textContent).toBe(expectedLabel);
			expect(appliedQueryLabel?.title).toBe(expectedLabel);
			expect(describedQuery?.source_config).toBe(undefined);
		});
	}

	it("routes source filter reset through the forced reset command", () => {
		act(() => root.render(<TargetMenu source={mockSource} />));

		act(() => getMenuButton(container, "X").click());

		expect(mockInfo.filters_remove).toHaveBeenCalledWith(mockSource);
	});

	it("disables both history actions while cache activation is in progress", () => {
		mockSource = {
			...mockSource,
			queryCache: {
				...mockSource.queryCache,
				canUndo: true,
				canRedo: true,
				isActivating: true,
			},
		};
		mockApp.target.files = [mockSource];

		act(() => root.render(<TargetMenu source={mockSource} />));

		expect(getMenuButton(container, "Undo").disabled).toBe(true);
		expect(getMenuButton(container, "Redo").disabled).toBe(true);
	});
});
