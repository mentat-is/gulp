import { act, type ButtonHTMLAttributes, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
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
	}: {
		children?: ReactNode;
		title?: string;
	}) => <div title={title}>{children}</div>,
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
			describe: (): string => "described applied query",
		},
	},
}));

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
		settings: { offset: 0 },
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

describe("TargetMenu query-cache controls", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
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
		expect(
			container.textContent?.includes(
				"targetMenu.appliedQuery: failed-login cache",
			),
		).toBe(true);
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
