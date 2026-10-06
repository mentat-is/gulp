import { act, type ButtonHTMLAttributes, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { TimelineMenu } from "@/app/body/Timeline.menu";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const FIRST_SOURCE_ID = "timeline-menu-selected" as Source.Id;
const FILTERED_SOURCE_ID = "timeline-menu-filtered" as Source.Id;
const UNSELECTED_SOURCE_ID = "timeline-menu-unselected" as Source.Id;
const RESET_QUERY = {
	string: "",
	text_filter: "",
	filters: [],
} as Query.Type;
const mockRefetch = jest.fn(async () => undefined);
const mockStopSourceRequests = jest.fn(async () => undefined);
const mockNavigateSourceQueryCache = jest.fn(async () => true);
const mockLoadings = { byFileId: new Map<Source.Id, string>() };
const defaultQueryCalls: Array<[App.Type, Source.Type]> = [];

/** Records default-query creation while returning the shared reset fixture. */
function mockDefaultQuery(app: App.Type, source: Source.Type): Query.Type {
	defaultQueryCalls.push([app, source]);
	return RESET_QUERY;
}
const mockApp = {
	target: { files: [] as Source.Type[] },
	timeline: { filter: "only-visible-source" },
} as unknown as App.Type;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => ({
			Info: {
				refetch: mockRefetch,
				stopSourceRequests: mockStopSourceRequests,
				navigateSourceQueryCache: mockNavigateSourceQueryCache,
			},
			app: mockApp,
		}),
	},
}));

jest.mock("@/store/request.store", () => ({
	useRequestLoadings: (): typeof mockLoadings => mockLoadings,
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string, values?: { count?: number }) => string } => ({
			t: (key: string, values?: { count?: number }): string =>
				values?.count === undefined ? key : `${key}:${values.count}`,
		}),
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			selected: (app: App.Type): Source.Type[] =>
				app.target.files.filter((source) => source.selected),
		},
	},
}));

jest.mock("@/entities/Filter", () => ({
	Filter: {
		Entity: {
			default: (app: App.Type, source: Source.Type): Query.Type =>
				mockDefaultQuery(app, source),
		},
	},
}));

jest.mock("@/ui/ContextMenu", () => ({
	ContextMenuContent: ({ children }: { children?: ReactNode }) => (
		<div>{children}</div>
	),
	ContextMenuItem: ({
		children,
		icon,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & { icon?: string }) => (
		<button data-icon={icon} {...props}>
			{children}
		</button>
	),
	ContextMenuLabel: ({ children }: { children?: ReactNode }) => (
		<div>{children}</div>
	),
	ContextMenuSeparator: (): null => null,
}));

/** Creates a minimal source fixture for the global menu. */
function createSource(
	id: Source.Id,
	name: string,
	selected = true,
): Source.Type {
	return {
		id,
		name,
		selected,
		queryCache: {
			...createEmptyQueryCacheState(),
			canUndo: true,
			canRedo: true,
		},
	} as Source.Type;
}

/** Finds one mocked context-menu action by its icon. */
function getMenuButton(container: HTMLElement, icon: string): HTMLButtonElement {
	const button = container.querySelector<HTMLButtonElement>(
		`button[data-icon="${icon}"]`,
	);
	if (!button) throw new Error(`Missing menu button with icon ${icon}.`);
	return button;
}

describe("TimelineMenu", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockRefetch.mock.calls.splice(0);
		mockStopSourceRequests.mock.calls.splice(0);
		mockNavigateSourceQueryCache.mock.calls.splice(0);
		mockLoadings.byFileId.clear();
		defaultQueryCalls.splice(0);
		mockApp.target.files = [
			createSource(FIRST_SOURCE_ID, "only-visible-source.log"),
			createSource(FILTERED_SOURCE_ID, "filtered-out-source.log"),
			createSource(UNSELECTED_SOURCE_ID, "unselected-source.log", false),
		];
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	it("stops pending and active work for every selected source", () => {
		act(() => root.render(<TimelineMenu />));
		act(() => getMenuButton(container, "StopCircle").click());

		expect(mockStopSourceRequests).toHaveBeenCalledWith([
			FIRST_SOURCE_ID,
			FILTERED_SOURCE_ID,
		]);
	});

	it("navigates Undo and Redo for every selected source", () => {
		act(() => root.render(<TimelineMenu />));
		act(() => getMenuButton(container, "Undo").click());
		act(() => getMenuButton(container, "Redo").click());

		expect(mockNavigateSourceQueryCache.mock.calls).toEqual([
			[mockApp.target.files[0], "undo"],
			[mockApp.target.files[1], "undo"],
			[mockApp.target.files[0], "redo"],
			[mockApp.target.files[1], "redo"],
		]);
	});

	it("disables bulk cache navigation when no selected source is eligible", () => {
		mockApp.target.files = mockApp.target.files.map((source) => ({
			...source,
			queryCache: {
				...source.queryCache,
				canUndo: false,
				canRedo: false,
			},
		}));
		act(() => root.render(<TimelineMenu />));

		expect(getMenuButton(container, "Undo").disabled).toBe(true);
		expect(getMenuButton(container, "Redo").disabled).toBe(true);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("refetches every selected source through initial cache reconciliation", () => {
		act(() => root.render(<TimelineMenu />));

		expect(
			container.textContent?.includes("source.selectedSources:2"),
		).toBe(true);
		act(() => getMenuButton(container, "RefreshClockwise").click());

		expect(mockRefetch).toHaveBeenCalledWith({
			ids: [FIRST_SOURCE_ID, FILTERED_SOURCE_ID],
			cachePolicy: "initial",
		});
	});

	it("resets filters with one authoritative bulk refetch", () => {
		act(() => root.render(<TimelineMenu />));
		act(() => getMenuButton(container, "X").click());

		expect(defaultQueryCalls).toEqual([
			[mockApp, mockApp.target.files[0]],
		]);
		expect(mockRefetch).toHaveBeenCalledWith({
			ids: [FIRST_SOURCE_ID, FILTERED_SOURCE_ID],
			query: RESET_QUERY,
			cachePolicy: "force",
		});
	});

	it("renders no menu actions without selected sources", () => {
		mockApp.target.files = [];
		act(() => root.render(<TimelineMenu />));

		expect(container.querySelector("button")).toBeNull();
	});
});
