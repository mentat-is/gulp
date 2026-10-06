import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SOURCE_ID = "event-dialog-source" as Source.Id;
const mockSetTimelineTarget = jest.fn();
const mockSpawnDialog = jest.fn();
const mockSpawnBanner = jest.fn();
const mockMainEventActionRequests: unknown[] = [];
let mockShouldDelegateEventAction = false;
const mockRequestMainEventAction = (payload: unknown): boolean => {
	mockMainEventActionRequests.push(payload);
	return mockShouldDelegateEventAction;
};
const mockSetEventJson = jest.fn();
let mockDetachedEventDetail: Doc.Type | null = null;
let mockNavigationMounts = 0;
let mockNavigationUnmounts = 0;
let mockEventDetailJson: Record<string, unknown> | null = null;
let mockEventKeys: Record<string, string> = {};
let mockSourceAvailable = true;
let mockOrderedEventFields: string[] | null = null;
let mockHiddenEventFields = new Set<string>();
let mockFieldVisibilityMenuProps: Record<string, unknown> | null = null;
const mockEventKeyRequests: unknown[] = [];
const mockTableConfigCalls: Array<{
	persistId: string | undefined;
	columns: string[];
	enabled: boolean;
	schemaPolicy: string | undefined;
}> = [];
const mockConfiguredViewCalls: Array<{
	json: Record<string, unknown>;
	orderedFields: readonly string[];
	hiddenFields: ReadonlySet<string>;
}> = [];
const mockCopiedJson: string[] = [];
const mockDownloadedJson: Array<[string, string, string]> = [];

const mockApp = {
	target: { files: [] },
	timeline: { renderVersion: 0, target: null },
} as unknown as App.Type;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			Info: {
				query_single_id: jest.fn(),
				setTimelineTarget: mockSetTimelineTarget,
				event_keys: async (source: unknown) => {
					mockEventKeyRequests.push(source);
					if (mockEventDetailJson === null) {
						return await new Promise<Record<string, string>>(() => undefined);
					}
					return mockEventKeys;
				},
			},
			app: mockApp,
			spawnBanner: mockSpawnBanner,
			spawnDialog: mockSpawnDialog,
			currentDocument: globalThis.document,
			detachedEventDetail: mockDetachedEventDetail,
			requestMainEventAction: mockRequestMainEventAction,
		}),
	},
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Slot: {
			SendData: "send-data",
			EventActions: "event-actions",
		},
		use: () => ({ extensions: [] }),
		hasSlot: () => false,
		getBySlot: () => [],
		Components: (): null => null,
		Component: (): null => null,
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string) => key }),
	},
}));

jest.mock("next-themes", () => ({
	useTheme: () => ({ theme: "classic-dark" }),
}));

jest.mock("@/banners/Enrichment.banner", () => ({
	Enrichment: { Banner: (): null => null },
}));

jest.mock("@/banners/Collab.functionality", () => ({
	LinkFunctionality: {
		Create: { Banner: (): null => null },
		Connect: { Banner: (): null => null },
	},
	NoteFunctionality: {
		Create: { Banner: (): null => null },
	},
}));

jest.mock("@/banners/FilterFile.banner", () => ({
	FilterFileBanner: (): null => null,
}));

jest.mock("@/banners/SendData.banner", () => ({
	SendData: { Banner: (): null => null },
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: () =>
				mockSourceAvailable
					? {
						id: SOURCE_ID,
						operation_id: "event-dialog-operation",
						settings: { offset: 0 },
					}
					: undefined,
		},
	},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			notes: () => [],
			links: () => [],
			operationId: () => "event-dialog-operation",
			flag: {
				isFlagged: () => false,
				isLimitReached: () => false,
				getList: () => [],
				toggle: () => false,
			},
		},
	},
}));

jest.mock("@/entities/Filter", () => ({
	Filter: { Entity: { createCondition: jest.fn() } },
}));

jest.mock("@/entities/Note", () => ({
	Note: {},
}));

jest.mock("@/entities/Link", () => ({
	Link: {
		Entity: { icon: () => "GitPullRequestCreate" },
		Delete: { Banner: (): null => null },
	},
}));

jest.mock("@/utils", () => ({
	resolveEventRenderColor: () => undefined,
	normalizeEventFieldPath: (field: string) =>
		field
			.split(".")
			.filter((part) => !/^\d+$/.test(part))
			.join("."),
	getEventLeafFieldPaths: (json: Record<string, unknown>) =>
		Object.keys(json).sort(),
	buildConfiguredEventFieldViews: (
		json: Record<string, unknown>,
		orderedFields: readonly string[],
		hiddenFields: ReadonlySet<string>,
	) => {
		mockConfiguredViewCalls.push({ json, orderedFields, hiddenFields });
		const entries = orderedFields
			.filter(
				(field) =>
					Object.prototype.hasOwnProperty.call(json, field) &&
					!hiddenFields.has(field),
			)
			.map((field) => [field, json[field]] as const);
		const raw = Object.fromEntries(entries);
		return {
			raw,
			table: entries.map(([key, value]) => ({ key, value: String(value) })),
			tree: raw,
		};
	},
}));

jest.mock("@/hooks/useTableConfig", () => ({
	useTableConfig: (
		persistId: string | undefined,
		columns: string[],
		enabled: boolean,
		schemaPolicy?: string,
	) => {
		mockTableConfigCalls.push({
			persistId,
			columns: [...columns],
			enabled,
			schemaPolicy,
		});
		return {
			hiddenColumns: mockHiddenEventFields,
			orderedColumns: mockOrderedEventFields ?? columns,
			toggleColumn: jest.fn(),
			selectAllColumns: jest.fn(),
			deselectAllColumns: jest.fn(),
			moveColumn: jest.fn(),
			isLoaded: true,
		};
	},
}));

jest.mock("@/ui/FieldVisibilityMenu", () => ({
	FieldVisibilityMenu: (props: Record<string, unknown>) => {
		mockFieldVisibilityMenuProps = props;
		return <button data-field-visibility-trigger="true" />;
	},
}));

jest.mock("@/ui/utils", () => {
	return {
		cn: (...values: unknown[]) => values.filter(Boolean).join(" "),
		copy: (value: string) => mockCopiedJson.push(value),
		download: (value: string, type: string, name: string) =>
			mockDownloadedJson.push([value, type, name]),
		generateUUID: () => "event-dialog-test-uuid",
		isPlainObject: (value: unknown) =>
			Object.prototype.toString.call(value) === "[object Object]",
	};
});

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		subscribeOverlays: () => () => undefined,
		getOverlaysSnapshot: () => 0,
		Events: {
			getByIds: async () => [],
		},
	},
}));

jest.mock("@/lib/EventJsonTransforms", () => ({
	EventJsonTransforms: {
		subscribe: () => () => undefined,
		hasAny: () => false,
		apply: () => null,
	},
}));

jest.mock("@/dialogs/useEventDetail", () => ({
	useEventDetail: () => ({
		json: mockEventDetailJson,
		loading: mockEventDetailJson === null,
		error: null,
		setJson: mockSetEventJson,
		retry: jest.fn(),
	}),
}));

jest.mock("@/dialogs/useEventDetailView", () => ({
	useEventDetailViewPreference: () => ["tree", jest.fn()],
}));

jest.mock("@/dialogs/components/navigation", () => {
	const ReactModule = require("react") as typeof import("react");
	return {
		Navigation: ({ event }: { event: Doc.Type }) => {
			ReactModule.useEffect(() => {
				mockNavigationMounts++;
				return () => {
					mockNavigationUnmounts++;
				};
			}, []);
			return <div data-navigation-event={event._id} />;
		},
	};
});

jest.mock("@/dialogs/Group.dialog", () => ({
	DisplayGroupDialog: (): null => null,
}));

jest.mock("@/components/CollabList", () => ({
	Collab: { List: (): null => null },
}));

jest.mock("@/ui/ContextMenu", () => ({
	ContextMenu: ({ children }: { children: ReactNode }) => <>{children}</>,
	ContextMenuTrigger: ({ children }: { children: ReactNode }) => (
		<>{children}</>
	),
	ContextMenuContent: ({ children }: { children: ReactNode }) => (
		<div>{children}</div>
	),
	ContextMenuItem: ({ children }: { children: ReactNode }) => (
		<button>{children}</button>
	),
}));

jest.mock("@/ui/Tabs", () => ({
	Tabs: ({ children }: { children: ReactNode }) => <div>{children}</div>,
	TabsList: ({ children }: { children: ReactNode }) => <div>{children}</div>,
	TabsTrigger: ({ children }: { children: ReactNode }) => (
		<button>{children}</button>
	),
	TabsContent: ({
		children,
		value,
	}: {
		children: ReactNode;
		value: string;
	}) => <div data-tab-value={value}>{children}</div>,
}));

jest.mock("@/ui/Markdown", () => ({
	Markdown: ({ value }: { value: string }) => (
		<pre data-event-raw-view="true">{value}</pre>
	),
}));

jest.mock("@/components/Table", () => ({
	Table: ({ values }: { values: unknown[] }) => (
		<div data-event-table-view="true">{JSON.stringify(values)}</div>
	),
}));

jest.mock("react-json-view-lite", () => ({
	JsonView: ({ data }: { data: unknown }) => (
		<div data-event-tree-view="true">{JSON.stringify(data)}</div>
	),
	allExpanded: () => true,
	darkStyles: {},
	defaultStyles: {},
}));

jest.mock("@/ui/Popover", () => ({
	Popover: {
		Anchor: (): null => null,
		Root: ({ children }: { children: ReactNode }) => <>{children}</>,
		Trigger: ({ children }: { children: ReactNode }) => <>{children}</>,
		Content: ({ children }: { children: ReactNode }) => <div>{children}</div>,
	},
}));

/** Creates a minimal event summary accepted by the Event Dialog. */
function createEvent(id: string): Doc.Type {
	return {
		_id: id as Doc.Id,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": 1,
		gulp_timestamp: 1,
		number_hash: 1,
	} as Doc.Type;
}

describe("Event Dialog rendering", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockNavigationMounts = 0;
		mockNavigationUnmounts = 0;
		mockSetTimelineTarget.mock.calls.splice(0);
		mockSpawnDialog.mock.calls.splice(0);
		mockSpawnBanner.mock.calls.splice(0);
		mockMainEventActionRequests.splice(0);
		mockShouldDelegateEventAction = false;
		mockSetEventJson.mock.calls.splice(0);
		mockDetachedEventDetail = null;
		mockEventDetailJson = null;
		mockEventKeys = {};
		mockSourceAvailable = true;
		mockOrderedEventFields = null;
		mockHiddenEventFields = new Set<string>();
		mockFieldVisibilityMenuProps = null;
		mockEventKeyRequests.splice(0);
		mockTableConfigCalls.splice(0);
		mockConfiguredViewCalls.splice(0);
		mockCopiedJson.splice(0);
		mockDownloadedJson.splice(0);
		mockApp.timeline.target = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("keeps navigation mounted while a newly selected detail is loading", () => {
		const firstEvent = createEvent("event-one");
		const secondEvent = createEvent("event-two");

		act(() => root.render(<DisplayEventDialog event={firstEvent} />));
		expect(mockNavigationMounts).toBe(1);
		expect(mockNavigationUnmounts).toBe(0);

		act(() => root.render(<DisplayEventDialog event={secondEvent} />));
		expect(mockNavigationMounts).toBe(1);
		expect(mockNavigationUnmounts).toBe(0);
		expect(
			container
				.querySelector("[data-navigation-event]")
				?.getAttribute("data-navigation-event"),
		).toBe(secondEvent._id);
	});

	it("restores the timeline target while its event remains visible", () => {
		const event = createEvent("persistent-crosshair-event");
		mockApp.timeline.target = event;

		act(() => root.render(<DisplayEventDialog event={event} />));
		expect(mockSetTimelineTarget).not.toHaveBeenCalled();

		mockApp.timeline.target = null;
		act(() => root.render(<DisplayEventDialog event={event} />));

		expect(mockSetTimelineTarget).toHaveBeenCalledTimes(1);
		expect(mockSetTimelineTarget).toHaveBeenCalledWith(event);
	});

	it("delegates mutating toolbar actions when the Dialog is detached", () => {
		const event = createEvent("detached-action-event");
		mockShouldDelegateEventAction = true;

		act(() => root.render(<DisplayEventDialog event={event} />));
		const noteButton = container.querySelector<HTMLButtonElement>(
			'button[title="eventDialog.createNewNote"]',
		);
		if (!noteButton) throw new Error("Note action button was not rendered");
		act(() => noteButton.click());

		expect(mockMainEventActionRequests).toEqual([{
			eventId: event._id,
			action: { kind: "note-create", initialText: undefined },
		}]);
		expect(mockSpawnBanner).not.toHaveBeenCalled();
	});

	it("keeps the existing local banner path when the Dialog is docked", () => {
		const event = createEvent("docked-action-event");

		act(() => root.render(<DisplayEventDialog event={event} />));
		const noteButton = container.querySelector<HTMLButtonElement>(
			'button[title="eventDialog.createNewNote"]',
		);
		if (!noteButton) throw new Error("Note action button was not rendered");
		act(() => noteButton.click());

		expect(mockMainEventActionRequests).toHaveLength(1);
		expect(mockSpawnBanner).toHaveBeenCalledTimes(1);
	});

	it("applies a refreshed detached enrichment result without remounting", () => {
		const event = createEvent("detached-enriched-event");
		mockDetachedEventDetail = {
			...event,
			"enriched.field": "value",
		} as Doc.Type;

		act(() => root.render(<DisplayEventDialog event={event} />));

		expect(mockSetEventJson).toHaveBeenCalledWith(
			mockDetachedEventDetail as unknown as Record<string, unknown>,
		);
		expect(mockNavigationMounts).toBe(1);
		expect(mockNavigationUnmounts).toBe(0);
	});

	it("wires a source-scoped field configuration into views, copy, and download", async () => {
		const event = createEvent("configured-event");
		mockEventDetailJson = { beta: 2, alpha: 1 };
		mockEventKeys = {
			alpha: "keyword",
			beta: "long",
			delta: "keyword",
		};
		mockOrderedEventFields = ["delta", "alpha", "beta"];
		mockHiddenEventFields = new Set(["beta"]);

		await act(async () => {
			root.render(<DisplayEventDialog event={event} />);
			await Promise.resolve();
		});

		expect(mockEventKeyRequests).toHaveLength(1);
		expect(mockTableConfigCalls.at(-1)).toEqual({
			persistId: `event-dialog-fields-${SOURCE_ID}`,
			columns: ["alpha", "beta", "delta"],
			enabled: true,
			schemaPolicy: "reconcile",
		});
		expect(mockFieldVisibilityMenuProps?.orderedItems).toEqual([
			"delta",
			"alpha",
			"beta",
		]);
		expect(mockFieldVisibilityMenuProps?.hiddenItems).toBe(
			mockHiddenEventFields,
		);
		expect(mockFieldVisibilityMenuProps?.terminology).toBe("fields");
		expect(mockFieldVisibilityMenuProps?.disabled).toBe(false);
		expect(mockConfiguredViewCalls.at(-1)).toEqual({
			json: mockEventDetailJson,
			orderedFields: ["delta", "alpha", "beta"],
			hiddenFields: mockHiddenEventFields,
		});
		expect(
			container.querySelector('[data-event-raw-view="true"]')?.textContent,
		).toContain('"alpha": 1');
		expect(
			container.querySelector('[data-event-raw-view="true"]')?.textContent,
		).not.toContain('"beta"');

		act(() => {
			container
				.querySelector<HTMLButtonElement>(
					'button[title="eventDialog.copyJson"]',
				)
				?.click();
		});
		expect(mockCopiedJson).toEqual([JSON.stringify({ alpha: 1 }, null, 2)]);

		const downloadButton = Array.from(
			container.querySelectorAll<HTMLButtonElement>("button"),
		).find((button) => button.textContent === "eventDialog.downloadJson");
		act(() => downloadButton?.click());
		expect(mockDownloadedJson).toEqual([
			[
				JSON.stringify({ alpha: 1 }, null, 2),
				"application/json",
				`configured-event_from_${SOURCE_ID}.json`,
			],
		]);
	});

	it("uses the current event fields when source metadata is unavailable", async () => {
		const event = createEvent("deleted-source-event");
		mockSourceAvailable = false;
		mockEventDetailJson = { beta: 2, alpha: 1 };

		await act(async () => {
			root.render(<DisplayEventDialog event={event} />);
			await Promise.resolve();
		});

		expect(mockEventKeyRequests).toHaveLength(0);
		expect(mockTableConfigCalls.at(-1)).toEqual({
			persistId: `event-dialog-fields-${SOURCE_ID}`,
			columns: ["alpha", "beta"],
			enabled: true,
			schemaPolicy: "reconcile",
		});
		expect(
			container.querySelector('[data-field-visibility-trigger="true"]'),
		).not.toBeNull();
		expect(container.textContent).not.toContain("eventDialog.sourceDeleted");
	});
});
