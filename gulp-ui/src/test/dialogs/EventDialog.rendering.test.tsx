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
			id: () => ({
				id: SOURCE_ID,
				operation_id: "event-dialog-operation",
				settings: { offset: 0 },
			}),
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
}));

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
		json: null,
		loading: true,
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

jest.mock("@/ui/Markdown", () => ({
	Markdown: (): null => null,
}));

jest.mock("react-json-view-lite", () => ({
	JsonView: (): null => null,
	allExpanded: () => true,
	darkStyles: {},
	defaultStyles: {},
}));

jest.mock("@/ui/Popover", () => ({
	Popover: {
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
});
