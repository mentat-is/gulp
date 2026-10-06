import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { WindowBridge } from "@/lib/WindowBridge";
import { Navigation } from "@/dialogs/components/navigation";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SOURCE_ID = "navigation-source" as Source.Id;
const mockSpawnDialog = jest.fn();
const mockSelectDetachedDialogEvent = jest.fn();
let mockSourceEvents: Doc.Type[] = [];
const mockGetEvents = jest.fn<void, [App.Type, Source.Id]>();
const mockGetNeighbors = jest.fn<
	void,
	[Source.Id, Doc.Id, number, number]
>();
let mockNeighborResult: {
	events: Doc.Type[];
	currentIndex: number;
	sourceRevision: number;
} = { events: [], currentIndex: -1, sourceRevision: 0 };
let mockDetached = true;
let mockNavigation: WindowBridge.DetachedDialogNavigation | null = null;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			app: {
				target: { files: [] },
				timeline: { renderVersion: 0 },
			},
			spawnDialog: mockSpawnDialog,
			isDetachedWindow: mockDetached,
			detachedDialogNavigation: mockNavigation,
			selectDetachedDialogEvent: mockSelectDetachedDialogEvent,
		}),
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: () => ({ id: SOURCE_ID }),
		},
	},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			get: (app: App.Type, sourceId: Source.Id) => {
				mockGetEvents(app, sourceId);
				return mockSourceEvents;
			},
		},
	},
}));

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		subscribeDocuments: () => () => undefined,
		getDocumentsSnapshot: () => 0,
		getSourceRevision: () => 0,
		Events: {
			getNeighbors: (
				sourceId: Source.Id,
				documentId: Doc.Id,
				before: number,
				after: number,
			) => {
				mockGetNeighbors(sourceId, documentId, before, after);
				return Promise.resolve(mockNeighborResult);
			},
		},
	},
}));

jest.mock("@/dialogs/Event.dialog", () => ({
	DisplayEventDialog: (): null => null,
	EventIndicator: ({
		event,
		onClick,
	}: {
		event: Doc.Type;
		onClick?: () => void;
	}) => (
		<button
			data-event-id={event._id}
			onClick={onClick}
		>
			{event.indicatorValue ?? event["gulp.event_code"]}
		</button>
	),
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string) => key }),
	},
}));

jest.mock("@/ui/utils", () => ({
	cn: (...values: Array<string | false | null | undefined>) =>
		values.filter(Boolean).join(" "),
}));

jest.mock("@/ui/Stack", () => {
	const ReactModule = require("react") as typeof import("react");
	return {
	Stack: ReactModule.forwardRef(
		(
			{
				children,
				jc: _jc,
				gap: _gap,
				flex: _flex,
				ai: _ai,
				dir: _dir,
				...props
			}: {
				children?: ReactNode;
				jc?: unknown;
				gap?: unknown;
				flex?: unknown;
				ai?: unknown;
				dir?: unknown;
				[key: string]: unknown;
			},
			ref: React.ForwardedRef<HTMLDivElement>,
		) => (
			<div
				ref={ref}
				{...props}
			>
				{children}
			</div>
		),
	),
	};
});

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		...props
	}: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
		<button {...props}>{children}</button>
	),
}));

function createEvent(id: string, timestamp: number): Doc.Type {
	return {
		_id: id as Doc.Id,
		"gulp.source_id": SOURCE_ID,
		indicatorValue: timestamp,
		gulp_timestamp: timestamp,
		number_hash: timestamp,
	} as Doc.Type;
}

/** Returns the fixed-width indicator strip between the navigation arrows. */
function getNavigationStrip(container: HTMLElement): HTMLElement {
	const navigation = container.firstElementChild;
	const strip = navigation?.children[1];
	if (!(strip instanceof HTMLElement)) {
		throw new Error("Missing navigation indicator strip.");
	}
	return strip;
}

describe("Event Dialog navigation", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockSpawnDialog.mock.calls.splice(0);
		mockSelectDetachedDialogEvent.mock.calls.splice(0);
		mockGetEvents.mock.calls.splice(0);
		mockGetNeighbors.mock.calls.splice(0);
		mockNeighborResult = {
			events: [],
			currentIndex: -1,
			sourceRevision: 0,
		};
		mockSourceEvents = [];
		mockDetached = true;
		mockNavigation = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("renders the private detached window and selects events by ID", () => {
		const events = Array.from({ length: 11 }, (_, index) =>
			createEvent(`event-${index}`, index),
		);
		const currentEvent = events[5];
		mockNavigation = {
			currentEventId: currentEvent._id,
			items: events.map((event) => ({
				id: event._id,
				sourceId: event["gulp.source_id"],
				timestamp: event.gulp_timestamp,
				indicatorValue: event.indicatorValue ?? 0,
				numberHash: event.number_hash,
				hasNotes: false,
				hasLinks: false,
			})),
			previousEventId: events[4]._id,
			nextEventId: events[6]._id,
		};

		act(() => root.render(<Navigation event={currentEvent} />));
		expect(container.querySelectorAll("[data-event-id]")).toHaveLength(11);
		expect(mockGetEvents).not.toHaveBeenCalled();

		act(() => {
			container
				.querySelector<HTMLButtonElement>('[data-event-id="event-7"]')
				?.click();
		});
		expect(
			(
				mockSelectDetachedDialogEvent.mock.calls as unknown as Array<
					[Doc.Id]
				>
			).at(-1)?.[0],
		).toBe(events[7]._id);

		const nextButton = [...container.querySelectorAll("button")].find(
			(button) => button.title === "eventDialog.nextEvent",
		);
		act(() => nextButton?.click());
		expect(
			(
				mockSelectDetachedDialogEvent.mock.calls as unknown as Array<
					[Doc.Id]
				>
			).at(-1)?.[0],
		).toBe(events[6]._id);
		expect(mockSpawnDialog).not.toHaveBeenCalled();
	});

	it("reconstructs compact custom labels in detached navigation", () => {
		const currentEvent: Doc.Type = {
			...createEvent("custom-current", 10),
			indicatorValue: "cust",
		};
		mockNavigation = {
			currentEventId: currentEvent._id,
			items: [
				{
					id: currentEvent._id,
					sourceId: currentEvent["gulp.source_id"],
					timestamp: currentEvent.gulp_timestamp,
					indicatorValue: "cust",
					numberHash: currentEvent.number_hash,
					hasNotes: false,
					hasLinks: false,
				},
			],
			previousEventId: null,
			nextEventId: null,
		};

		act(() => root.render(<Navigation event={currentEvent} />));
		expect(container.textContent).toContain("cust");
	});

	it("uses the bounded async EventStore navigation path", async () => {
		const newest = createEvent("newest", 3);
		const current = createEvent("current", 2);
		const oldest = createEvent("oldest", 1);
		mockDetached = false;
		mockNeighborResult = {
			events: [newest, current, oldest],
			currentIndex: 1,
			sourceRevision: 1,
		};

		await act(async () => {
			root.render(<Navigation event={current} />);
			await Promise.resolve();
			await Promise.resolve();
		});
		const nextButton = [...container.querySelectorAll("button")].find(
			(button) => button.title === "eventDialog.nextEvent",
		);
		act(() => nextButton?.click());

		expect(mockGetNeighbors).toHaveBeenCalled();
		expect(mockGetNeighbors.mock.calls.at(-1)?.[0]).toBe(SOURCE_ID);
		expect(mockGetNeighbors.mock.calls.at(-1)?.[1]).toBe(current._id);
		const cacheRadius = Math.floor(
			WindowBridge.DIALOG_NAVIGATION_CACHE_WINDOW_SIZE / 2,
		);
		expect(mockGetNeighbors.mock.calls.at(-1)?.[2]).toBe(cacheRadius);
		expect(mockGetNeighbors.mock.calls.at(-1)?.[3]).toBe(cacheRadius);
		const strip = getNavigationStrip(container);
		const visibleCenter = Math.floor(
			WindowBridge.DIALOG_NAVIGATION_VISIBLE_WINDOW_SIZE / 2,
		);
		expect(strip.children).toHaveLength(
			WindowBridge.DIALOG_NAVIGATION_VISIBLE_WINDOW_SIZE,
		);
		expect(strip.children[visibleCenter].getAttribute("data-event-id")).toBe(
			current._id,
		);
		expect(mockGetEvents).not.toHaveBeenCalled();
		expect(mockSpawnDialog).toHaveBeenCalledTimes(1);
		expect(mockSelectDetachedDialogEvent).not.toHaveBeenCalled();
	});

	it("reuses the prefetched window until the visible strip reaches its edge", async () => {
		const events = Array.from({ length: 51 }, (_, index) =>
			createEvent(`event-${index}`, 51 - index),
		);
		mockDetached = false;
		mockNeighborResult = {
			events,
			currentIndex: 25,
			sourceRevision: 1,
		};

		await act(async () => {
			root.render(<Navigation event={events[25]} />);
			await Promise.resolve();
			await Promise.resolve();
		});
		expect(mockGetNeighbors).toHaveBeenCalledTimes(1);

		await act(async () => {
			root.render(<Navigation event={events[5]} />);
			await Promise.resolve();
		});
		expect(mockGetNeighbors).toHaveBeenCalledTimes(1);
		expect(
			getNavigationStrip(container).children[5].getAttribute("data-event-id"),
		).toBe(events[5]._id);

		mockNeighborResult = {
			events: events.slice(0, 30),
			currentIndex: 4,
			sourceRevision: 1,
		};
		await act(async () => {
			root.render(<Navigation event={events[4]} />);
			await Promise.resolve();
			await Promise.resolve();
		});
		expect(mockGetNeighbors).toHaveBeenCalledTimes(2);
		expect(mockGetNeighbors.mock.calls.at(-1)?.[1]).toBe(events[4]._id);
	});

	for (const [edge, currentIndex] of [
		["first", 2],
		["last", 0],
	] as const) {
		it(`keeps the ${edge} source event in the visual center`, async () => {
			const events = [
				createEvent("newest", 3),
				createEvent("middle", 2),
				createEvent("oldest", 1),
			];
			const current = events[currentIndex];
			mockDetached = false;
			mockNeighborResult = {
				events,
				currentIndex,
				sourceRevision: 1,
			};

			await act(async () => {
				root.render(<Navigation event={current} />);
				await Promise.resolve();
				await Promise.resolve();
			});

			const strip = getNavigationStrip(container);
			const visibleCenter = Math.floor(
				WindowBridge.DIALOG_NAVIGATION_VISIBLE_WINDOW_SIZE / 2,
			);
			expect(strip.children).toHaveLength(
				WindowBridge.DIALOG_NAVIGATION_VISIBLE_WINDOW_SIZE,
			);
			expect(
				strip.children[visibleCenter].getAttribute("data-event-id"),
			).toBe(current._id);
		});
	}
});
