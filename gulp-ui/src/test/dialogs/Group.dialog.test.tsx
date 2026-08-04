import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import type {
	EventPageCursor,
	EventPixelPage,
} from "@/event-store/EventStore.types";
import { DisplayGroupDialog } from "@/dialogs/Group.dialog";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SOURCE_ID = "group-source" as Source.Id;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			app: {
				general: { user: null },
				timeline: { dialogSize: 2_000 },
			},
			Info: {
				setTimelineTarget: () => undefined,
				setDialogSize: () => undefined,
				user_set_data: async () => undefined,
				query_single_id: async () => null,
			},
			spawnDialog: () => undefined,
			currentDocument: globalThis.document,
		}),
	},
}));

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		notes: [],
		subscribeOverlays: () => () => undefined,
		getOverlaysSnapshot: () => 0,
	},
}));

jest.mock("@tanstack/react-virtual", () => ({
	useVirtualizer: ({ count }: { count: number }) => ({
		getTotalSize: () => count * 44,
		getVirtualItems: () =>
			Array.from({ length: count }, (_, index) => ({
				index,
				key: index,
				size: 44,
				start: index * 44,
			})),
	}),
}));

jest.mock("@/dialogs/Event.dialog", () => ({
	DisplayEventDialog: (): null => null,
	EventIndicator: ({ event }: { event: Doc.Type }) => (
		<span data-event-id={event._id}>{event._id}</span>
	),
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: { operationId: () => null },
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {
			color: () => "#ffffff",
			icon: () => "Note",
		},
	},
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: { toNanos: () => 0n },
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({
			t: (key: string, values?: Record<string, string | number>) => {
				if (key === "group.eventsChangedRefresh") {
					return "Events changed while loading. The first page has been refreshed.";
				}
				return values ? `${key}:${JSON.stringify(values)}` : key;
			},
		}),
	},
}));

jest.mock("@/ui/Dialog", () => ({
	Dialog: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		icon,
		variant: _variant,
		size: _size,
		loading: _loading,
		shape: _shape,
		rounded: _rounded,
		revert: _revert,
		asChild: _asChild,
		placeholder: _placeholder,
		...props
	}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
		icon?: string;
		variant?: string;
		size?: string;
		loading?: boolean;
		shape?: string;
		rounded?: boolean;
		revert?: boolean;
		asChild?: boolean;
		placeholder?: string;
	}) => (
		<button
			data-icon={icon}
			{...props}
		>
			{children}
		</button>
	),
}));

jest.mock("@/ui/Icon", () => ({
	Icon: ({ name }: { name: string }) => <span data-icon={name} />,
}));

jest.mock("@/ui/Markdown", () => ({
	Markdown: ({ value }: { value: string }) => <pre>{value}</pre>,
}));

jest.mock("@/ui/Stack", () => {
	const ReactModule = require("react") as typeof import("react");
	return {
		Stack: ReactModule.forwardRef(
			(
				{
					children,
					gap: _gap,
					dir: _dir,
					pos: _pos,
					jc: _jc,
					ai: _ai,
					ac: _ac,
					flex: _flex,
					noShrink: _noShrink,
					...props
				}: React.HTMLAttributes<HTMLDivElement> & {
					gap?: unknown;
					dir?: unknown;
					pos?: unknown;
					jc?: unknown;
					ai?: unknown;
					ac?: unknown;
					flex?: unknown;
					noShrink?: boolean;
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

/**
 * Creates one minimal timeline event for group-page assertions.
 * @param id Stable event identity.
 * @param timestamp Timestamp and deterministic event code.
 * @returns Minimal event document.
 */
function createEvent(id: string, timestamp: number): Doc.Type {
	return {
		_id: id as Doc.Id,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": timestamp,
		gulp_timestamp: timestamp,
		number_hash: timestamp,
	};
}

/**
 * Creates a page cursor carrying its deterministic next-page number as timestamp.
 * @param pageNumber Page number addressed by the cursor.
 * @param sourceRevision Source revision expected by the read.
 * @returns Stable EventStore cursor fixture.
 */
function createCursor(
	pageNumber: number,
	sourceRevision: number,
): EventPageCursor {
	return {
		sourceId: SOURCE_ID,
		sourceRevision,
		timestamp: pageNumber,
		documentId: `cursor-${pageNumber}` as Doc.Id,
	};
}

/**
 * Creates a bounded EventStore page fixture.
 * @param events Events returned for the page.
 * @param total Total exact events in the pixel bucket.
 * @param sourceRevision Revision used to detect concurrent source changes.
 * @param nextPageNumber Optional next page encoded in the cursor.
 * @returns Complete EventStore page.
 */
function createPage(
	events: Doc.Type[],
	total: number,
	sourceRevision: number,
	nextPageNumber?: number,
): EventPixelPage {
	return {
		events,
		total,
		sourceRevision,
		nextCursor:
			nextPageNumber === undefined
				? null
				: createCursor(nextPageNumber, sourceRevision),
	};
}

/**
 * Flushes immediately resolved loader promises and their React updates.
 * @returns Promise settled after several microtask turns.
 */
async function flushPageLoad(): Promise<void> {
	await Promise.resolve();
	await Promise.resolve();
	await Promise.resolve();
}

describe("DisplayGroupDialog EventStore pagination", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("preserves the existing static event-list call shape", () => {
		act(() => {
			root.render(
				<DisplayGroupDialog
					events={[createEvent("static-a", 1), createEvent("static-b", 2)]}
					groupKind="links"
				/>,
			);
		});

		expect(container.querySelectorAll("[data-event-id]")).toHaveLength(2);
		expect(
			container.querySelector('button[title="tableView.nextPage"]'),
		).toBeNull();
	});

	it("retains at most 100 events and navigates next and previous pages", async () => {
		const oversizedPage = createPage(
			Array.from({ length: 105 }, (_, index) =>
				createEvent(`first-${index}`, index),
			),
			106,
			1,
			2,
		);
		const nextCursor = oversizedPage.nextCursor;
		if (!nextCursor) throw new Error("Expected a next-page cursor fixture");
		const loadCalls: Array<EventPageCursor | undefined> = [];
		const loadPage = async (
			cursor?: EventPageCursor,
		): Promise<EventPixelPage> => {
			loadCalls.push(cursor);
			return createPage([createEvent("second-page", 200)], 106, 1);
		};

		act(() => {
			root.render(
				<DisplayGroupDialog
					mode="event-store"
					initialPage={oversizedPage}
					loadPage={loadPage}
				/>,
			);
		});
		expect(container.querySelectorAll("[data-event-id]")).toHaveLength(100);

		const nextButton = container.querySelector<HTMLButtonElement>(
			'button[title="tableView.nextPage"]',
		);
		await act(async () => {
			nextButton?.click();
			await flushPageLoad();
		});

		expect(loadCalls).toEqual([nextCursor]);
		expect(
			container.querySelector('[data-event-id="second-page"]') !== null,
		).toBe(true);

		const previousButton = container.querySelector<HTMLButtonElement>(
			'button[title="tableView.previousPage"]',
		);
		act(() => previousButton?.click());
		expect(container.querySelectorAll("[data-event-id]")).toHaveLength(100);
		expect(loadCalls).toEqual([nextCursor]);
	});

	it("refreshes page one and announces a revision change during next", async () => {
		const initialPage = createPage([createEvent("old-first", 1)], 2, 4, 2);
		const nextCursor = initialPage.nextCursor;
		if (!nextCursor) throw new Error("Expected a next-page cursor fixture");
		const loadCalls: Array<EventPageCursor | undefined> = [];
		const loadPage = async (
			cursor?: EventPageCursor,
		): Promise<EventPixelPage> => {
			loadCalls.push(cursor);
			return cursor
				? createPage([createEvent("stale-next", 2)], 2, 5)
				: createPage([createEvent("fresh-first", 3)], 1, 5);
		};

		act(() => {
			root.render(
				<DisplayGroupDialog
					mode="event-store"
					initialPage={initialPage}
					loadPage={loadPage}
				/>,
			);
		});

		await act(async () => {
			container
				.querySelector<HTMLButtonElement>(
					'button[title="tableView.nextPage"]',
				)
				?.click();
			await flushPageLoad();
		});

		expect(loadCalls).toEqual([nextCursor, undefined]);
		expect(
			container.querySelector('[data-event-id="fresh-first"]') !== null,
		).toBe(true);
		expect(container.querySelector('[data-event-id="stale-next"]')).toBeNull();
		expect(container.querySelector('[role="status"]')?.textContent).toBe(
			"Events changed while loading. The first page has been refreshed.",
		);
	});

	it("reloads evicted history while retaining at most twenty document pages", async () => {
		const totalPages = 21;
		const loadPage = async (
			cursor?: EventPageCursor,
		): Promise<EventPixelPage> => {
			const pageNumber = cursor?.timestamp ?? 1;
			return createPage(
				[createEvent(`page-${pageNumber}`, pageNumber)],
				totalPages,
				1,
				pageNumber < totalPages ? pageNumber + 1 : undefined,
			);
		};

		act(() => {
			root.render(
				<DisplayGroupDialog
					mode="event-store"
					initialPage={createPage(
						[createEvent("page-1", 1)],
						totalPages,
						1,
						2,
					)}
					loadPage={loadPage}
				/>,
			);
		});

		for (let pageNumber = 2; pageNumber <= totalPages; pageNumber++) {
			await act(async () => {
				container
					.querySelector<HTMLButtonElement>(
						'button[title="tableView.nextPage"]',
					)
					?.click();
				await flushPageLoad();
			});
		}

		for (let step = 0; step < 19; step++) {
			act(() => {
				container
					.querySelector<HTMLButtonElement>(
						'button[title="tableView.previousPage"]',
					)
					?.click();
			});
		}

		expect(
			container.querySelector('[data-event-id="page-2"]') !== null,
		).toBe(true);
		expect(
			container.querySelector<HTMLButtonElement>(
				'button[title="tableView.previousPage"]',
			)?.disabled,
		).toBe(false);

		await act(async () => {
			container
				.querySelector<HTMLButtonElement>(
					'button[title="tableView.previousPage"]',
				)
				?.click();
			await flushPageLoad();
		});

		expect(
			container.querySelector('[data-event-id="page-1"]') !== null,
		).toBe(true);
		expect(
			container.querySelector<HTMLButtonElement>(
				'button[title="tableView.previousPage"]',
			)?.disabled,
		).toBe(true);
	});
});
