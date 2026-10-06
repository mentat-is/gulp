import {
	act,
	isValidElement,
	type MouseEvent as ReactMouseEvent,
	type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import type { Note } from "@/entities/Note";
import type { Link } from "@/entities/Link";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import type { User } from "@/entities/User";
import { DataStore } from "@/store/DataStore";
import { NotesLinksWIndows } from "@/components/NotesLinksWIndows";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockTableRow extends Record<string, unknown> {
	id: Note.Id | Link.Id;
	title: string;
	rawNote?: Note.Type;
	rawLink?: Link.Type;
	originEventId?: Doc.Id;
}

interface MockTableProps {
	values: MockTableRow[];
	columns?: Array<{
		key: string;
		render?: (value: unknown, row: MockTableRow) => ReactNode;
	}>;
	actions?: Array<{
		label: string;
		onClick: (
			row: MockTableRow,
			index: number,
			event: ReactMouseEvent<HTMLButtonElement>,
		) => void;
	}>;
	selectable?: boolean;
	selectedrows?: Set<number>;
	onrowselect?: (index: number, selected: boolean) => void;
	onSelectAll?: (selected: boolean) => void;
}

interface MockChildrenProps {
	children?: ReactNode;
}

interface MockCheckboxProps {
	id?: string;
	checked?: boolean;
	onCheckedChange?: (checked: boolean) => void;
}

interface MockButtonProps extends MockChildrenProps {
	disabled?: boolean;
	icon?: string;
	onClick?: () => void;
	title?: string;
}

interface MockInputProps {
	value?: string;
	onChange?: (event: { target: { value: string } }) => void;
	placeholder?: string;
}

const OPERATION_ID = "notes-behavior-operation" as Operation.Id;
const CONTEXT_ID = "notes-behavior-context" as Context.Id;
const SOURCE_ALPHA_ID = "notes-behavior-source-alpha" as Source.Id;
const SOURCE_BETA_ID = "notes-behavior-source-beta" as Source.Id;
const USER_ID = "notes-behavior-user" as User.Id;
const GLYPH_ID = "notes-behavior-glyph" as Glyph.Id;

let mockApp: App.Type;
let mockDetachedStatus:
	| "active"
	| "initializing"
	| "idle"
	| "auth_lost" = "active";
let mockTranslate = (key: string): string => key;
const mockSpawnBanner = jest.fn<void, [ReactNode, string?]>();
const mockSendControlMessage = jest.fn<
	void,
	[string, { docId: Doc.Id; operationId: Operation.Id }, string | undefined]
>();
const mockNotesListCalls: Note.ListFilter[] = [];
const mockNotesCountCalls: Note.ListFilter[] = [];
const mockLinksListCalls: Link.ListFilter[] = [];
const mockLinksCountCalls: Link.ListFilter[] = [];
let mockFlaggedDocIds = new Set<Doc.Id>();
const mockInvalidateFlagCache = jest.fn();
const mockDestroyBanner = jest.fn();
let mockEventStoreResults: Doc.Type[] = [];
const mockGetEventsByIdsCalls: Array<readonly Doc.Id[]> = [];

/** Returns configured event-store rows while recording requested IDs. */
async function mockGetEventsByIds(
	documentIds: readonly Doc.Id[],
): Promise<Doc.Type[]> {
	mockGetEventsByIdsCalls.push(documentIds);
	return mockEventStoreResults;
}
const originalGetEventsByIds = DataStore.Events.getByIds;
let mockNotesListImplementation = async (
	_filter: Note.ListFilter,
	_signal?: AbortSignal,
): Promise<Note.Type[]> => [];
let mockNotesCountImplementation = async (
	_filter: Note.ListFilter,
	_signal?: AbortSignal,
): Promise<number> => 0;
let mockLinksListImplementation = async (
	_filter: Link.ListFilter,
	_signal?: AbortSignal,
): Promise<Link.Type[]> => [];
let mockLinksCountImplementation = async (
	_filter: Link.ListFilter,
	_signal?: AbortSignal,
): Promise<number> => 0;

/**
 * Records and resolves one mocked Notes list request.
 * @param filter Requested backend filter.
 * @param signal Optional cancellation signal.
 * @returns Configured per-test note page.
 */
async function mockNotesList(
	filter: Note.ListFilter,
	signal?: AbortSignal,
): Promise<Note.Type[]> {
	mockNotesListCalls.push(filter);
	return mockNotesListImplementation(filter, signal);
}

/**
 * Records and resolves one mocked Notes count request.
 * @param filter Requested backend filter.
 * @param signal Optional cancellation signal.
 * @returns Configured per-test note count.
 */
async function mockNotesCount(
	filter: Note.ListFilter,
	signal?: AbortSignal,
): Promise<number> {
	mockNotesCountCalls.push(filter);
	return mockNotesCountImplementation(filter, signal);
}

/** Records and resolves one mocked Links list request. */
async function mockLinksList(
	filter: Link.ListFilter,
	signal?: AbortSignal,
): Promise<Link.Type[]> {
	mockLinksListCalls.push(filter);
	return mockLinksListImplementation(filter, signal);
}

/** Records and resolves one mocked Links count request. */
async function mockLinksCount(
	filter: Link.ListFilter,
	signal?: AbortSignal,
): Promise<number> {
	mockLinksCountCalls.push(filter);
	return mockLinksCountImplementation(filter, signal);
}

const mockInfo = {
	notes_list: mockNotesList,
	notes_count: mockNotesCount,
	links_list: mockLinksList,
	links_count: mockLinksCount,
	notes_delete_bulk: jest.fn(),
	links_delete_bulk: jest.fn(),
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		/** Returns the mutable application fixture used by each rerender. */
		use: (): unknown => ({
			Info: mockInfo,
			app: mockApp,
			spawnBanner: mockSpawnBanner,
			destroyBanner: mockDestroyBanner,
			banner: null,
			detachedStatus: mockDetachedStatus,
			setDetachedNotesRenderedSync: () => undefined,
		}),
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		/** Returns the replaceable translation function used to emulate theme/provider rerenders. */
		use: (): { t: (key: string) => string } => ({
			t: mockTranslate,
		}),
	},
}));

jest.mock("sonner", () => ({
	toast: { error: jest.fn() },
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			flag: {
				KEY: "flagged-events",
				getList: (): Set<Doc.Id> => new Set(mockFlaggedDocIds),
				invalidateCache: (): void => mockInvalidateFlagCache(),
			},
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			/** Fallback for tests that omit the explicit rendered-source snapshot. */
			rendered: (): never[] => [],
		},
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {
			/** Returns a deterministic row color. */
			color: (): string => "orange",
			/** Returns a deterministic row icon. */
			icon: (): string => "StickyNote",
		},
		Delete: {
			Banner: (): null => null,
		},
	},
}));

jest.mock("@/entities/Link", () => ({
	Link: {
		Entity: {
			/** Returns a deterministic row icon. */
			icon: (): string => "Link",
		},
		Delete: {
			Banner: (): null => null,
		},
	},
}));

jest.mock("@/dialogs/Group.dialog", () => ({
	DisplayGroupDialog: ({
		events,
		onClose,
		onSelectEvent,
	}: {
		events: Doc.Type[];
		onClose?: () => void;
		onSelectEvent?: (event: Doc.Type) => void;
	}): ReactNode =>
		<div data-testid="linked-events-dialog">
			{events.map((event, index) => (
				<button
					type="button"
					key={event._id}
					data-related-event-id={event._id}
					onClick={() => {
						onSelectEvent?.(event);
						onClose?.();
					}}
				>
					{index > 0 ? "," : ""}{event._id}
				</button>
			))}
		</div>,
}));

jest.mock("@/ui/Select", () => {
	const Wrapper = ({ children }: MockChildrenProps): ReactNode =>
		<>{children}</>;
	const Root = ({
		children,
		value,
		onValueChange,
	}: MockChildrenProps & {
		value?: string;
		onValueChange?: (value: string) => void;
	}): ReactNode =>
		<div data-current-select-value={value}>
			{children}
			{value && ["20", "50", "100"].includes(value) ? (
				[20, 50, 100].map((pageSize) => (
					<button
						type="button"
						key={pageSize}
						data-page-size-option={pageSize}
						onClick={() => onValueChange?.(pageSize.toString())}
					>
						{pageSize}
					</button>
				))
			) : null}
		</div>;
	return {
		Select: {
			Root,
			Trigger: Wrapper,
			Value: (): null => null,
			Content: Wrapper,
			Item: Wrapper,
			Multi: {
				Root: Wrapper,
				Value: (): null => null,
			},
		},
	};
});

jest.mock("@/ui/Stack", () => {
	return {
		Stack: ({ children }: MockChildrenProps): ReactNode =>
			<div>{children}</div>,
	};
});

jest.mock("@/ui/Input", () => {
	return {
		Input: ({
			value,
			onChange,
			placeholder,
		}: MockInputProps): ReactNode =>
			<input
				value={value}
				placeholder={placeholder}
				onChange={onChange}
			/>,
	};
});

jest.mock("@/ui/Banner", () => {
	return {
		Banner: ({
			children,
			done,
		}: MockChildrenProps & { done?: ReactNode }): ReactNode =>
			<div>
				{children}
				{done}
			</div>,
	};
});

jest.mock("@/ui/Checkbox", () => {
	return {
		Checkbox: ({
			id,
			checked,
			onCheckedChange,
		}: MockCheckboxProps): ReactNode =>
			<button
				id={id}
				type="button"
				data-testid="mock-checkbox"
				aria-pressed={Boolean(checked)}
				onClick={() => onCheckedChange?.(!checked)}
			/>,
	};
});

jest.mock("@/ui/Button", () => {
	return {
		Button: ({
			children,
			disabled,
			icon,
			onClick,
			title,
		}: MockButtonProps): ReactNode =>
			<button
				type="button"
				disabled={disabled}
				data-button-icon={icon}
				onClick={onClick}
				title={title}
			>
				{children}
			</button>,
	};
});

jest.mock("@/ui/Toggle", () => ({
	Toggle: ({
		checked,
		onCheckedChange,
	}: {
		checked?: boolean;
		onCheckedChange?: (checked: boolean) => void;
	}): ReactNode =>
		<button
			type="button"
			data-testid="mock-toggle"
			onClick={() => onCheckedChange?.(!checked)}
		/>,
}));

jest.mock("@/ui/Popover", () => {
	const Wrapper = ({ children }: MockChildrenProps): ReactNode =>
		<>{children}</>;
	return {
		Popover: {
			Root: Wrapper,
			Trigger: Wrapper,
			Content: Wrapper,
		},
	};
});

jest.mock("@/ui/Notification", () => {
	return {
		Notification: ({ children }: MockChildrenProps): ReactNode =>
			<div>{children}</div>,
	};
});

jest.mock("@/ui/Icon", () => ({
	Icon: ({ name, color }: { name: string; color?: string }): ReactNode =>
		<i
			data-icon={name}
			data-color={color}
		/>,
}));

jest.mock("@/ui/utils", () => ({
	/** Joins the class names used by Tabs in this isolated component test. */
	cn: (...classes: Array<string | undefined>): string =>
		classes.filter(Boolean).join(" "),
	/** Returns a deterministic timestamp label for the isolated table. */
	formatTimestampToReadableString: (timestamp: number): string =>
		timestamp.toString(),
}));

jest.mock("@/lib/WindowBridge", () => ({
	WindowBridge: {
		sendControlMessage: (
			type: string,
			payload: { docId: Doc.Id; operationId: Operation.Id },
			coordinatorId?: string,
		): void => mockSendControlMessage(type, payload, coordinatorId),
		MessageType: { NAVIGATE_TO_EVENT: "NAVIGATE_TO_EVENT" },
	},
}));

jest.mock("@/components/Table", () => {
	return {
		Table: ({
			values,
			columns,
			actions,
			selectable,
			selectedrows,
			onrowselect,
			onSelectAll,
		}: MockTableProps): ReactNode =>
			<div
				data-testid={values.some((row) => row.rawLink)
					? "links-table"
					: "notes-table"}
			>
				{selectable ? (
					<button
						type="button"
						data-table-select-all
						onClick={() => onSelectAll?.(true)}
					/>
				) : null}
				{values.map((row, rowIndex) => (
					<div
						key={row.id}
						data-note-id={row.rawNote ? row.id : undefined}
						data-link-id={row.rawLink ? row.id : undefined}
					>
						{selectable ? (
							<button
								type="button"
								data-row-select-id={row.id}
								aria-pressed={selectedrows?.has(rowIndex) ?? false}
								onClick={() =>
									onrowselect?.(
										rowIndex,
										!(selectedrows?.has(rowIndex) ?? false),
									)
								}
							/>
						) : null}
						<span>{row.title}</span>
						{columns?.map((column) => (
							<span key={column.key}>
								{column.render
									? column.render(
										row[column.key as keyof MockTableRow],
										row,
									)
									: String(
										row[column.key as keyof MockTableRow] ?? "",
									)}
							</span>
						))}
						{actions?.map((action) => (
							<button
								type="button"
								key={action.label}
								data-action-label={action.label}
								onClick={(event) =>
									action.onClick(row, rowIndex, event)}
							/>
						))}
					</div>
				))}
			</div>,
	};
});

/**
 * Creates a selected source fixture for Notes query tests.
 * @param id Stable source identifier.
 * @param name Display name.
 * @param selected Whether the source participates in the active query.
 * @returns Minimal source fixture accepted by the component.
 */
function createSource(
	id: Source.Id,
	name: string,
	selected: boolean,
): Source.Type {
	return {
		id,
		name,
		selected,
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
	} as Source.Type;
}

/**
 * Creates an application fixture with explicit Canvas-rendered source IDs.
 * @param renderedSourceIds Source IDs currently represented on the Canvas.
 * @returns Minimal application fixture required by NotesLinksWIndows.
 */
function createApp(renderedSourceIds: Source.Id[]): App.Type {
	return {
		target: {
			operations: [{ id: OPERATION_ID, selected: true }],
			files: [
				createSource(SOURCE_ALPHA_ID, "Alpha", true),
				createSource(SOURCE_BETA_ID, "Beta", false),
			],
			contexts: [
				{
					id: CONTEXT_ID,
					name: "Context",
					color: "blue",
				},
			],
		},
		timeline: {
			filter: "",
			renderVersion: 1,
			renderedSourceIds,
			target: null,
		},
		hidden: {
			sourcesWithNoEvents: false,
		},
	} as App.Type;
}

/**
 * Creates a canonical note fixture owned by the requested source.
 * @param id Stable note identifier.
 * @param sourceId Owning source.
 * @param overrides Optional note fields.
 * @returns Complete note fixture used by API and websocket assertions.
 */
function createNote(
	id: string,
	sourceId: Source.Id = SOURCE_ALPHA_ID,
	overrides: Partial<Note.Type> = {},
): Note.Type {
	return {
		id: id as Note.Id,
		type: "note",
		operation_id: OPERATION_ID,
		tags: ["shared"],
		name: "Original title",
		context_id: CONTEXT_ID,
		color: null,
		source_id: sourceId,
		doc: {
			_id: `${id}-doc` as Doc.Id,
			gulp_timestamp: 1,
			"gulp.source_id": sourceId,
			"gulp.event_code": 1,
			number_hash: 1,
		},
		time_pin: 1,
		owner_user_id: USER_ID,
		text: "Original body",
		glyph_id: GLYPH_ID,
		edits: [],
		...overrides,
	};
}

/**
 * Creates a canonical link fixture for the active operation.
 * @param id Stable link identifier.
 * @param overrides Optional link fields.
 * @returns Complete link fixture used by the Links tab assertions.
 */
function createLink(
	id: string,
	overrides: Partial<Link.Type> = {},
): Link.Type {
	return {
		id: id as Link.Id,
		type: "link",
		operation_id: OPERATION_ID,
		owner_user_id: USER_ID,
		description: "Link description",
		tags: ["link-tag"],
		color: "#ff6600",
		doc_id_from: `${id}-origin` as Doc.Id,
		doc_ids: [`${id}-destination` as Doc.Id],
		glyph_id: GLYPH_ID,
		name: "Link title",
		...overrides,
	} as Link.Type;
}

/**
 * Resolves the backend result from canonical notes and the requested source IDs.
 * @param filter Notes request filter.
 * @returns Matching canonical notes.
 */
function getFilteredNotes(filter: Note.ListFilter): Note.Type[] {
	const sourceIds = new Set(filter.source_ids ?? []);
	const documentIds = filter.doc_ids ? new Set(filter.doc_ids) : null;
	return DataStore.notes.filter(
		(note) =>
			sourceIds.has(note.source_id) &&
			(!documentIds || documentIds.has(note.doc._id)),
	);
}

/** Resolves matching canonical links, applying backend pagination when requested. */
function getFilteredLinks(
	filter: Link.ListFilter,
	paginate: boolean,
): Link.Type[] {
	const ids = filter.ids ? new Set(filter.ids) : null;
	const matchingLinks = DataStore.links.filter(
		(link) => !ids || ids.has(link.id),
	);
	if (!paginate) return matchingLinks;
	const offset = filter.offset ?? 0;
	return matchingLinks.slice(offset, offset + (filter.limit ?? matchingLinks.length));
}

describe("NotesLinksWIndows semantic requests", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockApp = createApp([SOURCE_ALPHA_ID]);
		mockDetachedStatus = "active";
		mockTranslate = (key: string): string => key;
		DataStore.notes = [createNote("note-1")];
		DataStore.links = [createLink("link-1")];
		mockNotesListCalls.length = 0;
		mockNotesCountCalls.length = 0;
		mockLinksListCalls.length = 0;
		mockLinksCountCalls.length = 0;
		mockFlaggedDocIds = new Set();
		mockInvalidateFlagCache.mock.calls.splice(0);
		mockSendControlMessage.mock.calls.splice(0);
		mockSpawnBanner.mock.calls.splice(0);
		mockDestroyBanner.mock.calls.splice(0);
		mockInfo.notes_delete_bulk.mock.calls.splice(0);
		mockInfo.links_delete_bulk.mock.calls.splice(0);
		mockGetEventsByIdsCalls.splice(0);
		mockEventStoreResults = [];
		DataStore.Events.getByIds = mockGetEventsByIds;
		mockNotesListImplementation = async (filter) =>
			getFilteredNotes(filter);
		mockNotesCountImplementation = async (filter) =>
			getFilteredNotes(filter).length;
		mockLinksListImplementation = async (filter) =>
			getFilteredLinks(filter, true);
		mockLinksCountImplementation = async (filter) =>
			getFilteredLinks(filter, false).length;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		jest.useRealTimers();
		act(() => root.unmount());
		container.remove();
		DataStore.notes = [];
		DataStore.links = [];
		DataStore.Events.getByIds = originalGetEventsByIds;
	});

	/**
	 * Renders or rerenders NotesLinksWIndows and flushes its async list/count request.
	 * @returns Promise resolved after React effects and API promises settle.
	 */
	async function renderWindow(): Promise<void> {
		await act(async () => {
			root.render(<NotesLinksWIndows onClose={() => undefined} />);
			await Promise.resolve();
		});
	}

	/** Activates the Links tab through the pointer interaction used by Radix Tabs. */
	async function openLinksTab(): Promise<void> {
		const linksTab = [...container.querySelectorAll<HTMLButtonElement>(
			'[role="tab"]',
		)].find((button) => button.textContent?.includes("eventDialog.links"));
		expect(linksTab).not.toBeUndefined();
		await act(async () => {
			linksTab?.dispatchEvent(
				new MouseEvent("mousedown", { bubbles: true, button: 0 }),
			);
			linksTab?.click();
			await Promise.resolve();
		});
	}

	/**
	 * Returns notes-list calls made during the active test.
	 * @returns Per-test list request count.
	 */
	function getNotesListCallCount(): number {
		return mockNotesListCalls.length;
	}

	/**
	 * Returns notes-count calls made during the active test.
	 * @returns Per-test count request count.
	 */
	function getNotesCountCallCount(): number {
		return mockNotesCountCalls.length;
	}

	/** Returns Links list calls made during the active test. */
	function getLinksListCallCount(): number {
		return mockLinksListCalls.length;
	}

	/** Returns Links count calls made during the active test. */
	function getLinksCountCallCount(): number {
		return mockLinksCountCalls.length;
	}

	/** Updates one controlled input through React's delegated input event. */
	function enterInputValue(input: HTMLInputElement, value: string): void {
		const valueSetter = Object.getOwnPropertyDescriptor(
			HTMLInputElement.prototype,
			"value",
		)?.set;
		act(() => {
			valueSetter?.call(input, value);
			input.dispatchEvent(new Event("input", { bubbles: true }));
		});
	}

	it("does not request again for equivalent app, context, status, or translation snapshots", async () => {
		await renderWindow();
		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(getLinksListCallCount()).toBe(0);
		expect(getLinksCountCallCount()).toBe(0);
		expect(container.textContent?.includes("Original title")).toBe(true);
		expect(
			container.querySelector(
				'[data-note-id="note-1"] [data-table-tooltip-text="Original title"]',
			),
		).not.toBeNull();

		mockApp = {
			...mockApp,
			target: {
				...mockApp.target,
				operations: mockApp.target.operations.map((operation) => ({
					...operation,
				})),
				files: mockApp.target.files.map((source) => ({ ...source })),
				contexts: mockApp.target.contexts.map((context) => ({
					...context,
				})),
			},
			timeline: {
				...mockApp.timeline,
				renderedSourceIds: [...(mockApp.timeline.renderedSourceIds ?? [])],
			},
		};
		mockDetachedStatus = "idle";
		mockTranslate = (key: string): string => `translated:${key}`;
		await renderWindow();

		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(getLinksListCallCount()).toBe(0);
		expect(getLinksCountCallCount()).toBe(0);
		expect(container.textContent?.includes("Original title")).toBe(true);
	});

	it("routes a note target by the embedded document ID even when no event is resident", async () => {
		await renderWindow();
		const note = DataStore.notes[0];
		const targetButton = container.querySelector<HTMLButtonElement>(
			'[data-action-label="notes.targetNote"]',
		);

		act(() => targetButton?.click());

		expect(mockSendControlMessage).toHaveBeenCalledWith(
			"NAVIGATE_TO_EVENT",
			{
				docId: note.doc._id,
				operationId: note.operation_id,
			},
			undefined,
		);
	});

	it("renders Links in the second tab and routes the origin event", async () => {
		await renderWindow();
		expect(container.querySelector('[data-testid="notes-table"]')).not.toBeNull();
		expect(container.querySelector('[data-testid="links-table"]')).toBeNull();

		await openLinksTab();
		expect(mockLinksListCalls).toEqual([
			{
				limit: 50,
				offset: 0,
			},
		]);
		expect(mockLinksCountCalls).toEqual([{}]);

		const link = DataStore.links[0];
		const linkRow = container.querySelector<HTMLElement>(
			`[data-link-id="${link.id}"]`,
		);
		expect(linkRow?.textContent).toContain("Link title");
		expect(
			linkRow?.querySelector(
				'[data-table-tooltip-text="Link title"]',
			),
		).not.toBeNull();
		expect(linkRow?.textContent).toContain("Link description");
		expect(linkRow?.textContent).toContain("link-tag");
		expect(linkRow?.textContent).toContain(link.doc_id_from);
		expect(linkRow?.querySelector('[data-icon="Link"]')).not.toBeNull();
		expect(
			linkRow?.querySelector<HTMLElement>('[style*="color"]')?.style.color,
		).toBe("rgb(255, 102, 0)");

		const originButton = linkRow?.querySelector<HTMLButtonElement>(
			'[title="tableView.targetEvent"]',
		);
		act(() => originButton?.click());
		expect(mockSendControlMessage).toHaveBeenLastCalledWith(
			"NAVIGATE_TO_EVENT",
			{
				docId: link.doc_id_from,
				operationId: link.operation_id,
			},
			undefined,
		);
	});

	it("paginates, selects across pages, and bulk deletes Links", async () => {
		DataStore.links = Array.from({ length: 55 }, (_, index) =>
			createLink(`link-${index}`, {
				name: `Link ${index}`,
				tags: [`tag-${index}`],
			}),
		);
		await renderWindow();
		await openLinksTab();

		expect(container.querySelectorAll("[data-link-id]")).toHaveLength(50);
		expect(
			container.querySelector('[data-link-id="link-0"]')?.textContent,
		).toContain("tag-0");

		act(() => {
			container.querySelector<HTMLButtonElement>(
				'[data-row-select-id="link-0"]',
			)?.click();
			container.querySelector<HTMLButtonElement>(
				"[data-table-select-all]",
			)?.click();
		});

		await act(async () => {
			container.querySelector<HTMLButtonElement>(
				'button[title="tableView.nextPage"]',
			)?.click();
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
		});
		expect(container.querySelectorAll("[data-link-id]")).toHaveLength(5);
		expect(
			container.querySelector('[data-link-id="link-50"]'),
		).not.toBeNull();
		expect(
			container.querySelector<HTMLButtonElement>(
				'[data-row-select-id="link-50"]',
			)?.getAttribute("aria-pressed"),
		).toBe("false");

		const selectAllFiltered = [...container.querySelectorAll("button")].find(
			(button) => button.textContent === "links.selectAllFiltered",
		);
		act(() => selectAllFiltered?.click());
		expect(
			container.querySelector<HTMLButtonElement>(
				'[data-row-select-id="link-50"]',
			)?.getAttribute("aria-pressed"),
		).toBe("true");

		await act(async () => {
			container.querySelector<HTMLButtonElement>(
				'[data-page-size-option="20"]',
			)?.click();
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
		});
		expect(container.querySelectorAll("[data-link-id]")).toHaveLength(20);
		expect(
			container.querySelector('[data-link-id="link-0"]'),
		).not.toBeNull();

		const bulkDeleteAction = [
			...container.querySelectorAll<HTMLButtonElement>('button[role="menuitem"]'),
		].find((button) => button.textContent === "links.deleteSelected");
		act(() => bulkDeleteAction?.click());

		const [deleteBanner, bannerTarget] = mockSpawnBanner.mock.calls.at(-1) ?? [];
		expect(bannerTarget).toBe("table");
		expect(isValidElement(deleteBanner)).toBe(true);
		if (!isValidElement<{ linkIds: Link.Id[] }>(deleteBanner)) return;
		expect(deleteBanner.props.linkIds).toEqual(
			DataStore.links.map((link) => link.id),
		);

		act(() => root.render(deleteBanner));
		act(() => {
			container.querySelector<HTMLButtonElement>(
				'[data-testid="mock-toggle"]',
			)?.click();
		});
		await act(async () => {
			container.querySelector<HTMLButtonElement>(
				'[data-button-icon="Trash2"]',
			)?.click();
			await Promise.resolve();
		});

		expect(mockInfo.links_delete_bulk).toHaveBeenCalledWith(
			DataStore.links.map((link) => link.id),
		);
		expect(mockInfo.links_delete_bulk).toHaveBeenCalledTimes(1);
		expect(mockDestroyBanner).toHaveBeenCalledTimes(1);
	});

	it("filters Links by title, description, and exact tag", async () => {
		jest.useFakeTimers();
		const titleLink = createLink("link-title", { name: "Suspicious Login" });
		const descriptionLink = createLink("link-description", {
			name: "Description match",
			description: "Credential access investigation",
		});
		const tagLink = createLink("link-tag", {
			name: "Tag match",
			tags: ["Needs-Review"],
		});
		DataStore.links = [titleLink, descriptionLink, tagLink];
		await renderWindow();
		await openLinksTab();

		const searchInput = container.querySelector<HTMLInputElement>(
			'input[placeholder="common.search"]',
		);
		expect(searchInput).not.toBeNull();
		if (!searchInput) return;

		enterInputValue(searchInput, "LOGIN");
		await act(async () => {
			jest.advanceTimersByTime(250);
			await Promise.resolve();
		});
		expect(
			container.querySelector(`[data-link-id="${titleLink.id}"]`),
		).not.toBeNull();
		expect(
			container.querySelector(`[data-link-id="${descriptionLink.id}"]`),
		).toBeNull();

		enterInputValue(searchInput, "access invest");
		await act(async () => {
			jest.advanceTimersByTime(250);
			await Promise.resolve();
		});
		expect(
			container.querySelector(`[data-link-id="${descriptionLink.id}"]`),
		).not.toBeNull();
		expect(
			container.querySelector(`[data-link-id="${titleLink.id}"]`),
		).toBeNull();

		enterInputValue(searchInput, "needs-review");
		await act(async () => {
			jest.advanceTimersByTime(250);
			await Promise.resolve();
		});
		expect(
			container.querySelector(`[data-link-id="${tagLink.id}"]`),
		).not.toBeNull();

		enterInputValue(searchInput, "needs");
		await act(async () => {
			jest.advanceTimersByTime(250);
			await Promise.resolve();
		});
		expect(container.querySelector('[data-testid="links-table"]')).toBeNull();
		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(mockLinksListCalls.at(-1)?.ids).toEqual([tagLink.id]);
		expect(getLinksListCallCount()).toBe(4);
		expect(getLinksCountCallCount()).toBe(4);
		jest.useRealTimers();
	});

	it("opens related events, deletes links, and reacts to canonical replacements", async () => {
		const link = DataStore.links[0];
		const relatedEvents = [
			{
				_id: link.doc_id_from,
				"gulp.source_id": SOURCE_ALPHA_ID,
				gulp_timestamp: 1,
			} as Doc.Type,
			{
				_id: link.doc_ids[0],
				"gulp.source_id": SOURCE_ALPHA_ID,
				gulp_timestamp: 2,
			} as Doc.Type,
		];
		mockEventStoreResults = relatedEvents;
		await renderWindow();
		await openLinksTab();

		const showEventsButton = container.querySelector<HTMLButtonElement>(
			'[data-action-label="eventDialog.showLinkedEvents"]',
		);
		await act(async () => {
			showEventsButton?.click();
			await Promise.resolve();
		});
		expect(mockGetEventsByIdsCalls[0]).toEqual([
			link.doc_id_from,
			...link.doc_ids,
		]);
		expect(
			container.querySelector('[data-testid="linked-events-dialog"]')
				?.textContent,
		).toBe(relatedEvents.map((event) => event._id).join(","));

		act(() => {
			container.querySelector<HTMLButtonElement>(
				`[data-related-event-id="${relatedEvents[1]._id}"]`,
			)?.click();
		});
		expect(mockSendControlMessage).toHaveBeenLastCalledWith(
			"NAVIGATE_TO_EVENT",
			{
				docId: relatedEvents[1]._id,
				operationId: link.operation_id,
			},
			undefined,
		);
		expect(
			container.querySelector('[data-testid="linked-events-dialog"]'),
		).toBeNull();

		const deleteButton = container.querySelector<HTMLButtonElement>(
			'[data-action-label="link.deleteTitle"]',
		);
		act(() => deleteButton?.click());
		const [deleteBanner, bannerTarget] = mockSpawnBanner.mock.calls.at(-1) ?? [];
		expect(bannerTarget).toBe("table");
		expect(isValidElement(deleteBanner)).toBe(true);
		if (isValidElement<{ link: Link.Type }>(deleteBanner)) {
			expect(deleteBanner.props.link).toBe(link);
		}

		const replacement = createLink("link-2", { name: "Replacement link" });
		await act(async () => {
			DataStore.replaceLinks([replacement], "immediate");
			await Promise.resolve();
		});
		expect(container.textContent).toContain("Replacement link");
		expect(container.textContent).not.toContain("Link title");
	});

	it("reloads the current Notes and Links page after row deletion from this window", async () => {
		DataStore.notes = Array.from({ length: 55 }, (_, index) =>
			createNote(`note-${index}`),
		);
		DataStore.links = Array.from({ length: 55 }, (_, index) =>
			createLink(`link-${index}`),
		);
		await renderWindow();

		await act(async () => {
			container.querySelector<HTMLButtonElement>(
				'button[title="tableView.nextPage"]',
			)?.click();
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
			await new Promise((resolve) => setTimeout(resolve, 0));
		});
		expect(mockNotesListCalls.at(-1)?.offset).toBe(50);
		expect(container.querySelector('[data-note-id="note-50"]')).not.toBeNull();
		const note = DataStore.notes[50];
		act(() => {
			container.querySelector<HTMLButtonElement>(
				'[data-note-id="note-50"] [data-action-label="note.deleteTitle"]',
			)?.click();
		});
		const [noteDeleteBanner] = mockSpawnBanner.mock.calls.at(-1) ?? [];
		expect(isValidElement(noteDeleteBanner)).toBe(true);
		if (
			!isValidElement<{
				note: Note.Type;
				onDeleted?: () => void;
			}>(noteDeleteBanner)
		) {
			return;
		}
		expect(noteDeleteBanner.props.note).toBe(note);
		const noteListCallsBeforeDelete = getNotesListCallCount();
		await act(async () => {
			DataStore.deleteNote(note.id, "immediate");
			DataStore.publishNotesDeleteDelta(OPERATION_ID, [note.id]);
			noteDeleteBanner.props.onDeleted?.();
			await Promise.resolve();
			await Promise.resolve();
		});
		expect(getNotesListCallCount()).toBe(noteListCallsBeforeDelete + 1);
		expect(mockNotesListCalls.at(-1)).toMatchObject({
			limit: 50,
			offset: 50,
		});
		expect(container.textContent).not.toContain("notes.changesAvailable");

		await openLinksTab();
		await act(async () => {
			container.querySelector<HTMLButtonElement>(
				'button[title="tableView.nextPage"]',
			)?.click();
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
			await new Promise((resolve) => setTimeout(resolve, 0));
		});
		expect(mockLinksListCalls.at(-1)?.offset).toBe(50);
		expect(container.querySelector('[data-link-id="link-50"]')).not.toBeNull();
		const link = DataStore.links[50];
		act(() => {
			container.querySelector<HTMLButtonElement>(
				'[data-link-id="link-50"] [data-action-label="link.deleteTitle"]',
			)?.click();
		});
		const [linkDeleteBanner] = mockSpawnBanner.mock.calls.at(-1) ?? [];
		expect(isValidElement(linkDeleteBanner)).toBe(true);
		if (
			!isValidElement<{
				link: Link.Type;
				onDeleted?: () => void;
			}>(linkDeleteBanner)
		) {
			return;
		}
		expect(linkDeleteBanner.props.link).toBe(link);
		const linkListCallsBeforeDelete = getLinksListCallCount();
		await act(async () => {
			DataStore.deleteLink(link.id, "immediate");
			DataStore.publishLinksDeleteDelta(OPERATION_ID, [link.id]);
			linkDeleteBanner.props.onDeleted?.();
			await Promise.resolve();
			await Promise.resolve();
		});
		expect(getLinksListCallCount()).toBe(linkListCallsBeforeDelete + 1);
		expect(mockLinksListCalls.at(-1)).toMatchObject({
			limit: 50,
			offset: 50,
		});
		expect(container.textContent).not.toContain("links.changesAvailable");
	});

	it("keeps realtime Link creation and deletion behind an explicit refresh", async () => {
		await renderWindow();
		await openLinksTab();
		const originalLink = DataStore.links[0];
		const createdLink = createLink("link-created", {
			name: "Realtime created link",
		});
		const initialListCalls = getLinksListCallCount();

		await act(async () => {
			DataStore.upsertLink(createdLink, "immediate");
			DataStore.publishLinksUpsertDelta(
				OPERATION_ID,
				"create",
				[createdLink],
			);
			await Promise.resolve();
		});

		expect(getLinksListCallCount()).toBe(initialListCalls);
		expect(container.textContent).toContain("links.changesAvailable");
		expect(container.textContent).not.toContain("Realtime created link");

		await act(async () => {
			DataStore.deleteLink(originalLink.id, "immediate");
			DataStore.publishLinksDeleteDelta(OPERATION_ID, [originalLink.id]);
			await Promise.resolve();
		});

		expect(getLinksListCallCount()).toBe(initialListCalls);
		expect(container.textContent).toContain("Link title");

		const refreshButton = [...container.querySelectorAll("button")].find(
			(button) => button.textContent === "common.refresh",
		);
		await act(async () => {
			refreshButton?.click();
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(getLinksListCallCount()).toBe(initialListCalls + 1);
		expect(container.textContent).not.toContain("links.changesAvailable");
		expect(container.textContent).toContain("Realtime created link");
		expect(container.textContent).not.toContain("Link title");
	});

	it("uses the exact rendered sources whenever visible-only filtering is toggled", async () => {
		mockApp = {
			...mockApp,
			target: {
				...mockApp.target,
				files: mockApp.target.files.map((source) => ({
					...source,
					selected: true,
				})),
			},
		};
		await renderWindow();
		mockApp = {
			...mockApp,
			timeline: {
				...mockApp.timeline,
				renderVersion: mockApp.timeline.renderVersion + 1,
				renderedSourceIds: [SOURCE_BETA_ID],
			},
		};
		await renderWindow();
		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);

		const visibleOnlyCheckbox = container.querySelector<HTMLButtonElement>(
			"#notes-visible-sources-only",
		);
		expect(visibleOnlyCheckbox).not.toBeNull();
		await act(async () => {
			visibleOnlyCheckbox?.click();
			await Promise.resolve();
		});
		expect(getNotesListCallCount()).toBe(2);
		expect(getNotesCountCallCount()).toBe(2);
		expect(mockNotesListCalls.at(-1)?.source_ids).toEqual([SOURCE_BETA_ID]);

		mockApp = {
			...mockApp,
			timeline: {
				...mockApp.timeline,
				renderVersion: mockApp.timeline.renderVersion + 1,
				renderedSourceIds: [SOURCE_ALPHA_ID],
			},
		};
		await renderWindow();
		expect(getNotesListCallCount()).toBe(3);
		expect(getNotesCountCallCount()).toBe(3);
		expect(mockNotesListCalls.at(-1)?.source_ids).toEqual([SOURCE_ALPHA_ID]);

		await act(async () => {
			visibleOnlyCheckbox?.click();
			await Promise.resolve();
		});
		expect(getNotesListCallCount()).toBe(4);
		expect(getNotesCountCallCount()).toBe(4);
		expect(mockNotesListCalls.at(-1)?.source_ids).toEqual([
			SOURCE_ALPHA_ID,
			SOURCE_BETA_ID,
		]);
	});

	it("sends flagged event IDs as doc_ids when flagged-only filtering is enabled", async () => {
		const unflaggedNote = createNote("note-unflagged");
		const flaggedNote = createNote("note-flagged");
		DataStore.notes = [unflaggedNote, flaggedNote];
		mockFlaggedDocIds = new Set([flaggedNote.doc._id]);
		await renderWindow();

		const flaggedOnlyCheckbox = container.querySelector<HTMLButtonElement>(
			"#notes-flagged-events-only",
		);
		expect(flaggedOnlyCheckbox).not.toBeNull();
		await act(async () => {
			flaggedOnlyCheckbox?.click();
			await Promise.resolve();
		});

		expect(mockNotesListCalls.at(-1)?.doc_ids).toEqual([
			flaggedNote.doc._id,
		]);
		expect(mockNotesCountCalls.at(-1)?.doc_ids).toEqual([
			flaggedNote.doc._id,
		]);
		expect(
			container.querySelector(`[data-note-id="${flaggedNote.id}"]`),
		).not.toBeNull();
		expect(
			container.querySelector('[data-note-id="note-unflagged"]'),
		).toBeNull();

		mockFlaggedDocIds = new Set([unflaggedNote.doc._id]);
		await act(async () => {
			window.dispatchEvent(
				new StorageEvent("storage", { key: "flagged-events" }),
			);
			await Promise.resolve();
		});
		expect(mockInvalidateFlagCache).toHaveBeenCalledTimes(1);
		expect(mockNotesListCalls.at(-1)?.doc_ids).toEqual([
			unflaggedNote.doc._id,
		]);
		expect(
			container.querySelector(`[data-note-id="${unflaggedNote.id}"]`),
		).not.toBeNull();

		await act(async () => {
			flaggedOnlyCheckbox?.click();
			await Promise.resolve();
		});
		expect(mockNotesListCalls.at(-1)?.doc_ids).toBeUndefined();
		expect(mockNotesCountCalls.at(-1)?.doc_ids).toBeUndefined();
	});

	it("does not request every note when flagged-only filtering has no event IDs", async () => {
		await renderWindow();
		const flaggedOnlyCheckbox = container.querySelector<HTMLButtonElement>(
			"#notes-flagged-events-only",
		);

		await act(async () => {
			flaggedOnlyCheckbox?.click();
			await Promise.resolve();
		});

		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(container.textContent?.includes("tableView.noData")).toBe(true);
	});

	it("patches safe realtime updates without a request and refreshes membership changes once", async () => {
		await renderWindow();
		const originalNote = DataStore.notes[0];
		const updatedNote = {
			...originalNote,
			name: "Locally updated title",
		};
		await act(async () => {
			DataStore.upsertNote(updatedNote, "immediate");
			DataStore.publishNotesUpsertDelta(
				OPERATION_ID,
				"update",
				[updatedNote],
			);
			await Promise.resolve();
		});

		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(container.textContent?.includes("Locally updated title")).toBe(true);

		const movedNote = {
			...updatedNote,
			source_id: SOURCE_BETA_ID,
			doc: {
				...updatedNote.doc,
				"gulp.source_id": SOURCE_BETA_ID,
			},
		};
		await act(async () => {
			DataStore.upsertNote(movedNote, "immediate");
			DataStore.publishNotesUpsertDelta(
				OPERATION_ID,
				"update",
				[movedNote],
			);
			await Promise.resolve();
		});

		expect(getNotesListCallCount()).toBe(2);
		expect(getNotesCountCallCount()).toBe(2);
		expect(container.textContent?.includes("tableView.noData")).toBe(true);
	});

	it("keeps create and delete deltas behind the existing stale-page warning", async () => {
		await renderWindow();
		const createdNote = createNote("note-created");
		await act(async () => {
			DataStore.upsertNote(createdNote, "immediate");
			DataStore.publishNotesUpsertDelta(
				OPERATION_ID,
				"create",
				[createdNote],
			);
			await Promise.resolve();
		});

		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(container.textContent?.includes("notes.changesAvailable")).toBe(true);

		await act(async () => {
			DataStore.deleteNote(createdNote.id, "immediate");
			DataStore.publishNotesDeleteDelta(
				OPERATION_ID,
				[createdNote.id],
			);
			await Promise.resolve();
		});

		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(container.textContent?.includes("notes.changesAvailable")).toBe(true);
	});
});
