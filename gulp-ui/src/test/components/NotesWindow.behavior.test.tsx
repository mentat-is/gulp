import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Glyph } from "@/entities/Glyph";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import type { User } from "@/entities/User";
import { DataStore } from "@/store/DataStore";
import { NotesWindow } from "@/components/NotesWindow";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockTableRow {
	id: Note.Id;
	title: string;
	rawNote: Note.Type;
}

interface MockTableProps {
	values: MockTableRow[];
	actions?: Array<{
		label: string;
		onClick: (row: MockTableRow) => void;
	}>;
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
const mockSpawnBanner = jest.fn();
const mockSendControlMessage = jest.fn<
	void,
	[string, { docId: Doc.Id; operationId: Operation.Id }, string | undefined]
>();
const mockNotesListCalls: Note.ListFilter[] = [];
const mockNotesCountCalls: Note.ListFilter[] = [];
let mockFlaggedDocIds = new Set<Doc.Id>();
const mockInvalidateFlagCache = jest.fn();
let mockNotesListImplementation = async (
	_filter: Note.ListFilter,
	_signal?: AbortSignal,
): Promise<Note.Type[]> => [];
let mockNotesCountImplementation = async (
	_filter: Note.ListFilter,
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

const mockInfo = {
	notes_list: mockNotesList,
	notes_count: mockNotesCount,
	notes_delete_bulk: jest.fn(),
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		/** Returns the mutable application fixture used by each rerender. */
		use: (): unknown => ({
			Info: mockInfo,
			app: mockApp,
			spawnBanner: mockSpawnBanner,
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

jest.mock("@/ui/Select", () => {
	const Wrapper = ({ children }: MockChildrenProps): ReactNode =>
		<>{children}</>;
	return {
		Select: {
			Root: Wrapper,
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
		Banner: ({ children }: MockChildrenProps): ReactNode =>
			<div>{children}</div>,
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
			onClick,
			title,
		}: MockButtonProps): ReactNode =>
			<button
				type="button"
				disabled={disabled}
				onClick={onClick}
				title={title}
			>
				{children}
			</button>,
	};
});

jest.mock("@/ui/Toggle", () => ({
	Toggle: (): null => null,
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
	Icon: (): null => null,
}));

jest.mock("@/ui/utils", () => ({
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
		Table: ({ values, actions }: MockTableProps): ReactNode =>
			<div data-testid="notes-table">
				{values.map((row) => (
					<div key={row.id} data-note-id={row.id}>
						<span>{row.title}</span>
						{actions?.map((action) => (
							<button
								type="button"
								key={action.label}
								data-action-label={action.label}
								onClick={() => action.onClick(row)}
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
 * @returns Minimal application fixture required by NotesWindow.
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

describe("NotesWindow semantic requests", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockApp = createApp([SOURCE_ALPHA_ID]);
		mockDetachedStatus = "active";
		mockTranslate = (key: string): string => key;
		DataStore.notes = [createNote("note-1")];
		mockNotesListCalls.length = 0;
		mockNotesCountCalls.length = 0;
		mockFlaggedDocIds = new Set();
		mockInvalidateFlagCache.mock.calls.splice(0);
		mockSendControlMessage.mock.calls.splice(0);
		mockNotesListImplementation = async (filter) =>
			getFilteredNotes(filter);
		mockNotesCountImplementation = async (filter) =>
			getFilteredNotes(filter).length;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		DataStore.notes = [];
	});

	/**
	 * Renders or rerenders NotesWindow and flushes its async list/count request.
	 * @returns Promise resolved after React effects and API promises settle.
	 */
	async function renderWindow(): Promise<void> {
		await act(async () => {
			root.render(<NotesWindow onClose={() => undefined} />);
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

	it("does not request again for equivalent app, context, status, or translation snapshots", async () => {
		await renderWindow();
		expect(getNotesListCallCount()).toBe(1);
		expect(getNotesCountCallCount()).toBe(1);
		expect(container.textContent?.includes("Original title")).toBe(true);

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
