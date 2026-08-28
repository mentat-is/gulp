import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Doc } from "@/entities/Doc";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import { NotePoint } from "@/ui/Note";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SOURCE_ID = "note-point-source" as Source.Id;
const OPERATION_ID = "note-point-operation" as Operation.Id;
interface InclusionResult {
	events: Doc.Type[];
	missingDocumentIds: Doc.Id[];
	stale: boolean;
}
let mockInclusionResults: InclusionResult[] = [];
let mockResolvedDocuments: Doc.Type[] = [];
const mockIncludeNoteEvents = jest.fn<
	void,
	[readonly Note.Type[]]
>();
const mockDestroyBanner = jest.fn();
const mockSpawnBanner = jest.fn<void, [React.ReactElement]>();
const mockSpawnDialog = jest.fn();
const mockToastError = jest.fn<void, [string]>();
const mockGetByIds = jest.fn<void, [Doc.Id[]]>();

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			Info: {
				includeNoteEvents: (notes: readonly Note.Type[]) => {
					mockIncludeNoteEvents(notes);
					return Promise.resolve(mockInclusionResults.shift()!);
				},
			},
			destroyBanner: mockDestroyBanner,
			spawnBanner: mockSpawnBanner,
			spawnDialog: mockSpawnDialog,
			app: {},
		}),
	},
}));

jest.mock("sonner", () => ({
	toast: { error: (message: string) => mockToastError(message) },
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({
			t: (key: string, variables?: { count?: number }) =>
				variables?.count === undefined
					? key
					: `${key}:${variables.count}`,
		}),
	},
}));

jest.mock("@/ui/Banner", () => ({
	Banner: ({
		title,
		done,
		children,
	}: {
		title: ReactNode;
		done: ReactNode;
		children: ReactNode;
	}) => (
		<section>
			<h1>{title}</h1>
			{children}
			{done}
		</section>
	),
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		onClick,
		disabled,
	}: {
		onClick?: () => void;
		disabled?: boolean;
	}) => (
		<button type="button" disabled={disabled} onClick={onClick}>
			confirm
		</button>
	),
}));

jest.mock("@/ui/Point", () => ({
	Point: ({ onClick }: { onClick?: () => void }) => (
		<button type="button" data-testid="note-point" onClick={onClick}>
			point
		</button>
	),
}));

jest.mock("@/dialogs/Event.dialog", () => ({
	DisplayEventDialog: (): null => null,
}));

jest.mock("@/dialogs/Group.dialog", () => ({
	DisplayGroupDialog: (): null => null,
}));

jest.mock("@/entities/Doc", () => ({ Doc: {} }));

jest.mock("@/entities/Context", () => ({
	Context: { Entity: { id: () => undefined } },
}));

jest.mock("@/entities/Source", () => ({
	Source: { Entity: { id: () => undefined } },
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {
			sourceId: (note: Note.Type) =>
				note.source_id ?? note.doc["gulp.source_id"],
		},
	},
}));

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		Events: {
			getByIds: (documentIds: Doc.Id[]) => {
				mockGetByIds(documentIds);
				return Promise.resolve(mockResolvedDocuments);
			},
		},
	},
}));

/** Creates one normalized note target fixture. */
function createNote(id: string): Note.Type {
	return {
		id: `note-${id}` as Note.Id,
		type: "note",
		operation_id: OPERATION_ID,
		source_id: SOURCE_ID,
		name: id,
		doc: {
			_id: id as Doc.Id,
			gulp_timestamp: 100,
			"gulp.source_id": SOURCE_ID,
			number_hash: 1,
		} as Doc.Type,
	} as Note.Type;
}

describe("NotePoint note-linked event inclusion", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockInclusionResults = [];
		mockResolvedDocuments = [];
		mockIncludeNoteEvents.mock.calls.splice(0);
		mockDestroyBanner.mock.calls.splice(0);
		mockSpawnBanner.mock.calls.splice(0);
		mockSpawnDialog.mock.calls.splice(0);
		mockToastError.mock.calls.splice(0);
		mockGetByIds.mock.calls.splice(0);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("keeps a grouped banner open after a partial fetch and retries through Info", async () => {
		const notes = [createNote("first"), createNote("second")];
		const includedEvents = notes.map((note) => note.doc);
		mockInclusionResults = [
			{
				events: [includedEvents[0]],
				missingDocumentIds: [includedEvents[1]._id],
				stale: false,
			},
			{
				events: includedEvents,
				missingDocumentIds: [],
				stale: false,
			},
		];
		const onIncluded = jest.fn();

		act(() => {
			root.render(
				<NotePoint.FetchEventBanner
					notes={notes}
					onIncluded={onIncluded}
				/>,
			);
		});
		expect(container.querySelector("h1")?.textContent).toBe(
			"notePoint.fetchTitleMany:2",
		);

		await act(async () => {
			container.querySelector("button")?.click();
		});
		expect(mockToastError).toHaveBeenCalledWith("notePoint.fetchFailed:1");
		expect(mockDestroyBanner).not.toHaveBeenCalled();
		expect(onIncluded).not.toHaveBeenCalled();

		await act(async () => {
			container.querySelector("button")?.click();
		});
		expect(mockIncludeNoteEvents).toHaveBeenCalledTimes(2);
		expect(mockDestroyBanner).toHaveBeenCalledTimes(1);
		expect(onIncluded).toHaveBeenCalledWith(includedEvents);
	});

	it("offers the shared banner when a grouped point has unresolved targets", async () => {
		const notes = [createNote("first"), createNote("second")];
		mockResolvedDocuments = [notes[0].doc];

		act(() =>
			root.render(<NotePoint.Point notes={notes} type="note" x={0} y={0} />),
		);
		await act(async () => {
			container.querySelector<HTMLButtonElement>("[data-testid='note-point']")?.click();
		});

		expect(mockSpawnBanner).toHaveBeenCalledTimes(1);
		const banner = mockSpawnBanner.mock.calls[0]![0] as React.ReactElement<{
			notes: Note.Type[];
		}>;
		expect(banner.type).toBe(NotePoint.FetchEventBanner);
		expect(banner.props.notes).toBe(notes);
		expect(mockSpawnDialog).not.toHaveBeenCalled();
	});
});
