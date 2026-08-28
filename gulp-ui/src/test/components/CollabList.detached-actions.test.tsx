import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Doc } from "@/entities/Doc";
import type { Note } from "@/entities/Note";
import type { Source } from "@/entities/Source";
import { Collab } from "@/components/CollabList";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SOURCE_ID = "collab-detached-source" as Source.Id;
const EVENT_ID = "collab-detached-event" as Doc.Id;
const mockActionRequests: unknown[] = [];
const mockSpawnBanner = jest.fn<void, [ReactNode]>();
const mockGetEventById = jest.fn<Promise<Doc.Type | null>, [Doc.Id]>();
let mockDelegateActions = true;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			requestMainEventAction: (payload: unknown): boolean => {
				mockActionRequests.push(payload);
				return mockDelegateActions;
			},
			spawnBanner: (node: ReactNode) => mockSpawnBanner(node),
		}),
	},
}));

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		Events: {
			getById: (eventId: Doc.Id) => mockGetEventById(eventId),
		},
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {
			color: () => "#ffffff",
			icon: () => "StickyNote",
			sourceId: () => SOURCE_ID,
		},
		Delete: { Banner: () => null },
	},
}));

jest.mock("@/entities/Link", () => ({
	Link: {
		Entity: { icon: () => "GitBranch" },
		Delete: { Banner: () => null },
	},
}));

jest.mock("@/entities/Glyph", () => ({
	Glyph: { List: { get: () => "StickyNote" } },
}));

jest.mock("@/banners/Collab.functionality", () => ({
	NoteFunctionality: { Create: { Banner: () => null } },
	LinkFunctionality: { Create: { Banner: () => null } },
}));

jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: (key: string) => key }) },
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		icon,
		onClick,
	}: {
		icon?: string;
		onClick?: () => void;
	}) => <button data-icon={icon} onClick={onClick} />,
}));

jest.mock("@/ui/Select", () => ({
	Select: {
		Root: ({ children }: { children: ReactNode }) => <div>{children}</div>,
		Trigger: ({ children }: { children: ReactNode }) => <div>{children}</div>,
		Content: ({ children }: { children: ReactNode }) => <div>{children}</div>,
		Item: ({ children }: { children: ReactNode }) => <div>{children}</div>,
		Icon: () => null,
	},
}));

jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

jest.mock("@/ui/Markdown", () => ({ Markdown: () => null }));
jest.mock("@/ui/Separator", () => ({ Separator: () => null }));
jest.mock("@/ui/Badge", () => ({
	Badge: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
}));
jest.mock("@/ui/utils", () => ({ cn: (...values: string[]) => values.join(" ") }));

/** Creates one note attached to the Event dialog under test. */
function createNote(): Note.Type {
	return {
		id: "collab-note" as Note.Id,
		type: "note",
		name: "Note",
		text: "Text",
		tags: [],
		color: "#ffffff",
		glyph_id: "glyph" as never,
		time_created: Date.now(),
		doc: {
			_id: EVENT_ID,
			"gulp.source_id": SOURCE_ID,
		} as Doc.Type,
		source_id: SOURCE_ID,
	} as unknown as Note.Type;
}

describe("CollabList detached Event actions", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockActionRequests.splice(0);
		mockSpawnBanner.mock.calls.splice(0);
		mockGetEventById.mock.calls.splice(0);
		mockDelegateActions = true;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("delegates note edit and delete without reading detached event state", () => {
		const note = createNote();
		act(() => {
			root.render(<Collab.List notes={[note]} links={[]} />);
		});

		const editButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="PencilEdit"]',
		);
		const deleteButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Trash2"]',
		);
		if (!editButton || !deleteButton) {
			throw new Error("Collaboration action buttons were not rendered");
		}
		act(() => {
			editButton.click();
			deleteButton.click();
		});

		expect(mockActionRequests).toEqual([
			{
				eventId: EVENT_ID,
				action: { kind: "note-edit", noteId: note.id },
			},
			{
				eventId: EVENT_ID,
				action: { kind: "note-delete", noteId: note.id },
			},
		]);
		expect(mockGetEventById).not.toHaveBeenCalled();
		expect(mockSpawnBanner).not.toHaveBeenCalled();
	});
});
