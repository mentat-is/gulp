import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import {
	MemoryRouter,
	Route,
	Routes,
} from "react-router-dom";
import { DetachedRouteShell } from "@/page/DetachedRoute.page";
import type { Doc } from "@/entities/Doc";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockDetachedProviderProps {
	bridgeId: string;
	coordinatorId: string;
	kind: string;
	loadExtensions?: boolean;
	assistantQuerySyncEnabled?: boolean;
	extensionFilenames?: string[];
	extensionSlots?: unknown[];
	children?: ReactNode;
}

const mockProviderCalls: MockDetachedProviderProps[] = [];
let mockDetachedDialog: {
	kind: "event";
	event: Doc.Type;
} | null = null;

jest.mock("@/context/DetachedApp.provider", () => ({
	DetachedAppProvider: (
		props: MockDetachedProviderProps,
	): ReactNode => {
		mockProviderCalls.push(props);
		return props.children;
	},
}));

jest.mock("@/components/NotesWindow", () => ({
	NotesWindow: (): null => null,
}));

jest.mock("@/components/TableViewWindow", () => ({
	TableViewWindow: (): null => null,
}));

jest.mock("@/components/DashboardViewWindow", () => ({
	DashboardViewWindow: (): null => null,
}));

jest.mock("@/components/AIAssistantWindow", () => ({
	AIAssistantWindow: (): null => null,
}));

jest.mock("@/dialogs/Event.dialog", () => ({
	DisplayEventDialog: () => <div data-event-dialog />,
}));

jest.mock("@/dialogs/Group.dialog", () => ({
	DisplayGroupDialog: (): null => null,
}));

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			dialog: null,
			detachedDialog: mockDetachedDialog,
		}),
	},
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Slot: {
			DashboardView: "dashboard-view",
			SendData: "send-data",
			EventActions: "event-actions",
			EventContextMenu: "event-context-menu",
		},
	},
}));

describe("DetachedRoute capability advertisement", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockProviderCalls.length = 0;
		mockDetachedDialog = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	/**
	 * Renders one independent detached route and returns its provider contract.
	 * @param path Detached route URL.
	 * @returns Capabilities advertised to DetachedAppProvider.
	 */
	function renderRoute(path: string): MockDetachedProviderProps {
		act(() => {
			root.render(
				<MemoryRouter
					key={path}
					initialEntries={[path]}
					future={{
						v7_relativeSplatPath: true,
						v7_startTransition: true,
					}}
				>
					<Routes>
						<Route
							path="/detached/:kind"
							element={<DetachedRouteShell />}
						/>
					</Routes>
				</MemoryRouter>,
			);
		});

		const props = mockProviderCalls.at(-1);
		if (!props) {
			throw new Error(`Detached route did not advertise capabilities: ${path}`);
		}
		return props;
	}

	it("keeps notes and table routes free of extension and assistant capabilities", () => {
		for (const kind of ["notes", "table"] as const) {
			const props = renderRoute(
				`/detached/${kind}?bridgeId=${kind}-client&coordinatorId=main`,
			);

			expect(props.kind).toBe(kind);
			expect(props.loadExtensions).toBe(false);
			expect(props.extensionFilenames).toBeUndefined();
			expect(props.extensionSlots).toBeUndefined();
			expect(props.assistantQuerySyncEnabled).toBe(false);
		}
	});

	it("targets plugin loading and query sync for a plugin assistant route", () => {
		const props = renderRoute(
			"/detached/assistant?bridgeId=assistant-client" +
				"&coordinatorId=main&assistantPlugin=extension-ai.js",
		);

		expect(props.kind).toBe("assistant");
		expect(props.loadExtensions).toBe(true);
		expect(props.extensionFilenames).toEqual(["extension-ai.js"]);
		expect(props.extensionSlots).toBeUndefined();
		expect(props.assistantQuerySyncEnabled).toBe(true);
	});

	it("keeps detached dialogs reactive in a definite-height route", () => {
		mockDetachedDialog = {
			kind: "event",
			event: {
				_id: "detached-dialog-event",
				"gulp.source_id": "dialog-source",
			} as Doc.Type,
		};
		const props = renderRoute(
			"/detached/dialog?bridgeId=dialog-client&coordinatorId=main",
		);

		expect(props.extensionSlots).toEqual([
			"send-data",
			"event-actions",
			"event-context-menu",
		]);
		const dialogRoot = container.querySelector<HTMLElement>(
			"[data-detached-dialog-root]",
		);
		expect(dialogRoot?.style.height).toBe("100vh");
		expect(dialogRoot?.style.minHeight).toBe("0");
		expect(dialogRoot?.style.flex).toBe("");
		expect(dialogRoot?.querySelector("[data-event-dialog]")).not.toBeNull();
		expect(
			container.querySelector(
				'button[aria-label="navigator.dockDialogPanel"]',
			),
		).toBeNull();
	});
});
