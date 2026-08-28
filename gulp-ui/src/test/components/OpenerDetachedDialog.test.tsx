import React, { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { Application } from "@/context/Application.context";
import { OpenerDetachedDialog } from "@/components/OpenerDetachedDialog";

jest.mock("@/context/Application.context", () => {
	const ReactRuntime = require("react");
	const Context = ReactRuntime.createContext(undefined);
	return {
		Application: {
			Context,
			use: () => {
				const value = ReactRuntime.useContext(Context);
				if (!value) throw new Error("Application.Context not found");
				return value;
			},
		},
	};
});

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface PopupHarness {
	beforeUnload: () => void;
	document: Document;
	focus: JestMock<void, []>;
	close: JestMock<void, []>;
	window: Window;
}

function createPopupHarness(): PopupHarness {
	const popupDocument = document.implementation.createHTMLDocument("Popup");
	const listeners = new Map<string, EventListenerOrEventListenerObject>();
	let closed = false;
	const focus = jest.fn();
	const close = jest.fn(() => {
		closed = true;
	});
	const popup = {
		document: popupDocument,
		get closed() {
			return closed;
		},
		focus,
		close,
		addEventListener: jest.fn(
			(type: string, listener: EventListenerOrEventListenerObject) => {
				listeners.set(type, listener);
			},
		),
		removeEventListener: jest.fn((type: string) => listeners.delete(type)),
	} as unknown as Window;

	return {
		document: popupDocument,
		focus,
		close,
		window: popup,
		beforeUnload: () => {
			const listener = listeners.get("beforeunload");
			if (typeof listener === "function") listener(new Event("beforeunload"));
			else listener?.handleEvent(new Event("beforeunload"));
		},
	};
}

describe("OpenerDetachedDialog", () => {
	let container: HTMLDivElement;
	let root: Root;
	let originalWindowOpen: typeof window.open;

	beforeEach(() => {
		originalWindowOpen = window.open;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

		afterEach(() => {
		act(() => root.unmount());
		container.remove();
		window.open = originalWindowOpen;
		document.documentElement.removeAttribute("data-theme");
	});

	it("renders plugin content in the popup with main actions and popup document ownership", () => {
		const popup = createPopupHarness();
		const spawnDialog = jest.fn();
		const onBlocked = jest.fn();
		const onClosed = jest.fn();
		window.open = jest.fn(() => popup.window) as typeof window.open;
		document.documentElement.setAttribute("data-theme", "classic-dark");

		function PluginDialog() {
			const context = Application.use();
			return (
				<button
					data-popup-document={
						context.currentDocument === popup.document ? "true" : "false"
					}
					onClick={() => context.spawnDialog(null)}
				>
					Knowledge Base
				</button>
			);
		}

		act(() => {
			root.render(
				<OpenerDetachedDialog
					application={{
						currentDocument: document,
						spawnDialog,
					} as unknown as Application.Context.Props}
					features="width=640,height=800"
					name="GulpPluginDialog-test"
					title="Plugin dialog"
					onBlocked={onBlocked}
					onClosed={onClosed}
				>
					<PluginDialog />
				</OpenerDetachedDialog>,
			);
		});

		const button = popup.document.querySelector("button");
		expect(button?.textContent).toBe("Knowledge Base");
		expect(button?.getAttribute("data-popup-document")).toBe("true");
		expect(
			popup.document.documentElement.getAttribute("data-theme"),
		).toBe("classic-dark");
		expect(popup.focus).toHaveBeenCalledTimes(1);
		expect(onBlocked).not.toHaveBeenCalled();

		act(() => button?.dispatchEvent(new MouseEvent("click", { bubbles: true })));
		expect(spawnDialog).toHaveBeenCalledWith(null);

		act(() => popup.beforeUnload());
		expect(onClosed).toHaveBeenCalledTimes(1);
	});

	it("redocks when the browser blocks the popup", () => {
		const onBlocked = jest.fn();
		window.open = jest.fn(() => null) as typeof window.open;

		act(() => {
			root.render(
				<OpenerDetachedDialog
					application={{} as Application.Context.Props}
					features="width=640,height=800"
					name="GulpPluginDialog-blocked"
					title="Plugin dialog"
					onBlocked={onBlocked}
					onClosed={jest.fn()}
				>
					<div>Plugin</div>
				</OpenerDetachedDialog>,
			);
		});

		expect(onBlocked).toHaveBeenCalledTimes(1);
	});
});
