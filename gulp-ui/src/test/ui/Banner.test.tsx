import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Application as ApplicationTypes } from "@/context/Application.context";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockChildrenProps {
	children?: ReactNode;
}

interface MockButtonProps extends MockChildrenProps {
	icon?: string;
	onClick?: () => void;
}

jest.mock("@/context/Application.context", () => {
	const React = require("react") as typeof import("react");
	return {
		Application: {
			Context: React.createContext(null),
		},
	};
});

jest.mock("@/ui/Button", () => ({
	Button: ({ children, icon, onClick }: MockButtonProps): ReactNode => (
		<button type="button" data-icon={icon} onClick={onClick}>
			{children}
		</button>
	),
}));

jest.mock("@/ui/Skeleton", () => ({ Skeleton: (): null => null }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
}));
jest.mock("@/ui/utils", () => ({
	cn: (...values: unknown[]): string => values.filter(Boolean).join(" "),
}));

const { Application } = require("@/context/Application.context") as typeof import("@/context/Application.context");
const { Banner } = require("@/ui/Banner") as typeof import("@/ui/Banner");

describe("Banner close transition", () => {
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

	it("destroys the active banner before running its close callback", () => {
		const calls: string[] = [];
		const contextValue = {
			destroyBanner: (): void => {
				calls.push("destroy");
			},
			currentDocument: document,
		} as unknown as ApplicationTypes.Context.Props;

		act(() => {
			root.render(
				<Application.Context.Provider value={contextValue}>
					<Banner onClose={() => calls.push("onClose")}>content</Banner>
				</Application.Context.Provider>,
			);
		});

		act(() => {
			document
				.querySelector<HTMLButtonElement>('button[data-icon="X"]')
				?.click();
		});

		expect(calls).toEqual(["destroy", "onClose"]);
	});
});
