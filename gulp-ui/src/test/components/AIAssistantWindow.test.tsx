import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { AIAssistantWindow } from "@/components/AIAssistantWindow";

jest.mock("@/banners/AIAssistant.banner", () => ({
	AIAssistant: { Panel: () => <div data-built-in-assistant /> },
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Component: ({ name }: { name: string }) => (
			<div data-plugin-assistant={name} />
		),
	},
}));

describe("AIAssistantWindow", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		flushSync(() => root.unmount());
		container.remove();
	});

	it("keeps the host title for the built-in assistant", () => {
		flushSync(() => root.render(
			<AIAssistantWindow title="AI Assistant" onClose={jest.fn()} />,
		));
		expect(container.querySelector("h2")?.textContent).toBe("AI Assistant");
		expect(container.querySelector("[data-built-in-assistant]")).not.toBeNull();
	});

	it("lets an assistant plugin own its title inside its sidebar", () => {
		flushSync(() => root.render(
			<AIAssistantWindow
				title="AI Assistant Pro"
				pluginFilename="AIAssistantPro.banner.tsx"
				onClose={jest.fn()}
			/>,
		));
		expect(container.querySelector("h2")?.textContent).toBe("AI Assistant Pro");
		expect(
			container.querySelector("[data-plugin-assistant]")?.getAttribute(
				"data-plugin-assistant",
			),
		).toBe("AIAssistantPro.banner.tsx");
	});
});
