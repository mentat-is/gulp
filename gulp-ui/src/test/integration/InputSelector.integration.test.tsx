import { act, type ReactNode, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { Input } from "@/ui/Input";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

jest.mock("@/ui/Popover", () => ({
	Popover: {
		Root: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Trigger: ({ children }: { children?: ReactNode }) => children,
		Content: ({
			children,
			role,
			"aria-multiselectable": ariaMultiselectable,
		}: {
			children?: ReactNode;
			role?: string;
			"aria-multiselectable"?: boolean;
		}) => (
			<div
				role={role}
				aria-multiselectable={ariaMultiselectable}
			>
				{children}
			</div>
		),
	},
}));

jest.mock("@/ui/Icon", () => ({
	Icon: Object.assign(
		({ name }: { name: string }) => <span data-icon={name} />,
		{ icons: {} },
	),
}));

jest.mock("@/ui/utils", () => ({
	cn: (...values: unknown[]) => values.filter(Boolean).join(" "),
}));

const VALUES: Input.Selector.Value[] = [
	{ key: "text", label: "Text", icon: "ALargeSmall" },
	{ key: "regexp", label: "Regex", icon: "Regex" },
];

/** Renders a controlled Input with the default single-value selector. */
function SingleSelectorInput() {
	const [selected, setSelected] = useState("text");
	return (
		<>
			<Input
				value=""
				selector={{
					values: VALUES,
					selected,
					onSelectedChange: setSelected,
				}}
			/>
			<output data-selected>{selected}</output>
		</>
	);
}

/** Renders a controlled Input with multi-value selection enabled. */
function MultipleSelectorInput() {
	const [selected, setSelected] = useState<string[]>(["text"]);
	return (
		<>
			<Input
				value=""
				selector={{
					values: VALUES,
					multiple: true,
					selected,
					onSelectedChange: setSelected,
				}}
			/>
			<output data-selected>{selected.join(",")}</output>
		</>
	);
}

describe("Input embedded selector", () => {
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

	it("uses the selected value icon and keeps single selection exclusive", () => {
		act(() => root.render(<SingleSelectorInput />));

		const trigger = container.querySelector<HTMLButtonElement>(
			'button[aria-haspopup="listbox"]',
		);
		expect(trigger?.querySelector('[data-icon="ALargeSmall"]')).not.toBeNull();
		expect(
			container.querySelector('[role="option"][aria-selected="true"]')
				?.textContent,
		).toBe("Text");

		act(() => {
			Array.from(
				container.querySelectorAll<HTMLButtonElement>('[role="option"]'),
			)
				.find((option) => option.textContent === "Regex")
				?.click();
		});

		expect(container.querySelector("[data-selected]")?.textContent).toBe(
			"regexp",
		);
		expect(trigger?.querySelector('[data-icon="Regex"]')).not.toBeNull();
		expect(
			container.querySelector('[role="option"][aria-selected="true"]')
				?.textContent,
		).toBe("Regex");
	});

	it("uses ListChecks and toggles independent values in multiple mode", () => {
		act(() => root.render(<MultipleSelectorInput />));

		const trigger = container.querySelector<HTMLButtonElement>(
			'button[aria-haspopup="listbox"]',
		);
		expect(trigger?.querySelector('[data-icon="ListChecks"]')).not.toBeNull();
		expect(
			container.querySelector('[role="listbox"]')?.getAttribute(
				"aria-multiselectable",
			),
		).toBe("true");

		const options = Array.from(
			container.querySelectorAll<HTMLButtonElement>('[role="option"]'),
		);
		act(() => options.find((option) => option.textContent === "Regex")?.click());
		expect(container.querySelector("[data-selected]")?.textContent).toBe(
			"text,regexp",
		);

		act(() => options.find((option) => option.textContent === "Text")?.click());
		expect(container.querySelector("[data-selected]")?.textContent).toBe(
			"regexp",
		);
	});
});
