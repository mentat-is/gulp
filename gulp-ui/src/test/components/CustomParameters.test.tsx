import { CustomParameters } from "@/components/CustomParameters";
import { act, type ReactNode, useEffect, useState } from "react";
import { createRoot, type Root } from "react-dom/client";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string) => key }),
	},
}));

jest.mock("@/ui/Input", () => ({
	Input: ({
		label,
		icon: _icon,
		variant: _variant,
		valid: _valid,
		skeleton: _skeleton,
		...props
	}: import("react").InputHTMLAttributes<HTMLInputElement> & {
		label?: string;
		icon?: string;
		variant?: string;
		valid?: boolean;
		skeleton?: boolean;
	}) => (
		<label>
			{label}
			<input {...props} />
		</label>
	),
}));

jest.mock("@/ui/Label", () => ({
	Label: ({ value, className }: { value?: string; className?: string }) => (
		<label className={className}>{value}</label>
	),
}));

jest.mock("@/ui/Textarea", () => ({
	Textarea: ({
		error: _error,
		...props
	}: import("react").TextareaHTMLAttributes<HTMLTextAreaElement> & {
		error?: boolean;
	}) => <textarea {...props} />,
}));

jest.mock("@/ui/Select", () => {
	const React = require("react") as typeof import("react");
	interface MockSelectContext {
		value?: string;
		onValueChange?: (value: string) => void;
	}
	const Context = React.createContext<MockSelectContext>({});
	return {
		Select: {
			Root: ({
				children,
				value,
				onValueChange,
			}: {
				children?: ReactNode;
				value?: string;
				onValueChange?: (value: string) => void;
			}) => (
				<Context.Provider value={{ value, onValueChange }}>
					<div data-select>{children}</div>
				</Context.Provider>
			),
			Trigger: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
			Value: ({ placeholder }: { placeholder?: string }) => {
				const context = React.useContext(Context);
				return <span>{context.value ?? placeholder}</span>;
			},
			Content: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
			Item: ({ children, value }: { children?: ReactNode; value: string }) => {
				const context = React.useContext(Context);
				return (
					<button
						type="button"
						data-select-option={value}
						onClick={() => context.onValueChange?.(value)}
					>
						{children}
					</button>
				);
			},
		},
	};
});

jest.mock("@/ui/Switch", () => ({
	Switch: ({
		checked,
		name,
		onCheckedChange,
	}: {
		checked?: boolean;
		name?: string;
		onCheckedChange?: (checked: boolean) => void;
	}) => (
		<input
			type="checkbox"
			name={name}
			checked={checked}
			onChange={(event) => onCheckedChange?.(event.target.checked)}
		/>
	),
}));

interface EditorHarnessProps {
	parameters: readonly CustomParameters.Interface[];
	initialValues: CustomParameters.ValueMap;
}

/**
 * Hosts the editor with observable values and aggregate validity for DOM tests.
 * @param props.parameters Explicit parameter definitions rendered by the editor.
 * @param props.initialValues Initial controlled values.
 * @returns The editor and serialized state probes.
 */
function EditorHarness({ parameters, initialValues }: EditorHarnessProps) {
	const [values, setValues] = useState(initialValues);
	const [isValid, setIsValid] = useState(true);
	return (
		<>
			<CustomParameters.Editor
				parameters={parameters}
				customParameters={values}
				setCustomParameters={setValues}
				onValidityChange={setIsValid}
			/>
			<output data-values>{JSON.stringify(values)}</output>
			<output data-validity>{String(isValid)}</output>
		</>
	);
}

interface ParentResetHarnessProps {
	parameters: readonly CustomParameters.Interface[];
	onObserve: (values: CustomParameters.ValueMap) => void;
}

/**
 * Simulates a parent clearing controlled values in its mount effect after child effects.
 * @param props.parameters Definitions that must be restored after the reset.
 * @param props.onObserve Callback receiving the values visible to a submit action.
 * @returns The controlled editor and an observation action.
 */
function ParentResetHarness({
	parameters,
	onObserve,
}: ParentResetHarnessProps) {
	const [values, setValues] = useState<CustomParameters.ValueMap>({});

	useEffect(() => {
		setValues({});
	}, []);

	return (
		<>
			<CustomParameters.Editor
				parameters={parameters}
				customParameters={values}
				setCustomParameters={setValues}
			/>
			<button type="button" data-observe onClick={() => onObserve(values)} />
		</>
	);
}

/**
 * Dispatches a React-compatible input event after invoking the native value setter.
 * @param element Input or textarea whose value is changed.
 * @param value New textual value.
 * @returns Nothing.
 */
function changeValue(
	element: HTMLInputElement | HTMLTextAreaElement,
	value: string,
): void {
	const prototype =
		element instanceof HTMLTextAreaElement
			? HTMLTextAreaElement.prototype
			: HTMLInputElement.prototype;
	const setter = Object.getOwnPropertyDescriptor(prototype, "value")?.set;
	setter?.call(element, value);
	element.dispatchEvent(new Event("input", { bubbles: true }));
}

/** Reads the serialized controlled values rendered by the test harness. */
function readValues(container: HTMLElement): CustomParameters.ValueMap {
	const serialized = container.querySelector("[data-values]")?.textContent ?? "{}";
	return JSON.parse(serialized) as CustomParameters.ValueMap;
}

describe("CustomParameters.Editor", () => {
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

	it("renders typed controls and preserves existing falsy values", () => {
		const parameters: CustomParameters.Interface[] = [
			{
				name: "mode",
				type: "str",
				desc: "Execution mode",
				location: "header",
				required: true,
				default_value: "slow",
				values: ["slow", "fast"],
			},
			{
				name: "enabled",
				type: "bool",
				desc: "Enabled",
				location: "query",
				required: false,
				default_value: true,
			},
			{
				name: "retries",
				type: "int",
				desc: "Retries",
				location: "query",
				required: false,
				default_value: 5,
			},
			{
				name: "ratio",
				type: "float",
				desc: "Ratio",
				location: "body",
				required: false,
				default_value: 1.5,
			},
			{
				name: "names",
				type: "list",
				desc: "Names",
				location: "body",
				required: false,
				default_value: ["fallback"],
			},
			{
				name: "payload",
				type: "dict",
				desc: "Payload",
				location: "body",
				required: false,
				default_value: { fallback: true },
			},
			{
				name: "token",
				type: "bool",
				desc: "Token",
				location: "header",
				required: true,
				is_secret: true,
				values: [true, false],
			},
			{
				name: "title",
				type: "str",
				desc: "Title",
				location: "query",
				required: false,
				default_value: "fallback",
			},
		];

		act(() => {
			root.render(
				<EditorHarness
					parameters={parameters}
					initialValues={{
						mode: "slow",
						enabled: false,
						retries: 0,
						names: [],
						payload: {},
						token: "",
						title: "",
					}}
				/>,
			);
		});

		const values = readValues(container);
		expect(values.enabled).toBe(false);
		expect(values.retries).toBe(0);
		expect(values.names).toEqual([]);
		expect(values.payload).toEqual({});
		expect(values.title).toBe("");
		expect(values.ratio).toBe(1.5);
		expect(container.querySelector<HTMLInputElement>('input[name="enabled"]')?.checked).toBe(false);
		expect(container.querySelector<HTMLInputElement>('input[name="retries"]')?.type).toBe("number");
		expect(container.querySelector<HTMLInputElement>('input[name="ratio"]')?.step).toBe("any");
		expect(container.querySelector<HTMLInputElement>('input[name="token"]')?.type).toBe("password");
		expect(container.querySelector<HTMLTextAreaElement>('textarea[name="payload"]')).not.toBeNull();

		act(() => {
			Array.from(
				container.querySelectorAll<HTMLButtonElement>("[data-select-option]"),
			)
				.find((option) => option.dataset.selectOption === '"fast"')
				?.click();
		});
		expect(readValues(container).mode).toBe("fast");

		const retries = container.querySelector<HTMLInputElement>('input[name="retries"]');
		const names = container.querySelector<HTMLInputElement>('input[name="names"]');
		expect(retries).not.toBeNull();
		expect(names).not.toBeNull();
		act(() => {
			if (retries) changeValue(retries, "7");
			if (names) changeValue(names, "alpha, beta");
		});
		expect(readValues(container).retries).toBe(7);
		expect(readValues(container).names).toEqual(["alpha", "beta"]);
	});

	it("keeps an invalid dictionary draft local and reports aggregate validity", () => {
		const parameters: CustomParameters.Interface[] = [
			{
				name: "payload",
				type: "dict",
				desc: "Payload",
				location: "body",
				required: true,
			},
		];

		act(() => {
			root.render(
				<EditorHarness
					parameters={parameters}
					initialValues={{ payload: { ready: true } }}
				/>,
			);
		});
		const textarea = container.querySelector<HTMLTextAreaElement>(
			'textarea[name="payload"]',
		);
		expect(textarea).not.toBeNull();

		act(() => {
			if (textarea) changeValue(textarea, "{invalid");
		});
		expect(readValues(container).payload).toEqual({ ready: true });
		expect(container.querySelector("[data-validity]")?.textContent).toBe("false");
		expect(container.textContent?.includes("filterFile.invalidJson")).toBe(true);

		act(() => {
			if (textarea) changeValue(textarea, '{"count": 2}');
		});
		expect(readValues(container).payload).toEqual({ count: 2 });
		expect(container.querySelector("[data-validity]")?.textContent).toBe("true");
	});

	it("reinitializes missing defaults after a parent mount effect clears the controlled map", () => {
		const parameters: CustomParameters.Interface[] = [
			{
				name: "enabled",
				type: "bool",
				required: false,
				default_value: false,
			},
			{
				name: "limit",
				type: "int",
				required: false,
				default_value: 0,
			},
			{
				name: "prefix",
				type: "str",
				required: false,
				default_value: "",
			},
		];
		const observe = jest.fn<void, [CustomParameters.ValueMap]>();

		act(() => {
			root.render(
				<ParentResetHarness parameters={parameters} onObserve={observe} />,
			);
		});
		act(() => {
			container.querySelector<HTMLButtonElement>("[data-observe]")?.click();
		});

		expect(observe).toHaveBeenCalledWith({ enabled: false, limit: 0, prefix: "" });
	});
});
