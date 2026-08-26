import {
	act,
	useState,
	type ButtonHTMLAttributes,
	type InputHTMLAttributes,
	type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import { OpenSearchQueryBuilder } from "@/components/QueryBuilder";
import { Filter } from "@/entities/Filter";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockChildrenProps {
	children?: ReactNode;
}

jest.mock("react-syntax-highlighter", () => ({
	__esModule: true,
	default: ({ children }: MockChildrenProps) => <pre>{children}</pre>,
}));

jest.mock("react-syntax-highlighter/dist/esm/styles/hljs", () => ({
	vs2015: {},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string): string => key }),
	},
}));

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			id: (): never => {
				throw new Error("Source lookup is not used by QueryBuilder tests");
			},
		},
	},
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			toNanos: (value: number): bigint => BigInt(value) * 1_000_000n,
		},
	},
}));

jest.mock("@/ui/utils", () => ({
	cn: (...values: unknown[]): string => values.filter(Boolean).join(" "),
	copy: (): void => undefined,
	fws: {},
}));

jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: MockChildrenProps) => <div>{children}</div>,
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		icon,
		loading,
		revert,
		shape,
		variant,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & {
		icon?: string;
		loading?: boolean;
		revert?: boolean;
		shape?: string;
		variant?: string;
	}) => <button {...props}>{children ?? icon}</button>,
}));

jest.mock("@/ui/Input", () => ({
	Input: ({
		icon,
		prefix,
		valid,
		variant,
		...props
	}: InputHTMLAttributes<HTMLInputElement> & {
		icon?: string;
		prefix?: string;
		valid?: boolean;
		variant?: string;
	}) => <input {...props} />,
}));

jest.mock("@/ui/Select", () => {
	const React = require("react") as typeof import("react");
	const SelectContext = React.createContext<{
		onValueChange?: (value: string) => void;
		onOpenChange?: (open: boolean) => void;
	}>({});

	return {
		Select: {
		Root: ({
			children,
			onValueChange,
			onOpenChange,
		}: MockChildrenProps & {
			onValueChange?: (value: string) => void;
			onOpenChange?: (open: boolean) => void;
		}) => (
			<SelectContext.Provider value={{ onValueChange, onOpenChange }}>
				<div>{children}</div>
			</SelectContext.Provider>
		),
		Trigger: ({
			children,
			onClick,
			...props
		}: MockChildrenProps & ButtonHTMLAttributes<HTMLButtonElement>) => {
			const context = React.useContext(SelectContext);
			return (
				<button
					type="button"
					{...props}
					onClick={(event) => {
						onClick?.(event);
						context.onOpenChange?.(true);
					}}
				>
					{children}
				</button>
			);
		},
		Value: (): null => null,
		Content: ({ children }: MockChildrenProps) => <div>{children}</div>,
		Item: ({
			children,
			value,
			disabled,
		}: MockChildrenProps & { value: string; disabled?: boolean }) => {
			const context = React.useContext(SelectContext);
			return (
				<button
					data-select-item={value}
					disabled={disabled}
					onClick={() => {
						context.onValueChange?.(value);
						context.onOpenChange?.(false);
					}}
				>
					{children}
				</button>
			);
		},
		},
	};
});

jest.mock("@/ui/Switch", () => ({
	Switch: (): null => null,
}));

jest.mock("@/ui/Toggle", () => ({
	Toggle: (): null => null,
}));

jest.mock("@/ui/Icon", () => ({
	Icon: (): null => null,
}));

jest.mock("@/ui/Label", () => ({
	Label: (): null => null,
}));

jest.mock("@/ui/Tooltip", () => ({
	Tooltip: ({ children }: MockChildrenProps) => <>{children}</>,
	TooltipContent: ({ children }: MockChildrenProps) => <>{children}</>,
	TooltipTrigger: ({ children }: MockChildrenProps) => <>{children}</>,
}));

const FIELD_OPTIONS: Filter.Options = {
	message: "text",
	"event.sequence": "long",
};

function QueryBuilderHarness({ initialFilters }: { initialFilters: Filter.Item[] }) {
	const [filters, setFilters] = useState<Filter.Item[]>(initialFilters);
	return (
		<>
			<OpenSearchQueryBuilder.Query.Add
				filters={filters}
				setFilters={setFilters}
			/>
			<OpenSearchQueryBuilder.Query.Filters
				filters={filters}
				setFilters={setFilters}
				options={FIELD_OPTIONS}
			/>
			<output data-filters={JSON.stringify(filters)} />
		</>
	);
}

/** Reads the current filter state exposed by the test harness. */
function readFilters(container: HTMLElement): Filter.Item[] {
	const value = container.querySelector("output")?.getAttribute("data-filters");
	return JSON.parse(value ?? "[]") as Filter.Item[];
}

/**
 * Changes a controlled input through React's native input event path.
 * @param input Input element rendered by the test harness.
 * @param value New value to dispatch.
 */
function changeInputValue(input: HTMLInputElement, value: string): void {
	const valueSetter = Object.getOwnPropertyDescriptor(
		HTMLInputElement.prototype,
		"value",
	)?.set;
	valueSetter?.call(input, value);
	input.dispatchEvent(new Event("input", { bubbles: true }));
}

/** Lets the field selector's deferred focus restoration run. */
async function flushFocusTimer(): Promise<void> {
	await act(async () => {
		await new Promise((resolve) => globalThis.setTimeout(resolve, 0));
	});
}

describe("OpenSearchQueryBuilder condition policy", () => {
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

	it("creates new conditions with eq as the default", () => {
		act(() => root.render(<QueryBuilderHarness initialFilters={[]} />));

		const addButton = [...container.querySelectorAll("button")].find(
			(button) => button.textContent === "queryBuilder.addCondition",
		);
		act(() => addButton?.click());

		expect(readFilters(container)[0]).toMatchObject({
			type: "eq",
			operator: "must",
			enabled: true,
		});
	});

	it("disables incompatible conditions for the selected field type", () => {
		const filter = Filter.Entity.createCondition("text-filter" as Filter.Id, {
			field: "message",
			value: "failure",
		});
		act(() => root.render(<QueryBuilderHarness initialFilters={[filter]} />));

		expect(
			container.querySelector<HTMLButtonElement>(
				'button[data-select-item="wildcard"]',
			)?.disabled,
		).toBe(false);
		expect(
			container.querySelector<HTMLButtonElement>(
				'button[data-select-item="regexp"]',
			)?.disabled,
		).toBe(true);
		expect(
			container.querySelector<HTMLButtonElement>(
				'button[data-select-item="range"]',
			)?.disabled,
		).toBe(true);
	});

	it("resets wildcard to eq immediately when the field becomes numeric", () => {
		const filter = Filter.Entity.createCondition("wildcard-filter" as Filter.Id, {
			type: "wildcard",
			field: "message",
			value: "fail*",
		});
		act(() => root.render(<QueryBuilderHarness initialFilters={[filter]} />));

		const fieldInput = container.querySelector<HTMLInputElement>(
			'input[placeholder="queryBuilder.fieldName"]',
		);
		act(() => {
			if (!fieldInput) throw new Error("Missing field input");
			changeInputValue(fieldInput, "event.sequence");
		});

		expect(readFilters(container)[0]).toMatchObject({
			field: "event.sequence",
			type: "eq",
		});
	});

	it("keeps dropdown search separate and resets only after field selection", async () => {
		const filter = Filter.Entity.createCondition("search-filter" as Filter.Id, {
			type: "wildcard",
			field: "message",
			value: "fail*",
		});
		act(() => root.render(<QueryBuilderHarness initialFilters={[filter]} />));

		const fieldTrigger = container.querySelector<HTMLButtonElement>(
			'button[aria-label="queryBuilder.fieldName"]',
		);
		act(() => fieldTrigger?.click());
		await flushFocusTimer();

		const searchInput = container.querySelector<HTMLInputElement>(
			'input[placeholder="common.search"]',
		);
		expect(searchInput?.value).toBe("");
		expect(document.activeElement).toBe(searchInput);

		act(() => {
			if (!searchInput) throw new Error("Missing field search input");
			changeInputValue(searchInput, "event");
		});
		await flushFocusTimer();
		expect(readFilters(container)[0]).toMatchObject({
			field: "message",
			type: "wildcard",
		});
		expect(document.activeElement).toBe(searchInput);

		act(() => changeInputValue(searchInput!, ""));
		expect(readFilters(container)[0]).toMatchObject({
			field: "message",
			type: "wildcard",
		});

		act(() => {
			changeInputValue(searchInput!, "sequence");
		});
		const numericFieldOption = container.querySelector<HTMLButtonElement>(
			'button[data-select-item="event.sequence"]',
		);
		act(() => numericFieldOption?.click());
		expect(readFilters(container)[0]).toMatchObject({
			field: "event.sequence",
			type: "eq",
		});

		act(() => fieldTrigger?.click());
		expect(searchInput?.value).toBe("");
	});
});
