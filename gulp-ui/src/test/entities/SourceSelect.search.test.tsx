import {
	act,
	type Dispatch,
	type InputHTMLAttributes,
	type ReactNode,
	type SetStateAction,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import { Source } from "@/entities/Source";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "source-search-operation" as Operation.Id;
const NETWORK_CONTEXT_ID = "network-context" as Context.Id;
const ENDPOINT_CONTEXT_ID = "endpoint-context" as Context.Id;
const FIREWALL_SOURCE_ID = "firewall-source" as Source.Id;
const DNS_SOURCE_ID = "dns-source" as Source.Id;
const PROCESS_SOURCE_ID = "process-source" as Source.Id;

const mockApp = {
	target: {
		operations: [{ id: OPERATION_ID, selected: true }],
		contexts: [
			{
				id: NETWORK_CONTEXT_ID,
				name: "Network evidence",
				operation_id: OPERATION_ID,
				selected: true,
			},
			{
				id: ENDPOINT_CONTEXT_ID,
				name: "Endpoint evidence",
				operation_id: OPERATION_ID,
				selected: true,
			},
		],
		files: [],
	},
} as unknown as App.Type;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): { app: App.Type } => ({ app: mockApp }),
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

jest.mock("@/entities/addon/Parser", () => ({
	Parser: {
		use: (value: App.Type | Source.Type[]): Source.Type[] =>
			Array.isArray(value) ? value : value.target.files,
		useUUID: (value: Source.Type | Source.Id): Source.Id =>
			typeof value === "string" ? value : value.id,
	},
}));

jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		IconExtractor: { activate: () => (): string => "File" },
	},
}));

jest.mock("@/entities/Context", () => ({
	Context: {
		Entity: {
			id: (app: App.Type, contextId: Context.Id): Context.Type | undefined =>
				app.target.contexts.find((context) => context.id === contextId),
		},
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: { Entity: { sourceId: (): undefined => undefined } },
}));

jest.mock("@/banners/FilterFile.banner", () => ({
	FilterFileBanner: (): null => null,
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: (): string => "test-uuid",
	HASH_FUNCTIONS: [],
	Refractor: {
		get: (): undefined => undefined,
		any: { toNumber: (): number => 0 },
	},
}));

jest.mock("@/ui/Input", () => ({
	Input: ({
		icon: _icon,
		...props
	}: InputHTMLAttributes<HTMLInputElement> & { icon?: string }) => (
		<input {...props} />
	),
}));

jest.mock("@/ui/Icon", () => ({
	Icon: (): null => null,
}));

jest.mock("@/ui/Checkbox", () => ({
	Checkbox: (): null => null,
}));

jest.mock("@/ui/Select", () => ({
	Select: {
		Multi: {
			Root: ({
				children,
				onOpenChange,
			}: {
				children?: ReactNode;
				onOpenChange?: (open: boolean) => void;
			}) => (
				<div>
					<button
						type="button"
						data-open-source-select
						onClick={() => onOpenChange?.(true)}
					>
						open
					</button>
					{children}
				</div>
			),
			Value: (): null => null,
			ToggleAll: ({
				label,
				checked,
				onToggle,
			}: {
				label: string;
				checked: boolean;
				onToggle: (checked: boolean) => void;
			}) => (
				<button
					type="button"
					data-toggle-all
					onClick={() => onToggle(!checked)}
				>
					{label}
				</button>
			),
		},
		Trigger: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Content: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Group: ({ children }: { children?: ReactNode }) => <section>{children}</section>,
		Label: ({ children }: { children?: ReactNode }) => <h2>{children}</h2>,
		Item: ({
			children,
			value,
		}: {
			children?: ReactNode;
			value: Source.Id;
		}) => <div data-source-id={value}>{children}</div>,
		Separator: () => <hr />,
	},
}));

/** Creates a source fixture for the grouped selector. */
function createSource(
	id: Source.Id,
	name: string,
	contextId: Context.Id,
): Source.Type {
	return {
		id,
		name,
		context_id: contextId,
		operation_id: OPERATION_ID,
		selected: true,
		pinned: false,
	} as Source.Type;
}

const sources = [
	createSource(FIREWALL_SOURCE_ID, "firewall.log", NETWORK_CONTEXT_ID),
	createSource(DNS_SOURCE_ID, "dns.log", NETWORK_CONTEXT_ID),
	createSource(PROCESS_SOURCE_ID, "process.log", ENDPOINT_CONTEXT_ID),
];

/** Opens the mocked source selector and lets the focus effect settle. */
async function openSourceSelector(container: HTMLElement): Promise<void> {
	const openButton = container.querySelector<HTMLButtonElement>(
		"[data-open-source-select]",
	);
	if (!openButton) throw new Error("Expected the source selector trigger");

	await act(async () => {
		openButton.click();
		await new Promise((resolve) => globalThis.setTimeout(resolve, 0));
	});
}

/** Updates the controlled search input through its browser input event. */
function searchFor(container: HTMLElement, value: string): void {
	const input = container.querySelector<HTMLInputElement>(
		'input[aria-label="common.search"]',
	);
	if (!input) throw new Error("Expected the source search input");
	const valueSetter = Object.getOwnPropertyDescriptor(
		HTMLInputElement.prototype,
		"value",
	)?.set;
	if (!valueSetter) throw new Error("Expected the browser input value setter");

	act(() => {
		valueSetter.call(input, value);
		input.dispatchEvent(new Event("input", { bubbles: true }));
	});
}

describe("Source.Select.Multi search", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockApp.target.files = sources;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("keeps the shared selector unchanged unless search is enabled", async () => {
		act(() => {
			root.render(
				<Source.Select.Multi
					sources={sources}
					selected={[]}
					setSelected={() => undefined}
				/>,
			);
		});
		await openSourceSelector(container);

		expect(container.querySelector('input[aria-label="common.search"]')).toBeNull();
		expect(container.textContent).toContain("Network evidence");
		expect(container.textContent).toContain("Endpoint evidence");
	});

	it("filters source and context names without changing selection semantics", async () => {
		const selectionCalls: Source.Id[][] = [];
		const setSelected: Dispatch<SetStateAction<Source.Id[]>> = (action) => {
			const nextSelection =
				typeof action === "function" ? action([FIREWALL_SOURCE_ID]) : action;
			selectionCalls.push(nextSelection);
		};

		act(() => {
			root.render(
				<Source.Select.Multi
					sources={sources}
					selected={[FIREWALL_SOURCE_ID]}
					setSelected={setSelected}
					searchable
				/>,
			);
		});
		await openSourceSelector(container);

		searchFor(container, "ENDPOINT");
		expect(container.textContent).toContain("Endpoint evidence");
		expect(container.textContent).toContain("process.log");
		expect(container.textContent).not.toContain("Network evidence");
		expect(selectionCalls).toEqual([]);

		searchFor(container, "dns");
		expect(container.textContent).toContain("Network evidence");
		expect(container.textContent).toContain("dns.log");
		expect(container.textContent).not.toContain("firewall.log");
		expect(container.textContent).not.toContain("Endpoint evidence");
		expect(selectionCalls).toEqual([]);

		const toggleAll = container.querySelector<HTMLButtonElement>(
			"[data-toggle-all]",
		);
		if (!toggleAll) throw new Error("Expected the select-all action");
		act(() => toggleAll.click());

		expect(selectionCalls).toEqual([
			[FIREWALL_SOURCE_ID, DNS_SOURCE_ID, PROCESS_SOURCE_ID],
		]);
	});
});
