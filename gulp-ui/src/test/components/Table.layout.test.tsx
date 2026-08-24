import { act } from "react";
import type {
	ButtonHTMLAttributes,
	HTMLAttributes,
	ReactNode,
} from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import { Table } from "@/components/Table";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockChildrenProps {
	children?: ReactNode;
}

interface MockContextMenuProps extends MockChildrenProps {
	onOpenChange?: (open: boolean) => void;
}

jest.mock("@tanstack/react-virtual", () => ({
	useVirtualizer: () => ({
		getVirtualItems: () => [],
		getTotalSize: () => 0,
	}),
}));

jest.mock("@/hooks/useTableConfig", () => ({
	useTableConfig: () => ({
		hiddenColumns: new Set<string>(),
		toggleColumn: () => undefined,
		resetConfig: () => undefined,
		isLoaded: true,
	}),
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string): string => key }),
	},
}));

jest.mock("@/entities/Color", () => ({
	Color: {
		Themer: { getTargetGuideColor: (): string => "#ff0000" },
	},
}));

jest.mock("@/entities/Glyph", () => ({
	Glyph: { List: new Map() },
}));

jest.mock("@/ui/utils", () => ({
	cn: (...values: unknown[]): string => values.filter(Boolean).join(" "),
}));

jest.mock("@/ui/Icon", () => ({
	Icon: (): null => null,
}));

jest.mock("@/ui/Checkbox", () => ({
	Checkbox: ({ checked }: { checked?: boolean }) => (
		<button
			role="checkbox"
			aria-checked={checked}
		/>
	),
}));

jest.mock("@/ui/Button", () => ({
	Button: (props: ButtonHTMLAttributes<HTMLButtonElement>) => (
		<button {...props} />
	),
}));

jest.mock("@/ui/Tooltip", () => ({
	TooltipProvider: ({ children }: MockChildrenProps) => <>{children}</>,
	Tooltip: ({ children }: MockChildrenProps) => <>{children}</>,
	TooltipTrigger: ({ children }: MockChildrenProps) => <>{children}</>,
	TooltipContent: ({ children }: MockChildrenProps) => <>{children}</>,
}));

jest.mock("@/ui/ContextMenu", () => ({
	ContextMenu: ({ children, onOpenChange }: MockContextMenuProps) => (
		<div onContextMenu={() => onOpenChange?.(true)}>{children}</div>
	),
	ContextMenuTrigger: ({ children }: MockChildrenProps) => <>{children}</>,
	ContextMenuContent: ({ children, ...props }: HTMLAttributes<HTMLDivElement>) => (
		<div {...props}>{children}</div>
	),
	ContextMenuCheckboxItem: ({ children }: MockChildrenProps) => (
		<div>{children}</div>
	),
	ContextMenuLabel: ({ children }: MockChildrenProps) => <div>{children}</div>,
	ContextMenuSeparator: () => <hr />,
}));

describe("Table fixed edge columns", () => {
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

	it("marks optional selection and action cells as sticky edges", () => {
		act(() => {
			root.render(
				<Table
					values={[{ name: "row" }]}
					columns={["name"]}
					includeIndex={false}
					selectable
					actions={[
						{
							icon: "Search",
							label: "Inspect",
							onClick: () => undefined,
						},
					]}
				/>,
			);
		});

		expect(
			container.querySelectorAll('[data-sticky-column="selection"]'),
		).toHaveLength(2);
		expect(
			container.querySelectorAll('[data-sticky-column="actions"]'),
		).toHaveLength(2);
	});

	it("limits the column menu to the current table viewport height", () => {
		act(() => {
			root.render(
				<Table
					values={[{ first: "one", second: "two" }]}
					columns={["first", "second"]}
					includeIndex={false}
					columnVisibility
				/>,
			);
		});

		const table = container.querySelector("table");
		const tableViewport = container.firstElementChild;
		if (!table || !tableViewport) {
			throw new Error("Table viewport was not rendered");
		}

		Object.defineProperty(tableViewport, "clientHeight", {
			configurable: true,
			value: 240,
		});

		act(() => {
			table.dispatchEvent(
				new MouseEvent("contextmenu", {
					bubbles: true,
					clientX: 20,
					clientY: 20,
				}),
			);
		});

		const columnMenu = document.querySelector<HTMLElement>(
			'[data-column-visibility-menu="true"]',
		);
		expect(columnMenu?.style.maxHeight).toBe("240px");
	});
});
