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

const mockCopiedValues: string[] = [];
const mockToggledColumns: string[] = [];
let mockHorizontalStartIndex = 0;
let mockColumnMenuStartIndex = 0;

jest.mock("@tanstack/react-virtual", () => ({
	useVirtualizer: (options: {
		count: number;
		enabled?: boolean;
		horizontal?: boolean;
		estimateSize: (index: number) => number;
	}) => {
		const sizes = Array.from({ length: options.count }, (_value, index) =>
			options.estimateSize(index),
		);
		let nextStart = 0;
		const starts = sizes.map((size) => {
			const start = nextStart;
			nextStart += size;
			return start;
		});
		const virtualItems =
			options.enabled && options.horizontal
				? Array.from(
						{
							length: Math.min(
								6,
								Math.max(0, options.count - mockHorizontalStartIndex),
							),
						},
						(_value, offset) => {
							const index = mockHorizontalStartIndex + offset;
							return {
								index,
								start: starts[index],
								end: starts[index] + sizes[index],
							};
						},
					)
				: !options.horizontal
					? Array.from(
							{
								length: Math.min(
									8,
									Math.max(
										0,
										options.count -
											(options.enabled ? mockColumnMenuStartIndex : 0),
									),
								),
							},
							(_value, offset) => {
								const index =
									(options.enabled ? mockColumnMenuStartIndex : 0) + offset;
								return {
									index,
									start: starts[index],
									end: starts[index] + sizes[index],
									size: sizes[index],
								};
							},
						)
					: [];
		return {
			getVirtualItems: () => virtualItems,
			getTotalSize: () => sizes.reduce((total, size) => total + size, 0),
			measure: () => undefined,
			scrollToOffset: () => undefined,
		};
	},
}));

jest.mock("@/hooks/useTableConfig", () => ({
	useTableConfig: () => ({
		hiddenColumns: new Set<string>(),
		toggleColumn: (column: string) => mockToggledColumns.push(column),
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
	copy: (value: string): void => {
		mockCopiedValues.push(value);
	},
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

jest.mock("@/ui/Input", () => ({
	Input: (props: import("react").InputHTMLAttributes<HTMLInputElement>) => (
		<input {...props} />
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
	ContextMenuCheckboxItem: ({
		children,
		checked,
		onCheckedChange,
		onSelect: _onSelect,
		...props
	}: MockChildrenProps & {
		checked?: boolean;
		onCheckedChange?: (checked: boolean) => void;
		onSelect?: (event: Event) => void;
		[key: string]: unknown;
	}) => (
		<button
			{...(props as ButtonHTMLAttributes<HTMLButtonElement>)}
			role="menuitemcheckbox"
			aria-checked={checked}
			onClick={() => onCheckedChange?.(!checked)}
		>
			{children}
		</button>
	),
	ContextMenuLabel: ({ children }: MockChildrenProps) => <div>{children}</div>,
	ContextMenuSeparator: () => <hr />,
}));

describe("Table layout and value resolution", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockCopiedValues.length = 0;
		mockToggledColumns.length = 0;
		mockHorizontalStartIndex = 0;
		mockColumnMenuStartIndex = 0;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		jest.useRealTimers();
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

	it("delegates row double clicks while ignoring interactive controls", () => {
		const values = [{ name: "first" }, { name: "second" }];
		const onRowClick = jest.fn<void, [typeof values[number], number]>();
		const onRowDoubleClick = jest.fn<
			void,
			[typeof values[number], number]
		>();
		const onAction = jest.fn();

		act(() => {
			root.render(
				<Table
					values={values}
					columns={["name"]}
					includeIndex={false}
					selectable
					onRowClick={onRowClick}
					onRowDoubleClick={onRowDoubleClick}
					actions={[
						{
							label: "Open row",
							onClick: onAction,
						},
					]}
				/>,
			);
		});

		const rows = container.querySelectorAll("tbody tr");
		const secondValueCell = rows[1]?.querySelectorAll("td")[1];
		act(() => {
			secondValueCell?.dispatchEvent(
				new MouseEvent("click", { bubbles: true }),
			);
			secondValueCell?.dispatchEvent(
				new MouseEvent("dblclick", { bubbles: true }),
			);
		});

		expect(onRowClick).toHaveBeenCalledWith(values[1], 1);
		expect(onRowDoubleClick).toHaveBeenCalledWith(values[1], 1);
		expect(container.querySelector("tbody")?.dataset.clickable).toBe("true");

		const textAction = rows[0]?.querySelector<HTMLButtonElement>(
			'button[aria-label="Open row"]',
		);
		expect(textAction?.textContent).toBe("Open row");

		act(() => {
			textAction?.click();
			textAction?.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
			rows[0]
				?.querySelector<HTMLButtonElement>('[role="checkbox"]')
				?.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
		});

		expect(onAction).toHaveBeenCalledTimes(1);
		expect(onRowClick).toHaveBeenCalledTimes(1);
		expect(onRowDoubleClick).toHaveBeenCalledTimes(1);
	});

	it("resolves literal, nested, hybrid dotted, and array field paths", () => {
		act(() => {
			root.render(
				<Table
					values={[
						{
							"gulp.unmapped.Guid": "literal-wins",
							"gulp.unmapped": {
								Guid: "hybrid-loses",
								records: [{ value: "array-value" }],
							},
							gulp: {
								enriched: { analyst: { name: "nested-value" } },
							},
							event: { code: 42 },
						},
					]}
					columns={[
						"gulp.unmapped.Guid",
						"gulp.unmapped.records.0.value",
						"gulp.enriched.analyst.name",
						"event.code",
					]}
					includeIndex={false}
				/>,
			);
		});

		const bodyText = container.querySelector("tbody")?.textContent ?? "";
		expect(bodyText).toContain("literal-wins");
		expect(bodyText).not.toContain("hybrid-loses");
		expect(bodyText).toContain("array-value");
		expect(bodyText).toContain("nested-value");
		expect(bodyText).toContain("42");
	});

	it("bounds mounted cells while preserving wide-table sort, resize, and sticky edges", () => {
		const columns = Array.from({ length: 2_000 }, (_value, index) =>
			`field.${index.toString().padStart(4, "0")}`,
		);
		const row = Object.fromEntries(
			columns.map((column, index) => [column, `value-${index}`]),
		);
		const rows = Array.from({ length: 200 }, (_value, index) => ({
			...row,
			_id: `row-${index}`,
		}));
		const handleSort = jest.fn();
		const actions = [
			{
				icon: "Search" as const,
				label: "Inspect",
				onClick: () => undefined,
			},
		];
		const renderTable = (): void => {
			root.render(
				<Table
					values={rows}
					columns={columns}
					includeIndex={false}
					selectable
					actions={actions}
					onSort={handleSort}
					horizontalVirtualization={{ threshold: 40, overscan: 3 }}
				/>,
			);
		};

		act(renderTable);
		expect(container.querySelectorAll("tbody td").length < 100).toBe(true);
		expect(container.querySelectorAll("tbody tr").length < 12).toBe(true);
		expect(container.textContent).toContain("field.0000");
		expect(container.textContent).not.toContain("field.1999");
		expect(
			container.querySelectorAll('[data-sticky-column="selection"]'),
		).toHaveLength(9);
		expect(
			container.querySelectorAll('[data-sticky-column="actions"]'),
		).toHaveLength(9);

		const firstHeader = Array.from(container.querySelectorAll("th")).find(
			(header) => header.textContent?.includes("field.0000"),
		);
		act(() => firstHeader?.click());
		expect(handleSort).toHaveBeenCalledWith("field.0000");

		const resizeHandle = container.querySelector<HTMLElement>(
			'[data-table-resize-column="field.0000"]',
		);
		const firstColumn = container.querySelector<HTMLTableColElement>(
			"colgroup col:nth-child(2)",
		);
		if (!resizeHandle || !firstHeader || !firstColumn) {
			throw new Error("Expected the first virtualized column resize controls");
		}
		firstHeader.getBoundingClientRect = () =>
			({ width: 100 } as DOMRect);
		act(() => {
			resizeHandle.dispatchEvent(
				new MouseEvent("mousedown", { bubbles: true, clientX: 10 }),
			);
			window.dispatchEvent(new MouseEvent("mousemove", { clientX: 50 }));
			window.dispatchEvent(new MouseEvent("mouseup", { clientX: 50 }));
		});
		expect(firstColumn.style.width).toBe("140px");

		mockHorizontalStartIndex = 1_994;
		act(renderTable);
		expect(container.querySelectorAll("tbody td").length < 100).toBe(true);
		expect(container.querySelectorAll("tbody tr").length < 12).toBe(true);
		expect(container.textContent).toContain("field.1999");
		expect(container.textContent).not.toContain("field.0000");
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

	it("virtualizes every column menu match while search keeps fields reachable", () => {
		const columns = Array.from({ length: 2_000 }, (_value, index) =>
			`wide-field-${index.toString().padStart(4, "0")}`,
		);
		act(() => {
			root.render(
				<Table
					values={[{ "wide-field-0000": "first" }]}
					columns={columns}
					includeIndex={false}
					columnVisibility
					horizontalVirtualization={{ threshold: 40 }}
				/>,
			);
		});

		const table = container.querySelector("table");
		act(() => {
			table?.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true }));
		});
		expect(
			document
				.querySelector("[data-column-menu-count]")
				?.getAttribute("data-column-menu-count"),
		).toBe("2000");
		expect(document.querySelectorAll("[data-column-menu-option]")).toHaveLength(
			8,
		);

		mockColumnMenuStartIndex = 1_992;
		act(() => {
			root.render(
				<Table
					values={[{ "wide-field-0000": "first" }]}
					columns={columns}
					includeIndex={false}
					columnVisibility
					horizontalVirtualization={{ threshold: 40 }}
				/>,
			);
		});
		expect(document.body.textContent).toContain("wide-field-1999");

		const searchInput = document.querySelector<HTMLInputElement>(
			'input[aria-label="common.search"]',
		);
		if (!searchInput) throw new Error("Expected the column search input");
		mockColumnMenuStartIndex = 0;
		act(() => {
			const valueSetter = Object.getOwnPropertyDescriptor(
				HTMLInputElement.prototype,
				"value",
			)?.set;
			valueSetter?.call(searchInput, "wide-field-1999");
			searchInput.dispatchEvent(new Event("input", { bubbles: true }));
		});

		const matchingOptions = document.querySelectorAll<HTMLElement>(
			"[data-column-menu-option]",
		);
		expect(matchingOptions).toHaveLength(1);
		expect(matchingOptions[0].dataset.columnMenuOption).toBe(
			"wide-field-1999",
		);
		act(() => matchingOptions[0].click());
		expect(mockToggledColumns).toEqual(["wide-field-1999"]);
	});

	it("places an interactive overflow tooltip in the roomier viewport direction", () => {
		Object.defineProperty(window, "innerWidth", {
			configurable: true,
			value: 800,
		});
		Object.defineProperty(window, "innerHeight", {
			configurable: true,
			value: 600,
		});
		const longValue = "a long cell value that must remain available in full";

		act(() => {
			root.render(
				<Table
					values={[{ value: longValue }]}
					columns={["value"]}
					includeIndex={false}
				/>,
			);
		});

		const trigger = container.querySelector<HTMLElement>(
			'[data-table-tooltip-trigger="true"]',
		);
		if (!trigger) {
			throw new Error("Overflow tooltip trigger was not rendered");
		}
		Object.defineProperty(trigger, "clientWidth", {
			configurable: true,
			value: 100,
		});
		Object.defineProperty(trigger, "scrollWidth", {
			configurable: true,
			value: 500,
		});
		trigger.getBoundingClientRect = () =>
			({
				x: 700,
				y: 540,
				width: 80,
				height: 28,
				top: 540,
				right: 780,
				bottom: 568,
				left: 700,
				toJSON: () => undefined,
			}) as DOMRect;

		act(() => {
			trigger.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
		});

		const tooltip = document.querySelector<HTMLElement>(
			'[data-table-tooltip="true"]',
		);
		if (!tooltip) {
			throw new Error("Overflow tooltip was not rendered");
		}
		expect((tooltip.textContent ?? "").includes(longValue)).toBe(true);
		expect(tooltip?.style.bottom).toBe("66px");
		expect(tooltip?.style.right).toBe("20px");
		expect(tooltip?.style.maxWidth).toBe("400px");
		expect(tooltip?.style.maxHeight).toBe("300px");

		const copyButton = tooltip?.querySelector<HTMLButtonElement>(
			'button[aria-label="common.copy"]',
		);
		act(() => copyButton?.click());
		expect(mockCopiedValues).toEqual([longValue]);
	});

	it("keeps a pinned overflow tooltip mounted for partial text selection", () => {
		jest.useFakeTimers();
		const longValue = "alpha bravo charlie";

		act(() => {
			root.render(
				<Table
					values={[{ value: longValue }]}
					columns={["value"]}
					includeIndex={false}
				/>,
			);
		});

		const trigger = container.querySelector<HTMLElement>(
			'[data-table-tooltip-trigger="true"]',
		);
		if (!trigger) {
			throw new Error("Overflow tooltip trigger was not rendered");
		}
		Object.defineProperty(trigger, "clientWidth", {
			configurable: true,
			value: 80,
		});
		Object.defineProperty(trigger, "scrollWidth", {
			configurable: true,
			value: 300,
		});

		act(() => {
			trigger.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
		});

		const tooltip = document.querySelector<HTMLElement>(
			'[data-table-tooltip="true"]',
		);
		const tooltipText = tooltip?.querySelector<HTMLElement>('[tabindex="0"]');
		const textNode = tooltipText?.firstChild;
		if (!tooltip || !tooltipText || !textNode) {
			throw new Error("Selectable tooltip text was not rendered");
		}

		act(() => {
			tooltipText.dispatchEvent(new Event("pointerdown", { bubbles: true }));
		});
		expect(tooltip.dataset.tableTooltipPinned).toBe("true");

		const range = document.createRange();
		range.setStart(textNode, 6);
		range.setEnd(textNode, 11);
		const selection = window.getSelection();
		selection?.removeAllRanges();
		selection?.addRange(range);
		expect(selection?.toString()).toBe("bravo");

		act(() => {
			trigger.dispatchEvent(
				new MouseEvent("mouseout", {
					bubbles: true,
					relatedTarget: document.body,
				}),
			);
			jest.advanceTimersByTime(200);
		});
		expect(document.body.contains(tooltip)).toBe(true);
		expect(selection?.toString()).toBe("bravo");
		selection?.removeAllRanges();
		jest.useRealTimers();
	});
});
