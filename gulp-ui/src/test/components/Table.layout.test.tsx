import { act } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import { Table } from "@/components/Table";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

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
		const tableViewport = table?.parentElement;
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
