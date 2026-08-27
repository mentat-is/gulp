import {
	act,
	type ButtonHTMLAttributes,
	type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import {
	parseTablePageInput,
	TablePagination,
} from "@/ui/TablePagination";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const onPageChange = jest.fn();
const onPageSizeChange = jest.fn();

jest.mock("@/locales", () => ({
	Locale: {
		/** Returns deterministic localized messages for pagination assertions. */
		use: (): {
			t: (key: string, vars?: Record<string, string | number>) => string;
		} => ({
			t: (key, vars) => {
				if (key === "tableView.pageOf") {
					return `Page ${vars?.page} of ${vars?.total}`;
				}
				if (key === "tableView.previousPage") return "Previous Page";
				if (key === "tableView.nextPage") return "Next Page";
				return key;
			},
		}),
	},
}));

jest.mock("@/ui/Select", () => {
	const Wrapper = ({ children }: { children?: ReactNode }): ReactNode =>
		<div>{children}</div>;
	return {
		Select: {
			Root: Wrapper,
			Trigger: Wrapper,
			Value: (): null => null,
			Content: Wrapper,
			Item: Wrapper,
		},
	};
});

jest.mock("@/ui/utils", () => ({
	/** Joins the class tokens used by the shared primitive components. */
	cn: (...classNames: unknown[]): string => classNames.filter(Boolean).join(" "),
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		icon,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & { icon?: string }): ReactNode =>
		<button {...props}>{icon}</button>,
}));

/**
 * Updates a controlled input through the native setter so React receives the event.
 * @param input Input element being edited.
 * @param value Draft value entered by the test user.
 */
function enterInputValue(input: HTMLInputElement, value: string): void {
	const valueSetter = Object.getOwnPropertyDescriptor(
		HTMLInputElement.prototype,
		"value",
	)?.set;
	valueSetter?.call(input, value);
	input.dispatchEvent(new Event("input", { bubbles: true }));
}

describe("TablePagination", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
		onPageChange.mock.calls.splice(0);
		onPageSizeChange.mock.calls.splice(0);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	/** Renders pagination with localized labels and overridable values. */
	const renderPagination = (
		overrides: Partial<TablePagination.Props> = {},
	): void => {
		act(() => {
			root.render(
				<TablePagination
					currentPage={2}
					totalItems={250}
					pageSize={50}
					onPageChange={onPageChange}
					onPageSizeChange={onPageSizeChange}
					{...overrides}
				/>,
			);
		});
	};

	it("accepts only positive integer pages inside the maximum", () => {
		expect(parseTablePageInput("1", 5)).toBe(1);
		expect(parseTablePageInput("5", 5)).toBe(5);
		expect(parseTablePageInput("", 5)).toBeNull();
		expect(parseTablePageInput("0", 5)).toBeNull();
		expect(parseTablePageInput("-1", 5)).toBeNull();
		expect(parseTablePageInput("1.5", 5)).toBeNull();
		expect(parseTablePageInput("6", 5)).toBeNull();
	});

	it("commits a valid edited page and rejects invalid drafts", () => {
		renderPagination();
		const pageInput = container.querySelector<HTMLInputElement>(
			'input[type="number"]',
		);
		if (!pageInput) throw new Error("Expected the editable page input");

		act(() => enterInputValue(pageInput, "4"));
		expect(pageInput.getAttribute("aria-invalid")).toBe("false");
		act(() => {
			pageInput.dispatchEvent(
				new KeyboardEvent("keydown", { key: "Enter", bubbles: true }),
			);
		});
		expect(onPageChange).toHaveBeenCalledWith(4);

		act(() => enterInputValue(pageInput, "0"));
		expect(pageInput.getAttribute("aria-invalid")).toBe("true");
		act(() => {
			pageInput.dispatchEvent(
				new KeyboardEvent("keydown", { key: "Enter", bubbles: true }),
			);
		});

		act(() => enterInputValue(pageInput, "6"));
		expect(pageInput.getAttribute("aria-invalid")).toBe("true");
		act(() => pageInput.blur());
		expect(onPageChange).toHaveBeenCalledTimes(1);
	});

	it("navigates within the derived page range", () => {
		renderPagination();

		act(() => {
			container
				.querySelector<HTMLButtonElement>('button[title="Previous Page"]')
				?.click();
		});
		expect(onPageChange.mock.calls[onPageChange.mock.calls.length - 1]).toEqual([
			1,
		]);

		act(() => {
			container
				.querySelector<HTMLButtonElement>('button[title="Next Page"]')
				?.click();
		});
		expect(onPageChange.mock.calls[onPageChange.mock.calls.length - 1]).toEqual([
			3,
		]);

		renderPagination({ currentPage: 5 });
		expect(
			container.querySelector<HTMLButtonElement>(
				'button[title="Next Page"]',
			)?.disabled,
		).toBe(true);
	});
});
