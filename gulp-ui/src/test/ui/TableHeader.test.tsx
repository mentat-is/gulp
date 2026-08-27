import {
	act,
	type ButtonHTMLAttributes,
	type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import { TableHeader } from "@/ui/TableHeader";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const mockSelectionChange = jest.fn();
const mockAction = jest.fn();
const mockSortChange = jest.fn();
const mockSortDirectionToggle = jest.fn();
const mockPageChange = jest.fn();
const mockPageSizeChange = jest.fn();
let mockPaginationContainer: HTMLElement | null | undefined;

jest.mock("@/locales", () => ({
	Locale: {
		/** Returns the key so shared labels remain deterministic. */
		use: (): { t: (key: string) => string } => ({ t: (key) => key }),
	},
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		icon,
		...props
	}: ButtonHTMLAttributes<HTMLButtonElement> & { icon?: string }): ReactNode =>
		<button {...props}>{props.children ?? icon}</button>,
}));

jest.mock("@/ui/Checkbox", () => ({
	Checkbox: ({
		checked,
		onCheckedChange,
	}: {
		checked: boolean;
		onCheckedChange: (checked: boolean) => void;
	}): ReactNode =>
		<button
			type="button"
			data-testid="select-all-checkbox"
			onClick={() => onCheckedChange(!checked)}
		/>,
}));

jest.mock("@/ui/Icon", () => ({
	Icon: ({ name }: { name: string }): ReactNode => <span>{name}</span>,
}));

jest.mock("@/ui/Label", () => ({
	Label: ({ value }: { value: string }): ReactNode => <span>{value}</span>,
}));

jest.mock("@/ui/Popover", () => {
	const Wrapper = ({ children }: { children?: ReactNode }): ReactNode =>
		<>{children}</>;
	return {
		Popover: {
			Root: Wrapper,
			Trigger: Wrapper,
			Content: Wrapper,
		},
	};
});

jest.mock("@/ui/Select", () => {
	const Wrapper = ({ children }: { children?: ReactNode }): ReactNode =>
		<>{children}</>;
	return {
		Select: {
			Root: ({
				children,
				onValueChange,
			}: {
				children?: ReactNode;
				onValueChange: (value: string) => void;
			}): ReactNode =>
				<div
					data-testid="sort-select"
					onClick={() => onValueChange("title")}
				>
					{children}
				</div>,
			Trigger: Wrapper,
			Value: (): null => null,
			Content: Wrapper,
			Item: Wrapper,
		},
	};
});

jest.mock("@/ui/TablePagination", () => ({
	TablePagination: ({
		currentPage,
		container,
		onPageChange,
	}: {
		currentPage: number;
		container?: HTMLElement | null;
		onPageChange: (page: number) => void;
	}): ReactNode => {
		mockPaginationContainer = container;
		return (
			<button
				type="button"
				data-testid="pagination"
				onClick={() => onPageChange(currentPage + 1)}
			>
				{currentPage}
			</button>
		);
	},
}));

describe("TableHeader", () => {
	let container: HTMLDivElement;
	let portalContainer: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		container = document.createElement("div");
		portalContainer = document.createElement("div");
		document.body.append(container, portalContainer);
		root = createRoot(container);
		mockPaginationContainer = undefined;
		[
			mockSelectionChange,
			mockAction,
			mockSortChange,
			mockSortDirectionToggle,
			mockPageChange,
			mockPageSizeChange,
		].forEach((mockFunction) => mockFunction.mock.calls.splice(0));
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		portalContainer.remove();
	});

	it("delegates selection, actions, sorting, and pagination to the owning view", () => {
		act(() => {
			root.render(
				<TableHeader
					allSelected={false}
					selectionLabel="Select results"
					onSelectionChange={mockSelectionChange}
					actions={[
						{
							id: "delete",
							label: "Delete results",
							onSelect: mockAction,
						},
					]}
					sorting={{
						value: "timestamp",
						options: [
							{ value: "timestamp" },
							{ value: "title" },
						],
						direction: "asc",
						onValueChange: mockSortChange,
						onDirectionToggle: mockSortDirectionToggle,
					}}
					pagination={{
						currentPage: 2,
						totalItems: 100,
						pageSize: 20,
						onPageChange: mockPageChange,
						onPageSizeChange: mockPageSizeChange,
					}}
					container={portalContainer}
				/>,
			);
		});

		act(() => {
			[...container.querySelectorAll("button")]
				.find((button) => button.textContent === "Select results")
				?.click();
			[...container.querySelectorAll("button")]
				.find((button) => button.textContent === "Delete results")
				?.click();
			container.querySelector<HTMLElement>('[data-testid="sort-select"]')
				?.click();
			container
				.querySelector<HTMLButtonElement>(
					'button[title="tableView.toggleSortDirection"]',
				)
				?.click();
			container
				.querySelector<HTMLButtonElement>('[data-testid="pagination"]')
				?.click();
		});

		expect(mockSelectionChange).toHaveBeenCalledWith(true);
		expect(mockAction).toHaveBeenCalledTimes(1);
		expect(mockSortChange).toHaveBeenCalledWith("title");
		expect(mockSortDirectionToggle).toHaveBeenCalledTimes(1);
		expect(mockPageChange).toHaveBeenCalledWith(3);
		expect(mockPaginationContainer).toBe(portalContainer);
	});
});
