import {
	act,
	forwardRef as mockForwardRef,
	type ButtonHTMLAttributes,
	type InputHTMLAttributes,
	type ReactNode,
} from "react";
import { createPortal as mockCreatePortal } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { FieldVisibilityMenu } from "@/ui/FieldVisibilityMenu";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockChildrenProps {
	children?: ReactNode;
}

interface MockButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
	icon?: string;
	variant?: string;
	size?: string;
}

interface MockInputProps extends InputHTMLAttributes<HTMLInputElement> {}

let mockOpenChange: ((open: boolean) => void) | undefined;
let mockDndContextProps: {
	onDragEnd?: (event: {
		active: { id: string };
		over: { id: string } | null;
	}) => void;
} | null = null;

jest.mock("@dnd-kit/core", () => ({
	DndContext: ({
		children,
		...props
	}: MockChildrenProps & Record<string, unknown>) => {
		mockDndContextProps = props as typeof mockDndContextProps;
		return <>{children}</>;
	},
	DragOverlay: ({ children }: MockChildrenProps) => <>{children}</>,
	KeyboardSensor: (): null => null,
	PointerSensor: (): null => null,
	closestCenter: (): null => null,
	useSensor: (...values: unknown[]) => values,
	useSensors: (...values: unknown[]) => values,
}));

jest.mock("@dnd-kit/sortable", () => ({
	SortableContext: ({ children }: MockChildrenProps) => <>{children}</>,
	sortableKeyboardCoordinates: (): null => null,
	verticalListSortingStrategy: (): null => null,
	useSortable: () => ({
		attributes: {},
		listeners: {},
		setNodeRef: () => undefined,
		transform: null,
		transition: undefined,
		isDragging: false,
	}),
}));

jest.mock("@dnd-kit/utilities", () => ({
	CSS: { Transform: { toString: () => undefined } },
}));

jest.mock("@tanstack/react-virtual", () => ({
	useVirtualizer: ({ count }: { count: number }) => ({
		getVirtualItems: () =>
			Array.from({ length: count }, (_value, index) => ({
				index,
				start: index * 30,
				size: 30,
			})),
		getTotalSize: () => count * 30,
		scrollToOffset: () => undefined,
	}),
}));

jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: (key: string) => key }) },
}));

jest.mock("@/ui/utils", () => ({
	cn: (...values: unknown[]) => values.filter(Boolean).join(" "),
}));

jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));

jest.mock("@/ui/Button", () => ({
	Button: mockForwardRef<HTMLButtonElement, MockButtonProps>(
		({ icon: _icon, variant: _variant, size: _size, ...props }, ref) => (
			<button ref={ref} {...props} />
		),
	),
}));

jest.mock("@/ui/Input", () => ({
	Input: mockForwardRef<HTMLInputElement, MockInputProps>((props, ref) => (
		<input ref={ref} {...props} />
	)),
}));

jest.mock("@/ui/Popover", () => ({
	Popover: {
		Anchor: (): null => null,
		Root: ({
			children,
			onOpenChange,
		}: MockChildrenProps & { onOpenChange?: (open: boolean) => void }) => {
			mockOpenChange = onOpenChange;
			return <>{children}</>;
		},
		Trigger: ({ children }: MockChildrenProps) => (
			<span onClick={() => mockOpenChange?.(true)}>{children}</span>
		),
		Content: ({
			children,
			container,
			align: _align,
			sideOffset: _sideOffset,
			...props
		}: MockChildrenProps & {
			container?: HTMLElement | null;
			align?: string;
			sideOffset?: number;
		}) =>
			container
				? mockCreatePortal(<div {...props}>{children}</div>, container)
				: <div {...props}>{children}</div>,
	},
}));

describe("FieldVisibilityMenu", () => {
	let container: HTMLDivElement;
	let portalDocument: Document;
	let root: Root;

	beforeEach(() => {
		mockOpenChange = undefined;
		mockDndContextProps = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		portalDocument = document.implementation.createHTMLDocument("detached");
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("supports field copy, detached portals, filtering, selection, and reorder", () => {
		const onToggleItem = jest.fn();
		const onSelectAll = jest.fn();
		const onDeselectAll = jest.fn();
		const onMoveItem = jest.fn();

		act(() => {
			root.render(
				<FieldVisibilityMenu
					orderedItems={["alpha", "beta", "gamma"]}
					hiddenItems={new Set(["beta"])}
					onToggleItem={onToggleItem}
					onSelectAll={onSelectAll}
					onDeselectAll={onDeselectAll}
					onMoveItem={onMoveItem}
					terminology="fields"
					portalContainer={portalDocument.body}
				/>,
			);
		});

		expect(
			container.querySelector('[aria-label="eventDialog.fieldVisibility"]'),
		).not.toBeNull();
		act(() => {
			container
				.querySelector<HTMLButtonElement>(
					'[data-field-visibility-trigger="true"]',
				)
				?.click();
		});

		const menu = portalDocument.body.querySelector(
			'[data-field-visibility-menu="true"]',
		);
		expect(menu).not.toBeNull();
		expect(document.body.contains(menu)).toBe(false);
		expect(menu?.textContent).toContain("eventDialog.selectedFields");
		expect(menu?.textContent).toContain("eventDialog.unselectedFields");

		act(() => {
			menu
				?.querySelector<HTMLButtonElement>(
					'[data-column-visibility-action="deselect-all"]',
				)
				?.click();
			menu
				?.querySelector<HTMLButtonElement>(
					'[data-column-visibility-action="select-all"]',
				)
				?.click();
			menu
				?.querySelector<HTMLElement>('[data-column-menu-option="beta"]')
				?.querySelector<HTMLButtonElement>('button[role="checkbox"]')
				?.click();
		});
		expect(onDeselectAll).toHaveBeenCalledTimes(1);
		expect(onSelectAll).toHaveBeenCalledTimes(1);
		expect(onToggleItem).toHaveBeenCalledWith("beta");

		const search = menu?.querySelector<HTMLInputElement>(
			'input[aria-label="common.search"]',
		);
		if (!search) throw new Error("Expected the field search input");
		act(() => {
			const setter = Object.getOwnPropertyDescriptor(
				HTMLInputElement.prototype,
				"value",
			)?.set;
			setter?.call(search, "gamma");
			search.dispatchEvent(new Event("input", { bubbles: true }));
		});
		expect(
			menu?.querySelector<HTMLButtonElement>(
				'button[title="eventDialog.fieldReorderSearchDisabled"]',
			)?.disabled,
		).toBe(true);
		expect(menu?.querySelectorAll("[data-column-menu-option]")).toHaveLength(1);

		act(() => {
			mockDndContextProps?.onDragEnd?.({
				active: { id: "gamma" },
				over: { id: "alpha" },
			});
		});
		expect(onMoveItem).toHaveBeenCalledWith("gamma", "alpha");
	});
});
