import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import { Frame } from "@/banners/Frame.banner";
import { requestStore } from "@/store/request.store";
import { scrollStore } from "@/store/scroll.store";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockChildrenProps {
	children?: ReactNode;
}

interface MockBannerProps extends MockChildrenProps {
	done?: ReactNode;
}

interface MockButtonProps extends MockChildrenProps {
	icon?: string;
	onClick?: () => void;
}

const mockRefetch = jest.fn();
const mockSetTimelineFrame = jest.fn();
const mockDestroyBanner = jest.fn();
const mockSelectedSourceId = "selected-frame-source" as Source.Id;
const mockTimelineElement = document.createElement("div");
Object.defineProperty(mockTimelineElement, "clientWidth", {
	configurable: true,
	value: 600,
});

jest.mock("@/context/Application.context", () => ({
	Application: {
		/** Returns the minimal application services required by Frame.Banner. */
		use: (): unknown => ({
			Info: {
				width: 1_000,
				getTimelineScaleSnapshot: (): number => 1,
				setTimelineFrame: mockSetTimelineFrame,
				refetch: mockRefetch,
			},
			destroyBanner: mockDestroyBanner,
			app: {},
			timeline: { current: mockTimelineElement },
		}),
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		/** Uses translation keys as deterministic labels in the isolated banner. */
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

jest.mock("@/entities/Context", () => ({
	Context: {
		Entity: {
			frame: (): { min: number; max: number } => ({ min: 0, max: 1 }),
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			selected: (): Array<{ id: Source.Id }> => [
				{ id: mockSelectedSourceId },
			],
		},
	},
}));

jest.mock("@/dto/Logger.class", () => ({
	Logger: {
		error: jest.fn(),
	},
}));

jest.mock("@/ui/Banner", () => ({
	Banner: ({ children, done }: MockBannerProps): ReactNode => (
		<div>
			{done}
			{children}
		</div>
	),
}));

jest.mock("@/ui/Button", () => ({
	Button: ({ children, icon, onClick }: MockButtonProps): ReactNode => (
		<button type="button" data-icon={icon} onClick={onClick}>
			{children}
		</button>
	),
}));

jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
}));

jest.mock("@/ui/Toggle", () => ({
	Toggle: (): null => null,
}));

jest.mock("@/ui/Input", () => ({
	Input: ({
		label,
		valid: _valid,
		variant: _variant,
		icon: _icon,
		onIconClick: _onIconClick,
		...props
	}: React.InputHTMLAttributes<HTMLInputElement> & {
		label?: string;
		valid?: boolean;
		variant?: string;
		icon?: string;
		onIconClick?: () => void;
	}): ReactNode => <input aria-label={label} {...props} />,
}));

/** Updates a native input through React's delegated input event. */
function changeInput(input: HTMLInputElement, value: string): void {
	const valueSetter = Object.getOwnPropertyDescriptor(
		HTMLInputElement.prototype,
		"value",
	)?.set;

	act(() => {
		valueSetter?.call(input, value);
		input.dispatchEvent(new Event("input", { bubbles: true }));
	});
}

describe("Frame.Banner date selection", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		requestStore.reset();
		scrollStore.setScroll(0, -26);
		mockRefetch.mock.calls.length = 0;
		mockSetTimelineFrame.mock.calls.length = 0;
		mockDestroyBanner.mock.calls.length = 0;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		requestStore.reset();
		scrollStore.setScroll(0, -26);
	});

	it("preserves an uncommitted date while render loading state changes", () => {
		const callback = jest.fn();
		const initialFrame = {
			min: new Date("2024-01-01T00:00:00").valueOf(),
			max: new Date("2024-01-31T23:59:00").valueOf(),
		};

		act(() => {
			root.render(
				<Frame.Banner frame={initialFrame} callback={callback} />,
			);
		});

		const fromInput = container.querySelector<HTMLInputElement>(
			'input[type="datetime-local"]',
		);
		expect(fromInput).not.toBeNull();

		changeInput(fromInput!, "2024-01-02T03:04");
		expect(fromInput!.value).toBe("2024-01-02T03:04");

		act(() => {
			requestStore.setLoading(
				"render-request" as Request.Id,
				"render-source" as Source.Id,
			);
		});

		expect(fromInput!.isConnected).toBe(true);
		expect(fromInput!.value).toBe("2024-01-02T03:04");

		act(() => {
			fromInput!.dispatchEvent(new FocusEvent("focusout", { bubbles: true }));
		});
		const doneButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		expect(doneButton).not.toBeNull();
		act(() => doneButton!.click());

		expect(callback).toHaveBeenCalledWith({
			min: new Date("2024-01-02T03:04").valueOf(),
			max: initialFrame.max,
		});
	});

	it("refetches every selected source with the committed frame", () => {
		const initialFrame = {
			min: new Date("2025-03-17T00:00:00").valueOf(),
			max: new Date("2025-03-27T00:00:00").valueOf(),
		};

		act(() => {
			root.render(<Frame.Banner frame={initialFrame} />);
		});

		const doneButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		expect(doneButton).not.toBeNull();
		act(() => doneButton!.click());

		expect(mockRefetch).toHaveBeenCalledWith({
			ids: [mockSelectedSourceId],
			frame: initialFrame,
		});
		expect(mockSetTimelineFrame).not.toHaveBeenCalled();
		expect(mockDestroyBanner).toHaveBeenCalled();
	});

	it("centers a fresh operation frame in the visible viewport", () => {
		const initialFrame = { min: 0, max: 1_000 };

		act(() => {
			root.render(
				<Frame.Banner
					frame={initialFrame}
					centerViewportOnSave
				/>,
			);
		});

		const doneButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		act(() => doneButton?.click());

		expect(scrollStore.getX()).toBe(200);
	});

	it("does not replace an existing scroll unless initial centering is requested", () => {
		const initialFrame = { min: 0, max: 1_000 };
		scrollStore.setScrollX(137);

		act(() => {
			root.render(<Frame.Banner frame={initialFrame} />);
		});

		const doneButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		act(() => doneButton?.click());

		expect(scrollStore.getX()).toBe(137);
	});
});
