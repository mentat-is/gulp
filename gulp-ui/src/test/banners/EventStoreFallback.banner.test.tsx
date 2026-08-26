import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { EventStoreFallbackBanner } from "@/banners/EventStoreFallback.banner";
import { EVENT_STORE_LOD_ROLLBACK_KEY } from "@/event-store/EventStore.rollout";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

let mockDismissRequest = jest.fn();

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({ dismissEventStoreFallbackRequest: mockDismissRequest }),
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({
			language: "en",
			t: (key: string, vars?: Record<string, string | number>) =>
				vars?.count === undefined ? key : `${key}:${vars.count}`,
		}),
	},
}));

jest.mock("@/ui/Banner", () => ({
	Banner: ({
		children,
		done,
		onClose,
	}: {
		children?: ReactNode;
		done?: ReactNode;
		onClose?: () => void;
	}) => (
		<div>
			<button type="button" data-action="dismiss" onClick={onClose} />
			{children}
			{done}
		</div>
	),
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		onClick,
		disabled,
	}: {
		children?: ReactNode;
		onClick?: () => void;
		disabled?: boolean;
	}) => (
		<button type="button" disabled={disabled} onClick={onClick}>
			{children}
		</button>
	),
}));

jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));

jest.mock("@/ui/Toggle", () => ({
	Toggle: ({ onCheckedChange }: { onCheckedChange?: (value: boolean) => void }) => (
		<button
			type="button"
			data-action="confirm"
			onClick={() => onCheckedChange?.(true)}
		/>
	),
}));

describe("EventStoreFallbackBanner", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockDismissRequest = jest.fn();
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("does not persist or reload before the user chooses an action", () => {
		const getItem = jest.fn(() => null);
		const setItem = jest.fn();
		const reload = jest.fn();
		act(() => {
			root.render(
				<EventStoreFallbackBanner
					request={{
						reason: "quota",
						message: "quota",
						estimatedEventCount: 14_000_000,
					}}
					storage={{ getItem, setItem }}
					reloadTarget={{ reload }}
				/>,
			);
		});

		expect(getItem).not.toHaveBeenCalled();
		expect(setItem).not.toHaveBeenCalled();
		expect(reload).not.toHaveBeenCalled();
		expect(mockDismissRequest).not.toHaveBeenCalled();
	});

	it("retries scalable mode without disabling LOD", () => {
		const values = new Map<string, string>();
		const setItem = jest.fn((key: string, value: string) =>
			values.set(key, value),
		);
		const reload = jest.fn();
		act(() => {
			root.render(
				<EventStoreFallbackBanner
					request={{
						reason: "renderer-fatal",
						message: "fatal",
						estimatedEventCount: 10,
					}}
					storage={{
						getItem: (key) => values.get(key) ?? null,
						setItem,
					}}
					reloadTarget={{ reload }}
				/>,
			);
		});
		const retry = Array.from(container.querySelectorAll("button")).find(
			(button) =>
				button.textContent === "eventStore.fallback.retryScalable",
		);
		act(() => retry?.click());

		expect(setItem).not.toHaveBeenCalled();
		expect(values.get(EVENT_STORE_LOD_ROLLBACK_KEY)).toBeUndefined();
		expect(reload).toHaveBeenCalledTimes(1);
		expect(mockDismissRequest).toHaveBeenCalledTimes(1);
	});

	it("requires confirmation and verifies the legacy marker before reloading", () => {
		const values = new Map<string, string>();
		const reload = jest.fn();
		act(() => {
			root.render(
				<EventStoreFallbackBanner
					request={{
						reason: "storage",
						message: "storage",
						estimatedEventCount: 1,
					}}
					storage={{
						getItem: (key) => values.get(key) ?? null,
						setItem: (key, value) => values.set(key, value),
					}}
					reloadTarget={{ reload }}
				/>,
			);
		});
		const compatibility = (): HTMLButtonElement | undefined =>
			Array.from(container.querySelectorAll("button")).find(
				(button) =>
					button.textContent === "eventStore.fallback.useCompatibility",
			);
		expect(compatibility()?.disabled).toBe(true);
		act(() =>
			container
				.querySelector<HTMLButtonElement>('[data-action="confirm"]')
				?.click(),
		);
		expect(compatibility()?.disabled).toBe(false);
		act(() => compatibility()?.click());

		expect(values.get(EVENT_STORE_LOD_ROLLBACK_KEY)).toBe("false");
		expect(reload).toHaveBeenCalledTimes(1);
		expect(mockDismissRequest).toHaveBeenCalledTimes(1);
	});

	it("keeps the banner recoverable when rollout storage is blocked", () => {
		const reload = jest.fn();
		act(() => {
			root.render(
				<EventStoreFallbackBanner
					request={{
						reason: "storage",
						message: "storage",
						estimatedEventCount: 1,
					}}
					storage={{
						getItem: () => null,
						setItem: () => {
							throw new Error("blocked");
						},
					}}
					reloadTarget={{ reload }}
				/>,
			);
		});
		act(() =>
			container
				.querySelector<HTMLButtonElement>('[data-action="confirm"]')
				?.click(),
		);
		const compatibility = Array.from(
			container.querySelectorAll("button"),
		).find(
			(button) =>
				button.textContent === "eventStore.fallback.useCompatibility",
		);
		act(() => compatibility?.click());

		expect(reload).not.toHaveBeenCalled();
		expect(mockDismissRequest).not.toHaveBeenCalled();
		expect(container.querySelector('[role="alert"]')?.textContent).toBe(
			"eventStore.fallback.storageFailed",
		);
	});

	it("dismisses without changing storage or reloading", () => {
		const setItem = jest.fn();
		const reload = jest.fn();
		act(() => {
			root.render(
				<EventStoreFallbackBanner
					request={{
						reason: "renderer-unavailable",
						message: "unsupported",
						estimatedEventCount: 0,
					}}
					storage={{ getItem: () => null, setItem }}
					reloadTarget={{ reload }}
				/>,
			);
		});
		act(() =>
			container
				.querySelector<HTMLButtonElement>('[data-action="dismiss"]')
				?.click(),
		);

		expect(mockDismissRequest).toHaveBeenCalledTimes(1);
		expect(setItem).not.toHaveBeenCalled();
		expect(reload).not.toHaveBeenCalled();
	});
});
