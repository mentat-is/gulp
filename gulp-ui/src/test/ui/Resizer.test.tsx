import { act, createRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { Resizer } from "@/ui/Resizer";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/utils", () => ({
	cn: (...values: unknown[]): string => values.filter(Boolean).join(" "),
}));

describe("Resizer deferred commit", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		Object.defineProperty(window, "innerWidth", {
			configurable: true,
			value: 1_000,
		});
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("previews width in the DOM and commits only the final bounded value", () => {
		const target = createRef<HTMLDivElement>();
		const commit = jest.fn();

		act(() => {
			root.render(
				<>
					<div ref={target} style={{ width: 400 }} />
					<Resizer
						init={400}
						limits={{ min: 0.2, max: 0.6 }}
						set={commit}
						target={target}
					/>
				</>,
			);
		});

		const handle = container.children[1] as HTMLElement;
		act(() => {
			handle.dispatchEvent(
				new MouseEvent("mousedown", { bubbles: true, clientX: 400 }),
			);
			window.dispatchEvent(new MouseEvent("mousemove", { clientX: 250 }));
		});

		expect(target.current?.style.width).toBe("550px");
		expect(commit).not.toHaveBeenCalled();

		act(() => {
			window.dispatchEvent(new MouseEvent("mousemove", { clientX: 100 }));
			window.dispatchEvent(new MouseEvent("mouseup"));
		});

		expect(target.current?.style.width).toBe("600px");
		expect(commit).toHaveBeenCalledTimes(1);
		expect(commit).toHaveBeenCalledWith(600);
	});

	it("does not publish state for a resize handle click without movement", () => {
		const target = createRef<HTMLDivElement>();
		const commit = jest.fn();

		act(() => {
			root.render(
				<>
					<div ref={target} style={{ width: 400 }} />
					<Resizer init={400} set={commit} target={target} />
				</>,
			);
		});

		const handle = container.children[1] as HTMLElement;
		act(() => {
			handle.dispatchEvent(
				new MouseEvent("mousedown", { bubbles: true, clientX: 400 }),
			);
			window.dispatchEvent(new MouseEvent("mouseup"));
		});

		expect(commit).not.toHaveBeenCalled();
		expect(target.current?.style.width).toBe("400px");
	});

	it("starts from the rendered target width instead of stale persisted state", () => {
		const target = createRef<HTMLDivElement>();
		const commit = jest.fn();

		act(() => {
			root.render(
				<>
					<div ref={target} style={{ width: 360 }} />
					<Resizer init={400} set={commit} target={target} />
				</>,
			);
		});

		jest.spyOn(target.current as HTMLDivElement, "getBoundingClientRect").mockReturnValue({
			bottom: 900,
			height: 900,
			left: 640,
			right: 1_000,
			top: 0,
			width: 360,
			x: 640,
			y: 0,
			toJSON: () => undefined,
		});

		const handle = container.children[1] as HTMLElement;
		act(() => {
			handle.dispatchEvent(
				new MouseEvent("mousedown", { bubbles: true, clientX: 640 }),
			);
			window.dispatchEvent(new MouseEvent("mousemove", { clientX: 590 }));
			window.dispatchEvent(new MouseEvent("mouseup"));
		});

		expect(target.current?.style.width).toBe("410px");
		expect(commit).toHaveBeenCalledTimes(1);
		expect(commit).toHaveBeenCalledWith(410);
	});
});
