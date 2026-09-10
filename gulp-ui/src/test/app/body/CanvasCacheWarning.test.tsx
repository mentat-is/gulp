import { act, createRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import {
	CanvasCacheWarningTooltip,
	hitTestCanvasCacheWarning,
	resolveCanvasCacheWarningTooltipPosition,
	type CanvasCacheWarningTooltipHandle,
} from "@/app/body/CanvasCacheWarning";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

describe("CanvasCacheWarningTooltip", () => {
	let container: HTMLDivElement;
	let root: Root;
	let handle: ReturnType<typeof createRef<CanvasCacheWarningTooltipHandle>>;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
		handle = createRef<CanvasCacheWarningTooltipHandle>();
		act(() => root.render(<CanvasCacheWarningTooltip ref={handle} />));
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("mounts one hidden non-interactive tooltip and no per-source trigger", () => {
		const tooltips = container.querySelectorAll(
			"[data-canvas-cache-warning-tooltip]",
		);
		expect(tooltips).toHaveLength(1);
		expect((tooltips[0] as HTMLElement).hidden).toBe(true);
		expect(tooltips[0].textContent).toBe(
			"source.cacheWarning.loadedFromCache",
		);
		expect(container.querySelector("[data-source-cache-warning]")).toBeNull();
		expect(container.querySelector('[tabindex="0"]')).toBeNull();
	});

	it("anchors once per source entry, switches source, and hides imperatively", () => {
		const tooltip = container.querySelector<HTMLElement>(
			"[data-canvas-cache-warning-tooltip]",
		);
		expect(tooltip).not.toBeNull();
		const measure = jest.fn(() => ({
			width: 80,
			height: 30,
		}) as DOMRect);
		if (tooltip) tooltip.getBoundingClientRect = measure;

		act(() => {
			handle.current?.show({
				sourceId: "source-a",
				x: 100,
				y: 100,
				bounds: { width: 300, height: 200 },
			});
		});
		expect(tooltip?.hidden).toBe(false);
		expect(tooltip?.dataset.sourceId).toBe("source-a");
		expect(tooltip?.style.transform).toBe("translate3d(112px, 58px, 0)");
		expect(measure).toHaveBeenCalledTimes(1);

		act(() => {
			handle.current?.show({
				sourceId: "source-a",
				x: 250,
				y: 150,
				bounds: { width: 300, height: 200 },
			});
		});
		expect(tooltip?.style.transform).toBe("translate3d(112px, 58px, 0)");
		expect(measure).toHaveBeenCalledTimes(1);

		act(() => {
			handle.current?.show({
				sourceId: "source-b",
				x: 290,
				y: 5,
				bounds: { width: 300, height: 200 },
			});
		});
		expect(tooltip?.dataset.sourceId).toBe("source-b");
		expect(tooltip?.style.transform).toBe("translate3d(198px, 17px, 0)");
		expect(measure).toHaveBeenCalledTimes(2);

		act(() => handle.current?.hide());
		expect(tooltip?.hidden).toBe(true);
		expect(tooltip?.getAttribute("aria-hidden")).toBe("true");
		expect(tooltip?.dataset.sourceId).toBeUndefined();
	});

	it("clamps oversized tooltips to the Canvas margin", () => {
		expect(
			resolveCanvasCacheWarningTooltipPosition(
				{
					sourceId: "source-a",
					x: 299,
					y: 199,
					bounds: { width: 300, height: 200 },
				},
				{ width: 500, height: 400 },
			),
		).toEqual({ x: 8, y: 8 });
	});

	it("resolves a 14-pixel warning region before underlying Canvas content", () => {
		const regions = [
			{ sourceId: "source-a", rect: { x: 20, y: 10, w: 14, h: 14 } },
			{ sourceId: "source-b", rect: { x: 20, y: 10, w: 14, h: 14 } },
		];
		expect(hitTestCanvasCacheWarning(regions, 34, 24)).toEqual({
			kind: "cache-warning",
			id: "source-b",
		});
		expect(hitTestCanvasCacheWarning(regions, 35, 24)).toBeNull();
	});
});
