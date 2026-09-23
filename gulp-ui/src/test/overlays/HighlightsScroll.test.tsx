import { act } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import type { Highlight } from "@/entities/Highlight";
import { Highlights } from "@/overlays/Highlights";
import { scrollStore } from "@/store/scroll.store";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

let mockApplicationUseCount = 0;
let mockTimelineScale = 1;
let mockHighlights: Highlight.Type[] = [];
const mockTimelineScaleListeners = new Set<() => void>();
let mockOverlayVersion = 0;
const mockOverlayListeners = new Set<() => void>();
const mockInfo = {
	/** Returns the scale-derived timeline width used by highlight geometry. */
	get width(): number {
		return 100 * mockTimelineScale;
	},
	/** Returns the current scale-store snapshot. */
	getTimelineScaleSnapshot: (): number => mockTimelineScale,
	/** Subscribes a geometry consumer to scale-store changes. */
	subscribeTimelineScale: (listener: () => void): (() => void) => {
		mockTimelineScaleListeners.add(listener);
		return () => mockTimelineScaleListeners.delete(listener);
	},
};

/**
 * Commits a deterministic scale and notifies subscribed highlight geometry.
 * @param scale Next test scale.
 * @returns Nothing.
 */
function mockCommitTimelineScale(scale: number): void {
	mockTimelineScale = scale;
	mockTimelineScaleListeners.forEach((listener) => listener());
}

/** Publishes one deterministic canonical overlay revision. */
function mockPublishOverlayRevision(): void {
	mockOverlayVersion++;
	mockOverlayListeners.forEach((listener) => listener());
}

jest.mock("@/store/DataStore", () => ({
	DataStore: {
		/** Subscribes a deterministic overlay consumer. */
		subscribeOverlays: (listener: () => void): (() => void) => {
			mockOverlayListeners.add(listener);
			return () => mockOverlayListeners.delete(listener);
		},
		/** Returns the current deterministic overlay revision. */
		getOverlaysSnapshot: (): number => mockOverlayVersion,
	},
}));

jest.mock("@/context/Application.context", () => ({
	Application: {
		/** Returns the minimum application context required by an empty overlay. */
		use: (): unknown => {
			mockApplicationUseCount += 1;
			return {
				app: {
					timeline: {
						frame: { min: 0, max: 100 },
						renderVersion: 0,
						scale: mockTimelineScale,
					},
					target: { operations: [] },
				},
				Info: mockInfo,
			};
		},
	},
}));

jest.mock("@/locales/core", () => ({
	/** Returns a deterministic translation value for component modules. */
	translate: (key: string): string => key,
}));

jest.mock("@/locales", () => ({
	Locale: {
		/** Returns the translation surface used by highlight components. */
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

jest.mock("@/ui/utils", () => ({
	/** Joins truthy class names for the lightweight component tree. */
	cn: (...classNames: unknown[]): string => classNames.filter(Boolean).join(" "),
	Algorhithm: class MockAlgorhithm {},
}));

jest.mock("@/entities/Highlight", () => ({
	Highlight: {
		Entity: {
			/** Returns the current deterministic highlight fixtures. */
			selected: (): Highlight.Type[] => mockHighlights,
			/** Returns one flat depth per deterministic highlight fixture. */
			computeDepths: (highlights: Highlight.Type[]): number[] =>
				highlights.map(() => 0),
		},
	},
}));

jest.mock("@/entities/Color", () => ({
	Color: {
		GEIST: {},
		GRADIENT: {},
		Themer: {
			/** Returns a deterministic readable text color for fixture highlights. */
			getReadablePaletteTextColor: (): string => "white",
		},
	},
}));

describe("highlight overlay scrolling", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockApplicationUseCount = 0;
		mockTimelineScale = 1;
		mockHighlights = [];
		mockTimelineScaleListeners.clear();
		mockOverlayVersion = 0;
		mockOverlayListeners.clear();
		scrollStore.setScrollX(0);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	/**
	 * Returns the transform layer nested inside the overlay root.
	 * @returns Rendered highlight scroll layer.
	 */
	function getScrollContent(): HTMLElement {
		const scrollContent = container.firstElementChild?.firstElementChild;
		if (!(scrollContent instanceof HTMLElement)) {
			throw new Error("Highlight scroll content was not rendered");
		}
		return scrollContent;
	}

	/** Returns the first rendered highlight element. */
	function getHighlightElement(): HTMLElement {
		const highlight = container.querySelector<HTMLElement>("[data-highlight]");
		if (!highlight) throw new Error("Highlight was not rendered");
		return highlight;
	}

	it("updates the live transform and registry without rerendering React", () => {
		act(() => root.render(<Highlights.List.Overlay />));
		const renderCount = mockApplicationUseCount;
		const scrollContent = getScrollContent();
		const highlightId = "highlight-1" as Highlight.Id;
		Highlights.set(highlightId, 100, 20, 0, "orange");

		act(() => scrollStore.setScrollX(48));

		expect(
			scrollContent.style.getPropertyValue("--highlight-scroll-offset"),
		).toBe("-48px");
		expect(Highlights.list()).toEqual([[52, 20, 0, "orange"]]);
		expect(mockApplicationUseCount).toBe(renderCount);
	});

	it("keeps standalone previews independent from timeline scrolling", () => {
		act(() => root.render(<Highlights.List.Overlay layoutWidth={240} />));
		const renderCount = mockApplicationUseCount;
		const scrollContent = getScrollContent();

		act(() => scrollStore.setScrollX(48));

		expect(
			scrollContent.style.getPropertyValue("--highlight-scroll-offset"),
		).toBe("0px");
		expect(mockApplicationUseCount).toBe(renderCount);
	});

	it("recomputes timeline highlight geometry after a renderless scale commit", () => {
		mockHighlights = [
			{
				id: "highlight-scale" as Highlight.Id,
				time_range: [25, 75],
				color: "orange",
			} as Highlight.Type,
		];
		act(() => root.render(<Highlights.List.Overlay />));

		expect(Highlights.list()).toEqual([[25, 50, 0, "orange"]]);
		expect(getHighlightElement().style.getPropertyValue("--highlight-left")).toBe("25%");
		expect(getHighlightElement().style.getPropertyValue("--highlight-width")).toBe("50%");
		act(() => mockCommitTimelineScale(2));
		expect(Highlights.list()).toEqual([[50, 100, 0, "orange"]]);
		expect(getHighlightElement().style.getPropertyValue("--highlight-left")).toBe("25%");
		expect(getHighlightElement().style.getPropertyValue("--highlight-width")).toBe("50%");
	});

	it("renders a newly published highlight without waiting for unrelated app state", () => {
		act(() => root.render(<Highlights.List.Overlay />));
		expect(container.querySelector("[data-highlight]")).toBeNull();

		mockHighlights = [
			{
				id: "highlight-created" as Highlight.Id,
				time_range: [10, 20],
				name: "Created highlight",
				color: "orange",
			} as Highlight.Type,
		];
		act(() => mockPublishOverlayRevision());

		expect(Boolean(getHighlightElement())).toBe(true);
		expect(container.textContent).toContain("Created highlight");
	});

	it("keeps a native creation selection in pixel coordinates", () => {
		const nativeHighlight = {
			id: "highlight-native" as Highlight.Id,
			time_range: [0, 0],
			range: [80, 20],
			name: "Native highlight",
			color: "orange",
		} as unknown as Highlight.Type;

		act(() => root.render(
			<Highlights.Component highlight={nativeHighlight} native index={2} />,
		));

		expect(getHighlightElement().style.getPropertyValue("--highlight-left")).toBe("20px");
		expect(getHighlightElement().style.getPropertyValue("--highlight-width")).toBe("60px");
		expect(getHighlightElement().style.getPropertyValue("--highlight-ruler-height")).toBe("25px");
		expect(getHighlightElement().style.getPropertyValue("--highlight-label-top")).toBe("64px");
	});
});
