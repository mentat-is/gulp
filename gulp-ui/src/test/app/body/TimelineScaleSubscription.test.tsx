import { act } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import { Timeline } from "@/app/body/Timeline";

jest.mock("@/locales/core", () => ({
	/** Returns the requested key for the isolated Timeline tree. */
	translate: (key: string): string => key,
}));

jest.mock("@/ui/utils", () => ({
	/** Joins truthy class names for the lightweight Timeline tree. */
	cn: (...classNames: unknown[]): string => classNames.filter(Boolean).join(" "),
	Algorhithm: class MockAlgorhithm {},
}));

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

let mockScale = 1;
let mockCanvasRenderCount = 0;
let mockNavigatorRenderCount = 0;
let mockAutosaveScheduleCount = 0;
const mockScaleListeners = new Set<() => void>();
const mockTimelineRef = { current: null };
const mockApp = {
	timeline: {
		scale: 1,
		frame: { min: 0, max: 100 },
		filter: "",
		target: null,
	},
	target: {
		files: [],
		contexts: [],
		operations: [],
		filters: {},
	},
	hidden: {},
};
const mockInfo = {
	width: 100,
	session_autosave: jest.fn(),
	session_snapshot: jest.fn(() => null),
	/** Returns the current test scale snapshot. */
	getTimelineScaleSnapshot: (): number => mockScale,
	/** Subscribes the Timeline to deterministic scale changes. */
	subscribeTimelineScale: (listener: () => void): (() => void) => {
		mockScaleListeners.add(listener);
		return () => mockScaleListeners.delete(listener);
	},
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		/** Returns the stable Timeline test context. */
		use: (): unknown => ({
			app: mockApp,
			Info: mockInfo,
			timeline: mockTimelineRef,
		}),
	},
}));

jest.mock("@/app/body/Canvas", () => ({
	/** Records Canvas boundary renders. */
	Canvas: (): null => {
		mockCanvasRenderCount++;
		return null;
	},
}));

jest.mock("@/app/body/Navigator", () => ({
	/** Records Navigator boundary renders. */
	Navigator: (): null => {
		mockNavigatorRenderCount++;
		return null;
	},
}));

jest.mock("@/app/body/TimelineAutosave", () => ({
	/** Records each autosave scheduling dependency change. */
	scheduleTimelineAutosave: (): void => {
		mockAutosaveScheduleCount++;
	},
	/** No-op cleanup for the isolated scheduling test. */
	cleanupTimelineAutosave: (): void => undefined,
}));

jest.mock("@/entities/Source", () => ({
	Source: { Entity: { selected: (): unknown[] => [] } },
}));

jest.mock("@/entities/Context", () => ({
	Context: { Entity: { selected: (): unknown[] => [] } },
}));

jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: { get: (): unknown[] => [] } },
}));

/**
 * Publishes one deterministic renderless scale-store change.
 * @param scale Next canonical scale.
 * @returns Nothing.
 */
function mockCommitScale(scale: number): void {
	mockScale = scale;
	mockScaleListeners.forEach((listener) => listener());
}

describe("Timeline scale subscription", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockScale = 1;
		mockCanvasRenderCount = 0;
		mockNavigatorRenderCount = 0;
		mockAutosaveScheduleCount = 0;
		mockScaleListeners.clear();
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("reschedules autosave without rerendering Canvas or Navigator", () => {
		act(() => root.render(<Timeline />));

		expect(mockAutosaveScheduleCount).toBe(1);
		expect(mockCanvasRenderCount).toBe(1);
		expect(mockNavigatorRenderCount).toBe(1);

		act(() => mockCommitScale(2));

		expect(mockAutosaveScheduleCount).toBe(2);
		expect(mockCanvasRenderCount).toBe(1);
		expect(mockNavigatorRenderCount).toBe(1);
	});
});
