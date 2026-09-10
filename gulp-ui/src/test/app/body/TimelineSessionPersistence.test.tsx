import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { Timeline } from "@/app/body/Timeline";
import type { Operation } from "@/entities/Operation";

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/ui/utils", () => ({
	cn: (...classNames: unknown[]): string => classNames.filter(Boolean).join(" "),
	Algorhithm: class MockAlgorithm {},
}));

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "operation-a" as Operation.Id;
let mockCanvasRenderCount = 0;
let mockNavigatorRenderCount = 0;
let snapshotMarker = 1;
const capturedSnapshotMarkers: number[] = [];
const scaleListeners = new Set<() => void>();
const timelineRef = { current: null };
const mockApp = {
	timeline: {
		scale: 1,
		frame: { min: 0, max: 100 },
		filter: "",
		target: null,
	},
	target: {
		files: [{ id: "source-a", selected: true }],
		contexts: [{ id: "context-a", selected: true }],
		operations: [{ id: OPERATION_ID, selected: true }],
	},
	hidden: {
		notes: false,
		links: false,
		sourcesWithNoEvents: true,
		toasts: false,
	},
};

/**
 * Returns the semantic snapshot currently exposed by the mock application.
 */
function currentSnapshot() {
	return {
		selected: {
			files: ["source-a"],
			contexts: ["context-a"],
		},
		timeline: {
			scale: snapshotMarker,
			frame: { min: snapshotMarker, max: snapshotMarker + 100 },
			scroll: { x: 0, y: 0 },
			filter: `filter-${snapshotMarker}`,
			target: null,
		},
		filters: {},
		hidden: mockApp.hidden,
		source_settings: { source_settings: {} },
	};
}

const mockInfo = {
	width: 100,
	session_autosave: jest.fn<
		Promise<boolean>,
		[ReturnType<typeof currentSnapshot>, Operation.Id | undefined]
	>(async () => true),
	session_snapshot: jest.fn(() => {
		capturedSnapshotMarkers.push(snapshotMarker);
		return currentSnapshot();
	}),
	getTimelineScaleSnapshot: (): number => mockApp.timeline.scale,
	subscribeTimelineScale: (listener: () => void): (() => void) => {
		scaleListeners.add(listener);
		return () => scaleListeners.delete(listener);
	},
};

/**
 * Publishes one semantic Timeline change through the external scale store.
 * @param marker Snapshot marker and scale value.
 * @returns Promise settled after React processes the store notification.
 */
async function publishTimelineChange(marker: number): Promise<void> {
	snapshotMarker = marker;
	mockApp.timeline.frame = { min: marker, max: marker + 100 };
	mockApp.timeline.scale = marker;
	await act(async () => {
		scaleListeners.forEach((listener) => listener());
	});
}

/**
 * Flushes promise continuations produced by async autosave completion.
 * @returns Promise settled after queued continuations.
 */
async function flushAutosavePromises(): Promise<void> {
	await act(async () => {
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
	});
}

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: (): unknown => ({
			app: mockApp,
			Info: mockInfo,
			timeline: timelineRef,
		}),
	},
}));

jest.mock("@/app/body/Canvas", () => ({
	Canvas: (): null => {
		mockCanvasRenderCount++;
		return null;
	},
}));

jest.mock("@/app/body/Navigator", () => ({
	Navigator: (): null => {
		mockNavigatorRenderCount++;
		return null;
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			selected: (): unknown[] => [{ id: "source-a" }],
			rendered: (): unknown[] => [],
		},
	},
}));

jest.mock("@/entities/Context", () => ({
	Context: { Entity: { selected: (): unknown[] => [{ id: "context-a" }] } },
}));

jest.mock("@/entities/Doc", () => ({
	Doc: { Entity: { get: (): unknown[] => [] } },
}));

describe("Timeline workspace-session persistence", () => {
	let container: HTMLDivElement;
	let root: Root;
	let originalRequestIdleCallback: typeof window.requestIdleCallback;

	beforeEach(() => {
		jest.useFakeTimers();
		originalRequestIdleCallback = window.requestIdleCallback;
		Object.defineProperty(window, "requestIdleCallback", {
			configurable: true,
			value: (callback: IdleRequestCallback): number => {
				callback({
					didTimeout: false,
					timeRemaining: () => 10,
				});
				return 1;
			},
		});
		mockCanvasRenderCount = 0;
		mockNavigatorRenderCount = 0;
		mockInfo.session_autosave = jest.fn<
			Promise<boolean>,
			[ReturnType<typeof currentSnapshot>, Operation.Id | undefined]
		>(async () => true);
		mockInfo.session_snapshot = jest.fn(() => {
			capturedSnapshotMarkers.push(snapshotMarker);
			return currentSnapshot();
		});
		snapshotMarker = 1;
		capturedSnapshotMarkers.length = 0;
		mockApp.timeline.scale = 1;
		mockApp.timeline.frame = { min: 0, max: 100 };
		mockApp.timeline.filter = "";
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		jest.advanceTimersByTime(0);
		jest.useRealTimers();
		Object.defineProperty(window, "requestIdleCallback", {
			configurable: true,
			value: originalRequestIdleCallback,
		});
		container.remove();
	});

	it("skips the initial baseline and persists one settled change without rendering Canvas", async () => {
		await act(async () => {
			root.render(<Timeline />);
		});
		const initialCanvasRenders = mockCanvasRenderCount;
		const initialNavigatorRenders = mockNavigatorRenderCount;

		await act(async () => {
			jest.advanceTimersByTime(10_000);
			jest.advanceTimersByTime(0);
		});
		expect(mockInfo.session_autosave).not.toHaveBeenCalled();

		await publishTimelineChange(2);
		await act(async () => {
			jest.advanceTimersByTime(10_000);
			jest.advanceTimersByTime(1);
		});
		await flushAutosavePromises();

		expect(mockInfo.session_snapshot.mock.calls).toEqual([
			[],
			[OPERATION_ID],
			[OPERATION_ID],
		]);
		expect(capturedSnapshotMarkers).toEqual([1, 1, 2]);
		expect(mockInfo.session_autosave).toHaveBeenCalledTimes(1);
		const [persistedSnapshot, persistedOperationId] =
			mockInfo.session_autosave.mock.calls[0];
		expect(persistedSnapshot.timeline.scale).toBe(2);
		expect(persistedOperationId).toBe(OPERATION_ID);
		expect(mockCanvasRenderCount).toBe(initialCanvasRenders);
		expect(mockNavigatorRenderCount).toBe(initialNavigatorRenders);
	});

	it("forces one save by the 60-second maximum wait during continuous activity", async () => {
		await act(async () => {
			root.render(<Timeline />);
		});

		for (let marker = 2; marker <= 7; marker++) {
			await act(async () => {
				jest.advanceTimersByTime(9_000);
			});
			await publishTimelineChange(marker);
		}
		expect(mockInfo.session_autosave).not.toHaveBeenCalled();

		await act(async () => {
			jest.advanceTimersByTime(6_000);
		});
		await flushAutosavePromises();

		expect(mockInfo.session_autosave).toHaveBeenCalledTimes(1);
		expect(mockInfo.session_autosave.mock.calls[0][0].timeline.scale).toBe(7);
	});

	it("keeps maximum-wait serialization outside active zoom input", async () => {
		await act(async () => {
			root.render(<Timeline />);
		});

		for (let marker = 2; marker <= 7; marker++) {
			await act(async () => {
				jest.advanceTimersByTime(9_000);
			});
			await publishTimelineChange(marker);
		}
		await act(async () => {
			jest.advanceTimersByTime(5_500);
		});
		await publishTimelineChange(8);
		await act(async () => {
			jest.advanceTimersByTime(500);
		});

		expect(mockInfo.session_autosave).not.toHaveBeenCalled();

		await act(async () => {
			jest.advanceTimersByTime(500);
		});
		await flushAutosavePromises();

		expect(mockInfo.session_autosave).toHaveBeenCalledTimes(1);
		expect(mockInfo.session_autosave.mock.calls[0][0].timeline.scale).toBe(8);
	});

	it("coalesces changes made during a write without overlapping requests", async () => {
		let resolveFirst: ((success: boolean) => void) | undefined;
		let firstSettled = false;
		let secondStartedBeforeFirstSettled = false;
		let requestCount = 0;
		mockInfo.session_autosave = jest.fn<
			Promise<boolean>,
			[ReturnType<typeof currentSnapshot>, Operation.Id | undefined]
		>(() => {
			requestCount++;
			if (requestCount === 1) {
				return new Promise<boolean>((resolve) => {
					resolveFirst = (success) => {
						firstSettled = true;
						resolve(success);
					};
				});
			}
			return Promise.resolve().then(() => {
				secondStartedBeforeFirstSettled = !firstSettled;
				return true;
			});
		});
		await act(async () => {
			root.render(<Timeline />);
		});

		await publishTimelineChange(2);
		await act(async () => {
			jest.advanceTimersByTime(10_000);
		});
		expect(mockInfo.session_autosave).toHaveBeenCalledTimes(1);

		await publishTimelineChange(3);
		await act(async () => {
			jest.advanceTimersByTime(10_000);
		});
		expect(mockInfo.session_autosave).toHaveBeenCalledTimes(1);

		resolveFirst?.(true);
		await flushAutosavePromises();
		await act(async () => {
			jest.advanceTimersByTime(10_000);
		});
		await flushAutosavePromises();

		expect(mockInfo.session_autosave).toHaveBeenCalledTimes(2);
		expect(mockInfo.session_autosave.mock.calls[1][0].timeline.scale).toBe(3);
		expect(secondStartedBeforeFirstSettled).toBe(false);
	});
});
