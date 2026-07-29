import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import {
	DEFAULT_EVENT_DETAIL_VIEW,
	EVENT_DETAIL_VIEW_STORAGE_KEY,
	parseEventDetailView,
	useEventDetailViewPreference,
} from "@/dialogs/useEventDetailView";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

function PreferenceHarness({ eventId }: { eventId: string }) {
	const [view, selectView] = useEventDetailViewPreference(document);
	return (
		<div data-event-id={eventId} data-view={view}>
			<button onClick={() => selectView("raw")}>Raw</button>
			<button onClick={() => selectView("table")}>Table</button>
		</div>
	);
}

describe("Event detail view preference", () => {
	let container: HTMLDivElement;
	let root: Root;
	let previousValue: string | null;

	beforeEach(() => {
		previousValue = localStorage.getItem(EVENT_DETAIL_VIEW_STORAGE_KEY);
		localStorage.removeItem(EVENT_DETAIL_VIEW_STORAGE_KEY);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		if (previousValue === null) {
			localStorage.removeItem(EVENT_DETAIL_VIEW_STORAGE_KEY);
		} else {
			localStorage.setItem(EVENT_DETAIL_VIEW_STORAGE_KEY, previousValue);
		}
	});

	it("falls back to Tree for missing or invalid persisted values", () => {
		expect(parseEventDetailView(null)).toBe(DEFAULT_EVENT_DETAIL_VIEW);
		expect(parseEventDetailView("unsupported")).toBe(
			DEFAULT_EVENT_DETAIL_VIEW,
		);
		localStorage.setItem(EVENT_DETAIL_VIEW_STORAGE_KEY, "unsupported");

		act(() => root.render(<PreferenceHarness eventId="event-one" />));
		expect(container.firstElementChild?.getAttribute("data-view")).toBe(
			"tree",
		);
	});

	it("keeps the selected view through navigation and a new dialog mount", () => {
		act(() => root.render(<PreferenceHarness eventId="event-one" />));
		act(() => {
			[...container.querySelectorAll("button")]
				.find((button) => button.textContent === "Raw")
				?.click();
		});
		expect(localStorage.getItem(EVENT_DETAIL_VIEW_STORAGE_KEY)).toBe("raw");
		expect(container.firstElementChild?.getAttribute("data-view")).toBe("raw");

		act(() => root.render(<PreferenceHarness eventId="event-two" />));
		expect(container.firstElementChild?.getAttribute("data-event-id")).toBe(
			"event-two",
		);
		expect(container.firstElementChild?.getAttribute("data-view")).toBe("raw");

		act(() => root.unmount());
		root = createRoot(container);
		act(() => root.render(<PreferenceHarness eventId="event-three" />));
		expect(container.firstElementChild?.getAttribute("data-view")).toBe("raw");
	});
});
