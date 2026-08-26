import {
	clearEventStoreFallbackRequest,
	getEventStoreFallbackRequest,
	publishEventStoreFallbackRequest,
	subscribeEventStoreFallbackRequest,
} from "@/event-store/EventStoreFallback.request";

describe("EventStore fallback requests", () => {
	beforeEach(() => clearEventStoreFallbackRequest());
	afterEach(() => clearEventStoreFallbackRequest());

	it("publishes one typed request without changing browser rollout state", () => {
		const listener = jest.fn();
		const unsubscribe = subscribeEventStoreFallbackRequest(listener);
		const previousMarker = localStorage.getItem("gulp:event-store-lod-enabled");

		expect(
			publishEventStoreFallbackRequest({
				reason: "storage",
				message: "IndexedDB unavailable",
				estimatedEventCount: 14_000_000.9,
			}),
		).toBe(true);
		expect(getEventStoreFallbackRequest()).toEqual({
			reason: "storage",
			message: "IndexedDB unavailable",
			estimatedEventCount: 14_000_000,
		});
		expect(localStorage.getItem("gulp:event-store-lod-enabled")).toBe(
			previousMarker,
		);
		expect(listener).toHaveBeenCalledTimes(1);
		unsubscribe();
	});

	it("deduplicates simultaneous requests until the active banner is dismissed", () => {
		expect(
			publishEventStoreFallbackRequest({
				reason: "quota",
				message: "quota",
				estimatedEventCount: 1,
			}),
		).toBe(true);
		expect(
			publishEventStoreFallbackRequest({
				reason: "renderer-fatal",
				message: "fatal",
				estimatedEventCount: 2,
			}),
		).toBe(false);
		expect(getEventStoreFallbackRequest()?.reason).toBe("quota");

		clearEventStoreFallbackRequest();
		expect(getEventStoreFallbackRequest()).toBeNull();
		expect(
			publishEventStoreFallbackRequest({
				reason: "renderer-fatal",
				message: "fatal",
				estimatedEventCount: 2,
			}),
		).toBe(true);
	});
});
