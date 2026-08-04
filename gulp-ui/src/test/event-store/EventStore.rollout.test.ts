import {
	EVENT_STORE_LOD_ROLLBACK_KEY,
	isEventStoreLodEnabled,
} from "@/event-store/EventStore.rollout";

describe("EventStore rollout", () => {
	it("enables the new pipeline by default", () => {
		expect(isEventStoreLodEnabled({ getItem: () => null })).toBe(true);
	});

	it("supports one explicit browser-local rollback", () => {
		expect(
			isEventStoreLodEnabled({
				getItem: (key) =>
					key === EVENT_STORE_LOD_ROLLBACK_KEY ? "false" : null,
			}),
		).toBe(false);
	});

	it("fails open when storage access is unavailable", () => {
		expect(
			isEventStoreLodEnabled({
				getItem: () => {
					throw new DOMException("blocked");
				},
			}),
		).toBe(true);
	});
});
