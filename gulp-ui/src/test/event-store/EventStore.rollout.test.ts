import {
	EVENT_STORE_LOD_ROLLBACK_KEY,
	getEventStoreLodDecision,
	isEventStoreLodEnabled,
} from "@/event-store/EventStore.rollout";

describe("EventStore rollout", () => {
	const completeCapabilities = {
		isSecureContext: true,
		hasIndexedDb: true,
		hasStorageEstimate: true,
		hasWorker: true,
	};
	const originalBuildFlag = process.env.REACT_APP_EVENT_STORE_LOD;

	beforeEach(() => {
		delete process.env.REACT_APP_EVENT_STORE_LOD;
	});

	afterEach(() => {
		if (originalBuildFlag === undefined) {
			delete process.env.REACT_APP_EVENT_STORE_LOD;
		} else {
			process.env.REACT_APP_EVENT_STORE_LOD = originalBuildFlag;
		}
	});

	it("enables the new pipeline by default", () => {
		expect(isEventStoreLodEnabled({ getItem: () => null })).toBe(true);
	});

	it("keeps the persistent pipeline in a secure, fully capable browser", () => {
		expect(
			getEventStoreLodDecision(
				{ getItem: () => null },
				completeCapabilities,
			),
		).toEqual({ enabled: true, reason: null, automatic: false });
	});

	it("selects compatibility mode for an insecure context", () => {
		expect(
			getEventStoreLodDecision(
				{ getItem: () => null },
				{ ...completeCapabilities, isSecureContext: false },
			),
		).toEqual({
			enabled: false,
			reason: "insecure-context",
			automatic: true,
		});
	});

	for (const [capability, reason] of [
		["hasIndexedDb", "indexeddb-unavailable"],
		["hasStorageEstimate", "storage-estimate-unavailable"],
		["hasWorker", "worker-unavailable"],
	] as const) {
		it(`selects compatibility mode when ${capability} is missing`, () => {
			expect(
				getEventStoreLodDecision(
					{ getItem: () => null },
					{ ...completeCapabilities, [capability]: false },
				),
			).toEqual({ enabled: false, reason, automatic: true });
		});
	}

	it("supports one explicit browser-local rollback", () => {
		expect(
			getEventStoreLodDecision(
				{
				getItem: (key) =>
					key === EVENT_STORE_LOD_ROLLBACK_KEY ? "false" : null,
				},
				{ ...completeCapabilities, isSecureContext: false },
			),
		).toEqual({ enabled: false, reason: "manual-disabled", automatic: false });
	});

	it("gives the build rollback precedence over browser capabilities", () => {
		process.env.REACT_APP_EVENT_STORE_LOD = "false";
		expect(
			getEventStoreLodDecision(
				{ getItem: () => null },
				completeCapabilities,
			),
		).toEqual({ enabled: false, reason: "build-disabled", automatic: false });
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
