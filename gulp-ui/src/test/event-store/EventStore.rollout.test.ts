import {
	EVENT_STORE_LOD_ROLLBACK_KEY,
	getEventStoreLodDecision,
	isEventStoreLodEnabled,
} from "@/event-store/EventStore.rollout";

describe("EventStore rollout", () => {
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

	it("keeps IndexedDB enabled in an insecure context", () => {
		const secureContextDescriptor = Object.getOwnPropertyDescriptor(
			globalThis,
			"isSecureContext",
		);
		Object.defineProperty(globalThis, "isSecureContext", {
			configurable: true,
			value: false,
		});
		try {
			expect(getEventStoreLodDecision({ getItem: () => null })).toEqual({
				enabled: true,
				reason: null,
			});
		} finally {
			if (secureContextDescriptor) {
				Object.defineProperty(
					globalThis,
					"isSecureContext",
					secureContextDescriptor,
				);
			} else {
				Reflect.deleteProperty(globalThis, "isSecureContext");
			}
		}
	});

	it("supports one explicit browser-local rollback", () => {
		expect(
			getEventStoreLodDecision(
				{
					getItem: (key) =>
						key === EVENT_STORE_LOD_ROLLBACK_KEY ? "false" : null,
				},
			),
		).toEqual({ enabled: false, reason: "manual-disabled" });
	});

	it("gives the build rollback precedence over IndexedDB persistence", () => {
		process.env.REACT_APP_EVENT_STORE_LOD = "false";
		expect(getEventStoreLodDecision({ getItem: () => null })).toEqual({
			enabled: false,
			reason: "build-disabled",
		});
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
