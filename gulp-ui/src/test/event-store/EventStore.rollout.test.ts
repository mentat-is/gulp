import {
	EVENT_STORE_LOD_ROLLBACK_KEY,
	getEventStoreLodDecision,
	isEventStoreLodEnabled,
	reloadScalableEventPipeline,
	reloadWithLegacyEventPipeline,
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

	it("retries scalable mode without touching the rollout marker", () => {
		const reload = jest.fn();
		const setItem = jest.fn();
		const storage = { getItem: () => null, setItem };

		expect(reloadScalableEventPipeline({ reload })).toBe(true);
		expect(setItem).not.toHaveBeenCalled();
		expect(storage.getItem()).toBeNull();
		expect(reload).toHaveBeenCalledTimes(1);
	});

	it("persists and verifies compatibility mode before its explicit reload", () => {
		const values = new Map<string, string>();
		const reload = jest.fn();
		const storage = {
			getItem: (key: string) => values.get(key) ?? null,
			setItem: (key: string, value: string) => values.set(key, value),
		};

		expect(reloadWithLegacyEventPipeline(storage, { reload })).toBe(true);
		expect(values.get(EVENT_STORE_LOD_ROLLBACK_KEY)).toBe("false");
		expect(reload).toHaveBeenCalledTimes(1);
	});

	it("does not reload when compatibility mode cannot be persisted", () => {
		const reload = jest.fn();
		const storage = {
			getItem: () => null,
			setItem: () => {
				throw new Error("blocked");
			},
		};

		expect(reloadWithLegacyEventPipeline(storage, { reload })).toBe(false);
		expect(reload).not.toHaveBeenCalled();
	});
});
