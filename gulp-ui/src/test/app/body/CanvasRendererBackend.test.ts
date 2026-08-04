import {
	getCanvasRendererPreference,
	reloadWithLegacyEventPipeline,
	RENDERER_PREFERENCE_KEY,
	type RendererPreferenceStorage,
} from "@/app/body/CanvasRendererBackend";
import { EVENT_STORE_LOD_ROLLBACK_KEY } from "@/event-store/EventStore.rollout";

/**
 * Creates a storage fixture containing one renderer preference.
 * @param value Stored renderer preference value.
 * @returns Minimal renderer-preference storage fixture.
 */
function createStorage(value: string | null): RendererPreferenceStorage {
	return {
		getItem: (key) => (key === RENDERER_PREFERENCE_KEY ? value : null),
	};
}

describe("Canvas renderer backend preference", () => {
	it("defaults normal sessions to automatic worker-first selection", () => {
		expect(getCanvasRendererPreference(createStorage(null))).toBe("auto");
		expect(getCanvasRendererPreference(createStorage("unknown"))).toBe("auto");
	});

	(["auto", "worker", "legacy"] as const).forEach((preference) => {
		it(`honors the explicit ${preference} preference`, () => {
			expect(getCanvasRendererPreference(createStorage(preference))).toBe(
				preference,
			);
		});
	});

	it("falls back to automatic selection when storage access fails", () => {
		const storage: RendererPreferenceStorage = {
			getItem: () => {
				throw new Error("storage denied");
			},
		};

		expect(getCanvasRendererPreference(storage)).toBe("auto");
	});
});

describe("Canvas EventStore LOD fallback", () => {
	it("persists rollback before reloading into the legacy event pipeline", () => {
		const values = new Map<string, string>();
		const storage = {
			getItem: (key: string) => values.get(key) ?? null,
			setItem: (key: string, value: string) => values.set(key, value),
		};
		const reload = jest.fn();

		expect(reloadWithLegacyEventPipeline(storage, { reload })).toBe(true);
		expect(values.get(EVENT_STORE_LOD_ROLLBACK_KEY)).toBe("false");
		expect(reload).toHaveBeenCalledTimes(1);
	});

	it("does not reload when rollback persistence fails", () => {
		const reload = jest.fn();
		const storage = {
			getItem: () => null,
			setItem: () => {
				throw new Error("storage unavailable");
			},
		};

		expect(reloadWithLegacyEventPipeline(storage, { reload })).toBe(false);
		expect(reload).not.toHaveBeenCalled();
	});
});
