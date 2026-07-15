import {
	getCanvasRendererPreference,
	RENDERER_PREFERENCE_KEY,
	type RendererPreferenceStorage,
} from "@/app/body/CanvasRendererBackend";

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
