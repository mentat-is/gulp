/** Persisted renderer selection understood by the Canvas backend. */
export type CanvasRendererPreference = "auto" | "worker" | "legacy";

/** Renderer currently responsible for the timeline canvas. */
export type CanvasRendererBackend = "initializing" | "worker" | "legacy";

/** Observable renderer selection and fallback state used by diagnostics. */
export interface RendererBackendStatus {
	requestedBackend: CanvasRendererPreference;
	activeBackend: CanvasRendererBackend;
	fallbackReason: string | null;
}

/** Local-storage key used for an explicit renderer override. */
export const RENDERER_PREFERENCE_KEY = "gulp.rendererBackend";

/** Storage subset required to resolve a renderer override. */
export interface RendererPreferenceStorage {
	/**
	 * Reads one persisted renderer preference value.
	 * @param key Storage key to read.
	 * @returns Stored value, or null when no value exists.
	 */
	getItem(key: string): string | null;
}

/**
 * Reads an explicit renderer override and otherwise selects automatic worker-first mode.
 * @param storage Browser storage containing an optional renderer preference.
 * @returns Requested renderer preference, defaulting to automatic worker-first selection.
 */
export function getCanvasRendererPreference(
	storage?: RendererPreferenceStorage | null,
): CanvasRendererPreference {
	try {
		const preferenceStorage =
			storage === undefined && typeof window !== "undefined"
				? window.localStorage
				: storage;
		const preference = preferenceStorage?.getItem(RENDERER_PREFERENCE_KEY);
		if (
			preference === "auto" ||
			preference === "worker" ||
			preference === "legacy"
		) {
			return preference;
		}
	} catch {
		// Storage is optional; automatic capability selection remains available.
	}

	return "auto";
}
