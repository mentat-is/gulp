import {
	EVENT_STORE_LOD_ROLLBACK_KEY,
	isEventStoreLodEnabled,
	type EventStoreRolloutStorage,
} from "@/event-store/EventStore.rollout";

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

/** Storage contract required to persist an automatic LOD rollback. */
export interface RendererLodRollbackStorage extends EventStoreRolloutStorage {
	/**
	 * Persists the rollback value before the browser is reloaded.
	 * @param key Rollout storage key.
	 * @param value Disabled rollout marker.
	 */
	setItem(key: string, value: string): void;
}

/** Minimal reload target used by the guarded LOD-to-legacy transition. */
export interface RendererReloadTarget {
	/** Reloads the current application after the rollback marker is durable. */
	reload(): void;
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

/**
 * Disables EventStore LOD and reloads before a legacy renderer can be mounted.
 * Legacy rendering needs canonical documents in main-thread RAM, so switching in place
 * after QueryWorker took ownership would otherwise produce a silently empty Canvas.
 * @param storage Optional browser storage, injectable for tests.
 * @param reloadTarget Optional browser location, injectable for tests.
 * @returns True after a verified rollback marker was stored and reload requested.
 */
export function reloadWithLegacyEventPipeline(
	storage?: RendererLodRollbackStorage,
	reloadTarget?: RendererReloadTarget,
): boolean {
	const selectedStorage =
		storage ??
		(typeof window !== "undefined" ? window.localStorage : undefined);
	const selectedReloadTarget =
		reloadTarget ??
		(typeof window !== "undefined" ? window.location : undefined);
	if (
		!selectedStorage ||
		!selectedReloadTarget ||
		!isEventStoreLodEnabled(selectedStorage)
	) {
		return false;
	}

	try {
		selectedStorage.setItem(EVENT_STORE_LOD_ROLLBACK_KEY, "false");
		if (isEventStoreLodEnabled(selectedStorage)) return false;
		selectedReloadTarget.reload();
		return true;
	} catch {
		return false;
	}
}
