/** Browser-local rollback switch for the EventStore/LOD pipeline. */
export const EVENT_STORE_LOD_ROLLBACK_KEY = "gulp:event-store-lod-enabled";

/** Minimal storage contract used to keep rollout checks testable. */
export interface EventStoreRolloutStorage {
	getItem(key: string): string | null;
}

/** Storage contract required to persist an EventStore rollback. */
export interface EventStoreRolloutWritableStorage
	extends EventStoreRolloutStorage {
	setItem(key: string, value: string): void;
}

/** Minimal reload target used by the guarded EventStore-to-legacy transition. */
export interface EventStoreReloadTarget {
	reload(): void;
}

/** Reason why the scalable EventStore pipeline was not selected. */
export type EventStoreLodFallbackReason =
	| "build-disabled"
	| "manual-disabled";

/** One startup decision shared by stream ownership and renderer selection. */
export interface EventStoreLodDecision {
	enabled: boolean;
	reason: EventStoreLodFallbackReason | null;
}

/**
 * Selects IndexedDB persistence unless a build or persisted rollback disables it.
 * Browser capability and quota telemetry are deliberately not used as preflight
 * gates: real IndexedDB failures trigger the controlled RAM fallback instead.
 * @param storage Optional storage implementation, injectable for tests.
 * @returns Stable rollout decision and fallback reason.
 */
export function getEventStoreLodDecision(
	storage?: EventStoreRolloutStorage,
): EventStoreLodDecision {
	if (process.env.REACT_APP_EVENT_STORE_LOD === "false") {
		return { enabled: false, reason: "build-disabled" };
	}

	try {
		const selectedStorage =
			storage ??
			("localStorage" in globalThis ? globalThis.localStorage : undefined);
		if (selectedStorage?.getItem(EVENT_STORE_LOD_ROLLBACK_KEY) === "false") {
			return { enabled: false, reason: "manual-disabled" };
		}
	} catch {
		// A blocked rollback store does not prevent the initial IndexedDB attempt.
	}
	return { enabled: true, reason: null };
}

/**
 * Returns whether QueryWorker persistence and viewport LOD are enabled.
 * The build-time switch can disable the feature globally, while a browser-local
 * `false` value provides a temporary, reload-scoped operational rollback.
 * @param storage Optional storage implementation, injectable for tests.
 * @returns True by default; false only after an explicit rollback setting.
 */
export function isEventStoreLodEnabled(
	storage?: EventStoreRolloutStorage,
): boolean {
	return getEventStoreLodDecision(storage).enabled;
}

/**
 * Persists an EventStore rollback and reloads into the legacy RAM pipeline.
 * @param storage Optional browser storage, injectable for tests.
 * @param reloadTarget Optional browser location, injectable for tests.
 * @returns True after the disabled marker was verified and reload requested.
 */
export function reloadWithLegacyEventPipeline(
	storage?: EventStoreRolloutWritableStorage,
	reloadTarget?: EventStoreReloadTarget,
): boolean {
	const selectedStorage =
		storage ??
		(typeof window !== "undefined" ? window.localStorage : undefined);
	const selectedReloadTarget =
		reloadTarget ??
		(typeof window !== "undefined" ? window.location : undefined);
	if (!selectedStorage || !selectedReloadTarget) return false;

	try {
		if (selectedStorage.getItem(EVENT_STORE_LOD_ROLLBACK_KEY) === "false") {
			return false;
		}
		selectedStorage.setItem(EVENT_STORE_LOD_ROLLBACK_KEY, "false");
		if (selectedStorage.getItem(EVENT_STORE_LOD_ROLLBACK_KEY) !== "false") {
			return false;
		}
		selectedReloadTarget.reload();
		return true;
	} catch {
		return false;
	}
}
