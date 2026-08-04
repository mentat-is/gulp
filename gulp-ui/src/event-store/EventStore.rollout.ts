/** Browser-local rollback switch for the EventStore/LOD pipeline. */
export const EVENT_STORE_LOD_ROLLBACK_KEY = "gulp:event-store-lod-enabled";

/** Minimal storage contract used to keep rollout checks testable. */
export interface EventStoreRolloutStorage {
	getItem(key: string): string | null;
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
	if (process.env.REACT_APP_EVENT_STORE_LOD === "false") return false;
	try {
		const selectedStorage =
			storage ??
			("localStorage" in globalThis ? globalThis.localStorage : undefined);
		return selectedStorage?.getItem(EVENT_STORE_LOD_ROLLBACK_KEY) !== "false";
	} catch {
		return true;
	}
}
