/** Browser-local rollback switch for the EventStore/LOD pipeline. */
export const EVENT_STORE_LOD_ROLLBACK_KEY = "gulp:event-store-lod-enabled";

/** Minimal storage contract used to keep rollout checks testable. */
export interface EventStoreRolloutStorage {
	getItem(key: string): string | null;
}

/** Storage contract required to persist an automatic EventStore rollback. */
export interface EventStoreRolloutWritableStorage
	extends EventStoreRolloutStorage {
	setItem(key: string, value: string): void;
}

/** Minimal reload target used by the guarded EventStore-to-legacy transition. */
export interface EventStoreReloadTarget {
	reload(): void;
}

/** Browser capabilities required by the persistent EventStore pipeline. */
export interface EventStoreLodCapabilities {
	isSecureContext?: boolean;
	hasIndexedDb?: boolean;
	hasStorageEstimate?: boolean;
	hasWorker?: boolean;
}

/** Reason why the scalable EventStore pipeline was not selected. */
export type EventStoreLodFallbackReason =
	| "build-disabled"
	| "manual-disabled"
	| "insecure-context"
	| "indexeddb-unavailable"
	| "storage-estimate-unavailable"
	| "worker-unavailable";

/** One startup decision shared by stream ownership and renderer selection. */
export interface EventStoreLodDecision {
	enabled: boolean;
	reason: EventStoreLodFallbackReason | null;
	automatic: boolean;
}

/**
 * Reads browser capabilities without assuming that a non-browser test runtime is insecure.
 * @param overrides Optional capability values, injectable for tests.
 * @returns Explicit capabilities when the runtime exposes secure-context state.
 */
function readEventStoreCapabilities(
	overrides?: EventStoreLodCapabilities,
): EventStoreLodCapabilities {
	const isSecureContext =
		overrides?.isSecureContext ??
		(typeof globalThis.isSecureContext === "boolean"
			? globalThis.isSecureContext
			: undefined);
	if (isSecureContext === undefined) return { ...overrides };

	return {
		isSecureContext,
		hasIndexedDb:
			overrides?.hasIndexedDb ?? typeof globalThis.indexedDB !== "undefined",
		hasStorageEstimate:
			overrides?.hasStorageEstimate ??
			typeof globalThis.navigator?.storage?.estimate === "function",
		hasWorker:
			overrides?.hasWorker ?? typeof globalThis.Worker !== "undefined",
	};
}

/**
 * Selects the persistent EventStore pipeline or its in-memory compatibility path.
 * Build and manual rollback switches retain precedence over runtime capabilities.
 * @param storage Optional storage implementation, injectable for tests.
 * @param capabilities Optional browser capabilities, injectable for tests.
 * @returns Stable rollout decision and fallback reason.
 */
export function getEventStoreLodDecision(
	storage?: EventStoreRolloutStorage,
	capabilities?: EventStoreLodCapabilities,
): EventStoreLodDecision {
	if (process.env.REACT_APP_EVENT_STORE_LOD === "false") {
		return { enabled: false, reason: "build-disabled", automatic: false };
	}

	try {
		const selectedStorage =
			storage ??
			("localStorage" in globalThis ? globalThis.localStorage : undefined);
		if (selectedStorage?.getItem(EVENT_STORE_LOD_ROLLBACK_KEY) === "false") {
			return { enabled: false, reason: "manual-disabled", automatic: false };
		}
	} catch {
		// Storage access is optional; capability detection can still select a safe path.
	}

	const detected = readEventStoreCapabilities(capabilities);
	if (detected.isSecureContext === undefined) {
		return { enabled: true, reason: null, automatic: false };
	}
	if (!detected.isSecureContext) {
		return { enabled: false, reason: "insecure-context", automatic: true };
	}
	if (!detected.hasIndexedDb) {
		return { enabled: false, reason: "indexeddb-unavailable", automatic: true };
	}
	if (!detected.hasStorageEstimate) {
		return {
			enabled: false,
			reason: "storage-estimate-unavailable",
			automatic: true,
		};
	}
	if (!detected.hasWorker) {
		return { enabled: false, reason: "worker-unavailable", automatic: true };
	}
	return { enabled: true, reason: null, automatic: false };
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
 * Persists a manual EventStore rollback and reloads into the legacy RAM pipeline.
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
