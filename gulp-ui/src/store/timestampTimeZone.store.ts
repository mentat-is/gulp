import { Internal } from "@/entities/addon/Internal";
import { DEFAULT_TIMESTAMP_TIME_ZONE } from "@/timeZone";
import { useSyncExternalStore } from "react";

const timeZoneListeners = new Set<() => void>();

/**
 * Subscribes to timestamp time-zone changes in this window and sibling windows.
 * @param listener Callback invoked after the persisted preference changes.
 * @returns Cleanup callback that releases local and storage-event subscriptions.
 */
function subscribeToTimestampTimeZone(listener: () => void): () => void {
	timeZoneListeners.add(listener);

	/**
	 * Notifies this subscriber when another window updates the preference.
	 * @param event Browser storage event carrying the changed preference key.
	 * @returns Nothing.
	 */
	const handleStorage = (event: StorageEvent): void => {
		if (event.key === Internal.LocalStorageItemsList.TIMESTAMP_TIME_ZONE) {
			listener();
		}
	};
	globalThis.window?.addEventListener("storage", handleStorage);
	return () => {
		timeZoneListeners.delete(listener);
		globalThis.window?.removeEventListener("storage", handleStorage);
	};
}

/**
 * Persists a validated timestamp time zone and notifies same-window consumers.
 * @param timeZone IANA time-zone identifier selected by the user.
 * @returns Nothing.
 */
export function setTimestampTimeZone(timeZone: string): void {
	Internal.Settings.timestampTimeZone = timeZone;
	timeZoneListeners.forEach((listener) => listener());
}

/**
 * Subscribes a React component to the persisted timestamp time-zone preference.
 * @returns Current validated IANA time-zone identifier.
 */
export function useTimestampTimeZone(): string {
	return useSyncExternalStore(
		subscribeToTimestampTimeZone,
		() => Internal.Settings.timestampTimeZone,
		() => DEFAULT_TIMESTAMP_TIME_ZONE,
	);
}
