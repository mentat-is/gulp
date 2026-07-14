export type PersistedDetachedWindowKind = "notes" | "table" | "dashboard";

export interface PersistedDetachedWindowRecord {
	key: string;
	kind: PersistedDetachedWindowKind;
	windowName: string;
	features: string;
}

const DETACHED_WINDOWS_STORAGE_KEY = "gulp-detached-windows";

const DETACHED_WINDOW_RECORDS: Record<
	PersistedDetachedWindowKind,
	PersistedDetachedWindowRecord
> = {
	notes: {
		key: "notes",
		kind: "notes",
		windowName: "GulpNotes",
		features: "width=1480,height=760,left=100,top=100",
	},
	table: {
		key: "table",
		kind: "table",
		windowName: "GulpTableView",
		features: "width=800,height=600,left=150,top=150",
	},
	dashboard: {
		key: "dashboard",
		kind: "dashboard",
		windowName: "GulpDashboardView",
		features: "width=1480,height=820,left=180,top=120",
	},
};

/**
 * Applies the active theme attributes and CSS custom properties from one document to another.
 *
 * @param sourceDoc - Document that provides the currently computed theme variables.
 * @param targetDoc - Document that receives the theme attributes and variables.
 * @param nextTheme - Theme name to write to the target document root.
 * @returns True when the target document was updated, or false when either document is not ready.
 */
export function applyThemeToWindow(
	sourceDoc: Document,
	targetDoc: Document,
	nextTheme: string | undefined,
): boolean {
	const sourceRoot = sourceDoc.documentElement;
	const targetRoot = targetDoc.documentElement;

	if (!sourceRoot || !targetRoot) {
		return false;
	}

	targetRoot.setAttribute("data-theme", nextTheme ?? "dark");

	const styles = getComputedStyle(sourceRoot);
	for (let index = 0; index < styles.length; index++) {
		const key = styles[index];
		if (key.startsWith("--")) {
			targetRoot.style.setProperty(key, styles.getPropertyValue(key));
		}
	}

	return true;
}

/**
 * Gets the persisted metadata used to reopen or reconnect a detached routed window.
 *
 * @param kind - Detached route kind.
 * @returns Detached window persistence record for the requested kind.
 */
export function getPersistedDetachedWindowRecord(
	kind: PersistedDetachedWindowKind,
): PersistedDetachedWindowRecord {
	return { ...DETACHED_WINDOW_RECORDS[kind] };
}

/**
 * Reads the detached windows that should be reattached after main-tab reloads or login redirects.
 *
 * @returns Persisted detached window records.
 */
export function readPersistedDetachedWindows(): PersistedDetachedWindowRecord[] {
	try {
		const raw = localStorage.getItem(DETACHED_WINDOWS_STORAGE_KEY);
		if (!raw) return [];

		const records = JSON.parse(raw) as PersistedDetachedWindowRecord[];
		if (!Array.isArray(records)) return [];

		return records.filter((record) =>
			["notes", "table", "dashboard"].includes(record.kind) &&
			!!record.key &&
			!!record.windowName &&
			!!record.features,
		);
	} catch (error) {
		console.warn("Failed to read persisted detached windows", {
			error,
		});
		return [];
	}
}

/**
 * Writes detached window records used for post-login reattachment.
 *
 * @param records - Persisted detached window records.
 * @returns Nothing.
 */
function writePersistedDetachedWindows(
	records: PersistedDetachedWindowRecord[],
): void {
	localStorage.setItem(DETACHED_WINDOWS_STORAGE_KEY, JSON.stringify(records));
}

/**
 * Stores or updates a detached window record in localStorage.
 *
 * @param record - Detached window metadata needed to reattach by window name.
 * @returns Nothing.
 */
export function persistDetachedWindow(
	record: PersistedDetachedWindowRecord,
): void {
	const records = readPersistedDetachedWindows();
	writePersistedDetachedWindows([
		...records.filter((item) => item.key !== record.key),
		record,
	]);
}

/**
 * Removes a detached window record from localStorage.
 *
 * @param key - Detached window key to remove.
 * @returns Nothing.
 */
export function removePersistedDetachedWindow(key: string): void {
	const records = readPersistedDetachedWindows();
	writePersistedDetachedWindows(records.filter((record) => record.key !== key));
}
