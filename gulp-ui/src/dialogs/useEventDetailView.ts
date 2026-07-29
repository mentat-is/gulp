import {
	useCallback,
	useEffect,
	useMemo,
	useState,
} from "react";

export type EventDetailView = "raw" | "table" | "tree";

export const EVENT_DETAIL_VIEW_STORAGE_KEY = "gulp.eventDialog.detailView";
export const DEFAULT_EVENT_DETAIL_VIEW: EventDetailView = "tree";

/**
 * Validates a persisted or UI-provided Event detail view.
 *
 * @param value Candidate storage or Tabs value.
 * @returns A supported detail view, falling back to Tree.
 */
export function parseEventDetailView(value: unknown): EventDetailView {
	return value === "raw" || value === "table" || value === "tree"
		? value
		: DEFAULT_EVENT_DETAIL_VIEW;
}

/**
 * Resolves localStorage for the document that owns an Event Dialog.
 *
 * Access can throw for restricted browser contexts, in which case the
 * preference remains an in-memory Tree fallback.
 */
function getEventDetailViewStorage(
	currentDocument: Document,
): Storage | null {
	try {
		return currentDocument.defaultView?.localStorage ?? null;
	} catch {
		return null;
	}
}

/**
 * Keeps the Event detail view stable across event navigation and dialog
 * remounts. Docked and detached windows share the same-origin storage key.
 */
export function useEventDetailViewPreference(
	currentDocument: Document,
): readonly [EventDetailView, (value: string) => void] {
	const storage = useMemo(
		() => getEventDetailViewStorage(currentDocument),
		[currentDocument],
	);
	const [view, setView] = useState<EventDetailView>(() => {
		try {
			return parseEventDetailView(
				storage?.getItem(EVENT_DETAIL_VIEW_STORAGE_KEY),
			);
		} catch {
			return DEFAULT_EVENT_DETAIL_VIEW;
		}
	});

	const selectView = useCallback(
		(value: string): void => {
			const nextView = parseEventDetailView(value);
			setView(nextView);
			try {
				storage?.setItem(EVENT_DETAIL_VIEW_STORAGE_KEY, nextView);
			} catch {
				// The selected view still remains valid for this mounted Dialog.
			}
		},
		[storage],
	);

	useEffect(() => {
		const ownerWindow = currentDocument.defaultView;
		if (!ownerWindow) return;

		const handleStorage = (event: StorageEvent): void => {
			if (event.key !== EVENT_DETAIL_VIEW_STORAGE_KEY) return;
			setView(parseEventDetailView(event.newValue));
		};
		ownerWindow.addEventListener("storage", handleStorage);
		return () => ownerWindow.removeEventListener("storage", handleStorage);
	}, [currentDocument]);

	return [view, selectView] as const;
}
