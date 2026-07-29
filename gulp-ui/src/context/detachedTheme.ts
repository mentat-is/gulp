import type { WindowBridge } from "@/lib/WindowBridge";

/**
 * Applies one complete main-document theme snapshot to a detached document.
 *
 * @param targetDocument Document owned by the detached root.
 * @param payload Theme identity and computed custom properties from the main tab.
 * @param previousVariables Custom properties applied by the preceding snapshot.
 * @returns Custom-property names owned by the new snapshot.
 */
export function applyDetachedThemeSnapshot(
	targetDocument: Document,
	payload: WindowBridge.ThemeChangePayload,
	previousVariables: ReadonlySet<string>,
): Set<string> {
	const root = targetDocument.documentElement;
	root.setAttribute("data-theme", payload.theme);
	previousVariables.forEach((variable) => {
		if (!(variable in payload.cssVariables)) {
			root.style.removeProperty(variable);
		}
	});
	Object.entries(payload.cssVariables).forEach(([variable, value]) => {
		root.style.setProperty(variable, value);
	});
	return new Set(Object.keys(payload.cssVariables));
}
