/**
 * Resolves the body-level portal host shared by Table View modal banners and
 * their floating controls.
 * @param currentDocument Document owned by the active main or detached window.
 * @returns The active document body.
 */
export function resolveTableNotePortalContainer(
	currentDocument: Document,
): HTMLElement {
	return currentDocument.body;
}
