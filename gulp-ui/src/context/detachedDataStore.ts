/**
 * Determines whether a detached React tree runs in the document's own JavaScript
 * realm and therefore owns an independent module-level DataStore instance.
 *
 * React roots rendered by the opener into a popup document still execute against
 * the opener's module graph. Their document has a different `defaultView`, so
 * hydrating DataStore there would overwrite the main application's canonical data.
 *
 * @param detachedDocument Document that hosts the detached React tree.
 * @param runtimeWindow Window global that evaluates the React component.
 * @returns True only when the detached document and component share one realm.
 */
export function ownsIndependentDetachedDataStore(
	detachedDocument: Document,
	runtimeWindow: Window,
): boolean {
	return detachedDocument.defaultView === runtimeWindow;
}
