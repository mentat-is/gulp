/** Viewport-ready tuple consumed by the legacy highlight API. */
export type LegacyHighlightTuple = [number, number, number, string];

/** Document-space highlight entry retained independently of viewport scroll. */
export interface HighlightRegistryEntry {
	id: string;
	left: number;
	width: number;
	index: number;
	color: string;
}

/** Stores stable highlight geometry and applies viewport scroll lazily on read. */
export class HighlightRegistry {
	private entries = new Map<string, Omit<HighlightRegistryEntry, "id">>();
	private viewportOffset = 0;

	/**
	 * Replaces all registered geometry and removes entries absent from the collection.
	 * @param entries Complete document-space highlight collection.
	 * @returns Nothing.
	 */
	public replace(entries: HighlightRegistryEntry[]): void {
		this.entries = new Map(
			entries.map(({ id, ...entry }) => [id, entry]),
		);
	}

	/**
	 * Registers or replaces one document-space highlight.
	 * @param entry Highlight geometry and appearance.
	 * @returns Nothing.
	 */
	public set(entry: HighlightRegistryEntry): void {
		const { id, ...geometry } = entry;
		this.entries.set(id, geometry);
	}

	/**
	 * Stores the current horizontal viewport offset in constant time.
	 * @param viewportOffset Current timeline horizontal scroll.
	 * @returns Nothing.
	 */
	public setViewportOffset(viewportOffset: number): void {
		this.viewportOffset = viewportOffset;
	}

	/**
	 * Removes one highlight immediately from the legacy registry.
	 * @param id Highlight identifier to remove.
	 * @returns Nothing.
	 */
	public remove(id: string): void {
		this.entries.delete(id);
	}

	/**
	 * Clears all geometry and resets the viewport offset.
	 * @returns Nothing.
	 */
	public clear(): void {
		this.entries.clear();
		this.viewportOffset = 0;
	}

	/**
	 * Materializes legacy viewport-relative tuples only when a consumer requests them.
	 * @returns Highlight tuples in registration order.
	 */
	public list(): LegacyHighlightTuple[] {
		return Array.from(this.entries.values(), (entry) => [
			entry.left - this.viewportOffset,
			entry.width,
			entry.index,
			entry.color,
		]);
	}
}

export const highlightRegistry = new HighlightRegistry();
