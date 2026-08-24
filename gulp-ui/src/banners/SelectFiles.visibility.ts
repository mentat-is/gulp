import type { Source } from "@/entities/Source";

export interface SelectFilesSourceActivity {
	/** Latest event count received for the source during ingestion. */
	ingestionProgress: number;
	/** Whether an ingestion request currently owns the source. */
	isIngesting: boolean;
}

/**
 * Decides whether a source belongs in the Select Files list.
 *
 * @param source Source metadata currently stored in application state.
 * @param showEmptySources Whether the user explicitly enabled empty sources.
 * @param activity Live ingestion state received after the source was created.
 * @returns True when the source has persisted events, live events, or active ingestion.
 */
export function isSelectFilesSourceVisible(
	source: Pick<Source.Type, "total">,
	showEmptySources: boolean,
	activity: SelectFilesSourceActivity,
): boolean {
	return (
		showEmptySources ||
		source.total > 0 ||
		activity.ingestionProgress > 0 ||
		activity.isIngesting
	);
}
