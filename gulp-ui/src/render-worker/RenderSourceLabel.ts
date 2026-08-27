import type { Source } from "@/entities/Source";

/**
 * Resolves the source name shown by both Canvas rendering backends.
 * @param source Source metadata carrying the configured timeline-label mode.
 * @returns Full source path or its final filename segment.
 */
export function getRenderSourceName(
	source: Pick<Source.Type, "name" | "settings">,
): string {
	return source.settings.timeline_label === "full_path"
		? source.name
		: source.name.split(/[\\/]/).pop() || source.name;
}

/**
 * Builds the complete source label rendered on the timeline.
 * @param source Source metadata carrying name and timeline-label mode.
 * @param loadingSuffix Optional localized request-state decoration.
 * @returns Visible source name followed by its loading suffix.
 */
export function getRenderSourceLabel(
	source: Pick<Source.Type, "name" | "settings">,
	loadingSuffix = "",
): string {
	return `${getRenderSourceName(source)}${loadingSuffix}`;
}
