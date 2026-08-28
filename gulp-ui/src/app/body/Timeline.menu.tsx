import s from "../Gulp.module.css";
import { Application } from "@/context/Application.context";
import { Filter } from "@/entities/Filter";
import { Source } from "@/entities/Source";
import { Locale } from "@/locales";
import type { ReactElement } from "react";
import {
	ContextMenuContent,
	ContextMenuItem,
	ContextMenuLabel,
	ContextMenuSeparator,
} from "@/ui/ContextMenu";

/**
 * Renders timeline-wide actions for every source selected by the user.
 * Canvas visibility filters are intentionally ignored.
 * @returns Global timeline context menu, or nothing without selected sources.
 */
export function TimelineMenu(): ReactElement | null {
	const { app, Info } = Application.use();
	const { t } = Locale.use();
	const selectedSources = Source.Entity.selected(app);
	const selectedSourceIds = selectedSources.map((source) => source.id);

	if (selectedSources.length === 0) return null;

	/**
	 * Reconciles existing caches before fetching missing selected-source data.
	 * @returns Nothing after dispatching the shared refetch flow.
	 */
	const refetchAll = (): void => {
		void Info.refetch({
			ids: selectedSourceIds,
			cachePolicy: "initial",
		});
	};

	/**
	 * Clears every selected source query and runs one authoritative bulk refetch.
	 * This preserves the existing single-source reset behavior instead of restoring
	 * a latest-cache fallback whose query may still contain filters.
	 * @returns Nothing after dispatching the shared refetch flow.
	 */
	const resetAllFilters = (): void => {
		void Info.refetch({
			ids: selectedSourceIds,
			query: Filter.Entity.default(app, selectedSources[0]),
			cachePolicy: "force",
		});
	};

	return (
		<ContextMenuContent data-state="open">
			<ContextMenuLabel className={s.cm_title}>
				{t("source.selectedSources", { count: selectedSources.length })}
			</ContextMenuLabel>
			<ContextMenuSeparator />
			<ContextMenuItem onClick={refetchAll} icon="RefreshClockwise">
				{t("timelineMenu.refetchAll")}
			</ContextMenuItem>
			<ContextMenuItem onClick={resetAllFilters} icon="X">
				{t("timelineMenu.resetAllFilters")}
			</ContextMenuItem>
		</ContextMenuContent>
	);
}
