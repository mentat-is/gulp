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
import { useRequestLoadings } from "@/store/request.store";

/**
 * Renders timeline-wide actions for every source selected by the user.
 * Canvas visibility filters are intentionally ignored.
 * @returns Global timeline context menu, or nothing without selected sources.
 */
export function TimelineMenu(): ReactElement | null {
	const { app, Info } = Application.use();
	const { t } = Locale.use();
	const loadings = useRequestLoadings();
	const selectedSources = Source.Entity.selected(app);
	const selectedSourceIds = selectedSources.map((source) => source.id);
	const canUndoQueryCache = selectedSources.some(
		(source) =>
			!source.queryCache.isActivating &&
			!loadings.byFileId.has(source.id) &&
			source.queryCache.canUndo,
	);
	const canRedoQueryCache = selectedSources.some(
		(source) =>
			!source.queryCache.isActivating &&
			!loadings.byFileId.has(source.id) &&
			source.queryCache.canRedo,
	);

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

	/**
	 * Stops pending and active work for every selected source.
	 * @returns Nothing after dispatching the shared cancellation flow.
	 */
	const stopAll = (): void => {
		void Info.stopSourceRequests(selectedSourceIds);
	};

	/**
	 * Moves every eligible selected source through its local query-cache history.
	 * @param direction Cache-history direction to apply.
	 * @returns Nothing after dispatching each source-local activation.
	 */
	const navigateAllQueryCaches = (direction: "undo" | "redo"): void => {
		void Promise.all(
			selectedSources.map((source) =>
				Info.navigateSourceQueryCache(source, direction),
			),
		);
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
			<ContextMenuItem onClick={stopAll} icon="StopCircle">
				{t("timelineMenu.stopAll")}
			</ContextMenuItem>
			<ContextMenuItem onClick={resetAllFilters} icon="X">
				{t("timelineMenu.resetAllFilters")}
			</ContextMenuItem>
			<ContextMenuItem
				disabled={!canUndoQueryCache}
				onClick={() => navigateAllQueryCaches("undo")}
				icon="Undo"
			>
				{t("timelineMenu.undoAllQueryCache")}
			</ContextMenuItem>
			<ContextMenuItem
				disabled={!canRedoQueryCache}
				onClick={() => navigateAllQueryCaches("redo")}
				icon="Redo"
			>
				{t("timelineMenu.redoAllQueryCache")}
			</ContextMenuItem>
		</ContextMenuContent>
	);
}
