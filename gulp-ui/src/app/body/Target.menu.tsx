import s from "../Gulp.module.css";
import { FilterFileBanner } from "@/banners/FilterFile.banner";
import { Application } from "@/context/Application.context";
import { enginesBase } from "@/dto/Engine.dto";
import {
	ContextMenuContent,
	ContextMenuGroup,
	ContextMenuItem,
	ContextMenuLabel,
	ContextMenuSeparator,
	ContextMenuSub,
	ContextMenuSubContent,
	ContextMenuSubTrigger,
} from "@/ui/ContextMenu";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { Enrichment } from "@/banners/Enrichment.banner";
import { Sigma } from "@/banners/Sigma";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { Refractor } from "@/ui/utils";
import { toast } from "sonner";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { Source } from "@/entities/Source";
import { Locale } from "@/locales";
import { Request } from "@/entities/Request";
import { requestStore, useRequestLoadings } from "@/store/request.store";

interface TargetMenuProps {
	source: Source.Type;
}

export function TargetMenu({ source }: TargetMenuProps) {
	const { Info, spawnBanner, spawnDialog, app } = Application.use();
	const { t } = Locale.use();
	const loadings = useRequestLoadings();
	const hasOffset =
		Number.isFinite(source.settings.offset) && source.settings.offset !== 0;

	const removeFilters = (source: Source.Type) => {
		Info.filters_remove(source);
		setTimeout(() => {
			Info.refetch({
				ids: source.id,
			});
		}, 300);
	};

	/**
	 * Cancels the request currently associated with a source.
	 * @param source Source whose active request should be canceled.
	 * @returns Nothing.
	 */
	const cancelRequest = (source: Source.Type): void => {
		const request = loadings.byFileId.get(source.id);

		if (request) {
			if (
				requestStore.isManagedQueryRequest(request) ||
				String(request).startsWith(Request.Prefix.QUERY)
			) {
				void Info.cancelManagedRequest(request);
				return;
			}

			Info.request_cancel(request);
		}
	};

	const showEvent = (last = false) => {
		const events = Source.Entity.events(app, source);
		if (!events.length) {
			toast.error(t("targetMenu.noEvents"), {
				icon: <Icon name="FileQuestion" />,
				richColors: true,
			});
			return;
		}

		const event = last ? events[0] : events[events.length - 1];

		if (!event) {
			return;
		}

		spawnDialog(<DisplayEventDialog event={event} />);
	};

	return (
		<ContextMenuContent data-state="open">
			<TooltipProvider>
				<Tooltip>
					<TooltipTrigger asChild>
						<ContextMenuLabel className={s.cm_title}>
							{source.name}
						</ContextMenuLabel>
					</TooltipTrigger>
					<TooltipContent>{source.name}</TooltipContent>
				</Tooltip>
			</TooltipProvider>
			<ContextMenuSeparator />
			<ContextMenuItem
				onClick={() => Info.refetch({ ids: source.id })}
				icon="RefreshClockwise"
			>
				{t("targetMenu.refetch")}
			</ContextMenuItem>
			<ContextMenuItem
				onClick={() => cancelRequest(source)}
				icon="StopCircle"
			>
				{t("targetMenu.stop")}
			</ContextMenuItem>
			<ContextMenuSub>
				<ContextMenuSubTrigger icon="Cpu">
					{t("common.renderEngine")}
				</ContextMenuSubTrigger>
				<ContextMenuSubContent>
					{enginesBase.map((i) => (
						<ContextMenuItem
							key={i.plugin}
							onClick={() =>
								Info.file_set_settings(source.id, { render_engine: i.plugin })
							}
							icon={i.img}
						>
							{i.title}
						</ContextMenuItem>
					))}
				</ContextMenuSubContent>
			</ContextMenuSub>
			<ContextMenuItem
				onClick={() => spawnBanner(<Source.Settings.Banner source={source} />)}
				icon="Settings"
			>
				{t("settings.title")}
			</ContextMenuItem>
			<ContextMenuItem
				onClick={() => spawnBanner(<Enrichment.Banner source={source} />)}
				icon="PrismColor"
			>
				{t("targetMenu.enrich")}
			</ContextMenuItem>
			<ContextMenuSeparator />
			<ContextMenuGroup>
				<ContextMenuLabel>{t("common.filters")}</ContextMenuLabel>
				<ContextMenuItem
					onClick={() => spawnBanner(<FilterFileBanner sources={[source]} />)}
					icon="Filter"
				>
					{t("targetMenu.manageFilters")}
				</ContextMenuItem>
				<ContextMenuItem
					onClick={() => removeFilters(source)}
					icon="X"
				>
					{t("targetMenu.resetFilters")}
				</ContextMenuItem>
				{app.timeline.cache.data.has(source.id) && (
					<ContextMenuItem
						onClick={() => Info.filters_undo([source])}
						icon="Undo"
					>
						{t("targetMenu.undoLastFiltersChange")}
					</ContextMenuItem>
				)}
			</ContextMenuGroup>
			<ContextMenuSeparator />
			<ContextMenuGroup>
				<ContextMenuLabel>{t("common.actions")}</ContextMenuLabel>
				<ContextMenuItem
					onClick={() =>
						Info.setInfoByKey(source, "general", "tableViewSource")
					}
					icon="Table"
				>
					{t("targetMenu.tableView")}
				</ContextMenuItem>
				<ContextMenuItem
					disabled={!hasOffset}
					onClick={() => {
						if (!hasOffset) return;
						void Info.rebase_by_offset(source);
					}}
					icon="RefreshCw"
				>
					{t("targetMenu.rebaseByOffset")}
				</ContextMenuItem>
				<ContextMenuItem
					onClick={() =>
						Info.setInfoByKey(
							Refractor.array(
								...app.target.files.map((f) => ({
									...f,
									selected: f.id === source.id ? false : f.selected,
								})),
							),
							"target",
							"files",
						)
					}
					icon="EyeOff"
				>
					{t("common.hide")}
				</ContextMenuItem>
				<ContextMenuSub>
					<ContextMenuSubTrigger icon="Move">
						{t("targetMenu.reorder")}
					</ContextMenuSubTrigger>
					<ContextMenuSubContent>
						<ContextMenuItem
							onClick={() => Info.files_repin(source.id)}
							icon={source.pinned ? "PinOff" : "Pin"}
						>
							{source
								? source.pinned
									? t("targetMenu.unpin")
									: t("targetMenu.pin")
								: "..."}
						</ContextMenuItem>
						<ContextMenuItem
							onClick={() => Info.files_reorder_upper(source.id)}
							icon="ArrowBigUp"
						>
							{t("common.moveUp")}
						</ContextMenuItem>
						<ContextMenuItem
							onClick={() => Info.files_reorder_lower(source.id)}
							icon="ArrowBigDown"
						>
							{t("common.moveDown")}
						</ContextMenuItem>
					</ContextMenuSubContent>
				</ContextMenuSub>
				<Stack gap={2}>
					<ContextMenuItem
						onClick={() => showEvent()}
						icon="ArrowLeftFromLine"
					>
						{t("targetMenu.showFirstEvent")}
					</ContextMenuItem>
					<ContextMenuItem
						revert
						onClick={() => showEvent(true)}
						icon="ArrowRightFromLine"
					>
						{t("targetMenu.showLastEvent")}
					</ContextMenuItem>
				</Stack>
			</ContextMenuGroup>
			<ContextMenuSeparator />
			<ContextMenuGroup>
				<ContextMenuLabel>{t("common.sigma")}</ContextMenuLabel>
				<ContextMenuItem
					onClick={() => spawnBanner(<Sigma.Banner sources={[source.id]} />)}
					icon="Sigma"
				>
					{t("targetMenu.uploadRule")}
				</ContextMenuItem>
			</ContextMenuGroup>
			<ContextMenuItem
				className={s.delete}
				icon="Trash2"
				onClick={() => spawnBanner(<Source.Delete.Banner source={source} />)}
			>
				{t("common.delete")}
			</ContextMenuItem>
		</ContextMenuContent>
	);
}
