import type { QueryCacheState } from "@/entities/QueryCache";
import type { Source } from "@/entities/Source";
import { Request } from "@/entities/Request";
import { Locale } from "@/locales";
import { useScrollY } from "@/store/scroll.store";
import { requestStore } from "@/store/request.store";
import { Icon } from "@/ui/Icon";
import { getRenderSourceLabel } from "@/render-worker/RenderSourceLabel";
import { useSyncExternalStore } from "react";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import s from "./styles/Canvas.module.css";

const SOURCE_ROW_HEIGHT = 48;
const CACHE_WARNING_TOP_OFFSET = 14;
const CACHE_WARNING_ICON_SIZE = 14;
const CACHE_WARNING_COLOR = "#facc15";

interface CanvasCacheWarningOverlayProps {
	sources: readonly Source.Type[];
}

interface SourceLoadingTranslations {
	ingestingSuffix: string;
	loadingSuffix: string;
}

/**
 * Resolves the request decoration appended by the Canvas source renderers.
 * @param requestId Active backend request identifier for the source.
 * @param translations Localized loading and ingestion decorations.
 * @returns Matching localized suffix, or an empty string for no known request.
 */
export function getCanvasSourceLoadingSuffix(
	requestId: Request.Id | undefined,
	translations: SourceLoadingTranslations,
): string {
	if (!requestId) return "";
	const parts = requestId.split("-");
	if (parts.length === 1) return "";
	const prefix = parts[0] as Request.Prefix;
	if (!Object.values(Request.Prefix).includes(prefix)) return "";
	return prefix === Request.Prefix.INGESTION
		? translations.ingestingSuffix
		: translations.loadingSuffix;
}

/**
 * Determines whether one source is displaying warning-state data from its cache pool.
 * @param queryCache Runtime cache authority and freshness state for the source.
 * @returns True only for an active, settled cache carrying a freshness warning.
 */
export function shouldShowCanvasCacheWarning(
	queryCache: QueryCacheState,
): boolean {
	return (
		queryCache.activeGeneration !== null &&
		!queryCache.isActivating &&
		queryCache.warningReason !== null &&
		queryCache.freshness !== "fresh"
	);
}

/**
 * Determines whether a warning-state cache belongs to the rendered source row.
 * Cached queries are source-scoped, so their persisted source id must match the
 * runtime source id exactly; a display name is never an identity boundary.
 * @param source Source row carrying cache state and its applied query provenance.
 * @returns True only when the warning belongs to this exact source id.
 */
export function shouldShowCanvasSourceCacheWarning(
	source: Pick<Source.Type, "id" | "appliedQuery" | "queryCache">,
): boolean {
	const querySourceIds = source.appliedQuery?.source_config?.source_ids;
	return (
		querySourceIds?.length === 1 &&
		querySourceIds[0] === source.id &&
		shouldShowCanvasCacheWarning(source.queryCache)
	);
}

/**
 * Positions one warning icon on the source-name baseline for the current scroll.
 * @param rowIndex Source index in rendered timeline order.
 * @param scrollY Current vertical Canvas scroll offset.
 * @returns CSS top coordinate for the icon row.
 */
export function getCanvasCacheWarningTop(
	rowIndex: number,
	scrollY: number,
): number {
	return rowIndex * SOURCE_ROW_HEIGHT - scrollY + CACHE_WARNING_TOP_OFFSET;
}

/**
 * Renders accessible cache-warning icons over both worker and legacy Canvas labels.
 * @param props Ordered rendered sources used to align each icon with its label.
 * @returns One shared tooltip layer containing only active cached warnings.
 */
export function CanvasCacheWarningOverlay({
	sources,
}: CanvasCacheWarningOverlayProps) {
	const scrollY = useScrollY();
	const requests = useSyncExternalStore(
		requestStore.subscribe,
		requestStore.getSnapshot,
		requestStore.getSnapshot,
	);
	const { t } = Locale.use();
	const tooltip = t("source.cacheWarning.loadedFromCache");
	const loadingTranslations: SourceLoadingTranslations = {
		ingestingSuffix: t("renderEngine.ingestingSuffix"),
		loadingSuffix: t("renderEngine.loadingSuffix"),
	};

	return (
		<TooltipProvider>
			<div className={s.cache_warning_layer}>
				{sources.map((source, rowIndex) =>
					shouldShowCanvasSourceCacheWarning(source) ? (
						<div
							key={source.id}
							className={s.cache_warning_row}
							style={{ top: getCanvasCacheWarningTop(rowIndex, scrollY) }}
							data-source-cache-warning={source.id}
						>
							<span className={s.cache_warning_label} aria-hidden="true">
								{getRenderSourceLabel(
									source,
									getCanvasSourceLoadingSuffix(
										requests.loadings.byFileId.get(source.id),
										loadingTranslations,
									),
								)}
							</span>
							<Tooltip>
								<TooltipTrigger asChild>
									<span
										className={s.cache_warning_icon}
										role="img"
										aria-label={tooltip}
										tabIndex={0}
									>
										<Icon
											name="AlertTriangle"
											size={CACHE_WARNING_ICON_SIZE}
											color={CACHE_WARNING_COLOR}
											aria-hidden="true"
										/>
									</span>
								</TooltipTrigger>
								<TooltipContent side="top">{tooltip}</TooltipContent>
							</Tooltip>
						</div>
					) : null,
				)}
			</div>
		</TooltipProvider>
	);
}
