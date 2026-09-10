import { Locale } from "@/locales";
import {
	forwardRef,
	useImperativeHandle,
	useRef,
	type ForwardedRef,
	type ReactElement,
} from "react";
import tooltipStyles from "@/ui/styles/Tooltip.module.css";
import s from "./styles/Canvas.module.css";

const TOOLTIP_POINTER_OFFSET = 12;
const TOOLTIP_VIEWPORT_MARGIN = 8;

/** Canvas-relative tooltip anchor and clipping bounds. */
export interface CanvasCacheWarningTooltipAnchor {
	sourceId: string;
	x: number;
	y: number;
	bounds: { width: number; height: number };
}

/** Imperative tooltip API used without scheduling a Canvas React render. */
export interface CanvasCacheWarningTooltipHandle {
	show: (anchor: CanvasCacheWarningTooltipAnchor) => void;
	hide: () => void;
}

/** Measured tooltip size used by the pure clamp calculation. */
export interface CanvasCacheWarningTooltipSize {
	width: number;
	height: number;
}

/** Minimal legacy warning region consumed by main-thread pointer handling. */
export interface CanvasCacheWarningHitRegion {
	sourceId: string;
	rect: { x: number; y: number; w: number; h: number };
}

/**
 * Resolves the topmost legacy cache warning at one Canvas coordinate.
 * @param regions Warning rectangles in paint order.
 * @param x Canvas-relative pointer x coordinate.
 * @param y Canvas-relative pointer y coordinate.
 * @returns Compact cache-warning identity, or null outside every warning.
 */
export function hitTestCanvasCacheWarning(
	regions: readonly CanvasCacheWarningHitRegion[],
	x: number,
	y: number,
): { kind: "cache-warning"; id: string } | null {
	for (let index = regions.length - 1; index >= 0; index--) {
		const region = regions[index];
		if (
			x >= region.rect.x &&
			x <= region.rect.x + region.rect.w &&
			y >= region.rect.y &&
			y <= region.rect.y + region.rect.h
		) {
			return { kind: "cache-warning", id: region.sourceId };
		}
	}
	return null;
}

/**
 * Positions a warning tooltip near its entry pointer and clamps it to the Canvas.
 * @param anchor Pointer entry coordinate and Canvas bounds.
 * @param size Tooltip dimensions measured once for this entry.
 * @returns Canvas-relative tooltip origin.
 */
export function resolveCanvasCacheWarningTooltipPosition(
	anchor: CanvasCacheWarningTooltipAnchor,
	size: CanvasCacheWarningTooltipSize,
): { x: number; y: number } {
	const maximumX = Math.max(
		TOOLTIP_VIEWPORT_MARGIN,
		anchor.bounds.width - size.width - TOOLTIP_VIEWPORT_MARGIN,
	);
	const maximumY = Math.max(
		TOOLTIP_VIEWPORT_MARGIN,
		anchor.bounds.height - size.height - TOOLTIP_VIEWPORT_MARGIN,
	);
	let x = anchor.x + TOOLTIP_POINTER_OFFSET;
	if (x + size.width > anchor.bounds.width - TOOLTIP_VIEWPORT_MARGIN) {
		x = anchor.x - TOOLTIP_POINTER_OFFSET - size.width;
	}
	let y = anchor.y - TOOLTIP_POINTER_OFFSET - size.height;
	if (y < TOOLTIP_VIEWPORT_MARGIN) y = anchor.y + TOOLTIP_POINTER_OFFSET;
	return {
		x: Math.min(maximumX, Math.max(TOOLTIP_VIEWPORT_MARGIN, x)),
		y: Math.min(maximumY, Math.max(TOOLTIP_VIEWPORT_MARGIN, y)),
	};
}

/**
 * Renders the one DOM tooltip shared by every Canvas cache-warning hit region.
 * @param props No declarative state is required; visibility is fully imperative.
 * @param forwardedRef Handle receiving show and hide operations.
 * @returns One normally hidden, non-interactive tooltip node.
 */
function CanvasCacheWarningTooltipComponent(
	_props: object,
	forwardedRef: ForwardedRef<CanvasCacheWarningTooltipHandle>,
): ReactElement {
	const { t } = Locale.use();
	const tooltipRef = useRef<HTMLDivElement>(null);
	const visibleSourceIdRef = useRef<string | null>(null);

	useImperativeHandle(
		forwardedRef,
		() => ({
			/** Shows and measures the tooltip only when entering a different warning. */
			show: (anchor): void => {
				if (visibleSourceIdRef.current === anchor.sourceId) return;
				const tooltip = tooltipRef.current;
				if (!tooltip) return;
				visibleSourceIdRef.current = anchor.sourceId;
				tooltip.hidden = false;
				tooltip.setAttribute("aria-hidden", "false");
				tooltip.dataset.sourceId = anchor.sourceId;
				tooltip.style.transform = "translate3d(0, 0, 0)";
				const measured = tooltip.getBoundingClientRect();
				const position = resolveCanvasCacheWarningTooltipPosition(anchor, {
					width: measured.width,
					height: measured.height,
				});
				tooltip.style.transform = `translate3d(${position.x}px, ${position.y}px, 0)`;
			},
			/** Hides the tooltip and releases its entry identity. */
			hide: (): void => {
				visibleSourceIdRef.current = null;
				const tooltip = tooltipRef.current;
				if (!tooltip) return;
				tooltip.hidden = true;
				tooltip.setAttribute("aria-hidden", "true");
				delete tooltip.dataset.sourceId;
			},
		}),
		[],
	);

	return (
		<div
			ref={tooltipRef}
			className={`${tooltipStyles.tooltip} ${s.cache_warning_tooltip}`}
			role="tooltip"
			aria-hidden="true"
			data-canvas-cache-warning-tooltip="true"
			hidden
		>
			{t("source.cacheWarning.loadedFromCache")}
		</div>
	);
}

export const CanvasCacheWarningTooltip = forwardRef<
	CanvasCacheWarningTooltipHandle,
	object
>(
	CanvasCacheWarningTooltipComponent,
);
CanvasCacheWarningTooltip.displayName = "CanvasCacheWarningTooltip";
