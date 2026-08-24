import { Application } from "@/context/Application.context";
import { Color } from "@/entities/Color";
import { User } from "@/entities/User";
import { Locale } from "@/locales";
import { scrollStore } from "@/store/scroll.store";
import { useTimestampTimeZone } from "@/store/timestampTimeZone.store";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { cn, formatTimestampToReadableString } from "@/ui/utils";
import { useTheme } from "next-themes";
import {
	memo,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import type { PointerPositionStore } from "./PointerPosition.store";
import {
	applyLocalPointerSnapshot,
	applyRemotePointerOrientations,
	applyRemotePointerScrollX,
	applyRemotePointerScrollY,
	resolveRemotePointerHorizontalPosition,
} from "./PointerPosition.store";
import s from "./styles/Pointers.module.css";

type RGB = { r: number; g: number; b: number };

interface PointerAppearance {
	backgroundColor: string;
	textColor: string;
}

interface LocalPointerProps {
	selfStore: PointerPositionStore;
	themeKey: string;
	formatLabel: (timestamp: number) => string;
}

interface RemotePointerProps {
	positionX: number;
	positionY: number;
	viewportX: number;
	documentX?: number;
	id: User.Id;
	color: string;
	themeKey: string;
	viewportWidth: number;
}

type RemotePointerLayerStyle = React.CSSProperties & {
	"--remote-pointer-scroll-x"?: string;
	"--remote-pointer-scroll-y"?: string;
};

const LOCAL_POINTER_COLOR = "var(--green-700)";
const pointerAppearanceCache = new Map<string, PointerAppearance>();

/**
 * Parses a three- or six-digit hexadecimal color.
 * @param value CSS hexadecimal color value.
 * @returns Parsed RGB channels, or null for an unsupported value.
 */
function parseHexToRgb(value: string): RGB | null {
	const hex = value.trim().replace("#", "");

	if (hex.length === 3) {
		const r = parseInt(hex[0] + hex[0], 16);
		const g = parseInt(hex[1] + hex[1], 16);
		const b = parseInt(hex[2] + hex[2], 16);
		return Number.isNaN(r + g + b) ? null : { r, g, b };
	}

	if (hex.length === 6) {
		const r = parseInt(hex.slice(0, 2), 16);
		const g = parseInt(hex.slice(2, 4), 16);
		const b = parseInt(hex.slice(4, 6), 16);
		return Number.isNaN(r + g + b) ? null : { r, g, b };
	}

	return null;
}

/**
 * Parses an rgb() or rgba() CSS color.
 * @param value CSS functional color value.
 * @returns Parsed RGB channels, or null for an unsupported value.
 */
function parseRgbToRgb(value: string): RGB | null {
	const match = value.match(/^rgba?\(([^)]+)\)$/i);
	if (!match) return null;
	const channels = match[1].split(",").map((part) => Number(part.trim()));
	if (channels.length < 3) return null;

	const [r, g, b] = channels;
	if ([r, g, b].some((channel) => Number.isNaN(channel))) return null;
	return { r, g, b };
}

/**
 * Converts HSL channels to RGB channels.
 * @param hue Hue in degrees.
 * @param saturation Saturation percentage.
 * @param lightness Lightness percentage.
 * @returns Converted RGB channels.
 */
function hslToRgb(hue: number, saturation: number, lightness: number): RGB {
	const normalizedSaturation = saturation / 100;
	const normalizedLightness = lightness / 100;
	const chroma =
		(1 - Math.abs(2 * normalizedLightness - 1)) * normalizedSaturation;
	const hueSection = hue / 60;
	const secondaryChannel = chroma * (1 - Math.abs((hueSection % 2) - 1));

	let red = 0;
	let green = 0;
	let blue = 0;

	if (hueSection >= 0 && hueSection < 1) {
		red = chroma;
		green = secondaryChannel;
	} else if (hueSection < 2) {
		red = secondaryChannel;
		green = chroma;
	} else if (hueSection < 3) {
		green = chroma;
		blue = secondaryChannel;
	} else if (hueSection < 4) {
		green = secondaryChannel;
		blue = chroma;
	} else if (hueSection < 5) {
		red = secondaryChannel;
		blue = chroma;
	} else {
		red = chroma;
		blue = secondaryChannel;
	}

	const lightnessOffset = normalizedLightness - chroma / 2;
	return {
		r: Math.round((red + lightnessOffset) * 255),
		g: Math.round((green + lightnessOffset) * 255),
		b: Math.round((blue + lightnessOffset) * 255),
	};
}

/**
 * Parses an hsl() or hsla() CSS color.
 * @param value CSS functional color value.
 * @returns Parsed RGB channels, or null for an unsupported value.
 */
function parseHslToRgb(value: string): RGB | null {
	const match = value.match(/^hsla?\(([^)]+)\)$/i);
	if (!match) return null;

	const parts = match[1].split(",").map((part) => part.trim());
	if (parts.length < 3) return null;

	const hue = Number(parts[0].replace("deg", ""));
	const saturation = Number(parts[1].replace("%", ""));
	const lightness = Number(parts[2].replace("%", ""));
	if ([hue, saturation, lightness].some((channel) => Number.isNaN(channel))) {
		return null;
	}

	return hslToRgb(((hue % 360) + 360) % 360, saturation, lightness);
}

/**
 * Resolves one CSS custom-property color against a previously read root style.
 * @param value Original CSS color.
 * @param rootStyle Computed root style read once for this appearance.
 * @returns Concrete CSS color when resolvable, otherwise the original value.
 */
function resolveCssVariableColor(
	value: string,
	rootStyle: CSSStyleDeclaration | null,
): string {
	const variableMatch = value.match(/^var\((--[^),\s]+)\)/);
	if (!variableMatch || !rootStyle) return value;
	return rootStyle.getPropertyValue(variableMatch[1]).trim() || value;
}

/**
 * Parses a concrete supported CSS color into RGB channels.
 * @param value Concrete CSS color value.
 * @returns Parsed RGB channels, or null for unsupported syntax.
 */
function parseResolvedColor(value: string): RGB | null {
	if (value.startsWith("#")) return parseHexToRgb(value);
	if (value.toLowerCase().startsWith("rgb")) return parseRgbToRgb(value);
	if (value.toLowerCase().startsWith("hsl")) return parseHslToRgb(value);
	return null;
}

/**
 * Resolves and caches pointer colors once for each active theme and source color.
 * @param backgroundColor Pointer label background color.
 * @param themeKey Active theme cache partition.
 * @returns Background and accessible text colors.
 */
function getPointerAppearance(
	backgroundColor: string,
	themeKey: string,
): PointerAppearance {
	const cacheKey = `${themeKey}:${backgroundColor}`;
	const cachedAppearance = pointerAppearanceCache.get(cacheKey);
	if (cachedAppearance) return cachedAppearance;

	const rootStyle =
		typeof window === "undefined"
			? null
			: getComputedStyle(document.documentElement);
	const resolvedBackground = resolveCssVariableColor(backgroundColor, rootStyle);
	const rgb = parseResolvedColor(resolvedBackground);
	const luminance = rgb
		? (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255
		: null;
	const appearance = {
		backgroundColor,
		textColor:
			luminance === null
				? Color.Themer.theme.FONT_ACCENT
				: luminance > 0.55
					? "#111827"
					: "#f8f8f2",
	};
	pointerAppearanceCache.set(cacheKey, appearance);
	return appearance;
}

/**
 * Renders the local pointer once and applies store updates without React renders.
 * @param props Pointer store, localized label formatter, and active theme key.
 * @returns Stable local pointer element.
 */
const LocalPointer = memo(function LocalPointer({
	selfStore,
	themeKey,
	formatLabel,
}: LocalPointerProps) {
	const pointerRef = useRef<HTMLDivElement>(null);
	const labelRef = useRef<HTMLParagraphElement>(null);
	const appearance = useMemo(
		() => getPointerAppearance(LOCAL_POINTER_COLOR, themeKey),
		[themeKey],
	);

	useEffect(() => {
		/**
		 * Applies the latest store snapshot to the mounted local pointer.
		 * @returns Nothing.
		 */
		const updatePointer = (): void => {
			const pointerElement = pointerRef.current;
			const labelElement = labelRef.current;
			if (!pointerElement || !labelElement) return;
			const snapshot = selfStore.getSnapshot();
			applyLocalPointerSnapshot(
				pointerElement,
				labelElement,
				snapshot,
				formatLabel(snapshot.timestamp),
				s.right,
			);
		};

		updatePointer();
		return selfStore.subscribe(updatePointer);
	}, [formatLabel, selfStore]);

	return (
		<Stack
			ref={pointerRef}
			className={cn(s.pointerPosition, s.localPointer)}
			ai="flex-start"
			pos="absolute"
		>
			<Stack className={s.pointer} ai="flex-start">
				<Icon
					name="MousePointer2"
					size={14}
					color={appearance.backgroundColor}
					fill={appearance.backgroundColor}
				/>
				<p
					ref={labelRef}
					style={{
						background: appearance.backgroundColor,
						color: appearance.textColor,
					}}
				/>
			</Stack>
		</Stack>
	);
});

/**
 * Renders one collaborative pointer from scalar memoization inputs.
 * @param props Collaborative pointer values and viewport transform state.
 * @returns Remote pointer element.
 */
const RemotePointer = memo(function RemotePointer({
	positionX,
	positionY,
	viewportX,
	documentX,
	id,
	color,
	themeKey,
	viewportWidth,
}: RemotePointerProps) {
	const appearance = useMemo(
		() => getPointerAppearance(color, themeKey),
		[color, themeKey],
	);
	const isRightSide = viewportX > viewportWidth / 2;

	return (
		<Stack
			className={cn(
				s.pointerPosition,
				s.remotePointer,
				isRightSide && s.right,
			)}
			style={{ transform: `translate3d(${positionX}px, ${positionY}px, 0)` }}
			data-remote-pointer-document-x={documentX}
			ai="flex-start"
			pos="absolute"
		>
			<Stack className={s.pointer} ai="flex-start">
				<Icon
					name="MousePointer2"
					size={14}
					color={appearance.backgroundColor}
					fill={appearance.backgroundColor}
				/>
				<p
					style={{
						background: appearance.backgroundColor,
						color: appearance.textColor,
					}}
				>
					{id}
				</p>
			</Stack>
		</Stack>
	);
});

export namespace Pointers {
	export interface Props extends Stack.Props {
		getPixelPosition: (timestamp: number) => number;
		selfStore: PointerPositionStore;
	}

	export interface Pointer {
		timestamp: number;
		y: number;
		x?: number;
		id: User.Id;
		color: string;
	}
}

/**
 * Renders an imperative local pointer and memoized collaborative pointers.
 * @param props Remote timestamp transform, isolated local store, and Stack properties.
 * @returns Pointer overlay component.
 */
export function Pointers({
	getPixelPosition,
	selfStore,
	...props
}: Pointers.Props) {
	const { app } = Application.use();
	const { t } = Locale.use();
	const timestampTimeZone = useTimestampTimeZone();
	const { resolvedTheme, theme } = useTheme();
	const pointerLayerRef = useRef<HTMLDivElement>(null);
	const remoteVerticalLayerRef = useRef<HTMLDivElement>(null);
	const remoteTimelineLayerRef = useRef<HTMLDivElement>(null);
	const viewportWidthRef = useRef(1);
	const [viewportWidth, setViewportWidth] = useState(1);
	const themeKey = resolvedTheme ?? theme ?? "default";
	const currentScrollX = scrollStore.getX();
	/**
	 * Formats the localized label for one local-pointer timestamp.
	 * @param timestamp Timeline timestamp under the local pointer.
	 * @returns Localized pointer label.
	 */
	const formatLocalLabel = useCallback(
		(timestamp: number): string =>
			t("pointers.localLabel", {
				timestamp: formatTimestampToReadableString(
					timestamp,
					timestampTimeZone,
				),
			}),
		[t, timestampTimeZone],
	);
	/**
	 * Applies current horizontal scroll and direction to timestamp-based pointers.
	 * @returns Nothing.
	 */
	const updateRemotePointerHorizontalState = useCallback((): void => {
		const timelineLayer = remoteTimelineLayerRef.current;
		if (!timelineLayer) return;
		const scrollX = scrollStore.getX();
		applyRemotePointerScrollX(timelineLayer, scrollX);
		applyRemotePointerOrientations(
			timelineLayer,
			scrollX,
			viewportWidthRef.current,
			s.right,
		);
	}, []);

	useEffect(() => {
		const pointerLayer = pointerLayerRef.current;
		if (!pointerLayer) return;

		/**
		 * Stores the latest pointer-layer width only when it changes.
		 * @param width Current remote-pointer viewport width in CSS pixels.
		 * @returns Nothing.
		 */
		const updateViewportWidth = (width: number): void => {
			viewportWidthRef.current = width;
			setViewportWidth((currentWidth) =>
				currentWidth === width ? currentWidth : width,
			);
			updateRemotePointerHorizontalState();
		};

		updateViewportWidth(pointerLayer.clientWidth || 1);
		const resizeObserver = new ResizeObserver((entries) => {
			const entry = entries[0];
			if (entry) updateViewportWidth(Math.max(1, entry.contentRect.width));
		});
		resizeObserver.observe(pointerLayer);
		return () => resizeObserver.disconnect();
	}, [updateRemotePointerHorizontalState]);

	useEffect(() => {
		/**
		 * Applies the latest vertical scroll without rerendering remote pointers.
		 * @returns Nothing.
		 */
		const updateVerticalScroll = (): void => {
			const verticalLayer = remoteVerticalLayerRef.current;
			if (verticalLayer) {
				applyRemotePointerScrollY(verticalLayer, scrollStore.getY());
			}
		};

		updateRemotePointerHorizontalState();
		updateVerticalScroll();
		const unsubscribeX = scrollStore.subscribeX(
			updateRemotePointerHorizontalState,
		);
		const unsubscribeY = scrollStore.subscribeY(updateVerticalScroll);
		return () => {
			unsubscribeX();
			unsubscribeY();
		};
	}, [updateRemotePointerHorizontalState]);
	const remotePointerPlacements = app.timeline.pointers.map((pointer) => ({
		pointer,
		horizontalPosition: resolveRemotePointerHorizontalPosition(
			pointer.x,
			pointer.timestamp,
			getPixelPosition,
			currentScrollX,
		),
	}));

	return (
		<Stack
			ref={pointerLayerRef}
			data-aria-pointers
			pos="absolute"
			className={s.pointers}
			{...props}
		>
			<LocalPointer
				selfStore={selfStore}
				themeKey={themeKey}
				formatLabel={formatLocalLabel}
			/>
			<Stack
				ref={remoteVerticalLayerRef}
				className={s.remoteVerticalLayer}
				pos="absolute"
				style={{
					"--remote-pointer-scroll-y": `${-scrollStore.getY()}px`,
				} as RemotePointerLayerStyle}
			>
				<Stack
					ref={remoteTimelineLayerRef}
					className={s.remoteTimelineLayer}
					pos="absolute"
					style={{
						"--remote-pointer-scroll-x": `${-currentScrollX}px`,
					} as RemotePointerLayerStyle}
				>
					{remotePointerPlacements.map(({ pointer, horizontalPosition }) => {
						if (!horizontalPosition.followsTimelineScroll) return null;

						return (
							<RemotePointer
								key={pointer.id}
								positionX={horizontalPosition.positionX}
								positionY={pointer.y}
								viewportX={horizontalPosition.viewportX}
								documentX={horizontalPosition.positionX}
								id={pointer.id}
								color={pointer.color}
								themeKey={themeKey}
								viewportWidth={viewportWidth}
							/>
						);
					})}
				</Stack>
				<Stack className={s.remoteFixedLayer} pos="absolute">
					{remotePointerPlacements.map(({ pointer, horizontalPosition }) => {
						if (horizontalPosition.followsTimelineScroll) return null;

						return (
							<RemotePointer
								key={pointer.id}
								positionX={horizontalPosition.positionX}
								positionY={pointer.y}
								viewportX={horizontalPosition.viewportX}
								id={pointer.id}
								color={pointer.color}
								themeKey={themeKey}
								viewportWidth={viewportWidth}
							/>
						);
					})}
				</Stack>
			</Stack>
		</Stack>
	);
}
