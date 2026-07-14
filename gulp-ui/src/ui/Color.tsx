import {
	createContext,
	HTMLAttributes,
	useContext,
	useMemo,
	useState,
} from "react";
import { Popover } from "./Popover";
import { arrayToLinearGradientCSS } from "./utils";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./Tabs";
import s from "./styles/Color.module.css";
import { cn } from "@/ui/utils";
import { Button } from "./Button";
import { Stack } from "./Stack";
import { Input } from "./Input";
import { Color } from "@/entities/Color";
import { Locale } from "@/locales";

interface ColorProps extends HTMLAttributes<HTMLDivElement> {
	images?: string[];
	gradients?: Record<string, string[]>;
	solids?: string[];
	customColors?: string[];
	color?: string;
	setColor?: React.Dispatch<React.SetStateAction<string>>;
	container?: HTMLElement | null;
}

interface ColorPickerContext {
	color: string;
	setColor: React.Dispatch<React.SetStateAction<string>>;
}

const ColorContext =
	createContext<ColorPickerContext | undefined>(undefined) ||
	(() => {
		throw new Error(
			"ColorPickerTrigger and ColorPickerPopover must be inside <ColorPicker> element.",
		);
	})();

export const useColor = (): ColorPickerContext =>
	useContext(ColorContext) as ColorPickerContext;

type ColorPickerProps =
	| Button.Props
	| ((Button.Props & ColorPickerContext) & {
			default?: string;
	  });

export function ColorPicker(props: ColorPickerProps) {
	const [_color, _setColor] = useState<string>("#ffa647");
	const context: ColorPickerContext = {
		color: "color" in props ? props.color! : _color,
		setColor: "setColor" in props ? props.setColor : _setColor,
	};

	return (
		<ColorContext.Provider value={context}>
			<Popover.Root>{props.children}</Popover.Root>
		</ColorContext.Provider>
	);
}

type ColorPickerTriggerProps = Stack.Props;

export function ColorPickerTrigger({
	className,
	...props
}: ColorPickerTriggerProps) {
	const { color } = useColor();

	const isGradient = useMemo(
		() => Object.keys(Color.GRADIENT).includes(color),
		[color],
	);

	return (
		<Popover.Trigger asChild>
			<Stack
				pos="relative"
				gap={0}
				ai="flex-start"
				className={cn(s.picker, s.trigger, className)}
				{...props}
			>
				{isGradient ? (
					<div
						className={s.gradient_indicator}
						style={{
							background: arrayToLinearGradientCSS(
								Color.GRADIENT[color as never],
							),
						}}
					/>
				) : (
					<Input
						className={s.select}
						variant="color"
						value={color}
					/>
				)}
				<Input
					className={s.manual}
					variant="highlighted"
					icon="Dot"
					id="custom"
					value={color}
				/>
			</Stack>
		</Popover.Trigger>
	);
}

export type Tab = "solid" | "gradient";

const DEFAULT_SOLID_COLORS = [
	...Object.values(Color.GEIST).map(
		(s) => "#" + s.toString(16).padStart(6, "0"),
	),
	"#ff7a00",
	"#ffd60a",
	"#7c3aed",
	"#14b8a6",
	"#22c55e",
	"#0f172a",
	"#f8fafc",
];

export function ColorPickerPopover({
	color: _color,
	setColor: _setColor,
	gradients = {},
	solids = DEFAULT_SOLID_COLORS,
	customColors = [],
	container,
}: ColorProps) {
	const { t } = Locale.use();
	const { color: newColor, setColor: setNewColor } = useColor() || {};

	const color = _color ?? newColor;
	const setColor = _setColor ?? setNewColor;

	const [tab, setTab] = useState<Tab>(
		Object.keys(gradients).length ? "gradient" : "solid",
	);

	return (
		<Popover.Content
			className={s.popover}
			container={container}
		>
			<Tabs
				onValueChange={(v) => setTab(v as Tab)}
				defaultValue={tab}
				value={tab}
				className={s.tabs}
			>
				{!!solids.length && !!Object.keys(gradients).length && (
					<TabsList className={s.list}>
						<TabsTrigger
							className={s.trigger}
							value="solid"
						>
							{t("color.solid")}
						</TabsTrigger>
						<TabsTrigger
							className={s.trigger}
							value="gradient"
						>
							{t("color.gradient")}
						</TabsTrigger>
					</TabsList>
				)}

				{!!solids.length && (
					<TabsContent
						value="solid"
						className={s.content}
					>
						{solids.map((solid) => (
							<div
								key={solid}
								style={{ background: solid }}
								className={s.color}
								onClick={() => setColor(solid.toString())}
							/>
						))}
						{!!customColors.length && (
							<div className={s.custom_group}>
								<span className={s.custom_label}>{t("color.custom")}</span>
								<div className={s.custom_colors}>
									{customColors.map((customColor) => (
										<div
											key={customColor}
											style={{ background: customColor }}
											className={s.color}
											onClick={() => setColor(customColor.toString())}
										/>
									))}
								</div>
							</div>
						)}
					</TabsContent>
				)}

				{!!Object.keys(gradients).length && (
					<TabsContent
						value="gradient"
						className={s.content}
					>
						{Object.keys(gradients).map((key) => (
							<div
								key={key}
								style={{ background: arrayToLinearGradientCSS(gradients[key]) }}
								className={s.color}
								onClick={() => setColor(key)}
							/>
						))}
					</TabsContent>
				)}
			</Tabs>

			{tab !== "gradient" && (
				<Stack
					pos="relative"
					gap={0}
					ai="flex-start"
					className={s.picker}
				>
					<Input
						className={s.select}
						variant="color"
						value={color}
						onChange={(e) => setColor(e.currentTarget.value)}
					/>
					<Input
						className={s.manual}
						variant="highlighted"
						icon="Dot"
						id="custom"
						value={color}
						onChange={(e) => setColor(e.currentTarget.value)}
					/>
				</Stack>
			)}
		</Popover.Content>
	);
}
