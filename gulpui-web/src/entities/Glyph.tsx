import {
	ChangeEvent,
	CSSProperties,
	useCallback,
	useMemo,
	useState,
	useRef,
} from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Icon } from "@/ui/Icon";
import { Popover } from "@/ui/Popover";
import { cn } from "@/ui/utils";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Stack } from "@/ui/Stack";
import { Locale } from "@/locales";
type UUID = string;

import s from "./styles/Glyph.module.css";

export namespace Glyph {
	const _ = Symbol("Glyph");
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export interface Type {
		id: Glyph.Id;
		img?: string;
		name: Icon.Name;
	}

	export interface Props extends Omit<Icon.Props, "name"> {
		glyph: Glyph.Id;
	}

	const builtInIcons = new Map(Object.entries(Icon.icons));
	export const Raw = Array.from(builtInIcons.keys()).sort() as Icon.Name[];

	const builtInEntries = () =>
		Raw.map((name) => [name as Glyph.Id, name] as [Glyph.Id, Icon.Name]);

	export const List: Map<Glyph.Id, Icon.Name> = new Map(builtInEntries());

	export const Images: Map<Icon.Name, string> = new Map();

	export const getIdByName = (name: Icon.Name): Glyph.Id =>
		Glyph.List.entries().find(([_, n]) => name === n)?.[0]!;

	const dataUrl = (img: string) => {
		const data = img.trim();
		if (data.startsWith("data:")) return data;
		if (data.startsWith("<svg") || data.startsWith("<?xml")) {
			return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(data)}`;
		}

		let decoded = "";
		try {
			decoded = atob(data.slice(0, 128)).trimStart().toLowerCase();
		} catch {
			decoded = "";
		}

		if (decoded.startsWith("<svg") || decoded.startsWith("<?xml"))
			return `data:image/svg+xml;base64,${data}`;
		if (data.startsWith("/9j/")) return `data:image/jpeg;base64,${data}`;
		if (data.startsWith("R0lGOD")) return `data:image/gif;base64,${data}`;
		return `data:image/png;base64,${data}`;
	};

	export const register = (glyph: Glyph.Type) => {
		const glyphId = glyph.name as Glyph.Id;
		Glyph.List.set(glyphId, glyph.name);

		if (!glyph.img) return;

		const src = dataUrl(glyph.img);
		Glyph.Images.set(glyph.name, src);
		(Icon.icons as Record<string, React.ComponentType<any>>)[glyph.name] = ({
			color,
			size,
			...props
		}) => (
			<svg
				width={size ?? 24}
				height={size ?? 24}
				viewBox="0 0 24 24"
				{...props}
			>
				<image
					href={src}
					width="24"
					height="24"
					preserveAspectRatio="xMidYMid meet"
				/>
			</svg>
		);
	};

	export const reset = () => {
		Object.keys(Icon.icons).forEach(
			(name) => delete (Icon.icons as Record<string, unknown>)[name],
		);
		builtInIcons.forEach((component, name) => {
			(Icon.icons as Record<string, React.ComponentType<any>>)[name] =
				component;
		});
		Glyph.Images.clear();
		Glyph.List.clear();
		builtInEntries().forEach(([id, name]) => Glyph.List.set(id, name));
	};

	export namespace Chooser {
		export interface Props {
			className?: string;
			rootClassName?: string;
			label?: string;
			style?: CSSProperties;
			asButton?: boolean;
			icon: Glyph.Id | null;
			setIcon: React.Dispatch<React.SetStateAction<Glyph.Id | null>>;
			container?: HTMLElement | null;
		}
	}

	export const Chooser = ({
		style,
		className,
		rootClassName,
		label,
		icon,
		setIcon,
		asButton,
		container,
	}: Chooser.Props) => {
		const { t } = Locale.use();
		const [search, setSearch] = useState<string>("");
		const parentRef = useRef<HTMLDivElement | null>(null);
		const entities = useMemo(() => {
			return Array.from(Glyph.List.entries()).filter((e) =>
				e[1].toLowerCase().includes(search.toLowerCase()),
			);
		}, [search]);

		const handleGlyphSearchInput = useCallback(
			(event: ChangeEvent<HTMLInputElement>) => {
				setSearch(event.target.value);
			},
			[setSearch],
		);

		const SearchInput = useMemo(() => {
			return (
				<Input
					variant="highlighted"
					icon="MagnifyingGlass"
					placeholder={t("glyph.searchPlaceholder")}
					value={search}
					onChange={handleGlyphSearchInput}
					name="glyph-search"
				/>
			);
		}, [search, handleGlyphSearchInput, t]);

		const rowVirtualizer = useVirtualizer({
			count: entities.length,
			getScrollElement: () => parentRef.current,
			estimateSize: () => 40,
			overscan: 10,
		});

		return (
			<Popover.Root
				onOpenChange={(open) => {
					if (open) {
						setTimeout(() => {
							rowVirtualizer.measure();
						}, 0);
					}
				}}
			>
				<Popover.Trigger asChild>
					{asButton ? (
						<Button
							className={rootClassName}
							icon={icon ? Glyph.List.get(icon) : "SquareDashed"}
							variant="secondary"
						/>
					) : (
						<Input
							variant="highlighted"
							className={cn(s.input, className)}
							style={style}
							icon={icon ? Glyph.List.get(icon) : "SquareDashed"}
							value={icon ? Glyph.List.get(icon) : t("glyph.chooseIcon")}
							label={label}
						/>
					)}
				</Popover.Trigger>
				<Popover.Content
					align="end"
					forceMount
					container={container}
				>
					<Stack
						dir="column"
						className={s.wrapper}
						ai="stretch"
					>
						{SearchInput}
						<div
							ref={parentRef}
							className={s.list}
							style={{ width: "300px" }}
						>
							<div
								style={{
									height: `${rowVirtualizer.getTotalSize()}px`,
									position: "relative",
									width: "100%",
								}}
							>
								{rowVirtualizer.getVirtualItems().map((virtualRow) => {
									const [k, n] = entities[virtualRow.index];
									if (!k) return null;

									return (
										<div
											key={n}
											style={{
												position: "absolute",
												top: 0,
												left: 0,
												width: "100%",
												transform: `translateY(${virtualRow.start}px)`,
											}}
										>
											<Button
												variant={k === icon ? "glass" : "tertiary"}
												icon={n}
												onClick={() => setIcon(k)}
												className={s.iconButton}
											>
												{n}
											</Button>
										</div>
									);
								})}
							</div>
						</div>
					</Stack>
				</Popover.Content>
			</Popover.Root>
		);
	};
}
