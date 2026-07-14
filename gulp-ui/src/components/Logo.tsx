import React from "react";
import { useTheme } from "next-themes";
import { getThemeDefinitions } from "@/themes/index";
import { Locale } from "@/locales";

export namespace Logo {
	export interface Props extends React.ImgHTMLAttributes<HTMLImageElement> {}
}

export function Logo({ width = 256, height = 256, ...props }: Logo.Props) {
	const { theme } = useTheme();
	const { t } = Locale.use();
	const themes = getThemeDefinitions();
	const activeTheme = themes.find((entry) => entry.name === theme) ?? themes[0];
	const activeMode = activeTheme?.mode ?? "dark";

	const logoSrc = activeMode === "light" ? "/logo_dark.svg" : "/logo.svg";

	return (
		<img
			src={logoSrc}
			width={width}
			height={height}
			alt={t("logo.alt")}
			{...props}
		/>
	);
}
