let didLoadThemes = false;

export type ThemeMode = 'light' | 'dark';

export interface ThemeDefinition {
    name: string;
    mode: ThemeMode;
}

declare const require: {
    context: (path: string, deep?: boolean, filter?: RegExp) => {
        keys: () => string[];
        <T = unknown>(id: string): T;
    };
};

const themeContext = () =>
    require.context('./', false, /^\.\/.+\.css$/);

function normalizeThemeMode(value: string | null): ThemeMode | null {
    if (value === 'light' || value === 'dark') {
        return value;
    }

    return null;
}

function readThemeDefinitionsFromStyleSheets(): ThemeDefinition[] {
    if (typeof document === 'undefined') {
        return [];
    }

    const themes = new Map<string, ThemeDefinition>();
    const themeSelectorPattern = /^:root\[data-theme=(['"]?)([^'"\]]+)\1\]$/;

    Array.from(document.styleSheets).forEach((styleSheet) => {
        try {
            Array.from(styleSheet.cssRules).forEach((rule) => {
                if (!(rule instanceof CSSStyleRule)) {
                    return;
                }

                const match = rule.selectorText.match(themeSelectorPattern);
                const name = match?.[2];
                const mode = normalizeThemeMode(rule.style.getPropertyValue('--theme-mode').trim());

                if (name && mode) {
                    themes.set(name, { name, mode });
                }
            });
        } catch {
            // Ignore stylesheets whose rules are not readable.
        }
    });

    return Array.from(themes.values()).sort((left, right) => left.name.localeCompare(right.name));
}

function readThemeNamesFromStyleSheets(): string[] {
    return readThemeDefinitionsFromStyleSheets().map((theme) => theme.name);
}

/** Returns theme names derived from loaded CSS data-theme selectors, sorted alphabetically. */
export function getThemeNames(): string[] {
    loadThemesFromDirectory();
    return readThemeNamesFromStyleSheets();
}

/** Returns theme names plus their declared light/dark mode from CSS variables. */
export function getThemeDefinitions(): ThemeDefinition[] {
    loadThemesFromDirectory();
    return readThemeDefinitionsFromStyleSheets();
}

/** Loads all theme CSS files from this directory once at runtime. */
export function loadThemesFromDirectory(): void {
    if (didLoadThemes) {
        return;
    }

    const context = themeContext();
    context.keys().forEach((file: string) => {
        context(file);
    });

    didLoadThemes = true;
}
