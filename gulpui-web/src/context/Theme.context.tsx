import { Color } from '@/entities/Color';
import { Switch } from '@/ui/Switch';
import { Select } from '@/ui/Select';
import { loadThemesFromDirectory, getThemeDefinitions, ThemeDefinition, ThemeMode } from '@/themes/index';
import { SwitchProps } from '@radix-ui/react-switch';
import { ThemeProvider, useTheme } from 'next-themes'
import { useCallback, useEffect, useState } from 'react'

const DEFAULT_THEME_NAME = 'classic-dark';

function getAvailableThemes(): ThemeDefinition[] {
  return getThemeDefinitions();
}

function getFallbackMode(): ThemeMode {
  if (typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: light)').matches) {
    return 'light';
  }

  return 'dark';
}

function getDefaultTheme(themes: ThemeDefinition[]): string | undefined {
  if (!themes.length) {
    return undefined;
  }

  const defaultTheme = themes.find((theme) => theme.name === DEFAULT_THEME_NAME);
  if (defaultTheme) {
    return defaultTheme.name;
  }

  const preferredMode = getFallbackMode();
  return themes.find((theme) => theme.mode === preferredMode)?.name ?? themes[0]?.name;
}

function rememberThemeSelection(name: string, mode: ThemeMode) {
  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(`gulp-theme-${mode}`, name);
}

function readRememberedTheme(mode: ThemeMode): string | null {
  if (typeof window === 'undefined') {
    return null;
  }

  return window.localStorage.getItem(`gulp-theme-${mode}`);
}

/** Syncs Color.Themer with the active next-themes value on mount and changes. */
function ThemeInitializer() {
  const { theme } = useTheme();
  const [themes, setThemes] = useState<ThemeDefinition[]>(() => getAvailableThemes());

  useEffect(() => {
    loadThemesFromDirectory();
    setThemes(getAvailableThemes());
  }, []);

  useEffect(() => {
    const activeTheme = themes.find((entry) => entry.name === theme);
    Color.Themer.setTheme(activeTheme?.mode ?? getFallbackMode());
  }, [theme, themes]);

  return null;
}

function _({ children }: any) {
  const [themes, setThemes] = useState<ThemeDefinition[]>(() => getAvailableThemes());

  useEffect(() => {
    loadThemesFromDirectory();
    setThemes(getAvailableThemes());
  }, [])

  const themeNames = themes.map((entry) => entry.name);
  const defaultTheme = getDefaultTheme(themes);

  return (
    <ThemeProvider attribute="data-theme" defaultTheme={defaultTheme} themes={themeNames}>
      <ThemeInitializer />
      {children}
    </ThemeProvider>
  )
}

export namespace Theme {
  export function Switcher({ ...props }: Theme.Switcher.Props) {
    const { theme, setTheme } = useTheme();
    const themes = getAvailableThemes();
    const activeTheme = themes.find((entry) => entry.name === theme) ?? themes[0];
    const activeMode = activeTheme?.mode ?? getFallbackMode();
    const nextMode: ThemeMode = activeMode === 'dark' ? 'light' : 'dark';
    const rememberedTheme = readRememberedTheme(nextMode);
    const nextTheme = themes.find((entry) => entry.name === rememberedTheme && entry.mode === nextMode)
      ?? themes.find((entry) => entry.mode === nextMode)
      ?? activeTheme;

    const themeSwitchHandler = useCallback(() => {
      if (!nextTheme) {
        return;
      }

      rememberThemeSelection(nextTheme.name, nextTheme.mode);
      setTheme(nextTheme.name);
      Color.Themer.setTheme(nextTheme.mode);
    }, [nextTheme, setTheme]);

    return (
      <Switch onCheckedChange={themeSwitchHandler} checked={activeMode === 'dark'} icons={['Sun', 'Moon']} {...props} />
    )
  }

  export namespace Switcher {
    export interface Props extends SwitchProps { };
  }

  export function Selector() {
    const { theme, setTheme } = useTheme();
    const themes = getAvailableThemes();
    const defaultTheme = getDefaultTheme(themes);

    const handleThemeChange = useCallback((value: string) => {
      const selectedTheme = themes.find((entry) => entry.name === value);

      if (!selectedTheme) {
        return;
      }

      rememberThemeSelection(selectedTheme.name, selectedTheme.mode);
      setTheme(value);
      Color.Themer.setTheme(selectedTheme.mode);
    }, [setTheme, themes]);

    return (
      <Select.Root value={theme ?? defaultTheme} onValueChange={handleThemeChange}>
        <Select.Trigger data-no-icon>
          <Select.Value />
        </Select.Trigger>
        <Select.Content>
          {themes.map(({ name }) => (
            <Select.Item key={name} value={name}>{name}</Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
    );
  }

  export const Provider = _;
}
