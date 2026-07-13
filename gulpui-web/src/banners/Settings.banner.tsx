import { Application } from '@/context/Application.context'
import { Theme } from '@/context/Theme.context'
import { Internal } from '@/entities/addon/Internal'
import { Banner as UIBanner } from '@/ui/Banner'
import { Toggle } from '@/ui/Toggle'
import { Input } from '@/ui/Input'
import { Stack } from '@/ui/Stack'
import { Label } from '@/ui/Label'
import { Select } from '@/ui/Select'
import { Locale, localeList } from '@/locales'
import { useState } from 'react'

export namespace Settings {
  export type Section = 'timestamps' | 'scroll' | 'realtime' | 'theme' | 'language'

  export type Visibility = Partial<Record<Section, boolean>>

  export namespace Banner {
    export interface Props extends UIBanner.Props {
      /** Optional map controlling which settings sections are rendered. */
      visibility?: Visibility
    }
  }
  const defaultVisibility: Required<Visibility> = {
    timestamps: true,
    scroll: true,
    realtime: true,
    theme: true,
    language: true,
  }

  /**
   * Resolves the settings sections that should be visible in the banner.
   *
   * @param visibility - Optional per-section visibility overrides.
   * @returns A complete visibility map with defaults applied.
   */
  function resolveVisibility(visibility?: Visibility): Required<Visibility> {
    return {
      ...defaultVisibility,
      ...visibility,
    }
  }

  /**
   * Renders the user settings banner with optionally filtered sections.
   *
   * @param props - Banner props plus optional settings section visibility.
   * @returns A banner containing the requested settings controls.
   */
  export function Banner({ visibility, ...props }: Settings.Banner.Props) {
    const { app, Info } = Application.use()
    const { t, language, setLanguage } = Locale.use()
    const [isUTCTimestamps, setIsUTCTimestamps] = useState<boolean>(Internal.Settings.isUTCTimestamps);
    const resolvedVisibility = resolveVisibility(visibility)

    const isRealtime = !!app.settings.realtimeEnabled;
    const timeoff = app.settings.realtimeTimeoff ?? 30;
    const [timeoffInput, setTimeoffInput] = useState<string>(String(timeoff));

    const timeoffValue = parseInt(timeoffInput, 10);
    const isTimeoffValid = !isNaN(timeoffValue) && timeoffValue >= 10;

    return (
      <UIBanner title={t("settings.title")} {...props}>
        {resolvedVisibility.timestamps ? (
          <Toggle
            option={[t("settings.timestamps.local"), t("settings.timestamps.utc")]}
            checked={isUTCTimestamps}
            onCheckedChange={(v) => {
              setIsUTCTimestamps(v)
              Internal.Settings.isUTCTimestamps = v
            }}
          />
        ) : null}
        {resolvedVisibility.scroll ? (
          <Toggle
            option={[t("settings.scroll.normal"), t("settings.scroll.reverse")]}
            checked={app.timeline.isScrollReversed}
            onCheckedChange={Info.useReverseScroll}
          />
        ) : null}
        {resolvedVisibility.realtime ? (
          <>
            <Toggle
              option={[t("settings.realtime.off"), t("settings.realtime.on")]}
              checked={isRealtime}
              onCheckedChange={(v) => {
                Info.setRealtime(v, timeoffValue);
              }}
            />
            <Input
              label={t("settings.timeoff.label")}
              placeholder={t("settings.timeoff.placeholder")}
              type="number"
              min={10}
              value={timeoffInput}
              disabled={!isRealtime}
              valid={!isRealtime || isTimeoffValid}
              onChange={(e) => {
                const val = e.target.value;
                setTimeoffInput(val);
                const parsed = parseInt(val, 10);
                if (!isNaN(parsed) && parsed >= 10 && isRealtime) {
                  Info.setRealtime(true, parsed);
                }
              }}
            />
          </>
        ) : null}
        {resolvedVisibility.theme ? (
          <Stack jc='space-between' ai='center'>
            <Label value={t("settings.theme")} />
            <Theme.Selector />
          </Stack>
        ) : null}
        {resolvedVisibility.language ? (
          <Stack jc='space-between' ai='center'>
            <Label value={t("settings.language")} />
            <Select.Root value={language} onValueChange={setLanguage}>
              <Select.Trigger data-no-icon>
                <Select.Value />
              </Select.Trigger>
              <Select.Content>
                {localeList.map(({ code, label }) => (
                  <Select.Item key={code} value={code}>{label}</Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
          </Stack>
        ) : null}
      </UIBanner>
    )
  }
}
