import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Button } from "@/ui/Button";
import { Stack } from "@/ui/Stack";
import { Locale } from "@/locales";

export namespace Debug {
  export namespace Banner {
    export interface Props extends UIBanner.Props {

    }
  }
  export function Banner({ ...props }: Debug.Banner.Props) {
    const { Info } = Application.use();
    const { t } = Locale.use();

    return (
      <UIBanner title={t('debug.title')} {...props}>
        <Stack dir='column' gap={8}>
          <Button style={{ width: '100%' }} variant='secondary' onClick={Info.notes_reload} icon='StickyNote'>{t('debug.reloadNotes')}</Button>
          <Button style={{ width: '100%' }} variant='secondary' onClick={Info.links_reload} icon='GitBranch'>{t('debug.reloadLinks')}</Button>
          <Button style={{ width: '100%' }} variant='secondary' onClick={Info.highlights_reload} icon='AlignHorizontalSpaceAround'>{t('debug.reloadHighlights')}</Button>
          <Button style={{ width: '100%' }} variant='secondary' onClick={Info.plugin_list} icon='Puzzle'>{t('debug.reloadPlugins')}</Button>
        </Stack>
      </UIBanner>
    )
  }
}
