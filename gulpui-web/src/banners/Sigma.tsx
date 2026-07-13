import { Application } from '@/context/Application.context'
import { Extension } from '@/context/Extension.context'
import { Banner as UIBanner } from '@/ui/Banner'
import { ChangeEvent, useCallback, useState, useMemo } from 'react'
import { Source } from '@/entities/Source'
import { Checkbox } from '@/ui/Checkbox'
import { Icon } from '@/ui/Icon'
import { NodeFile } from '@/ui/utils'
import { Button } from '@/ui/Button'
import { Toggle } from '@/ui/Toggle'
import { Label } from '@/ui/Label'
import { Input } from '@/ui/Input'
import { Stack } from '@/ui/Stack'
import { toast } from 'sonner'
import { Locale } from '@/locales'

export namespace Sigma {
  export namespace Banner {
    export interface Props extends UIBanner.Props {
      sources: Source.Id[]
    }
  }

  export function Banner({
    sources: initialSources = [],
    ...props
  }: Sigma.Banner.Props) {
    const [createNotes, setCreateNotes] = useState<boolean>(false);
    const [sources, setSources] = useState(initialSources)
    const [rules, setRules] = useState<NodeFile[]>([]);
    const [isZip, setIsZip] = useState<boolean>(false);
    const { Info, destroyBanner } = Application.use();
    const { t } = Locale.use();
    const { extensions } = Extension.use();

    const [zipSubmit, setZipSubmit] = useState<{ run: () => Promise<void> }>();

    /**
     * Resolves the optional Sigma upload-mode plugin, such as ZIP upload support.
     */
    const sigmaUploadModePlugin = useMemo(
      () => Extension.getBySlot(extensions, Extension.Slot.SigmaUploadMode)[0] ?? null,
      [extensions],
    )

    const DoneButton = () => {
      const submit = async () => {
        if (isZip) {
          if (zipSubmit) {
            await zipSubmit.run();
          }
          return;
        }

        if (!rules.length) {
          return toast(t('sigma.selectRuleOrZip'))
        }

        await Info.query_sigma(sources, rules, createNotes);
        destroyBanner()
      }

      return (
        <Button
          icon="Check"
          onClick={submit}
          variant="glass"
        />
      )
    }

    const rulesInputChangeHandler = async (
      event: ChangeEvent<HTMLInputElement>,
    ) => {
      const { files } = event.target
      if (!files?.length) {
        toast.warning(t('sigma.noRulesSelected'), {
          richColors: true,
          icon: <Icon name='Warning' />
        });
        return;
      }

      const fileData: NodeFile[] = Array.from(files);
      setRules(fileData);
    }

    const onRegisterSubmit = useCallback((callback: () => Promise<void>) => setZipSubmit({ run: callback }), []);

    const sourcePlaceholder = useMemo(() => {
      if (isZip) {
        return t('sigma.selectSourcesForZip')
      }
      return t('sigma.selectSourcesForRules')
    }, [isZip, t])

    return (
      <UIBanner title={t('sigma.title')} done={<DoneButton />} {...props}>
        {sigmaUploadModePlugin ? (
          <Toggle option={[t('common.file'), t('common.zip')]} checked={isZip} onClick={() => setIsZip(!isZip)} />
        ) : null}

        <Source.Select.Multi selected={sources} setSelected={setSources} placeholder={sourcePlaceholder} />
        {isZip && sigmaUploadModePlugin ? (
          <Extension.Component
            name={sigmaUploadModePlugin.filename}
            props={{
              sources,
              createNotes,
              onRegisterSubmit
            }}
          />
        ) : (
          <Input type="file" icon="Sigma" accept='.yml' multiple variant="highlighted" onChange={rulesInputChangeHandler} />
        )}
        <Stack ai='center' gap={6}>
          <Checkbox id='isCreateNotes' checked={createNotes} onCheckedChange={v => setCreateNotes(!!v)} />
          <Label htmlFor='isCreateNotes' cursor='pointer' value={t('sigma.createNotes')} />
        </Stack>
      </UIBanner>
    )
  }
}
