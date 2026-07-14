import { Default, Selectable } from '@/dto/Dataset'
type UUID = string
import { Operation } from './Operation'
import { User } from './User'
import { Glyph } from './Glyph'
import { MinMax, Arrayed } from '@/class/Info'
import { Parser } from './addon/Parser'
import { App } from './App'
import { Source } from './Source'
import { Banner as UIBanner } from '@/ui/Banner'
import { Application } from '@/context/Application.context'
import { Button } from '@/ui/Button'
import { Toggle } from '@/ui/Toggle'
import { useState } from 'react'
import { toast } from 'sonner'
import { Internal } from './addon/Internal'
import { Locale } from '@/locales'

export namespace Context {
  export const name = 'Context'
  const _ = Symbol(Context.name)
  export type Id = UUID & {
    readonly [_]: unique symbol
  }

  export interface Type extends Selectable {
    operation_id: Operation.Id,
    color: string,
    id: Context.Id,
    type: 'context',
    owner_user_id: User.Id,
    granted_user_ids: User.Id[],
    granted_user_group_ids: string[],
    time_created: number,
    time_updated: number,
    glyph_id: Glyph.Id,
    name: string
  }

  export class Entity {
    public static icon = Internal.IconExtractor.activate<Context.Type | null>(
      Default.Icon.CONTEXT,
    )

    public static reload = (newContexts: Context.Type[], app: App.Type): Context.Type[] =>
      Context.Entity.select(newContexts, Context.Entity.selected(app))

    public static frame = (app: App.Type): MinMax => {
      const files = Source.Entity.selected(app);

      return {
        min: Math.min(...files.map(file => file.timestamp.min)),
        max: Math.max(...files.map(file => file.timestamp.max)),
      }
    };

    public static selected = (use: App.Type | Context.Type[]): Context.Type[] =>
      Parser.use(use, 'contexts').filter(
        (c) =>
          c.selected &&
          ('target' in use
            ? Operation.Entity.selected(use)?.id === c.operation_id
            : true),
      )

    public static findByName = (app: App.Type, name: Context.Type['name']) =>
      Context.Entity.selected(app).find((c) => c.name === name)

    public static select = (
      use: App.Type | Context.Type[],
      selected: Arrayed<Context.Type | Context.Id>,
    ): Context.Type[] =>
      Parser.use(use, 'contexts').map((c) =>
        Parser.array(selected).find((s) => c.id === Parser.useUUID(s))
          ? Context.Entity._select(c)
          : c,
      )

    public static unselect = (
      use: App.Type | Context.Type[],
      unselected: Arrayed<Context.Type | Context.Id>,
    ): Context.Type[] =>
      Parser.use(use, 'contexts').map((c) =>
        Parser.array(unselected).find((s) => c.id === Parser.useUUID(s))
          ? Context.Entity._unselect(c)
          : c,
      )

    public static check = (
      use: App.Type | Context.Type[],
      selected: Arrayed<Context.Type | UUID>,
      check?: boolean,
    ): Context.Type[] =>
      Parser.use(use, 'contexts').map((c) =>
        Parser.array(selected).find((s) => c.id === Parser.useUUID(s))
          ? check
            ? Context.Entity._select(Context.Entity.id(use, c))
            : Context.Entity._unselect(Context.Entity.id(use, c))
          : c,
      )

    public static id = (
      use: App.Type | Context.Type[],
      context: Context.Type | Context.Id,
    ) =>
      Parser.use(use, 'contexts').find(
        (c) => c.id === Parser.useUUID(context),
      ) as Context.Type

    public static sources = (
      app: App.Type,
      context: Context.Type | Context.Id,
    ): Source.Type[] =>
      app.target.files.filter((p) => p.context_id === Parser.useUUID(context))

    private static _select = (c: Context.Type): Context.Type => ({ ...c, selected: true })

    private static _unselect = (c: Context.Type): Context.Type => ({
      ...c,
      selected: false,
    })
  }

  export namespace Delete {
    export namespace Banner {
      export interface Props extends UIBanner.Props {
        context: Context.Type
      }
    }
    export function Banner({ context, ...props }: Context.Delete.Banner.Props) {
      const { Info } = Application.use()
      const { t } = Locale.use()
      const [isSubmited, setIsSubmited] = useState<boolean>(false)
      const [isWipe, setIsWipe] = useState<boolean>(true)
      const [loading, setLoading] = useState<boolean>(false)

      const DeleteButton = () => (
        <Button
          loading={loading}
          icon='Trash2'
          variant='glass'
          onClick={deleteContext}
          disabled={!isSubmited}
        />
      )

      const deleteContext = async () => {
        setLoading(true)
        await Info.context_delete(context, isWipe)
        setLoading(false)
        if (props.back) {
          props.back()
        }
        toast(t('context.deleted', { name: context.name }))
      }

      return (
        <UIBanner title={t('context.deleteTitle')} done={<DeleteButton />} {...props}>
          <p>
            {t('context.deleteConfirm')} <code>{context.name}</code>
          </p>
          <Toggle
            option={[t('common.noDontDelete'), t('common.yesImSure')]}
            checked={isSubmited}
            onCheckedChange={setIsSubmited}
          />
          {isSubmited && (
	            <Toggle
	              option={[t('context.keepData'), t('context.deleteDataInside')]}
              checked={isWipe}
              onCheckedChange={setIsWipe}
            />
          )}
        </UIBanner>
      )
    }
  }
}
