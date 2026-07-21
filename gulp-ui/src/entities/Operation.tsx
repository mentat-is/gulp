import { Application } from '@/context/Application.context'
import { SelectFiles } from '@/banners/SelectFiles.banner'
import { Default, Selectable } from '@/dto/Dataset'
import { Banner as UIBanner } from '@/ui/Banner'
import { Select as UISelect } from '@/ui/Select'
import { useState, useEffect } from 'react'
import { Logger } from '@/dto/Logger.class'
import { Internal } from './addon/Internal'
import { Skeleton } from '@/ui/Skeleton'
import { Icon } from '@/ui/Icon'
import { Parser } from './addon/Parser'
import { Refractor } from '@/ui/utils'
import { Button } from '@/ui/Button'
import { Context } from './Context'
import { Input } from '@/ui/Input'
import { Glyph } from './Glyph'
import { User } from './User'
type UUID = string
import { App } from './App'
import { Stack } from '@/ui/Stack'
import { Label } from '@/ui/Label'
import { Textarea } from '@/ui/Textarea'

import s from './styles/Operation.module.css'
import { toast } from 'sonner'
import { Toggle } from '@/ui/Toggle'
import { Locale } from '@/locales'

export namespace Operation {
  export const name = 'Operation'
  const _ = Symbol(Operation.name)
  export type Id = UUID & {
    readonly [_]: unique symbol
  }

  export interface Type extends Selectable {
    id: Id;
    index: string;
    name: string;
    glyph_id: Glyph.Id;
    description?: string;
  }

  export class Entity {
    public static icon = Internal.IconExtractor.activate<Operation.Type | null>(
      Default.Icon.OPERATION,
    )

    public static reload = (newOperations: Operation.Type[], app: App.Type) => Operation.Entity.select(newOperations, Operation.Entity.selected(app)?.id)

    public static selected = (app: App.Type): Operation.Type | undefined =>
      Logger.assert(app.target.operations.find((o) => o.selected), 'No operation selected', Operation.name)

    public static id = (use: App.Type, id: Operation.Id): Operation.Type =>
      Parser.use(use, 'operations').find((o) => o.id === id) as Operation.Type

    public static findByName = (
      app: App.Type,
      name: Operation.Type['name'],
    ): Operation.Type | undefined =>
      app.target.operations.find((o) => o.name === name)

    public static select = (use: App.Type | Operation.Type[], operation: Operation.Id | undefined): Operation.Type[] => Refractor.array(...Parser.use(use, 'operations').map((o) => o.id === operation ? Operation.Entity._select(o) : Operation.Entity._unselect(o)));

    public static contexts = (app: App.Type, operationId?: Operation.Id): Context.Type[] =>
      app.target.contexts.filter(
        (c) => c.operation_id === (operationId || Operation.Entity.selected(app)?.id),
      )

    private static _select = (o: Operation.Type): Operation.Type => ({
      ...o,
      selected: true,
    })

    private static _unselect = (o: Operation.Type): Operation.Type => ({
      ...o,
      selected: false,
    })
  }

  export namespace Select {
    export namespace Banner {
      export interface Props extends UIBanner.Props { }
    }

    export function Banner({ ...props }: Operation.Select.Banner.Props) {
      const { Info, app, spawnBanner } = Application.use()
      const { t } = Locale.use()

      const InitializeNewOperaion = () => (
        <Button
          variant='tertiary'
          onClick={() =>
            spawnBanner(
              <Operation.CreateOrUpdate.Banner
                back={() => spawnBanner(<Operation.Select.Banner />)}
              />,
            )
          }
          icon='BookPlus'
        />
      )

      const DoneButton = () => (
        <Button
          disabled={!Operation.Entity.selected(Info.app)}
          onClick={() =>
            spawnBanner(
              <SelectFiles.Banner
                back={() => spawnBanner(<Operation.Select.Banner />)}
              />,
            )
          }
          shape='icon'
          variant='glass'
          icon='Check'
        />
      )

      const NoOperations = () => (
        <UISelect.Item value='X' disabled>{t('operationSelect.empty')}</UISelect.Item>
      )

      const SelectTrigger = () => {
        const selected = Operation.Entity.selected(Info.app)

        return (
          <UISelect.Trigger>
            <UISelect.Icon name={Operation.Entity.icon((selected || {}) as Operation.Type)} />
            {selected ? selected.name : t('operationSelect.placeholder')}
          </UISelect.Trigger>
        )
      }

      const [loading, setLoading] = useState<boolean>(Info.app.target.operations.length === 0);

      useEffect(() => {
        setTimeout(() => {
          setLoading(false);
        }, 500);
      }, [loading])

      return (
        <UIBanner
          title={t('operationSelect.title')}
          done={<DoneButton />}
          loading={loading}
          {...props}
        >
          {loading ? (
            <Skeleton width='full' height='default' />
          ) : (
            <UISelect.Root
              defaultValue={Operation.Entity.selected(Info.app)?.id}
              onValueChange={(id) => Info.operations_select(id as Operation.Id)}>
              <SelectTrigger />
              <UISelect.Content>
                {Info.app.target.operations.length ? (
                  Info.app.target.operations.map((operation) => (
                    <UISelect.Item key={operation.id} value={operation.id}>
                      <UISelect.Icon name={Operation.Entity.icon(operation)} />
                      {operation.name}
                    </UISelect.Item>
                  ))
                ) : (
                  <NoOperations />
                )}
              </UISelect.Content>
            </UISelect.Root>
          )}
        </UIBanner>
      )
    }
  }

  export namespace CreateOrUpdate {
    export namespace Banner {
      export interface Props extends UIBanner.Props {
        operation?: Operation.Type;
        onSuccess?: () => void;
      }
    }

    /**
     * Banner component that allows the user to either create a new operation
     * or update an existing operation's glyph and description.
     *
     * @param props - Component props.
     * @param props.operation - The operation to update (optional, triggers create mode if omitted).
     * @returns The React element for the create/update banner.
     */
    export function Banner({ operation = {} as Operation.Type, ...props }: Operation.CreateOrUpdate.Banner.Props) {
      const { Info, spawnBanner, destroyBanner } = Application.use();
      const { t } = Locale.use();
      const [name, setName] = useState<string>(operation.name ?? '');
      const [icon, setIcon] = useState<Glyph.Id | null>(operation.glyph_id ?? Glyph.getIdByName(Default.Icon.OPERATION));
      const [description, setDescription] = useState<string>(operation.description ?? '');
      const [loading, setLoading] = useState<boolean>(false);

      const createOperation = () => {
        api<Operation.Type>('/operation_create', {
          method: 'POST',
          setLoading,
          query: { name },
          body: { description },
          showErrorToast: false,
          toast: {
            onSuccess: () => toast.success(t('operationEdit.created'), {
              icon: <Icon name='Check' />,
              richColors: true,
            }),
            onError: response => toast.error(t('operationEdit.createFailed'), {
              description: t('common.reason', { reason: response.data.__error.msg }),
              icon: <Icon name='Check' />,
              richColors: true,
            })
          }
        }, operation => {
          updateOperation(operation).then(Info.sync);
        });
      }

      const updateOperation = (operationType?: Operation.Type) => api<any>('/operation_update', {
        method: 'PATCH',
        setLoading,
        query: {
          operation_id: operationType?.id ?? operation.id,
          glyph_id: icon,
        },
        body: { glyph_id: icon, description },
      }, Info.sync)
        .then(() => {
          const isNewOperation = !operation.id;
          Logger.log(`Operation ${operationType?.name ?? operation.name} has been successfully ${isNewOperation ? 'created' : 'updated'}`, 'Operation.CreateOrUpdate.Banner.updateOperation', {
            icon: <Icon name='Check' />,
            richColors: true
          })
          Info.setInfoByKey(
            Info.app.target.operations.map((op) =>
              op.id === (operationType?.id ?? operation.id)
                ? { ...op, glyph_id: icon!, description }
                : op
            ),
            "target",
            "operations"
          );
          destroyBanner();
          if (props.onSuccess) {
            props.onSuccess();
          }
        });

      const DoneButton = () => {
        const doneButtonClickHandler = () => {
          if (operation.id) {
            updateOperation();
          } else {
            createOperation();
          }
        };
        return (
          <Button
            variant='glass'
            disabled={!name}
            loading={loading}
            icon='Check'
            onClick={doneButtonClickHandler} />
        )
      }

      return (
        <UIBanner title={operation.id ? t('operationEdit.updateTitle') : t('operationEdit.createTitle')} done={<DoneButton />} className={s.wrapper} {...props}>
          <Input
            label={t('common.name')}
            value={name}
            variant='highlighted'
            icon='TextTitle'
            onChange={(e) => setName(e.currentTarget.value)}
            placeholder={t('operationEdit.namePlaceholder')}
            readOnly={!!operation.id}
            disabled={!!operation.id}
          />
          <Glyph.Chooser icon={icon} setIcon={setIcon} label={t('operationEdit.icon')} />
          <Stack dir='column' gap={6} ai='flex-start' data-input style={{ width: '100%' }}>
            <Label htmlFor="description" value={t('common.description')} />
            <Textarea
              id="description"
              value={description}
              onChange={(e) => setDescription(e.currentTarget.value)}
              placeholder={t('operationEdit.descriptionPlaceholder')}
              style={{ width: '100%' }}
            />
          </Stack>
        </UIBanner>
      )
    }
  }

  export namespace BulkDelete {
    export namespace Banner {
      export interface Props extends UIBanner.Props {
        operationIds: Operation.Id[];
        onDeleted: (deletedIds: Operation.Id[]) => void;
      }
    }

    /**
     * Banner component for confirming bulk deletion of selected operations.
     *
     * @param props - Component props.
     * @param props.operationIds - The list of operation IDs to delete.
     * @param props.onDeleted - Callback triggered when the operations have been successfully deleted.
     * @returns The React element for the confirmation banner.
     */
    export function Banner({
      operationIds,
      onDeleted,
      ...props
    }: Operation.BulkDelete.Banner.Props) {
      const { Info, app, destroyBanner } = Application.use();
      const { t } = Locale.use();
      const [loading, setLoading] = useState<boolean>(false);
      const [isSubmitted, setIsSubmitted] = useState<boolean>(false);

      /**
       * Confirms the deletion by resolving the full operation objects,
       * calling Info.deleteOperation, and invoking onDeleted callback.
       */
      const confirmDelete = async () => {
        const operationsToDelete = operationIds
          .map((id) => app.target.operations.find((op) => op.id === id))
          .filter((op): op is Operation.Type => !!op);

        const deletedIds = await Info.deleteOperation(
          operationsToDelete,
          setLoading,
        );
        if (deletedIds.length > 0) {
          onDeleted(deletedIds);
        }
        destroyBanner();
      };

      return (
        <UIBanner
          title={t('operationDelete.title')}
          done={
            <Button
              loading={loading}
              icon="Trash2"
              variant="secondary"
              onClick={confirmDelete}
              disabled={!isSubmitted}
            />
          }
          {...props}
        >
          <p>
            {t('operationDelete.confirm', { count: operationIds.length })}
          </p>
          <Toggle
            option={[t('common.noDontDelete'), t('common.yesImSure')]}
            checked={isSubmitted}
            onCheckedChange={setIsSubmitted}
          />
        </UIBanner>
      );
    }
  }
}
