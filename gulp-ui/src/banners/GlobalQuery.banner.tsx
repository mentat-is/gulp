import { Application } from '@/context/Application.context';
import { Banner as UIBanner } from '@/ui/Banner';
import { ChangeEvent, useCallback, useMemo, useState } from 'react';
import { Separator } from '@/ui/Separator';
import { Preview } from './Preview.banner';
import { Notification } from '@/ui/Notification';
import { Checkbox } from '@/ui/Checkbox';
import { Label } from '@/ui/Label';
import { OpenSearchQueryBuilder } from '@/components/QueryBuilder';
import { Button } from '@/ui/Button';
import { Input } from '@/ui/Input';
import { Stack } from '@/ui/Stack';
import { Query } from '@/entities/Query';
import { Filter } from '@/entities/Filter';
import { Doc } from '@/entities/Doc';
import { Locale } from '@/locales';

export namespace GlobalQuery {
  export namespace Banner {
    export interface Props extends UIBanner.Props {
      query?: Query.Type;
    }
  }

  export function Banner({ query: initQuery, ...props }: GlobalQuery.Banner.Props) {
    const { Info, spawnBanner, destroyBanner } = Application.use();
    const { t } = Locale.use();
    const [query, setQuery] = useState<Query.Type>(initQuery ?? {
      string: '',
      filters: []
    });
    const [separately, setSeparately] = useState(false);

    const resetQueryButtonClickHandler = () => {
      setQuery({
        string: '',
        filters: []
      })
    }

    const normalizedQuery = useMemo(
      () => Filter.Entity.query(query) as unknown as Query.Type,
      [query]
    );

    const QueryStringBuilder = useMemo(() => {
      return (
        <OpenSearchQueryBuilder.Query.String textFilter={query.text_filter || ''} setTextFilter={text_filter => setQuery(q => ({ ...q, text_filter }))} />
      )
    }, [query.text_filter, setQuery]);

    const setFilters = useCallback((action: Filter.Item[] | ((prev: Filter.Item[]) => Filter.Item[])) => {
      setQuery(q => ({
        ...q,
        filters: typeof action === 'function' ? action(q.filters) : action
      }))
    }, [setQuery]);

    const QueryFilterBuilder = useMemo(() => {
      return (
        <OpenSearchQueryBuilder.Query.Filters filters={query.filters} setFilters={setFilters} keys={[]} />
      )
    }, [query, setFilters]);

    const AddFilter = useMemo(() => {
      return (
        <OpenSearchQueryBuilder.Query.Add setFilters={setFilters} filters={query.filters} />
      )
    }, [query, setFilters]);

    const [isQueryLoading, setIsQueryLoading] = useState<boolean>(false);
    const doneButtonClickHandler = async () => {
      setIsQueryLoading(true);

      const { total_hits: total } = await Info.query_file(normalizedQuery, {
        preview: true
      });

      await Info.query_global({
        filename,
        context,
        separately,
        query: normalizedQuery,
        total
      });

      setIsQueryLoading(false);
      destroyBanner();
    }

    const [isPreviewLoading, setIsPreviewLoading] = useState<boolean>(false);
    const tabularPreviewButtonClickHandler = async () => {
      setIsPreviewLoading(true);
      const { docs, total_hits } = await Info.query_file(normalizedQuery, {
        preview: true
      });
      setIsPreviewLoading(false);

      spawnBanner(<Preview.Banner total={total_hits} values={docs} back={() => spawnBanner(<GlobalQuery.Banner query={query} {...props} />)} />)
    }

    const DoneButton = () => <Button onClick={doneButtonClickHandler} icon='Check' loading={isQueryLoading} disabled={separately ? filename.length < 3 || context.length < 3 || !isFilenameValid || !isContextValid : false} variant='glass' />

    const [filename, setFilename] = useState<string>('');
    const [isFilenameValid, setIsFilenameValid] = useState<boolean>(true);
    const [context, setContext] = useState<string>('');
    const [isContextValid, setIsContextValid] = useState<boolean>(true);

    const FileDefiner = useMemo(() => {
      if (!separately) {
        return null;
      }

      const filenameInputChangeHandler = (event: ChangeEvent<HTMLInputElement>) => {
        const { value } = event.target;

        setIsFilenameValid(value.length >= 3 && !Info.app.target.files.some(file => file.name === value));

        setFilename(value);
      };

      const contextInputChangeHandler = (event: ChangeEvent<HTMLInputElement>) => {
        const { value } = event.target;

        setIsContextValid(value.length >= 3 && !Info.app.target.contexts.some(context => context.name === value));

        setContext(value);
      };

      return (
        <>
          <Input value={filename} valid={isFilenameValid} onChange={filenameInputChangeHandler} variant='highlighted' icon='TextTitle' placeholder={t('globalQuery.sourceNamePlaceholder')} />
          <Input value={context} valid={isContextValid} onChange={contextInputChangeHandler} variant='highlighted' icon='TextTitle' placeholder={t('globalQuery.contextNamePlaceholder')} />
        </>
      )

    }, [separately, filename, setFilename, isFilenameValid, setIsFilenameValid, context, setContext, isContextValid, setIsContextValid, t]);

    return (
      <UIBanner done={<DoneButton />} title={t('globalQuery.title')} side={<OpenSearchQueryBuilder.Preview query={normalizedQuery} />} {...props}>
        {QueryStringBuilder}
        {AddFilter}
        <Separator />
        {QueryFilterBuilder}
        <Stack ai='stretch' style={{ width: '100%' }}>
          <Button style={{ flex: 1 }} variant='secondary' icon='Undo2' onClick={resetQueryButtonClickHandler}>{t('globalQuery.resetQuery')}</Button>
          <Button style={{ flex: 1 }} variant='secondary' icon='Table' loading={isPreviewLoading} onClick={tabularPreviewButtonClickHandler}>{t('globalQuery.tabularPreview')}</Button>
        </Stack>
        <Stack ai='center' gap={4}>
          <Checkbox id='isNewLine' checked={separately} onCheckedChange={v => setSeparately(!!v)} />
          <Label htmlFor='isNewLine' value={t('globalQuery.saveAsNewFile')} />
        </Stack>
        {FileDefiner}
        <Notification variant='error' icon='Warning'>
          <p><span>{t('common.warning')}</span>: {t('globalQuery.performanceWarning')}</p>
        </Notification>
      </UIBanner>
    )
  }

  export namespace Apply {
    export interface Props extends UIBanner.Props {
      query: Query.Type;
      total: number;
      docs: Doc.Minified[];
      separately: boolean;
    }
  }
}
