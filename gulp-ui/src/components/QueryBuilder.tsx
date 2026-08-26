import { Toggle } from "@/ui/Toggle"
import { copy, fws } from "@/ui/utils"
import { Icon } from "@/ui/Icon"
import { cn } from "@/ui/utils"
import { Select } from "@/ui/Select"
import { Switch } from "@/ui/Switch"
import * as highlight from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { useMemo, useCallback, ChangeEvent, KeyboardEvent, PointerEvent as ReactPointerEvent, memo, useState, useEffect, useRef } from "react"
import SyntaxHighlighter from "react-syntax-highlighter"
import s from './styles/QueryBuilder.module.css';
import { Stack } from "@/ui/Stack"
import { Button } from "@/ui/Button"
import { Input } from "@/ui/Input"
import { Filter } from "@/entities/Filter"
import { Label } from "@/ui/Label"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/ui/Tooltip"
import { Locale } from "@/locales"

export namespace OpenSearchQueryBuilder {
  export type Condition = Filter.Condition

  export type Operator = Filter.BoolOperator

  interface Object<T> {
    value: T
    labelKey: string
    icon: Icon.Name
  }

  export namespace Entity {
    export type Condition = Object<OpenSearchQueryBuilder.Condition>

    export type Operator = Object<OpenSearchQueryBuilder.Operator>
  }

  export const CONDITIONS: Entity.Condition[] = [
    { value: 'regexp', labelKey: 'queryBuilder.condition.regexp', icon: 'Asterisk' },
    { value: 'wildcard', labelKey: 'queryBuilder.condition.wildcard', icon: 'Dices' },
    { value: 'range', labelKey: 'queryBuilder.condition.range', icon: 'CalendarRange' },
    { value: 'LTE', labelKey: 'queryBuilder.condition.lte', icon: 'ArrowDown' },
    { value: 'GTE', labelKey: 'queryBuilder.condition.gte', icon: 'ArrowUp' },
    { value: 'eq', labelKey: 'queryBuilder.condition.eq', icon: 'Check' }
  ]

  export const OPERATORS: Entity.Operator[] = [
    { value: 'must', labelKey: 'queryBuilder.operator.must', icon: 'Ampersand' },
    { value: 'should', labelKey: 'queryBuilder.operator.should', icon: 'SlashForward' },
    { value: 'must_not', labelKey: 'queryBuilder.operator.mustNot', icon: 'CircleSlash2' },
    { value: 'filter', labelKey: 'queryBuilder.operator.filter', icon: 'Filter' },
  ]

  export const INITIAL = {
    bool: {
      must: [],
      should: [],
      must_not: [],
      filter: [],
    },
  }

  export namespace Preview {
    export interface Props extends Stack.Props {
      query: Record<string, any>;
    }
  }

  const JsonPreview = memo(({ value }: { value: string }) => (
    <SyntaxHighlighter language='JSON' style={highlight.vs2015} customStyle={{ background: 'none', height: '100%' }}>
      {value}
    </SyntaxHighlighter>
  ));

  export const Preview = memo(({ query, className, ...props }: Preview.Props) => {
    const [debouncedString, setDebouncedString] = useState(() => JSON.stringify(query, null, 2));

    useEffect(() => {
      const handler = setTimeout(() => {
        setDebouncedString(JSON.stringify(query, null, 2));
      }, 800); // Higher debounce for the heavy highlighter
      return () => clearTimeout(handler);
    }, [query]);

    const copyQueryButtonClickHandler = useCallback(() => copy(debouncedString), [debouncedString]);

    return (
      <Stack pos='relative' className={cn(s.preview, className)} {...props}>
        <Button className={s.copy} icon='Copy' onClick={copyQueryButtonClickHandler} variant='glass' />
        <JsonPreview value={debouncedString} />
      </Stack>
    )
  });

  export namespace Query {
    export namespace String {
      export interface Props extends Stack.Props {
        textFilter: string;
        setTextFilter: (text: string) => void;
        reset?: () => void;
      }
    }

    export const String = ({ textFilter, setTextFilter, reset, ...props }: Query.String.Props) => {
      const { t } = Locale.use();
      const queryStringInputChangeHandler = (event: ChangeEvent<HTMLInputElement>) => setTextFilter(event.target.value);

      const resetQueryStringButtonClickHandler = () => setTextFilter('');

      const copyQueryStringButtonClickHandler = () => copy(textFilter);

      return (
        <Stack dir='column' gap={6} style={fws} ai='stretch' {...props}>
          <Label value={t('queryBuilder.searchEventOriginal')} />
          <Stack>
            <Input
              style={{ flex: 1 }}
              variant='highlighted'
              icon='Code'
              className={s.query_string_input}
              placeholder={t('queryBuilder.searchRawEvent')}
              value={textFilter}
              onChange={queryStringInputChangeHandler}
            />
            <Button icon='Copy' variant='secondary' onClick={copyQueryStringButtonClickHandler} />
            <Button icon='Undo2' variant='secondary' onClick={reset ? reset : resetQueryStringButtonClickHandler} revert>
              {t('common.reset')}
            </Button>
          </Stack>
        </Stack>
      )
    }

    /** Shared type for the setFilters callback accepted by all builder components. */
    export type SetFilters = (action: Filter.Item[] | ((prev: Filter.Item[]) => Filter.Item[])) => void;

    export namespace Add {
      export interface Props extends Stack.Props {
        filters: Filter.Item[];
        setFilters: SetFilters;
        container?: HTMLDivElement | null;
      }
    }

    export const Add = ({ filters, setFilters, container, ...props }: Query.Add.Props) => {
      const { t } = Locale.use();

      /** Adds a leaf filter using the centralized domain defaults. */
      const addCondition = useCallback(() => {
        const filter = Filter.Entity.createCondition(
          `condition-${Date.now()}` as Filter.Id,
        );
        setFilters([...filters, filter]);
      }, [filters, setFilters]);

      /** Adds an empty nested filter group. */
      const addGroup = useCallback(() => {
        const group: Filter.Group = {
          id: `group-${Date.now()}` as Filter.Id,
          type: 'group',
          operator: 'must',
          children: [],
          enabled: true,
        };
        setFilters([...filters, group]);
      }, [filters, setFilters]);

      return (
        <Stack jc='space-between' {...props}>
          <p>{t('queryBuilder.conditions')}</p>
          <Stack>
            <Button onClick={addCondition} variant='secondary' icon='Plus'>
              {t('queryBuilder.addCondition')}
            </Button>
            <Button onClick={addGroup} variant='secondary' icon='FolderPlus'>
              {t('queryBuilder.addGroup')}
            </Button>
          </Stack>
        </Stack>
      )
    }

    export namespace Filters {
      export interface Props extends Stack.Props {
        filters: Filter.Item[];
        setFilters: SetFilters;
        options: Filter.Options;
        container?: HTMLDivElement | null;
      }
    }

    /** Renders one field-aware leaf condition without owning domain policy. */
    const FilterRow = memo(({
      filter,
      setFilters,
      keys,
      options,
      container,
    }: {
      filter: Filter.Type;
      setFilters: SetFilters;
      keys: string[];
      options: Filter.Options;
      container?: HTMLDivElement | null;
    }) => {
      const [isOpen, setIsOpen] = useState(false);
      const [fieldSearch, setFieldSearch] = useState('');
      const { t } = Locale.use();
      const fieldSearchInputRef = useRef<HTMLInputElement>(null);

      useEffect(() => {
        if (!isOpen) return;

        const focusTimeout = globalThis.setTimeout(() => {
          fieldSearchInputRef.current?.focus({ preventScroll: true });
        }, 0);

        return () => globalThis.clearTimeout(focusTimeout);
      }, [fieldSearch, isOpen]);

      /** Updates one property on the current leaf filter. */
      const update = useCallback(<Key extends keyof Filter.Type>(
        key: Key,
        value: Filter.Type[Key],
      ) => {
        setFilters((prev) => prev.map((condition) =>
          condition.id === filter.id ? { ...condition, [key]: value } : condition
        ) as Filter.Item[]);
      }, [filter.id, setFilters]);

      /** Removes the current leaf filter from its containing list. */
      const remove = useCallback(() => {
        setFilters((prev) => prev.filter((condition) => condition.id !== filter.id));
      }, [filter.id, setFilters]);

      const filteredKeys = useMemo(() => {
        if (!isOpen) return [];
        const search = fieldSearch.trim().toLowerCase();
        if (!search) return keys.slice(0, 100);

        const matches: string[] = [];
        for (const key of keys) {
          if (key.toLowerCase().includes(search)) matches.push(key);
          if (matches.length === 100) break;
        }
        return matches;
      }, [fieldSearch, isOpen, keys]);

      /**
       * Assigns a field after direct editing or an explicit dropdown selection.
       * @param field Field name chosen by the user.
       * @returns Nothing; persists the field value into the filter state.
       */
      const handleFieldSelection = useCallback((field: string): void => {
        setFilters((prev) => prev.map((condition) =>
          condition.id === filter.id && !Filter.isGroup(condition)
            ? Filter.Entity.assignField(condition, field, options)
            : condition
        ));
      }, [filter.id, options, setFilters]);

      /**
       * Tracks dropdown visibility and clears its search-only value before each opening.
       * @param open Whether the field selector is opening.
       * @returns Nothing; updates local presentation state only.
       */
      const handleFieldMenuOpenChange = useCallback((open: boolean): void => {
        setIsOpen(open);
        if (open) setFieldSearch('');
      }, []);

      /**
       * Keeps field-search keystrokes inside the input instead of triggering Select navigation.
       * @param event Keyboard event emitted by the search input rendered in the field dropdown.
       * @returns Nothing; only stops propagation to the parent select menu.
       */
      const handleFieldSearchKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
        event.stopPropagation();
      };

      /**
       * Prevents Radix Select hover handling from moving focus away from the search input.
       * @param event Pointer movement over a field option.
       * @returns Nothing; marks the hover event as handled.
       */
      const handleFieldItemPointerMove = useCallback(
        (event: ReactPointerEvent<HTMLDivElement>): void => {
          event.preventDefault();
        },
        [],
      );

      const allowedConditions = useMemo(
        () => new Set(Filter.Entity.getAllowedConditions(options[filter.field])),
        [filter.field, options],
      );

      return (
        <Stack
          dir='column'
          ai='stretch'
          className={cn(s.card, !filter.enabled && s.disabled)}
        >
          <Stack style={fws}>
            <Select.Root
              value={filter.operator}
              onValueChange={(value) => update('operator', value as Filter.BoolOperator)}
            >
              <Select.Trigger className={s.value}>
                <Select.Value placeholder={t('queryBuilder.operatorPlaceholder')} />
              </Select.Trigger>
              <Select.Content container={container}>
                {OpenSearchQueryBuilder.OPERATORS.map((op) => (
                  <Select.Item key={op.value} value={op.value}>
                    <Icon name={op.icon} />
                    {t(op.labelKey)}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
            <Select.Root
              value={filter.type}
              onValueChange={(value) => update('type', value as Filter.Condition)}
            >
              <Select.Trigger className={s.value}>
                <Select.Value placeholder={t('queryBuilder.typePlaceholder')} />
              </Select.Trigger>
              <Select.Content container={container}>
                {OpenSearchQueryBuilder.CONDITIONS.map((type) => (
                  <Select.Item
                    key={type.value}
                    value={type.value}
                    disabled={!allowedConditions.has(type.value)}
                  >
                    <Icon name={type.icon} />
                    {t(type.labelKey)}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
            <Switch checked={filter.enabled} onCheckedChange={value => update('enabled', value)} />
            <Button
              variant='tertiary'
              onClick={remove}
              icon='Trash2'
            />
          </Stack>
          <Stack style={fws}>
            <Stack pos='relative' flex={1}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Input
                    className={s.key_input}
                    icon='Dot'
                    variant='highlighted'
                    placeholder={t('queryBuilder.fieldName')}
                    value={filter.field}
                    onChange={(e) => handleFieldSelection(e.target.value)}
                  />
                </TooltipTrigger>
                <TooltipContent>{filter.field}</TooltipContent>
              </Tooltip>
              <Select.Root
                value={filter.field}
                onValueChange={handleFieldSelection}
                onOpenChange={handleFieldMenuOpenChange}
              >
                <Select.Trigger
                  aria-label={t('queryBuilder.fieldName')}
                  className={s.trigger}
                />
                <Select.Content style={{ minHeight: 60 }} container={container}>
                  <Input
                    ref={fieldSearchInputRef}
                    value={fieldSearch}
                    icon='MagnifyingGlass'
                    variant='highlighted'
                    placeholder={t('common.search')}
                    autoComplete='off'
                    spellCheck={false}
                    onChange={(event) => setFieldSearch(event.target.value)}
                    onKeyDown={handleFieldSearchKeyDown}
                  />
                  {isOpen && filteredKeys.map((k) => (
                    <Select.Item
                      key={k}
                      value={k}
                      onPointerMove={handleFieldItemPointerMove}
                    >
                      {k}
                    </Select.Item>
                  ))}
                </Select.Content>
              </Select.Root>
            </Stack>
            <Stack flex={1}>
              {filter.type === 'LTE' ? (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Input
                      variant='highlighted'
                      icon='ChevronRightSmall'
                      placeholder={t('queryBuilder.minValue')}
                      value={filter.value}
                      onChange={(e) => update('value', e.target.value)}
                      prefix='<='
                    />
                  </TooltipTrigger>
                  <TooltipContent>{filter.value}</TooltipContent>
                </Tooltip>
              ) : filter.type === 'GTE' ? (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Input
                      variant='highlighted'
                      icon='ChevronRightSmall'
                      placeholder={t('queryBuilder.maxValue')}
                      value={filter.value}
                      onChange={(e) => update('value', e.target.value)}
                      prefix='>='
                    />
                  </TooltipTrigger>
                  <TooltipContent>{filter.value}</TooltipContent>
                </Tooltip>
              ) : filter.type === 'range' ? (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Input
                      variant='highlighted'
                      icon='ChevronRightSmall'
                      placeholder={t('queryBuilder.rangeValue')}
                      value={filter.value}
                      onChange={(e) => update('value', e.target.value)}
                    />
                  </TooltipTrigger>
                  <TooltipContent>{filter.value}</TooltipContent>
                </Tooltip>
              ) : (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Input
                      variant='highlighted'
                      icon='ChevronRightSmall'
                      placeholder={t('common.value')}
                      value={filter.value}
                      onChange={(e) => update('value', e.target.value)}
                    />
                  </TooltipTrigger>
                  <TooltipContent>{filter.value}</TooltipContent>
                </Tooltip>
              )}
            </Stack>
          </Stack>
          {filter.type === 'wildcard' ? <Toggle option={[t('queryBuilder.caseSensitive'), t('queryBuilder.caseInsensitive')]} checked={filter.case_insensitive} onCheckedChange={v => update('case_insensitive', v)} /> : null}
        </Stack>
      );
    });

    /**
     * Renders a nested group of conditions — effectively a parenthesis around its children.
     *
     * The group's `operator` controls where the whole group lands in its parent's bool.
     * Each child carries its own `operator`, so a single group can hold a free mix of
     * must / should / must_not / filter children.
     */
    const GroupRow = memo(({
      group,
      setFilters,
      keys,
      options,
      container,
    }: {
      group: Filter.Group;
      setFilters: SetFilters;
      keys: string[];
      options: Filter.Options;
      container?: HTMLDivElement | null;
    }) => {
      const { t } = Locale.use();

      /** Updates one property on the current filter group. */
      const updateGroup = useCallback(<Key extends keyof Filter.Group>(
        key: Key,
        value: Filter.Group[Key],
      ) => {
        setFilters(prev =>
          prev.map(item => item.id === group.id ? { ...item, [key]: value } : item) as Filter.Item[]
        );
      }, [group.id, setFilters]);

      /** Removes the current group from its containing filter list. */
      const removeGroup = useCallback(() => {
        setFilters(prev => prev.filter(item => item.id !== group.id));
      }, [group.id, setFilters]);

      /** Applies a filter-state update to this group's child list. */
      const setChildFilters: SetFilters = useCallback(
        (action) => {
          setFilters(prev =>
            prev.map(item =>
              item.id === group.id
                ? {
                  ...item,
                  children: typeof action === 'function'
                    ? action((item as Filter.Group).children)
                    : action,
                } as Filter.Group
                : item
            )
          );
        },
        [group.id, setFilters]
      );

      /** Adds a child condition using the centralized domain defaults. */
      const addChildCondition = useCallback(() => {
        const filter = Filter.Entity.createCondition(
          `condition-${Date.now()}` as Filter.Id,
        );
        setChildFilters(prev => [...prev, filter]);
      }, [setChildFilters]);

      /** Adds an empty group below the current group. */
      const addChildGroup = useCallback(() => {
        const child: Filter.Group = {
          id: `group-${Date.now()}` as Filter.Id,
          type: 'group',
          operator: 'must',
          children: [],
          enabled: true,
        };
        setChildFilters(prev => [...prev, child]);
      }, [setChildFilters]);

      return (
        <Stack dir='column' ai='stretch' className={cn(s.group_container, !group.enabled && s.disabled)}>
          <Stack style={fws}>
            {/* Where this group goes in the parent bool */}
            <Select.Root value={group.operator} onValueChange={v => updateGroup('operator', v as Filter.BoolOperator)}>
              <Select.Trigger className={s.value}>
                <Select.Value placeholder={t('queryBuilder.operatorPlaceholder')} />
              </Select.Trigger>
              <Select.Content container={container}>
                {OpenSearchQueryBuilder.OPERATORS.map(op => (
                  <Select.Item key={op.value} value={op.value}>
                    <Icon name={op.icon} />
                    {t(op.labelKey)}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
            <Switch checked={group.enabled} onCheckedChange={v => updateGroup('enabled', v)} />
            <Button variant='secondary' icon='Plus' onClick={addChildCondition}>
              {t('queryBuilder.addCondition')}
            </Button>
            <Button variant='secondary' icon='FolderPlus' onClick={addChildGroup}>
              {t('queryBuilder.addGroup')}
            </Button>
            <Button variant='tertiary' icon='Trash2' onClick={removeGroup} />
          </Stack>
          <Stack dir='column' ai='stretch' className={s.group_children}>
            {group.children.map(child =>
              Filter.isGroup(child)
                ? <GroupRow key={child.id} group={child} setFilters={setChildFilters} keys={keys} options={options} container={container} />
                : <FilterRow key={child.id} filter={child} setFilters={setChildFilters} keys={keys} options={options} container={container} />
            )}
          </Stack>
        </Stack>
      );
    });

    /** Renders and normalizes a generic list of top-level and nested filter items. */
    export const Filters = memo(({ filters, setFilters, options, container }: Query.Filters.Props) => {
      const keys = useMemo(
        () => Object.keys(options).sort((left, right) => left.localeCompare(right)),
        [options],
      );

      useEffect(() => {
        const normalizedFilters = Filter.Entity.normalizeConditions(filters, options);
        if (normalizedFilters !== filters) setFilters(normalizedFilters);
      }, [filters, options, setFilters]);

      return (
        <Stack ai='stretch' dir='column'>
          {filters.map(item =>
            Filter.isGroup(item)
              ? <GroupRow key={item.id} group={item} setFilters={setFilters} keys={keys} options={options} container={container} />
              : <FilterRow key={item.id} filter={item} setFilters={setFilters} keys={keys} options={options} container={container} />
          )}
        </Stack>
      )
    });
  }
}
