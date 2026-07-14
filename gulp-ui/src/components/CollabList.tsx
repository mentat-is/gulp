import { LinkFunctionality, NoteFunctionality } from "@/banners/Collab.functionality";
import { Application } from "@/context/Application.context";
import { cn } from "@/ui/utils";
import { formatDistanceToNow } from "date-fns";
import { useState, useEffect, useMemo, useCallback, memo } from "react";
import { Markdown } from "@/ui/Markdown";
import s from './styles/Collab.module.css';
import { Select } from "@/ui/Select";
import { Separator } from "@/ui/Separator";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import { Badge } from "@/ui/Badge";
import { Note } from "@/entities/Note";
import { Link } from "@/entities/Link";
import { Doc } from "@/entities/Doc";
import { Glyph } from "@/entities/Glyph";
import { Locale } from "@/locales";

export namespace Collab {
  export namespace List {
    export interface Props extends Stack.Props {
      notes: Note.Type[],
      links: Link.Type[]
      container?: HTMLElement | null;
    }
  }

  const FLEX_WRAP_STYLE = { flexWrap: 'wrap' } as const;

  /**
   * Resolves the display color for collaboration list items.
   * @param item Note or link item rendered by the collaboration list.
   * @returns Theme-aware note color fallback, or the persisted link color.
   */
  function getCollabItemColor(item: Note.Type | Link.Type): string {
    return item.type === 'note' ? Note.Entity.color(item) : item.color;
  }

  /**
   * Finds the currently selected collaboration item in the latest rendered items.
   * @param items Notes and links available to the collaboration list.
   * @param targetId Selected item identifier.
   * @returns The matching note or link, or null when the item is unavailable.
   */
  function findCollabItemById(
    items: Array<Note.Type | Link.Type>,
    targetId?: string,
  ): Note.Type | Link.Type | null {
    if (!targetId) return null;
    return items.find((item) => item.id === targetId) ?? null;
  }

  /**
   * Resolves the first available collaboration item identifier.
   * @param items Notes and links available to the collaboration list.
   * @returns First item identifier, or undefined when the list is empty.
   */
  function getFirstCollabItemId(
    items: Array<Note.Type | Link.Type>,
  ): string | undefined {
    return items[0]?.id;
  }

  const SelectItem = memo(({ item }: { item: Note.Type | Link.Type }) => {
    const itemColorStyle = useMemo(() => ({ color: getCollabItemColor(item) }), [item]);
    const itemIcon = useMemo(() =>
      item.type === 'note' ? Note.Entity.icon(item) : Link.Entity.icon(item),
      [item.type, item]
    );
    return (
      <Select.Item style={itemColorStyle} value={item.id}>
        <Select.Icon name={itemIcon} />
        <p>{item.name}</p>
      </Select.Item>
    );
  }, (prevProps, nextProps) => {
    return prevProps.item.id === nextProps.item.id &&
      prevProps.item.color === nextProps.item.color &&
      prevProps.item.name === nextProps.item.name &&
      prevProps.item.type === nextProps.item.type;
  });
  export function List({ notes, links, container, className, ...props }: Collab.List.Props) {
    const { app, spawnBanner } = Application.use();
    const { t } = Locale.use();
    const collabItems = useMemo(() => [...notes, ...links], [notes, links]);
    const hasItems = notes.length > 0 || links.length > 0;
    const [targetId, setTargetId] = useState<string | undefined>(
      getFirstCollabItemId(collabItems),
    );

    const target = useMemo(() => {
      return findCollabItemById(collabItems, targetId);
    }, [collabItems, targetId]);

    useEffect(() => {
      if (findCollabItemById(collabItems, targetId)) return;
      setTargetId(getFirstCollabItemId(collabItems));
    }, [collabItems, targetId]);

    const handleEditClick = useCallback(() => {
      if (!target) return;
      const banner = target.type === 'note'
        ? <NoteFunctionality.Create.Banner event={Doc.Entity.id(app, target.doc._id)} note={target} />
        : <LinkFunctionality.Create.Banner event={Doc.Entity.id(app, target.doc_id_from)} link={target as unknown as Link.Type} />;
      spawnBanner(banner);
    }, [target, app, spawnBanner]);
    const handleDeleteClick = useCallback(() => {
      if (!target) return;
      const banner = target.type === 'note'
        ? <Note.Delete.Banner note={target} />
        : <Link.Delete.Banner link={target as unknown as Link.Type} />;
      spawnBanner(banner);
    }, [target, spawnBanner]);
    const handleValueChange = useCallback((v: string) => {
      setTargetId(v);
    }, []);

    const Edit = useMemo(() => <Button variant='secondary' icon='PencilEdit' onClick={handleEditClick} />, [handleEditClick]);

    const List = useMemo(() => {
      return collabItems.map(c => (
        <SelectItem key={c.id} item={c} />
      ));
    }, [collabItems]);

    const Tags = useMemo(() => {
      return (
        <>
          {notes.map(n => n.tags).flat().map(tag => (
            <Badge key={tag} value={tag} variant='gray-subtle' size='sm' />
          ))}
        </>
      )
    }, [notes]);

    const MemoizedSelect = useMemo(() => {
      if (!target) return null;

      return (
        <Select.Root onValueChange={handleValueChange}>
          <Select.Trigger style={{ color: getCollabItemColor(target) }} value={target.id}>
            <Select.Icon name={Glyph.List.get(target.glyph_id!) ?? 'Target'} />
            <p>{target.name}</p>
          </Select.Trigger>
          <Select.Content container={container}>
            {List}
          </Select.Content>
        </Select.Root>
      );
    }, [handleValueChange, target, List]);

    return hasItems && target && (
      <Stack dir='column' ai='stretch' className={cn(s.detailed, className)} {...props}>
        <>
          <Stack dir='column' ai='stretch'>
            <Stack>
              {MemoizedSelect}
              {Edit}
              <Button onClick={handleDeleteClick} variant='tertiary' icon='Trash2' />
            </Stack>
            <Stack style={FLEX_WRAP_STYLE} jc='flex-start' ai='center'>
              <Badge variant='gray-subtle' icon='ClockRewind' size='sm'>
                {t('collab.createdAgo', { time: formatDistanceToNow(target.time_created, { addSuffix: true }) })}
              </Badge>
              {Tags}
            </Stack>
          </Stack>
          <Separator />
          <Description value={target.type === 'note' ? target.text : target.description} />
        </>
      </Stack>
    )
  }
  export namespace Description {
    export interface Props {
      value: string
    }
  }

  /**
   * Renders the full collaboration item description without a collapsed text state.
   *
   * @param value Text or markdown content to display for the active note or link.
   * @returns Markdown content for the selected collaboration item.
   */
  export function Description({ value }: Description.Props) {
    return (
      <Stack dir='column' className={s.descriptionWrapper} gap={0} ai='unset'>
        <Markdown className={s.description} value={value} />
      </Stack>
    )
  }
}
