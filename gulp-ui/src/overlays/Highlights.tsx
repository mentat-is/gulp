import s from './Highlights.module.css';
import { useScroll } from '@/store/scroll.store';
import { Icon } from '@/ui/Icon';
import { useMemo, useRef, useState } from 'react';
import { MinMax, Range } from '@/class/Info';
import { Application } from '@/context/Application.context';
import { Default } from '@/dto/Dataset';
import { Algorhithm } from '@/ui/utils';
import { cn } from '@/ui/utils';
import { Stack } from '@/ui/Stack';
import { Badge } from '@/ui/Badge';
import { Input } from '@/ui/Input';
import { Button } from '@/ui/Button';
import { Glyph } from '@/entities/Glyph';
import { Highlight } from '@/entities/Highlight';
import { Color } from '@/entities/Color';
import { ColorPicker, ColorPickerPopover, ColorPickerTrigger } from '@/ui/Color';
import { Label } from '@/ui/Label';
import { Locale } from '@/locales';

const HIGHLIGHT_VARIANTS = new Set<string>(Badge.Variants);

function isHighlightVariant(color: string): color is NonNullable<Badge.Variant> {
  return HIGHLIGHT_VARIANTS.has(color);
}

function isLiteralCssColor(color: string): boolean {
  const value = color?.trim();

  return Boolean(
    value
    && typeof CSS !== 'undefined'
    && CSS.supports('color', value),
  );
}

function getHighlightColors(color: string) {
  const fallbackColor = 'var(--gray-700)';

  if (isHighlightVariant(color) && !isLiteralCssColor(color)) {
    return {
      badgeVariant: color,
      badgeStyle: undefined,
      borderColor: `var(--${color}-800)`,
      fillColor: `color-mix(in srgb, var(--${color}-800) 16%, transparent)`,
      labelBackground: `var(--${color}-700)`,
      labelTextColor: 'var(--accent)',
      fadeColor: `color-mix(in srgb, var(--${color}-700) 68%, transparent)`,
    };
  }

  const resolvedColor = color?.trim() || fallbackColor;
  const readableTextColor = Color.Themer.getReadablePaletteTextColor(resolvedColor);

  return {
    badgeVariant: undefined,
    badgeStyle: {
      backgroundColor: resolvedColor,
      color: readableTextColor,
    },
    borderColor: resolvedColor,
    fillColor: `color-mix(in srgb, ${resolvedColor} 16%, transparent)`,
    labelBackground: resolvedColor,
    labelTextColor: readableTextColor,
    fadeColor: `color-mix(in srgb, ${resolvedColor} 68%, transparent)`,
  };
}

export namespace Highlights {
  export namespace Create {
    export namespace Overlay {
      export interface Props extends Stack.Props {

      }
    }

    export function Overlay({ className, ...props }: Highlights.Create.Overlay.Props) {
      const { Info, setHighlightsOverlay } = Application.use();
      const { t } = Locale.use();
      const { x: scrollX, y: scrollY } = useScroll();
      const [icon, setIcon] = useState<Glyph.Id | null>(Glyph.List.keys().next().value!);
      const [name, setName] = useState<string>('');
      const [color, setColor] = useState<string>('#ffa647');
      const [range, setSelection] = useState<[number, number] | null>(null);
      const [isSelected, setIsSelected] = useState<boolean>(false);
      const overlay = useRef<HTMLDivElement>(null);

      const destroyOverlay = () => {
        setHighlightsOverlay(null)
      };

      const unselect = () => {
        setIsSelected(false);
        setSelection(null);
      }

      const submit = () => {
        if (!range) {
          return;
        }

        const algo = new Algorhithm({
          frame: Info.app.timeline.frame,
          scale: Info.app.timeline.scale,
          scroll: {
            x: scrollX,
            y: scrollY
          },
          width: Info.width
        })

        const time_range: Range = range.map(v => algo.timestamp_from_rel_x(v)) as unknown as Range;

        Info.highlight_create({
          name,
          color,
          icon,
          time_range
        });

        setHighlightsOverlay(null);
      }

      const Hint = useMemo(() => {
        if (range) {
          return (
            <Stack className={s.hint} onMouseDown={e => e.stopPropagation()} pos='relative'>
              <Input className={s.name} variant='highlighted' icon={Glyph.List.get(icon!) ?? Default.Icon.HIGHLIGHT} placeholder={t('highlights.namePlaceholder')} value={name} onChange={e => setName(e.target.value)} />
              <Glyph.Chooser rootClassName={s.icon} asButton icon={icon} setIcon={setIcon} />
              <Stack dir='column' gap={6} ai='flex-start' data-input>
                <Label value={t('common.pickColor')} />
                <ColorPicker color={color} setColor={setColor}>
                  <ColorPickerTrigger className={s.select} />
                  <ColorPickerPopover />
                </ColorPicker>
              </Stack>
              <Button variant='glass' disabled={!name} icon='Check' onMouseDown={submit}>{t('common.create')}</Button>
              <Button className={s.x} variant='secondary' icon='X' onMouseDown={unselect} />
            </Stack>
          )
        }

        return (
          <Stack className={s.hint} onMouseDown={e => e.stopPropagation()} pos='relative'>
            <Icon name='ChartBarBig' />
            <code>{t('highlights.selectArea')}</code>
            <Button className={s.close} variant='secondary' size='sm' icon='Logout' onMouseDown={destroyOverlay}>{t('common.exit')}</Button>
          </Stack>
        )
      }, [destroyOverlay, unselect, icon, name, setName, range, submit, t])

      const createHighlightOverlayMouseDownHandler = (event: React.MouseEvent) => {
        if (!overlay.current) {
          return;
        }

        setIsSelected(false);

        const rect = overlay.current.getBoundingClientRect();

        const x = Math.round(event.clientX - rect.x);

        setSelection([x, x]);
      }

      const createHighlightOverlayMouseUpHandler = () => {
        setIsSelected(true);
      }

      const createHighlightOverlayMouseMoveHandler = (event: React.MouseEvent) => {
        if (!overlay.current || isSelected || !range) {
          return;
        }

        const rect = overlay.current.getBoundingClientRect();

        const x = Math.round(event.clientX - rect.x);
        setSelection([range[0], x]);
      }

      const Highlight = useMemo(() => {
        if (!range) {
          return null;
        }

        return (
          <Highlights.Component highlight={{
            color,
            name,
            glyph_id: icon!,
            range,
          } as unknown as Highlight.Type} native />
        )
      }, [color, name, icon, range]);

      return (
        <Stack
          ref={overlay}
          className={cn(s.overlay, s.focus)}
          pos='absolute'
          onMouseDown={createHighlightOverlayMouseDownHandler}
          onMouseMove={createHighlightOverlayMouseMoveHandler}
          onMouseLeave={createHighlightOverlayMouseUpHandler}
          onMouseUp={createHighlightOverlayMouseUpHandler}
          {...props}>
          {Highlight}
          <Stack pos='absolute' className={s.hint_wrapper} jc='center'>
            {Hint}
          </Stack>
        </Stack>
      )
    }
  }

  export namespace List {
    export namespace Overlay {
      export interface Props extends Stack.Props {
        frame?: Partial<MinMax>;
        layoutWidth?: number;
        fixed?: boolean;
      }
    }

    export function Overlay({ frame, fixed, layoutWidth, ...props }: Highlights.List.Overlay.Props) {
      const { app } = Application.use();

      const highlights = useMemo(() => Highlight.Entity.selected(app), [app.timeline.renderVersion, app.target.operations]);

      const computedDepths = useMemo(() => Highlight.Entity.computeDepths(highlights), [highlights]);

      return (
        <Stack pos='absolute' className={cn(s.overlay, s.ignore)} {...props}>
          {highlights.map((highlight, index) => {
            return (
              <Highlights.Component key={index} fixed={fixed} frame={frame} layoutWidth={layoutWidth} highlight={highlight} index={computedDepths[index]} />
            )
          })}
        </Stack>
      )
    }
  }

  export namespace Component {
    export interface Props extends Stack.Props {
      highlight: Highlight.Type;
      index?: number;
      native?: boolean;
      frame?: Partial<MinMax>;
      layoutWidth?: number;
      fixed?: boolean;
    }
  }

  export function Component({ highlight, fixed, layoutWidth, frame = {}, style, className, index = 0, native, ...props }: Highlights.Component.Props) {
    const { Info, app } = Application.use();
    const { t } = Locale.use();
    const { x: scrollX } = useScroll();
    const highlightColors = useMemo(() => getHighlightColors(highlight.color), [highlight.color]);

    if (layoutWidth) {
      native = true;
    }

    const range = useMemo((): Range => {
      if ('range' in highlight && Array.isArray(highlight.range)) {
        return highlight.range as Range;
      }

      return highlight.time_range.map(t => Math.round(((t - (frame.min ?? app.timeline.frame.min)) / ((frame.max ?? app.timeline.frame.max) - (frame.min ?? app.timeline.frame.min))) * (layoutWidth ?? Info.width)) - (fixed ? 0 : scrollX)) as Range;
    }, [app.timeline.frame, app.timeline.scale, fixed ? undefined : scrollX, highlight, layoutWidth]);

    const [left, width] = useMemo(() => {
      const left = Math.min(...range);
      const width = Math.max(...range) - left;

      if (!native) {
        Highlights.set(highlight.id, left, width, index, highlight.color);
      }

      return [left, width];
    }, [range]);

    return (
      <Stack
        pos='absolute'
        className={cn(className, s.highlight)}
        style={{
          ...style,
          left,
          width,
          '--variant': highlightColors.borderColor,
          '--index': index,
          '--highlight-fade': highlightColors.fadeColor,
          background: native ? highlightColors.fillColor : 'transparent',
        } as React.CSSProperties}
        jc='flex-end'
        ai='flex-start'
        {...props}
      >
        <Stack style={{ background: highlightColors.labelBackground, color: highlightColors.labelTextColor }}>
          <Badge
            className={s.title}
            data-type='badge'
            variant={highlightColors.badgeVariant}
            style={highlightColors.badgeStyle}
            value={highlight.name || t('highlights.fallbackName')}
            icon={Glyph.List.get(highlight.glyph_id) || Default.Icon.HIGHLIGHT}
          />
          <Button tabIndex={-1} size='sm' variant='glass' className={s.deleteButton} icon='Trash2' onClick={() => Info.highlight_delete(highlight.id)} />
        </Stack>
      </Stack>
    )
  }

  export function init() {
    // @ts-ignore
    window.highlights = window.highlights ?? {};
  }

  export function list(): [number, number, number, string][] {
    Highlights.init();
    // @ts-ignore
    return Object.values(window.highlights);
  }

  export function set(id: Highlight.Id, left: number, width: number, index: number, color: string) {
    init();
    // @ts-ignore
    window.highlights[id] = [left, width, index, color];
  }

  export function remove(id: Highlight.Id) {
    // @ts-ignore
    delete window.highlights[id]
  }
}
