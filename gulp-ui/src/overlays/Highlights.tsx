import s from './Highlights.module.css';
import { scrollStore } from '@/store/scroll.store';
import { Icon } from '@/ui/Icon';
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
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
import {
  getHighlightCssPosition,
  getHighlightLayout,
  shouldTranslateHighlightOverlay,
} from './HighlightLayout';
import type { HighlightLayout } from './HighlightLayout';
import { highlightRegistry } from './HighlightRegistry';
import { DataStore } from '@/store/DataStore';
import { TIMELINE_RULER_HEIGHT_PX } from '@/app/body/CanvasViewport';

const HIGHLIGHT_VARIANTS = new Set<string>(Badge.Variants);
const HIGHLIGHT_LABEL_OFFSET_PX = 32;

interface HighlightColors {
  badgeVariant: NonNullable<Badge.Variant> | undefined;
  isLiteral: boolean;
  borderColor: string;
  fillColor: string;
  labelBackground: string;
  labelTextColor: string;
  fadeColor: string;
}

type HighlightCssProperties = React.CSSProperties & {
  [key: `--highlight-${string}`]: string | number;
};

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

function getHighlightColors(color: string): HighlightColors {
  const fallbackColor = 'var(--gray-700)';

  if (isHighlightVariant(color) && !isLiteralCssColor(color)) {
    return {
      badgeVariant: color,
      isLiteral: false,
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
    isLiteral: true,
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

    /**
     * Renders the interactive highlight range selection overlay.
     * @param props Overlay Stack presentation properties.
     * @returns Highlight creation overlay.
     */
    export function Overlay({ className, ...props }: Highlights.Create.Overlay.Props) {
      const { Info, setHighlightsOverlay } = Application.use();
      const { t } = Locale.use();
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
            x: scrollStore.getX(),
            y: scrollStore.getY()
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

    interface OverlayContentProps extends Highlights.List.Overlay.Props {
      followsTimelineScroll: boolean;
    }

    /**
     * Renders highlight geometry for either a scrolling timeline or a static preview.
     * @param props Layout overrides, scroll behavior, and Stack presentation properties.
     * @returns Highlight overlay content.
     */
    function OverlayContent({ frame, fixed, layoutWidth, followsTimelineScroll, ...props }: OverlayContentProps) {
      const { app, Info } = Application.use();
      const scrollContentRef = useRef<HTMLDivElement>(null);

      // Recompute document-space geometry after a renderless settled zoom commit.
      const settledTimelineScale = useSyncExternalStore(
        Info.subscribeTimelineScale,
        Info.getTimelineScaleSnapshot,
        Info.getTimelineScaleSnapshot,
      );
      const overlayDataVersion = useSyncExternalStore(
        DataStore.subscribeOverlays,
        DataStore.getOverlaysSnapshot,
        DataStore.getOverlaysSnapshot,
      );

      const highlights = useMemo(
        () => Highlight.Entity.selected(app),
        [app.timeline.renderVersion, app.target.operations, overlayDataVersion],
      );

      const computedDepths = useMemo(() => Highlight.Entity.computeDepths(highlights), [highlights]);

      const frameMin = frame?.min ?? app.timeline.frame.min;
      const frameMax = frame?.max ?? app.timeline.frame.max;
      const canvasViewportWidth = Info.width / Math.max(app.timeline.scale, 0.01);
      const resolvedLayoutWidth = layoutWidth ?? canvasViewportWidth * settledTimelineScale;
      const layouts = useMemo(
        () => highlights.map((highlight) => (
          getHighlightLayout(highlight, frameMin, frameMax, resolvedLayoutWidth)
        )),
        [frameMax, frameMin, highlights, resolvedLayoutWidth],
      );
      const highlightElements = useMemo(
        () => highlights.map((highlight, index) => (
          <Highlights.Component
            key={highlight.id ?? index}
            fixed={fixed}
            frame={frame}
            layoutWidth={layoutWidth}
            highlight={highlight}
            index={computedDepths[index]}
            layout={layouts[index]}
          />
        )),
        [computedDepths, fixed, frame, highlights, layoutWidth, layouts],
      );

      useEffect(() => {
        if (layoutWidth !== undefined) return;
        highlightRegistry.replace(highlights.map((highlight, index) => ({
          id: highlight.id,
          left: layouts[index].left,
          width: layouts[index].width,
          index: computedDepths[index],
          color: highlight.color,
        })));

        return () => highlightRegistry.clear();
      }, [computedDepths, highlights, layoutWidth, layouts]);

      useEffect(() => {
        if (!followsTimelineScroll) {
          if (layoutWidth === undefined) {
            highlightRegistry.setViewportOffset(0);
          }
          return;
        }

        /**
         * Applies the latest timeline scroll to both the visual layer and legacy registry.
         * @returns Nothing.
         */
        const synchronizeHorizontalScroll = (): void => {
          const viewportOffset = scrollStore.getX();
          scrollContentRef.current?.style.setProperty(
            '--highlight-scroll-offset',
            `${-viewportOffset}px`,
          );
          highlightRegistry.setViewportOffset(viewportOffset);
        };

        synchronizeHorizontalScroll();
        return scrollStore.subscribeX(synchronizeHorizontalScroll);
      }, [followsTimelineScroll, layoutWidth]);

      const initialViewportOffset = followsTimelineScroll ? scrollStore.getX() : 0;

      return (
        <Stack pos='absolute' className={cn(s.overlay, s.ignore)} {...props}>
          <Stack
            ref={scrollContentRef}
            pos='absolute'
            className={s.scrollContent}
            style={{
              width: layoutWidth === undefined
                ? `var(--highlight-timeline-width, ${resolvedLayoutWidth}px)`
                : `${resolvedLayoutWidth}px`,
              '--highlight-scroll-offset': `${-initialViewportOffset}px`,
            } as HighlightCssProperties}
          >
            {highlightElements}
          </Stack>
        </Stack>
      )
    }

    /**
     * Selects a scrolling implementation for the main timeline and a static one for previews.
     * @param props Optional frame/layout overrides and Stack presentation properties.
     * @returns Highlight list overlay.
     */
    export function Overlay(props: Highlights.List.Overlay.Props) {
      return (
        <OverlayContent
          {...props}
          followsTimelineScroll={shouldTranslateHighlightOverlay(props.fixed, props.layoutWidth)}
        />
      );
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
      layout?: HighlightLayout;
    }
  }

  /**
   * Renders one highlight using precomputed document-space geometry when available.
   * @param props Highlight entity, layout metadata, and Stack presentation properties.
   * @returns Highlight label and interaction surface.
   */
  export function Component({ highlight, fixed: _fixed, layout, layoutWidth, frame = {}, style, className, index = 0, native, ...props }: Highlights.Component.Props) {
    const { Info, app } = Application.use();
    const { t } = Locale.use();
    const highlightColors = useMemo(() => getHighlightColors(highlight.color), [highlight.color]);
    const rendersNative = native || layoutWidth !== undefined;
    const resolvedLayoutWidth = layoutWidth ?? Info.width;
    const resolvedLayout = useMemo(
      () => layout ?? getHighlightLayout(
        highlight,
        frame.min ?? app.timeline.frame.min,
        frame.max ?? app.timeline.frame.max,
        resolvedLayoutWidth,
      ),
      [
        app.timeline.frame.max,
        app.timeline.frame.min,
        frame.max,
        frame.min,
        highlight,
        layout,
        resolvedLayoutWidth,
      ],
    );
    const horizontalPosition = getHighlightCssPosition(
      resolvedLayout,
      resolvedLayoutWidth,
      native,
    );

    return (
      <Stack
        pos='absolute'
        className={cn(className, s.highlight)}
        data-highlight
        style={{
          ...style,
          '--highlight-left': horizontalPosition.left,
          '--highlight-width': horizontalPosition.width,
          '--variant': highlightColors.borderColor,
          '--index': index,
          '--highlight-ruler-height': `${TIMELINE_RULER_HEIGHT_PX}px`,
          '--highlight-label-top': `${index * HIGHLIGHT_LABEL_OFFSET_PX}px`,
          '--highlight-fade': highlightColors.fadeColor,
          '--highlight-background': rendersNative ? highlightColors.fillColor : 'transparent',
          '--highlight-label-background': highlightColors.labelBackground,
          '--highlight-label-text': highlightColors.labelTextColor,
        } as HighlightCssProperties}
        jc='flex-end'
        ai='flex-start'
        {...props}
      >
        <Stack pos='absolute' className={s.highlightLabel}>
          <Badge
            className={cn(s.title, highlightColors.isLiteral && s.literalTitle)}
            data-type='badge'
            variant={highlightColors.badgeVariant}
            value={highlight.name || t('highlights.fallbackName')}
            icon={Glyph.List.get(highlight.glyph_id) || Default.Icon.HIGHLIGHT}
          />
          <Button tabIndex={-1} size='sm' variant='glass' className={s.deleteButton} icon='Trash2' onClick={() => Info.highlight_delete(highlight.id)} />
        </Stack>
      </Stack>
    )
  }

  /** Ensures the module-level legacy registry is initialized. */
  export function init(): void {
    // The typed registry is initialized when this module is evaluated.
  }

  /**
   * Returns legacy viewport-relative highlight tuples on demand.
   * @returns Registered highlights adjusted by the current viewport offset.
   */
  export function list(): [number, number, number, string][] {
    Highlights.init();
    return highlightRegistry.list();
  }

  /**
   * Registers one document-space highlight for legacy consumers.
   * @param id Highlight identifier.
   * @param left Document-space horizontal position.
   * @param width Highlight width.
   * @param index Highlight overlap depth.
   * @param color Highlight color.
   * @returns Nothing.
   */
  export function set(id: Highlight.Id, left: number, width: number, index: number, color: string): void {
    init();
    highlightRegistry.set({ id, left, width, index, color });
  }

  /**
   * Removes one highlight from the legacy registry.
   * @param id Highlight identifier.
   * @returns Nothing.
   */
  export function remove(id: Highlight.Id): void {
    highlightRegistry.remove(id);
  }
}
