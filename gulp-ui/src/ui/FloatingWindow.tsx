import React, {
  HTMLAttributes,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { cn } from '@/ui/utils';
import s from './styles/FloatingWindow.module.css';
import { Icon } from '@/ui/Icon';

export namespace FloatingWindow {
  export type PositionOptions = { top?: number; left?: number };
  export type SizeOptions = { height?: number; width?: number };
  type NumericOptions = [number, number];

  export interface OptionalProps {
    /** Enable SVG noise overlay */
    noise?: boolean;

    /** If set, pressing this key will open the window */
    trigger?: string;

    /** initial open state */
    defaultOpen?: boolean;

    /** initial fullscreen state */
    defaultFullscreen?: boolean;

    /** initial hidden state */
    defaultHidden?: boolean;

    /** default size */
    size?: SizeOptions | NumericOptions;

    /** default position */
    position?: PositionOptions | NumericOptions;

    /** min constraints */
    minSize?: { width: number; height: number };

    /** If true, prevents body scroll when fullscreen and not hidden */
    lockScrollOnFullscreen?: boolean;

    /** notify open changes */
    onOpenChange?: (open: boolean) => void;
  }

  export interface RequiredProps {
    title: string;
    icon?: Icon.Name;
  }

  export interface Props
    extends Omit<HTMLAttributes<HTMLDivElement>, 'title'>,
    RequiredProps,
    OptionalProps {
    children?: React.ReactNode;
  }

  export interface NoiseProps extends HTMLAttributes<SVGSVGElement> {
    enable?: boolean;
  }

  export type Api = {
    open: () => void;
    close: () => void;
    toggle: () => void;
    setFullscreen: (v: boolean) => void;
    setHidden: (v: boolean) => void;
  };
}

export function FloatingWindow({
  className,
  noise,
  title,
  icon,
  defaultOpen = false,
  defaultFullscreen = false,
  defaultHidden = false,
  trigger,
  size,
  position,
  minSize = { width: 240, height: 120 },
  lockScrollOnFullscreen = true,
  onOpenChange,
  children,
  ...props
}: FloatingWindow.Props) {
  const self = useRef<HTMLDivElement>(null);

  const [settings, setSettings] = useState<
    Required<FloatingWindow.SizeOptions & FloatingWindow.PositionOptions>
  >({
    top: (Array.isArray(position) ? position[0] : position?.top) ?? 100,
    left: (Array.isArray(position) ? position[1] : position?.left) ?? 100,
    height: (Array.isArray(size) ? size[0] : size?.height) ?? 480,
    width: (Array.isArray(size) ? size[1] : size?.width) ?? 960,
  });

  const [open, setOpen] = useState<boolean>(defaultOpen);
  useEffect(() => setOpen(defaultOpen), [defaultOpen]);
  const [fullscreen, setFullscreen] = useState<boolean>(defaultFullscreen);
  const [hidden, setHidden] = useState<boolean>(defaultHidden);

  const setOpenSafe = useCallback(
    (v: boolean) => {
      setOpen(v);
      onOpenChange?.(v);
    },
    [onOpenChange]
  );

  function Noise({ className, enable, ...props }: FloatingWindow.NoiseProps) {
    const NoiseImpl = useCallback(
      () =>
        enable ? (
          <svg className={cn(className, s.noise)} {...props}>
            <filter id="noise">
              <feTurbulence seed={128} type="fractalNoise" baseFrequency={0.8} />
            </filter>
            <rect width="100%" height="100%" filter="url(#noise)" />
          </svg>
        ) : null,
      [enable]
    );
    return <NoiseImpl />;
  }

  const toggleScroll = useCallback((enable: boolean) => {
    document.body.style.overflow = enable ? 'auto' : 'hidden';
    document.documentElement.style.overflow = enable ? 'auto' : 'hidden';
  }, []);

  useEffect(() => {
    if (!open) return toggleScroll(true);
    if (!lockScrollOnFullscreen) return toggleScroll(true);
    if (fullscreen && !hidden) return toggleScroll(false);
    return toggleScroll(true);
  }, [open, fullscreen, hidden, lockScrollOnFullscreen, toggleScroll]);

  useEffect(() => {
    if (!trigger) return;

    const onKeyPress = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      const tag = target.tagName.toLowerCase();
      if (['input', 'textarea', 'select'].includes(tag) || target.isContentEditable) return;
      if (e.key === trigger) {
        e.preventDefault();
        setOpenSafe(true);
      }
    };

    document.addEventListener('keypress', onKeyPress);
    return () => document.removeEventListener('keypress', onKeyPress);
  }, [trigger, setOpenSafe]);

  const onMouseDownMove = useCallback(() => {
    if (hidden) return;

    const onMouseMove = (event: MouseEvent) => {
      setSettings(prev => {
        const el = self.current;
        if (!el) return prev;

        const nextTop = prev.top + event.movementY;
        const nextLeft = prev.left + event.movementX;

        return {
          ...prev,
          top: Math.min(Math.max(nextTop, 0), window.innerHeight - el.clientHeight),
          left: Math.min(Math.max(nextLeft, 0), window.innerWidth - el.clientWidth),
        };
      });
    };

    const onMouseUp = () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  }, [hidden]);

  const onMouseDownResize = useCallback(
    (direction: string) => {
      if (hidden) return;

      const onMouseMove = (e: MouseEvent) => {
        setSettings(prev => {
          const next = { ...prev };

          if (direction.includes('bottom')) {
            next.height = Math.max(next.height + e.movementY, minSize.height);
          }
          if (direction.includes('right')) {
            next.width = Math.max(next.width + e.movementX, minSize.width);
          }
          if (direction.includes('left')) {
            const newWidth = Math.max(next.width - e.movementX, minSize.width);
            const delta = next.width - newWidth;
            next.width = newWidth;
            next.left = next.left + (e.movementX - delta);
          }
          if (direction.includes('top')) {
            const newHeight = Math.max(next.height - e.movementY, minSize.height);
            const delta = next.height - newHeight;
            next.height = newHeight;
            next.top = next.top + (e.movementY - delta);
          }

          return next;
        });
      };

      const onMouseUp = () => {
        window.removeEventListener('mousemove', onMouseMove);
        window.removeEventListener('mouseup', onMouseUp);
      };

      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', onMouseUp);
    },
    [hidden, minSize.height, minSize.width]
  );

  const close = useCallback(() => setOpenSafe(false), [setOpenSafe]);

  const toggleFullscreen = useCallback(() => {
    setFullscreen(v => !v);
    setHidden(false);
  }, []);

  const Window = useMemo(
    () =>
      open ? (
        <div
          ref={self}
          className={cn(s.window, hidden && s.hidden, fullscreen && s.fullscreen)}
          style={settings}
          {...props}
        >
          <div className={cn(s.heading, s.glass)} onMouseDown={onMouseDownMove}>
            <span className={cn(s.button, s.close)} onClick={close} />
            <span className={cn(s.button, s.hide)} onClick={() => setHidden(v => !v)} />
            <span className={cn(s.button, s.open)} onClick={toggleFullscreen} />
            <div className={s.title}>
              {icon && <Icon size={14} name={icon} />}
              <h1>{title}</h1>
            </div>
          </div>

          <div className={cn(s.content, s.glass, className)}>
            <Noise enable={noise} className={s.noise} />
            {children}
          </div>

          {[
            'top',
            'left',
            'right',
            'bottom',
            'top left',
            'top right',
            'bottom left',
            'bottom right',
          ].map(resize => (
            <div
              key={resize}
              className={cn(s.resizeable, ...resize.split(' ').map(r => s[r]))}
              onMouseDown={() => onMouseDownResize(resize)}
            />
          ))}
        </div>
      ) : null,
    [
      open,
      hidden,
      fullscreen,
      settings,
      props,
      onMouseDownMove,
      close,
      toggleFullscreen,
      className,
      noise,
      children,
      onMouseDownResize,
      title,
      icon,
    ]
  );

  return Window;
}
