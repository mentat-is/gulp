import { HTMLAttributes, useMemo } from 'react';
import s from './styles/Spinner.module.css';
import { cn } from '@/ui/utils';

export namespace Spinner {
  export interface Props extends HTMLAttributes<HTMLDivElement> {
    size?: number;
    color?: string;
  }
}

export function Spinner({ size = 20, color, className, style, ...props }: Spinner.Props) {
  const spinnerStyle = useMemo(() => ({
    '--spinner-size': `${size}px`,
    '--spinner-color': color,
    ...style
  }), [size, color]) as React.CSSProperties;

  return (
    <div className={cn(s.wrapper, className)} style={spinnerStyle} data-spinner-wrapper {...props}>
      <div className={s.spinner} data-spinner>
        {Array.from({ length: 12 }).map((_, i) => (
          <i key={i} className={s.bar} />
        ))}
      </div>
    </div>
  )
}
