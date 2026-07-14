import React, { useMemo } from 'react';
import s from './styles/Skeleton.module.css';
import { cva, VariantProps } from 'class-variance-authority';

const { avatar, short, badge, button, long, low, high } = s;

const skeletonVariants = cva(s.skeleton, {
  variants: {
    variant: {
      default: s.default,
      avatar,
      badge,
      button
    },
    width: {
      default: s.defaultWidth,
      short,
      long,
      unset: s.w_unset,
      full: s.w_full
    },
    height: {
      default: s.defaultHeight,
      low,
      high,
      unset: s.h_unset,
      full: s.h_full
    },
    border: {
      default: s.defaultBorder,
      s: s.border_s,
      m: s.border_m,
      l: s.border_l,
      r: s.border_r
    }
  },
  defaultVariants: {
    variant: 'default',
    width: 'default',
    height: 'default',
    border: 'default'
  },
});

export type ChadNumber = number | `${number}` | `${number}%`;

type SkeletonProps = React.HTMLAttributes<HTMLDivElement> & Omit<VariantProps<typeof skeletonVariants>, 'height' | 'width'> & {
  show?: boolean;
  height?: VariantProps<typeof skeletonVariants>['height'] | ChadNumber;
  width?: VariantProps<typeof skeletonVariants>['width'] | ChadNumber;
  delay?: boolean | number
};

const Skeleton = React.forwardRef<HTMLDivElement, SkeletonProps>(({
  className,
  variant,
  height,
  width,
  border,
  style = {},
  show,
  delay,
  ...props
}, ref) => show || !props.children ? (
  <span
    ref={ref}
    // @ts-ignore
    style={{
      ...style,
      height: height as ChadNumber,
      width: width as ChadNumber,
      ['--delay' as string]: (typeof delay === 'undefined' || typeof delay === 'number' || delay === true) && `-${useMemo(() => Math.random() * Math.abs(Number(delay || 1)), [])}s`
    }}

    className={skeletonVariants({
      variant,
      width: show && !props.children ? null : width as VariantProps<typeof skeletonVariants>['width'],
      height: show && !props.children ? null : height as VariantProps<typeof skeletonVariants>['height'],
      border,
      className
    })}
    {...props} />
) : props.children as never
);
Skeleton.displayName = 'Skeleton';

export { Skeleton, skeletonVariants };
