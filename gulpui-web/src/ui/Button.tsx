import * as React from 'react';
import { Slot } from '@radix-ui/react-slot';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '@/ui/utils';
import s from './styles/Button.module.css';
import { Icon } from '@/ui/Icon';
import { Spinner } from './Spinner';
import { translate } from '@/locales/core';

const buttonVariants = cva(s.button, {
  variants: {
    variant: {
      default: s.default,
      secondary: s.secondary,
      tertiary: s.tertiary,
      glass: s.glass,
      destructive: s.destructive,
      disabled: s.disabled
    },
    size: {
      default: s.defaultSize,
      sm: s.sm,
      md: s.md,
      lg: s.lg,
    },
    shape: {
      default: '',
      icon: s.icon
    },
    disabled: {
      true: s.disabled
    }
  },
  defaultVariants: {
    variant: 'default',
    size: 'default',
    shape: 'default'
  },
});

export namespace Button {
  export interface Props extends Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'disabled'>, VariantProps<typeof buttonVariants> {
    asChild?: boolean;
    icon?: Icon.Name;
    revert?: boolean;
    loading?: boolean;
    placeholder?: string;
    rounded?: boolean;
  }

  export type Variant = Props['variant'];

  export type Size = Props['size'];
}

const Button = React.forwardRef<HTMLButtonElement, Button.Props>(
  ({ className, shape, variant, rounded, size, icon, revert, disabled, loading, asChild = false, placeholder, ...props }, ref) => {
    const Comp = asChild ? Slot : 'button';
    const paddingClass = icon ? (props.children ? (revert ? s.revert : s.withImage) : s.onlyImage) : null;

    const children = !asChild && props.children;

    if (!shape && icon && !children) {
      shape = 'icon'
    }

    const color = convertButtonVariantToSpinnerColor(variant);
    const iconSize = convertButtonVariantToIconSize(size);

    return (
      <Comp
        className={cn(buttonVariants({ variant, size, className, disabled, shape }), paddingClass, loading && s.loading, rounded && s.rounded)}
        ref={ref}
        {...props}>
        {asChild ? props.children : (loading
          ? (
            <>
              <Spinner color={color} size={iconSize + 4} />
              {shape !== 'icon' ? placeholder ?? translate('common.loading') : null}
            </>
          )
          : <>
            {icon && <Icon name={icon} size={iconSize} color='currentColor' />}
            {children}
          </>
        )}
      </Comp>
    )
  }
);
Button.displayName = 'Button';

const spinner_colors = {
  default: 'black',
  secondary: 'white',
  tertiary: 'white',
  glass: 'var(--gray-700)',
  destructive: 'white',
  disabled: 'var(--gray-700)',
};
const convertButtonVariantToSpinnerColor = (variant: Button.Variant): string => spinner_colors[variant || 'default'];

const icon_sizes = { default: 16, sm: 12, md: 18, lg: 20, icon: 16 };
const convertButtonVariantToIconSize = (size: Button.Size): NonNullable<Icon.Size> => icon_sizes[size || 'default'];


export { Button, buttonVariants };
