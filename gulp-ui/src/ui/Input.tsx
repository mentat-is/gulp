import React from 'react'
import s from './styles/Input.module.css'
import { cn } from '@/ui/utils';
import { cva, type VariantProps } from 'class-variance-authority';
import { Icon } from '@/ui/Icon';
import { Label } from './Label';
import { Skeleton } from './Skeleton';
import { Stack } from './Stack';

const inputVariants = cva(s.button, {
  variants: {
    variant: {
      default: s.default,
      highlighted: s.highlighted,
      color: s.color
    },
    size: {
      default: s.defaultSize,
      sm: s.sm,
      lg: s.lg
    },
  },
  defaultVariants: {
    variant: 'default',
    size: 'default',
  },
});

export namespace Input {
  export interface Props extends Omit<React.InputHTMLAttributes<HTMLInputElement>, 'size'>, VariantProps<typeof inputVariants> {
    icon?: Icon.Name | null;
    onIconClick?: () => void;
    iconTitle?: string;
    endIcon?: Icon.Name | null;
    onEndIconClick?: () => void;
    endIconTitle?: string;
    label?: string;
    revert?: boolean;
    skeleton?: boolean;
    valid?: boolean;
    onChange?: React.ChangeEventHandler<HTMLInputElement>;
  }
}


const Input = React.forwardRef<HTMLInputElement, Input.Props>(
  ({ className, valid = true, revert, label, skeleton, variant, disabled, type, size, icon, onIconClick, iconTitle, endIcon, onEndIconClick, endIconTitle, onChange, ...props }, ref) => {
    const classes = cn(
      inputVariants({ variant, size, className }),
      s.wrapper,
      icon || type === 'file' ? s.image : null,
      endIcon ? s.hasEndIcon : null,
      revert && s.revert,
      !valid && s.invalid,
      disabled && s.disabled
    );

    const InputIcon = () => {
      if (!icon && (type !== 'file' || icon === null)) {
        return null;
      }

      const iconElement = <Icon variant='dimmed' name={icon ?? 'Upload'} />;

      if (onIconClick) {
        return (
          <button
            type="button"
            className={s.iconButton}
            onClick={onIconClick}
            title={iconTitle}
          >
            {iconElement}
          </button>
        );
      }

      return iconElement;
    }

    const EndIcon = () => {
      if (!endIcon) {
        return null;
      }

      const iconElement = <Icon variant='dimmed' name={endIcon} />;

      if (onEndIconClick) {
        return (
          <button
            type="button"
            className={cn(s.iconButton, s.endIconButton)}
            onClick={onEndIconClick}
            title={endIconTitle}
          >
            {iconElement}
          </button>
        );
      }

      return <span className={s.endIcon}>{iconElement}</span>;
    }

    return (
      <Skeleton show={skeleton}>
        <Stack dir='column' gap={6} ai='flex-start' data-input style={{ width: '100%' }}>
          <Label htmlFor={label} value={label} />
          <div className={classes}>
            <InputIcon />
            <input id={label} ref={ref} type={variant === 'color' ? 'color' : type} {...props} onChange={onChange || (() => { })} />
            <EndIcon />
          </div>
        </Stack>
      </Skeleton>
    )
  }
)
Input.displayName = 'Input'

export { Input }
