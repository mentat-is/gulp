import React from 'react'
import s from './styles/Input.module.css'
import { cn } from '@/ui/utils';
import { cva, type VariantProps } from 'class-variance-authority';
import { Icon } from '@/ui/Icon';
import { Label } from './Label';
import { Skeleton } from './Skeleton';
import { Stack } from './Stack';
import { Popover } from './Popover';

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
  export namespace Selector {
    export interface Value {
      key: string;
      label: string;
      icon: Icon.Name;
    }

    interface BaseProps {
      values: Value[];
      ariaLabel?: string;
    }

    export interface SingleProps extends BaseProps {
      multiple?: false;
      selected: string;
      onSelectedChange: (selected: string) => void;
    }

    export interface MultipleProps extends BaseProps {
      multiple: true;
      selected: string[];
      onSelectedChange: (selected: string[]) => void;
    }

    export type Props = SingleProps | MultipleProps;
  }

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
    selector?: Selector.Props;
  }
}

/**
 * Renders an icon-driven single- or multi-value selector inside an Input.
 * @param props Controlled selector configuration and inherited disabled state.
 * @returns An embedded selector trigger and its option menu.
 */
function InputSelector({
  selector,
  disabled,
}: {
  selector: Input.Selector.Props;
  disabled?: boolean;
}): React.ReactElement {
  const [open, setOpen] = React.useState(false);
  const selectedKeys = selector.multiple
    ? selector.selected
    : [selector.selected];
  const selectedValue = selector.multiple
    ? undefined
    : selector.values.find((value) => value.key === selector.selected);
  const triggerIcon = selector.multiple
    ? 'ListChecks'
    : selectedValue?.icon ?? selector.values[0]?.icon ?? 'ListChecks';
  const triggerLabel = selector.ariaLabel ?? selectedValue?.label;

  /**
   * Applies one option while preserving single- and multi-select semantics.
   * @param key Key of the option activated by the user.
   * @returns Nothing.
   */
  const handleValueClick = (key: string): void => {
    if (selector.multiple) {
      const nextSelected = selectedKeys.includes(key)
        ? selectedKeys.filter((selectedKey) => selectedKey !== key)
        : [...selectedKeys, key];
      selector.onSelectedChange(nextSelected);
      return;
    }

    selector.onSelectedChange(key);
    setOpen(false);
  };

  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger asChild>
        <button
          type='button'
          className={s.selectorTrigger}
          disabled={disabled}
          title={triggerLabel}
          aria-label={triggerLabel}
          aria-haspopup='listbox'
        >
          <Icon name={triggerIcon} />
        </button>
      </Popover.Trigger>
      <Popover.Content
        align='end'
        className={s.selectorContent}
        role='listbox'
        aria-multiselectable={selector.multiple || undefined}
      >
        {selector.values.map((value) => {
          const selected = selectedKeys.includes(value.key);
          return (
            <button
              type='button'
              role='option'
              aria-selected={selected}
              className={cn(
                s.selectorOption,
                selected && s.selectorOptionSelected,
              )}
              key={value.key}
              onClick={() => handleValueClick(value.key)}
            >
              <span className={s.selectorCheck}>
                {selected && <Icon name='Check' />}
              </span>
              <Icon name={value.icon} />
              <span>{value.label}</span>
            </button>
          );
        })}
      </Popover.Content>
    </Popover.Root>
  );
}


/** Renders a shared text input with optional leading, trailing, and selector controls. */
const Input = React.forwardRef<HTMLInputElement, Input.Props>(
  ({
    className,
    valid = true,
    revert,
    label,
    skeleton,
    variant,
    disabled,
    type,
    size,
    icon,
    onIconClick,
    iconTitle,
    endIcon,
    onEndIconClick,
    endIconTitle,
    onChange,
    selector,
    ...props
  }, ref) => {
    const classes = cn(
      inputVariants({ variant, size, className }),
      s.wrapper,
      icon || type === 'file' ? s.image : null,
      endIcon ? s.hasEndIcon : null,
      selector ? s.hasSelector : null,
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
            <input id={label} ref={ref} type={variant === 'color' ? 'color' : type} disabled={disabled} {...props} onChange={onChange || (() => { })} />
            <EndIcon />
            {selector && <InputSelector selector={selector} disabled={disabled} />}
          </div>
        </Stack>
      </Skeleton>
    )
  }
)
Input.displayName = 'Input'

export { Input }
