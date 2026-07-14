import s from './styles/Select.module.css'
import * as SelectPrimitive from '@radix-ui/react-select'
import { cn } from '@/ui/utils'
import { Icon, Icon as BaseIcon } from '@/ui/Icon'
import { ComponentRef, forwardRef, createContext, useContext, useState, useCallback, useMemo, useEffect } from 'react'
import { ComponentPropsWithoutRef } from 'react'
import { Checkbox } from './Checkbox'
import { Arrayed } from '@/class/Info'
import { Application } from '@/context/Application.context'
import { Locale } from '@/locales'

export namespace Select {
  export const Root = (props: SelectPrimitive.SelectProps) => {
    return <SelectPrimitive.Root {...props} value={props.value ?? (props.defaultValue ? undefined : "")} />
  }
  export const Group = SelectPrimitive.Group
  export const Value = SelectPrimitive.Value
  export namespace Multi {
    export namespace Context {
      export interface Type {
        isMultiSelect: boolean
        selectedValues: string[]
        onValueChange: (value: string) => void
        onOpenChange?: (open: boolean) => void
      }
    }

    export const Context = createContext<Select.Multi.Context.Type | null>(null);

    export const use = () => useContext(Select.Multi.Context);

    export namespace Root {
      export interface Props extends Omit<SelectPrimitive.SelectProps, 'value' | 'onValueChange' | 'defaultValue'> {
        value?: string[]
        onValueChange?: (value: string[]) => void
        defaultValue?: string[]
        placeholder?: string
        children: React.ReactNode
        open?: boolean
        onOpenChange?: (open: boolean) => void
      }
    }

    export const Root = ({
      value,
      onValueChange,
      children,
      defaultValue,
      open,
      onOpenChange,
      ...props
    }: Select.Multi.Root.Props) => {
      const [selectedValues, setSelectedValues] = useState<string[]>(defaultValue ?? value ?? [])
      const [isOpen, setIsOpen] = useState(false);

      useEffect(() => {
        if (value !== undefined) {
          setSelectedValues(value)
        }
      }, [value])

      const handleValueChange = useCallback((newValue: string) => {
        const updatedValues = selectedValues.includes(newValue)
          ? selectedValues.filter(v => v !== newValue)
          : [...selectedValues, newValue]

        if (value === undefined) {
          setSelectedValues(updatedValues)
        }
        onValueChange?.(updatedValues)
      }, [selectedValues, onValueChange, value])

      const handleOpenChange = useCallback((newOpen: boolean) => {
        setIsOpen(newOpen)
        onOpenChange?.(newOpen)
      }, [onOpenChange])

      const contextValue: Select.Multi.Context.Type = {
        isMultiSelect: true,
        selectedValues,
        onValueChange: handleValueChange,
        onOpenChange: handleOpenChange
      }

      return (
        <Select.Multi.Context.Provider value={contextValue}>
          <SelectPrimitive.Root open={open ?? isOpen} onOpenChange={handleOpenChange} {...props}>
            {children}
          </SelectPrimitive.Root>
        </Select.Multi.Context.Provider>
      )
    }

    export namespace Value {
      export interface Props {
        placeholder?: string;
        text?: string | ((length: number | string) => string);
        icon?: Arrayed<Icon.Name>;
      }
    }

    export const Value = ({ placeholder, text, icon }: Value.Props) => {
      const multiSelect = Select.Multi.use()
      const { t } = Locale.use()

      if (!multiSelect?.isMultiSelect) return null

      const { selectedValues } = multiSelect

      const Text = useMemo(() => {
        if (selectedValues.length === 0) {
          return placeholder || t('common.choosePlaceholder');
        }

        if (selectedValues.length === 1) {
          return typeof text === 'function' ? text(selectedValues[0]) : selectedValues[0];
        }

        return typeof text === 'function' ? text(selectedValues.length) : text ?? selectedValues.length;
      }, [placeholder, text, selectedValues.length, t]);

      // icon can be array of icons, icon depends on len. if len > icon.len use icon.last (in fact its simplier than sounds like)
      return (
        <>
          <Icon name={(Array.isArray(icon) ? icon[Math.min(selectedValues.length, icon.length) - 1] : icon) || 'Status'} color='currentColor' />
          {Text}
        </>
      )
    }

    export const ToggleAll = ({
      label,
      onToggle,
      checked
    }: {
      label?: string,
      onToggle: (checked: boolean) => void,
      checked: boolean
    }) => {

      const isSelected = checked;
      const { t } = Locale.use()

      const handleClick = (event: React.MouseEvent) => {
        event.preventDefault()
        onToggle(!isSelected)
      }

      return (
        <div className={cn(s.item, isSelected && s.selected)} onClick={handleClick} role="option" aria-selected={isSelected}>
          <Checkbox checked={isSelected} />
          <span className={s.itemText}>{label ?? t('common.selectAll')}</span>
        </div>
      )
    }
  }

  /**
   * Use data-no-icon to omit icon from trigger
   */
  export const Trigger = forwardRef<
    ComponentRef<typeof SelectPrimitive.Trigger>,
    ComponentPropsWithoutRef<typeof SelectPrimitive.Trigger>
  >(({ className, children, ...props }, ref) => (
    <SelectPrimitive.Trigger ref={ref} className={cn(s.trigger, className)} {...props}>
      {children}
      <Select.Icon style={{ marginInlineStart: 'auto' }} name='ChevronDown' />
    </SelectPrimitive.Trigger>
  ))

  export const Icon = ({ name, className, color = 'currentColor', ...props }: BaseIcon.Props) => (
    <SelectPrimitive.Icon asChild>
      <BaseIcon name={name} className={cn(s.icon, className)} color={color} {...props} />
    </SelectPrimitive.Icon>
  )

  export namespace ScrollButton {
    export const Up = forwardRef<
      ComponentRef<typeof SelectPrimitive.ScrollUpButton>,
      ComponentPropsWithoutRef<typeof SelectPrimitive.ScrollUpButton>
    >(({ className, ...props }, ref) => (
      <SelectPrimitive.ScrollUpButton
        ref={ref}
        className={cn(s.scroll, s.up, className)}
        {...props}
      >
        <Select.Icon name='ChevronUp' />
      </SelectPrimitive.ScrollUpButton>
    ))
    export const Down = forwardRef<
      ComponentRef<typeof SelectPrimitive.ScrollDownButton>,
      ComponentPropsWithoutRef<typeof SelectPrimitive.ScrollDownButton>
    >(({ className, ...props }, ref) => (
      <SelectPrimitive.ScrollDownButton ref={ref} className={cn(s.scroll, s.down, className)} {...props}>
        <Select.Icon name='ChevronDown' />
      </SelectPrimitive.ScrollDownButton>
    ))
  }

  export const Content = forwardRef<
    ComponentRef<typeof SelectPrimitive.Content>,
    ComponentPropsWithoutRef<typeof SelectPrimitive.Content> & { container?: HTMLElement | null }
  >(({ className, children, position = 'popper', container, ...props }, ref) => {
    const ctx = useContext(Application.Context);
    const resolvedContainer = container || ctx?.currentDocument?.body || globalThis.document?.body;

    return (
      <SelectPrimitive.Portal container={resolvedContainer}>
        <SelectPrimitive.Content
          ref={ref}
          className={cn(s.content, position === 'popper' && s.popper, className)}
          position={position}
          {...props}
        >
          <Select.ScrollButton.Up />
          <SelectPrimitive.Viewport className={cn(s.primitive, position === 'popper' && s.podder)}>
            {children}
          </SelectPrimitive.Viewport>
          <Select.ScrollButton.Down />
        </SelectPrimitive.Content>
      </SelectPrimitive.Portal>
    );
  });

  export const Label = forwardRef<
    ComponentRef<typeof SelectPrimitive.Label>,
    ComponentPropsWithoutRef<typeof SelectPrimitive.Label>
  >(({ className, ...props }, ref) => (
    <SelectPrimitive.Label ref={ref} className={cn(s.label, className)} {...props} />
  ))

  export const Item = forwardRef<
    ComponentRef<typeof SelectPrimitive.Item>,
    ComponentPropsWithoutRef<typeof SelectPrimitive.Item> & {
      value: string
    }
  >(({ className, children, value, ...props }, ref) => {
    const multiSelect = Select.Multi.use();

    if (!multiSelect?.isMultiSelect) {
      return (
        <SelectPrimitive.Item ref={ref} className={cn(s.item, className)} value={value} {...props}>
          <SelectPrimitive.ItemIndicator>
            <Select.Icon name='Check' />
          </SelectPrimitive.ItemIndicator>
          <SelectPrimitive.ItemText>{children}</SelectPrimitive.ItemText>
        </SelectPrimitive.Item>
      )
    }

    const isSelected = multiSelect.selectedValues.includes(value)

    const handleClick = (event: React.MouseEvent) => {
      event.preventDefault()
      multiSelect.onValueChange(value)
    }

    return (
      <div className={cn(s.item, isSelected && s.selected, className)} onClick={handleClick} role="option" aria-selected={isSelected} {...props}>
        <Checkbox checked={isSelected} />
        <span className={s.itemText}>{children}</span>
      </div>
    )
  })

  export const Separator = forwardRef<
    ComponentRef<typeof SelectPrimitive.Separator>,
    ComponentPropsWithoutRef<typeof SelectPrimitive.Separator>
  >(({ className, ...props }, ref) => (
    <SelectPrimitive.Separator ref={ref} className={cn(s.separator, className)} {...props} />
  ))
}
