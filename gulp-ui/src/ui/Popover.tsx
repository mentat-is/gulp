import React from 'react'
import * as PopoverPrimitive from '@radix-ui/react-popover'
import s from './styles/Popover.module.css'
import { cn } from '@/ui/utils'
import { Application } from '@/context/Application.context'

export namespace Popover {
  export const Root = PopoverPrimitive.Root

  export const Trigger = PopoverPrimitive.Trigger

  export const Content = React.forwardRef<
    React.ComponentRef<typeof PopoverPrimitive.Content>,
    React.ComponentPropsWithoutRef<typeof PopoverPrimitive.Content> & { container?: HTMLElement | null }
  >(({ className, align = 'center', sideOffset = 4, container, ...props }, ref) => {
    const ctx = React.useContext(Application.Context);
    const resolvedContainer = container || ctx?.currentDocument?.body || globalThis.document?.body;

    return (
      <PopoverPrimitive.Portal container={resolvedContainer}>
        <PopoverPrimitive.Content
          ref={ref}
          align={align}
          sideOffset={sideOffset}
          className={cn(s.popover, className)}
          {...props}
        />
      </PopoverPrimitive.Portal>
    );
  })
  Content.displayName = PopoverPrimitive.Content.displayName
}
