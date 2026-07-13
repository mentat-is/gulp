import React from 'react'
import * as TooltipPrimitive from '@radix-ui/react-tooltip'
import s from './styles/Tooltip.module.css'
import { cn } from '@/ui/utils'
import { Application } from '@/context/Application.context'

const TooltipProvider = TooltipPrimitive.Provider

const Tooltip = TooltipPrimitive.Root

const TooltipTrigger = TooltipPrimitive.Trigger

const TooltipContent = React.forwardRef<
  React.ElementRef<typeof TooltipPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content> & { container?: HTMLElement | null }
>(({ className, sideOffset = 4, container: portalContainer, ...props }, ref) => {
  const ctx = React.useContext(Application.Context);
  const [detectedContainer, setDetectedContainer] = React.useState<HTMLElement | null>(null);
  const triggerRef = React.useRef<HTMLDivElement>(null);
  const resolvedContainer = portalContainer || ctx?.currentDocument?.body || detectedContainer;

  React.useLayoutEffect(() => {
    if (triggerRef.current) {
      setDetectedContainer(triggerRef.current.ownerDocument.body);
    }
  }, []);

  return (
    <>
      <div ref={triggerRef} style={{ display: 'none' }} />
      {resolvedContainer && (
        <TooltipPrimitive.Portal container={resolvedContainer}>
          <TooltipPrimitive.Content
            ref={ref}
            sideOffset={sideOffset}
            className={cn(s.tooltip, className)}
            {...props}
          />
        </TooltipPrimitive.Portal>
      )}
    </>
  );
})
TooltipContent.displayName = TooltipPrimitive.Content.displayName

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider }
