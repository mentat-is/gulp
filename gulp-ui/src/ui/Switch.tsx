import React from 'react'
import * as SwitchPrimitives from '@radix-ui/react-switch'
import s from './styles/Switch.module.css'
import { cn } from '@/ui/utils'
import { Icon } from '@/ui/Icon'

export type { SwitchProps } from '@radix-ui/react-switch'

const Switch = React.forwardRef<
  React.ComponentRef<typeof SwitchPrimitives.Root>,
  React.ComponentPropsWithoutRef<typeof SwitchPrimitives.Root> & {
    icons?: [Icon.Name, Icon.Name]
  }
>(({ className, icons, ...props }, ref) => (
  <SwitchPrimitives.Root
    className={cn(s.switch, className)}
    {...props}
    ref={ref}
  >
    <SwitchPrimitives.Thumb className={s.thumb} />
    {icons?.map((icon, i) => {
      return <Icon key={icon} className={cn(s.icon, i === 0 ? s.left : s.right)} name={icon} size={12} />
    })}
  </SwitchPrimitives.Root>
))
Switch.displayName = SwitchPrimitives.Root.displayName

export { Switch }
