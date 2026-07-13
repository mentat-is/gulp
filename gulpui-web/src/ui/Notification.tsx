import { Icon } from "@/ui/Icon";
import s from './styles/Notification.module.css'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/ui/utils'
import { Stack } from "./Stack";

export const notificationVariants = cva(s.notification, {
  variants: {
    variant: {
      default: s.default,
      success: s.success,
      warning: s.warning,
      error: s.error,
      informational: s.informational
    }
  },
  defaultVariants: {
    variant: 'default',
  },
})

export namespace Notification {
  export interface Props extends Stack.Props,
    VariantProps<typeof notificationVariants> {
    value?: string
    icon?: Icon.Name
  }

  export type Variant = Props['variant'];
}

export function Notification({ children, value, icon = 'SquareDashed', className, variant, ...props }: Notification.Props) {
  return (
    <Stack jc='center' className={cn(notificationVariants({ variant }), s.atention, className)} gap={0}>
      <Icon name={icon} color='currentColor' />
      {children || <p>{value}</p>}
    </Stack>
  )
}
