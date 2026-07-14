import React from 'react'
import { Slot } from '@radix-ui/react-slot'
import s from './styles/Textarea.module.css'
import { Icon } from '@/ui/Icon'
import { cn } from '@/ui/utils'

export interface TextareaProps
  extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  asChild?: boolean
  img?: Icon.Name
  revert?: boolean
  loading?: boolean
  fixed?: boolean
  error?: boolean
}

const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(
  ({ className, asChild = false, fixed, error, ...props }, ref) => {
    const Comp = asChild ? Slot : 'textarea'

    return (
      <Comp className={cn(s.textarea, className, fixed && s.fixed, error && s.invalid)} ref={ref} {...props}></Comp>
    )
  },
)
Textarea.displayName = 'Textarea'

export { Textarea }
