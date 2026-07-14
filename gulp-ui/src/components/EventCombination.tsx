import s from './styles/Combination.module.css'
import { HTMLAttributes, useMemo } from 'react'
import { cn } from '@/ui/utils'
import { EventIndicator } from '@/dialogs/Event.dialog'
import { Doc } from '@/entities/Doc'

interface EventCombinationProps extends HTMLAttributes<HTMLDivElement> {
  event: Doc.Type
}

export function EventCombination({
  event,
  children,
  className,
  ...props
}: EventCombinationProps) {
  const icon = useMemo(() => <EventIndicator event={event} />, [event]);
  return (
    <div className={cn(s.unit, className)} {...props}>
      {icon}
      <div className={s.text}>
        <p className={s.top}>{event._id}</p>
        <p className={s.bottom}>{event['gulp.source_id']}</p>
      </div>
      {children}
    </div>
  )
}
