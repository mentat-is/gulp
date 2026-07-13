import { Icon } from '@/ui/Icon'
import { cn } from '@/ui/utils'
import s from './styles/Point.module.css'
import { Button } from './Button'

export namespace Point {
  export interface Props extends Omit<Button.Props, 'type'> {
    x: number
    y: number
    icon: Icon.Name
    accent: string
    name: string
    description?: string
    type: 'link' | 'note'
  }
}

export function Point({
  x,
  y,
  icon,
  accent,
  className,
  name,
  description,
  type,
  ...props
}: Point.Props) {
  return (
    <Button
      shape='icon'
      tabIndex={-1}
      variant="secondary"
      className={cn(type === 'link' && s.round, className, s.target)}
      style={{ ...props.style, left: x, top: y, borderColor: accent }}
      {...props}
    >
      {props.children ?? <Icon name={icon} color={accent} />}
      <hr style={{ background: accent }} />
      {name && <p className={s.desc} style={{ y, x, borderColor: accent }}>
        {name}
      </p>}
    </Button>
  )
}
