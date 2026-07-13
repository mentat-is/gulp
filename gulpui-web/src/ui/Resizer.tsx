import { Stack } from './Stack'
import s from './styles/Resizer.module.css'
import { Application } from '@/context/Application.context'
import { Icon } from '@/ui/Icon'
import { useRef, useEffect } from 'react'
import { MinMax } from '@/class/Info'
import { cn } from '@/ui/utils'

export namespace Resizer {
  export interface Props extends Stack.Props {
    init: number
    set: (num: number) => void
    horizontal?: boolean
    limits?: MinMax
  }
}

export function Resizer({
  init,
  set,
  horizontal,
  limits,
  ...props
}: Resizer.Props) {
  const { Info } = Application.use()
  const ref = useRef<HTMLDivElement>(null)
  const pos = useRef(0)
  const start = useRef(init)
  const is = useRef(false)

  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      if (!is.current) return

      const delta = pos.current - (horizontal ? e.clientY : e.clientX)
      const end = Math.max(
        (horizontal ? window.innerHeight : window.innerWidth) *
        (limits ? limits.min : 0.2),
        Math.min(
          start.current + delta,
          (horizontal ? window.innerHeight : window.innerWidth) *
          (limits ? limits.max : 0.5),
        ),
      )

      set(end)
    }

    const onUp = () => (is.current = false)

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
  }, [])

  const onMouseDown = (e: React.MouseEvent) => {
    is.current = true
    pos.current = e.clientX
    start.current = Info.app.timeline.dialogSize
  }

  return (
    <Stack
      ref={ref}
      className={cn(s.resize, horizontal && s.horizontal)}
      ai="center"
      jc="center"
      pos="absolute"
      onMouseDown={onMouseDown}
      {...props}
    >
      <Icon name={horizontal ? 'MoreHorizontal' : 'MoreVertical'} size={8} />
    </Stack>
  )
}
