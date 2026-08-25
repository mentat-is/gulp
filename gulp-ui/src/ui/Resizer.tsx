import { Stack } from './Stack'
import s from './styles/Resizer.module.css'
import { Icon } from '@/ui/Icon'
import { useRef, useEffect } from 'react'
import type { MinMax } from '@/class/Info'
import { cn } from '@/ui/utils'

export namespace Resizer {
  export interface Props extends Stack.Props {
    init: number
    set: (num: number) => void
    horizontal?: boolean
    limits?: MinMax
    /** Element whose visual size is previewed during the drag. */
    target: React.RefObject<HTMLElement | null>
  }
}

/**
 * Previews a bounded target resize imperatively and commits its final size once.
 * @param props Resize direction, bounds, target element, and persistence callback.
 * @returns Interactive resize handle.
 */
export function Resizer({
  init,
  set,
  horizontal,
  limits,
  target,
  ...props
}: Resizer.Props) {
  const ref = useRef<HTMLDivElement>(null)
  const pos = useRef(0)
  const start = useRef(init)
  const pending = useRef(init)
  const is = useRef(false)

  useEffect(() => {
    /**
     * Previews the bounded target size without publishing application state.
     * @param e Native pointer movement captured at window level.
     * @returns Nothing.
     */
    const onMove = (e: MouseEvent) => {
      if (!is.current) return

      const pointerPosition = horizontal ? e.clientY : e.clientX
      const viewportSize = horizontal ? window.innerHeight : window.innerWidth
      const delta = pos.current - pointerPosition
      const end = Math.max(
        viewportSize * (limits ? limits.min : 0.2),
        Math.min(
          start.current + delta,
          viewportSize * (limits ? limits.max : 0.5),
        ),
      )

      pending.current = end
      const targetElement = target.current
      if (targetElement) {
        targetElement.style[horizontal ? 'height' : 'width'] = `${end}px`
      }
    }

    /**
     * Commits the final previewed size once when the drag finishes.
     * @returns Nothing.
     */
    const onUp = () => {
      if (!is.current) return
      is.current = false
      if (pending.current === start.current) return
      set(pending.current)
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
  }, [horizontal, limits, set, target])

  /**
   * Captures the current pointer and canonical size at drag start.
   * @param e React mouse event emitted by the resize handle.
   * @returns Nothing.
   */
  const onMouseDown = (e: React.MouseEvent) => {
    e.preventDefault()
    is.current = true
    pos.current = horizontal ? e.clientY : e.clientX
    const targetElement = target.current
    const renderedSize = targetElement
      ? targetElement.getBoundingClientRect()[horizontal ? 'height' : 'width']
      : init
    start.current = renderedSize || init
    pending.current = start.current
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
