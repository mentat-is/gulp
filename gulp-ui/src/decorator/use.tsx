import { DragDealer } from '@/class/dragDealer.class'
import { Application } from '@/context/Application.context'
import { scrollStore } from '@/store/scroll.store'
import { StartEnd, StartEndBase } from '@/dto/StartEnd.dto'
import { RefObject, useCallback, useEffect, useRef, useState } from 'react'
import { toast } from 'sonner'
import { translate } from '@/locales'

export function useKeyHandler(key: string) {
  const [isKeyPressed, setIsKeyPressed] = useState(false)

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === key) {
        setIsKeyPressed(true)
      }
    }

    const handleKeyUp = (event: KeyboardEvent) => {
      if (event.key === key) {
        setIsKeyPressed(false)
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    window.addEventListener('keyup', handleKeyUp)

    return () => {
      window.removeEventListener('keydown', handleKeyDown)
      window.removeEventListener('keyup', handleKeyUp)
    }
  }, [key])

  return [isKeyPressed]
}

export const useDrugs = (timeline: RefObject<HTMLCanvasElement>) => {
  const { Info, highlightsOverlay } = Application.use()
  const [resize, setResize] = useState<StartEnd>(StartEndBase)
  const [isResizing, setIsResizing] = useState(false)

  /**
   * Applies a drag delta to both scroll axes with one external-store notification.
   * @param deltaX Horizontal movement produced by canvas dragging.
   * @param deltaY Vertical movement produced by canvas dragging.
   * @returns Nothing.
   */
  const moveScroll = useCallback((deltaX: number, deltaY: number): void => {
    scrollStore.setScroll(
      Math.round(scrollStore.getX() + deltaX),
      Math.round(scrollStore.getY() + deltaY),
    )
  }, [])

  const dragState = useRef(
    new DragDealer({ info: Info, timeline, moveScroll }),
  )

  useEffect(() => {
    dragState.current = new DragDealer({
      info: Info,
      timeline,
      moveScroll,
    })
  }, [timeline, moveScroll])

  const handleMouseDown = useCallback((event: React.MouseEvent) => {
    if (highlightsOverlay) {
      return;
    }

    dragState.current.clicked = true;
    dragState.current.dragging = false;
    dragState.current.dragStart(event)
    const rect = timeline.current?.getBoundingClientRect()
    if (!rect) {
      return
    }

    if (event.altKey) {
      setResize({ start: event.clientX - rect.x, end: event.clientX - rect.x })
      setIsResizing(true)
    }
  }, [highlightsOverlay, dragState])

  const handleMouseMove = useCallback((event: React.MouseEvent) => {
    const rect = timeline.current?.getBoundingClientRect()
    if (!rect) {
      return
    }

    if (isResizing) {
      setResize((prev) => ({ ...prev, end: event.clientX - rect.x }))
      return
    }

    dragState.current.dragMove(event)
  }, [isResizing, highlightsOverlay, dragState])

  const handleMouseUpOrLeave = useCallback((event: MouseEvent) => {
    const wasDragging = dragState.current.dragging;
    const wasClicked = dragState.current.clicked;

    dragState.current.dragging = false;
    dragState.current.clicked = false;

    if (event.type === 'mouseup' && wasClicked && !wasDragging && !isResizing) {
      // @ts-ignore
      window.xxc(event);
    }

    event.preventDefault()
    dragState.current.dragStop()

    if (isResizing) {
      const min = Math.min(resize.end, resize.start)
      const max = Math.max(resize.end, resize.start)
      const scale =
        (document.getElementById('canvas')!.clientWidth *
          Info.app.timeline.scale) /
        (max - min)

      if (!isFinite(scale)) return toast(translate('timeline.selectedFrameTooSmall'))

      scrollStore.setScrollX(x => (x + min) * (scale / Info.app.timeline.scale))
      setTimeout(() => {
        Info.setTimelineScale(scale)
      }, 10)
    }

    setResize(StartEndBase)
    setIsResizing(false)
  }, [dragState, isResizing, resize, Info.app.timeline.scale]);

  return {
    dragState,
    resize,
    isResizing,
    handleMouseDown,
    handleMouseMove,
    handleMouseUpOrLeave,
  }
}
