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

/**
 * Provides Canvas drag and range-selection handlers.
 * @param timeline Timeline canvas receiving pointer gestures.
 * @param queueScrollDelta Canvas-owned callback that coalesces drag movement.
 * @returns Drag state and React handlers used by the Canvas wrapper.
 */
export const useDrugs = (
  timeline: RefObject<HTMLCanvasElement>,
  queueScrollDelta: (deltaX: number, deltaY: number, inputTimestamp: number) => void,
) => {
  const { Info, highlightsOverlay } = Application.use()
  const [resize, setResize] = useState<StartEnd>(StartEndBase)
  const [isResizing, setIsResizing] = useState(false)

  /**
   * Applies a drag delta to both scroll axes with one external-store notification.
   * @param deltaX Horizontal movement produced by canvas dragging.
   * @param deltaY Vertical movement produced by canvas dragging.
   * @param inputTimestamp Native input timestamp represented by the drag movement.
   * @returns Nothing.
   */
  const moveScroll = useCallback(
    (deltaX: number, deltaY: number, inputTimestamp: number): void => {
      queueScrollDelta(deltaX, deltaY, inputTimestamp)
    },
    [queueScrollDelta],
  )

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

  /**
   * Starts a Canvas drag or alternate-key range selection.
   * @param event React mouse event emitted by the Canvas wrapper.
   * @returns Nothing.
   */
  const handleMouseDown = useCallback((event: React.MouseEvent): void => {
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

  /**
   * Updates an active drag or range selection without measuring layout while idle.
   * @param event React mouse movement emitted by the Canvas wrapper.
   * @returns Nothing.
   */
  const handleMouseMove = useCallback((event: React.MouseEvent): void => {
    if (!isResizing && !dragState.current.clicked) {
      return
    }
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

  /**
   * Finalizes Canvas dragging or range selection when the pointer is released.
   * @param event Native mouse event ending the interaction.
   * @returns Toast identifier for an invalid selection, otherwise nothing.
   */
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
