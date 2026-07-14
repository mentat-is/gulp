import { useCallback, useState, useEffect, useRef } from 'react'

interface MousePosition {
  x: number
  y: number
}

export const useMagnifier = (
  canvas_ref: React.RefObject<HTMLCanvasElement>,
  dependencies: unknown[],
  magnifierSize = 100,
  magnificationFactor = 2,
) => {
  const magnifier_ref = useRef<HTMLCanvasElement>(null as unknown as HTMLCanvasElement)
  const [isAltPressed, setIsAltPressed] = useState<boolean>(false)
  const [mousePosition, setMousePosition] = useState<MousePosition>({
    x: 0,
    y: 0,
  })
  const latestMousePositionRef = useRef<MousePosition>(mousePosition)
  const pendingMouseFrameRef = useRef<number | null>(null)

  /**
   * Draws the magnified canvas snapshot around the latest mouse position.
   * @returns Nothing.
   */
  const drawMagnifier = useCallback((): void => {
    const canvas = canvas_ref.current
    const magnifier = magnifier_ref.current
    if (!magnifier || !canvas) return

    const ctx = canvas.getContext('2d')
    const magnifierCtx = magnifier.getContext('2d')

    if (ctx && magnifierCtx) {
      magnifierCtx.clearRect(0, 0, magnifier.width, magnifier.height)

      const x = Math.max(
        0,
        mousePosition.x - magnifierSize / (2 * magnificationFactor),
      )
      const y = Math.max(
        0,
        mousePosition.y - magnifierSize / (2 * magnificationFactor),
      )
      const w = magnifierSize / magnificationFactor
      const d = magnifierSize * magnificationFactor

      magnifierCtx.imageSmoothingEnabled = false
      magnifierCtx.drawImage(canvas, x, y, w, w, 0, 0, d, d)
    }
  }, [canvas_ref, magnificationFactor, magnifierSize, mousePosition])

  useEffect(() => {
    isAltPressed && drawMagnifier()
  }, [isAltPressed, mousePosition, dependencies, drawMagnifier])

  useEffect(() => {
    return () => {
      if (pendingMouseFrameRef.current !== null) {
        cancelAnimationFrame(pendingMouseFrameRef.current)
      }
    }
  }, [])

  /**
   * Commits the latest mouse position to React state at most once per frame.
   * @returns Nothing.
   */
  const commitMousePosition = useCallback((): void => {
    pendingMouseFrameRef.current = null
    const nextPosition = latestMousePositionRef.current
    setMousePosition((previousPosition) => {
      if (
        previousPosition.x === nextPosition.x &&
        previousPosition.y === nextPosition.y
      ) {
        return previousPosition
      }

      return nextPosition
    })
  }, [])

  /**
   * Schedules a mouse position state update for the next animation frame.
   * @returns Nothing.
   */
  const scheduleMousePositionCommit = useCallback((): void => {
    if (pendingMouseFrameRef.current !== null) {
      return
    }

    pendingMouseFrameRef.current = requestAnimationFrame(commitMousePosition)
  }, [commitMousePosition])

  /**
   * Tracks the pointer position relative to the canvas without rerendering on every native event.
   * @param event Mouse event emitted by the canvas wrapper.
   * @returns Nothing.
   */
  const handleMouseMove = useCallback(({
    clientX,
    clientY,
  }: React.MouseEvent<HTMLDivElement>): void => {
    if (!canvas_ref.current) {
      return
    }

    const { left, top } = canvas_ref.current.getBoundingClientRect()
    latestMousePositionRef.current = {
      x: clientX - left,
      y: clientY - top,
    }
    scheduleMousePositionCommit()
  }, [canvas_ref, scheduleMousePositionCommit])

  /**
   * Toggles the magnifier when the configured modifier key is pressed.
   * @param event Keyboard event emitted by the canvas wrapper.
   * @returns Nothing.
   */
  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>): void => {
    if (event.ctrlKey) {
      setIsAltPressed((v) => !v)
    }
  }

  return {
    toggler: handleKeyDown,
    move: handleMouseMove,
    magnifier_ref,
    isAltPressed,
    mousePosition,
  }
}
