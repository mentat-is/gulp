import { useCallback, useState, useEffect, useRef } from 'react'

interface MousePosition {
  x: number
  y: number
}

export interface MagnifierFrameRequest {
  x: number
  y: number
  sourceSize: number
  destinationSize: number
}

export type MagnifierFrameProvider = (
  request: MagnifierFrameRequest,
) => Promise<ImageBitmap | null>

/**
 * Tracks magnifier input and paints either the local canvas or worker-provided frames.
 * @param canvas_ref Primary timeline canvas reference.
 * @param dependencies Values that invalidate the magnifier contents.
 * @param magnifierSize Logical magnifier source size.
 * @param magnificationFactor Scale applied to the sampled source region.
 * @param frameProvider Optional worker bitmap provider used after canvas ownership transfer.
 * @returns Magnifier refs, visibility state, pointer state, and DOM event handlers.
 */
export const useMagnifier = (
  canvas_ref: React.RefObject<HTMLCanvasElement>,
  dependencies: unknown[],
  magnifierSize = 100,
  magnificationFactor = 2,
  frameProvider?: MagnifierFrameProvider,
) => {
  const magnifier_ref = useRef<HTMLCanvasElement>(null as unknown as HTMLCanvasElement)
  const [isAltPressed, setIsAltPressed] = useState<boolean>(false)
  const [mousePosition, setMousePosition] = useState<MousePosition>({
    x: 0,
    y: 0,
  })
  const latestMousePositionRef = useRef<MousePosition>(mousePosition)
  const pendingMouseFrameRef = useRef<number | null>(null)
  const magnifierRequestSequenceRef = useRef<number>(0)
  const displayedBitmapRef = useRef<ImageBitmap | null>(null)
  const isAltPressedRef = useRef<boolean>(isAltPressed)
  isAltPressedRef.current = isAltPressed

  /**
   * Draws the magnified canvas snapshot around the latest mouse position.
   * @returns Nothing.
   */
  const drawMagnifier = useCallback(async (): Promise<void> => {
    const canvas = canvas_ref.current
    const magnifier = magnifier_ref.current
    if (!magnifier || !canvas) return

    const magnifierCtx = magnifier.getContext('2d')
    if (!magnifierCtx) return

    const sourceSize = magnifierSize / magnificationFactor
    const sourceX = Math.max(0, mousePosition.x - sourceSize / 2)
    const sourceY = Math.max(0, mousePosition.y - sourceSize / 2)
    const destinationSize = magnifierSize * magnificationFactor

    if (frameProvider) {
      const requestSequence = ++magnifierRequestSequenceRef.current
      const bitmap = await frameProvider({
        x: mousePosition.x,
        y: mousePosition.y,
        sourceSize,
        destinationSize,
      })

      if (!bitmap) return
      if (requestSequence !== magnifierRequestSequenceRef.current) {
        bitmap.close()
        return
      }

      magnifierCtx.clearRect(0, 0, magnifier.width, magnifier.height)
      magnifierCtx.imageSmoothingEnabled = false
      magnifierCtx.drawImage(
        bitmap,
        0,
        0,
        magnifier.width,
        magnifier.height,
      )
      displayedBitmapRef.current?.close()
      displayedBitmapRef.current = bitmap
      return
    }

    magnifierCtx.clearRect(0, 0, magnifier.width, magnifier.height)
    magnifierCtx.imageSmoothingEnabled = false
    magnifierCtx.drawImage(
      canvas,
      sourceX,
      sourceY,
      sourceSize,
      sourceSize,
      0,
      0,
      destinationSize,
      destinationSize,
    )
  }, [
    canvas_ref,
    frameProvider,
    magnificationFactor,
    magnifierSize,
    mousePosition,
  ])

  useEffect(() => {
    isAltPressed && drawMagnifier()
  }, [isAltPressed, mousePosition, dependencies, drawMagnifier])

  useEffect(() => {
    return () => {
      magnifierRequestSequenceRef.current++
      displayedBitmapRef.current?.close()
      displayedBitmapRef.current = null
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
   * Tracks a coordinator-provided pointer position and updates React only while active.
   * @param position Canvas-relative pointer coordinates from the shared display tick.
   * @returns Nothing.
   */
  const handleMouseMove = useCallback((position: MousePosition): void => {
    latestMousePositionRef.current = position
    if (isAltPressedRef.current) scheduleMousePositionCommit()
  }, [scheduleMousePositionCommit])

  /**
   * Toggles the magnifier when the configured modifier key is pressed.
   * @param event Keyboard event emitted by the canvas wrapper.
   * @returns Nothing.
   */
  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>): void => {
    if (event.ctrlKey) {
      setIsAltPressed((value) => !value)
    }
  }

  useEffect(() => {
    if (!isAltPressed) return
    setMousePosition(latestMousePositionRef.current)
  }, [isAltPressed])

  return {
    toggler: handleKeyDown,
    move: handleMouseMove,
    magnifier_ref,
    isAltPressed,
    mousePosition,
  }
}
