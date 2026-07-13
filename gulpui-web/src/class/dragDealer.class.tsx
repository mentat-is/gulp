import React from 'react'
import { Info } from './Info'

interface DragDealerProps {
  info: DragDealer['info']
  timeline: DragDealer['timeline']
  moveScroll: DragDealer['moveScroll']
}

export class DragDealer implements DragDealerProps {
  clicked: boolean
  y: number
  x: number
  info: Info
  dragging: boolean
  timeline: React.RefObject<HTMLCanvasElement>
  moveScroll: (deltaX: number, deltaY: number) => void
  private pendingDeltaX: number
  private pendingDeltaY: number
  private pendingScrollFrame: number | null

  constructor({
    info,
    timeline,
    moveScroll,
  }: DragDealerProps) {
    this.clicked = false
    this.x = 0
    this.y = 0
    this.info = info
    this.timeline = timeline
    this.moveScroll = moveScroll
    this.dragging = false
    this.pendingDeltaX = 0
    this.pendingDeltaY = 0
    this.pendingScrollFrame = null
  }

  /**
   * Stores the pointer position where a drag interaction begins.
   * @param ev Mouse event that started the drag interaction.
   * @returns Nothing.
   */
  dragStart = (ev: React.MouseEvent): void => {
    this.y = ev.clientY
    this.x = ev.clientX
  }

  /**
   * Stops the current drag interaction and flushes any queued scroll delta.
   * @returns Nothing.
   */
  dragStop = (): void => {
    this.clicked = false
    if (this.pendingScrollFrame !== null) {
      cancelAnimationFrame(this.pendingScrollFrame)
      this.pendingScrollFrame = null
    }
    this.flushPendingScroll()
  }

  /**
   * Accumulates drag movement and applies it once per animation frame.
   * @param ev Mouse event emitted during a drag interaction.
   * @returns Nothing.
   */
  dragMove = (ev: React.MouseEvent): void => {
    if (!this.clicked) return

    this.dragging = true

    const newY = this.y - ev.clientY
    const newX = this.x - ev.clientX

    this.y = ev.clientY
    this.x = ev.clientX

    this.pendingDeltaX += newX
    this.pendingDeltaY += newY
    this.schedulePendingScroll()
  }

  /**
   * Schedules the accumulated drag delta for the next paint frame.
   * @returns Nothing.
   */
  private schedulePendingScroll = (): void => {
    if (this.pendingScrollFrame !== null) {
      return
    }

    this.pendingScrollFrame = requestAnimationFrame(() => {
      this.pendingScrollFrame = null
      this.flushPendingScroll()
    })
  }

  /**
   * Applies accumulated drag movement to the shared scroll store.
   * @returns Nothing.
   */
  private flushPendingScroll = (): void => {
    if (this.pendingDeltaX === 0 && this.pendingDeltaY === 0) {
      return
    }

    const deltaX = this.pendingDeltaX
    const deltaY = this.pendingDeltaY
    this.pendingDeltaX = 0
    this.pendingDeltaY = 0
    this.moveScroll(deltaX, deltaY)
  }
}
