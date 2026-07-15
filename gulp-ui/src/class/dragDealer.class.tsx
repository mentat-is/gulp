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
  moveScroll: (deltaX: number, deltaY: number, inputTimestamp: number) => void

  /**
   * Creates drag state backed by the Canvas-owned scroll coordinator.
   * @param props Application state, Canvas reference, and scroll callback.
   */
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
   * Stops the current drag interaction.
   * @returns Nothing.
   */
  dragStop = (): void => {
    this.clicked = false
  }

  /**
   * Routes drag movement to the Canvas-owned display-tick coordinator.
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

    this.moveScroll(newX, newY, performance.now())
  }
}
