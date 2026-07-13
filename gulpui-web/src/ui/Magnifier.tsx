import React from 'react'
import s from './styles/Magnifier.module.css'
import { Stack } from './Stack'

interface MagnifierProps {
  isVisible: boolean
  self: React.RefObject<HTMLCanvasElement>
  mousePosition: { x: number; y: number }
  size?: number
}

export const Magnifier: React.FC<MagnifierProps> = ({
  isVisible,
  self,
  mousePosition,
  size = 200,
}) => {
  if (!isVisible) return null

  return (
    <Stack
      style={{
        top: `${mousePosition.y}px`,
        left: `${mousePosition.x}px`,
        height: size,
        width: size
      }}
      className={s.magnifier}
      pos='absolute'>
      <canvas
        ref={self}
        width={size}
        height={size}
      />
      <i className={s.glass} />
    </Stack>

  )
}
