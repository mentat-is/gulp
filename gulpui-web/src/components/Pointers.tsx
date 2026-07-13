import { Application } from '@/context/Application.context'
import { useScroll } from '@/store/scroll.store'
import s from './styles/Pointers.module.css'
import { Icon } from '@/ui/Icon'
import { cn } from '@/ui/utils'
import { XY } from '@/dto/XY.dto'
import { formatTimestampToReadableString } from '@/ui/utils'
import { Stack } from '@/ui/Stack'
import { User } from '@/entities/User'

type RGB = { r: number; g: number; b: number }

const parseHexToRgb = (value: string): RGB | null => {
  const hex = value.trim().replace('#', '')

  if (hex.length === 3) {
    const r = parseInt(hex[0] + hex[0], 16)
    const g = parseInt(hex[1] + hex[1], 16)
    const b = parseInt(hex[2] + hex[2], 16)
    return Number.isNaN(r + g + b) ? null : { r, g, b }
  }

  if (hex.length === 6) {
    const r = parseInt(hex.slice(0, 2), 16)
    const g = parseInt(hex.slice(2, 4), 16)
    const b = parseInt(hex.slice(4, 6), 16)
    return Number.isNaN(r + g + b) ? null : { r, g, b }
  }

  return null
}

const parseRgbToRgb = (value: string): RGB | null => {
  const match = value.match(/^rgba?\(([^)]+)\)$/i)
  if (!match) return null
  const channels = match[1].split(',').map((part) => Number(part.trim()))
  if (channels.length < 3) return null

  const [r, g, b] = channels
  if ([r, g, b].some((channel) => Number.isNaN(channel))) return null
  return { r, g, b }
}

const hslToRgb = (h: number, s: number, l: number): RGB => {
  const saturation = s / 100
  const lightness = l / 100
  const chroma = (1 - Math.abs(2 * lightness - 1)) * saturation
  const hPrime = h / 60
  const x = chroma * (1 - Math.abs((hPrime % 2) - 1))

  let r1 = 0
  let g1 = 0
  let b1 = 0

  if (hPrime >= 0 && hPrime < 1) {
    r1 = chroma
    g1 = x
  } else if (hPrime < 2) {
    r1 = x
    g1 = chroma
  } else if (hPrime < 3) {
    g1 = chroma
    b1 = x
  } else if (hPrime < 4) {
    g1 = x
    b1 = chroma
  } else if (hPrime < 5) {
    r1 = x
    b1 = chroma
  } else {
    r1 = chroma
    b1 = x
  }

  const m = lightness - chroma / 2
  return {
    r: Math.round((r1 + m) * 255),
    g: Math.round((g1 + m) * 255),
    b: Math.round((b1 + m) * 255),
  }
}

const parseHslToRgb = (value: string): RGB | null => {
  const match = value.match(/^hsla?\(([^)]+)\)$/i)
  if (!match) return null

  const parts = match[1].split(',').map((part) => part.trim())
  if (parts.length < 3) return null

  const h = Number(parts[0].replace('deg', ''))
  const s = Number(parts[1].replace('%', ''))
  const l = Number(parts[2].replace('%', ''))

  if ([h, s, l].some((channel) => Number.isNaN(channel))) return null
  return hslToRgb(((h % 360) + 360) % 360, s, l)
}

const resolveCssVariableColor = (value: string): string => {
  const varMatch = value.match(/^var\((--[^),\s]+)\)/)
  if (!varMatch || typeof window === 'undefined') return value

  const resolved = getComputedStyle(document.documentElement)
    .getPropertyValue(varMatch[1])
    .trim()

  return resolved || value
}

const parseToRgb = (value: string): RGB | null => {
  const resolved = resolveCssVariableColor(value)

  if (resolved.startsWith('#')) return parseHexToRgb(resolved)
  if (resolved.toLowerCase().startsWith('rgb')) return parseRgbToRgb(resolved)
  if (resolved.toLowerCase().startsWith('hsl')) return parseHslToRgb(resolved)

  return null
}

const getReadableTextColor = (backgroundColor: string): string => {
  const fallback = resolveCssVariableColor('var(--accent)') || '#f8f8f2'
  const rgb = parseToRgb(backgroundColor)

  if (!rgb) return fallback

  const luminance = (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255
  return luminance > 0.55 ? '#111827' : '#f8f8f2'
}

export namespace Pointers {
  export interface Props extends Stack.Props {
    getPixelPosition: (t: number) => number
    width: number
    self: XY
    timestamp: number
  }

  export interface Pointer {
    timestamp: number
    y: number
    x?: number
    id: User.Id
    color: string
  }
}

export function Pointers({
  getPixelPosition,
  width,
  self,
  timestamp,
  ...props
}: Pointers.Props) {
  const { app } = Application.use()
  const { y: scrollY } = useScroll()

  const you: Pointers.Pointer = {
    ...self,
    color: 'var(--green-700)',
    id: 'You' as User.Id,
    timestamp,
  }

  return (
    <Stack data-aria-pointers pos="absolute" className={s.pointers} {...props}>
      {[you, ...app.timeline.pointers].map((p) => {
        const isYours = p.id === ('You' as User.Id)
        const x = p.x || getPixelPosition(p.timestamp)
        const isRightSide = x > width / 2
        const textColor = getReadableTextColor(p.color)

        return (
          <Stack
            key={p.id}
            className={cn(s.pointer, isRightSide && s.right, !isYours && s.guest)}
            style={{ top: isYours ? p.y : -scrollY + p.y, left: x }}
            ai="flex-start"
            pos="absolute"
          >
            <Icon name="MousePointer2" size={14} color={p.color} fill={p.color} />
            <p style={{ background: p.color, color: textColor }}>
              {p.id} {isYours ? `on ${formatTimestampToReadableString(timestamp)}ms` : null}
            </p>
          </Stack>
        )
      })}
    </Stack>
  )
}
