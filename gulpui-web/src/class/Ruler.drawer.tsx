import { MinMax } from '@/class/Info'
import { Theme } from '@/context/Theme.context'
import { Color } from '@/entities/Color'
import { getDateFormat } from '@/ui/utils'
import { addMilliseconds, differenceInMilliseconds, formatDate } from 'date-fns'

interface RulerDrawerConstructor {
  ctx: CanvasRenderingContext2D
  selected: MinMax | null
  width: number
  scale: number
  getPixelPosition: (timestamp: number) => number
  scrollX: number
}

interface RulerSectionProps {
  timestamp: number
  position: number
  format: string
  step: number
  unit: string
  value: number
  even: boolean
}

type RulerSectionTemplate = Omit<RulerSectionProps, 'position'>
const RULER_SECTION_CACHE_LIMIT = 200

export class RulerDrawer implements RulerDrawerConstructor {
  ctx!: CanvasRenderingContext2D
  selected!: MinMax | null
  width!: number
  scale!: number
  getPixelPosition!: (timestamp: number) => number
  scrollX!: number
  cache: RulerSectionProps[] = []
  private static instance: RulerDrawer
  private static sectionCache = new Map<string, RulerSectionTemplate[]>()

  constructor({
    ctx,
    getPixelPosition,
    scrollX,
    scale,
    selected,
    width,
  }: RulerDrawerConstructor) {
    if (RulerDrawer.instance) {
      RulerDrawer.instance.ctx = ctx
      RulerDrawer.instance.scrollX = scrollX
      RulerDrawer.instance.scale = scale
      RulerDrawer.instance.selected = selected
      RulerDrawer.instance.width = width
      RulerDrawer.instance.getPixelPosition = getPixelPosition
      return RulerDrawer.instance
    }

    this.ctx = ctx
    this.scrollX = scrollX
    this.scale = scale
    this.selected = selected
    this.width = width
    this.getPixelPosition = getPixelPosition
    RulerDrawer.instance = this
    return RulerDrawer.instance
  }

  /**
   * Draws ruler background walls and stores section metadata for label rendering.
   * @returns Nothing.
   */
  draw(): void {
    if (!this.selected) return

    const templates = this.getSectionTemplates()
    this.cache = templates.map((template) => {
      const position = this.getPixelPosition(template.timestamp)
      this.wall({ even: template.even, position })
      return {
        ...template,
        position,
      }
    })
  }

  /**
   * Returns cached ruler section templates for the current visible time range.
   * @returns Ruler section templates without pixel positions.
   */
  private getSectionTemplates(): RulerSectionTemplate[] {
    this.cache = []
    if (!this.selected) return []

    const { max, min } = this.selected

    const total = differenceInMilliseconds(max, min)
    const width = this.width / this.scale || 0

    const start = addMilliseconds(
      min,
      (this.scrollX / this.width) * total,
    ).valueOf()
    const end = addMilliseconds(
      min,
      ((this.scrollX + width) / this.width) * total,
    ).valueOf()

    const [step, unit, value] = this.step(end - start)
    if (step <= 0 || !Number.isFinite(step)) return []

    const roundedStart = Math.floor(start / step) * step
    const roundedEnd = Math.ceil(end / step) * step

    const format = getDateFormat(step)
    const cacheKey = [
      min,
      max,
      this.scale,
      this.ctx.canvas.width,
      roundedStart,
      roundedEnd,
      step,
    ].join(':')
    const cached = RulerDrawer.sectionCache.get(cacheKey)
    if (cached) return cached

    let timestamp = roundedStart
    let even = Math.floor(new Date(timestamp).getTime() / step) % 2 === 0
    const templates: RulerSectionTemplate[] = []

    while (timestamp <= roundedEnd) {
      templates.push({ timestamp, format, step, unit, value, even })
      timestamp += step
      even = !even
    }

    if (RulerDrawer.sectionCache.size >= RULER_SECTION_CACHE_LIMIT) {
      const oldestKey = RulerDrawer.sectionCache.keys().next().value
      if (oldestKey) {
        RulerDrawer.sectionCache.delete(oldestKey)
      }
    }
    RulerDrawer.sectionCache.set(cacheKey, templates)
    return templates
  }

  step(totalMilliseconds: number): [number, string, number] {
    let optimalInterval = 0
    let _unit = 'nanoseconds'
    let _value = 0
    let bestNumSections = Infinity

    const intervals = [
      { unit: 'nanoseconds', values: [1, 10, 100, 500] },
      { unit: 'milliseconds', values: [1, 10, 100, 250, 500] },
      { unit: 'seconds', values: [1, 2, 5, 10, 15, 30] },
      { unit: 'minutes', values: [1, 2, 5, 10, 15, 30] },
      { unit: 'hours', values: [1, 3, 6, 12] },
      { unit: 'days', values: [1, 2, 3, 7, 14] },
      { unit: 'months', values: [1, 3, 6] },
      { unit: 'years', values: [1, 2, 3, 5, 10, 25, 50, 100] },
    ]

    intervals.forEach(({ unit, values }) => {
      values.forEach((value) => {
        const intervalMs =
          value *
          {
            nanoseconds: 0.001,
            milliseconds: 1,
            seconds: 1000,
            minutes: 60 * 1000,
            hours: 60 * 60 * 1000,
            days: 24 * 60 * 60 * 1000,
            months: 30 * 24 * 60 * 60 * 1000,
            years: 365 * 24 * 60 * 60 * 1000,
          }[unit]!

        const numSections = totalMilliseconds / intervalMs

        if (
          numSections >= 4 &&
          numSections <= 40 &&
          numSections < bestNumSections
        ) {
          bestNumSections = numSections
          _unit = unit
          _value = value
          optimalInterval = intervalMs
        }
      })
    })

    return [optimalInterval, _unit, _value]
  }

  separator() {
    this.ctx.beginPath()
    this.ctx.moveTo(0, 25)
    this.ctx.lineTo(this.ctx.canvas.width, 25)
    this.ctx.strokeStyle = Color.Themer.theme.BORDER
    this.ctx.stroke()
  }

  private wall(props: Pick<RulerSectionProps, 'even' | 'position'>) {
    this.ctx.fillStyle = props.even ? Color.Themer.theme.BACKGROUND_ACCENT : Color.Themer.theme.BACKGROUND_SECOND
    this.ctx.fillRect(
      props.position,
      25,
      this.ctx.canvas.width,
      this.ctx.canvas.height,
    )
    this.ctx.beginPath()
    this.ctx.moveTo(props.position, 0)
    this.ctx.lineTo(props.position, this.ctx.canvas.height)
    this.ctx.strokeStyle = Color.Themer.theme.BORDER
    this.ctx.stroke()
  }

  sections() {
    for (let i = 0; i < this.cache.length; i++) {
      const nextPosition = i + 1 < this.cache.length ? this.cache[i + 1].position : this.ctx.canvas.width;
      this.section(this.cache[i], nextPosition);
    }
    this.separator()
  }

  section(props: RulerSectionProps, nextPosition?: number) {
    const sectionWidth = (nextPosition ?? this.ctx.canvas.width) - props.position;
    this.ctx.fillStyle = props.even ? Color.Themer.theme.BACKGROUND_ACCENT : Color.Themer.theme.BACKGROUND_SECOND
    this.ctx.fillRect(props.position, 0, sectionWidth, 25)
    const timeUnit = props.format || 'MMM yyyy'
    const label = formatDate(props.timestamp, timeUnit)
    this.ctx.font = '12px Arial'
    this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT
    this.ctx.textAlign = 'left';
    this.ctx.fillText(label, props.position + 6, 16)
    this.ctx.fillStyle = Color.Themer.theme.FONT_SECOND
    this.ctx.textAlign = 'right';
    this.ctx.fillText(
      `${props.value} ${props.unit}`,
      this.getPixelPosition(props.timestamp + props.step) - 6,
      16,
    )
    this.ctx.beginPath()
    this.ctx.moveTo(props.position, 0)
    this.ctx.lineTo(props.position, 26)
    this.ctx.strokeStyle = Color.Themer.theme.BORDER
    this.ctx.stroke()
  }
}
