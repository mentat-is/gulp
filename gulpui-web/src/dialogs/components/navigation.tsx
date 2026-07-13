import { DisplayEventDialog, EventIndicator } from '../Event.dialog'
import { Application } from '@/context/Application.context'
import { useCallback, useMemo, useRef } from 'react'
import { Source } from '@/entities/Source'
import s from './navigation.module.css'
import { cn } from '@/ui/utils'
import { Doc } from '@/entities/Doc'
import { Stack } from '@/ui/Stack'
import { Button } from '@/ui/Button'
import { Locale } from '@/locales'

export namespace Navigation {
  export interface Props {
    event: Doc.Type
  }
}

const WINDOW_SIZE = 11

export function Navigation({ event }: Navigation.Props) {
  const { app, spawnDialog } = Application.use()
  const { t } = Locale.use()
  const navRef = useRef<HTMLDivElement>(null)

  const file = Source.Entity.id(app, event['gulp.source_id'])

  /** Local deduplication to ensure a clean list even if DataStore has stale duplicates */
  const allEvents = useMemo(() => {
    const raw = file ? Doc.Entity.get(app, file.id).toReversed() : []
    const unique: Doc.Type[] = []
    const seen = new Set<Doc.Id>()
    for (const e of raw) {
      if (e && !seen.has(e._id)) {
        seen.add(e._id)
        unique.push(e)
      }
    }
    return unique
  }, [file, app.timeline.renderVersion])

  const currentIndex = allEvents.findIndex((e) => e && e._id === event._id)

  /** Strictly centered window: always WINDOW_SIZE elements with current at center */
  const windowEvents = useMemo(() => {
    const half = Math.floor(WINDOW_SIZE / 2)
    return Array.from({ length: WINDOW_SIZE }, (_, i) => {
      if (currentIndex === -1) return null
      const targetIndex = currentIndex + (i - half)
      return (targetIndex >= 0 && targetIndex < allEvents.length) ? allEvents[targetIndex] : null
    })
  }, [allEvents, currentIndex])

  /**
   * Opens the event dialog for a specific event from the navigation strip.
   *
   * @param targetEvent Event selected by the user.
   * @returns Click handler that opens the selected event.
   */
  const openEvent = (targetEvent: Doc.Type) => () => {
    spawnDialog(<DisplayEventDialog event={targetEvent} />)
  }

  /**
   * Moves the dialog selection by a relative amount in the source event list.
   *
   * @param direction Relative movement, where -1 is previous and 1 is next.
   */
  const changeEvent = useCallback((direction: number) => {
    if (currentIndex === -1) return
    const nextIndex = currentIndex + direction
    if (nextIndex >= 0 && nextIndex < allEvents.length) {
      spawnDialog(<DisplayEventDialog event={allEvents[nextIndex]} />)
    }
  }, [allEvents, currentIndex, spawnDialog])

  const hasPreviousEvent = currentIndex > 0
  const hasNextEvent = currentIndex >= 0 && currentIndex < allEvents.length - 1

  return (
    <Stack
      ref={navRef}
      className={s.navigation}
      jc="space-between"
      tabIndex={0}
      onKeyDown={(event) => {
        if (event.key === 'ArrowRight') changeEvent(1)
        if (event.key === 'ArrowLeft') changeEvent(-1)
      }}
      onWheel={(event) => {
        if (Math.abs(event.deltaY) >= Math.abs(event.deltaX)) {
          event.preventDefault()
          changeEvent(event.deltaY > 0 ? 1 : -1)
        }
      }}
    >
      <Button
        className={s.arrow}
        variant="secondary"
        icon="ChevronLeft"
        title={t('eventDialog.previousEvent')}
        disabled={!hasPreviousEvent}
        onClick={() => changeEvent(-1)}
      />
      <Stack className={s.content} jc="center" gap={4} flex>
        {windowEvents.map((e, i) =>
          e ? (
            <EventIndicator
              key={e._id}
              className={cn(e._id === event._id && s.focus)}
              onClick={openEvent(e)}
              event={e}
            />
          ) : (
            <div key={`placeholder-${i}`} className={s.placeholder} />
          )
        )}
      </Stack>
      <Button
        className={s.arrow}
        variant="secondary"
        icon="ChevronRight"
        title={t('eventDialog.nextEvent')}
        disabled={!hasNextEvent}
        onClick={() => changeEvent(1)}
      />

    </Stack>
  )
}
