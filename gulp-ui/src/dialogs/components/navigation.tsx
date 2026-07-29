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
import { WindowBridge } from '@/lib/WindowBridge'

export namespace Navigation {
  export interface Props {
    event: Doc.Type
  }
}

const WINDOW_SIZE = WindowBridge.DETACHED_DIALOG_NAVIGATION_WINDOW_SIZE

interface NavigationEntry {
  event: Doc.Type
  color?: string
  hasNotes?: boolean
  hasLinks?: boolean
}

export function Navigation({ event }: Navigation.Props) {
  const {
    app,
    spawnDialog,
    isDetachedWindow,
    detachedDialogNavigation,
    selectDetachedDialogEvent,
  } = Application.use()
  const { t } = Locale.use()
  const navRef = useRef<HTMLDivElement>(null)
  const detachedNavigation =
    isDetachedWindow &&
    detachedDialogNavigation?.currentEventId === event._id
      ? detachedDialogNavigation
      : null

  const file = Source.Entity.id(app, event['gulp.source_id'])

  /** Local deduplication to ensure a clean list even if DataStore has stale duplicates */
  const allEvents = useMemo(() => {
    const raw =
      !detachedNavigation && file
        ? Doc.Entity.get(app, file.id).toReversed()
        : []
    const unique: Doc.Type[] = []
    const seen = new Set<Doc.Id>()
    for (const e of raw) {
      if (e && !seen.has(e._id)) {
        seen.add(e._id)
        unique.push(e)
      }
    }
    return unique
  }, [file, app.timeline.renderVersion, detachedNavigation])

  const currentIndex = allEvents.findIndex((e) => e && e._id === event._id)

  /** Strictly centered window: always WINDOW_SIZE elements with current at center */
  const windowEvents = useMemo<Array<NavigationEntry | null>>(() => {
    if (detachedNavigation) {
      return detachedNavigation.items.map((item) =>
        item
          ? {
              event: {
                _id: item.id,
                gulp_timestamp: item.timestamp,
                'gulp.source_id': item.sourceId,
                'gulp.event_code': item.eventCode,
                number_hash: item.numberHash,
              } as Doc.Type,
              color: item.color,
              hasNotes: item.hasNotes,
              hasLinks: item.hasLinks,
            }
          : null
      )
    }

    const half = Math.floor(WINDOW_SIZE / 2)
    return Array.from({ length: WINDOW_SIZE }, (_, i) => {
      if (currentIndex === -1) return null
      const targetIndex = currentIndex + (i - half)
      const targetEvent =
        targetIndex >= 0 && targetIndex < allEvents.length
          ? allEvents[targetIndex]
          : null
      return targetEvent ? { event: targetEvent } : null
    })
  }, [
    allEvents,
    currentIndex,
    detachedNavigation,
  ])

  /**
   * Opens the event dialog for a specific event from the navigation strip.
   *
   * @param targetEvent Event selected by the user.
   * @returns Click handler that opens the selected event.
   */
  const openEvent = (targetEvent: Doc.Type) => () => {
    if (detachedNavigation) {
      if (targetEvent._id !== event._id) {
        selectDetachedDialogEvent(targetEvent._id)
      }
      return
    }
    spawnDialog(<DisplayEventDialog event={targetEvent} />)
  }

  /**
   * Moves the dialog selection by a relative amount in the source event list.
   *
   * @param direction Relative movement, where -1 is previous and 1 is next.
   */
  const changeEvent = useCallback((direction: number) => {
    if (detachedNavigation) {
      const targetEventId =
        direction < 0
          ? detachedNavigation.previousEventId
          : detachedNavigation.nextEventId
      if (targetEventId) selectDetachedDialogEvent(targetEventId)
      return
    }
    if (currentIndex === -1) return
    const nextIndex = currentIndex + direction
    if (nextIndex >= 0 && nextIndex < allEvents.length) {
      spawnDialog(<DisplayEventDialog event={allEvents[nextIndex]} />)
    }
  }, [
    allEvents,
    currentIndex,
    detachedNavigation,
    selectDetachedDialogEvent,
    spawnDialog,
  ])

  const hasPreviousEvent = detachedNavigation
    ? detachedNavigation.previousEventId !== null
    : currentIndex > 0
  const hasNextEvent = detachedNavigation
    ? detachedNavigation.nextEventId !== null
    : currentIndex >= 0 && currentIndex < allEvents.length - 1

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
        {windowEvents.map((entry, i) =>
          entry ? (
            <EventIndicator
              key={entry.event._id}
              className={cn(entry.event._id === event._id && s.focus)}
              onClick={openEvent(entry.event)}
              event={entry.event}
              resolvedColor={entry.color}
              hasNotesOverride={entry.hasNotes}
              hasLinksOverride={entry.hasLinks}
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
