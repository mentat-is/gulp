import { DisplayEventDialog, EventIndicator } from '../Event.dialog'
import { Application } from '@/context/Application.context'
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import { Source } from '@/entities/Source'
import s from './navigation.module.css'
import { cn } from '@/ui/utils'
import { Doc } from '@/entities/Doc'
import { Stack } from '@/ui/Stack'
import { Button } from '@/ui/Button'
import { Locale } from '@/locales'
import { WindowBridge } from '@/lib/WindowBridge'
import { DataStore } from '@/store/DataStore'
import type { EventNeighborWindow } from '@/event-store/EventStore.reader'
import {
  findDialogNavigationEventIndex,
  hasDialogNavigationCoverage,
} from '@/dialogs/navigationCache'

export namespace Navigation {
  export interface Props {
    event: Doc.Type
  }
}

const CACHE_WINDOW_RADIUS = Math.floor(
  WindowBridge.DIALOG_NAVIGATION_CACHE_WINDOW_SIZE / 2
)
const VISIBLE_WINDOW_SIZE =
  WindowBridge.DIALOG_NAVIGATION_VISIBLE_WINDOW_SIZE

interface NavigationEntry {
  event: Doc.Type
  color?: string
  hasNotes?: boolean
  hasLinks?: boolean
}

interface NavigationCache {
  sourceId: Source.Id
  documentsRevision: number
  neighborWindow: EventNeighborWindow
}

/**
 * Renders the centered Event Dialog navigation strip from a bounded retained
 * neighbor window.
 */
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
  const sourceId = file?.id
  const [navigationCache, setNavigationCache] =
    useState<NavigationCache | null>(null)
  const navigationCacheRef = useRef<NavigationCache | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const documentRevision = useSyncExternalStore(
    DataStore.subscribeDocuments,
    DataStore.getDocumentsSnapshot,
    DataStore.getDocumentsSnapshot
  )

  useEffect(() => {
    if (detachedNavigation || !sourceId) {
      navigationCacheRef.current = null
      setNavigationCache(null)
      setIsLoading(false)
      return
    }

    const retainedWindow =
      navigationCacheRef.current?.sourceId === sourceId &&
      navigationCacheRef.current.documentsRevision === documentRevision
        ? navigationCacheRef.current.neighborWindow
        : null
    if (
      hasDialogNavigationCoverage(
        retainedWindow,
        event,
        DataStore.getSourceRevision(sourceId)
      )
    ) {
      setIsLoading(false)
      return
    }

    let canceled = false
    setIsLoading(true)
    void DataStore.Events.getNeighbors(
      sourceId,
      event._id,
      CACHE_WINDOW_RADIUS,
      CACHE_WINDOW_RADIUS
    )
      .then((result) => {
        if (canceled) return
        const nextCache = {
          sourceId,
          documentsRevision: documentRevision,
          neighborWindow: result,
        }
        navigationCacheRef.current = nextCache
        setNavigationCache(nextCache)
      })
      .catch(() => {
        if (!canceled) {
          navigationCacheRef.current = null
          setNavigationCache(null)
        }
      })
      .finally(() => {
        if (!canceled) setIsLoading(false)
      })

    return () => {
      canceled = true
    }
  }, [
    detachedNavigation,
    documentRevision,
    event._id,
    sourceId,
  ])

  const neighborWindow = navigationCache?.neighborWindow ?? null
  const currentIndex = findDialogNavigationEventIndex(
    neighborWindow,
    event._id
  )

  /** Projects the fixed visible strip with the selected event at its center. */
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

    const half = Math.floor(VISIBLE_WINDOW_SIZE / 2)
    return Array.from({ length: VISIBLE_WINDOW_SIZE }, (_, i) => {
      if (currentIndex === -1) return null
      const targetIndex = currentIndex - (i - half)
      const targetEvent =
        targetIndex >= 0 && targetIndex < (neighborWindow?.events.length ?? 0)
          ? neighborWindow?.events[targetIndex]
          : null
      return targetEvent ? { event: targetEvent } : null
    })
  }, [currentIndex, detachedNavigation, neighborWindow])

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
    const nextIndex = currentIndex - direction
    const nextEvent = neighborWindow?.events[nextIndex]
    if (nextEvent) {
      spawnDialog(<DisplayEventDialog event={nextEvent} />)
    }
  }, [
    currentIndex,
    detachedNavigation,
    neighborWindow,
    selectDetachedDialogEvent,
    spawnDialog,
  ])

  const hasPreviousEvent = detachedNavigation
    ? detachedNavigation.previousEventId !== null
    : !isLoading &&
      currentIndex >= 0 &&
      currentIndex < (neighborWindow?.events.length ?? 0) - 1
  const hasNextEvent = detachedNavigation
    ? detachedNavigation.nextEventId !== null
    : !isLoading && currentIndex > 0

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
