/**
 * DetachedApp.Provider — Lightweight Application context for detached browser windows.
 *
 * ARCHITECTURE: Detached windows (TableViewWindow, NotesWindow) previously used
 * ReactDOM.createPortal which kept them in the main tab's React tree. Every state
 * change in the main tab caused cross-window DOM reconciliation, degrading performance.
 *
 * This provider creates an INDEPENDENT React context for each detached window:
 * - Own [app, setInfo] state (initialized from a snapshot of the main tab's state)
 * - Own Info instance (uses global `api()` for network requests)
 * - Own banner system (spawnBanner/destroyBanner work locally)
 * - BroadcastChannel listener for cross-window sync (theme, notes, flags)
 *
 * PERFORMANCE: The main tab no longer reconciles detached window DOM. State changes
 * in the main tab only reach detached windows via lightweight BroadcastChannel messages.
 */
import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
  ReactNode,
} from 'react'
import { Application } from '@/context/Application.context'
import { Info } from '@/class/Info'
import { App } from '@/entities/App'
import { WindowBridge } from '@/lib/WindowBridge'
import { DataStore } from '@/store/DataStore'
import { Note } from '@/entities/Note'
import { RenderEngine } from '@/class/RenderEngine'
import { Color } from '@/entities/Color'
import { Operation } from '@/entities/Operation'
import { Source } from '@/entities/Source'
import { Request } from '@/entities/Request'
import { Extension } from '@/context/Extension.context'
import { Locale } from '@/locales'
import { ownsIndependentDetachedDataStore } from '@/context/detachedDataStore'
import s from './styles/DetachedApp.module.css'

export namespace DetachedApp {
  export interface ProviderProps {
    /** Serialized snapshot of the main tab's app state */
    initialApp?: App.Type
    /** Initial notes data from DataStore */
    initialNotes?: Note.Type[]
    /** BroadcastChannel bridge ID (used to create a listener) */
    bridgeId: string
    /** The detached window's document, used for portal/selection APIs */
    detachedDocument?: Document
    /** Main window's spawnBanner — banners should open in the main window, not the detached one */
    mainSpawnBanner?: (node: ReactNode, target?: string) => void
    /** Children to render inside the provider */
    children: ReactNode
  }
}

/**
 * Restores a detached source summary into the Source.Type shape expected by UI code.
 * @param source Serializable source summary received from WindowBridge.
 * @returns Source entity with bigint nanosecond timestamps restored.
 */
function hydrateDetachedSource(
  source: WindowBridge.DetachedSourceSummary,
): Source.Type {
  return {
    ...source,
    nanotimestamp: {
      min: BigInt(source.nanotimestamp.min),
      max: BigInt(source.nanotimestamp.max),
    },
  }
}

export function DetachedAppProvider({
  initialApp = App.Base,
  initialNotes = [],
  bridgeId,
  detachedDocument = document,
  mainSpawnBanner = () => { },
  children,
}: DetachedApp.ProviderProps) {
  const [app, setRawInfo] = useState<App.Type>(initialApp)
  const [banner, setBanner] = useState<{ node: ReactNode; target: string } | null>(null)
  const [dialog, setDialog] = useState<ReactNode>(null)
  const timeline = useRef<HTMLDivElement>(null as unknown as HTMLDivElement)
  const [highlightsOverlay, setHighlightsOverlay] = useState<ReactNode>(null)
  const [detachedStatus, setDetachedStatus] =
    useState<WindowBridge.MainContextStatus>('initializing')
  const [detachedContextVersion, setDetachedContextVersion] = useState(0)
  const selectedSourceIdsRef = useRef<Source.Id[] | null | undefined>(null)
  const initialNotesRef = useRef(initialNotes)
  const ownsIndependentDataStore = ownsIndependentDetachedDataStore(
    detachedDocument,
    window,
  )

  /**
   * Hydrates notes only for standalone detached routes with their own module realm.
   * Opener-rendered popup roots already share the main canonical DataStore.
   */
  useEffect(() => {
    if (!ownsIndependentDataStore) return
    DataStore.replaceNotes(initialNotesRef.current, 'deferred')
  }, [ownsIndependentDataStore])

  /**
   * Authoritative loadings ref — prevents loading maps from being lost
   * to concurrent state interleaving between setLoading, events_reset_in_file,
   * and other setInfoByKey calls during the refetch flow.
   *
   * This ref is the single source of truth for which sources are currently loading.
   * It is updated BEFORE the React state and re-applied AFTER every state update.
   */
  const loadingsRef = useRef<{
    byRequestId: Map<Request.Id, Source.Id>;
    byFileId: Map<Source.Id, Request.Id>;
  }>({
    byRequestId: new Map(),
    byFileId: new Map(),
  })

  /**
   * Wrapped setInfo that re-applies authoritative loadings after every state update.
   * This ensures that concurrent setInfoByKey calls (e.g. from events_reset_in_file)
   * cannot overwrite the loading maps set by setLoading.
   */
  const setInfo = useCallback(
    (action: React.SetStateAction<App.Type>) => {
      setRawInfo((prev) => {
        const next = typeof action === 'function' ? action(prev) : action
        // Re-apply authoritative loadings to prevent state interleaving
        return {
          ...next,
          general: {
            ...next.general,
            loadings: {
              byRequestId: new Map(loadingsRef.current.byRequestId),
              byFileId: new Map(loadingsRef.current.byFileId),
            },
          },
        }
      })
    },
    []
  )

  // Create a stable Info instance for the detached window
  const infoRef = useRef<Info | null>(null)
  if (!infoRef.current) {
    infoRef.current = new Info({ app, setInfo, timeline })
  } else {
    infoRef.current.app = app
    infoRef.current.setInfo = setInfo
  }
  const instance = infoRef.current

  /**
   * Patch setLoading/delLoading on the detached Info instance to keep
   * the authoritative loadingsRef in sync. This ensures the loading
   * state persists across concurrent React state updates.
   */
  if (!(infoRef.current as any)._detachedPatched) {
    const origSetLoading = instance.setLoading.bind(instance)
    instance.setLoading = (reqId: Request.Id, fileId: Source.Id) => {
      loadingsRef.current.byRequestId.set(reqId, fileId)
      loadingsRef.current.byFileId.set(fileId, reqId)
      origSetLoading(reqId, fileId)
    }

    const origDelLoading = instance.delLoading.bind(instance)
    instance.delLoading = (reqId: Request.Id) => {
      loadingsRef.current.byRequestId.delete(reqId)
      const fileId = [...loadingsRef.current.byFileId.entries()]
        .find(([, v]) => v === reqId)?.[0]
      if (fileId) loadingsRef.current.byFileId.delete(fileId)
      origDelLoading(reqId)
    }

    ;(infoRef.current as any)._detachedPatched = true
  }

  /**
   * Applies a main-tab lifecycle status received from either BroadcastChannel
   * or the localStorage replay fallback.
   *
   * @param payload - Lifecycle status payload from the main tab.
   * @returns Nothing.
   */
  const applyMainContextStatus = useCallback((
    payload: WindowBridge.MainContextStatusPayload,
  ) => {
    const { status, contextVersion } = payload
    setDetachedStatus(status)
    setDetachedContextVersion(contextVersion)
  }, [])

  /**
   * Applies a main-tab app snapshot received from either BroadcastChannel or
   * the localStorage replay fallback.
   *
   * @param payload - App snapshot payload from the main tab.
   * @returns Nothing.
   */
  const applyAppSnapshot = useCallback((
    payload: WindowBridge.AppSnapshotPayload,
  ) => {
    const { selectedSourceIds, renderedSourceIds } = payload
    const { frame, filter, renderVersion } = payload.timeline
    selectedSourceIdsRef.current = selectedSourceIds

    setInfo(prev => {
      const files = payload.sources.map((source) => {
        const file = hydrateDetachedSource(source)
        return {
          ...file,
          selected: selectedSourceIds.includes(file.id),
        }
      })

      return {
        ...prev,
        target: {
          ...prev.target,
          operations: payload.operations,
          contexts: payload.contexts,
          files,
          filters: payload.filters as App.Type['target']['filters'],
          events: new Map(),
        },
        timeline: {
          ...prev.timeline,
          frame,
          filter,
          renderVersion,
          renderedSourceIds,
        },
        hidden: payload.hidden,
      }
    })
  }, [setInfo])

  /**
   * Applies a selected timeline event replayed by the main tab.
   *
   * @param payload - Selected event payload.
   * @returns Nothing.
   */
  const applyEventSelected = useCallback((
    payload: WindowBridge.EventSelectedPayload,
  ) => {
    const { event } = payload
    setInfo(prev => ({
      ...prev,
      timeline: {
        ...prev.timeline,
        target: event
      }
    }))
  }, [setInfo])

  /**
   * Applies a notes snapshot from the main tab to this detached DataStore.
   *
   * @param payload - Current selected-source notes from the main tab.
   * @returns Nothing.
   */
  const applyNotesChanged = useCallback((
    payload: WindowBridge.NotesChangedPayload,
  ) => {
    if (!ownsIndependentDataStore) return
    DataStore.replaceNotes(payload.notes, 'deferred')
    Note.Entity.invalidateCache()
    RenderEngine.reset('notes')
  }, [ownsIndependentDataStore])

  // Listen for incoming BroadcastChannel messages from the main tab
  useEffect(() => {
    const bridge = WindowBridge.create(bridgeId, (message) => {
      switch (message.type) {
        case WindowBridge.MessageType.THEME_CHANGE: {
          const { theme } = message.payload as WindowBridge.ThemeChangePayload
          // Set data-theme attribute; full CSS variable sync is handled by
          // direct DOM access via applyThemeToWindow in Navigator.tsx
          const targetRoot = document.documentElement
          if (!targetRoot) {
            break
          }
          targetRoot.setAttribute('data-theme', theme)
          Color.Themer.setTheme(theme as any)
          // Trigger re-render to pick up new colors from getTargetGuideColor()
          setInfo(prev => ({
            ...prev,
            timeline: {
              ...prev.timeline,
              renderVersion: prev.timeline.renderVersion + 1,
            },
          }))
          break
        }
        case WindowBridge.MessageType.APP_SNAPSHOT: {
          applyAppSnapshot(message.payload as WindowBridge.AppSnapshotPayload)
          break
        }
        case WindowBridge.MessageType.NOTES_CHANGED: {
          applyNotesChanged(message.payload as WindowBridge.NotesChangedPayload)
          break
        }
        case WindowBridge.MessageType.MAIN_CONTEXT_STATUS: {
          applyMainContextStatus(message.payload as WindowBridge.MainContextStatusPayload)
          break
        }
        case WindowBridge.MessageType.EVENT_SELECTED: {
          applyEventSelected(message.payload as WindowBridge.EventSelectedPayload)
          break
        }
      }
    })

    return () => {
      bridge.destroy()
    }
  }, [
    applyAppSnapshot,
    applyEventSelected,
    applyMainContextStatus,
    applyNotesChanged,
    bridgeId,
  ])

  useEffect(() => {
    /**
     * Applies the last stored main-context replay when BroadcastChannel delivery
     * was missed during login redirects or page reloads.
     *
     * @returns Nothing.
     */
    const applyStoredReplay = () => {
      const replay = WindowBridge.readStoredMainContext()
      if (!replay) return

      applyMainContextStatus(replay.status)
      if (replay.snapshot) applyAppSnapshot(replay.snapshot)
    }

    applyStoredReplay()

    const handleStorage = (event: StorageEvent) => {
      if (event.key !== WindowBridge.getMainContextStorageKey()) return
      applyStoredReplay()
    }

    window.addEventListener('storage', handleStorage)
    return () => window.removeEventListener('storage', handleStorage)
  }, [applyAppSnapshot, applyEventSelected, applyMainContextStatus])

  // Reactively fetch new sources/contexts when the selected operation changes.
  // This avoids sending large source lists over BroadcastChannel (APP_SNAPSHOT).
  const selectedOperationId = app.target.operations.find(o => o.selected)?.id
  const prevOperationIdRef = useRef<string | null | undefined>(undefined)

  useEffect(() => {
    if (selectedOperationId && prevOperationIdRef.current !== undefined && selectedOperationId !== prevOperationIdRef.current) {
      // Operation changed and it's not the initial mount -> sync fresh data from server
      instance.sync().then(() => {
        // Apply selection status after sync finishes
        if (selectedSourceIdsRef.current) {
          setInfo(prev => ({
            ...prev,
            target: {
              ...prev.target,
              files: prev.target.files.map((f: Source.Type) => ({
                ...f,
                selected: selectedSourceIdsRef.current!.includes(f.id)
              }))
            }
          }))
        }
      }).catch(err => console.error('Failed to sync detached app', err))
    }
    if (selectedOperationId) {
      prevOperationIdRef.current = selectedOperationId
    }
  }, [selectedOperationId, instance])

  const destroyBanner = useCallback(() => {
    setBanner(null)
    detachedDocument.body.classList.remove('no-scroll')
  }, [detachedDocument])

  const spawnBanner = useCallback((node: ReactNode, target: string = 'main') => {
    if (!node) {
      setBanner(null)
      detachedDocument.body.classList.remove('no-scroll')
      return
    }

    if (target === 'table' || target === 'main') {
      setBanner({ node, target })
      detachedDocument.body.classList.add('no-scroll')
      return
    }

    mainSpawnBanner(node, target)
  }, [detachedDocument, mainSpawnBanner])

  const spawnDialog = useCallback((dialog: ReactNode) => {
    setDialog(dialog)
  }, [])

  const outboundBridgeRef = useRef<ReturnType<typeof WindowBridge.create> | null>(null)
  useEffect(() => {
    const sendId = WindowBridge.generateId()
    const bridge = WindowBridge.create(sendId, () => { })
    outboundBridgeRef.current = bridge
    bridge.send(WindowBridge.MessageType.DETACHED_READY, { windowId: bridgeId })
    return () => {
      outboundBridgeRef.current?.destroy()
      outboundBridgeRef.current = null
    }
  }, [bridgeId])

  useEffect(() => {
    if (detachedStatus === 'active') return

    const interval = window.setInterval(() => {
      outboundBridgeRef.current?.send(WindowBridge.MessageType.DETACHED_READY, {
        windowId: bridgeId,
      })
    }, 1000)

    return () => window.clearInterval(interval)
  }, [bridgeId, detachedStatus])

  // Sync timeline target selection back to the main window so the canvas crosshair updates.
  // Use _id as dep to avoid re-firing when the same event is echoed back as a new object ref.
  useEffect(() => {
    outboundBridgeRef.current?.send(WindowBridge.MessageType.EVENT_SELECTED, {
      event: app.timeline.target ?? null,
    })
  }, [app.timeline.target?._id])

  const setDockedState = useCallback(() => {
    outboundBridgeRef.current?.send(WindowBridge.MessageType.DOCK_DIALOG, {})
  }, [])

  useEffect(() => {
    if (detachedStatus === 'active') return

    loadingsRef.current.byRequestId.clear()
    loadingsRef.current.byFileId.clear()
    setBanner(null)
    setDialog(null)
    detachedDocument.body.classList.remove('no-scroll')
    setInfo(prev => ({
      ...prev,
      target: {
        ...App.Base.target,
        operations: prev.target.operations.map((operation: Operation.Type) => ({
          ...operation,
          selected: false,
        })),
      },
      timeline: {
        ...prev.timeline,
        target: null,
        cache: {
          data: new Map(),
          filters: {},
        },
        renderVersion: prev.timeline.renderVersion + 1,
      },
      general: {
        ...prev.general,
        loadings: {
          byRequestId: new Map(),
          byFileId: new Map(),
        },
      },
    }))
  }, [detachedDocument, detachedStatus, setInfo])

  const props = useMemo(
    () => ({
      spawnBanner,
      destroyBanner,
      banner,
      spawnDialog,
      dialog,
      app,
      setInfo,
      Info: instance,
      timeline,
      highlightsOverlay,
      setHighlightsOverlay,
      canvasDocked: true,
      setCanvasDocked: setDockedState,
      dialogsDocked: true,
      setDialogsDocked: setDockedState,
      hintOpen: false,
      setHintOpen: () => { },
      toggleHintOpen: () => { },
      isDetachedWindow: true,
      currentDocument: detachedDocument,
      detachedStatus,
      detachedContextVersion,
    }),
    [
      spawnBanner,
      destroyBanner,
      banner,
      spawnDialog,
      dialog,
      app,
      setInfo,
      instance,
      timeline,
      highlightsOverlay,
      setHighlightsOverlay,
      setDockedState,
      detachedStatus,
      detachedContextVersion,
    ],
  )

  return (
    <Application.Context.Provider value={props}>
      <Locale.Provider>
        <Extension.Provider>
          {detachedStatus === 'active'
            ? children
            : <DetachedIdleState status={detachedStatus} />}
        </Extension.Provider>
      </Locale.Provider>
    </Application.Context.Provider>
  )
}

/**
 * DetachedIdleState renders a guarded state while the main tab cannot provide
 * a valid operation context to the detached root.
 *
 * @param props.status - Current detached lifecycle status.
 * @returns Idle state content for the detached window.
 */
function DetachedIdleState({ status }: { status: WindowBridge.MainContextStatus }) {
  const { t } = Locale.use()
  const titleKey = status === 'auth_lost'
    ? 'detachedApp.authLost'
    : status === 'initializing'
      ? 'detachedApp.initializing'
      : 'detachedApp.idle'
  const descriptionKey = status === 'auth_lost'
    ? 'detachedApp.authLostDescription'
    : status === 'initializing'
      ? 'detachedApp.initializingDescription'
      : 'detachedApp.idleDescription'

  return (
    <div className={s.idle}>
      <div className={s.panel}>
        <h2>{t(titleKey)}</h2>
        <p>{t(descriptionKey)}</p>
      </div>
    </div>
  )
}
