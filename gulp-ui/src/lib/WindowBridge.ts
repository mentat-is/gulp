/**
 * WindowBridge — BroadcastChannel-based communication layer for detached windows.
 *
 * Replaces ReactDOM.createPortal cross-window DOM coupling with a message-passing
 * architecture. Registration/close lifecycle messages share a control channel;
 * established detached actions and main-to-detached state use one private
 * channel per detached client.
 *
 * PERFORMANCE: This eliminates the primary cause of main-tab slowdown — React
 * reconciliation of cross-window DOM nodes on every state change.
 */
import type { Note } from '@/entities/Note'
import type { App } from '@/entities/App'
import type { Doc } from '@/entities/Doc'
import type { Operation } from '@/entities/Operation'
import type { Source } from '@/entities/Source'
import type { Context } from '@/entities/Context'
import type { Query } from '@/entities/Query'
import type { Link } from '@/entities/Link'
import type { User } from '@/entities/User'
import {
  DataStore,
  type NotesRealtimeSyncChange,
} from '@/store/DataStore'

const CONTROL_CHANNEL_PREFIX = 'gulp-window-bridge:control:'
const CLIENT_CHANNEL_PREFIX = 'gulp-window-bridge:client:'
const ASSISTANT_HISTORY_CHANNEL_PREFIX =
  'gulp-window-bridge:assistant-history:'
const MAIN_CONTEXT_STORAGE_PREFIX = 'gulp-window-bridge:main-context:'
const COORDINATOR_SESSION_KEY = 'gulp-window-bridge:coordinator'
const COORDINATOR_OWNERSHIP_SESSION_KEY =
  'gulp-window-bridge:coordinator-ownership'
const DOCUMENT_ID_GLOBAL_KEY = '__gulpWindowBridgeDocumentId__'

export namespace WindowBridge {
  const controlSenders = new Map<string, Bridge>()
  const privateActionSenders = new Map<string, Bridge>()

  interface CoordinatorOwnership {
    coordinatorId: string
    documentId: string
    phase: 'active' | 'transitioning'
  }

  /**
   * Message types for cross-window communication.
   */
  export enum MessageType {
    /** Main → Detached: Atomic, capability-scoped initial state */
    DETACHED_BOOTSTRAP = 'DETACHED_BOOTSTRAP',
    /** Main → Detached: Selected operation/source/context state changed */
    CONTEXT_SNAPSHOT = 'CONTEXT_SNAPSHOT',
    /** Main → Detached: Query filters or timeline frame changed */
    QUERY_CONTEXT = 'QUERY_CONTEXT',
    /** Main → Notes: Canvas-rendered source membership changed */
    RENDERED_SOURCES_CHANGED = 'RENDERED_SOURCES_CHANGED',
    /** Main → Table: Selected event identity changed */
    EVENT_TARGET_CHANGED = 'EVENT_TARGET_CHANGED',
    /** Main → Assistant: Websocket connection identity changed */
    ASSISTANT_CONNECTION_CHANGED = 'ASSISTANT_CONNECTION_CHANGED',
    /** Main → Dialog: Serializable dialog content changed */
    DIALOG_CHANGED = 'DIALOG_CHANGED',
    /** Main → Detached: Theme changed */
    THEME_CHANGE = 'THEME_CHANGE',
    /** Main → Detached: Authoritative selected-source notes snapshot */
    NOTES_CHANGED = 'NOTES_CHANGED',
    /** Main → Detached: Realtime note collaboration delta */
    NOTES_DELTA = 'NOTES_DELTA',
    /** Main → Detached: Select specific source in table view */
    TABLE_SELECT_SOURCE = 'TABLE_SELECT_SOURCE',
    /** Detached → Main: Resolve and focus an event in the main application */
    NAVIGATE_TO_EVENT = 'NAVIGATE_TO_EVENT',
    /** Detached → Main: Synchronize only the selected event identity */
    EVENT_TARGET_REQUEST = 'EVENT_TARGET_REQUEST',
    /** Detached → Main: Focus the main Canvas on one timestamp/source */
    FOCUS_TIMELINE_REQUEST = 'FOCUS_TIMELINE_REQUEST',
    /** Detached → Main: Apply one explicit frame to the main Canvas */
    TIMELINE_ZOOM_REQUEST = 'TIMELINE_ZOOM_REQUEST',
    /** Detached → Main: Open the main query/filter workflow */
    OPEN_FILTER_REQUEST = 'OPEN_FILTER_REQUEST',
    /** Detached → Main: A detached API request lost authentication */
    AUTH_LOST = 'AUTH_LOST',
    /** Detached ↔ Main: AI chat history was updated in IndexedDB */
    AI_HISTORY_UPDATED = 'AI_HISTORY_UPDATED',
    /** Detached → Main: User clicked "Dock" in the detached dialog window */
    DOCK_DIALOG = 'DOCK_DIALOG',
    /** Main → Detached: Current main-tab operation/auth lifecycle status */
    MAIN_CONTEXT_STATUS = 'MAIN_CONTEXT_STATUS',
    /** Detached → Main: Detached root mounted/reconnected and needs current state replay */
    DETACHED_READY = 'DETACHED_READY',
    /** Detached → Main (private): Established client lease renewal */
    DETACHED_HEARTBEAT = 'DETACHED_HEARTBEAT',
    /** Detached → Main: Detached root is closing */
    DETACHED_CLOSED = 'DETACHED_CLOSED',
    /** Detached Table → Main: Toggle query-context subscription */
    TABLE_SYNC_CHANGED = 'TABLE_SYNC_CHANGED',
    /** Detached Notes → Main: Toggle Canvas-rendered source subscription */
    NOTES_RENDERED_SYNC_CHANGED = 'NOTES_RENDERED_SYNC_CHANGED',
  }

  export type MainContextStatus = 'active' | 'initializing' | 'idle' | 'auth_lost'
  export type DetachedClientKind =
    | 'notes'
    | 'table'
    | 'dashboard'
    | 'dialog'
    | 'assistant'

  export type StateSyncDomain =
    | 'status'
    | 'context'
    | 'query'
    | 'rendered'
    | 'event'
    | 'assistant'
    | 'dialog'

  export interface DetachedClientCapabilities {
    kind: DetachedClientKind
    tableSyncEnabled: boolean
    notesRenderedSyncEnabled: boolean
    assistantQuerySyncEnabled: boolean
  }

  /**
   * Checks whether at least one detached client consumes a state domain.
   *
   * This is intentionally independent of bridge instances so the coordinator can
   * avoid scheduling work before an idle callback when no matching capability is
   * registered.
   *
   * @param clients Detached client capability records.
   * @param domain State domain being considered for synchronization.
   * @returns True when the domain has at least one subscriber.
   */
  export function hasStateDomainSubscriber(
    clients: Iterable<DetachedClientCapabilities>,
    domain: StateSyncDomain,
  ): boolean {
    for (const client of clients) {
      switch (domain) {
        case 'status':
        case 'context':
          return true
        case 'query':
          if (
            (client.kind === 'table' && client.tableSyncEnabled) ||
            (client.kind === 'assistant' && client.assistantQuerySyncEnabled)
          ) {
            return true
          }
          break
        case 'rendered':
          if (
            client.kind === 'notes' &&
            client.notesRenderedSyncEnabled
          ) {
            return true
          }
          break
        case 'event':
          if (
            client.kind === 'notes' ||
            client.kind === 'table'
          ) {
            return true
          }
          break
        case 'assistant':
          if (client.kind === 'assistant') return true
          break
        case 'dialog':
          if (client.kind === 'dialog') return true
          break
      }
    }
    return false
  }

  export interface ThemeChangePayload {
    theme: string
    mode: 'light' | 'dark'
    /** Computed CSS custom properties applied by the main document. */
    cssVariables: Record<string, string>
  }

  export interface ContextSnapshotPayload {
    operation: Operation.Type | null
    contexts: Array<Context.Type | DetachedNotesContextSummary>
    sources: Array<DetachedSourceSummary | DetachedNotesSourceSummary>
    /** Compact identity/preference subset needed by supported Dialogs. */
    user?: {
      id: User.Id
      showPreview?: boolean
    } | null
  }

  export interface ContextSnapshotOptions {
    /** Send only the selected operation, without source/context summaries. */
    operationOnly?: boolean
    /** Restrict source summaries to the IDs consumed by one detached surface. */
    sourceIds?: readonly Source.Id[]
    /** Include the compact Dialog user preference subset. */
    includeDialogUser?: boolean
    /** Project only the source/context fields consumed by Notes. */
    compactNotes?: boolean
  }

  export interface QueryContextPayload {
    filters: Partial<Record<Source.Id, Query.Type>>
    frame: App.Type['timeline']['frame']
  }

  export interface RenderedSourcesChangedPayload {
    sourceIds: Source.Id[]
  }

  export interface EventTargetChangedPayload {
    docId: Doc.Id | null
  }

  export interface AssistantConnectionChangedPayload {
    wsId: string
  }

  export type DetachedDialogDescriptor =
    | {
      kind: 'event'
      event: Doc.Type
    }
    | {
      kind: 'group'
      events: Doc.Type[]
      groupKind: 'events' | 'links' | 'notes'
    }

  export const DETACHED_DIALOG_NAVIGATION_WINDOW_SIZE = 11

  export interface DetachedDialogNavigationItem {
    id: Doc.Id
    sourceId: Source.Id
    timestamp: number
    eventCode: number
    numberHash: number
    color?: string
    hasNotes: boolean
    hasLinks: boolean
  }

  export interface DetachedDialogNavigation {
    currentEventId: Doc.Id
    items: Array<DetachedDialogNavigationItem | null>
    previousEventId: Doc.Id | null
    nextEventId: Doc.Id | null
  }

  export interface DialogChangedPayload {
    dialog: DetachedDialogDescriptor | null
    /** Dialog-related events available in the main document index. */
    events: Doc.Type[]
    /** Notes related only to the transmitted events. */
    notes: Note.Type[]
    /** Links touching one of the descriptor events. */
    links: Link.Type[]
    /** Main-resolved presentation colors, keyed by event ID. */
    eventColors: Partial<Record<Doc.Id, string>>
    /** Compact centered source-event window for a single-event Dialog. */
    navigation: DetachedDialogNavigation | null
  }

  /**
   * Rebuilds a detached dialog descriptor from the canonical events included in
   * its payload. Group ordering is preserved and descriptor events that are not
   * present in the canonical index retain their original value.
   *
   * @param dialog Dialog descriptor captured when the detached window opened.
   * @param eventsById Latest projected events keyed by document ID.
   * @returns Descriptor whose event references match the projected event index.
   */
  export function canonicalizeDetachedDialogDescriptor(
    dialog: DetachedDialogDescriptor,
    eventsById: ReadonlyMap<Doc.Id, Doc.Type>,
  ): DetachedDialogDescriptor {
    if (dialog.kind === 'event') {
      return {
        kind: 'event',
        event: eventsById.get(dialog.event._id) ?? dialog.event,
      }
    }

    return {
      kind: 'group',
      events: dialog.events.map(
        (event) => eventsById.get(event._id) ?? event,
      ),
      groupKind: dialog.groupKind,
    }
  }

  export interface DetachedBootstrapPayload {
    status: MainContextStatus
    theme: ThemeChangePayload
    context?: ContextSnapshotPayload
    queryContext?: QueryContextPayload
    renderedSourceIds?: Source.Id[]
    notes?: NotesChangedPayload
    selectedEventId?: Doc.Id | null
    wsId?: string
    tableSourceId?: Source.Id
    dialog?: DialogChangedPayload
  }

  export interface NotesChangedPayload {
    operationId?: Operation.Id | null
    notes: Note.Type[]
  }

  export type NotesDeltaPayload = NotesRealtimeSyncChange

  export type SerializedNanoMinMax = {
    min: string
    max: string
  }

  export type DetachedSourceSummary = Omit<
    Source.Type,
    'nanotimestamp' | '_sampleDataCached' | 'settings'
  > & {
    nanotimestamp: SerializedNanoMinMax
    settings: Omit<Source.Type['settings'], 'color_override'>
    _sampleDataCached: Omit<Source.Type['_sampleDataCached'], 'sample_data'> & {
      sample_data: null
    }
  }

  /** Minimal source metadata needed for Notes labels and doc normalization. */
  export interface DetachedNotesSourceSummary {
    id: Source.Id
    operation_id: Operation.Id
    context_id: Context.Id
    type: 'source'
    name: string
    color?: string
    selected: true
    settings: Pick<Source.Type['settings'], 'field' | 'hash_function'>
  }

  /** Minimal context metadata used by Notes row labels. */
  export interface DetachedNotesContextSummary {
    id: Context.Id
    operation_id: Operation.Id
    type: 'context'
    name: string
    color: string
    selected: true
  }

  export interface TableSelectSourcePayload {
    sourceId: Source.Id
  }

  export interface NavigateToEventPayload {
    /** The document ID associated with the note */
    docId: Doc.Id
    /** The operation ID associated with the note */
    operationId: Operation.Id
  }

  export interface EventTargetRequestPayload {
    docId: Doc.Id | null
  }

  export interface FocusTimelineRequestPayload {
    timestamp: number
    onLeft?: boolean
    sourceId?: Source.Id
  }

  export interface TimelineZoomRequestPayload {
    min: number
    max: number
  }

  export interface OpenFilterRequestPayload {
    sourceIds: Source.Id[]
    query: Query.Type
    name?: string
  }

  export interface MainContextStatusPayload {
    status: MainContextStatus
    /** True only when a main-document unload requires a fresh bootstrap. */
    reconnectRequired?: boolean
  }

  export interface DetachedReadyPayload {
    kind: DetachedClientKind
    /** True while the client lacks an active atomic bootstrap. */
    needsBootstrap: boolean
    tableSyncEnabled?: boolean
    notesRenderedSyncEnabled?: boolean
    assistantQuerySyncEnabled?: boolean
  }

  export interface TableSyncChangedPayload {
    enabled: boolean
  }

  export interface NotesRenderedSyncChangedPayload {
    enabled: boolean
  }


  export type MessagePayload = {
    [MessageType.DETACHED_BOOTSTRAP]: DetachedBootstrapPayload
    [MessageType.CONTEXT_SNAPSHOT]: ContextSnapshotPayload
    [MessageType.QUERY_CONTEXT]: QueryContextPayload
    [MessageType.RENDERED_SOURCES_CHANGED]: RenderedSourcesChangedPayload
    [MessageType.EVENT_TARGET_CHANGED]: EventTargetChangedPayload
    [MessageType.ASSISTANT_CONNECTION_CHANGED]: AssistantConnectionChangedPayload
    [MessageType.DIALOG_CHANGED]: DialogChangedPayload
    [MessageType.THEME_CHANGE]: ThemeChangePayload
    [MessageType.NOTES_CHANGED]: NotesChangedPayload
    [MessageType.NOTES_DELTA]: NotesDeltaPayload
    [MessageType.TABLE_SELECT_SOURCE]: TableSelectSourcePayload
    [MessageType.NAVIGATE_TO_EVENT]: NavigateToEventPayload
    [MessageType.EVENT_TARGET_REQUEST]: EventTargetRequestPayload
    [MessageType.FOCUS_TIMELINE_REQUEST]: FocusTimelineRequestPayload
    [MessageType.TIMELINE_ZOOM_REQUEST]: TimelineZoomRequestPayload
    [MessageType.OPEN_FILTER_REQUEST]: OpenFilterRequestPayload
    [MessageType.AUTH_LOST]: Record<string, never>
    [MessageType.AI_HISTORY_UPDATED]: Record<string, never>
    [MessageType.DOCK_DIALOG]: Record<string, never>
    [MessageType.MAIN_CONTEXT_STATUS]: MainContextStatusPayload
    [MessageType.DETACHED_READY]: DetachedReadyPayload
    [MessageType.DETACHED_HEARTBEAT]: Record<string, never>
    [MessageType.DETACHED_CLOSED]: Record<string, never>
    [MessageType.TABLE_SYNC_CHANGED]: TableSyncChangedPayload
    [MessageType.NOTES_RENDERED_SYNC_CHANGED]: NotesRenderedSyncChangedPayload
  }

  export interface Message<T extends MessageType = MessageType> {
    type: T
    payload: MessagePayload[T]
    /** Sender window identifier to prevent echo */
    senderId: string
  }

  export interface Bridge {
    send<T extends MessageType>(type: T, payload: MessagePayload[T]): void
    destroy(): void
  }

  export interface StoredMainContextReplay {
    createdAt: number
    status: MainContextStatusPayload
  }

  /**
   * Creates a new BroadcastChannel instance for cross-window communication.
   *
   * @param id — Unique identifier for the sender (prevents echo)
   * @param onMessage — Callback for incoming messages (filtered to exclude own messages)
   * @returns An object with `send()` and `destroy()` methods
   */
  export function create(
    id: string,
    onMessage: (message: Message) => void,
    coordinatorId = getOrCreateCoordinatorId(),
  ): Bridge {
    return createChannel(
      `${CONTROL_CHANNEL_PREFIX}${coordinatorId}`,
      id,
      onMessage,
    )
  }

  /**
   * Creates one isolated data channel for a detached client.
   *
   * Large main-to-detached payloads use this channel so they are not cloned into
   * unrelated browser realms sharing the control channel.
   *
   * @param clientId Stable detached client identifier.
   * @param senderId Identifier used to suppress sender echoes.
   * @param onMessage Callback for data delivered to this client channel.
   * @returns Client-specific bridge.
   */
  export function createClient(
    clientId: string,
    senderId: string,
    onMessage: (message: Message) => void,
    coordinatorId = getOrCreateCoordinatorId(),
  ): Bridge {
    return createChannel(
      `${CLIENT_CHANNEL_PREFIX}${coordinatorId}:${clientId}`,
      senderId,
      onMessage,
    )
  }

  /**
   * Creates a narrow channel for IndexedDB chat-history invalidation.
   *
   * History notifications are relevant only to assistant panels for the same
   * operation. Keeping them off the coordinator control channel avoids waking
   * Notes, Table, Dashboard, and Dialog clients.
   *
   * @param id Sender identifier used for echo suppression.
   * @param operationId Operation whose assistant history is synchronized.
   * @param onMessage Incoming history notification callback.
   * @param coordinatorId Owning main-tab namespace.
   * @returns Bridge bound to the assistant-history channel.
   */
  export function createAssistantHistory(
    id: string,
    operationId: Operation.Id,
    onMessage: (message: Message) => void,
    coordinatorId = getOrCreateCoordinatorId(),
  ): Bridge {
    return createChannel(
      `${ASSISTANT_HISTORY_CHANNEL_PREFIX}${coordinatorId}:${operationId}`,
      id,
      onMessage,
    )
  }

  /**
   * Creates a bridge on one explicit BroadcastChannel name.
   *
   * @param channelName Browser BroadcastChannel name.
   * @param id Sender identifier used for echo suppression.
   * @param onMessage Incoming message callback.
   * @returns Bridge bound to the selected channel.
   */
  function createChannel(
    channelName: string,
    id: string,
    onMessage: (message: Message) => void,
  ): Bridge {
    const channel = new BroadcastChannel(channelName)

    channel.onmessage = (event: MessageEvent<Message>) => {
      // Ignore own messages
      if (event.data.senderId === id) return
      onMessage(event.data)
    }

    return {
      send<T extends MessageType>(type: T, payload: MessagePayload[T]) {
        const message = { type, payload, senderId: id } satisfies Message<T>
        channel.postMessage(message)
      },
      destroy() {
        channel.close()
      },
    }
  }

  /**
   * Generate a unique window identifier for BroadcastChannel filtering.
   */
  export function generateId(): string {
    return `win_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
  }

  const documentGlobal = globalThis as typeof globalThis & {
    [DOCUMENT_ID_GLOBAL_KEY]?: string
  }
  const documentId =
    documentGlobal[DOCUMENT_ID_GLOBAL_KEY] ?? generateId()
  if (!documentGlobal[DOCUMENT_ID_GLOBAL_KEY]) {
    Object.defineProperty(documentGlobal, DOCUMENT_ID_GLOBAL_KEY, {
      configurable: true,
      value: documentId,
    })
  }
  let ownedMainCoordinatorId: string | null = null
  let coordinatorLifecycleRegistered = false

  /**
   * Reads the coordinator owner written by the current top-level page session.
   *
   * A new tab can receive a cloned sessionStorage snapshot from its opener.
   * The document ID lets the new realm recognize that the copied coordinator
   * is still actively owned elsewhere.
   *
   * @returns Parsed ownership record, or null when it is absent or invalid.
   */
  function readCoordinatorOwnership(): CoordinatorOwnership | null {
    try {
      const raw = sessionStorage.getItem(COORDINATOR_OWNERSHIP_SESSION_KEY)
      if (!raw) return null

      const ownership = JSON.parse(raw) as CoordinatorOwnership
      if (
        !ownership.coordinatorId ||
        !ownership.documentId ||
        (ownership.phase !== 'active' &&
          ownership.phase !== 'transitioning')
      ) {
        return null
      }
      return ownership
    } catch {
      return null
    }
  }

  /**
   * Records which document currently owns a main coordinator.
   *
   * @param coordinatorId Coordinator namespace retained by this page session.
   * @param phase Whether this document is active or handing off to a navigation.
   * @returns Nothing.
   */
  function writeCoordinatorOwnership(
    coordinatorId: string,
    phase: CoordinatorOwnership['phase'],
  ): void {
    sessionStorage.setItem(
      COORDINATOR_OWNERSHIP_SESSION_KEY,
      JSON.stringify({
        coordinatorId,
        documentId,
        phase,
      } satisfies CoordinatorOwnership),
    )
  }

  /**
   * Registers navigation hand-off markers without polling or heartbeat traffic.
   *
   * `pagehide` runs for same-tab reloads and navigations, but not when another
   * top-level context merely clones sessionStorage. A BFCache restore reclaims
   * the coordinator when its document becomes active again.
   */
  function registerCoordinatorLifecycle(): void {
    if (
      coordinatorLifecycleRegistered ||
      typeof window === 'undefined'
    ) {
      return
    }
    coordinatorLifecycleRegistered = true

    window.addEventListener('pagehide', () => {
      if (!ownedMainCoordinatorId) return
      try {
        const storedCoordinatorId = sessionStorage.getItem(
          COORDINATOR_SESSION_KEY,
        )
        const ownership = readCoordinatorOwnership()
        if (
          storedCoordinatorId !== ownedMainCoordinatorId ||
          ownership?.coordinatorId !== ownedMainCoordinatorId ||
          ownership.documentId !== documentId
        ) {
          return
        }
        writeCoordinatorOwnership(
          ownedMainCoordinatorId,
          'transitioning',
        )
      } catch {
        // Storage may be unavailable during document teardown.
      }
    })

    window.addEventListener('pageshow', (event) => {
      if (!event.persisted || !ownedMainCoordinatorId) return
      try {
        if (
          sessionStorage.getItem(COORDINATOR_SESSION_KEY) ===
          ownedMainCoordinatorId
        ) {
          writeCoordinatorOwnership(ownedMainCoordinatorId, 'active')
        }
      } catch {
        // Storage may be unavailable after a browser privacy-policy change.
      }
    })
  }

  /**
   * Returns a coordinator namespace owned by this top-level browsing context.
   *
   * Detached routes always adopt their explicit query parameter. Main documents
   * retain their coordinator across same-tab navigation hand-offs, while a new
   * or duplicated tab rejects an actively-owned sessionStorage clone.
   */
  export function getOrCreateCoordinatorId(): string {
    const routedCoordinatorId =
      typeof window === 'undefined' ||
      !window.location.pathname.startsWith('/detached/')
        ? null
        : new URLSearchParams(window.location.search).get('coordinatorId')
    if (routedCoordinatorId) {
      if (
        sessionStorage.getItem(COORDINATOR_SESSION_KEY) !==
        routedCoordinatorId
      ) {
        sessionStorage.setItem(
          COORDINATOR_SESSION_KEY,
          routedCoordinatorId,
        )
      }
      return routedCoordinatorId
    }

    if (ownedMainCoordinatorId) return ownedMainCoordinatorId

    try {
      const existing = sessionStorage.getItem(COORDINATOR_SESSION_KEY)
      const ownership = readCoordinatorOwnership()
      const coordinatorId =
        existing &&
        ownership?.coordinatorId === existing &&
        (ownership.phase === 'transitioning' ||
          ownership.documentId === documentId)
          ? existing
          : generateId()

      sessionStorage.setItem(COORDINATOR_SESSION_KEY, coordinatorId)
      ownedMainCoordinatorId = coordinatorId
      writeCoordinatorOwnership(coordinatorId, 'active')
      registerCoordinatorLifecycle()
      return coordinatorId
    } catch {
      // Preserve isolation when storage is disabled, although reload retention
      // cannot be provided without a page-session store.
      ownedMainCoordinatorId = generateId()
      return ownedMainCoordinatorId
    }
  }

  /**
   * Registers the established client channel as this detached realm's action
   * transport. The returned disposer is identity-safe when a provider remounts.
   *
   * @param coordinatorId Owning main-tab namespace.
   * @param bridge Established private bridge for this detached client.
   * @returns Function that removes this exact registration.
   */
  export function registerPrivateActionSender(
    coordinatorId: string,
    bridge: Bridge,
  ): () => void {
    privateActionSenders.set(coordinatorId, bridge)
    return () => {
      if (privateActionSenders.get(coordinatorId) === bridge) {
        privateActionSenders.delete(coordinatorId)
      }
    }
  }

  /**
   * Sends a detached action through the established client channel when one is
   * available. Registration and close lifecycle messages fall back to the
   * lightweight coordinator control channel.
   *
   * @param type Control message type.
   * @param payload Action payload consumed by the owning main document.
   * @returns Nothing.
   */
  export function sendControlMessage<T extends MessageType>(
    type: T,
    payload: MessagePayload[T],
    coordinatorId = getOrCreateCoordinatorId(),
  ): void {
    const privateSender = privateActionSenders.get(coordinatorId)
    if (privateSender) {
      privateSender.send(type, payload)
      return
    }

    let sender = controlSenders.get(coordinatorId)
    if (!sender) {
      sender = create(generateId(), () => {}, coordinatorId)
      controlSenders.set(coordinatorId, sender)
    }
    sender.send(type, payload)
  }

  /**
   * Persists the last main-tab context so detached windows can recover when
   * BroadcastChannel delivery is interrupted by login redirects or reloads.
   *
   * @param replay - Serializable main-context replay payload.
   * @returns Nothing.
   */
  function writeStoredMainContext(
    replay: StoredMainContextReplay,
    coordinatorId: string,
  ): void {
    try {
      const storedReplay: StoredMainContextReplay = {
        createdAt: replay.createdAt,
        status: replay.status,
      }
      const serializedReplay = JSON.stringify(storedReplay)
      localStorage.setItem(
        `${MAIN_CONTEXT_STORAGE_PREFIX}${coordinatorId}`,
        serializedReplay,
      )
    } catch (error) {
      console.warn('Failed to store detached main context fallback', {
        error,
        status: replay.status.status,
      })
    }
  }

  /**
   * Creates a source summary that is safe to post between windows.
   *
   * @param source - Source from the main application state.
   * @returns Serializable detached-window source summary.
   */
  function createDetachedSourceSummary(
    source: Source.Type,
  ): DetachedSourceSummary {
    const { nanotimestamp, _sampleDataCached, settings, ...summary } = source
    const { color_override: _colorOverride, ...detachedSettings } = settings
    const fallbackTimestamp = source.timestamp ?? { min: 0, max: 0 }
    const fallbackCache: Source.Type['_sampleDataCached'] = {
      frequency_sample: source.settings?.frequency_sample ?? 0,
      min_timestamp: fallbackTimestamp.min,
      max_timestampe: fallbackTimestamp.max,
      sample_data: null,
    }
    const sampleDataCache = _sampleDataCached ?? fallbackCache

    /**
     * Serializes a nanosecond timestamp while tolerating partially-normalized sources.
     *
     * @param value - Source nanosecond timestamp value.
     * @param fallbackMs - Millisecond timestamp fallback.
     * @returns Nanosecond timestamp string for detached hydration.
     */
    const serializeNanoTimestamp = (
      value: bigint | number | string | undefined,
      fallbackMs: number,
    ): string => {
      if (typeof value === 'bigint') return value.toString()
      if (typeof value === 'number' || typeof value === 'string') return String(value)
      return Math.round(fallbackMs * 1_000_000).toString()
    }

    return {
      ...summary,
      settings: detachedSettings,
      nanotimestamp: {
        min: serializeNanoTimestamp(nanotimestamp?.min, fallbackTimestamp.min),
        max: serializeNanoTimestamp(nanotimestamp?.max, fallbackTimestamp.max),
      },
      _sampleDataCached: {
        frequency_sample: sampleDataCache.frequency_sample,
        min_timestamp: sampleDataCache.min_timestamp,
        max_timestampe: sampleDataCache.max_timestampe,
        sample_data: null,
      },
    }
  }

  /**
   * Projects only the source identity and normalization settings consumed by
   * Notes. Render palettes, grants, timestamps, totals, and plugin metadata are
   * deliberately excluded.
   */
  function createDetachedNotesSourceSummary(
    source: Source.Type,
  ): DetachedNotesSourceSummary {
    return {
      id: source.id,
      operation_id: source.operation_id,
      context_id: source.context_id,
      type: 'source',
      name: source.name,
      color: source.color,
      selected: true,
      settings: {
        field: source.settings.field ?? '_id',
        hash_function: source.settings.hash_function ?? 'fnv1a',
      },
    }
  }

  /**
   * Selects filters that detached synced views actually need.
   *
   * @param app - Current application state.
   * @param sourceIds - Source identifiers included in the detached snapshot.
   * @returns Serializable filters keyed by source id.
   */
  function createDetachedFilters(
    app: App.Type,
    sourceIds: Source.Id[],
  ): Partial<Record<Source.Id, Query.Type>> {
    return Object.fromEntries(
      sourceIds
        .map((sourceId) => [sourceId, app.target.filters[sourceId]] as const)
        .filter(([, filter]) => Boolean(filter)),
    ) as Partial<Record<Source.Id, Query.Type>>
  }

  /**
   * Projects the selected operation context required by detached views.
   *
   * @param app Main application state.
   * @param operationId Operation selected by the current route.
   * @returns Serializable operation, context, and selected-source state.
   */
  export function createContextSnapshot(
    app: App.Type,
    operationId?: Operation.Id | null,
    options: ContextSnapshotOptions = {},
  ): ContextSnapshotPayload {
    const operation = operationId
      ? app.target.operations.find((item) => item.id === operationId) ?? null
      : app.target.operations.find((item) => item.selected) ?? null
    const selectedOperationId = operation?.id ?? operationId ?? null
    const requestedSourceIds = options.sourceIds
      ? new Set(options.sourceIds)
      : null
    const selectedSources = options.operationOnly
        ? []
      : app.target.files.filter(
        (source) =>
          (!selectedOperationId || source.operation_id === selectedOperationId) &&
          (requestedSourceIds
            ? requestedSourceIds.has(source.id)
            : source.selected),
      )
    const selectedContextIds = new Set(
      selectedSources.map((source) => source.context_id),
    )

    return {
      operation: operation
        ? {
          ...operation,
          selected: true,
        }
        : null,
      contexts: selectedOperationId && !options.operationOnly
        ? app.target.contexts.filter(
          (context) =>
            context.operation_id === selectedOperationId &&
            selectedContextIds.has(context.id),
        ).map((context) =>
          options.compactNotes
            ? {
              id: context.id,
              operation_id: context.operation_id,
              type: 'context' as const,
              name: context.name,
              color: context.color,
              selected: true as const,
            }
            : context,
        )
        : [],
      sources: selectedSources.map((source) =>
        options.compactNotes
          ? createDetachedNotesSourceSummary(source)
          : createDetachedSourceSummary(source),
      ),
      user: options.includeDialogUser
        ? app.general.user
          ? {
            id: app.general.user.id,
            showPreview: app.general.user.user_data?.show_preview,
          }
          : null
        : undefined,
    }
  }

  /**
   * Projects filters and the main timeline frame for query-synced clients.
   *
   * @param app Main application state.
   * @param operationId Active operation identifier.
   * @returns Query synchronization state.
   */
  export function createQueryContext(
    app: App.Type,
    operationId?: Operation.Id | null,
  ): QueryContextPayload {
    const sourceIds = app.target.files
      .filter(
        (source) =>
          source.selected &&
          (!operationId || source.operation_id === operationId),
      )
      .map((source) => source.id)

    return {
      filters: createDetachedFilters(app, sourceIds),
      frame: app.timeline.frame,
    }
  }

  /**
   * Computes Canvas-rendered source rows without importing UI-heavy source helpers.
   *
   * @param app - Current application state.
   * @param operationId - Optional operation scope for detached windows.
   * @returns Ordered source IDs rendered as timeline rows.
   */
  export function createRenderedSourceIds(
    app: App.Type,
    operationId?: Operation.Id | null,
  ): Source.Id[] {
    const filter = (app.timeline.filter || '').toLowerCase()
    const contextNameById = new Map<Context.Id, string>(
      app.target.contexts.map((context) => [
        context.id,
        context.name?.toLowerCase() ?? '',
      ]),
    )

    return app.target.files
      .filter(
        (source) =>
          source.selected &&
          (!operationId || source.operation_id === operationId),
      )
      .sort((left, right) =>
        left.pinned === right.pinned ? 0 : left.pinned ? -1 : 1,
      )
      .filter((source) =>
        app.hidden.sourcesWithNoEvents
          ? DataStore.hasRenderableSourceEvents(source.id, source.total)
          : true,
      )
      .filter((source) => {
        if (!filter) return true
        return (
          source.name?.toLowerCase().includes(filter) ||
          (contextNameById.get(source.context_id) ?? '').includes(filter)
        )
      })
      .map((source) => source.id)
  }

  /**
   * Reads the latest stored main-tab context replay.
   *
   * @returns Stored replay payload, or null when none exists or parsing fails.
   */
  export function readStoredMainContext(
    coordinatorId = getOrCreateCoordinatorId(),
  ): StoredMainContextReplay | null {
    try {
      const raw = localStorage.getItem(
        `${MAIN_CONTEXT_STORAGE_PREFIX}${coordinatorId}`,
      )
      if (!raw) return null

      const replay = JSON.parse(raw) as StoredMainContextReplay
      if (!replay.status?.status || typeof replay.createdAt !== 'number') {
        return null
      }

      return replay
    } catch (error) {
      console.warn('Failed to read detached main context fallback', {
        error,
      })
      return null
    }
  }

  /**
   * Gets the storage key used for stored detached-window context replay.
   *
   * @returns LocalStorage key that contains the latest main context.
   */
  export function getMainContextStorageKey(
    coordinatorId = getOrCreateCoordinatorId(),
  ): string {
    return `${MAIN_CONTEXT_STORAGE_PREFIX}${coordinatorId}`
  }

  /**
   * Stores one semantic main lifecycle state for detached-window recovery.
   *
   * Active clients still request an atomic private bootstrap; the storage value
   * is only a lightweight wake-up and idle/auth guard.
   *
   * @param status Current main application lifecycle state.
   * @returns Nothing.
   */
  export function persistMainStatus(
    status: MainContextStatus,
    coordinatorId = getOrCreateCoordinatorId(),
    reconnectRequired = false,
  ): void {
    writeStoredMainContext({
      createdAt: Date.now(),
      status: {
        status,
        ...(reconnectRequired ? { reconnectRequired: true } : {}),
      },
    }, coordinatorId)
  }

  /**
   * Resolves the source id carried by a note, tolerating legacy embedded-doc payloads.
   *
   * @param note - Note from the shared DataStore.
   * @returns Source id used to filter notes for detached operation snapshots.
   */
  function getNoteSourceId(note: Note.Type): Source.Id {
    return note.source_id ?? note.doc?.['gulp.source_id']
  }

  /**
   * Restricts a realtime note upsert to the sources selected for a detached
   * Notes client before the payload reaches BroadcastChannel.
   *
   * Delete deltas contain only identifiers and are retained as-is. A fully
   * irrelevant upsert returns null so the coordinator sends no private message.
   *
   * @param change Realtime note delta published by the main DataStore.
   * @param selectedSourceIds Sources represented by the detached Notes snapshot.
   * @returns Original or filtered delta, or null when no upsert note is relevant.
   */
  export function filterNotesDeltaForSelectedSources(
    change: NotesDeltaPayload,
    selectedSourceIds: ReadonlySet<Source.Id>,
  ): NotesDeltaPayload | null {
    if (change.mutation === 'delete') return change

    const notes = change.notes.filter((note) =>
      selectedSourceIds.has(getNoteSourceId(note)),
    )
    if (notes.length === 0) return null
    if (notes.length === change.notes.length) return change

    return {
      ...change,
      notes,
    }
  }

  /**
   * Builds the notes payload for the selected sources in the active operation.
   *
   * @param app - Current application state.
   * @param operationId - Operation whose selected notes should be sent.
   * @returns Notes snapshot payload for detached windows.
   */
  export function createNotesChangedPayload(
    app: App.Type,
    operationId?: Operation.Id | null,
  ): NotesChangedPayload {
    const selectedSourceIds = new Set(
      app.target.files
        .filter((source) =>
          source.selected &&
          (!operationId || source.operation_id === operationId),
        )
        .map((source) => source.id),
    )

    return {
      operationId,
      notes: DataStore.notes.filter((note) =>
        selectedSourceIds.has(getNoteSourceId(note)),
      ),
    }
  }

}
