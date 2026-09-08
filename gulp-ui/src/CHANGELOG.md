# v2.0.0

## Advanced Filtering and Querying

- `Source query cache pool`: Added up to five IndexedDB query generations per source, exact startup reuse with latest-cache fallback, source-local Undo/Redo, query provenance restoration, rollback-safe forced queries, and cache invalidation for rebase/source deletion.
- `Source query range coverage`: Reuses fresh same-query caches when their source-local range covers the newly selected timeframe, while preserving source-change warnings and latest-cache fallback behavior.
- `Table View deep pagination`: Keeps a point-in-time snapshot per semantic table query, reuses page cursors for adjacent navigation, supports direct jumps beyond the OpenSearch result window, and releases snapshots when the table query changes or closes.
- `Table View wide schemas`: Added opt-in horizontal column virtualization,
  complete virtualized and searchable column visibility, non-blocking
  eight-source field discovery, compact detached projections, and hybrid
  dotted-field resolution for `gulp.unmapped.*` and `gulp.enriched.*` values;
  the timestamp sorting selector now remains populated after asynchronous field
  discovery.

## UI/UX and Rendering

- `Post-ingest timeline movement`: Reused complete transferred projections while
  normal pans remain inside horizontal overscan, while preserving fresh requests
  for building sources, authority changes, and terminal convergence.
- `Progressive uncached queries`: Rendered partial `query_raw` replacement pages
  as their chunks arrive for both initial operation loading and Navigator event
  filters, immediately revealing sources hidden by their zero-event building
  summary without waiting for a full EventStore page or committing small pages;
  in-flight IndexedDB appends and selected sources whose query has not registered
  yet no longer block the complete multi-source viewport.
- `Global timeline source actions`: Right-clicking outside source rows now opens
  a source-menu-styled context menu that can reuse caches while refetching or
  authoritatively reset filters in bulk for every selected source, including
  sources hidden from the Canvas.
- `Cache freshness`: Added localized query summaries and freshness reasons to the source menu. A shared DOM `AlertTriangle` with a cache-origin tooltip now marks only the exact source ID owning a settled active warning; same-name sources, valid caches, and newly fetched backend results remain unmarked.
- `Frame-local counts`: Canvas source labels now show the exact event count inside the selected timeframe even when the active IndexedDB generation covers a wider range.
- `External query ingestion`: Opened live source selection when an external
  ingestion is accepted and made closing it return to the configured Query
  External banner.
- `Live ingestion source counts`: Added compact cumulative QueryWorker progress
  for file, external-query, and registered realtime ingestion so source badges
  advance for partial EventStore pages without transferring documents to the
  main thread; query, refetch, and enrichment streams remain excluded.

## Architecture and Documentation

- `Viewport projection locality`: Batched visible-source manifest fences and
  added revision-qualified 16 MiB exact, 4 MiB LOD, and 4 MiB HeightMap page
  working sets in QueryWorker, avoiding repeated IndexedDB clones for overlapping
  ingestion pages under a fixed memory bound.
- `Transient replacement projection`: Added revision-fenced, worker-local partial
  pages and payload-free Canvas invalidations while preserving dense final
  IndexedDB pages and the existing normalized-document memory ceiling.
- `Ingestion EventStore build`: Combined 1,500–2,000-event ingestion chunks
  into 16,384-event pages, journaled raw overflow in IndexedDB without backend
  ACK/backpressure, and promoted completed ingestion generations into the
  reusable source query-cache pool using `gulp.event_code` hashing.
- `Independent ingestion failures`: Browser cache failures now leave partial
  generations incomplete and keep the authoritative backend ingestion running
  instead of issuing `/request_cancel`; realtime ingestion is unchanged.
- `Ingestion event ordering`: Persisted `event.sequence` in EventStore schema
  v6 and used it as the descending tie-breaker after descending timestamps,
  including across ingestion chunks, page boundaries, cursors, and neighbors.
- `IndexedDB`: Added `source_query_caches`, logical `authorityRevision`, bounded resumable pruning, QueryWorker cache commands, detached-window synchronization, and updated EventStore architecture documentation.
- `IndexedDB identity`: Namespaced EventStore databases, revision channels, and writer locks by canonical backend plus authenticated user so caches survive browser restarts; retained tab-scoped WindowBridge coordination and isolated auth-user transitions.
- `Render protocol v9`: Added `frameEventCount`, kept `loadedCount` as terminal authority, and removed obsolete cache-freshness metadata from the render-worker contract.
- `External query lifecycle`: Treated `ext_query` statistics as ingestion
  completion fences so provisional EventStore sources finalize, leave loading
  state, and remain renderable and refetchable without using `query_done` as a
  storage fence.

# v1.7.400

## Performance and Engine Optimizations

- `High Volume Event Rendering`: Optimized the rendering engine to load over 10 million events with virtualizer support.
- `Web Worker Ingestion`: Used a web worker during the file upload ingestion phase to prevent blocking the UI thread.
- `Viewport Highlights Culling`: Configured highlights and documents to render only within the visible viewport to reduce browser memory usage.
- `Graph Engine Alignment`: Refactored the graph rendering engine to improve bucket alignment, viewport culling, and timestamp correction.

## Multi-Window and Detached Tabs

- `Cross-Window Synchronization`: Implemented real-time synchronization for timeline events, frames, and filters across detached windows.
- `Source Table in Detached Window`: Added a Table View window to display paginated source events, with support for row actions, column sorting, and query-builder filtering.
- `Notes Table in Detached Window`: Added a Table View window to display notes, with support for row actions (delete and focus on event), bulk delete and show notes filtering.

## UI/UX and Customization

- `Session Saving`: Defaulted unnamed sessions to the username-based session name and selected the panel-bottom icon.
- `New Theme System`: Add new themes management, allowing users to define custom styles and themes with fallback logic.
- `Context-Based Event Coloring`: Upgraded the rendering engine to support dynamic hex alpha values and custom context-based color schemes.
- `Dockable Event Dialog`: Supported docking and undocking behavior for the Event Dialog window.
- `Dynamic Scale Zoom`: Added dynamic timeline scaling zoom-in with key controls.
- `Refined Data Tree and Table Views`: Redesigned JSON document tree and table views to support text selection, scrolling, sorted keys, and tooltips.

## Advanced Filtering and Querying

- `Nested Filter Conditions`: Added support for nested filter rules within the query builder.
- `Global Event Log Search`: Added an interactive global filtering input component in the Navigator.
- `Query History`: Added last filter popover component.

## Bug Fixes and Stability

- `Selected Event Crosshair`: Kept the timeline crosshair on the event visible
  in Event Detail when an empty position inside a source row is clicked.
- `Timeline Frame Queries`: Applied the selected frame as the offset-adjusted backend range for every selected source, including in-flight rendering and subsequent structured or manual filters.
- `Timeline Frame Counts`: Displayed the exact IndexedDB manifest count for frame-filtered queries across every source label, independently from bounded LOD representatives, while preserving the canonical source total.
- `Frame Date Selection`: Preserved in-progress From and To edits while timeline request-loading updates rerender the frame banner.
- `Timeline Source Labels`: Added a source setting to display either a file name or full path.
- `Autosave Refactoring`: Replaced interval-based autosaving with a debounced effect.
- `WebSocket Reconnection`: Handled WebSocket reconnections using existing WSID configurations.
- `Safe Component Unmounting`: Implemented unmount delays and fallback clipboard logic for UI consistency.
- `Source Deletion Safety`: Resolved an application crash when deleting a source that was actively selected in the event dialog.
- `Plugin Configuration Fixes`: Corrected advanced parameter plugins configurations and bridge status reloads.
