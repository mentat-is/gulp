# v2.0.0

Changes since the last published gULP tag, `v1.7.605`.

## Repository Integration

- Moved `gulp-ui` into the main gULP repository and production image, with a
  shared release version, a dedicated `gulp-webui` launcher, and corrected
  paid-plugin discovery paths.

## Query, Timeline, and Rendering

- Moved Canvas rendering to a dedicated RenderWorker with bounded queues,
  acknowledgements, telemetry, cache invalidation, and a RAM compatibility
  fallback.
- Added managed query lifecycles with cancellation, viewport-prioritized
  refetching, and an IndexedDB-backed source-field cache.
- Added stable PIT-based Table View pagination and preserved deterministic
  ordering for events that share an ingestion timestamp.
- Applied selected frame ranges consistently to queries and source counts,
  preserved in-progress date edits, and hid sources with no matching events.
- Added configurable IANA time zones for timestamp presentation and corrected
  pointer, graph, source-offset, rebase, and active-viewport calculations.
- Added filename/full-path source labels and persisted table column visibility.

## Ingestion and Collaboration

- Added ingestion recovery polling, clearer in-progress source state, stronger
  plugin detection and validation, and optional mapping selection and removal.
- Added real-time note delta synchronization, paginated and filtered note
  views, note-creation banners, and request-aware bulk deletion updates.
- Improved user-session restoration and autosave behavior so active sessions
  are not removed by unnecessary saves.

## Detached Windows and User Experience

- Strengthened detached-window synchronization for timeline targets, notes,
  filters, AI history, and inter-window messages without redundant rerenders.
- Added default session names and icons, corrected Dashboard query conditions,
  made column menus safe without a maximum height, and expanded localized
  labels for polling and file-size errors.
- Added worker, cache, request-lifecycle, timestamp, detached-window, and
  rendering regression coverage.

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

- `Timeline Frame Queries`: Applied the selected frame as the offset-adjusted backend range for every selected source, including in-flight rendering and subsequent structured or manual filters.
- `Timeline Frame Counts`: Limited rendered source counts to loaded events inside the selected timeline frame while preserving the canonical source total.
- `Frame Date Selection`: Preserved in-progress From and To edits while timeline request-loading updates rerender the frame banner.
- `Timeline Source Labels`: Added a source setting to display either a file name or full path.
- `Autosave Refactoring`: Replaced interval-based autosaving with a debounced effect.
- `WebSocket Reconnection`: Handled WebSocket reconnections using existing WSID configurations.
- `Safe Component Unmounting`: Implemented unmount delays and fallback clipboard logic for UI consistency.
- `Source Deletion Safety`: Resolved an application crash when deleting a source that was actively selected in the event dialog.
- `Plugin Configuration Fixes`: Corrected advanced parameter plugins configurations and bridge status reloads.
