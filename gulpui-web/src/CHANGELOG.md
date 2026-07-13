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

- `Autosave Refactoring`: Replaced interval-based autosaving with a debounced effect.
- `WebSocket Reconnection`: Handled WebSocket reconnections using existing WSID configurations.
- `Safe Component Unmounting`: Implemented unmount delays and fallback clipboard logic for UI consistency.
- `Source Deletion Safety`: Resolved an application crash when deleting a source that was actively selected in the event dialog.
- `Plugin Configuration Fixes`: Corrected advanced parameter plugins configurations and bridge status reloads.
