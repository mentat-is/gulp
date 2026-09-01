# Engine

The render engine controls how each source row is drawn on the timeline canvas.
Source settings also control offset, sampling, colors, and field hashing.

The primary timeline canvas is owned by a render worker. A query worker reads
the operation EventStore and supplies only the event representatives and
aggregates required for the visible viewport.
[Query worker and event storage architecture](query-worker.md) and
[Render worker and LOD projection architecture](render-worker.md) describe the
component internals. [Timeline rendering architecture](engine.architecture.md)
provides the end-to-end map.

![source settings](./images/source_settings.png)

## Render Engines

gULP UI has three source render engines:

- `Default`: draws event activity as one-pixel vertical bars colored from the
  configured event field and palette.
- `HeightMap`: groups events into fixed one-minute buckets and draws vertical
  bars proportional to event density.
- `Graph`: aggregates source sample data into dynamic buckets, draws a semi-log
  density graph, and connects graph points.

The source context menu can switch engines directly.

## Source Settings

The source settings banner is opened from the source context menu. It saves
settings through the frontend source settings flow and refreshes rendering.

Configurable source settings:

- timestamp offset in milliseconds;
- minimum frequency sample for graph rendering;
- render engine;
- hash function used for event value coloring;
- render color palette;
- event field used for the color scheme;
- context color.

If a knowledge-base color override or custom palette is applied, source settings
can show the active override and allow clearing it.

## Timestamp Offset

Offset shifts a source's rendered event timestamps without changing the original
event data. This is useful when a source clock is known to be ahead or behind the
operation timeline.

Offset is stored in milliseconds and applied during rendering, event hit testing,
note positioning, and timeline targeting.

## Frequency Sample

The Graph engine uses `frequency_sample` as the minimum sample size. When zoomed
out, the engine dynamically increases the sample rate based on visible time range
and canvas width to avoid excessive overlap and keep rendering performant.

## Timeline Zoom Limit

All Canvas zoom paths use a maximum scale derived from the active frame. Maximum
zoom keeps a ten-millisecond duration visible, so the ruler displays approximately
ten one-millisecond sections independently of Canvas width. Timeline events are
normalized to whole milliseconds; maximum zoom separates adjacent millisecond
timestamps but does not invent sub-millisecond event precision.

Holding `Alt` while dragging horizontally over the timeline zooms the Canvas to
the selected interval. The selected scale and horizontal position are committed
together, and the resulting viewport is rendered on the next Canvas display
frame.

Render work at maximum zoom remains based on viewport pixels rather than the
overall frame span. The query worker projects only the visible source rows and an
overscanned horizontal viewport into pixel-sized buckets. A render window is
capped at 1,000,000 representatives and automatically uses wider degraded-LOD
buckets if that cap would be exceeded. Deep zoom can require additional exact-page
refinement for dense sub-second intervals, but those reads remain worker-owned,
page-bounded, and superseded by newer viewport requests.

## Colors and Fields

Default rendering hashes the configured event field into stable numeric values
for palette lookup. Explicit color overrides can replace palette colors for
selected field values. HeightMap colors its density counts across the selected
palette; it does not color individual buckets from the configured event field.
Graph maps its semi-log count values across the selected palette.

Context color is used as the row tint and separator color so related sources can
be visually grouped.

## Notes whose events are outside the active query

Notes and grouped notes are positioned from the document identity, source, and
timestamp embedded in `note.doc`. They therefore remain visible when their
events are outside the active source query. When empty source rows are hidden, a
source still remains on the timeline while it has a visible note; hiding notes
removes that note-only exception.

Selecting a note whose event is unavailable shows a localized confirmation to
retrieve it. A grouped marker uses one confirmation for its missing targets,
deduplicates repeated document IDs, and retains successful results if retrieval
is only partially successful. Retrying requests only unresolved targets.

Retrieved targets become session-only timeline events. They are drawn in all
three engines, can open the standard event dialog, and participate in
previous/next and first/last navigation in main and detached dialogs. They do
not change source totals, the selected timeframe, LOD aggregates, query-result
counts, or cache freshness.

Included note targets are intentionally separate from the IndexedDB query-cache
pool. A new replacement query for the source, successful source-local Undo/Redo,
source removal, or operation reset clears them. A failed Undo/Redo attempt,
viewport movement, note reload, and dialog close leave them available.

The retrieval, overlay, and rendering boundaries are documented in
[Timeline rendering architecture](engine.architecture.md#supplemental-note-linked-events)
and [Render worker and LOD projection architecture](render-worker.md#supplemental-note-linked-events).

## Caching

Exact timeline events are stored in an operation-scoped IndexedDB EventStore.
The render worker keeps a bounded render window for the visible source rows and
time range, including horizontal and vertical overscan. It does not cache the
complete event collection.

The Default engine receives pixel-equivalent event representatives, HeightMap
receives persisted density counts, and Graph receives viewport-scale samples.
Palette-only changes reuse the exact stored pages; source revisions, offsets,
engine settings, overrides, notes, flags, or viewport changes publish the
corresponding revisioned render state.

The small operation-scoped collection of explicitly included note targets is a
revisioned render overlay owned by the main thread. It is never written to an
IndexedDB query generation and is not restored by query-cache Undo/Redo.
