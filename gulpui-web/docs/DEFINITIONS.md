# Definitions

The main gulpui-web documentation starts at the root [README.md](../README.md).

## Operation

An operation is the investigation workspace. It groups contexts, sources, events,
notes, links, filters, requests, sessions, permissions, and plugin output for one
incident or case. See [Flow](flow.md).

## Context

A context is a logical container inside an operation, commonly representing a
host, system, data owner, or investigation scope. Contexts group related sources.

## Source

A source is a log or event collection inside a context. Sources are selected into
the timeline and can be filtered, enriched, rendered, opened in table view, or
included in dashboards.

## Event

An event is a single document from a source. Event details can be viewed as raw
JSON, table rows, or a tree. Events can be flagged, enriched, sent as IOC data,
annotated with notes, and connected with links.

## Timeline

The timeline is the central operation view. It renders selected sources as rows,
places events by timestamp, displays notes and links, and opens event detail
views when events are selected.

## Timeline Frame

The timeline frame is the active time range. It controls what the timeline loads
and renders, and it is reused by filters, table view, dashboard, and enrichment
defaults.

## Session

A session stores selected operation state such as selected sources, timeline
frame, dialog state, target event, notes, filters, and visual state. Sessions can
be autosaved or manually saved.

## Ingestion

Ingestion adds local files or zip packages to an operation. It requires a
context, selected files, parser plugin settings, and any required mapping
configuration. See [Ingestion](ingestion.md).

## Ingestion Plugin

An ingestion plugin is a backend parser used to process a source file format,
for example EVTX. The frontend selects the plugin and sends its settings during
file ingestion.

## Mapping File

A mapping file is a parser-specific mapping set. It tells the backend how source
fields should be converted into the event schema used by gULP.

## Mapping ID

A mapping ID selects one mapping inside a mapping file. Some plugins expose a
single mapping and can be auto-selected; others require the analyst to choose.

## Advanced Plugin Parameters

Advanced plugin parameters are optional plugin settings beyond the basic plugin,
mapping file, and mapping ID controls. They can include chunk size, timestamp
offset, unmapped-field handling, custom mappings, Sigma mappings, and custom
plugin parameters.

## Filter

A filter is a source-scoped query that narrows visible or fetched events. Filters
can be built with the query builder or written manually as JSON. See
[Filters](filters.md).

## Query

A query is the structured search payload used by filters, previews, table view,
dashboards, external queries, and backend search calls.

## Preview

Preview runs a query without committing it to the timeline. It lets an analyst
inspect matching events before applying a filter or external query result.

## Note

A note is an analyst annotation attached to one or more events. Notes have a
title, glyph, color, tags, visibility, and markdown-supported text. See
[Features](features.md#notes).

## Link

A link connects related events. Links have a title, glyph, color, and
markdown-supported description, and can be reused across multiple events. See
[Features](features.md#links).

## Glyph

A glyph is the icon selected for operations, notes, links, stories, groups, and
other visual markers.

## Enrichment

Enrichment runs backend-provided enrichment plugins against an event or source
time range. It can add context to selected observable fields or extract fields
from existing event data. See [Features](features.md#enrichment).

## Observable

An observable is a value selected for enrichment or IOC workflows, such as an IP
address, domain, email, hash, URL, MAC address, or custom field value.

## IOC

IOC means indicator of compromise. In gulpui-web, Send IOC actions pass selected
event data to a destination plugin such as MISP.

## Send IOC

Send IOC is the event-detail workflow for plugins targeting the `send-data` slot.
The parent banner selects a destination plugin and calls the plugin's send action.

## Query External

Query External runs backend-provided plugins whose type includes `external`.
External query results can be previewed or ingested as new operation sources. See
[External Integrations](external.md).

## Bridge

A bridge is a registered external ingestion connection managed by the backend.
The UI can check bridge status and manage bridge ingestion tasks.

## Bridge Manager

Bridge Manager is the frontend banner for selecting a bridge, checking its
status, creating ingestion tasks, starting or stopping tasks, deleting tasks, and
refreshing task lists. See [External Integrations](external.md#bridge-manager).

## Plugin

A plugin extends gULP or gulpui-web behavior. Backend plugins can ingest,
enrich, query, or export data. UI plugins are React components loaded into known
frontend slots. See [Plugins](plugins.md).

## UI Plugin Slot

A UI plugin slot is a frontend mount point such as `operation-menu`,
`send-data`, `event-actions`, `sigma-upload-mode`, `dashboard-view`, or
`ai-assistant-window`.

## Operation Menu

The operation menu is the left-side workspace menu. It opens notes, table view,
dashboard, source selection, ingestion, filters, external integrations, plugins,
requests, permissions, settings, and logout/session actions.

## Permission

Permission controls access to operations and resources. The UI exposes user and
group management for granting access to operations.

## User

A user is an authenticated account that can own operations, run actions, and be
granted access directly or through groups.

## Group

A group is a collection of users used to grant permissions more efficiently.

## Render Engine

A render engine controls how source events are drawn on the timeline. Available
engines are `Default`, `HeightMap`, and `Graph`. See [Engine](engine.md).

## Default Engine

The Default engine draws event activity as narrow vertical bars colored from the
configured hash field and color palette.

## HeightMap Engine

The HeightMap engine groups events into fixed buckets and draws bars
proportional to event density.

## Graph Engine

The Graph engine aggregates sample data into dynamic buckets, draws a semi-log
density graph, and connects graph points.

## Source Settings

Source settings control timestamp offset, render engine, frequency sample, hash
function, render color palette, color field, color overrides, and context color.

## Timestamp Offset

Timestamp offset shifts a source's rendered event timestamps by positive or
negative milliseconds without changing the original event payload.

## Frequency Sample

Frequency sample is the minimum sampling interval used by graph rendering. The
Graph engine can increase it dynamically when the timeline is zoomed out.

## Hash Function

The hash function converts event field values into stable numeric values for
color palette lookup during rendering.

## Color Palette

A color palette maps numeric render values to colors. Palettes can be default
gradients, generated custom palettes, or value-specific overrides.

## Table View

Table View is a detached or focused table for source events. It supports source
selection, synced or local filters, sorting, pagination, row selection, and note
creation. See [View](view.md#source-events-table).

## Dashboard

Dashboard is a detached analytics view with fixed dashboards and ad-hoc field
aggregations such as top values, top values by time, and rare values. See
[View](view.md#dashboard).

## Detached Window

A detached window is a separate browser window that runs a small React app while
receiving operation context from the main tab. Notes, Table View, Dashboard, and
event dialogs can be detached.

## Sigma Rule

A Sigma rule is a detection rule that can be uploaded and executed against
selected sources.
