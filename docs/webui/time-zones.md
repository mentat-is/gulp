# Timeline time zones

The Web UI uses each event's `@timestamp` as the authoritative event instant
for Canvas placement and display. The timeline ruler, source boundary dates,
and local pointer label all project that same instant into one selected IANA
time zone.

## Selecting a time zone

1. Open **Settings** from an operation view.
2. Open **Time zone**.
3. Select `UTC` or another IANA identifier, such as `Europe/Rome` or
   `America/New_York`.

UTC is the default. The preference is saved in browser local storage and takes
effect immediately in both Canvas renderer modes and the local pointer overlay.
Other windows update through the browser storage event.

The local pointer timestamp is recalculated when the timeline moves beneath a
stationary pointer, including event focus, panning, zooming, frame changes, and
Canvas resizing. Its label therefore remains aligned with the ruler even when
no new mouse-move event occurs. Pointer movement uses the render worker's active
viewport width as well, so docking or resizing the event panel cannot make the
pointer and worker-rendered ruler use different timeline transforms.

Changing the time zone changes only the displayed calendar date and time. It
does not change the underlying `@timestamp`, the event's Canvas position, or a
source's configured timeline offset.

The available choices come from the browser's supported IANA time-zone data.
If a stored value is no longer supported, the UI resets the preference to UTC.
