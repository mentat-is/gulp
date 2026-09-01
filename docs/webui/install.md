# Web UI installation and development

gULP UI is a React and TypeScript frontend for the gULP platform. It connects
to a running gULP backend from the login page.

![Login page](images/login.png)

## Requirements

- Node.js `22.14.1`
- pnpm `10.8.1`
- A reachable gULP backend server

The published Python package already contains the compiled UI. Node.js and
pnpm are only needed to serve that bundle separately or to develop the UI.

## Run the UI from the Python package

Install and start the backend, then run the packaged UI launcher in another
terminal:

```bash
gulp
gulp-webui
```

Open [http://localhost:3000](http://localhost:3000). `gulp-webui` serves the
compiled bundle included under `gulp/webui`; it does not rebuild the UI. It
uses the pinned `serve` package through pnpm, which may download that package
on first use. Use `--host` and `--port` to change the default
`0.0.0.0:3000` listener.

The scalable EventStore pipeline attempts to persist streamed events in
IndexedDB on every origin, including a non-secure hostname such as
`http://gulp:3000`. The UI deliberately does not use
`navigator.storage.estimate()` or secure-context detection as a preflight gate:
IndexedDB remains usable in browsers that expose it even when quota telemetry is
not available.

If opening or writing IndexedDB fails, including a real quota-exceeded error,
the UI preserves the last coherent cache and shows an explicit compatibility
decision. It never writes a marker or reloads automatically. The user can retry
the scalable pipeline, or confirm the RAM compatibility pipeline; only that
confirmed choice writes and verifies `gulp:event-store-lod-enabled=false` before
reloading. Sources and events still load in compatibility mode, but large
operations can consume substantially more RAM and persistent query-cache
Undo/Redo is unavailable. HTTPS remains recommended for deployed environments
because it enables the full set of browser security and storage APIs, but it is
not required by this EventStore selection logic. Deleting site data also deletes
the persisted fallback marker.

## Install Dependencies

From a gULP repository checkout, install UI dependencies. These commands are
only for UI development:

```bash
pnpm --dir gulp-ui install
```

If pnpm blocks dependency build scripts, approve the required scripts:

```bash
pnpm --dir gulp-ui approve-builds
```

## Run for Development

Start the React development server:

```bash
pnpm --dir gulp-ui start
```

Open the printed local URL in a browser. On the login page, enter the backend
server address, username, and password.

## Build for Deployment

Create a production build:

```bash
pnpm --dir gulp-ui run build
```

Preview the generated `build/` folder locally:

```bash
pnpm --dir gulp-ui run server
```

The `server` script serves `build/` through the `serve` package. For production,
use a dedicated static server such as nginx; the supplied Docker UI image does
this.

## Current Scripts

- `pnpm start`: run the development server.
- `pnpm run build`: create a production build.
- `pnpm run server`: serve the production build from `build/`.
- `pnpm run check:locales`: validate locale fallback coverage.
