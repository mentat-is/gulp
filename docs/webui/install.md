# Web UI installation and development

gULP UI is a React and TypeScript frontend for the gULP platform. It connects
to a running gULP backend from the login page.

![Login page](images/login.png)

## Requirements

- Node.js `22.14.1`
- pnpm `10.8.1`
- A reachable gULP backend server

For a containerized UI, follow the [Docker installation guide](../install_docker.md).

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

The UI uses browser history routes such as `/operations/<operation_id>`. When
serving the production build, configure the web server to return `index.html`
for routes that do not match a file so that direct links and page refreshes
reach React Router. The supplied Docker image includes this nginx configuration;
requests for missing files under `/static/` still return 404. If an external
reverse proxy serves the build itself, apply the same fallback there. The
backend API is a separate service and should not be routed to the UI fallback.

## Current Scripts

- `pnpm start`: run the development server.
- `pnpm run build`: create a production build.
- `pnpm run server`: serve the production build from `build/`.
- `pnpm run check:locales`: validate locale fallback coverage.
