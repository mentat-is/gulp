# Web UI development

gULP UI is a React and TypeScript frontend for the gULP platform. It connects
to a running gULP backend from the login page.

![Login page](images/login.png)

## Requirements

- Node.js `22.14.1`
- pnpm `10.8.1`
- A reachable gULP backend server

## Install Dependencies

From a gULP repository checkout, install UI dependencies. For a PyPI install,
the UI is already bundled and served by `gulp`; these commands are only for UI
development:

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

Serve the generated `build/` folder with the bundled static server:

```bash
pnpm --dir gulp-ui run server
```

The `server` script serves `build/`. You can also deploy the
contents of `build/` with any static web server.

## Current Scripts

- `pnpm start`: run the development server.
- `pnpm run build`: create a production build.
- `pnpm run server`: serve the production build from `build/`.
- `pnpm run check:locales`: validate locale fallback coverage.
