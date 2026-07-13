<div align="center">

<picture>
 <source media="(prefers-color-scheme: dark)" srcset="./logo.svg">
 <source media="(prefers-color-scheme: light)" srcset="./logo_dark.svg">
 <img alt="gULP" src="./logo.svg" width="40%" height="40%">
</picture>

the graphical, universal Log processor for incident response!

_made with :heart: by Mentat._

</div>

<div align="center">

![GitHub followers](https://img.shields.io/github/followers/mentat-is?style=social)
![GitHub stars](https://img.shields.io/github/stars/mentat-is/gulp?style=social)
![GitHub languages](https://img.shields.io/github/languages/top/mentat-is/gulp)
![GitHub issues](https://img.shields.io/github/issues/mentat-is/gulp)
![GitHub sponsors](https://img.shields.io/github/sponsors/mentat-is)

</div>

<div align="center">

[Description](#description) · [Quick start](#quick-start) · [Web UI](#web-ui) · [Documentation](#documentation) · [Examples](#backend-run-examples)

</div>

## description

gULP is a log-ingestion and incident-response platform. The FastAPI backend uses
OpenSearch, PostgreSQL, Redis, and optional MinIO storage; the included React
web UI provides the investigation workspace.

- **Ingestion plugins** for many log and artifact formats.
- **OpenSearch and ECS** for storage and a consistent event schema.
- **Multiprocessing** for ingestion and querying.
- **Sigma queries**, collaboration, notes, links, enrichment, and plugins.
- **AI assistant** for [automatic enrichment and analysis of events and documents](./plugins/extension/ai_assistant.py) (*).
- **Scalable deployments**; see the [scaling guide](docs/scaling.md).

(*) extra AI features, including an enhanced assistant and an automated `MCP server` to completely automatize incident response, are available in our paid tier.

## quick start

Requirements: Docker Compose and Git. This starts the backend services and
builds the included UI from this checkout:

```bash
git clone https://github.com/mentat-is/gulp.git && cd gulp && docker compose --profile gui up -d --build
```

Open [http://localhost:3000](http://localhost:3000), sign in with `admin/admin`, backend API reference is available at [http://localhost:8080/docs](http://localhost:8080/docs).

The default compose setup is an example for local evaluation and to be used as a base for more complex setups; see the [Docker guide](docs/install_docker.md) before production use.

To run the UI directly while developing it, install Node.js `22.14.1` and pnpm `10.8.1`, then run:

```bash
pnpm --dir gulpui-web install && pnpm --dir gulpui-web start
```

For a source backend install, use [development installation](docs/install_dev.md).

## web UI

The browser UI lives in [`gulpui-web/`](gulpui-web/README.md). Its focused
guides cover [installation](gulpui-web/docs/installation.md),
[ingestion](gulpui-web/docs/ingestion.md), [workflow](gulpui-web/docs/flow.md),
[features](gulpui-web/docs/features.md), and [views](gulpui-web/docs/view.md).

Start by connecting to a backend and choosing an investigation operation.

![gULP login](gulpui-web/docs/images/login.png)

The timeline groups sources by context, lets analysts zoom into activity, and
opens an event in place for investigation.

![gULP timeline](gulpui-web/docs/images/timeline.png)

Analysts can add collaboration notes directly to event details.

![Event-detail notes](gulpui-web/docs/images/event-detail-notes.png)

Dashboards summarize event rate and source distribution while the timeline stays
focused on the investigation.

![gULP dashboard](gulpui-web/docs/images/dashboard-fixed.png)

## documentation

- [Architecture](docs/architecture.md)
- [Docker installation and services](docs/install_docker.md)
- [Development installation](docs/install_dev.md)
- [Web UI documentation](gulpui-web/README.md)
- [Plugins and mappings](docs/plugins_and_mapping.md)
- [Integration API and raw ingestion](docs/integration.md)
- [Query behavior](docs/query.md)
- [Testing](docs/testing.md)
- [Observability](docs/observability.md)
- [Troubleshooting](docs/troubleshooting.md)

## backend configuration

The following environment variables override common runtime configuration:

- `GULP_BIND_TO_ADDR`, `GULP_BIND_TO_PORT`: set both to change the listen address (defaults: `0.0.0.0:8080`).
- `GULP_WORKING_DIR`: defaults to `~/.config/gulp` and contains the generated `gulp_cfg.json`, optional `plugins/` and `mapping_files/`, certificates, local ingestion files, and temporary uploads.
- `GULP_OPENSEARCH_URL`, `GULP_POSTGRES_URL`, `GULP_REDIS_URL`, `GULP_S3_URL`: override their corresponding configuration values.

## backend run examples

The `gulp` command creates the default configuration and initial `test_operation`
on its first run.

```bash
gulp
```

Reset all collaboration and OpenSearch operation data:

```bash
gulp --reset-collab
```

Reset all data and create an operation:

```bash
gulp --reset-collab --create my_operation
```

Recreate only one operation and its data:

```bash
gulp --create my_operation
```
