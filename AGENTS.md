# gulp

This repository contains gULP, a cybersecurity log ingestion and analysis platform.
The backend exposes a FastAPI API for ingestion, querying, enrichment, plugin management,
and websocket-driven collaboration features. Background workers process asynchronous tasks.

Core services used by the backend:

- OpenSearch for document storage
- PostgreSQL for collaboration metadata
- Redis for task queuing and pub/sub
- MinIO for object storage when needed

## How To Work In This Repo

- Read this file before editing.
- Follow existing code style and local patterns before introducing anything new.
- Prefer small, scoped changes over broad refactors.
- Use type hints for function signatures.
- Write clear, concise docstrings for functions and classes.
- When adding or changing functionality, update tests and docs when relevant.
- If you are following a plan file, keep it updated with the implementation that actually landed.
- If requirements are unclear, stop and ask before implementing.

## Key Backend Paths

- `collab_migrate/`: PostgreSQL migration scripts for collaboration data
- `docs/`: architecture, testing, operations, and plugin documentation
- `tests/`: unit and integration tests for the backend and plugins
- `src/gulp/plugins/`: plugin implementations
  - `src/gulp/plugins/extensions/`: extension plugins
- `src/gulp/mapping_files/`: mapping files used by plugins
- `src/gulp/plugin.py`: base plugin interface and shared behavior
- `src/gulp/config.py`: backend configuration management
- `src/gulp/process.py`: worker task execution with multiprocessing
- `src/gulp/api/collab/`: collaboration models and database logic
- `src/gulp/api/opensearch/`: OpenSearch access layer
- `src/gulp/api/server/`: FastAPI endpoints
- `src/gulp/api/server_api.py`: FastAPI app setup and queue integration
- `src/gulp/api/s3_api.py`: S3-compatible storage integration
- `src/gulp/api/redis_api.py`: Redis queueing and pub/sub helpers
- `src/gulp/api/ws_api.py`, `src/gulp/api/server/ws.py`: websocket APIs and routing
- `src/gulp/structs.py`, `src/gulp/api/collab/structs.py`, `src/gulp/api/opensearch/structs.py`, `src/gulp/api/server/structs.py`: shared data structures and models

## Related Repositories In This Workspace

- `gulp-sdk/`: Python SDK for the gULP API
- `gulp-cli/`: CLI built on top of the SDK
- `gulp-paid-plugins/`: non-free plugins
- `muty-python/`: shared utility library
- `gulpui-web/`: React + TypeScript web client

## Backend conventions

- Retrying requests is up to the client by design, but the backend will return appropriate status codes and headers to indicate when to retry a request ("retry_after_msec" in 503 responses for queue backpressure, for example).

## Change Coordination Rules

- If you change backend API behavior, check whether `gulp-sdk/` must be updated.
- If you update `gulp-sdk/`, also check whether `gulp-cli/` must change to stay aligned.
- Reuse existing examples in the repo instead of inventing new file structure or test style.
- For plugin work, prefer the documented reference plugins below.
- Commit only after the relevant step is implemented and verified.

## Plugin Development References

When working on plugins, mirror the closest existing plugin:

- ingestion plugins: `src/gulp/plugins/win_evtx.py`
- enrichment plugins: `src/gulp/plugins/enrich_whois.py`
- external query plugins: `src/gulp/plugins/query_elasticsearch.py`
- extension plugins: `src/gulp/plugins/extensions/ai_assistant.py`

For plugin changes, add or update both unit and integration coverage when practical.

## UI Integration

To run the web client against the local backend:

- backend: `http://localhost:8080`
- UI: `http://localhost:3000`
- start the UI from `gulpui-web/` with `pnpm start`

Start the backend first if it is not already running.

## Testing Rules

Before running backend tests, first check whether a backend is already available:

```bash
curl -fsS http://localhost:8080/docs >/dev/null && echo up || echo down
```

If no backend is running, start one:

```bash
gulp --reset-collab --create test_operation
```

When you started a backend instance for testing, stop it when finished:

```bash
gulp --stop
```

If you need multiple backend instances, bind extra instances to ports `>= 8100`:

```bash
GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8100 gulp
```

Default expectations:

- an already running primary instance usually lives on `:8080`
- secondary instances should use ports `>= 8100`

### Test Locations

- integration tests: `tests/integration/`
- unit tests: `tests/unit/`

Use existing tests as references:

- ingestion: `tests/integration/test_ingest.py`
- enrichment: `tests/integration/test_enrich.py`
- query: `tests/integration/test_queries.py`
- raw plugin: `tests/integration/test_raw_plugin.py`

If no close example exists for the kind of test you need to add, ask before proceeding.

### Test Authoring Expectations

- Tests must assert expected results.
- Tests should also print important API responses to stdout to help debug failures.
- Keep tests consistent with the surrounding suite's style and fixtures.
- Prefer integration coverage for API behavior and plugins and unit coverage for mere logic.
- Tests for specific functionality and plugins must be each in their own separate files, not lumped together in a single file.

### Special Testing Cases

- If you need to tweak runtime configuration, edit `~/.config/gulp/gulp_cfg.json`, restart gULP, and revert the config changes after testing.
- If you need observability, enable Prometheus as described in [docs/observability.md](docs/observability.md), and stop any extra tooling you started when done.
- See [docs/testing.md](docs/testing.md) for broader test commands and helper scripts.
