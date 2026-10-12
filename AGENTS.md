# gulp

gULP is a cybersecurity log-ingestion and incident-response platform. Its
FastAPI backend ingests and normalizes events, queries and enriches evidence,
manages plugins, stores collaboration data, and streams live updates over
websockets. Background workers execute asynchronous tasks.

Core services are OpenSearch for event storage, PostgreSQL for collaboration
metadata, Redis for queues and pub/sub, and SeaweedFS for S3-compatible
object storage.

## Repository map

- `src/gulp/api/server/` and `src/gulp/api/server_api.py`: FastAPI endpoints,
  application setup, and queue integration.
- `src/gulp/process.py` and `src/gulp/api/redis_api.py`: worker execution,
  task queues, and pub/sub.
- `src/gulp/api/collab/`: PostgreSQL collaboration models and data access.
- `src/gulp/api/opensearch/`: OpenSearch access.
- `src/gulp/api/s3_api.py`: S3-compatible storage.
- `src/gulp/api/ws_api.py` and `src/gulp/api/server/ws.py`: websocket APIs and
  routing.
- `src/gulp/plugin.py`, `src/gulp/plugins/`, and `src/gulp/mapping_files/`:
  plugin interfaces, implementations, and mappings.
- `src/gulp/config.py` and `src/gulp/structs.py`: configuration and shared data
  structures; API-specific structures live below their respective API package.
- `src/muty/`: built-in shared utilities.
- `collab_migrate/`, `docs/`, and `tests/`: database migrations,
  documentation, and test suites.
- `gulp-sdk/` and `gulp-cli/`: Python SDK and CLI maintained with affected
  backend interfaces.
- `gulp-ui/`: separate React and TypeScript development area that shares the
  root release version and root changelog with the backend.

## Working rules

- Read this file and follow existing code style and local patterns before
  editing. Prefer small, scoped changes over broad refactors.
- Use type hints for function signatures and concise docstrings for functions
  and classes. Comment non-obvious intent, preserve useful existing code
  comments, and update stale comments when behavior changes instead of deleting
  them.
- Do not add legacy compatibility unless the user explicitly asks for it.
- Preserve the agreed structure. If a deviation is necessary, explain why and
  record the change before proceeding.
- Stop and ask for feedback when requirements are unclear or when a test or
  investigation suggests a backend or plugin bug.
- Update the root `CHANGELOG.md` under `Unreleased` with a short summary when a
  goal is complete. Create the section with the standard categories if absent.

## Documentation and change coordination

- Every codebase change must update the relevant documentation. Put detailed
  architecture, development, testing, and operations documentation in `docs/`;
  keep `README.md` limited to the high-level overview and project capabilities.
- When backend behavior or interfaces change, update `gulp-sdk/` and
  `gulp-cli/` wherever either is affected. If the SDK changes, always check the
  CLI for corresponding work.
- Treat `gulp-ui/` as a separate development area, but coordinate any affected
  behavior and document all backend and UI changes in the single root changelog.
- Request retries are the client's responsibility. The backend should return
  appropriate status codes and retry metadata, such as `retry_after_msec` in a
  queue-backpressure `503`; do not add backend retry loops.
- Plans must be Markdown (`.md`) files. Keep their progress and landed details
  current through completion. Explain every necessary deviation and update the
  plan to match what actually landed.

## Versioning and releases

- Use `# Unreleased` as the first changelog section during development, with
  `Breaking Changes and Migrations`, `Added`, `Fixed`, and `Removed` headings.
- Before a release, a developer manually renames `Unreleased` to the final
  `vMAJOR.MINOR.PATCH` tag and commits that change on `develop` before the
  fast-forward merge to `master`.
- Never edit or commit `src/gulp/_version.py`; `setuptools-scm` generates it
  from the Git tag and commit during installation or build.
- The React application has no independent version. If it needs to display the
  product version, obtain it from the backend `/version` API.
- Only a version tag on the current `master` head may run the release workflow.
  Branch pushes, pull requests, and manual dispatches must not start a pipeline.
- Do not publish the gULP core to PyPI. `gulp-sdk` and `gulp-cli` maintain their
  own versions and publication workflows.

## Plugin reference implementations

Mirror the closest reference for each Python `GulpPluginType`:

- ingestion: `src/gulp/plugins/win_evtx.py`
- enrichment: `src/gulp/plugins/enrich_whois.py`
- external query: `src/gulp/plugins/query_elasticsearch.py`
- extension: `src/gulp/plugins/extension/ai_assistant.py`

For plugin changes, add or update integration coverage and, for isolated logic,
unit coverage when practical.

## Testing

- Validate every code or functionality change with integration tests; unit
  tests are only for isolated logic. Tests belong in `tests/integration/` or
  `tests/unit/`, must assert results, and must not be adapted to mask failures.
- Before integration tests, check or start the backend, then run the relevant
  integration test:

```bash
curl -fsS http://localhost:8080/docs >/dev/null || gulp --create test_operation
.venv/bin/python -m pytest -v -s -x tests/integration/<test_file>
```

- Stop only a backend you started, and revert temporary runtime configuration
  after testing:

```bash
gulp --stop
```

See [docs/testing.md](docs/testing.md) only for special, multi-instance,
configuration, or observability scenarios.
