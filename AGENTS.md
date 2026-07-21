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
- `gulp-ui/`: separate React and TypeScript project; do not include it in
  backend change coordination.

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
- Update `CHANGELOG.md` with a short summary when a goal is complete.

## Documentation and change coordination

- Every codebase change must update the relevant documentation. Put detailed
  architecture, development, testing, and operations documentation in `docs/`;
  keep `README.md` limited to the high-level overview and project capabilities.
- When backend behavior or interfaces change, update `gulp-sdk/` and
  `gulp-cli/` wherever either is affected. If the SDK changes, always check the
  CLI for corresponding work.
- Treat `gulp-ui/` as a separate project. Do not change it as part of backend
  coordination; document any required UI work for its maintainers.
- Request retries are the client's responsibility. The backend should return
  appropriate status codes and retry metadata, such as `retry_after_msec` in a
  queue-backpressure `503`; do not add backend retry loops.
- Plans must be Markdown (`.md`) files. Keep their progress and landed details
  current through completion. Explain every necessary deviation and update the
  plan to match what actually landed.

## Plugin reference implementations

Mirror the closest reference for each Python `GulpPluginType`:

- ingestion: `src/gulp/plugins/win_evtx.py`
- enrichment: `src/gulp/plugins/enrich_whois.py`
- external query: `src/gulp/plugins/query_elasticsearch.py`
- extension: `src/gulp/plugins/extension/ai_assistant.py`

For plugin changes, add or update integration coverage and, for isolated logic,
unit coverage when practical.

## Testing

- Validate every code or functionality change with integration tests. Use unit
  tests only for isolated logic; they do not replace integration coverage.
- Integration tests live in `tests/integration/`; unit tests live in
  `tests/unit/`. Use the existing ingestion, enrichment, query, and raw-plugin
  tests as references.
- Tests must assert expected results and print important API responses for
  failure diagnosis. Keep each specific feature or plugin in its own test file.
- Never adapt a test to unexpected results. Let it fail, investigate, and stop
  for guidance if the failure suggests a product bug.
- If no close test example exists, ask before creating a new test structure.
- If you need to tweak runtime configuration, edit `~/.config/gulp/gulp_cfg.json`, restart gULP, and revert the config changes after testing.
- If you need observability, enable Prometheus as described in [docs/observability.md](docs/observability.md)

Before integration tests, check whether the backend is already available:

```bash
curl -fsS http://localhost:8080/docs >/dev/null && echo up || echo down
```

If it is down, start it with a clean test operation:

```bash
gulp --reset-collab --create test_operation
```

Use ports `>= 8100` for additional instances:

```bash
GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8100 gulp
```

Stop any backend instance you started when testing finishes:

```bash
gulp --stop
```

See `docs/testing.md` for test commands and `docs/observability.md` for optional
diagnostics. Revert temporary runtime configuration changes and stop any extra
tooling when finished.
