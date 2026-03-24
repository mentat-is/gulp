# gULP — Copilot instructions (for coding agents)

Purpose: give an AI coding agent the exact, actionable knowledge it needs to be immediately productive in this repository.

## Quick context (big picture) 🔧
- gULP is a Python FastAPI service + workers that stores documents in OpenSearch and keeps collaboration metadata in PostgreSQL; Redis is used for the task queue & pub/sub. (see `docs/architecture.md`).
- Main code lives in `src/gulp/` — important entry points: `src/gulp/__main__.py` (CLI), `src/gulp/api/server_api.py` (FastAPI server), `src/gulp/plugin.py` (plugin base).
- Separate repositories are symlinked for the client SDK (`gulp-sdk-python/`), non-free plugins (`gulp-paid-plugins`), and utility library (`muty-python/`).

## Testing & manual QA pointers

- for tests, a gulp instance should be available on localhost:8080. **if you cannot find the instance, you can start it with `gulp --reset-collab --create test_operation`, make sure to run the command inside the venv. any plugin extension startup errors can be ignored. be sure to stop the instance with `gulp --stop` when done.**
- for authentication, initially only the admin/admin user (admin permissions) and guest/guest (read-only, i.e. no ingestion) are available
- after authentication, the gulp protocol requires that a websocket connection be started to then be used for (almost) all APIs in the client usage session.
- for file ingestion tests, use the files in `/gulp/samples/win_evtx` with the "win_evtx" plugin
- to test the ingest raw API, use a json with different GulpDocuments. test both the ingest_raw API and the websocket version /ws_ingest_raw
- to test the query_external API, use the "query_elasticsearch" plugin using the local gulp instance (setting the necessary parameters in the plugin, i.e. configure it for opensearch): for example, ingest some test documents with win_evtx first, then run query_external with query_elasticsearch to retrieve them
- to test different plugin_params parameters, use the "csv" plugin, see as an example the test inside /gulp/tests/ingest/test_ingest.py in the test_csv_standalone, test_csv_file_mapping methods
- **in every test, always make sure to verify that (for example) data has been ingested (i.e. via query), and make sure that any data created on collab is deleted (for example, if you create an operation, make sure to delete it at the end of the test)**
- **make sure that any temporary files created during tests are deleted at the end of the test**
- **always make sure at the beginning of each test to start from a clean state, for example by creating new operations or new documents with unique IDs, so as not to have interference with pre-existing data or with other tests**
- **if you suspect bugs in the gulp backend, or if you notice strange behavior during tests, document them, stop and ask for clarifications**

### notes for LLM
- avoid copying code from the existing SDK in gulp-sdk-python, which is deprecated and no longer maintained
- ask questions if there are ambiguities in the requirements or if you need clarifications on how the SDK should work
- use memory to track progress and decisions made during implementation
- implement the SDK in successive phases (using memory to track state) so as not to lose context or have to repeat information already discussed

## Project-specific conventions & patterns 💡
- Plugin model: plugins implement `display_name`, `type`, `_record_to_gulp_document`. Ingestion plugins implement `ingest_file` / `ingest_raw`; external plugins implement `query_external`. See `src/gulp/plugins/win_evtx.py`, `json.py`, `elasticsearch.py` for canonical examples.
- Plugin filename (no extension) is the "internal plugin name" — used across APIs and mapping (see `GulpPluginEntry.filename` in `src/gulp/plugin.py`).
- Plugins are loaded first from `$GULP_WORKING_DIR/plugins` (override) then from installed `plugins/`.
- Stacked plugins: call `setup_stacked_plugin(lower_plugin)` or use `load_plugin()` as shown in `chrome_history_sqlite_stacked.py` and `stacked_example.py`.
- Worker-process model: ingestion/enrichment/external plugin entrypoints (usually) run in worker processes — avoid global mutable state and prefer `GulpServer.spawn_worker_task` / thread-pool for blocking IO.

## API & realtime patterns 🔁
- FastAPI + OpenAPI at `/docs` (server: `src/gulp/api/server_api.py`).
- Websockets used for live ingestion/query updates: `/ws`, `/ws_ingest_raw`, `/ws_client_data` (implementation in `src/gulp/api/ws_api.py`).
- Responses use the project’s JSend pattern — tests are the canonical spec (see `tests/`).

## Configuration & env vars (must-know) ⚙️
- Primary config template: `gulp_cfg_template.json` → runtime config at `~/.config/gulp/gulp_cfg.json` (or override with `GULP_WORKING_DIR`).
- Helpful env vars: `GULP_INTEGRATION_TEST`, `GULP_OPENSEARCH_URL`, `GULP_POSTGRES_URL`, `GULP_REDIS_URL`, `GULP_BIND_TO_ADDR`, `GULP_BIND_TO_PORT`.
- Worker/concurrency knobs: `parallel_processes_max`, `concurrency_*` in `gulp_cfg.json`.

## Adding / changing functionality — checklist for PRs ✍️
1. Update or add unit/integration tests under `tests/` (look at `tests/ingest/`, `tests/query/`, `tests/enrich/, `tests/extension/`).
2. If plugin or mapping changes, add/update mapping in `mapping_files/` and tests that use `GulpMappingParameters`.
3. Preserve worker-safety (no module-level mutable state used by workers).
4. If DB schema changes, add migration under `collab_migrate/`.
5. Ensure tests starts with a clean state (use pytest fixtures as i.e. in `tests/ingest/test_ingest.py:::_setup` or `--reset-collab --create test_operation` to start gulp in a clean state with a test operation).
6. Run tests and ensure they pass before submitting PR.

## Where to look first (high-value files) 📚
- runtime / CLI: `src/gulp/__main__.py`
- server boot & lifecycle: `src/gulp/api/server_api.py`
- websocket: `src/gulp/api/ws_api.py`
- plugin base & examples: `src/gulp/plugin.py`, `src/gulp/plugins/*` (see `win_evtx.py`, `json.py`, `elasticsearch.py`, `plugins/extension/example.py`)
- config defaults: `gulp_cfg_template.json` and `src/gulp/config.py`
- mapping & plugin examples: `docs/plugins_and_mapping.md` and `tests/ingest/test_ingest.py`
- client SDK used in tests: `gulp-sdk-python/`

## Examples of useful prompts for code changes 🔎
- "Add an ingestion plugin `foo` that parses X format — create `src/gulp/plugins/foo.py`, tests in `tests/ingest/`, and a mapping file under `mapping_files/`. Follow `win_evtx.py` and `json.py` patterns."
- "Find tests that rely on GULP_INTEGRATION_TEST and update them to use a temporary operation id."
