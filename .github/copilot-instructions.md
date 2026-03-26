<!-- vscode-markdown-toc -->
* 1. [Quick context (big picture) 🔧](#Quickcontextbigpicture)
* 2. [General instructions for coding agents 🤖](#Generalinstructionsforcodingagents)
* 3. [Testing instructions](#Testinginstructions)
* 4. [Adding / changing functionality — checklist for PRs ✍️](#AddingchangingfunctionalitychecklistforPRs)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->
# gULP — Copilot instructions (for coding agents)

Purpose: give an AI coding agent the exact, actionable knowledge it needs to be immediately productive in this repository.


##  1. <a name='Quickcontextbigpicture'></a>Quick context (big picture) 🔧
- gULP is a Python FastAPI service + workers that stores documents in OpenSearch and keeps collaboration metadata in PostgreSQL; Redis is used for the task queue & pub/sub. (see `docs/architecture.md`).
- Main code lives in `src/gulp/` — important entry points: `src/gulp/__main__.py` (CLI), `src/gulp/api/server_api.py` (FastAPI server), `src/gulp/plugin.py` (plugin base).
- Separate repositories are symlinked for the client SDK (`gulp-sdk/`), non-free plugins (`gulp-paid-plugins`), and utility library (`muty-python/`).

##  2. <a name='Generalinstructionsforcodingagents'></a>General instructions for coding agents 🤖
- **ALWAYS** use memory to track progress and decisions made during implementation

##  3. <a name='Testinginstructions'></a>Testing instructions
- for tests, a gulp instance should be available on `localhost:8080`. 
  - **if you cannot find the instance, you can start it with `gulp --reset-collab --create test_operation`, make sure to run the command inside the venv. any plugin extension startup errors can be ignored. be sure to stop the instance with `gulp --stop` when done.**
- for authentication, initially only the `admin/admin` user (admin permissions) and `guest/guest` (read-only, i.e. no ingestion) are available
- **after authentication, (most of) the gulp API requires that a websocket connection is started.**
- for file ingestion tests, use the files in `/gulp/samples/win_evtx` with the `win_evtx` plugin
- to test the ingest raw API, use a json with different `GulpDocument`s. test both the `/ingest_raw` API and the websocket version `/ws_ingest_raw`
- to test the query_external API, use the `query_elasticsearch` plugin using the local gulp instance (setting the necessary parameters in the plugin, i.e. configure it for opensearch): for example, ingest some test documents with win_evtx first, then call `/query_external` with `query_elasticsearch` plugin to retrieve them
- to test different `plugin_params` parameters during ingestion, use the `csv` plugin, see as an example the test inside `/gulp/tests_old/ingest/test_ingest.py in the `test_csv_standalone`, `test_csv_file_mapping` methods
- **in every test, always make sure to verify that (for example) data has been ingested (i.e. via query), and make sure that any data created on collab is deleted (for example, if you create an operation, make sure to delete it at the end of the test)**
- **make sure that any temporary files created during tests are deleted at the end of the test**
- **always make sure at the beginning of each test to start from a clean state, for example by creating new operations or new documents with unique IDs, so as not to have interference with pre-existing data or with other tests**
- **if you suspect bugs in the gulp backend, or if you notice strange behavior during tests, document them, stop and ask for clarifications**

##  4. <a name='AddingchangingfunctionalitychecklistforPRs'></a>Adding / changing functionality — checklist for PRs ✍️
1. Update or add unit/integration tests under `tests/` (look at `tests/ingest/`, `tests/query/`, `tests/enrich/, `tests/extension/`).
2. If plugin or mapping changes, add/update mapping in `mapping_files/` and tests that use `GulpMappingParameters`.
3. If DB schema changes, add migration under `collab_migrate/`.
4. Ensure tests starts with a clean state (use pytest fixtures as i.e. in `tests/ingest/test_ingest.py:::_setup` or `--reset-collab --create test_operation` to start gulp in a clean state with a test operation).
5. Run tests and ensure they pass before submitting PR.

