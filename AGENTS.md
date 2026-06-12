# gulp

gULP is a log ingestion and analysis platform for cybersecurity, aiming to provide a collaborative environment for analysts.
It consists of a FastAPI service which exposes an API for document ingestion, querying, enrichment, and plugin management, and a worker system that processes tasks asynchronously.

The system uses OpenSearch for document storage, PostgreSQL for collaboration metadata, MinIO for object storage when needed, and Redis for task queuing and pub/sub.

## coding style

- Follow existing code style and patterns in the codebase.
- Use type hints for function signatures.
- Write clear and concise docstrings for functions and classes.
- When working on plugins, use existing plugins in `src/gulp/plugins` as references for structure and implementation:
  - for ingestion plugins, use `win_evtx.py` plugin
  - for enrichment plugins, use `enrich_whois.py` plugin
  - for external_query plugins, use `query_elasticsearch` plugin
  - for extension plugins, use `extension/ai_assistant.py` plugin
- For any new functionality, ensure that it is well-tested with unit and integration tests.

## relevant part of the backend codebase

- `collab_migrate/`: directory containing database migration scripts for collaboration database (PostgreSQL)
- `docs`: documentation (architecture, plugin development, etc...)
- `tests`: directory containing unit and integration tests for the gULP service and plugins.
- `src/gulp/plugins`: the plugins (`src/gulp/plugins/extensions` for extensions, `src/gulp/pluginsì` for any other type of plugins.
- `src/gulp/mapping_files/`: directory containing mapping files for different plugins
- `src/gulp/plugin.py`: base class for plugins, which defines the interface and common functionality for all plugins in the system.
- `src/gulp/config.py`: configuration management for the gULP service, including settings for database connections, OpenSearch, Redis, MinIO, etc...
- `src/gulp/process.py`: worker processes (using `aiomultiprocesses`) that execute tasks from the queue, including document ingestion, enrichment, querying, etc...
- `src/gulp/api/collab`: collaboration objects and database models for managing operations, documents, and other collaborative entities in PostgreSQL (notes, links, ...)
- `src/gulp/api/opensearch`: functions for interacting with OpenSearch, including document indexing, querying, index management, etc...
- `src/gulp/api/server`: the API endpoints for the gULP FastAPI server, which handle incoming requests (`src/gulp/api/server/ingest.py`, `src/gulp/api/server/query.py` ), interact with the collaboration database and OpenSearch, and manage plugin execution.
- `src/gulp/api/server_api.py`: setup FastAPI server, initialization, task queue/dequeuing
- `src/gulp/api/s3_api.py`: functions for interacting with S3-compatible storage, used for file uploads and plugin file management
- `src/gulp/api/redis_api.py`: functions for interacting with Redis, including task queue management and pub/sub for real-time updates to clients.
- `src/gulp/api/ws_api.py`, `src/gulp/api/server/ws.py`: websocket API and management for real-time communication between clients and the server, used for streaming results, updates, etc...
- `src/gulp/structs.py`, `src/gulp/api/collab/structs.py`, `src/gulp/api/opensearch/structs.py`, `src/gulp/api/server/structs.py`: data structures and models used throughout the codebase, including Pydantic models for API requests/responses, database models for collaboration entities, and data structures for OpenSearch interactions.

### symlinked repositories

- `gulp-sdk/`: client SDK for interacting with the gULP API
- `gulp-paid-plugins/`: repository for non-free plugins
- `muty-python/`: utility library used by gULP and plugins
- `gulpui-web/`: the web client for gULP, built with React and TypeScript, which interacts with the gULP API to provide a user interface for document ingestion, querying, enrichment, and collaboration features.

## working with gulpui-web

use pnpm start to start the development server for the web client on `http://localhost:3000`, you can interact with the gulp API on `http://localhost:8080` (make sure to start the gulp backend if you haven't already, see testing instructions below).

## testing instructions

- tests are (and should be created in) in `/gulp/tests/integration` (integration tests) and `/gulp/tests/unit` (unit tests)

- first, always check if there is a a gulp instance available on `http://localhost:8080` by checking `http://localhost:8080/docs`. either, you can start one with `gulp --reset-collab --create test_operation`: make sure to run the command inside the venv, be sure to stop the instance with `gulp --stop` when done ONLY IF YOU STARTED IT.
- for ingestion tests, you have examples in `tests/integration/test_ingest.py`
- for enrichment tests, you have examples in `tests/integration/test_enrich.py`
- for query tests, you have examples in `tests/integration/test_queries.py`
- the `raw` plugin is tested in `tests/integration/test_raw_plugin.py`
- for any other tests, look for examples in the `tests/integration` directory, and if you cannot find any, ask for clarifications before proceeding
- in the tests, always print the response from the API on stdout to make sure you understand what is being returned, and to help with debugging if something goes wrong
