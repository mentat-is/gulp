# v1.6.5

## new features

- architecture: filestore via `minio` S3-compatible storage, which can be used by plugins to store files (e.g. PCAPs, extracted files, ...)
- architecture: removed `sftpd` from the default set of microservices (a management console may use the new API endpoints instead)
- core/api: new API endpoints, `remove_enrich` to remove enriched data, endpoints to manage plugin/config/mapping files (`plugin_delete`, `plugin_upload`, `plugin_download`, `config_upload`, `config_download`, `mapping_file_delete_upload`), `mapping_file_download`, `mapping_file_delete`, endpoints to manage files from storage (`storage_delete_by_id`, `storage_get_file_by_id`, `storage_delete_by_tags`, `storage_list_files`)

## bugfixes

- fixed a very long standing bug which leaked resources at every `ingest` operation and prevented clean shutdown of workers.

# v1.6.2

## new features

- realtime ingestion supported in the UI
- new plugin: `otel_receiver` to ingest OpenTelemetry traces, logs and metrics from an OpenTelemetry Collector

## improvements

- core/query: major boost in parallel query handling and overall performance improvements (Redis)
- core/collab: refactored advisory locks to be more robust and performant (PostgreSQL)
- core/mapping: added `mapping.fields.timestamp_format` and `mapping.default_encoding` to the mapping engine, to respectively use a default timestamp format string and string encoding

## unresolved issues

`timestamp_format` in `plugin_params` is currently **NOT SUPPORTED** in the UI: in the `regex` plugin it is workarounded passing it via `plugin_params.custom_parameters`, other plugins using it (i.e. `apache_access_clf`) have hardcoded defaults (which is, of course, not ideal and will be fixed ASAP when the UI issue is resolved).

# v1.6.1

## fixes

- solves issues with the devcontainer (https://github.com/yarnpkg/yarn/issues/9216)
- some minor fixes

# v1.6.0

## major changes

- core: introducing redis instead of a shared multiprocessing queue to exchange messages core<->workers - (major speedup and less memory usage!)
- core: scaling horizontally using multiple instances of the core running simultaneously
- plugins: allow caching and reusing values through `DocValueCache` in `plugin.py` (major speedup when used properly)
- core: properly structured `GulpDocument`
- api/ws: introducing WebSocket API for real-time ingestion `/ingest_ws_raw` (allow i.e. real-time ingestion from network sensors, try https://github.com/mentat-is/slurp-ebpf)

## changes/improvements

- all: our internal repos `muty-python` and `gulp-sdk-python` now included as submodules
- core/collab: upgraded to OpenSearch latest (3.x)
- core/collab: reworked most of the collab code to be more SQLAlchemy compliant
- core/collab: stats (GulpRequestStats) processing completely reworked (now they are updated consistently across the whole modules)
- core/mapping: allowing aliasies to be applied post-mapping (`value_aliases` in the mapping files/definitions)
- core/mapping: support for windows filetime for `timestamp` fields
- core/api: added `query_aggregation` to the API to allow aggregation queries
core/ws: better backpressure handling for higher loads

## plugins

- plugins/extension: `ai-assistant` to help analsyts with investigations using LLMs (OpenRouter API support)
- plugins/ingestion: `suricata`, `memprocfs`, `zeek` ingestion plugins/mappings added

## all

- all: generic fixes and improvements across the whole codebase
