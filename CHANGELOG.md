# Unreleased

## Breaking Changes and Migrations

- Existing installations must run `collab_migrate/v2.0.0/migrate.py` to upgrade
  the collaboration database and must update their configuration with the new
  required fields documented in `src/gulp/gulp_cfg_template.json`.
- The application server is now HTTP-only and must run behind nginx or another
  TLS terminator for public deployments. PostgreSQL, OpenSearch, Redis, and S3
  encryption use their native secure URIs and trust settings.
- SeaweedFS 4.29 replaces MinIO while retaining the S3-compatible integration;
  the obsolete `s3_minio` and `s3_region` options have been removed.
- `muty-python` is now built into this repository under `src/muty/`; existing
  environments should remove the external package and rebuild their virtual
  environment.

## Added

- Added tag-and-commit release metadata shared by the Python package, GitHub
  release, and both Docker images, including support for version discovery from
  GitHub source archives.
- Added a single tag-only release pipeline that validates the tagged `master`
  commit, verifies the release artifacts, publishes both Docker images, updates
  `latest`, and creates the GitHub Release from this changelog.
- Added a Links tab to the detached Notes window with search, tags, pagination,
  cross-page selection, individual and bulk deletion, related-event navigation,
  realtime updates, and cancellable page loading.
- Added authenticated-session mapping discovery caching, metadata fallbacks,
  Navigator Text/Regex event search, shared source query scheduling, operation
  history management, and a five-generation per-source IndexedDB query cache.
- Added source-range-aware and post-ingest cache reuse, source-local Undo/Redo,
  cache freshness warnings, exact frame-local counts, and global timeline
  refetch/reset actions.
- Added point-in-time plus `search_after` pagination for deep Table View pages,
  matching SDK and CLI controls, horizontal column virtualization, bounded field
  discovery, searchable visibility controls, and hybrid dotted-field resolution.
- Added Event Detail field configuration shared by Raw, Table, Tree, Copy JSON,
  and Download JSON, while keeping configuration isolated from Table View.
- Added progressive rendering for uncached and large-source queries, bounded
  projection and boundary caches, visible-source preemption, frame telemetry,
  QueryWorker diagnostics, and a deterministic streaming replay validator.
- Added a `query-external` extension slot, live external-ingestion source
  selection, external-query completion fencing, and cumulative source progress
  for file, external, and realtime ingestion.
- Added negotiated WebSocket flow control with independent normalization and
  persistence stages, bounded raw backlog accounting, isolated overload
  cancellation, and RAM compatibility fallback.
- Added a bounded source-local timestamp cache for ingestion plugins, exact
  NDJSON adaptive bulk sizing, one overlapped OpenSearch bulk task, and a shared
  4 MiB incremental text reader in `muty.file`.
- Added single-pass timestamp normalization controls to `GulpDocument`, reusable
  empty mapping fallbacks, operation-aware S3 object cleanup, and note
  attachments across REST, WebSocket, SDK, and CLI interfaces.
- Added a unified `gulp.events` API for core and namespaced custom events with a
  main-process listener registry and Redis worker bridge.
- Added the new scheduler and task-management system, multi-user stress coverage,
  and documented Prometheus multi-instance scraping requirements.

## Fixed

- Fixed non-final OpenSearch document failures missing from request statistics
  and prevented acknowledged chunks from being submitted twice during exception
  finalization.
- Fixed ingestion bulk duplication and made full time ranges mutually exclusive
  from one-sided filters so out-of-range documents are skipped correctly.
- Kept backend ingestion authoritative when browser cache capture fails instead
  of sending cancellation requests.
- Preserved `event.sequence` as the descending tie-breaker across ingestion
  chunks, IndexedDB pages, cursors, projections, and neighboring-event reads.
- Preserved request-scoped Redis/WebSocket ordering so large `docs_chunk`
  payloads cannot be overtaken by `query_done`.
- Prevented projection starvation and post-ingest timeline stalls by coalescing
  viewport refreshes, fencing terminal projections, and reusing bounded
  revision-qualified worker caches.
- Fixed source reset, first/last-event navigation, progressive projection
  delivery, same-name cache warnings, websocket reconnection, safe unmounting,
  source deletion, and plugin configuration behavior.
- Improved error handling, documentation, code organization, configuration
  cleanup, and automated coverage throughout the project.

## Removed

- Removed publication of the `mentat-gulp` core package to PyPI; `gulp-sdk` and
  `gulp-cli` retain their independent versioning and publication processes.
- Removed the independent React version, generated UI version files, and UI
  build-time version variables. The UI must query the backend `/version` API if
  it needs to display the product version.
- Removed the separate UI changelog in favor of this repository-wide changelog.
- Removed redundant single- and multi-instance test wrappers.
- Removed the legacy plugin event manager, websocket-broker hooks, and duplicated
  synchronous/asynchronous implementations from `muty.file`.

# v1.7.601

- api cleanup (removed unused API)
- support for compressed zip in ingestion API
- devcontainer improvements for developers
- bugfixes

# v1.7.600

- some API improvements (ingest, better compatibility with recent gulp-cli improvements)
- revamped devcontainer
- reworked glyph support, removed unnecessary handling
- redis improvements
- better observability and metrics support

# v1.7.500

- fully code reviewed (gpt5.5/xhigh) and polished, with many improvements across the whole codebase, including better error handling, more robust and performant code, better documentation, and more tests.

# v1.7.403

- fixed some regressions (raw plugin, enrichment)

# v1.7.402

- docker fixes
- generic fixes and improvements

# v1.7.400

## Added

- Added the `shared_object` plugin for sharing arbitrary objects stored in the
  collaboration database; existing installations required the
  `/migrate/v1.7.400` migration available at the time of release.
- Added virtualized rendering for investigations with more than ten million
  events, worker-based file ingestion, viewport highlight culling, and improved
  graph alignment.
- Added cross-window synchronization, detachable Source and Notes tables,
  paginated row actions, bulk note deletion, and query-builder filtering.
- Added themes, context-based event coloring, dockable Event Detail, dynamic
  scale zoom, refined tree/table views, nested filters, global event search, and
  query history.

## Fixed

- Fixed selected-event crosshairs, frame query offsets and counts, in-progress
  frame dates, source labels, autosave scheduling, WebSocket reconnection, safe
  unmounting, active-source deletion, and plugin configuration reloads.
- Included the remaining minor stability and usability fixes from the release.

# v1.7.300

- task batching improvements
- postgresql pool handling improvements (fixes some issues with too many connections), read docs/troubleshooting.md for details.

# v1.7.201

## improvements

-plugins/raw: reworked to support mapping

- core: moved mapping code in a separate module to ease import by other tools
- all: minor fixes and improvements

# v1.7.15

- minor fixes

# v1.7.14

- `python3.14` support: existing (virtual) environments must be recreated to upgrade dependencies.
- generic bugfixes and improvements

# v1.7.10

## new features

### architecture

- architecture: observability via `Prometheus` metrics and `Grafana` dashboards (check the `docs/observability.md` for instructions)
- core: added `synchronous` internal events callback, allows extension plugins to i.e. change chunks of documents during ingestion via `EVENT_CHUNK_PRE_INGEST` event
- installation: removed submodules, welcome pypi installation! our Mentat's dependencies `gulp-sdk` and `muty-python` packages and also gulp itself (`mentat-gulp`) are now on pypi, check the [installation instructions](./docs/install_dev.md) for details.
- ci/cd: added github workflows to build pypi packages

### sdk and tools

- sdk: deprecated the old `gulp-sdk-python` and integrated new polished python SDK available at <https://github.com/mentat-is/gulp-sdk>, integration tests in `/tests` as usual (missing most of the old per-plugin tests, will be updated soon)
- cli: `gulp-cli` is now available to use most of the gulp features from the command line, get it at <https://github.com/mentat-is/gulp-cli> or via the `gulp-cli` package on pypi!

### api

added `enhance_document_map` API to allow the UI to "color" different documents based on its content, which will make it more flexible and powerful for different use cases.

### plugins

- plugins/pcap: massive improvements with 50+ protocols supported with metadata extraction, including:
  
  ```text
  Application: HTTP, DNS, TLS, DHCP, DHCPv6, BOOTP, TFTP, SNMP, NTP, RADIUS, SSH (banner + KEX/version/disconnect layers), MGCP, Skinny (SCCP)

  Auth/Enterprise: Kerberos, LDAP, DCE-RPC, SMB1, SMB2, NTLM, SPNEGO

  Routing/Transport: ICMP, ICMPv6, ARP, GRE, VXLAN, SCTP, IPsec, PPP, RTP, ISAKMP, RIP, VRRP, HSRP, L2TP, PPTP, MobileIP, NetFlow

  LAN/WiFi: 802.11 (Dot11), LLMNR, NetBIOS, EAP, LLTD, LLC/CLNS

  IoT/Embedded: Bluetooth HCI, BTLE, CAN/CAN-FD, USB, IEEE 802.15.4, ZigBee, 6LoWPAN, IrDA

  Other: PFLog, GPRS, line protocols (SMTP/FTP/SIP/POP3/IMAP)
  ```

## bugfixes

- core/logging: fixed syslog logging (integrated rsyslog both in the devcontainer and production Dockerfile deployment)
- core/redis: reworked message routing and handling
- all: multiple fixes

# v1.6.5

## new features

- architecture: filestore via `minio` S3-compatible storage to be used by plugins to store binary files needed for the analysis (configuration must be updated, check the `gulp_cfg_template.json`)
- architecture: removed `sftpd` from the default set of microservices (a management console may use the new API endpoints instead)
- plugins/pcap: reworked to use the filestore
- core/api: new API endpoints, `remove_enrich` to remove enriched data, endpoints to manage plugin/config/mapping files (`plugin_delete`, `plugin_upload`, `plugin_download`, `config_upload`, `config_download`, `mapping_file_delete_upload`), `mapping_file_download`, `mapping_file_delete`, endpoints to manage files from storage (`storage_delete_by_id`, `storage_get_file_by_id`, `storage_delete_by_tags`, `storage_list_files`)

## bugfixes

- **fixed a very long standing bug which leaked resources at every `ingest` operation and prevented clean shutdown of workers**.

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

- solves issues with the devcontainer (<https://github.com/yarnpkg/yarn/issues/9216>)
- some minor fixes

# v1.6.0

## major changes

- core: introducing redis instead of a shared multiprocessing queue to exchange messages core<->workers - (major speedup and less memory usage!)
- core: scaling horizontally using multiple instances of the core running simultaneously
- plugins: allow caching and reusing values through `DocValueCache` in `plugin.py` (major speedup when used properly)
- core: properly structured `GulpDocument`
- api/ws: introducing WebSocket API for real-time ingestion `/ingest_ws_raw` (allow i.e. real-time ingestion from network sensors, try <https://github.com/mentat-is/slurp-ebpf>)

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
