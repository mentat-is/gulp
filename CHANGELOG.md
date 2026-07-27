# v2.0.0

- refactored the repository agent guidance to clarify architecture, workflow, documentation, compatibility, plugin, and integration-testing requirements,optimized repository testing guidance for the standard integration-test workflow and documented virtual-environment test commands.
- made the gULP application server HTTP-only for private deployment behind nginx or another TLS terminator; outbound PostgreSQL, OpenSearch, Redis, and S3 encryption now uses native secure URIs and trust settings.
- replaced MinIO with SeaweedFS 4.29 while retaining the existing aiobotocore integration, object tags, multipart transfers, and S3 ports; removed the obsolete `s3_minio` and `s3_region` options.
- made operation deletion remove all S3-compatible objects tagged with its operation ID when `delete_data` is enabled.
- added note attachments with PostgreSQL metadata, S3-compatible binaries, REST, websocket updates, SDK, and CLI support.
- removed redundant single/multi-instance test wrappers in favor of the consolidated multi-user scenario.
- replaced the legacy plugin event manager and websocket-broker hooks with the unified `gulp.events` API. Core and namespaced custom events now share one main-process listener registry and a Redis worker bridge; pre/post-ingest events are skipped entirely for sources without matching listeners.
- integrated the `ui` into the main gulp repo, no more separated repository: now the web UI is built and served by the gulp backend. this allows a very easy installation for basic usage: just install gulp from pypi and you're done!
- completely new scheduler and task management system, with better observability and metrics, more robust and performant code: now supports up to 10 users (in a single instance) in a very irrealistic stress test with thousands of concurrent ingestion and queries, millions of generated notes, and hundreds of concurrent tasks (see `test_scripts/sigma_zip_big_harness.py` for details), this means that the realistic deployment will support many more users on a single instance, and more instances can be added to scale horizontally (as before, see the [scaling guide](./docs/scaling.md) for details)
- moved `muty-python` inside the gulp repo, now it is a built-in shared utility library (see `src/muty/`), and the old `muty-python` repo is deprecated (you may remove it from your local clone or uninstall it if you installed it via pip, **then rebuilding the .venv is advised**)
- removed duplicated sync/async implementations from `muty.file`
- many bugfixes and improvements across the whole codebase, including better error handling, better documentation, code and configuration cleanup and more tests.

> IMPORTANT: existing users must run the migration script in `/migrate/v2.0.0` to upgrade the collab database to the new schema, and also update the configuration file with the new required fields (see `gulp_cfg_template.json` for details).

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

- plugins: added shared_object plugin, to allow users to share arbitrary objects stored on the collab db (needs migrate script in /migrate/v1.7.400)
- all: usual minor improvements and bugfixes

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
