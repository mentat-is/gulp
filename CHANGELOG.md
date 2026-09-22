# v2.0.0

- cached successful Web UI ingestion mapping discovery for the authenticated
  session, stopped persisting pending empty source-field catalogs, and preserved
  all structured query operators when field type metadata is unavailable.
- made Navigator event search the default, with an icon-driven Text/Regex
  selector embedded in the shared Input component; its reusable API also
  supports multi-selection through a `ListChecks` trigger, while searches apply
  to `event.original` across every selected source and remain editable in the
  per-source filter banner.
- added `gulp.glug` (Gulp Log Unified Gateway), a new structured JSON logging
  package coexisting with `MutyLogger` pending call-site migration: singleton
  per-process rotating file output (no stdout/syslog/colors), `dev_mode`-aware
  levels, `contextvars`-based request/operation/user/plugin/client_ip
  enrichment merged into every record, and a Redis pub/sub channel for live
  main+worker level/`dev_mode` reconfiguration with persisted-state catch-up
  for processes started after a change; see `plans/log-revolution.md`. Wired
  the new `--dev-mode`/`--log-dir` CLI flags through `GulpServer`/`GulpProcess`
  so the main process and every worker bootstrap their own `Glug` instance
  and reconfig subscriber on startup and stop them on shutdown; glug stays
  fully disabled (no behavior change) when `--log-dir` is omitted. Fixed a
  logger-name collision found by end-to-end testing this wiring: `Glug` was
  reusing the same logger names as `MutyLogger` (`"gulp"`, `"gulp-worker-N"`),
  and since `logging.getLogger()` is a process-wide singleton keyed by name,
  glug's setup was wiping out MutyLogger's handlers on the same underlying
  logger; glug's main/worker loggers are now named `"glug"`/`"glug-worker-N"`.
  Gave `Glug` a disabled/no-op mode (`get_instance()` with no `log_dir` and no
  prior instance) so the 934 remaining `MutyLogger` call sites can migrate to
  a bare `Glug.get_instance()` mechanically, without extra guards, staying
  silent when glug isn't bootstrapped. Migrated `gulp.events` (12 call sites)
  as the first Fase 4 module, reviewing each one's log level for verbosity in
  the process (none needed changing). Migrated all of `src/gulp/api/collab/`
  (12 files, 109 call sites) as the second Fase 4 module: mechanical rename
  plus `%s`-style to f-string conversion, and converted bare
  `.error(exception)` calls inside `except` blocks to `.exception(...)` so
  the traceback is captured (previously silently dropped under `MutyLogger`).
  No log level changes were needed. Integration testing against this module
  surfaced a pre-existing, unrelated bug: `gulp_sdk.api.plugins.object_delete_bulk`
  never forwards `ws_id` to the backend, so `object_delete_bulk_handler` always
  receives `ws_id=None` and the documented bulk-delete websocket notification
  is never sent; reproduced identically on the pre-migration baseline,
  tracked separately from log-revolution.
- revised `gulp.glug`'s output design: it now always logs a human-readable,
  colorized (only when `sys.stdout.isatty()`) console line to stdout instead
  of staying disabled — interactive use and Docker/systemd all already
  capture stdout themselves. `--log-dir`, when passed, adds the structured
  JSON per-process file *on top of* stdout, never instead of it (mirrors
  `MutyLogger`'s own always-stdout-plus-optional-file/syslog behavior; also
  keeps stdout available once `--log-dir` becomes mandatory for the future
  audit plugin, see decision 12). Same `LogRecord`, two formatters:
  `GlugJsonFormatter` (file) and the new `GlugConsoleFormatter` (stdout) —
  timestamp, level, `module:line`, message, then a compact `{key=value}`
  context suffix when non-empty. Added an optional `hint: str | None` kwarg
  to `.warning/.error/.critical/.exception` for a short remedy line under
  the message, surfaced in both the console line and the JSON record's new
  `"hint"` field; opt-in per call site, not retrofitted everywhere.
  `server_api.py`/`process.py` now bootstrap `Glug` unconditionally (main
  and every worker) instead of only when `--log-dir` was passed, so both
  stdout output and the Redis reconfig subscriber always work. This retires
  the earlier disabled/no-op `Glug` mode entirely: a bare
  `Glug.get_instance()` now always logs somewhere by default. See
  `plans/log-revolution.md`, decision 6, for the full rationale and a
  before/after comparison against `MutyLogger`'s output.
- migrated all of `src/gulp/api/opensearch/` (3 files, 29 call sites) as the
  third Fase 4 module: mechanical rename plus `%s`-style to f-string
  conversion, and `.error(e)`/`.error(msg, ..., e)` inside real `except`
  blocks converted to `.exception(...)` (same rationale as the previous
  module). Two content/level reviews: `GulpDocument.normalize_timestamp`
  (runs per ingested document) downgraded `.error()` to `.warning()` — a
  malformed timestamp is handled input, already counted in the request's
  `records_failed` stat, not a system fault, and an ERROR-per-record on a
  malformed source could flood the log; and `sigmas_to_queries_wrapper`'s
  "using all sources" message no longer echoes the (at that point always
  empty) `src_ids` parameter, and its `count_only` message moved from
  `WARNING` to `INFO` (a normal request mode, not a degraded condition).
  Integration testing surfaced a pre-existing, unrelated failure:
  `test_query_raw_paginate_supports_deep_offset_and_stable_pit_jumps`
  returns `total_hits=1000` instead of `10025` after a 10025-document bulk
  ingestion; reproduced identically on the pre-migration baseline, and
  consistent with this devcontainer's disk usage sitting above
  OpenSearch's default 90% high watermark — tracked separately from
  log-revolution.
- migrated `src/gulp/api/s3_api.py` (22 call sites) as the fourth Fase 4
  module, then added a `detail=None` kwarg to every `Glug` logging method
  (`.debug/.info/.warning/.error/.critical/.exception`) and applied it
  retroactively across every module migrated so far: the message stays a
  short summary of what happened, and the full structure (previously
  glued into the message as a `muty.string.make_shorter`-truncated
  `str(dict)`/`str(object)`, a pattern inherited unchanged from
  `MutyLogger`) moves to `detail`, which `Glug._emit()` drops entirely
  outside `dev_mode` before any serialization, and which `dev_mode`
  renders as a pretty-printed multi-line structure on the console
  (`GlugConsoleFormatter`) and as-is in the JSON file
  (`GlugJsonFormatter`, via the same `orjson.dumps(..., default=str)`
  already used for the rest of the record). Retrofitted
  `api/collab/structs.py`, `api/collab/context.py`, `api/collab/stats.py`
  (the `**FINISHED**` messages, which were dumping the whole
  `GulpRequestStats` ORM object via its dataclass-generated `__repr__`),
  and `api/opensearch/sigma.py`; in `api/s3_api.py` most of the dumped
  status dicts were already redundant with the message text and were
  simply dropped rather than routed through `detail`. See
  `plans/log-revolution.md`, decision 16.
- migrated `src/gulp/api/collab_api.py` (21 call sites) as the fifth Fase 4
  module, and used it as the first real application of `hint`: a startup
  failure to reach PostgreSQL (`_check_all_tables_exist`, the first point
  that actually opens a connection — engine creation itself is lazy) now
  logs `Glug.get_instance().exception(..., hint="verify PostgreSQL is
  running and reachable at the configured postgres_url")` before
  re-raising, instead of leaving the process to crash with only a raw
  SQLAlchemy/uvicorn traceback and no application-level log line at all.
  Also fixed a pre-existing credential leak found while migrating: 6 call
  sites logged `postgres_url` (which embeds the DB username/password in
  plaintext) as-is; one of them could even surface the plaintext password
  in an `ObjectNotFound` HTTP error body. All 6 now use SQLAlchemy's
  `make_url(url).render_as_string(hide_password=True)`. Also applied
  decision 16 here: `create_default_users` used to unconditionally dump
  the full admin user (including `pwd_hash`, the password hash) via
  `orjson.dumps(..., OPT_INDENT_2)` regardless of `dev_mode`; it now logs
  one summary line with the created group/user ids and moves the full
  structure to `detail` (dev_mode-gated) with `pwd_hash` excluded.
- migrated all of `src/gulp/api/server/` (10 files, 148 call sites) as the
  sixth Fase 4 module, the largest so far: `storage.py`, `utility.py`,
  `operation.py`, `db.py`, `server_utils.py`, `user.py`, `enrich.py`,
  `query.py`, `ingest.py`, `ws.py`. Rewrote
  `ServerUtils.dump_params()` — called with `locals()` from nearly every
  handler in the module — once, applying decision 16 (summary + `detail`)
  to every call site that uses it without touching each handler
  individually. That rewrite also fixed a pre-existing credential leak of
  the same kind found in `collab_api.py`: `dump_params()` was dumping the
  full handler parameter dict, which for nearly every handler includes
  `token` (the raw auth header) and, for auth handlers, `password` in
  plaintext — logged on every single HTTP request. Added
  `ServerUtils._SENSITIVE_PARAM_NAMES = {"token", "password", "pwd_hash"}`,
  redacted to `"***"` unconditionally (unlike the rest of `detail`, which
  is dropped outside `dev_mode` but would otherwise still show these) —
  verified live that `password`/`token` show as `"***"` even with
  `--dev-mode`. The same redaction covers the two spots that log
  `r.headers`/multipart headers, since the auth token travels as a header.
  Applied decision 16 elsewhere in the module too: `GulpRequestStats`
  dumped via its dataclass-generated `__repr__` in several places, queued
  task dicts in `run_rebase_task`/`run_ingest_file_task`/`run_query_task`,
  the per-chunk query callback context, query/plugin-params objects,
  arbitrary `user_data`, and an `update_by_query` body. One existing unit
  test mocked `MutyLogger` directly on the `query` module and needed
  updating to mock `Glug` instead. Integration testing (a large batch,
  given the module's size) surfaced one new failure alongside the two
  already-tracked pre-existing ones:
  `test_ingest_win_evtx_multiple_parallel` undercounts ingested documents
  with `RuntimeError: Failed to parse chunk header` from a third-party
  EVTX parser during parallel ingestion of sample files; reproduced
  identically on the pre-migration baseline, consistent with I/O or
  memory contention in this devcontainer under parallel load — tracked
  separately from log-revolution.
- migrated `src/gulp/api/redis_api.py` (84 call sites) as the seventh Fase 4
  module: mechanical `%s`-style to f-string conversion, and
  `.exception(str(ex))` instead of `.exception(ex)` inside `except` blocks
  so the traceback is captured. In the per-message hot path `publish()`,
  downgraded the "published large ... message stored in N chunks" log from
  `WARNING` to `DEBUG` (routine chunking of oversized messages, not an
  anomaly), keeping `WARNING` for the non-atomic fallback branch only;
  `_apply_publish_backpressure()` stayed at `WARNING` (a legitimate
  throttling signal). Applied decision 16 to `publish()`'s serialization
  failure and to `task_enqueue()`'s missing-`task_type` and enqueued-task
  debug dump. Two existing unit tests
  (`test_task_handler_idempotency.py`, `test_task_queue_semantics.py`)
  mocked `MutyLogger` directly on the `redis_api`/`query` modules and
  needed updating to mock `Glug` instead.
- added decision 17 to `plans/log-revolution.md` and applied it
  retroactively across every Fase 4 module migrated so far: banned
  decorative arrow prefixes (`--->`, `-->`), asterisk emphasis
  (`***TEXT***`, `**word**`) and exclamation marks used for emphasis
  (e.g. `"...DONE!"`) from `Glug` log messages, inherited unchanged from
  `MutyLogger`-era call sites — these are redundant with the structured
  `level`/`module_name`/`func`/`line` fields `Glug` already captures, and
  aren't machine-parseable. Cleaned up `api/collab/structs.py`,
  `api/collab/source_field_types.py`, `api/collab/stats.py`,
  `api/collab/user_session.py`, `api/collab_api.py`,
  `api/opensearch/sigma.py`, `api/server/db.py`, `api/server/enrich.py`,
  `api/server/ingest.py`, `api/server/query.py`,
  `api/server/server_utils.py`, `api/server/user.py`, `api/server/ws.py`,
  and `api/redis_api.py` (`events.py` was already clean). One instance in
  `sigma.py` was more than cosmetic: an `.exception()` in
  `sigmas_to_queries_wrapper` glued the entire original and patched sigma
  YAML into the message between `***ORIGINAL***`/`***PATCHED***`
  markers — moved to `detail=` (decision 16) instead of just stripping the
  asterisks. Left untouched, out of scope: `raise Exception(...)`/
  `raise ObjectNotFound(...)` message text (not `Glug` log calls),
  legitimate markdown emphasis in FastAPI/OpenAPI docstrings, the `"***"`
  redaction mask in `server_utils.py::dump_params()`, and `__main__.py`/
  `api/server_api.py`/`process.py` (not yet migrated to `Glug`, still on
  `MutyLogger` — will get the same style treatment when they're migrated).
  Validated with the full unit suite (564/564) and a representative
  integration batch (`test_db.py`, `test_collab.py`, `test_users.py`,
  `test_queries.py`, `test_enrich.py`, `test_ingest.py`,
  `test_ws_collab_notifications.py` and related websocket tests); the
  only 3 failures reproduce identically on the pre-cleanup baseline (via
  `git stash`) and match the pre-existing, already-tracked issues above
  (`object_delete_bulk` ws_id bug, OpenSearch disk-watermark pressure,
  parallel EVTX parser flakiness).
- migrated `src/gulp/api/ws_api.py` (62 call sites), `src/gulp/api/opensearch_api.py`
  (63 call sites), and `src/gulp/plugin.py` (96 call sites) as the eighth, ninth, and
  tenth Fase 4 modules, chosen together by the user and migrated in sequence: mechanical
  `%s`-style to f-string conversion, `.exception()` instead of `.error(ex)`/
  `.error(msg, ..., ex)` in real `except` blocks, and decision 17's style rules (no
  decorative arrows/asterisks/exclamation marks) applied directly since none of the
  three had been touched by the earlier retroactive cleanup pass. Applied decision 16
  to several `orjson.dumps(..., OPT_INDENT_2)` dumps already present in
  `opensearch_api.py` (mapping, query DSL, aggregations, rebase) and `plugin.py`
  (`_initialize`, `_ensure_plugin_params`, the enrich query, and a full-document dump in
  the "does not have context_id or source_id set" warning). Two verbosity downgrades in
  `opensearch_api.py`'s bulk-ingestion hot path: the replica-count log moved from
  `WARNING` to `DEBUG` (a config echo, not an anomaly) and `search_dsl`'s "this is the
  last chunk" moved from `WARNING` to `DEBUG` (normal flow control, not a problem); no
  level changes were needed in `plugin.py` (its heavier debug calls were already gated
  behind cache-miss or a configurable frequency, not unconditional per-record/per-chunk
  logging). Removed imports left dead by the cleanup: `muty.string` and the bare `muty`
  import in `ws_api.py`, `muty.string` in `opensearch_api.py`, and `orjson` in
  `plugin.py`. Validated with the full unit suite after each of the three modules
  (564/564 throughout) and, after all three, a broad integration batch (31 files, nearly
  the full suite excluding `test_stress.py`, `test_concurrent_multi_user_scenario.py`,
  and `test_internal_events.py`, which needs dedicated multi-instance setup): 146 passed,
  2 skipped, 3 failures — all three the same pre-existing environmental issues already
  tracked above (`object_delete_bulk` ws_id bug, OpenSearch disk-watermark pressure,
  parallel EVTX parser flakiness), not new regressions.
- fixed a pre-existing credential leak found live by the user while reviewing the new
  `Glug` console output during Fase 4's plugin migration: `AsyncOpenSearch`'s `repr()`
  embeds `http_auth` in cleartext (e.g. `{'host': 'localhost', 'port': 9200, 'http_auth':
  'admin:Gulp1234!'}`), unlike `AsyncElasticsearch` or `redis.Redis` (both redact or omit
  credentials in their own `repr()`). Two call sites in `opensearch_api.py` were logging
  the client object directly: `GulpOpenSearch._get_client()`/`.shutdown()` (gulp's own
  OpenSearch connection) and `search_dsl()` (an externally-provided `el` client, which for
  external-query plugins may hold a customer's own OpenSearch/Elasticsearch credentials —
  the more sensitive of the two). Fixed by never interpolating the client object itself:
  `_get_client()` now logs the URL through a new `_masked_opensearch_url()` helper
  (`urllib.parse`-based, strips `user:pass@` from the netloc, same idea as
  `make_url(...).render_as_string(hide_password=True)` used for the Postgres URL in
  `collab_api.py`), and `search_dsl()` logs only the provided client's class name.
- revised `Glug`'s console output format (`GlugConsoleFormatter`), also found live by the
  user while comparing a `Glug` line against the equivalent `MutyLogger` line during the
  same review: the timestamp now includes the full date (`YYYY-mm-dd HH:MM:SS.mmm`, not
  just `HH:MM:SS.mmm`), which was ambiguous scrolling back more than a few minutes or
  across a midnight rollover. Also colored the message text itself (and its `{k=v}`
  context suffix) to match its level, not just the level tag — confirming the existing
  palette (DEBUG dim/gray, INFO green, WARNING yellow, ERROR/CRITICAL red/bold red) but
  applying it beyond just the level word, after the user noticed and liked this on a
  terminal screenshot — makes severity readable scanning down a column of stdout.
  Briefly restored, then removed again: `process_role`/`pid` and the current asyncio task
  name, present in every `MutyLogger` line (`gulp-worker-63145|Task-6`) but missing from
  `Glug`'s console line — added as `proc=`/`task=` key=value pairs (per explicit request
  for key=value over `MutyLogger`'s positional style), then taken back out once the user
  saw it live: this line is for a human watching stdout, and both fields are already in
  the JSON file (`GlugJsonFormatter`, always-present `"process_role"`/`"pid"`/
  `"async_task_name"` fields) for whatever actually parses logs. See
  `plans/log-revolution.md`, "Output console (stdout)", for the full history and the real
  captured examples.
- migrated `src/gulp/plugins/*` (33 files touching `MutyLogger`, ~190 active call sites)
  as the eleventh and final Fase 4 module, completing the call-site migration to `Glug`
  everywhere except `__main__.py`/`api/server_api.py`/`process.py` (bootstrap-only,
  untouched log call sites, the last thing before Fase 5). 11 files had only a dead
  import and no active call sites (`apache_access_clf.py`, `apache_error_clf.py`,
  `eml.py`, `iis_access.py`, `iis_access_ncsa.py`, `iis_access_w3c.py`, `lin_syslog.py`,
  `mbox.py`, `mysql_general.py`, `pfsense.py`, `systemd_journal.py`); the rest ranged
  from 1 to 53 call sites, with `pcap.py` (53), `enrich_whois.py` (44, the reference
  enrichment plugin) and `query_elasticsearch.py` (16, the reference external-query
  plugin) the largest. Same mechanical pattern as prior modules (`%s`-style to f-string,
  `.exception()` in real `except` blocks, decision 17 applied directly since these files
  had never been touched by the earlier retroactive cleanup, decision 16 for the handful
  of object/dict dumps found in messages).
  - **Self-caught bug from the migration's own shortcut**: to speed up 5 files with many
    already-f-string call sites (`enrich_circl_hash.py`, `enrich_abuse.py`,
    `extension/bridge_manager.py`, `enrich_whois.py`, `pcap.py`), a plain
    `sed 's/MutyLogger\.get_instance()/Glug.get_instance()/g'` was used instead of
    editing each call site by hand. This correctly renamed the class but didn't touch
    arguments — and one surviving `enrich_whois.py` call site still had
    `.warning("...: %r", original_input)`, a second positional argument. `Glug`'s API
    (decision 16) is `.warning(msg, exc_info=None, hint=None, detail=None, **context)`:
    a second positional argument silently binds to `exc_info` instead of being
    interpolated — a quiet bug with no import-time error, wrong behavior only at
    runtime. Caught before the user found it, by writing a small `ast`-based checker
    (walks the AST, flags any `Glug.get_instance().<level>(...)` call with more than one
    positional argument) and running it against every `sed`-touched file *and* the rest
    of the already-migrated repo, to make sure no other instance existed (found none
    elsewhere). Fixed the one real case, then kept the same script as part of final
    validation for every remaining file. Lesson for the rest of Fase 4/Fase 5: a bare
    rename `sed` is only safe when the call site never had extra positional arguments to
    begin with (which `MutyLogger`, stdlib-`logging`-style, freely accepted) — it needs
    an automated check like this one afterward, since `py_compile`/`pytest` alone won't
    catch it (this is a runtime log-content bug, not a syntax error).
  - **Security bug found during the plugin migration** (spotted live by the user
    watching console output while reviewing the rest of the work — not in the plugins
    themselves but in `opensearch_api.py`, touched in the eighth/ninth/tenth module):
    `AsyncOpenSearch.__repr__()` embeds `http_auth` in cleartext (e.g. `{'host':
    'localhost', 'port': 9200, 'http_auth': 'admin:Gulp1234!'}`), unlike
    `AsyncElasticsearch`/`redis.Redis` (verified neither exposes credentials in its own
    `repr()`). Two call sites in `opensearch_api.py` were logging the client object
    directly: `GulpOpenSearch._get_client()`/`.shutdown()` (gulp's own client) and
    `search_dsl()` (a caller-supplied `el` client — for external-query plugins this may
    be the customer's own OpenSearch/Elasticsearch credentials, the more sensitive
    case). Fixed by never interpolating the client object: `_get_client()` now logs the
    URL through a new `_masked_opensearch_url()` helper (`urllib.parse`-based, same idea
    as `make_url(...).render_as_string(hide_password=True)` already used for
    `postgres_url` in `collab_api.py`), and `search_dsl()` (both in `opensearch_api.py`
    and the duplicated copy in `query_elasticsearch.py`) now logs only the provided
    client's class name.
  - **Pre-existing bug fixed in passing**: `extension/otel_receiver.py`'s
    `_flatten_attributes()` built its error message from a plain string literal instead
    of an f-string (`"error during parse attributes for {attributes}, error: {ex}"`, no
    `f` prefix) — `{attributes}`/`{ex}` were never interpolated, a pre-existing logging
    bug unrelated to the migration, fixed alongside the rest of the call site.
  - Validated: `pytest tests/unit/` (564/564 after every batch), the `ast`-based checker
    across the whole repo (zero violations), and a broad integration batch after a clean
    backend restart (`gulp --reset-collab --create test_operation` — needed because the
    running process predated every change in this session): `test_ingest.py`,
    `test_ingestion_pipeline.py`, `test_enrich.py`, `test_enrich_whois_raw.py`,
    `test_queries.py`, `test_plugins.py`, `test_pcap_plugin.py`, `test_raw_plugin.py`,
    `test_shared_object_plugin.py`, `test_shared_object_default_dashboards.py`,
    `test_enhance_doc_map.py`, `test_timestamp_normalization.py`. A first pass (right
    after too-quick a restart, before the backend had finished initializing) produced 6
    spurious `GulpOperation … not found` failures; restarted with a full wait and
    re-ran just the failed tests: all green. Only the two already-tracked environmental
    failures remain (`test_ingest_win_evtx_multiple_parallel`, parallel EVTX parser;
    `test_query_raw_paginate_supports_deep_offset_and_stable_pit_jumps`, OpenSearch disk
    watermark), both re-verified one final time.
- completed the `MutyLogger` → `Glug` migration (Fase 4/5 of `plans/log-revolution.md`):
  migrated the last three bootstrap files (`src/gulp/__main__.py`, `src/gulp/api/server_api.py`,
  `src/gulp/process.py`, 95 call sites) plus four files never counted in the original Fase 4
  inventory (`src/gulp/config.py`, `src/gulp/api/mapping/mapping_utils.py`,
  `src/gulp/api/prometheus_api.py`, `src/gulp/structs.py`, 41 more call sites). No gulp
  application code calls `MutyLogger` anymore.
  - Removed the `--log-to-file`/`--log-to-syslog` CLI flags and the `logger_file_path`/
    `log_to_syslog` parameters threaded through `GulpServer.start()`,
    `GulpProcess.finish_initialization()`, and `_worker_initializer()` — they only ever
    configured `MutyLogger`, so keeping them would have left silently-no-op flags once the
    last call site moved to `Glug`. `--log-dir` (Glug) is now the only file-logging option.
  - Fixed two credential/secret leaks found during the migration: `GulpServer.start()` dumped
    every environment variable at `INFO` on every startup (the largest leak found in this
    whole migration, since `postgres_url`/`opensearch_url`/S3 keys are commonly passed via
    env vars in production), and `GulpConfig.dump_config()` dumped the entire configuration
    dict (same secrets, from the config file) at `INFO`. Both now log a summary at `INFO` with
    the full structure in `detail=` (`dev_mode`-only), and `dump_config()` additionally masks
    known credential-bearing URL keys (`postgres_url`, `redis_url`, `s3_url`,
    `opensearch_url`) unconditionally, even in `dev_mode`, via a new `GulpConfig._masked_url()`.
  - Fixed a two-part regression the user caught live testing `--log-level critical` right
    after the migration: (1) `src/muty/*.py` (a standalone shared library, architecturally
    independent from `gulp.glug`) still called bare `MutyLogger.get_instance()` internally;
    removing `__main__.py`'s prior `MutyLogger.get_instance("gulp", level=lv, ...)` bootstrap
    meant the first such bare call now configured the **root logger** at `MutyLogger`'s DEBUG
    default instead, flooding stdout with every third-party library's propagated records
    regardless of `--log-level`. (2) Even after fixing that, `Glug`'s own *real* bootstrap
    (with the requested level) ran late in `_startup()`, after Postgres/OpenSearch/Redis had
    already logged through a bare, default-level `Glug` instance; moved the real bootstrap to
    the very start of `GulpServer.start()` and `_lifespan_handler()` (main process) and to the
    top of the worker branch in `finish_initialization()` (workers), keeping only the
    Redis-dependent reconfig-subscriber startup at its later position. Fixing this exposed a
    latent bug where `self._log_level` can be `None` when `_lifespan_handler()` runs without
    `start()` having been called first (as in 3 existing unit tests) — `Glug._setup()` didn't
    tolerate an explicit `level=None`; fixed with `level=self._log_level or logging.INFO` at
    all three bootstrap call sites.
  - Per explicit user request after seeing the cause of the regression above, removed every
    remaining `MutyLogger` **call** from the whole project, not just `src/gulp/`: migrated
    `src/muty/dynload.py`, `file.py`, `uploadfile.py`, `os.py`, `jsend.py`, `time.py` to plain
    `logging.getLogger(__name__)` (stdlib-only, keeping `muty` independent from both
    `MutyLogger` and `Glug`), removed a dead `MutyLogger` import from `src/muty/obj.py`, and
    migrated the standalone dev scripts `test_scripts/ingest_raw.py`/`query_external.py` to
    `logging.basicConfig()` + `logging.getLogger(__name__)`. The `MutyLogger.get_instance(...)`
    bootstrap calls added minutes earlier in `__main__.py`/`process.py` to fix the regression
    became unnecessary once nothing called `MutyLogger` anymore, and were removed too.
  - `src/muty/log.py` was **not** deleted, a deviation from the original Fase 5 plan: live
    validation found an external plugin in the user's runtime plugins directory (outside this
    repo) still does `from muty.log import MutyLogger`, which would break with an
    `ImportError` if the class were removed. Asked the user how to proceed; kept the full,
    unmodified original `MutyLogger` implementation (plus its private formatter/filter/syslog
    helpers) for external-plugin compatibility, alongside `exception_to_string()` (still used
    by `muty/jsend.py` and 6 files in `src/gulp/`). Confirmed zero call sites anywhere in
    `src/gulp/`/`src/muty/` — only the class definition remains, unused by this repo.
  - Validated: `pytest tests/unit/` (564/564, including the fix for 3 `test_collab_api.py`
    tests broken by the bootstrap-ordering change), a full clean backend restart with
    `--log-level warning` (confirmed live: all `Glug` `INFO` noise from Postgres/OpenSearch/
    Redis/websocket startup is now correctly suppressed, and the external `webhook.py` plugin
    loads again without an `ImportError`), and a broad integration batch (11 files): 91 passed,
    1 skipped, 2 failures — the same two already-tracked pre-existing environmental issues
    (parallel EVTX parser flakiness, OpenSearch disk-watermark pressure), no new regressions.
- turned the kept-for-compatibility `MutyLogger` (`src/muty/log.py`) into an active `Glug`
  forwarding wrapper: live validation showed its external-plugin output (e.g. `webhook.py`,
  `oauth.py`) still rendered in `MutyLogger`'s old pipe-delimited stdout format, visually
  inconsistent with the rest of the now-`Glug`-formatted console. Added
  `_GlugForwardingHandler(logging.Handler)`, whose `emit()` does a deferred, `try/except
  ImportError`-guarded `from gulp.glug import Glug` (never a module-level import, preserving
  `muty`'s architectural independence from `gulp`), maps the record's level to the matching
  `Glug` method, uses `record.getMessage()` (correctly applies `%`-style positional `args`),
  and forwards `exc_info=record.exc_info or None` so tracebacks survive. `MutyLogger._reconfigure()`
  now installs this handler as the default stdout handler instead of the old
  `StreamHandler`+`MultiLineFormatter`/`coloredlogs` combination; syslog/rotating-file handlers
  are unchanged, still available as opt-in extras when `log_to_syslog`/`logger_file_path` are
  passed explicitly. Removed the now-dead `MultiLineFormatter` class and `coloredlogs` usage.
  Known, accepted cosmetic limitation: forwarded lines show `muty.log:NN` as their
  `module:line` in `Glug`'s console output rather than the original external caller's location,
  since `Glug`'s fixed `_STACKLEVEL` assumes a direct call, not one relayed through a
  `logging.Handler`. Re-verified, per explicit user request, that zero active
  `MutyLogger.get_instance()`/`MutyLogger(...)` calls remain anywhere in the project outside
  `src/muty/log.py`'s own definition (`grep -rn` across `src/`, excluding that file: no
  results); the only remaining textual mentions are comparative comments in
  `tests/unit/test_glug.py`, `glug/core.py`, and `glug/console_formatter.py`. Validated live
  with two full backend restarts (`--log-level warning` and `--log-level debug`): both
  external plugins' output now renders through `Glug`'s formatter, zero pipe-delimited lines
  remain anywhere in the log; re-ran `pytest tests/unit/` (564/564) and an integration smoke
  batch (`test_db.py`, `test_collab.py`, `test_users.py`, `test_plugins.py` — 34 passed), no
  regressions.
- reverted the `_GlugForwardingHandler` wrapper above: the plugins actually producing the
  pipe-delimited `MutyLogger` output (`webhook.py`, `oauth.py`, and everything else under
  `gulp-paid-plugins/`) turned out to belong to `gulp-paid-plugins`, a separate git repo
  that `CLAUDE.md` explicitly says is "maintained like `src/gulp/plugins/`" — i.e. in scope
  for this migration, unlike `gulp-ui`/`gulp-collector`/`gulp-licensing`. Forwarding
  `MutyLogger` to `Glug` was the wrong fix for that: it papered over paid plugins never
  having been migrated. `src/muty/log.py` is back to the plain, unmodified `MutyLogger`
  kept only for any genuinely third-party/unmaintained plugin outside both repos; the real
  fix is migrating `gulp-paid-plugins` itself to call `Glug` directly (289 call sites across
  43 files, tracked separately).
- migrated `gulp-paid-plugins` (separate repo, branch `log-revolution` off `develop`) from
  `MutyLogger` to `Glug`: 43 files, 289 call sites, same pattern as `src/gulp/plugins/`.
  Unlike gulp's own already-f-string-style plugins, these used stdlib `logging`'s `%s`/`%d`
  positional style extensively, which `Glug` doesn't support (a second positional argument
  silently binds to `exc_info` instead of interpolating); re-ran the AST-based checker from
  the `src/gulp/plugins/` migration across the whole repo and found 53 call sites with this
  latent bug, all converted to f-strings, plus a manual sweep caught ~15 more call sites using
  a pre-interpolated `"..." % (...)` string (syntactically valid, still wrong style) and ~13
  bare `.exception(ex)` sites (the exception object as the whole message, no context) — all
  fixed. Found and fixed two real credential leaks in `extension/oauth.py` while at it: a debug
  line logged the Google/Microsoft OAuth `client_secret` in cleartext, and another dumped the
  full OAuth token payload (access/id/refresh tokens) at DEBUG unconditionally; the secret was
  removed from the message, the token dump moved into `detail=` (dev_mode-gated). Validated:
  AST checker and `py_compile` clean on every touched file, two full backend restarts with
  `--log-level debug` (paid plugins sync automatically into `~/.config/gulp/plugins/` in this
  devcontainer) showing zero pipe-delimited `MutyLogger` lines and no new plugin-load failures
  (the 3 that do fail — `ai_report.py`, `oauth.py`, `send_data_opencti.py` — are pre-existing,
  caused by missing paid-plugin configuration in this devcontainer, not by this change).
  Committed locally in the `gulp-paid-plugins` repo, not pushed.
- fixed `docker-entrypoint.sh` leaving the bind-mounted working directory
  root-owned when Docker auto-creates a missing host mount path as root
  regardless of the container's own user, blocking `GULP_RUNTIME_USER` from
  writing to it after the privilege drop; the entrypoint now reclaims
  ownership of `GULP_WORKING_DIR` while still root, before `gosu`.
- kept Table View responsive across operations with many disjoint source fields
  through horizontal column virtualization, bounded field discovery, a complete
  virtualized column menu, compact detached payloads, and correct hybrid
  `gulp.unmapped.*` value resolution; timestamp sorting now stays selected when
  the backend `@timestamp` metadata replaces the initial fallback.
- documented Web UI global-query timeline performance, including per-source
  React invalidations and measured operation-wide IndexedDB lookup amplification.
- replaced deep `query_raw_paginate` offsets with bounded PIT plus `search_after`
  seeking while preserving the legacy offset contract; added stable Table View
  pagination sessions, explicit PIT cleanup and expiry handling, and matching
  SDK and CLI controls; PIT boolean query parameters are serialized as lowercase
  OpenSearch literals for `opensearch-py` compatibility.
- added a `query-external` Web UI extension slot with exact backend-plugin manifest association, controlled request/validation handoff, full fallback behavior, and state restoration across Preview and Ingest navigation.
- added a bounded, source-local consecutive timestamp cache shared by every core and paid ingestion plugin, with isolated streams for multi-timestamp parsers; also removed repeated group extraction and invariant parser setup from Apache, IIS, and Zsh record loops while preserving timestamp offsets, validation, and document IDs.
- changed CSV Pro, JSON Lines Pro, and XRY parallel parsing to feed one source-owned ingestion pipeline, preserving bounded prefetch while restoring adaptive sizing, ordered callbacks/statistics, complete final bulk acknowledgement, and one final refresh.
- replaced file-size-based ingestion chunk tiers with exact-NDJSON adaptive sizing, validated every bulk against OpenSearch's advertised HTTP limit, and reused the serialized request body.
- overlapped file parsing with one OpenSearch-only bulk task, including an explicit event-loop handoff before buffered parsing resumes, while preserving chunk order, bounded ownership, final refresh, cancellation, callbacks, and ingestion statistics; the server-only pipeline is enabled by default and can be disabled in `gulp_cfg`.
- fixed non-final OpenSearch document failures missing from request statistics and prevented acknowledged chunks from being submitted again during exception finalization.
- refactor ingestion bulks methods to avoid unusefull duplicate loop; made the full time-range filter mutually exclusive from its one-sided cases so out-of-range documents are skipped correctly.
- changed Web UI file ingestion from per-chunk inside indexDB cache, and no backend cancellation when browser-local cache capture fails.
- preserved `event.sequence` in Web UI EventStore pages and used it as the descending for equal ingestion timestamps across chunks, pages, cursors, and neighbor reads.
- added a shared 4 MiB incremental text-file reader to `muty.file` and migrated ingestion plugins while preserving newline and multiline semantics; the 100,000-line benchmark reduced read time from 7.701 s to 0.183 s (97.63%).
- normalized each main ingestion timestamp once in `GulpDocument`, added explicit format/timezone/invalid-value controls and pre-normalized values, and migrated built-in parsers without changing timestamp offsets or document IDs.
- reused one empty `GulpMapping` fallback per plugin instance while preserving dynamic mapping selection, reducing parsing time in the 100,000-event benchmark by 31.20%.
- documented the Prometheus endpoint, service, and two-target scrape prerequisites for the multi-instance integration launcher.
- added a timeline empty-area context menu for cache-aware bulk refetch and bulk filter reset across every user-selected source, including sources hidden from the current Canvas view.
- added a browser-local per-source query cache pool to `gulp-ui`, with IndexedDB provenance, startup reuse, source-local Undo/Redo, authority-safe rebase handling, and visible freshness warnings without backend, SDK, or CLI changes.
- made initial `gulp-ui` cache reuse source-range aware: a fresh same-query cache can satisfy a covered timeframe without a false warning, while render projections report an exact frame-local event count separately from generation authority.
- added negotiated query WebSocket flow control and independent UI normalization/persistence stages, preventing slow IndexedDB commits from accumulating an unbounded raw backlog; expected request cancellations are no longer logged as query execution errors.
- added passive on-demand QueryWorker diagnostics, ACK-backed Canvas frame telemetry.
- made RAM compatibility fallback explicit for persistence and render-worker failures, with a deduplicated localized decision banner, scalable retry, verified opt-in marker, and no automatic reload.
- bounded accepted raw `docs_chunk` backlog to 128 MiB globally and 64 MiB per request, with synchronous overload rejection, request cancellation, isolated stream failure, and current/high-water diagnostics.
- guaranteed a visible RenderWorker frame within 100 ms of continuous committed data traffic while retaining the normal 8 ms quiet-window batching.
- prevented EventStore projection starvation by coalescing same-viewport refreshes, preserving active coherent building snapshots, and fencing terminal projections by generation, status, revision, and count.
- reduced post-ingest timeline stalls by batching visible-source manifest fences, retaining revision-qualified EventStore projection pages in bounded worker LRU caches, and reusing complete horizontal-overscan projections during normal pans.
- made uncached operation loads and Navigator event queries render every partial `query_raw` replacement chunk through bounded worker-local preview pages, immediately revealing sources hidden by their initial zero-event building summary while keeping the final IndexedDB pages densely packed; neither in-flight cache appends nor selected sources whose queries have not registered yet block the complete multi-source viewport.
- preserved request-scoped Redis/WebSocket message ordering so large `docs_chunk` payloads cannot be overtaken by `query_done`, while retaining concurrency between independent requests.
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
