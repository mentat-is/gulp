- [Logging in gULP: Glug](#logging-in-gulp-glug)
  - [Why Glug exists](#why-glug-exists)
  - [Design overview](#design-overview)
  - [One record, several outputs](#one-record-several-outputs)
    - [Console line](#console-line)
    - [Log file (`--log-file`)](#log-file---log-file)
    - [JSON file (`system_logs` plugin)](#json-file-system_logs-plugin)
  - [Using Glug in application code](#using-glug-in-application-code)
    - [Levels and the `exception()` shorthand](#levels-and-the-exception-shorthand)
    - [`detail=`: structured payloads, dev\_mode-gated](#detail-structured-payloads-dev_mode-gated)
    - [`hint=`: a short suggestion for a human](#hint-a-short-suggestion-for-a-human)
    - [Automatic request/operation/user context](#automatic-requestoperationuser-context)
    - [Writing style](#writing-style)
  - [Configuration (CLI flags)](#configuration-cli-flags)
  - [Runtime reconfiguration](#runtime-reconfiguration)
  - [Process bootstrap: main and workers](#process-bootstrap-main-and-workers)
  - [Third-party loggers](#third-party-loggers)
  - [Shipping logs to OpenSearch](#shipping-logs-to-opensearch)
  - [What about `muty`?](#what-about-muty)
  - [Known limitations](#known-limitations)

# Logging in gULP: Glug

This document explains **why** gULP has its own logger, **what it looks
like** from a developer's point of view, and **how it's put together**
underneath — for anyone who needs the big picture without reading through
the full decision-by-decision migration log. That log lives in
`plans/log-revolution.md` (a working document, not shipped as part of this
repo's tracked history) for anyone who wants the "why did we choose X over
Y" detail behind every design call summarized here.

## Why Glug exists

Until gULP v2.0.x, all application logging went through `MutyLogger`, a
class in the shared `muty` utility library that subclassed `logging.Logger`
directly. That design had grown uncomfortable for gULP specifically:

- **Architectural inversion**: `src/muty/` is meant to be a standalone
  library gULP depends on, never the other way around — but `MutyLogger`
  was gULP's *only* logger, coupling gULP's own logging behavior to a
  generic shared utility that other, unrelated consumers of `muty` also
  depend on.
- **One format for two very different audiences**: the same pipe-delimited
  text line went to an interactive terminal *and* to whatever might parse
  logs downstream (a log shipper, an audit trail). Neither audience was
  well served: humans got a dense, hard-to-scan line; a machine parser got
  unstructured text instead of a real schema.
- **No place for structured context**: attaching request/operation/user
  identifiers, or a full data structure worth inspecting during a specific
  bug hunt, meant hand-formatting them into the message string every time.

`Glug` (**G**ulp **L**og **U**nified **G**ateway, `src/gulp/glug/`) replaces
`MutyLogger` for all of gULP's own code (`src/gulp/**`). It is a small,
gULP-owned package with no dependency beyond the stdlib, built around one
idea: **the same `LogRecord` renders differently for a human glancing at a
terminal than it does for a JSON file meant to be searched, aggregated, or
shipped elsewhere** — so it renders both, from the same call, and lets each
caller attach real structure instead of formatting it into a string.

## Design overview

`Glug` is a **process-local singleton** — one instance per OS process (the
main process and each worker each have their own). It **composes** a plain
`logging.Logger` internally rather than subclassing it (the design
`MutyLogger` used, and the source of the "why does this even have its own
`__init__`" friction that made `MutyLogger` awkward to reason about).
Composition keeps `Glug`'s own public surface small and explicit: five level
methods, an `exception()` shorthand, and a couple of context/reconfiguration
helpers — everything else is an internal detail of how those methods happen
to be implemented on top of stdlib `logging`.

```mermaid
flowchart LR
    call["Glug.get_instance().info(msg, hint=, detail=, **context)"]
    emit["Glug._emit()
    merges contextvars + extra fields"]
    stdlib["stdlib logging.Logger.log()"]
    console["GlugConsoleFormatter
    → stdout (always on)"]
    logfile["GlugConsoleFormatter, no colors
    → shared text file (--log-file)"]
    json["GlugJsonFormatter
    → per-process JSON file (system_logs plugin)"]

    call --> emit --> stdlib
    stdlib --> console
    stdlib --> logfile
    stdlib --> json
```

## One record, several outputs

Every log call builds exactly one `LogRecord`, carrying the message plus
whatever context/detail/hint were attached. Two formatters turn that same
record into up to three outputs: the console line on stdout (always), the
same line in a plain-text file (`--log-file`, optional, core), and a JSON
document (only with the paid `system_logs` extension plugin).

### Console line

Always on, unconditionally, on stdout — an interactive terminal, a Docker
log, or whatever `journald`/`supervisor`/etc. captures a process's stdout
already gets this without any extra configuration. Colorized only when
stdout is a real terminal (`sys.stdout.isatty()`, checked once at setup, not
per line). One line per record, deliberately terse:

```text
2026-09-22 12:41:41.011 INFO gulp.api.server_api:start:420[]  starting server at 0.0.0.0, port=8080, reset_collab=True, create_operation=test_operation
2026-09-30 10:19:35.434 INFO gulp.api.server_api:_lifespan_handler:1387[Task-2]  gULP main server process is starting
```

`<timestamp> <LVL> <module>:<funcName>:<line>[<task name>]  <message>
{context k=v...}` — the message itself is colored by level (not just the
level tag), so severity reads at a glance scrolling down a column of
lines.

The level is a fixed 4-character tag — `DEBG`, `INFO`, `WARN`, `ERRR`,
`CRIT` — so the module column lines up without padding every line out to
the width of `CRITICAL`. Tags are abbreviated by dropping vowels rather
than by truncation (`DEBG`, not `DEBU`), so each still reads as its level.
The JSON file keeps the full `levelname`.

The brackets glued to the line number hold the current asyncio task's
name (e.g. `[Task-2]`), in grey when colored, so it's readable without
competing with the message. They are always present: empty (`[]`) for
sync code running outside any task (before the event loop starts, e.g.
`start()`'s own initial lines), so whether a line came from a task is
obvious at a glance. A `hint=`, a `detail=` (only when `dev_mode` is on,
which only the `system_logs` plugin can do; pretty-printed, indented), and an exception traceback (when `exc_info` is
set) each add their own indented lines below the header when present.

Deliberately **not** on the console line: the process identity (which
worker, PID). Early revisions tried adding it, alongside the task name, as
`proc=`/`task=` key=value pairs; after trying it live, the call was that
stdout should stay terse for a human — `proc=`/`pid` stayed out (still one
field away in the JSON file for whatever needs it), while the task name was
brought back on its own in later revisions: first as a bare name after the
line number, then in the compact `[Task-2]` form above.

### Log file (`--log-file`)

`--log-file <path>` appends to `<path>` exactly the lines printed on stdout,
without ANSI colors — nothing more. It needs no plugin. The main process and
every worker append to the same file, as they all print to the same stdout;
each record is a single `write()` on a file opened in append mode, so lines
from different processes never interleave.

gulp never rotates this file: several processes write it, and in-process
rotation (`RotatingFileHandler`) corrupts a file rotated by more than one of
them. Rotate it with an external tool such as logrotate. The file is opened
with `WatchedFileHandler`, which reopens it once logrotate moves it away, so
`copytruncate` is not needed:

```text
/var/log/gulp/gulp.log {
    daily
    rotate 14
    compress
    missingok
}
```

### JSON file (`system_logs` plugin)

Off in the core: there is no CLI flag for it. The paid `system_logs`
extension plugin turns it on at runtime, through
[runtime reconfiguration](#runtime-reconfiguration), always under the
fixed `<working_dir>/logs` (`~/.config/gulp/logs` or
`$GULP_WORKING_DIR/logs`). One rotating file **per process**,
`<log_dir>/<hostname>/gulp-main.jsonl` or `gulp-worker-<pid>.jsonl`, written
*in addition to* stdout, never instead of it (one file per process, never
shared across processes, specifically to avoid the corruption risk
`RotatingFileHandler` has when multiple processes rotate the same file).
Every field `MutyLogger`'s old format string carried, plus more:

```json
{
  "timestamp_msec": 1758544901011,
  "level": "INFO",
  "process_role": "main",
  "hostname": "devcontainer",
  "pid": 310561,
  "thread_id": 310561,
  "async_task_name": null,
  "module_name": "gulp.api.server_api",
  "module_path": "gulp/api/server_api.py",
  "func": "start",
  "line": 420,
  "message": "starting server at 0.0.0.0, port=8080, reset_collab=True, create_operation=test_operation",
  "dev_mode": false,
  "context": {"req_id": null, "operation_id": null, "user_id": null, "plugin": null, "client_ip": null},
  "hint": null,
  "detail": null,
  "exception": null
}
```

Records from third-party loggers routed through `Glug` (uvicorn, libraries
propagating to the root logger) carry no `Glug` fields of their own: their
`process_role` and `hostname` are the writing process's, and
`async_task_name` the task name stdlib `logging` records.

**A second, exception-only file** sits alongside the main one —
`gulp-main-exceptions.jsonl` / `gulp-worker-<pid>-exceptions.jsonl` — collecting
the exact same records as the main file, filtered to those with `exc_info`
set. Nothing is removed from the main file (it keeps every exception too,
for whoever already queries it); the dedicated file is a focused stream for
postmortem analysis without grepping the whole log.

## Using Glug in application code

```python
from gulp.glug import Glug

Glug.get_instance().info(f"loaded {count} extension plugins")
Glug.get_instance().warning(f"disk usage high: {pct}%")
Glug.get_instance().debug("query result", detail=result_dict)
```

`Glug.get_instance()` returns the current process's singleton — cheap to
call repeatedly, no need to hold a reference. (The real bootstrap, the one
call per process that sets the actual `name`/`level`/`log_file`,
happens once at process startup — see
[Process bootstrap](#process-bootstrap-main-and-workers).)

### Levels and the `exception()` shorthand

`debug` / `info` / `warning` / `error` / `critical` all share the same
signature:

```python
Glug.get_instance().<level>(msg: str, exc_info: bool | BaseException | None = None, hint: str | None = None, detail=None, **context)
```

`exception(msg, hint=None, detail=None, **context)` is shorthand for
`.error(msg, exc_info=True, ...)`, meant for use inside an `except` block:

```python
try:
    ...
except Exception as ex:
    Glug.get_instance().exception(f"failed to enrich document: {ex}")
```

**`msg` is always a single positional string.** Unlike stdlib
`logging.Logger.log()`, `Glug` does not support the old `"...%s...", arg`
positional-args style — a second positional argument silently binds to
`exc_info` instead of interpolating, with no error at import time. Always
use an f-string for the message.

### `detail=`: structured payloads, dev_mode-gated

Pass a dict, a list, an ORM object, whatever is already on hand — `Glug`
only pays the `str()`/JSON-serialization cost inside the formatters, which
only run when a handler actually emits the record, so nothing is paid when
`dev_mode` is off. **`detail` is dropped entirely (never even reaches a
formatter) unless `dev_mode` is on** — this is the one place gULP owns a
structured secrets boundary: never log a raw credential/token/URL directly
in `msg` (that always renders), but a full request payload or config dict
worth inspecting only during active debugging belongs in `detail=`.

```python
Glug.get_instance().debug(f"working_dir={self._working_dir}", detail=redacted_config)
```

### `hint=`: a short suggestion for a human

A one-line, actionable suggestion attached to a warning/error, rendered on
its own indented line (`→ hint: ...`) only when given:

```python
Glug.get_instance().error("bulk ingestion failed", hint="check that OpenSearch is reachable")
```

### Automatic request/operation/user context

`gulp.glug.set_context(**fields)` sets one or more of `req_id`,
`operation_id`, `user_id`, `plugin`, `client_ip` as `contextvars.ContextVar`s
— they follow the current async task automatically (unlike thread-locals)
and get merged into every record emitted while set, without passing them at
every call site:

```python
from gulp.glug import set_context, reset_context

tokens = set_context(req_id=req_id, operation_id=operation_id)
try:
    ...  # every Glug call in here carries req_id/operation_id automatically
finally:
    reset_context(tokens)
```

An explicit keyword at the call site (`Glug.get_instance().debug(msg,
plugin="x")`) always overrides the contextvar-derived value for that field.

### Writing style

- Message = a concise, lowercase summary. No decorative arrow prefixes
  (`--->`), asterisk emphasis (`***`), or exclamation marks — the level tag
  and color already carry emphasis.
- Never log a credential, token, password, or a client/URL object whose
  `__repr__()` might expose one (checked case by case — `opensearch-py`'s
  `AsyncOpenSearch`, in particular, exposes `http_auth` in its own `repr()`,
  unlike `AsyncElasticsearch`/`redis.Redis`). Mask, or move to `detail=` if
  it must exist only for active debugging.

## Configuration (CLI flags)

| Flag | Effect |
|---|---|
| `--log-level {critical,error,warning,info,debug}` | Sets the effective level (default: `info`). Given explicitly, it also wins over the level saved by the `system_logs` plugin, and overwrites it. |
| `--log-file <path>` | Also appends every stdout line, without colors, to `<path>`, shared by all processes. Never rotated by gulp. See [Log file](#log-file---log-file). |

The core has no `--dev-mode` or `--log-dir`: `dev_mode` and the JSON files
are subscription features, enabled only by the paid `system_logs`
extension plugin. Without it, `detail=` payloads never render anywhere.
`--log-to-file`/`--log-to-syslog` (the old `MutyLogger`-only flags) no
longer exist either.

The startup banner and version string are plain `print()`s, not log records:
they go to stdout only (never to `--log-file` or the JSON file), the banner in a random
color and the version in bold, in a different color — plain text when
stdout is not a terminal.

At startup gulp no longer prints the raw command line; once Glug is
configured, it logs each server option's effective value at `DEBUG`, marking
the ones left to their default:

```text
... DEBG gulp.api.server_api:start:386[]  command line option log_level='debug'
... DEBG gulp.api.server_api:start:386[]  command line option log_file=None (default)
```

## Runtime reconfiguration

`gulp.glug.reconfig` broadcasts a `level`/`dev_mode`/`log_dir` change to
every gulp process (main + every worker) live, via a dedicated Redis pub/sub
channel (`gulp:glug:reconfig`). Each published field is also merged into a
persisted Redis hash (`gulp:glug:state`), so a worker started *after* the
change (e.g. respawned after `parallel_processes_respawn_after_tasks`)
still picks up the current state through `sync_from_persisted_state()`,
instead of missing a one-shot pub/sub message.

`publish_reconfig(level=, dev_mode=, log_dir=)` is the entry point. Every
argument left out keeps its current value; `log_dir=None` closes the JSON
files. A directory that cannot be opened is logged as an error, with a
hint, and the JSON files stay off; the level/`dev_mode` change still
applies. The core never calls it, and exposes no REST endpoint for it: it
is the paid `system_logs` extension plugin's entry point, which also keeps
the level and `dev_mode` in its own saved configuration.

At startup the main process clears `gulp:glug:state` before spawning its
workers, so only the command line applies until a plugin publishes again:
a removed plugin cannot leave `dev_mode` or the JSON files on from a
previous run. Every gulp instance sharing the same Redis shares this state.

## Process bootstrap: main and workers

Each process (main, and every worker) does its own real bootstrap — one
`Glug.get_instance(name=..., level=..., log_file=..., process_role=...,
reconfigure=True)` call, as early as possible:

- **Main**: `GulpServer.start()` (sync, before `uvicorn.run()`) and
  `_lifespan_handler()` (async, first thing once the event loop is running)
  both bootstrap with `name="glug"` — the second call is a cheap,
  idempotent safety net in case anything logged between the two.
- **Worker**: `GulpProcess._worker_initializer()` bootstraps with
  `name="glug-worker-<pid>"` as the very first thing, before touching
  `GulpConfig` or anything else — `log_level`/`log_file` are already
  available as parameters there, so there's no reason to wait.
  `finish_initialization()`'s worker branch reconfigures the same name
  again, same safety-net pattern as main, then applies the persisted
  [runtime reconfiguration](#runtime-reconfiguration) state.

**Why this matters**: `Glug.get_instance()` with no prior bootstrap in a
process creates a "bare" instance at `level=INFO` (the class default). Any
`.debug()` call before the real bootstrap runs is silently dropped against
that default, regardless of what `--log-level` actually asked for — and the
first-ever call defaulting to `name=None` would attach a handler to the
Python **root logger**, picking up every other library's propagating
records. Code that must log something before a process's real bootstrap can
run (there are a couple of spots in `config.py`, reached even from
minimal CLI paths like `gulp --stop` that never bootstrap `Glug` at all)
always passes an explicit `name="glug"` rather than a bare call, for exactly
this reason.

## Third-party loggers

Two integration points exist for libraries that use plain stdlib `logging`
themselves, independent of gULP's own log level:

- **Noise suppression**: `opensearch-py`'s own transport logger (named
  `"opensearch"` — not `"opensearchpy"`, despite the package name) logs
  every single HTTP request at `INFO`, including periodic internal
  polling. `Glug._setup()` caps `opensearch`, `opensearchpy`,
  `elasticsearch`, `botocore`, `aiobotocore`, and `urllib3` to `WARNING`
  unconditionally (even in `dev_mode`) — their error-level output still
  comes through normally.
- **`Glug.route_stdlib_logger(name)`**: attaches the calling process's
  already-configured `Glug` handlers to another stdlib-named logger, so its
  output renders identically to every other gULP log line instead of the
  library's own format. Used for `uvicorn`/`uvicorn.error` right before
  `uvicorn.run(..., log_config=None)` (the `log_config=None` is required —
  otherwise uvicorn's own `logging.config.dictConfig()` call inside `run()`
  overwrites the handlers just attached). A routed logger keeps following
  later changes: the JSON files turned on or off, a new level.
- **Root logger**: libraries without handlers of their own propagate to
  the root logger. The bootstrapped `Glug` instance installs its handlers
  there too, so their records reach stdout, `--log-file` and the JSON file
  alike, and keeps the root level at `WARNING` (the stdlib default) so
  library `INFO`/`DEBUG` chatter never shows up. Handlers `Glug` did not
  install are left in place.

## Shipping logs to OpenSearch

gULP itself never writes its logs to OpenSearch (or anywhere else over the
network) from inside `Glug`: a log call depending on a network write to
OpenSearch would lose exactly the logs explaining *why* OpenSearch is
unreachable, and would turn every synchronous, local log call into network
I/O. An external shipper tails the JSON files instead.

Shipping is part of the paid `system_logs` extension plugin
(`gulp-paid-plugins`): it turns the JSON files on, creates the hidden
`.gulp-internal-logs-info` / `.gulp-internal-logs-debug` indices with their
retention policies, and ships a reference [Fluent Bit](https://fluentbit.io/)
setup and a `docker-compose` override. See its
`README_system_logs.md`, and `plans/log-shipping.md` /
`plans/system-logs-plugin.md` for the design discussion (why not write to
OpenSearch from `Glug`, why Fluent Bit, why the retention split by level).

## What about `muty`?

`src/muty/` stays architecturally independent — `muty` never imports
`gulp.glug`. Its own internal logging (`src/muty/*.py`) uses plain
`logging.getLogger(__name__)`.

`MutyLogger` itself (`src/muty/log.py`) is not removed — it's kept,
unmodified, purely for backward compatibility with plugins genuinely
outside both this repo and `gulp-paid-plugins` that might still import it
directly. No call site in `src/gulp/`, `src/muty/`, or `gulp-paid-plugins/`
uses it anymore.

## Known limitations

- **Startup logs never reach the JSON files or `dev_mode`**: extension
  plugins load at the end of startup, after the workers are spawned, so
  everything before the `system_logs` plugin publishes its state goes to
  stdout and `--log-file` only, at the command-line level.

- **Console `module:funcName:line` reflects the frame `Glug` computed its
  fixed stack-depth constant for** — a direct `Glug.<level>()` call. Code
  that forwards a stdlib `LogRecord` from elsewhere (there is currently no
  such path in this repo, since `MutyLogger` forwarding was tried and
  reverted — see `plans/log-revolution.md`) would show the forwarding
  point's location, not the original caller's.
- **Ctrl-C on a foreground instance** produces a noisy `aiomultiprocess`/
  `multiprocessing` traceback dump from worker processes before the
  (otherwise correct) graceful shutdown sequence completes — a
  signal-handling gap unrelated to logging itself; see the note at the end
  of `plans/log-revolution.md` for the cause and the proposed fix.
