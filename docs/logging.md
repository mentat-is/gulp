- [Logging in gULP: Glug](#logging-in-gulp-glug)
  - [Why Glug exists](#why-glug-exists)
  - [Design overview](#design-overview)
  - [Two outputs, one record: console and JSON](#two-outputs-one-record-console-and-json)
    - [Console line](#console-line)
    - [JSON file](#json-file)
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
    json["GlugJsonFormatter
    → per-process JSON file (--log-dir only)"]

    call --> emit --> stdlib
    stdlib --> console
    stdlib --> json
```

## Two outputs, one record: console and JSON

Every log call builds exactly one `LogRecord`, carrying the message plus
whatever context/detail/hint were attached. Two formatters turn that same
record into two different outputs:

### Console line

Always on, unconditionally, on stdout — an interactive terminal, a Docker
log, or whatever `journald`/`supervisor`/etc. captures a process's stdout
already gets this without any extra configuration. Colorized only when
stdout is a real terminal (`sys.stdout.isatty()`, checked once at setup, not
per line). One line per record, deliberately terse:

```text
2026-09-22 12:41:41.011 INFO     gulp.api.server_api:start:420  starting server at 0.0.0.0, port=8080, reset_collab=True, create_operation=test_operation
```

`<timestamp> <LEVEL> <module>:<funcName>:<line>  <message>  {context k=v...}`
— the message itself is colored by level (not just the level tag), so
severity reads at a glance scrolling down a column of lines. A `hint=`, a
`detail=` (only when `dev_mode` is on, pretty-printed, indented), and an
exception traceback (when `exc_info` is set) each add their own indented
lines below the header when present.

Deliberately **not** on the console line: the process identity (which
worker, PID) and the current asyncio task name. Early revisions tried
adding them as `proc=`/`task=` key=value pairs; after trying it live, the
call was that stdout should stay terse for a human — that information is
one field away in the JSON file for whatever actually needs it.

### JSON file

Off by default — opt in with `--log-dir <path>`. One rotating file **per
process**, `<log-dir>/<hostname>/gulp-main.log` or
`gulp-worker-<pid>.log`, written *in addition to* stdout, never instead of
it (one file per process, never shared across processes, specifically to
avoid the corruption risk `RotatingFileHandler` has when multiple processes
rotate the same file). Every field `MutyLogger`'s old format string carried,
plus more:

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

**A second, exception-only file** sits alongside the main one —
`gulp-main-exceptions.log` / `gulp-worker-<pid>-exceptions.log` — collecting
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
call per process that sets the actual `name`/`level`/`dev_mode`/`log_dir`,
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
| `--log-level {critical,error,warning,info,debug}` | Sets the effective level (default: `debug`). |
| `--dev-mode` | Forces `DEBUG` regardless of `--log-level`, and marks every record `dev_mode=true` — unlocks `detail=` payloads everywhere. |
| `--log-dir <path>` | Enables the per-process JSON file, in addition to stdout (which is always on). Omit to keep stdout only. |

`--log-to-file`/`--log-to-syslog` (the old `MutyLogger`-only flags) no
longer exist — `--log-dir` is the only file-logging option now.

## Runtime reconfiguration

`gulp.glug.reconfig` broadcasts a `level`/`dev_mode` change to every gulp
process (main + every worker) live, via a dedicated Redis pub/sub channel
(`gulp:glug:reconfig`) plus a persisted state key (`gulp:glug:state`) so a
process started *after* the change still picks up the current state on its
next `sync_from_persisted_state()` call, instead of missing a one-shot
pub/sub message. `publish_reconfig(level=, dev_mode=)` is the entry point;
as of this writing it has no REST endpoint wired to it yet — it's called
programmatically, ready for an admin API route or CLI subcommand to use.

## Process bootstrap: main and workers

Each process (main, and every worker) does its own real bootstrap — one
`Glug.get_instance(name=..., level=..., dev_mode=..., log_dir=...,
process_role=..., reconfigure=True)` call, as early as possible:

- **Main**: `GulpServer.start()` (sync, before `uvicorn.run()`) and
  `_lifespan_handler()` (async, first thing once the event loop is running)
  both bootstrap with `name="glug"` — the second call is a cheap,
  idempotent safety net in case anything logged between the two.
- **Worker**: `GulpProcess._worker_initializer()` bootstraps with
  `name="glug-worker-<pid>"` as the very first thing, before touching
  `GulpConfig` or anything else — `log_level`/`log_dir`/`dev_mode` are
  already available as parameters there, so there's no reason to wait.
  `finish_initialization()`'s worker branch reconfigures the same name
  again, same safety-net pattern as main.

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
  overwrites the handlers just attached).

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
