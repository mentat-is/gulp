"""
Glug (Gulp Log Unified Gateway) — process-local structured logger.

Composes a plain stdlib `logging.Logger` (composition, not subclassing:
`MutyLogger` subclassed `logging.Logger` while making it impossible to
actually construct one via the normal protocol, see
plans/log-revolution.md). Sinks:

- stdout: always attached, one short human-readable colorized line per
  record (`gulp.glug.console_formatter.GlugConsoleFormatter`) — interactive
  use and Docker/systemd all already capture stdout themselves.
- `log_file` given (core `--log-file`): additionally, the very same lines
  as stdout, without ANSI colors, appended to one plain-text file shared by
  the main process and every worker. No internal rotation: several
  processes write it, and `RotatingFileHandler` corrupts a file rotated by
  more than one process. A `WatchedFileHandler` reopens the file when an
  external tool (logrotate) moves it away.
- `log_dir` given: additionally, a JSON-lines rotating file dedicated to
  this process (`gulp.glug.formatter.GlugJsonFormatter`) — one file per
  process, never shared, to avoid the concurrent-rotation corruption risk
  `RotatingFileHandler` has when several processes write to the same file.
- `log_dir` given: also a second, exception-only JSON-lines rotating file
  for this process, fed the exact same records as the main file (nothing
  is removed from the main file), filtered to those with `exc_info` set —
  a focused stream for postmortem analysis without grepping the whole log.

`log_dir` is not a core CLI option: the JSON files (like `dev_mode`) are
turned on and off at runtime through `apply_reconfig()`, by the paid
`system_logs` extension plugin — see plans/system-logs-plugin.md.

See plans/log-revolution.md, "Output su stdout vs file", for the full
rationale.
"""

import asyncio
import logging
import os
import socket
import sys
import threading
from logging.handlers import RotatingFileHandler, WatchedFileHandler

from gulp.glug.console_formatter import GlugConsoleFormatter
from gulp.glug.context import current_context
from gulp.glug.formatter import GlugJsonFormatter

# stack frames between the application call site (e.g. a call to
# Glug.debug()) and the internal logging.Logger.log() call that finally
# records pathname/lineno/funcName: Glug.debug() -> Glug._emit() ->
# logging.Logger.log(). kept as a constant so it stays correct in one place
# if that call chain shape ever changes.
_STACKLEVEL = 3

# apply_reconfig() sentinel for "leave log_dir unchanged", since None already
# means "turn the JSON files off"
_UNCHANGED = object()


def _current_task_name() -> str | None:
    """
    Returns:
        str | None: the current asyncio task's name, or None outside a running event loop / task
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    return task.get_name() if task is not None else None


class _ExceptionOnlyFilter(logging.Filter):
    """passes only records with exc_info set — used by the dedicated exceptions file handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        return bool(record.exc_info)


class Glug:
    """
    a singleton, per-process structured JSON logger for gULP.
    """

    _instance: "Glug | None" = None

    # stdlib logger names taken over by route_stdlib_logger(), kept at class level
    # so a reconfigured instance (and every later handler change) still reaches them
    _routed_logger_names: set[str] = set()

    # handlers a named instance installed on the root logger, replaced on its next refresh
    _root_handlers: list[logging.Handler] = []

    def __init__(self) -> None:
        raise RuntimeError("call Glug.get_instance() instead")

    @classmethod
    def get_instance(
        cls,
        name: str = None,
        log_file: str = None,
        log_dir: str = None,
        level: int = logging.INFO,
        dev_mode: bool = False,
        process_role: str = "main",
        max_log_size_mb: int = 4,
        max_kept_log: int = 10,
        reconfigure: bool = False,
    ) -> "Glug":
        """
        get the process-local Glug singleton, creating it on first call

        Args:
            name (str, optional): logger name, passed to logging.getLogger(). Defaults to None (root logger) —
                real bootstrap (server_api.py/process.py) always passes an explicit name ("glug",
                "glug-worker-<pid>") to avoid attaching a handler to the root logger.
            log_file (str, optional): plain-text file receiving the same lines as stdout (no colors),
                shared by every gulp process, never rotated by Glug itself. Defaults to None (stdout only).
            log_dir (str, optional): base directory for per-process JSON log files. Stdout (short,
                human-readable, colorized lines) is always on regardless of this parameter — see
                plans/log-revolution.md, "Output su stdout vs file". When log_dir is given, Glug
                additionally writes JSON to log_dir/<hostname>/gulp-main.jsonl or
                gulp-worker-<pid>.jsonl, on top of stdout, never instead of it. The server never passes
                it at bootstrap: it is turned on later via apply_reconfig().
            level (int, optional): stdlib logging level. Defaults to logging.INFO.
            dev_mode (bool, optional): forces the effective level to DEBUG and marks every record with
                dev_mode=true, so call sites can use is_dev_mode() to gate extra diagnostic content.
                Defaults to False.
            process_role (str, optional): "main" or "worker", selects the JSON log filename. Defaults to "main".
            max_log_size_mb (int, optional): rotate the JSON files after this size. Defaults to 4.
            max_kept_log (int, optional): number of rotated JSON files to keep. Defaults to 10.
            reconfigure (bool, optional): rebuild the singleton (and its handlers) even if one already
                exists. Defaults to False.

        Returns:
            Glug: the process-local singleton instance, always able to log to stdout, plus the files
                above when asked for — a bare Glug.get_instance() from any migrated call site works out
                of the box even before the process's real bootstrap has run.

        Throws:
            OSError: if log_file or the JSON files under log_dir cannot be created or opened
        """
        if cls._instance is None or reconfigure:
            instance = object.__new__(cls)
            instance._setup(
                name=name,
                log_file=log_file,
                log_dir=log_dir,
                level=level,
                dev_mode=dev_mode,
                process_role=process_role,
                max_log_size_mb=max_log_size_mb,
                max_kept_log=max_kept_log,
            )
            previous = cls._instance
            cls._instance = instance
            instance._refresh_handlers()
            if previous is not None and previous is not instance:
                # the previous instance's files are no longer reachable from anywhere:
                # its logger and routed loggers now carry the new handlers
                previous._close_file_handlers()
        return cls._instance

    def _setup(
        self,
        name: str,
        log_file: str,
        log_dir: str,
        level: int,
        dev_mode: bool,
        process_role: str,
        max_log_size_mb: int,
        max_kept_log: int,
    ) -> None:
        """
        (re)configures this instance's underlying logger and handlers: stdout (console,
        human-readable) is always attached; the shared plain-text file and the per-process
        JSON files are attached too, additionally, when asked for — never instead of stdout.
        """
        self._hostname = socket.gethostname()
        self._process_role = process_role
        self._dev_mode = dev_mode
        self._level = level
        self._max_log_size_mb = max_log_size_mb
        self._max_kept_log = max_kept_log

        self._logger = logging.getLogger(name)
        self._logger.propagate = False
        self._logger.handlers = []

        # third-party HTTP/transport client libraries log every single request at INFO
        # (opensearch-py in particular: one line per request, including routine polling
        # every few seconds) — this is noise unrelated to gulp's own diagnostics, capped
        # here unconditionally (even in dev_mode) rather than gated by gulp's own level.
        # note: opensearch-py's actual logger is named "opensearch" (see
        # opensearchpy/connection/base.py), not "opensearchpy" — both are covered here since
        # opensearchpy.helpers/opensearchpy.trace do exist as separate logger names too.
        for _noisy_logger_name in (
            "opensearch",
            "opensearchpy",
            "elasticsearch",
            "botocore",
            "aiobotocore",
            "urllib3",
        ):
            logging.getLogger(_noisy_logger_name).setLevel(logging.WARNING)

        # stdout is always on, human-readable, colorized only against a real terminal —
        # interactive use and Docker/systemd all already capture stdout themselves (see
        # plans/log-revolution.md, "Output su stdout vs file"). the files below, when asked
        # for, are added on top: this mirrors how MutyLogger already behaves (stdout plus an
        # optional additional file/syslog sink, never file instead of stdout).
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(GlugConsoleFormatter(use_color=sys.stdout.isatty()))
        self._base_handlers: list[logging.Handler] = [stdout_handler]
        self._json_handlers: list[logging.Handler] = []
        self._log_dir: str | None = None

        if log_file:
            self._base_handlers.append(self._open_log_file(log_file))

        if log_dir:
            # raises on failure here, unlike a runtime reconfig: a bootstrap that asked
            # for files it cannot have must not silently continue without them
            self._json_handlers = self._open_json_files(log_dir)
            self._log_dir = log_dir

    @staticmethod
    def _open_log_file(log_file: str) -> logging.Handler:
        """
        opens the shared plain-text file mirroring stdout.

        Args:
            log_file (str): the file path; its directory is created if missing.

        Returns:
            logging.Handler: a WatchedFileHandler with the console formatter, colors off.

        Throws:
            OSError: if the file cannot be created or opened
        """
        try:
            parent = os.path.dirname(os.path.abspath(log_file))
            os.makedirs(parent, exist_ok=True)
            # append mode: every process opens it O_APPEND, so each record (one write())
            # lands whole at the end of the file instead of overwriting another process's
            handler = WatchedFileHandler(log_file, mode="a", encoding="utf-8")
        except OSError as ex:
            # stdout is still on, but this message must never depend on a handler
            print("glug: cannot open log file %r: %s" % (log_file, ex))
            raise
        handler.setFormatter(GlugConsoleFormatter(use_color=False))
        return handler

    def _open_json_files(self, log_dir: str) -> list[logging.Handler]:
        """
        opens this process's JSON-lines file and its exception-only companion.

        Args:
            log_dir (str): base directory; files go under log_dir/<hostname>/.

        Returns:
            list[logging.Handler]: the main and the exception-only rotating file handlers.

        Throws:
            OSError: if log_dir/<hostname> cannot be created, or a file cannot be opened
        """
        log_subdir = os.path.join(log_dir, self._hostname)
        base_name = (
            "gulp-worker-%d" % os.getpid()
            if self._process_role == "worker"
            else "gulp-main"
        )
        file_path = os.path.join(log_subdir, base_name + ".jsonl")
        try:
            os.makedirs(log_subdir, exist_ok=True)
            handler = RotatingFileHandler(
                filename=file_path,
                maxBytes=1024 * 1024 * self._max_log_size_mb,
                backupCount=self._max_kept_log,
            )
        except OSError as ex:
            # without a working file handler there is nowhere else for the error to go but
            # stdout, and it must never disappear silently (e.g. under systemd)
            print("glug: cannot open log file %r: %s" % (file_path, ex))
            raise
        handler.setFormatter(GlugJsonFormatter(self._process_role, self._hostname))

        # a second, exception-only JSON file alongside the main one — same full record
        # (nothing removed from the main file: it keeps carrying every exception too, for
        # whoever is already querying it), but collected separately so a postmortem doesn't
        # have to grep the whole log for the "exception" field. Filtered at the handler
        # level, not the logger level, so it never affects what reaches the main file/stdout.
        exc_file_path = os.path.join(log_subdir, base_name + "-exceptions.jsonl")
        try:
            exc_handler = RotatingFileHandler(
                filename=exc_file_path,
                maxBytes=1024 * 1024 * self._max_log_size_mb,
                backupCount=self._max_kept_log,
            )
        except OSError as ex:
            handler.close()
            print("glug: cannot open log file %r: %s" % (exc_file_path, ex))
            raise
        exc_handler.addFilter(_ExceptionOnlyFilter())
        exc_handler.setFormatter(GlugJsonFormatter(self._process_role, self._hostname))
        return [handler, exc_handler]

    def effective_level(self) -> int:
        """
        Returns:
            int: the level this process logs at: DEBUG while dev_mode is on, the configured one otherwise
        """
        return logging.DEBUG if self._dev_mode else self._level

    def _refresh_handlers(self) -> None:
        """
        pushes this instance's current handlers and level to its logger and to every routed
        stdlib logger. Lists are replaced, never mutated in place: another thread may be
        iterating the old one inside Logger.callHandlers().
        """
        handlers = self._base_handlers + self._json_handlers
        level = self.effective_level()
        self._logger.handlers = list(handlers)
        self._logger.setLevel(level)
        for routed_name in Glug._routed_logger_names:
            target = logging.getLogger(routed_name)
            target.handlers = list(handlers)
            target.setLevel(level)

        root = logging.getLogger()
        if self._logger is not root:
            # third-party loggers without handlers of their own propagate to the root
            # logger: give it this instance's handlers too, so their records reach every
            # sink (stdout, log_file, JSON) instead of whatever a bare placeholder instance
            # left there. Handlers Glug did not install (e.g. pytest's) are kept; the root
            # level stays WARNING, the stdlib default, so libraries never flood the log.
            root.handlers = [
                h for h in root.handlers if h not in Glug._root_handlers
            ] + list(handlers)
            root.setLevel(logging.WARNING)
        # a bare placeholder instance logs through the root logger itself: its handlers
        # are Glug's too, and must be dropped from it once a named instance takes over
        Glug._root_handlers = list(handlers)

    def _close_file_handlers(self) -> None:
        """closes every file handler of this instance (stdout is left alone)."""
        for handler in self._base_handlers[1:] + self._json_handlers:
            handler.close()

    @classmethod
    def apply_reconfig(
        cls, level: int = None, dev_mode: bool = None, log_dir=_UNCHANGED
    ) -> None:
        """
        applies a level/dev_mode/log_dir change to the existing singleton in place.
        used both at bootstrap and by the Redis reconfig listener (glug/reconfig.py).

        Args:
            level (int, optional): new stdlib logging level, or None to leave unchanged.
            dev_mode (bool, optional): new dev_mode flag, or None to leave unchanged.
            log_dir (str | None, optional): base directory for this process's JSON files, None to
                close them, omitted to leave them unchanged. If the files cannot be opened the error
                is logged and the JSON files stay off; level/dev_mode are applied regardless.

        Throws:
            RuntimeError: if called before get_instance() has created the singleton
        """
        if cls._instance is None:
            raise RuntimeError(
                "Glug.get_instance() must be called before apply_reconfig()"
            )
        instance = cls._instance
        if dev_mode is not None:
            instance._dev_mode = dev_mode
        if level is not None:
            instance._level = level

        old_json_handlers: list[logging.Handler] = []
        open_error: OSError | None = None
        if log_dir is not _UNCHANGED and log_dir != instance._log_dir:
            new_json_handlers: list[logging.Handler] = []
            if log_dir:
                try:
                    new_json_handlers = instance._open_json_files(log_dir)
                except OSError as ex:
                    open_error = ex
            if open_error is None:
                old_json_handlers = instance._json_handlers
                instance._json_handlers = new_json_handlers
                instance._log_dir = log_dir or None

        instance._refresh_handlers()
        # closed only once no logger references them anymore
        for handler in old_json_handlers:
            handler.close()

        if open_error is not None:
            instance.error(
                f"cannot open the JSON log files under {log_dir!r}, they stay off: {open_error}",
                hint="check that the directory exists and is writable by the gulp user",
            )

    @classmethod
    def route_stdlib_logger(cls, name: str) -> None:
        """
        route a third-party stdlib-named logger (e.g. "uvicorn", "uvicorn.error") through
        this Glug instance's own handlers/formatter, instead of whatever the library itself
        installed — so its output looks like every other gulp log line instead of its own,
        differently-formatted one. The logger keeps following later handler/level changes
        (apply_reconfig(), a reconfigured instance).

        must be called after this process's real Glug bootstrap (get_instance() with the
        real name/level/log_file), and, for uvicorn specifically, before uvicorn.run(...,
        log_config=None) — passing log_config=None is required, otherwise uvicorn's own
        logging.config.dictConfig() call inside run() overwrites these handlers again.

        Args:
            name (str): the stdlib logger name to take over (logging.getLogger(name))
        """
        instance = cls.get_instance()
        cls._routed_logger_names.add(name)
        logging.getLogger(name).propagate = False
        instance._refresh_handlers()

    def is_dev_mode(self) -> bool:
        """
        Returns:
            bool: whether dev_mode is currently active for this process — check this before building
                expensive payloads meant only for dev_mode diagnostics.
        """
        return self._dev_mode

    def _emit(
        self,
        level: int,
        msg: str,
        exc_info,
        hint: str | None,
        detail,
        context: dict,
    ) -> None:
        merged_context = current_context()
        merged_context.update(context)
        extra = {
            "process_role": self._process_role,
            "hostname": self._hostname,
            "thread_id": threading.get_native_id(),
            "async_task_name": _current_task_name(),
            "dev_mode": self._dev_mode,
            "context": merged_context,
            "hint": hint,
            # dev_mode-gated here, not at the call site: callers pass the structure they
            # already have on hand (a dict reference, an ORM object, ...) unconditionally,
            # and the actual str()/JSON serialization cost — the expensive part — only
            # happens inside the formatters, which only run when a handler emits the
            # record, so nothing is paid for this when dev_mode is off.
            "detail": detail if self._dev_mode else None,
        }
        self._logger.log(level, msg, exc_info=exc_info, extra=extra, stacklevel=_STACKLEVEL)

    def debug(
        self,
        msg: str,
        exc_info: bool | BaseException | None = None,
        hint: str | None = None,
        detail=None,
        **context,
    ) -> None:
        self._emit(logging.DEBUG, msg, exc_info, hint, detail, context)

    def info(
        self,
        msg: str,
        exc_info: bool | BaseException | None = None,
        hint: str | None = None,
        detail=None,
        **context,
    ) -> None:
        self._emit(logging.INFO, msg, exc_info, hint, detail, context)

    def warning(
        self,
        msg: str,
        exc_info: bool | BaseException | None = None,
        hint: str | None = None,
        detail=None,
        **context,
    ) -> None:
        self._emit(logging.WARNING, msg, exc_info, hint, detail, context)

    def error(
        self,
        msg: str,
        exc_info: bool | BaseException | None = None,
        hint: str | None = None,
        detail=None,
        **context,
    ) -> None:
        self._emit(logging.ERROR, msg, exc_info, hint, detail, context)

    def critical(
        self,
        msg: str,
        exc_info: bool | BaseException | None = None,
        hint: str | None = None,
        detail=None,
        **context,
    ) -> None:
        self._emit(logging.CRITICAL, msg, exc_info, hint, detail, context)

    def exception(
        self, msg: str, hint: str | None = None, detail=None, **context
    ) -> None:
        """shorthand for .error(msg, exc_info=True, hint=hint, detail=detail, **context), for use inside an `except` block"""
        self._emit(logging.ERROR, msg, True, hint, detail, context)
