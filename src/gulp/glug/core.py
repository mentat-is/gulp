"""
Glug (Gulp Log Unified Gateway) — process-local structured logger.

Composes a plain stdlib `logging.Logger` (composition, not subclassing:
`MutyLogger` subclassed `logging.Logger` while making it impossible to
actually construct one via the normal protocol, see
plans/log-revolution.md). Two sinks, set up once at `get_instance()`/
`_setup()` time — stdout always, the file additionally when asked for:

- stdout: always attached, one short human-readable colorized line per
  record (`gulp.glug.console_formatter.GlugConsoleFormatter`) — interactive
  use and Docker/systemd all already capture stdout themselves, and a
  future audit consumer of the JSON file should not have to trade away
  console visibility to get it.
- `log_dir` given: additionally, a JSON-lines rotating file dedicated to
  this process (`gulp.glug.formatter.GlugJsonFormatter`) — one file per
  process, never shared, to avoid the concurrent-rotation corruption risk
  `RotatingFileHandler` has when several processes write to the same file.

See plans/log-revolution.md, "Output su stdout vs file", for the full
rationale.
"""

import asyncio
import logging
import os
import socket
import sys
import threading
from logging.handlers import RotatingFileHandler

from gulp.glug.console_formatter import GlugConsoleFormatter
from gulp.glug.context import current_context
from gulp.glug.formatter import GlugJsonFormatter

# stack frames between the application call site (e.g. a call to
# Glug.debug()) and the internal logging.Logger.log() call that finally
# records pathname/lineno/funcName: Glug.debug() -> Glug._emit() ->
# logging.Logger.log(). kept as a constant so it stays correct in one place
# if that call chain shape ever changes.
_STACKLEVEL = 3


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


class Glug:
    """
    a singleton, per-process structured JSON logger for gULP.
    """

    _instance: "Glug | None" = None

    def __init__(self) -> None:
        raise RuntimeError("call Glug.get_instance() instead")

    @classmethod
    def get_instance(
        cls,
        name: str = None,
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
            log_dir (str, optional): base directory for per-process JSON log files. Stdout (short,
                human-readable, colorized lines) is always on regardless of this parameter — see
                plans/log-revolution.md, "Output su stdout vs file". When log_dir is given, Glug
                additionally writes JSON to log_dir/<hostname>/gulp-main.log or
                gulp-worker-<pid>.log, on top of stdout, never instead of it.
            level (int, optional): stdlib logging level. Defaults to logging.INFO.
            dev_mode (bool, optional): forces the effective level to DEBUG and marks every record with
                dev_mode=true, so call sites can use is_dev_mode() to gate extra diagnostic content.
                Defaults to False.
            process_role (str, optional): "main" or "worker", selects the log filename in file mode. Defaults to "main".
            max_log_size_mb (int, optional): rotate the file after this size, when log_dir is given. Defaults to 4.
            max_kept_log (int, optional): number of rotated files to keep, when log_dir is given. Defaults to 10.
            reconfigure (bool, optional): rebuild the singleton (and its handlers) even if one already
                exists. Defaults to False.

        Returns:
            Glug: the process-local singleton instance, always able to log to stdout, plus the JSON file
                when log_dir is given — a bare Glug.get_instance() from any migrated call site works out
                of the box even before the process's real bootstrap has run.

        Throws:
            OSError: if log_dir/<hostname> cannot be created, or the log file cannot be opened (file mode only)
        """
        if cls._instance is None or reconfigure:
            instance = object.__new__(cls)
            instance._setup(
                name=name,
                log_dir=log_dir,
                level=level,
                dev_mode=dev_mode,
                process_role=process_role,
                max_log_size_mb=max_log_size_mb,
                max_kept_log=max_kept_log,
            )
            cls._instance = instance
        return cls._instance

    def _setup(
        self,
        name: str,
        log_dir: str,
        level: int,
        dev_mode: bool,
        process_role: str,
        max_log_size_mb: int,
        max_kept_log: int,
    ) -> None:
        """
        (re)configures this instance's underlying logger and handlers: stdout (console,
        human-readable) is always attached; the per-process JSON file is attached too,
        additionally, when log_dir is given — never instead of stdout.
        """
        self._hostname = socket.gethostname()
        self._process_role = process_role
        self._dev_mode = dev_mode
        self._level = level

        self._logger = logging.getLogger(name)
        self._logger.propagate = False
        self._logger.handlers = []
        self._logger.setLevel(logging.DEBUG if dev_mode else level)

        # stdout is always on, human-readable, colorized only against a real terminal —
        # interactive use and Docker/systemd all already capture stdout themselves (see
        # plans/log-revolution.md, "Output su stdout vs file"). log_dir, when given, adds
        # the JSON file on top: this mirrors how MutyLogger already behaves (stdout plus an
        # optional additional file/syslog sink, never file instead of stdout).
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(GlugConsoleFormatter(use_color=sys.stdout.isatty()))
        self._logger.addHandler(stdout_handler)

        if not log_dir:
            return

        log_subdir = os.path.join(log_dir, self._hostname)
        filename = (
            "gulp-worker-%d.log" % os.getpid()
            if process_role == "worker"
            else "gulp-main.log"
        )
        file_path = os.path.join(log_subdir, filename)
        try:
            os.makedirs(log_subdir, exist_ok=True)
            handler = RotatingFileHandler(
                filename=file_path,
                maxBytes=1024 * 1024 * max_log_size_mb,
                backupCount=max_kept_log,
            )
        except OSError as ex:
            # this is the one case glug still talks to stdout even in file mode: without a
            # working file handler there is nowhere else for the error to go, and it must
            # never disappear silently (e.g. under systemd)
            print("glug: cannot open log file %r: %s" % (file_path, ex))
            raise

        handler.setFormatter(GlugJsonFormatter())
        self._logger.addHandler(handler)

    @classmethod
    def apply_reconfig(cls, level: int = None, dev_mode: bool = None) -> None:
        """
        applies a level/dev_mode change to the existing singleton in place, without rebuilding handlers.
        used both at bootstrap and by the Redis reconfig listener (glug/reconfig.py).

        Args:
            level (int, optional): new stdlib logging level, or None to leave unchanged.
            dev_mode (bool, optional): new dev_mode flag, or None to leave unchanged.

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
        instance._logger.setLevel(logging.DEBUG if instance._dev_mode else instance._level)

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
