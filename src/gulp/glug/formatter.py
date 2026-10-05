"""JSON line formatter for Glug log records, per the schema in plans/log-revolution.md."""

import logging
import os
import sys
import traceback

import orjson


def _relative_module_path(pathname: str) -> str:
    """
    converts an absolute source file path to a path relative to the closest sys.path entry

    Args:
        pathname (str): absolute path, as stdlib logging captures it (record.pathname)

    Returns:
        str: relative path (e.g. "gulp/plugin.py"), or the original pathname if no sys.path entry matches
    """
    abs_sys_paths = sorted((os.path.abspath(p) for p in sys.path), key=len, reverse=True)
    for path in abs_sys_paths:
        prefix = path if path.endswith(os.sep) else path + os.sep
        if pathname.startswith(prefix):
            return os.path.relpath(pathname, prefix)
    return pathname


def _dotted_module_name(module_path: str) -> str:
    """
    derives a dotted module name from a relative module path (e.g. "gulp/plugin.py" -> "gulp.plugin")

    Args:
        module_path (str): path as returned by _relative_module_path()

    Returns:
        str: dotted module name
    """
    without_ext = module_path[:-3] if module_path.endswith(".py") else module_path
    return without_ext.replace(os.sep, ".").replace("/", ".")


class GlugJsonFormatter(logging.Formatter):
    """
    formats a LogRecord as a single JSON line.

    expects `process_role`, `hostname`, `thread_id`, `async_task_name`,
    `dev_mode` and `context` to already be set on the record (Glug.core sets
    them via `extra=` on every call); `module_name`/`module_path` are instead
    derived here from `record.pathname`, which stdlib logging already
    populates correctly thanks to the `stacklevel` Glug passes internally.

    records that did not go through Glug (third-party loggers routed to its
    handlers, e.g. uvicorn) lack those fields: process_role/hostname then
    fall back to this process's own, given at construction, and the task
    name to the one stdlib logging records itself.
    """

    def __init__(self, process_role: str = None, hostname: str = None) -> None:
        """
        Args:
            process_role (str, optional): this process's role, for records without one. Defaults to None.
            hostname (str, optional): this host's name, for records without one. Defaults to None.
        """
        super().__init__()
        self._process_role = process_role
        self._hostname = hostname

    def format(self, record: logging.LogRecord) -> str:
        exception = None
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            exception = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value) if exc_value else None,
                "stack": "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
            }

        module_path = _relative_module_path(record.pathname)
        payload = {
            "timestamp_msec": int(record.created * 1000),
            "level": record.levelname,
            "process_role": getattr(record, "process_role", self._process_role),
            "hostname": getattr(record, "hostname", self._hostname),
            "pid": record.process,
            "thread_id": getattr(record, "thread_id", record.thread),
            "async_task_name": getattr(
                record, "async_task_name", getattr(record, "taskName", None)
            ),
            "module_name": _dotted_module_name(module_path),
            "module_path": module_path,
            "func": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
            "dev_mode": getattr(record, "dev_mode", False),
            "context": getattr(record, "context", {}),
            "hint": getattr(record, "hint", None),
            "detail": getattr(record, "detail", None),
            "exception": exception,
        }
        return orjson.dumps(payload, default=str).decode("utf-8")
