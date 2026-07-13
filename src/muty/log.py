"""logging utilities"""

import asyncio
import inspect
import logging
import os
import stat
import sys
import threading
import traceback
from logging.handlers import RotatingFileHandler, SysLogHandler

import coloredlogs


class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record).replace("\\n", "\n")
        return message


class TruncateFormatter(logging.Formatter):
    """
    a formatter that truncates log messages to a specified maximum length

    Args:
        fmt (str): log format string
        max_length (int): maximum length of the log message

    Returns:
        None

    Throws:
        ValueError: if max_length is not positive
    """

    def __init__(self, fmt: str, max_length: int = 1000) -> None:
        # NOTE: call parent constructor
        super().__init__(fmt)
        # NOTE: store max_length for truncation
        if max_length <= 0:
            raise ValueError("max_length must be positive")
        self.max_length: int = max_length

    def format(self, record: logging.LogRecord) -> str:
        # NOTE: format the message using parent formatter
        message: str = super().format(record)
        # NOTE: truncate if message is too long
        if len(message) > self.max_length:
            # NOTE: add ellipsis to indicate truncation
            message = message[: self.max_length - 3] + "..."
        return message


class MutyLogger(logging.Logger):
    """
    a singleton logger class, represents a logger for the process
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    log_level = None
    logger_file_path = None

    @classmethod
    def get_instance(
        cls,
        name: str = None,
        logger_file_path: str = None,
        level: int = None,
        max_log_size_mb: int = 4,
        max_kept_log: int = 10,
        format_string: str = None,
        custom_field_styles: dict = None,
        use_multiline_formatter: bool = True,
        log_to_syslog: tuple[str, str] = None,
        reconfigure: bool = False,
        **kwargs,
    ) -> "MutyLogger":
        """
        get the singleton logger instance, if it does not exist, create it

        Args:
            name (str, optional): the name of the logger, mandatory on the first call. Defaults to None (uses the global Logger).
            logger_file_path (str, optional): path to the logger file. Defaults to None (log to stdout only), ignored if log_to_syslog is True.
            level (int, optional): the debug level. Defaults to logging.DEBUG.
            max_log_size_mb (int, optional): the maximum size of each log file in MB. Defaults to 4.
            max_kept_log (int, optional): the maximum number of log files to keep. Defaults to 10.
            format_string (str, optional): the log format string. Defaults to None (uses default).
            custom_field_styles (dict, optional): a dictionary of custom field styles for coloredlogs. Defaults to None (uses default).
            use_multiline_formatter (bool, optional): whether to use a multiline formatter or not. Defaults to False.
            log_to_syslog (tuple[str,str], optional): if set, logs to syslog at the specified address and facility.
                if (None, None) is passed, it auto-detects a local Unix syslog socket
                (e.g. /dev/log or /var/run/syslog) and falls back to ("127.0.0.1", 514)
                when no socket is available (common in Docker containers).
                cannot be used with logger_file_path.
            reconfigure (bool, optional): if True, reconfigure the logger with the new parameters even if it already exists. Defaults to False.
            **kwargs: additional parameters to pass to configure_logger()
        """
        if not hasattr(cls, "_instance") or reconfigure:
            cls._instance = logging.getLogger(name)

            # save the logger configuration as class variables
            cls.log_level = level
            cls.logger_file_path = logger_file_path

            cls._reconfigure(
                cls._instance,
                logger_file_path=logger_file_path,
                level=level,
                max_log_size_mb=max_log_size_mb,
                max_kept_log=max_kept_log,
                format_string=format_string,
                custom_field_styles=custom_field_styles,
                use_multiline_formatter=use_multiline_formatter,
                log_to_syslog=log_to_syslog,
            )
        return cls._instance

    @classmethod
    def _reconfigure(
        cls,
        l: logging.Logger,
        logger_file_path: str = None,
        level: int = None,
        max_log_size_mb: int = 4,
        max_kept_log: int = 10,
        format_string: str = None,
        custom_field_styles: dict = None,
        use_multiline_formatter: bool = True,
        log_to_syslog: tuple[str, str] = None,
        **kwargs,
    ) -> None:
        """
        reconfigure the process logger instance with the given parameters
        """
        if log_to_syslog and logger_file_path:
            raise ValueError("syslog and logger_file_path cannot be used together")

        level = level or logging.DEBUG
        l.setLevel(level)
        l.handlers = []

        # configure a default log format, including thread id
        log_format = (
            '%(asctime)s|%(name)s|%(task)s|%(levelname)s|%(process)d,%(thread_id)d|%(funcName)s|"%(pathname)s", line %(lineno)d|%(message)s'
            if format_string is None
            else format_string
        )
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(level)
        stdout_handler.setFormatter(
            logging.Formatter(log_format)
            if not use_multiline_formatter
            else MultiLineFormatter(log_format)
        )

        if _thread_id_filter not in stdout_handler.filters:
            stdout_handler.addFilter(_thread_id_filter)
        if _path_filter not in stdout_handler.filters:
            stdout_handler.addFilter(_path_filter)
        if _taskname_filter not in stdout_handler.filters:
            stdout_handler.addFilter(_taskname_filter)

        if stdout_handler not in l.handlers:
            l.handlers.append(stdout_handler)

        if log_to_syslog:
            # configure syslog handler
            address = _parse_syslog_address(log_to_syslog[0])
            facility = _parse_syslog_facility(log_to_syslog[1])

            try:
                syslog_handler = SysLogHandler(
                    address=address,
                    facility=facility,
                )
                syslog_handler.setLevel(level)
                formatter = TruncateFormatter(fmt=log_format, max_length=1000)
                syslog_handler.setFormatter(formatter)
                syslog_handler.addFilter(_thread_id_filter)
                syslog_handler.addFilter(_path_filter)
                syslog_handler.addFilter(_taskname_filter)
                if syslog_handler not in l.handlers:
                    l.handlers.append(syslog_handler)
                    l.debug(
                        "syslog handler configured for address: %s, facility: %s",
                        address,
                        facility,
                    )
            except Exception as ex:
                l.warning(
                    "syslog handler not configured (address=%s, facility=%s): %s",
                    address,
                    facility,
                    ex,
                )

        elif logger_file_path:
            rotating_handler = RotatingFileHandler(
                filename=logger_file_path,
                maxBytes=1024 * 1000 * max_log_size_mb,
                backupCount=max_kept_log,
            )
            # add a rotating file handler which breaks every 4mb, and keeps the last 10 log files
            rotating_handler.setLevel(level)
            rotating_handler.setFormatter(logging.Formatter(log_format))
            rotating_handler.addFilter(_thread_id_filter)
            rotating_handler.addFilter(_path_filter)
            rotating_handler.addFilter(_taskname_filter)
            if rotating_handler not in l.handlers:
                l.handlers.append(rotating_handler)

        if custom_field_styles:
            # use default
            custom_field_styles = coloredlogs.DEFAULT_FIELD_STYLES
            custom_field_styles.update(
                {
                    "pathname": {
                        "color": "blue",
                    },
                    "funcname": {
                        "color": "blue",
                    },
                    "lineno": {
                        "color": "yellow",
                    },
                    "thread_id": {
                        "color": "green",
                    },
                    "process": {
                        "color": "green",
                    },
                }
            )

        coloredlogs.install(
            fmt=log_format,
            level=level,
            logger=l,
            milliseconds=True,
            field_styles=custom_field_styles,
        )
        l.debug('logger "%s" configured!' % (l))


def _thread_id_filter(record: logging.LogRecord) -> bool:
    """
    adds native thread id to log record

    Args:
        record (logging.LogRecord): the log record to modify

    Returns:
        bool: always returns True to allow the record through
    """
    record.thread_id = threading.get_native_id()
    return True


def _is_unix_socket(path: str) -> bool:
    """Returns True if path exists and points to a Unix domain socket."""
    try:
        return stat.S_ISSOCK(os.stat(path).st_mode)
    except OSError:
        return False


def _default_syslog_address() -> str | tuple[str, int]:
    """
    Returns a platform-aware syslog endpoint.

    Prefer Unix sockets when available; fallback to UDP for Docker/minimal systems
    where the socket is not mounted.
    """
    if sys.platform == "darwin":
        socket_candidates = ["/var/run/syslog", "/dev/log"]
    else:
        socket_candidates = ["/dev/log", "/var/run/syslog", "/var/run/log"]

    for candidate in socket_candidates:
        if _is_unix_socket(candidate):
            return candidate

    # Docker containers often do not provide /dev/log.
    return ("127.0.0.1", 514)


def _parse_syslog_address(raw_address: str | tuple[str, str] | None) -> str | tuple[str, int]:
    """Parses user-provided syslog endpoint into a SysLogHandler-compatible address."""
    if raw_address is None:
        return _default_syslog_address()

    if isinstance(raw_address, tuple):
        if len(raw_address) != 2:
            raise ValueError("syslog address tuple must contain exactly host and port")
        return (str(raw_address[0]), int(raw_address[1]))

    address = str(raw_address).strip()
    if address.startswith("unix://"):
        return address[len("unix://") :]
    if address.startswith("/"):
        return address

    if address.startswith("[") and "]:" in address:
        host, port = address.rsplit("]:", 1)
        return (host[1:], int(port))

    if ":" in address:
        host, port = address.rsplit(":", 1)
        return (host, int(port))

    # hostname only: use default syslog UDP port
    return (address, 514)


def _parse_syslog_facility(raw_facility: str | int | None) -> int:
    """Parses numeric or named syslog facility value."""
    if raw_facility is None:
        return SysLogHandler.LOG_LOCAL0

    if isinstance(raw_facility, int):
        return raw_facility

    facility = str(raw_facility).strip()
    if facility.isdigit():
        return int(facility)

    normalized = facility.lower()
    if normalized.startswith("log_"):
        normalized = normalized[4:]

    if normalized in SysLogHandler.facility_names:
        return SysLogHandler.facility_names[normalized]

    raise ValueError(f"invalid syslog facility: {raw_facility}")


def _taskname_filter(record) -> bool:
    """
    adds current asyncio task name to log record

    Args:
        record (logging.LogRecord): the log record to modify

    Returns:
        bool: always returns True to allow the record through
    """
    # assign task name when running in an asyncio task
    try:
        task: asyncio.Task | None = asyncio.current_task()
        record.task = task.get_name() if task is not None else "-"
    except RuntimeError:
        # no running event loop in this thread
        record.task = ""
    except Exception:
        record.task = ""
        return True

    return True


def _path_filter(record) -> bool:
    """
    converts absolute path to relative path in log record

    Args:
        record (logging.LogRecord): the log record to modify

    Returns:
        bool: always returns True to allow the record through
    """
    # get relative path in log messages
    pathname = record.pathname
    tmp = None
    abs_sys_paths = map(os.path.abspath, sys.path)
    for path in sorted(abs_sys_paths, key=len, reverse=True):
        if not path.endswith(os.sep):
            path += os.sep
        if pathname.startswith(path):
            tmp = os.path.relpath(pathname, path)
            record.pathname = tmp
            break
    return True


def exception_to_string(ex: Exception, with_full_traceback: bool = False) -> str:
    """
    Converts an exception to a string representation.

    Args:
        ex (Exception): The exception to convert.
        with_full_traceback (bool): Whether to include the full traceback or not.

    Returns:
        str: The string representation of the exception, or None if ex is None.
    """
    if not ex:
        return None

    ex_t = ex.__class__.__name__
    tb = ex.__traceback__
    ex_str = ""

    if with_full_traceback:
        # get full traceback
        ex_str = "".join(traceback.format_exception(ex_t, ex, tb))
    else:
        if tb:
            # get only the frame causing the exception
            while tb.tb_next:
                tb = tb.tb_next
            ex_str = "".join(traceback.format_exception(ex_t, ex, tb))
        else:
            # get the caller frame
            caller_frame = inspect.currentframe().f_back
            filename = caller_frame.f_code.co_filename
            lineno = caller_frame.f_lineno
            fun = caller_frame.f_code.co_name
            ex_str = '%s, "%s", line %d, %s' % (fun, filename, lineno, ex)

    return ex_str
