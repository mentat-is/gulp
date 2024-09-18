import logging
import os
import ssl
from email.message import EmailMessage
from importlib import resources as impresources
from queue import Queue

import aiosmtplib
import muty.file
import muty.log
import muty.string
import muty.time
import muty.version

from gulp import mapping_files

_logger: logging.Logger = None


def logger() -> logging.Logger:
    """
    Returns the global logger.

    Returns:
        logging.Logger: The global logger.
    """
    global _logger
    return _logger


async def send_mail(
    smtp_server: str,
    subject: str,
    content: str,
    sender: str,
    to: list[str],
    username: str = None,
    password: str = None,
    use_ssl: bool = False,
) -> None:
    """
    Sends an email using the specified SMTP server.

    Args:
        smtp_server (str): The SMTP server address as host:port.
        subject (str): The subject of the email.
        content (str): The content of the email.
        sender (str): The email address of the sender.
        to (list[str]): The email addresses of the recipients: to[0]=TO, optionally to[1...n]=CC.
        username (str, optional): The username for authentication. Defaults to None.
        password (str, optional): The password for authentication. Defaults to None.
        use_ssl (bool, optional): Whether to use SSL/TLS for the connection. Defaults to False.

    Returns:
        None: This function does not return anything.
    """

    splitted = smtp_server.split(":")
    server = splitted[0]
    port = int(splitted[1])
    to_email = to[0]
    cc_list = None
    if len(to) > 1:
        cc_list = to[1:]

    m = EmailMessage()
    logger().info(
        "sending mail using %s:%d, from %s to %s, cc=%s, subject=%s"
        % (server, port, sender, to_email, cc_list, subject)
    )
    m["From"] = sender
    m["To"] = to_email
    m["Subject"] = subject
    if cc_list is not None:
        m["cc"] = cc_list
    m.set_content(content)
    ssl_ctx = None
    if use_ssl is not None:
        ssl_ctx = ssl.create_default_context()
    await aiosmtplib.send(
        m,
        hostname=server,
        port=port,
        username=username,
        password=password,
        tls_context=ssl_ctx,
        validate_certs=False,
    )


def configure_logger(
    log_to_file: str = None,
    level: int = logging.DEBUG,
    force_reconfigure: bool = False,
    prefix: str = None,
) -> logging.Logger:
    """
    get the gulp logger. if already configured (and force_reconfigure is not set), returns the existing logger.

    Args:
        log_to_file (str, optional): path to the log file. Defaults to None (log to stdout only)
        level (int, optional): the debug level. Defaults to logging.DEBUG.
        force_reconfigure (bool, optional): if True, will reconfigure the logger also if it already exists. Defaults to True.
        prefix (str, optional): prefix to add to the logger name (ignored if log_to_file is None). Defaults to None.
    Returns:
        logging.Logger: configured logger
    """
    global _logger

    # if _logger is not None:
    # _logger.debug('using global logger')

    if not force_reconfigure and _logger is not None:
        return _logger

    n = "gulp"
    if log_to_file is not None:
        # if log_to_file is not None, build the filename
        d = os.path.dirname(log_to_file)
        filename = os.path.basename(log_to_file)
        if prefix is not None:
            # add prefix to filename
            filename = "%s-%s" % (prefix, filename)
            log_to_file = muty.file.safe_path_join(d, filename)

    _logger = muty.log.configure_logger(name=n, log_file=log_to_file, level=level)

    _logger.warning(
        "reconfigured logger %s, level=%d, file_path=%s"
        % (_logger, _logger.level, log_to_file)
    )

    # also reconfigure muty logger with the same level
    muty.log.internal_logger(
        log_to_file=log_to_file, level=level, force_reconfigure=force_reconfigure
    )
    return _logger


def init_modules(
    l: logging.Logger,
    logger_prefix: str = None,
    log_level: int = None,
    log_file_path: str = None,
    ws_queue: Queue = None,
) -> logging.Logger:
    """
    Initializes the gulp modules **in the current process**.

    @param l: the source logger, ignored if log_level is set
    @param log_level: if set, l is reinitialized with the given log level and l is ignored
    @param logger_prefix: if set, will prefix the reinitialized logger name with this string (ignored if log_level is not set)
    @param log_file_path: if set, will log to this file (ignored if log_level is not set)
    @param ws_queue: the proxy queue for websocket messages
    returns the logger (reinitialized if log_level is set)
    """
    global _logger

    if log_level is not None:
        # recreate logger
        _logger = configure_logger(
            log_to_file=log_file_path,
            level=log_level,
            force_reconfigure=True,
            prefix=logger_prefix,
        )
    else:
        # use provided
        _logger = l

    # initialize modules
    from gulp import config
    from gulp.api.rest import ws as ws_api
    config.init()
    ws_api.init(ws_queue, main_process=False)
    return _logger


def build_mapping_file_path(filename: str) -> str:
    """
    get path of a file in the gulp/mapping_files directory (or the overridden one from configuration/env)

    @return the full path of a file in the mapping_files directory
    """
    import gulp.config as config

    if filename is None:
        return None

    configured_mappings_path = config.path_mapping_files()
    if configured_mappings_path is not None:
        # use provided
        p = muty.file.safe_path_join(configured_mappings_path, filename)
    else:
        # default, internal mapping_files directory with default mappings
        p = muty.file.safe_path_join(impresources.files(mapping_files), filename)
    return p


def version_string() -> str:
    """
    returns the version string

    Returns:
        str: version string
    """
    return "gulp v%s (muty v%s)" % (
        muty.version.pkg_version("gulp"),
        muty.version.muty_version(),
    )


def ensure_req_id(req_id: str = None) -> str:
    """
    Ensures a request ID is not None, either returns a new one.

    Args:
        req_id (str, optional): The request ID. Defaults to None.

    Returns:
        str: The request ID.
    """
    if req_id is None:
        return muty.string.generate_unique()
    return req_id
