import logging
import os
import ssl
from email.message import EmailMessage
from importlib import resources as impresources

import aiosmtplib
import muty.file
import muty.log
import muty.string
import muty.time
import muty.version

from gulp import mapping_files

class GulpLogger:
    """
    singleton logger class, represents a logger for the process
    """
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance
    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._logger = self.create()

    def debug(self, msg:object, *args:object) -> None:
        """
        log a debug message
        """
        self._logger.debug(msg, args)
    
    def warning(self, msg:object, *args:object) -> None:
        """
        log a warning message
        """
        self._logger.warning(msg, args)
    
    def error(self, msg:object, *args:object) -> None:
        """
        log an error message
        """
        self._logger.error(msg, args)

    def info(self, msg:object, *args:object) -> None:
        """
        log an info message
        """
        self._logger.info(msg, args)

    
    def exception(self, msg:object, *args:object) -> None:
        """
        log an exception message
        """
        self._logger.exception(msg, args)
    
    def critical(self, msg:object, *args:object) -> None:
        """
        log a critical message
        """
        self._logger.critical(msg, args)

    def get(self) -> logging.Logger:
        """
        get the logger instance
        
        Returns:
            logging.Logger: the logger
        """
        return self._logger
    
    def reconfigure(self, log_to_file: str = None, level: int = logging.DEBUG, prefix: str = None) -> logging.Logger:
        """
        reconfigure the logger instance with the given parameters and return it

        Args:
            log_to_file (str, optional): path to the log file. Defaults to None (log to stdout only)
            level (int, optional): the debug level. Defaults to logging.DEBUG.
            prefix (str, optional): prefix to add to the logger name (ignored if log_to_file is None). Defaults to None.

        Returns:
            logging.Logger: the reconfigured logger
        """
        self._logger = self.create(log_to_file, level, prefix)
        return self._logger

    def create(self,
        log_to_file: str = None,
        level: int = logging.DEBUG,
        prefix: str = None,
    ) -> logging.Logger:
        """
        create a new logger with the given parameters (do not touch the singleton logger)

        Args:
            log_to_file (str, optional): path to the log file. Defaults to None (log to stdout only)
            level (int, optional): the debug level. Defaults to logging.DEBUG.
            prefix (str, optional): prefix to add to the logger name (ignored if log_to_file is None). Defaults to None.
        Returns:
            logging.Logger: the new logger
        """

        n = "gulp"
        if log_to_file is not None:
            # if log_to_file is not None, build the filename
            d = os.path.dirname(log_to_file)
            filename = os.path.basename(log_to_file)
            if prefix is not None:
                # add prefix to filename
                filename = "%s-%s" % (prefix, filename)
                log_to_file = muty.file.safe_path_join(d, filename)

        l = muty.log.configure_logger(name=n, log_file=log_to_file, level=level, use_multiline_formatter=True)

        l.warning(
            "created logger %s, level=%d, file_path=%s"
            % (l, l.level, log_to_file)
        )

        # also reconfigure muty logger with the same level
        muty.log.internal_logger(
            log_to_file=log_to_file, level=level, force_reconfigure=True, use_multiline_formatter=True
        )
        return l

def logger() -> logging.Logger:
    """
    Returns the global logger.

    Returns:
        logging.Logger: The global logger.
    """
    return GulpLogger().get()


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
    GulpLogger().info(
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

