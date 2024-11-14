import logging
import os
import ssl
from email.message import EmailMessage

import aiosmtplib
import muty.file
import muty.log
import muty.string
import muty.time
import muty.version
from logging import Logger
class GulpLogger:
    """
    singleton logger class, represents a logger for the process
    """
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._logger = self.create()
            self.log_file_path = None            

    @classmethod
    def get_instance(cls) -> "GulpLogger":
        """
        returns the singleton instance
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    @classmethod
    def get_logger(cls) -> Logger:
        """
        returns the singleton's logger instance
        """
        return cls.get_instance()._logger

    def reconfigure(self, log_file_path: str = None, level: int = None, prefix: str = None) -> None:
        """
        reconfigure the process logger instance with the given parameters

        Args:
            log_file_path (str, optional): path to the log file. Defaults to None (log to stdout only)
            level (int, optional): the debug level. Defaults to logging.DEBUG.
            prefix (str, optional): prefix to add to the logger name (ignored if log_to_file is None). Defaults to None.
        """
        if level is None:
            level = logging.DEBUG

        self._logger = self.create(log_file_path=log_file_path, level=level, prefix=prefix)
        # self._logger.warning("reconfigured logger %s, level=%d, file_path=%s" % (self._logger, self._logger.level, log_file_path))        

    def create(self,
        log_file_path: str = None,
        level: int = None,
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
        self.log_file_path = log_file_path
        if level is None:
            level = logging.DEBUG

        n = "gulp"
        if log_file_path:
            # if log_to_file is not None, build the filename
            d = os.path.dirname(log_file_path)
            filename = os.path.basename(log_file_path)
            if prefix is not None:
                # add prefix to filename
                filename = "%s-%s" % (prefix, filename)
                log_file_path = muty.file.safe_path_join(d, filename)

        l = muty.log.configure_logger(name=n, log_file=log_file_path, level=level, use_multiline_formatter=True)

        # also reconfigure muty logger with the same level
        muty.log.internal_logger(
            log_to_file=log_file_path, level=level, force_reconfigure=True, use_multiline_formatter=True
        )
        return l

class GulpUtils:
    """
    utility class
    """
    @staticmethod
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
        GulpLogger.get_logger().info(
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


