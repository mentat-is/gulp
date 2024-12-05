import multiprocessing
import os
import pathlib
from copy import deepcopy
from importlib import resources as impresources

import aiofiles.ospath
import json5
import muty.file
import muty.os
from muty.log import MutyLogger

from gulp import mapping_files


class GulpConfig:
    """
    Gulp configuration singleton class.
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpConfig":
        """
        returns the singleton instance of the GulpConfig class, initializes it reading the configuration file if needed.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._config_file_path: str = None
            self._config: dict = None

            # read/initialize configuration and directories
            self._read_config()

    def config(self) -> dict:
        """
        Returns the configuration dictionary.
        """
        return self._config

    def path_config(self) -> str:
        """
        get the configuration file path (default: ~/.config/gulp/gulp_cfg.json)

        returns:
            str: the configuration file path
        """
        if self._config_file_path:
            # shortcut ...
            return self._config_file_path

        p = os.getenv("PATH_CONFIG")
        if p:
            # provided
            return p

        # ensure directory exists
        home_path = os.path.expanduser("~")
        gulp_config_dir = muty.file.safe_path_join(
            home_path, ".config/gulp", allow_relative=True
        )
        if not os.path.exists(gulp_config_dir):
            os.makedirs(gulp_config_dir, exist_ok=True)

        # return path
        p = muty.file.safe_path_join(gulp_config_dir, "gulp_cfg.json")
        return p

    def is_integration_test(self) -> bool:
        """
        Returns whether the integration test mode is enabled.
        """
        n = os.getenv("GULP_INTEGRATION_TEST", None)
        if n:
            MutyLogger.get_instance().warning(
                "!!!WARNING!!! GULP_INTEGRATION_TEST is set, debug features disabled!"
            )
            
        return n is not None

    @staticmethod
    async def check_copy_mappings_and_plugins_to_custom_directories():
        """
        checks configured custom directories for mapping files and plugins and copies the default directories to the custom directories if they are different.

        doing so, the user can have custom mapping files and plugins directories without touching the default ones.

        Returns:
            None
        """

        async def _copy_if_different(default_path, custom_path, description):
            if (
                custom_path
                and pathlib.Path(default_path).resolve()
                != pathlib.Path(custom_path).resolve()
            ):
                # we will use custom_path so, copy the whole directory there
                if not await aiofiles.ospath.exists(custom_path):
                    MutyLogger.get_instance().info(
                        f"copying {description} to custom directory: {custom_path}"
                    )
                    await muty.file.copy_dir_async(default_path, custom_path)
                else:
                    MutyLogger.get_instance().warning(
                        f"custom {description} directory already exists: {custom_path}"
                    )

        # defaults
        default_mapping_files_path = os.path.abspath(
            impresources.files("gulp.mapping_files")
        )
        default_plugins_path = os.path.abspath(impresources.files("gulp.plugins"))

        # custom folders
        custom_mapping_files_path = os.path.abspath(
            GulpConfig.get_instance().path_mapping_files() or ""
        )
        custom_plugins_path = os.path.abspath(
            GulpConfig.get_instance().path_plugins() or ""
        )

        MutyLogger.get_instance().debug(
            f"default_mapping_files_path: {default_mapping_files_path}"
        )
        MutyLogger.get_instance().debug(
            f"custom_mapping_files_path: {custom_mapping_files_path}"
        )
        MutyLogger.get_instance().debug(f"default_plugins_path: {default_plugins_path}")
        MutyLogger.get_instance().debug(f"custom_plugins_path: {custom_plugins_path}")

        await _copy_if_different(
            default_mapping_files_path, custom_mapping_files_path, "mapping files"
        )
        await _copy_if_different(default_plugins_path, custom_plugins_path, "plugins")

    def _read_config(self) -> None:
        """
        Reads the configuration file (default: ~/.config/gulp/gulp_cfg.json).

        if the configuration file does not exist, it will be created with default values from gulp_cfg_template.json.
        """
        config_file_path = self.path_config()

        # MutyLogger.get_instance().info("configuration path: %s" % (config_file_path))

        if not os.path.exists(config_file_path):
            # copy default configuration file
            src = os.path.abspath(
                muty.file.safe_path_join(os.getcwd(), "gulp_cfg_template.json")
            )
            muty.file.copy_file(src, config_file_path)
            os.chmod(config_file_path, 0o0600)
            MutyLogger.get_instance().warning(
                "no configuration file found, applying defaults from %s ..." % (src)
            )

        cfg_perms = oct(os.stat(config_file_path).st_mode & 0o777)
        if cfg_perms != oct(0o0600):
            MutyLogger.get_instance().warning(
                "careful, weak configuration file permissions %s != 0600" % cfg_perms
            )

        # read
        with open(config_file_path, "rb") as f:
            js = f.read()
            self._config = json5.loads(js)

    def set_config(self, config: dict):
        """
        Sets the configuration dictionary.

        Args:
            config (dict): the configuration dictionary

        Returns:
            None
        """
        self._config = config

    def get_config_override(self, d: dict) -> dict:
        """
        Returns a new configuration dictionary with the overrides applied.

        Args:
            d (dict): the overrides

        Returns:
            dict: the new configuration dictionary
        """
        dd = deepcopy(self._config)
        dd.update(d)
        return dd

    def bind_to(self) -> tuple[str, int]:
        """
        Returns the server bind address and port.

        Returns:
            tuple[str,int]: the bind address and port
        """
        p = os.getenv("BIND_TO")
        if p is None:
            p = self._config.get("bind_to", None)
            if p is None:
                MutyLogger.get_instance().debug("bind_to not set, using default!")
                p = "0.0.0.0:8080"

        MutyLogger.get_instance().debug("bind_to: %s" % (p))
        splitted = p.split(":")
        if len(splitted) != 2:
            raise ValueError("invalid bind_to format: %s" % (p))

        return (splitted[0], int(splitted[1]))

    def index_template_default_total_fields_limit(self) -> int:
        """
        Returns the default total fields limit for the index template (default=10000).
        """
        n = self._config.get("index_template_default_total_fields_limit", None)
        if not n:
            n = 10000
            MutyLogger.get_instance().debug("using default total fields limit")

        MutyLogger.get_instance().debug(
            "index_template_default_total_fields_limit: %d" % (n)
        )
        return n

    def index_template_default_refresh_interval(self) -> str:
        """
        Returns the default refresh interval for the index template (i.e. usually in seconds, 5s, 30s, ...).
        """
        n = self._config.get("index_template_default_refresh_interval", None)
        if n:
            n = "5s"
            MutyLogger.get_instance().debug(
                "using default refresh interval for index template"
            )

        MutyLogger.get_instance().debug(
            "index_template_default_refresh_interval: %s" % (n)
        )
        return n

    def ingestion_request_timeout(self) -> int:
        """
        Returns the ingestion request timeout in seconds.
        """
        n = self._config.get("ingestion_request_timeout", 60)
        return n

    def config_dir(self) -> str:
        """
        get the configuration directory (it also ensures it exists)

        returns:
            str: the configuration directory
        """
        p = os.path.dirname(self.path_config())
        return p

    def upload_tmp_dir(self) -> str:
        """
        get the upload temporary directory (it also ensures it exists)

        returns:
            str: the upload temporary directory
        """
        upload_dir = muty.file.safe_path_join(self.config_dir(), "upload_tmp")
        os.makedirs(upload_dir, exist_ok=True)
        return upload_dir

    def token_ttl(self) -> int:
        """
        Returns the number of seconds a non-admin token is valid for.
        """
        n = self._config.get("token_ttl", None)
        if not n:
            n = 604800
            MutyLogger.get_instance().debug(
                "using default number of seconds for token expiration=%d (%f days)"
                % (n, n / 86400)
            )
        return n

    def token_admin_ttl(self) -> int:
        """
        Returns the number of seconds an admin token is valid for.
        """
        n = self._config.get("token_admin_ttl", None)
        if not n:
            n = 600
            MutyLogger.get_instance().debug(
                "using default number of seconds for admin token expiration=%d (%f days)"
                % (n, n / 86400)
            )
        return n

    def documents_chunk_size(self) -> int:
        """
        size of documents chunk to send/request in one go
        """
        n = self._config.get("documents_chunk_size", None)
        if not n:
            n = 1000
            # MutyLogger.get_instance().debug("using default documents_chunk_size=%d" % (n))
        return n

    def ingestion_evt_failure_threshold(self) -> int:
        """
        Returns the number of events that can fail before the ingestion of the current file is marked as FAILED (0=never abort an ingestion even with multiple failures).
        """
        n = self._config.get("ingestion_evt_failure_threshold", 0)
        if not n:
            return 0
        return n

    def debug_collab(self) -> bool:
        """
        Returns whether to enable the collaborative API debug mode (prints SQL queries, etc...), default is False.
        """
        n = False
        if __debug__:
            n = self._config.get("debug_collab", False)
        return n

    def debug_no_token_expiration(self) -> bool:
        """
        Returns whether to disable token expiration.

        if GULP_INTEGRATION_TEST is set, this will be disabled (token expiration untouched).
        """
        n = False

        if __debug__:
            if self.is_integration_test():
                return False

            n = self._config.get("debug_no_token_expiration", False)
            if n:
                MutyLogger.get_instance().warning(
                    "!!!WARNING!!! debug_no_token_expiration is set to True !"
                )
        return n

    def stats_ttl(self) -> int:
        """
        Returns the number of seconds stats are kept.
        """
        n = self._config.get("stats_ttl", None)
        if n is None:
            n = 86400
            MutyLogger.get_instance().debug(
                "using default number of seconds for stats expiration=%d (%d days)"
                % (n, n / 86400)
            )
        return n

    def https_cert_password(self) -> str:
        """
        Returns the password for the HTTPS certificate of the gulp server.
        """
        n = self._config.get("https_cert_password", None)
        return n

    def enforce_https(self) -> bool:
        """
        Returns whether to enforce HTTPS.
        """
        n = self._config.get("https_enforce", False)
        return n

    def enforce_https_client_certs(self) -> bool:
        """
        Returns whether to enforce HTTPS client certificates.
        """
        n = self._config.get("https_enforce_client_certs", False)
        return n

    def debug_allow_any_token_as_admin(self) -> bool:
        """
        Returns whether to allow any token as admin in debug mode.

        this is disabled (False) if GULP_INTEGRATION_TEST is set.
        """
        n = False
        if __debug__:
            if self.is_integration_test():
                return False

            n = self._config.get("debug_allow_any_token_as_admin", False)
            if n:
                MutyLogger.get_instance().warning(
                    "!!!WARNING!!! debug_allow_any_token_as_admin is set to True !"
                )
        return n

    def debug_abort_on_opensearch_ingestion_error(self) -> bool:
        """
        Returns whether to abort ingestion of the current file when an error occurs during indexing on opensearch (=something's wrong in GulpDocument).

        this should be kept to True in production also...
        """
        n = True
        if __debug__:
            n = self._config.get("debug_abort_on_opensearch_ingestion_error", True)

        # MutyLogger.get_instance().warning('debug_abort_on_opensearch_ingestion_error is set to True.')
        return n

    def concurrency_max_tasks(self) -> int:
        """
        maximum number of concurrent coroutines per process which can be spawned by the API server

        default: 16

        @return the maximum number of tasks per process
        """
        n = self._config.get("concurrency_max_tasks", 0)
        if not n:
            n = 16
            MutyLogger.get_instance().debug(
                "using default number of tasks per process=%d" % (n)
            )
        return n

    def opensearch_client_cert_password(self) -> str:
        """
        Returns the password for the opensearch client certificate.
        """
        n = self._config.get("opensearch_client_cert_password", None)
        return n

    def opensearch_multiple_nodes(self) -> bool:
        """
        Returns whether to use multiple nodes for opensearch.
        """
        n = self._config.get("opensearch_multiple_nodes", False)
        return n

    def parallel_processes_max(self) -> int:
        """
        Returns the maximum number of processes to use for ingestion.
        if not set, the number of cores will be used.
        """
        n = self._config.get("parallel_processes_max", 0)
        if not n:
            n = multiprocessing.cpu_count()
            MutyLogger.get_instance().debug(
                "using default number of processes for ingestion (=number of cores=%d)."
                % (n)
            )
        return n

    def parallel_processes_respawn_after_tasks(self) -> int:
        """
        Returns the number of tasks to spawn before respawning a process.
        if not set, 0 will be used (no respawn).
        """
        n = self._config.get("parallel_processes_respawn_after_tasks", 0)
        if not n:
            return 0
        return n

    def debug_allow_insecure_passwords(self) -> bool:
        """
        Returns whether to disable password validation when creating users.
        """
        n = False
        if self.is_integration_test():
            return False

        if __debug__:
            n = self._config.get("debug_allow_insecure_passwords", False)

        MutyLogger.get_instance().warning(
            "!!!WARNING!!! debug_allow_insecure_passwords is set to True !"
        )
        return n

    def postgres_url(self) -> str:
        """
        Returns the postgres url (i.e. postgresql://user:password@localhost:5432)

        raises:
            Exception: If the postgres_url is not set in the configuration.
        """
        n = os.getenv("POSTGRES_URL", None)
        if not n:
            n = self._config.get("postgres_url", None)
            if not n:
                raise Exception(
                    "postgres_url not set (tried configuration and POSTGRES_URL environment_variable)."
                )

        return n

    def postgres_ssl(self) -> bool:
        """
        Returns whether to use SSL for postgres.
        if this is set, the certificates used to connect to postgres will be:

        - $PATH_CERTS/postgres-ca.pem
        - $PATH_CERTS/postgres.pem, $PATH_CERTS/postgres.key (client cert used if found)
        """
        n = self._config.get("postgres_ssl", False)
        return n

    def postgres_verify_certs(self) -> bool:
        """
        Returns whether to verify the certificates when connecting to postgres with SSL.

        default: False
        """
        n = self._config.get("postgres_verify_certs", False)
        return n

    def postgres_client_cert_password(self) -> str:
        """
        Returns the password for the postgres client certificate.
        """
        n = self._config.get("postgres_client_cert_password", None)
        return n

    def opensearch_url(self) -> str:
        """
        Returns the opensearch url

        if this is an https url, the certificates used to connect to opensearch will be:

        - $PATH_CERTS/opensearch-ca.pem
        - $PATH_CERTS/opensearch.pem, $PATH_CERTS/opensearch.key (client cert used if found)


        raises:
            Exception: If the opensearch_url is not set in the configuration.
        """
        n = os.getenv("OPENSEARCH_URL", None)
        if not n:
            n = self._config.get("opensearch_url", None)
            if not n:
                raise Exception(
                    "opensearch_url not set (tried configuration and OPENSEARCH_URL environment_variable)."
                )

        return n

    def opensearch_verify_certs(self) -> bool:
        """
        Returns whether to verify the certificates when connecting to opensearch with SSL.

        default: False

        """
        n = self._config.get("opensearch_verify_certs", False)
        return n

    def path_plugins(self, extension: bool = False) -> str:
        """
        returns the plugins path

        Args:
            extension (bool, optional): whether to return the extension plugins path. Defaults to False.

        Returns:
            str: the plugins path
        """
        default_path = impresources.files("gulp.plugins")
        # try env
        p = os.getenv("PATH_PLUGINS", None)
        if not p:
            # try configuration
            p = self._config.get("path_plugins", None)
            if not p:
                # use default
                p = default_path

        pp = os.path.expanduser(p)
        # MutyLogger.get_instance().debug("plugins path: %s" % (pp))
        if extension:
            return muty.file.safe_path_join(pp, "extension")
        return pp

    def path_index_template(self) -> str:
        """
        Returns the path of the opensearch index template file.
        """
        p = impresources.files("gulp.api.mapping.index_template")
        default_path = muty.file.safe_path_join(p, "template.json")

        # try env
        p = os.getenv("PATH_INDEX_TEMPLATE", None)
        if not p:
            # try configuration
            p = self._config.get("path_index_template", None)
            if not p:
                p = default_path

        pp = os.path.expanduser(p)
        MutyLogger.get_instance().debug("path_index_template: %s" % (pp))
        return p

    def path_mapping_files(self) -> str:
        """
        Returns the directory where mapping files for plugins are stored (default=None=GULPDIR/mapping_files).
        """
        # try env
        default_path = impresources.files("gulp.mapping_files")
        p = os.getenv("PATH_MAPPING_FILES", None)
        if not p:
            # try configuration
            p = self._config.get("path_mapping_files", None)
            if not p:
                p = default_path

        pp = os.path.expanduser(p)
        MutyLogger.get_instance().debug("mapping files path: %s" % (pp))
        return p

    def path_certs(self) -> str:
        """
        Returns the directory where the certificates are stored.

        - gulp-ca.pem, gulp.pem, gulp.key: the gulp server certificates
        - os-ca.pem, os.pem, os.key: the gulp's opensearch client certificates
        - postgres-ca.pem, postgres.pem, postgres.key: the gulp's postgres client certificates
        """
        # try env
        p = os.getenv("PATH_CERTS", None)
        if not p:
            # try configuration
            p = self._config.get("path_certs", None)
            if not p:
                MutyLogger.get_instance().debug('"path_certs" is not set !')
                return None

        pp = os.path.expanduser(p)
        # MutyLogger.get_instance().debug("certs directory: %s" % (pp))
        return pp

    def aggregation_max_buckets(self) -> int:
        """
        Returns the maximum number of buckets to return for aggregations (default: 999).

        this should not be touched unless you know what you are doing...
        """
        n = self._config.get("aggregation_max_buckets", None)
        if not n:
            # default
            return 999

        return n

    def ws_rate_limit_delay(self) -> float:
        """
        Returns the delay in seconds to wait before sending a message to a client.
        """
        n = self._config.get("ws_rate_limit_delay", 0.01)
        return n

    def plugin_cache_enabled(self) -> bool:
        """
        Returns whether to enable the plugin cache (default: True).
        """
        n = self._config.get("plugin_cache_enabled", True)
        return n

    @staticmethod
    def build_mapping_file_path(filename: str) -> str:
        """
        get path of a file in the gulp/mapping_files directory (or the overridden one from configuration/env)

        @return the full path of a file in the mapping_files directory
        """

        if not filename:
            return None

        configured_mappings_path = GulpConfig.get_instance().path_mapping_files()
        if configured_mappings_path is not None:
            # use provided
            p = muty.file.safe_path_join(configured_mappings_path, filename)
        else:
            # default, internal mapping_files directory with default mappings
            p = muty.file.safe_path_join(impresources.files(mapping_files), filename)
        return p
