"""
Module for managing configuration in Gulp application.

This module provides the `GulpConfig` singleton class that handles loading, accessing,
and managing configuration settings for the Gulp application. It reads configuration from
a JSON file (by default at ~/.config/gulp/gulp_cfg.json) and provides methods to access
various configuration parameters needed across the application.

Configuration can be overridden by environment variables for certain settings.
If the configuration file doesn't exist, it will be created with default values.
"""

import multiprocessing
import os
from copy import deepcopy
from importlib import resources as impresources

import json5
import muty.file
from muty.log import MutyLogger


class GulpConfig:
    _instance: "GulpConfig" = None

    def __init__(self):
        self._config_file_path: str = None
        self._tmp_upload_dir: str = None
        self._ingest_local_dir = None
        self._logs_dir: str = None
        self._working_dir: str = None
        self._path_certs: str = None
        self._path_mapping_files_extra: str = None
        self._path_plugins_extra: str = None
        self._config: dict = None
        self._concurrency_num_tasks: int = None  # avoid to recalculate every time

        # read configuration on init
        self._read_config()

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpConfig":
        """
        get the singleton instance of the GulpConfig class, initializes it reading the configuration file if needed.

        Return:
            GulpConfig: the singleton
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def config(self) -> dict:
        """
        Returns the configuration dictionary.
        """
        return self._config

    def get(self, key: str, default=None):
        """
        Returns the value of a key in the configuration dictionary.

        Args:
            key (str): the key
            default: the default value

        Returns:
            any: the value of the key
        """
        return self._config.get(key, default)

    def is_integration_test(self) -> bool:
        """
        Returns whether the integration test mode is enabled.
        """
        n: str = os.getenv("GULP_INTEGRATION_TEST", None)
        if not n:
            n = "0"

        # if int(n) > 0:
        #     MutyLogger.get_instance().warning(
        #         "!!!WARNING!!! GULP_INTEGRATION_TEST is set, debug features disabled!"
        #     )

        return int(n) > 0

    def dump_config(self):
        """
        Dumps the configuration dictionary to the logger.
        """
        MutyLogger.get_instance().info(
            "working_dir=%s, configuration read from %s:\n%s"
            % (
                self._working_dir,
                self._config_file_path,
                json5.dumps(self._config, indent=2),
            )
        )

    def _read_config(self) -> None:
        """
        Reads the configuration file (default: ~/.config/gulp/gulp_cfg.json).

        if the configuration file does not exist, it will be created with default values from gulp_cfg_template.json.
        """
        config_file_path = self.path_config()

        # MutyLogger.get_instance().info("configuration path: %s" % (config_file_path))

        # note that we use prints here since this is called before the logger is initialized
        if not os.path.exists(config_file_path):
            # copy default configuration file
            src = os.path.abspath(
                muty.file.safe_path_join(os.getcwd(), "gulp_cfg_template.json")
            )
            muty.file.copy_file(src, config_file_path)
            os.chmod(config_file_path, 0o0600)
            print(
                "****** PID=%d, NO CONFIGURATION FILE FOUND, creating default configuration %s from template ******"
                % (os.getpid(), config_file_path)
            )

        # read
        with open(config_file_path, "rb") as f:
            js = f.read()
            self._config = json5.loads(js)

        print(
            "******  PID=%d, reading configuration file DONE: %s ******"
            % (os.getpid(), config_file_path)
        )

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
        # check env
        addr = os.getenv("GULP_BIND_TO_ADDR")
        port = os.getenv("GULP_BIND_TO_PORT")
        if addr and port:
            MutyLogger.get_instance().debug("bind_to (from env): %s:%s" % (addr, port))
            return (addr, int(port))

        # get from configuration "bind_to
        p = self._config.get("bind_to", None)
        if p is None:
            MutyLogger.get_instance().debug("bind_to not set, using default!")
            p = "0.0.0.0:8080"

        MutyLogger.get_instance().debug("bind_to: %s" % (p))
        splitted = p.split(":")
        if len(splitted) != 2:
            raise ValueError("invalid bind_to format: %s" % (p))

        return (splitted[0], int(splitted[1]))

    def index_dynamic_keyword_ignore_above(self) -> int:
        """
        Returns the default ignore_above value for dynamic keyword fields in the index template (default=not specified, use default).
        """
        return self._config.get("index_dynamic_keyword_ignore_above", None)

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
        if not n:
            n = "5s"
            MutyLogger.get_instance().debug(
                "using default refresh interval for index template"
            )

        MutyLogger.get_instance().debug(
            "index_template_default_refresh_interval: %s" % (n)
        )
        return n

    def ingestion_allow_unmapped_fields(self) -> bool:
        """
        Returns whether to allow unmapped fields during ingestion (default: True).
        """
        n = self._config.get("ingestion_allow_unmapped_fields", True)
        return n
    

    def ingestion_retry_max(self) -> int:
        """
        Returns the maximum number of retries for ingestion.
        """
        n = self._config.get("ingestion_retry_max", 3)
        if not n:
            n = 3
            MutyLogger.get_instance().debug(
                "using default number of retries for ingestion=%d" % (n)
            )
        return n

    def ingestion_retry_delay(self) -> int:
        """
        Returns the delay in seconds between ingestion retries.
        """
        n = self._config.get("ingestion_retry_delay", 1)
        if not n:
            n = 1
            MutyLogger.get_instance().debug(
                "using default delay between ingestion retries=%d" % (n)
            )
        return n

    def opensearch_request_timeout(self) -> int:
        """
        Returns the requests timeout for opensearch (default: 60 seconds, use 0 for no timeout).
        """
        n = self._config.get("opensearch_request_timeout", 60)
        return n

    def token_expiration_time(self, is_admin: bool) -> int:
        """
        Returns the expiration time for the token, for admin or not

        Args:
            is_admin (bool): Whether to return the admin token expiration time or normal user token expiration time.

        Returns:
            int: The expiration time in milliseconds from the unix epoch.
        """
        # get expiration time
        if GulpConfig.get_instance().debug_no_token_expiration():
            time_expire = 0
        else:
            # setup session expiration
            if is_admin:
                time_expire = (
                    muty.time.now_msec()
                    + GulpConfig.get_instance()._token_admin_ttl() * 1000
                )
            else:
                time_expire = (
                    muty.time.now_msec() + GulpConfig.get_instance()._token_ttl() * 1000
                )
        return time_expire

    def _token_ttl(self) -> int:
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

    def _token_admin_ttl(self) -> int:
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

    def ingestion_documents_chunk_size(self) -> int:
        """
        size of documents chunk (during ingestion) to send/request in one go
        """
        n = self._config.get("ingestion_documents_chunk_size", None)
        if not n:
            n = 1000
            # MutyLogger.get_instance().debug("using default ingestion_documents_chunk_size=%d" % (n))
        return n

    def ingestion_documents_adaptive_chunk_size(self) -> bool:
        """
        whether to enable adaptive documents chunk size during ingestion
        """
        n = self._config.get("ingestion_documents_adaptive_chunk_size", True)
        return n

    def query_circuit_breaker_disable_highlights(self) -> bool:
        """
        Returns whether to disable highlights when query circuit breaker is triggered.

        Default: True
        """
        n = self._config.get("query_circuit_breaker_disables_highlights", True)
        return n
    
    def query_circuit_breaker_backoff_attempts(self) -> int:
        """
        Returns how many times query execution should retry with backoff when OpenSearch circuit breakers trip.

        Default: 3 attempts.
        """
        n = self._config.get("query_circuit_breaker_backoff_attempts", 5)
        try:
            n = int(n)
        except (TypeError, ValueError):
            n = 5
        if n <= 0:
            n = 5
            MutyLogger.get_instance().warning("invalid query_circuit_breaker_backoff_attempts, set to default=5")

        return n

    def query_circuit_breaker_min_limit(self) -> int:
        """
        Returns the lowest per-chunk document limit allowed during circuit breaker backoff.

        Default: 100 documents.
        """
        n = self._config.get("query_circuit_breaker_min_limit", 100)
        try:
            n = int(n)
        except (TypeError, ValueError):
            n = 100
        if n < 1:
            n = 1
        return n

    def ingestion_evt_failure_threshold(self) -> int:
        """
        Returns the number of events that can fail before the ingestion of the current file is marked as FAILED (0=never abort an ingestion even with multiple failures).
        """
        n = self._config.get("ingestion_evt_failure_threshold", 0)
        if not n:
            return 0
        return n

    def ws_adaptive_rate_limit(self) -> bool:
        """
        Returns whether to enable adaptive rate limiting for websockets (default: False).
        """
        n = GulpConfig.get_instance()._config.get("ws_adaptive_rate_limit", True)
        return n

    def debug_collab(self) -> bool:
        """
        Returns whether to enable the collaborative API debug mode (prints SQL queries, etc...), default is False.
        """
        n = False
        if __debug__:
            n = self._config.get("debug_collab", False)
        return n

    def debug_ignore_missing_ws(self) -> bool:
        """
        Returns whether to ignore missing websocket connection (default: False).
        """
        force_check_ws: str = os.getenv("GULP_FORCE_CHECK_WS", "0")

        if int(force_check_ws) == 0:
            # on integration test, skip
            if self.is_integration_test():
                MutyLogger.get_instance().warning("IGNORING MISSING WEBSOCKET CONNECTIONS FOR INTEGRATION TESTS!")
                return True
        n = self._config.get("debug_ignore_missing_ws", False)
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

    def stats_delete_pending_on_shutdown(self) -> bool:
        """
        Returns whether to delete pending stats on server shutdown (default: True).

        this is useful to avoid having pending stats from previous runs.
        """
        n = GulpConfig.get_instance()._config.get(
            "stats_delete_pending_on_shutdown", True
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

    def concurrency_tasks_cap_per_process(self) -> int:
        """
        maximum number of concurrent coroutines per process which can be spawned by the API server when adaptive concurrency is enabled

        default: 64

        @return the maximum number of tasks executing concurrently in a process
        """
        n = self._config.get("concurrency_tasks_cap_per_process", 64)
        return n

    def concurrency_num_tasks(self) -> int:
        """
        maximum number of concurrent coroutines per process which can be spawned by the API server

        default: 16

        @return the maximum number of tasks executing concurrently in a process
        """
        if self._concurrency_num_tasks is not None:
            # already calculated
            return self._concurrency_num_tasks

        adaptive: bool = self.concurrency_adaptive_num_tasks()
        num_tasks: int = self._config.get("concurrency_num_tasks", 0)
        if not adaptive:
            if num_tasks < 2:
                MutyLogger.get_instance().warning(
                    "invalid concurrency_num_tasks=%d, set to default=16", num_tasks)
                num_tasks = 16
            return num_tasks

        # adaptive, calculate
        opensearch_num_nodes: int = self.concurrency_opensearch_num_nodes()
        postgres_num_nodes: int = self.concurrency_postgres_num_nodes()
        base_num_tasks: int = num_tasks if num_tasks > 0 else 16
        cap_per_process: int = self.concurrency_tasks_cap_per_process()

        # scale linearly with OS nodes and clamp to cap_per_process
        scaled = (
            base_num_tasks * max(1, opensearch_num_nodes) * max(1, postgres_num_nodes)
        )
        self._concurrency_num_tasks = max(8, min(cap_per_process, scaled))
        MutyLogger.get_instance().debug(
            "calculated adaptive concurrency_num_tasks=%d (base=%d, os_nodes=%d, pg_nodes=%d, cap_per_process=%d)"
            % (
                self._concurrency_num_tasks,
                base_num_tasks,
                opensearch_num_nodes,
                postgres_num_nodes,
                cap_per_process,
            )
        )
        return self._concurrency_num_tasks

    def concurrency_adaptive_num_tasks(self) -> bool:
        """
        whether to enable adaptive concurrency max tasks per process
        """
        n = self._config.get("concurrency_adaptive_num_tasks", False)
        return n

    def concurrency_opensearch_num_nodes(self) -> int:
        """
        number of opensearch nodes used to determine concurrency max tasks when adaptive concurrency is enabled
        """
        n = self._config.get("concurrency_opensearch_num_nodes", 1)
        return n

    def concurrency_postgres_num_nodes(self) -> int:
        """
        number of postgres nodes used to determine concurrency max tasks when adaptive concurrency is enabled
        """
        n = self._config.get("concurrency_postgres_num_nodes", 1)
        return n

    def opensearch_client_cert_password(self) -> str:
        """
        Returns the password for the opensearch client certificate.
        """
        n = self._config.get("opensearch_client_cert_password", None)
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
        this can be set to -1 to never respawn processes, every other value < 100 will be set to 100.
        """
        n = self._config.get("parallel_processes_respawn_after_tasks", 100)
        if n == -1:
            # -1 means never respawn
            MutyLogger.get_instance().warning(
                "parallel_processes_respawn_after_tasks is set to -1, never respawning processes."
            )
            return 0

        if n < 100:
            MutyLogger.get_instance().warning(
                "parallel_processes_respawn_after_tasks n=%d too low, set to 100 (minimum)"
                % (n)
            )
            n = 100

        return n

    def debug_allow_insecure_passwords(self) -> bool:
        """
        Returns whether to disable password validation when creating users.
        """
        n = False
        # if self.is_integration_test():
        #     return False

        if __debug__:
            n = self._config.get("debug_allow_insecure_passwords", False)

        if n:
            MutyLogger.get_instance().warning(
                "!!!WARNING!!! debug_allow_insecure_passwords is set to True !"
            )
        return n

    def postgres_adaptive_pool_size(self) -> bool:
        """
        whether to adapt the postgres connection pool size to concurrency (max tasks/num workers)
        """
        n = self._config.get("postgres_adaptive_pool_size", True)
        return n

    def postgres_url(self) -> str:
        """
        Returns the postgres url (i.e. postgresql://user:password@localhost:5432)

        raises:
            Exception: If the postgres_url is not set in the configuration.
        """
        n = os.getenv("GULP_POSTGRES_URL", None)
        if not n:
            n = self._config.get("postgres_url", None)
            if not n:
                raise Exception(
                    "postgres_url not set (tried configuration and GULP_POSTGRES_URL environment_variable)."
                )

        return n

    def postgres_ssl(self) -> bool:
        """
        Returns whether to use SSL for postgres.
        if this is set, the certificates used to connect to postgres will be:

        - $GULP_WORKING_DIR/certs/postgres-ca.pem
        - $GULP_WORKING_DIR/certs/postgres.pem, $PATH_CERTS/postgres.key (client cert used if found)
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

    def redis_url(self) -> str:
        """
        Returns the redis URL

        Raises:
            Exception if neither redis_url or GULP_REDIS_URL is set
        """
        n = os.getenv("GULP_REDIS_URL", None)
        if not n:
            n = self._config.get("redis_url", None)
            if not n:
                raise Exception(
                    "redis_url not set (tried configuration and GULP_REDIS_URL environment_variable)."
                )

        return n

    def instance_roles(self) -> list[str]:
        """
        Returns the list of roles this gulp instance should serve.

        default supported roles are: "query", "ingest", "rebase"

        """
        roles = self._config.get("instance_roles", None)
        if not roles:
            return []
        return roles
    
    def opensearch_url(self) -> str:
        """
        Returns the opensearch url

        if this is an https url, the certificates used to connect to opensearch will be:

        - $PATH_CERTS/opensearch-ca.pem
        - $PATH_CERTS/opensearch.pem, $PATH_CERTS/opensearch.key (client cert used if found)


        raises:
            Exception: If the opensearch_url is not set in the configuration.
        """
        n = os.getenv("GULP_OPENSEARCH_URL", None)
        if not n:
            n = self._config.get("opensearch_url", None)
            if not n:
                raise Exception(
                    "opensearch_url not set (tried configuration and GULP_OPENSEARCH_URL environment_variable)."
                )

        return n

    def opensearch_verify_certs(self) -> bool:
        """
        Returns whether to verify the certificates when connecting to opensearch with SSL.

        default: False

        """
        n = self._config.get("opensearch_verify_certs", False)
        return n

    def path_working_dir(self) -> str:
        """
        Returns the path to the gulp working directory

        this is used to hold the configuration, temporary directory, custom plugins, mapping files, certs, logs, etc.

        can be overridden with GULP_WORKING_DIR environment variable.

        default: ~/.config/gulp
        """
        if self._working_dir:
            # shortcut ...
            return self._working_dir

        p = os.getenv("GULP_WORKING_DIR", None)
        if not p:
            # env var not set, create default or ensure it already exists
            home_path = os.path.expanduser("~")
            p = muty.file.safe_path_join(home_path, ".config/gulp", allow_relative=True)
            print(
                "****** PID=%d, GULP_WORKING_DIR not set, using default working directory in $HOME: %s"
                % (os.getpid(), p)
            )

        self._working_dir = p

        # ensure all directories exists
        os.makedirs(p, exist_ok=True)
        os.makedirs(self.path_certs(), exist_ok=True)
        os.makedirs(self.path_mapping_files_extra(), exist_ok=True)
        os.makedirs(self.path_plugins_extra(), exist_ok=True)
        os.makedirs(os.path.join(self.path_plugins_extra(), "extension"), exist_ok=True)
        os.makedirs(os.path.join(self.path_plugins_extra(), "ui"), exist_ok=True)
        os.makedirs(self.path_logs(), exist_ok=True)
        os.makedirs(self.path_tmp_upload(), exist_ok=True)
        os.makedirs(self.path_ingest_local(), exist_ok=True)
        # print paths
        print(
            "****** PID=%d, working_dir=%s, certs=%s, mapping_files_extra=%s, plugins_extra=%s, tmp_upload=%s, ingest_local=%s, logs=%s ******"
            % (
                os.getpid(),
                self._working_dir,
                self.path_certs(),
                self.path_mapping_files_extra(),
                self.path_plugins_extra(),
                self.path_tmp_upload(),
                self.path_ingest_local(),
                self.path_logs(),
            )
        )
        return p

    def path_config(self) -> str:
        """
        get the configuration file path (default: ~/.config/gulp/gulp_cfg.json)

        returns:
            str: the configuration file path
        """
        if self._config_file_path:
            # shortcut ...
            return self._config_file_path

        p = self.path_working_dir()
        p = muty.file.safe_path_join(p, "gulp_cfg.json")
        self._config_file_path = p
        return p

    def path_ingest_local(self) -> str:
        """
        get the local ingestion path, default: ~/.config/gulp/ingest_local

        Returns:
            str: the local path for ingestion
        """
        if self._ingest_local_dir:
            # shortcut ...
            return self._ingest_local_dir

        p = self.path_working_dir()
        p = os.path.abspath(os.path.join(p, "ingest_local"))
        self._ingest_local_dir = p
        return p

    def path_logs(self) -> str:
        """
        get the logs path, default: ~/.config/gulp/logs

        Returns:
            str: the local path for ingestion
        """
        if self._logs_dir:
            # shortcut ...
            return self._logs_dir

        p = self.path_working_dir()
        p = os.path.abspath(os.path.join(p, "logs"))
        self._logs_dir = p
        return p

    def path_tmp_upload(self) -> str:
        """
        get the upload temporary directory, default: ~/.config/gulp/tmp_upload

        returns:
            str: the upload temporary directory
        """
        if self._tmp_upload_dir:
            # shortcut ...
            return self._tmp_upload_dir

        p = self.path_working_dir()
        p = muty.file.safe_path_join(p, "tmp_upload")
        return p

    def path_plugins_default(self) -> str:
        """
        Returns the built-in (default) plugins path.
        """
        return str(impresources.files("gulp.plugins"))

    def path_plugins_extra(self) -> str:
        """
        Returns the extra plugins path.
        """
        if self._path_plugins_extra:
            # shortcut ...
            return self._path_plugins_extra

        p = self.path_working_dir()
        p = os.path.join(p, "plugins")
        self._path_plugins_extra = p
        return p

    def path_mapping_files_default(self) -> str:
        """
        Returns the built-in (default) path of the mapping files.
        """
        return str(impresources.files("gulp.mapping_files"))

    def path_mapping_files_extra(self) -> str:
        """
        Returns the extra path of the mapping files.
        """
        if self._path_mapping_files_extra:
            # shortcut ...
            return self._path_mapping_files_extra

        p = self.path_working_dir()
        p = os.path.join(p, "mapping_files")
        self._path_mapping_files_extra = p
        return p

    def path_certs(self) -> str:
        """
        Returns path to the certificates directory, default: ~/.config/gulp/certs
        """
        if self._path_certs:
            # shortcut ...
            return self._path_certs

        p = self.path_working_dir()
        p = os.path.join(p, "certs")
        self._path_certs = p
        return p

    def preview_mode_num_docs(self) -> int:
        """
        Returns the number of documents to show in preview mode.
        """
        n = self._config.get("preview_mode_num_docs", None)
        if not n:
            if self.is_integration_test():
                # integration test mode, use a small number of documents
                n = 10
            else:
                n = 100
            # MutyLogger.get_instance().debug("using default number of documents for preview mode=%d" % (n))
        return n

    def query_history_max_size(self) -> int:
        """
        Returns the maximum size of the query history (default: 20).

        this is the maximum number of queries to keep as history for each user.

        Returns:
            int: the maximum size of the query history
        """
        n = self._config.get("query_history_max_size", None)
        if not n:
            # default
            return 20

        if n > 50:
            MutyLogger.get_instance().warning(
                "!!!WARNING!!! query_history_max_size value too big (%d), set to max(50)!"
                % (n)
            )
            n = 50

        return n

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

    def ws_adaptive_rate_limit(self) -> bool:
        """
        Returns whether to enable adaptive rate limiting for websockets (default: False).
        """
        n = GulpConfig.get_instance()._config.get("ws_adaptive_rate_limit", True)
        return n


    def ws_adaptive_rate_limit_delay(self) -> bool:
        """
        Returns whether to enable adaptive rate limiting for websockets (default: False).
        """
        n = GulpConfig.get_instance()._config.get("ws_adaptive_rate_limit_delay", True)
        return n

    def ws_rate_limit_delay(self) -> float:
        """
        Returns the delay in seconds to wait in between sending messages to connected clients.
        """
        n = self._config.get("ws_rate_limit_delay", 0.01)
        return n
    
    def ws_queue_max_size(self) -> int:
        """
        Returns the maximum size of the websocket message queue per client.

        Default: 1024 messages.
        """
        n = self._config.get("ws_queue_max_size", 1024)
        return n
    
    def ws_server_cache_ttl(self) -> float:
        """
        Returns the TTL in seconds for cached websocket ownership lookups.

        Default: 1 second.
        """
        ttl = self._config.get("ws_server_cache_ttl", 1.0)
        try:
            ttl = float(ttl)
        except (TypeError, ValueError):
            ttl = 1.0

        if ttl <= 0:
            ttl = 1.0
        return ttl    
    
    def redis_compression_enabled(self) -> bool:
        """
        Returns whether Redis payload compression is enabled for large messages.

        Default: False.
        """
        n = self._config.get("redis_compression_enabled", False)
        return n

    def plugin_cache_enabled(self) -> bool:
        """
        Returns whether to enable the plugin cache (default: True).
        """
        n = self._config.get("plugin_cache_enabled", True)
        return n

    def build_mapping_file_path(self, filename: str) -> str:
        """
        get mapping file path, giving precedence to the extra path if set and the file exists

        @return the full path of a file in the mapping_files directory
        """

        if not filename:
            return None

        extra_path = GulpConfig.get_instance().path_mapping_files_extra()
        if extra_path:
            p = muty.file.safe_path_join(extra_path, filename)
            if os.path.exists(p):
                # prefer path in extra path if exists
                return p

        # default path
        default_path = GulpConfig.get_instance().path_mapping_files_default()
        p = muty.file.safe_path_join(default_path, filename)
        return p
