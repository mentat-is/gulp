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

    def _get_int_config(self, key: str, default: int, minimum: int = 0) -> int:
        """
        Return an integer configuration value clamped to a minimum.
        """
        n = self._config.get(key, default)
        try:
            n = int(n)
        except Exception:
            n = default
        if n < minimum:
            n = minimum
        return n

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
            pkg_dir = os.path.dirname(os.path.abspath(__file__))  # src/gulp/
            src = os.path.join(pkg_dir, "gulp_cfg_template.json")
            print(
                "****** configuration file not found at %s, creating it from template %s ..."
                % (config_file_path, src)
            )
            muty.file.copy_file(src, config_file_path)
            os.chmod(config_file_path, 0o0600)
            print(
                "****** PID=%d, NO CONFIGURATION FILE FOUND, created default configuration %s from template ******"
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

    def ingestion_raw_update_stats_chunk_frequency(self) -> int:
        """
        Returns after how many chunks to update raw ingestion stats (default=10).
        """
        n = self._config.get("ingestion_raw_update_stats_chunk_frequency", 10)
        if not n:
            n = 10
            MutyLogger.get_instance().debug(
                "using default chunk frequency for raw ingestion stats update=%d" % (n)
            )
        return n

    def ingestion_retry_max(self) -> int:
        """
        Returns max bulk ingestion attempts on transient OpenSearch pressure.
        """
        return self._get_int_config("ingestion_retry_max", 3, minimum=1)

    def ingestion_retry_delay(self) -> float:
        """
        Returns delay in seconds between bulk ingestion attempts.
        """
        try:
            return max(0.0, float(self._config.get("ingestion_retry_delay", 1.0)))
        except (TypeError, ValueError):
            return 1.0

    def ws_ingest_stream_max_entries(self) -> int:
        """
        Returns maximum queued raw websocket ingest packets per websocket stream.

        Default: 1000. Set to 0 to disable the entry-count cap.
        """
        return self._config.get("ws_ingest_stream_max_entries", 1000)

    def ws_ingest_stream_max_buffered_bytes(self) -> int:
        """
        Returns maximum approximate buffered raw websocket ingest bytes per websocket.

        Default: 268435456 (256 MiB). Set to 0 to disable the byte cap.
        """
        return self._config.get("ws_ingest_stream_max_buffered_bytes", 268435456)

    def opensearch_request_timeout(self) -> int:
        """
        Returns the requests timeout for opensearch (default: 60 seconds, use 0 for no timeout).
        """
        n = self._config.get("opensearch_request_timeout", 60)
        return n

    def query_circuit_breaker_backoff_attempts(self) -> int:
        """
        Returns max query retries after OpenSearch circuit-breaker responses.
        """
        return self._get_int_config(
            "query_circuit_breaker_backoff_attempts", 5, minimum=0
        )

    def query_circuit_breaker_min_limit(self) -> int:
        """
        Returns the minimum query page size during circuit-breaker backoff.
        """
        return self._get_int_config("query_circuit_breaker_min_limit", 100, minimum=1)

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
        n = self._config.get("debug_collab", False)
        if n:
            MutyLogger.get_instance().warning(
                "!!!WARNING!!! COLLAB API DEBUG MODE ACTIVE!"
            )
        return n

    def debug_enrich_dry_run(self) -> bool:
        """
        Returns whether to enable dry run mode (= do not update data on storage) for enrich plugins (default: False).
        """
        n = self._config.get("debug_enrich_dry_run", False)
        if n:
            MutyLogger.get_instance().warning(
                "!!!WARNING!!! ENRICH DRY RUN MODE active, no real update will happen!"
            )
        return n

    def debug_no_token_expiration(self) -> bool:
        """
        Returns whether to disable token expiration.
        """
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
        Returns whether to delete pending stats on server shutdown for the current instance (default: True).

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
        """
        n = False
        if __debug__:
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

    def workers_concurrency_max(self) -> int:
        """
        maximum number of concurrent coroutines per process which can be spawned by the API server

        default: 16

        @return the maximum number of tasks executing concurrently in a process
        """
        return self._get_int_config("workers_concurrency_max", 16, minimum=1)

    def opensearch_index_replicas(self) -> int:
        """
        Number of replica shards for newly created OpenSearch index templates.
        """
        return self._get_int_config("opensearch_index_replicas", 0, minimum=0)

    def opensearch_pool_maxsize(self) -> int:
        """
        Per-process OpenSearch HTTP connection pool size.
        """
        return self._get_int_config("opensearch_pool_maxsize", 10, minimum=1)

    def concurrency_opensearch_num_nodes(self) -> int:
        """
        number of opensearch nodes used to determine concurrency max tasks when adaptive concurrency is enabled
        """
        return self._get_int_config("concurrency_opensearch_num_nodes", 1, minimum=1)

    def concurrency_postgres_num_nodes(self) -> int:
        """
        number of postgres nodes used to determine concurrency max tasks when adaptive concurrency is enabled
        """
        return self._get_int_config("concurrency_postgres_num_nodes", 1, minimum=1)

    def opensearch_client_cert_password(self) -> str:
        """
        Returns the password for the opensearch client certificate.
        """
        n = self._config.get("opensearch_client_cert_password", None)
        return n

    def opensearch_pool_maxsize(self) -> int:
        """
        Per-process OpenSearch HTTP connection pool size.
        """
        return self._get_int_config("opensearch_pool_maxsize", 10, minimum=1)

    def workers_max(self) -> int:
        """
        Returns the maximum number of processes to use for ingestion.
        if not set, the number of cores will be used.
        """
        n = self._config.get("workers_max", 0)
        if not n:
            n = multiprocessing.cpu_count()
            """MutyLogger.get_instance().debug(
                "using default number of processes for ingestion (=number of cores=%d)."
                % (n)
            )"""
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

    def postgres_pool_size(self) -> int:
        """
        Per-process postgres SQLAlchemy pool size.

        This is a hard cap also applied to adaptive pool sizing.
        """
        default = self.workers_concurrency_max()
        n = self._config.get("postgres_pool_size", default)
        try:
            n = int(n)
        except Exception:
            n = default
        if n < 1:
            n = 1
        return n

    def postgres_max_overflow(self) -> int:
        """
        Per-process postgres SQLAlchemy max_overflow.
        """
        n = self._config.get("postgres_max_overflow", 0)
        try:
            n = int(n)
        except Exception:
            n = 0
        if n < 0:
            n = 0
        return n

    def postgres_pool_timeout_sec(self) -> int:
        """
        SQLAlchemy PostgreSQL pool checkout timeout in seconds.
        """
        return self._get_int_config("postgres_pool_timeout_sec", 10, minimum=1)

    def postgres_pool_recycle_sec(self) -> int:
        """
        SQLAlchemy PostgreSQL pool recycle age in seconds.
        """
        return self._get_int_config("postgres_pool_recycle_sec", 3600, minimum=1)

    def postgres_lock_timeout_ms(self) -> int:
        """
        PostgreSQL lock timeout in milliseconds.
        """
        return self._get_int_config("postgres_lock_timeout_ms", 5000, minimum=0)

    def postgres_statement_timeout_ms(self) -> int:
        """
        PostgreSQL statement timeout in milliseconds. 0 disables the timeout.
        """
        return self._get_int_config("postgres_statement_timeout_ms", 0, minimum=0)

    def postgres_idle_in_transaction_session_timeout_ms(self) -> int:
        """
        PostgreSQL idle-in-transaction timeout in milliseconds.
        """
        return self._get_int_config(
            "postgres_idle_in_transaction_session_timeout_ms", 30000, minimum=0
        )

    def postgres_user_session_touch_interval_ms(self) -> int:
        """
        Minimum interval before refreshing a validated user session row.

        Set to 0 to preserve the previous write-on-every-check behavior.
        """
        return self._get_int_config(
            "postgres_user_session_touch_interval_ms", 60000, minimum=0
        )

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

    def s3_minio(self) -> bool:
        """
        Returns whether the S3-compatible storage is MinIO, which requires some specific handling (e.g. path style access).

        default: True
        """
        n = self._config.get("s3_minio", True)
        return n

    def s3_region(self) -> str:
        """
        Returns the region to use when connecting to S3-compatible storage.

        default: us-east-1
        """
        n = self._config.get("s3_region", "us-east-1")
        return n

    def s3_url(self) -> str:
        """
        Returns the s3-compatible storage URL

        Raises:
            Exception if neither s3_url or GULP_S3_URL is set
        """
        n = os.getenv("GULP_S3_URL", None)
        if not n:
            n = self._config.get("s3_url", None)
            if not n:
                raise Exception(
                    "s3_url not set (tried configuration and GULP_S3_URL environment_variable)."
                )

        return n

    def s3_verify_certs(self) -> bool:
        """
        Returns whether to verify the certificates when connecting to S3-compatible storage with SSL.

        default: False
        """
        n = self._config.get("s3_verify_certs", False)
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

    def ws_rate_limit_delay(self) -> float:
        """
        Returns the delay in seconds to wait in between sending messages to connected clients.
        """
        n = self._config.get("ws_rate_limit_delay", 0.01)
        return n

    def ws_queue_max_size(self) -> int:
        """
        Returns the maximum size of the websocket message queue per client.

        Default: 8192 messages.
        """
        n = self._config.get("ws_queue_max_size", 8192)
        return n

    def ws_enqueue_timeout(self) -> float:
        """
        Returns the timeout in seconds for blocking enqueue operations.
        If a message cannot be enqueued within this timeout, the client is considered unresponsive.

        Default: 5.0 seconds.
        """
        n = self._config.get("ws_enqueue_timeout", 5.0)
        return n

    def redis_task_lease_refresh_interval_ms(self) -> int:
        """
        Returns the interval in milliseconds used to refresh live task leases.

        Default: 20000 (20 seconds)
        """
        return self._config.get("redis_task_lease_refresh_interval_ms", 20000)

    def redis_task_autoclaim_idle_ms(self) -> int:
        """
        Returns idle time before stale pending Redis stream tasks are dropped.

        Default: 60000 (60 seconds)
        """
        return self._config.get("redis_task_autoclaim_idle_ms", 60000)

    def redis_task_execution_timeout_sec(self) -> int:
        """
        Returns the optional timeout for one queued task execution attempt.

        Default: 0 (disabled)
        """
        return self._config.get("redis_task_execution_timeout_sec", 0)

    def redis_stream_task_maxlen(self) -> int:
        """
        Returns the hard queued task backlog cap per task type.

        Default: 10000
        """
        return self._config.get("redis_stream_task_maxlen", 10000)

    def login_ip_rate_limit_max(self) -> int:
        """
        Returns the maximum login attempts allowed per IP in the configured window.

        Default: 0 (disabled)
        """
        return self._config.get("login_ip_rate_limit_max", 0)

    def login_ip_rate_limit_window_sec(self) -> int:
        """
        Returns the per-IP login rate limit window in seconds.

        Default: 60
        """
        return self._config.get("login_ip_rate_limit_window_sec", 60)

    def redis_max_connections(self) -> int:
        """
        Returns the maximum number of Redis connections per process for the
        redis-py async connection pool.

        Default: 0 (unlimited / redis-py default behavior).
        """
        n = self._config.get("redis_max_connections", 0)
        try:
            n = int(n)
        except (TypeError, ValueError):
            n = 0

        if n < 0:
            n = 0
        return n

    def plugin_disabled(self) -> list[str]:
        """
        Returns the list of disabled plugins (default: empty list).
        """
        disabled = self._config.get("plugin_disabled", [])
        if not isinstance(disabled, list):
            disabled = []
        return disabled

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
        # if it is an absolute path, return it if exists
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

    def prometheus_enabled(self) -> bool:
        """
        Returns whether Prometheus metrics endpoint is enabled (default: False).
        """
        return self._config.get("prometheus_enabled", False)

    def prometheus_endpoint(self) -> str:
        """
        Returns the Prometheus metrics endpoint path (default: /metrics).
        """
        return self._config.get("prometheus_endpoint", "/metrics")

    def prometheus_collect_interval(self) -> int:
        """
        Returns the interval in seconds for collecting gauge metrics (default: 15).
        """
        return self._config.get("prometheus_collect_interval", 15)
