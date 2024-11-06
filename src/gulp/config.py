import multiprocessing
import os
import pathlib
from importlib import resources as impresources

import aiofiles.ospath
import json5
import muty.file
import muty.os

from gulp.defs import GulpPluginType
from gulp.utils import logger

_config: dict = None
_config_file_path = None

# TODO: turn to a singleton class
# TODO: simplify (i.e. path_config, read_or_init_configuration, initialize_custom_directories ...)

def init():
    """
    initialize the configuration module

    """
    global _config

    if _config is None:
        # only if not initialized yet
        _config = _read_or_init_configuration()
    return _config


async def initialize_custom_directories():
    """
    Initializes custom directories for mapping files and plugins.

    This function checks if the default mapping files and plugins directories
    are different from the custom directories specified by the user. If they
    are different, it copies the entire directory from the default location
    to the custom location.

    This way, the user can have custom mapping files and plugins directories without touching the default ones (which may not be writable)

    Returns:
        None
    """
    # base mappings directory
    default_mapping_files_path = os.path.abspath(
        impresources.files("gulp.mapping_files")
    )
    default_plugins_path = os.path.abspath(impresources.files("gulp.plugins"))
    custom_mapping_files_path = path_mapping_files()
    if custom_mapping_files_path is not None:
        custom_mapping_files_path = os.path.abspath(custom_mapping_files_path)
    custom_plugins_path = path_plugins(t=None)
    if custom_plugins_path is not None:
        custom_plugins_path = os.path.abspath(custom_plugins_path)

    logger().debug("default_mapping_files_path: %s" % (default_mapping_files_path))
    logger().debug("custom_mapping_files_path: %s" % (custom_mapping_files_path))
    logger().debug("default_plugins_path: %s" % (default_plugins_path))
    logger().debug("custom_plugins_path: %s" % (custom_plugins_path))

    if (
        custom_mapping_files_path is not None
        and pathlib.Path(default_mapping_files_path).resolve()
        != pathlib.Path(custom_mapping_files_path).resolve()
    ):
        # we will use custom_mapping_files_path so, copy the whole directory there
        if not await aiofiles.ospath.exists(custom_mapping_files_path):
            logger().info(
                "copying mapping files to custom directory: %s"
                % (custom_mapping_files_path)
            )
            await muty.file.copy_dir_async(
                default_mapping_files_path, custom_mapping_files_path
            )
        else:
            logger().warning(
                "custom mapping files directory already exists: %s"
                % (custom_mapping_files_path)
            )

    if (
        custom_plugins_path is not None
        and pathlib.Path(default_plugins_path).resolve()
        != pathlib.Path(custom_plugins_path).resolve()
    ):
        # we will use custom_plugins_path so, copy the whole directory there

        if not await aiofiles.ospath.exists(custom_plugins_path):
            logger().info(
                "copying plugins to custom directory: %s" % (custom_plugins_path)
            )
            await muty.file.copy_dir_async(default_plugins_path, custom_plugins_path)
        else:
            logger().warning(
                "custom plugins directory already exists: %s" % (custom_plugins_path)
            )


def override_runtime_parameter(k: str, v: any):
    """
    Overrides a runtime parameter in the configuration.

    Args:
        k (str): the key to override
        v (any): the value to set

    Returns:
        None
    """
    global _config
    old_value = _config.get(k, None)
    logger().warning(
        'overriding configuration parameter "%s": old value=%s, new value=%s'
        % (k, old_value, v)
    )
    _config[k] = v
    return

def check_path_index_template_override() -> bool:
    """
    Returns whether the index template path is overridden.
    """
    p = os.getenv("PATH_INDEX_TEMPLATE")
    if p is not None:
        return True
    p = _config.get("path_index_template", None)
    if p is not None:
        return True
    return False

def bind_to() -> tuple[str,int]:
    """
    Returns the bind address and port.

    Returns:
        tuple[str,int]: the bind address and port
    """
    p = os.getenv("BIND_TO")
    if p is None:
        p = _config.get("bind_to", None)
        if p is None:
            logger().warning("bind_to not set, using default!")
            p = '0.0.0.0:8080'

    logger().debug("bind_to: %s" % (p))
    splitted = p.split(":")
    if len(splitted) != 2:
        raise ValueError("invalid bind_to format: %s" % (p))

    return (splitted[0], int(splitted[1]))

def index_template_default_total_fields_limit() -> int:
    """
    Returns the default total fields limit for the index template (default=10000).
    """
    n = _config.get("index_template_default_total_fields_limit", None)
    if n is None:
        n = 10000
        logger().warning("using default total fields limit")

    logger().debug("index_template_default_total_fields_limit: %d" % (n))
    return n

def index_template_default_refresh_interval() -> str:
    """
    Returns the default refresh interval for the index template (i.e. usually in seconds, 5s, 30s, ...).
    """
    n = _config.get("index_template_default_refresh_interval", None)
    if n is None:
        n = "5s"
        logger().warning("using default refresh interval for index template")

    logger().debug("index_template_default_refresh_interval: %s" % (n))
    return n

def ingestion_request_timeout() -> int:
    """
    Returns the ingestion request timeout in seconds.
    """
    n = _config.get("ingestion_request_timeout", 60)
    return n


def _read_or_init_configuration(path: str = None) -> dict:
    global _config, _config_file_path
    _config_file_path = path_config()

    logger().info("configuration path: %s" % (_config_file_path))

    if not os.path.exists(_config_file_path):
        # copy default configuration file
        src = os.path.abspath(
            muty.file.safe_path_join(os.getcwd(), "gulp_cfg_template.json")
        )
        muty.file.copy_file(src, _config_file_path)
        os.chmod(_config_file_path, 0o0600)
        logger().warning(
            "no configuration file found, applying defaults from %s ..." % (src)
        )

    cfg_perms = oct(os.stat(_config_file_path).st_mode & 0o777)
    if cfg_perms != oct(0o0600):
        logger().warning(
            "careful, weak configuration file permissions %s != 0600" % cfg_perms
        )

    # read
    with open(_config_file_path, "rb") as f:
        js = f.read()
        n = json5.loads(js)

    # set global
    _config = n
    return _config


def config_dir() -> str:
    """
    get the configuration directory (it also ensures it exists)

    returns:
        str: the configuration directory
    """
    p = os.path.dirname(path_config())
    return p


def path_config() -> str:
    """
    get the configuration file path
    """
    global _config_file_path
    if _config_file_path is not None:
        # shortcut ...
        return _config_file_path

    p = os.getenv("PATH_CONFIG")
    if p is not None:
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


def ws_notes_on_match_batch_size() -> int:
    """
    Returns the batch size for notes on match websocket messages.
    """
    n = _config.get("ws_notes_on_match_batch_size", 1000)
    return n


def upload_tmp_dir() -> str:
    """
    get the upload temporary directory (it also ensures it exists)

    returns:
        str: the upload temporary directory
    """
    cfg_dir = config_dir()
    upload_dir = muty.file.safe_path_join(cfg_dir, "upload_tmp")
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


def token_ttl() -> int:
    """
    Returns the number of seconds a non-admin token is valid for.
    """
    n = _config.get("token_ttl", None)
    if n is None:
        n = 604800
        logger().warning(
            "using default number of seconds for token expiration=%d (%f days)"
            % (n, n / 86400)
        )
    return n


def token_admin_ttl() -> int:
    """
    Returns the number of seconds an admin token is valid for.
    """
    n = _config.get("token_admin_ttl", None)
    if n is None:
        n = 600
        logger().warning(
            "using default number of seconds for admin token expiration=%d (%f days)"
            % (n, n / 86400)
        )
    return n


def ingestion_evt_failure_threshold() -> int:
    """
    Returns the number of events that can fail before the ingestion of the current file is marked as FAILED (0=never abort an ingestion even with multiple failures).
    """
    n = _config.get("ingestion_evt_failure_threshold", 0)
    if n == 0 or n is None:
        return 0
    return n


def debug_collab() -> bool:
    """
    Returns whether to enable the collaborative API debug mode (prints SQL queries, etc...), default is False.
    """
    n = False
    if __debug__:
        n = _config.get("debug_collab", False)
    return n


def debug_no_token_expiration() -> bool:
    """
    Returns whether to disable token expiration.
    """
    n = False

    if __debug__:
        if os.getenv("GULP_INTEGRATION_TEST", None) is not None:
            logger().warning(
                "!!!WARNING!!! GULP_INTEGRATION_TEST is set, debug_no_token_expiration disabled!"
            )
            return False

        n = _config.get("debug_no_token_expiration", False)
        if n:
            logger().warning("!!!WARNING!!! debug_no_token_expiration is set to True !")
    return n


def stats_ttl() -> int:
    """
    Returns the number of seconds stats are kept.
    """
    n = _config.get("stats_ttl", None)
    if n is None:
        n = 86400
        logger().warning(
            "using default number of seconds for stats expiration=%d (%d days)"
            % (n, n / 86400)
        )
    return n

def https_cert_password() -> str:
    """
    Returns the password for the HTTPS certificate.
    """
    n = _config.get("https_cert_password", None)
    return n


def enforce_https() -> bool:
    """
    Returns whether to enforce HTTPS.
    """
    n = _config.get("https_enforce", False)
    return n


def enforce_https_client_certs() -> bool:
    """
    Returns whether to enforce HTTPS client certificates.
    """
    n = _config.get("https_enforce_client_certs", False)
    return n


def debug_allow_any_token_as_admin() -> bool:
    """
    Returns whether to allow any token as admin in debug mode.
    """
    n = False
    if __debug__:
        if os.getenv("GULP_INTEGRATION_TEST", None) is not None:
            logger().warning(
                "!!!WARNING!!! GULP_INTEGRATION_TEST is set, debug_allow_any_token_as_admin disabled!"
            )
            return False

        n = _config.get("debug_allow_any_token_as_admin", False)
        if n:
            logger().warning(
                "!!!WARNING!!! debug_allow_any_token_as_admin is set to True !"
            )
    return n


def debug_abort_on_opensearch_ingestion_error() -> bool:
    """
    Returns whether to abort ingestion of the current file when an error occurs during indexing on opensearch (=something's wrong in GulpDocument).
    """
    n = True
    if __debug__:
        n = _config.get("debug_abort_on_opensearch_ingestion_error", True)

    # logger().warning('debug_abort_on_opensearch_ingestion_error is set to True.')
    return n


def multiprocessing_batch_size() -> int:
    """
    Returns the number of files to ingest per batch.
    """
    n = _config.get("multiprocessing_batch_size", 0)
    if n == 0 or n is None:
        n = multiprocessing.cpu_count()
        logger().warning(
            "using default multiprocessing_batch_size(=number of cores)=%d" % (n)
        )
    return n


def concurrency_max_tasks() -> int:
    """
    Returns the maximum number of tasks to run per spawned process.
    """
    n = _config.get("concurrency_max_tasks", 0)
    if n == 0 or n is None:
        n = 16
        logger().warning("using default number of tasks per process=%d" % (n))
    return n


def opensearch_client_cert_password() -> str:
    """
    Returns the password for the opensearch client certificate.
    """
    n = _config.get("opensearch_client_cert_password", None)
    return n


def opensearch_multiple_nodes() -> bool:
    """
    Returns whether to use multiple nodes for opensearch.
    """
    n = _config.get("opensearch_multiple_nodes", False)
    return n


def parallel_processes_max() -> int:
    """
    Returns the maximum number of processes to use for ingestion.
    if not set, the number of cores will be used.
    """
    n = _config.get("parallel_processes_max", 0)
    if n == 0 or n is None:
        n = multiprocessing.cpu_count()
        logger().warning(
            "using default number of processes for ingestion (=number of cores=%d)."
            % (n)
        )
    return n


def parallel_processes_respawn_after_tasks() -> int:
    """
    Returns the number of tasks to spawn before respawning a process.
    if not set, 0 will be used (no respawn).
    """
    n = _config.get("parallel_processes_respawn_after_tasks", 0)
    if n is None:
        n = 0
    return n


def debug_allow_insecure_passwords() -> bool:
    """
    Returns whether to disable password validation when creating users.
    """
    n = False
    if __debug__:
        n = _config.get("debug_allow_insecure_passwords", False)

    logger().warning("!!!WARNING!!! debug_allow_insecure_passwords is set to True !")
    return n


def postgres_url() -> str:
    """
    Returns the postgres url (i.e. postgresql://user:password@localhost:5432)

    raises:
        Exception: If the postgres_url is not set in the configuration.
    """
    n = os.getenv("POSTGRES_URL", None)
    if n is None:
        n = _config.get("postgres_url", None)
        if n is None:
            raise Exception(
                "postgres_url not set (tried configuration and POSTGRES_URL environment_variable)."
            )

    return n


def postgres_ssl() -> bool:
    """
    Returns whether to use SSL for postgres.
    if this is set, the certificates used to connect to postgres will be:

    - $PATH_CERTS/postgres-ca.pem
    - $PATH_CERTS/postgres.pem, $PATH_CERTS/postgres.key (client cert used if found)
    """
    n = _config.get("postgres_ssl", False)
    return n


def postgres_verify_certs() -> bool:
    """
    Returns whether to verify the certificates when connecting to postgres with SSL.

    default: False
    """
    n = _config.get("postgres_verify_certs", False)
    return n


def postgres_client_cert_password() -> str:
    """
    Returns the password for the postgres client certificate.
    """
    n = _config.get("postgres_client_cert_password", None)
    return n


def opensearch_url() -> str:
    """
    Returns the opensearch url

    if this is an https url, the certificates used to connect to opensearch will be:

    - $PATH_CERTS/opensearch-ca.pem
    - $PATH_CERTS/opensearch.pem, $PATH_CERTS/opensearch.key (client cert used if found)


    raises:
        Exception: If the opensearch_url is not set in the configuration.
    """
    n = os.getenv("OPENSEARCH_URL", None)
    if n is None:
        n = _config.get("opensearch_url", None)
        if n is None:
            raise Exception(
                "opensearch_url not set (tried configuration and OPENSEARCH_URL environment_variable)."
            )

    return n


def query_sigma_max_notes() -> int:
    """
    Returns the maximum number of notes to generate for a sigma query (default=0=no limit).
    """
    n = _config.get("query_sigma_max_notes", 0)
    if n == 0 or n is None:
        n = 0

    return n


def opensearch_verify_certs() -> bool:
    """
    Returns whether to verify the certificates when connecting to opensearch with SSL.

    default: False

    """
    n = _config.get("opensearch_verify_certs", False)
    return n


def path_plugins(extension: bool=False) -> str:
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
        p = _config.get("path_plugins", None)
        if not p:
            # use default
            p = default_path

    pp = os.path.expanduser(p)
    logger().debug("plugins path: %s" % (pp))    
    if extension:
        return muty.file.safe_path_join(pp, "extension")
    return pp

def path_index_template() -> str:
    """
    Returns the path of the opensearch index template file.
    """
    p = impresources.files("gulp.api.mapping.index_template")
    default_path = muty.file.safe_path_join(p,'template.json')

    # try env
    p = os.getenv("PATH_INDEX_TEMPLATE", None)
    if not p:
        # try configuration
        p = _config.get("path_index_template", None)
        if not p:
            p = default_path

    pp = os.path.expanduser(p)
    logger().debug("path_index_template: %s" % (pp))
    return p

def path_mapping_files() -> str:
    """
    Returns the directory where mapping files for plugins are stored (default=None=GULPDIR/mapping_files).
    """
    # try env
    default_path = impresources.files("gulp.mapping_files")
    p = os.getenv("PATH_MAPPING_FILES", None)
    if not p:
        # try configuration
        p = _config.get("path_mapping_files", None)
        if not p:
            p = default_path

    pp = os.path.expanduser(p)
    logger().debug("mapping files path: %s" % (pp))
    return p


def path_certs() -> str:
    """
    Returns the directory where the certificates are stored.
    """
    # try env
    p = os.getenv("PATH_CERTS", None)
    if not p:
        # try configuration
        p = _config.get("path_certs", None)
        if not p:
            logger().warning('"path_certs" is not set !')
            return None
    
    pp = os.path.expanduser(p)
    logger().debug("certs directory: %s" % (pp))
    return pp


def aggregation_max_buckets() -> int:
    """
    Returns the maximum number of buckets to return for aggregations (default: 999).
    """
    n = _config.get("aggregation_max_buckets", None)
    if n is None or n == 0:
        # default
        n = 999

    return n


def ws_rate_limit_delay() -> float:
    """
    Returns the delay in seconds to wait before sending a message to a client.
    """
    n = _config.get("ws_rate_limit_delay", 0.01)
    return n


def plugin_cache_enabled() -> bool:
    """
    Returns whether to enable the plugin cache (default: True).
    """
    n = _config.get("plugin_cache_enabled", True)
    return n


def config() -> dict:
    """
    returns the configuration dictionary
    """
    global _config
    return _config
