"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import asyncio
import multiprocessing
import os
import ssl
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.managers import SyncManager
from queue import Queue

import aiomultiprocess
import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
import uvicorn
from asyncio_pool import AioPool
from fastapi import FastAPI, Request
from fastapi.concurrency import asynccontextmanager
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from muty.jsend import JSendException, JSendResponse
from opensearchpy import RequestError
from gulp.api.ws_api import GulpSharedWsQueue
from gulp.utils import GulpLogger
import gulp.api.collab_api as collab_api
import gulp.api.opensearch_api as opensearch_api
import gulp.api.rest.ws as gulp_ws
from gulp.config import GulpConfig
import gulp.utils as gulp_utils
from gulp.api.collab import db
from gulp.api.collab.structs import (
    MissingPermission,
    SessionExpired,
    WrongUsernameOrPassword,
)
from gulp.defs import GulpPluginType, InvalidArgument, ObjectNotFound
from gulp.utils import delete_first_run_file, logger

_process_executor: aiomultiprocess.Pool = None
_thread_pool_executor: ThreadPoolExecutor = None
_log_file_path: str = None
_is_first_run: bool = False
_reset_collab_on_start: bool = False
_elastic_index_to_reset: str = None
_aiopool: AioPool = None
_mpManager: SyncManager = None
_ws_q: Queue = None
_shutting_down: bool = False
_extension_plugins: list = []

def set_shutdown(*args):
    """
    Sets the global `_shutting_down` flag to True.
    """
    global _shutting_down
    GulpLogger.get_logger().debug("setting _shutting_down=True ...")
    _shutting_down = True


def is_shutdown() -> bool:
    """
    Returns the value of the global `_shutting_down` flag.

    Returns:
        bool: True if the server is shutting down, False otherwise.
    """
    global _shutting_down
    # GulpLogger.get_logger().debug("is_shutdown()=%r" % (_shutting_down))
    if _shutting_down:
        GulpLogger.get_logger().debug("is_shutdown()=True")

    return _shutting_down


def fastapi_app() -> FastAPI:
    """
    Returns the global FastAPI instance.

    Returns:
        FastAPI: The global FastAPI instance.
    """
    global _app
    return _app


def aiopool() -> AioPool:
    """
    Returns the global AioPool instance.

    Returns:
        AioPool: The global AioPool instance.
    """
    global _aiopool
    return _aiopool


def process_executor() -> aiomultiprocess.Pool:
    """
    Returns the global aiomultiprocess.Pool instance.

    Returns:
        aiomultiprocess.Pool: The global aiomultiprocess.Pool instance.
    """
    global _process_executor
    return _process_executor


def thread_pool_executor() -> ThreadPoolExecutor:
    """
    Returns the global ThreadPoolExecutor instance (per-process).

    Returns:
        ThreadPoolExecutor: The global ThreadPoolExecutor instance.
    """
    global _thread_pool_executor
    if _thread_pool_executor is None:
        GulpLogger.get_logger().debug("creating thread pool executor for the current process...")
        _thread_pool_executor = ThreadPoolExecutor()

    return _thread_pool_executor


def _unload_extension_plugins():
    """
    Unload extension plugins.
    """
    from gulp import plugin

    global _extension_plugins
    GulpLogger.get_logger().debug("unloading extension plugins ...")
    for p in _extension_plugins:
        plugin.unload_plugin(p)

    _extension_plugins = []


async def _load_extension_plugins() -> list["PluginBase"]:
    """
    Load available extension plugins

    Returns:
        list: A list of loaded extension plugins.
    """
    from gulp import plugin

    GulpLogger.get_logger().debug("loading extension plugins ...")
    path_plugins = GulpConfig.get_instance().path_plugins(GulpPluginType.EXTENSION)
    files = await muty.file.list_directory_async(path_plugins, "*.py*", recursive=True)
    l = []
    for f in files:
        if "__init__" not in f and "__pycache__" not in f:
            # get base filename without extension
            plugin_name = os.path.basename(f).rsplit(".", 1)[0]
            p = plugin.load_plugin(plugin_name, GulpPluginType.EXTENSION)
            l.append(p)
    GulpLogger.get_logger().debug("loaded %d extension plugins: %s" % (len(l), l))
    return l



@_app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _: Request, ex: RequestValidationError
) -> JSendResponse:
    status_code = 400
    jsend_ex = JSendException(ex=ex, status_code=status_code)
    GulpLogger.get_logger().debug(
        "in request-validation exception handler, status_code=%d" % (status_code)
    )
    return muty.jsend.fastapi_jsend_exception_handler(jsend_ex, status_code)


@_app.exception_handler(RequestError)
async def bad_request_exception_handler(
    _: Request, ex: RequestValidationError
) -> JSendResponse:
    status_code = 400
    jsend_ex = JSendException(ex=ex, status_code=status_code)
    GulpLogger.get_logger().debug("in bad-request exception handler, status_code=%d" % (status_code))
    return muty.jsend.fastapi_jsend_exception_handler(jsend_ex, status_code)


@_app.exception_handler(JSendException)
async def gulp_exception_handler(_: Request, ex: JSendException) -> JSendResponse:
    """
    A handler function for JSendException exceptions raised by the Gulp API.
    """
    # tweak status code, 500 is default
    status_code = 500

    if ex.status_code is not None:
        # force status code
        status_code = ex.status_code
    elif ex.ex is not None:
        # get from exception
        if isinstance(ex.ex, ObjectNotFound) or isinstance(ex.ex, FileNotFoundError):
            status_code = 404
        elif isinstance(ex.ex, InvalidArgument) or isinstance(ex.ex, ValueError):
            status_code = 400
        elif (
            isinstance(ex.ex, MissingPermission)
            or isinstance(ex.ex, WrongUsernameOrPassword)
            or isinstance(ex.ex, SessionExpired)
        ):
            status_code = 401
        else:
            status_code = 500
    GulpLogger.get_logger().debug("in exception handler, status_code=%d" % (status_code))
    return muty.jsend.fastapi_jsend_exception_handler(ex, status_code)



class GulpRestServer():
    """
    manages the gULP REST server.
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
            self._app = _app
            self._log_file_path = None
            self._reset_collab_on_start = False
            self._elastic_index_to_reset = None
            self._first_run = False
            self._shutting_down: bool = False
            self._extension_plugins: list = []

            if self._check_first_run():
                # first run, create index
                self._elastic_index_to_reset = "gulpidx"
                self._reset_collab_on_start = True
                self._first_run=True
                GulpLogger.get_logger().info("first run detected!")
                
            else:
                GulpLogger.get_logger().info("not first run")

            # create multiprocessing manager
            self._mp_manager: SyncManager = multiprocessing.Manager()

            # initialize websocket shared queue
            q = _mpManager.Queue()
            GulpSharedWsQueue.get_instance().init_queue(q)

            # aio_task_pool is used to run tasks in THE MAIN PROCESS (the main event loop), such as the trivial tasks (query_max_min, collabobj handling, etc...).
            # ingest and query tasks are always run in parallel processes through self._process_executor.
            self._aio_task_pool = AioPool(GulpConfig.get_instance().concurrency_max_tasks())

            # threadpool for the main process
            self.thread_pool_executor: ThreadPoolExecutor = ThreadPoolExecutor()
            
            # executor for worker processes
            self.process_executor: aiomultiprocess.Pool = None
            

            # initialize 

    @classmethod
    def get_instance(cls) -> "GulpRestServer":
        """
        returns the singleton instance
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()

            
        return cls._instance

    @staticmethod
    def _worker_exception_handler(ex: Exception):
        """
        for debugging purposes only, to catch exception eaten by aiopool (they're critical exceptions, the process dies) ...
        """
        GulpLogger.get_logger().exception("WORKER EXCEPTION: %s" % (ex))


    @staticmethod
    @asynccontextmanager
    async def lifespan_handler(app: FastAPI):
        """
        A handler function for FastAPI's lifespan events.

                Args:
            app (FastAPI): The FastAPI instance.

        Yields:
            None
        """
        instance = GulpRestServer.
        GulpLogger.get_logger().info("gULP starting!")
        GulpLogger.get_logger().warning(
            "concurrency_max_tasks=%d, parallel_processes_max=%d, parallel_processes_respawn_after_tasks=%d"
            % (
                GulpConfig.get_instance().concurrency_max_tasks(),
                GulpConfig.get_instance().parallel_processes_max(),
                GulpConfig.get_instance().parallel_processes_respawn_after_tasks(),
            )
        )

        # create multiprocessing manager
        _mpManager = multiprocessing.Manager()

        # and queue for websocket messages
        _ws_q = _mpManager.Queue()
        gulp_ws.init(_ws_q, main_process=True)

        # aiopool is used to run tasks in THIS process (the main event loop), such as the trivial tasks (query_max_min, collabobj handling, etc...).
        # ingest and query tasks are always run in parallel processes through aiomultiprocess.
        _aiopool = AioPool(GulpConfig.get_instance().concurrency_max_tasks())

        # create thread pool executor
        _thread_pool_executor = ThreadPoolExecutor()

        # setup collab and elastic
        if _reset_collab_on_start:
            GulpLogger.get_logger().warning("--reset-collab is set!")
        try:
            await collab_db.setup(force_recreate=_reset_collab_on_start)
        except Exception as ex:
            if _is_first_run:
                # on first run, we delete the first run file as well, to allow the server to start again for the first time
                delete_first_run_file()
            raise ex

        elastic = opensearch_api.elastic()
        collab_session = await collab_api.session()
        GulpLogger.get_logger().debug(
            "main process collab_session=%s, elastic=%s" % (collab_session, elastic)
        )

        if _elastic_index_to_reset is not None:
            GulpLogger.get_logger().warning(
                "--reset-elastic is set, dropping and recreating the ElasticSearch index %s ..."
                % (_elastic_index_to_reset)
            )

            # loop for up to 60 seconds to wait for elasticsearch ...
            elastic_reset_ok = False
            elastic_reset_failed = 0
            while elastic_reset_ok is False:
                try:
                    await opensearch_api.datastream_create(elastic, _elastic_index_to_reset)
                    elastic_reset_ok = True
                except Exception as ex:
                    GulpLogger.get_logger().exception(
                        "waiting elasticsearch to come up, or error in datastream_create() ... retrying in 1 second ..."
                    )
                    await asyncio.sleep(1)
                    elastic_reset_failed += 1
                    if elastic_reset_failed > 60:
                        # give up after 120 seconds
                        raise ex
                    continue

        # aiomultiprocess is used to run parallel process for the ingestion and sigma_query tasks,
        # each with concurrency_max_tasks() tasks in their event loop
        _process_executor = await recreate_process_executor()

        # create thread pool executor
        _extension_plugins = await _load_extension_plugins()

        # set a SIGINT (ctrl-c) handler for clean shutdown
        # NOTE: this seems to break with fastapi 0.110, remove it for now ...

        # wait for shutdown
        yield

        GulpLogger.get_logger().info("gULP shutting down!, logger level=%d" % (GulpLogger.get_logger().level))
        set_shutdown()

        # wait websockets close
        await gulp_ws.wait_all_connected_ws_close()

        # unload extension plugins
        _unload_extension_plugins()

        # shutdown pg_process_executor
        await collab_api.shutdown()

        # shutdown elastic
        await opensearch_api.shutdown_client(elastic)

        # close queues
        GulpLogger.get_logger().debug("closing ws queue ...")
        gulp_ws.shared_queue_close(_ws_q)

        GulpLogger.get_logger().debug("shutting down aiopool ...")
        await _aiopool.cancel()
        await _aiopool.join()

        GulpLogger.get_logger().debug("shutting down thread pool executor for the main process...")
        _thread_pool_executor.shutdown(wait=True)

        GulpLogger.get_logger().debug("shutting down aiomultiprocess ...")
        _process_executor.close()
        await _process_executor.join()

        GulpLogger.get_logger().debug("executors shut down, we can gracefully exit.")

    async def recreate_process_executor(self):
        """
        Creates (or recreates, if it already exists) the process executor pool.

        This function closes the existing process executor pool, waits for it to finish,
        and then creates a new process executor pool with the specified configuration.

        Returns:
            The newly created process executor pool.

        """
        from gulp.workers import process_worker_init

        # close and wait
        self._process_executor.close()
        await self._process_executor.join()

        spawned_processes = self._mpManager.Value(int, 0)
        num_processes = GulpConfig.get_instance().parallel_processes_max()
        lock = self._mpManager.Lock()
        self._process_executor = aiomultiprocess.Pool(
            exception_handler=aiopool_exception_handler,
            processes=num_processes,
            childconcurrency=GulpConfig.get_instance().concurrency_max_tasks(),
            maxtasksperchild=GulpConfig.get_instance().parallel_processes_respawn_after_tasks(),
            initializer=process_worker_init,
            initargs=(
                spawned_processes,
                lock,
                _ws_q,
                GulpLogger.get_logger().level,
                _log_file_path,
            ),
        )

        # wait for all processes are spawned
        while spawned_processes.value < num_processes:
            # GulpLogger.get_logger().debug('waiting for all processes to be spawned ...')
            await asyncio.sleep(0.1)

        GulpLogger.get_logger().debug(
            "all processes spawned, spawned_processes=%d" % (spawned_processes.value)
        )
        return _process_executor



    def _check_first_run(self) -> bool:
        """
        check if this is the first run of the server.

        Returns:
            bool: True if this is the first run, False otherwise.           
        """        
        # check if this is the first run
        
        config_directory = GulpConfig.get_instance().config_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            GulpLogger.get_logger().debug("first run file exists: %s" % (check_first_run_file))
            return False

        # create firstrun file
        GulpLogger.get_logger().warning("first run file does not exist: %s" % (check_first_run_file))
        with open(check_first_run_file, "w") as f:
            f.write("gulp!")
        return True

    def _reset_first_run() -> None:
        """
        Resets the first run flag.
        """
        
        config_directory = GulpConfig.get_instance().config_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            muty.file.delete_file_or_dir(check_first_run_file)
            GulpLogger.get_logger().info("first run file deleted: %s" % (check_first_run_file))
        else:
            GulpLogger.get_logger().warning("first run file does not exist: %s" % (check_first_run_file))


    def start(
        self,
        address: str,
        port: int,
        log_file_path: str = None,
        reset_collab: bool = False,
        index: str = None
    ):
        """
        Starts a server at the specified address and port using the global `_app` instance.

        Args:
            address (str): The IP address to bind the server to.
            port (int): The port number to bind the server to.
            log_file_path (str): The path to the log file.
            reset_collab (bool): If True, the collab will be reset on start.
            index (str): name of the OpenSearch/Elasticsearch index to create (if --reset-elastic is passed).
        """
        # set sigint handler (no more needed)
        # signal.signal(signal.SIGINT, set_shutdown)

        self._log_file_path = log_file_path
        self._reset_collab_on_start = reset_collab
        self._elastic_index_to_reset = index

        # Append all routers to main app
        import gulp.api.rest.collab_utility
        import gulp.api.rest.db
        import gulp.api.rest.glyph
        import gulp.api.rest.highlight
        import gulp.api.rest.ingest
        import gulp.api.rest.link
        import gulp.api.rest.note
        import gulp.api.rest.operation
        import gulp.api.rest.query
        import gulp.api.rest.user_session
        import gulp.api.rest.shared_data
        import gulp.api.rest.stats
        import gulp.api.rest.stored_query
        import gulp.api.rest.story
        import gulp.api.rest.user
        import gulp.api.rest.user_data
        import gulp.api.rest.utility

        _app.include_router(gulp.api.rest.client.router())
        _app.include_router(gulp.api.rest.collab_utility.router())
        _app.include_router(gulp.api.rest.db.router())
        _app.include_router(gulp.api.rest.glyph.router())
        _app.include_router(gulp.api.rest.highlight.router())
        _app.include_router(gulp.api.rest.ingest.router())
        _app.include_router(gulp.api.rest.link.router())
        _app.include_router(gulp.api.rest.note.router())
        _app.include_router(gulp.api.rest.operation.router())
        _app.include_router(gulp.api.rest.query.router())
        _app.include_router(gulp.api.rest.user_session.router())
        _app.include_router(gulp.api.rest.shared_data.router())
        _app.include_router(gulp.api.rest.stats.router())
        _app.include_router(gulp.api.rest.stored_query.router())
        _app.include_router(gulp.api.rest.story.router())
        _app.include_router(gulp.api.rest.user.router())
        _app.include_router(gulp.api.rest.utility.router())
        _app.include_router(gulp.api.rest.user_data.router())
        _app.include_router(gulp_ws.router())

        GulpLogger.get_logger().info(
            "starting server at %s, port=%d, logger level=%d, config path=%s, log_file_path=%s, reset_collab=%r, elastic_index to reset=%s ..."
            % (
                address,
                port,
                GulpLogger.get_logger().level,
                GulpConfig.get_instance().path_config(),
                log_file_path,
                reset_collab,
                index,
            )
        )
        if GulpConfig.get_instance().enforce_https():
            GulpLogger.get_logger().info("enforcing HTTPS ...")

            certs_path: str = GulpConfig.get_instance().path_certs()
            cert_password: str = GulpConfig.get_instance().https_cert_password()
            gulp_ca_certs = muty.file.safe_path_join(certs_path, "gulp-ca.pem")
            if not os.path.exists(gulp_ca_certs):
                # use server cert as CA cert
                gulp_ca_certs = muty.file.safe_path_join(certs_path, "gulp.pem")

            ssl_cert_verify_mode: int = ssl.VerifyMode.CERT_OPTIONAL
            if GulpConfig.get_instance().enforce_https_client_certs():
                ssl_cert_verify_mode = ssl.VerifyMode.CERT_REQUIRED
                GulpLogger.get_logger().info("enforcing HTTPS client certificates ...")

            ssl_keyfile = muty.file.safe_path_join(certs_path, "gulp.key")
            ssl_certfile = muty.file.safe_path_join(certs_path, "gulp.pem")
            GulpLogger.get_logger().info(
                "ssl_keyfile=%s, ssl_certfile=%s, cert_password=%s, ssl_ca_certs=%s, ssl_cert_verify_mode=%d"
                % (
                    ssl_keyfile,
                    ssl_certfile,
                    cert_password,
                    gulp_ca_certs,
                    ssl_cert_verify_mode,
                )
            )
            uvicorn.run(
                _app,
                host=address,
                port=port,
                ssl_keyfile=muty.file.safe_path_join(certs_path, "gulp.key"),
                ssl_keyfile_password=cert_password,
                ssl_certfile=muty.file.safe_path_join(certs_path, "gulp.pem"),
                ssl_ca_certs=gulp_ca_certs,
                ssl_cert_reqs=ssl_cert_verify_mode,
            )
        else:
            # http
            GulpLogger.get_logger().warning("HTTP!")
            uvicorn.run(_app, host=address, port=port)

_app: FastAPI = FastAPI(
    title="gULP",
    description="(gui)Universal Log Processor",
    swagger_ui_parameters={"operationsSorter": "alpha", "tagsSorter": "alpha"},
    version=gulp_utils.version_string(),
    lifespan=GulpRestServer.lifespan_handler,
)

_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


