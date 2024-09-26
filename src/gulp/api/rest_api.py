"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import asyncio
import multiprocessing
import os
import ssl
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
from opensearchpy import AsyncOpenSearch as AsyncElasticsearch
from opensearchpy import RequestError
from sqlalchemy.ext.asyncio import AsyncEngine

import gulp.api.collab.db as collab_db
import gulp.api.collab_api as collab_api
import gulp.api.elastic_api as elastic_api
import gulp.api.rest.ws as gulp_ws
import gulp.config as config
import gulp.utils as gulp_utils
from gulp.api.collab.base import (
    MissingPermission,
    SessionExpired,
    WrongUsernameOrPassword,
)
from gulp.defs import GulpPluginType, InvalidArgument, ObjectNotFound
from gulp.utils import logger

_process_executor: aiomultiprocess.Pool = None
_log_file_path: str = None
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
    logger().debug("setting _shutting_down=True ...")
    _shutting_down = True


def is_shutdown() -> bool:
    """
    Returns the value of the global `_shutting_down` flag.

    Returns:
        bool: True if the server is shutting down, False otherwise.
    """
    global _shutting_down
    # logger().debug("is_shutdown()=%r" % (_shutting_down))
    if _shutting_down:
        logger().debug("is_shutdown()=True")

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


def _unload_extension_plugins():
    """
    Unload extension plugins.
    """
    from gulp import plugin

    global _extension_plugins
    logger().debug("unloading extension plugins ...")
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

    logger().debug("loading extension plugins ...")
    path_plugins = config.path_plugins(GulpPluginType.EXTENSION)
    files = await muty.file.list_directory_async(path_plugins, "*.py*", recursive=True)
    l = []
    for f in files:
        if "__init__" not in f and "__pycache__" not in f:
            # get base filename without extension
            plugin_name = os.path.basename(f).rsplit(".", 1)[0]
            p = plugin.load_plugin(plugin_name, GulpPluginType.EXTENSION)
            l.append(p)
    logger().debug("loaded %d extension plugins: %s" % (len(l), l))
    return l


@asynccontextmanager
async def lifespan_handler(app: FastAPI):
    """
    A handler function for FastAPI's lifespan events.

    On startup, initializes a shared client if multiprocessing is not enabled (gulp is using multithreading/single process).
    On shutdown, shuts down the shared client if multiprocessing is enabled (gulp is using multithreading/single process)

    Args:
        app (FastAPI): The FastAPI instance.

    Yields:
        None
    """
    global _mpManager, _aiopool, _process_executor, _reset_collab_on_start, _ws_q, _extension_plugins

    logger().info("gULP starting!")
    logger().warning(
        "concurrency_max_tasks=%d, parallel_processes_max=%d, parallel_processes_respawn_after_tasks=%d"
        % (
            config.concurrency_max_tasks(),
            config.parallel_processes_max(),
            config.parallel_processes_respawn_after_tasks(),
        )
    )

    # create multiprocessing manager
    _mpManager = multiprocessing.Manager()

    # and queue for websocket messages
    _ws_q = _mpManager.Queue()
    gulp_ws.init(_ws_q, main_process=True)

    # aiopool is used to run tasks in THIS process (the main event loop), such as the trivial tasks (query_max_min, collabobj handling, etc...).
    # ingest and query tasks are always run in parallel processes through aiomultiprocess.
    _aiopool = AioPool(config.concurrency_max_tasks())

    # setup collab and elastic
    if _reset_collab_on_start:
        logger().warning(
            "--reset-collab is set, dropping and recreating the collaboration database ..."
        )
        await collab_db.drop(config.postgres_url())

    # create main process clients
    collab = await collab_api.collab()
    elastic = elastic_api.elastic()
    logger().debug("main process collab=%s, elastic=%s" % (collab, elastic))

    if _elastic_index_to_reset is not None:
        logger().warning(
            "--reset-elastic is set, dropping and recreating the ElasticSearch index %s ..."
            % (_elastic_index_to_reset)
        )

        # loop for up to 120 seconds to wait for elasticsearch ...
        elastic_reset_ok = False
        elastic_reset_failed = 0
        while elastic_reset_ok is False:
            try:
                await elastic_api.datastream_create(elastic, _elastic_index_to_reset)
                elastic_reset_ok = True
            except Exception as ex:
                logger().exception(
                    "waiting elasticsearch to come up, or error in index_create() ... retrying in 1 second ..."
                )
                await asyncio.sleep(1)
                elastic_reset_failed += 1
                if elastic_reset_failed > 120:
                    # give up after 120 seconds
                    raise ex
                continue

    # aiomultiprocess is used to run parallel process for the ingestion and sigma_query tasks,
    # each with concurrency_max_tasks() tasks in their event loop
    _process_executor = await recreate_process_executor()

    # load extension plugins, if any
    _extension_plugins = await _load_extension_plugins()

    # set a SIGINT (ctrl-c) handler for clean shutdown
    # NOTE: this seems to break with fastapi 0.110, remove it for now ...

    # wait for shutdown
    yield

    logger().info("gULP shutting down!, logger level=%d" % (logger().level))
    set_shutdown()

    # wait websockets close
    await gulp_ws.wait_all_connected_ws_close()

    # unload extension plugins
    _unload_extension_plugins()

    # shutdown pg_process_executor
    await collab_db.engine_close(collab)

    # shutdown elastic
    await elastic_api.shutdown_client(elastic)

    # close queues
    logger().debug("closing ws queue ...")
    gulp_ws.shared_queue_close(_ws_q)

    logger().debug("shutting down aiopool ...")
    await _aiopool.cancel()
    await _aiopool.join()

    logger().debug("shutting down aiomultiprocess ...")
    _process_executor.close()
    await _process_executor.join()

    logger().debug("executors shut down, we can gracefully exit.")


_app: FastAPI = FastAPI(
    title="gULP",
    description="(gui)Universal Log Processor",
    swagger_ui_parameters={"operationsSorter": "alpha", "tagsSorter": "alpha"},
    version=gulp_utils.version_string(),
    lifespan=lifespan_handler,
)

_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@_app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _: Request, ex: RequestValidationError
) -> JSendResponse:
    status_code = 400
    jsend_ex = JSendException(ex=ex, status_code=status_code)
    logger().debug(
        "in request-validation exception handler, status_code=%d" % (status_code)
    )
    return muty.jsend.fastapi_jsend_exception_handler(jsend_ex, status_code)


@_app.exception_handler(RequestError)
async def bad_request_exception_handler(
    _: Request, ex: RequestValidationError
) -> JSendResponse:
    status_code = 400
    jsend_ex = JSendException(ex=ex, status_code=status_code)
    logger().debug("in bad-request exception handler, status_code=%d" % (status_code))
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
    logger().debug("in exception handler, status_code=%d" % (status_code))
    return muty.jsend.fastapi_jsend_exception_handler(ex, status_code)


async def recreate_process_executor() -> aiomultiprocess.Pool:
    """
    Creates (or recreates, if it already exists) the process executor pool.

    This function closes the existing process executor pool, waits for it to finish,
    and then creates a new process executor pool with the specified configuration.

    Returns:
        The newly created process executor pool.

    """
    global _process_executor, _log_file_path, _ws_q
    import gulp.workers as workers

    if _process_executor is not None:
        # close and wait
        _process_executor.close()
        await _process_executor.join()

    spawned_processes = _mpManager.Value(int, 0)
    num_processes = config.parallel_processes_max()
    lock = _mpManager.Lock()
    _process_executor = aiomultiprocess.Pool(
        exception_handler=aiopool_exception_handler,
        processes=num_processes,
        childconcurrency=config.concurrency_max_tasks(),
        maxtasksperchild=config.parallel_processes_respawn_after_tasks(),
        initializer=workers.process_worker_init,
        initargs=(
            spawned_processes,
            lock,
            _ws_q,
            logger().level,
            _log_file_path,
        ),
    )

    # wait for all processes are spawned
    while spawned_processes.value < num_processes:
        # logger().debug('waiting for all processes to be spawned ...')
        await asyncio.sleep(0.1)

    logger().debug(
        "all processes spawned, spawned_processes=%d" % (spawned_processes.value)
    )
    return _process_executor


def aiopool_exception_handler(ex: Exception):
    """
    for debugging purposes only, to catch exception eaten by aiopool (they're critical exceptions, the process dies) ...
    """
    l = muty.log.configure_logger("aiopool")
    l.exception("AIOPOOLEX: %s" % (ex))


def start_server(
    address: str,
    port: int,
    log_file_path: str = None,
    reset_collab: bool = False,
    elastic_index: str = None,
):
    """
    Starts a server at the specified address and port using the global `_app` instance.

    Args:
        address (str): The IP address to bind the server to.
        port (int): The port number to bind the server to.
        reset_collab (bool): If True, the collab will be reset on start.
        elastic_index (str): The name of the ElasticSearch index to create (if --reset-elastic is passed).
    """

    # set these globals in the main server process
    global _app, _log_file_path, _reset_collab_on_start, _elastic_index_to_reset

    # set sigint handler (no more needed)
    # signal.signal(signal.SIGINT, set_shutdown)

    _log_file_path = log_file_path
    _reset_collab_on_start = reset_collab
    _elastic_index_to_reset = elastic_index

    # Append all routers to main app
    import gulp.api.rest.client
    import gulp.api.rest.collab_utility
    import gulp.api.rest.db
    import gulp.api.rest.glyph
    import gulp.api.rest.highlight
    import gulp.api.rest.ingest
    import gulp.api.rest.link
    import gulp.api.rest.note
    import gulp.api.rest.operation
    import gulp.api.rest.query
    import gulp.api.rest.session
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
    _app.include_router(gulp.api.rest.session.router())
    _app.include_router(gulp.api.rest.shared_data.router())
    _app.include_router(gulp.api.rest.stats.router())
    _app.include_router(gulp.api.rest.stored_query.router())
    _app.include_router(gulp.api.rest.story.router())
    _app.include_router(gulp.api.rest.user.router())
    _app.include_router(gulp.api.rest.utility.router())
    _app.include_router(gulp.api.rest.user_data.router())
    _app.include_router(gulp_ws.router())

    logger().info(
        "starting server at %s, port=%d, logger level=%d, config path=%s, log_file_path=%s, reset_collab=%r, elastic_index to reset=%s ..."
        % (
            address,
            port,
            logger().level,
            config.config_path(),
            log_file_path,
            reset_collab,
            elastic_index,
        )
    )
    if config.enforce_https():
        logger().info("enforcing HTTPS ...")

        certs_path: str = config.certs_directory()
        cert_password: str = config.https_cert_password()
        gulp_ca_certs = muty.file.safe_path_join(certs_path, "gulp-ca.pem")
        if not os.path.exists(gulp_ca_certs):
            # use server cert as CA cert
            gulp_ca_certs = muty.file.safe_path_join(certs_path, "gulp.pem")

        ssl_cert_verify_mode: int = ssl.VerifyMode.CERT_OPTIONAL
        if config.enforce_https_client_certs():
            ssl_cert_verify_mode = ssl.VerifyMode.CERT_REQUIRED
            logger().info("enforcing HTTPS client certificates ...")

        ssl_keyfile = muty.file.safe_path_join(certs_path, "gulp.key")
        ssl_certfile = muty.file.safe_path_join(certs_path, "gulp.pem")
        logger().info(
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
        logger().warning("HTTP!")
        uvicorn.run(_app, host=address, port=port)
