"""
This module manages the REST API server for the gULP application.

It provides a singleton server instance that handles API routes, request validation,
exception handling, and server lifecycle. The server supports both HTTP and HTTPS
configurations and integrates with various gULP components like WebSockets,
collaborative features, and plugins.

The module also handles server initialization, shutdown procedures, and restart
capabilities for the application.
"""

import asyncio
import os
import ssl
import sys
from typing import Any, Awaitable, Callable, Coroutine, Sequence

import asyncio_atexit
import muty.file
import muty.os
import muty.string
import muty.version
import orjson
import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from opensearchpy import RequestError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.middleware.sessions import SessionMiddleware

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.ws_api import GulpConnectedSockets, GulpRedisBroker
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import ObjectAlreadyExists, ObjectNotFound


class GulpServer:
    """
    manages the gULP REST server.
    """

    _instance: "GulpServer" = None

    def __init__(self):
        self._initialized: bool = True
        self._app: FastAPI = None
        self._server_id: str = muty.string.generate_unique()
        self._logger_file_path: str = None
        self._log_to_syslog: tuple[str, str] = None
        self._log_level: int = None
        self._reset_collab: bool = False
        self._create_operation: str = None
        self._lifespan_task: asyncio.Task = None
        self._poll_tasks_task: asyncio.Task = None
        self._extension_plugins: list[GulpPluginBase] = []

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpServer":
        """
        returns the singleton instance

        Returns:
            GulpServer: the singleton instance
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def server_id(self) -> str:
        """
        returns the server unique ID

        Returns:
            str: server unique ID
        """
        return self._server_id

    def running_tasks(self) -> int:
        """
        Returns the number of currently running tasks.

        Returns:
            int: Number of running tasks.
        """
        return self._running_tasks

    def version_string(self) -> str:
        """
        returns the version string

        Returns:
            str: version string
        """
        return "gulp v%s (muty v%s)" % (
            muty.version.pkg_version("gulp"),
            muty.version.muty_version(),
        )

    async def _unload_extension_plugins(self) -> None:
        """
        unload extension plugins
        """
        if self._extension_plugins:
            MutyLogger.get_instance().debug(
                "unloading %d extension plugins ...", len(self._extension_plugins)
            )
            for p in self._extension_plugins:
                await p.unload()
            self._extension_plugins = []

    async def _load_extension_plugins(self) -> None:
        """
        load available extension plugins
        """
        MutyLogger.get_instance().debug("loading extension plugins ...")

        # default
        p = GulpConfig.get_instance().path_plugins_default()
        path_extension = os.path.join(p, "extension")
        files: list[str] = await muty.file.list_directory_async(path_extension, "*.py*")
        MutyLogger.get_instance().debug(
            "found %d extensions in default path: %s", len(files), path_extension
        )

        # extras, if any
        p = GulpConfig.get_instance().path_plugins_extra()
        if p:
            path_extension = os.path.join(p, "extension")
            ff: list[str] = await muty.file.list_directory_async(
                path_extension, "*.py*"
            )
            MutyLogger.get_instance().debug(
                "found %d extensions in extra path: %s", len(ff), path_extension
            )
            files.extend(ff)

        # remove non .py or .pyc files and remove __init__.py
        files = [f for f in files if (f.endswith(".py") or f.endswith(".pyc"))]
        files = [f for f in files if "__init__.py" not in f]

        # order files alphabetically, considering only basename
        files = sorted(files, key=lambda x: os.path.basename(x).lower())

        count: int = 0
        for f in files:
            # load
            try:
                p = await GulpPluginBase.load(f, extension=True)
            except Exception as ex:
                # plugin failed to load
                MutyLogger.get_instance().error(
                    "failed to load (extension) plugin file: %s", f
                )
                MutyLogger.get_instance().exception(ex)
                continue
            self._extension_plugins.append(p)
            count += 1

        MutyLogger.get_instance().debug("loaded %d extension plugins" % (count))

    async def _bad_request_exception_handler(
        self, r: Request, ex: Any
    ) -> JSendResponse:
        """
        set error code 400 to generic bad requests
        """
        from gulp.api.collab.structs import (
            MissingPermission,
            SessionExpired,
            WrongUsernameOrPassword,
        )

        status_code = 500
        req_id = None
        name = ex.__class__.__name__

        if isinstance(ex, JSendException):
            req_id = ex.req_id
            if ex.status_code is not None:
                status_code = ex.status_code

            if ex.__cause__ is not None:
                # use the inner exception
                ex = ex.__cause__
                name = ex.__class__.__name__

        if isinstance(ex, RequestValidationError):
            status_code = 400
            try:
                # convert to dict
                # ex = literal_eval(str(ex))
                ex = str(ex)
            except:
                # fallback to string
                ex = str(ex)
        elif isinstance(ex, ObjectNotFound) or isinstance(ex, FileNotFoundError):
            status_code = 404
        elif isinstance(ex, ObjectAlreadyExists):
            status_code = 409
        elif isinstance(ex, ValueError):
            status_code = 400
        elif (
            isinstance(ex, MissingPermission)
            or isinstance(ex, WrongUsernameOrPassword)
            or isinstance(ex, SessionExpired)
        ):
            status_code = 401

        try:
            body = await r.json()
            body = orjson.dumps(body, option=orjson.OPT_INDENT_2).decode()
        except:
            try:
                body = await r.body()
                body = body.decode(errors="replace")
            except:
                body = ""

        # take the first 1k bytes of the body
        body = muty.string.make_shorter(str(body), max_len=1024)
        js = JSendResponse.error(
            req_id=req_id,
            ex=ex,
            data={
                "request": {
                    "path": r.url.path,
                    "method": r.method,
                    "query": str(r.query_params),
                    "headers": dict(r.headers),
                    "body": body,
                }
            },
            name=name,
        )
        return JSONResponse(js, status_code=status_code)

    def _add_routers(self):
        from gulp.api.server.db import router as db_router
        from gulp.api.server.enrich import router as enrich_router
        from gulp.api.server.glyph import router as glyph_router
        from gulp.api.server.highlight import router as highlight_router
        from gulp.api.server.ingest import router as ingest_router
        from gulp.api.server.link import router as link_router
        from gulp.api.server.note import router as note_router
        from gulp.api.server.object_acl import router as object_acl_router
        from gulp.api.server.operation import router as operation_router
        from gulp.api.server.query import router as query_router
        from gulp.api.server.user import router as user_router
        from gulp.api.server.user_group import router as user_group_router
        from gulp.api.server.utility import router as utility_router
        from gulp.api.server.ws import router as ws_router

        self._app.include_router(db_router)
        self._app.include_router(operation_router)
        self._app.include_router(ingest_router)
        self._app.include_router(ws_router)
        self._app.include_router(user_router)
        self._app.include_router(note_router)
        self._app.include_router(link_router)
        self._app.include_router(highlight_router)
        self._app.include_router(glyph_router)
        self._app.include_router(user_group_router)
        self._app.include_router(object_acl_router)
        self._app.include_router(utility_router)
        self._app.include_router(query_router)
        self._app.include_router(enrich_router)

    def add_api_route(self, path: str, handler: callable, **kwargs):
        """
        add a new API route, just bridges to FastAPI.add_api_route

        NOTE: if the route already exists, it will be replaced.

        Args:
            path (str): the path
            handler: the handler
            **kwargs: additional arguments to FastAPI.add_api_route
        """
        # find and remove existing route
        if self._app:
            for route in self._app.routes:
                if route.path == path:
                    self._app.router.routes.remove(route)

            # add route
            self._app.add_api_route(path, handler, **kwargs)

    def start(
        self,
        logger_file_path: str = None,
        level: int = None,
        reset_collab: bool = False,
        create_operation: str = None,
        log_to_syslog: tuple[str, str] = None,
    ):
        """
        starts the server.

        Args:
            logger_file_path (str, optional): path to the logger file for logging to file, cannot be used with log_to_syslog.
            level (int, optional): the log level.
            reset_collab (bool, optional): if True, reset the collab database
            create_operation (str, optional): the operation to be re/created (--create)
            log_to_syslog (tuple, optional): if set, logs to syslog at the specified address, facility.
                if (None, None) is passed, it defaults to ("/var/log" or "/var/run/syslog" depending what is available, "LOG_USER").
                cannot be used with logger_file_path.
        """
        self._logger_file_path = logger_file_path
        self._log_to_syslog = log_to_syslog
        self._log_level = level
        self._reset_collab = reset_collab
        self._create_operation = create_operation

        # read configuration
        cfg = GulpConfig.get_instance()
        cfg.is_integration_test()

        # dump environment variables and configuration
        MutyLogger.get_instance().info("environment variables:")
        for k, v in os.environ.items():
            MutyLogger.get_instance().info("%s=%s" % (k, v))
        cfg.dump_config()

        # init fastapi
        self._app: FastAPI = FastAPI(
            title="gULP",
            description="(generic)Universal Log Processor",
            swagger_ui_parameters={"operationsSorter": "alpha", "tagsSorter": "alpha"},
            version=self.version_string(),
            lifespan=self._lifespan_handler,
        )

        self._app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self._app.add_middleware(
            SessionMiddleware, secret_key=muty.string.generate_unique()
        )

        # add our custom exception handlers
        self._app.add_exception_handler(
            RequestValidationError, self._bad_request_exception_handler
        )
        self._app.add_exception_handler(
            RequestError, self._bad_request_exception_handler
        )
        self._app.add_exception_handler(
            JSendException, self._bad_request_exception_handler
        )

        # add routers in other modules
        self._add_routers()

        address, port = GulpConfig.get_instance().bind_to()
        MutyLogger.get_instance().info(
            "starting server at %s, port=%d, logger_file_path=%s, reset_collab=%r, create_operation=%s ...",
            address,
            port,
            logger_file_path,
            reset_collab,
            create_operation,
        )

        if cfg.enforce_https():
            MutyLogger.get_instance().info("enforcing HTTPS ...")

            path_certs: str = cfg.path_certs()
            cert_password: str = cfg.https_cert_password()
            gulp_ca_certs = muty.file.safe_path_join(path_certs, "gulp-ca.pem")
            if not os.path.exists(gulp_ca_certs):
                # use server cert as CA cert
                gulp_ca_certs = muty.file.safe_path_join(path_certs, "gulp.pem")

            ssl_cert_verify_mode: int = ssl.VerifyMode.CERT_OPTIONAL
            if cfg.enforce_https_client_certs():
                ssl_cert_verify_mode = ssl.VerifyMode.CERT_REQUIRED
                MutyLogger.get_instance().warning(
                    "HTTPS client certificates ARE ENFORCED."
                )

            # gulp server certificate
            ssl_keyfile = muty.file.safe_path_join(path_certs, "gulp.key")
            ssl_certfile = muty.file.safe_path_join(path_certs, "gulp.pem")
            MutyLogger.get_instance().info(
                "ssl_keyfile=%s, ssl_certfile=%s, cert_password=%s, ssl_ca_certs=%s, ssl_cert_verify_mode=%d",
                ssl_keyfile,
                ssl_certfile,
                cert_password,
                gulp_ca_certs,
                ssl_cert_verify_mode,
            )
            uvicorn.run(
                self._app,
                host=address,
                port=port,
                ssl_keyfile=ssl_keyfile,  # gulp server cert key
                ssl_keyfile_password=cert_password,  # key password
                ssl_certfile=ssl_certfile,  # gulp server cert
                ssl_ca_certs=gulp_ca_certs,  # gulp CA
                ssl_cert_reqs=ssl_cert_verify_mode,
            )
        else:
            # http
            MutyLogger.get_instance().warning("HTTP!")
            uvicorn.run(self._app, host=address, port=port)

    async def _dispatch_tasks(self):
        """
        Blocking task loop using Redis BLPOP for ingestion tasks.
        Waits for a task, then drains up to concurrency limit and dispatches to workers.
        """
        from gulp.api.server.ingest import run_ingest_file_task
        from gulp.api.server.query import run_query_task

        # compute total capacity: number of worker processes * per-worker concurrency
        child_concurrency: int = GulpConfig.get_instance().concurrency_num_tasks()
        num_workers: int = GulpConfig.get_instance().parallel_processes_max()
        total_capacity: int = max(1, num_workers * child_concurrency)
        limit: int = total_capacity
        MutyLogger.get_instance().info(
            "STARTING blocking task loop, max batch size=%d (num_workers=%d, child_concurrency=%d) ...",
            limit,
            num_workers,
            child_concurrency,
        )

        try:
            while True:
                try:
                    # block for first task (up to 5s) to avoid busy waiting
                    first = await GulpRedis.get_instance().task_pop_blocking(timeout=5)
                    if first is None:
                        continue  # timeout, loop again

                    batch: list[dict] = [first]
                    
                    # drain remaining up to limit-1 non-blocking
                    rest = await GulpRedis.get_instance().task_dequeue_batch(limit - 1)
                    batch.extend(rest)

                    MutyLogger.get_instance().debug(
                        "processing batch of %d task(s)", len(batch)
                    )

                    for obj in batch:
                        ttype = obj.get("task_type")
                        if ttype == "ingest":
                            await self.spawn_worker_task(run_ingest_file_task, obj)
                        elif ttype == "query":
                            # run_query_task runs in main process and spawns workers itself
                            self.spawn_bg_task(run_query_task(obj), name=f"query_task_{obj.get('req_id')}")
                        elif ttype == "rebase":
                            # rebase task: run main-process handler that will spawn a worker
                            from gulp.api.server.query_task import run_rebase_task

                            self.spawn_bg_task(
                                run_rebase_task(obj), name=f"rebase_task_{obj.get('req_id')}"
                            )

                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug("blocking task loop cancelled!")
                    break
                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)
                    await asyncio.sleep(1.0)
        finally:
            MutyLogger.get_instance().info("EXITING blocking task loop ...")

    @staticmethod
    def spawn_bg_task(coro, name: str = None) -> bool:
        """
        spawns a background task without awaiting it

        Args:
            coro (Coroutine|Task): the coroutine to spawn
            name (str, optional): the name of the task: if a task with the same name already exists in the current process, it will not spawn another one.

        Returns:
            bool: True if the task was spawned, False otherwise (e.g. a task with the same name already exists).
        """

        import inspect

        # determine how to obtain the coroutine: either coro is a callable factory
        # (not yet created) or it is an already-created coroutine object.
        is_callable = callable(coro)
        is_coro_obj = asyncio.iscoroutine(coro)
        is_task = isinstance(coro, asyncio.Task)

        def _make_coro_from_input():
            if is_callable:
                return coro()
            return coro

        async def _run_and_await():
            try:
                _coro = _make_coro_from_input()
                await _coro
            except Exception as ex:
                MutyLogger.get_instance().exception(
                    "***ERROR*** in background task: %s", ex
                )
            finally:
                MutyLogger.get_instance().debug("background task completed!")

        if name:
            # check if a task with the same name already exists in the current process
            for t in asyncio.all_tasks():
                if t.get_name() == name and not t.done():
                    MutyLogger.get_instance().warning(
                        "a task with the same name (%s) already exists, not spawning another one!",
                        name,
                    )
                    # if the caller passed an already-created coroutine object, make
                    # sure we close it to avoid leaking an un-awaited coroutine.
                    try:
                        if is_coro_obj:
                            coro.close()
                        elif is_task:
                            # cancel the task if it was somehow created already
                            coro.cancel()
                    except Exception:
                        pass
                    return False
        _ = asyncio.create_task(_run_and_await(), name=name)
        return True
    
    async def spawn_worker_task(
        self,
        func: Callable[..., Awaitable[Any]],
        *args,
        wait: bool = False,
        task_name: str = None,
        **kwargs,
    ) -> Any | None:
        """
        spawns a task in the process pool of a worker process

        NOTE: can only be called from the main process!

        Args:
            func (Callable[..., Awaitable[Any]]): the function to call
            *args: the arguments to pass to the function
            wait (bool, optional): if True, wait for the result and return it
            task_name (str, optional): the name of the task, ignored if wait=True: if a task with the same name already exists, it will not spawn another one.
            **kwargs: the keyword arguments to pass to the function

        Returns:
            Any|None: the result of the function if wait is True, None otherwise
        Raises:
            RuntimeError: if called from a worker process
            Exception: any exception raised by the function
        """
        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError("cannot spawn worker task from a worker process!")

        MutyLogger.get_instance().debug(
            "spawning worker task, func=%s, args=%s, kwargs=%s, wait=%r",
            func,
            args,
            kwargs,
            wait,
        )

        coro = GulpProcess.get_instance().process_pool.apply(
            func, args=args, kwds=kwargs
        )
        if wait:
            # wait for result
            try:
                return await coro
            except Exception as ex:
                MutyLogger.get_instance().exception(
                    "***ERROR*** in (awaited) worker task: %s", ex
                )
                raise

        # fire and forget (just use spawn_bg_task to schedule the coro)
        GulpServer.spawn_bg_task(coro, task_name)
        return

    async def _cleanup(self):
        """
        called when lifespan handler returns from yield, to cleanup the MAIN process
        """
        MutyLogger.get_instance().debug("MAIN process cleanup initiated!")

        # close process pool and clients
        try:
            await GulpConnectedSockets.get_instance().cancel_all()

            # cancel dequeue task
            if self._poll_tasks_task:
                try:
                    MutyLogger.get_instance().debug("cancelling poll_tasks task ...")
                    self._poll_tasks_task.cancel()
                    await asyncio.wait_for(self._poll_tasks_task, timeout=5.0)
                    MutyLogger.get_instance().debug(
                        "poll_tasks task cancelled successfully"
                    )
                except asyncio.TimeoutError:
                    MutyLogger.get_instance.warning(
                        "poll_tasks task cancellation timed out"
                    )
                except Exception as e:
                    pass

            # close clients in the main process
            await GulpCollab.get_instance().shutdown()
            await GulpOpenSearch.get_instance().shutdown()
            from gulp.api.redis_api import GulpRedis
            await GulpRedisBroker.get_instance().shutdown()
            await GulpRedis.get_instance().shutdown()

            # close coro pool in the main process
            await GulpProcess.get_instance().close_thread_pool()

        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            # self._kill_gulp_processes()
            MutyLogger.get_instance().info(
                "MAIN process cleanup DONE, just closing process pool is the only thing remaining ..."
            )

            # close process pool
            await GulpProcess.get_instance().close_process_pool()
            MutyLogger.get_instance().info(
                "everything shut down, we can gracefully exit."
            )

    async def _test(self):
        # to quick test code snippets, called by lifespan_handler
        """
        from gulp.api.opensearch_api import GulpOpenSearch
        api = GulpOpenSearch.get_instance()
        p = await api.index_template_get("test_operation")
        MutyLogger.get_instance().info(orjson.dumps(p, option=orjson.OPT_INDENT_2).decode())
        """
        return

    async def _lifespan_handler(self, app: FastAPI):
        """
        fastapi lifespan handler
        """
        from gulp.api.opensearch_api import GulpOpenSearch

        MutyLogger.get_instance().info("gULP main server process is starting!")

        main_process = GulpProcess.get_instance()

        # load configuration
        GulpConfig.get_instance()

        first_run: bool = self._check_first_run()

        # check for db integrity
        try:
            await GulpCollab.get_instance().init(main_process=True)
        except SchemaMismatch as ex:
            MutyLogger.get_instance().warning(
                "collab database schema mismatch, forcing recreate!\n%s", ex
            )
            # force reset
            self._reset_collab = True

        if first_run and not self._create_operation:
            # force creating a default operation on first run
            self._create_operation = "test_operation"

        MutyLogger.get_instance().warning(
            "******** first_run=%r, _reset_collab=%r, _create_operation=%s ********",
            first_run,
            self._reset_collab,
            self._create_operation,
        )

        # initialize the opensearch client
        GulpOpenSearch.get_instance()

        # initialize the redis client
        from gulp.api.redis_api import GulpRedis
        GulpRedis.get_instance().initialize(self._server_id)

        # initialize Redis pub/sub for worker->main process and instance<->instance communication
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.initialize()

        # initialize collab database and create operation if needed
        try:
            if self._reset_collab:
                from gulp.api.server.db import db_reset

                # reset the collab database and recreate tables from scratch (also deletes all data in operations)
                await db_reset()

            if self._create_operation:
                from gulp.api.collab.operation import GulpOperation

                async with GulpCollab.get_instance().session() as sess:
                    await GulpOperation.create_operation(
                        sess, self._create_operation, "admin", set_default_grants=True
                    )
        except Exception as ex:
            if first_run:
                # allow restart on first run
                self._reset_first_run()
            raise

        # finish initializing main process, will spawn workers as well
        await main_process.finish_initialization(
            self._server_id,
            log_level=self._log_level,
            logger_file_path=self._logger_file_path,
            log_to_syslog=self._log_to_syslog,
        )

        if __debug__:
            # to test some snippet with gulp freshly initialized
            await self._test()

        # start the dequeue task for long running tasks
        self._poll_tasks_task = asyncio.create_task(self._dispatch_tasks())

        # wait for termination
        try:
            self._lifespan_task = asyncio.current_task()
            yield
        except asyncio.CancelledError:
            MutyLogger.get_instance().warning(
                "CancelledError caught in _lifespan handler!"
            )

        # cleaning up will be done through _cleanup called via atexit
        MutyLogger.get_instance().info("gulp shutting down!")
        
        if GulpConfig.get_instance().stats_delete_pending_on_shutdown():
            # delete pending stats
            await GulpRequestStats.purge_ongoing_requests()

        # unload extension plugins
        await self._unload_extension_plugins()

        # cleanup main process
        await self._cleanup()

    def _reset_first_run(self) -> None:
        """
        deletes the ".first_run_done" file in the config directory.
        """
        config_directory = GulpConfig.get_instance().path_working_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            muty.file.delete_file_or_dir(check_first_run_file)
            MutyLogger.get_instance().warning("deleted: %s", check_first_run_file)

    def _check_first_run(self) -> bool:
        """
        check if this is the first run of the server.

        Returns:
            bool: True if this is the first run, False otherwise.
        """
        # check if this is the first run
        config_directory = GulpConfig.get_instance().path_working_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            MutyLogger.get_instance().debug(
                "NOT FIRST RUN, first run file exists: %s", check_first_run_file
            )
            return False

        # create firstrun file
        MutyLogger.get_instance().warning(
            "FIRST RUN! first run file does not exist: %s", check_first_run_file
        )
        with open(check_first_run_file, "w", encoding="utf-8") as f:
            f.write("gulp!")
        return True
        return True
