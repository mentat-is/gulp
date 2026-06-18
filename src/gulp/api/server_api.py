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
from contextlib import suppress
import os
import ssl
import time
from typing import Any, Awaitable, Callable, Sequence

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
from starlette.middleware.sessions import SessionMiddleware

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.s3_api import GulpS3
from gulp.api.server.structs import (
    TASK_TYPE_ENRICH,
    TASK_TYPE_EXTERNAL_QUERY,
    TASK_TYPE_INGEST,
    TASK_TYPE_QUERY,
    TASK_TYPE_REBASE,
)
from gulp.api.ws_api import GulpConnectedSockets, GulpRedisBroker
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import ObjectAlreadyExists, ObjectNotFound
from gulp.api.prometheus_api import GulpMetrics, cleanup_prometheus
from gulp import __version__, commit_id


class GulpServer:
    """
    manages the gULP REST server.
    """

    _instance: "GulpServer" = None

    def __init__(self):
        self._initialized: bool = True
        self._app: FastAPI = None
        self.server_id: str = muty.string.generate_unique()
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

    def version_string(self) -> str:
        """
        returns the version string

        Returns:
            str: version string
        """
        ver: str = f"{__version__} ({commit_id})" if commit_id else __version__
        return "gulp_%s, muty_%s" % (
            ver,
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

        print_error: bool = True
        # take the first 1k bytes of the body
        body = muty.string.make_shorter(str(body), max_len=1024)
        if "/request_get_by_id" in r.url.path and status_code == 404:
            # suppress printing of 404s for request_get_by_id to avoid log spam, since it's frequently polled by the client for pending requests
            print_error = False
        # print("***** print_response=%r, r.url.path=%s, status_code=%d, ex=%s" % (print_error, r.url.path, status_code, ex))
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
            print_response=print_error,
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
        from gulp.api.server.storage import router as storage_router

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
        self._app.include_router(storage_router)

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

        # set up Prometheus metrics (if enabled)
        from gulp.api.prometheus_api import setup_prometheus

        setup_prometheus(self._app)

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
            # write pidfile so other invocations can stop this instance
            try:
                pidfile = muty.file.safe_path_join(
                    GulpConfig.get_instance().path_working_dir(),
                    "gulp%s.pid" % (os.getpid()),
                )
                with open(pidfile, "w", encoding="utf-8") as f:
                    f.write(str(os.getpid()))
                MutyLogger.get_instance().info("wrote pidfile=%s", pidfile)
            except Exception:
                MutyLogger.get_instance().exception("failed to write pidfile")

            uvicorn.run(self._app, host=address, port=port)

    @staticmethod
    def _task_lease_refresh_interval() -> float:
        """Return the lease refresh interval in seconds."""
        interval_ms = GulpRedis.TASK_LEASE_REFRESH_INTERVAL_MS
        if interval_ms <= 0:
            interval_ms = max(1_000, GulpRedis.TASK_AUTOCLAIM_IDLE_MS // 3)
        return max(0.001, interval_ms / 1000.0)

    async def _refresh_task_lease_until_done(self, task: dict) -> None:
        """Refresh a Redis stream task lease until cancelled by the dispatcher."""
        interval = self._task_lease_refresh_interval()
        while True:
            await asyncio.sleep(interval)
            await GulpRedis.get_instance().task_refresh_lease(task)

    async def _await_task_with_lease(self, task: dict, awaitable: Awaitable[Any]) -> Any:
        """Await a task while keeping its Redis stream pending entry fresh."""
        lease_task = asyncio.create_task(self._refresh_task_lease_until_done(task))
        try:
            timeout_sec = self._task_execution_timeout_sec(task)
            if timeout_sec > 0:
                return await asyncio.wait_for(awaitable, timeout=timeout_sec)
            return await awaitable
        finally:
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await lease_task

    @staticmethod
    def _task_execution_timeout_sec(task: dict) -> int:
        """Return optional per-attempt execution timeout for a queued task."""
        value = task.get("__task_timeout_sec__")
        if value is None:
            params = task.get("params") if isinstance(task.get("params"), dict) else {}
            value = params.get("__task_timeout_sec__")
        if value is None:
            value = GulpConfig.get_instance().redis_task_execution_timeout_sec()
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    async def _mark_failed_task_stats_failed(
        self, task: dict, reason: str
    ) -> None:
        """Mark an existing request stats object failed after terminal task failure."""
        req_id = task.get("req_id")
        if not req_id:
            return

        try:
            async with GulpCollab.get_instance().session() as sess:
                stats = await GulpRequestStats.get_by_id(
                    sess,
                    req_id,
                    throw_if_not_found=False,
                )
                if not stats:
                    MutyLogger.get_instance().warning(
                        "failed task has no request stats: task_type=%s req_id=%s",
                        task.get("task_type"),
                        req_id,
                    )
                    return

                if stats.status != GulpRequestStatus.ONGOING.value:
                    MutyLogger.get_instance().warning(
                        "failed task request stats already terminal: task_type=%s req_id=%s status=%s",
                        task.get("task_type"),
                        req_id,
                        stats.status,
                    )
                    return

                await stats.set_finished(
                    sess,
                    status=GulpRequestStatus.FAILED,
                    user_id=task.get("user_id"),
                    ws_id=task.get("ws_id"),
                    errors=[reason],
                )
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to mark task request stats failed: task_type=%s req_id=%s",
                task.get("task_type"),
                req_id,
            )

    def _run_claimed_task(self, task: dict) -> Awaitable[Any]:
        """Return the handler awaitable for a claimed queued task."""
        from gulp.api.server.db import run_rebase_task
        from gulp.api.server.enrich import run_enrich_task
        from gulp.api.server.ingest import run_ingest_file_task
        from gulp.api.server.query import run_query_task

        ttype = task.get("task_type")
        if ttype == TASK_TYPE_INGEST:
            return self.spawn_worker_task(run_ingest_file_task, task, wait=True)
        if ttype == TASK_TYPE_ENRICH:
            return run_enrich_task(task)
        if ttype == TASK_TYPE_QUERY or ttype == TASK_TYPE_EXTERNAL_QUERY:
            return run_query_task(task)
        if ttype == TASK_TYPE_REBASE:
            return run_rebase_task(task)

        raise ValueError(f"unknown task_type: {ttype!r}")

    async def _dispatch_claimed_tasks(
        self,
        redis_inst: GulpRedis,
        tasks: Sequence[dict],
        *,
        source: str,
    ) -> None:
        """Execute and finalize queued tasks."""
        task_coros: list[tuple[dict, Awaitable[Any]]] = []
        for obj in tasks:
            try:
                task_coros.append((obj, self._run_claimed_task(obj)))
            except ValueError as ex:
                reason = str(ex)
                MutyLogger.get_instance().warning(
                    "unknown task_type while processing %s task: %r",
                    source,
                    obj.get("task_type"),
                )
                failure_state = await redis_inst.task_mark_failed(obj, reason)
                if failure_state == "failed":
                    await self._mark_failed_task_stats_failed(obj, reason)

        if not task_coros:
            return

        task_results = await asyncio.gather(
            *[
                self._await_task_with_lease(obj, coro)
                for obj, coro in task_coros
            ],
            return_exceptions=True,
        )
        for (obj, _), res in zip(task_coros, task_results):
            if isinstance(res, Exception) or res is False:
                MutyLogger.get_instance().exception(
                    "error while awaiting %s task for task_type=%s req_id=%s: %s",
                    source,
                    obj.get("task_type"),
                    obj.get("req_id"),
                    res,
                )
                reason = repr(res)
                failure_state = await redis_inst.task_mark_failed(
                    obj,
                    reason,
                )
                if failure_state == "failed":
                    await self._mark_failed_task_stats_failed(obj, reason)
                continue

            await redis_inst.task_ack_delete(obj)

    async def _dispatch_tasks(self):
        """
        Blocking task loop for long running tasks that need workers.

        Keeps up to the configured capacity in flight instead of waiting for a
        whole drained batch to finish before dequeuing more work.
        """
        # compute total capacity: number of worker processes * per-worker concurrency
        child_concurrency: int = GulpConfig.get_instance().concurrency_num_tasks()
        num_workers: int = GulpConfig.get_instance().parallel_processes_max()
        total_capacity: int = max(1, num_workers * child_concurrency)
        limit: int = total_capacity
        MutyLogger.get_instance().info("STARTING dispatch_tasks loop ...")

        in_flight: set[asyncio.Task[None]] = set()
        def _consume_dispatch_result(task: asyncio.Task[None]) -> None:
            in_flight.discard(task)
            with suppress(asyncio.CancelledError):
                try:
                    task.result()
                except Exception as ex:
                    MutyLogger.get_instance().exception(
                        "error while dispatching queued task: %s", ex
                    )

        def _start_dispatch_task(
            redis_inst: GulpRedis,
            task_obj: dict,
            *,
            source: str,
        ) -> None:
            task = asyncio.create_task(
                self._dispatch_claimed_tasks(redis_inst, [task_obj], source=source)
            )
            in_flight.add(task)
            task.add_done_callback(_consume_dispatch_result)

        try:
            while True:
                try:
                    redis_inst = GulpRedis.get_instance()
                    now = time.monotonic()
                    last_cleanup = getattr(
                        redis_inst, "_last_stale_pending_cleanup_time", 0.0
                    )
                    if now - last_cleanup >= 30.0:
                        redis_inst._last_stale_pending_cleanup_time = now
                        try:
                            failed = await redis_inst.task_drop_stale_pending()
                            if failed:
                                MutyLogger.get_instance().warning(
                                    "dropped %d stale pending task(s)",
                                    len(failed),
                                )
                                reason = "task abandoned by worker"
                                for obj in failed:
                                    await self._mark_failed_task_stats_failed(
                                        obj, reason
                                    )
                        except Exception:
                            MutyLogger.get_instance().exception(
                                "error during stale pending task cleanup"
                            )

                    capacity = limit - len(in_flight)
                    if capacity > 0:
                        # block briefly while tasks are running so new arrivals can
                        # fill idle slots without waiting for current work to finish.
                        first = await redis_inst.task_pop_blocking(
                            timeout=1 if in_flight else 5
                        )
                        if first is not None:
                            batch: list[dict] = [first]
                            if capacity > 1:
                                batch.extend(
                                    await redis_inst.task_dequeue_batch(capacity - 1)
                                )
                            MutyLogger.get_instance().debug(
                                "dispatching %d task(s), in_flight=%d, capacity=%d",
                                len(batch),
                                len(in_flight),
                                limit,
                            )
                            for obj in batch:
                                _start_dispatch_task(
                                    redis_inst,
                                    obj,
                                    source="queued",
                                )
                        elif not in_flight:
                            # timeout; avoid tight-looping if blocking returns without a task
                            await asyncio.sleep(0.1)

                    if len(in_flight) >= limit:
                        await asyncio.wait(
                            in_flight,
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug("blocking task loop cancelled!")
                    break
                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)
                    await asyncio.sleep(1.0)
        finally:
            pending_dispatches = tuple(in_flight)
            for task in pending_dispatches:
                task.cancel()
            if pending_dispatches:
                await asyncio.gather(*pending_dispatches, return_exceptions=True)
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
                MutyLogger.get_instance().debug("background task name=%s completed!", name)

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
            muty.string.make_shorter(
                "spawning worker task, func=%s, args=%s, kwargs=%s, wait=%r"
                % (func, args, kwargs, wait),
                max_len=260,
            )
        )

        coro = GulpProcess.get_instance().process_pool.apply(
            func, args=args, kwds=kwargs
        )

        if GulpConfig.get_instance().prometheus_enabled():
            # update Prometheus counter
            try:
                GulpMetrics.worker_tasks_spawned_total.inc()
            except Exception:
                pass

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
        # NOTE: may be appropriate to directly call process_pool.queue_work instead ? from testing, it works as expected and
        # we don't have the minimal overhead of an extra task in the main process (it just await for apply() to complete, which in turn awaits for the coroutine supplied to the pool in a busy loop).
        # but, using the process_pool.queue_work function directly we would loose task name check and exception handling...
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
                    MutyLogger.get_instance().warning(
                        "poll_tasks task cancelled successfully"
                    )
                except asyncio.TimeoutError:
                    MutyLogger.get_instance().warning(
                        "poll_tasks task cancellation timed out"
                    )
                except Exception as e:
                    pass

            # close thread pool in the main process
            await GulpProcess.get_instance().close_thread_pool()

            # close process pool
            await GulpProcess.get_instance().close_process_pool()

            # close clients in the main process
            await GulpCollab.get_instance().shutdown()
            await GulpOpenSearch.get_instance().shutdown()
            await GulpS3.get_instance().shutdown()

            # remove this server's consumer entries from Redis streams
            from gulp.api.redis_api import GulpRedis

            try:
                await GulpRedis.get_instance().cleanup_consumers_for_server(
                    self.server_id
                )
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to cleanup redis consumers for server %s", self.server_id
                )
            await GulpRedisBroker.get_instance().shutdown()
            await GulpRedis.get_instance().shutdown()

        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            MutyLogger.get_instance().info(
                "MAIN process cleanup DONE (server_id=%s), just closing process pool is the only thing remaining ...",
                self.server_id,
            )

            MutyLogger.get_instance().info(
                "everything shut down, we can gracefully exit."
            )
            # remove pidfile if exists
            try:
                pidfile = muty.file.safe_path_join(
                    GulpConfig.get_instance().path_working_dir(),
                    "gulp%d.pid" % (os.getpid()),
                )
                if os.path.exists(pidfile):
                    try:
                        os.remove(pidfile)
                        MutyLogger.get_instance().info("removed pidfile=%s", pidfile)
                    except Exception:
                        MutyLogger.get_instance().exception(
                            "failed to remove pidfile=%s", pidfile
                        )
            except Exception:
                pass

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

        GulpRedis.get_instance().initialize(self.server_id)

        # initialize Redis pub/sub for worker->main process and instance<->instance communication
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.initialize()

        # initialize collab database and create operation if needed
        try:
            if self._reset_collab:
                from gulp.api.server.db import db_reset

                # reset the collab database and recreate tables from scratch (also deletes all data in operations)
                await db_reset()

                # cleanup redis as well
                await GulpRedis.get_instance().cleanup_redis()

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
            self.server_id,
            log_level=self._log_level,
            logger_file_path=self._logger_file_path,
            log_to_syslog=self._log_to_syslog,
        )

        if __debug__:
            # to test some snippet with gulp freshly initialized
            await self._test()

        cfg = GulpConfig.get_instance()
        configured_tasks: int = getattr(cfg, "_config", {}).get(
            "concurrency_num_tasks", 0
        )
        child_concurrency: int = cfg.concurrency_num_tasks()
        num_workers: int = cfg.parallel_processes_max()
        total_concurrency: int = max(1, num_workers * child_concurrency)
        if cfg.concurrency_adaptive_num_tasks():
            base_tasks = configured_tasks if configured_tasks > 0 else 16
            opensearch_nodes = max(1, cfg.concurrency_opensearch_num_nodes())
            postgres_nodes = max(1, cfg.concurrency_postgres_num_nodes())
            scaled_tasks = base_tasks * opensearch_nodes * postgres_nodes
            MutyLogger.get_instance().debug(
                "startup concurrency: queued worker operations<=%d "
                "(workers=%d, per_worker=%d, adaptive=true: "
                "max(8, min(cap_per_process=%d, base_tasks=%d * "
                "opensearch_nodes=%d * postgres_nodes=%d = %d)))",
                total_concurrency,
                num_workers,
                child_concurrency,
                cfg.concurrency_tasks_cap_per_process(),
                base_tasks,
                opensearch_nodes,
                postgres_nodes,
                scaled_tasks,
            )
        else:
            MutyLogger.get_instance().debug(
                "startup concurrency: queued worker operations<=%d "
                "(workers=%d, per_worker=%d, adaptive=false: "
                "configured_concurrency_num_tasks=%d)",
                total_concurrency,
                num_workers,
                child_concurrency,
                configured_tasks,
            )

        # start the dequeue task for long running tasks
        self._poll_tasks_task = asyncio.create_task(self._dispatch_tasks())

        # start Prometheus gauge collection loop (main process only)
        if GulpConfig.get_instance().prometheus_enabled():
            metrics = GulpMetrics.get_instance()
            metrics.server_info.info(
                {
                    "version": self.version_string(),
                    "server_id": self.server_id,
                }
            )
            await metrics.start_collect_loop()

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

        # stop Prometheus gauge collection
        if GulpConfig.get_instance().prometheus_enabled():
            await GulpMetrics.get_instance().stop_collect_loop()
            cleanup_prometheus()

        if GulpConfig.get_instance().stats_delete_pending_on_shutdown():
            # delete pending stats created by THIS instance only
            await GulpRequestStats.purge_ongoing_requests(self.server_id)

        # unload extension plugins
        await self._unload_extension_plugins()

        # cleanup main process
        await self._cleanup()
        MutyLogger.get_instance().info(
            "MAIN process cleanup complete, exiting lifespan handler ..."
        )

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
