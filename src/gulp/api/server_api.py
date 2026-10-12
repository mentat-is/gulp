"""
This module manages the REST API server for the gULP application.

It provides a singleton server instance that handles API routes, request validation,
exception handling, and server lifecycle. The HTTP application server integrates
with various gULP components like WebSockets,
collaborative features, and plugins.

The module also handles server initialization, shutdown procedures, and restart
capabilities for the application.
"""

import asyncio
from contextlib import suppress
import logging
import os
import socket
import ssl
import time
import urllib.error
import urllib.request
from http.client import HTTPException
from typing import Any, Awaitable, Callable, Sequence
from urllib.parse import urlsplit

import muty.file
import muty.os
import muty.string
import orjson
import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from opensearchpy import RequestError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from starlette.middleware.sessions import SessionMiddleware

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.s3_api import GulpS3
from gulp.api.server.request_observer import (
    GulpRequestObserver,
    RequestObserverDispatcher,
    RequestObservers,
)
from gulp.api.server.structs import (
    TASK_TYPE_ENRICH,
    TASK_TYPE_EXTERNAL_QUERY,
    TASK_TYPE_INGEST,
    TASK_TYPE_QUERY,
    TASK_TYPE_REBASE,
)
from gulp.api.ws_api import GulpConnectedSockets, GulpRedisBroker
from gulp.config import GulpConfig
from gulp.glug import Glug
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import (
    ObjectAlreadyExists,
    ObjectNotFound,
    PaginationSnapshotExpired,
)
from gulp.api.prometheus_api import GulpMetrics, cleanup_prometheus
from gulp import format_version


class GulpServer:
    """
    manages the gULP REST server.
    """

    _instance: "GulpServer" = None

    def __init__(self):
        self._initialized: bool = True
        self._app: FastAPI = None
        self.server_id: str = muty.string.generate_unique()
        self._log_level: int = logging.INFO
        self._cli_log_level: int | None = None
        self._log_file: str = None
        self._reset_collab: bool = False
        self._create_operation: str = None
        self._lifespan_task: asyncio.Task = None
        self._poll_tasks_task: asyncio.Task = None
        self._extension_plugins: list[GulpPluginBase] = []
        self._request_observers: RequestObservers = RequestObservers()
        self._last_opensearch_search_rejected: int | None = None
        self._opensearch_search_capacity: int | None = None

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
        return format_version(prefix="gulp_")

    def cli_log_level(self) -> int | None:
        """
        the log level explicitly forced on the command line, for extension plugins that keep
        their own saved level: an explicit --log-level wins over the saved one.

        Returns:
            int | None: the --log-level value, or None when it was not given (INFO applies then)
        """
        return self._cli_log_level

    def extension_plugins(self) -> list[GulpPluginBase]:
        """
        Returns:
            list[GulpPluginBase]: the extension plugins loaded in this (main) process, e.g. for
                one extension to find and notify another
        """
        return list(self._extension_plugins)

    async def _unload_extension_plugins(self) -> None:
        """
        unload extension plugins
        """
        if self._extension_plugins:
            Glug.get_instance().debug(
                f"unloading {len(self._extension_plugins)} extension plugins"
            )
            for p in self._extension_plugins:
                try:
                    await p.unload()
                except Exception:
                    Glug.get_instance().exception(
                        f"failed to unload extension plugin {p.name}"
                    )
            self._extension_plugins = []
        # whatever a plugin left registered must not outlive the plugin
        self._request_observers.clear()

    async def _load_extension_plugins(self) -> None:
        """
        load available extension plugins
        """
        Glug.get_instance().debug("loading extension plugins")

        # default
        p = GulpConfig.get_instance().path_plugins_default()
        path_extension = os.path.join(p, "extension")
        files: list[str] = await muty.file.list_directory_async(path_extension, "*.py*")
        Glug.get_instance().debug(
            f"found {len(files)} extensions in default path: {path_extension}"
        )

        # extras, if any
        p = GulpConfig.get_instance().path_plugins_extra()
        if p:
            path_extension = os.path.join(p, "extension")
            ff: list[str] = await muty.file.list_directory_async(
                path_extension, "*.py*"
            )
            Glug.get_instance().debug(
                f"found {len(ff)} extensions in extra path: {path_extension}"
            )
            files.extend(ff)

        # Remove non .py/.pyc files, prefer compiled bytecode, and avoid loading
        # both source and bytecode for the same extension basename.
        files = [
            f
            for f in files
            if (f.endswith(".py") or f.endswith(".pyc")) and "__init__.py" not in f
        ]
        files = sorted(
            files,
            key=lambda x: (
                os.path.splitext(os.path.basename(x))[0].lower(),
                0 if x.endswith(".pyc") else 1,
                x.lower(),
            ),
        )
        seen: set[str] = set()

        count: int = 0
        for f in files:
            basename = os.path.splitext(os.path.basename(f))[0].lower()
            if basename in seen:
                Glug.get_instance().warning(
                    f"skipping extension plugin {f}, already loaded same basename"
                )
                continue
            seen.add(basename)

            # load
            try:
                p = await GulpPluginBase.load(f, extension=True)
            except Exception:
                # plugin failed to load
                Glug.get_instance().exception(
                    f"failed to load (extension) plugin file: {f}"
                )
                continue
            self._extension_plugins.append(p)
            count += 1

        Glug.get_instance().debug(f"loaded {count} extension plugins")

    async def _bad_request_exception_handler(
        self, r: Request, ex: Any
    ) -> JSendResponse:
        """
        turns an exception raised while serving a request into the JSend error
        response and its HTTP status code, and logs it (see _log_api_error).

        the response says as little as the caller needs: the error name and message
        for a client error (4xx), a generic message with the req_id for a server
        error (5xx). the traceback and the request (headers, body) never go back to
        the client: they are logged.
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
            # which parameters are wrong and why, without their values (str(ex) would
            # echo them, a rejected password included, and the handler's source file)
            ex = self._validation_error_message(ex)
        elif isinstance(ex, PaginationSnapshotExpired):
            status_code = 410
        elif isinstance(ex, ObjectNotFound) or isinstance(ex, FileNotFoundError):
            status_code = 404
        elif isinstance(ex, ObjectAlreadyExists):
            status_code = 409
        elif isinstance(ex, ValueError):
            status_code = 400
        elif isinstance(ex, MissingPermission):
            status_code = 403
        elif isinstance(ex, WrongUsernameOrPassword) or isinstance(ex, SessionExpired):
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
        request_info = {
            "path": r.url.path,
            "method": r.method,
            "query": str(r.query_params),
            "headers": dict(r.headers),
            "body": body,
        }
        if status_code >= 500:
            # not the caller's mistake, and nothing the caller can act upon: the
            # details stay in the log, found again by req_id
            error = {
                "name": "InternalError",
                "msg": "internal error, see the server logs"
                + (f" (req_id={req_id})" if req_id else ""),
            }
        else:
            error = {
                "name": name,
                "msg": muty.string.make_shorter(str(ex), max_len=1024),
            }
        js = JSendResponse.error(
            req_id=req_id,
            ex=error,
            # the response is not logged as a whole (it used to be, at ERROR, for
            # every failed request): _log_api_error writes one readable line
            print_response=False,
        )
        if print_error:
            self._log_api_error(status_code, name, ex, request_info, req_id)
        return JSONResponse(js, status_code=status_code)

    @staticmethod
    def _validation_error_message(ex: RequestValidationError) -> str:
        """
        Args:
            ex (RequestValidationError): the validation error raised by FastAPI or by a
                parameter dependency

        Returns:
            str: one "location: reason" per rejected parameter, e.g.
                "query.user_id: String should match pattern '...'"
        """
        problems: list[str] = []
        for error in ex.errors():
            if isinstance(error, dict):
                location = ".".join(str(part) for part in error.get("loc") or ())
                reason = str(error.get("msg") or error.get("type") or "invalid")
                problems.append(f"{location}: {reason}" if location else reason)
            else:
                problems.append("invalid parameter")
        return "; ".join(problems) or "invalid request"

    @staticmethod
    def _log_api_error(
        status_code: int, name: str, ex: Any, request_info: dict, req_id: str | None
    ) -> None:
        """
        logs a failed request as one line: method, path, status, error name and message.

        A client error (4xx: wrong password, missing permission, bad parameters,
        not found) is the caller's mistake, logged at WARNING without a traceback;
        a server error (5xx) is logged at ERROR with its traceback. The request
        details (query, headers, body) go to detail=, so only in dev_mode.

        Args:
            status_code (int): the HTTP status code returned.
            name (str): the error (exception class) name.
            ex (Any): the exception, or its text.
            request_info (dict): path, method, query, headers and body of the request.
            req_id (str | None): the request id, added to the log context.
        """
        message = (
            f"{request_info['method']} {request_info['path']} -> {status_code} "
            f"{name}: {muty.string.make_shorter(str(ex), max_len=300)}"
        )
        if status_code >= 500:
            Glug.get_instance().error(
                message,
                exc_info=ex if isinstance(ex, BaseException) else None,
                detail=request_info,
                req_id=req_id,
            )
        else:
            Glug.get_instance().warning(message, detail=request_info, req_id=req_id)

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

        Args:
            path (str): the path
            handler: the handler
            **kwargs: additional arguments to FastAPI.add_api_route
        """
        # add route
        if self._app:
            self._app.add_api_route(path, handler, **kwargs)

    def add_request_observer(self, observer: GulpRequestObserver) -> None:
        """
        registers a read-only observer of the http requests on behalf of an extension
        plugin (to be called in post_init, main process only).

        the observer is told about every http request, when it arrives and when it is
        answered, error responses and requests no route matches included. it gets copies:
        it cannot change or block a request, and an exception it raises is logged and does
        not affect the request. see gulp.api.server.request_observer.

        Args:
            observer (GulpRequestObserver): the observer

        Throws:
            RuntimeError: if not called in the main process
        """
        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError("request observers are main-process-only")
        if not self._request_observers.add(observer):
            Glug.get_instance().warning(
                f"request observer {observer!r} already registered, not adding"
            )
            return
        Glug.get_instance().debug(f"registered request observer {observer!r}")

    def remove_request_observer(self, observer: GulpRequestObserver) -> None:
        """
        removes an observer registered with add_request_observer (to be called in the
        plugin's unload); does nothing if it is not registered.

        Args:
            observer (GulpRequestObserver): the same object passed to add_request_observer
        """
        if self._request_observers.remove(observer):
            Glug.get_instance().debug(f"removed request observer {observer!r}")

    def start(
        self,
        level: int = None,
        reset_collab: bool = False,
        create_operation: str = None,
        log_file: str = None,
        cli_options: dict[str, str] = None,
    ):
        """
        starts the server.

        Args:
            level (int, optional): the log level given on the command line (--log-level), or None
                when not given: INFO applies, and cli_log_level() reports None.
            reset_collab (bool, optional): if True, reset the collab database
            create_operation (str, optional): the operation to be re/created (--create)
            log_file (str, optional): plain-text file receiving the same lines as stdout, shared by
                every gulp process (--log-file). Defaults to None (stdout only).
            cli_options (dict[str, str], optional): option -> effective value (marked
                " (default)" when not given) from the command line, logged at DEBUG.
                Defaults to None.

        Returns:
            bool: False if a required service is unreachable and the server was not
                started (the reason is already logged), True once the server exits.
        """
        self._cli_log_level = level
        self._log_level = level or logging.INFO
        self._log_file = log_file
        self._reset_collab = reset_collab
        self._create_operation = create_operation

        # bootstrap glug for the main process here, as the very first thing: every
        # log call below (including the rest of this synchronous method, which
        # runs before uvicorn.run() ever triggers _lifespan_handler) must already
        # see the requested --log-level, not the bare-instance INFO
        # default a call site gets when nothing has configured Glug yet.
        # _lifespan_handler reconfigures it again with the same values once the
        # event loop is running — cheap and idempotent, kept as a safety net for
        # any bare Glug.get_instance() a migrated call site may have created
        # between this point and then.
        Glug.get_instance(
            name="glug",
            log_file=self._log_file,
            level=self._log_level,
            process_role="main",
            reconfigure=True,
        )

        # logged only now, since glug's level is unknown before the bootstrap above
        for option, value in (cli_options or {}).items():
            Glug.get_instance().debug(f"command line option {option}={value}")

        # read configuration
        cfg = GulpConfig.get_instance()

        # dump environment variables and configuration: env vars routinely carry secrets
        # (DB passwords, API keys, embedded-credential URLs), so this is dev_mode-only via
        # detail= (decision 16) instead of the previous unconditional per-var INFO dump.
        Glug.get_instance().debug(
            f"environment: {len(os.environ)} variables set", detail=dict(os.environ)
        )
        cfg.dump_config()

        # fail fast, before uvicorn starts, if a backing service is down: the same
        # failure inside _lifespan_handler can only surface as uvicorn's own
        # startup traceback plus a cascade of cleanup errors, burying the cause.
        if not self._check_required_services():
            return False

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

        # added last, so it is the outermost: the observers registered later by
        # extension plugins (see add_request_observer) are told about every request
        self._app.add_middleware(
            RequestObserverDispatcher, registry=self._request_observers
        )

        # add routers in other modules
        self._add_routers()

        address, port = GulpConfig.get_instance().bind_to()
        Glug.get_instance().info(
            f"starting server at {address}, port={port}, reset_collab={reset_collab!r}, "
            f"create_operation={create_operation}"
        )

        # write pidfile so other invocations can stop this instance
        try:
            pidfile = muty.file.safe_path_join(
                cfg.path_working_dir(),
                "gulp%s.pid" % (os.getpid()),
            )
            with open(pidfile, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
            Glug.get_instance().info(f"wrote pidfile={pidfile}")
        except Exception:
            Glug.get_instance().exception("failed to write pidfile")

        # route uvicorn's own startup/shutdown/error banner through glug instead of its
        # own default handler, so it looks like every other gulp log line — log_config=None
        # is required, otherwise uvicorn's own logging.config.dictConfig() call inside
        # run() would overwrite the handlers just attached here.
        for _uvicorn_logger_name in ("uvicorn", "uvicorn.error"):
            Glug.route_stdlib_logger(_uvicorn_logger_name)

        uvicorn.run(
            self._app,
            host=address,
            port=port,
            ws_ping_interval=None,
            log_config=None,
        )
        return True

    @staticmethod
    def _check_required_services(timeout_sec: float = 3.0) -> bool:
        """
        checks that the services the main process connects to at startup accept
        TCP connections, logging an error with a hint for each one that does not.

        OpenSearch opens its port well before it can serve requests, so it also
        gets an HTTP probe; PostgreSQL and Redis only open their TCP port once
        they are ready, so a connect is enough for them.

        this is a reachability check only (credentials, database existence and
        schema are still verified by the regular startup path).

        Args:
            timeout_sec (float, optional): connect timeout per service. Defaults to 3.0.

        Returns:
            bool: True if every required service is reachable.
        """
        cfg = GulpConfig.get_instance()
        pg_url = make_url(cfg.postgres_url())
        os_url = urlsplit(cfg.opensearch_url())
        redis_url = urlsplit(cfg.redis_url())

        # (name, host, port, config key, environment variable)
        services: list[tuple[str, str, int, str, str]] = [
            (
                "PostgreSQL",
                pg_url.host or "localhost",
                pg_url.port or 5432,
                "postgres_url",
                "GULP_POSTGRES_URL",
            ),
            (
                "OpenSearch",
                os_url.hostname or "localhost",
                os_url.port or (443 if os_url.scheme == "https" else 9200),
                "opensearch_url",
                "GULP_OPENSEARCH_URL",
            ),
            (
                "Redis",
                redis_url.hostname or "localhost",
                redis_url.port or 6379,
                "redis_url",
                "GULP_REDIS_URL",
            ),
        ]

        missing: list[str] = []
        for name, host, port, cfg_key, env_var in services:
            try:
                with socket.create_connection((host, port), timeout=timeout_sec):
                    pass
            except OSError as ex:
                missing.append(name)
                # only host:port is logged, the configured URLs may embed credentials
                Glug.get_instance().error(
                    f"{name} is not reachable at {host}:{port} ({ex})",
                    hint=f"make {name} available to gulp: start the "
                    "backing services (e.g. `docker compose --profile dev up -d`) "
                    f"or point {cfg_key} (or {env_var}) to a reachable server",
                )
                continue

            if name == "OpenSearch":
                reason = GulpServer._opensearch_not_ready(
                    os_url.scheme or "http", host, port, timeout_sec
                )
                if reason:
                    missing.append(name)
                    Glug.get_instance().error(
                        f"{name} at {host}:{port} accepts connections but is not "
                        f"ready yet ({reason})",
                        hint=f"{name} is probably still starting up: wait until it "
                        "answers (e.g. `curl http://localhost:9200`) and start gulp "
                        "again, or check its container logs if it never comes up",
                    )

        if missing:
            Glug.get_instance().critical(
                f"gulp cannot start, required services unreachable: {', '.join(missing)}"
            )
            return False
        return True

    @staticmethod
    def _opensearch_not_ready(
        scheme: str, host: str, port: int, timeout_sec: float
    ) -> str | None:
        """
        probes OpenSearch over HTTP, without credentials.

        any HTTP answer but 503 means it is serving requests (a 401/403 is fine here,
        credentials are checked later by the real client); a dropped connection or a
        503 means it is still starting up.

        Args:
            scheme (str): "http" or "https".
            host (str): OpenSearch host.
            port (int): OpenSearch port.
            timeout_sec (float): request timeout.

        Returns:
            str | None: why OpenSearch is not ready, or None if it is.
        """
        # no credentials are sent, so skipping certificate verification for this
        # liveness probe leaks nothing; the real client keeps its own TLS settings
        ctx = ssl._create_unverified_context() if scheme == "https" else None
        try:
            with urllib.request.urlopen(
                f"{scheme}://{host}:{port}/", timeout=timeout_sec, context=ctx
            ):
                return None
        except urllib.error.HTTPError as ex:
            return f"HTTP {ex.code}" if ex.code == 503 else None
        except (urllib.error.URLError, HTTPException, OSError) as ex:
            return str(getattr(ex, "reason", None) or ex) or type(ex).__name__

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

    async def _await_task_with_lease(
        self, task: dict, awaitable: Awaitable[Any]
    ) -> Any:
        """Await a task while keeping its Redis stream pending entry fresh."""
        lease_task = asyncio.create_task(self._refresh_task_lease_until_done(task))
        try:
            return await awaitable
        finally:
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await lease_task

    async def _mark_failed_task_stats_failed(self, task: dict, reason: str) -> None:
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
                    Glug.get_instance().warning(
                        f"failed task has no request stats: "
                        f"task_type={task.get('task_type')} req_id={req_id}"
                    )
                    return

                if stats.status != GulpRequestStatus.ONGOING.value:
                    Glug.get_instance().warning(
                        f"failed task request stats already terminal: "
                        f"task_type={task.get('task_type')} req_id={req_id} "
                        f"status={stats.status}"
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
            Glug.get_instance().exception(
                f"failed to mark task request stats failed: "
                f"task_type={task.get('task_type')} req_id={req_id}"
            )

    def _run_claimed_task(self, task: dict) -> Awaitable[Any]:
        """Return the handler awaitable for a claimed queued task."""
        from gulp.api.server.db import run_rebase_task
        from gulp.api.server.enrich import run_enrich_task
        from gulp.api.server.ingest import run_ingest_file_task
        from gulp.api.server.query import run_query_task

        ttype = task.get("task_type")
        # all the below spawn worker tasks are blocking, so we wait for them to finish before returning
        if ttype == TASK_TYPE_INGEST:
            return self.spawn_worker_task(run_ingest_file_task, task, wait=True)
        if ttype == TASK_TYPE_ENRICH:
            # internally calls spawn_worker_task
            return run_enrich_task(task)
        if ttype == TASK_TYPE_QUERY or ttype == TASK_TYPE_EXTERNAL_QUERY:
            return self.spawn_worker_task(run_query_task, task, wait=True)
        if ttype == TASK_TYPE_REBASE:
            # internally calls spawn_worker_task
            return run_rebase_task(task)

        raise ValueError(f"unknown task_type: {ttype!r}")

    @staticmethod
    def _task_dispatch_capacity() -> int:
        """Return the broad worker-task capacity for this instance."""
        cfg = GulpConfig.get_instance()
        child_concurrency = max(1, cfg.workers_concurrency_max())
        num_workers = max(1, cfg.workers_max())
        return max(1, num_workers * child_concurrency)

    async def _resolve_workers_concurrency(self) -> None:
        """Resolve automatic worker concurrency before spawning workers."""
        cfg = GulpConfig.get_instance()
        if cfg.get("workers_concurrency_max", 16) != 0:
            return

        try:
            redis_info = await GulpRedis.get_instance().client().info("cluster")
            redis_nodes = max(1, int(redis_info.get("cluster_known_nodes", 1)))
        except Exception as ex:
            redis_nodes = 1
            Glug.get_instance().warning(
                f"failed to inspect Redis node count, assuming one node: {ex}"
            )

        try:
            opensearch_info = (
                await GulpOpenSearch.get_instance()._opensearch.nodes.info()
            )
            opensearch_nodes = max(1, len(opensearch_info.get("nodes", {})))
        except Exception as ex:
            opensearch_nodes = 1
            Glug.get_instance().warning(
                f"failed to inspect OpenSearch node count, assuming one node: {ex}"
            )

        try:
            async with GulpCollab.get_instance().session() as sess:
                result = await sess.execute(
                    text(
                        "SELECT CASE WHEN pg_is_in_recovery() THEN 1 "
                        "ELSE 1 + count(*) END "
                        "FROM pg_stat_replication WHERE state = 'streaming'"
                    )
                )
                postgres_nodes = max(1, int(result.scalar_one()))
        except Exception as ex:
            postgres_nodes = 1
            Glug.get_instance().warning(
                f"failed to inspect PostgreSQL node count, assuming one node: {ex}"
            )

        # calculate weighted concurrency based on node counts, with OpenSearch and PostgreSQL weighted higher than Redis
        # 7 is the sum of the weights: 3 for OpenSearch, 3 for PostgreSQL, and 1 for Redis
        # on single-node deployments, this will resolve to 16, which is the default value
        concurrency = (
            16 * (3 * opensearch_nodes + 3 * postgres_nodes + redis_nodes) // 7
        )
        cfg.config()["workers_concurrency_max"] = concurrency
        Glug.get_instance().info(
            f"resolved workers_concurrency_max={concurrency} "
            f"(base=16, weights: opensearch_nodes={opensearch_nodes}*3, "
            f"postgres_nodes={postgres_nodes}*3, redis_nodes={redis_nodes}*1)"
        )

    @staticmethod
    def _next_task_dispatch_limit(
        current: int,
        maximum: int,
        service_ceiling: int,
        under_pressure: bool,
        recovery_step: int = 4,
    ) -> int:
        """Halve under pressure and recover slots per healthy sample."""
        ceiling = max(1, min(maximum, service_ceiling))
        if under_pressure:
            return max(1, min(current, ceiling) // 2)
        return min(ceiling, current + max(1, recovery_step))

    async def _task_service_state(self) -> tuple[bool, int, dict[str, int]]:
        """Return service pressure and a safe concurrency ceiling."""

        async def _postgres_state() -> tuple[int, int]:
            """
            Return PostgreSQL max connections and current usage.
            """
            async with GulpCollab.get_instance().session() as sess:
                row = (
                    await sess.execute(
                        text(
                            "SELECT current_setting('max_connections')::int, "
                            "count(*)::int FROM pg_stat_activity"
                        )
                    )
                ).one()
                return int(row[0]), int(row[1])

        async def _opensearch_state() -> tuple[int, int, int, int, int]:
            """
            Return OpenSearch search thread usage, capacity, heap %, CPU %, and rejected count.
            """
            nodes_client = GulpOpenSearch.get_instance()._opensearch.nodes
            stats = await nodes_client.stats(metric="jvm,process,thread_pool")
            nodes = stats.get("nodes", {}).values()
            search_threads = heap = cpu = rejected = 0
            for node in nodes:
                search = node.get("thread_pool", {}).get("search", {})
                search_threads += int(search.get("threads", 0))
                rejected += int(search.get("rejected", 0))
                heap = max(
                    heap,
                    int(node.get("jvm", {}).get("mem", {}).get("heap_used_percent", 0)),
                )
                cpu = max(
                    cpu,
                    int(node.get("process", {}).get("cpu", {}).get("percent", 0)),
                )
                if int(search.get("queue", 0)) > 0:
                    cpu = 100

            if self._opensearch_search_capacity is None:
                info = await nodes_client.info(metric="thread_pool")
                search_capacity = sum(
                    int(node.get("thread_pool", {}).get("search", {}).get("size", 0))
                    for node in info.get("nodes", {}).values()
                )
                if search_capacity:
                    self._opensearch_search_capacity = search_capacity

            return (
                search_threads,
                self._opensearch_search_capacity or search_threads,
                heap,
                cpu,
                rejected,
            )

        try:
            """
            Check the health of Redis, PostgreSQL, and OpenSearch, returning a tuple of:
            - under_pressure (bool): True if any service is under pressure
            - service_ceiling (int): a safe concurrency ceiling based on the least capable service
            - service_metrics (dict): a dictionary of service metrics for logging and monitoring
            """
            async with asyncio.timeout(2):
                redis_ok, postgres, opensearch = await asyncio.gather(
                    GulpRedis.get_instance().client().ping(),
                    _postgres_state(),
                    _opensearch_state(),
                )
        except Exception as ex:
            Glug.get_instance().warning(
                f"task scheduler health check failed, reducing concurrency: {ex}"
            )
            return True, 1, {}

        postgres_max, postgres_used = postgres
        (
            search_threads,
            search_capacity,
            heap,
            cpu,
            rejected,
        ) = opensearch
        rejected_increased = (
            self._last_opensearch_search_rejected is not None
            and rejected > self._last_opensearch_search_rejected
        )
        self._last_opensearch_search_rejected = rejected
        # Dispatch is limited by the smaller of PostgreSQL's usable connections
        # and OpenSearch's configured search-thread pool.
        postgres_ceiling = max(1, int(postgres_max * 0.8))
        service_ceiling = max(1, min(postgres_ceiling, search_capacity or 1))
        under_pressure = (
            not redis_ok
            or postgres_used >= postgres_ceiling
            or heap >= 85
            or cpu >= 90
            or rejected_increased
        )
        # return a dict of service metrics for logging and monitoring
        return (
            under_pressure,
            service_ceiling,
            {
                "postgres_used": postgres_used,
                "postgres_max": postgres_max,
                "postgres_dispatch_capacity": postgres_ceiling,
                "opensearch_heap": heap,
                "opensearch_cpu": cpu,
                "opensearch_search_threads": search_threads,
                "opensearch_search_capacity": search_capacity,
                "opensearch_search_rejected": rejected,
                "service_capacity_ceiling": service_ceiling,
            },
        )

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
                Glug.get_instance().warning(
                    f"unknown task_type while processing {source} task: "
                    f"{obj.get('task_type')!r}"
                )
                failure_state = await redis_inst.task_mark_failed(obj, reason)
                if failure_state == "failed":
                    await self._mark_failed_task_stats_failed(obj, reason)

        if not task_coros:
            return

        # run all tasks concurrently and wait for them to finish, while keeping their leases refreshed
        task_results = await asyncio.gather(
            *[self._await_task_with_lease(obj, coro) for obj, coro in task_coros],
            return_exceptions=True,
        )
        for (obj, _), res in zip(task_coros, task_results):
            if isinstance(res, Exception) or res is False:
                Glug.get_instance().error(
                    f"error while awaiting {source} task for "
                    f"task_type={obj.get('task_type')} req_id={obj.get('req_id')}",
                    exc_info=res if isinstance(res, Exception) else None,
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
        self-balancing task dispatcher loop that claims and dispatches tasks from Redis streams.

        the scheduler loop runs continuously, checking for new tasks to dispatch and adjusting concurrency limits based on
        postgres/opensearch/redis services health.
        """
        maximum: int = self._task_dispatch_capacity()
        limit: int = maximum
        query_limit = 1
        query_tuning_active = False
        last_health_check = 0.0
        last_upload_cleanup = time.monotonic()
        Glug.get_instance().info("starting task scheduler loop")

        in_flight: set[asyncio.Task[None]] = set()
        query_in_flight: set[asyncio.Task[None]] = set()

        def _consume_dispatch_result(task: asyncio.Task[None]) -> None:
            """
            Callback to consume the result of a dispatched task and remove it from the in-flight sets.
            """
            in_flight.discard(task)
            query_in_flight.discard(task)
            with suppress(asyncio.CancelledError):
                try:
                    task.result()
                except Exception as ex:
                    Glug.get_instance().exception(f"error while dispatching queued task: {ex}")

        def _start_dispatch_task(
            redis_inst: GulpRedis,
            task_obj: dict,
            *,
            source: str,
        ) -> None:
            """
            Start dispatching a claimed task and track it in the in-flight sets.
            """
            task = asyncio.create_task(
                self._dispatch_claimed_tasks(redis_inst, [task_obj], source=source)
            )
            in_flight.add(task)
            if task_obj.get("task_type") == TASK_TYPE_QUERY:
                query_in_flight.add(task)
            task.add_done_callback(_consume_dispatch_result)

        try:
            while True:
                try:
                    # check for new tasks to dispatch
                    redis_inst = GulpRedis.get_instance()
                    now = time.monotonic()
                    if now - last_upload_cleanup >= 60 * 60:
                        from gulp.api.server.server_utils import ServerUtils

                        ServerUtils.cleanup_stale_multipart_uploads()
                        last_upload_cleanup = now
                    known_types = await redis_inst.task_known_types()
                    if now - last_health_check >= 2.0:
                        # check for service pressure and adjust dispatch limits accordingly
                        query_demand = bool(query_in_flight) or (
                            await redis_inst.task_queue_depth(TASK_TYPE_QUERY) > 0
                        )
                        under_pressure, service_ceiling, state = (
                            await self._task_service_state()
                        )
                        new_limit = self._next_task_dispatch_limit(
                            limit,
                            maximum,
                            service_ceiling,
                            under_pressure,
                        )
                        if new_limit != limit:
                            Glug.get_instance().warning(
                                f"adjusted task dispatch concurrency from {limit} to "
                                f"{new_limit} to fit service capacity "
                                f"ceiling={service_ceiling} "
                                f"(under_pressure={under_pressure!r})",
                                detail=state,
                            )
                            limit = new_limit
                        if not query_demand:
                            query_tuning_active = False
                            new_query_limit = 1
                        elif not query_tuning_active:
                            query_tuning_active = True
                            new_query_limit = 1
                        else:
                            # Queries can consume much more OpenSearch heap than other
                            # tasks. Ramp them one slot at a time so the next service
                            # sample can react before admitting another large wave.
                            new_query_limit = self._next_task_dispatch_limit(
                                query_limit,
                                maximum,
                                min(new_limit, service_ceiling),
                                under_pressure,
                                recovery_step=1,
                            )
                        if new_query_limit != query_limit:
                            Glug.get_instance().info(
                                f"query dispatch concurrency {query_limit} -> "
                                f"{new_query_limit}, pressure={under_pressure!r}",
                                detail=state,
                            )
                            query_limit = new_query_limit
                        last_health_check = now

                    last_cleanup = getattr(
                        redis_inst, "_last_stale_pending_cleanup_time", 0.0
                    )
                    if now - last_cleanup >= 30.0:
                        redis_inst._last_stale_pending_cleanup_time = now
                        try:
                            failed = await redis_inst.task_drop_stale_pending()
                            if failed:
                                Glug.get_instance().warning(
                                    f"dropped {len(failed)} stale pending task(s)"
                                )
                                reason = "task abandoned by worker"
                                for obj in failed:
                                    await self._mark_failed_task_stats_failed(
                                        obj, reason
                                    )
                        except Exception:
                            Glug.get_instance().exception(
                                "error during stale pending task cleanup"
                            )
                        try:
                            removed = await redis_inst.cleanup_dead_task_consumers()
                            if removed:
                                Glug.get_instance().info(
                                    f"removed {removed} dead empty task consumer(s)"
                                )
                        except Exception:
                            Glug.get_instance().exception(
                                "error during dead task consumer cleanup"
                            )

                    capacity = limit - len(in_flight)
                    if capacity > 0:
                        # determine how many query slots are available and which task types can be dequeued
                        non_query_types = [
                            task_type
                            for task_type in known_types
                            if task_type != TASK_TYPE_QUERY
                        ]
                        query_slots = max(
                            0,
                            min(
                                capacity,
                                query_limit - len(query_in_flight),
                            ),
                        )
                        allowed_types = list(non_query_types)
                        if query_slots:
                            allowed_types.append(TASK_TYPE_QUERY)

                        # block briefly while tasks are running so new arrivals can
                        # fill idle slots without waiting for current work to finish.
                        first = await redis_inst.task_pop_blocking(
                            timeout=1 if in_flight else 5,
                            task_types=allowed_types,
                        )
                        if first is not None:
                            # build a batch of tasks to dispatch, prioritizing queries if slots are available
                            batch: list[dict] = [first]
                            remaining = capacity - 1
                            query_slots = max(
                                0,
                                min(
                                    remaining,
                                    query_limit
                                    - len(query_in_flight)
                                    - (
                                        1
                                        if first.get("task_type") == TASK_TYPE_QUERY
                                        else 0
                                    ),
                                ),
                            )
                            if query_slots:
                                # fill query slots with additional query tasks
                                batch.extend(
                                    await redis_inst.task_dequeue_batch(
                                        query_slots,
                                        task_types=[TASK_TYPE_QUERY],
                                    )
                                )
                            remaining = capacity - len(batch)
                            if remaining and non_query_types:
                                # fill remaining capacity with non-query tasks
                                batch.extend(
                                    await redis_inst.task_dequeue_batch(
                                        remaining,
                                        task_types=non_query_types,
                                    )
                                )
                            Glug.get_instance().debug(
                                f"dispatching {len(batch)} task(s), "
                                f"in_flight={len(in_flight)}, capacity={limit}"
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
                        # wait for at least one in-flight task to complete before dequeuing more work
                        await asyncio.wait(
                            in_flight,
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                except asyncio.CancelledError:
                    Glug.get_instance().debug("task scheduler loop cancelled")
                    break
                except Exception:
                    Glug.get_instance().exception("error in task scheduler loop")
                    await asyncio.sleep(1.0)
        finally:
            pending_dispatches = tuple(in_flight)
            for task in pending_dispatches:
                task.cancel()
            if pending_dispatches:
                await asyncio.gather(*pending_dispatches, return_exceptions=True)
            Glug.get_instance().info("exiting task scheduler loop")

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
            except Exception:
                Glug.get_instance().exception(f"error in background task: {name}")
            finally:
                Glug.get_instance().debug(f"background task name={name} completed")

        if name:
            # check if a task with the same name already exists in the current process
            for t in asyncio.all_tasks():
                if t.get_name() == name and not t.done():
                    Glug.get_instance().warning(
                        f"a task with the same name ({name}) already exists, "
                        "not spawning another one"
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
        task = asyncio.create_task(_run_and_await(), name=name)
        process = GulpProcess.get_instance()
        process._background_tasks.add(task)
        task.add_done_callback(process._background_tasks.discard)
        return True

    async def spawn_worker_task(
        self,
        func: Callable[..., Awaitable[Any]],
        *args,
        wait: bool = False,
        task_name: str = None,
        cancel_cleanup: Callable[[], Awaitable[None]] = None,
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
            cancel_cleanup (Callable, optional): cleanup to run after a cancelled waited task finishes.
            **kwargs: the keyword arguments to pass to the function

        Returns:
            Any|None: the result of the function if wait is True, None otherwise
        Raises:
            RuntimeError: if called from a worker process
            Exception: any exception raised by the function
        """
        process = GulpProcess.get_instance()
        if not process.is_main_process():
            raise RuntimeError("cannot spawn worker task from a worker process!")

        Glug.get_instance().debug(
            f"spawning worker task, func={func}, wait={wait!r}",
            detail={"args": args, "kwargs": kwargs},
        )

        coro = process.process_pool.apply(func, args=args, kwds=kwargs)

        if GulpConfig.get_instance().prometheus_enabled():
            # update Prometheus counter
            try:
                GulpMetrics.worker_tasks_spawned_total.inc()
            except Exception:
                pass

        if wait:
            # Shield the pool result waiter so caller cancellation cannot leave
            # the completed result retained in aiomultiprocess.Pool._results.
            worker_task = asyncio.create_task(coro)
            try:
                return await asyncio.shield(worker_task)
            except asyncio.CancelledError:

                async def _drain_cancelled_worker() -> None:
                    try:
                        await worker_task
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        Glug.get_instance().exception("error in cancelled worker task")
                    finally:
                        if cancel_cleanup:
                            try:
                                await cancel_cleanup()
                            except Exception:
                                Glug.get_instance().exception(
                                    "failed to clean up cancelled worker task"
                                )

                drain_task = asyncio.create_task(_drain_cancelled_worker())
                process._worker_result_tasks.add(drain_task)
                drain_task.add_done_callback(process._worker_result_tasks.discard)
                raise
            except Exception:
                Glug.get_instance().exception("error in (awaited) worker task")
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
        Glug.get_instance().debug("MAIN process cleanup initiated")

        # Close resources in dependency order. Each step is isolated so one
        # cleanup failure cannot prevent later resources from closing.
        async def cleanup_step(
            name: str, cleanup: Callable[[], Awaitable[Any]]
        ) -> None:
            try:
                await cleanup()
            except Exception:
                Glug.get_instance().exception(f"MAIN process cleanup failed for {name}")

        await cleanup_step(
            "connected sockets",
            lambda: GulpConnectedSockets.get_instance().cancel_all(),
        )
        await cleanup_step(
            "background tasks", GulpProcess.get_instance().cancel_bg_tasks
        )

        # Cancel the dequeue task before closing workers and backing services.
        if self._poll_tasks_task:

            async def stop_dispatcher() -> None:
                Glug.get_instance().debug("cancelling poll_tasks task")
                self._poll_tasks_task.cancel()
                try:
                    await asyncio.wait_for(self._poll_tasks_task, timeout=5.0)
                except asyncio.CancelledError:
                    return
                except asyncio.TimeoutError:
                    Glug.get_instance().warning("poll_tasks task cancellation timed out")

            await cleanup_step("task dispatcher", stop_dispatcher)

        # Let workers finish producing events before listeners or storage close.
        await cleanup_step(
            "process pool", lambda: GulpProcess.get_instance().close_process_pool()
        )

        from gulp.events import events

        await cleanup_step("event drain", events.drain)
        await cleanup_step("extension plugins", self._unload_extension_plugins)
        await cleanup_step("final event drain", events.drain)
        # Close the thread pool in the main process.
        await cleanup_step("thread pool", GulpProcess.get_instance().close_thread_pool)
        # Close clients in dependency order.
        await cleanup_step("PostgreSQL", lambda: GulpCollab.get_instance().shutdown())
        await cleanup_step(
            "OpenSearch", lambda: GulpOpenSearch.get_instance().shutdown()
        )
        await cleanup_step("S3", lambda: GulpS3.get_instance().shutdown())
        # Remove this server's empty consumer entries before Redis closes.
        await cleanup_step(
            "Redis consumers",
            lambda: GulpRedis.get_instance().cleanup_consumers_for_server(
                self.server_id
            ),
        )
        await cleanup_step(
            "Redis broker", lambda: GulpRedisBroker.get_instance().shutdown()
        )
        await cleanup_step("Redis", lambda: GulpRedis.get_instance().shutdown())

        Glug.get_instance().info(f"MAIN process cleanup done (server_id={self.server_id})")
        # Remove pidfile if it exists.
        try:
            pidfile = muty.file.safe_path_join(
                GulpConfig.get_instance().path_working_dir(),
                "gulp%d.pid" % (os.getpid()),
            )
            if os.path.exists(pidfile):
                os.remove(pidfile)
                Glug.get_instance().info(f"removed pidfile={pidfile}")
        except Exception:
            Glug.get_instance().exception("failed to remove pidfile")

    async def _test(self):
        # to quick test code snippets, called by lifespan_handler
        """
        from gulp.api.opensearch_api import GulpOpenSearch
        api = GulpOpenSearch.get_instance()
        p = await api.index_template_get("test_operation")
        Glug.get_instance().info("got index template for operation 'test_operation'", detail=p)
        """
        return

    async def _startup(self) -> None:
        """Initialize main-process services and background tasks."""
        main_process = GulpProcess.get_instance()
        # Load configuration.
        GulpConfig.get_instance()
        from gulp.api.server.server_utils import ServerUtils

        ServerUtils.cleanup_stale_multipart_uploads()
        first_run: bool = self._check_first_run()
        if first_run:
            self._reset_collab = True

        # Check database integrity; schema mismatches abort unless reset was requested.
        collab_recreated = False
        try:
            await GulpCollab.get_instance().init(main_process=True)
        except SchemaMismatch:
            if not self._reset_collab:
                raise
            await GulpCollab.get_instance().init(main_process=True, force_recreate=True)
            collab_recreated = True

        if first_run and not self._create_operation:
            # Force creating a default operation on first run.
            self._create_operation = "test_operation"

        Glug.get_instance().info(
            f"first_run={first_run!r}, _reset_collab={self._reset_collab!r}, "
            f"_create_operation={self._create_operation}"
        )

        # Initialize the OpenSearch client.
        opensearch = GulpOpenSearch.get_instance()
        await opensearch.bulk_max_content_length_bytes()

        # Initialize the Redis client.
        GulpRedis.get_instance().initialize(self.server_id)
        # Tune worker concurrency when workers_concurrency_max is automatic.
        await self._resolve_workers_concurrency()

        # Initialize Redis pub/sub for worker-to-main and instance communication.
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.initialize()

        # Start glug's live level/dev_mode/log_dir reconfig subscriber now that
        # Redis is ready — glug itself was already bootstrapped at the top of
        # _lifespan_handler, before any of this method's own logging ran. The
        # persisted state is cleared first, before any worker is spawned: the
        # command line alone applies until an extension plugin
        # publishes again, so nothing a previous run published survives a
        # plugin being removed. Note that every gulp instance sharing this
        # Redis shares this state too.
        from gulp.glug import reconfig as glug_reconfig

        await glug_reconfig.reset_persisted_state()
        await glug_reconfig.start_subscriber()

        # Initialize collaboration data and create the requested operation.
        try:
            if self._reset_collab:
                if not collab_recreated:
                    from gulp.api.server.db import db_reset

                    # Reset the collaboration database and all operation data.
                    await db_reset()

                # Clean Redis only for the explicit reset path.
                await GulpRedis.get_instance().cleanup_redis()

            if self._create_operation:
                from gulp.api.collab.operation import GulpOperation

                async with GulpCollab.get_instance().session() as sess:
                    await GulpOperation.create_operation(
                        sess, self._create_operation, "admin", set_default_grants=True
                    )
        except Exception:
            if first_run:
                # Allow restart on first-run initialization failures.
                await self._reset_first_run()
            raise

        # Finish initializing the main process and spawn workers.
        await main_process.finish_initialization(
            self.server_id,
            log_level=self._log_level,
            log_file=self._log_file,
        )

        if __debug__:
            # Test a snippet with gULP freshly initialized.
            await self._test()

        cfg = GulpConfig.get_instance()
        child_concurrency: int = cfg.workers_concurrency_max()
        num_workers: int = cfg.workers_max()
        dispatch_concurrency: int = self._task_dispatch_capacity()
        ingest_concurrency: int = max(1, num_workers)
        Glug.get_instance().debug(
            f"startup concurrency: queued Redis task dispatches<={dispatch_concurrency} "
            f"(worker_processes={num_workers} * "
            f"workers_concurrency_max={child_concurrency})"
        )
        Glug.get_instance().debug(
            f"startup ingest concurrency: ingest execution<={ingest_concurrency} "
            f"(worker_processes={num_workers})"
        )
        # Start the dequeue task for long-running work.
        self._poll_tasks_task = asyncio.create_task(self._dispatch_tasks())

        # Start Prometheus gauge collection in the main process.
        if GulpConfig.get_instance().prometheus_enabled():
            metrics = GulpMetrics.get_instance()
            metrics.server_info.info(
                {
                    "version": self.version_string(),
                    "server_id": self.server_id,
                }
            )
            await metrics.start_collect_loop()

    async def _lifespan_handler(self, app: FastAPI):
        """Initialize FastAPI services and always release acquired resources."""
        # Bootstrap glug for the main process as the very first thing: every
        # log call from here on (including the rest of this handler and all of
        # _startup()) must already see the requested --log-level,
        # not the bare-instance INFO default a call site gets when nothing has
        # configured Glug yet. The Redis-dependent half of glug's bootstrap
        # (persisted-state sync, live reconfig subscriber) still happens later
        # in _startup(), once Redis is actually initialized.
        Glug.get_instance(
            name="glug",
            log_file=self._log_file,
            level=self._log_level,
            process_role="main",
            reconfigure=True,
        )
        Glug.get_instance().info("gULP main server process is starting")

        try:
            await self._startup()
            # Wait for termination after startup completes.
            self._lifespan_task = asyncio.current_task()
            try:
                yield
            except asyncio.CancelledError:
                Glug.get_instance().warning("CancelledError caught in _lifespan handler")
        finally:
            # Startup failures and normal shutdown share the same cleanup path.
            Glug.get_instance().info("gulp shutting down")
            # Stop Prometheus collection independently from other resources.
            try:
                if GulpConfig.get_instance().prometheus_enabled():
                    await GulpMetrics.get_instance().stop_collect_loop()
            except Exception:
                Glug.get_instance().exception("failed to stop Prometheus collection")
            # Delete pending stats created by this instance only.
            try:
                if GulpConfig.get_instance().stats_delete_pending_on_shutdown():
                    await GulpRequestStats.purge_ongoing_requests(self.server_id)
            except Exception:
                Glug.get_instance().exception("failed to purge pending request stats")
            # Stop the glug reconfig subscriber, if it was started.
            try:
                from gulp.glug import reconfig as glug_reconfig

                await glug_reconfig.stop_subscriber()
            except Exception:
                Glug.get_instance().exception("failed to stop glug reconfig subscriber")
            # Clean up main-process resources even when initialization failed.
            await self._cleanup()
            # Workers are stopped before their shared multiprocess metrics directory is removed.
            try:
                cleanup_prometheus()
            except Exception:
                Glug.get_instance().exception("failed to clean up Prometheus")
            Glug.get_instance().info(
                "MAIN process cleanup complete, exiting lifespan handler"
            )

    async def _reset_first_run(self) -> None:
        """
        deletes the ".first_run_done" file in the config directory.
        """
        config_directory = GulpConfig.get_instance().path_working_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            await muty.file.delete_file_or_dir(check_first_run_file)
            Glug.get_instance().warning(f"deleted: {check_first_run_file}")

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
            Glug.get_instance().debug(
                f"not first run, first run file exists: {check_first_run_file}"
            )
            return False

        # create firstrun file
        Glug.get_instance().info(
            f"first run, first run file does not exist: {check_first_run_file}"
        )
        with open(check_first_run_file, "w", encoding="utf-8") as f:
            f.write("gulp!")
        return True
