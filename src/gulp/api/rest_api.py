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
from typing import Any, Coroutine

import asyncio_atexit
import muty.file
import muty.os
import muty.string
import muty.version
import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from opensearchpy import RequestError
from starlette.middleware.sessions import SessionMiddleware

from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.rest.test_values import TEST_OPERATION_ID
from gulp.api.ws_api import GulpConnectedSockets, GulpWsSharedQueue
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import ObjectAlreadyExists, ObjectNotFound


class GulpRestServer:
    """
    manages the gULP REST server.
    """

    _instance: "GulpRestServer" = None

    def __init__(self):
        self._initialized: bool = True
        self._app: FastAPI = None
        self._logger_file_path: str = None
        self._log_level: int = None
        self._recreate_collab: bool = False
        self._delete_data: bool = False
        self._reset_operation: str = None
        self._lifespan_task: asyncio.Task = None
        self._shutdown: bool = False
        self._restart_signal: asyncio.Event = asyncio.Event()
        self._extension_plugins: list[GulpPluginBase] = []

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpRestServer":
        """
        returns the singleton instance

        Returns:
            GulpRestServer: the singleton instance
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def trigger_restart(self) -> None:
        """Triggers server restart by setting event"""
        MutyLogger.get_instance().info("Triggering server restart...")
        if self._lifespan_task:
            self._lifespan_task.cancel()
        self._restart_signal.set()

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
        MutyLogger.get_instance().debug("unloading extension plugins ...")
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

        # extras, if any
        p = GulpConfig.get_instance().path_plugins_extra()
        if p:
            path_extension = os.path.join(p, "extension")
            files.extend(await muty.file.list_directory_async(path_extension, "*.py*"))

        count: int = 0
        for f in files:
            if "__init__.py" not in f:
                # load
                try:
                    p = await GulpPluginBase.load(f, extension=True)
                except Exception as ex:
                    # plugin failed to load
                    MutyLogger.get_instance().exception(ex)
                    continue
                self._extension_plugins.append(p)
                count += 1

        MutyLogger.get_instance().debug("loaded %d extension plugins" % (count))

    def set_shutdown(self, *args):
        """
        Sets the global `_shutting_down` flag to True.
        """
        MutyLogger.get_instance().debug("setting shutdown=True !")
        self._shutdown = True

    def is_shutdown(self) -> bool:
        """
        Returns the value of the global `_shutting_down` flag.

        Returns:
            bool: True if the server is shutting down, False otherwise.
        """
        if self._shutdown:
            MutyLogger.get_instance().warning("_shutdown set!")
        return self._shutdown

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
        except:
            try:
                # take the first 1k bytes of the body
                body = await r.body()
                body = str(body[:1024]) + "... (truncated)"
            except:
                body = None

        if body and isinstance(body, bytes):
            body = body.decode("utf-8")

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
        from gulp.api.rest.db import router as db_router
        from gulp.api.rest.enrich import router as enrich_router
        from gulp.api.rest.glyph import router as glyph_router
        from gulp.api.rest.highlight import router as highlight_router
        from gulp.api.rest.ingest import router as ingest_router
        from gulp.api.rest.link import router as link_router
        from gulp.api.rest.note import router as note_router
        from gulp.api.rest.object_acl import router as object_acl_router
        from gulp.api.rest.operation import router as operation_router
        from gulp.api.rest.query import router as query_router
        from gulp.api.rest.user import router as user_router
        from gulp.api.rest.user_group import router as user_group_router
        from gulp.api.rest.utility import router as utility_router
        from gulp.api.rest.ws import router as ws_router

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
        for route in self._app.routes:
            if route.path == path:
                self._app.router.routes.remove(route)

        # add route
        self._app.add_api_route(path, handler, **kwargs)

    def start(
        self,
        logger_file_path: str = None,
        level: int = None,
        recreate_collab: bool = False,
        reset_operation: str = None,
        delete_data: bool = False,
    ):
        """
        starts the server.

        Args:
            logger_file_path (str, optional): path to the logger file.
            level (int, optional): the log level.
            recreate_collab (bool, optional): if True, reset the collab database
            reset_operation (str, optional): the operation to be reset/created (--reset)
            delete_data (bool, optional): if True, delete the data on OpenSearch for one (--reset) or all (--reset-full) operations
        """
        self._logger_file_path = logger_file_path
        self._log_level = level
        self._recreate_collab = recreate_collab
        self._delete_data = delete_data
        self._reset_operation = reset_operation

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
            description="(gui)Universal Log Processor",
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
            "starting server at %s, port=%d, logger_file_path=%s, recreate_collab=%r, reset_operation=%s ..."
            % (address, port, logger_file_path, recreate_collab, reset_operation)
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

            ssl_keyfile = muty.file.safe_path_join(path_certs, "gulp.key")
            ssl_certfile = muty.file.safe_path_join(path_certs, "gulp.pem")
            MutyLogger.get_instance().info(
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
                self._app,
                host=address,
                port=port,
                ssl_keyfile=ssl_keyfile,
                ssl_keyfile_password=cert_password,
                ssl_certfile=ssl_certfile,
                ssl_ca_certs=gulp_ca_certs,
                ssl_cert_reqs=ssl_cert_verify_mode,
            )
        else:
            # http
            MutyLogger.get_instance().warning("HTTP!")
            uvicorn.run(self._app, host=address, port=port)

    def _kill_gulp_processes(self) -> None:
        """
        kills all child processes of the main process
        """
        import multiprocessing

        for process in multiprocessing.active_children():
            process.terminate()
        MutyLogger.get_instance().info("killed all child processes!")

    async def handle_bg_future(self, future: asyncio.Future) -> None:
        """
        waits for a future running in background to complete, logs any exceptions

        future (asyncio.Future): the future to wait for
        """
        try:
            await future
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            MutyLogger.get_instance().debug("future completed!")

    async def spawn_bg_task(self, coro: Coroutine) -> None:
        """
        spawns a background task, future is awaited in the background and exceptions are logged

        Args:
            coro (Coroutine): the coroutine to spawn
            kwds (dict, optional): the keyword arguments to pass to the coroutine
        """
        f = await GulpProcess.get_instance().coro_pool.spawn(coro)
        _ = asyncio.create_task(self.handle_bg_future(f))

    async def _cleanup(self):
        """
        called when lifespan handler returns from yield, to cleanup
        """
        MutyLogger.get_instance().info("atexit() cleanup")
        self._kill_gulp_processes()

        if self._restart_signal.is_set():
            MutyLogger.get_instance().info("restarting server ...")
            self._restart_signal.clear()

            # respawn
            muty.os.respawn_current_process()

    async def _test(self):
        # to quick test code snippets, called by lifespan_handler
        """import json

        from gulp.api.opensearch_api import GulpOpenSearch
        api = GulpOpenSearch.get_instance()
        p = await api.index_template_get("test_operation")
        MutyLogger.get_instance().info(json.dumps(p, indent=2))"""
        return

    async def _lifespan_handler(self, app: FastAPI):
        """
        fastapi lifespan handler
        """
        from gulp.api.opensearch_api import GulpOpenSearch

        MutyLogger.get_instance().info("gULP main server process is starting!")

        asyncio_atexit.register(self._cleanup)

        main_process = GulpProcess.get_instance()

        # load configuration
        GulpConfig.get_instance()

        first_run: bool = False
        if self._check_first_run():
            # first run, force --reset and --recreate-collab
            self._reset_operation = TEST_OPERATION_ID
            self._recreate_collab = True
            first_run = True
            MutyLogger.get_instance().warning(
                "FIRST RUN, (re)creating collab database and operation '%s' ..."
                % (self._reset_operation)
            )

        # check for reset flags
        try:
            if self._recreate_collab:
                from gulp.api.rest.db import db_reset

                # reset collab database
                MutyLogger.get_instance().warning(
                    "recreate_collab set, first_run=%r !" % (first_run)
                )
                await db_reset(delete_data=self._delete_data)

            if self._reset_operation:
                # delete and recreate operation and index
                try:
                    collab = GulpCollab.get_instance()
                    await collab.init(main_process=True)

                    # reset operation
                    from gulp.api.rest.operation import operation_reset
                    async with GulpCollab.get_instance().session() as sess:
                        await operation_reset(
                            sess, self._reset_operation, delete_data=self._delete_data
                        )
                except SchemaMismatch as ex:
                    # clear and recreate collab database, then create operation
                    await db_reset(delete_data=self._delete_data, create_operation_id=self._reset_operation)

        except Exception as ex:
            if first_run:
                # allow restart on first run
                self._reset_first_run()
            raise ex

        # init the main process
        await main_process.init_gulp_process(
            log_level=self._log_level, logger_file_path=self._logger_file_path
        )

        if __debug__:
            # to test some snippet with gulp freshly initialized
            await self._test()

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
        self.set_shutdown()

        await self._unload_extension_plugins()

        # close shared ws and process pool
        try:
            await GulpConnectedSockets.get_instance().cancel_all()
            wsq = GulpWsSharedQueue.get_instance()
            await wsq.close()

            # close clients in the main process
            await GulpCollab.get_instance().shutdown()
            await GulpOpenSearch.get_instance().shutdown()

            # close coro and thread pool in the main process
            await GulpProcess.get_instance().close_coro_pool()
            await GulpProcess.get_instance().close_thread_pool()

            # close process pool
            await GulpProcess.get_instance().close_process_pool()
            MutyLogger.get_instance().info(
                "everything shut down, we can gracefully exit."
            )
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            await self._cleanup()

    def _reset_first_run(self) -> None:
        """
        deletes the ".first_run_done" file in the config directory.
        """
        config_directory = GulpConfig.get_instance().config_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            muty.file.delete_file_or_dir(check_first_run_file)
            MutyLogger.get_instance().warning("deleted: %s" % (check_first_run_file))

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
            MutyLogger.get_instance().debug(
                "NOT FIRST RUN, first run file exists: %s" % (check_first_run_file)
            )
            return False

        # create firstrun file
        MutyLogger.get_instance().warning(
            "FIRST RUN! first run file does not exist: %s" % (check_first_run_file)
        )
        with open(check_first_run_file, "w", encoding="utf-8") as f:
            f.write("gulp!")
        return True
