"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import os
import signal
import ssl
from typing import Any

import muty.crypto
import muty.file
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
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

from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.test_values import TEST_INDEX
from gulp.api.ws_api import GulpConnectedSockets, GulpSharedWsQueue
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import ObjectAlreadyExists, ObjectNotFound
import asyncio_atexit


class GulpRestServer:
    """
    manages the gULP REST server.
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._app = None
            self._logger_file_path = None
            self._log_level = None
            self._reset_collab = False
            self._reset_index = None
            self._shutdown: bool = False
            self._extension_plugins: list[GulpPluginBase] = []

    @classmethod
    def get_instance(cls) -> "GulpRestServer":
        """
        returns the singleton instance
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()

        return cls._instance

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
        l = await GulpPluginBase.list(extension_only=True)
        count: int = 0
        for p in l:
            p = await GulpPluginBase.load(p.path, extension=True)
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
            if ex.ex is not None:
                # use the inner exception
                ex = ex.ex
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
        from gulp.api.rest.ingest import router as ingest_router
        from gulp.api.rest.operation import router as operation_router
        from gulp.api.rest.ws import router as ws_router
        from gulp.api.rest.user import router as user_router
        from gulp.api.rest.note import router as note_router
        from gulp.api.rest.db import router as db_router
        from gulp.api.rest.link import router as link_router
        from gulp.api.rest.highlight import router as highlight_router
        from gulp.api.rest.story import router as story_router
        from gulp.api.rest.glyph import router as glyph_router
        from gulp.api.rest.stored_query import router as stored_query_router
        from gulp.api.rest.user_group import router as user_group_router
        from gulp.api.rest.object_acl import router as object_acl_router
        from gulp.api.rest.utility import router as utility_router
        from gulp.api.rest.query import router as query_router

        self._app.include_router(db_router)
        self._app.include_router(operation_router)
        self._app.include_router(ingest_router)
        self._app.include_router(ws_router)
        self._app.include_router(user_router)
        self._app.include_router(note_router)
        self._app.include_router(link_router)
        self._app.include_router(highlight_router)
        self._app.include_router(story_router)
        self._app.include_router(glyph_router)
        self._app.include_router(stored_query_router)
        self._app.include_router(user_group_router)
        self._app.include_router(object_acl_router)
        self._app.include_router(utility_router)
        self._app.include_router(query_router)

        """
        import gulp.api.rest.utility
        _app.include_router(gulp.api.rest.utility.router())
        """
        pass

    def add_api_route(self, path: str, handler: callable, **kwargs):
        """
        add a new API route, just bridges to FastAPI.add_api_route

        Args:
            path (str): the path
            handler: the handler
            **kwargs: additional arguments to FastAPI.add_api_route
        """
        self._app.add_api_route(path, handler, **kwargs)

    def start(
        self,
        logger_file_path: str = None,
        level: int = None,
        reset_collab: bool = False,
        reset_index: str = None,
    ):
        """
        starts the server.

        Args:
            logger_file_path (str, optional): path to the logger file.
            level (int, optional): the log level.
            reset_collab (bool, optional): if True, the collab database will be reset on start.
            reset_index (str, optional): name of the OpenSearch/Elasticsearch index to reset (if --reset-data is provided on the commandline).
        """
        self._logger_file_path = logger_file_path
        self._log_level = level
        self._reset_collab = reset_collab
        self._reset_index = reset_index

        # read configuration
        cfg = GulpConfig.get_instance()

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
            "starting server at %s, port=%d, logger_file_path=%s, reset_collab=%r, reset_index=%s ..."
            % (address, port, logger_file_path, reset_collab, reset_index)
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
        kills all processes with 'gulp' in their command line

        TODO: this is a last resort to kill any stale processes, should be investigated why they are left behind (by aiomultiprocess?)
        """
        try:
            # Get process list with PIDs
            ps_output = os.popen("ps -aux | grep gulp").read()

            # extract PIDs
            pids = []
            output = ps_output.splitlines()
            for line in output:
                if "gulp" in line and not "resource_tracker" in line:
                    try:
                        tokens = line.strip().split()
                        pids.append(int(tokens[1].strip()))
                    except (IndexError, ValueError) as ex:
                        MutyLogger.get_instance().exception(
                            "cannot parse %s (%s)" % (ex)
                        )
                        continue

            # kill each process
            for pid in pids:
                try:
                    # check if its not the current pid
                    if pid != os.getpid():
                        os.kill(pid, signal.SIGKILL)
                        # MutyLogger.get_instance().debug(f"killed gulp process {pid}")
                except ProcessLookupError:
                    continue
        except Exception as e:
            MutyLogger.get_instance().error(f"error killing gulp processes: {e}")

    async def _cleanup(self):
        """
        called when lifespan handler returns from yield, to cleanup
        """
        MutyLogger.get_instance().info("atexit() cleanup")
        self._kill_gulp_processes()

    async def _lifespan_handler(self, app: FastAPI):
        """
        fastapi lifespan handler
        """
        MutyLogger.get_instance().info("gULP main server process is starting!")
        asyncio_atexit.register(self._cleanup)

        main_process = GulpProcess.get_instance()

        # load configuration
        GulpConfig.get_instance()

        first_run: bool = False
        if self._check_first_run():
            # first run, create index
            self._reset_index = TEST_INDEX
            self._reset_collab = True
            first_run = True
            MutyLogger.get_instance().warning(
                "FIRST RUN, creating collab database and data index '%s' ..."
                % (self._reset_index)
            )

        # check for reset flags
        try:
            if self._reset_collab:
                # reinit collab
                MutyLogger.get_instance().warning("resetting collab!")
                collab = GulpCollab.get_instance()
                await collab.init(force_recreate=True)
            if self._reset_index:
                # reinit elastic
                MutyLogger.get_instance().warning(
                    "resetting data, recreating index '%s' ..." % (self._reset_index)
                )
                gos = GulpOpenSearch.get_instance()
                await gos.datastream_create(self._reset_index)
        except Exception as ex:
            if first_run:
                # allow restart on first run
                self._reset_first_run()
            raise ex

        # init the main process
        await main_process.init_gulp_process(
            log_level=self._log_level, logger_file_path=self._logger_file_path
        )

        # load extension plugins
        await self._load_extension_plugins()

        # wait for shutdown
        yield

        # cleaning up will be done through _cleanup called via atexit
        MutyLogger.get_instance().info("gulp shutting down!")
        self.set_shutdown()

        await self._unload_extension_plugins()

        # close shared ws and process pool
        await GulpConnectedSockets.get_instance().cancel_all()
        GulpSharedWsQueue.get_instance().close()
        await GulpProcess.get_instance().close_process_pool()

        # close clients in the main process
        await GulpCollab.get_instance().shutdown()
        await GulpOpenSearch.get_instance().shutdown()

        # close coro and thread pool in the main process
        await GulpProcess.get_instance().close_coro_pool()
        await GulpProcess.get_instance().close_thread_pool()

        MutyLogger.get_instance().info("everything shut down, we can gracefully exit.")

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
        with open(check_first_run_file, "w") as f:
            f.write("gulp!")
        return True

