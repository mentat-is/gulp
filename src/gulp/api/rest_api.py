"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import os
import ssl
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpSharedWsQueue
from gulp.plugin import GulpPluginBase
import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.version
import muty.uploadfile
import uvicorn
from fastapi import FastAPI, Request
from fastapi.concurrency import asynccontextmanager
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from muty.jsend import JSendException, JSendResponse
from opensearchpy import RequestError
from gulp.process import GulpProcess
from gulp.utils import GulpLogger
from gulp.config import GulpConfig
from gulp.api.collab.structs import (
    MissingPermission,
    SessionExpired,
    WrongUsernameOrPassword,
)
from gulp.structs import ObjectNotFound

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
            self._app = None
            self._log_file_path = None
            self._reset_collab = False
            self._reset_index = None
            self._first_run = False
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

    @staticmethod
    def ensure_req_id(req_id: str = None) -> str:
        """
        Ensures a request ID is not None, either returns a new one.

        Args:
            req_id (str, optional): The request ID. Defaults to None.

        Returns:
            str: The request ID.
        """
        if req_id is None:
            return muty.string.generate_unique()
        return req_id


    async def _unload_extension_plugins(self) -> None:
        """
        unload extension plugins
        """
        GulpLogger.get_logger().debug("unloading extension plugins ...")
        for p in self._extension_plugins:
            p.unload()
        self._extension_plugins = []


    async def _load_extension_plugins(self) -> None:
        """
        load available extension plugins
        """
        GulpLogger.get_logger().debug("loading extension plugins ...")
        path_plugins = GulpConfig.get_instance().path_plugins(extension=True)
        files = await muty.file.list_directory_async(path_plugins, "*.py*", recursive=True)
        for f in files:
            if "__init__" not in f and "__pycache__" not in f:
                # get base filename without extension
                p = await GulpPluginBase.load(f, extension=True)
                self._extension_plugins.append(p)
        
        GulpLogger.get_logger().debug("loaded %d extension plugins: %s" % (len(self._extension_plugins), self._extension_plugins))

    def set_shutdown(self, *args):
        """
        Sets the global `_shutting_down` flag to True.
        """
        GulpLogger.get_logger().debug("shutting down!")
        self._shutdown = True


    def is_shutdown(self) -> bool:
        """
        Returns the value of the global `_shutting_down` flag.

        Returns:
            bool: True if the server is shutting down, False otherwise.
        """
        if self._shutdown:
            GulpLogger.get_logger().warning("_shutdown set!")
        return self._shutdown


    async def _validation_exception_handler(
        _: Request, ex: RequestValidationError
    ) -> JSendResponse:
        """
        set error code to 400 on validation exceptions
        """
        status_code = 400
        jsend_ex = JSendException(ex=ex, status_code=status_code)
        GulpLogger.get_logger().debug(
            "in request-validation exception handler, status_code=%d" % (status_code)
        )
        return muty.jsend.fastapi_jsend_exception_handler(jsend_ex, status_code)


    async def _bad_request_exception_handler(
        _: Request, ex: RequestValidationError
    ) -> JSendResponse:
        """
        set error code to 400 on bad request exceptions
        """
        status_code = 400
        jsend_ex = JSendException(ex=ex, status_code=status_code)
        GulpLogger.get_logger().debug("in bad-request exception handler, status_code=%d" % (status_code))
        return muty.jsend.fastapi_jsend_exception_handler(jsend_ex, status_code)


    async def _gulp_rest_api_exception_handler(_: Request, ex: JSendException) -> JSendResponse:
        """
        handles gulp rest api errors

        Args:
            _: Request: the request
            ex: JSendException: the exception

        Returns:
            JSendResponse
        """
        status_code = 500

        if ex.status_code is not None:
            # force status code
            status_code = ex.status_code
        elif ex.ex is not None:
            # get from exception
            if isinstance(ex.ex, ObjectNotFound) or isinstance(ex.ex, FileNotFoundError):
                status_code = 404
            elif isinstance(ex.ex, ValueError):
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

    def _add_routers(self):
        """
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
        """
        pass

    def start(self, log_file_path: str=None, reset_collab: bool = False, reset_index: str = None):
        """
        starts the server.

        Args:
            log_file_path (str, optional): path to the log file.
            reset_collab (bool, optional): if True, the collab database will be reset on start.
            reset_index (str, optional): name of the OpenSearch/Elasticsearch index to reset (if --reset-data is provided on the commandline).
        """
        self._log_file_path = log_file_path
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
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # add our custom exception handlers
        self._app.add_exception_handler(RequestValidationError, self._validation_exception_handler)
        self._app.add_exception_handler(RequestError, self._bad_request_exception_handler)
        self._app.add_exception_handler(JSendException, self._gulp_rest_api_exception_handler)

        # add routers in other modules
        self._add_routers()

        address, port = GulpConfig.get_instance().bind_to()
        GulpLogger.get_logger().info("starting server at %s, port=%d, log_file_path=%s, reset_collab=%r, reset_index=%s ..." % (address, port, log_file_path, reset_collab, reset_index))
        
        if cfg.enforce_https():
            GulpLogger.get_logger().info("enforcing HTTPS ...")

            path_certs: str = cfg.path_certs()
            cert_password: str = cfg.https_cert_password()
            gulp_ca_certs = muty.file.safe_path_join(path_certs, "gulp-ca.pem")
            if not os.path.exists(gulp_ca_certs):
                # use server cert as CA cert
                gulp_ca_certs = muty.file.safe_path_join(path_certs, "gulp.pem")

            ssl_cert_verify_mode: int = ssl.VerifyMode.CERT_OPTIONAL
            if cfg.enforce_https_client_certs():
                ssl_cert_verify_mode = ssl.VerifyMode.CERT_REQUIRED
                GulpLogger.get_logger().warning("HTTPS client certificates ARE ENFORCED.")

            ssl_keyfile = muty.file.safe_path_join(path_certs, "gulp.key")
            ssl_certfile = muty.file.safe_path_join(path_certs, "gulp.pem")
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
            GulpLogger.get_logger().warning("HTTP!")
            uvicorn.run(self._app, host=address, port=port)

    async def _lifespan_handler(self, app: FastAPI):
        """
        fastaapi lifespan handler
        """
        GulpLogger.get_logger().info("gULP main server process is starting!")
        main_process = GulpProcess.get_instance()

        # check configuration directories
        cfg = GulpConfig.get_instance()
        await cfg.check_copy_mappings_and_plugins_to_custom_directories()

        first_run: bool=False
        if self._check_first_run():
            # first run, create index
            self._reset_index = "gulpidx"
            self._reset_collab = True
            first_run=True
            GulpLogger.get_logger().warning("first run, creating collab database and data index '%s' ..." % (self._reset_index))
            
        else:
            GulpLogger.get_logger().info("not first run")

        # init the main process
        await main_process.init_gulp_process(is_main_process=True)

        # check for reset flags
        try:
            if self._reset_collab:
                # reinit collab
                GulpLogger.get_logger().warning("resetting collab!")
                collab = GulpCollab.get_instance()
                await collab.init(force_recreate=True)
            if self._reset_index:
                # reinit elastic
                GulpLogger.get_logger().warning("resetting data, recreating index '%s' ..." % (self._reset_index))  
                el = GulpOpenSearch.get_instance()
                await el.datastream_create(self._reset_index)
        except Exception as ex:
            if first_run:
                # allow restart on first run
                self._delete_first_run_file()
            raise ex

        # load extension plugins
        await self._load_extension_plugins()

        # wait for shutdown
        yield

        GulpLogger.get_logger().info("gulp shutting down!")
        self.set_shutdown()

        await self._unload_extension_plugins()
        
        # close shared ws and process pool
        GulpSharedWsQueue.get_instance().close()
        await GulpProcess.get_instance().close_process_pool()

        # close clients in the main process
        await GulpCollab.get_instance().shutdown()
        await GulpOpenSearch.get_instance().shutdown()

        # close coro and thread pool in the main process
        await GulpProcess.get_instance().close_coro_pool()
        await GulpProcess.get_instance().close_thread_pool()

        GulpLogger.get_logger().debug("everything shut down, we can gracefully exit.")

    def _delete_first_run_file() -> None:
        """
        deletes the ".first_run_done" file in the config directory.
        """
        config_directory = GulpConfig.get_instance().config_dir()
        check_first_run_file = os.path.join(config_directory, ".first_run_done")
        if os.path.exists(check_first_run_file):
            muty.file.delete_file_or_dir(check_first_run_file)
            GulpLogger.get_logger().warning("deleted: %s" % (check_first_run_file))

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

