"""
The `process` module implements the core processing functionality for the Gulp application.

It provides the `GulpProcess` class which manages the application's process architecture,
handling both the main process and worker processes. This module establishes the foundation
for parallel processing, thread management, and inter-process communication within the Gulp
application.

The module implements a singleton pattern for the GulpProcess class to ensure only one
instance exists per process. It manages shared resources like thread pools, coroutine pools,
process pools, and websocket queues, facilitating efficient communication between processes.

Key components:
- Process pool management for parallel task execution
- Thread and coroutine pools for concurrent operations
- Inter-process communication through shared queues
- Lifecycle management for graceful startup and shutdown
- Integration with other Gulp components (Collab, OpenSearch, WebSocket)

This architecture enables Gulp to efficiently handle concurrent requests and distribute
workloads across multiple processes while maintaining consistent state management.
"""

import asyncio
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Manager, Queue, Value
from multiprocessing.managers import DictProxy, SyncManager

from aiomultiprocess import Pool as AioProcessPool
from asyncio_pool import AioPool as AioCoroPool
from muty.log import MutyLogger

from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.ws_api import GulpRedisBroker
from gulp.config import GulpConfig


class GulpProcess:
    """
    represents the main or one of the worker processes for the Gulp application.

    It manages shared resources like thread pools, coroutine pools, process pools,
    and websocket queues, facilitating efficient communication between processes.

    The GulpProcess class is responsible for initializing and managing the lifecycle of
    the main and worker processes, including graceful startup and shutdown procedures.
    """

    _instance: "GulpProcess" = None

    def __init__(self):
        self._initialized: bool = True
        self.mp_manager: SyncManager = None

        # allow main/worker processes to spawn threads
        self.thread_pool: ThreadPoolExecutor = None

        # allow the main process to spawn worker processes
        self.process_pool: AioProcessPool = None
        self._log_level: int = None
        self._logger_file_path: str = None
        self._log_to_syslog: bool = False
        self._main_process: bool = True

    def __new__(cls) -> "GulpProcess":
        """
        creates a new instance of GulpProcess, or returns the existing one.

        implements the singleton pattern by ensuring only one instance exists.

        Args:
            None

        Returns:
            GulpProcess: the singleton instance
        """
        if cls._instance is None:
            # initialize the singleton instance
            cls._instance = super().__new__(cls)

        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpProcess":
        """
        returns the singleton instance of the gulp process.

        this method implements the singleton pattern to ensure only one
        gulp process instance exists per actual process.

        Args:
            None

        Returns:
            GulpProcess: the singleton instance of the gulp process
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @staticmethod
    def _worker_exception_handler(ex: Exception):
        """
        for debugging purposes only, to catch exception eaten by the aiomultiprocess pool (they're critical exceptions, the process dies) ...
        """
        # MutyLogger.get_instance().exception("WORKER EXCEPTION: %s" % (ex))
        return

    @staticmethod
    def _worker_initializer(server_id: str, spawned_processes: Value, lock: Lock, log_level: int = None, logger_file_path: str = None, log_to_syslog: tuple[str, str] = None):  # type: ignore
        """
        initializes a worker process

        NOTE: this is run IN THE WORKER process before anything else.

        Args:
            server_id (str): the server ID
            spawned_processes (Value): shared counter for the number of spawned processes (for ordered initialization)
            lock (Lock): shared lock for spawned_processes (for ordered initialization)
            log_level (int, optional): the log level. Defaults to None.
            logger_file_path (str, optional): the logger file path to log to file. Defaults to None, cannot be used with log_to_syslog.
            log_to_syslog (tuple[str,str], optional): the syslog address and facility to log to syslog. Defaults to (None, None).
                if (None, None) is passed, it defaults to ("/var/log" or "/var/run/syslog" depending what is available, "LOG_USER").
                cannot be used with logger_file_path.
        """
        # initialize paths immediately, before any unpickling happens
        plugins_path = GulpConfig.get_instance().path_plugins_default()
        plugins_path_extra = GulpConfig.get_instance().path_plugins_extra()

        # add plugin paths to sys.path immediately
        def _add_to_syspath(p: str):
            if p and p not in sys.path:
                # sys.path.insert(0, p)  # insert at beginning for priority
                sys.path.append(p)
                extension_path = os.path.join(p, "extension")
                if os.path.isdir(extension_path):
                    # sys.path.insert(0, extension_path)
                    sys.path.append(extension_path)

        _add_to_syspath(plugins_path)
        _add_to_syspath(plugins_path_extra)
        # note that we use prints here since this is called before the logger is initialized
        # print("******* PID=%d, sys.path=%s *******" % (os.getpid(), sys.path))

        p = GulpProcess.get_instance()
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                p.finish_initialization(
                    server_id,
                    lock=lock,
                    log_level=log_level,
                    logger_file_path=logger_file_path,
                    log_to_syslog=log_to_syslog,
                    is_worker=True,
                )
            )
        except Exception as ex:
            MutyLogger.get_instance(name="gulp").exception(ex)
        # done
        lock.acquire()
        spawned_processes.value += 1
        lock.release()
        MutyLogger.get_instance().warning(
            "_worker_initializer DONE, sys.path=%s, logger level=%d, logger_file_path=%s, spawned_processes=%d"
            % (
                sys.path,
                MutyLogger.get_instance().level,
                logger_file_path,
                spawned_processes.value,
            )
        )

    async def close_thread_pool(self, wait: bool = True):
        """
        closes the thread pool

        Args:
            wait (bool, optional): whether to wait for all threads to finish. Defaults to True
        """
        if self.thread_pool:
            MutyLogger.get_instance().debug("closing thread pool...")
            self.thread_pool.shutdown(wait)
            MutyLogger.get_instance().debug("thread pool closed!")

    async def close_process_pool(self):
        """
        closes the worker process pool
        """
        if self.process_pool:
            MutyLogger.get_instance().debug(
                "closing mp pool %s ..." % (self.process_pool)
            )
            try:
                self.process_pool.close()
                MutyLogger.get_instance().debug("joining mp pool...")
                await asyncio.wait_for(self.process_pool.join(), timeout=2)
                MutyLogger.get_instance().debug("mp pool joined!")

            except asyncio.TimeoutError:
                # if the graceful join times out, it means workers are stuck.
                MutyLogger.get_instance().warning(
                    "mp pool join timed out, terminating forcefully..."
                )
                # forcefully terminate the worker processes
                self.process_pool.terminate()
                await asyncio.sleep(1)
            except Exception as ex:
                MutyLogger.get_instance().exception(ex)

            finally:
                if self.mp_manager:
                    MutyLogger.get_instance().debug("shutting down mp manager...")
                    self.mp_manager.shutdown()
                    MutyLogger.get_instance().debug("mp manager shut down!")

                # clear the reference to the pool
                self.process_pool = None
                MutyLogger.get_instance().debug("mp pool closed!")

    async def finish_initialization(
        self,
        server_id: str,
        lock: Lock = None,  # type: ignore
        log_level: int = None,
        logger_file_path: str = None,
        log_to_syslog: tuple[str, str] = None,
        is_worker: bool = False,
    ) -> None:
        """
        last initializion steps in main or worker gulp process, called both by main process and also by each worker initializer

        Args:
            server_id (str): the server ID
            lock (Lock, optional): if set, will be acquired (and then released) during getting configuration instance in worker processes
            log_level (int, optional): the log level for the logger. Defaults to None.
            logger_file_path (str, optional): the log file path for the logger. Defaults to None, cannot be used with log_to_syslog.
            log_to_syslog (bool, optional): whether to log to syslog. Defaults to (None, None).
                if (None, None) is passed, it defaults to ("/var/log" or "/var/run/syslog" depending what is available, "LOG_USER").
                cannot be used with logger_file_path.
            is_worker (bool): True if this is a worker process, False if main process
        """

        # only in a worker process we're passed the queue and shared_memory by the process pool initializer
        self._main_process = not is_worker

        # initializes thread pool for the main or worker process
        self.thread_pool = ThreadPoolExecutor()

        if self._main_process:
            ###############################
            # main process initialization
            ###############################
            MutyLogger.get_instance().info("initializing MAIN process...")
            self._log_level = log_level
            self._logger_file_path = logger_file_path
            self._log_to_syslog = log_to_syslog

            # creates the process pool and shared queue
            MutyLogger.get_instance().debug(
                "creating process pool (respawn after %d tasks)..."
                % (GulpConfig.get_instance().parallel_processes_respawn_after_tasks())
            )

            # initializes the multiprocessing manager and structs
            self.mp_manager = Manager()
            spawned_processes = self.mp_manager.Value(int, 0)
            num_workers = GulpConfig.get_instance().parallel_processes_max()
            lock = self.mp_manager.Lock()

            # start workers
            # each worker will call finish_initialization as well
            self.process_pool = AioProcessPool(
                exception_handler=GulpProcess._worker_exception_handler,
                processes=num_workers,
                childconcurrency=GulpConfig.get_instance().concurrency_num_tasks(),
                maxtasksperchild=GulpConfig.get_instance().parallel_processes_respawn_after_tasks(),
                initializer=GulpProcess._worker_initializer,
                initargs=(
                    server_id,
                    spawned_processes,
                    lock,
                    MutyLogger.log_level,
                    MutyLogger.logger_file_path,
                    self._log_to_syslog,
                ),
            )
            # wait for all workers to be spawned
            MutyLogger.get_instance().debug(
                "waiting for all processes to be spawned ..."
            )
            while spawned_processes.value < num_workers:
                # MutyLogger.get_instance().debug('waiting for all processes to be spawned ...')
                await asyncio.sleep(0.1)

            MutyLogger.get_instance().debug(
                "all %d processes spawned!" % (spawned_processes.value)
            )

            MutyLogger.get_instance().warning(
                "MAIN process initialized, sys.path=%s" % (sys.path,)
            )

            # load extension plugins
            from gulp.api.server_api import GulpServer

            await GulpServer.get_instance()._load_extension_plugins()
        else:
            ###############################
            # worker process initialization
            ###############################
            # in the worker process, initialize redis, opensearch and collab clients (main process already did it)

            # we must initialize mutylogger here
            MutyLogger.get_instance(
                "gulp-worker-%d" % (os.getpid()),
                logger_file_path=logger_file_path,
                log_to_syslog=log_to_syslog,
                level=log_level,
            )
            MutyLogger.get_instance().info(
                "initializing WORKER process ...."
            )
            # read configuration in worker
            GulpConfig.get_instance()

            # in the worker process, initialize opensearch and collab clients (main process already did it)
            GulpOpenSearch.get_instance()
            await GulpCollab.get_instance().init()
            GulpRedis.get_instance().initialize(server_id, main_process=False) # do not initialize pub/sub in worker process, just redis client

            # worker process initialized
            MutyLogger.get_instance().info(
                "WORKER process initialized"
            )

            # register sigterm handler for the worker process
            #signal.signal(signal.SIGTERM, GulpProcess.sigterm_handler)



    @staticmethod
    async def _worker_cleanup():
        """
        cleanup the worker process (called as atexit handler)
        """
        MutyLogger.get_instance().debug(
            "WORKER process PID=%d cleanup initiated!" % (os.getpid())
        )
        # close shared ws and process pool
        try:
            # close clients
            await GulpCollab.get_instance().shutdown()
            await GulpOpenSearch.get_instance().shutdown()
            await GulpRedis.get_instance().shutdown(main_process=False)

            # close thread pool
            await GulpProcess.get_instance().close_thread_pool()

        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            MutyLogger.get_instance().info(
                "WORKER process PID=%d cleanup DONE!" % (os.getpid())
            )

    def sigterm_handler(signum, frame):
        MutyLogger.get_instance().debug(
            "SIGTERM received, cleaning up worker process PID=%d..." % (os.getpid())
        )
        try:
            # get the current event loop
            loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
            if loop.is_running():
                # if the loop is running, create a task for cleanup
                loop.create_task(GulpProcess._worker_cleanup())
            else:
                # if the loop is not running, run the cleanup synchronously
                asyncio.run(GulpProcess._worker_cleanup())
        except Exception as ex:
            # log any exception during cleanup
            MutyLogger.get_instance().exception(ex)

    def is_main_process(self) -> bool:
        """
        returns whether this is the main gulp process.
        either, it is a worker process.
        """
        return self._main_process
