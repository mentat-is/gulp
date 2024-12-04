import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Manager, Queue, Value

from aiomultiprocess import Pool as AioProcessPool
from asyncio_pool import AioPool as AioCoroPool
from muty.log import MutyLogger

from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpSharedWsQueue
from gulp.config import GulpConfig


class GulpProcess:
    """
    represents the main or one of the worker processes for the Gulp application.

    how a GulpProcess starts depending on it is the main process or a worker:

    - each GulpProcess is initialized by calling `init_gulp_process`, which handles both main and worker processes initialization

    - the main GulpProcess is initialized at application startup and is responsible for creating the worker processes pool and the shared websocket queue
    which is used by workers to fill the websocket with data to be sent to the clients.

    - each worker GulpProcess is initialized when a worker process is spawned and is responsible for initializing the worker process.

    - each GulpProcess, main and worker, have its own executors and clients to communicate with other parts of gulp.
        specifically, they are implemented as singletons to guarantee only one instance per-process is created.
            - GulpProcess.get_instance().process_pool: process pool executor (only the main process, to spawn worker)
            - GulpProcess.get_instance().thread_pool: thread pool executor
            - GulpProcess.get_instance().coro_pool: coroutine pool executor
            - GulpCollab.get_instance(): the collab client
            - GulpOpenSearch.get_instance(): the opensearch client
            - GulpSharedWsQueue.get_instance(): the shared websocket queue
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self.mp_manager = Manager()

            # allow main/worker processes to spawn threads
            self.thread_pool: ThreadPoolExecutor = None
            # allow main/worker processes to spawn coroutines
            self.coro_pool: AioCoroPool = None
            # allow the main process to spawn worker processes
            self.process_pool: AioProcessPool = None

            self._log_level: int = None
            self._logger_file_path: str = None
            self._main_process = True

    @classmethod
    def get_instance(cls) -> "GulpProcess":
        """
        returns the singleton instance of the OpenSearch client.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    @staticmethod
    def _worker_exception_handler(ex: Exception):
        """
        for debugging purposes only, to catch exception eaten by the aiomultiprocess pool (they're critical exceptions, the process dies) ...
        """
        MutyLogger.get_instance().exception("WORKER EXCEPTION: %s" % (ex))

    @staticmethod
    def _worker_initializer(spawned_processes: Value, lock: Lock, q: Queue, log_level: int = None, logger_file_path: str = None):  # type: ignore
        """
        initializes a worker process

        NOTE: this is run IN THE WORKER process before anything else.

        Args:
            spawned_processes (Value): shared counter for the number of spawned processes (for ordered initialization)
            lock (Lock): shared lock for spawned_processes (for ordered initialization)
            q (Queue): the shared websocket queue created by the main process
            log_level (int, optional): the log level. Defaults to None.
            logger_file_path (str, optional): the logger file path to log to file. Defaults to None.
        """
        p = GulpProcess.get_instance()
        asyncio.run(
            p.init_gulp_process(
                log_level=log_level, logger_file_path=logger_file_path, q=q
            )
        )

        # done
        lock.acquire()
        spawned_processes.value += 1
        lock.release()
        MutyLogger.get_instance().debug(
            "workerprocess initializer DONE, sys.path=%s, logger level=%d, logger_file_path=%s, spawned_processes=%d, ws_queue=%s"
            % (
                sys.path,
                MutyLogger.get_instance().level,
                logger_file_path,
                spawned_processes.value,
                q,
            )
        )

    async def close_coro_pool(self):
        """
        closes the coroutine pool
        """
        if self.coro_pool:
            MutyLogger.get_instance().debug("closing coro pool...")
            await self.coro_pool.cancel()
            await self.coro_pool.join()
            MutyLogger.get_instance().debug("coro pool closed!")

    async def close_thread_pool(self, wait: bool = True):
        """
        closes the thread pool

        Args:
            wait (bool, optional): whether to wait for all threads to finish. Defaults
        """
        if self.thread_pool:
            MutyLogger.get_instance().debug("closing thread pool...")
            self.thread_pool.shutdown(wait=wait)
            MutyLogger.get_instance().debug("thread pool closed!")

    async def close_process_pool(self):
        """
        closes the worker process pool
        """
        if self.process_pool:
            MutyLogger.get_instance().debug("closing mp pool...")
            self.process_pool.close()
            await self.process_pool.join()
            self.process_pool.terminate()
            MutyLogger.get_instance().debug("mp pool closed!")

    async def recreate_process_pool_and_shared_queue(self):
        """
        creates (or recreates if already running) the worker processes pool
        and the shared websocket queue, waiting for them to terminate first.

        each worker starts in _worker_initializer, which further calls init_gulp_process to initialize the worker process.
        """
        if not self._main_process:
            raise RuntimeError("only the main process can recreate the process pool")

        if self.process_pool:
            # close the worker process pool gracefully if it is already running
            await self.close_process_pool()

        spawned_processes = self.mp_manager.Value(int, 0)
        num_workers = GulpConfig.get_instance().parallel_processes_max()
        lock = self.mp_manager.Lock()

        # re/create the shared websocket queue (closes it first if already running)
        q = GulpSharedWsQueue.get_instance().init_queue(self.mp_manager)

        # start workers, pass the shared queue to each
        self.process_pool = AioProcessPool(
            exception_handler=GulpProcess._worker_exception_handler,
            processes=num_workers,
            childconcurrency=GulpConfig.get_instance().concurrency_max_tasks(),
            maxtasksperchild=GulpConfig.get_instance().parallel_processes_respawn_after_tasks(),
            initializer=GulpProcess._worker_initializer,
            initargs=(
                spawned_processes,
                lock,
                q,
                MutyLogger.log_level,
                MutyLogger.logger_file_path,
            ),
        )

        # wait for all processes are spawned
        MutyLogger.get_instance().debug("waiting for all processes to be spawned ...")
        while spawned_processes.value < num_workers:
            # MutyLogger.get_instance().debug('waiting for all processes to be spawned ...')
            await asyncio.sleep(0.1)

        MutyLogger.get_instance().debug(
            "all %d processes spawned!" % (spawned_processes.value)
        )

    async def init_gulp_process(
        self, log_level: int = None, logger_file_path: str = None, q: Queue = None
    ) -> None:
        """
        initializes main or worker gulp process

        Args:
            log_level (int, optional): the log level for the logger. Defaults to None.
            logger_file_path (str, optional): the log file path for the logger. Defaults to None.
            q: (Queue, optional): the shared websocket queue created by the main process(we are called in a worker process).
                Defaults to None (we are called in the main process)
        """

        # only in a worker process we're passed the queue by the process pool initializer
        self._main_process = q is None
        if self._main_process:
            if self._log_level:
                log_level = self._log_level
                logger_file_path = self._logger_file_path
                MutyLogger.get_instance().warning("reinitializing main process...")
            else:
                MutyLogger.get_instance().info("initializing main process...")
                self._log_level = log_level
                self._logger_file_path = logger_file_path
        else:
            # we must initialize mutylogger here
            MutyLogger.get_instance(
                "gulp-worker-%d" % (os.getpid()),
                logger_file_path=logger_file_path,
                level=log_level,
            )
            MutyLogger.get_instance().info(
                "initializing worker process, q=%s ..." % (q)
            )

        # sys.path fix is needed to load plugins from the plugins directories correctly
        plugins_path = GulpConfig.get_instance().path_plugins()
        ext_plugins_path = GulpConfig.get_instance().path_plugins(extension=True)
        MutyLogger.get_instance().debug(
            "plugins_path=%s, extension plugins_path=%s"
            % (plugins_path, ext_plugins_path)
        )
        if plugins_path not in sys.path:
            sys.path.append(plugins_path)
        if ext_plugins_path not in sys.path:
            sys.path.append(ext_plugins_path)

        # read configuration
        GulpConfig.get_instance()

        # initializes executors
        await self.close_coro_pool()
        await self.close_thread_pool()
        self.coro_pool = AioCoroPool(GulpConfig.get_instance().concurrency_max_tasks())
        self.thread_pool = ThreadPoolExecutor()

        # initialize collab and opensearch clients
        collab = GulpCollab.get_instance()
        await collab.init(main_process=self._main_process)
        GulpOpenSearch.get_instance()

        if self._main_process:
            # creates the process pool and shared queue
            await self.recreate_process_pool_and_shared_queue()
        else:
            # worker process, set the queue
            GulpSharedWsQueue.get_instance().set_queue(q)

    def is_main_process(self) -> bool:
        """
        returns whether this is the main gulp process.
        either, it is a worker process.
        """
        return GulpProcess.get_instance()._main_process