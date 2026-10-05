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
- Lifecycle management for graceful startup and shutdown
- Integration with other Gulp components (Collab, OpenSearch, WebSocket)

This architecture enables Gulp to efficiently handle concurrent requests and distribute
workloads across multiple processes while maintaining consistent state management.
"""

import atexit
import asyncio
import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue

from aiomultiprocess import Pool as AioProcessPool
from asyncio_pool import AioPool as AioCoroPool

from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.s3_api import GulpS3
from gulp.api.ws_api import GulpRedisBroker
from gulp.config import GulpConfig
from gulp.glug import Glug


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

        # allow main/worker processes to spawn threads
        self.thread_pool: ThreadPoolExecutor = None
        self.server_id: str = None

        # allow the main process to spawn worker processes
        self.process_pool: AioProcessPool = None
        self._log_level: int = None
        self._main_process: bool = True
        self._background_tasks: set[asyncio.Task] = set()
        self._worker_result_tasks: set[asyncio.Task] = set()

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

    async def worker_cleanup(self) -> None:
        """
        cleanup routine called when a worker process terminates.

        override this method in a subclass to perform custom cleanup logic
        (e.g. flushing buffers, closing connections) when a worker exits.
        this method runs in the worker's event loop, after all tasks have finished.
        """
        async def _stop_glug_reconfig() -> None:
            from gulp.glug import reconfig as glug_reconfig

            await glug_reconfig.stop_subscriber()

        cleanup_steps = (
            ("background tasks", self.cancel_bg_tasks),
            ("thread pool", lambda: self.close_thread_pool(wait=True)),
            ("OpenSearch", lambda: GulpOpenSearch.get_instance().shutdown()),
            ("PostgreSQL", lambda: GulpCollab.get_instance().shutdown()),
            ("glug reconfig subscriber", _stop_glug_reconfig),
            ("Redis", lambda: GulpRedis.get_instance().shutdown()),
            ("S3", lambda: GulpS3.get_instance().shutdown()),
        )
        for name, cleanup in cleanup_steps:
            try:
                await cleanup()
            except Exception:
                Glug.get_instance().exception(f"worker cleanup failed for {name}")

        Glug.get_instance().debug(f"worker_cleanup completed (pid={os.getpid()})")

    async def cancel_bg_tasks(self) -> None:
        """Cancel and await fire-and-forget tasks owned by this process."""
        tasks = tuple(self._background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.difference_update(tasks)

    @staticmethod
    def _worker_finalizer() -> None:
        """
        finalizer for a worker process, registered via atexit by _worker_initializer.

        runs :meth:`worker_cleanup` in the same event loop used during worker
        initialization so that async cleanup code can safely await coroutines.
        """
        p = GulpProcess.get_instance()
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(p.worker_cleanup())
        except Exception:
            Glug.get_instance().exception("_worker_finalizer error")

    @staticmethod
    def _worker_exception_handler(ex: Exception):
        """
        for debugging purposes only, to catch exception eaten by the aiomultiprocess pool (they're critical exceptions, the process dies) ...
        """
        # Glug.get_instance().exception(f"worker exception: {ex}")
        return

    @staticmethod
    def _apply_worker_runtime_config(workers_concurrency_max: int) -> None:
        """Apply main-process runtime values to a newly spawned worker."""
        GulpConfig.get_instance().config()["workers_concurrency_max"] = max(
            1, workers_concurrency_max
        )

    @staticmethod
    def _worker_initializer(
        server_id: str,
        spawn_key: str,
        log_level: int = None,
        workers_concurrency_max: int = 1,
        log_file: str = None,
    ):  # type: ignore
        """
        initializes a worker process

        NOTE: this is run IN THE WORKER process before anything else.

        Args:
            server_id (str): the server ID
            spawn_key (str): the redis key to INCR to notify main process of worker process spawn
            log_level (int, optional): the log level. Defaults to None.
            workers_concurrency_max (int): runtime concurrency resolved by the main process.
            log_file (str, optional): plain-text file shared with the main process, receiving the
                same lines as stdout. Defaults to None (stdout only).
        """
        # bootstrap glug for this worker process as the very first thing, before any other
        # logging (including GulpConfig's own early debug calls just below): log_level/
        # log_file are already available as parameters here, so there is no reason
        # to wait for finish_initialization() below — a bare or default-named get_instance()
        # this early would lock in a level=INFO, name=None (root logger) instance that every
        # .debug() call before the real bootstrap would silently be dropped against.
        # finish_initialization() reconfigures the same name again — cheap and idempotent,
        # kept as a safety net, mirroring GulpServer.start()/_lifespan_handler()'s main-process
        # pattern.
        Glug.get_instance(
            name="glug-worker-%d" % (os.getpid()),
            log_file=log_file,
            level=log_level or logging.INFO,
            process_role="worker",
            reconfigure=True,
        )
        Glug.get_instance().debug(
            f"_worker_initializer starting, server_id={server_id}, spawn_key={spawn_key}"
        )

        GulpProcess._apply_worker_runtime_config(workers_concurrency_max)

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
                    log_level=log_level,
                    log_file=log_file,
                    is_worker=True,
                )
            )
        except Exception:
            Glug.get_instance().exception("_worker_initializer failed")

        # notify main process via Redis counter (spawn_key)
        try:
            redis_client = GulpRedis.get_instance().client()
            if redis_client is not None and spawn_key:
                try:
                    loop.run_until_complete(redis_client.incr(spawn_key))
                    # refresh TTL in case main set a short expiry
                    loop.run_until_complete(redis_client.expire(spawn_key, 40))
                except Exception:
                    Glug.get_instance().warning(
                        f"_worker_initializer: failed to INCR spawn key {spawn_key}"
                    )
        except Exception:
            Glug.get_instance().warning(
                "_worker_initializer: redis client not available to INCR spawn key "
                f"{spawn_key}"
            )

        # register cleanup to run when this worker process exits
        atexit.register(GulpProcess._worker_finalizer)
        Glug.get_instance().debug(
            f"_worker_initializer done, server_id={server_id}, sys.path={sys.path}, "
            f"spawn_key={spawn_key}"
        )

    async def close_thread_pool(self, wait: bool = True):
        """
        closes the thread pool

        Args:
            wait (bool, optional): whether to wait for all threads to finish. Defaults to True
        """
        if self.thread_pool:
            Glug.get_instance().debug("closing thread pool")
            self.thread_pool.shutdown(wait)
            Glug.get_instance().debug("thread pool closed")

    async def close_process_pool(self):
        """
        closes the worker process pool
        """
        if self.process_pool:
            pool = self.process_pool
            join_task: asyncio.Task | None = None
            pool_stopped = False
            Glug.get_instance().debug(f"closing mp pool {pool}")
            try:
                pool.close()
                Glug.get_instance().debug("joining mp pool")
                join_task = asyncio.create_task(pool.join())
                try:
                    await asyncio.wait_for(asyncio.shield(join_task), timeout=5)
                    pool_stopped = True
                    Glug.get_instance().debug("mp pool joined")
                except asyncio.TimeoutError:
                    # If the graceful join times out, workers are stuck. Shielding
                    # keeps the join alive so it can reap them after termination.
                    Glug.get_instance().warning(
                        "mp pool join timed out, terminating forcefully"
                    )
                    # Forcefully terminate the workers, then finish joining.
                    pool.terminate()
                    Glug.get_instance().warning("mp pool forced terminated")
                    await join_task
                    pool_stopped = True
            except asyncio.CancelledError:
                if join_task:
                    if not join_task.done():
                        pool.terminate()
                        await asyncio.shield(join_task)
                    pool_stopped = True
                raise
            except Exception:
                Glug.get_instance().exception("error closing process pool")
            finally:
                if pool_stopped:
                    drain_tasks = tuple(self._worker_result_tasks)
                    for task in drain_tasks:
                        task.cancel()
                    if drain_tasks:
                        await asyncio.gather(*drain_tasks, return_exceptions=True)
                    self._worker_result_tasks.difference_update(drain_tasks)
                # clear the reference to the pool
                self.process_pool = None
                Glug.get_instance().debug("close_process_pool done")

    async def finish_initialization(
        self,
        server_id: str,
        log_level: int = None,
        log_file: str = None,
        is_worker: bool = False,
    ) -> None:
        """
        last initializion steps in main or worker gulp process, called both by main process and also by each worker initializer

        Args:
            server_id (str): the server ID
            log_level (int, optional): the log level for the logger. Defaults to None.
            log_file (str, optional): plain-text file shared by every gulp process, receiving the
                same lines as stdout. Defaults to None (stdout only; main already bootstraps its own
                instance earlier in GulpServer._startup(); this is only used to bootstrap each
                worker's own instance).
            is_worker (bool): True if this is a worker process, False if main process
        """

        # only in a worker process we're passed the queue and shared_memory by the process pool initializer
        self._main_process = not is_worker

        # initializes thread pool for the main or worker process
        self.thread_pool = ThreadPoolExecutor()
        self.server_id = server_id

        if self._main_process:
            ###############################
            # main process initialization
            ###############################
            Glug.get_instance().info(f"initializing MAIN process (server_id={self.server_id})")
            self._log_level = log_level

            # creates the process pool and shared queue
            Glug.get_instance().debug(
                "creating process pool (respawn after "
                f"{GulpConfig.get_instance().parallel_processes_respawn_after_tasks()} tasks)"
            )

            num_workers: int = GulpConfig.get_instance().workers_max()

            # prepare a Redis-based spawn counter key (unique per server run)
            spawn_key: str = "gulp:spawn:%s:%d" % (server_id, int(time.time()))
            try:
                await GulpRedis.get_instance().client().set(spawn_key, 0, ex=40)
            except Exception:
                Glug.get_instance().exception("failed to create/reset spawn key in redis")
                raise

            # load extension plugins
            from gulp.api.server_api import GulpServer

            # start workers
            # each worker will call finish_initialization as well and INCR the spawn_key
            child_concurrency = GulpConfig.get_instance().workers_concurrency_max()
            self.process_pool = AioProcessPool(
                exception_handler=GulpProcess._worker_exception_handler,
                processes=num_workers,
                childconcurrency=child_concurrency,
                maxtasksperchild=GulpConfig.get_instance().parallel_processes_respawn_after_tasks(),
                initializer=GulpProcess._worker_initializer,
                initargs=(
                    server_id,
                    spawn_key,
                    log_level,
                    child_concurrency,
                    log_file,
                ),
            )

            # wait for all workers to be spawned by polling Redis counter
            Glug.get_instance().debug("waiting for all processes to be spawned")
            start_time = time.time()
            timeout = 60
            spawned_count = 0
            while True:
                try:
                    val = await GulpRedis.get_instance().client().get(spawn_key)
                    spawned_count = int(val or 0)
                except Exception:
                    spawned_count = 0
                if spawned_count >= num_workers:
                    break
                if time.time() - start_time > timeout:
                    Glug.get_instance().critical(
                        f"timeout waiting for workers to spawn (got {spawned_count}/{num_workers})"
                    )
                    raise TimeoutError("timeout waiting for workers to spawn")

                await asyncio.sleep(0.1)

            Glug.get_instance().debug(f"all {spawned_count} processes spawned")

            # load extension plugins
            await GulpServer.get_instance()._load_extension_plugins()

            Glug.get_instance().warning(
                f"MAIN process initialized, server_id={server_id}, sys.path={sys.path}"
            )
        else:
            ###############################
            # worker process initialization
            ###############################
            # in the worker process, initialize redis, opensearch and collab clients (main process already did it)
            # bootstrap glug for this worker process as the very first thing, before
            # any other logging in this branch: every call below must already see
            # the requested level, not the bare-instance INFO default.
            # Its live level/dev_mode/log_dir reconfig subscriber (independent from
            # GulpRedisBroker's pub/sub, which workers deliberately do not get)
            # starts later, once Redis is actually initialized below.
            Glug.get_instance(
                name="glug-worker-%d" % (os.getpid()),
                log_file=log_file,
                level=log_level or logging.INFO,
                process_role="worker",
                reconfigure=True,
            )
            Glug.get_instance().info(
                f"initializing WORKER process (server_id={self.server_id})"
            )
            # read configuration in worker
            GulpConfig.get_instance()

            # in the worker process, initialize opensearch and collab clients (main process already did it)
            opensearch = GulpOpenSearch.get_instance()
            await opensearch.bulk_max_content_length_bytes()
            await GulpCollab.get_instance().init()
            GulpRedis.get_instance().initialize(
                server_id, main_process=False
            )  # do not initialize pub/sub in worker process, just redis client

            from gulp.glug import reconfig as glug_reconfig

            await glug_reconfig.sync_from_persisted_state()
            await glug_reconfig.start_subscriber()

        # always connect to storage (both worker/main) since both may need it
        if self._main_process:
            # ensure the "gulp" bucket exists in the storage, creating it if it doesn't exist and we have permissions to create it
            await GulpS3.get_instance().create_bucket()

    def is_main_process(self) -> bool:
        """
        returns whether this is the main gulp process.
        either, it is a worker process.
        """
        return self._main_process
