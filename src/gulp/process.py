import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
from queue import Queue
import sys
import timeit
from multiprocessing import Lock, Queue, Value

import json5
import muty.file
import muty.list
import muty.log
import muty.string
import muty.time
import muty.uploadfile
from sqlalchemy.ext.asyncio import AsyncEngine

from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.collab.stats import GulpIngestionStats
from gulp.structs import GulpPluginParameters, GulpPluginType, ObjectNotFound
from gulp.plugin import GulpPluginBase
from gulp.utils import GulpLogger
from gulp.config import GulpConfig
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpSharedWsQueue
from asyncio_pool import AioPool as AioCoroPool
from aiomultiprocess import Pool as AioProcessPool
from multiprocessing import Manager

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

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self.mp_manager = Manager()

            # allow main/worker processes to spawn threads
            self.thread_pool: ThreadPoolExecutor = None
            # allow main/worker processes to spawn coroutines
            self.coro_pool:AioCoroPool = None
            # allow the main process to spawn worker processes
            self.process_pool:AioProcessPool = None
            
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
        GulpLogger.get_logger().exception("WORKER EXCEPTION: %s" % (ex))

    @staticmethod
    def _worker_initializer(spawned_processes: Value, lock: Lock, q: Queue, log_level: int = None, log_file_path: str = None):  # type: ignore
        """
        initializes a worker process

        NOTE: this is run IN THE WORKER process before anything else.

        Args:
            spawned_processes (Value): shared counter for the number of spawned processes (for ordered initialization)
            lock (Lock): shared lock for spawned_processes (for ordered initialization)
            q (Queue): the shared websocket queue created by the main process
            log_level (int, optional): the log level. Defaults to None.
            log_file_path (str, optional): the log file path. Defaults to None.
        """
        p = GulpProcess.get_instance()
        asyncio.run(p.init_gulp_process(log_level=log_level, log_file_path=log_file_path, q=q))

        # done
        lock.acquire()
        spawned_processes.value += 1
        lock.release()
        GulpLogger.get_logger().warning(
            "workerprocess initializer DONE, sys.path=%s, logger level=%d, log_file_path=%s, spawned_processes=%d, ws_queue=%s"
            % (
                sys.path,
                GulpLogger.get_logger().level,
                log_file_path,
                spawned_processes.value,
                q,
            )
        )

    async def close_coro_pool(self):
        """
        closes the coroutine pool
        """
        if self.coro_pool:
            GulpLogger.get_logger().debug("closing coro pool...")
            await self.coro_pool.cancel()
            await self.coro_pool.join()
            GulpLogger.get_logger().debug("coro pool closed!")
    
    async def close_thread_pool(self, wait: bool=True):
        """
        closes the thread pool

        Args:
            wait (bool, optional): whether to wait for all threads to finish. Defaults
        """
        if self.thread_pool:
            GulpLogger.get_logger().debug("closing thread pool...")
            self.thread_pool.shutdown(wait=wait)
            GulpLogger.get_logger().debug("thread pool closed!")

    async def close_process_pool(self):
        """
        closes the worker process pool
        """
        if self.process_pool:
            GulpLogger.get_logger().debug("closing mp pool...")
            self.process_pool.close()
            await self.process_pool.join()
            GulpLogger.get_logger().debug("mp pool closed!")

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
            self.close_process_pool()

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
                GulpLogger.get_logger().level,
                GulpLogger.get_instance().log_file_path,
            ),
        )

        # wait for all processes are spawned
        GulpLogger.get_logger().debug("waiting for all processes to be spawned ...")
        while spawned_processes.value < num_workers:
            # GulpLogger.get_logger().debug('waiting for all processes to be spawned ...')
            await asyncio.sleep(0.1)

        GulpLogger.get_logger().debug(
            "all %d processes spawned!" % (spawned_processes.value)
        )        

    async def init_gulp_process(self, log_level: int=None, log_file_path: str=None, is_main_process: bool=True, q: Queue=None) -> None:
        """
        initializes main or worker gulp process

        Args:
            log_level (int, optional): the log level for the logger. Defaults to None.
            log_file_path (str, optional): the log file path for the logger. Defaults to None.
            q: (Queue, optional): the shared websocket queue created by the main process(we are called in a worker process).
                Defaults to None (we are called in the main process)
        """
        GulpLogger.get_instance().reconfigure(log_file_path=log_file_path, level=log_level)
        
        # only in a worker process we're passed the queue by the process pool initializer
        self._main_process = (q is None)
        if self._main_process:
            GulpLogger.get_logger().info("initializing main process...")
        else:
            GulpLogger.get_logger().info("initializing worker process...")
        
        # sys.path fix is needed to load plugins from the plugins directories correctly
        plugins_path = GulpConfig.get_instance().path_plugins()
        ext_plugins_path = GulpConfig.get_instance().path_plugins(extension=True)
        GulpLogger.get_logger().debug('plugins_path=%s, extension plugins_path=%s' % (plugins_path, ext_plugins_path))
        if plugins_path not in sys.path:
            sys.path.append(plugins_path)
        if ext_plugins_path not in sys.path:
            sys.path.append(ext_plugins_path)

        # read configuration
        GulpConfig.get_instance()

        # initializes executors
        self.coro_pool = AioCoroPool(GulpConfig.get_instance().concurrency_max_tasks())
        self.thread_pool = ThreadPoolExecutor()

        # initialize collab and opensearch clients
        collab = GulpCollab.get_instance()
        await collab.init()
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
    
# async def _print_debug_ingestion_stats(collab: AsyncEngine, req_id: str):
#     """
#     get the stats for an ingestion request and print the time elapsed
#     """
#     if __debug__:
#         # get stats for this request
#         cs = await GulpStats.get(collab, GulpCollabFilter(req_id=[req_id]))
#         cs = cs[0]

#         # compute some time stats
#         elapsed = cs.time_end - cs.time_created
#         seconds = elapsed / 1000
#         minutes = seconds / 60
#         hours = minutes / 60
#         started_at = muty.time.unix_millis_to_datetime(cs.time_created).isoformat()

#         # we use error just to print the exception in any case, even when most of the debug messages are skipped
#         GulpLogger.get_logger().error(
#             "(NOT AN ERROR) req_id=%s, started_at=%s, took seconds=%f, (minutes=%f, hours=%f)\n"
#             "final stats req_id=%s: processed=%d, failed=%d, skipped=%d, num (UNIQUE) ingest_errors=%d, QueuePool status=%s"
#             % (
#                 cs.req_id,
#                 started_at,
#                 seconds,
#                 minutes,
#                 hours,
#                 cs.req_id,
#                 cs.ev_processed,
#                 cs.ev_failed,
#                 cs.ev_skipped,
#                 len(cs.ingest_errors) if cs.ingest_errors is not None else 0,
#                 collab.pool.status(),
#             )
#         )


# async def _ingest_file_task_internal(
#     index: str,
#     req_id: str,
#     client_id: int,
#     operation_id: int,
#     context: str,
#     plugin: str,
#     src_file: str | list[dict],
#     ws_id: str,
#     plugin_params: GulpPluginParameters = None,
#     flt: GulpIngestionFilter = None,
#     user_id: int = None,
#     **kwargs,
# ) -> GulpRequestStatus:
#     """
#     Ingests a single file using the specified plugin.

#     NOTE: This runs in an async task in one of the worker process.

#     Args:
#         index (str): The elasticsearch index name.
#         req_id (str): The request ID.
#         client_id (int): The client id in the collab database.
#         operation_id (int): The operation id.
#         context (str): The context.
#         plugin (str): The plugin name.
#         src_file (str): The source file path or a list of events (for the "raw" plugin).
#         ws_id (str): The websocket id
#         plugin_params (GulpPluginParameters, optional): The ingestion plugin parameters. Defaults to None.
#         flt (GulpIngestionFilter, optional): The filter to apply during ingestion. Defaults to None.
#         user_id (int, optional): The user id. Defaults to None.
#         kwargs: Additional keyword arguments.

#     Returns:
#         GulpRequestStatus

#     Raises:
#         Any exceptions raised by the plugin during ingestion.
#     """
#     collab = await collab_api.session()
#     elastic = opensearch_api.elastic()

#     mod = None
#     if plugin == "raw":
#         # src_file is a list of events
#         GulpLogger.get_logger().debug(
#             "ingesting %d raw events with plugin=%s, collab=%s, elastic=%s, flt(%s)=%s, plugin_params=%s ..."
#             % (len(src_file), plugin, collab, elastic, type(flt), flt, plugin_params)
#         )
#     else:
#         GulpLogger.get_logger().debug(
#             "ingesting file=%s with plugin=%s, collab=%s, elastic=%s, flt(%s)=%s, plugin_params=%s ..."
#             % (src_file, plugin, collab, elastic, type(flt), flt, plugin_params)
#         )

#     # load plugin
#     try:
#         mod: GulpPluginBase = gulp.plugin.load_plugin(plugin)
#     except Exception as ex:
#         # can't load plugin ...
#         t = TmpIngestStats(src_file).update(ingest_errors=[ex])
#         status, _ = await GulpStats.update(
#             collab,
#             req_id,
#             ws_id,
#             fs=t,
#             file_done=True,
#             new_status=GulpRequestStatus.FAILED,
#         )
#         return status

#     # ingest
#     start_time = timeit.default_timer()
#     if flt is None:
#         flt = GulpIngestionFilter()

#     status = await mod.ingest_file(
#         index,
#         req_id,
#         client_id,
#         operation_id,
#         context,
#         src_file,
#         ws_id,
#         plugin_params=plugin_params,
#         flt=flt,
#         user_id=user_id,
#     )
#     end_time = timeit.default_timer()
#     execution_time = end_time - start_time
#     GulpLogger.get_logger().debug(
#         "execution time for ingesting file %s: %f sec." % (src_file, execution_time)
#     )

#     # unload plugin
#     gulp.plugin.unload_plugin(mod)
#     return status


# async def ingest_directory_task(
#     **kwargs,
# ) -> None:
#     """
#     Ingests all files in a directory using multiple processes and a task per file.

#     NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

#     Args:
#         **kwargs: Additional keyword arguments.

#     Keyword Args:
#         index (str): The elasticsearch index name.
#         req_id (str): The request ID.
#         directory (str): The directory path.
#         plugin (str): The plugin name.
#         client_id(int): the client
#         operation_id (int): The operation id.
#         context (str): The context.
#         ws_id (str): The websocket id.
#         plugin_params (GulpPluginParameters, optional): The plugin parameters.
#         flt (GulpIngestionFilter, optional): The filter.
#         q (list[GulpQueryParameter], optional): The list of GulpQueryParameter objects. Defaults to None.
#         q_options (GulpQueryOptions, optional): The query options. Defaults to None.
#         user_id (int, optional): The user id. Defaults to None.
#         sigma_group_flts (list[SigmaGroupFilter], optional): to filter results on groups of sigma rule ids. Defaults to None.
#     """
#     executor = rest_api.process_executor()
#     collab = await collab_api.session()

#     index = kwargs["index"]
#     req_id = kwargs["req_id"]
#     directory = kwargs["directory"]
#     plugin = kwargs["plugin"]
#     client_id = kwargs["client_id"]
#     operation_id = kwargs["operation_id"]
#     ws_id = kwargs["ws_id"]
#     plugin_params = kwargs.get("plugin_params", None)
#     context = kwargs["context"]
#     flt = kwargs.get("flt", None)
#     user_id = kwargs.get("user_id", None)

#     # ingestion started for this request
#     files = await muty.file.list_directory_async(directory)
#     GulpLogger.get_logger().info("ingesting directory %s with files=%s ..." % (directory, files))
#     if len(files) == 0:
#         ex = ObjectNotFound("directory %s empty or not found!" % (directory))
#         await GulpStats.create(
#             collab,
#             GulpCollabType.STATS_INGESTION,
#             req_id,
#             ws_id,
#             operation_id,
#             client_id,
#             context,
#         )
#         await GulpStats.update(
#             collab,
#             req_id,
#             ws_id,
#             fs=TmpIngestStats(directory).update(ingest_errors=[ex]),
#             file_done=True,
#         )

#         return

#     # create the request stats
#     await GulpStats.create(
#         collab,
#         GulpCollabType.STATS_INGESTION,
#         req_id,
#         ws_id,
#         operation_id,
#         client_id,
#         context,
#         total_files=len(files),
#     )

#     tasks = []

#     # if not sync, parallelize ingestion through multiple worker processes, each one running asyncio tasks
#     for src_file in files:
#         tasks.append(
#             executor.apply(
#                 _ingest_file_task_internal,
#                 (
#                     index,
#                     req_id,
#                     client_id,
#                     operation_id,
#                     context,
#                     plugin,
#                     src_file,
#                     ws_id,
#                     plugin_params,
#                     flt,
#                     user_id,
#                 ),
#             )
#         )
#         if len(tasks) == GulpConfig.get_instance().multiprocessing_batch_size():
#             # run a batch
#             await asyncio.gather(*tasks, return_exceptions=True)
#             tasks = []

#     if len(tasks) > 0:
#         # run remaining tasks
#         await asyncio.gather(*tasks, return_exceptions=True)

#     await _print_debug_ingestion_stats(collab, req_id)


# async def ingest_single_file_or_events_task(
#     **kwargs,
# ) -> None:
#     """
#     ingests a single uploaded file or a chunk of events using a worker process

#     NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed (one) tasks.

#     Args:
#         **kwargs: Additional keyword arguments.

#     Keyword Args:
#         index (str): The elasticsearch index name.
#         req_id (str): The request ID.
#         f (str): path to the uploaded file to ingest (will be deleted when done).
#         plugin (str): The plugin name.
#         client_id (int): The client.
#         operation_id (int): The operation.
#         context (str): The context.
#         plugin_params (GulpPluginParameters): The plugin parameters.
#         flt (GulpIngestionFilter, optional): The filter.
#         user_id: (int): The user id, optional.
#         ws_id (str): The websocket id
#     """
#     executor = rest_api.process_executor()
#     collab = await collab_api.session()

#     index = kwargs["index"]
#     req_id = kwargs["req_id"]
#     f = kwargs["f"]
#     plugin = kwargs["plugin"]
#     client_id = kwargs["client"]
#     operation_id = kwargs["operation"]
#     context = kwargs["context"]
#     ws_id = kwargs["ws_id"]
#     flt = kwargs.get("flt", None)
#     plugin_params = kwargs.get("plugin_params", None)
#     user_id = kwargs.get("user_id", None)

#     # create the request stats
#     await GulpStats.create(
#         collab,
#         GulpCollabType.STATS_INGESTION,
#         req_id,
#         ws_id,
#         operation_id,
#         client_id,
#         context,
#         total_files=1,
#     )

#     # process single file or chunk of events
#     tasks = []
#     tasks.append(
#         executor.apply(
#             _ingest_file_task_internal,
#             (
#                 index,
#                 req_id,
#                 client_id,
#                 operation_id,
#                 context,
#                 plugin,
#                 f,
#                 ws_id,
#                 plugin_params,
#                 flt,
#                 user_id,
#             ),
#         )
#     )
#     await asyncio.gather(*tasks, return_exceptions=True)

#     if plugin != "raw":
#         # delete uploaded file folder
#         await muty.file.delete_file_or_dir_async(os.path.dirname(f))
#     await _print_debug_ingestion_stats(collab, req_id)


# async def ingest_zip_task(
#     **kwargs,
# ) -> None:
#     """
#     processes an uploaded zip file (i.e. from slurp) using multiple processes and a task per file.

#     NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

#     Args:
#         **kwargs: Additional keyword arguments.

#     Keyword Args:
#         index (str): The elasticsearch index name.
#         req_id (str): The request ID.
#         f (str): The uploaded zip file path
#         client_id (int): The client.
#         operation_id (int): The operation.
#         context (str): The context.
#         flt (GulpIngestionFilter, optional): The filter.
#         user_id: (int): The user id, optional.
#         ws_id (str): The websocket id
#     Returns:
#         None
#     """
#     executor = rest_api.process_executor()
#     collab = await collab_api.session()

#     index = kwargs["index"]
#     req_id = kwargs["req_id"]
#     f = kwargs["f"]
#     client_id = kwargs["client_id"]
#     operation_id = kwargs["operation_id"]
#     context = kwargs["context"]
#     ws_id = kwargs["ws_id"]
#     flt = kwargs.get("flt", None)
#     user_id = kwargs.get("user_id", None)
#     files_path = None

#     GulpLogger.get_logger().debug("ingesting zip file %s ..." % (f))

#     try:
#         # decompress
#         files_path = await muty.file.unzip(f)
#         GulpLogger.get_logger().debug("zipfile unzipped to %s" % (files_path))

#         # read metadata file
#         metadata_path = muty.file.safe_path_join(files_path, "metadata.json")
#         metadata = json5.loads(await muty.file.read_file_async(metadata_path))
#         GulpLogger.get_logger().debug(
#             "metadata.json content:\n %s" % (json5.dumps(metadata, indent=2))
#         )
#     except Exception as ex:
#         GulpLogger.get_logger().exception(ex)
#         await GulpStats.create(
#             collab,
#             GulpCollabType.STATS_INGESTION,
#             req_id,
#             ws_id,
#             operation_id,
#             client_id,
#             context,
#         )
#         await GulpStats.update(
#             collab,
#             req_id,
#             ws_id,
#             fs=TmpIngestStats(f).update(ingest_errors=[ex]),
#             new_status=GulpRequestStatus.FAILED,
#             file_done=True,
#             force=True,
#         )
#         await muty.file.delete_file_or_dir_async(files_path)
#         return

#     finally:
#         # delete uploaded file folder
#         await muty.file.delete_file_or_dir_async(os.path.dirname(f))

#     # get number of files
#     num_files = 0
#     for plugin in metadata.keys():
#         files = metadata[plugin]["files"]
#         for file in files:
#             num_files += 1

#     tasks = []
#     await GulpStats.create(
#         collab,
#         GulpCollabType.STATS_INGESTION,
#         req_id,
#         ws_id,
#         operation_id,
#         client_id,
#         context,
#         total_files=num_files,
#     )

#     # walk each key(=plugin)
#     for plugin in metadata.keys():
#         files = metadata[plugin]["files"]
#         plugin_params = metadata[plugin].get("plugin_params", None)
#         for file in files:
#             ff = muty.file.safe_path_join(files_path, file)
#             tasks.append(
#                 executor.apply(
#                     _ingest_file_task_internal,
#                     (
#                         index,
#                         req_id,
#                         client_id,
#                         operation_id,
#                         context,
#                         plugin,
#                         ff,
#                         ws_id,
#                         plugin_params,
#                         flt,
#                         user_id,
#                     ),
#                 )
#             )
#             if len(tasks) == GulpConfig.get_instance().multiprocessing_batch_size():
#                 # run a batch
#                 await asyncio.gather(*tasks, return_exceptions=True)
#                 tasks = []

#     if len(tasks) > 0:
#         # run remaining tasks
#         await asyncio.gather(*tasks, return_exceptions=True)

#     # delete unzipped files
#     await muty.file.delete_file_or_dir_async(files_path)
#     await _print_debug_ingestion_stats(collab, req_id)


# async def _rebase_internal(
#     index: str,
#     dest_index: str,
#     offset: int,
#     ws_id: str,
#     req_id: str,
#     flt: GulpIngestionFilter = None,
#     **kwargs,
# ) -> None:
#     """
#     rebase index to dest_index using a given offset

#     Args:
#         index (str): The elasticsearch index name.
#         dest_index (str): The destination index name.
#         offset (int): The rebase offset, in milliseconds.
#         ws_id (str): The websocket id
#         req_id (str): The request ID.
#         flt (GulpIngestionFilter, optional): The filter.
#         kwargs: Additional keyword arguments.
#     Returns:
#         None
#     """
#     elastic = opensearch_api.elastic()
#     ds = None
#     rebase_result = {}
#     template_file: str = None
#     try:
#         # get template for index and use it to create the destination datastream
#         template = await opensearch_api.index_template_get(elastic, index)
#         template_file = await muty.file.write_temporary_file_async(
#             json.dumps(template).encode()
#         )

#         # create another datastream (if it exists, it will be deleted)
#         ds = await opensearch_api.datastream_create(
#             elastic,
#             dest_index,
#             index_template=template_file,
#         )

#         # rebase
#         rebase_result = await opensearch_api.rebase(
#             elastic, index, dest_index, offset, flt=flt
#         )
#     except Exception as ex:
#         # can't rebase, delete the datastream
#         GulpLogger.get_logger().exception(ex)
#         ws_api.shared_queue_add_data(
#             gulp.api.ws_api.WsQueueDataType.REBASE_DONE,
#             req_id,
#             data={
#                 "status": GulpRequestStatus.FAILED,
#                 "index": index,
#                 "test_index": dest_index,
#                 "error": muty.log.exception_to_string(ex),
#             },
#             ws_id=ws_id,
#         )

#         if ds is not None:
#             await opensearch_api.datastream_delete(elastic, dest_index)
#         return
#     finally:
#         if template_file is not None:
#             await muty.file.delete_file_or_dir_async(template_file)
#     # done
#     GulpLogger.get_logger().debug("rebase result: %s" % (json.dumps(rebase_result, indent=2)))
#     ws_api.shared_queue_add_data(
#         gulp.api.ws_api.WsQueueDataType.REBASE_DONE,
#         req_id,
#         data={
#             "status": GulpRequestStatus.DONE,
#             "index": index,
#             "dest_index": dest_index,
#             "result": rebase_result,
#         },
#         ws_id=ws_id,
#     )


# async def rebase_task(
#     **kwargs,
# ) -> None:
#     """
#     perform index rebase

#     NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

#     Args:
#         collab (AsyncEngine): The SQL engine from SQLAlchemy.
#         elastic (AsyncElasticsearch): The Elasticsearch client.
#         executor (aiomultiprocess.Pool): The executor for running worker processes.
#         **kwargs: Additional keyword arguments.

#     Keyword Args:
#         index (str): The elasticsearch index name.
#         dest_index (str): The destination index name.
#         offset (int): The rebase offset, in milliseconds.
#         flt (GulpIngestionFilter, optional): The filter.
#     Returns:
#         None
#     """
#     executor = rest_api.process_executor()

#     index = kwargs["index"]
#     dest_index = kwargs["dest_index"]
#     offset = kwargs["offset"]
#     ws_id = kwargs["ws_id"]
#     flt = kwargs.get("flt", None)
#     req_id = kwargs["req_id"]

#     # use a worker process to perform rebase
#     tasks = []

#     # TODO: create stats
#     tasks.append(
#         executor.apply(
#             _rebase_internal,
#             (
#                 index,
#                 dest_index,
#                 offset,
#                 ws_id,
#                 req_id,
#                 flt,
#             ),
#         )
#     )
#     await asyncio.gather(*tasks, return_exceptions=True)


# async def _query_external_internal(
#     operation_id: int,
#     client_id: int,
#     user_id: int,
#     username: str,
#     plugin: str,
#     plugin_params: GulpPluginParameters,
#     ws_id: str,
#     req_id: str,
#     flt: GulpQueryFilter,
#     options: GulpQueryOptions,
# ) -> tuple[int, GulpRequestStatus]:

#     collab = await collab_api.session()
#     qs = TmpQueryStats()
#     qs.queries_total = 1

#     # load plugin
#     mod = None
#     raw_plugin = None
#     try:
#         mod: GulpPluginBase = gulp.plugin.load_plugin(
#             plugin, plugin_type=GulpPluginType.QUERY
#         )
#     except Exception as ex:
#         # can't load plugin ...
#         GulpLogger.get_logger().exception(ex)
#         await GulpStats.update(
#             collab,
#             req_id,
#             ws_id,
#             qs=qs,
#             force=True,
#             new_status=GulpRequestStatus.FAILED,
#             errors=[str(ex)],
#         )
#         return 0, GulpRequestStatus.FAILED

#     ingest_index = plugin_params.extra.get("ingest_index", None)
#     if ingest_index is not None:
#         # # load the raw plugin and pass the instance over in extra
#         raw_plugin: GulpPluginBase = gulp.plugin.load_plugin(
#             "raw", plugin_type=GulpPluginType.INGESTION
#         )
#         plugin_params.extra["raw_plugin"] = raw_plugin
#         plugin_params.extra["ingest_index"] = ingest_index

#     # query
#     try:
#         start_time = timeit.default_timer()
#         num_results, status = await mod.query(
#             operation_id,
#             client_id,
#             user_id,
#             username,
#             ws_id,
#             req_id,
#             plugin_params,
#             flt,
#             options,
#         )

#         end_time = timeit.default_timer()
#         execution_time = end_time - start_time
#         GulpLogger.get_logger().debug(
#             "execution time for querying plugin %s: %f sec." % (plugin, execution_time)
#         )
#     except Exception as ex:
#         # can't query external source ...
#         GulpLogger.get_logger().exception(ex)
#         await GulpStats.update(
#             collab,
#             req_id,
#             ws_id,
#             qs=qs,
#             force=True,
#             new_status=GulpRequestStatus.FAILED,
#             errors=[str(ex)],
#         )
#         return 0, GulpRequestStatus.FAILED

#     # unload plugin/s
#     gulp.plugin.unload_plugin(mod)
#     if raw_plugin:
#         gulp.plugin.unload_plugin(raw_plugin)
#     return num_results, status


# async def query_external_task(**kwargs):
#     """
#     Asynchronously handles a query plugin task by offloading the work to a worker process and
#     then processing the results.

#     Keyword Arguments:
#     req_id (str): The request ID.
#     ws_id (str): The workspace ID.
#     plugin (str): The plugin to be queried.
#     user_id (str): The user ID.
#     username (str): The username of the user.
#     plugin_params (GulpPluginParameters): Parameters for the plugin (external source parameters in "extra" dict).
#     operation_id (str): The operation ID.
#     client_id (str): The client ID.
#     flt (str): Filter criteria.
#     options (GulpQueryOptions, optional): Additional options for the query. Defaults to None.
#     Returns:
#     None
#     """

#     req_id = kwargs["req_id"]
#     ws_id = kwargs["ws_id"]
#     plugin = kwargs["plugin"]
#     user_id = kwargs["user_id"]
#     username = kwargs["username"]
#     plugin_params = kwargs["plugin_params"]
#     operation_id = kwargs["operation_id"]
#     client_id = kwargs["client_id"]
#     flt = kwargs["flt"]
#     options = kwargs.get("options", None)

#     # offload to a worker process
#     executor = rest_api.process_executor()
#     coro = executor.apply(
#         _query_external_internal,
#         (
#             operation_id,
#             client_id,
#             user_id,
#             username,
#             plugin,
#             plugin_params,
#             ws_id,
#             req_id,
#             flt,
#             options,
#         ),
#     )

#     num_results, status = await coro

#     # done
#     qs = TmpQueryStats()
#     qs.matches_total = num_results
#     qs.queries_total = 1
#     qs.queries_processed = 1
#     await GulpStats.update(
#         await collab_api.session(),
#         req_id,
#         ws_id,
#         qs=qs,
#         force=True,
#         new_status=status,
#     )
#     ws_api.shared_queue_add_data(
#         gulp.api.ws_api.WsQueueDataType.QUERY_DONE,
#         req_id,
#         {
#             "status": status,
#             # all queries combined total hits
#             "combined_total_hits": num_results,
#         },
#         ws_id=ws_id,
#     )


# async def query_multi_task(**kwargs):
#     """
#     Executes one or more queries, using multiple worker processes and a task per query.

#     Args:
#         **kwargs: Additional keyword arguments.

#     Keyword Args:
#         index (str): The elasticsearch index name.
#         user_id (int): The user id who performs the query.
#         username (str): The user name who performs the query.
#         req_id (str): The request ID.
#         q (list[GulpQueryParameter]): The list of GulpQueryParameter objects.
#         ws_id (str): The websocket id.
#         flt (GulpQueryFilter, optional): to further filter query result. Defaults to None.
#         options (GulpQueryOptions): Additional query options (sort, limit, ...).
#     Returns:
#         None
#     """

#     # GulpLogger.get_logger().debug("query_multi_task: %s" % (kwargs))
#     index = kwargs["index"]
#     user_id = kwargs["user_id"]
#     username = kwargs["username"]
#     req_id = kwargs["req_id"]
#     ws_id = kwargs["ws_id"]
#     q = kwargs["q"]
#     flt = kwargs.get("flt", None)
#     options = kwargs.get("options", None)
#     sigma_group_flts = kwargs.get("sigma_group_flts", None)

#     if flt is None:
#         flt = GulpQueryFilter()

#     from gulp.api.ws_api import WsQueueDataType

#     collab = await collab_api.session()
#     executor = rest_api.process_executor()

#     qs: TmpQueryStats = TmpQueryStats()
#     qs.queries_total = len(q)
#     qres_list: list[QueryResult] = []
#     batch_size = GulpConfig.get_instance().multiprocessing_batch_size()
#     status: GulpRequestStatus = GulpRequestStatus.ONGOING
#     # GulpLogger.get_logger().debug("sigma_group_filters=%s" % (sigma_group_flts))

#     # run tasks in batch_size chunks
#     for i in range(0, len(q), batch_size):
#         # build task list for this chunk
#         tasks = []
#         batch = q[i : i + batch_size]
#         for r in batch:
#             sigma_rule_file = None
#             files_path = None
#             # INDEX type do not have name...
#             if r.name is not None:
#                 splitted = r.name.split(",,")
#                 if len(splitted) > 1:
#                     files_path = splitted[0]
#                     sigma_rule_file = splitted[1]
#             tasks.append(
#                 executor.apply(
#                     query_utils.query_by_gulpqueryparam,
#                     (
#                         user_id,
#                         username,
#                         req_id,
#                         ws_id,
#                         index,
#                         r,
#                         flt,
#                         options,
#                         sigma_rule_file,
#                         files_path,
#                         sigma_group_flts,
#                     ),
#                 )
#             )

#         # run a batch of tasks
#         GulpLogger.get_logger().debug("running %d query tasks ..." % (len(tasks)))
#         ql: list[QueryResult] = await asyncio.gather(*tasks, return_exceptions=True)
#         qres_list.extend(ql)

#         # update stats on db
#         for qres in ql:
#             if isinstance(qres, QueryResult):
#                 qs.queries_processed += 1
#                 qs.matches_total += qres.total_hits

#         status, _ = await GulpStats.update(
#             collab,
#             req_id,
#             ws_id,
#             qs=qs,
#             force=True,
#         )
#         if status in [GulpRequestStatus.FAILED, GulpRequestStatus.CANCELED]:
#             GulpLogger.get_logger().error(
#                 "query_multi_task: request failed or canceled, stopping further queries!"
#             )
#             break

#     # end of request, signal the websocket
#     combined_total_hits = 0
#     for r in qres_list:
#         if isinstance(r, QueryResult):
#             combined_total_hits += r.total_hits
#     ws_api.shared_queue_add_data(
#         WsQueueDataType.QUERY_DONE,
#         req_id,
#         {
#             "status": status,
#             # all queries combined total hits
#             "combined_total_hits": combined_total_hits,
#         },
#         username=username,
#         ws_id=ws_id,
#     )

#     if sigma_group_flts is not None:
#         # apply sigma group filters on all results
#         qr = []
#         for r in qres_list:
#             if isinstance(r, QueryResult):
#                 if len(r.events) > 0:
#                     qr.append(r)
#         GulpLogger.get_logger().debug("applying sigma group filters on %d results ..." % (len(qr)))
#         sgr = await query_utils.apply_sigma_group_filters(
#             sigma_group_flts,
#             [x.to_dict() for x in qr],
#         )
#         if sgr is not None and len(sgr) > 0:
#             GulpLogger.get_logger().debug("sigma group filter %s matched!" % (len(qr)))
#             # send sigma group result over websocket
#             ws_api.shared_queue_add_data(
#                 WsQueueDataType.SIGMA_GROUP_RESULT,
#                 req_id,
#                 {
#                     "sigma_group_results": sgr,
#                 },
#                 username=username,
#                 ws_id=ws_id,
#             )


# async def gather_sigma_directories_to_stored_queries(
#     token: str,
#     req_id: str,
#     files_path: str,
#     ws_id: str,
#     pysigma_plugin: str = None,
#     tags_from_directories: bool = True,
# ) -> dict:
#     """
#         convert all sigma files in a directory to stored queries using multiple worker processes.

#         Args:
#             token (str): The authentication token.
#             req_id (str): The request ID.
#             files_path (str): The path to the directory containing the sigma files.
#             pysigma_plugin (str, optional): fallback pysigma plugin `filename with or without .py/.pyc`. Defaults to None (use "logsource.product" from each sigma rule, if present).
#             tags_from_directories (bool, optional): Whether to extract tags from directories. Defaults to True.
#             ws_id (str, optional): The websocket id. Defaults to None.
#         Returns:
#     g        dict: A dictionary containing the created object IDs and, possibly, error strings.

#         Raises:
#             ObjectNotFound: If no sigma files are found in the directory.
#     """
#     from gulp.api import rest_api

#     executor = rest_api.process_executor()

#     # get files
#     files = await muty.file.list_directory_async(
#         files_path, recursive=True, mask="*.yml", files_only=True
#     )
#     if len(files) == 0:
#         raise ObjectNotFound("no sigma files found in directory %s" % (files_path))

#     GulpLogger.get_logger().debug("sigma files in directory %s: %s" % (files_path, files))

#     # parallelize queries through multiple worker processes, each one running asyncio tasks
#     tasks = []
#     GulpLogger.get_logger().debug(
#         "gathering results for %d sigma files to be converted ..." % (len(files))
#     )
#     for f in files:
#         tasks.append(
#             executor.apply(
#                 query_utils.sigma_to_stored_query,
#                 (
#                     token,
#                     req_id,
#                     files_path,
#                     f,
#                     ws_id,
#                     pysigma_plugin,
#                     tags_from_directories,
#                 ),
#             )
#         )

#     # wait and gather the list of results
#     l = {"ids": [], "errors": []}
#     success_ids = l["ids"]
#     errors = l["errors"]
#     results = await asyncio.gather(*tasks, return_exceptions=True)
#     for r in results:
#         if isinstance(r, str):
#             errors.append(r)
#         else:
#             success_ids.append(r)
#     return l
