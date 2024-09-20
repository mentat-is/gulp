import asyncio
import json
import os
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

import gulp.api.elastic.query_utils as query_utils
import gulp.api.elastic_api as elastic_api
import gulp.api.rest.ws as ws_api
import gulp.config as config
import gulp.plugin
import gulp.utils
from gulp.api import collab_api, elastic_api, rest_api
from gulp.api.collab.base import GulpCollabFilter, GulpCollabType, GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats, TmpQueryStats
from gulp.api.elastic.query import QueryResult, SigmaGroupFilter
from gulp.api.elastic.structs import (
    GulpIngestionFilter,
    GulpQueryFilter,
    GulpQueryOptions,
    GulpQueryParameter,
)
from gulp.defs import GulpPluginType, ObjectNotFound
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams
from gulp.utils import logger


def process_worker_init(spawned_processes: Value, lock: Lock, ws_queue: Queue, log_level: int = None, log_file_path: str = None):  # type: ignore
    """
    Initializes the worker process.

    Args:
        spawned_processes (Value): A shared value representing the number of spawned processes.
        lock (Lock): to synchronize startup of worker processes ()
        ws_queue (Queue): The queue (proxy for multiprocessing) for websocket messages.
        config_path (str, optional): The path to the configuration file. Defaults to None.
        log_level (int, optional): The log level. Defaults to None.
        log_file_path (str, optional): The path to the log file. Defaults to None.
    """

    # initialize modules in the worker process, this will also initialize LOGGER
    gulp.utils.init_modules(
        l=None,
        logger_prefix=str(os.getpid()),
        log_level=log_level,
        log_file_path=log_file_path,
        ws_queue=ws_queue,
    )

    # add plugins paths
    ext_plugins_path = config.path_plugins(GulpPluginType.EXTENSION)
    ing_plugins_path = config.path_plugins(GulpPluginType.INGESTION)
    if ing_plugins_path not in sys.path:
        sys.path.append(ing_plugins_path)
    if ext_plugins_path not in sys.path:
        sys.path.append(ext_plugins_path)

    # initialize per-process clients
    asyncio.run(collab_api.collab())
    elastic_api.elastic()

    lock.acquire()
    spawned_processes.value += 1
    lock.release()
    logger().warning(
        "workerprocess initializer DONE, sys.path=%s, logger=%s, logger level=%d, log_file_path=%s, spawned_processes=%d, ws_queue=%s"
        % (
            sys.path,
            logger(),
            logger().level,
            log_file_path,
            spawned_processes.value,
            ws_queue,
        )
    )


async def _print_debug_ingestion_stats(collab: AsyncEngine, req_id: str):
    """
    get the stats for an ingestion request and print the time elapsed
    """
    if __debug__:
        # get stats for this request
        cs = await GulpStats.get(collab, GulpCollabFilter(req_id=[req_id]))
        cs = cs[0]

        # compute some time stats
        elapsed = cs.time_end - cs.time_created
        seconds = elapsed / 1000
        minutes = seconds / 60
        hours = minutes / 60
        started_at = muty.time.unix_millis_to_datetime(cs.time_created).isoformat()

        # we use error just to print the exception in any case, even when most of the debug messages are skipped
        logger().error(
            "(NOT AN ERROR) req_id=%s, started_at=%s, took seconds=%f, (minutes=%f, hours=%f)\n"
            "final stats req_id=%s: processed=%d, failed=%d, skipped=%d, num (UNIQUE) ingest_errors=%d, QueuePool status=%s"
            % (
                cs.req_id,
                started_at,
                seconds,
                minutes,
                hours,
                cs.req_id,
                cs.ev_processed,
                cs.ev_failed,
                cs.ev_skipped,
                len(cs.ingest_errors) if cs.ingest_errors is not None else 0,
                collab.pool.status(),
            )
        )


async def _ingest_file_task_internal(
    index: str,
    req_id: str,
    client_id: int,
    operation_id: int,
    context: str,
    plugin: str,
    src_file: str | list[dict],
    ws_id: str,
    plugin_params: GulpPluginParams = None,
    flt: GulpIngestionFilter = None,
    user_id: int = None,
    **kwargs,
) -> GulpRequestStatus:
    """
    Ingests a single file using the specified plugin.

    NOTE: This runs in an async task in one of the worker process.

    Args:
        index (str): The elasticsearch index name.
        req_id (str): The request ID.
        client_id (int): The client id in the collab database.
        operation_id (int): The operation id.
        context (str): The context.
        plugin (str): The plugin name.
        src_file (str): The source file path or a list of events (for the "raw" plugin).
        ws_id (str): The websocket id
        plugin_params (GulpPluginParams, optional): The ingestion plugin parameters. Defaults to None.
        flt (GulpIngestionFilter, optional): The filter to apply during ingestion. Defaults to None.
        user_id (int, optional): The user id. Defaults to None.
        kwargs: Additional keyword arguments.

    Returns:
        GulpRequestStatus

    Raises:
        Any exceptions raised by the plugin during ingestion.
    """
    collab = await collab_api.collab()
    elastic = elastic_api.elastic()

    mod = None
    if plugin == "raw":
        # src_file is a list of events
        logger().debug(
            "ingesting %d raw events with plugin=%s, collab=%s, elastic=%s, flt(%s)=%s, plugin_params=%s ..."
            % (len(src_file), plugin, collab, elastic, type(flt), flt, plugin_params)
        )
    else:
        logger().debug(
            "ingesting file=%s with plugin=%s, collab=%s, elastic=%s, flt(%s)=%s, plugin_params=%s ..."
            % (src_file, plugin, collab, elastic, type(flt), flt, plugin_params)
        )

    # load plugin
    try:
        mod: PluginBase = gulp.plugin.load_plugin(plugin)
    except Exception as ex:
        # can't load plugin ...
        t = TmpIngestStats(src_file).update(ingest_errors=[ex])
        status, _ = await GulpStats.update(
            collab,
            req_id,
            ws_id,
            fs=t,
            file_done=True,
            new_status=GulpRequestStatus.FAILED,
        )
        return status

    # ingest
    start_time = timeit.default_timer()
    if flt is None:
        flt = GulpIngestionFilter()

    status = await mod.ingest(
        index,
        req_id,
        client_id,
        operation_id,
        context,
        src_file,
        ws_id,
        plugin_params=plugin_params,
        flt=flt,
        user_id=user_id,
    )
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    logger().debug(
        "execution time for ingesting file %s: %f sec." % (src_file, execution_time)
    )

    # unload plugin
    gulp.plugin.unload_plugin(mod)
    return status


async def ingest_directory_task(
    **kwargs,
) -> None:
    """
    Ingests all files in a directory using multiple processes and a task per file.

    NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

    Args:
        **kwargs: Additional keyword arguments.

    Keyword Args:
        index (str): The elasticsearch index name.
        req_id (str): The request ID.
        directory (str): The directory path.
        plugin (str): The plugin name.
        client_id(int): the client
        operation_id (int): The operation id.
        context (str): The context.
        ws_id (str): The websocket id.
        plugin_params (GulpPluginParams, optional): The plugin parameters.
        flt (GulpIngestionFilter, optional): The filter.
        q (list[GulpQueryParameter], optional): The list of GulpQueryParameter objects. Defaults to None.
        q_options (GulpQueryOptions, optional): The query options. Defaults to None.
        user_id (int, optional): The user id. Defaults to None.
        sigma_group_flts (list[SigmaGroupFilter], optional): to filter results on groups of sigma rule ids. Defaults to None.
    """
    executor = rest_api.process_executor()
    collab = await collab_api.collab()

    index = kwargs["index"]
    req_id = kwargs["req_id"]
    directory = kwargs["directory"]
    plugin = kwargs["plugin"]
    client_id = kwargs["client_id"]
    operation_id = kwargs["operation_id"]
    ws_id = kwargs["ws_id"]
    plugin_params = kwargs.get("plugin_params", None)
    context = kwargs["context"]
    flt = kwargs.get("flt", None)
    user_id = kwargs.get("user_id", None)

    # ingestion started for this request
    files = await muty.file.list_directory_async(directory)
    logger().info("ingesting directory %s with files=%s ..." % (directory, files))
    if len(files) == 0:
        ex = ObjectNotFound("directory %s empty or not found!" % (directory))
        await GulpStats.create(
            collab,
            GulpCollabType.STATS_INGESTION,
            req_id,
            ws_id,
            operation_id,
            client_id,
            context,
        )
        await GulpStats.update(
            collab,
            req_id,
            ws_id,
            fs=TmpIngestStats(directory).update(ingest_errors=[ex]),
            file_done=True,
        )

        return

    # create the request stats
    await GulpStats.create(
        collab,
        GulpCollabType.STATS_INGESTION,
        req_id,
        ws_id,
        operation_id,
        client_id,
        context,
        total_files=len(files),
    )

    tasks = []

    # if not sync, parallelize ingestion through multiple worker processes, each one running asyncio tasks
    for src_file in files:
        tasks.append(
            executor.apply(
                _ingest_file_task_internal,
                (
                    index,
                    req_id,
                    client_id,
                    operation_id,
                    context,
                    plugin,
                    src_file,
                    ws_id,
                    plugin_params,
                    flt,
                    user_id,
                ),
            )
        )
        if len(tasks) == config.multiprocessing_batch_size():
            # run a batch
            await asyncio.gather(*tasks, return_exceptions=True)
            tasks = []

    if len(tasks) > 0:
        # run remaining tasks
        await asyncio.gather(*tasks, return_exceptions=True)

    await _print_debug_ingestion_stats(collab, req_id)


async def ingest_single_file_or_events_task(
    **kwargs,
) -> None:
    """
    ingests a single uploaded file or a chunk of events using a worker process

    NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed (one) tasks.

    Args:
        **kwargs: Additional keyword arguments.

    Keyword Args:
        index (str): The elasticsearch index name.
        req_id (str): The request ID.
        f (str): path to the uploaded file to ingest (will be deleted when done).
        plugin (str): The plugin name.
        client_id (int): The client.
        operation_id (int): The operation.
        context (str): The context.
        plugin_params (GulpPluginParams): The plugin parameters.
        flt (GulpIngestionFilter, optional): The filter.
        user_id: (int): The user id, optional.
        ws_id (str): The websocket id
    """
    executor = rest_api.process_executor()
    collab = await collab_api.collab()

    index = kwargs["index"]
    req_id = kwargs["req_id"]
    f = kwargs["f"]
    plugin = kwargs["plugin"]
    client_id = kwargs["client"]
    operation_id = kwargs["operation"]
    context = kwargs["context"]
    ws_id = kwargs["ws_id"]
    flt = kwargs.get("flt", None)
    plugin_params = kwargs.get("plugin_params", None)
    user_id = kwargs.get("user_id", None)

    # create the request stats
    await GulpStats.create(
        collab,
        GulpCollabType.STATS_INGESTION,
        req_id,
        ws_id,
        operation_id,
        client_id,
        context,
        total_files=1,
    )

    # process single file or chunk of events
    tasks = []
    tasks.append(
        executor.apply(
            _ingest_file_task_internal,
            (
                index,
                req_id,
                client_id,
                operation_id,
                context,
                plugin,
                f,
                ws_id,
                plugin_params,
                flt,
                user_id,
            ),
        )
    )
    await asyncio.gather(*tasks, return_exceptions=True)

    if plugin != "raw":
        # delete uploaded file folder
        await muty.file.delete_file_or_dir_async(os.path.dirname(f))
    await _print_debug_ingestion_stats(collab, req_id)


async def ingest_zip_task(
    **kwargs,
) -> None:
    """
    processes an uploaded zip file (i.e. from slurp) using multiple processes and a task per file.

    NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

    Args:
        **kwargs: Additional keyword arguments.

    Keyword Args:
        index (str): The elasticsearch index name.
        req_id (str): The request ID.
        f (str): The uploaded zip file path
        client_id (int): The client.
        operation_id (int): The operation.
        context (str): The context.
        flt (GulpIngestionFilter, optional): The filter.
        user_id: (int): The user id, optional.
        ws_id (str): The websocket id
    Returns:
        None
    """
    executor = rest_api.process_executor()
    collab = await collab_api.collab()

    index = kwargs["index"]
    req_id = kwargs["req_id"]
    f = kwargs["f"]
    client_id = kwargs["client_id"]
    operation_id = kwargs["operation_id"]
    context = kwargs["context"]
    ws_id = kwargs["ws_id"]
    flt = kwargs.get("flt", None)
    user_id = kwargs.get("user_id", None)
    files_path = None

    logger().debug("ingesting zip file %s ..." % (f))

    try:
        # decompress
        files_path = await muty.file.unzip(f)
        logger().debug("zipfile unzipped to %s" % (files_path))

        # read metadata file
        metadata_path = muty.file.safe_path_join(files_path, "metadata.json")
        metadata = json5.loads(await muty.file.read_file_async(metadata_path))
        logger().debug(
            "metadata.json content:\n %s" % (json5.dumps(metadata, indent=2))
        )
    except Exception as ex:
        logger().exception(ex)
        await GulpStats.create(
            collab,
            GulpCollabType.STATS_INGESTION,
            req_id,
            ws_id,
            operation_id,
            client_id,
            context,
        )
        await GulpStats.update(
            collab,
            req_id,
            ws_id,
            fs=TmpIngestStats(f).update(ingest_errors=[ex]),
            new_status=GulpRequestStatus.FAILED,
            file_done=True,
            force=True,
        )
        await muty.file.delete_file_or_dir_async(files_path)
        return

    finally:
        # delete uploaded file folder
        await muty.file.delete_file_or_dir_async(os.path.dirname(f))

    # get number of files
    num_files = 0
    for plugin in metadata.keys():
        files = metadata[plugin]["files"]
        for file in files:
            num_files += 1

    tasks = []
    await GulpStats.create(
        collab,
        GulpCollabType.STATS_INGESTION,
        req_id,
        ws_id,
        operation_id,
        client_id,
        context,
        total_files=num_files,
    )

    # walk each key(=plugin)
    for plugin in metadata.keys():
        files = metadata[plugin]["files"]
        plugin_params = metadata[plugin].get("plugin_params", None)
        for file in files:
            ff = muty.file.safe_path_join(files_path, file)
            tasks.append(
                executor.apply(
                    _ingest_file_task_internal,
                    (
                        index,
                        req_id,
                        client_id,
                        operation_id,
                        context,
                        plugin,
                        ff,
                        ws_id,
                        plugin_params,
                        flt,
                        user_id,
                    ),
                )
            )
            if len(tasks) == config.multiprocessing_batch_size():
                # run a batch
                await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

    if len(tasks) > 0:
        # run remaining tasks
        await asyncio.gather(*tasks, return_exceptions=True)

    # delete unzipped files
    await muty.file.delete_file_or_dir_async(files_path)
    await _print_debug_ingestion_stats(collab, req_id)


async def _rebase_internal(
    index: str,
    dest_index: str,
    offset: int,
    ws_id: str,
    req_id: str,
    flt: GulpIngestionFilter = None,
    max_total_fields: int = 10000,
    refresh_interval_msec: int = 5000,
    force_date_detection: bool = False,
    event_original_text_analyzer: str = "standard",
    **kwargs,
) -> None:
    """
    rebase index to dest_index using a given offset

    Args:
        index (str): The elasticsearch index name.
        dest_index (str): The destination index name.
        offset (int): The rebase offset, in milliseconds.
        ws_id (str): The websocket id
        req_id (str): The request ID.
        flt (GulpIngestionFilter, optional): The filter.
        max_total_fields (int, optional): The maximum number of fields in the index. Defaults to 10000.
        refresh_interval_msec (int, optional): The refresh interval in milliseconds. Defaults to 5000.
        force_date_detection (bool, optional): Whether to force date detection. Defaults to False.
        event_original_text_analyzer (str, optional): The analyzer for the original text. Defaults to 'standard'.
        kwargs: Additional keyword arguments.
    Returns:
        None
    """
    elastic = elastic_api.elastic()

    ds = None
    res = {}
    try:
        # create another datastream (if it exists, it will be deleted)
        ds = await elastic_api.datastream_create(
            elastic,
            dest_index,
            max_total_fields=max_total_fields,
            refresh_interval_msec=refresh_interval_msec,
            force_date_detection=force_date_detection,
            event_original_text_analyzer=event_original_text_analyzer,
        )

        # rebase
        res = await elastic_api.rebase(elastic, index, dest_index, offset, flt=flt)
    except Exception as ex:
        # can't rebase, delete the datastream
        logger().exception(ex)
        ws_api.shared_queue_add_data(
            ws_api.WsQueueDataType.REBASE_DONE,
            req_id,
            data={
                "status": GulpRequestStatus.FAILED,
                "index": index,
                "test_index": dest_index,
                "error": muty.log.exception_to_string(ex),
            },
            ws_id=ws_id,
        )

        if ds is not None:
            await elastic_api.datastream_delete(elastic, dest_index)
        return

    # done
    logger().debug("rebase result: %s" % (json.dumps(res, indent=2)))
    ws_api.shared_queue_add_data(
        ws_api.WsQueueDataType.REBASE_DONE,
        req_id,
        data={
            "status": GulpRequestStatus.DONE,
            "index": index,
            "dest_index": dest_index,
            "result": res,
        },
        ws_id=ws_id,
    )


async def rebase_task(
    **kwargs,
) -> None:
    """
    perform index rebase

    NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

    Args:
        collab (AsyncEngine): The SQL engine from SQLAlchemy.
        elastic (AsyncElasticsearch): The Elasticsearch client.
        executor (aiomultiprocess.Pool): The executor for running worker processes.
        **kwargs: Additional keyword arguments.

    Keyword Args:
        index (str): The elasticsearch index name.
        dest_index (str): The destination index name.
        offset (int): The rebase offset, in milliseconds.
        flt (GulpIngestionFilter, optional): The filter.
    Returns:
        None
    """
    executor = rest_api.process_executor()

    index = kwargs["index"]
    dest_index = kwargs["dest_index"]
    offset = kwargs["offset"]
    ws_id = kwargs["ws_id"]
    flt = kwargs.get("flt", None)
    req_id = kwargs["req_id"]
    max_total_fields = kwargs.get("max_total_fields", 10000)
    refresh_interval_msec = kwargs.get("refresh_interval_msec", 5000)
    force_date_detection = kwargs.get("force_date_detection", False)
    event_original_text_analyzer = kwargs.get(
        "event_original_text_analyzer", "standard"
    )

    # use a worker process to perform rebase
    tasks = []

    # TODO: create stats
    tasks.append(
        executor.apply(
            _rebase_internal,
            (
                index,
                dest_index,
                offset,
                ws_id,
                req_id,
                flt,
                max_total_fields,
                refresh_interval_msec,
                force_date_detection,
                event_original_text_analyzer,
            ),
        )
    )
    await asyncio.gather(*tasks, return_exceptions=True)


async def ingest_zip_simple_task(
    **kwargs,
) -> None:
    """
    processes an uploaded zip file with a single plugin, using multiple processes and a task per file.

    NOTE: this task runs in the MAIN process, not in a worker process. It is used to parallelize ingestion and will spawn the needed tasks.

    Args:
        **kwargs: Additional keyword arguments.

    Keyword Args:
        index (str): The elasticsearch index name.
        req_id (str): The request ID.
        f (str): The uploaded zip file path
        plugin (str): The plugin name.
        client_id (int): The client.
        operation_id (int): The operation.
        context (str): The context.
        ws_id (str): The websocket id
        mask(str, optional): An optional filemask (e.g. "*.evtx" to only ingest .evtx files), optional. either, '*' is used and invalid files will be processed and skipped as errors.
        plugin_params (GulpPluginParams, optional): The plugin parameters.
        flt (GulpIngestionFilter, optional): The filter.
        user_id: (int): The user id, optional.
    """
    executor = rest_api.process_executor()

    index = kwargs["index"]
    req_id = kwargs["req_id"]
    f = kwargs["f"]
    client_id = kwargs["client_id"]
    operation_id = kwargs["operation_id"]
    context = kwargs["context"]
    ws_id = kwargs["ws_id"]
    mask = kwargs.get("mask", None)
    flt = kwargs.get("flt", None)
    params = kwargs.get("params", None)
    plugin_params = kwargs.get("plugin_params", None)
    plugin = kwargs.get("plugin", None)
    user_id = kwargs.get("user_id", None)
    files_path = None

    logger().debug("ingesting (simple) zip file %s, params=%s ..." % (f, params))

    try:
        # decompress
        files_path = await muty.file.unzip(f)
        logger().debug("zipfile unzipped to %s" % (files_path))
    except Exception as ex:
        logger().exception(ex)
        await GulpStats.create(
            await collab_api.collab(),
            GulpCollabType.STATS_INGESTION,
            req_id,
            ws_id,
            operation_id,
            client_id,
            context,
        )
        await GulpStats.update(
            await collab_api.collab(),
            req_id,
            ws_id,
            fs=TmpIngestStats(f).update(ingest_errors=[ex]),
            file_done=True,
            force=True,
        )
        await muty.file.delete_file_or_dir_async(files_path)
        return

    finally:
        # delete uploaded file folder
        await muty.file.delete_file_or_dir_async(os.path.dirname(f))

    # get files
    files = await muty.file.list_directory_async(
        files_path, mask=mask, recursive=True, files_only=True, case_sensitive=False
    )
    logger().debug("found files: %s" % (files))
    num_files = len(files)

    tasks = []
    await GulpStats.create(
        await collab_api.collab(),
        GulpCollabType.STATS_INGESTION,
        req_id,
        ws_id,
        operation_id,
        client_id,
        context,
        total_files=num_files,
    )

    for ff in files:
        if plugin is None or len(plugin) == 0:
            # strip ff (the scanned full file path) from the base path (files_path)
            fff = ff[len(files_path) + 1 :]

            # and get the plugin name from the first parent directory
            # i.e. if fff is plugin_name/bla/blu/file.evtx, then plugin_name is the plugin name
            plugin = fff.split("/")[0]
            logger().debug("plugin is None, setting plugin to: %s" % (plugin))

        tasks.append(
            executor.apply(
                _ingest_file_task_internal,
                (
                    None,
                    None,
                    index,
                    req_id,
                    client_id,
                    operation_id,
                    context,
                    plugin,
                    ff,
                    ws_id,
                    plugin_params,
                    flt,
                    user_id,
                ),
            )
        )
        if len(tasks) == config.multiprocessing_batch_size():
            # run a batch
            await asyncio.gather(*tasks, return_exceptions=True)
            tasks = []

    if len(tasks) > 0:
        # run remaining tasks
        await asyncio.gather(*tasks, return_exceptions=True)

    # delete unzipped files
    await muty.file.delete_file_or_dir_async(files_path)
    await _print_debug_ingestion_stats(await collab_api.collab(), req_id)


async def _query_plugin_internal(
    operation_id: int,
    client_id: int,
    user_id: int,
    username: str,
    plugin: str,
    plugin_params: GulpPluginParams,
    ws_id: str,
    req_id: str,
    flt: GulpQueryFilter,
    options: GulpQueryOptions,
) -> tuple[int, GulpRequestStatus]:

    collab = await collab_api.collab()
    qs = TmpQueryStats()
    qs.queries_total = 1

    # load plugin
    mod = None
    try:
        mod: PluginBase = gulp.plugin.load_plugin(
            plugin, plugin_type=GulpPluginType.QUERY
        )
    except Exception as ex:
        # can't load plugin ...
        logger().exception(ex)
        await GulpStats.update(
            collab,
            req_id,
            ws_id,
            qs=qs,
            force=True,
            new_status=GulpRequestStatus.FAILED,
            errors=[str(ex)],
        )
        return 0, GulpRequestStatus.FAILED

    # query
    try:
        start_time = timeit.default_timer()
        num_results, status = await mod.query(
            operation_id,
            client_id,
            user_id,
            username,
            ws_id,
            req_id,
            plugin_params,
            flt,
            options,
        )

        end_time = timeit.default_timer()
        execution_time = end_time - start_time
        logger().debug(
            "execution time for querying plugin %s: %f sec." % (plugin, execution_time)
        )
    except Exception as ex:
        # can't query external source ...
        logger().exception(ex)
        await GulpStats.update(
            collab,
            req_id,
            ws_id,
            qs=qs,
            force=True,
            new_status=GulpRequestStatus.FAILED,
            errors=[str(ex)],
        )
        return 0, GulpRequestStatus.FAILED

    # unload plugin
    gulp.plugin.unload_plugin(mod)
    return num_results, status


async def query_plugin_task(**kwargs):
    """
    Asynchronously handles a query plugin task by offloading the work to a worker process and
    then processing the results.

    Keyword Arguments:
    req_id (str): The request ID.
    ws_id (str): The workspace ID.
    plugin (str): The plugin to be queried.
    user_id (str): The user ID.
    username (str): The username of the user.
    plugin_params (GulpPluginParams): Parameters for the plugin (external source parameters in "extra" dict).
    operation_id (str): The operation ID.
    client_id (str): The client ID.
    flt (str): Filter criteria.
    options (GulpQueryOptions, optional): Additional options for the query. Defaults to None.
    Returns:
    None
    """

    req_id = kwargs["req_id"]
    ws_id = kwargs["ws_id"]
    plugin = kwargs["plugin"]
    user_id = kwargs["user_id"]
    username = kwargs["username"]
    plugin_params = kwargs["plugin_params"]
    operation_id = kwargs["operation_id"]
    client_id = kwargs["client_id"]
    flt = kwargs["flt"]
    options = kwargs.get("options", None)

    # offload to a worker process
    executor = rest_api.process_executor()
    coro = executor.apply(
        _query_plugin_internal,
        (
            operation_id,
            client_id,
            user_id,
            username,
            plugin,
            plugin_params,
            ws_id,
            req_id,
            flt,
            options,
        ),
    )

    num_results, status = await coro

    # done   
    qs = TmpQueryStats() 
    qs.matches_total = num_results
    qs.queries_total = 1
    qs.queries_processed = 1
    await GulpStats.update(
        await collab_api.collab(),
        req_id,
        ws_id,
        qs=qs,
        force=True,
        new_status=status,
    )
    ws_api.shared_queue_add_data(
        ws_api.WsQueueDataType.QUERY_DONE,
        req_id,
        {
            "status": status,
            # all queries combined total hits
            "combined_total_hits": num_results,
        },
        ws_id=ws_id,
    )


async def query_multi_task(**kwargs):
    """
    Executes one or more queries, using multiple worker processes and a task per query.

    Args:
        **kwargs: Additional keyword arguments.

    Keyword Args:
        index (str): The elasticsearch index name.
        user_id (int): The user id who performs the query.
        username (str): The user name who performs the query.
        req_id (str): The request ID.
        q (list[GulpQueryParameter]): The list of GulpQueryParameter objects.
        ws_id (str): The websocket id.
        flt (GulpQueryFilter, optional): to further filter query result. Defaults to None.
        options (GulpQueryOptions): Additional query options (sort, limit, ...).
    Returns:
        None
    """

    # logger().debug("query_multi_task: %s" % (kwargs))
    index = kwargs["index"]
    user_id = kwargs["user_id"]
    username = kwargs["username"]
    req_id = kwargs["req_id"]
    ws_id = kwargs["ws_id"]
    q = kwargs["q"]
    flt = kwargs.get("flt", None)
    options = kwargs.get("options", None)
    sigma_group_flts = kwargs.get("sigma_group_flts", None)

    if flt is None:
        flt = GulpQueryFilter()

    from gulp.api.rest.ws import WsQueueDataType

    collab = await collab_api.collab()
    executor = rest_api.process_executor()

    qs: TmpQueryStats = TmpQueryStats()
    qs.queries_total = len(q)
    qres_list: list[QueryResult] = []
    batch_size = config.multiprocessing_batch_size()
    status: GulpRequestStatus = GulpRequestStatus.ONGOING
    # logger().debug("sigma_group_filters=%s" % (sigma_group_flts))

    # run tasks in batch_size chunks
    for i in range(0, len(q), batch_size):
        # build task list for this chunk
        tasks = []
        batch = q[i : i + batch_size]
        for r in batch:
            sigma_rule_file = None
            files_path = None
            # INDEX type do not have name...
            if r.name is not None:
                splitted = r.name.split(",,")
                if len(splitted) > 1:
                    files_path = splitted[0]
                    sigma_rule_file = splitted[1]
            tasks.append(
                executor.apply(
                    query_utils.query_by_gulpqueryparam,
                    (
                        user_id,
                        username,
                        req_id,
                        ws_id,
                        index,
                        r,
                        flt,
                        options,
                        sigma_rule_file,
                        files_path,
                        sigma_group_flts,
                    ),
                )
            )

        # run a batch of tasks
        logger().debug("running %d query tasks ..." % (len(tasks)))
        ql: list[QueryResult] = await asyncio.gather(*tasks, return_exceptions=True)
        qres_list.extend(ql)

        # update stats on db
        for qres in ql:
            if isinstance(qres, QueryResult):
                qs.queries_processed += 1
                qs.matches_total += qres.total_hits

        status, _ = await GulpStats.update(
            collab,
            req_id,
            ws_id,
            qs=qs,
            force=True,
        )
        if status in [GulpRequestStatus.FAILED, GulpRequestStatus.CANCELED]:
            logger().error(
                "query_multi_task: request failed or canceled, stopping further queries!"
            )
            break

    # end of request, signal the websocket
    combined_total_hits = 0
    for r in qres_list:
        if isinstance(r, QueryResult):
            combined_total_hits += r.total_hits
    ws_api.shared_queue_add_data(
        WsQueueDataType.QUERY_DONE,
        req_id,
        {
            "status": status,
            # all queries combined total hits
            "combined_total_hits": combined_total_hits,
        },
        username=username,
        ws_id=ws_id,
    )

    if sigma_group_flts is not None:
        # apply sigma group filters on all results
        qr = []
        for r in qres_list:
            if isinstance(r, QueryResult):
                if len(r.events) > 0:
                    qr.append(r)
        logger().debug("applying sigma group filters on %d results ..." % (len(qr)))
        sgr = await query_utils.apply_sigma_group_filters(
            sigma_group_flts,
            [x.to_dict() for x in qr],
        )
        if sgr is not None and len(sgr) > 0:
            logger().debug("sigma group filter %s matched!" % (len(qr)))
            # send sigma group result over websocket
            ws_api.shared_queue_add_data(
                WsQueueDataType.SIGMA_GROUP_RESULT,
                req_id,
                {
                    "sigma_group_results": sgr,
                },
                username=username,
                ws_id=ws_id,
            )


async def gather_sigma_directories_to_stored_queries(
    token: str,
    req_id: str,
    files_path: str,
    ws_id: str,
    pysigma_plugin: str = None,
    tags_from_directories: bool = True,
) -> dict:
    """
        convert all sigma files in a directory to stored queries using multiple worker processes.

        Args:
            token (str): The authentication token.
            req_id (str): The request ID.
            files_path (str): The path to the directory containing the sigma files.
            pysigma_plugin (str, optional): fallback pysigma plugin name. Defaults to None (use "logsource.product" from each sigma rule, if present).
            tags_from_directories (bool, optional): Whether to extract tags from directories. Defaults to True.
            ws_id (str, optional): The websocket id. Defaults to None.
        Returns:
    g        dict: A dictionary containing the created object IDs and, possibly, error strings.

        Raises:
            ObjectNotFound: If no sigma files are found in the directory.
    """
    from gulp.api import rest_api

    executor = rest_api.process_executor()

    # get files
    files = await muty.file.list_directory_async(
        files_path, recursive=True, mask="*.yml", files_only=True
    )
    if len(files) == 0:
        raise ObjectNotFound("no sigma files found in directory %s" % (files_path))

    logger().debug("sigma files in directory %s: %s" % (files_path, files))

    # parallelize queries through multiple worker processes, each one running asyncio tasks
    tasks = []
    logger().debug(
        "gathering results for %d sigma files to be converted ..." % (len(files))
    )
    for f in files:
        tasks.append(
            executor.apply(
                query_utils.sigma_to_stored_query,
                (
                    token,
                    req_id,
                    files_path,
                    f,
                    ws_id,
                    pysigma_plugin,
                    tags_from_directories,
                ),
            )
        )

    # wait and gather the list of results
    l = {"ids": [], "errors": []}
    success_ids = l["ids"]
    errors = l["errors"]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, str):
            errors.append(r)
        else:
            success_ids.append(r)
    return l
