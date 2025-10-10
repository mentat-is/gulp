"""
Query API endpoints for Gulp providing various query capabilities.

This module contains FastAPI router endpoints for different types of queries:
- Raw OpenSearch DSL queries
- Simplified Gulp queries with filters
- External data source queries
- Sigma rule queries (single and batch via ZIP)
- Single document queries
- Field mapping and aggregate operations

Each endpoint handles authentication, authorization, and supports both direct responses
and asynchronous processing with results streamed to websockets.

"""

# pylint: disable=too-many-lines

import asyncio
from copy import deepcopy
import json
import os
import tempfile
from typing import Annotated, Any, Optional

import aiofiles
import muty.file
import muty.log
import muty.pydantic
import muty.string
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    Query,
)
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import FileResponse, JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
import orjson

from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpQueryStats, GulpRequestStats, RequestStatsType
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user import GulpUser, GulpUserDataQueryHistoryEntry
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.structs import GulpQueryHelpers
from gulp.api.opensearch.structs import GulpDocument, GulpQuery, GulpQueryParameters
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.ingest import GulpIngestionStats
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import (
    WSDATA_QUERY_DONE,
    WSDATA_QUERY_GROUP_DONE,
    WSDATA_QUERY_GROUP_MATCH,
    GulpQueryDonePacket,
    GulpQueryGroupMatchPacket,
    GulpWsSharedQueue,
    WsQueueFullException,
)
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters
from sqlalchemy.ext.asyncio import AsyncSession

router: APIRouter = APIRouter()

EXAMPLE_SIGMA_RULE = """title: Match All Events
id: 1a070ea4-87f4-467c-b1a9-f556c56b2449
status: test
description: Matches all events in the data source
logsource:
    category: '*'
    product: '*'
detection:
    selection:
        '*': '*'
    condition: selection
falsepositives:
    - This rule matches everything
level: informational
"""

EXAMPLE_QUERY_RAW = {
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "query": "(gulp.operation_id: test_operation AND gulp.context_id: dbdcd5d70efd3e4242cedd1e4a0c9b2d186a5a8f AND gulp.source_id: 7344ed16e93ee2dcb2a1e019c01596e72249d4c3 AND gulp.timestamp: [1727734017000000000 TO 1730414835000000000])"
                    }
                },
                {
                    "wildcard": {
                        "gulp.unmapped.Guid": {
                            "value": "*8-4994-a5bA*",
                            "case_insensitive": True,
                        }
                    }
                },
            ]
        }
    }
}


async def _query_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    q: Any,
    q_options: GulpQueryParameters,
    plugin: str,
    plugin_params: GulpPluginParameters,
    flt: GulpQueryFilter,
    sigma_yml: str = None,
) -> tuple[int, Exception, str]:
    """
    runs in a worker process and perform a query, streaming results to the `ws_id` websocket

    Returns:
        int: number of hits
        Exception: if any
        query_name: str
    """
    hits: int = 0
    mod: GulpPluginBase = None
    q_name: str = q_options.name if q_options else None

    # ensure no preview mode is active here
    q_options.preview_mode = False

    try:
        if plugin:
            # external query, load plugin (it is guaranteed to be the same for all queries)
            mod = await GulpPluginBase.load(plugin)

        async with GulpCollab.get_instance().session() as sess:
            # MutyLogger.get_instance().debug("mod=%s, running query %s " % (mod, gq))
            # wait based on the size of query
            asyncio.sleep(min(0.1 + len(repr(q)) / 10000.0, 2.0))
            if not mod:
                # local query, gq.q is a dict
                _, hits, q_name = await GulpQueryHelpers.query_raw(
                    sess=sess,
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    index=index,
                    q=q,
                    q_options=q_options,
                    flt=flt,
                    source_q=sigma_yml,  # this is used for notes text
                )
            else:
                # external query
                _, hits, q_name = await mod.query_external(
                    sess=sess,
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    q=q,
                    index=index,
                    plugin_params=plugin_params,
                    q_options=q_options,
                )

                # for external queries, we also perform ingest, so broadcast ingest internal event
                await mod.broadcast_ingest_internal_event()

    except Exception as ex:
        MutyLogger.get_instance().exception("***q_name=%s***:%s" % (q_name, ex))
        return 0, ex, q_name

    finally:
        if mod:
            await mod.unload()
    return hits, None, q_name


async def _process_batch_results(
    batch_results: list[tuple[int, Exception, str]],
    user_id: str,
    req_id: str,
    ws_id: str,
) -> tuple[int, int, list[str], list[str]]:
    """
    process batch results and send query_done packets for each result.

    Args:
        batch_results (list[tuple[int, Exception, str]]): results from batch processing
        user_id (str): the user id
        req_id (str): request id
        ws_id (str): websocket id

    Returns:
        tuple[int, int, list[str], list[str]]: matched queries count, total document matches,
                                              query names that matched, errors encountered
    """
    query_matched: int = 0
    total_doc_matches: int = 0
    query_names: list[str] = []
    errors: list[str] = []

    # process each result in the batch
    for r in batch_results:
        # res is a tuple (hits, exception, query_name)
        hits: int = 0
        ex: Exception = None
        q_name: str = None
        err: str = None
        hits, ex, q_name = r

        MutyLogger.get_instance().debug(
            "query %s matched %d hits, ex=%s" % (q_name, hits, ex)
        )

        # track stats
        if hits > 0:
            # we have a match
            query_matched += 1
            query_names.append(q_name)
            total_doc_matches += hits

        if ex:
            # we have an error
            # err = muty.log.exception_to_string(ex)  # , with_full_traceback=True)
            err = "%s: %s" % (q_name, muty.log.exception_to_string(ex))
            if err not in errors:
                # just keep one error per kind
                errors.append(err)

        # send a query_done on the ws for this query, with the status
        p = GulpQueryDonePacket(
            status=GulpRequestStatus.DONE if not ex else GulpRequestStatus.FAILED,
            errors=[err] if err else None,
            total_hits=hits,
            name=q_name,
        )
        wsq = GulpWsSharedQueue.get_instance()
        try:
            await wsq.put(
                t=WSDATA_QUERY_DONE,
                ws_id=ws_id,
                user_id=user_id,
                req_id=req_id,
                d=p.model_dump(exclude_none=True),
            )
        except WsQueueFullException as ex:
            MutyLogger.get_instance().exception(ex)

    return query_matched, total_doc_matches, query_names, errors


async def _handle_query_group_match(
    operation_id: str,
    user_id: str,
    req_id: str,
    ws_id: str,
    q_options: GulpQueryParameters,
    query_names: list[str],
    total_doc_matches: int,
) -> None:
    """
    handle query group matching - update note tags with group name and signal websocket.

    Args:
        operation_id (str): operation id
        user_id (str): user id
        req_id (str): request id
        ws_id (str): websocket id
        q_options (GulpQueryParameters): query options
        query_names (list[str]): list of query names that matched
        total_doc_matches (int): total number of document matches across all queries
    """
    # all queries in the group matched, update note tags with group name
    MutyLogger.get_instance().info(
        "query group '%s' matched, updating notes!" % (q_options.group)
    )

    if q_options.note_parameters.create_notes:
        async with GulpCollab.get_instance().session() as sess:
            # look for tags = query name and update them with the group name
            await GulpNote.bulk_update_tags(
                sess,
                query_names,
                [q_options.group],
                operation_id=operation_id,
                user_id=user_id,
            )

    # and signal websocket
    p = GulpQueryGroupMatchPacket(name=q_options.group, total_hits=total_doc_matches)
    wsq = GulpWsSharedQueue.get_instance()
    await wsq.put(
        t=WSDATA_QUERY_GROUP_MATCH,
        ws_id=ws_id,
        user_id=user_id,
        req_id=req_id,
        d=p.model_dump(exclude_none=True),
    )


# async def _process_query_batch(
#     batch: list[GulpQuery],
#     user_id: str,
#     req_id: str,
#     ws_id: str,
#     operation_id: str,
#     index: str,
#     q_options: GulpQueryParameters,
#     plugin: str,
#     plugin_params: GulpPluginParameters,
#     flt: GulpQueryFilter,
#     current_batch: int,
#     num_batches: int,
# ) -> tuple[list[tuple[int, Exception, str]], int, int, list[str], list[str]]:
#     """
#     process a single batch of queries, each in a task in one of the worker processes.

#     Args:
#         batch (list[GulpQuery]): batch of queries to process
#         user_id (str): user id
#         req_id (str): request id
#         ws_id (str): websocket id
#         operation_id (str): operation id
#         index (str): index to query
#         q_options (GulpQueryParameters): query options
#         plugin (str): plugin to use
#         plugin_params (GulpPluginParameters): plugin parameters
#         flt (GulpQueryFilter): query filter
#         current_batch (int): current batch number
#         num_batches (int): total number of batches

#     Returns:
#         tuple[list[tuple[int, Exception, str]], int, int, list[str], list[str]]:
#             batch results, matched queries count, total document matches, query names that matched, errors
#     """
#     tuples_arr: list[dict] = []
#     # print("************************************************************")
#     # print(batch)
#     # print("************************************************************")
#     # create a task for each query in the batch
#     for gq in batch:
#         q_opt = deepcopy(q_options)

#         # set name, i.e. for sigma rules we want the sigma rule name to be used
#         q_opt.name = gq.name

#         # note name set to query name
#         q_opt.note_parameters.note_name = gq.name

#         if gq.name not in q_opt.note_parameters.note_tags:
#             # query name in note tags (this will allow to identify the results in the end)
#             q_opt.note_parameters.note_tags.append(gq.name)

#         # add note tags
#         for t in gq.tags:
#             if t not in q_opt.note_parameters.note_tags:
#                 q_opt.note_parameters.note_tags.append(t)

#         # add task
#         d: tuple = (
#             user_id,
#             req_id,
#             ws_id,
#             operation_id,
#             index,
#             gq.q,
#             q_opt,
#             plugin,
#             plugin_params,
#             flt,
#             gq.sigma_yml,
#         )
#         tuples_arr.append(d)

#     # run the queries and wait to complete
#     MutyLogger.get_instance().debug(
#         "waiting for queries batch %d/%d, size of batch=%d"
#         % (current_batch, num_batches, len(tuples_arr))
#     )
#     batch_results: list[tuple[int, Exception, str]] = []
#     pool = GulpProcess.get_instance().process_pool
#     async for res in pool.starmap(_query_internal, tuples_arr):
#         # res is a tuple (hits, exception, query_name)
#         batch_results.append(res)

#     # process batch results and send query_done packets
#     query_matched, total_matches, names, errors = await _process_batch_results(
#         batch_results, user_id, req_id, ws_id
#     )

#     return batch_results, query_matched, total_matches, names, errors


async def _worker_coro(kwds: dict) -> None:
    """
    runs in background and processes queries in batches (each batch is run in tasks in worker processes, in parallel).

    1. processes queries in batches to limit resource usage
    2. sends query_done packets immediately after each result
    3. if all queries in a group match, updates note tags and signals websocket

    Args:
        kwds (dict): dictionary containing all parameters for query processing
    """
    # extract parameters from kwds
    queries: list[GulpQuery] = kwds["queries"]
    operation_id: str = kwds["operation_id"]
    q_options: GulpQueryParameters = kwds["q_options"]
    user_id: str = kwds["user_id"]
    req_id: str = kwds["req_id"]
    ws_id: str = kwds["ws_id"]
    index: str = kwds["index"]
    flt: GulpQueryFilter = kwds["flt"]
    plugin: str = kwds.get("plugin")
    plugin_params: GulpPluginParameters = kwds.get("plugin_params")
    batch_size: int = GulpConfig.get_instance().concurrency_max_tasks()
    callback: callable = kwds.get("callback")
    callback_args: Any = kwds.get("callback_args")
    callback_async: bool = kwds.get("callback_async", False)

    # track overall stats
    all_results: list[tuple[int, Exception, str]] = []
    query_matched_total: int = 0
    total_doc_matches: int = 0
    all_errors: list[str] = []
    all_query_names: list[str] = []
    try:
        # process in batches to limit resource usage
        num_queries = len(queries)
        MutyLogger.get_instance().info(
            "will spawn %d queries in batches of %d !" % (num_queries, batch_size)
        )
        # compute number of batches using ceiling division
        if batch_size <= 0:
            batch_size = max(1, num_queries)
        num_batches = (num_queries + batch_size - 1) // batch_size

        # build batches of batch_size
        processed: int = 0
        canceled: bool = False
        shutdown: bool = False
        for i in range(0, num_queries, batch_size):
            current_batch = i // batch_size + 1
            batch = queries[i : i + batch_size]

            # process this batch using workers
            batch_results, batch_matched, batch_matches, batch_names, batch_errors = (
                await _process_query_batch(
                    batch=batch,
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    index=index,
                    q_options=q_options,
                    plugin=plugin,
                    plugin_params=plugin_params,
                    flt=flt,
                    current_batch=current_batch,
                    num_batches=num_batches,
                )
            )
            # increment by the actual number of processed queries
            processed += len(batch)
            if processed > 0 and (processed % 100 == 0):
                # send progress packet to the websocket every 100 queries
                wsq = GulpWsSharedQueue.get_instance()
                await wsq.put(
                    t="TOREMOVEprogress",
                    ws_id=ws_id,
                    user_id=user_id,
                    req_id=req_id,
                )
                MutyLogger.get_instance().debug(
                    "processed %d queries, total=%d, total_matches=%d"
                    % (processed, num_queries, total_doc_matches)
                )

                # check if request is canceled
                async with GulpCollab.get_instance().session() as sess:
                    canceled = await GulpRequestStats.is_canceled(sess, req_id)
                    if canceled:
                        MutyLogger.get_instance().warning("request canceled!")
                        break

                # check shutdown initiated
                if GulpRestServer.get_instance().is_shutdown():
                    shutdown = True
                    break

            # accumulate results for later processing if needed
            all_results.extend(batch_results)
            query_matched_total += batch_matched
            total_doc_matches += batch_matches
            all_query_names.extend(batch_names)

            for err in batch_errors:
                # only add error if its not already there
                if err not in all_errors:
                    all_errors.append(err)

        # log summary of query group results
        MutyLogger.get_instance().debug(
            "FINISHED query group=%s matched %d/%d queries, total hits=%d"
            % (q_options.group, query_matched_total, num_queries, total_doc_matches)
        )
        if shutdown:
            # we're done
            return

        # TODO:send progress packet (done) to the websocket
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            t="TOREMOVE",
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            # d=p.model_dump(exclude_none=True),
        )

        # if query groups is set and all queries in the group matched, update note tags and send notification
        if q_options.group and (num_queries > 1 and query_matched_total == num_queries):
            await _handle_query_group_match(
                operation_id=operation_id,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                q_options=q_options,
                query_names=all_query_names,
                total_doc_matches=total_doc_matches,
            )

    finally:
        # also update stats
        async with GulpCollab.get_instance().session() as sess:
            await GulpRequestStats.finalize_query_stats(
                sess,
                req_id=req_id,
                ws_id=ws_id,
                user_id=user_id,
                hits=total_doc_matches,
                errors=all_errors,
                ws_data_type=WSDATA_QUERY_GROUP_DONE,
                num_queries=num_queries,
                q_group=q_options.group,
            )

        if callback:
            MutyLogger.get_instance().debug(
                "calling callback %s, args=%s, async=%s"
                % (callback, callback_args, callback_async)
            )
            # call the callback
            if callback_async:
                await callback(*callback_args) if callback_args else await callback()
            else:
                callback(*callback_args) if callback_args else callback()

        # cleanup
        all_results.clear()
        all_errors.clear()
        all_query_names.clear()


async def _spawn_query_group_workers(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    queries: list[GulpQuery],
    q_options: GulpQueryParameters = None,
    plugin: str = None,
    plugin_params: GulpPluginParameters = None,
    flt: GulpQueryFilter = None,
    create_stats: bool = True,
    callback: callable = None,
    callback_args: Any = None,
    callback_async: bool = False,
) -> None:
    """
    spawns worker tasks for each query and wait them all
    """
    MutyLogger.get_instance().debug("spawning %d queries ..." % (len(queries)))
    kwds = dict(
        user_id=user_id,
        req_id=req_id,
        ws_id=ws_id,
        operation_id=operation_id,
        index=index,
        queries=queries,
        q_options=q_options,
        plugin=plugin,
        plugin_params=plugin_params,
        flt=flt,
        callback=callback,
        callback_args=callback_args,
        callback_async=callback_async,
    )

    if create_stats:
        # create a stats, just to allow request canceling
        async with GulpCollab.get_instance().session() as sess:
            await GulpRequestStats.create_or_get(
                sess=sess,
                req_id=req_id,
                user_id=user_id,
                ws_id=ws_id,
                operation_id=operation_id,
                object_data=None,  # uses default
                stats_type=RequestStatsType.REQUEST_TYPE_QUERY,
            )

    # run _worker_coro in background, it will spawn a worker for each query and wait them
    GulpRestServer.get_instance().spawn_bg_task(_worker_coro(kwds))


async def _query_raw_internal(
    sess: AsyncSession,
    user_id: str,
    operation_id: str,
    req_id: str,
    ws_id: str,
    index: str,
    q: list[dict] | list[GulpQuery],
    q_options: GulpQueryParameters,
) -> dict:
    # if q_options.preview_mode:
    #     if len(q) > 1:
    #         raise ValueError(
    #             "if `q_options.preview_mode` is set, only one query is allowed."
    #         )

    #     # preview mode, run the query and return the data
    #     total, docs = await _preview_query(
    #         sess,
    #         operation_id=operation_id,
    #         user_id=user_id,
    #         req_id=req_id,
    #         q=q[0],
    #         query_index=index,
    #         q_options=q_options,
    #     )
    #     return {
    #         "total_hits": total,
    #         "docs": docs
    #     }

    #     queries: list[GulpQuery] = []
    #     if isinstance(q[0], GulpQuery):
    #         queries=q
    #     else:
    #         for qq in q:
    #             # build query
    #             gq = GulpQuery(name=q_options.name, q=qq)
    #             queries.append(gq)

    #     # add query to history (first one only)
    #     await GulpUser.add_query_history_entry(
    #         user_id, queries[0].q, q_options=q_options
    #     )

    #         await _spawn_query_group_workers(
    #             user_id=user_id,
    #             req_id=req_id,
    #             ws_id=ws_id,
    #             operation_id=operation_id,
    #             index=op.index,
    #             queries=queries,
    #             q_options=q_options,
    #         )

    pass


async def _run_query(user_id: str,
    req_id: str,
    operation_id: str,
    ws_id: str,
    queries: list[GulpQuery],
    num_total_queries: int,
    q_options: GulpQueryParameters,
    index: str = None,
    plugin: str = None,
    plugin_params: GulpPluginParameters = None,
) -> tuple[int, str]:
    """
    runs a single query in a worker process and wait for the result.
    """
    total_hits: int = 0
    total_processed: int = 0    
    mod: GulpPluginBase = None

    try:
        if plugin:
            # external query, load plugin (it is guaranteed to be the same for all queries)
            mod = await GulpPluginBase.load(plugin)
            total_processed, total_hits = await mod.query_external()
        else:
            # raw gulp query
    finally:
        if mod:
            await mod.unload()
    return total_hits, total_processed

async def process_query_batch(
    user_id: str,
    req_id: str,
    operation_id: str,
    ws_id: str,
    queries: list[GulpQuery],
    num_total_queries: int,
    q_options: GulpQueryParameters,
    index: str = None,
    plugin: str = None,
    plugin_params: GulpPluginParameters = None,
    write_history: bool = True,
) -> None:
    """ 
    runs in a background task and spawns workers to process the query batch.
    if plugin is set, queries size is always 1 (external query)
    """
    l: int = len(queries)
    stats: GulpRequestStats = None
    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, or get it if it doesn't exist yet
            stats, created = await GulpRequestStats.create_or_get_existing_stats(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_EXTERNAL_QUERY if plugin else RequestStatsType.REQUEST_TYPE_QUERY,
                ws_id=ws_id,
                data=GulpIngestionStats() if plugin else GulpQueryStats(num_queries=num_total_queries, q_group=q_options.group)
            )

            # spawn a worker for each query and wait them all
            batch_size = len(queries)
            if len(queries) > GulpConfig.get_instance().concurrency_max_tasks():
                # sub-batching
                batch_size = GulpConfig.get_instance().concurrency_max_tasks()
            
            for i in range(0, l, batch_size):
                batch = queries[i : i + batch_size]
                coros = []
                for q in batch:
                    coros.append(_run_query(
                        user_id=user_id,
                        req_id=req_id,
                        operation_id=operation_id,
                        ws_id=ws_id,
                        queries=[q],
                        num_total_queries=num_total_queries,
                        q_options=q_options,
                        index=index,
                        plugin=plugin,
                        plugin_params=plugin_params
                    ))

        except:
            await sess.rollback()
            raise
    #     # 2. batch queries in configured batch size
    #     batch_size: int = GulpConfig.get_instance().concurrency_max_tasks()
    #     for i in range(0, len(queries), batch_size):
    #         batch = queries[i : i + batch_size]
    #         MutyLogger.get_instance().debug(
    #             "processing queries batch %d/%d, size of batch=%d",
    #             i // batch_size + 1,
    #             (len(queries) + batch_size - 1) // batch_size,
    #             len(batch),
    #         )
    #         if write_history:
    #             # add each query in the batch to user history
    #             async with GulpCollab.get_instance().session() as sess:
    #                 try:
    #                     for gq in batch:
    #                         await GulpUser.add_query_history_entry(
    #                             sess,
    #                             gq.q,
    #                             q_options=q_options,
    #                             sigma_yml=gq.sigma_yml,
    #                             external=True if plugin else False,
    #                             plugin=plugin,
    #                             plugin_params=plugin_params,
    #                         )
    #                 except:
    #                     await sess.rollback()
    #                     raise
    # finally:
    #     pass


async def _preview_query(
    sess: AsyncSession,
    q: Any,
    q_options: GulpQueryParameters,
    index: str = None,
    plugin: str = None,
    plugin_params: GulpPluginParameters = None,
) -> tuple[int, list[dict]]:
    """
    runs a preview query (no ingestion) and returns the results directly.

    plugin and plugin_params are meant for external querie only, index is ignored in that case.
    
    Returns:
        tuple[int, list[dict]]: total hits, list of documents
    """
    q_options.loop = False
    q_options.fields = "*"
    q_options.limit = GulpConfig.get_instance().preview_mode_num_docs()
    MutyLogger.get_instance().debug("running preview query %s" % (q))

    mod: GulpPluginBase = None
    total_hits: int = 0
    docs: list[dict] = []

    if plugin:
        # this is an external query, load plugin
        try:
            mod = await GulpPluginBase.load(plugin)
            # external query
            total_hits, _ = await mod.query_external(
                sess,
                stats=None,
                user_id=None,
                req_id=None,
                ws_id=None,
                operation_id=None,
                q=q,
                index=None,
                plugin_params=plugin_params,
                q_options=q_options,
            )
            docs = mod.preview_chunk()
            total_hits = len(docs)
        finally:
            if mod:
                await mod.unload()
    else:
        # standard query
        total_hits, docs, _ = await GulpOpenSearch.get_instance().search_dsl_sync(
            index, q, q_options, raise_on_error=True
        )
        for d in docs:
            # remove highlight, not needed in preview
            d.pop("highlight", None)

    return total_hits, docs


@router.post(
    "/query_raw",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "default": {
                            "value": {
                                "status": "pending",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                            }
                        },
                        "preview": {
                            "value": {
                                "status": "success",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                                "data": {
                                    "total_hits": 1234,
                                    "docs": [
                                        muty.pydantic.autogenerate_model_example_by_class(
                                            GulpDocument
                                        )
                                    ],
                                },
                            }
                        },
                    }
                }
            }
        }
    },
    summary="Advanced query.",
    description="""
query Gulp with a raw OpenSearch DSL query.

- this API returns `pending` and results are streamed to the `ws_id` websocket (unless `q_options.preview_mode` is set, read later).
- `q` must be one or more queries with a format according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/)
- if `q_options.create_notes` is set, notes are created for each query match: each note `tags` will include `q_options.name` and, if set, `q_options.group` if all queries in `q` matches.
- if `q_options.preview_mode` is set, this API takes the first query in the `q` array and the data is returned directly without using the websocket.

## tracking progress

during `raw_query, the flow of data on `ws_id` is the following:

- `WSDATA_STATS_CREATE`: `GulpRequestStats`, data=`GulpQueryStats` (at start)
- `WSDATA_STATS_UPDATE`: `GulpRequestStats`, data=updated `GulpQueryStats` (once every `q_options.limit` documents, default=1000)
- `WSDATA_DOCUMENTS_CHUNK`: `GulpDocumentsChunkPacket` on each documents chunk retrieved
- `WSDATA_QUERY_DONE`: `GulpIngestSourceDonePacket` when each query in `q` is done
- `WSDATA_QUERY_GROUP_MATCH`: `GulpQueryGroupMatchPacket` if `q_options.q_group` is set and all queries in `q` matches.
""",
)
async def query_raw_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    q: Annotated[
        list[dict],
        Body(
            description="""
one or more queries according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/).

- each query in `q` is run in a task in one of the worker processes
""",
            examples=[[EXAMPLE_QUERY_RAW], [{"query": {"match_all": {}}}]],
        ),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    ##### WIP
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s = await GulpUserSession.check_token(sess, token, obj=op)
                user_id = s.user.id
                if q_options.preview_mode:
                    total_hits, docs = await _preview_query(
                        sess,
                        q=q[0],
                        q_options=q_options,
                        index=op.index,
                    )
                    return JSONResponse(
                        JSendResponse.success(
                            req_id=req_id, data={"total_hits": total_hits, "docs": docs}
                        )
                    )

                # build queries
                queries: list[GulpQuery] = []
                count: int = 0
                for qq in q:
                    gq = GulpQuery(
                        name="%s-%d" % (q_options.name, count),
                        q=qq,
                    )
                    count += 1
                    queries.append(gq)

                # run a background task (will batch queries, spawn workers, gather results)
                coro = process_query_batch(
                    user_id, req_id, operation_id, ws_id, queries, len(queries)
                )
                GulpRestServer.get_instance().spawn_bg_task(coro)

                # and return pending
                return JSONResponse(JSendResponse.pending(req_id=req_id))
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    # try:
    #     async with GulpCollab.get_instance().session() as sess:
    #         try:
    #             # get operation and check acl
    #             op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
    #             s = await GulpUserSession.check_token(
    #                 sess, token, obj=op
    #             )
    #             user_id = s.user.id
    #             if not q_options.name:
    #                 q_options.name = muty.string.generate_unique()
    #             if q_options.preview_mode:
    #                 if len(q) > 1:
    #                     raise ValueError(
    #                         "if `q_options.preview_mode` is set, only one query is allowed."
    #                     )

    #                 # preview mode, run the query and return the data
    #                 total, docs = await _preview_query(
    #                     sess,
    #                     operation_id=operation_id,
    #                     user_id=user_id,
    #                     req_id=req_id,
    #                     q=q[0],
    #                     query_index=op.index,
    #                     q_options=q_options,
    #                 )
    #                 return JSONResponse(
    #                     JSendResponse.success(
    #                         req_id=req_id, data={"total_hits": total, "docs": docs}
    #                     )
    #                 )

    #             queries: list[GulpQuery] = []
    #             for qq in q:
    #                 # build query
    #                 gq = GulpQuery(name=q_options.name, q=qq)
    #                 queries.append(gq)

    #         # add query to history (first one only)
    #         await GulpUser.add_query_history_entry(
    #             user_id, queries[0].q, q_options=q_options
    #         )

    #         await _spawn_query_group_workers(
    #             user_id=user_id,
    #             req_id=req_id,
    #             ws_id=ws_id,
    #             operation_id=operation_id,
    #             index=op.index,
    #             queries=queries,
    #             q_options=q_options,
    #         )

    #         # and return pending
    #         return JSONResponse(JSendResponse.pending(req_id=req_id))
    #     except Exception as ex:
    #         await sess.rollback()
    #         raise
    # except Exception as ex:
    #     raise JSendException(req_id=req_id) from ex


@router.post(
    "/query_gulp",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "default": {
                            "value": {
                                "status": "pending",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                            }
                        },
                        "preview": {
                            "value": {
                                "status": "success",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                                "data": {
                                    "total_hits": 1234,
                                    "docs": [
                                        muty.pydantic.autogenerate_model_example_by_class(
                                            GulpDocument
                                        )
                                    ],
                                },
                            }
                        },
                    }
                }
            }
        }
    },
    summary="Simple Gulp query.",
    description="""
use this API just for simple query using the pre-made filters in `GulpQueryFilter`.

for anything else, it is advised to use the more powerful `query_raw` API.

- flt.operation_ids is enforced to the provided `operation_id`.
- this API returns `pending` and results are streamed to the `ws_id` websocket.
- if `q_options.preview_mode` is set, this API returns the data (a chunk) itself, without using the websocket.
""",
)
async def query_gulp_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt)],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        # setup flt if not provided, must include operation_id
        if not flt:
            flt = GulpQueryFilter()
        flt.operation_ids = [operation_id]

        async with GulpCollab.get_instance().session() as sess:
            permission = GulpUserPermission.READ

            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, permission=permission, obj=op
            )
            user_id = s.user.id
            index = op.index

        # convert gulp query to raw query
        dsl = flt.to_opensearch_dsl()
        if not q_options.name:
            q_options.name = muty.string.generate_unique()
        gq = GulpQuery(name=q_options.name, q=dsl)

        if q_options.preview_mode:
            # preview mode, run the query and return the data
            total, docs = await _preview_query(
                operation_id=operation_id,
                user_id=user_id,
                req_id=req_id,
                q=dsl,
                query_index=index,
                q_options=q_options,
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id, data={"total_hits": total, "docs": docs}
                )
            )

        # add query to history
        await GulpUser.add_query_history_entry(
            user_id, gq.q, q_options=q_options, flt=flt
        )

        # spawn worker
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=index,
            queries=[gq],
            q_options=q_options,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/query_external",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "default": {
                            "value": {
                                "status": "pending",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                            }
                        },
                        "preview": {
                            "value": {
                                "status": "success",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                                "data": {
                                    "total_hits": 1234,
                                    "docs": [
                                        muty.pydantic.autogenerate_model_example_by_class(
                                            GulpDocument
                                        )
                                    ],
                                },
                            }
                        },
                    }
                }
            }
        }
    },
    summary="Query an external source.",
    description="""
queries an external source using the target source query language, ingesting data back into gulp at `operation_id`'s index.

- this API returns `pending` and results are streamed to the `ws_id` websocket.
- `plugin` must implement `query_external` method.
- `plugin_params.custom_parameters` must include all the parameters needed to connect to the external source.
- token must have `ingest` permission (unless `q_options.preview_mode` is set).
- if `q_options.preview_mode` is set, this API only accepts a single query in the `q` array and the data is returned directly without using the websocket.
- `q` runs in a task in one of the worker processes, unless `preview_mode` is set.

## tracking progress

during `external query` (which, from the gulp's side, is an `ingestion`), the flow of data on `ws_id` is the same as for `ingest_file` API.

- `WSDATA_STATS_CREATE`: `GulpRequestStats`, data=`GulpIngestionStats` (at start)
- `WSDATA_STATS_UPDATE`: `GulpRequestStats`, data=updated `GulpIngestionStats` (once every `q_options.limit` documents, default=1000)
- `WSDATA_DOCUMENTS_CHUNK`: `GulpDocumentsChunkPacket` on each documents chunk retrieved and ingested
- `WSDATA_INGEST_SOURCE_DONE`: `GulpIngestSourceDonePacket` when the query is done
""",
)
async def query_external_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    q: Annotated[
        str,
        Body(
            description="""a query according to the source language specifications.""",
            examples=[[{"query": {"match_all": {}}}]],
        ),
    ],
    plugin: Annotated[
        str,
        Query(
            description="the plugin implementing `query_external` to handle the external query."
        ),
    ],
    plugin_params: Annotated[
        GulpPluginParameters, Depends(APIDependencies.param_plugin_params)
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        try:
            async with GulpCollab.get_instance().session() as sess:
                # check token and get caller user id
                if q_options.preview_mode:
                    # preview mode, no ingest needed
                    permission = GulpUserPermission.READ

                else:
                    # external query with ingest, needs ingest permission
                    permission = GulpUserPermission.INGEST

                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s = await GulpUserSession.check_token(
                    sess, token, permission=permission, obj=op
                )
                user_id = s.user.id
                index = op.index
                if q_options.preview_mode:
                    # preview, query and return synchronously
                    total_hits, docs = await _preview_query(
                        sess,
                        q,
                        q_options,
                        index=index,
                        plugin=plugin,
                        plugin_params=plugin_params,
                    )
                    return JSONResponse(
                        JSendResponse.success(
                            req_id=req_id, data={"total_hits": total_hits, "docs": docs}
                        )
                    )

                # run a background task (will batch queries, spawn workers, gather results)
                queries: list[GulpQuery] = [GulpQuery(name=q_options.name, q=q)]
                coro = process_query_batch(
                    user_id, req_id, operation_id, ws_id, queries, 1, q_options,
                    index=index, plugin=plugin, plugin_params=plugin_params
                )
                GulpRestServer.get_instance().spawn_bg_task(coro)

                # and return pending
                return JSONResponse(JSendResponse.pending(req_id=req_id))
        except Exception as ex:
            await sess.rollback()
            raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/query_sigma",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "default": {
                            "value": {
                                "status": "pending",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                            }
                        },
                        "preview": {
                            "value": {
                                "status": "success",
                                "timestamp_msec": 1704380570434,
                                "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                                "data": {
                                    "total_hits": 1234,
                                    "docs": [
                                        muty.pydantic.autogenerate_model_example_by_class(
                                            GulpDocument
                                        )
                                    ],
                                },
                            }
                        },
                    }
                }
            }
        }
    },
    summary="Query using sigma rules.",
    description="""
query using [sigma rules](https://github.com/SigmaHQ/sigma).

- sigma rules in `sigmas` are batched and runs in parallel in concurrent tasks into worker processes.
- this API returns `pending` and results are streamed to the `ws_id` websocket.

### q_options

- `create_notes` is set to `True` to create notes on match (if not set explicitly to False).
- if more than one query is provided, `q_options.group` must be set.
- if `q_options.preview_mode` is set, this API only accepts a single query in the `sigmas` array and the data is returned directly without using the websocket.

### src_ids

if not provided, `all sources in the operation` are used (not advised).

### pre-filtering sigma rules

sigma rules may be filtered using `levels`, `products`, `categories`, `services` and `tags` parameters.

### tracking query progresses

look at `query_raw` API for details about progress packets sent to the `ws_id` websocket.

""",
)
async def query_sigma_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    sigmas: Annotated[
        list[str],
        Body(
            description="one or more sigma rule YAML to be converted into queries to Gulp.",
            examples=[[EXAMPLE_SIGMA_RULE]],
        ),
    ],
    src_ids: Annotated[
        list[str],
        Body(description="ids of the source to apply the query/ies to."),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ] = None,
    levels: Annotated[
        Optional[list[str]],
        Body(
            description="optional `sigma.level` to restrict the applied sigma rules (`high`, `low`, `medium`, `critical`, `informational`)",
            examples=[["high", "critical"]],
        ),
    ] = None,
    products: Annotated[
        Optional[list[str]],
        Body(
            description="optional `sigma.logsource.product` to restrict the applied sigma rules.",
            examples=[["windows"]],
        ),
    ] = None,
    categories: Annotated[
        Optional[list[str]],
        Body(
            description="optional `sigma.logsource.category` to restrict the applied sigma rules."
        ),
    ] = None,
    services: Annotated[
        Optional[list[str]],
        Body(
            description="optional `sigma.logsource.service` to restrict the applied sigma rules.",
            examples=[["windefend"]],
        ),
    ] = None,
    tags: Annotated[
        Optional[list[str]],
        Body(description="optional `sigma.tags` to restrict the applied sigma rules."),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # setup flt if not provided, must include operation_id
    if not src_ids:
        # all sources
        flt = GulpQueryFilter()
    else:
        # only query these sources
        flt = GulpQueryFilter(source_ids=src_ids)

    flt.operation_ids = [operation_id]

    try:
        if len(sigmas) > 1 and not q_options.group:
            raise ValueError(
                "if more than one query is provided, `q_options.group` must be set."
            )

        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, obj=op)
            user_id = s.user.id
            index = op.index

            # convert sigma rule/s using pysigma
            queries: list[GulpQuery] = await sigmas_to_queries(
                sess,
                user_id,
                operation_id,
                sigmas,
                src_ids=src_ids,
                levels=levels,
                products=products,
                categories=categories,
                services=services,
                tags=tags,
                req_id=req_id,
                ws_id=ws_id,
            )

        if q_options.preview_mode:
            if len(sigmas) > 1:
                raise ValueError(
                    "if `q_options.preview_mode` is set, only one query is allowed."
                )

            # preview mode, run the query and return the data
            total, docs = await _preview_query(
                operation_id=operation_id,
                user_id=user_id,
                req_id=req_id,
                q=queries[0].q,
                query_index=op.index,
                q_options=q_options,
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id, data={"total_hits": total, "docs": docs}
                )
            )

        # add query to history (first one only)
        await GulpUser.add_query_history_entry(
            user_id, queries[0].q, q_options=q_options, flt=flt, sigma=sigmas[0]
        )

        # spawn one aio task, it will spawn n multiprocessing workers and wait them
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=index,
            queries=queries,
            q_options=q_options,
            flt=flt,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/query_single_id",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": autogenerate_model_example_by_class(GulpDocument),
                    }
                }
            }
        }
    },
    summary="Query a single document.",
    description="""
query Gulp for a single document.
""",
)
async def query_single_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document on Gulp `index`."),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index

        d = await GulpOpenSearch.get_instance().query_single_document(index, doc_id)
        return JSONResponse(JSendResponse.success(req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/query_history_get",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": autogenerate_model_example_by_class(
                            GulpUserDataQueryHistoryEntry
                        ),
                    }
                }
            }
        }
    },
    summary="Get the query history for the calling user.",
    description="""
returns the last queries performed by the user.

if `query_history_max_size` is not set in the configuration, it defaults to 20.
""",
)
async def query_history_get_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession = await GulpUserSession.check_token(sess, token)
                d = await s.user.get_query_history()
                return JSONResponse(JSendResponse.success(req_id, data=d))
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/query_max_min_per_field",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": {
                            "buckets": [
                                {
                                    "*": {
                                        "doc_count": 98632,
                                        "max_event.code": 62171,
                                        "min_gulp.timestamp": 1289373941000000000,
                                        "max_gulp.timestamp": 1637340783836550912,
                                        "min_event.code": 0,
                                    }
                                }
                            ],
                            "total": 98632,
                        },
                    }
                }
            }
        }
    },
    summary="gets max/min `@timestamp` and `event.code` in the given `index`",
    description="""
- use `flt` to restrict the query (flt.operation_ids is enforced to the provided `operation_id`).
""",
)
async def query_max_min_per_field(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    group_by: Annotated[
        Optional[str],
        Query(description="group by field (i.e. `event.code`), default=no grouping"),
    ] = None,
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # setup flt if not provided, must include operation_id
    if not flt:
        flt = GulpQueryFilter()
    flt.operation_ids = [operation_id]

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index

        d = await GulpOpenSearch.get_instance().query_max_min_per_field(
            index, group_by=group_by, flt=flt
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/query_operations",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": [
                            {
                                "name": "example operation",
                                "id": "test_operation",
                                "index": "test_operation",
                                "contexts": [
                                    {
                                        "name": "test_context",
                                        "id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                                        "doc_count": 98632,
                                        "plugins": [
                                            {
                                                "name": "win_evtx",
                                                "sources": [
                                                    {
                                                        "name": "/home/valerino/repos/gulp/samples/win_evtx/security_big_sample.evtx",
                                                        "id": "fabae8858452af6c2acde7f90786b3de3a928289",
                                                        "doc_count": 62031,
                                                        "max_event.code": 5158,
                                                        "min_event.code": 1102,
                                                        "min_gulp.timestamp": 1475718427166301952,
                                                        "max_gulp.timestamp": 1475833104749409792,
                                                    },
                                                    {
                                                        "name": "/home/valerino/repos/gulp/samples/win_evtx/2-system-Security-dirty.evtx",
                                                        "id": "60213bb57e849a624b7989c448b7baec75043a1b",
                                                        "doc_count": 14621,
                                                        "max_event.code": 5061,
                                                        "min_event.code": 1100,
                                                        "min_gulp.timestamp": 1532738204663494144,
                                                        "max_gulp.timestamp": 1553118827379374080,
                                                    },
                                                ],
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="query operations with aggregations.",
    description="""
for each `operation` returns `sources` and `contexts` with their max/min `event.code` and `gulp.timestamp`.
""",
)
async def query_operations(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession = await GulpUserSession.check_token(sess, token)
            user_id = s.user.id

        # check token and get its accessible operations
        ops: list[dict] = await GulpOperation.get_by_filter_wrapper(
            token,
            GulpCollabFilter(),
        )
        operations: list[dict] = []
        for o in ops:
            # get each op details by querying the associated index
            d = await GulpOpenSearch.get_instance().query_operations(
                o["index"], user_id
            )
            operations.extend(d)

        return JSONResponse(JSendResponse.success(req_id=req_id, data=operations))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/query_fields_by_source",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701266243057,
                        "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
                        "data": {
                            "@timestamp": "date_nanos",
                            "agent.type": "keyword",
                            "destination.ip": "ip",
                            "destination.port": "long",
                            "event.category": "keyword",
                            "event.code": "keyword",
                            "event.duration": "long",
                            "event.original": "text",
                            "event.sequence": "long",
                            "event.type": "keyword",
                            "gulp.context_id": "keyword",
                            "gulp.event_code": "long",
                            "gulp.operation_id": "keyword",
                            "gulp.source_id": "keyword",
                            "gulp.timestamp": "long",
                            "gulp.timestamp_invalid": "boolean",
                            "gulp.unmapped.AccessList": "keyword",
                            "gulp.unmapped.AccessMask": "keyword",
                            "gulp.unmapped.AccountExpires": "keyword",
                            "gulp.unmapped.AdditionalInfo": "keyword",
                            "gulp.unmapped.AllowedToDelegateTo": "keyword",
                            "gulp.unmapped.AuthenticationPackageName": "keyword",
                            "gulp.unmapped.Data": "keyword",
                        },
                    }
                }
            }
        }
    },
    summary="get source fields=>type mapping.",
    description="""
get all `key=type` source fields-to-type mappings for the given given `operation_id`, `context_id` and `source_id`.

- if this api returns an empty mapping, it means the mapping is not yet available: a worker task is spawned to update the mapping in background and the client should retry later (no new task is spawned if one is already running for the same `operation_id`, `context_id` and `source_id`).
""",
)
async def query_fields_by_source_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    # TODO: make it return pending and send the result to websocket ?
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s: GulpUserSession = await GulpUserSession.check_token(
                    sess, token, obj=op
                )
                index = op.index
                user_id = s.user.id

                # check if there is at least one document with operation_id, context_id and source_id
                await GulpOpenSearch.get_instance().search_dsl_sync(
                    index=index,
                    q={
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"gulp.operation_id": operation_id}},
                                    {"term": {"gulp.context_id": context_id}},
                                    {"term": {"gulp.source_id": source_id}},
                                ]
                            }
                        }
                    },
                    q_options=GulpQueryParameters(limit=1),
                    raise_on_error=True,
                )

                m = await GulpOpenSearch.get_instance().datastream_get_field_types_by_src(
                    sess,
                    operation_id,
                    context_id,
                    source_id,
                    user_id,
                )
                if m:
                    # return field types
                    return JSONResponse(JSendResponse.success(req_id=req_id, data=m))

                # spawn a task to run fields mapping in a worker and return empty
                await GulpRestServer.get_instance().spawn_worker_task(
                    GulpOpenSearch.get_instance().datastream_update_source_field_types_by_src,
                    index,
                    user_id,
                    task_name="query_fields_by_source_handler_%s_%s_%s"
                    % (operation_id, context_id, source_id),
                    operation_id=operation_id,
                    context_id=context_id,
                    source_id=source_id,
                )
                return JSONResponse(JSendResponse.success(req_id=req_id, data={}))
            except Exception as ex:
                await sess.rollback()
                raise ex
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def _write_file_callback(
    sess: AsyncSession,
    chunk: list[dict],
    chunk_num: int = 0,
    total_hits: int = 0,
    index: str = None,
    last: bool = False,
    req_id: str = None,
    q_name: str = None,
    q_group: str = None,
    **kwargs,
) -> list[dict]:
    """
    export file callback,  to write each chunk to file
    """
    f = kwargs.get("f")

    if chunk_num == 0:
        # first chunk, write json header
        await f.write('{\n\t"docs": [\n'.encode())

    # write this chunk
    for d in chunk:
        # remove highlight if any
        d.pop("highlight", None)
        d = json.dumps(d, indent="\t")
        await f.write(f"\t{d},\n".encode())

    if last:
        # terminate json
        await f.seek(-2, os.SEEK_END)
        await f.truncate()
        await f.write("\t]\n}\n".encode())


async def _export_json_internal(
    index: str,
    operation_id: str,
    q: dict,
    req_id: str = None,
    q_options: "GulpQueryParameters" = None,
) -> str | Exception:
    """
    runs in a worker process, exports the query results as json file and returns the file path (or Exception on error).
    """

    file_path: str = tempfile.mkstemp(
        suffix=".json", prefix="gulp_export_%s" % (req_id)
    )[1]
    async with aiofiles.open(file_path, "wb") as f:
        try:
            await GulpOpenSearch.get_instance().search_dsl(
                None,  # we don't need to check for request cancellation here
                index,
                q,
                operation_id=operation_id,
                req_id=req_id,
                q_options=q_options,
                callback=_write_file_callback,
                f=f,
            )
        except Exception as ex:
            MutyLogger.get_instance().error(
                "export json failed, removing file %s" % file_path
            )
            await muty.file.delete_file_or_dir_async(file_path)
            return ex
    return file_path


@router.post(
    "/query_gulp_export_json",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {"examples": {"default": {"value": {"docs": [{}]}}}}
            }
        }
    },
    summary="Export documents as JSON.",
    description="""
same as performing a `query_gulp` but streams a JSON file as response to the client.

- flt.operation_ids is enforced to the provided `operation_id`.
- `q_options.preview_mode` and `q_options.note_parameters.create_notes` are ignored.
- if unset, `q_options.fields` is set to `*` to export all fields.
""",
)
async def query_gulp_export_json_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt)],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> FileResponse:
    """
    export documents as json file and stream to client.

    performs the same operation as query_gulp but returns a json file instead of websocket results.

    Args:
        bt (BackgroundTasks): used to cleanup temporary file after response is sent
        token (str): authentication token
        operation_id (str): operation identifier
        flt (GulpQueryFilter, optional): query filter parameters
        q_options (GulpQueryParameters, optional): query execution options
        req_id (str, optional): request identifier

    Returns:
        FileResponse: the exported json file for streaming to client

    Throws:
        JSendException: if authentication fails or export operation fails
    """
    params = locals()
    params.pop("bt")  # remove background tasks from params
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # ignore preview mode and note creation for export
    q_options.preview_mode = False
    q_options.note_parameters.create_notes = False
    if not q_options.fields:
        # export all fields if not specified
        q_options.fields = "*"
    # setup filter: if not provided, must include operation_id
    if not flt.operation_ids:
        flt.operation_ids = [operation_id]
    else:
        if operation_id not in flt.operation_ids:
            flt.operation_ids.append(operation_id)
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                _ = await GulpUserSession.check_token(sess, token, obj=op)
                index = op.index
            except Exception as ex:
                await sess.rollback()
                raise ex

            # convert gulp query to opensearch dsl
            dsl: dict = flt.to_opensearch_dsl()

            # execute export operation in worker process (non-blocking)
            file_path = await GulpRestServer.get_instance().spawn_worker_task(
                _export_json_internal,
                index,
                dsl,
                wait=True,
                req_id=req_id,
                operation_id=operation_id,
                q_options=q_options,
            )
            if isinstance(file_path, Exception):
                # is an exception
                raise file_path

            async def _cleanup(file_path: str) -> None:
                """
                cleanup function to remove temporary export file after response is sent.
                """
                if file_path:
                    MutyLogger.get_instance().debug(
                        "cleanup file %s after response sent" % file_path
                    )
                    await muty.file.delete_file_or_dir_async(file_path)

            # schedule cleanup after response is sent to client
            bt.add_task(_cleanup, file_path)

            # return file response for streaming to client
            return FileResponse(
                file_path,
                media_type="application/json",
                filename=f"gulp_export_{req_id}.json",
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
