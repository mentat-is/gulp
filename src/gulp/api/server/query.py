"""
Query API endpoints for Gulp providing various query capabilities.

This module contains FastAPI router endpoints for different types of queries:
- Raw OpenSearch DSL queries
- Simplified Gulp queries with filters
- External data source queries
- Sigma rule queries
- Single document queries
- Field mapping and aggregate operations

Each endpoint handles authentication, authorization, and supports both direct responses
and asynchronous processing with results streamed to websockets.

"""

# pylint: disable=too-many-lines

import asyncio
import json
import os
import tempfile
from copy import deepcopy
from typing import Annotated, Any, Optional

import aiofiles
import muty.file
import muty.log
import muty.pydantic
import muty.string
import muty.time
import orjson
from click import group
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    Query,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpQueryStats,
    GulpRequestStats,
    RequestCanceledError,
    RequestStatsType,
)
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user import GulpUser, GulpUserDataQueryHistoryEntry
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.sigma import (
    sigma_to_severity,
    sigmas_to_queries,
    sigmas_to_queries_wrapper,
)
from gulp.api.opensearch.structs import (
    GulpDocument,
    GulpQuery,
    GulpQueryHelpers,
    GulpQueryParameters,
)
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.server.ingest import GulpIngestionStats
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import (
    TASK_TYPE_EXTERNAL_QUERY,
    TASK_TYPE_INGEST,
    TASK_TYPE_QUERY,
    APIDependencies,
)
from gulp.api.server_api import GulpServer
from gulp.api.ws_api import (
    WSDATA_DOCUMENTS_CHUNK,
    WSDATA_QUERY_DONE,
    WSDATA_QUERY_GROUP_DONE,
    WSDATA_QUERY_GROUP_MATCH,
    GulpConnectedSocket,
    GulpDocumentsChunkPacket,
    GulpQueryDonePacket,
    GulpQueryGroupMatchPacket,
    GulpRedisBroker,
    redis_brokerueueFullException,
)
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters, ObjectNotFound

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


async def _query_raw_chunk_callback(
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
    callback called by GulpOpenSearch.search_dsl when a chunk of documents is available:
    - sends the chunk to the websocket
    - creates notes if requested (and send them to the websocket as well)
    """
    cb_context: dict = kwargs["cb_context"]
    MutyLogger.get_instance().debug(
        "query chunk callback, chunk_num=%d, cb_context=%s",
        chunk_num,
        muty.string.make_shorter(cb_context, max_len=260),
    )

    cb_context["total_hits"] = total_hits
    cb_context["total_processed"] = cb_context.get("total_processed", 0) + len(chunk)

    user_id: str = cb_context["user_id"]
    operation_id: str = cb_context["operation_id"]
    ws_id: str = cb_context["ws_id"]
    q_options: GulpQueryParameters = cb_context["q_options"]
    q: dict = cb_context["q"]

    # we also have these for sigma query
    sigma_yml: str = cb_context.get("sigma_yml")
    tags: list[str] = cb_context.get("tags", [])

    if sigma_yml and q_options.create_notes:
        severity = await sigma_to_severity(sigma_yml)
        # severity value: critical | high | medium | low | informational
        match severity:
            case "high" | "critical":
                q_options.notes_color = "#ff0000"
            case "medium":
                q_options.notes_color = "#ffa500"
            case "low":
                q_options.notes_color = "#ffff00"
            case _:
                q_options.notes_color = "#ffffff"

    # send chunk of documents to the websocket
    c = GulpDocumentsChunkPacket(
        docs=chunk,
        chunk_size=len(chunk),
        chunk_num=chunk_num,
        total_hits=total_hits,
        name=q_name,
        last=last,
    )
    await GulpRedisBroker.get_instance().put(
        t=WSDATA_DOCUMENTS_CHUNK,
        ws_id=ws_id,
        user_id=user_id,
        operation_id=operation_id,
        req_id=req_id,
        d=c.model_dump(exclude_none=True),
        force_ignore_missing_ws=q_options.force_ignore_missing_ws,
    )

    if q_options.create_notes:
        # create notes for this chunk and send them to the websocket as well
        await GulpNote.bulk_create_for_documents_and_send_to_ws(
            sess,
            operation_id,
            user_id,
            ws_id,
            req_id,
            chunk,
            q_name,
            orjson.dumps(q, option=orjson.OPT_INDENT_2).decode(),
            q_options,
            sigma_yml=sigma_yml,
            tags=tags,
            color=q_options.notes_color,
            glyph_id=q_options.notes_glyph_id,
            last=last,
        )
    return chunk


async def run_query_task(t: dict) -> None:
    """
    runs in a worker process and executes a queued query task.

    Expected task dict (same shape used by enqueue in handlers):
      {
        "task_type": "query",
        "operation_id": <str>,
        "user_id": <str>,
        "ws_id": <str>,
        "req_id": <str>,
        "params": {
            "queries": [<GulpQuery dict>],
            "q_options": <dict>,
            "index": <str|null>,
            "plugin": <str|null>,
            "plugin_params": <dict|null>
         }
      }

    This function rehydrates the payload and calls the existing `process_queries` routine.
    """
    # MutyLogger.get_instance().debug("run_query_task, t=%s", muty.string.make_shorter(str(t),max_len=260))
    try:
        params: dict = t.get("params", {})
        user_id: str = t.get("user_id")
        req_id: str = t.get("req_id")
        operation_id: str = t.get("operation_id")
        ws_id: str = t.get("ws_id")

        queries = params.get("queries", [])
        q_options = params.get("q_options", {})
        index = params.get("index")
        plugin = params.get("plugin")
        plugin_params = params.get("plugin_params")
        total_num_queries: int = params.get(
            "total_num_queries", 0
        )  # this may be provided upfront
        ignore_failures: bool = params.get("ignore_failures", False)

        # rebuild pydantic models from dict payloads
        q_models: list[GulpQuery] = []
        for qq in queries:
            if isinstance(qq, dict):
                q_models.append(GulpQuery.model_validate(qq))
            elif isinstance(qq, GulpQuery):
                q_models.append(qq)
            else:
                # try to coerce
                q_models.append(GulpQuery(q=qq))

        q_options_model: GulpQueryParameters = (
            GulpQueryParameters.model_validate(q_options)
            if isinstance(q_options, dict)
            else q_options
        )

        # rebuild plugin_params if needed
        plugin_params_model = None
        if plugin_params:
            plugin_params_model = (
                GulpPluginParameters.model_validate(plugin_params)
                if isinstance(plugin_params, dict)
                else plugin_params
            )

        # call process_queries to handle all queries together (needed for group matching logic)
        async with GulpCollab.get_instance().session() as sess:
            # process
            await process_queries(
                sess,
                user_id=user_id,
                req_id=req_id,
                operation_id=operation_id,
                ws_id=ws_id,
                queries=q_models,
                num_total_queries=total_num_queries or len(q_models),
                q_options=q_options_model,
                index=index,
                plugin=plugin,
                plugin_params=plugin_params_model,
                ignore_failures=ignore_failures,
            )
    except Exception as ex:
        MutyLogger.get_instance().exception("error in run_query_task: %s", ex)


async def run_query(
    user_id: str,
    req_id: str,
    operation_id: str,
    ws_id: str,
    gq: GulpQuery,
    q_options: GulpQueryParameters,
    index: str = None,
    plugin: str = None,
    plugin_params: GulpPluginParameters = None,
) -> tuple[int, int, str, bool]:
    """
    runs a single query in a coroutine running in a worker process (through aiomultiprocess)

    Args:
        user_id: str - user id
        req_id: str - request id
        operation_id: str - operation id
        ws_id: str - websocket id
        gq: GulpQuery - query to run
        q_options: GulpQueryParameters - query options
        index: str - index to query (for external queries)
        plugin: str - plugin name (for external queries)
        plugin_params: GulpPluginParameters - plugin parameters (for external queries)

    Returns:
        total_hits: int - number of hits
        total_processed: int - number of processed documents (for external queries)
        query_name: str - name of the query
        canceled: bool - True if the request was canceled
    """
    total_hits: int = 0
    total_processed: int = 0
    err: str = None
    mod: GulpPluginBase = None
    canceled: bool = False
    stats: GulpRequestStats = None
    exx: Exception = None
    MutyLogger.get_instance().debug(
        "---> run_query: user_id=%s, req_id=%s, operation_id=%s, ws_id=%s, gq=%s, q_options=%s, index=%s, plugin=%s, plugin_params=%s",
        user_id,
        req_id,
        operation_id,
        ws_id,
        muty.string.make_shorter(str(gq), max_len=260),
        q_options,
        index,
        plugin,
        plugin_params,
    )

    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                if plugin:
                    # external query, load plugin (it is guaranteed to be the same for all queries)
                    mod = await GulpPluginBase.load(plugin)

                    # get the stats we previously created
                    stats = await GulpRequestStats.get_by_id(sess, req_id)
                    total_processed, total_hits = await mod.query_external(
                        sess,
                        stats,
                        user_id,
                        req_id,
                        ws_id,
                        operation_id,
                        gq.q,
                        index,
                        plugin_params,
                        q_options,
                    )
                else:
                    # raw query to gulp's opensearch
                    cb_context: dict = {
                        "user_id": user_id,
                        "operation_id": operation_id,
                        "ws_id": ws_id,
                        "q_options": q_options,
                        "q": gq.q,
                        "tags": gq.tags,
                        "sigma_yml": gq.sigma_yml,
                        "total_hits": 0,
                        "q_name": gq.q_name,
                    }
                    await GulpOpenSearch.get_instance().search_dsl(
                        sess,
                        index,
                        gq.q,
                        req_id=req_id,
                        q_options=q_options,
                        callback=_query_raw_chunk_callback,
                        cb_context=cb_context,
                    )
                    # MutyLogger.get_instance().debug(
                    #     "after search_dsl, cb_context=%s", cb_context
                    # )
                    total_hits = cb_context.get("total_hits", 0)
                    total_processed = cb_context.get("total_processed", 0)

                if total_hits == 0:
                    raise ObjectNotFound("no results found!")
                # return processed first then hits to match caller expectations
                # process_queries expects a tuple in the form: (processed, hits, q_name, canceled)
                return total_processed, total_hits, q_options.name, canceled

            except Exception as ex:
                if not isinstance(ex, ObjectNotFound):
                    # log this
                    MutyLogger.get_instance().exception(
                        "EXCEPTION=%s,\nquery=\n%s", str(ex), gq
                    )
                if isinstance(ex, RequestCanceledError):
                    # request is canceled
                    canceled = True
                exx = ex
                if not isinstance(ex, ObjectNotFound):
                    # do not reraise ObjectNotFound here, we want to send a done packet with 0 hits
                    raise
            finally:
                if mod:
                    # finalize external query stats
                    await mod.update_final_stats_and_flush(ex=exx)
                    await mod.unload()
    finally:
        if not isinstance(exx, RequestCanceledError):
            err = muty.log.exception_to_string(exx)

        # signal WSDATA_QUERY_DONE on the websocket
        status: GulpRequestStatus = GulpRequestStatus.DONE
        if canceled:
            status = GulpRequestStatus.CANCELED
        elif err:
            status = GulpRequestStatus.FAILED
        p = GulpQueryDonePacket(
            q_name=gq.q_name,
            status=status.value,
            errors=[err] if err else [],
            total_hits=total_hits,
            q_group=gq.q_group,
        )
        await GulpRedisBroker.get_instance().put(
            t=WSDATA_QUERY_DONE,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            d=p.model_dump(exclude_none=True),
            force_ignore_missing_ws=q_options.force_ignore_missing_ws,
        )


async def process_queries(
    sess: AsyncSession,
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
    ignore_failures: bool = False,
) -> bool:
    """
    runs in a background task and spawns workers to process queries, batching them if needed.

    index, plugin, plugin_params are used for external queries only

    Args:
        sess: AsyncSession - database session
        user_id: str - user id
        req_id: str - request id
        operation_id: str - operation id
        ws_id: str - websocket id
        queries: list[GulpQuery] - list of queries to run
        num_total_queries: int - total number of queries (for stats)
        q_options: GulpQueryParameters - query parameters
        index: str - index to query (for external queries)
        plugin: str - plugin name (for external queries)
        plugin_params: GulpPluginParameters - plugin parameters (for external queries)
        ignore_failures: bool - whether to ignore failures when finalizing stats (sets "failed" status only if all queries failed)

    Returns:
        bool: True if the request was canceled, False otherwise.
    """
    stats: GulpRequestStats = None
    batching_step_reached: bool = False
    req_canceled: bool = False
    stats: GulpRequestStats = None
    try:
        # get a stats, or create it if it doesn't exist yet
        # for query stats, we will have a GulpQueryStats payload
        # either, for external queries, we will have a GulpIngestionStats payload
        try:
            stats, _ = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=(
                    RequestStatsType.REQUEST_TYPE_EXTERNAL_QUERY
                    if plugin
                    else RequestStatsType.REQUEST_TYPE_QUERY
                ),
                ws_id=ws_id,
                data=(
                    GulpIngestionStats().model_dump(exclude_none=True)
                    if plugin
                    else GulpQueryStats(
                        num_queries=num_total_queries, q_group=q_options.group
                    ).model_dump(exclude_none=True)
                ),
            )
        except WebSocketDisconnect:
            # ignore if force_ignore_missing_ws is set
            if not q_options.force_ignore_missing_ws:
                raise
            

        if stats and stats.status == GulpRequestStatus.CANCELED.value:
            MutyLogger.get_instance().warning(
                "request %s already canceled, abort processing!", req_id
            )
            req_canceled = True
            return req_canceled

        # 1. add query history entries, only if group is not set: we want this only for manual queries
        if q_options.add_to_history and not q_options.group:
            history: list[GulpUserDataQueryHistoryEntry] = []
            for gq in queries:
                q_opts = deepcopy(
                    q_options
                )  # we must copy and put the query name, the rest of the structure is unchanged for every query
                q_opts.name = gq.q_name
                h: GulpUserDataQueryHistoryEntry = GulpUserDataQueryHistoryEntry(
                    q=gq.q,
                    timestamp_msec=muty.time.now_msec(),
                    external=True if plugin else False,
                    q_options=q_opts,
                    operation_id=operation_id,
                    plugin=plugin,
                    plugin_params=plugin_params,
                    sigma_yml=gq.sigma_yml,
                )

                history.append(h)
                u: GulpUser = await GulpUser.get_by_id(sess, user_id)
                await u.add_query_history_entry_batch(sess, history)

        # 2. batch queries and gather results: spawn a worker for each query and wait them all.
        # NOTE: for query_external, we will always have just one query to run
        batch_size: int = len(queries)
        if len(queries) > GulpConfig.get_instance().concurrency_num_tasks():
            batch_size = GulpConfig.get_instance().concurrency_num_tasks()

        MutyLogger.get_instance().debug(
            "processing %d queries in batches of %d ...",
            len(queries),
            batch_size,
        )

        results: list[tuple[int, int, str, bool] | Exception] = []
        matched_queries: dict[str, int] = {}
        for i in range(0, len(queries), batch_size):
            # run one batch
            batch: list[GulpQuery] = queries[i : i + batch_size]
            coros = []
            for gq in batch:
                q_opts = deepcopy(
                    q_options
                )  # we must copy and put the query name, the rest of the structure is unchanged for every query
                q_opts.name = gq.q_name
                run_query_args = dict(
                    user_id=user_id,
                    req_id=req_id,
                    operation_id=operation_id,
                    ws_id=ws_id,
                    gq=gq,
                    q_options=q_opts,
                    index=index,
                    plugin=plugin,
                    plugin_params=plugin_params,
                )
                # MutyLogger.get_instance().debug("about to run _run_query with: %s", run_query_args)
                coros.append(
                    GulpProcess.get_instance().process_pool.apply(
                        run_query, kwds=run_query_args
                    )
                )

            # gather results for this batch and accumulate
            batching_step_reached = True
            MutyLogger.get_instance().debug(
                "gathering results for %d queries ...", len(coros)
            )
            batch_res = await asyncio.gather(*coros, return_exceptions=True)
            results.extend(batch_res)

            # check results
            total_hits: int = 0
            total_processed: int = 0
            errors: list[str] = []
            for r in batch_res:
                if isinstance(r, tuple) and len(r) == 4:
                    # we have a result
                    processed, hits, q_name, canceled = r
                    if hits > 0 and q_options.group:
                        # increment matches for this query
                        if not matched_queries.get(q_name):
                            matched_queries[q_name] = 0
                        MutyLogger.get_instance().debug(
                            "query group, adding match for query %s", q_name
                        )
                        matched_queries[q_name] += 1

                    total_processed += processed
                    total_hits += hits
                    if canceled:
                        # the whole request has been canceled
                        req_canceled = True
                elif isinstance(r, Exception):
                    # we have an error
                    if "RequestCanceledError" in str(r):
                        # request is canceled
                        req_canceled = True

                    err = muty.log.exception_to_string(r)
                    if err not in errors:
                        errors.append(err)

            if not plugin and stats:
                # for default (non-external queries), update request stats, will auto-finalize the stats when completion is detected (all queries completed or request canceled)
                # in external queries, stats is updated by the ingestion loop
                try:
                    await stats.update_query_stats(
                        sess,
                        user_id=user_id,
                        ws_id=ws_id,
                        hits=total_hits,
                        inc_completed=len(coros),
                        errors=errors,
                        ignore_failures=ignore_failures,
                    )
                except WebSocketDisconnect:
                    # ignore if force_ignore_missing_ws is set
                    if not q_options.force_ignore_missing_ws:
                        raise
                    
            if req_canceled:
                # request is canceled, stop processing more batches
                MutyLogger.get_instance().warning(
                    "request canceled, stop processing batches!"
                )
                break

        # 3. process results
        if not q_options.group:
            # we're done
            MutyLogger.get_instance().debug("no query group set, we're done!")
            return req_canceled

        num_queries_in_group = len(queries)
        if len(matched_queries) < num_queries_in_group:
            # no matches at all
            MutyLogger.get_instance().warning(
                "query group set but no query group match (%d/%d matched)",
                len(matched_queries),
                num_queries_in_group,
            )
            return req_canceled
        MutyLogger.get_instance().info(
            "we have a query_group match for group %s !", q_options.group
        )

        # now, get all the keys in matched_queries and retag each note having the key as tag also with the group name
        query_names = list(matched_queries.keys())
        await GulpNote.bulk_update_for_group_match(
            sess, operation_id, query_names, q_options
        )

        # finally send the group match to the websocket
        p = GulpQueryGroupMatchPacket(
            group=q_options.group,
            matches=matched_queries,
            color=q_options.group_color,
            glyph_id=q_options.group_glyph_id,
        )
        await GulpRedisBroker.get_instance().put(
            WSDATA_QUERY_GROUP_MATCH,
            user_id,
            ws_id=ws_id,
            operation_id=operation_id,
            req_id=req_id,
            d=p.model_dump(exclude_none=True),
            force_ignore_missing_ws=q_options.force_ignore_missing_ws,
        )
    except Exception as ex:
        if stats and not batching_step_reached:
            # ensure we set the stats as failed on error (unless we reached batching code, in which case the stats will be auto-finalized)
            try:
                await stats.set_finished(
                    sess,
                    GulpRequestStatus.FAILED,
                    user_id=user_id,
                    ws_id=ws_id,
                    errors=[muty.log.exception_to_string(ex)],
                )
            except WebSocketDisconnect:
                # ignore if force_ignore_missing_ws is set
                if not q_options.force_ignore_missing_ws:
                    raise

        raise


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

    plugin and plugin_params are meant for external queries only, index is ignored for external queries.

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
                operation_id="preview",
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
        (
            total_hits,
            docs,
            _,
            _,
        ) = await GulpOpenSearch.get_instance().search_dsl_sync(
            index, q, q_options, raise_on_error=True
        )
        for d in docs:
            # remove highlight, not needed in preview
            d.pop("_highlight", None)

    return total_hits, docs


@router.post(
    "/aggregation_query",
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
                                "data": {"aggregations": {}},
                            }
                        },
                    }
                }
            }
        }
    },
    summary="Advanced query.",
    description="""
aggregations query Gulp with a raw OpenSearch DSL query.

- this API returns `success` and results are send in HTTP Response.
- `q` must be one aggregation querie with a format according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/)
""",
)
async def aggregation_query_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    q: Annotated[
        dict,
        Body(
            description="""
one aggregation querie according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/).
""",
            examples=[[EXAMPLE_QUERY_RAW], [{"query": {"match_all": {}}}]],
        ),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, obj=op)
            q_options = GulpQueryParameters()
            q_options.limit = 0
            (
                total_hits,
                _,
                _,
                aggregations,
            ) = await GulpOpenSearch.get_instance().search_dsl_sync(
                operation_id, q, q_options, raise_on_error=True
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={
                        "total_hits": total_hits,
                        "aggregations": aggregations,
                    },
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
- if `q_options.preview_mode` is set, this API takes the first query in the `q` array and the data is returned directly as `{ "total_hits": <total_hits>, "docs": <docs> }` in the response.
  size of `docs` in preview mode is limited to `gulp.config.preview_mode_num_docs` (default=10).

### tracking progress

during `raw_query, the flow of data on `ws_id` is the following:

- `WSDATA_STATS_CREATE`.payload: `GulpRequestStats`, data=`GulpQueryStats` (at start)
- `WSDATA_STATS_UPDATE`.payload: `GulpRequestStats`, data=updated `GulpQueryStats` (once every batch of queries in `q` completed)
- `WSDATA_DOCUMENTS_CHUNK`.payload: `GulpDocumentsChunkPacket` on each documents chunk retrieved
- `WSDATA_QUERY_DONE`.payload: `GulpIngestSourceDonePacket` when each **single** query in `q` is completed
- `WSDATA_QUERY_GROUP_MATCH`.payload: `GulpQueryGroupMatchPacket` if `q_options.q_group` is set and all queries in `q` matches.
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
        Depends(APIDependencies.param_q_options_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, obj=op)
            user_id = s.user.id
            if q_options.preview_mode:
                # preview mode, run the first query and return sync
                total_hits, docs = await _preview_query(
                    sess,
                    q[0],
                    q_options,
                    index=op.index,
                )
                return JSONResponse(
                    JSendResponse.success(
                        req_id=req_id,
                        data={
                            "total_hits": total_hits,
                            "docs": docs,
                        },
                    )
                )

            # build queries
            queries: list[GulpQuery] = []
            count: int = 0
            if len(q) == 1:
                # single query, use the provided name
                gq = GulpQuery(q_name=q_options.name, q=q[0])
                queries.append(gq)
            else:
                # we build names for each query
                for qq in q:
                    gq = GulpQuery(
                        q_name="%s-%d" % (q_options.name, count),
                        q=qq,
                        q_group=q_options.group,
                    )
                    count += 1
                    queries.append(gq)

            # run a background task (will batch queries, spawn workers, gather results)
            # enqueue task for worker dispatch (always enqueue unless preview)
            task_msg = {
                "task_type": TASK_TYPE_QUERY,
                "operation_id": operation_id,
                "user_id": user_id,
                "ws_id": ws_id,
                "req_id": req_id,
                "params": {
                    "queries": [q.model_dump(exclude_none=True) for q in queries],
                    "q_options": q_options.model_dump(exclude_none=True),
                },
            }
            await GulpRedis.get_instance().task_enqueue(task_msg)

            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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

### tracking progress

same as for `query_raw` API.

""",
)
async def query_gulp_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)
    ] = None,
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        # enforce operation_id in filter
        flt.operation_ids = [operation_id]

        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, obj=op)
            user_id = s.user.id

            # convert gulp query to raw query
            q: dict = flt.to_opensearch_dsl()
            if q_options.preview_mode:
                # preview mode, run the query and return the data
                total_hits, docs = await _preview_query(
                    sess,
                    q=q,
                    q_options=q_options,
                    index=op.index,
                )
                return JSONResponse(
                    JSendResponse.success(
                        req_id=req_id, data={"total_hits": total_hits, "docs": docs}
                    )
                )

            # run a background task to process the query: always enqueue (unless preview handled above)
            queries: list[GulpQuery] = [GulpQuery(q_name=q_options.name, q=q)]
            task_msg = {
                "task_type": TASK_TYPE_QUERY,
                "operation_id": operation_id,
                "user_id": user_id,
                "ws_id": ws_id,
                "req_id": req_id,
                "params": {
                    "queries": [q.model_dump(exclude_none=True) for q in queries],
                    "q_options": q_options.model_dump(exclude_none=True),
                    "index": op.index,
                },
            }
            await GulpRedis.get_instance().task_enqueue(task_msg)

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
    summary="Query an external source to import data into Gulp.",
    description="""
queries an external source using the target source query language, ingesting data back into gulp at `operation_id`'s index.

- this API returns `pending` and results are streamed to the `ws_id` websocket.
- `plugin` must implement `query_external` method.
- `plugin_params.custom_parameters` must include all the parameters needed to connect to the external source.
- token must have `ingest` permission (unless `q_options.preview_mode` is set).

### q_options

- `group` and `create_notes` are ignored for this API: notes are never created for external queries, and grouping is not supported.
- if `q_options.preview_mode` is set, this API only accepts a single query in the `q` array and the data is returned directly without using the websocket.
- `q` runs in a task in one of the worker processes, unless `preview_mode` is set.

### tracking progress

during `external query` (which, from the gulp's side, is an `ingestion`), the flow of data on `ws_id` is the same as for `ingest_file` API.

- `WSDATA_STATS_CREATE`.payload: `GulpRequestStats`, data=`GulpIngestionStats` (at start)
- `WSDATA_STATS_UPDATE`.payload: `GulpRequestStats`, data=updated `GulpIngestionStats` (once every `q_options.limit` documents, default=1000)
- `WSDATA_DOCUMENTS_CHUNK`.payload: `GulpDocumentsChunkPacket` on each documents chunk retrieved and ingested
- `WSDATA_INGEST_SOURCE_DONE`.payload: `GulpIngestSourceDonePacket` when the query is done
""",
)
async def query_external_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    q: Annotated[
        Any,
        Body(
            description="""a query according to the source language specifications.""",
            examples=[{"query": {"match_all": {}}}],
        ),
    ],
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    plugin_params: Annotated[
        GulpPluginParameters, Depends(APIDependencies.param_plugin_params)
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # ensure these are not set
    q_options.group = None
    q_options.create_notes = False
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

            # run a background task to process the query: always enqueue (unless preview handled above)
            queries: list[GulpQuery] = [GulpQuery(q_name=q_options.name, q=q)]
            task_msg = {
                # external query is an ingestion task
                "task_type": TASK_TYPE_EXTERNAL_QUERY,
                "operation_id": operation_id,
                "user_id": user_id,
                "ws_id": ws_id,
                "req_id": req_id,
                "params": {
                    "queries": [q.model_dump(exclude_none=True) for q in queries],
                    "q_options": q_options.model_dump(exclude_none=True),
                    "index": index,
                    "plugin": plugin,
                    "plugin_params": (
                        plugin_params.model_dump(exclude_none=True)
                        if plugin_params
                        else None
                    ),
                },
            }
            await GulpRedis.get_instance().task_enqueue(task_msg)

            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))
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
- this API returns `pending` and results are streamed to the `ws_id` websocket (unless `q_options.preview_mode` is set, read later).

### q_options

- `create_notes` may be set to `True` to create notes on match (as in `query_raw`)
- if `q_options.preview_mode` is set, only the first sigma rule in `sigmas` is converted and run, and the data is returned directly without using the websocket.
- if `q_options.group` is set, and all sigma rules match, notes created will include the group in their tags.

### src_ids

if not provided, **all sources** in the `operation_id` are used.

### pre-filtering sigma rules

sigma rules may be filtered using `levels`, `products`, `categories`, `services` and `tags` parameters.

### tracking progress

same as for `query_raw` API.

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
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, obj=op)
            user_id = s.user.id

            # convert sigma rule/s using pysigma
            queries: list[GulpQuery] = await GulpServer.get_instance().spawn_worker_task(
                sigmas_to_queries_wrapper,
                user_id,
                operation_id,
                (
                    sigmas[:1] if q_options.preview_mode else sigmas
                ),  # use only first sigma in preview mode
                src_ids,
                levels,
                products,
                services,
                categories,
                tags,
                False, # no paths
                False, # no count, create queries
                wait=True)

            if q_options.preview_mode:
                # preview mode, return sync
                total_hits, docs = await _preview_query(
                    sess, queries[0].q, q_options, index=op.index
                )
                return JSONResponse(
                    JSendResponse.success(
                        req_id=req_id, data={"total_hits": total_hits, "docs": docs}
                    )
                )

            # run a background task (will batch queries, spawn workers, gather results)
            # enqueue task for worker dispatch (always enqueue unless preview)
            task_msg = {
                "task_type": TASK_TYPE_QUERY,
                "operation_id": operation_id,
                "user_id": user_id,
                "ws_id": ws_id,
                "req_id": req_id,
                "params": {
                    "queries": [q.model_dump(exclude_none=True) for q in queries],
                    "q_options": q_options.model_dump(exclude_none=True),
                },
            }
            await GulpRedis.get_instance().task_enqueue(task_msg)

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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession = await GulpUserSession.check_token(sess, token)
            d = await s.user.get_query_history()
            return JSONResponse(JSendResponse.success(req_id, data=d))
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
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)
    ] = None,
    group_by: Annotated[
        Optional[str],
        Query(description="group by field (i.e. `event.code`), default=no grouping"),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # enforce operation_id in filter
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

NOTE: if there is no data in any operation, this function returns an empty array.
""",
)
async def query_operations(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    # TODO: make it return pending and send the result to websocket ?
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s: GulpUserSession = await GulpUserSession.check_token(sess, token, obj=op)
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
            await GulpServer.get_instance().spawn_worker_task(
                GulpOpenSearch.datastream_update_source_field_types_by_src_wrapper,
                None,  # sess=None to create a temporary one (a worker can't use the current one)
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
        d.pop("_highlight", None)
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

    file_path = tempfile.mkstemp(suffix=".json", prefix="gulp_export_%s" % (req_id))[1]
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
- `q_options.preview_mode` is ignored and set to `False`.
- if unset, `q_options.fields` is set to `*` to export all fields.
""",
)
async def query_gulp_export_json_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)
    ] = None,
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
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

    # ignore preview mode, highlight, note creation for export
    q_options.preview_mode = False
    q_options.create_notes = False
    q_options.highlight_results = False

    if not q_options.fields:
        # export all fields if not specified
        q_options.fields = "*"

    # enforce operation_id in filter
    flt.operation_ids = [operation_id]

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            _ = await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index

            # convert gulp query to opensearch dsl
            dsl: dict = flt.to_opensearch_dsl()

            # execute export operation in worker process (non-blocking)
            file_path = await GulpServer.get_instance().spawn_worker_task(
                _export_json_internal,
                index,
                operation_id,
                dsl,
                wait=True,
                req_id=req_id,
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
                        "cleanup file %s after response sent", file_path
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
