import asyncio
from copy import deepcopy
from typing import Any

import muty.log
import muty.time
import orjson
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.note import GulpNote
from gulp.api.collab.stats import (
    GulpQueryStats,
    GulpRequestStats,
    RequestCanceledError,
    RequestStatsType,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab.user import GulpUser, GulpUserDataQueryHistoryEntry
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpQuery, GulpQueryParameters
from gulp.api.redis_api import GulpRedis
from gulp.api.server.ingest import GulpIngestionStats
from gulp.api.server.query import run_query
from gulp.api.ws_api import (
    WSDATA_QUERY_GROUP_MATCH,
    GulpQueryGroupMatchPacket,
    GulpRedisBroker,
)
from gulp.config import GulpConfig
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters


async def _process_query_batch(
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
) -> bool:
    """
    processes a batch of queries in the main process by spawning workers for each query.
    handles query history, batching, result collection, and group matching.
    
    returns:
        bool: true if the request was canceled, false otherwise.
    """
    stats: GulpRequestStats = None
    batching_step_reached: bool = False
    req_canceled: bool = False
    
    try:
        # create or get existing stats
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

        # add query history entries (only if group is not set)
        if not q_options.group:
            history: list[GulpUserDataQueryHistoryEntry] = []
            for gq in queries:
                q_opts = deepcopy(q_options)
                q_opts.name = gq.q_name
                h: GulpUserDataQueryHistoryEntry = GulpUserDataQueryHistoryEntry(
                    q=gq.q,
                    timestamp_msec=muty.time.now_msec(),
                    external=True if plugin else False,
                    q_options=q_opts,
                    plugin=plugin,
                    plugin_params=plugin_params,
                    sigma_yml=gq.sigma_yml,
                )
                history.append(h)
            
            if history:
                u: GulpUser = await GulpUser.get_by_id(sess, user_id)
                await u.add_query_history_entry_batch(sess, history)

        # batch queries and spawn workers for each
        batch_size: int = len(queries)
        if len(queries) > GulpConfig.get_instance().concurrency_num_tasks():
            batch_size = GulpConfig.get_instance().concurrency_num_tasks()

        MutyLogger.get_instance().debug(
            "processing %d queries in batches of %d ...",
            len(queries),
            batch_size,
        )

        matched_queries: dict[str, int] = {}
        for i in range(0, len(queries), batch_size):
            # run one batch
            batch: list[GulpQuery] = queries[i : i + batch_size]
            coros = []
            for gq in batch:
                q_opts = deepcopy(q_options)
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
                # spawn worker for this query
                coros.append(
                    GulpProcess.get_instance().process_pool.apply(
                        run_query, kwds=run_query_args
                    )
                )

            # gather results for this batch
            batching_step_reached = True
            MutyLogger.get_instance().debug(
                "gathering results for %d queries ...", len(coros)
            )
            batch_res = await asyncio.gather(*coros, return_exceptions=True)

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
                        req_canceled = True
                elif isinstance(r, Exception):
                    # we have an error
                    if "RequestCanceledError" in str(r):
                        req_canceled = True

                    err = muty.log.exception_to_string(r)
                    if err not in errors:
                        errors.append(err)

            if not plugin:
                # for non-external queries, update request stats
                await stats.update_query_stats(
                    sess,
                    user_id=user_id,
                    ws_id=ws_id,
                    hits=total_hits,
                    inc_completed=len(coros),
                    errors=errors,
                )
            
            if req_canceled:
                MutyLogger.get_instance().warning(
                    "request canceled, stop processing batches!"
                )
                break

        # handle group matching
        if not q_options.group:
            MutyLogger.get_instance().debug("no query group set, we're done!")
            return req_canceled

        num_queries_in_group = len(queries)
        if len(matched_queries) < num_queries_in_group:
            MutyLogger.get_instance().warning(
                "query group set but no query group match (%d/%d matched)",
                len(matched_queries),
                num_queries_in_group,
            )
            return req_canceled
        
        MutyLogger.get_instance().info(
            "we have a query_group match for group %s !", q_options.group
        )

        # retag notes with group name
        query_names = list(matched_queries.keys())
        await GulpNote.bulk_update_for_group_match(
            sess, operation_id, query_names, q_options
        )

        # send group match to websocket
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
        )
        
    except Exception as ex:
        if stats and not batching_step_reached:
            # set stats as failed on error
            await stats.set_finished(
                sess,
                GulpRequestStatus.FAILED,
                user_id=user_id,
                ws_id=ws_id,
                errors=[muty.log.exception_to_string(ex)],
            )
        raise
    
    return req_canceled


async def run_query_task(t: dict) -> None:
    """
    runs in the MAIN PROCESS (not a worker) and executes a queued query task.

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

    This function batches queries, spawns workers for each query via _run_query,
    collects results, and handles group matching logic.
    """
    MutyLogger.get_instance().debug("run_query_task, t=%s", t)
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

        # run the batching and group-matching logic here in the main process
        async with GulpCollab.get_instance().session() as sess:
            await _process_query_batch(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                operation_id=operation_id,
                ws_id=ws_id,
                queries=q_models,
                num_total_queries=len(q_models),
                q_options=q_options_model,
                index=index,
                plugin=plugin,
                plugin_params=plugin_params_model,
            )
    except Exception as ex:
        MutyLogger.get_instance().exception("error in run_query_task: %s", ex)


async def run_rebase_task(t: dict) -> None:
    """
    runs in the MAIN PROCESS and spawns a worker for a queued rebase task.

    Expected task dict:
      {
        "task_type": "rebase",
        "operation_id": <str>,
        "user_id": <str>,
        "ws_id": <str>,
        "req_id": <str>,
        "params": {
            "index": <str>,
            "offset_msec": <int>,
            "flt": <dict|null>,
            "script": <str|null>
         }
      }

    This function runs in the main process, reconstructs the filter model and
    spawns a worker task to actually perform the rebase using
    `_rebase_by_query_internal` defined in `gulp.api.server.db`.
    """
    MutyLogger.get_instance().debug("run_rebase_task, t=%s", t)
    try:
        params: dict = t.get("params", {})
        user_id: str = t.get("user_id")
        req_id: str = t.get("req_id")
        operation_id: str = t.get("operation_id")
        ws_id: str = t.get("ws_id")

        index = params.get("index")
        offset_msec = params.get("offset_msec")
        flt = params.get("flt")
        script = params.get("script")

        # rebuild filter model if provided
        from gulp.api.opensearch.filters import GulpQueryFilter
        from gulp.api.server.db import _rebase_by_query_internal
        from gulp.api.server_api import GulpServer

        flt_model = None
        if flt:
            # if dict, validate into model, otherwise assume already a model
            if isinstance(flt, dict):
                flt_model = GulpQueryFilter.model_validate(flt)
            else:
                flt_model = flt

        # spawn worker to run the actual rebase (non-blocking)
        await GulpServer.get_instance().spawn_worker_task(
            _rebase_by_query_internal,
            req_id,
            ws_id,
            user_id,
            operation_id,
            index,
            offset_msec,
            flt_model,
            script,
            task_name=f"rebase_{req_id}",
        )

    except Exception as ex:
        MutyLogger.get_instance().exception("error in run_rebase_task: %s", ex)
