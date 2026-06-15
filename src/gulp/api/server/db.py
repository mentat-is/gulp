"""
Database and Data Management REST API endpoints for Gulp.

This module provides endpoints to manage the underlying OpenSearch and PostgreSQL databases used by Gulp,
including operations like creating/deleting indexes, resetting the collaboration database, and rebasing indexes.

The endpoints are organized into several main operations:
- OpenSearch datastream operations (create, delete, list)
- PostgreSQL collaboration database management
- Index rebasing operations

Most operations require admin-level permissions, as they can potentially delete or modify significant amounts of data.
"""

from typing import Annotated

import muty.log
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
import orjson
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpRequestStats,
    GulpUpdateDocumentsStats,
    RequestCanceledError,
    RequestStatsType,
)
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis, TaskQueueFullError
from gulp.api.s3_api import GulpS3
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import TASK_TYPE_REBASE, APIDependencies
from gulp.api.server_api import GulpServer
from gulp.api.ws_api import WSDATA_REBASE_DONE, GulpRedisBroker
from gulp.structs import GulpInternalEventsManager

router: APIRouter = APIRouter()


async def db_reset() -> None:
    """
    resets the collab database (deletes everything, including operations (deleting their data on OpenSearch and files on storage, if any))

    NOTE: must be called on the main process!

    """
    # delete all operations first
    async with GulpCollab.get_instance().session() as sess:
        # enumerate all operations
        ops = await GulpOperation.get_by_filter(sess, throw_if_not_found=False)
        for op in ops:
            op: GulpOperation
            MutyLogger.get_instance().debug(
                "found operation: %s, index=%s" % (op.id, op.index)
            )
            MutyLogger.get_instance().info("deleting data for operation %s" % (op.id))

            # delete the whole datastream
            await GulpOpenSearch.get_instance().datastream_delete(op.index)

            # delete files on storage if any
            try:
                await GulpS3.get_instance().delete_by_tags({"operation_id": op.id})
            except Exception as ex:
                MutyLogger.get_instance().exception(ex)

            # delete the operation itself
            await op.delete(sess)

    # reset
    # TODO: try to handle worker processes termination gracefully
    MutyLogger.get_instance().warning(
        "resetting collab database, all data will be lost!"
    )
    await GulpCollab.get_instance().init(main_process=True, force_recreate=True)


async def _rebase_callback(
    sess: AsyncSession,
    total: int,
    current: int,
    req_id: str,
    last: bool = False,
    **kwargs,
):
    """
    GulpProgressCallback to report progress during rebase.
    """
    cb_context = kwargs["cb_context"]
    flt: GulpQueryFilter = cb_context["flt"]
    errors: list[str] = cb_context["errors"]
    stats: GulpRequestStats = cb_context["stats"]
    ws_id: str = cb_context["ws_id"]
    chunk_num: int = cb_context.get("stats_update_chunk_num", 0)
    cb_context["stats_update_chunk_num"] = chunk_num + 1
    # print("******************** rebase callback, total=%d, current=%d, last=%s, errors=%s, status=%s" % (total, current, last, errors, stats.status if stats else None))
    await stats.update_updatedocuments_stats(
        sess,
        user_id=stats.user_id,
        ws_id=ws_id,
        total_hits=total,
        updated=current,
        flt=flt,
        errors=errors,
        last=last,
        update_key=f"rebase:{req_id}:{chunk_num}:{last}",
    )


async def run_rebase_task(t: dict) -> bool:
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
            "fields": <list[str]|null>
         }
      }

    This function runs in the main process, reconstructs the filter model and
    spawns a worker task to actually perform the rebase
    """
    MutyLogger.get_instance().debug("run_rebase_task, t=%s", t)
    try:
        params: dict = t.get("params", {})
        user_id: str = t.get("user_id")
        req_id: str = t.get("req_id")
        fields: list[str] = params.get("fields")
        operation_id: str = t.get("operation_id")
        ws_id: str = t.get("ws_id")

        index = params.get("index")
        offset_msec = params.get("offset_msec")
        flt = params.get("flt")

        # rebuild filter model if provided
        from gulp.api.opensearch.filters import GulpQueryFilter
        from gulp.api.server_api import GulpServer

        flt_model = None
        if flt:
            # if dict, validate into model, otherwise assume already a model
            if isinstance(flt, dict):
                flt_model = GulpQueryFilter.model_validate(flt)
            else:
                flt_model = flt

        # wait for the actual rebase to finish so the queue can ack only on success
        await GulpServer.get_instance().spawn_worker_task(
            _rebase_by_query_internal,
            req_id,
            ws_id,
            user_id,
            operation_id,
            index,
            offset_msec,
            flt_model,
            fields,
            task_name=f"rebase_{req_id}",
            wait=True,
        )
        return True

    except Exception as ex:
        MutyLogger.get_instance().exception(
            "***ERROR*** in run_rebase_task, task=%s", orjson.dumps(t, option=orjson.OPT_INDENT_2).decode()
        )
        return False

async def _rebase_by_query_internal(
    req_id: str,
    ws_id: str,
    user_id: str,
    operation_id: str,
    index: str,
    offset_msec: int,
    flt: GulpQueryFilter,
    fields: list[str]
):
    """
    runs in a worker process to rebase the index using update_by_query.
    """
    stats: GulpRequestStats = None
    total_hits: int = 0
    errors: list[str] = []
    total_hits: int = 0
    total_updated: int = 0
    errors: list[str] = []
    cb_context = {
        "flt": flt,
        "errors": errors,
        "ws_id": ws_id,
    }
    try:
        # rebase
        async with GulpCollab.get_instance().session() as sess:
            try:
                stats, stats_created = await GulpRequestStats.create_or_get_existing(
                    sess,
                    req_id,
                    user_id,
                    operation_id,
                    req_type=RequestStatsType.REQUEST_TYPE_REBASE,
                    ws_id=ws_id,
                    data=GulpUpdateDocumentsStats(flt=flt).model_dump(
                        exclude_none=True
                    ),
                )
                if (
                    stats
                    and not stats_created
                    and GulpRequestStats.is_terminal_status(stats.status)
                ):
                    MutyLogger.get_instance().warning(
                        "rebase request %s already terminal with status=%s, skipping replay",
                        req_id,
                        stats.status,
                    )
                    total_updated = -1
                    return
                cb_context["stats"] = stats

                (
                    total_hits,
                    total_updated,
                    errors,
                ) = await GulpOpenSearch.get_instance().opensearch_rebase_by_query(
                    sess,
                    index,
                    offset_msec,
                    req_id,
                    flt=flt,
                    callback=_rebase_callback,
                    fields=fields,
                    cb_context=cb_context,
                )
            except Exception as ex:
                if stats and not total_hits:
                    if not isinstance(ex, RequestCanceledError):
                        # close the stats as failed
                        errors.append(muty.log.exception_to_string(ex))
                        await stats.set_finished(
                            sess,
                            status=GulpRequestStatus.FAILED,
                            errors=errors,
                            user_id=user_id,
                            ws_id=ws_id,
                        )
                raise
            finally:
                MutyLogger.get_instance().debug(
                    "****** rebase_by_query finished, total_hits=%d, total_updated=%d, errors=%s",
                    total_hits,
                    total_updated,
                    errors)
                if stats and total_updated >= 0:
                    # send a WSDATA_REBASE_DONE packet to the websocket
                    await GulpRedisBroker.get_instance().put(
                        WSDATA_REBASE_DONE,
                        user_id,
                        ws_id=ws_id,
                        operation_id=operation_id,
                        req_id=req_id,
                        d=stats.to_dict(exclude_none=True) if stats else None,
                    )

    except Exception as ex:
        MutyLogger.get_instance().exception(ex)
        errors.append(muty.log.exception_to_string(ex))
        return


@router.post(
    "/opensearch_rebase_by_query",
    tags=["db"],
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
                        "data": {"total_matches": 1234, "updated": 1234, "errors": []},
                    }
                }
            }
        }
    },
    summary="rebases documents specifying a time offset.",
    description="""
rebases documents `in-place` using `update_by_query`, shifting timestamps.

- `token` needs `ingest` permission.
- `flt` may be used to filter the documents to rebase.

### tracking progress

from the gulp's point of view, the rebase operation is an `enrichment`, so the flow of data on `ws_id` is the same:

- `WSDATA_STATS_CREATE.payload`: `GulpRequestStats`, data=`GulpUpdateDocumentsStats` (at start)
- `WSDATA_STATS_UPDATE.payload`: `GulpRequestStats`, data=updated `GulpUpdateDocumentsStats` (once every 1000 documents)

plus, in the end of rebase the final stats is broadcasted:

- `WSDATA_REBASE_DONE.payload`: `GulpRequestStats` when rebase is done (with at least one document updated), broadcasted to all connected websockets
""",
)
async def opensearch_rebase_by_query_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    offset_msec: Annotated[
        int,
        Query(
            examples={"default":{"value":3600000}},            
            description="""offset in milliseconds to add to document `@timestamp`, `gulp.timestamp` and `fields`.

- to subtract, use a negative offset.
""",
        ),
    ],
    fields: Annotated[list[str], 
                    Body(description="""optional list of extra fields to rebase other than `@timestamp` and `gulp.timestamp`.
                        - they must be of `date`, `integer` or `long` type.
                        - integer/long values are treated as epoch timestamps and the the conversion code heuristically infers seconds, milliseconds, microseconds, or nanoseconds before applying the offset.
                        - if not provided, only `@timestamp` and `gulp.timestamp` are rebased
                        """)]=None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            user_id = s.user.id
            index = op.index

            # enqueue rebase task to Redis for main process dispatcher
            task_msg = {
                "task_type": TASK_TYPE_REBASE,
                "operation_id": operation_id,
                "user_id": user_id,
                "ws_id": ws_id,
                "req_id": req_id,
                "params": {
                    "index": index,
                    "offset_msec": offset_msec,
                    "flt": (
                        flt.model_dump(exclude_none=True)
                        if hasattr(flt, "model_dump")
                        else flt
                    ),
                    "fields": fields,
                },
            }
            try:
                await GulpRedis.get_instance().task_enqueue(task_msg)
            except TaskQueueFullError as ex:
                return ServerUtils.task_queue_full_response("db", req_id, ex)
            return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/opensearch_delete_index",
    tags=["db"],
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
                            "index": "test_operation",
                            "operation_id": "test_operation",
                        },
                    }
                }
            }
        }
    },
    summary="deletes an opensearch datastream.",
    description="""
deletes the datastream `index`, including the backing index/es and the index template.

- **WARNING**: all data in the `index` will be deleted!
- if `delete_operation` is set, the corresponding operation on the collab database is deleted if it exists..
- `token` needs `admin` permission.
""",
)
async def opensearch_delete_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[
        str,
        Query(
            description="the OpenSearch index (usually equal to `operation_id`)",
            examples={"default":{"value":"test_operation"}},            
        ),
    ],
    delete_operation: Annotated[
        bool,
        Query(
            description="if set, the corresponding operation (if any) on the collab database is deleted as well (default: true)."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # we must be admin
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )
            user_id: str = s.user.id
            op: GulpOperation = None
            if delete_operation:
                # get operation
                op = await GulpOperation.get_first_by_filter(
                    sess,
                    GulpCollabFilter(index=[index]),
                    throw_if_not_found=False,
                    user_id=user_id,
                )
                if op:
                    # delete the operation on collab
                    await op.delete(sess, ws_id=None, user_id=s.user.id, req_id=req_id)
                else:
                    MutyLogger.get_instance().warning(
                        f"operation with index={index} not found, skipping deletion..."
                    )

            # delete the datastream (deletes the corresponding index and template)
            await GulpOpenSearch.get_instance().datastream_delete(
                ds=index, throw_on_error=True
            )
            await GulpInternalEventsManager.get_instance().dispatch_internal_event(
                GulpInternalEventsManager.EVENT_DELETE_OPERATION,
                data=dict(index=index),
                user_id=user_id,
                req_id=req_id,
            )

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"index": index, "operation_id": op.id if op else None},
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/opensearch_list_index",
    tags=["db"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734619572441,
                        "req_id": "test_req",
                        "data": [
                            {
                                "name": "new_index",
                                "count": 7,
                                "indexes": [
                                    {
                                        "index_name": ".ds-new_index-000001",
                                        "index_uuid": "qwrMdTrgRI6fdU_SsIxbzw",
                                    }
                                ],
                                "template": "new_index-template",
                            },
                            {
                                "name": "test_operation",
                                "count": 7,
                                "indexes": [
                                    {
                                        "index_name": ".ds-test_operation-000001",
                                        "index_uuid": "ZUuTB5KrSw6V-JVt9jtbcw",
                                    }
                                ],
                                "template": "test_operation-template",
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="lists available datastreams.",
    description="""
lists all the available datastreams and their backing indexes

- `token` needs `admin` permission.
""",
)
async def opensearch_list_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )

            l = await GulpOpenSearch.get_instance().datastream_list()
            return JSONResponse(JSendResponse.success(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/opensearch_refresh_index",
    tags=["db"],
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
                            "index": "test_operation",
                        },
                    }
                }
            }
        }
    },
    summary="refreshes an opensearch datastream/index.",
    description="""
refreshes the datastream/index `index`, making all operations performed since the last refresh available for search.

- `token` needs `ingest` permission.
""",
)
async def opensearch_refresh_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[
        str,
        Query(
            description="the OpenSearch index (usually equal to `operation_id`)",
            examples={"default":{"value":"test_operation"}},            
        ),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # we must have ingest permission
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.INGEST
            )

            # refresh the index
            await GulpOpenSearch.get_instance().index_refresh(index)

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"index": index},
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
