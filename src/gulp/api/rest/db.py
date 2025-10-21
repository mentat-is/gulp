"""
Database and Data Management REST API endpoints for Gulp.

This module provides endpoints to manage the underlying OpenSearch and PostgreSQL databases used by Gulp,
including operations like creating/deleting indexes, resetting the collaboration database, and rebasing indexes.

The endpoints are organized into several main operations:
- OpenSearch datastream operations (create, delete, list)
- PostgreSQL collaboration database management
- Complete system reset
- Index rebasing operations

Most operations require admin-level permissions, as they can potentially delete or modify significant amounts of data.
"""

import asyncio
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from llvmlite.tests.test_ir import flt
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
import muty.log
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpUpdateDocumentsStats,
    GulpRequestStats,
    RequestCanceledError,
    RequestStatsType,
)
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import WSDATA_REBASE_DONE, GulpWsSharedQueue
from gulp.config import GulpConfig
from gulp.process import GulpProcess
from sqlalchemy.ext.asyncio import AsyncSession

router: APIRouter = APIRouter()


async def db_reset() -> None:
    """
    resets the collab database (deletes everything, including operations (deleting their data on OpenSearch))

    NOTE: must be called on the main process!

    """
    # delete all operations first
    async with GulpCollab.get_instance().session() as sess:
        try:
            # enumerate all operations
            ops = await GulpOperation.get_by_filter(sess, throw_if_not_found=False)
            for op in ops:
                op: GulpOperation
                MutyLogger.get_instance().debug(
                    "found operation: %s, index=%s" % (op.id, op.index)
                )
                MutyLogger.get_instance().info(
                    "deleting data for operation %s" % (op.id)
                )

                # delete the whole datastream
                await GulpOpenSearch.get_instance().datastream_delete(op.index)

                # delete the operation itself
                await op.delete(sess)
        except Exception as ex:
            MutyLogger.get_instance().exception("cannot delete data on opensearch!")
            await sess.rollback()
            raise

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
    flt: GulpQueryFilter = kwargs["flt"]
    errors: list[str] = kwargs["errors"]
    stats: GulpRequestStats = kwargs["stats"]
    ws_id: str = kwargs["ws_id"]
    await stats.update_updatedocuments_stats(
        sess,
        user_id=stats.user_id,
        ws_id=ws_id,
        total_hits=total,
        updated=current,
        flt=flt,
        errors=errors,
        last=last
    )


async def _rebase_by_query_internal(
    req_id: str,
    ws_id: str,
    user_id: str,
    operation_id: str,
    index: str,
    offset_msec: int,
    flt: GulpQueryFilter,
    script: str,
):
    """
    runs in a worker process to rebase the index using update_by_query.
    """
    stats: GulpRequestStats = None
    total_hits: int = 0
    canceled: bool = False
    errors: list[str] = []
    status = GulpRequestStatus.DONE
    cb_context = {
        "total_updated": 0,
        "flt": flt,
        "errors": errors,
        "ws_id": ws_id,
    }
    try:
        # rebase
        async with GulpCollab.get_instance().session() as sess:
            try:
                stats, _ = await GulpRequestStats.create_or_get_existing(
                    sess,
                    req_id,
                    user_id,
                    operation_id,
                    req_type=RequestStatsType.REQUEST_TYPE_REBASE,
                    ws_id=ws_id,
                    data=p.model_dump(exclude_none=True),
                )
                cb_context["stats"] = stats

                (
                    total_hits,
                    _,
                    errors,
                ) = await GulpOpenSearch.get_instance().opensearch_rebase_by_query(
                    sess,
                    index,
                    offset_msec,
                    req_id,
                    flt=flt,
                    script=script,
                    callback=_rebase_callback,
                    cb_context=cb_context,
                )
            except Exception as ex:
                await sess.rollback()
                raise
            finally:
                # send a WSDATA_REBASE_DONE packet to the websocket
                GulpWsSharedQueue.get_instance().put(
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

- `WSDATA_REBASE_DONE.payload`: `GulpRequestStats` when rebase is done, broadcasted to all connected websockets
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
            example=3600000,
            description="""offset in milliseconds to add to document `@timestamp` and `gulp.timestamp`.

- to subtract, use a negative offset.
""",
        ),
    ],
    script: Annotated[
        str,
        Body(
            description="""
optional custom [painless script](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-guide.html) rebase script to run on the documents.

- the default script rebases `@timestamp` and `gulp.timestamp` and is implemented in `gulp/api/opensearch_api::rebase_by_query`.
"""
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    """
    rebases documents in-place on the same index using update_by_query, shifting timestamps.

    Args:
        token (str): API token with ingest permission
        operation_id (str): operation to rebase
        offset_msec (int): offset in milliseconds to apply
        script (str, optional): optional painless script to run on the documents
        flt (GulpQueryFilter, optional): filter for documents to update
        ws_id (str, optional): websocket id to send progress updates to
        req_id (str, optional): request id
    Returns:
        JSONResponse: JSend-style response with total_matches, updated, errors
    Throws:
        JSendException: on error
    """
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s = await GulpUserSession.check_token(
                    sess, token, obj=op, permission=GulpUserPermission.INGEST
                )
                user_id = s.user.id
                index = op.index
            except Exception as ex:
                await sess.rollback()
                raise

            # offload to a worker process and return pending
            await GulpRestServer.get_instance().spawn_worker_task(
                _rebase_by_query_internal,
                req_id,
                ws_id,
                user_id,
                operation_id,
                index,
                offset_msec,
                flt,
                script,
            )
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
            example="test_operation",
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
            try:
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
                        await op.delete(
                            sess, ws_id=None, user_id=s.user.id, req_id=req_id
                        )
                    else:
                        MutyLogger.get_instance().warning(
                            f"operation with index={index} not found, skipping deletion..."
                        )

                # delete the datastream (deletes the corresponding index and template)
                await GulpOpenSearch.get_instance().datastream_delete(
                    ds=index, throw_on_error=True
                )
                return JSONResponse(
                    JSendResponse.success(
                        req_id=req_id,
                        data={"index": index, "operation_id": op.id if op else None},
                    )
                )
            except Exception as ex:
                await sess.rollback()
                raise
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
            try:
                await GulpUserSession.check_token(
                    sess, token, permission=GulpUserPermission.ADMIN
                )

                l = await GulpOpenSearch.get_instance().datastream_list()
                return JSONResponse(JSendResponse.success(req_id=req_id, data=l))
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
