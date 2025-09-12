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
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats, RequestStatsType
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.config import GulpConfig
from gulp.process import GulpProcess

router: APIRouter = APIRouter()


async def _delete_operations(operation_id: str = None) -> None:
    """
    Deletes (all) operations in the collaboration database

    Args:
        operation_id (str, optional): If specified, only the operation with this ID will be deleted.
    """
    MutyLogger.get_instance().warning(
        "deleting ALL operations on collab (except operation_id=%s)" % (operation_id)
    )

    async with GulpCollab.get_instance().session() as sess:
        # enumerate all operations
        ops = await GulpOperation.get_by_filter(sess, throw_if_not_found=False)
        for op in ops:
            op: GulpOperation
            MutyLogger.get_instance().debug(
                "found operation: %s, index=%s" % (op.id, op.index)
            )

            # if operation_id is specified, only delete that operation
            if operation_id and op.id != operation_id:
                # skip this operation
                MutyLogger.get_instance().debug(
                    "skipping deleting operation_id=%s" % (op.id)
                )
                continue

            MutyLogger.get_instance().info("deleting data for operation %s" % (op.id))
            # delete the whole datastream
            await GulpOpenSearch.get_instance().datastream_delete(op.index)

            # delete the operation itself
            await op.delete(sess)


async def db_reset(
    operation_id: str = None,
    force_recreate_db: bool = False,
) -> None:
    """
    resets the collab database

    NOTE: must be called on the main process!

    Args:
        operation_id (str, optional): if set, a new operation with this id will be created after reset.
        force_recreate_db (bool, optional): if True, the collab database will be recreated even if it exists. Defaults to False.

    """
    MutyLogger.get_instance().debug(
        "db_reset called with params: user_id=%s, operation_id=%s, force_recreate_db=%r"
        % (user_id, operation_id, force_recreate_db)
    )

    # check if the database exists
    url = GulpConfig.get_instance().postgres_url()
    exists = await GulpCollab.db_exists(url)
    if exists:
        MutyLogger.get_instance().warning("collab database exists !")
        # clear all existing operations
        await _delete_operations(user_id)
        
    user_id = "admin"
    if not exists or force_recreate_db:
        MutyLogger.get_instance().warning(
            "collab database does not exist/must be recreated, creating it (exist=%r, force_recreate_db=%r) ..."
            % (exists, force_recreate_db)
        )


        # reset
        await GulpCollab.get_instance().init(main_process=True, force_recreate=True)
        await GulpCollab.get_instance().create_default_users()
        await GulpCollab.get_instance().create_default_glyphs()

    # collab database exists, delete all data for all or just for the specific operation
    if operation_id:
        MutyLogger.get_instance().debug(
            "db_reset done, creating operation=%s" % (operation_id)
        )
        await GulpOperation.create_wrapper(
            operation_id,
            user_id=user_id,
            set_default_grants=True,
            fail_if_exists=False,
        )


@router.post(
    "/gulp_reset",
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
                    }
                }
            }
        }
    },
    summary="reset gulp.",
    description="""
> **WARNING: THE WHOLE COLLAB DATABASE WILL BE DELETED AND RECREATED**

- `token` needs to have `admin` permission.
- use this only when the database is corrupted (or the structure needs to be updated) and/or you want to start from scratch.
""",
)
async def gulp_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    create_default_operation: Annotated[
        bool,
        Query(
            description='if set, a default operation named "test_operation" will be created.',
        ),
    ] = True,
    restart_processes: Annotated[
        bool,
        Query(
            description="if true, the process pool is restarted as well.",
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )

        # reset
        await db_reset(
            operation_id="test_operation" if create_default_operation else None,
            force_recreate_db=True,
        )
        if restart_processes:
            # restart the process pool by calling init in the main process (we are in the main process here)
            await GulpProcess.get_instance().init_gulp_process()

        return JSONResponse(JSendResponse.success(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def _rebase_by_query_internal(
    req_id: str,
    ws_id: str,
    user_id: str,
    index: str,
    offset_msec: int,
    flt: GulpQueryFilter,
    script: str,
):
    """
    runs in a worker process to rebase the index using update_by_query.
    """
    errors: list[str] = []
    updated: int = 0
    try:
        # rebase
        async with GulpCollab.get_instance().session() as sess:
            res = await GulpOpenSearch.get_instance().opensearch_rebase_by_query(
                sess, index, offset_msec, req_id, ws_id, user_id, flt, script
            )
            errors = res.get("errors", [])
            updated = res.get("updated", 0)
    except Exception as ex:
        MutyLogger.get_instance().exception(ex)
        errors.append(str(ex))
        return
    finally:
        # finalize the stats
        await GulpRequestStats.finalize(
            sess,
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            hits=updated,
            errors=errors,
        )


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
    summary="rebases documents in-place on the same index using update_by_query.",
    description="""
rebases documents in-place on the same index using update_by_query, shifting timestamps.

- `token` needs `ingest` permission.
- `flt` may be used to filter the documents to rebase.
- rebase happens in the background: progress updates are sent to the `ws_id` websocket.

### checking progress

- during rebase, GulpProgressPacket with `msg=ws_api.PROGRESS_REBASE` are sent on the websocket with data set to total_matches, updated, errors.
""",
)
async def opensearch_rebase_by_query_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
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
    flt: Annotated[str, Depends(APIDependencies.param_query_flt_optional)] = None,
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
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
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            user_id = s.user_id
            index = op.index

        async def _worker_coro():
            # create a stats, just to allow request canceling
            async with GulpCollab.get_instance().session() as sess:
                await GulpRequestStats.create_or_get(
                    sess=sess,
                    req_id=req_id,
                    user_id=user_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    object_data=None,  # uses default
                    stats_type=RequestStatsType.REQUEST_TYPE_REBASE,
                )

            # offload to a worker process withouyt waiting for it
            asyncio.create_task(
                GulpProcess.get_instance().process_pool.apply(
                    _rebase_by_query_internal,
                    kwds=dict(
                        req_id=req_id,
                        ws_id=ws_id,
                        user_id=user_id,
                        index=index,
                        offset_msec=offset_msec,
                        flt=flt,
                        script=script,
                    ),
                )
            )

        # spawn a background worker to perform the rebase and return pending
        await GulpRestServer.get_instance().spawn_bg_task(_worker_coro())
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
    index: Annotated[str, Depends(APIDependencies.param_index)],
    delete_operation: Annotated[
        bool,
        Query(
            description="if set, the corresponding operation (if any) on the collab database is deleted as well (default: true)."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # we must be admin
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )
            user_id: str = s.user_id
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
                    await op.delete(sess, ws_id=None, user_id=s.user_id, req_id=req_id)
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
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
