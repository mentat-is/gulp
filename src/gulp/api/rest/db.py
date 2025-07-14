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

import orjson
from typing import Annotated

import muty.log
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.operation import GulpOperation
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
from gulp.api.rest.test_values import TEST_OPERATION_ID
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import WSDATA_REBASE_DONE, GulpRebaseDonePacket, GulpWsSharedQueue
from gulp.config import GulpConfig
from gulp.process import GulpProcess

router: APIRouter = APIRouter()


async def _delete_operations(
    user_id: str, operation_id: str = None
) -> None:
    """
    Deletes (all) operations in the collaboration database

    Args:
        user_id (str): The user ID for which to clear operations.
        operation_id (str, optional): If specified, only the operation with this ID will be deleted.
    """
    MutyLogger.get_instance().warning(
        "deleting ALL operations on collab (except operation_id=%s)" % (operation_id))
    
    async with GulpCollab.get_instance().session() as sess:
        # enumerate all operations
        ops = await GulpOperation.get_by_filter(
            sess, user_id=user_id, throw_if_not_found=False
        )
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

            MutyLogger.get_instance().info(
                "deleting data for operation %s" % (op.id)
            )
            # delete the whole datastream
            await GulpOpenSearch.get_instance().datastream_delete(op.index)

            # delete the operation itself
            await op.delete(sess)


async def db_reset(
    user_id: str = None,
    operation_id: str = None,
    force_recreate_db: bool = False,
) -> None:
    """
    resets the collab database

    Args:
        user_id (str, optional): user id to use to delete the data. If None, "admin" will be used.
        operation_id (str, optional): if set, a new operation with this id will be created after reset.
        force_recreate_db (bool, optional): if True, the collab database will be recreated even if it exists. Defaults to False.

    """
    MutyLogger.get_instance().debug(
        "db_reset called with params: user_id=%s, operation_id=%s, force_recreate_db=%r"
        % (user_id, operation_id, force_recreate_db)
    )

    # check if the database exists
    url = GulpConfig.get_instance().postgres_url()
    collab = GulpCollab.get_instance()
    try:
        await collab.init(main_process=True)
    except SchemaMismatch as ex:
        MutyLogger.get_instance().warning(
            "collab database schema mismatch, resetting it: %s" % (ex)
        )
        force_recreate_db = True
    exists = await collab.db_exists(url)

    if user_id is None:
        # if user_id is not specified, use "admin"
        user_id = "admin"

    if not exists or force_recreate_db:
        MutyLogger.get_instance().warning(
            "collab database does not exist/must be recreated, creating it (exist=%r, force_recreate_db=%r) ..."
            % (exists, force_recreate_db)
        )

        # clear all existing operations
        try:
            await _delete_operations(user_id)
        except:
            # will fail if the collab database does not exist
            pass

        # reset
        await collab.init(main_process=True, force_recreate=True)
        collab = GulpCollab.get_instance()
        await collab.create_default_users()
        await collab.create_default_glyphs()

    # collab database exists, delete all data for all or just for the specific operation
    MutyLogger.get_instance().warning("collab database exists !")
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
            user_id=s.user_id,
            operation_id=TEST_OPERATION_ID if create_default_operation else None,
            force_recreate_db=True,
        )
        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()

        return JSONResponse(JSendResponse.success(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def _rebase_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    dest_index: str,
    offset_msec: int,
    flt: GulpQueryFilter,
    rebase_script: str,
):
    """
    runs in a worker process to rebase the index.
    """
    try:
        # get existing index datastream
        template = await GulpOpenSearch.get_instance().index_template_get(index)

        # create the destination datastream, applying the same template
        await GulpOpenSearch.get_instance().datastream_create_from_raw_dict(
            dest_index, template
        )

        # rebase
        res = await GulpOpenSearch.get_instance().rebase(
            index,
            dest_index=dest_index,
            offset_msec=offset_msec,
            flt=flt,
            rebase_script=rebase_script,
        )

        # the operation object must now point to the new index
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            d = {
                "index": dest_index,
            }
            await op.update(
                sess,
                d,
                ws_id=None,  # do not propagate on the websocket
                req_id=req_id,
                user_id=user_id,
            )

    except Exception as ex:
        # signal failure on the websocket
        MutyLogger.get_instance().exception(ex)
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            type=WSDATA_REBASE_DONE,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
            data=GulpRebaseDonePacket(
                operation_id=operation_id,
                src_index=index,
                dest_index=dest_index,
                status=GulpRequestStatus.FAILED,
                result=muty.log.exception_to_string(ex),
            ),
        )
        return

    # done
    MutyLogger.get_instance().debug(
        "rebase done, result=%s" % (orjson.dumps(res, option=orjson.OPT_INDENT_2).decode())
    )
    # signal the websocket
    wsq = GulpWsSharedQueue.get_instance()
    await wsq.put(
        type=WSDATA_REBASE_DONE,
        ws_id=ws_id,
        user_id=user_id,
        req_id=req_id,
        operation_id=operation_id,
        data=GulpRebaseDonePacket(
            operation_id=operation_id,
            src_index=index,
            dest_index=dest_index,
            status=GulpRequestStatus.DONE,
            result=res,
        ),
    )


@router.post(
    "/opensearch_rebase_index",
    response_model=JSendResponse,
    tags=["db"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "pending",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                    }
                }
            }
        }
    },
    summary="rebases operation's index to a different time.",
    description="""
rebases `operation_id.index` and creates a new `dest_index` with rebased `@timestamp` + `offset`, then set the operation's index to `dest_index` on success.

- `token` needs `ingest` permission.
- `flt` may be used to filter the documents to rebase.
- rebase happens in the background: when it is done, a `WSDATA_REBASE_DONE` event is sent to the `ws_id` websocket.
""",
)
async def opensearch_rebase_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    dest_index: Annotated[
        str,
        Query(
            description="name of the destination index, will be reset or created if not exists.",
            example="new_index",
        ),
    ],
    offset_msec: Annotated[
        int,
        Query(
            example=3600000,
            description="""offset in milliseconds to add to document `@timestamp` and `gulp.timestamp`.

- to subtract, use a negative offset.
""",
        ),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[str, Depends(APIDependencies.param_query_flt_optional)] = None,
    rebase_script: Annotated[
        str,
        Body(
            description="""
optional custom rebase script to run on the documents.

- must be a [painless script](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-guide.html).
- the script receives as input a single parameter `offset_nsec`, the offset in `nanoseconds from the unix epoch` to be applied to the fields during the rebase operation.
- the example script is the one applied by default (rebases `@timestamp` and `gulp.timestamp`).
""",
            examples=[
                """if (ctx._source['@timestamp'] != 0) {
    def ts = ZonedDateTime.parse(ctx._source['@timestamp']);
    def new_ts = ts.plusNanos(params.offset_nsec);
    ctx._source['@timestamp'] = new_ts.toString();
    ctx._source["gulp.timestamp"] += params.offset_nsec;
}"""
            ],
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id and index
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            user_id = s.user_id
            index = op.index

        if index == dest_index:
            raise JSendException(req_id=req_id) from Exception(
                "index and dest_index must be different! (index=%s, dest_index=%s)"
                % (index, dest_index)
            )

        # spawn a task which runs the rebase in a worker process
        kwds = dict(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=index,
            dest_index=dest_index,
            offset_msec=offset_msec,
            flt=flt,
            rebase_script=rebase_script,
        )

        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _rebase_internal, kwds=kwds
            )

        await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))

        # and return pending
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
