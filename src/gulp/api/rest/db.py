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

import json
from typing import Annotated, Union

import muty.file
import muty.log
import muty.uploadfile
from fastapi import APIRouter, Body, Depends, File, Query, UploadFile
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
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest.test_values import TEST_INDEX
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import GulpRebaseDonePacket, GulpWsQueueDataType, GulpWsSharedQueue
from gulp.process import GulpProcess
from gulp.structs import ObjectAlreadyExists

router: APIRouter = APIRouter()


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
                    user_id_is_admin=True,
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


async def _recreate_index_internal(
    index: str, restart_processes: bool, index_template: str = None
) -> None:
    # reset data
    await GulpOpenSearch.get_instance().reinit()
    await GulpOpenSearch.get_instance().datastream_create(
        index, index_template=index_template
    )

    if restart_processes:
        # restart the process pool
        await GulpProcess.get_instance().init_gulp_process()


@router.post(
    "/opensearch_create_index",
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
                        "data": {"index": "testidx"},
                    }
                }
            }
        }
    },
    summary="creates or recreates the opensearch datastream `index`, including its backing index.",
    description="""
> **WARNING: ANY EXISTING DOCUMENT WILL BE ERASED !**

- `token` needs `admin` permission.
- if `index` exists, it is **deleted** and recreated unless `fail_if_exists` is set.
- if `index_template` is provided, it is used to create the index, otherwise the default template is used.
""",
)
async def opensearch_create_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    index_template: Annotated[
        Union[UploadFile, str],
        File(
            description="optional custom index template, for advanced usage only: see [here](https://opensearch.org/docs/latest/im-plugin/index-templates/)",
        ),
    ] = None,
    fail_if_exists: Annotated[
        bool,
        Query(
            description="if set, the API fails if the index already exists (default: false).",
        ),
    ] = False,
    restart_processes: Annotated[
        bool,
        Query(
            description="if set, the process pool is restarted as well (default: false).",
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params.pop("index_template", None)
    ServerUtils.dump_params(params)
    f: str = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            # op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )
            if fail_if_exists:
                # check if index exists
                if await GulpOpenSearch.get_instance().datastream_exists(index):
                    raise ObjectAlreadyExists(f"index {index} already exists")

            if index_template and isinstance(index_template, UploadFile):
                # get index template from file
                f = await muty.uploadfile.to_path(index_template)

            await _recreate_index_internal(index, restart_processes, index_template=f)
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data={"index": index})
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    finally:
        if f is not None:
            await muty.file.delete_file_or_dir_async(f)


async def postgres_reset_collab_internal(reinit: bool = False) -> None:
    """
    resets the collab database.

    Args:
        reinit: if true, the whole collab database is dropped and recreated and initialized with default data (users, groups, etc. BUT not operation).
                either, just collab objects and stats are reset.

    """
    MutyLogger.get_instance().warning("resetting collab!")
    if reinit:
        MutyLogger.get_instance().warning("drop and recreate whole collab database...")
        # reinit whole collab
        collab = GulpCollab.get_instance()
        await collab.init(main_process=True, force_recreate=True)
        await collab.create_default_users()
        await collab.create_default_data()
    else:
        MutyLogger.get_instance().warning("collab: leaving operation table untouched!")
        collab = GulpCollab.get_instance()
        await collab.init(main_process=True)

        # do not touch these tables
        await collab.clear_tables(
            exclude=[
                "operation",
                "user",
                "user_associations",
                "user_group",
                "source",
                "source_fields",
                "context",
            ]
        )
        await collab.create_default_data()


@router.post(
    "/postgres_reset_collab",
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
    summary="(re)creates gulp collab database.",
    description="""
> **WARNING: ALL COLLABORATION AND INDEXED DATA WILL BE ERASED IF `full_reset` IS SET**

- `token` needs to have `admin` permission.
- if `full_reset` is set, default `users` and `user groups` are recreated, a new operation must be created then using the `operation_create` API.
- if `full_reset` is not set, the following tables are left untouched: 'operation', 'user', 'user_groups', 'user_associations', 'context', 'source', 'source_fields'.
""",
)
async def postgres_reset_collab_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    full_reset: Annotated[
        bool,
        Query(
            description="if set, the whole collab database is cleared and also operation data on gulp's OpenSearch is DELETED as well.",
        ),
    ] = False,
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
            user_id = s.user_id
            if full_reset:
                # check if we have operations
                ops = await GulpOperation.get_by_filter(
                    sess,
                    throw_if_not_found=False,
                    user_id=user_id,
                    user_id_is_admin=True,  # user can't be other than admin here
                )
                if ops:
                    # delete all indexes
                    for op in ops:
                        # delete index data
                        MutyLogger.get_instance().debug(
                            "operation=%s, deleting index=%s ..." % (op.id, op.index)
                        )
                        await GulpOpenSearch.get_instance().datastream_delete(
                            ds=op.index, throw_on_error=False
                        )

        if full_reset:
            # reinit whole collab
            await postgres_reset_collab_internal(reinit=True)
        else:
            # do not delete operations
            await postgres_reset_collab_internal(reinit=False)

        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()

        return JSONResponse(JSendResponse.success(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    summary="reset whole gulp's OpenSearch and PostgreSQL storages.",
    description="""
> **WARNING: ALL COLLABORATION AND INDEXED DATA WILL BE ERASED AND DEFAULT USERS/DATA RECREATED ON THE COLLAB DATABASE**

- `token` needs to have `admin` permission.
""",
)
async def gulp_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        # reset collab
        await postgres_reset_collab_handler(
            token, full_reset=True, restart_processes=False, req_id=req_id
        )

        # create default index and operation
        collab = GulpCollab.get_instance()

        # recreate gulp test index
        await _recreate_index_internal(TEST_INDEX, restart_processes=True)

        # recreate default operation
        await collab.create_default_operation()
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
            op: GulpOperation = await GulpOperation.get_by_id(
                sess, operation_id
            )
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
        GulpWsSharedQueue.get_instance().put(
            type=GulpWsQueueDataType.REBASE_DONE,
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
        "rebase done, result=%s" % (json.dumps(res, indent=2))
    )
    # signal the websocket
    GulpWsSharedQueue.get_instance().put(
        type=GulpWsQueueDataType.REBASE_DONE,
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
- rebase happens in the background: when it is done, a `GulpWsQueueDataType.REBASE_DONE` event is sent to the `ws_id` websocket.
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
