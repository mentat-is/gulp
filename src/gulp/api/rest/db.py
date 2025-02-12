import json
from typing import Annotated, Optional

import muty.file
import muty.log
import muty.uploadfile
from fastapi import APIRouter, Body, Depends, File, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import GulpRebaseDonePacket, GulpSharedWsQueue, GulpWsQueueDataType
from gulp.process import GulpProcess
from gulp.structs import ObjectNotFound

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
                        "data": {"index": "test_idx", "operation_id": "test_operation"},
                    }
                }
            }
        }
    },
    summary="deletes an opensearch datastream.",
    description="""
deletes the datastream `index`, including all its backing indexes and template.

- **all the data will be erased and the corresponding `operation_id` on the collab database will be deleted**.
- `token` needs `admin` permission.
""",
)
async def opensearch_delete_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(sess, token, obj=op, permission=GulpUserPermission.ADMIN)
            index = op.index

        # delete the datastream
        await GulpOpenSearch.get_instance().datastream_delete(
            ds=index, throw_on_error=True
        )

        # delete the operation
        await GulpOperation.delete_by_id(sess, operation_id)        
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"index": index, "operation_id": operation_id}))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
    "/opensearch_get_mapping_by_src",
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
    summary="get fields mapping.",
    description="get all `key=type` mappings for the given datastream `index` matching the given `operation_id`, `context_id` and `source_id`.",
)
async def opensearch_get_mapping_by_src_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index

        m = await GulpOpenSearch.get_instance().datastream_get_mapping_by_src(
            index, operation_id=operation_id, context_id=context_id, source_id=source_id
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=m))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
                                "indexes": [
                                    {
                                        "index_name": ".ds-new_index-000001",
                                        "index_uuid": "qwrMdTrgRI6fdU_SsIxbzw",
                                    }
                                ],
                                "template": "new_index-template",
                            },
                            {
                                "name": "test_idx",
                                "indexes": [
                                    {
                                        "index_name": ".ds-test_idx-000001",
                                        "index_uuid": "ZUuTB5KrSw6V-JVt9jtbcw",
                                    }
                                ],
                                "template": "test_idx-template",
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
            await GulpUserSession.check_token(sess, token, permission=GulpUserPermission.ADMIN)

        l = await GulpOpenSearch.get_instance().datastream_list()
        return JSONResponse(JSendResponse.success(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    "/opensearch_init_index",
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
- if `index` exists, it is **deleted** and recreated.
""",
)
async def opensearch_init_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    index_template: Optional[UploadFile] = File(
        None,
        description="optional custom index template, for advanced usage only: see [here](https://opensearch.org/docs/latest/im-plugin/index-templates/)",
    ),
    restart_processes: Annotated[
        bool,
        Query(
            description="if true, the process pool is restarted as well.",
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params.pop("index_template", None)
    ServerUtils.dump_params(params)
    f: str = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN, obj=op
            )
            index = op.index

        if index_template:
            # get index template from file
            f = await muty.uploadfile.to_path(index_template)

        await _recreate_index_internal(index, restart_processes, index_template=f)
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"index": index}))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
    finally:
        if f is not None:
            await muty.file.delete_file_or_dir_async(f)


@router.post(
    "/postgres_init_collab",
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
> **WARNING: ALL DATA WILL BE ERASED AND RESET TO DEFAULT !**

- default users are created: `admin/admin`, `guest/guest`, `ingest/ingest`, `editor/editor`, `power/power`.
- `token` needs to have `admin` permission.
""",
)
async def postgres_init_collab_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
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
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )

        collab = GulpCollab.get_instance()
        await collab.init(main_process=True, force_recreate=True)
        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()
        return JSONResponse(JSendResponse.success(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
- `token` needs to have `admin` permission.
- **WARNING: all documents at `operation_id.index` will be erased and the whole collab database will be reset with default data!**
-  same as calling `opensearch_init_index` and `postgres_init_collab` in one shot
""",
)
async def gulp_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
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
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )

        # reset collab
        collab = GulpCollab.get_instance()
        await collab.init(main_process=True, force_recreate=True)

        # reset data
        await _recreate_index_internal(index, restart_processes)
        return JSONResponse(JSendResponse.success(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


async def _rebase_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    dest_index: str,
    offset_msec: int,
    flt: GulpQueryFilter,
    delete_if_exists: bool,
    rebase_script: str,
):
    """
    runs in a worker process to rebase the index.
    """
    try:
        # create the destination datastream
        await GulpOpenSearch.get_instance().datastream_create(
            dest_index, delete_first=delete_if_exists
        )

        # rebase
        res = await GulpOpenSearch.get_instance().rebase(
            index,
            dest_index=dest_index,
            offset_msec=offset_msec,
            flt=flt,
            rebase_script=rebase_script,
        )
    except Exception as ex:
        # signal the websocket
        MutyLogger.get_instance().exception(ex)
        GulpSharedWsQueue.get_instance().put(
            type=GulpWsQueueDataType.REBASE_DONE,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
            data=GulpRebaseDonePacket(
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
    GulpSharedWsQueue.get_instance().put(
        type=GulpWsQueueDataType.REBASE_DONE,
        ws_id=ws_id,
        user_id=user_id,
        req_id=req_id,
        operation_id=operation_id,
        data=GulpRebaseDonePacket(
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
    summary="rebases index/datastream to a different time.",
    description="""
rebases `index` and creates a new `dest_index` with rebased `@timestamp` + `offset`.

- `token` needs `ingest` permission.
- `flt` may be used to filter the documents to rebase.
- rebase happens in the background: when it is done, a `GulpWsQueueDataType.REBASE_DONE` event is sent to the `ws_id` websocket.
""",
)
async def opensearch_rebase_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    dest_index: Annotated[
        str,
        Query(
            description="name of the destination index.",
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
    create_dest_index: Annotated[
        bool,
        Query(
            description="if true, the destination index is created if it not exists. either, an error is reported if `dest_index` does not exists.",
        ),
    ] = True,
    delete_if_exists: Annotated[
        bool,
        Query(
            description="if true, the destination index is deleted first (and recreated), if it exists.",
        ),
    ] = True,
    flt: Annotated[str, Depends(
        APIDependencies.param_query_flt_optional)] = None,
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
        if index == dest_index:
            raise JSendException(
                req_id=req_id, ex=Exception(
                    "index and dest_index should be different!")
            )

        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.INGEST]
            )
            user_id = s.user_id

        if not create_dest_index:
            # check if the destination index exists
            exists = await GulpOpenSearch.get_instance().datastream_exists(dest_index)
            if not exists:
                raise ObjectNotFound(
                    "destination index %s does not exist!" % (dest_index)
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
            delete_if_exists=delete_if_exists,
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
        raise JSendException(ex=ex, req_id=req_id)
