"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import json
from typing import Annotated, Optional
from fastapi import APIRouter, Body, Depends, File, Form, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
import muty.log
import muty.uploadfile
import muty.file
from pydantic import BaseModel, ConfigDict, field_validator, validator
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.ws_api import GulpRebaseDonePacket, GulpSharedWsQueue, GulpWsQueueDataType
from gulp.process import GulpProcess

router: APIRouter = APIRouter()


@router.post(
    "/opensearch_init",
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
    summary="(re)creates gulp data index.",
    description="""
- `token` needs to have `admin` permission.
- **WARNING: all documents will be erased and `index` recreated !**
""",
)
async def opensearch_init_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
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
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )

        if index_template:
            # get index template from file
            f = await muty.uploadfile.to_path(index_template)

        await GulpOpenSearch.get_instance().datastream_create(index, index_template=f)

        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"index": index}))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
    finally:
        if f is not None:
            await muty.file.delete_file_or_dir_async(f)


@router.post(
    "/postgres_init",
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
- `token` needs to have `admin` permission.
- **WARNING: all collab database will be erased !**
""",
)
async def postgres_init_handler(
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
        await collab.init(force_recreate=True)
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
- **WARNING: all documents at `index` and the whole collab database will be erased!**
-  same as calling `opensearch_init` and `postgres_init` in one shot
""",
)
async def gulp_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
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
        await collab.init(force_recreate=True)

        # reset data
        await GulpOpenSearch.get_instance().reinit()
        await GulpOpenSearch.get_instance().datastream_create(index)

        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()
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
    flt: str,
):
    """
    runs in a worker process to rebase the index.
    """
    try:
        # create the destination datastream
        await GulpOpenSearch.get_instance().datastream_create(dest_index)

        # rebase
        res = await GulpOpenSearch.get_instance().rebase(
            index, dest_index=dest_index, offset_msec=offset_msec, flt=flt
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
- rebase happens in the background and **uses one of the worker processes**: when it is done, a `GulpWsQueueDataType.REBASE_DONE` event is sent to the `ws_id` websocket.
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
            description="""offset, in milliseconds from the unix epoch, to be added to the `@timestamp` field.
            
- to subtract, use a negative offset.
""",
        ),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[str, Depends(APIDependencies.param_query_flt_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)

    try:
        if index == dest_index:
            raise JSendException(
                req_id=req_id, ex=Exception("index and dest_index should be different!")
            )

        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.INGEST]
            )
            user_id = s.user_id

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
        )

        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _rebase_internal, kwds=kwds
            )

        await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
