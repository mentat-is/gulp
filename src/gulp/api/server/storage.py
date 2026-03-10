"""
storage management API: manage files on S3-compatible instance
"""
import tempfile
from typing import Annotated, Optional

import aiofiles
from fastapi import APIRouter, BackgroundTasks, Body, Depends, Query
from fastapi.responses import FileResponse, JSONResponse
from muty.jsend import JSendException, JSendResponse
import muty.crypto
from muty.log import MutyLogger
from gulp.api.collab.link import GulpLink
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.s3_api import GulpS3
from gulp.api.server_api import GulpServer

router: APIRouter = APIRouter()

async def _storage_file_get_by_id_internal(
    storage_id: str,
    file_path: str,
) -> dict|None:
    try:
        d: dict = await GulpS3.get_instance().download_file(storage_id, file_path)
        return d
    except Exception as ex:
        return ex

@router.delete(
    "/storage_delete_by_id",
    tags=["storage"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    summary="deletes a file on the filestore.",
    description="""
deletes a file on the filestore providing the `gulp.storage_id`.
- `token` needs `edit` permission.
""",
)
async def storage_delete_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    storage_id: Annotated[str, Query(description="the storage ID of the file to delete.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token on operation
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, GulpUserPermission.EDIT
            )
            
            # delete file
            d = await GulpS3.get_instance().delete_file(storage_id)
            return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.delete(
    "/storage_delete_by_tags",
    tags=["storage"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    summary="delete files on the filestore by tags.",
    description="""
deletes files on the filestore by `operation_id` and/or `context_id` tags.
- `token` needs `edit` permission.
""",
)
async def storage_delete_by_tags_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        Optional[str],
        Query(description="the operation ID to filter files, if not provided, files from all operations will be deleted.",
              example="test_operation"),
    ] = None,
    context_id: Annotated[
        Optional[str],
        Query(description="the context ID to filter files, if not provided, files from all contexts will be deleted.",
            example="66d98ed55d92b6b7382ffc77df70eda37a6efaa1"),
    ]=None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token on operation
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, GulpUserPermission.EDIT
            )
            tags: dict = {}
            if operation_id:
                tags["operation_id"] = operation_id
            if context_id:
                tags["context_id"] = context_id
            
            # delete files
            d = await GulpS3.get_instance().delete_by_tags(tags=tags)
            return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/storage_list_files",
    tags=["storage"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": [
                            {
                                "storage_id": "operation123/context456/source789/logfile.evtx",
                                "size": 123456,
                                "tags": {
                                    "operation_id": "operation123",
                                    "context_id": "context456",
                                }
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="lists files on the filestore.",
    description="""
lists files on the filestore providing `operation_id` and/or `context_id`.

- results may be paginated via `continuation_token` and `max_results` query parameters: if so, if results contains a `continuation_token` field, the client can make another request with the same parameters and the returned `continuation_token` to get the next page of results, until no `continuation_token` is returned, which means there is no more result.
- `token` needs `edit` permission.
""",
)
async def storage_list_files_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        Optional[str],
        Query(description="the operation ID to filter files, if not provided, files from all operations will be listed.",
              example="test_operation"),
    ] = None,
    context_id: Annotated[
        Optional[str],
        Query(description="the context ID to filter files, if not provided, files from all contexts will be listed.",
            example="66d98ed55d92b6b7382ffc77df70eda37a6efaa1"),
    ]=None,
    continuation_token: Annotated[
        Optional[str],
        Query(description="the continuation token for pagination, if not provided, the first page of results will be returned."),
    ]=None,
    max_results: Annotated[
        Optional[int],
        Query(description="the maximum number of results to return, default is 100, maximum is 1000."),
    ]=100,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token on operation
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, GulpUserPermission.EDIT
            )
            tags: dict = {}
            if operation_id:
                tags["operation_id"] = operation_id
            if context_id:
                tags["context_id"] = context_id
            
            # get files
            files = await GulpS3.get_instance().list_by_tags(
                tags=tags,
                continuation_token=continuation_token,
                max_results=max_results,
            )
            return JSendResponse.success(data=files, req_id=req_id)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.get(
    "/storage_get_file_by_id",
    tags=["storage"],
    response_class=FileResponse,
    responses={
        200: {
            "content": {
                "application/octet-stream": {
                    "example": {
                        # file content
                    }
                }
            }
        }
    },
    summary="retrieves a file from the filestore.",
    description="""
downloads a file by `gulp.storage_id`.

- `token` needs `edit` permission.
""",
)
async def storage_file_get_by_id_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    storage_id: Annotated[str, Query(description="the storage ID of the file to retrieve.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> FileResponse:
    params=locals()
    params.pop("bt")  # remove background tasks from params
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token on operation
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, GulpUserPermission.EDIT)
            file_path: str = tempfile.mkstemp(prefix=muty.crypto.hash_xxh64(storage_id+req_id))[1]
            
            # execute download in worker
            d: dict = await GulpServer.get_instance().spawn_worker_task(
                _storage_file_get_by_id_internal,
                storage_id,
                file_path,
                wait=True,
            )
            if isinstance(d, Exception):
                # is an exception
                raise d

            async def _cleanup(file_path: str) -> None:
                """
                cleanup function to remove temporary export file after response is sent.
                """
                if file_path:
                    MutyLogger.get_instance().debug(
                        "cleanup file %s after response sent", file_path
                    )
                    await muty.file.delete_file_or_dir_async(file_path)

            # schedule cleanup of the temporary file after response is sent to the client
            res_file_path: str=d.get("file_path")
            bt.add_task(_cleanup, file_path)

            # return file response for streaming to client
            return FileResponse(
                res_file_path,
                media_type="binary/octet-stream",
                filename=storage_id.split("/")[-1],
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
