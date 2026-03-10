"""
This module provides utility REST API endpoints for Gulp.

It includes various endpoints for:
- Request management (get, cancel, list)
- Server management (restart, status, garbage collection)
- Plugin management (list, get, upload, delete)
- Mapping file management (list, get, upload, delete)
- Configuration management
- Version information

These endpoints support system administration, debugging, and customizing
the Gulp platform through plugin and mapping management.

"""

import base64
import os
from typing import Annotated, Literal

import muty.file
import orjson
import psutil
from fastapi import APIRouter, Depends, File, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
import muty.crypto
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.enhance_doc_map import GulpEnhanceDocumentMap
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import (
    COLLABTYPE_ENHANCE_DOCUMENT_MAP,
    COLLABTYPE_NOTE,
    GulpCollabBase,
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.redis_api import GulpRedis
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.structs import ObjectAlreadyExists, ObjectNotFound

router: APIRouter = APIRouter()


@router.get(
    "/request_get_by_id",
    tags=["stats"],
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
                        "data": GulpRequestStats.example(),
                    }
                }
            }
        }
    },
    summary="gets a request stats.",
)
async def request_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpRequestStats
            _, obj, _ = await GulpRequestStats.get_by_id_wrapper(
                sess,
                token,
                obj_id,
            )
            return JSendResponse.success(req_id=req_id, data=obj.to_dict())
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/request_cancel",
    tags=["stats"],
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
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="cancel a request.",
    description="""
cancel a running request by setting its `status` to `canceled` (or `failed` or `done`), so the engine can delete it after the expiration time.

- `token` needs `admin` permission or to be the owner of the request.
- any scheduled task for the request is deleted as well.
""",
)
async def request_cancel_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id_to_cancel: Annotated[
        str, Query(description="request id to cancel.", example="test_req")
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    expire_now: Annotated[
        bool,
        Query(
            description="if set, the request expiration time is set to now, so the stats are deleted immediately from the collab database (default is delete in 5 minutes)."
        ),
    ] = False,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpRequestStats
            _, obj, op = await GulpRequestStats.get_by_id_wrapper(
                sess,
                token,
                req_id_to_cancel,
                enforce_owner=True,
            )
            await obj.set_canceled(sess, expire_now=expire_now)

            # also delete related queued tasks (if any) in Redis
            d: int = await GulpRedis.get_instance().task_purge_by_filter(
                operation_id=op.id, req_id=req_id_to_cancel
            )
            MutyLogger.get_instance().debug(
                "also deleted %d pending tasks for request %s" % (d, req_id_to_cancel)
            )

            return JSendResponse.success(req_id=req_id, data={"id": req_id_to_cancel})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/request_set_completed",
    tags=["stats"],
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
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="complete the request.",
    description="""
sets the request either as `done` or `failed`.

- `token` needs `admin` permission or to be the owner of the request.
- any scheduled task for the request is deleted as well.
""",
)
async def request_set_completed_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id_to_complete: Annotated[
        str, Query(description="request id to set completed.", example="test_req")
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    failed: Annotated[
        bool, Query(description="if set, the request is marked as failed.")
    ] = False,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpRequestStats
            _, obj, op = await GulpRequestStats.get_by_id_wrapper(
                sess,
                token,
                req_id_to_complete,
                enforce_owner=True,
            )
            await obj.set_finished(
                sess,
                status=GulpRequestStatus.FAILED if failed else GulpRequestStatus.DONE,
            )

            return JSendResponse.success(req_id=req_id, data={"id": req_id_to_complete})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/request_delete",
    tags=["stats"],
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
                        "data": {"deleted": 123},
                    }
                }
            }
        }
    },
    summary="deletes one or more requests on the collab database.",
    description="""
- `token` needs `admin` permission or to be the owner of the request/s.
""",
)
async def request_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    obj_id: Annotated[
        str,
        Query(
            description="the request id to delete: if not set, all the requests are deleted for the given `operation_id`.",
            example="obj_id",
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession
            op: GulpOperation
            s, op, _ = await GulpOperation.get_by_id_wrapper(sess, token, operation_id)
            flt = GulpCollabFilter()
            flt.operation_ids = [op.id]
            if obj_id:
                # delete only the given request id
                flt.ids = [obj_id]
            deleted = await GulpRequestStats.delete_by_filter(sess, flt, s.user_id)
            return JSendResponse.success(req_id=req_id, data={"deleted": deleted})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/request_list",
    tags=["stats"],
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
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="get requests list.",
    description="""
get a list of all requests (identified by their `req_id`) issued by the calling user.

- if token has `admin` permission, requests from all users are returned (according to the `operation_id` filter).
""",
)
async def request_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    running_only: Annotated[
        bool, Query(description="if set, only return requests that are still running.")
    ] = False,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        if running_only:
            # only return ongoing requests
            flt = GulpCollabFilter(status=["ongoing"])
        else:
            # all requests
            flt = GulpCollabFilter()

        # restrict to operation id
        flt.operation_ids = [operation_id]
        stats: list[dict] = await GulpRequestStats.get_by_filter_wrapper(
            token, flt, operation_id=operation_id
        )
        return JSendResponse.success(req_id=req_id, data=stats)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.delete(
    "/object_delete_bulk",
    tags=["utility"],
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
                        "data": {"deleted": 123}, # number of deleted objects
                    }
                }
            }
        }
    },
    summary="deletes one or more (bulk delete) objects of the given type on the collab database.",
    description="""
- `token` needs at least `edit` permission.
""",
)
async def object_delete_bulk_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    obj_type: Annotated[
        str,
        Query(description="one of the collab object types.", example=COLLABTYPE_NOTE),
    ],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt) 
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession
            op: GulpOperation
            s, op, _ = await GulpOperation.get_by_id_wrapper(sess, token, operation_id, permission=GulpUserPermission.EDIT)
            if not flt.operation_ids:
                # set operation id in filter
                flt.operation_ids = [op.id]
            
            obj_class: GulpCollabBase = GulpCollabBase.object_type_to_class(obj_type)
            deleted = await obj_class.delete_by_filter(sess, flt, s.user_id)
            return JSendResponse.success(req_id=req_id, data={"deleted": deleted})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/enhance_document_map_create",
    tags=["utility"],
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
                        "data": GulpEnhanceDocumentMap.example(),
                    }
                }
            }
        }
    },
    summary="creates an enhance document map entry.",
    description="""
creates a `enhance_document_map` entry: those allows the UI to map a `gulp_event_code` in a specific plugin to a `glyph_id` and/or a `color` for enhanced visualization.

- `token` needs `edit` permission.
""",
)
async def enhance_document_map_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    gulp_event_code: Annotated[
        int,
        Query(description="the `gulp.event_code` to enhance."),
    ],
    plugin: Annotated[
        str,
        Query(description="the plugin to enhance the `gulp.event_code` in."),
    ],
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not glyph_id and not color:
            raise ValueError("At least one of glyph_id or color must be provided.")

        obj_id: str = muty.crypto.hash_sha1(str(gulp_event_code)+plugin+(str(glyph_id) if glyph_id else "")+(color if color else ""))
        d = await GulpEnhanceDocumentMap.create(
            token,
            permission=[GulpUserPermission.EDIT],
            private=False,
            gulp_event_code=gulp_event_code,
            plugin=plugin,
            glyph_id=glyph_id,
            color=color,
            obj_id=obj_id
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/enhance_document_map_update",
    tags=["utility"],
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
                        "data": GulpEnhanceDocumentMap.example(),
                    }
                }
            }
        }
    },
    summary="updates an enhance document map entry.",
    description="""
- `token` needs `edit` permission.
""",
)
async def enhance_document_map_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any(
            [
                glyph_id is not None,
                color is not None,
            ]
        ):
            raise ValueError(
                "At least one of glyph_id or color must be provided."
            )

        async with GulpCollab.get_instance().session() as sess:
            obj: GulpEnhanceDocumentMap
            _, obj, _ = await GulpEnhanceDocumentMap.get_by_id_wrapper(
                sess,
                token,
                obj_id,
                permission=GulpUserPermission.EDIT,
            )

            if glyph_id is not None:
                obj.glyph_id = glyph_id
            if color is not None:
                obj.color = color

            dd: dict = await obj.update(sess)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/enhance_document_map_delete",
    tags=["utility"],
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
                        "data": {"id": "obj_id"},
                    }
                }
            }
        }
    },
    summary="deletes an enhance document map entry.",
    description="""
- `token` needs `edit` permission.
""",
)
async def enhance_document_map_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpEnhanceDocumentMap.delete_by_id_wrapper(
            token,
            obj_id,
            permission=GulpUserPermission.EDIT,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/enhance_document_map_get_by_id",
    tags=["utility"],
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
                        "data": GulpEnhanceDocumentMap.example(),
                    }
                }
            }
        }
    },
    summary="gets an enhance document map entry.",
)
async def enhance_document_map_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpEnhanceDocumentMap
            _, obj, _ = await GulpEnhanceDocumentMap.get_by_id_wrapper(
                sess,
                token,
                obj_id,
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/enhance_document_map_list",
    tags=["utility"],
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
                            GulpEnhanceDocumentMap.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="lists enhance document map entries, optionally using a filter.",
    description="""
- `you may use `flt.gulp_event_code` (**as string**) and `flt.plugin` to filter the entries. if not set, all entries are returned.
""",
)
async def enhance_document_map_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    flt.operation_ids = None
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpEnhanceDocumentMap.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.get(
    "/metrics",
    tags=["utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1763549859568,
                        "req_id": "test_req",
                        "data": {
                            "memory": {
                                "total": 31490965504,
                                "total_mb": 30032.125,
                                "available": 15975964672,
                                "available_mb": 15235.8671875,
                                "percent": 49.3,
                                "used": 14363516928,
                                "used_mb": 13698.1171875,
                                "free": 11258863616,
                                "free_mb": 10737.2890625,
                            },
                            "cpu": {
                                "percent": 34.3,
                                "count": 8,
                                "freq": {
                                    "current": 2222.6521249999996,
                                    "min": 1400,
                                    "max": 2100,
                                },
                            },
                            "disk": {
                                "total": 477153402880,
                                "total_mb": 455048.9453125,
                                "used": 152457015296,
                                "used_mb": 145394.33984375,
                                "free": 322606174208,
                                "free_mb": 307661.22265625,
                                "percent": 32.1,
                            },
                            "processes": [
                                {
                                    "pid": 33207,
                                    "name": "gulp",
                                    "cpu_percent": 0,
                                    "memory": 200396800,
                                    "memory_mb": 191.11328125,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 /gulp/.venv/bin/gulp",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 33215,
                                    "name": "python3",
                                    "cpu_percent": 0,
                                    "memory": 14151680,
                                    "memory_mb": 13.49609375,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.resource_tracker import main;main(14)",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 33216,
                                    "name": "python3",
                                    "cpu_percent": 0,
                                    "memory": 134287360,
                                    "memory_mb": 128.06640625,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=22) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 33249,
                                    "name": "python3",
                                    "cpu_percent": 45.8,
                                    "memory": 331464704,
                                    "memory_mb": 316.109375,
                                    "status": "running",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=36) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 36450,
                                    "name": "python3",
                                    "cpu_percent": 3.4,
                                    "memory": 215597056,
                                    "memory_mb": 205.609375,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=39) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 36625,
                                    "name": "python3",
                                    "cpu_percent": 3.4,
                                    "memory": 302477312,
                                    "memory_mb": 288.46484375,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=25) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 36660,
                                    "name": "python3",
                                    "cpu_percent": 3.2,
                                    "memory": 193953792,
                                    "memory_mb": 184.96875,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=25) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 36759,
                                    "name": "python3",
                                    "cpu_percent": 3.4,
                                    "memory": 193638400,
                                    "memory_mb": 184.66796875,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=44) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 36804,
                                    "name": "python3",
                                    "cpu_percent": 3.3,
                                    "memory": 193785856,
                                    "memory_mb": 184.80859375,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=47) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 36872,
                                    "name": "python3",
                                    "cpu_percent": 3.4,
                                    "memory": 193945600,
                                    "memory_mb": 184.9609375,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=22) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 37087,
                                    "name": "python3",
                                    "cpu_percent": 3.3,
                                    "memory": 193224704,
                                    "memory_mb": 184.2734375,
                                    "status": "sleeping",
                                    "cmdline": "/gulp/.venv/bin/python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=27) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                            ],
                        },
                    }
                }
            }
        }
    },
    summary="""gets gulp node metrics.
    """,
    description="""
gets detailed gulp node metrics, including memory, cpu, disk usage, and running processes.

- token needs `admin` permission.
    """,
)
async def metrics_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    gulp_only: Annotated[
        bool,
        Query(
            description="if set, only return process information for gulp processes only. either, return process information for all the running processes."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:

    params = locals()
    ServerUtils.dump_params(params)
    async with GulpCollab.get_instance().session() as sess:
        await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

    def _get_process_info(proc, gulp_only):
        # check if the process is the current pid, or a child of the current pid
        if gulp_only and proc.ppid() != os.getpid():
            return None

        try:
            mem_info = proc.memory_info()
            return {
                "pid": proc.pid,
                "name": proc.name(),
                "cpu_percent": proc.cpu_percent(),
                # only return the memory occupied by the process (rss) in bytes and megabytes
                "memory": mem_info.rss,
                "memory_mb": mem_info.rss / 1024 / 1024,
                "status": proc.status(),
                "cmdline": " ".join(proc.cmdline()) if proc.cmdline() else "",
                "username": proc.username(),
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None

    # get system stats
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    stats = {
        "memory": {
            "total": memory.total,
            "total_mb": memory.total / 1024 / 1024,
            "available": memory.available,
            "available_mb": memory.available / 1024 / 1024,
            "percent": memory.percent,
            "used": memory.used,
            "used_mb": memory.used / 1024 / 1024,
            "free": memory.free,
            "free_mb": memory.free / 1024 / 1024,
        },
        "cpu": {
            "percent": psutil.cpu_percent(interval=1),
            "count": psutil.cpu_count(),
            "freq": psutil.cpu_freq()._asdict() if psutil.cpu_freq() else {},
        },
        "disk": {
            "total": disk.total,
            "total_mb": disk.total / 1024 / 1024,
            "used": disk.used,
            "used_mb": disk.used / 1024 / 1024,
            "free": disk.free,
            "free_mb": disk.free / 1024 / 1024,
            "percent": disk.percent,
        },
        "processes": [
            info
            for info in [
                _get_process_info(proc, gulp_only) for proc in psutil.process_iter()
            ]
            if info is not None
        ],
    }

    return JSendResponse.success(data=stats, req_id=req_id)


@router.get(
    "/plugin_list",
    tags=["plugin"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734609216840,
                        "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                        "data": [
                            {
                                "display_name": "AI report generator",
                                "type": ["extension"],
                                "desc": "autogenerate security incident reports using AI.",
                                "path": "/Users/valerino/repos/gulp/gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension/ai_report.py",
                                "data": {},
                                "filename": "ai_report.py",
                                "sigma_support": False,
                                "custom_parameters": [],
                                "depends_on": ["story"],
                                "tags": [],
                                "version": "",
                            },
                            {
                                "display_name": "OpenSearch anomalies detector",
                                "type": ["extension"],
                                "desc": "find anomalies using Opensearch Anomaly Detection plugin.",
                                "path": "/Users/valerino/repos/gulp/gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension/os_anomaly_detector.py",
                                "data": {},
                                "filename": "os_anomaly_detector.py",
                                "sigma_support": False,
                                "custom_parameters": [],
                                "depends_on": [],
                                "tags": [],
                                "version": "",
                            },
                            {
                                "display_name": "enrich_abuse",
                                "type": ["enrichment"],
                                "desc": "abuse.ch url enrichment plugin",
                                "path": "/Users/valerino/repos/gulp/src/gulp/plugins/enrich_abuse.py",
                                "data": {},
                                "filename": "enrich_abuse.py",
                                "sigma_support": False,
                                "custom_parameters": [
                                    {
                                        "name": "url_fields",
                                        "type": "list",
                                        "default_value": [
                                            "url.full",
                                            "url.original",
                                            "http.request.referrer",
                                        ],
                                        "desc": "a list of url fields to enrich.",
                                        "location": "query",
                                        "required": False,
                                    },
                                    {
                                        "name": "auth_key",
                                        "type": "str",
                                        "desc": "abuse.ch auth-key (if not provided, the config file is checked for it)",
                                        "location": "query",
                                        "required": False,
                                    },
                                ],
                                "depends_on": [],
                                "tags": [],
                                "version": "",
                            },
                            {
                                "display_name": "chrome_history_sqlite_stacked",
                                "type": ["ingestion"],
                                "desc": "chrome based browsers history sqlite stacked plugin",
                                "path": "/Users/valerino/repos/gulp/src/gulp/plugins/chrome_history_sqlite_stacked.py",
                                "data": {},
                                "filename": "chrome_history_sqlite_stacked.py",
                                "sigma_support": False,
                                "custom_parameters": [],
                                "depends_on": ["sqlite"],
                                "tags": [],
                                "version": "",
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="list available plugins.",
)
async def plugin_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.READ)

            l = await GulpPluginBase.list_plugins()

            # turn to model_dump
            ll: list[dict] = []
            for p in l:
                ll.append(p.model_dump(exclude_none=True))
            return JSONResponse(JSendResponse.success(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.post(
        "/plugin_upload",
        tags=["plugin"],
        response_model=JSendResponse,
        response_model_exclude_none=True,
        responses={
            200: {
                "content": {
                    "application/json": {
                        "example": {
                            "status": "success",
                            "timestamp_msec": 1734609216840,
                            "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                            "data": {
                                "file_paths": [
                                    "GULP_WORKING_DIR/plugins/ui/test_plugin.py",
                                ],
                            },
                        }
                    }
                }
            }
        },
        summary="upload one or more plugin files.",
        description="""
- token needs `admin` permission.
- files are saved under `GULP_WORKING_DIR/plugins/<type>` (type defaults to `default`).
""",
)
async def plugin_upload_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    files: Annotated[
        list[UploadFile],
        File(description="the plugin file(s) to upload. For ui plugins, multiple files may be uploaded."),
    ],
    plugin_type: Annotated[Literal["default","extension","ui"], Query(description="the plugin type.")]= "default",
    fail_if_exists: Annotated[
        bool,
        Query(
            description="if set to true, the upload will fail if a file with the same name already exists. otherwise, it will be overwritten."
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    params = locals()
    params.pop("files", None)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        # determine destination directory based on plugin type
        extra_path = GulpConfig.get_instance().path_plugins_extra()
        if not extra_path:
            raise Exception("plugins extra path not set")

        if plugin_type in ("extension", "ui"):
            dest_dir = os.path.join(extra_path, plugin_type)
        else:
            dest_dir = extra_path
        os.makedirs(dest_dir, exist_ok=True)

        # write each file
        saved: list[str] = []
        for up in files:
            content = await up.read()
            filename = up.filename
            save_path = os.path.join(dest_dir, filename)
            if fail_if_exists and await muty.file.exists_async(save_path):
                raise ObjectAlreadyExists(
                    f"file with name {filename} already exists in {dest_dir}"
                )
            await muty.file.write_file_async(save_path, content)
            saved.append(save_path)

        return JSONResponse(JSendResponse.success(req_id=req_id, data={"file_paths": saved}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.delete(
    "/plugin_delete",
    tags=["plugin"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734609216840,
                        "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                        "data": {
                            "deleted": True,
                        },
                    }
                }
            }
        }
    },
    summary="delete a plugin file.",
    description="""
- token needs `admin` permission.
- file is deleted from `GULP_WORKING_DIR/plugins` directory (or the subdirectory corresponding to the `plugin_type`) if found.
""",
)
async def plugin_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    filename: Annotated[
        str,
        Query(description='filename of the plugin to delete, i.e. "my_plugin.py"'),
    ],
    plugin_type: Annotated[
        Literal["default", "extension", "ui"],
        Query(description="the plugin type."),
    ] = "default",
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        extra_path = GulpConfig.get_instance().path_plugins_extra()

        # construct candidate paths
        subdir = plugin_type if plugin_type in ("extension", "ui") else ""
        file_path = os.path.join(extra_path, subdir, filename) if subdir else os.path.join(extra_path, filename)
        if not await muty.file.exists_async(file_path):
            raise ObjectNotFound(
                "file with name %s not found in %s, type=%s" %
                (filename, extra_path, plugin_type)
            )

        await muty.file.delete_file_async(file_path)
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"deleted": True}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.get(
    "/plugin_download",
    tags=["plugin"],
    response_class=FileResponse,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        # plugin file content (streamed as download)
                    }
                }
            }
        }
    },
    summary="download a plugin file.",
    description="""
- token needs `admin` permission.
- file is read from `GULP_WORKING_DIR/plugins` directory (or the subdirectory
  corresponding to the `plugin_type`) if found, first checking the extra path
  then the default installation path.
""",
)
async def plugin_download_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    filename: Annotated[
        str,
        Query(description='filename of the plugin to download, i.e. "my_plugin.py"'),
    ],
    plugin_type: Annotated[
        Literal["default", "extension", "ui"],
        Query(description="the plugin type."),
    ] = "default",
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> FileResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        extra_path = GulpConfig.get_instance().path_plugins_extra()
        default_path = GulpConfig.get_instance().path_plugins_default()

        # construct candidate paths
        subdir = plugin_type if plugin_type in ("extension", "ui") else ""
        file_path = os.path.join(extra_path, subdir, filename) if subdir else os.path.join(extra_path, filename)
        if not await muty.file.exists_async(file_path):
            file_path = os.path.join(default_path, subdir, filename) if subdir else os.path.join(default_path, filename)
            if not await muty.file.exists_async(file_path):
                raise ObjectNotFound(
                    "file with name %s not found in both %s and %s, type=%s" %
                    (filename, extra_path, default_path, plugin_type)
                )

        return FileResponse(path=file_path, media_type="application/octet-stream", filename=filename)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/ui_plugin_list",
    tags=["plugin"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734609216840,
                        "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                        "data": {
                            "status": "success",
                            "timestamp_msec": 1748263060378,
                            "req_id": "test_req",
                            "data": [
                                {
                                    "display_name": "Test UI Plugin",
                                    "plugin": "some_gulp_plugin",
                                    "extension": True,
                                    "version": "1.0.0",
                                    "desc": "A plugin for testing UI components",
                                }
                            ],
                        },
                    }
                }
            }
        }
    },
    summary="list available UI plugins.",
)
async def ui_plugin_list_handler(
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            l = await GulpPluginBase.list_ui_plugins()

            # turn to model_dump
            ll: list[dict] = []
            for p in l:
                d: dict = p.model_dump(exclude_none=True)
                ll.append(d)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/version",
    tags=["utility"],
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
                        "data": {"version": "gulp v0.0.9 (muty v0.2)"},
                    }
                }
            }
        }
    },
    summary="get gULP version.",
)
async def get_version_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.READ)

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"version": GulpServer.get_instance().version_string()},
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/mapping_file_list",
    tags=["mapping"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1735294441540,
                        "req_id": "test_req",
                        "data": [
                            {
                                "metadata": {"plugin": ["splunk.py"]},
                                "filename": "splunk.json",
                                "path": "/opt/gulp/mapping_files/splunk.json",
                                "mapping_ids": ["splunk"],
                            },
                            {
                                "metadata": {"plugin": ["csv.py"]},
                                "filename": "mftecmd_csv.json",
                                "path": "/opt/gulp/mapping_files/mftecmd_csv.json",
                                "mapping_ids": ["record", "boot", "j", "sds"],
                            },
                            {
                                "metadata": {"plugin": ["win_evtx.py"]},
                                "filename": "windows.json",
                                "path": "/opt/gulp/mapping_files/windows.json",
                                "mapping_ids": ["windows"],
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="lists available mapping files.",
)
async def mapping_file_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())

    def _exists(d: list[dict], filename: str) -> bool:
        for f in d:
            if f["filename"].lower() == filename.lower():
                return True
        return False

    async def _list_mapping_files_internal(path: str, d: list[dict]) -> None:
        """
        lists mapping files in path and append to d
        """
        MutyLogger.get_instance().info("listing mapping files in %s" % (path))
        if not path:
            return

        files = await muty.file.list_directory_async(path)

        # for each file, get metadata and mapping_ids
        for f in files:
            if not f.lower().endswith(".json"):
                continue

            filename = os.path.basename(f)
            if _exists(d, filename):
                MutyLogger.get_instance().warning(
                    "skipping mapping file %s (already exists)" % (f)
                )
                continue

            # read file
            content = await muty.file.read_file_async(f)
            js = orjson.loads(content)

            # get metadata
            mtd = js.get("metadata", {})

            # get mapping_id for each mapping
            mappings: dict = js.get("mappings", {})
            mapping_ids = list(str(key) for key in mappings.keys())

            d.append(
                {
                    "metadata": mtd,
                    "filename": filename.lower(),
                    "path": f,
                    "mapping_ids": mapping_ids,
                }
            )

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token)

        d: list[dict] = []
        default_path = GulpConfig.get_instance().path_mapping_files_default()
        extra_path = GulpConfig.get_instance().path_mapping_files_extra()

        # extra_path first
        await _list_mapping_files_internal(extra_path, d)

        # then default
        await _list_mapping_files_internal(default_path, d)

        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.post(
    "/mapping_file_upload",
    tags=["mapping"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1735294441540,
                        "req_id": "test_req",
                        "data": {
                            "path": "GULP_WORKING_DIR/mapping_files/custom_mapping.json",
                        }
                    }
                }
            }
        }
    },
    summary="upload a mapping file.",
    description="""
- token needs `admin` permission.
- file is saved to `GULP_WORKING_DIR/mapping_files` directory.
""",
)
async def mapping_file_upload_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    file: Annotated[UploadFile, File(description="the mapping file to upload")],
    fail_if_exists: Annotated[
        bool,
        Query(
            description="if set to true, the upload will fail if a file with the same name already exists. otherwise, it will be overwritten."
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    params = locals()
    params.pop("file", None)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        # save file to mapping_files directory
        content = await file.read()
        filename = file.filename
        mapping_extra_path = GulpConfig.get_instance().path_mapping_files_extra()
        save_path = os.path.join(mapping_extra_path, filename)

        if fail_if_exists and await muty.file.exists_async(save_path):
            raise ObjectAlreadyExists(
                "file with name %s already exists in %s" % (filename, mapping_extra_path)
            )

        await muty.file.write_file_async(save_path, content)
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"path": save_path}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.get(
    "/mapping_file_download",
    tags=["mapping"],
    response_class=FileResponse,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        # mapping file content (streamed as download)
                    }
                }
            }
        }
    },
    summary="download a mapping file.",
    description="""
- token needs `admin` permission.
- file is read from `GULP_WORKING_DIR/mapping_files` directory if found, either from the main installation directory in `mapping_files`.
""",
)
async def mapping_file_download_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    filename: Annotated[
        str,
        Query(description='filename of the mapping file to download, i.e. "custom_mapping.json"'),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> FileResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        extra_path = GulpConfig.get_instance().path_mapping_files_extra()
        default_path = GulpConfig.get_instance().path_mapping_files_default()

        file_path = os.path.join(extra_path, filename)
        if not await muty.file.exists_async(file_path):
            file_path = os.path.join(default_path, filename)
            if not await muty.file.exists_async(file_path):
                raise ObjectNotFound(
                    "file with name %s not found in both %s and %s" % (filename, extra_path, default_path)
                )

        return FileResponse(path=file_path, media_type="application/json", filename=filename)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    
@router.delete(
    "/mapping_file_delete",
    tags=["mapping"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1735294441540,
                        "req_id": "test_req",
                        "data": {
                            "file_path": "GULP_WORKING_DIR/mapping_files/custom_mapping.json",
                        }
                    }
                }
            }
        }
    },
    summary="delete a mapping file.",
    description="""
- token needs `admin` permission.
- file is deleted from `GULP_WORKING_DIR/mapping_files` directory.
""",
)
async def mapping_file_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    filename: Annotated[
        str,
        Query(description='filename of the mapping file to delete, i.e. "custom_mapping.json"'),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        extra_path = GulpConfig.get_instance().path_mapping_files_extra()
        file_path = os.path.join(extra_path, filename)
        if not await muty.file.exists_async(file_path):
            raise ObjectNotFound(
                "file with name %s not found in %s" % (filename, extra_path)
            )

        await muty.file.delete_file_async(file_path)
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"file_path": file_path}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.post(
    "/config_upload",
    tags=["config"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1735294441540,
                        "req_id": "test_req",
                        "data": {
                            "file_path": "GULP_WORKING_DIR/gulp_cfg.json",                            
                        }
                    }
                }
            }
        }
    },
    summary="upload a config file.",
    description="""
- token needs `admin` permission.
- file is saved to `GULP_WORKING_DIR/gulp_cfg.json`.
- gulp must be restarted for the new config to take effect.
""",
)
async def config_upload_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    file: Annotated[UploadFile, File(description="the config file to upload")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    params = locals()
    params.pop("file", None)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        # save file to working directory as gulp_cfg.json
        content = await file.read()
        filename = "gulp_cfg.json"
        save_path = os.path.join(GulpConfig.get_instance().path_working_dir(), filename)

        await muty.file.write_file_async(save_path, content)
        return JSONResponse(JSendResponse.success(req_id=req_id, data={"file_path": save_path}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
                        
@router.get(
    "/config_download",
    tags=["config"],
    response_class=FileResponse,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        # mapping file content (streamed as download)
                    }
                }
            }
        }
    },
    summary="download the current config file.",
    description="""
- token needs `admin` permission.
- file is read from `GULP_WORKING_DIR/gulp_cfg.json`.
""",
)
async def config_download_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> FileResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

        file_path = os.path.join(GulpConfig.get_instance().path_working_dir(), "gulp_cfg.json")
        if not await muty.file.exists_async(file_path):
            raise ObjectNotFound(
                "config file not found in %s" % (file_path)
            )

        return FileResponse(path=file_path, media_type="application/json", filename="gulp_cfg.json")
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex