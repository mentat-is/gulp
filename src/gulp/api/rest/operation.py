"""
gulp operations rest api
"""

from muty.jsend import JSendException, JSendResponse
from typing import Annotated, Optional
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from muty.log import MutyLogger
import muty.string

from gulp.api.rest.structs import APIDependencies

router: APIRouter = APIRouter()


@router.post(
    "/operation_create",
    tags=["operation"],
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
                        "data": GulpOperation.example(),
                    }
                }
            }
        }
    },
    summary="creates a operation.",
    description="""
- `token` needs `admin` permission.
""",
)
async def operation_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    name: Annotated[
        str,
        Depends(APIDependencies.param_display_name),
    ],
    index: Annotated[
        str,
        Depends(APIDependencies.param_index),
    ],
    description: Annotated[
        str,
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    d = {
        "index": index,
        "name": name,
        "description": description,
        "glyph_id": glyph_id,
    }
    try:
        d = await GulpOperation.create(
            token,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            object_data=d,
            permission=[GulpUserPermission.ADMIN],
            id=muty.string.ensure_no_space_no_special(name.lower()),
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/operation_update",
    tags=["operation"],
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
                        "data": GulpOperation.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing operation.",
    description="""
- `token` needs `admin` permission.
""",
)
async def operation_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    index: Annotated[
        str,
        Depends(APIDependencies.param_index_optional),
    ] = None,
    description: Annotated[
        str,
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals)
    try:
        if not any([index, description, glyph_id]):
            raise ValueError(
                "At least one of index, description, or glyph_id must be provided."
            )
        d = {}
        d["index"] = index
        d["description"] = description
        d["glyph_id"] = glyph_id
        d = await GulpOperation.update_by_id(
            token,
            object_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            d=d,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.delete(
    "/operation_delete",
    tags=["operation"],
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
    summary="deletes a operation.",
    description="""
- `token` needs `admin` permission.
""",
)
async def operation_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="delete related data on gulp collab and opensearch index (`index` must be provided)."
        ),
    ] = True,
    index: Annotated[str, Depends(APIDependencies.param_index_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if delete_data and not index:
            raise ValueError("If `delete_data` is set, `index` must be provided.")

        await GulpOperation.delete_by_id(
            token,
            object_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            permission=[GulpUserPermission.ADMIN],
        )

        if delete_data:
            # delete all data
            MutyLogger.get_instance().info(
                f"deleting data related to operation_id={object_id} on index={index} ..."
            )
            await GulpOpenSearch.get_instance().delete_data_by_operation(
                index, object_id
            )

        return JSendResponse.success(req_id=req_id, data={"id": object_id})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
    "/operation_get_by_id",
    tags=["operation"],
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
                        "data": GulpOperation.example(),
                    }
                }
            }
        }
    },
    summary="gets a operation.",
)
async def operation_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpOperation.get_by_id_wrapper(
            token,
            object_id,
            nested=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.post(
    "/operation_list",
    tags=["operation"],
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
                            GulpOperation.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list operations, optionally using a filter.",
    description="",
)
async def operation_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpOperation.get_by_filter_wrapper(
            token,
            flt,
            nested=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
