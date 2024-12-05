"""
gulp highlights rest api
"""

from muty.jsend import JSendException, JSendResponse
from typing import Annotated
from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from gulp.api.collab.highlight import GulpHighlight
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from gulp.api.rest.structs import APIDependencies

router: APIRouter = APIRouter()


@router.post(
    "/highlight_create",
    tags=["highlight"],
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
                        "data": GulpHighlight.example(),
                    }
                }
            }
        }
    },
    summary="creates an highlight.",
    description="""
highlights a time range on a source.

- `token` needs `edit` permission.
- default `color` is `green`.
""",
)
async def highlight_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    time_range: Annotated[
        tuple[int, int],
        Body(
            description="the time range of the highlight, in nanoseconds from unix epoch."
        ),
    ],
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        object_data = GulpHighlight.build_dict(
            operation_id=operation_id,
            glyph_id=glyph_id,
            tags=tags,
            color=color or "green",
            name=name,
            source_id=source_id,
            time_range=time_range,
        )
        d = await GulpHighlight.create(
            token,
            ws_id=ws_id,
            req_id=req_id,
            object_data=object_data,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/highlight_update",
    tags=["highlight"],
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
                        "data": GulpHighlight.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing highlight.",
    description="""
- `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
""",
)
async def highlight_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    time_range: Annotated[
        tuple[int, int],
        Body(
            description="the time range of the highlight, in nanoseconds from unix epoch."
        ),
    ] = None,
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals)
    try:
        if not any([time_range, name, tags, glyph_id, color]):
            raise ValueError(
                "at least one of time_range, name, tags, glyph_id, color must be set."
            )
        d = {}
        d["time_range"] = time_range
        d["name"] = name
        d["tags"] = tags
        d["glyph_id"] = glyph_id
        d["color"] = color
        d = await GulpHighlight.update_by_id(
            token,
            object_id,
            ws_id=ws_id,
            req_id=req_id,
            d=d,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.delete(
    "/highlight_delete",
    tags=["highlight"],
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
    summary="deletes an highlight.",
    description="""
- `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
""",
)
async def highlight_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpHighlight.delete_by_id(
            token,
            object_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": object_id})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
    "/highlight_get_by_id",
    tags=["highlight"],
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
                        "data": GulpHighlight.example(),
                    }
                }
            }
        }
    },
    summary="gets an highlight.",
)
async def highlight_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpHighlight.get_by_id_wrapper(
            token,
            object_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.post(
    "/highlight_list",
    tags=["link"],
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
                            GulpHighlight.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list highlights, optionally using a filter.",
    description="",
)
async def highlight_list_handler(
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
        d = await GulpHighlight.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
