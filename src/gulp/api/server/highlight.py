"""
This module handles REST API endpoints for highlights in the GULP system.

It provides functionality to create, update, delete, get by ID, and list highlights.
Highlights represent time ranges on sources that can be marked with colors and
additional metadata such as names, tags, and glyphs.

The module includes route handlers for the following operations:
- Creating a new highlight
- Updating an existing highlight
- Deleting a highlight
- Retrieving a highlight by its ID
- Listing highlights with optional filtering

Each endpoint follows a consistent pattern of request validation, authorization checking,
and response formatting using JSendResponse structure.

"""

from typing import Annotated

from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

from gulp.api.collab.highlight import GulpHighlight
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from sqlalchemy.ext.asyncio import AsyncSession

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
    name: Annotated[str, Depends(APIDependencies.param_name_optional)] = None,
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token on operation
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, GulpUserPermission.EDIT
            )
            user_id: str = s.user.id

            l: GulpHighlight = await GulpHighlight.create_internal(
                sess,
                user_id,
                ws_id=ws_id,
                private=private,
                req_id=req_id,
                operation_id=operation_id,
                glyph_id=glyph_id,
                tags=tags,
                color=color,
                description=description,
                name=name,
                source_id=source_id,
                time_range=time_range,
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id, data=l.to_dict(exclude_none=True)
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    name: Annotated[str, Depends(APIDependencies.param_name_optional)] = None,
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    time_range: Annotated[
        tuple[int, int],
        Body(
            description="the time range of the highlight, in nanoseconds from unix epoch."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    sess: AsyncSession = None
    try:
        if not any([time_range, name, tags, glyph_id, color, description]):
            raise ValueError(
                "at least one of time_range, name, description, tags, glyph_id, color must be set."
            )
        async with GulpCollab.get_instance().session() as sess:
            # check permissions on both operation and object
            s: GulpUserSession
            obj: GulpHighlight
            s, obj, _ = await GulpHighlight.get_by_id_wrapper(
                sess,
                token,
                obj_id,
                permission=GulpUserPermission.EDIT,
            )

            # update
            if name:
                obj.name = name
            if description:
                obj.description = description
            if tags:
                obj.tags = tags
            if glyph_id:
                obj.glyph_id = glyph_id
            if color:
                obj.color = color
            if time_range:
                obj.time_range = list(time_range)

            dd: dict = await obj.update(sess, ws_id=ws_id, user_id=s.user.id)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpHighlight.delete_by_id_wrapper(
            token,
            obj_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpHighlight
            _, obj, _ = await GulpHighlight.get_by_id_wrapper(
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
    "/highlight_list",
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
                        "data": [
                            GulpHighlight.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list highlights, optionally using a filter.",
    description="""
- `operation_id` is set in `flt.operation_ids` automatically.
""",
)
async def highlight_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpHighlight.get_by_filter_wrapper(
            token,
            flt,
            operation_id=operation_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
