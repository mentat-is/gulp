"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

from typing import Annotated

import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import APIRouter, Body, Header, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.api.rest.collab_utility as collab_utility
import gulp.structs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpCollabFilter, GulpCollabLevel, GulpCollabType
from gulp.api.collab.base import GulpCollabObject
from gulp.structs import InvalidArgument

_app: APIRouter = APIRouter()


@_app.post(
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
                            "CollabObj",
                        ],
                    }
                }
            }
        }
    },
    summary="list highlights, optionally using a filter.",
    description="available filters: id, owner_id, operation_id, context, src_file, name, start_msec, end_msec, private_only, level, limit, offset.",
)
async def highlight_list_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_list(
        token, req_id, GulpCollabType.HIGHLIGHT, flt
    )


@_app.get(
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="get an hightlight.",
)
async def highlight_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    hl_id: Annotated[int, Query(description="highlight id to be retrieved.")],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_get_by_id(
        token, req_id, GulpCollabType.HIGHLIGHT, hl_id
    )


@_app.delete(
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
                        "data": {"id": 1},
                    }
                }
            }
        }
    },
    summary="deletes an highlight.",
)
async def highlight_delete_handler(
    token: Annotated[
        str,
        Header(
            description=gulp.structs.API_DESC_TOKEN
            + " (must have at least EDIT permission to delete own highlights, or DELETE to delete other users highlights)."
        ),
    ],
    hl_id: Annotated[int, Query(description="highlight id to be deleted.")],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await collab_utility.collabobj_delete(
            await collab_api.session(),
            token,
            GulpCollabType.HIGHLIGHT,
            hl_id,
            ws_id=ws_id,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data={"id": hl_id}))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="updates an existing highlight.",
)
async def highlight_update_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
    hl_id: Annotated[int, Query(description="highlight id to be updated.")],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    time_start: Annotated[
        int,
        Query(
            description="optional new start timestamp in milliseconds from unix epoch."
        ),
    ] = None,
    time_end: Annotated[
        int,
        Query(
            description="optional new end timestamp in milliseconds from unix epoch."
        ),
    ] = None,
    name: Annotated[str, Query(description="optional new name.")] = None,
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[
        int,
        Query(
            description="optional new glyph ID to be associated with this highlight."
        ),
    ] = None,
    color: Annotated[
        str, Query(description="optional new color in #rrggbb or css-name format. ")
    ] = None,
    private: Annotated[bool, Query(description=gulp.structs.API_DESC_PRIVATE)] = None,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if (
            time_start is None
            and time_end is None
            and name is None
            and description is None
            and glyph_id is None
            and color is None
            and private is None
        ):
            raise InvalidArgument(
                "at least one of time_start, time_end, name, description, glyph_id, color, private must be set."
            )

        o = await GulpCollabObject.update(
            await collab_api.session(),
            token,
            req_id,
            hl_id,
            ws_id,
            time_start=time_start,
            time_end=time_end,
            name=name,
            description=description,
            glyph_id=glyph_id,
            data={"color": color} if color is not None else None,
            type=GulpCollabType.HIGHLIGHT,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="creates an highlight for events between start and end timestamp.",
)
async def highlight_create_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
    operation_id: Annotated[
        int, Query(description="operation to be associated with this highlight.")
    ],
    context: Annotated[
        str, Query(description="context to be associated with this highlight.")
    ],
    src_file: Annotated[
        str, Query(description="source file to be associated with this highlight.")
    ],
    time_start: Annotated[
        int, Query(description="start timestamp in milliseconds from unix epoch.")
    ],
    time_end: Annotated[
        int, Query(description="end timestamp in milliseconds from unix epoch.")
    ],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    name: Annotated[str, Query(description="name of the highlight.")] = None,
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[
        int,
        Query(description="optional glyph ID to be associated with this highlight."),
    ] = None,
    color: Annotated[
        str,
        Query(
            description='optional color in #rrggbb or css-name format, default is "yellow". '
        ),
    ] = "yellow",
    private: Annotated[bool, Query(description=gulp.structs.API_DESC_PRIVATE)] = False,
    level: Annotated[
        GulpCollabLevel, Query(description=gulp.structs.API_DESC_COLLAB_LEVEL)
    ] = GulpCollabLevel.DEFAULT,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        hl = await GulpCollabObject.create(
            await collab_api.session(),
            token,
            req_id,
            GulpCollabType.HIGHLIGHT,
            ws_id=ws_id,
            operation_id=operation_id,
            context=context,
            src_file=src_file,
            name=name,
            time_start=time_start,
            time_end=time_end,
            description=description,
            glyph_id=glyph_id,
            data={"color": color} if color is not None else None,
            private=private,
            level=level,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=hl.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
