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
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import (
    GulpAssociatedEvent,
    GulpCollabFilter,
    GulpCollabLevel,
    GulpCollabType,
)
from gulp.api.collab.collabobj import CollabObj
from gulp.defs import InvalidArgument

_app: APIRouter = APIRouter()


@_app.post(
    "/link_list",
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
                            "CollabObj",
                        ],
                    }
                }
            }
        }
    },
    summary="list links, optionally using a filter.",
    description="available filters: id, owner_id, events, operation_id, context, src_file, name, private_only, level, limit, offset",
)
async def link_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_list(token, req_id, GulpCollabType.LINK, flt)


@_app.get(
    "/link_get_by_id",
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="get link.",
)
async def link_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    link_id: Annotated[int, Query(description="id of the link to be retrieved.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_get_by_id(
        token, req_id, GulpCollabType.LINK, link_id
    )


@_app.delete(
    "/link_delete",
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
                        "data": {"id": 1},
                    }
                }
            }
        }
    },
    summary="deletes a link.",
)
async def link_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_DELETE_EDIT_TOKEN)],
    link_id: Annotated[int, Query(description="id of the link to be deleted.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_delete(
        token, req_id, GulpCollabType.LINK, link_id, ws_id=ws_id
    )


@_app.put(
    "/link_update",
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="updates an existing link.",
    description="in `events` array (if set), each entry's `operation_id`, `context` and `src_file` are optional.",
)
async def link_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    link_id: Annotated[int, Query(description="id of the link to be updated.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    events: Annotated[list[GulpAssociatedEvent], Body()] = None,
    name: Annotated[str, Query(description="optional new name.")] = None,
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[
        int, Query(description="optional new glyph ID for the link.")
    ] = None,
    color: Annotated[
        str, Query(description="optional new link color in #rrggbb or css-name format.")
    ] = None,
    private: Annotated[bool, Query(description=gulp.defs.API_DESC_PRIVATE)] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if (
            events is None
            and name is None
            and description is None
            and glyph_id is None
            and color is None
        ):
            raise InvalidArgument(
                "at least one of events, name, description, glyph_id, color, private must be set."
            )
        data = {}
        if color is not None:
            data["color"] = color

        if events is not None:
            # FIXME: is this still needed ?
            data["events"] = [e.to_dict() for e in events]

        l = await CollabObj.update(
            await collab_api.session(),
            token,
            req_id,
            link_id,
            ws_id,
            name=name,
            description=description,
            events=events,
            glyph_id=glyph_id,
            data=data if len(data) > 0 else None,
            t=GulpCollabType.LINK,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/link_create",
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    description="in `events` array, each entry's `operation_id`, `context` and `src_file` are optional.",
    summary="creates a link between the source and one or more destination events.",
)
async def link_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    operation_id: Annotated[
        int, Query(description="operation to be associated with this link.")
    ],
    context: Annotated[
        str, Query(description="context to be associated with this link.")
    ],
    src_file: Annotated[
        str, Query(description="source file to be associated with this link.")
    ],
    src: Annotated[str, Query(description="id of the source GulpEvent.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    events: Annotated[list[GulpAssociatedEvent], Body()],
    name: Annotated[str, Query(description="name of the link.")] = None,
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[int, Query(description="glyph ID for the new link.")] = None,
    color: Annotated[
        str,
        Query(
            description='optional color in #rrggbb or css-name format, default is "red".'
        ),
    ] = "red",
    private: Annotated[bool, Query(description=gulp.defs.API_DESC_PRIVATE)] = False,
    level: Annotated[
        GulpCollabLevel, Query(description=gulp.defs.API_DESC_COLLAB_LEVEL)
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    data = {"color": color, "src": src}

    # FIXME: is duplicating events into data still needed after the latest change ?
    data["events"] = [e.to_dict() for e in events]
    try:
        l = await CollabObj.create(
            await collab_api.session(),
            token,
            req_id,
            GulpCollabType.LINK,
            ws_id=ws_id,
            operation_id=operation_id,
            context=context,
            src_file=src_file,
            name=name,
            description=description,
            events=events,
            glyph_id=glyph_id,
            data=data,
            private=private,
            level=level,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l.to_dict()))
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
