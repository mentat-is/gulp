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
from gulp.api.collab.base import GulpAssociatedEvent, GulpCollabFilter, GulpCollabType
from gulp.api.collab.base import GulpCollabObject
from gulp.defs import InvalidArgument

_app: APIRouter = APIRouter()


@_app.post(
    "/story_list",
    tags=["story"],
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
                        "data": ["CollabObj"],
                    }
                }
            }
        }
    },
    summary="list stories, optionally matching a filter.",
    description="available filters: id, owner_id, operation_id, context, src_file, name, events, limit, offset",
)
async def story_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_list(token, req_id, GulpCollabType.STORY, flt)


@_app.get(
    "/story_get_by_id",
    tags=["story"],
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
    summary="get story.",
)
async def story_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    story_id: Annotated[int, Query(description="id of the story to be retrieved.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_get_by_id(
        token, req_id, GulpCollabType.STORY, story_id
    )


@_app.delete(
    "/story_delete",
    tags=["story"],
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
    summary="deletes a story",
)
async def story_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_DELETE_EDIT_TOKEN)],
    story_id: Annotated[int, Query(description="id of the story to be deleted.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_delete(
        token, req_id, GulpCollabType.STORY, story_id, ws_id=ws_id
    )


@_app.put(
    "/story_update",
    tags=["story"],
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
    summary="updates an existing story.",
)
async def story_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    story_id: Annotated[int, Query(description="id of the story to be updated.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    events: Annotated[list[GulpAssociatedEvent], Body()] = None,
    name: Annotated[str, Query(description="optional new name.")] = None,
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[
        int,
        Query(description="optional new glyph ID to be associated with this story."),
    ] = None,
    color: Annotated[
        str,
        Query(description="optional new story color in #rrggbb or css-name format."),
    ] = None,
    private: Annotated[bool, Query(description=gulp.defs.API_DESC_PRIVATE)] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if (
            name is None
            and description is None
            and glyph_id is None
            and color is None
            and events is None
            and private is None
        ):
            raise InvalidArgument(
                "at least one of name, description, glyph_id, color, events, private must be provided"
            )

        o = await GulpCollabObject.update(
            await collab_api.session(),
            token,
            req_id,
            story_id,
            ws_id,
            events=events,
            name=name,
            description=description,
            glyph_id=glyph_id,
            data={"color": color} if color is not None else None,
            type=GulpCollabType.STORY,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/story_create",
    tags=["story"],
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
    summary="creates a story from a list of events.",
)
async def story_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    operation_id: Annotated[int, Query(description="operation related to the story.")],
    name: Annotated[str, Query(description="name for the story.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    events: Annotated[list[GulpAssociatedEvent], Body()],
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[
        int, Query(description="optional glyph ID to be associated with this story.")
    ] = None,
    color: Annotated[
        str,
        Query(
            description='optional color in #rrggbb or css-name format, default is "blue". '
        ),
    ] = "blue",
    private: Annotated[bool, Query(description=gulp.defs.API_DESC_PRIVATE)] = False,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        o = await GulpCollabObject.create(
            await collab_api.session(),
            token,
            req_id,
            GulpCollabType.STORY,
            ws_id=ws_id,
            operation_id=operation_id,
            events=events,
            name=name,
            description=description,
            glyph_id=glyph_id,
            data={"color": color} if color is not None else None,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
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
