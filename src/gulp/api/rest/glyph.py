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
from fastapi import APIRouter, Body, File, Header, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.glyph import Glyph
from gulp.api.collab.session import UserSession

_app: APIRouter = APIRouter()


@_app.post(
    "/glyph_create",
    response_model=JSendResponse,
    tags=["glyph"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                            "name": "testglyph",
                            "img": "base64encodedimage",
                        },
                    }
                }
            }
        }
    },
    summary="creates a glyph to use with the collaboration API.",
)
async def glyph_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    glyph: Annotated[UploadFile, File(description="the glyph file to be uploaded.")],
    name: Annotated[str, Query(description="the name of the glyph.")] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        data = glyph.file.read()
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        g = await Glyph.create(await collab_api.collab(), data, name)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=g.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/glyph_update",
    response_model=JSendResponse,
    tags=["glyph"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                            "name": "testglyph",
                            "img": "base64encodedimage",
                        },
                    }
                }
            }
        }
    },
    summary="updates a glyph.",
)
async def glyph_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    glyph_id: Annotated[int, Query(description=gulp.defs.API_DESC_GLYPH)],
    glyph: Annotated[UploadFile, File(description="the glyph file to be uploaded.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        data = glyph.file.read()
        glyph = await Glyph.update(await collab_api.collab(), glyph_id, data)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=glyph.to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/glyph_list",
    response_model=JSendResponse,
    tags=["glyph"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": [
                            {"id": 1, "name": "testglyph", "img": "base64encodedimage"}
                        ],
                    }
                }
            }
        }
    },
    summary="list available glyphs, optionally using a filter.",
    description="available filters: name, id, opt_basic_fields_only, limit, offset.",
)
async def glyph_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
        l = await Glyph.get(await collab_api.collab(), flt)
        ll = []
        for g in l:
            if isinstance(g, Glyph):
                ll.append(g.to_dict())
            else:
                ll.append(g)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/glyph_get_by_id",
    response_model=JSendResponse,
    tags=["glyph"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                            "name": "testglyph",
                            "img": "base64encodedimage",
                        },
                    }
                }
            }
        }
    },
    summary="get a glyph by id.",
)
async def glyph_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    glyph_id: Annotated[int, Query(description=gulp.defs.API_DESC_GLYPH)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
        l = await Glyph.get(await collab_api.collab(), GulpCollabFilter(id=[glyph_id]))
        g = l[0].to_dict()
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=g))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/glyph_delete",
    response_model=JSendResponse,
    tags=["glyph"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701266243057,
                        "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
                        "data": {"id": 1},
                    }
                }
            }
        }
    },
    summary="deletes a glyph.",
)
async def glyph_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    glyph_id: Annotated[int, Query(description=gulp.defs.API_DESC_GLYPH)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        await Glyph.delete(await collab_api.collab(), glyph_id)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data={"id": glyph_id}))


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
