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
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpCollabFilter, GulpCollabType, GulpUserPermission
from gulp.api.collab.base import GulpCollabObject
from gulp.api.collab.session import GulpUserSession
from gulp.utils import GulpLogger

_app: APIRouter = APIRouter()


async def collabobj_delete(
    token: str, req_id: str, t: GulpCollabType, collab_obj_id: int, ws_id: str = None
) -> JSendResponse:
    """
    A helper function to delete a collab object.
    """
    try:
        await GulpCollabObject.delete(
            await collab_api.session(),
            token,
            req_id,
            collab_obj_id,
            t,
            ws_id=ws_id,
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"id": collab_obj_id})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


async def collabobj_list(
    token, req_id: str, t: GulpCollabType = None, flt: GulpCollabFilter = None
) -> JSendResponse:
    """
    A helper function to list collab objects.

    examples flt:

    {
        # event ids
        "events": [
            "event3", "event5"
        ],
        "opt_time_start_end_events": true
    }

    {
        # time range in "events"
        "time_start": 3,
        "time_end": 5,
        "opt_time_start_end_events": true
    }

    """
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        ll = []
        if flt is None:
            flt = GulpCollabFilter()
        if t is not None:
            flt.type = [t]
        if flt.limit is None or flt.limit == 0:
            flt.limit = 1000
            GulpLogger().warning("collabobj_list: setting limit to 1000 (default)")

        l = await GulpCollabObject.get(await collab_api.session(), flt)
        for n in l:
            if isinstance(n, GulpCollabObject):
                ll.append(n.to_dict())
            else:
                ll.append(n)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


async def collabobj_get_by_id(
    token, req_id: str, t: GulpCollabType, object_id: int
) -> JSendResponse:
    """
    A helper function to get a collab object by ID.
    """
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        flt = GulpCollabFilter()
        flt.id = [object_id]
        if t is not None:
            flt.type = [t]
        l = await GulpCollabObject.get(await collab_api.session(), flt)
        o = l[0].to_dict()
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/collab_edits_get_by_object",
    tags=["collab_utility"],
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
                            "CollabEdits",
                        ],
                    }
                }
            }
        }
    },
    summary="get editors for the specified collab object.",
)
async def collab_edits_get_by_object_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    collab_obj_id: Annotated[
        int, Query(description="id of the collab object to get edits information for.")
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        l = await GulpCollabObject.get_edits_by_object(
            await collab_api.session(), collab_obj_id
        )
        ll = []
        for e in l:
            ll.append(e.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/collab_edits_list",
    tags=["collab_utility"],
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
                            "CollabEdits",
                        ],
                    }
                }
            }
        }
    },
    summary="get editors, optionallyu using a filter.",
    description="available filters: owner_id(=editor user id), time_created_start(=creation time of the edit), time_created_end(=creation time of the edit)",
)
async def collab_edits_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        l = await GulpCollabObject.get_edits(await collab_api.session(), flt)
        ll = []
        for e in l:
            ll.append(e.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/collab_raw_list",
    tags=["collab_utility"],
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
    summary="get arbitrary(raw) collab objects, optionally using a filter.",
    description="available filters: id, owner_id, type, operation_id, context, src_file, name, text, time_start, time_end, opt_basic_fields_only, limit, offset",
)
async def collab_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        l = await GulpCollabObject.get(await collab_api.session(), flt)
        ll = []
        for e in l:
            ll.append(e.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ll))
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
