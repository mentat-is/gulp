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
from fastapi import APIRouter, Header, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.session import GulpUserSession

_app: APIRouter = APIRouter()


@_app.put(
    "/session_impersonate",
    tags=["session"],
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
                        "data": {
                            "id": 1,
                            "user_id": 2,
                            "token": "07c80375-629d-454f-b2a5-a9de9ef410cd",
                            "time_expire": 1708352351920,
                            "data": {"hello": "world"},
                        },
                    }
                }
            }
        }
    },
    summary="Let administrator impersonate(=login as) user.",
)
async def session_impersonate(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    user_id: Annotated[int, Query(description="id of the user to impersonate.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        s = await GulpUserSession.impersonate(await collab_api.session(), token, user_id)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=s.to_dict()))
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
