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
from gulp.api.collab.base import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.session import UserSession
from gulp.api.collab.user import User
from gulp.api.collab.user_data import UserData
from gulp.defs import InvalidArgument

_app: APIRouter = APIRouter()


@_app.put(
    "/user_data_update",
    tags=["user_data"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1720260380878,
                        "req_id": "1eee5d19-6458-40f7-9d51-d3e7f60b69db",
                        "data": {
                            "id": 1,
                            "user_id": 1,
                            "name": "test userdata",
                            "operation_id": 1,
                            "data": {"my data": 12345},
                        },
                    }
                }
            }
        }
    },
    summary="updates an existing `user_data` object associated with an user.",
)
async def user_data_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    user_data_id: Annotated[
        int, Query(description="ID of the user_data object to update.")
    ],
    name: Annotated[
        str, Query(description="optional new name of the user_data object.")
    ] = None,
    data: Annotated[dict, Body()] = None,
    user_id: Annotated[
        int,
        Query(
            description="if set, the ID of the user to update `user_data` into (if different from `token.user_id`, then `token` must be an ADMIN token). either, `token.user_id` is used."
        ),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if name is None and data is None:
            raise InvalidArgument("at least one of name, data must be specified.")

        ud = await UserData.update(
            await collab_api.collab(),
            token,
            user_data_id,
            name=name,
            data=data,
            user_id=user_id,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ud.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/user_data_list",
    tags=["user_data"],
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
                            {
                                "status": "success",
                                "timestamp_msec": 1720260380878,
                                "req_id": "1eee5d19-6458-40f7-9d51-d3e7f60b69db",
                                "data": {
                                    "id": 1,
                                    "user_id": 1,
                                    "name": "test userdata",
                                    "operation_id": 1,
                                    "data": {"my data": 12345},
                                },
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="lists available `user_data` (each associated with an user and operation), optionally using a filter.",
    description="available filters: id, owner_id, name, operation_id, limit, offset.",
)
async def user_data_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admin can list users
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        user_datas = await UserData.get(await collab_api.collab(), flt)
        l = []
        for u in user_datas:
            l.append(u.to_dict())

        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/user_data_get_by_id",
    tags=["user_data"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1720260380878,
                        "req_id": "1eee5d19-6458-40f7-9d51-d3e7f60b69db",
                        "data": {
                            "id": 1,
                            "user_id": 1,
                            "name": "test userdata",
                            "operation_id": 1,
                            "data": {"my data": 12345},
                        },
                    }
                }
            }
        }
    },
    summary="get an existing `user_data`.",
)
async def user_data_get_by_id_handler(
    token: Annotated[
        str,
        Header(description=gulp.defs.API_DESC_TOKEN),
    ],
    user_data_id: Annotated[
        int, Query(description="ID of the user_data object to get.")
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # get the specified user data
        user = await User.check_token_owner(await collab_api.collab(), token)
        user_datas = await UserData.get(
            await collab_api.collab(),
            GulpCollabFilter(id=[user_data_id], owner_id=[user.id]),
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=user_datas[0].to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/user_data_create",
    tags=["user_data"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1720260380878,
                        "req_id": "1eee5d19-6458-40f7-9d51-d3e7f60b69db",
                        "data": {
                            "id": 1,
                            "user_id": 1,
                            "name": "test userdata",
                            "operation_id": 1,
                            "data": {"my data": 12345},
                        },
                    }
                }
            }
        }
    },
    summary="create an `user_data` object and associate it with an existing `user`.",
)
async def user_data_create_handler(
    token: Annotated[
        str,
        Header(
            description="the token of the user to associate the new `user_data` with."
        ),
    ],
    name: Annotated[
        str, Query(description="name for the new `user_data` object (must be unique).")
    ],
    operation_id: Annotated[int, Query(description=gulp.defs.API_DESC_OPERATION)],
    data: Annotated[dict, Body()],
    user_id: Annotated[
        int,
        Query(
            description="if set, the ID of the user to associate this data with (if different from `token.user_id`, then `token` must be an ADMIN token). either, `token.user_id` is used."
        ),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        ud = await UserData.create(
            await collab_api.collab(), token, name, operation_id, data, user_id
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ud.to_dict()))


@_app.delete(
    "/user_data_delete",
    tags=["user_data"],
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
                        },
                    }
                }
            }
        }
    },
    summary="deletes an existing `user_data` and removes association with `user`.",
)
async def user_data_delete_handler(
    token: Annotated[
        str,
        Header(
            description="the token of the user to delete(=unassociate) `user_data_id` from."
        ),
    ],
    user_data_id: Annotated[
        int, Query(description="id of the `user_data` to be deleted.")
    ],
    user_id: Annotated[
        int,
        Query(
            description="if set, the ID of the user to delete this `user_data` from (if different from `token.user_id`, then `token` must be an ADMIN token). either, `token.user_id` is used."
        ),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserData.delete(await collab_api.collab(), token, user_data_id, user_id)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"id": user_data_id})
        )
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
