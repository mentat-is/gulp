"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import re
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
from pydantic import AfterValidator

import gulp.api.collab_api as collab_api
import gulp.config as config
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.session import GulpUserSession
from gulp.api.collab.user import GulpUser
from gulp.defs import InvalidArgument

_app: APIRouter = APIRouter()


def _pwd_regex_validator(value: str) -> str:
    """
    Validates a password against the password regex.

    Args:
        value (str): The password to validate.
    """
    if config.debug_allow_insecure_passwords():
        return value

    r = re.match(gulp.defs.REGEX_PASSWORD, value)
    assert r is not None, "password does not meet requirements."
    return value


@_app.put(
    "/user_update",
    tags=["user"],
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
                            "id": 3,
                            "name": "ingest",
                            "pwd_hash": "6eb7f2ea8ffbb37f44d41bdc3382d193c3de752f89d5bafe7b85afc93a65c32b",
                            "glyph_id": 1,
                            "email": None,
                            "time_last_login": 1707735259672,
                            "permission": 8,
                        },
                    }
                }
            }
        }
    },
    summary="updates an existing user on the platform.",
)
async def user_update_handler(
    token: Annotated[
        str,
        Header(
            description=gulp.defs.API_DESC_TOKEN
            + " (must be ADMIN if user_id != token.user_id)."
        ),
    ],
    user_id: Annotated[
        int,
        Query(
            description="if set, the ID of the user to update. either, token.user_id is used."
        ),
    ] = None,
    password: Annotated[
        str,
        Query(
            description="new password, leave empty to keep the old one.",
            annotation=Annotated[str, AfterValidator(_pwd_regex_validator)],
        ),
    ] = None,
    permission: Annotated[
        GulpUserPermission,
        Query(
            description="new permission, leave empty to keep the old one (needs ADMIN token)."
        ),
    ] = None,
    email: Annotated[
        str, Query(description="new email, leave empty to keep the old one.")
    ] = None,
    glyph_id: Annotated[
        int, Query(description="new glyph ID, leave empty to keep the old one.")
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if (
            password is None
            and permission is None
            and email is None
            and glyph_id is None
        ):
            raise InvalidArgument(
                "at least one of password, permission, email or glyph_id must be specified."
            )

        user = await GulpUser.update_by_id(
            await collab_api.session(),
            token,
            user_id,
            password,
            email,
            permission,
            glyph_id,
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=user.to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/user_list",
    tags=["user"],
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
                                "id": 1,
                                "name": "admin",
                                "pwd_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
                                "glyph_id": 1,
                                "email": None,
                                "time_last_login": 1707735259570,
                                "permission": 16,
                            },
                            {
                                "id": 3,
                                "name": "ingest",
                                "pwd_hash": "6eb7f2ea8ffbb37f44d41bdc3382d193c3de752f89d5bafe7b85afc93a65c32b",
                                "glyph_id": 1,
                                "email": None,
                                "time_last_login": 1707735259672,
                                "permission": 8,
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="lists available users, optionally using a filter.",
    description="available filters: id, name, limit, offset.",
)
async def user_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admin can list users
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        users = await GulpUser.get(await collab_api.session(), flt)
        l = []
        for u in users:
            l.append(u.to_dict())

        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/user_get_by_id",
    tags=["user"],
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
                            "name": "admin",
                            "pwd_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
                            "glyph_id": 1,
                            "email": None,
                            "time_last_login": 1707735259570,
                            "permission": 16,
                        },
                    }
                }
            }
        }
    },
    summary="get an existing user.",
)
async def user_get_by_id_handler(
    token: Annotated[
        str,
        Header(
            description=gulp.defs.API_DESC_TOKEN
            + " (must be ADMIN if user_id != token.user_id)."
        ),
    ],
    user_id: Annotated[
        int, Query(description="if None, user_id is taken from token.user_id.")
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # check if token has permission over user_id
        u = GulpUser.check_token_owner(await collab_api.session(), token, user_id)
        user_id = u.id
        users = await GulpUser.get(
            await collab_api.session(), GulpCollabFilter(id=[user_id])
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=users[0].to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/user_create",
    tags=["user"],
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
                            "name": "admin",
                            "pwd_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
                            "glyph_id": 1,
                            "email": None,
                            "time_last_login": 1707735259570,
                            "permission": 16,
                        },
                    }
                }
            }
        }
    },
    summary="create an user on the platform.",
)
async def user_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    username: Annotated[
        str,
        Query(
            description="username for the new user.", pattern=gulp.defs.REGEX_USERNAME
        ),
    ],
    password: Annotated[
        str,
        Query(
            description="password for the new user",
            annotation=Annotated[str, AfterValidator(_pwd_regex_validator)],
        ),
    ],
    permission: Annotated[
        GulpUserPermission,
        Query(
            description="permission for the new user, can be any combination of GulpUserPermission flags."
        ),
    ] = GulpUserPermission.READ,
    email: Annotated[str, Query(description="email for the new user.")] = None,
    glyph_id: Annotated[int, Query(description="glyph ID for the new user.")] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admin can create users
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        user = await GulpUser.create(
            await collab_api.session(),
            username,
            password,
            email,
            permission,
            glyph_id,
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=user.to_dict()))


@_app.delete(
    "/user_delete",
    tags=["user"],
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
    summary="deletes an existing user.",
)
async def user_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    user_id: Annotated[int, Query(description="id of the user to be deleted.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admin can delete users
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        await GulpUser.delete(await collab_api.session(), user_id)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"id": user_id})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/login",
    response_model=JSendResponse,
    tags=["user"],
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
                            "user_id": 1,
                            "token": "6c40c48a-f504-48ac-93fc-63948ec0c9cf",
                            "time_expire": 1707470710830,
                            "data": None,
                        },
                    }
                }
            }
        }
    },
    summary="login on the platform (creates a session).",
)
async def session_create_handler(
    username: Annotated[str, Query(description="username for authentication.")],
    password: Annotated[str, Query(description="password for authentication.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        _, s = await GulpUser.login(await collab_api.session(), username, password)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=s.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/logout",
    tags=["user"],
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
                            "token": "6c40c48a-f504-48ac-93fc-63948ec0c9cf",
                        },
                    }
                }
            }
        }
    },
    summary="logout a logged user (deletes a session).",
)
async def session_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        session_id = await GulpUser.logout(await collab_api.session(), token)
        return JSONResponse(
            muty.jsend.success_jsend(
                req_id=req_id, data={"id": session_id, "token": token}
            )
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
