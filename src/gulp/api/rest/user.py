"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import re
from typing import Annotated, Optional
from gulp.api.collab.structs import (
    GulpUserPermission,
    MissingPermission,
)
from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from muty.jsend import JSendException, JSendResponse
from fastapi import Depends
from gulp.api.rest.server_utils import APIDependencies, ServerUtils
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendResponse
from gulp.api.rest import defs as api_defs
from gulp.config import GulpConfig

router = APIRouter()


def _pwd_regex_validator(value: str) -> str:
    """
    Validates a password against the password regex.

    Args:
        value (str): The password to validate.

    Returns:
        str: The password if it is valid.
    """
    if GulpConfig.get_instance().debug_allow_insecure_passwords():
        return value

    r = re.match(api_defs.REGEX_CHECK_PASSWORD, value)
    assert r is not None, "password does not meet requirements."
    return value


def _email_regex_validator(value: Optional[str]) -> Optional[str]:
    """
    Validates an email against the email regex.

    Args:
        value (Optional[str]): The email to validate.

    Returns:
        Optional[str]: The email if it is valid.
    """
    if value is None:
        return None

    if not re.match(api_defs.REGEX_CHECK_EMAIL, value):
        raise ValueError(f"invalid email format: {value}")
    return value


@router.put(
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
                            "token": "6c40c48a-f504-48ac-93fc-63948ec0c9cf",
                            "user_id": "admin",
                            "time_expire": 1707470710830,
                        },
                    }
                }
            }
        }
    },
    summary="login on the platform.",
    description="""
an `user_session` object is created on the `collab` database to represent a logged user.

the returned `token` is then used in all other API calls to authenticate the user.

### websocket

a `GulpUserLoginLogoutPacket` with `login: true` is sent on the `ws_id` websocket.

### configuration

related configuration parameters:

- `debug_allow_insucure_passwords`: if set to `true`, the password regex is not enforced.
- `debug_allow_any_token_as_admin`: if set to `true`, token check is skipped and an `admin` token is generated.
- `token_ttl`: the time-to-live of the token in milliseconds.
- `token_admin_ttl`: the time-to-live of the token in milliseconds for admin users.

refer to `gulp_cfg_template.json` for more information.
""",
)
async def login_handler(
    user_id: Annotated[
        str,
        Depends(APIDependencies.param_user_id),
    ],
    password: Annotated[
        str, Query(description="password for authentication.", example="admin")
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)]=None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            s = await GulpUser.login(
                sess, user_id=user_id, password=password, ws_id=ws_id, req_id=req_id
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={
                        "token": s.id,
                        "user_id": s.user_id,
                        "time_expire": s.time_expire,
                    },
                )
            )
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.put(
    "/logout",
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
                        "data": {"user_id": "admin"},
                    }
                }
            }
        }
    },
    summary="logout user from the platform.",
    description="""
the `user_session` object corresponding to `token` is deleted from the `collab` database.

### websocket

a `GulpUserLoginLogoutPacket` with `login: false` is sent on the `ws_id` websocket.
""",
)
async def logout_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id
            s: GulpUserSession = await GulpUserSession.check_token(sess, token)
            await GulpUser.logout(sess, s, ws_id=ws_id, req_id=req_id)

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={
                        "user_id": s.user_id,
                    },
                )
            )
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.post(
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
                        "timestamp_msec": 1732901220291,
                        "req_id": "test_req",
                        "data": {
                            "pwd_hash": "c48b4df565b0c96f84fedf18f26596ed40aa9f46f11021af7125d34d1d3acffe",
                            "permission": ["read", "edit"],
                            "email": "user@mail.com",
                            "time_last_login": 0,
                            "id": "pippo",
                            "type": "user",
                            "owner_user_id": "pippo",
                            "granted_user_ids": [],
                            "granted_user_group_ids": [],
                            "time_created": 1732901220265,
                            "time_updated": 1732901220265,
                        },
                    }
                }
            }
        }
    },
    summary="creates an user on the platform.",
    description="""
- `token` needs **admin** permission.
    """,
)
async def user_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    user_id: Annotated[
        str,
        Query(
            description="user id.",
            pattern=api_defs.REGEX_CHECK_USERNAME,
            example="user",
        ),
    ],
    password: Annotated[
        str,
        Depends(APIDependencies.param_password),
    ],
    permission: Annotated[
        list[GulpUserPermission],
        Depends(APIDependencies.param_permission),
    ],
    email: Annotated[
        str,
        Depends(APIDependencies.param_email_optional),
    ] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # only admin can create users
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)
            user: GulpUser = await GulpUser.create(
                sess,
                user_id,
                password,
                permission=permission,
                email=email,
                glyph_id=glyph_id,
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data=user.to_dict(exclude_none=True),
                )
            )
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.delete(
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
                            "user_id": "user",
                        },
                    }
                }
            }
        }
    },
    summary="deletes an existing user.",
    description="""
- `token` needs **admin** permission.
    """,
)
async def user_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    user_id: Annotated[
        str,
        Depends(APIDependencies.param_user_id),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if user_id == "admin":
            raise ValueError('user "admin" cannot be deleted!')

        async with GulpCollab.get_instance().session() as sess:
            # only admin can delete users
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)
            user: GulpUser = await GulpUser.get_by_id(
                sess, user_id, with_for_update=True
            )
            await user.delete(sess)
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"user_id": user_id},
                )
            )

    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.patch(
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
                        "timestamp_msec": 1732908917521,
                        "req_id": "test_req",
                        "data": {
                            "pwd_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
                            "permission": ["edit", "read"],
                            "glyph_id": "d17626f3-8593-47c4-b585-4878f1ba8681",
                            "time_last_login": 1732908889572,
                            "id": "admin",
                            "type": "user",
                            "owner_user_id": "admin",
                            "granted_user_ids": [],
                            "granted_user_group_ids": [],
                            "time_created": 1732908889451,
                            "time_updated": 1732908917383,
                        },
                    }
                }
            }
        }
    },
    summary="updates an existing user on the platform.",
    description="""
- `token` needs **admin** permission if `user_id` is set and different from the token `user_id`, or if `permission` is set.
    
- `user_id` may not be set, in which case the target user is taken from `token`.

- `password`, `permission`, `email`, `glyph_id` are optional, depending on what needs to be updated, and can be set independently (**but at least one must be set**).
    """,
)
async def user_update_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id_optional)] = None,
    password: Annotated[
        str,
        Depends(APIDependencies.param_password_optional),
    ] = None,
    permission: Annotated[
        Optional[list[GulpUserPermission]],
        Depends(APIDependencies.param_permission_optional),
    ] = None,
    email: Annotated[
        str,
        Depends(APIDependencies.param_email_optional),
    ] = None,
    glyph_id: Annotated[str, Query(description="new user glyph id.")] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if (
            password is None
            and permission is None
            and email is None
            and glyph_id is None
        ):
            raise ValueError(
                "at least one of password, permission, email or glyph_id must be specified."
            )

        async with GulpCollab.get_instance().session() as sess:
            from gulp.api.collab.user_session import GulpUserSession

            s: GulpUserSession = await GulpUserSession.check_token(sess, token)
            d = {}
            if password:
                d["password"] = password
            if permission:
                d["permission"] = permission
            if email:
                d["email"] = email
            if glyph_id:
                d["glyph_id"] = glyph_id

            if user_id:
                # get user
                u: GulpUser = await GulpUser.get_by_id(
                    sess, user_id, with_for_update=True
                )
                if s.user_id != u.id and not s.user.is_admin():
                    raise MissingPermission("only admin can update other users.")                
                await u.update(sess, d, user_session=s)
            else:
                # use token user
                u = s.user
                await u.update(sess, d, user_session=s)

            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=u.to_dict(exclude_none=True))
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
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
                                "pwd_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
                                "permission": ["admin", "read"],
                                "glyph_id": "720fb3e5-06c2-4117-acf0-45d1fc07a983",
                                "time_last_login": 1732914332045,
                                "id": "admin",
                                "type": "user",
                                "owner_user_id": "admin",
                                "granted_user_ids": [],
                                "granted_user_group_ids": [],
                                "time_created": 1732914331944,
                                "time_updated": 1732914332084,
                                "groups": [
                                    {
                                        "name": "test_group",
                                        "glyph_id": None,
                                        "permission": ["admin"],
                                        "id": "d2eff6b2-394d-45b0-8f96-38536190de35",
                                        "type": "user_group",
                                        "owner_user_id": "admin",
                                        "granted_user_ids": [],
                                        "granted_user_group_ids": [],
                                        "time_created": 1732914332098,
                                        "time_updated": 1732914332098,
                                    }
                                ],
                                "session": {
                                    "user_id": "admin",
                                    "time_expire": 0,
                                    "id": "e9ed18b4-a3e2-453f-b346-e353f7993dd9",
                                    "type": "user_session",
                                    "owner_user_id": "admin",
                                    "granted_user_ids": [],
                                    "granted_user_group_ids": [],
                                    "time_created": 1732914355640,
                                    "time_updated": 1732914355640,
                                },
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="list users.",
    description="""
- `token` needs **admin** permission.
    """,
)
async def user_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    # only admin can get users list
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)
            users: list[GulpUser] = await GulpUser.get_by_filter(sess)
            l = []
            for u in users:
                l.append(u.to_dict(nested=True, exclude_none=True))

            return JSONResponse(JSendResponse.success(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
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
                            "pwd_hash": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
                            "permission": ["admin", "read"],
                            "glyph_id": "9efd86fb-a5f7-41f7-80b7-f1d3358b417f",
                            "time_last_login": 1732915710487,
                            "id": "admin",
                            "type": "user",
                            "owner_user_id": "admin",
                            "granted_user_ids": [],
                            "granted_user_group_ids": [],
                            "time_created": 1732915710390,
                            "time_updated": 1732915710530,
                            "groups": [
                                {
                                    "name": "test_group",
                                    "glyph_id": None,
                                    "permission": ["admin"],
                                    "id": "27554397-4950-48e4-9bc1-7c7a87a5e5d5",
                                    "type": "user_group",
                                    "owner_user_id": "admin",
                                    "granted_user_ids": [],
                                    "granted_user_group_ids": [],
                                    "time_created": 1732915710547,
                                    "time_updated": 1732915710547,
                                }
                            ],
                        },
                    }
                }
            }
        }
    },
    summary="get a single user.",
)
async def user_get_by_id(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    # check if token has permission over user_id
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token first
            s = await GulpUserSession.check_token(sess, token)

            # get user
            u = await GulpUser.get_by_id(sess, user_id)

            # check if user has permission to access the object
            if not s.user.is_admin() and s.user.id != u.id:
                raise MissingPermission("only admin can get other users.")

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id, data=u.to_dict(nested=True, exclude_none=True)
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
