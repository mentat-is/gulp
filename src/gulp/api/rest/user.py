"""
Module that defines API endpoints for user management for the Gulp API.

This module provides RESTful API endpoints to manage users within the Gulp platform.
Functionality includes:
- User authentication (login/logout)
- User creation, deletion and updates
- User listing and retrieval
- Permission management

The module uses FastAPI for API definition and JSendResponse for standardized JSON responses.
Authentication is handled via tokens that are checked against user permissions stored in the
collaboration database.

Most operations require specific permissions, particularly admin privileges for user management
operations beyond self-management.
"""

from typing import Annotated, Any, Optional
import orjson
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
import muty.crypto
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
    MissingPermission,
)
from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import REGEX_CHECK_USERNAME, APIDependencies
from gulp.structs import GulpAPIMethod, ObjectAlreadyExists, ObjectNotFound


class GulpLoginMethod(BaseModel):
    """
    the login methods supported by Gulp.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "name": "gulp",
                    "login": {
                        "method": "POST",
                        "url": "/login",
                        "params": [
                            {
                                "name": "user_id",
                                "type": "str",
                                "location": "body",
                                "description": "the user id.",
                                "required": True,
                            },
                            {
                                "name": "password",
                                "type": "str",
                                "location": "body",
                                "description": "the password.",
                                "required": True,
                            },
                            {
                                "name": "ws_id",
                                "type": "str",
                                "description": "the websocket id.",
                                "required": True,
                            },
                            {
                                "name": "req_id",
                                "type": "str",
                                "description": "the request id.",
                                "default_value": None,
                            },
                        ],
                    },
                    "logout": {
                        "method": "POST",
                        "url": "/logout",
                        "params": [
                            {
                                "name": "token",
                                "type": "str",
                                "location": "header",
                                "description": "the login token.",
                            },
                            {
                                "name": "ws_id",
                                "type": "str",
                                "description": "the websocket id.",
                            },
                            {
                                "name": "req_id",
                                "type": "str",
                                "description": "the request id.",
                                "optional": True,
                            },
                        ],
                    },
                }
            ]
        },
    )

    name: Annotated[str, Field(description="the name of the login method.")]
    login: Annotated[GulpAPIMethod, Field(description="the login method.")]
    logout: Annotated[GulpAPIMethod, Field(description="the logout method.")]


router = APIRouter()


@router.get(
    "/get_available_login_api",
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
                        "data": [autogenerate_model_example_by_class(GulpLoginMethod)],
                    }
                }
            }
        }
    },
    summary="get the available login methods.",
    description="""
depending on the installed plugins, you may login to gulp using different methods.

this api lists the available login methods and their corresponding API endpoints.

NOTE: the `gulp` login method is always available, `extension` plugins may override `get_login_methods` to add more methods.
""",
)
async def get_available_login_api_handler(
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)],
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    return JSONResponse(
        JSendResponse.success(
            req_id=req_id, data=[autogenerate_model_example_by_class(GulpLoginMethod)]
        )
    )


@router.post(
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
                            "id": "admin",
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

a `GulpUserAccessPacket` with `login: true` is sent on the `ws_id` websocket.

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
    r: Request,
    user_id: Annotated[
        str,
        Body(description="user ID for authentication", examples=["admin"]),
    ],
    password: Annotated[
        str, Body(description="password for authentication.", examples=["admin"])
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)],
) -> JSONResponse:
    ip: str = r.client.host if r.client else "unknown"
    params = locals()
    params.pop("r", None)
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                s = await GulpUser.login(
                    sess,
                    user_id=user_id,
                    password=password,
                    ws_id=ws_id,
                    req_id=req_id,
                    user_ip=ip,
                )
                return JSONResponse(
                    JSendResponse.success(
                        req_id=req_id,
                        data={
                            "token": s.id,
                            "id": s.user.id,
                            "time_expire": s.time_expire,
                        },
                    )
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
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
                        "data": {"id": "admin", "token": "token_admin"},
                    }
                }
            }
        }
    },
    summary="logout user from the platform.",
    description="""
the `user_session` object corresponding to `token` is deleted from the `collab` database.

### websocket

a `GulpUserAccessPacket` with `login: false` is sent on the `ws_id` websocket.
""",
)
async def logout_handler(
    r: Request,
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)],
) -> JSONResponse:
    ip: str = r.client.host if r.client else "unknown"
    params = locals()
    params.pop("r", None)
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # check permission and get user id
                s: GulpUserSession = await GulpUserSession.check_token(sess, token)
                await GulpUser.logout(sess, s, ws_id=ws_id, req_id=req_id, user_ip=ip)

                return JSONResponse(
                    JSendResponse.success(
                        req_id=req_id,
                        data={"id": s.user.id, "token": token},
                    )
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
                        "data": GulpUser.example(),
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
            description="""
the new user id.

- `user_id` must be unique
""",
            pattern=REGEX_CHECK_USERNAME,
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
        Depends(APIDependencies.param_email),
    ],
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)],
    user_data: Annotated[
        dict,
        Body(
            description="user data to set.",
            examples=[{"data1": "abcd", "data2": 1234, "data3": [1, 2, 3]}],
        ),
    ] = {},
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # only admin can create users
                await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

                # check if the user already exists
                u: Optional[GulpUser] = await GulpUser.get_by_id(
                    sess, user_id, throw_if_not_found=False
                )
                if u:
                    raise ObjectAlreadyExists("user %s already exists." % user_id)

                user: GulpUser = await GulpUser.create_user(
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
                        data=user.to_dict(),
                    )
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)],
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if user_id == "admin" or user_id == "guest":
            raise MissingPermission('user "admin" and user "guest" cannot be deleted!')

        await GulpUser.delete_by_id_wrapper(
            token,
            user_id,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSONResponse(
            JSendResponse.success(
                req_id=req_id,
                data={"id": user_id},
            )
        )

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
                        "data": GulpUser.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing user on the platform.",
    description="""
- `token` needs **admin** permission if `user_id` is different from the token `user_id`, or if `permission` is set.
- if `user_id` is not set, the token user is used instead (and if so, the token must be `admin` to operate on other users).
- `password`, `permission`, `email`, `glyph_id`, `user_data` are optional depending on what needs to be updated, and can be set independently (**but at least one must be set**).
    """,
)
async def user_update_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)],
    user_id: Annotated[
        str,
        Depends(APIDependencies.param_user_id_optional),
    ],
    password: Annotated[
        str,
        Depends(APIDependencies.param_password_optional),
    ],
    permission: Annotated[
        Optional[list[GulpUserPermission]],
        Depends(APIDependencies.param_permission_optional),
    ],
    email: Annotated[
        str,
        Depends(APIDependencies.param_email_optional),
    ],
    user_data: Annotated[
        dict,
        Body(
            description="user data to set.",
            examples=[{"data1": "abcd", "data2": 1234, "data3": [1, 2, 3]}],
        ),
    ] = {},
    merge_user_data: Annotated[
        bool,
        Query(
            description="if `true` (default), `user_data` is merged with the existing, if any. Either, it is replaced.",
            example=True,
        ),
    ] = True,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if (
            not password
            and not permission
            and not email
            and not glyph_id
            and not user_data
        ):
            raise ValueError(
                "at least one of password, permission, email, user_data or glyph_id must be specified."
            )

        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession
                u: GulpUser
                if user_id:
                    # get the requested user using the given token: it will work if the token is admin or it's the same user
                    s, u, _ = await GulpUser.get_by_id_wrapper(
                        sess, token, user_id, enforce_owner=True
                    )
                else:
                    # no user_id specified, use the token user
                    s = await GulpUserSession.check_token(sess, token)
                    u = s.user

                delete_existing_user_session: bool = False
                if permission:
                    if not s.user.is_admin():
                        # only admin can change permission
                        raise MissingPermission(
                            "only admin can change permission, session_user_id=%s"
                            % (s.user.id)
                        )

                    # ensure that all users have read permission
                    if GulpUserPermission.READ not in permission:
                        permission.append(GulpUserPermission.READ)
                    delete_existing_user_session = True

                pwd_hash: str = None
                if password:
                    if not s.is_admin() and s.user.id != u.id:
                        # only admin can change password to other users
                        raise MissingPermission(
                            "only admin can change password to other users, user_id=%s, session_user_id=%s"
                            % (u.id, s.user.id)
                        )
                    pwd_hash = muty.crypto.hash_sha256(password)
                    delete_existing_user_session = True

                ud: dict = u.user_data if u.user_data else {}
                if user_data:
                    if merge_user_data:
                        # merge with existing user data
                        MutyLogger.get_instance().debug("existing user data=%s" % (ud))
                        ud.update(user_data)
                        MutyLogger.get_instance().debug(
                            "provided user_data=%s, updated user data=%s"
                            % (user_data, ud)
                        )
                    else:
                        # replace existing user data
                        ud = user_data

                if delete_existing_user_session and u.session:
                    # invalidate session for the user being updated (user must login again then)
                    MutyLogger.get_instance().warning(
                        "updated user password or permission, invalidating session for user_id=%s"
                        % (u.id)
                    )
                    await u.session.delete(sess)
                    u.session = None

                dd: dict = await u.update(
                    sess,
                    password=pwd_hash,
                    permission=permission,
                    email=email,
                    glyph_id=glyph_id,
                    user_data=ud,
                )

                return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
                        "data": [GulpUser.example()],
                    }
                }
            }
        }
    },
    summary="list users.",
    description="""
- `token` needs `admin` permission.
    """,
)
async def user_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    # only admin can get users list
    try:
        d = await GulpUser.get_by_filter_wrapper(
            token,
            flt=GulpCollabFilter(),
            permission=[GulpUserPermission.ADMIN],
            recursive=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/user_session_keepalive",
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
                        "data": 123456789,  # new expiration time in milliseconds
                    }
                }
            }
        }
    },
    summary="refreshes user's session expiration time.",
    description="""
- can be used by the client to keep the session alive.
    """,
)
async def user_session_keepalive_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    sess: AsyncSession = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # check acl
                s: GulpUserSession = await GulpUserSession.check_token(
                    sess,
                    token,
                )
                return JSendResponse.success(req_id=req_id, data=s.time_expire)
            except Exception as ex:
                if sess:
                    await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
                        "data": GulpUser.example(),
                    }
                }
            }
        }
    },
    summary="get a single user.",
    description="""
- `token` needs `admin` permission to get users other than the token user.
- if `user_id` is not set, the token user is used instead (and if so, the token must be `admin` to operate on other users).
""",
)
async def user_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    user_id: Annotated[
        Optional[str],
        Query(
            description="an user to get: if not set, the token user is used instead."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession
                u: GulpUser
                if user_id:
                    # get the requested user using the given token: it will work if the token is admin or it's the same user
                    s, u, _ = await GulpUser.get_by_id_wrapper(
                        sess, token, user_id, enforce_owner=True
                    )
                else:
                    # no user_id specified, use the token user
                    s = await GulpUserSession.check_token(sess, token)
                    u = s.user

                return JSendResponse.success(req_id=req_id, data=u.to_dict())
            except Exception as ex:
                if sess:
                    await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/user_set_data",
    tags=["user_data"],
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
                            "requested_key": "new_value",
                        },
                    }
                }
            }
        }
    },
    summary="store data private to the user.",
    description="""
`user_data` is useful to store user-related data (i.e. saved sessions, ...).

- `token` needs **admin** permission if `user_id` is different from the token `user_id`, or if `permission` is set.
- if `user_id` is not set, the token user is used instead (and if so, the token must be `admin` to operate on other users).
- this is basically a shortcut for `user_update` tailored to just update a specific `key` in the `user_data` field.
    """,
)
async def user_set_data_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    key: Annotated[
        str, Query(description="key in `user_data` to be set.", example="my_key")
    ],
    value: Annotated[
        Any, Body(description="value to be set for the given `key`.", example="my_data")
    ],
    user_id: Annotated[
        Optional[str],
        Query(
            description="an user to set data for: if not set, the token user is used instead."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession
                u: GulpUser
                if user_id:
                    # get the requested user using the given token: it will work if the token is admin or it's the same user
                    s, u, _ = await GulpUser.get_by_id_wrapper(
                        sess, token, user_id, enforce_owner=True
                    )
                else:
                    # no user_id specified, use the token user
                    s = await GulpUserSession.check_token(sess, token)
                    u = s.user

                # get data
                ud: dict = u.user_data if u.user_data else {}
                MutyLogger.get_instance().debug(
                    "existing user data=%s"
                    % (orjson.dumps(ud, option=orjson.OPT_INDENT_2).decode())
                )

                # update
                ud[key] = value
                MutyLogger.get_instance().debug(
                    "new user data=%s"
                    % (orjson.dumps(ud, option=orjson.OPT_INDENT_2).decode())
                )
                await u.update(sess, user_data=ud)
                return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
            except Exception as ex:
                if sess:
                    await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/user_get_data",
    tags=["user_data"],
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
                            "requested_key": "my_data",
                        },
                    }
                }
            }
        }
    },
    summary="get user's private data.",
    description="""
- `token` needs **admin** permission if `user_id` is different from the token `user_id`, or if `permission` is set.
- if `user_id` is not set, the token user is used instead (and if so, the token must be `admin` to operate on other users).
    """,
)
async def user_get_data_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    key: Annotated[
        Optional[str],
        Query(
            description="key in `user_data` to get: if not set, all `user_data` is retrieved.",
            example="my_key",
        ),
    ] = None,
    user_id: Annotated[
        Optional[str],
        Query(
            description="an user to get data for: if not set, the token user is used instead."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession
                u: GulpUser
                if user_id:
                    # get the requested user using the given token: it will work if the token is admin or it's the same user
                    s, u, _ = await GulpUser.get_by_id_wrapper(
                        sess, token, user_id, enforce_owner=True
                    )
                else:
                    # no user_id specified, use the token user
                    s = await GulpUserSession.check_token(sess, token)
                    u = s.user

                # get data
                user_data: dict = u.user_data if u.user_data else {}
                MutyLogger.get_instance().debug(
                    "existing user data=%s"
                    % (orjson.dumps(user_data, option=orjson.OPT_INDENT_2).decode())
                )
                if key:
                    # get only the requested key
                    if key not in user_data:
                        raise ObjectNotFound(
                            "key %s not found in user_data for user %s" % (key, u.id)
                        )
                    return JSONResponse(
                        JSendResponse.success(req_id=req_id, data={key: user_data[key]})
                    )

                # all
                return JSONResponse(
                    JSendResponse.success(req_id=req_id, data=user_data)
                )
            except Exception as ex:
                if sess:
                    await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/user_delete_data",
    tags=["user_data"],
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
                        "data": {},  # the new user_data (empty if all deleted),
                    }
                }
            }
        }
    },
    summary="delete user's private data.",
    description="""
- `token` needs **admin** permission if `user_id` is different from the token `user_id`, or if `permission` is set.
- if `user_id` is not set, the token user is used instead (and if so, the token must be `admin` to operate on other users). 
    """,
)
async def user_delete_data_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    key: Annotated[
        Optional[str],
        Query(
            description="key in `user_data` to be deleted: if not set, the whole `user_data` is cleared.",
            example="my_key",
        ),
    ] = None,
    user_id: Annotated[
        Optional[str],
        Query(
            description="an user to delete data for: if not set, the token user is used instead."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession
                u: GulpUser
                if user_id:
                    # get the requested user using the given token: it will work if the token is admin or it's the same user
                    s, u, _ = await GulpUser.get_by_id_wrapper(
                        sess, token, user_id, enforce_owner=True
                    )
                else:
                    # no user_id specified, use the token user
                    s = await GulpUserSession.check_token(sess, token)
                    u = s.user

                # get data
                ud: dict = u.user_data if u.user_data else {}
                MutyLogger.get_instance().debug(
                    "existing user data=%s"
                    % (orjson.dumps(ud, option=orjson.OPT_INDENT_2).decode())
                )
                if key:
                    # delete only the requested key
                    if key not in ud:
                        raise ObjectNotFound(
                            "key %s not found in user_data for user %s" % (key, u.id)
                        )

                    del ud[key]
                    u.user_data = ud
                    await u.update(sess)
                    return JSONResponse(JSendResponse.success(req_id=req_id, data=ud))

                # delete all
                u.user_data = {}
                await u.update(sess)
                return JSONResponse(JSendResponse.success(req_id=req_id, data={}))
            except Exception as ex:
                if sess:
                    await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
