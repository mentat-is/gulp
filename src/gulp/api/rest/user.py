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

    name: str = Field(..., description="the name of the login method.")

    login: GulpAPIMethod = Field(..., description="the login method.")
    logout: GulpAPIMethod = Field(..., description="the logout method.")


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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
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
    r: Request,
    user_id: Annotated[
        str,
        Body(description="user ID for authentication", examples=["admin"]),
    ],
    password: Annotated[
        str, Body(description="password for authentication.", examples=["admin"])
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ip: str = r.client.host if r.client else "unknown"
    params = locals()
    params.pop("r", None)  # Remove Request object from params for logging
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
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
                        "id": s.user_id,
                        "time_expire": s.time_expire,
                    },
                )
            )
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

a `GulpUserLoginLogoutPacket` with `login: false` is sent on the `ws_id` websocket.
""",
)
async def logout_handler(
    r: Request,
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ip: str = r.client.host if r.client else "unknown"
    params = locals()
    params.pop("r", None)  # Remove Request object from params for logging
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id
            s: GulpUserSession = await GulpUserSession.check_token(sess, token)
            await GulpUser.logout(sess, s, ws_id=ws_id, req_id=req_id, user_ip=ip)

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"id": s.user_id, "token": token},
                )
            )
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
                    data=user.to_dict(exclude_none=True),
                )
            )
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
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


async def _get_session_and_user(
    sess: AsyncSession, token: str, user_id: Optional[str]
) -> tuple[str, GulpUserSession, GulpUser]:
    """
    helper to get the session, user session and user object, checking permissions.

    If `user_id` is None, the user_id from the token is used.

    Returns:
    - user_id
    - GulpUserSession
    - GulpUser

    Raises:
    - MissingPermission: if the token does not have permission to access the user.
    - ObjectNotFound: if the user does not exist.
    - Exception: for other errors.

    """
    s: GulpUserSession = await GulpUserSession.check_token(sess, token)
    if not user_id:
        MutyLogger.get_instance().debug(
            "user_id not specified, using token user_id %s" % (s.user_id)
        )
        user_id = s.user_id

    # get the user
    u: GulpUser = await GulpUser.get_by_id(sess, user_id)
    if s.user_id != u.id and not s.user.is_admin():
        raise MissingPermission(
            "only admin can access other users data (user_id=%s, requested user id=%s)."
            % (s.user_id, user_id)
        )
    return user_id, s, u


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
    user_id: Annotated[
        Optional[str],
        Query(
            description="an user to update: if not set, the token user is used instead."
        ),
    ] = None,
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
    user_data: Annotated[
        dict,
        Body(
            description="user data to set.",
            examples=[{"data1": "abcd", "data2": 1234, "data3": [1, 2, 3]}],
        ),
    ] = None,
    merge_user_data: Annotated[
        bool,
        Query(
            description="if `true` (default), `user_data` is merged with the existing, if any. Either, it is replaced.",
            example=True,
        ),
    ] = True,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if (
            password is None
            and permission is None
            and email is None
            and glyph_id is None
            and user_data is None
        ):
            raise ValueError(
                "at least one of password, permission, email, user_data or glyph_id must be specified."
            )

        async with GulpCollab.get_instance().session() as sess:
            user_id, s, u = await _get_session_and_user(sess, token, user_id)

            d = {}
            if password:
                d["password"] = password
            if permission:
                d["permission"] = permission
            if email:
                d["email"] = email
            if glyph_id:
                d["glyph_id"] = glyph_id
            if user_data:
                if merge_user_data:
                    dd = u.user_data if u.user_data else {}
                    MutyLogger.get_instance().debug("existing user data=%s" % (dd))
                    dd.update(user_data)
                    d["user_data"] = dd
                    MutyLogger.get_instance().debug(
                        "provided user_data=%s, updated user data=%s"
                        % (user_data, d["user_data"])
                    )
                else:
                    d["user_data"] = user_data

            await u.update_user(sess, user_session=s, **d)

            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=u.to_dict(exclude_none=True))
            )
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check acl
            s: GulpUserSession = await GulpUserSession.check_token(
                sess,
                token,
            )
            return JSendResponse.success(req_id=req_id, data=s.time_expire)
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            user_id, _, u = await _get_session_and_user(sess, token, user_id)
            return JSendResponse.success(
                req_id=req_id, data=u.to_dict(exclude_none=True)
            )
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            user_id, s, u = await _get_session_and_user(sess, token, user_id)

            # get data
            user_data: dict = u.user_data if u.user_data else {}
            MutyLogger.get_instance().debug(
                "existing user data=%s"
                % (orjson.dumps(user_data, option=orjson.OPT_INDENT_2).decode())
            )

            # update
            user_data[key] = value
            MutyLogger.get_instance().debug(
                "new user data=%s"
                % (orjson.dumps(user_data, option=orjson.OPT_INDENT_2).decode())
            )
            d = {"user_data": user_data}
            await u.update_user(sess, user_session=s, **d)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            user_id, _, u = await _get_session_and_user(sess, token, user_id)

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
            return JSONResponse(JSendResponse.success(req_id=req_id, data=user_data))
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
                        "data": "deleted_key",
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            user_id, s, u = await _get_session_and_user(sess, token, user_id)

            # get data
            user_data: dict = u.user_data if u.user_data else {}
            MutyLogger.get_instance().debug(
                "existing user data=%s"
                % (orjson.dumps(user_data, option=orjson.OPT_INDENT_2).decode())
            )
            if key:
                # delete only the requested key
                if key not in user_data:
                    raise ObjectNotFound(
                        "key %s not found in user_data for user %s" % (key, u.id)
                    )
                del user_data[key]
                d = {"user_data": user_data}
                await u.update_user(sess, user_session=s, **d)
                return JSONResponse(JSendResponse.success(req_id=req_id, data=key))

            # delete all
            d = {"user_data": {}}
            await u.update_user(sess, user_session=s, **d)
            return JSONResponse(JSendResponse.success(req_id=req_id, data={}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
