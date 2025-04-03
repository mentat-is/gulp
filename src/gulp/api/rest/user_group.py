"""
Module for user group management API endpoints.

This module provides REST API handlers for creating, updating, deleting, and managing user groups.
User groups contain collections of users who share the same permissions within the system.

The module includes endpoints for:
- Creating new user groups
- Updating existing user group properties
- Deleting user groups
- Retrieving user group details
- Listing user groups with optional filtering
- Adding users to groups
- Removing users from groups

All endpoints require administrative permissions for access.
"""

from typing import Annotated, Optional

import muty.string
from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID, GulpUserGroup
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies

router: APIRouter = APIRouter()


async def _add_remove_user(
    sess: AsyncSession,
    token: str,
    group_id: str,
    user_id: str,
    add: bool,
) -> GulpUserGroup:
    """
    add or remove an user from a group

    Args:
        sess (AsyncSession): sqlalchemy session
        token (str): admin token
        group_id (str): group id
        user_id (str): user id
        add (bool): add or remove

    Returns:
        GulpUserGroup: the updated group
    """

    # check admin token
    await GulpUserSession.check_token(
        sess, token, permission=[GulpUserPermission.ADMIN]
    )

    # get user group
    group: GulpUserGroup = await GulpUserGroup.get_by_id(
        sess, group_id
    )

    # add/remove user
    if add:
        await group.add_user(sess, user_id)
    else:
        await group.remove_user(sess, user_id)

    return group


@router.post(
    "/user_group_create",
    tags=["user_group"],
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
                        "data": GulpUserGroup.example(),
                    }
                }
            }
        }
    },
    summary="creates a user group.",
    description="""
an `user group` is a group of users sharing `permissions`: adding an user to an `user group` grants the user the same permissions of the group.

- `token` needs `admin` permission.
""",
)
async def user_group_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    name: Annotated[
        str,
        Depends(APIDependencies.param_display_name),
    ],
    permission: Annotated[
        list[GulpUserPermission],
        Body(description="One or more permissions for the group."),
    ],
    description: Annotated[
        str,
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    d = {
        "name": name,
        "description": description,
        "permission": permission,
        "glyph_id": glyph_id,
    }
    try:
        d = await GulpUserGroup.create(
            token,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            object_data=d,
            permission=[GulpUserPermission.ADMIN],
            obj_id=muty.string.ensure_no_space_no_special(name.lower()),
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/user_group_update",
    tags=["user_group"],
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
                        "data": GulpUserGroup.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing user_group.",
    description="""
this function only updates the group properties, to add or remove users use `add_user`, `remove_user`.

- `token` needs `admin` permission.
""",
)
async def user_group_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    group_id: Annotated[str, Depends(APIDependencies.param_group_id)],
    permission: Annotated[
        Optional[list[GulpUserPermission]],
        Body(description="One or more permissions for the group."),
    ] = None,
    description: Annotated[
        str,
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any([permission, description, glyph_id]):
            raise ValueError(
                "At least one of description, glyph_id, or permission must be provided."
            )
        d = {}
        d["permission"] = permission
        d["description"] = description
        d["glyph_id"] = glyph_id
        d = await GulpUserGroup.update_by_id(
            token,
            group_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            d=d,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/user_group_delete",
    tags=["user_group"],
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
                        "data": {"id": "obj_id"},
                    }
                }
            }
        }
    },
    summary="deletes a user_group.",
    description="""
- users in the group are **not deleted**.
- `token` needs `admin` permission.
""",
)
async def user_group_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    group_id: Annotated[str, Depends(APIDependencies.param_group_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if group_id.lower() == ADMINISTRATORS_GROUP_ID:
            raise ValueError("cannot delete the administrators group.")

        await GulpUserGroup.delete_by_id(
            token,
            group_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSendResponse.success(req_id=req_id, data={"id": group_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/user_group_get_by_id",
    tags=["user_group"],
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
                        "data": GulpUserGroup.example(),
                    }
                }
            }
        }
    },
    summary="gets a user_group.",
    description="""
- `token` needs `admin` permission.
""",
)
async def user_group_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    group_id: Annotated[str, Depends(APIDependencies.param_group_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpUserGroup.get_by_id_wrapper(
            token,
            group_id,
            recursive=True,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/user_group_list",
    tags=["user_group"],
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
                            GulpUserGroup.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list user groups, optionally using a filter.",
    description="""
- `token` needs `admin` permission.
""",
)
async def user_group_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpUserGroup.get_by_filter_wrapper(
            token,
            flt,
            recursive=True,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/user_group_add_user",
    tags=["user_group"],
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
                            GulpUserGroup.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="adds an user to the user group.",
    description="""
- `token` needs `admin` permission.
""",
)
async def user_group_add_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    group_id: Annotated[str, Depends(APIDependencies.param_group_id)],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _add_remove_user(sess, token, group_id, user_id, add=True)
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/user_group_remove_user",
    tags=["user_group"],
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
                            GulpUserGroup.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="removes an user from the user group.",
    description="""
- `token` needs `admin` permission.
""",
)
async def user_group_remove_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    group_id: Annotated[str, Depends(APIDependencies.param_group_id)],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _add_remove_user(sess, token, group_id, user_id, add=False)
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
