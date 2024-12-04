"""
gulp user groups managementrest api
"""

from muty.jsend import JSendException, JSendResponse
from typing import Annotated, Optional
from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from gulp.api.collab.user_group import GulpUserGroup
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import (
    APIDependencies,
    ServerUtils,
)
import muty.string

router: APIRouter = APIRouter()


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

- token needs `admin` permission.
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
            id=muty.string.ensure_no_space_no_special(name.lower()),
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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

- token needs `admin` permission.
""",
)
async def user_group_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
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
    ServerUtils.dump_params(locals)
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
            object_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            d=d,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
- token needs `admin` permission.
""",
)
async def user_group_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpUserGroup.delete_by_id(
            token,
            object_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSendResponse.success(req_id=req_id, data={"id": object_id})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
- token needs `admin` permission.
""",
)
async def user_group_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpUserGroup.get_by_id_wrapper(
            token,
            object_id,
            nested=True,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
- token needs `admin` permission.
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
            nested=True,
            permission=[GulpUserPermission.ADMIN],
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
- token needs `admin` permission.
""",
)
async def user_group_add_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check admin token
            s = await GulpUserSession.check_token(
                sess, token, permission=[GulpUserPermission.ADMIN]
            )

            # get user group
            group: GulpUserGroup = await GulpUserGroup.get_by_id(sess, object_id, with_for_update=True)

            # add user
            await group.add_user(sess, user_id)

        return JSendResponse.success(
            req_id=req_id, data=group.to_dict(nested=True, exclude_none=True)
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
- token needs `admin` permission.
""",
)
async def user_group_remove_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check admin token
            s = await GulpUserSession.check_token(
                sess, token, permission=[GulpUserPermission.ADMIN]
            )

            # get user group
            group: GulpUserGroup = await GulpUserGroup.get_by_id(sess, object_id, with_for_update=True)

            # delete user
            await group.remove_user(sess, user_id)

        return JSendResponse.success(
            req_id=req_id, data=group.to_dict(nested=True, exclude_none=True)
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
