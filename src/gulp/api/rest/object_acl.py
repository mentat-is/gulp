"""
objects ACLs
"""

from muty.jsend import JSendException, JSendResponse
from typing import Annotated
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    MissingPermission,
)
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.note import GulpNote
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from gulp.api.rest.structs import APIDependencies
from gulp.structs import ObjectNotFound

router: APIRouter = APIRouter()


async def _modify_grants(
    sess: AsyncSession,
    object_id: str,
    object_type: GulpCollabType,
    token: str,
    user_id: str,
    add: bool,
    group: bool,
) -> GulpCollabBase:
    """
    modify grants for an object

    Args:
        sess (AsyncSession): the session
        object_id (str): the object id to modify
        object_type (GulpCollabType): the object type
        token (str): the token of the user
        user_id (str): the user id to add or remove
        add (bool): add or remove
        group (bool): is a group or is a user

    Returns:
        GulpCollabBase: the modified object
    """
    # map object type to class
    obj_class: GulpCollabBase = GulpCollabBase.object_type_to_class(object_type)
    obj: GulpCollabBase = await obj_class.get_by_id(
        sess, object_id, with_for_update=True
    )
    if not obj:
        raise ObjectNotFound(f"Object with id {object_id} not found")

    # get token session
    await GulpUserSession.check_token(
        sess,
        token,
        obj=obj,
    )

    if add:
        # add grant
        if group:
            await obj.add_group_grant(sess, user_id)
        else:
            await obj.add_user_grant(sess, user_id)
    else:
        # remove grant
        if group:
            await obj.remove_group_grant(sess, user_id)
        else:
            await obj.remove_user_grant(sess, user_id)
    return obj


async def _make_public_or_private(
    sess: AsyncSession,
    object_id: str,
    object_type: GulpCollabType,
    token: str,
    private: bool,
) -> GulpCollabBase:
    """
    set object to private or public

    Args:
        sess (AsyncSession): the session
        object_id (str): the object id to modify
        object_type (GulpCollabType): the object type
        token (str): the token of the user
        private (bool): make object private or public

    Returns:
        GulpCollabBase: the modified object
    """
    # map object type to class
    obj_class = GulpCollabBase.object_type_to_class(object_type)

    obj: GulpCollabBase = await obj_class.get_by_id(
        sess, object_id, with_for_update=True
    )
    if not obj:
        raise ObjectNotFound(f"Object with id {object_id} not found")

    # get token session
    await GulpUserSession.check_token(
        sess,
        token,
        obj=obj,
        enforce_owner=True,
    )

    # set public/private
    if private:
        await obj.make_private(sess)
    else:
        await obj.make_public(sess)
    return obj


@router.patch(
    "/object_add_granted_user",
    tags=["object_acl"],
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
                            GulpNote.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="adds an user to the object's grants, allowing object access.",
    description="""
- `token` needs to be the owner of `object_id` or have `admin` permission.
""",
)
async def object_add_granted_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    type: Annotated[
        GulpCollabType,
        Query(..., description="the object type.", example=GulpCollabType.NOTE),
    ],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _modify_grants(
                sess, object_id, type, token, user_id, add=True, group=False
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/object_remove_granted_user",
    tags=["object_acl"],
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
                            GulpNote.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="remove user from the object's grant.",
    description="""
- `token` needs to be the owner of `object_id` or have `admin` permission.
""",
)
async def object_remove_granted_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    type: Annotated[
        GulpCollabType,
        Query(..., description="the object type.", example=GulpCollabType.NOTE),
    ],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _modify_grants(
                sess, object_id, type, token, user_id, add=False, group=False
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/object_add_granted_group",
    tags=["object_acl"],
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
                            GulpNote.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="adds an user to the object's grants, allowing object access.",
    description="""
- `token` needs to be the owner of `object_id` or have `admin` permission.
""",
)
async def object_add_granted_group_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    type: Annotated[
        GulpCollabType,
        Query(..., description="the object type.", example=GulpCollabType.NOTE),
    ],
    group_id: Annotated[str, Query(..., description="the group id to add.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _modify_grants(
                sess, object_id, type, token, group_id, add=True, group=True
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/object_remove_granted_group",
    tags=["object_acl"],
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
                            GulpNote.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="remove group from the object's grant.",
    description="""
- `token` needs to be the owner of `object_id` or have `admin` permission.
""",
)
async def object_remove_granted_group_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    type: Annotated[
        GulpCollabType,
        Query(..., description="the object type.", example=GulpCollabType.NOTE),
    ],
    group_id: Annotated[str, Query(..., description="the group id to remove.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _modify_grants(
                sess, object_id, type, token, group_id, add=False, group=True
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/object_make_private",
    tags=["object_acl"],
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
                            GulpNote.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="make the object *private*.",
    description="""
a private object is only accessible by the owner or by administrators.

- `token` needs to be the owner of `object_id` or have `admin` permission.
""",
)
async def object_make_private(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    type: Annotated[
        GulpCollabType,
        Query(..., description="the object type.", example=GulpCollabType.NOTE),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _make_public_or_private(
                sess, object_id=object_id, object_type=type, token=token, private=True
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/object_make_public",
    tags=["object_acl"],
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
                            GulpNote.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="make the object *private*.",
    description="""
a public object is accessible by anyone.

- by default, objects are accessible to anyone.
- `token` needs to be the owner of `object_id` or have `admin` permission.
""",
)
async def object_make_private(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    type: Annotated[
        GulpCollabType,
        Query(..., description="the object type.", example=GulpCollabType.NOTE),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj = await _make_public_or_private(
                sess, object_id=object_id, object_type=type, token=token, private=False
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
