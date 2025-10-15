"""
objects ACLs

implements "least privilege" principle on all the gulp objects:

- owner/admin can add/remove users or groups to the object's grants
- collab objects (notes, highlights, links, stories, stored queries, ...) are public by default and can optionally be set(or created directly) private so only owner/admin can access them.
- all other objects must be added explicit grants (users or groups) to be accessed: i.e. an operation must be granted to a user or group before being "seen".
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import COLLABTYPE_NOTE, GulpCollabBase
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.structs import ObjectNotFound

router: APIRouter = APIRouter()


async def _modify_grants(
    sess: AsyncSession,
    obj_id: str,
    obj_type: str,
    token: str,
    user_id: str,
    add: bool,
    group: bool,
) -> GulpCollabBase:
    """
    modify grants for an object

    Args:
        sess (AsyncSession): the session
        obj_id (str): the object id to modify
        obj_type (str): the object collab type
        token (str): the token of the user
        user_id (str): the user id to add or remove
        add (bool): add or remove
        group (bool): is a group or is a user

    Returns:
        GulpCollabBase: the modified object
    """
    # map object type to class and get object
    obj_class: GulpCollabBase = GulpCollabBase.object_type_to_class(obj_type)
    obj: GulpCollabBase
    _, obj, _ = await obj_class.get_by_id_wrapper(
        sess, token, obj_id, enforce_owner=True
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
    obj_id: str,
    obj_type: str,
    token: str,
    private: bool,
) -> GulpCollabBase:
    """
    set object to private or public

    Args:
        sess (AsyncSession): the session
        obj_id (str): the object id to modify
        obj_type (str): the object collab type
        token (str): the token of the user
        private (bool): make object private or public

    Returns:
        GulpCollabBase: the modified object
    """
    # map object type to class and get object
    obj_class: GulpCollabBase = GulpCollabBase.object_type_to_class(obj_type)
    obj: GulpCollabBase
    _, obj, _ = await obj_class.get_by_id_wrapper(
        sess, token, obj_id, enforce_owner=True
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
    summary="adds `user_id` to the object's grants, allowing object access.",
    description="""
- `token` needs to be the owner of `obj_id` or have `admin` permission.
""",
)
async def object_add_granted_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    obj_type: Annotated[
        str,
        Query(..., description="the object collab type.", example=COLLABTYPE_NOTE),
    ],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                obj = await _modify_grants(
                    sess, obj_id, obj_type, token, user_id, add=True, group=False
                )
                return JSendResponse.success(
                    req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    summary="remove `user_id` from the object's grant.",
    description="""
- `token` needs to be the owner of `obj_id` or have `admin` permission.
""",
)
async def object_remove_granted_user_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    obj_type: Annotated[
        str,
        Query(description="the object collab type.", example=COLLABTYPE_NOTE),
    ],
    user_id: Annotated[str, Depends(APIDependencies.param_user_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                obj = await _modify_grants(
                    sess, obj_id, obj_type, token, user_id, add=False, group=False
                )
                return JSendResponse.success(
                    req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    summary="adds `group_id` to the object's grants, allowing object access.",
    description="""
- `token` needs to be the owner of `obj_id` or have `admin` permission.
""",
)
async def object_add_granted_group_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    obj_type: Annotated[
        str,
        Query(description="the object collab type.", example=COLLABTYPE_NOTE),
    ],
    group_id: Annotated[str, Query(..., description="the group id to add.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                obj = await _modify_grants(
                    sess, obj_id, obj_type, token, group_id, add=True, group=True
                )
                return JSendResponse.success(
                    req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    summary="remove `group_id` from the object's grant.",
    description="""
- `token` needs to be the owner of `obj_id` or have `admin` permission.
""",
)
async def object_remove_granted_group_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    obj_type: Annotated[
        str,
        Query(description="the object collab type.", example=COLLABTYPE_NOTE),
    ],
    group_id: Annotated[str, Query(description="the group id to remove.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                obj = await _modify_grants(
                    sess, obj_id, obj_type, token, group_id, add=False, group=True
                )
                return JSendResponse.success(
                    req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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

- `token` needs to be the owner of `obj_id` or have `admin` permission.
""",
)
async def object_make_private_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    obj_type: Annotated[
        str,
        Query(description="the object collab type.", example=COLLABTYPE_NOTE),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                obj = await _make_public_or_private(
                    sess,
                    obj_id=obj_id,
                    obj_type=obj_type,
                    token=token,
                    private=True,
                )
                return JSendResponse.success(
                    req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
                )
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    summary="make the object *public*.",
    description="""
a public object is accessible by anyone.

- a public object has no granted users and groups: `granted_user_ids` and `granted_user_group_ids` are empty lists.
- `token` needs to be the owner of `obj_id` or have `admin` permission.
""",
)
async def object_make_public_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    obj_type: Annotated[
        str,
        Query(description="the object collab type.", example=COLLABTYPE_NOTE),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    sess: AsyncSession = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                obj = await _make_public_or_private(
                    sess,
                    obj_id=obj_id,
                    obj_type=obj_type,
                    token=token,
                    private=False,
                )
                return JSendResponse.success(
                    req_id=req_id, data=obj.to_dict(nested=True, exclude_none=True)
                )

            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
