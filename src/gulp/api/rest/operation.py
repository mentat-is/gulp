"""
gulp operations rest api
"""

import os
from typing import Annotated, Optional

import muty.file
import muty.string
import muty.uploadfile
from fastapi import APIRouter, Body, Depends, File, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest.test_values import TEST_INDEX, TEST_OPERATION_ID
from gulp.process import GulpProcess
from gulp.structs import ObjectAlreadyExists

router: APIRouter = APIRouter()


@router.post(
    "/operation_create",
    tags=["operation"],
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
                        "data": GulpOperation.example(),
                    }
                }
            }
        }
    },
    summary="creates an operation.",
    description="""
- `operation_id` is derived from `name` by removing spaces and special characters.
- if not set, `index` is set as `operation_id`.
- `token` needs `ingest` permission.
""",
)
async def operation_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    name: Annotated[
        str,
        Query(
            description="the name of the operation. It will be used to derive the `operation_id`.",
            example=TEST_OPERATION_ID,
        ),
    ],
    index: Annotated[
        str,
        Query(
            description="""
if set, the Gulp's OpenSearch index to associate with the operation (default: same as `operation_id`).

**NOTE**: `index` is **created** if it doesn't exist, and **recreated** if it exists.""",
            example=TEST_INDEX,
        ),
    ] = None,
    description: Annotated[
        str,
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    set_default_grants: Annotated[
        bool,
        Query(
            description="if set, default grants (READ access to default users) are set for the operation. Defaults to `False, this is intended mostly for DEBUGGING`."
        ),
    ] = False,
    index_template: Annotated[Optional[dict], Body(description="if set, the custom `index template` to use (refer to https://docs.opensearch.org/docs/latest/im-plugin/index-templates/)")]=None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        operation_id = muty.string.ensure_no_space_no_special(name.lower())
        if not index:
            index = operation_id

        async with GulpCollab.get_instance().session() as sess:
            # check token
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.INGEST
            )

            # fail if the operation already exists
            op: GulpOperation = await GulpOperation.get_by_id(
                sess, operation_id, throw_if_not_found=False
            )
            if op:
                raise ObjectAlreadyExists(
                    f"operation_id={operation_id} already exists."
                )

        # recreate the index first
        await GulpOpenSearch.get_instance().datastream_create_from_raw_dict(index, index_template=index_template)

        # create the operation
        d = {
            "index": index,
            "name": name,
            "description": description,
            "glyph_id": glyph_id,
            "operation_data": {},
        }
        if set_default_grants:
            MutyLogger.get_instance().info(
                "setting default grants for operation=%s" % (name)
            )
            d["granted_user_ids"] = ["admin", "guest", "ingest", "power", "editor"]
            d["granted_user_group_ids"] = [ADMINISTRATORS_GROUP_ID]
        try:
            dd = await GulpOperation.create(
                token,
                ws_id=None,  # do not propagate on the websocket
                req_id=req_id,
                object_data=d,
                permission=[GulpUserPermission.INGEST],
                obj_id=operation_id,
            )
        except Exception as exx:
            # fail, delete the previously created index
            await GulpOpenSearch.get_instance().datastream_delete(index)
            raise exx

        return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/operation_update",
    tags=["operation"],
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
                        "data": GulpOperation.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing operation.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def operation_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    index: Annotated[
        str,
        Depends(APIDependencies.param_index_optional),
    ] = None,
    description: Annotated[
        str,
        Depends(APIDependencies.param_description_optional),
    ] = None,
    operation_data: Annotated[
        dict, Body(description="arbitrary operation data.", examples=[{"op": "data"}])
    ] = None,
    merge_operation_data: Annotated[
        Optional[bool],
        Query(
            description="if `True`, `operation_data` will be merged with the existing data, if set. Either, it will be replaced."
        ),
    ] = True,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        if not any([index, description, glyph_id, operation_data]):
            raise ValueError(
                "At least one of index, description, operation_data or glyph_id must be provided."
            )
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=[GulpUserPermission.INGEST]
            )
            user_id = s.user_id

            # get the operation to be updated
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)

            # build update dict
            d = {}
            if index:
                d["index"] = index
            if description:
                d["description"] = description
            if glyph_id:
                d["glyph_id"] = glyph_id
            if operation_data:
                if merge_operation_data:
                    # merge with existing
                    if op.operation_data:
                        d["operation_data"] = {**op.operation_data, **operation_data}
                    else:
                        d["operation_data"] = operation_data
                else:
                    # replace
                    d["operation_data"] = operation_data

            # update
            dd: dict = await op.update(
                sess,
                d,
                ws_id=None,  # do not propagate on the websocket
                req_id=req_id,
                user_id=user_id,
            )
            return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/operation_delete",
    tags=["operation"],
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
    summary="deletes a operation.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def operation_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="also deletes the related data on the given opensearch `index`."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:

        # get operation
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            index = op.index

            if delete_data:
                MutyLogger.get_instance().info(
                    f"deleting data related to operation_id={
                        operation_id} on index={index} ..."
                )

                # delete the index
                await GulpOpenSearch.get_instance().datastream_delete(index)

            # delete the operation itself
            MutyLogger.get_instance().info(
                "deleting operation_id=%s ..." % operation_id
            )
            await op.delete(sess)

        return JSendResponse.success(req_id=req_id, data={"id": operation_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/operation_get_by_id",
    tags=["operation"],
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
                        "data": GulpOperation.example(),
                    }
                }
            }
        }
    },
    summary="get operation information.",
)
async def operation_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    get_count: Annotated[
        Optional[bool],
        Query(
            description="if set, the operation's document count is also retrieved as `doc_count` (default=True)."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpOperation.get_by_id_wrapper(
            token,
            operation_id,
            recursive=True,
        )
        if get_count:
            # also get count
            d["doc_count"] = await GulpOpenSearch.get_instance().datastream_get_count(
                d["index"]
            )

        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/operation_list",
    tags=["operation"],
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
                            GulpOperation.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list operations, optionally using a filter.",
    description="""
""",
)
async def operation_list_handler(
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
        d = await GulpOperation.get_by_filter_wrapper(
            token,
            flt,
            recursive=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/context_list",
    tags=["operation"],
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
                            GulpContext.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list contexts related to the given `operation_id`.",
    description="""
""",
)
async def context_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[
        Optional[str],
        Query(description="if set, only the context with this id is returned."),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        flt = GulpCollabFilter(operation_ids=[operation_id])
        if context_id:
            flt.ids = [context_id]
        d = await GulpContext.get_by_filter_wrapper(
            token,
            flt,
            recursive=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/context_delete",
    tags=["operation"],
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
    summary="deletes context in an operation, optionally deleting the related data.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def context_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="also deletes the related data on the given opensearch `index`."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            index = op.index

            # ok, delete context
            ctx: GulpContext = await GulpContext.get_by_id(sess, context_id)
            await ctx.delete(sess)

        if delete_data:
            # delete all data
            MutyLogger.get_instance().info(
                f"deleting data related to operation_id={
                    operation_id}, context_id={context_id} on index={index} ..."
            )
            await GulpOpenSearch.get_instance().delete_data_by_context(
                index, operation_id, context_id
            )

        return JSendResponse.success(req_id=req_id, data={"id": context_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/context_create",
    tags=["operation"],
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
    summary="creates a GulpContext if it does not already exists, either return the existing one's id.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def context_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_name: Annotated[
        str,
        Query(
            description="the name of the context. It will be used to derive the `context_id`.",
            example="test_context",
        ),
    ],
    ws_id: Annotated[
        Optional[str],
        Depends(APIDependencies.param_ws_id),
    ] = None,
    color: Annotated[
        str,
        Query(
            description="the color of the context. Defaults to `white`.",
            example="white",
        ),
    ] = None,
    fail_if_exists: Annotated[
        Optional[bool],
        Query(
            description="if set, the operation fails if the context already exists. Defaults to `False`."
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            user_id = s.user_id

            ctx, created = await op.add_context(
                sess, user_id, context_name, ws_id=ws_id, req_id=req_id, color=color
            )
            if not created and fail_if_exists:
                raise ObjectAlreadyExists(
                    f"context name={ctx.name}, id={ctx.id} already exists in operation_id={operation_id}."
                )

            return JSendResponse.success(req_id=req_id, data={"id": ctx.id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/source_list",
    tags=["operation"],
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
                            GulpContext.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list sources related to the given `operation_id` and `context_id`.",
    description="""
""",
)
async def source_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    source_id: Annotated[
        Optional[str],
        Query(description="if set, only the source with this id is returned."),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        flt = GulpCollabFilter(operation_ids=[operation_id], context_ids=[context_id])
        if source_id:
            flt.ids = [source_id]

        d = await GulpSource.get_by_filter_wrapper(token, flt)
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/source_create",
    tags=["operation"],
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
    summary="creates a GulpSource if it does not already exists, either return the existing one's id.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def source_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    source_name: Annotated[
        str,
        Query(
            description="the name of the source. It will be used to derive the `source_id`.",
            example="test_source",
        ),
    ],
    ws_id: Annotated[
        Optional[str],
        Depends(APIDependencies.param_ws_id),
    ] = None,
    color: Annotated[
        str,
        Query(
            description="the color of the source. Defaults to `purple`.",
            example="purple",
        ),
    ] = None,
    fail_if_exists: Annotated[
        Optional[bool],
        Query(
            description="if set, the operation fails if the source already exists. Defaults to `False`."
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            user_id = s.user_id

            # get context (must exist) and add source
            ctx: GulpContext = await GulpContext.get_by_id(sess, context_id)
            src, created = await ctx.add_source(
                sess, user_id, source_name, ws_id=ws_id, req_id=req_id, color=color
            )
            if not created and fail_if_exists:
                raise ObjectAlreadyExists(
                    f"source name={ctx.name}, id={src.id} already exists in operation_id={operation_id}, context_id={ctx.id}."
                )

            return JSendResponse.success(req_id=req_id, data={"id": src.id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/source_delete",
    tags=["operation"],
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
    summary="deletes `source` in an operation, optionally deleting the related data.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def source_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="also deletes the related data on the given opensearch `index`."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            index = op.index

            # ok, delete source
            src: GulpSource = await GulpSource.get_by_id(sess, source_id)
            await src.delete(sess)

        if delete_data:
            # delete all data
            MutyLogger.get_instance().info(
                f"deleting data related to operation_id={operation_id}, context_id={
                    context_id}, source_id={source_id} on index={index} ..."
            )
            await GulpOpenSearch.get_instance().delete_data_by_source(
                index, operation_id, context_id, source_id
            )

        return JSendResponse.success(req_id=req_id, data={"id": source_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def operation_reset(
    sess: AsyncSession, operation_id: str, user_id: str=None, delete_data: bool = False, recreate: bool = True
) -> None:
    """
    resets the operation with the given ID.

    if the operation does not exist, it is created anew.
    if the operation exists, its metadata is preserved.

    Args:
        sess: the session to use.
        operation_id: the ID of the operation to reset.
        user_id: the ID of the user who owns the operation, or "admin" if not set.
        delete_data: if true, operation documents on opensearch are deleted.
        recreate: if true, the operation is recreated.
    """
    try:
        # acquire the lock
        await GulpOperation.acquire_advisory_lock(sess, operation_id)

        if user_id is None:
            user_id = "admin"

        MutyLogger.get_instance().info("resetting operation %s, user_id=%s ..." % (operation_id, user_id))

        # check if the operation exists
        exists = False
        op: GulpOperation = await GulpOperation.get_by_id(
            sess, operation_id, throw_if_not_found=False
        )
        if op:
            # we will use this data to recreate the operation
            exists = True
            d = {
                "index": op.index,
                "name": op.name,
                "description": op.description,
                "glyph_id": op.glyph_id,
                "operation_data": op.operation_data,
                "granted_user_ids": op.granted_user_ids,
                "granted_user_group_ids": op.granted_user_group_ids,
            }
        else:
            # operation will be created anew
            d = {
                "index": operation_id,
                "name": operation_id,
                "description": "",
                "glyph_id": "book-dashed",
                "operation_data": {},
                "granted_user_ids": [],
                "granted_user_group_ids": [ADMINISTRATORS_GROUP_ID],
            }
            MutyLogger.get_instance().warning(
                "operation %s not found, will be created anew!" % (operation_id)
            )

        if delete_data and exists:
            # perform full reset (delete and recreate the operation)
            if exists:
                # delete data
                MutyLogger.get_instance().debug(
                    "deleting data for operation %s ..." % (operation_id)
                )
                await GulpOpenSearch.get_instance().delete_data_by_operation(
                    op.index, operation_id
                )

                # delete the operation
                MutyLogger.get_instance().debug(
                    "deleting operation %s ..." % (operation_id)
                )
                await op.delete(sess, ws_id=None, user_id=None, req_id=None)

        if recreate:
            # recreate the operation
            # pylint: disable=protected-access
            MutyLogger.get_instance().debug(
                "re/creating operation %s, exists=%r ..." % (operation_id, exists)
            )
            if exists and op:
                # delete the operation, will delete the related data on the collab database
                await op.delete(sess)

            await GulpOperation._create_internal(
                sess,
                d,
                obj_id=operation_id,
                owner_id=op.owner_user_id if exists else user_id,
                ws_id=None,
                private=False,
            )

            # create index on opensearch
            ds_exist = await GulpOpenSearch.get_instance().datastream_exists(
                ds=operation_id
            )
            if not ds_exist:
                await GulpOpenSearch.get_instance().datastream_create(ds=operation_id)

    finally:
        # release the lock
        await GulpOperation.release_advisory_lock(sess, operation_id)


@router.post(
    "/operation_reset",
    tags=["db"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701266243057,
                        "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
                    }
                }
            }
        }
    },
    summary="resets the operation, optionally deleting documents on OpenSearch.",
    description="""operation will be reset, deleting all the notes, highlights, links and stats.

- `token` needs to have `admin` permission.
- the operation will be recreated with the same metadata (i.e. glyph, description, granted users, ...), if it exists.
- if the operation does not exist it is created anew `using the default index template`.
""",
)
async def operation_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Query(
            description="if set, reset only affects this operation. either, affects all the operations found on the collab database.",
            example=TEST_OPERATION_ID,
        ),
    ] = None,
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="if set, the operation's data is deleted as well.",
        ),
    ] = True,
    restart_processes: Annotated[
        bool,
        Query(
            description="if true, the process pool is restarted as well.",
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )
            user_id = s.user_id
            await operation_reset(
                sess,
                operation_id=operation_id,
                user_id=user_id,
                delete_data=delete_data,
                recreate=True,
            )

        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()

        return JSONResponse(JSendResponse.success(req_id=req_id, data={"id": operation_id}))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
