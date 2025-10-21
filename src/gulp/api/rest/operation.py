"""
gulp operations rest api
"""

import asyncio
from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
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
            example="test_operation",
        ),
    ],
    description: Annotated[
        Optional[str],
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
    index_template: Annotated[
        Optional[dict],
        Body(
            description="if set, the custom `index template` to use (refer to https://docs.opensearch.org/docs/latest/im-plugin/index-templates/)"
        ),
    ] = None,
    create_index: Annotated[
        Optional[bool],
        Query(
            description="if `True`, re/create the corresponding OpenSearch index (will be overwritten if exists). Defaults to `True`."
        ),
    ] = True,
    operation_data: Annotated[
        dict, Body(description="arbitrary operation data.", examples=[{"op": "data"}])
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.INGEST
            )

            d: dict = await GulpOperation.create_operation(
                sess,
                name,
                s.user.id,
                description=description,
                glyph_id=glyph_id,
                create_index=create_index,
                set_default_grants=set_default_grants,
                index_template=index_template,
                operation_data=operation_data or {},
                fail_if_exists=False,
            )

            return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
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
        Optional[str],
        Query(
            description="the new index to be set for the operation (must exist on OpenSearch)."
        ),
    ],
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    operation_data: Annotated[
        dict, Body(description="arbitrary operation data.", examples=[{"op": "data"}])
    ] = None,
    merge_operation_data: Annotated[
        Optional[bool],
        Query(
            description="if `True`, `operation_data` will be merged with the existing data, if set. Either, it will be replaced."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any([index, description, glyph_id, operation_data]):
            raise ValueError(
                "At least one of index, description, operation_data or glyph_id must be provided."
            )
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation
            _, op, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, permission=GulpUserPermission.INGEST
            )

            # update
            if index:
                op.index = index
            if description:
                op.description = description
            if glyph_id:
                op.glyph_id = glyph_id
            if operation_data:
                op_data: dict = op.operation_data or {}
                if merge_operation_data:
                    # merge with existing
                    for k, v in operation_data.items():
                        op_data[k] = v
                else:
                    # replace
                    op_data = operation_data
                op.operation_data = op_data

            dd: dict = await op.update(sess)
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
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="also deletes the related data on the given opensearch `index`."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation
            s: GulpUserSession
            s, op, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, permission=GulpUserPermission.INGEST
            )

            index = op.index
            user_id = s.user.id

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
            await op.delete(sess, ws_id=ws_id, req_id=req_id, user_id=user_id)

            return JSendResponse.success(req_id=req_id, data={"id": operation_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/operation_cleanup",
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
                        "deleted": 123,
                    }
                }
            }
        }
    },
    summary="cleanup operation of collab objects and stats.",
    description="""
used to clear the operation of collab objects (i.e. `note`, `link`, ...) without deleting it, not touching tables like `source`, `context`, `user`, etc.
- `token` needs `admin` permission.
""",
)
async def operation_cleanup_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    additional_tables: Annotated[
        list[str],
        Body(
            description="list of additional database tables to clear.",
            example=["custom_table"],
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check operation access
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, permission=GulpUserPermission.ADMIN
            )

            # these are the tables to cleanup
            to_clear: list[str] = ["highlight", "note", "link", "request_stats"]
            if additional_tables:
                to_clear.extend(additional_tables)

            # cleanup
            deleted: int = 0
            for t in to_clear:
                obj_class: GulpCollabBase = GulpCollabBase.object_type_to_class(t)
                flt: GulpCollabFilter = GulpCollabFilter(operation_ids=[operation_id])
                deleted += await obj_class.delete_by_filter(
                    sess, flt=flt, user_id=s.user_id, throw_if_not_found=False
                )
                MutyLogger.get_instance().info(
                    "deleted %d objects from table=%s for operation_id=%s, user_id=%s.",
                    deleted,
                    t,
                    operation_id,
                    s.user_id,
                )
                await asyncio.sleep(0.1)  # yield to event loop
            return JSendResponse.success(req_id=req_id, data={"deleted": deleted})
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpOperation
            _, obj, _ = await GulpOperation.get_by_id_wrapper(
                sess,
                token,
                operation_id,
                recursive=True,
            )
            d = obj.to_dict(exclude_none=True)

            if get_count:
                # also get count
                d["doc_count"] = (
                    await GulpOpenSearch.get_instance().datastream_get_count(d["index"])
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        flt = GulpCollabFilter(operation_ids=[operation_id])
        d = await GulpContext.get_by_filter_wrapper(
            token,
            flt,
            operation_id=operation_id,
            recursive=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/context_get_by_id",
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
                        "data": GulpContext.example(),
                    }
                }
            }
        }
    },
    summary="gets a context.",
)
async def context_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpContext
            _, obj, _ = await GulpContext.get_by_id_wrapper(
                sess,
                token,
                obj_id,
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(exclude_none=True)
            )
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
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="also deletes the related data on the given opensearch `index`."
        ),
    ] = True,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl on it
            op: GulpOperation
            s: GulpUserSession
            ctx: GulpContext
            s, ctx, op = await GulpOperation.get_by_id_wrapper(
                sess, token, context_id, permission=GulpUserPermission.INGEST
            )
            if delete_data:
                # delete all data
                MutyLogger.get_instance().info(
                    f"deleting data related to operation_id={
                        ctx.operation_id}, context_id={context_id} on index={op.index} ..."
                )
                await GulpOpenSearch.get_instance().delete_data_by_context(
                    op.index, ctx.operation_id, context_id
                )

            # ok, delete context
            await ctx.delete(sess, ws_id=ws_id, req_id=ws_id, user_id=s.user.id)
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
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    color: Annotated[str, Depends(APIDependencies.param_color_optional)],
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ],
    fail_if_exists: Annotated[
        Optional[bool],
        Query(
            description="if set, fails if the context already exists. Defaults to `False`."
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation
            s: GulpUserSession
            s, op, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, permission=GulpUserPermission.INGEST
            )

            ctx, created = await op.add_context(
                sess,
                s.user.id,
                context_name,
                ws_id=ws_id,
                req_id=req_id,
                color=color,
                glyph_id=glyph_id,
            )
            if not created and fail_if_exists:
                raise ObjectAlreadyExists(
                    f"context name={ctx.name}, id={ctx.id} already exists in operation_id={operation_id}."
                )

            return JSendResponse.success(req_id=req_id, data={"id": ctx.id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/context_update",
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
                        "data": GulpContext.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing context.",
    description="""
- `token` needs `edit` permission.
""",
)
async def context_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    description: Annotated[
        Optional[str],
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        Optional[str],
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any([color, glyph_id, description]):
            raise ValueError(
                "At least one of color, description or glyph_id must be provided."
            )

        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession
            obj: GulpContext
            s, obj, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, context_id, permission=GulpUserPermission.EDIT
            )

            # update
            if description:
                obj.description = description
            if glyph_id:
                obj.glyph_id = glyph_id
            if color:
                obj.color = color

            # update
            dd: dict = await obj.update(
                sess, ws_id=ws_id, req_id=req_id, user_id=s.user.id
            )
            return JSendResponse.success(req_id=req_id, data=dd)
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        flt = GulpCollabFilter(operation_ids=[operation_id], context_ids=[context_id])
        d = await GulpSource.get_by_filter_wrapper(
            token, flt, operation_id=operation_id
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/source_get_by_id",
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
                        "data": GulpSource.example(),
                    }
                }
            }
        }
    },
    summary="gets a source.",
)
async def source_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpSource
            _, obj, _ = await GulpSource.get_by_id_wrapper(sess, token, obj_id)
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(exclude_none=True)
            )
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
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    fail_if_exists: Annotated[
        Optional[bool],
        Query(
            description="if set, the operation fails if the source already exists. Defaults to `False`."
        ),
    ] = False,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    glyph_id: Annotated[
        str,
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession
            ctx: GulpContext

            # get context (must exist) and add source
            s, ctx, _ = await GulpContext.get_by_id_wrapper(
                sess,
                token,
                context_id,
                permission=GulpUserPermission.INGEST,
            )
            assert ctx.operation_id == operation_id

            src, created = await ctx.add_source(
                sess,
                s.user.id,
                source_name,
                ws_id=ws_id,
                req_id=req_id,
                color=color,
                glyph_id=glyph_id,
            )
            if not created and fail_if_exists:
                raise ObjectAlreadyExists(
                    f"source name={ctx.name}, id={src.id} already exists in operation_id={operation_id}, context_id={ctx.id}."
                )

            return JSendResponse.success(req_id=req_id, data={"id": src.id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/source_update",
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
                        "data": GulpSource.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing source.",
    description="""
- `token` needs `edit` permission.
""",
)
async def source_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    description: Annotated[
        Optional[str],
        Depends(APIDependencies.param_description_optional),
    ] = None,
    glyph_id: Annotated[
        Optional[str],
        Depends(APIDependencies.param_glyph_id_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any([color, glyph_id, description]):
            raise ValueError(
                "At least one of color, description or glyph_id must be provided."
            )

        async with GulpCollab.get_instance().session() as sess:
            # get source
            obj: GulpSource
            s: GulpUserSession
            s, obj, _ = await GulpSource.get_by_id_wrapper(
                sess,
                token,
                source_id,
                permission=GulpUserPermission.EDIT,
            )

            # update
            if description:
                obj.description = description
            if glyph_id:
                obj.glyph_id = glyph_id
            if color:
                obj.color = color
            dd: dict = await obj.update(
                sess, ws_id=ws_id, req_id=req_id, user_id=s.user.id
            )
            return JSendResponse.success(req_id=req_id, data=dd)
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
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    delete_data: Annotated[
        Optional[bool],
        Query(
            description="also deletes the related data on the given opensearch `index`."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl on it
            op: GulpOperation
            s: GulpUserSession
            src: GulpSource
            s, src, op = await GulpOperation.get_by_id_wrapper(
                sess, token, source_id, permission=GulpUserPermission.INGEST
            )

            if delete_data:
                # delete all data
                MutyLogger.get_instance().info(
                    f"deleting data related to operation_id={src.operation_id}, context_id={
                        src.context_id}, source_id={source_id} on index={op.index} ..."
                )
                await GulpOpenSearch.get_instance().delete_data_by_source(
                    op.index, src.operation_id, src.context_id, source_id
                )
            # ok, delete source
            await src.delete(sess, ws_id=ws_id, req_id=req_id, user_id=s.user.id)
        return JSendResponse.success(req_id=req_id, data={"id": source_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
