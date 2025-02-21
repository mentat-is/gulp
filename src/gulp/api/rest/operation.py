"""
gulp operations rest api
"""

from typing import Annotated, Optional

import muty.string
from fastapi import APIRouter, Body, Depends, File, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest.test_values import TEST_INDEX, TEST_OPERATION_ID
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
- if not set, `index` is set as `operation_id` and it is created using the default template: to specify another template, create index first using `opensearch_create_index` and then create the operation with such `index`.
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
              
`index` is **created** if it doesn't exist, and **recreated** if it exists.""",
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
            description="if set, default grants (READ access to default users) are set for the operation. Defaults to `True`."
        ),
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        operation_id = muty.string.ensure_no_space_no_special(name.lower())
        if not index:
            index = operation_id

        # fail if the operation already exists
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(
                sess, operation_id, throw_if_not_found=False
            )
            if op:
                raise ObjectAlreadyExists(
                    f"operation_id={operation_id} already exists."
                )

        # recreate the index first
        await GulpOpenSearch.get_instance().datastream_create(index)

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
            d["granted_user_group_ids"] = ["administrators"]
        try:
            dd = await GulpOperation.create(
                token,
                ws_id=None,  # do not propagate on the websocket
                req_id=req_id,
                object_data=d,
                permission=[GulpUserPermission.INGEST],
                id=operation_id,
            )
        except Exception as exx:
            # fail, delete the previously created index
            await GulpOpenSearch.get_instance().datastream_delete(index)
            raise exx

        return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    from gulp.api.collab.user_session import GulpUserSession

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
            op: GulpOperation = await GulpOperation.get_by_id(
                sess, operation_id, with_for_update=True
            )

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
                    d["operation_data"] = {**op.operation_data, **operation_data}
                else:
                    # replace
                    d["operation_data"] = operation_data

            # update
            await op.update(
                sess,
                d,
                ws_id=None,  # do not propagate on the websocket
                req_id=req_id,
                user_id=user_id,
            )

            d = op.to_dict(nested=True)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


async def operation_reset_internal(operation_id: str, owner_id: str = None) -> None:
    """
    reset an operation (internal usage only)

    - if the operation exists, delete all data on the associated index
    - if the operation does not exist, create it and create the associated index

    Args:
        operation_id (str): the operation
        owner_id (str, optional): the owner of the operation to set if the operation do not exist. Defaults to None (set to "admin")
    """

    async with GulpCollab.get_instance().session() as sess:
        # get operation if exists
        op: GulpOperation = await GulpOperation.get_by_id(
            sess, operation_id, throw_if_not_found=False
        )

        if op:
            # operation exists
            index = op.index
            description = op.description
            glyph_id = op.glyph_id
            user_grants = op.granted_user_ids
            group_grants = op.granted_user_group_ids
            operation_data = op.operation_data
            owner_id = op.owner_user_id
            MutyLogger.get_instance().warning(
                "operation=%s exists: %s" % (operation_id, op)
            )
            # delete data by operation
            await GulpOpenSearch.get_instance().delete_data_by_operation(
                index, operation_id
            )

            # delete operation (cascading delete of all related data)
            await op.delete(sess)
            await sess.commit()
        else:
            # operation must be created anew
            index = operation_id
            description = None
            glyph_id = None
            user_grants = ["admin", "guest", "ingest", "power", "editor"]
            group_grants = ["administrators"]
            operation_data = {}
            MutyLogger.get_instance().debug(
                "operation_reset, creating new operation=%s!" % (operation_id)
            )

            # recreate the index
            MutyLogger.get_instance().info(
                "operation_reset, re/creating index=%s ..." % (index)
            )
            await GulpOpenSearch.get_instance().datastream_create(index)

        # recreate operation
        MutyLogger.get_instance().info(
            "operation_reset, re/creating operation=%s, index=%s"
            % (operation_id, index)
        )

        d = {
            "index": index,
            "name": operation_id,
            "description": description,
            "glyph_id": glyph_id,
            "operation_data": operation_data,
            "granted_user_ids": user_grants,
            "granted_user_group_ids": group_grants,
        }
        await GulpOperation._create(
            sess,
            d,
            id=operation_id,
            owner_id=owner_id or "admin",
            ws_id=None,
            private=False,
        )


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
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.post(
    "/operation_reset",
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
    summary="resets an operation.",
    description="""
this is a shortcut to recreate an existing operation, or create a fresh one with default grants if the operation doesn't exists.

- `token` needs `ingest` permission.
- `index` belonging to the operation is deleted and recreated on gulp's OpenSearch
- `granted_user_ids`, `granted_group_ids`, `glyph_id`, `description`, `operation_data` are kept if the operation exists.
- all operation related objects (i.e. `notes`, `requests`, ...) on the collab database is deleted.

""",
)
async def operation_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
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
            owner_id: str = op.owner_user_id

        await operation_reset_internal(operation_id, owner_id=owner_id)
        return JSendResponse.success(req_id=req_id, data={"id": operation_id})

    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
            nested=True,
        )
        if get_count:
            # also get count
            d["doc_count"] = await GulpOpenSearch.get_instance().datastream_get_count(
                d["index"]
            )

        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
            nested=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        flt = GulpCollabFilter(operation_ids=[operation_id])
        d = await GulpContext.get_by_filter_wrapper(
            token,
            flt,
            nested=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        flt = GulpCollabFilter(operation_ids=[operation_id], context_ids=[context_id])
        d = await GulpSource.get_by_filter_wrapper(token, flt)
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
        raise JSendException(req_id=req_id, ex=ex) from ex
