"""
gulp operations rest api
"""

from muty.jsend import JSendException, JSendResponse
from typing import Annotated, Optional
from fastapi import APIRouter, Body, Depends, File, Query, UploadFile
from fastapi.responses import JSONResponse
from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from muty.log import MutyLogger
import muty.string

from gulp.api.rest.structs import APIDependencies

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
    summary="creates a operation.",
    description="""
- `token` needs `ingest` permission.
""",
)
async def operation_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    name: Annotated[
        str,
        Depends(APIDependencies.param_display_name),
    ],
    index: Annotated[
        str,
        Query(description="the Gulp's OpenSearch index to associate with the operation.")
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
        "index": index,
        "name": name,
        "description": description,
        "glyph_id": glyph_id,
        "operation_data": {}
    }
    try:
        # check if index exists
        await GulpOpenSearch.get_instance().create_index(index)
        d = await GulpOperation.create(
            token,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            object_data=d,
            permission=[GulpUserPermission.INGEST],
            id=muty.string.ensure_no_space_no_special(name.lower()),
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
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
        dict,
        Body(description="arbitrary operation data.",
             examples=[{"op": "data"}])] = None,
    merge_operation_data: Annotated[
        Optional[bool],
        Query(
            description="if `True`, `operation_data` will be merged with the existing data, if set. Either, it will be replaced."
        )] = True,
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
            s: GulpUserSession = await GulpUserSession.check_token(sess, token, permission=[GulpUserPermission.INGEST])
            user_id = s.user_id

            # get the operation to be updated
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id, with_for_update=True)

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
                    d["operation_data"] = {
                        **op.operation_data, **operation_data}
                else:
                    # replace
                    d["operation_data"] = operation_data

            # update
            await op.update(sess,
                            d,
                            ws_id=None,  # do not propagate on the websocket
                            req_id=req_id,
                            user_id=user_id)

            d = op.to_dict(nested=True)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@ router.delete(
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
    index: Annotated[str, Depends(
        APIDependencies.param_index_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if delete_data and not index:
            raise ValueError(
                "If `delete_data` is set, `index` must be provided.")

        await GulpOperation.delete_by_id(
            token,
            operation_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            permission=[GulpUserPermission.INGEST],
        )

        if delete_data:
            # delete all data
            MutyLogger.get_instance().info(
                f"deleting data related to operation_id={
                    operation_id} on index={index} ..."
            )
            await GulpOpenSearch.get_instance().delete_data_by_operation(
                index, operation_id
            )

        return JSendResponse.success(req_id=req_id, data={"id": operation_id})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@ router.get(
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
    summary="gets an operation.",
    description="""
""",
)
async def operation_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpOperation.get_by_id_wrapper(
            token,
            operation_id,
            nested=True,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@ router.post(
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


@ router.get(
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


@ router.delete(
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
    index: Annotated[str, Depends(
        APIDependencies.param_index_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if delete_data and not index:
            raise ValueError(
                "If `delete_data` is set, `index` must be provided.")

        await GulpContext.delete_by_id(
            token,
            context_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            permission=[GulpUserPermission.INGEST],
        )

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


@ router.get(
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
        flt = GulpCollabFilter(
            operation_ids=[operation_id], context_ids=[context_id])
        d = await GulpSource.get_by_filter_wrapper(token, flt)
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@ router.delete(
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
    index: Annotated[str, Depends(
        APIDependencies.param_index_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if delete_data and not index:
            raise ValueError(
                "If `delete_data` is set, `index` must be provided.")

        await GulpSource.delete_by_id(
            token,
            source_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            permission=[GulpUserPermission.INGEST],
        )

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
