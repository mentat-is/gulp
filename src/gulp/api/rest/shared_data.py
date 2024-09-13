"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

from typing import Annotated

import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import APIRouter, Body, Header, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.api.rest.collab_utility as collab_utility
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpCollabFilter, GulpCollabType
from gulp.api.collab.collabobj import CollabObj
from gulp.defs import InvalidArgument

_app: APIRouter = APIRouter()


@_app.get(
    "/shared_data_get_by_id",
    tags=["shared_data"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1702980175195,
                        "req_id": "71202fa9-0a47-4485-bbee-067e68929cdd",
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="shortcut to get a single shared-data object by id.",
)
async def shared_data_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    obj_id: Annotated[int, Query(description="id of the shared-data to be retrieved.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_get_by_id(token, req_id, None, obj_id)


@_app.post(
    "/shared_data_list",
    tags=["shared_data"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1702980175195,
                        "req_id": "71202fa9-0a47-4485-bbee-067e68929cdd",
                        "data": ["CollabObj"],
                    }
                }
            }
        }
    },
    summary="list shared-data, optionally using a filter.",
    description="available filters: id, owner_id, operation_id, name, opt_basic_fields_only, limit, offset.",
)
async def shared_data_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_list(token, req_id, flt=flt)


@_app.delete(
    "/shared_data_delete",
    tags=["shared_data"],
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
                        "data": {"id": 1},
                    }
                }
            }
        }
    },
    summary="deletes a shared-data object.",
)
async def shared_data_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_DELETE_EDIT_TOKEN)],
    obj_id: Annotated[int, Query(description="id of the shared-data to be deleted.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await collab_utility.collabobj_delete(token, req_id, None, obj_id, ws_id=ws_id)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/shared_data_update",
    tags=["shared_data"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="updates a shared-data object.",
)
async def shared_data_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    obj_id: Annotated[int, Query(description="id of the shared-data to be updated.")],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    data: Annotated[dict, Body()] = None,
    operation_id: Annotated[
        int,
        Query(description="optional operation to be associated with this shared-data."),
    ] = None,
    private: Annotated[bool, Query(description=gulp.defs.API_DESC_PRIVATE)] = False,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if data is None and operation_id is None:
            raise InvalidArgument("at least one of data, operation_id must be set.")
        d = await CollabObj.update(
            await collab_api.collab(),
            token,
            req_id,
            obj_id,
            ws_id,
            operation_id=operation_id,
            data=data,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=d.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/shared_data_create",
    tags=["shared_data"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="creates a shared-data object with arbitrary application/user defined data.",
)
async def shared_data_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    name: Annotated[str, Query(description="the shared-data (unique) name.")],
    t: Annotated[
        GulpCollabType, Query(description="one of the available shared data types.")
    ],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    data: Annotated[dict, Body()],
    operation_id: Annotated[
        int,
        Query(description="optional operation to be associated with this shared-data."),
    ] = None,
    private: Annotated[bool, Query(description=gulp.defs.API_DESC_PRIVATE)] = False,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        d = await CollabObj.create(
            await collab_api.collab(),
            token,
            req_id,
            t,
            ws_id=ws_id,
            name=name,
            operation_id=operation_id,
            data=data,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=d.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/sigma_group_filter_create",
    tags=["shared_data"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="shortcut to shared_data_create to create a SigmaGroupFilter expression.",
)
async def sigma_group_filter_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    name: Annotated[str, Query(description="i.e. the attack common name")],
    expr: Annotated[str, Body()],
    operation_id: Annotated[
        int,
        Query(description="optional operation to be associated with this shared-data."),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        data: dict = {"expr": expr}
        return await shared_data_create_handler(
            token,
            name,
            GulpCollabType.SHARED_DATA_SIGMA_GROUP_FILTER,
            data,
            operation_id,
            req_id,
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/sigma_group_filter_update",
    tags=["shared_data"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="shortcut to shared_data_update to update a sigma group filter.",
)
async def sigma_group_filter_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    obj_id: Annotated[
        int, Query(description="id of the sigma group filter to be updated.")
    ],
    expr: Annotated[str, Body()] = None,
    operation_id: Annotated[
        int,
        Query(description="optional operation to be associated with this shared-data."),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    data: dict = {"expr": expr}
    return await shared_data_update_handler(token, obj_id, data, operation_id, req_id)


@_app.post(
    "/workflow_create",
    tags=["shared_data"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="shortcut to shared_data_create to create a workflow.",
)
async def workflow_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    name: Annotated[str, Query(description="name of the workflow.")],
    workflow: Annotated[dict, Body()],
    operation_id: Annotated[
        int,
        Query(description="optional operation to be associated with this shared-data."),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        data: dict = workflow
        return await shared_data_create_handler(
            token, name, GulpCollabType.SHARED_DATA_WORKFLOW, data, operation_id, req_id
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/workflow_update",
    tags=["shared_data"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="shortcut to shared_data_update to update a workflow.",
)
async def workflow_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    obj_id: Annotated[int, Query(description="id of the workflow to be updated.")],
    workflow: Annotated[dict, Body()] = None,
    operation_id: Annotated[
        int,
        Query(description="optional operation to be associated with this shared-data."),
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    return await shared_data_update_handler(
        token, obj_id, workflow, operation_id, req_id
    )


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
