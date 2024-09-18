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

import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api import collab_api, elastic_api
from gulp.api.collab.base import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.operation import Operation
from gulp.api.collab.session import UserSession
from gulp.defs import InvalidArgument
from gulp.utils import logger

_app: APIRouter = APIRouter()


@_app.delete(
    "/operation_delete",
    response_model=JSendResponse,
    tags=["operation"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                        },
                    }
                }
            }
        }
    },
    summary="deletes the specified operation.",
    description="related *notes, highlights, links, stories, alerts* are deleted.<br>"
    "related *clients* will have their `operation_id` cleared.<br><br>"
    "if `recreate_operation` is set, the operation is recreated after deletion, and the returned `id` is the recreated operation id.",
)
async def operation_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    operation_id: Annotated[int, Query(description=gulp.defs.API_DESC_OPERATION)],
    delete_data: Annotated[
        bool, Query(description="delete related data on elasticsearch.")
    ] = False,
    recreate_operation: Annotated[
        bool, Query(description="recreate operation after delete.")
    ] = False,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    res_data = {"id": operation_id}
    try:
        # only admin can delete
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )

        # get operation
        op: Operation = None
        l = await Operation.get(
            await collab_api.collab(),
            GulpCollabFilter(id=[operation_id]),
        )
        op = l[0]

        # delete operation
        await Operation.delete(await collab_api.collab(), operation_id)

        if delete_data is not None:
            # we must also delete elasticsearch data
            logger().info(
                "deleting data related to operation_id=%d ..." % (operation_id)
            )
            await elastic_api.delete_data_by_operation(
                elastic_api.elastic(), op.index, operation_id
            )

        if recreate_operation:
            # recreate operation
            new_op = await Operation.create(
                await collab_api.collab(),
                op.name,
                op.index,
                op.description,
                op.glyph_id,
                op.workflow_id,
            )
            res_data["id"] = new_op.id
            logger().info("recreated operation %s" % (new_op))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
    return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=res_data))


@_app.post(
    "/operation_create",
    response_model=JSendResponse,
    tags=["operation"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                            "name": "testoperation",
                            "description": "test",
                            "glyph_id": 3,
                            "workflow_id": None,
                        },
                    }
                }
            }
        }
    },
    summary="creates an operation to use with the collaboration API.",
)
async def operation_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    name: Annotated[str, Query(description="the name of the operation.")],
    index: Annotated[str, Query(description="the elasticsearch index to associate.")],
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[int, Query(description="optional glyph ID to assign.")] = None,
    workflow_id: Annotated[
        int, Query(description=gulp.defs.API_DESC_WORKFLOW_ID)
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admin can create
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        o = await Operation.create(
            await collab_api.collab(), name, index, description, glyph_id, workflow_id
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/operation_update",
    response_model=JSendResponse,
    tags=["operation"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                            "name": "testoperation",
                            "description": "test",
                            "glyph_id": 3,
                            "workflow_id": None,
                        },
                    }
                }
            }
        }
    },
    summary="updates an existing operation.",
)
async def operation_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    operation_id: Annotated[int, Query(description=gulp.defs.API_DESC_OPERATION)],
    description: Annotated[str, Body()] = None,
    glyph_id: Annotated[int, Query(description="optional glyph ID to assign.")] = None,
    workflow_id: Annotated[
        int, Query(description=gulp.defs.API_DESC_WORKFLOW_ID)
    ] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if description is None and glyph_id is None and workflow_id is None:
            raise InvalidArgument(
                "at least one of description, glyph_id or workflow_id must be specified."
            )

        # only admin can update
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        o = await Operation.update(
            await collab_api.collab(), operation_id, description, glyph_id, workflow_id
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/operation_list",
    response_model=JSendResponse,
    tags=["operation"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": [
                            {
                                "id": 1,
                                "name": "testoperation",
                                "description": "test",
                                "glyph_id": 3,
                                "workflow_id": None,
                            }
                        ],
                    }
                }
            }
        }
    },
    description="available filters: name, id, index, limit, offset.",
    summary="lists existing operations, optionally using a filter.",
)
async def operation_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
        ops = await Operation.get(await collab_api.collab(), flt)
        oo = []
        for o in ops:
            oo.append(o.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=oo))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/operation_get_by_id",
    response_model=JSendResponse,
    tags=["operation"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707476531591,
                        "req_id": "6108f2aa-d73c-41fa-8bd7-04e667edf0cc",
                        "data": {
                            "id": 1,
                            "name": "testoperation",
                            "description": "test",
                            "glyph_id": 3,
                            "workflow_id": None,
                        },
                    }
                }
            }
        }
    },
    summary="get an operation.",
)
async def operation_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    operation_id: Annotated[int, Query(description=gulp.defs.API_DESC_OPERATION)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
        ops = await Operation.get(
            await collab_api.collab(), GulpCollabFilter(id=[operation_id])
        )
        op = ops[0]
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=op.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
