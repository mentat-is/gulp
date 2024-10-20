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
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpClientType, GulpCollabFilter, GulpUserPermission
from gulp.api.collab.client import Client
from gulp.api.collab.session import UserSession
from gulp.defs import InvalidArgument

_app: APIRouter = APIRouter()


@_app.get(
    "/client_get_by_id",
    response_model=JSendResponse,
    tags=["client"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": {
                            "id": 1,
                            "name": "testclient",
                            "type": 0,
                            "operation_id": None,
                            "version": "1.0.0",
                            "glyph_id": 2,
                        },
                    }
                }
            }
        }
    },
    summary="get a client by its id.",
)
async def client_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    client_id: Annotated[int, Query(description="id of the client to be retrieved.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        clients = await Client.get(
            await collab_api.session(), GulpCollabFilter(id=[client_id])
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=clients[0].to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/client_list",
    response_model=JSendResponse,
    tags=["client"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": {
                            "total": 2,
                            "clients": [
                                {
                                    "id": "client:slurp-win_slurp_1.10",
                                    "name": "slurp-win",
                                    "client_type": "slurp",
                                    "description": "slurp agent for windows",
                                    "version": "1.10",
                                },
                                {
                                    "id": "client:test_test_1.0",
                                    "name": "test",
                                    "client_type": "test",
                                    "description": "this is a test client",
                                    "version": "1.0",
                                },
                            ],
                        },
                    }
                }
            }
        }
    },
    summary="list registered clients, optionally using a filter.",
    description="available filters: id, name, client_type, client_version, operation_id, limit, offset",
)
async def client_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
    flt: Annotated[GulpCollabFilter, Body()] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        clients = await Client.get(await collab_api.session(), flt)
        l = []
        for c in clients:
            l.append(c.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/client_create",
    tags=["client"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": [
                            {
                                "id": 1,
                                "name": "testclient",
                                "type": 0,
                                "operation_id": None,
                                "version": "1.0.0",
                                "glyph_id": 2,
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="register a client with the platform.",
)
async def client_create_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    name: Annotated[str, Query(description="client name.")],
    t: Annotated[GulpClientType, Query(description="client type.")],
    version: Annotated[str, Query(description="client version.")] = None,
    operation_id: Annotated[
        int, Query(description="operation id to associate.")
    ] = None,
    glyph_id: Annotated[int, Query(description=gulp.defs.API_DESC_GLYPH)] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admins can create clients
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        c = await Client.create(
            await collab_api.session(), name, t, operation_id, version, glyph_id
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=c.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/client_update",
    tags=["client"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": {
                            "id": 1,
                            "name": "testclient",
                            "type": 0,
                            "operation_id": None,
                            "version": "1.0.0",
                            "glyph_id": 2,
                        },
                    }
                }
            }
        }
    },
    summary="update registered client properties.",
)
async def client_update_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    client_id: Annotated[int, Query(description="id of the client to be updated.")],
    version: Annotated[str, Query(description="client version.")] = None,
    operation_id: Annotated[
        int, Query(description="operation id to associate.")
    ] = None,
    glyph_id: Annotated[int, Query(description=gulp.defs.API_DESC_GLYPH)] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if version is None and operation_id is None and glyph_id is None:
            raise InvalidArgument(
                "at least one of version, operation_id or glyph_id must be provided"
            )
        # only admins can update clients
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        c = await Client.update(
            await collab_api.session(),
            client_id,
            operation_id,
            version,
            glyph_id,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=c.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/client_delete",
    tags=["client"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": {"id": 1},
                    }
                }
            }
        }
    },
    summary="unregister a client.",
)
async def client_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    client_id: Annotated[int, Query(description="id of the client to be deleted.")],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # only admins can delete clients
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        await Client.delete(await collab_api.session(), client_id)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data={"id": client_id}))


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
