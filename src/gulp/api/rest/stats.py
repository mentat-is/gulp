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
import gulp.structs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.session import GulpUserSession
from gulp.api.collab.stats import GulpStats

_app: APIRouter = APIRouter()


@_app.delete(
    "/stats_delete_by_operation",
    response_model=JSendResponse,
    tags=["stats"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": {"num_deleted": 1},
                    }
                }
            }
        }
    },
    summary="delete all stats for the given operation.",
)
async def stats_delete_by_operation(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_DELETE_TOKEN)],
    operation_id: Annotated[
        int, Query(description="operation id to delete stats for.")
    ],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.DELETE
        )
        deleted = await GulpStats.delete(
            await collab_api.session(),
            GulpCollabFilter(operation_id=[operation_id]),
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"num_deleted": deleted})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/stats_get_by_operation",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "status": "success",
                        "timestamp_msec": 1707768939688,
                        "req_id": "6d03a6e4-8b60-440c-a949-0dd306d832bf",
                        "data": [
                            {
                                "id": 2,
                                "type": 7,
                                "req_id": "07d41db9-48c2-4841-b076-6b8f009ea366",
                                "operation_id": 1,
                                "client_id": 1,
                                "context": "testcontext",
                                "status": 2,
                                "time_created": 1707767607561,
                                "time_expire": 1707854007561,
                                "time_update": 1707767688628,
                                "time_end": 1707767688628,
                                "ev_failed": 4,
                                "ev_skipped": 98630,
                                "ev_processed": 98633,
                                "files_processed": 24,
                                "files_total": 24,
                                "ingest_errors": {
                                    "/home/valerino/repos/gulp/samples/win_evtx/post-Security.evtx": [],
                                    "/home/valerino/repos/gulp/samples/win_evtx/security.evtx": [
                                        '[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116]   File "<string>", line 33\n[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116] lxml.etree.XMLSyntaxError: PCDATA invalid Char value 3, line 33, column 33\n'
                                    ],
                                },
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="get all stats for a given operation.",
)
async def stats_get_by_operation(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    operation_id: Annotated[
        int, Query(description="operation id to retrieve stats for.")
    ],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        stats = await GulpStats.get(
            await collab_api.session(),
            GulpCollabFilter(operation_id=[operation_id]),
        )
        l = []
        for s in stats:
            l.append(s.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/stats_get_by_req_id",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "status": "success",
                        "timestamp_msec": 1707768939688,
                        "req_id": "6d03a6e4-8b60-440c-a949-0dd306d832bf",
                        "data": {
                            "id": 2,
                            "type": 7,
                            "req_id": "07d41db9-48c2-4841-b076-6b8f009ea366",
                            "operation_id": 1,
                            "client_id": 1,
                            "context": "testcontext",
                            "status": 2,
                            "time_created": 1707767607561,
                            "time_expire": 1707854007561,
                            "time_update": 1707767688628,
                            "time_end": 1707767688628,
                            "ev_failed": 4,
                            "ev_skipped": 98630,
                            "ev_processed": 98633,
                            "files_processed": 24,
                            "files_total": 24,
                            "ingest_errors": {
                                "/home/valerino/repos/gulp/samples/win_evtx/post-Security.evtx": [],
                                "/home/valerino/repos/gulp/samples/win_evtx/security.evtx": [
                                    '[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116]   File "<string>", line 33\n[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116] lxml.etree.XMLSyntaxError: PCDATA invalid Char value 3, line 33, column 33\n'
                                ],
                            },
                        },
                    }
                }
            }
        }
    },
    summary="get a single stats for the given request id.",
)
async def stats_get_by_req_id(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    r: Annotated[str, Query(description="the request id to retrieve stats for.")],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        stats = await GulpStats.get(
            await collab_api.session(), GulpCollabFilter(req_id=[r])
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=stats[0].to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/stats_list",
    response_model=JSendResponse,
    tags=["stats"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707768939688,
                        "req_id": "6d03a6e4-8b60-440c-a949-0dd306d832bf",
                        "data": [
                            {
                                "id": 2,
                                "type": 7,
                                "req_id": "07d41db9-48c2-4841-b076-6b8f009ea366",
                                "operation_id": 1,
                                "client_id": 1,
                                "context": "testcontext",
                                "status": 2,
                                "time_created": 1707767607561,
                                "time_expire": 1707854007561,
                                "time_update": 1707767688628,
                                "time_end": 1707767688628,
                                "ev_failed": 4,
                                "ev_skipped": 98630,
                                "ev_processed": 98633,
                                "files_processed": 24,
                                "files_total": 24,
                                "ingest_errors": {
                                    "/home/valerino/repos/gulp/samples/win_evtx/post-Security.evtx": [],
                                    "/home/valerino/repos/gulp/samples/win_evtx/security.evtx": [
                                        '[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116]   File "<string>", line 33\n[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116] lxml.etree.XMLSyntaxError: PCDATA invalid Char value 3, line 33, column 33\n'
                                    ],
                                },
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="list all stats, optionally using a filter.",
    description="available filters: id, req_id, operation_id, client_id, type, context, status, time_created_start, time_created_end, limit, offset.",
)
async def stats_list_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        stats = await GulpStats.get(await collab_api.session(), flt)
        ss = []
        for s in stats:
            ss.append(s.to_dict())
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ss))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/stats_delete",
    response_model=JSendResponse,
    tags=["stats"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701267433379,
                        "req_id": "8694703f-f788-4808-8637-961562fbaf47",
                        "data": {"num_deleted": 1},
                    }
                }
            }
        }
    },
    summary="delete stats, optionally using a filter.",
    description="available filters: id, req_id, operation_id, client_id, type, context, status, time_created_start, time_created_end.",
)
async def stats_delete(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_DELETE_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.DELETE
        )
        deleted = await GulpStats.delete(await collab_api.session(), flt)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"num_deleted": deleted})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/stats_get_by_id",
    response_model=JSendResponse,
    tags=["stats"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1707768939688,
                        "req_id": "6d03a6e4-8b60-440c-a949-0dd306d832bf",
                        "data": {
                            "id": 2,
                            "type": 7,
                            "req_id": "07d41db9-48c2-4841-b076-6b8f009ea366",
                            "operation_id": 1,
                            "client_id": 1,
                            "context": "testcontext",
                            "status": 2,
                            "time_created": 1707767607561,
                            "time_expire": 1707854007561,
                            "time_update": 1707767688628,
                            "time_end": 1707767688628,
                            "ev_failed": 4,
                            "ev_skipped": 98630,
                            "ev_processed": 98633,
                            "files_processed": 24,
                            "files_total": 24,
                            "ingest_errors": {
                                "/home/valerino/repos/gulp/samples/win_evtx/post-Security.evtx": [],
                                "/home/valerino/repos/gulp/samples/win_evtx/security.evtx": [
                                    '[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116]   File "<string>", line 33\n[/home/valerino/repos/gulp/src/gulp/plugins/ingestion/win_evtx.py:ingest:116] lxml.etree.XMLSyntaxError: PCDATA invalid Char value 3, line 33, column 33\n'
                                ],
                            },
                        },
                    }
                }
            }
        }
    },
    summary="get a single stats using the id.",
)
async def stats_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    stats_id: Annotated[int, Query(description="id of the stats to retrieve")],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        s = await GulpStats.get(
            await collab_api.session(), GulpCollabFilter(id=[stats_id])
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=s[0].to_dict())
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/stats_cancel_request",
    tags=["stats"],
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
                        "data": {"id": 4},
                    }
                }
            }
        }
    },
    summary="cancel a request, setting status to CANCELED (3). The request will terminate asap.",
)
async def stats_cancel_request_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    r: Annotated[str, Query(description="req_id of the request to cancel.")],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await GulpUserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.READ
        )
        s = await GulpStats.set_canceled(await collab_api.session(), r)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data={"id": s.id}))
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
