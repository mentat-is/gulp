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
from fastapi import APIRouter, BackgroundTasks, Body, Header, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab.db as collab_db
import gulp.api.collab_api as collab_api
from gulp.api.elastic.structs import GulpQueryFilter
import gulp.api.elastic_api as elastic_api
import gulp.api.rest_api as rest_api
import gulp.config as config
import gulp.defs
import gulp.plugin
import gulp.utils
import gulp.workers as workers
from gulp.api.collab.base import GulpUserPermission
from gulp.api.collab.session import UserSession

_app: APIRouter = APIRouter()


@_app.post(
    "/elastic_rebase_index",
    response_model=JSendResponse,
    tags=["db"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "pending",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                    }
                }
            }
        }
    },
    summary="rebases index/datastream to a different time.",    
    description="rebases `index` and creates a new `dest_index` with rebased `@timestamp` + `offset`.",    
)
async def rebase_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_EDIT_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    dest_index: Annotated[
        str,
        Query(
            description="name of the destination index.",
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    offset_msec: Annotated[
        int,
        Query(
            description="offset, in milliseconds from unix epoch, to be added to the `@timestamp` field.<br>"
                "to subtract, **use a negative offset.<br><br>"
                "rebasing happens in background and **uses one of the worker processess**: when it is done, a REBASE_DONE event is sent to the websocket.",
        )
    ],        
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    flt: Annotated[GulpQueryFilter, Body()] = None,
    max_total_fields: Annotated[
        int, Query(description="The maximum number of fields in the index.")
    ] = 10000,
    refresh_interval_msec: Annotated[
        int, Query(description="The index refresh interval in milliseconds.")
    ] = 5000,
    force_date_detection: Annotated[
        bool, Query(description="Force date detection in the index.")
    ] = False,
    event_original_text_analyzer: Annotated[
        str, Query(description="The event.original text analyzer.")
    ] = "standard",
    
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    # print parameters
    rest_api.logger().debug(
        "rebasing index=%s to dest_index=%s with offset=%d, ws_id=%s, flt=%s" % (index, dest_index, offset_msec, ws_id, flt)
    )
    req_id = gulp.utils.ensure_req_id(req_id)
    if index == dest_index:
        raise JSendException(req_id=req_id, ex=Exception("index and dest_index should be different!"))
        
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.EDIT
        )
        coro = workers.rebase_task(await collab_api.collab(), elastic_api.elastic(), rest_api.process_executor(), 
                                index=index, dest_index=dest_index, offset=offset_msec, ws_id=ws_id, flt=flt, req_id=req_id,
                                max_total_fields=max_total_fields, refresh_interval_msec=refresh_interval_msec,
                                force_date_detection=force_date_detection, event_original_text_analyzer=event_original_text_analyzer)
        await rest_api.aiopool().spawn(coro)
        
        # return pending
        return muty.jsend.pending_jsend(req_id=req_id)          
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

@_app.get(
    "/elastic_list_index",
    tags=["db"],
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
                            {
                                "name": "testidx",
                                "indexes": [
                                    {
                                        "index_name": ".ds-testidx-000001",
                                        "index_uuid": "W0hPc9nVS6qbCSy8m2WvFA",
                                    }
                                ],
                                "template": "testidx-template",
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="lists all existing datastreams on OpenSearch, with their backing indexes.",
)
async def elastic_list_index_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        l: list[str] = await elastic_api.datastream_list(elastic_api.elastic())
        # rest_api.logger().debug("datastreams=%s" % (l))
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/elastic_init",
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
                        "data": {"index": "testidx"},
                    }
                }
            }
        }
    },
    summary="(re)creates a datastream on OpenSearch, erasing all data.",
    description="an underlying backing index will be (re)created as well.<br>"
    "**WARNING**: if the datastream already exists, it is **DELETED** first together with its backing index(=all data on it is **deleted**).",
)
async def elastic_init_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    index: Annotated[str, Query(description=gulp.defs.API_DESC_INDEX)],
    max_total_fields: Annotated[
        int, Query(description="The maximum number of fields in the index.")
    ] = 10000,
    refresh_interval_msec: Annotated[
        int, Query(description="The index refresh interval in milliseconds.")
    ] = 5000,
    force_date_detection: Annotated[
        bool, Query(description="Force date detection in the index.")
    ] = False,
    event_original_text_analyzer: Annotated[
        str, Query(description="The event.original text analyzer.")
    ] = "standard",
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        await elastic_api.datastream_create(
            elastic_api.elastic(),
            index,
            max_total_fields=max_total_fields,
            refresh_interval_msec=refresh_interval_msec,
            force_date_detection=force_date_detection,
            event_original_text_analyzer=event_original_text_analyzer,
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"index": index})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/elastic_delete_index",
    response_model=JSendResponse,
    tags=["db"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701266243057,
                        "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
                        "data": {"index": "testidx"},
                    }
                }
            }
        },
    },
    summary="deletes an OpenSearch datastream together with its backing index.",
    description="**WARNING: all data on the index will be erased!**",
)
async def elastic_delete_index_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    index: Annotated[str, Query(description=gulp.defs.API_DESC_INDEX)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)

    try:
        # check token permission first
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        await elastic_api.datastream_delete(elastic_api.elastic(), index)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data={"index": index}))


@_app.get(
    "/elastic_get_mapping",
    tags=["db"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1715940385449,
                        "req_id": "45db2622-dd8d-43c2-ab7b-d60af82fa114",
                        "data": {
                            "@timestamp": "date",
                            "gulp.event.code": "long",
                            "event.sequence": "long",
                            "agent.build.original": "keyword",
                            "agent.ephemeral_id": "keyword",
                            "agent.id": "keyword",
                            "agent.name": "keyword",
                            "agent.type": "keyword",
                            "agent.version": "keyword",
                            "client.address": "keyword",
                            "client.as.number": "long",
                        },
                    }
                }
            }
        }
    },
    summary="get fields mapping for a given index or datastream on OpenSearch.",
)
async def elastic_get_mapping_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[str, Query(description=gulp.defs.API_DESC_INDEX)],
    return_raw_result: Annotated[
        bool, Query(description="if true, the raw result is returned.")
    ] = False,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.collab(), token)
        m = await elastic_api.index_get_mapping(
            elastic_api.elastic(), index, return_raw_result
        )
        # m = await elastic_api.datastream_get_mapping(elastic_api.elastic(), n)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=m))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/elastic_get_mapping_by_source",
    tags=["db"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1715940385449,
                        "req_id": "45db2622-dd8d-43c2-ab7b-d60af82fa114",
                        "data": {
                            "@timestamp": "date",
                            "gulp.event.code": "long",
                            "event.sequence": "long",
                            "agent.build.original": "keyword",
                            "agent.ephemeral_id": "keyword",
                            "agent.id": "keyword",
                            "agent.name": "keyword",
                            "agent.type": "keyword",
                            "agent.version": "keyword",
                            "client.address": "keyword",
                            "client.as.number": "long",
                        },
                    }
                }
            }
        }
    },
    summary="same as `elastic_get_mapping`, but considering `gulp.source.file=src AND gulp.context=context` only.",
)
async def elastic_get_mapping_by_source_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[str, Query(description=gulp.defs.API_DESC_INDEX)],
    context: Annotated[
        str, Query(description='the "gulp.context" to return the mapping for.')
    ],
    src: Annotated[
        str, Query(description='the "gulp.source.file" to return the mapping for.')
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.collab(), token)
        m = await elastic_api.index_get_mapping_by_src(
            elastic_api.elastic(), index, context, src
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=m))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/collab_init",
    tags=["db"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    summary="(re)creates collaboration database, erasing all data.",
    description="**WARNING**: ALL collaboration data (including users and session tokens) related to **ALL** operations will be **DELETED**."
    "<br><br>"
    "default accounts are recreated as well:<br>"
    "**admin** (password: admin) with ADMIN permission<br>"
    "**guest** (password: guest) with just READ permission<br>"
    "**ingest** (password: ingest) with INGEST(READ/EDIT/DELETE/INGEST) permission.<br>"
    "**test1** (password: test) with EDIT(READ/EDIT) permission.<br>"
    "**test2** (password: test) with DELETE(READ/EDIT/DELETE) permission.<br><br>"
    "This may take a few seconds to complete.",
)
async def collab_init_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )

        # drop and recreate collab
        c = await collab_api.collab()
        await collab_db.drop(config.postgres_url())
        await collab_db.engine_close(c)
        rest_api.logger().debug("previous main process collab=%s" % (c))
        c = await collab_api.collab(invalidate=True)
        rest_api.logger().debug("current main process collab=%s" % (c))

        # we need also to reinit all processes
        gulp.plugin.plugin_cache_clear()
        await rest_api.recreate_process_executor()
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
    "/gulp_init",
    tags=["db"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    summary="(re)creates both collaboration and OpenSearch databases.",
    description="calls `collab_init` first, then `elastic_init`, in one shot.<br>"
    "**WARNING**: **ALL** collaboration data (including users and session tokens) related to **ALL** operations will be **DELETED**, all data in the datastream's backing index will be **DELETED** too.",
)
async def gulp_init_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    index: Annotated[str, Query(description=gulp.defs.API_DESC_INDEX)],
    max_total_fields: Annotated[
        int, Query(description="The maximum number of fields in the index.")
    ] = 10000,
    refresh_interval_msec: Annotated[
        int, Query(description="The index refresh interval in milliseconds.")
    ] = "5000",
    force_date_detection: Annotated[
        bool, Query(description="Force date detection in the index.")
    ] = False,
    event_original_text_analyzer: Annotated[
        str, Query(description="The event.original text analyzer.")
    ] = "standard",
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.ADMIN
        )
        await elastic_init_handler(
            token,
            index,
            max_total_fields=max_total_fields,
            refresh_interval_msec=refresh_interval_msec,
            force_date_detection=force_date_detection,
            event_original_text_analyzer=event_original_text_analyzer,
            req_id=req_id,
        )
        await collab_init_handler(token, req_id)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id))
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
