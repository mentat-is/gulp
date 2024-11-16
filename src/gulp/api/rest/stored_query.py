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
from fastapi import APIRouter, Body, File, Header, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.api.rest.collab_utility as collab_utility
import gulp.api.rest_api as rest_api
import gulp.structs
import gulp.plugin
import gulp.utils
import gulp.process as process
from gulp.api.collab.base import GulpCollabFilter, GulpCollabType
from gulp.api.collab.base import GulpCollabObject
from gulp.api.elastic import query_utils
from gulp.api.opensearch.structs import GulpQueryParameter, GulpQueryType
from gulp.structs import API_DESC_PYSYGMA_PLUGIN, InvalidArgument
from muty.log import MutyLogger

_app: APIRouter = APIRouter()


@_app.post(
    "/stored_query_list",
    tags=["stored_query"],
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
    summary="list stored queries, optionally using a filter.",
    description="available filters: id, owner_id, name, opt_basic_fields_only, tags, limit, offset.",
)
async def stored_query_list_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    flt: Annotated[GulpCollabFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_list(
        token, req_id, GulpCollabType.STORED_QUERY, flt
    )


@_app.delete(
    "/stored_query_delete",
    tags=["stored_query"],
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
    summary="deletes a stored query.",
)
async def stored_query_delete_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_DELETE_EDIT_TOKEN)],
    stored_query_id: Annotated[
        int, Query(description="id of the stored query to be deleted.")
    ],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        await collab_utility.collabobj_delete(
            token, req_id, GulpCollabType.STORED_QUERY, stored_query_id, ws_id=ws_id
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.put(
    "/stored_query_update",
    tags=["stored_query"],
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
    summary="updates a stored query.",
)
async def stored_query_update_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
    stored_query_id: Annotated[
        int, Query(description="id of the stored query to be updated.")
    ],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    q: Annotated[GulpQueryParameter, Body()] = None,
    private: Annotated[bool, Query(description=gulp.structs.API_DESC_PRIVATE)] = False,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        if q is None and private is None:
            raise InvalidArgument("at least one of q, private must be provided.")

        if q.type == GulpQueryType.INDEX:
            raise InvalidArgument("INDEX type is not supported.")

        # convert and update rule
        r = await query_utils.gulpqueryparam_to_gulpquery(await collab_api.session(), q)
        d = await GulpCollabObject.update(
            await collab_api.session(),
            token,
            req_id,
            stored_query_id,
            ws_id,
            tags=q.tags,
            data=r.to_dict(),
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=d.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/stored_query_create",
    tags=["stored_query"],
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
    summary="creates a stored query from ElasticSearch raw query/DSL, SIGMA Rule YAML or GulpQueryFilter.",
)
async def stored_query_create_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
    q: Annotated[GulpQueryParameter, Body()],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    private: Annotated[bool, Query(description=gulp.structs.API_DESC_PRIVATE)] = False,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)

    try:
        if q.type == GulpQueryType.INDEX:
            raise InvalidArgument("INDEX type is not supported.")

        r = await query_utils.gulpqueryparam_to_gulpquery(await collab_api.session(), q)

        # create the entry
        d = await GulpCollabObject.create(
            await collab_api.session(),
            token,
            req_id,
            GulpCollabType.STORED_QUERY,
            ws_id=ws_id,
            name=q.name,
            tags=q.tags,
            data=r.to_dict(),
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=d.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/stored_query_create_from_sigma_zip",
    tags=["stored_query"],
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
                        "data": {
                            "success": [1, 2, 3],
                            "errors": ["filepath1: error", "filepath2: error"],
                        },
                    }
                }
            }
        }
    },
    summary="creates tagged stored queries from sigma rule YAMLs in a zip.",
)
async def stored_query_create_from_sigma_zip_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
    z: Annotated[UploadFile, File(description="zip with sigma rules.")],
    ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
    pysigma_plugin: Annotated[
        str,
        Query(
            description=API_DESC_PYSYGMA_PLUGIN,
        ),
    ] = None,
    tags_from_directories: Annotated[
        bool,
        Query(
            description="tags are normally created from sigma's own tags. if this is enabled, tags are created also from directory name/s inside the zip file.<br><br>"
            "to support this, rules in zip file must be organized like */windows/rule1, /windows/rule2, /windows/process/rule3, /windows/process/attacks/rule4, ...*."
        ),
    ] = True,
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    files_path = None
    try:
        # decompress
        files_path = await muty.uploadfile.unzip(z)
        MutyLogger.get_logger().debug("zipfile unzipped to %s" % (files_path))
        try:
            # use multiprocessing to gather the rules
            s = await process.gather_sigma_directories_to_stored_queries(
                token,
                req_id.files_path,
                pysigma_plugin,
                tags_from_directories=tags_from_directories,
                ws_id=ws_id,
            )
            return muty.jsend.success_jsend(req_id=req_id, data=s)
        except Exception as ex:
            raise JSendException(req_id=req_id, ex=ex) from ex
    except Exception as ex:
        # error unzipping
        raise JSendException(req_id=req_id, ex=ex) from ex
    finally:
        if files_path is not None:
            await muty.file.delete_file_or_dir_async(files_path)


@_app.get(
    "/stored_query_get_by_id",
    tags=["stored_query"],
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
async def stored_query_get_by_id_handler(
    token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
    stored_query_id: Annotated[
        int, Query(description="id of the stored query to be retrieved.")
    ],
    req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    return await collab_utility.collabobj_get_by_id(
        token, req_id, GulpCollabType.STORED_QUERY, stored_query_id
    )


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
