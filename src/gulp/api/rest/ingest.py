"""
API Routes for Ingestion Operations in the Gulp Framework

This module defines FastAPI routes that handle various ingestion operations:
- Single file ingestion (`ingest_file_handler`)
- Raw document ingestion (`ingest_raw_handler`)
- ZIP file ingestion with metadata (`ingest_zip_handler`)

Each handler implements a different ingestion strategy:
- File ingestion processes individual files through specified plugins
- Raw ingestion processes pre-structured document chunks
- ZIP ingestion extracts and processes multiple files based on metadata

The module also includes functions to be run in worker processes:
- `_ingest_file_internal`: Worker process function for file ingestion
- `_ingest_raw_internal`: Worker process function for raw data ingestion

All handlers support resumable uploads, progress tracking via websocket,
and preview mode for testing without persistence.

"""

import os
from copy import deepcopy
from typing import Annotated, Any, Optional

import muty.file
import muty.pydantic
import orjson
from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.collab.context import GulpContext
from gulp.api.collab.gulptask import GulpTask
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies, GulpUploadResponse
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters


class GulpBaseIngestPayload(BaseModel):
    """
    base payload for ingestion
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "flt": autogenerate_model_example_by_class(GulpIngestionFilter),
                }
            ]
        }
    )
    flt: Optional[GulpIngestionFilter] = Field(
        GulpIngestionFilter(), description="The ingestion filter."
    )


class GulpIngestPayload(GulpBaseIngestPayload):
    """
    payload for ingest_file
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "flt": autogenerate_model_example_by_class(GulpIngestionFilter),
                    "plugin_params": autogenerate_model_example_by_class(
                        GulpPluginParameters
                    ),
                    "file_sha1": "a1b2c3d4e5f6",
                    "original_file_path": "/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx",
                }
            ]
        },
    )

    plugin_params: Optional[GulpPluginParameters] = Field(
        None, description="The plugin parameters."
    )
    original_file_path: Optional[str] = Field(
        None, description="The original file path."
    )
    file_sha1: Optional[str] = Field(
        None, description="The SHA1 of the file being ingested."
    )


class GulpZipIngestPayload(GulpBaseIngestPayload):
    """
    payload for ingest_zip
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "flt": autogenerate_model_example_by_class(GulpIngestionFilter),
                }
            ]
        }
    )


class GulpZipMetadataEntry(BaseModel):
    """
    metadata entry for ingest_zip
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "plugin": "win_evtx",
                    "files": ["file1.evtx"],
                    "original_path": "c:\\logs",
                    "plugin_params": autogenerate_model_example_by_class(
                        GulpPluginParameters
                    ),
                }
            ]
        }
    )
    plugin: str = Field(..., description="The plugin to use.")
    files: list[str] = Field(..., description="The files to ingest.")
    original_path: Optional[str] = Field(
        None,
        description="The original base path where `files` are taken from.",
    )
    plugin_params: Optional[GulpPluginParameters] = Field(
        GulpPluginParameters(), description="The plugin parameters."
    )


router = APIRouter()

_EXAMPLE_INCOMPLETE_UPLOAD = {
    "content": {
        "application/json": {
            "example": {
                "status": "success",
                "data": {
                    "done": False,
                    "continue_offset": 12345678,
                },
                "req_id": "2fe81cdf-5f0a-482e-a5b2-74684c6e05fb",
                "timestamp_msec": 1618220400000,
            }
        }
    },
    "description": "The upload is incomplete, send another chunk from the `continue_offset`.",
}

_EXAMPLE_DONE_UPLOAD = {
    "content": {
        "application/json": {
            "examples": {
                "default": {
                    "value": {
                        "status": "pending",
                        "req_id": "2fe81cdf-5f0a-482e-a5b2-74684c6e05fb",
                        "timestamp_msec": 1618220400000,
                    }
                },
                "preview": {
                    "value": {
                        "status": "success",
                        "req_id": "2fe81cdf-5f0a-482e-a5b2-74684c6e05fb",
                        "timestamp_msec": 1618220400000,
                        "data": [
                            muty.pydantic.autogenerate_model_example_by_class(
                                GulpDocument
                            )
                        ],
                    }
                },
            }
        }
    }
}

_DESC_HEADER_SIZE = "the size of the header in bytes."
_DESC_HEADER_CONTINUE_OFFSET = "the offset in the file to continue the upload from."

_DESC_CONTEXT_NAME = """
`name` of a `context` object.

a `context` is a group of `sources` in the collab database, and is always referred to an `operation`.

- if not yet present, a `context` with `id=SHA1(operation_id+context_name)` and `name`=`context_id` will be created on the collab database.
"""

_EXAMPLE_CONTEXT_NAME = "test_context"

_DESC_SOURCE_NAME = """
`name` of a `source` object in the collab database, to identify the documents source (i.e. file name/path).

- it is always referred to an `operation` and a `context`.
- if not yet present, a `source` with id=SHA1(operation_id+context_id+source_name)` will be created on the collab database.
"""
_EXAMPLE_SOURCE_NAME = "test_source"


async def _ingest_file_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    context_id: str,
    source_id: str,
    index: str,
    plugin: str,
    file_path: str,
    file_total: int,
    payload: GulpIngestPayload,
    preview_mode: bool = False,
    delete_after: bool = False,
    **kwargs: Any,
) -> tuple[GulpRequestStatus, list[dict]]:
    """
    runs in a worker process to ingest a single file

    """
    # MutyLogger.get_instance().debug("---> _ingest_file_internal")
    preview_chunk: list[dict] = []
    async with GulpCollab.get_instance().session() as sess:
        mod: GulpPluginBase = None
        status = GulpRequestStatus.DONE

        if preview_mode:
            stats: GulpRequestStats = None
            MutyLogger.get_instance().warning(
                "PREVIEW MODE, no stats is created on the collab database."
            )
        else:
            # create stats
            stats = await GulpRequestStats.create(
                token=None,
                ws_id=ws_id,
                req_id=req_id,
                object_data={
                    "source_total": file_total,
                },
                operation_id=operation_id,
                sess=sess,
                user_id=user_id,
            )
        try:
            # run plugin
            mod = await GulpPluginBase.load(plugin)
            status = await mod.ingest_file(
                sess=sess,
                stats=stats,
                req_id=req_id,
                ws_id=ws_id,
                user_id=user_id,
                index=index,
                operation_id=operation_id,
                context_id=context_id,
                source_id=source_id,
                file_path=file_path,
                original_file_path=payload.original_file_path,
                plugin_params=payload.plugin_params,
                flt=payload.flt,
                preview_mode=preview_mode,
            )
        except Exception as ex:
            status = GulpRequestStatus.FAILED
            if preview_mode:
                # on preview (sync mode) raise exception
                raise ex
            else:
                d = {
                    "source_failed": 1,
                    "status": status,
                    "error": ex,
                }
                await stats.update(sess, d, ws_id=ws_id, user_id=user_id)
        finally:
            if preview_mode:
                if mod:
                    # get the accumulated preview chunk
                    preview_chunk = deepcopy(mod.preview_chunk())

            else:
                # create/update mappings on the collab db
                try:
                    await GulpOpenSearch.get_instance().datastream_update_mapping_by_src(
                        index=index,
                        operation_id=operation_id,
                        context_id=context_id,
                        source_id=source_id,
                    )
                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)

            if delete_after:
                # delete file
                await muty.file.delete_file_or_dir_async(file_path)

            # done
            if mod:
                if not preview_mode:
                    # broadcast ingest internal event
                    await mod.broadcast_ingest_internal_event()

                await mod.unload()

        return status, preview_chunk


async def run_ingest_file_task(t: dict):
    """
    runs the ingest file task in a task into a worker process

    :param t: a GulpTask dict to run
    """
    params: dict = t.get("params", {})
    params["payload"] = GulpIngestPayload.model_validate(params.get("payload"))
    # MutyLogger.get_instance().debug("run_ingest_file_task, params=%s" % (params))
    await GulpProcess.get_instance().process_pool.apply(
        _ingest_file_internal, kwds=params
    )


async def run_ingest_raw_task(t: dict):
    """
    runs the ingest raw task in a task into a worker process

    :param t: a GulpTask dict to run
    """
    params: dict = t.get("params", {})
    params["chunk"] = params.pop("raw_data", None)
    params["flt"] = GulpIngestionFilter.model_validate(params.get("flt"))
    params["plugin_params"] = GulpPluginParameters.model_validate(
        params.get("plugin_params")
    )
    # MutyLogger.get_instance().debug("run_ingest_raw_task, params=%s" % (params))
    await GulpProcess.get_instance().process_pool.apply(
        _ingest_raw_internal, kwds=params
    )


async def _handle_preview_or_enqueue_ingest_task(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    ctx_id: str,
    src_id: str,
    index: str,
    plugin: str,
    file_path: str,
    file_total: int,
    payload: GulpIngestPayload,
    preview_mode: bool,
    delete_after: bool = True,
) -> JSONResponse:
    """
    Handles preview mode or enqueues an ingestion task, returning the appropriate JSONResponse

    Args:
        user_id (str): ID of the user making the request
        req_id (str): Request ID for tracking
        ws_id (str): WebSocket ID for streaming updates
        operation_id (str): ID of the operation
        ctx_id (str): Context ID
        src_id (str): Source ID
        index (str): Index where data will be ingested
        plugin (str): Plugin to use for ingestion
        file_path (str): Path to the file being ingested
        file_total (int): Total number of files in a multi-file upload
        payload (GulpIngestPayload): Payload containing ingestion parameters
        preview_mode (bool): If True, runs in preview mode (immediate result, no task created)
        delete_after (bool, optional): If True, deletes the file after ingestion. Defaults to True.

    Returns:
        JSONResponse: the response to return to the client (either preview data or pending status)

    Throws:
        JSendException: if preview fails
    """
    kwds = dict(
        user_id=user_id,
        req_id=req_id,
        ws_id=ws_id,
        operation_id=operation_id,
        context_id=ctx_id,
        source_id=src_id,
        index=index,
        plugin=plugin,
        file_path=file_path,
        file_total=file_total,
        payload=payload,
        preview_mode=preview_mode,
        delete_after=delete_after,
    )

    # handle preview mode: run ingestion synchronously and return preview chunk
    if preview_mode:
        status, preview_chunk = await _ingest_file_internal(**kwds)
        if status == GulpRequestStatus.DONE:
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=preview_chunk)
            )

        # error in preview
        return JSONResponse(JSendResponse.error(req_id=req_id))

    # enqueue ingestion task on collab
    kwds["payload"] = payload.model_dump(exclude_none=True)
    object_data: dict[str, Any] = {
        "ws_id": ws_id,
        "operation_id": operation_id,
        "req_id": req_id,
        "task_type": "ingest",
        "params": kwds,
    }
    MutyLogger.get_instance().debug(
        "queueing ingestion task, object_data=%s" % (object_data)
    )
    await GulpTask.create(
        token=None,
        user_id=user_id,  # permission already checked
        ws_id=None,
        req_id=req_id,
        object_data=object_data,
    )
    # return pending response
    return JSONResponse(JSendResponse.pending(req_id=req_id))


@router.post(
    "/ingest_file",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        100: _EXAMPLE_INCOMPLETE_UPLOAD,
        200: _EXAMPLE_DONE_UPLOAD,
    },
    description="""
the request expects a multipart request with a JSON payload (content type `application/json`) and a bytes `chunk` (content type `application/octet-stream`) with a chunk of the file.

- **this function cannot be used from the `FastAPI /docs` page since it needs custom request handling to support resume**.

### internals

The following is an example CURL for the request:

```bash
curl -v -X POST http://localhost:8080/ingest_file?operation_id=test_operation&context_name=test_context&plugin=win_evtx&ws_id=test_ws&req_id=test_req&file_total=1&token=6ed2eee7-cfbe-4f06-904a-414ed6e5a926 -H content-type: multipart/form-data -H token: 6ed2eee7-cfbe-4f06-904a-414ed6e5a926 -H size: 69632 -H continue_offset: 0 -F payload={"flt": {}, "plugin_params": {}, "original_file_path": "/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx"}; type=application/json -F f=@/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx;type=application/octet-stream
```

the request have the following headers:

* `size`: The total size of the file being uploaded
* `continue_offset`: The offset of the chunk being uploaded (must be 0 if this is the first chunk)

### payload

the json payload is a `GulpIngestPayload`, which may contain the following fields:

* `flt` (GulpIngestionFilter): the ingestion filter, to restrict ingestion to a subset of the data specifying a `time_range`
* `plugin_params` (GulpPluginParameters): the plugin parameters, specific for the plugin being used
* `original_file_path` (str): the original file path, to indicate the original full path of the file being ingested on the machine where it was acquired from
* `file_sha1` (str): the SHA1 of the file being ingested (optional)

### response with resume handling

if the upload is interrupted before finishing, the **next** upload of the same file with the **same** `operation_id/context_id/filename` will return an **error** response
with `data` set to a `GulpUploadResponse`:

* `done` (bool): set to `False`
* `continue_offset` (int): the offset of the next chunk to be uploaded, if `done` is `False`

to resume file, the uploader must then forge a request (as in the example above) with the given `continue_offset` set.

once the upload is complete, the API will return a `pending` response and processing will start in the background.

### websocket

once the upload is complete, this function returns a `pending` response and the following will be sent on the `ws_id` websocket during processing, every `ingestion_buffer_size` documents (defined in the configuration):

- `WSDATA_STATS_UPDATE`: with totals and stats
- `WSDATA_DOCUMENTS_CHUNK`: the actual GulpDocuments chunk

### preview mode

if `preview_mode` is set, this function runs synchronously and returns the preview chunk of documents generated by the plugin, **without streaming data on the websocket, without saving data to the index and not counting data in the stats**.
""",
    summary="ingest file using the specified plugin.",
    openapi_extra={
        "parameters": [
            {
                "name": "size",
                "in": "header",
                "required": True,
                "schema": {"type": "integer"},
                "description": _DESC_HEADER_SIZE,
                "example": 69632,
            },
            {
                "name": "continue_offset",
                "in": "header",
                "required": False,
                "default": 0,
                "schema": {"type": "integer"},
                "description": _DESC_HEADER_CONTINUE_OFFSET,
                "example": 0,
            },
        ],
        "requestBody": {
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "f": {"type": "string", "format": "binary"},
                            "payload": {
                                "type": "object",
                                "properties": {
                                    "flt": {
                                        "$ref": "#/components/schemas/GulpIngestionFilter",
                                        "description": "Optional ingestion filter to restrict ingested data",
                                        "examples": [
                                            autogenerate_model_example_by_class(
                                                GulpIngestionFilter
                                            )
                                        ],
                                    },
                                    "plugin_params": {
                                        "$ref": "#/components/schemas/GulpPluginParameters",
                                        "description": "Optional plugin-specific parameters",
                                        "examples": [
                                            autogenerate_model_example_by_class(
                                                GulpPluginParameters
                                            )
                                        ],
                                    },
                                },
                            },
                        },
                    }
                }
            }
        },
    },
)
async def ingest_file_handler(
    r: Request,
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    context_name: Annotated[
        str,
        Query(description=_DESC_CONTEXT_NAME, example=_EXAMPLE_CONTEXT_NAME),
    ],
    plugin: Annotated[
        str,
        Depends(APIDependencies.param_plugin),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    file_total: Annotated[
        int,
        Query(
            description="set to the total number of files if this call is part of a multi-file upload (same `req_id` for multiple files), default=1.",
            example=1,
        ),
    ] = 1,
    preview_mode: Annotated[
        bool,
        Query(
            description="""
if set, this function is **synchronous** and returns the preview chunk of documents generated by the plugin, without streaming data on the websocket, without saving data to the index nor counting data in the stats.
""",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    ServerUtils.dump_params(params)
    file_path: str = None

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )
            index = operation.index
            user_id = s.user_id

            # handle multipart request manually
            file_path, payload, result = (
                await ServerUtils.handle_multipart_chunked_upload(
                    r=r,
                    operation_id=operation_id,
                    context_name=context_name,
                    prefix=req_id,
                )
            )
            if not result.done:
                # upload not done yet, must continue upload with a new chunk
                d = JSendResponse.error(
                    req_id=req_id, data=result.model_dump(exclude_none=True)
                )
                return JSONResponse(d, status_code=206)

            # ensure payload is valid
            payload = GulpIngestPayload.model_validate(payload)

            # proceed with the ingestion
            ctx_id: str = None
            src_id: str = None

            if preview_mode:
                MutyLogger.get_instance().warning(
                    "PREVIEW MODE, no context and source are created on the collab database."
                )
                ctx_id = "preview"
                src_id = "preview"
            else:
                ctx: GulpContext
                src: GulpSource

                # create (and associate) context and source on the collab db, if they do not exist
                ctx, _ = await operation.add_context(
                    sess, user_id=user_id, name=context_name, ws_id=ws_id, req_id=req_id
                )
                # MutyLogger.get_instance().debug( f"context in operation {operation.id}:  {ctx}")
                src, _ = await ctx.add_source(
                    sess,
                    user_id=user_id,
                    name=payload.original_file_path or os.path.basename(file_path),
                    ws_id=ws_id,
                    req_id=req_id,
                )
                ctx_id = ctx.id
                src_id = src.id

        # we have all we need, proceed with ingestion outside the session
        return await _handle_preview_or_enqueue_ingest_task(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            ctx_id=ctx_id,
            src_id=src_id,
            index=index,
            plugin=plugin,
            file_path=file_path,
            file_total=file_total,
            payload=payload,
            preview_mode=preview_mode,
        )

    except Exception as ex:
        # cleanup
        if file_path:
            await muty.file.delete_file_or_dir_async(file_path)
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/ingest_file_to_source",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        100: _EXAMPLE_INCOMPLETE_UPLOAD,
        200: _EXAMPLE_DONE_UPLOAD,
    },
    summary="ingest file to an existing source",
    description="""
refer to `ingest_file` documentation for further information.
""",
)
async def ingest_file_to_source_handler(
    r: Request,
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    source_id: Annotated[
        str,
        Depends(APIDependencies.param_source_id),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    plugin: Annotated[
        Optional[str],
        Query(
            description="the plugin to use, if not specified the plugin associated to the source will be used."
        ),
    ] = None,
    file_total: Annotated[
        int,
        Query(
            description="set to the total number of files if this call is part of a multi-file upload (same `req_id` for multiple files), default=1.",
            example=1,
        ),
    ] = 1,
    preview_mode: Annotated[
        bool,
        Query(
            description="""
if set, this function is **synchronous** and returns the preview chunk of documents generated by the plugin, without streaming data on the websocket, without saving data to the index nor counting data in the stats.
""",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    ServerUtils.dump_params(params)
    file_path: str = None

    ctx_id: str = None
    operation_id: str = None
    src_id: str = source_id
    context_name: str = None
    user_id: str = None
    index: str = None

    try:
        # get source, context, operation info first, since we assume source already exist (if not, we fail.... just use normal ingest_file api)
        async with GulpCollab.get_instance().session() as sess:
            src: GulpSource = await GulpSource.get_by_id(sess, source_id)
            operation_id = src.operation_id
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)

            # check token permission
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )

            # ok, fill the other info we need
            ctx_id = src.context_id
            ctx: GulpContext = await GulpContext.get_by_id(sess, ctx_id)
            context_name = ctx.name
            index = operation.index
            user_id = s.user_id
            plugin = plugin or src.plugin

        # handle multipart request manually
        file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
            r=r, operation_id=operation_id, context_name=context_name, prefix=req_id
        )
        if not result.done:
            # upload not done yet, must continue upload with a new chunk
            d = JSendResponse.error(
                req_id=req_id, data=result.model_dump(exclude_none=True)
            )
            return JSONResponse(d, status_code=206)

        # ensure payload is valid
        payload = GulpIngestPayload.model_validate(payload)

        # get plugin parameters either from payload or from the existing source
        plugin_params = payload.plugin_params or GulpPluginParameters(
            src.mapping_parameters
        )
        MutyLogger.get_instance().debug(
            f"ingesting to existing source {src.id}: context_name={context_name}, operation_id={operation_id}, index={index}, user_id={user_id}, plugin={plugin}, plugin_params={plugin_params}"
        )

        if preview_mode:
            MutyLogger.get_instance().warning("PREVIEW MODE.")
            ctx_id = "preview"
            src_id = "preview"

        return await _handle_preview_or_enqueue_ingest_task(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            ctx_id=ctx_id,
            src_id=src_id,
            index=index,
            plugin=plugin,
            file_path=file_path,
            file_total=file_total,
            payload=payload,
            preview_mode=preview_mode,
        )
    except Exception as ex:
        # cleanup
        if file_path:
            await muty.file.delete_file_or_dir_async(file_path)
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/ingest_file_local",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: _EXAMPLE_DONE_UPLOAD,
    },
    summary="ingest a `local` file using the specified plugin.",
    description="""
this is basically a local version of the `ingest_file` function, which does not require a multipart request and can be used from the `FastAPI /docs` page.

the following are the differences from the default `ingest_file` function:

- `plugin_params` and `flt` are passed as json in the payload, and are optional.
""",
)
async def ingest_file_local_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    path: Annotated[
        str,
        Query(
            description="the path of the file to be ingested, must be relative to `$WORKING_DIR/ingest_local` directory.",
            example="/samples/win_evtx/Security_short_selected.evtx",
        ),
    ],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    context_name: Annotated[
        str,
        Query(description=_DESC_CONTEXT_NAME, example=_EXAMPLE_CONTEXT_NAME),
    ],
    plugin: Annotated[
        str,
        Depends(APIDependencies.param_plugin),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    flt: Annotated[
        GulpIngestionFilter,
        Depends(APIDependencies.param_ingestion_flt_optional),
    ] = None,
    file_total: Annotated[
        Optional[int],
        Query(
            description="set to the total number of files if this call is part of a multi-file upload (same `req_id` for multiple files), default=1.",
            example=1,
        ),
    ] = 1,
    delete_after: Annotated[
        Optional[bool],
        Query(
            description="if set, the file will be deleted after processing.",
        ),
    ] = False,
    preview_mode: Annotated[
        Optional[bool],
        Query(
            description="""
if set, this function is **synchronous** and returns the preview chunk of documents generated by the plugin, without streaming data on the websocket, without saving data to the index nor counting data in the stats.
""",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # compute local path
    path = muty.file.safe_path_join(GulpConfig.get_instance().path_ingest_local(), path)
    MutyLogger.get_instance().info("ingesting local file: %s" % (path))

    try:
        ctx_id: str = None
        src_id: str = None
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )
            index = operation.index
            user_id = s.user_id

            if preview_mode:
                MutyLogger.get_instance().warning(
                    "PREVIEW MODE, no context and source are created on the collab database."
                )
                ctx_id = "preview"
                src_id = "preview"
            else:
                ctx: GulpContext
                src: GulpSource
                # create (and associate) context and source on the collab db, if they do not exist
                ctx, _ = await operation.add_context(
                    sess, user_id=user_id, name=context_name, ws_id=ws_id, req_id=req_id
                )
                # MutyLogger.get_instance().debug( f"context in operation {operation.id}:  {ctx}")
                src, _ = await ctx.add_source(
                    sess,
                    user_id=user_id,
                    name=os.path.basename(path),
                    ws_id=ws_id,
                    req_id=req_id,
                )
                ctx_id = ctx.id
                src_id = src.id

        payload = GulpIngestPayload(
            flt=flt,
            plugin_params=plugin_params,
            original_file_path=path,
            file_sha1=None,
        )
        return await _handle_preview_or_enqueue_ingest_task(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            ctx_id=ctx_id,
            src_id=src_id,
            index=index,
            plugin=plugin,
            file_path=path,
            file_total=file_total,
            payload=payload,
            preview_mode=preview_mode,
            delete_after=delete_after,
        )

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/ingest_file_local_to_source",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: _EXAMPLE_DONE_UPLOAD,
    },
    summary="local version of `ingest_file_to_source`.",
    description="""
refer to `ingest_file_to_local` documentation for further information.
""",
)
async def ingest_file_local_to_source_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    path: Annotated[
        str,
        Query(
            description="the path of the file to be ingested, must be relative to `$WORKING_DIR/ingest_local` directory.",
            example="/samples/win_evtx/Security_short_selected.evtx",
        ),
    ],
    source_id: Annotated[
        str,
        Depends(APIDependencies.param_source_id),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    plugin: Annotated[
        Optional[str],
        Query(
            description="the plugin to use, if not specified the plugin associated to the source will be used."
        ),
    ] = None,
    plugin_params: Annotated[
        Optional[GulpPluginParameters],
        Body(
            description="the plugin parameters, if not specified the plugin parameters associated to the source will be used."
        ),
    ] = None,
    flt: Annotated[
        GulpIngestionFilter,
        Depends(APIDependencies.param_ingestion_flt_optional),
    ] = None,
    file_total: Annotated[
        Optional[int],
        Query(
            description="set to the total number of files if this call is part of a multi-file upload (same `req_id` for multiple files), default=1.",
            example=1,
        ),
    ] = 1,
    delete_after: Annotated[
        Optional[bool],
        Query(
            description="if set, the file will be deleted after processing.",
        ),
    ] = False,
    preview_mode: Annotated[
        Optional[bool],
        Query(
            description="""
if set, this function is **synchronous** and returns the preview chunk of documents generated by the plugin, without streaming data on the websocket, without saving data to the index nor counting data in the stats.
""",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # compute local path
    path = muty.file.safe_path_join(GulpConfig.get_instance().path_ingest_local(), path)
    MutyLogger.get_instance().info("ingesting local file: %s" % (path))

    ctx_id: str = None
    operation_id: str = None
    src_id: str = source_id
    user_id: str = None
    index: str = None

    try:
        # get source, context, operation info first, since we assume source already exist (if not, we fail.... just use normal ingest_file api)
        async with GulpCollab.get_instance().session() as sess:
            src: GulpSource = await GulpSource.get_by_id(sess, source_id)
            operation_id = src.operation_id
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)

            # check token permission
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )

            # ok, fill the other info we need
            ctx_id = src.context_id
            index = operation.index
            user_id = s.user_id
            plugin = plugin or src.plugin
            plugin_params = plugin_params or GulpPluginParameters(
                src.mapping_parameters
            )

            if preview_mode:
                MutyLogger.get_instance().warning("PREVIEW MODE.")
                ctx_id = "preview"
                src_id = "preview"

            payload = GulpIngestPayload(
                flt=flt,
                plugin_params=plugin_params,
                original_file_path=path,
                file_sha1=None,
            )
            return await _handle_preview_or_enqueue_ingest_task(
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                ctx_id=ctx_id,
                src_id=src_id,
                index=index,
                plugin=plugin,
                file_path=path,
                file_total=file_total,
                payload=payload,
                preview_mode=preview_mode,
                delete_after=delete_after,
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def _ingest_raw_internal(
    req_id: str,
    ws_id: str,
    user_id: str,
    operation_id: str,
    index: str,
    chunk: bytes,
    flt: GulpIngestionFilter,
    plugin: str,
    plugin_params: GulpPluginParameters,
    last: bool,
    **kwargs: Any,
) -> None:
    """
    runs in a worker process to ingest a raw chunk of GulpDocuments
    """
    # MutyLogger.get_instance().debug("---> ingest_raw_internal")

    async with GulpCollab.get_instance().session() as sess:
        if last:
            # on last chunk, we will let the stats expire
            object_data = None
        else:
            # create a stats that never expire
            object_data = {
                "never_expire": True,
            }
        stats: GulpRequestStats = await GulpRequestStats.create(
            token=None,
            ws_id=ws_id,
            req_id=req_id,
            object_data=object_data,
            operation_id=operation_id,
            sess=sess,
            user_id=user_id,
        )

        mod: GulpPluginBase = None
        try:
            # run plugin
            plugin = plugin or "raw"
            mod = await GulpPluginBase.load(plugin)
            await mod.ingest_raw(
                sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                index=index,
                operation_id=operation_id,
                chunk=chunk,
                stats=stats,
                flt=flt,
                plugin_params=plugin_params,
                last=last,
                **kwargs,
            )
        except Exception as ex:
            # set failed
            status = GulpRequestStatus.FAILED
            d = {
                "source_failed": 1,
                "status": status,
                "error": ex,
            }
            await stats.update(sess, d, ws_id=ws_id, user_id=user_id)
        finally:
            # done
            if mod:
                await mod.unload()


@router.post(
    "/ingest_raw",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: _EXAMPLE_DONE_UPLOAD,
    },
    description="""
ingests a chunk of raw data, i.e. coming from a gulp SIEM agent, bridge or generic ingestion tool.

- the request expects a multipart request with a JSON payload (content type `application/json`) and a bytes `chunk` (content type `application/octet-stream`) with a chunk of the file.

- **this function cannot be used from the `FastAPI /docs` page since it needs custom request handling to support multipart**.

### data

the data is expected to contain everything for gulp to (re)generate a GulpDocument successfully.

### payload

the json payload may contain the following fields:

* `flt` (GulpIngestionFilter): the ingestion filter, to restrict ingestion to a subset of the data specifying a `time_range`
* `plugin_params` (GulpPluginParameters): the plugin parameters, specific for the plugin being used

### plugin

by default, the `raw` plugin is used (and it is the recommended one): the data `chunk` is expected as a JSON text with a list of `GulpDocument` dictionaries.
it is possible, however, to implement another plugin which sends raw data (i.e. bytes), as long as it can process the `chunk` data.
""",
    summary="ingest raw documents.",
)
async def ingest_raw_handler(
    r: Request,
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    plugin: Annotated[
        str,
        Query(
            description=""""
the plugin to be used, must be able to process the raw documents in `chunk`. """,
            example="raw",
        ),
    ] = None,
    last: Annotated[
        bool,
        Query(
            description="if set, indicates the last chunk in a raw ingestion.",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    if not plugin:
        plugin = "raw"
        MutyLogger.get_instance().debug("using default raw plugin: %s" % (plugin))

    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )
            index = operation.index
            user_id = s.user_id

        # get body (contains chunk, and optionally flt and plugin_params)
        payload, chunk = await ServerUtils.handle_multipart_body(r)
        # flt: GulpIngestionFilter = GulpIngestionFilter.model_validate(
        #     payload.get("flt", GulpIngestionFilter())
        # )
        # plugin_params: GulpPluginParameters = GulpPluginParameters.model_validate(
        #     payload.get("plugin_params", GulpPluginParameters())
        # )

        kwds = dict(
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            index=index,
            flt=payload.get("flt") or {},
            plugin=plugin,
            plugin_params=payload.get("plugin_params") or {},
            last=last,
        )

        # enqueue task on collab
        object_data = {
            "ws_id": ws_id,
            "operation_id": operation_id,
            "req_id": req_id,
            "task_type": "ingest_raw",
            "params": kwds,
        }
        MutyLogger.get_instance().debug(
            "queueing ingestion task (raw), object_data=%s" % (object_data)
        )
        await GulpTask.create(
            token,
            ws_id=None,
            req_id=req_id,
            object_data=object_data,
            raw_data=chunk,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def _process_metadata_json(
    unzipped_path: str, payload: GulpZipIngestPayload
) -> list[GulpIngestPayload]:
    """
    Process metadata.json file from unzipped path and extract files information.

    Args:
        unzipped_path (str): Path to unzipped directory containing metadata.json
        payload (GulpZipIngestPayload): The payload for the ingestion.

    Returns:
        list[GulpIngestPayload]: List of GulpIngestPayload objects.
        each have, in model_extra, local_file_path set to the path of the file to ingest and plugin set to the plugin to use.

    Raises:

    """
    # Read and parse metadata json
    js = await muty.file.read_file_async(os.path.join(unzipped_path, "metadata.json"))
    metadata = orjson.loads(js)

    # process files entries
    files = []
    for entry in metadata:
        e = GulpZipMetadataEntry.model_validate(entry)
        for f in e.files:
            files.append(
                GulpIngestPayload(
                    flt=payload.flt,
                    plugin_params=e.plugin_params,
                    original_file_path=(
                        os.path.join(e.original_path, f) if e.original_path else f
                    ),
                    local_file_path=os.path.join(unzipped_path, f),
                    plugin=e.plugin,
                )
            )

    files_total = len(files)
    if files_total == 0:
        raise ValueError("metadata.json is empty")

    return files


@router.post(
    "/ingest_zip",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        100: _EXAMPLE_INCOMPLETE_UPLOAD,
        200: _EXAMPLE_DONE_UPLOAD,
    },
    openapi_extra={
        "parameters": [
            {
                "name": "size",
                "in": "header",
                "required": True,
                "schema": {"type": "integer"},
                "description": _DESC_HEADER_SIZE,
                "example": 69632,
            },
            {
                "name": "continue_offset",
                "in": "header",
                "required": False,
                "default": 0,
                "schema": {"type": "integer"},
                "description": _DESC_HEADER_CONTINUE_OFFSET,
                "example": 0,
            },
        ],
        "requestBody": {
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "f": {"type": "string", "format": "binary"},
                            "payload": {
                                "type": "object",
                                "properties": {
                                    "flt": {
                                        "$ref": "#/components/schemas/GulpIngestionFilter",
                                        "description": "Optional ingestion filter to restrict ingested data",
                                        "examples": [
                                            autogenerate_model_example_by_class(
                                                GulpIngestionFilter
                                            )
                                        ],
                                    },
                                },
                            },
                        },
                    }
                }
            }
        },
    },
    description="""
- **this function cannot be used from the `FastAPI /docs` page since it needs custom request handling to support resume**.

the request expects a multipart request with a JSON payload (content type `application/json`) and a bytes `chunk` (content type `application/zip`) with a chunk of the file.

### zip format

the uploaded zip file **must include** a `metadata.json` with an array of `GulpZipMetadataEntry` to describe the zip content.

```json
[ GulpZipMetadataEntry, ... ]
```

### payload

the json payload is a `GulpZipIngestPayload`, which allows to specify a `GulpIngestionFilter` to be applied to all files in the zip.

for further information, refer to `ingest_file` API for the request and response internals.
""",
    summary="ingest a zip containing multiple sources, possibly using multiple plugins.",
)
async def ingest_zip_handler(
    r: Request,
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    context_name: Annotated[
        str,
        Query(description=_DESC_CONTEXT_NAME, example=_EXAMPLE_CONTEXT_NAME),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    ServerUtils.dump_params(params)
    file_path: str = None
    result: GulpUploadResponse = None

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )
            index = operation.index
            user_id = s.user_id

            # handle multipart request manually
            file_path, payload, result = (
                await ServerUtils.handle_multipart_chunked_upload(
                    r=r,
                    operation_id=operation_id,
                    context_name=context_name,
                    prefix=req_id,
                )
            )
            if not result.done:
                # must continue upload with a new chunk
                d = JSendResponse.error(
                    req_id=req_id, data=result.model_dump(exclude_none=True)
                )
                return JSONResponse(d, status_code=206)

            # ensure payload is valid
            payload = GulpZipIngestPayload.model_validate(payload)

            # unzip in a temporary directory
            unzipped = await muty.file.unzip(file_path)

            # read metadata json
            files: list[GulpIngestPayload] = await _process_metadata_json(
                unzipped, payload
            )
            file_total = len(files)

            # ingest each file in a worker
            MutyLogger.get_instance().debug(
                "spawning %d ingestion tasks ..." % (file_total)
            )

            # create (and associate) context on the collab db, if it does not exists
            ctx: GulpContext
            ctx, _ = await operation.add_context(
                sess, user_id=user_id, name=context_name, ws_id=ws_id, req_id=req_id
            )

            # add each source
            for f in files:
                src: GulpSource
                src, _ = await ctx.add_source(
                    sess,
                    user_id=user_id,
                    name=f.original_file_path,
                    ws_id=ws_id,
                    req_id=req_id,
                )

                kwds = dict(
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    context_id=ctx.id,
                    source_id=src.id,
                    index=index,
                    plugin=f.model_extra.get("plugin"),
                    file_path=f.model_extra.get("local_file_path"),
                    file_total=file_total,
                    payload=f,
                )

                # enqueue task on collab
                kwds["payload"] = GulpIngestPayload.model_dump(
                    kwds["payload"], exclude_none=True
                )

                object_data = {
                    "ws_id": ws_id,
                    "operation_id": operation_id,
                    "req_id": req_id,
                    "task_type": "ingest",
                    "params": kwds,
                }
                MutyLogger.get_instance().debug(
                    "queueing ingestion (from zip) task, object_data=%s" % (object_data)
                )
                await GulpTask.create(
                    token,
                    ws_id=None,
                    req_id=req_id,
                    object_data=object_data,
                )

            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    finally:
        # cleanup
        await muty.file.delete_file_or_dir_async(file_path)


@router.post(
    "/ingest_zip_local",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: _EXAMPLE_DONE_UPLOAD,
    },
    summary="ingest a `local` zip containing multiple sources, possibly using multiple plugins.",
    description="""
this is basically a local version of the `ingest_zipo` function, which does not require a multipart request and can be used from the `FastAPI /docs` page.

the following are the differences from the default `ingest_file` function:

- `flt` is optionally passed as json in the payload
""",
)
async def ingest_zip_local_handler(
    token: Annotated[
        str,
        Depends(APIDependencies.param_token),
    ],
    path: Annotated[
        str,
        Query(
            description="the path of the ZIP file to be ingested, must be relative to `$WORKING_DIR/ingest_local` directory.",
            example="/test.zip",
        ),
    ],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    context_name: Annotated[
        str,
        Query(description=_DESC_CONTEXT_NAME, example=_EXAMPLE_CONTEXT_NAME),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    flt: Annotated[
        GulpIngestionFilter,
        Depends(APIDependencies.param_ingestion_flt_optional),
    ] = None,
    delete_after: Annotated[
        Optional[bool],
        Query(
            description="if set, the ZIP file will be deleted after processing.",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # compute local path
    path = muty.file.safe_path_join(GulpConfig.get_instance().path_ingest_local(), path)
    MutyLogger.get_instance().info("ingesting local ZIP file: %s" % (path))

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=operation, permission=GulpUserPermission.INGEST
            )
            index = operation.index
            user_id = s.user_id

            payload = GulpZipIngestPayload(
                flt=flt,
            )

            # unzip in a temporary directory
            unzipped = await muty.file.unzip(path)

            # read metadata json
            files: list[GulpIngestPayload] = await _process_metadata_json(
                unzipped, payload
            )
            file_total = len(files)

            # ingest each file in a worker
            MutyLogger.get_instance().debug(
                "spawning %d ingestion tasks ..." % (file_total)
            )

            # create (and associate) context on the collab db, if it does not exists
            ctx: GulpContext
            ctx, _ = await operation.add_context(
                sess, user_id=user_id, name=context_name, ws_id=ws_id, req_id=req_id
            )

            # add each source
            for f in files:
                src: GulpSource
                src, _ = await ctx.add_source(
                    sess,
                    user_id=user_id,
                    name=f.original_file_path,
                    ws_id=ws_id,
                    req_id=req_id,
                )

                kwds = dict(
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    context_id=ctx.id,
                    source_id=src.id,
                    index=index,
                    plugin=f.model_extra.get("plugin"),
                    file_path=f.model_extra.get("local_file_path"),
                    file_total=file_total,
                    payload=f,
                    # they are temporary files, always delete
                    delete_after=True,
                )

                # enqueue task on collab
                kwds["payload"] = GulpIngestPayload.model_dump(
                    kwds["payload"], exclude_none=True
                )

                object_data = {
                    "ws_id": ws_id,
                    "operation_id": operation_id,
                    "req_id": req_id,
                    "task_type": "ingest",
                    "params": kwds,
                }
                MutyLogger.get_instance().debug(
                    "queueing ingestion (from local zip) task, object_data=%s"
                    % (object_data)
                )
                await GulpTask.create(
                    token,
                    ws_id=None,
                    req_id=req_id,
                    object_data=object_data,
                )
            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    finally:
        # cleanup
        if delete_after:
            await muty.file.delete_file_or_dir_async(path)


@router.get(
    "/ingest_local_list",
    tags=["ingest"],
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
                            "samples/win_evtx/Security_short_selected_1.evtx",
                            "samples/win_evtx/Security_short_selected_2.evtx",
                            "samples/win_evtx/Security_short_selected_3.evtx",
                        ],
                    }
                }
            }
        }
    },
    summary="lists the local ingestion directory.",
    description="""
- needs the `ingestion_local_directory` to be set in the configuration, default=`~/.config/gulp/ingest_local`
- returned paths are relative to the `ingestion_local_directory`
""",
)
async def ingest_local_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.INGEST
            )

        local_dir = GulpConfig.get_instance().path_ingest_local()
        MutyLogger.get_instance().info(
            "listing local ingestion directory: %s" % (local_dir)
        )
        l = await muty.file.list_directory_async(
            local_dir, files_only=True, recursive=True
        )

        # remove the local_dir prefix from the path
        d = []
        for f in l:
            f = os.path.relpath(f, local_dir)
            d.append(f)

        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
