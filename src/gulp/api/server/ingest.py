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

All handlers support resumable uploads, progress tracking via websocket,
and preview mode for testing without persistence.

"""

import os
from copy import deepcopy
from typing import Annotated, Any, Optional

import muty.file
import muty.log
import muty.pydantic
import orjson
from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.context import GulpContext
from gulp.api.collab.gulptask import GulpTask
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpIngestionStats, GulpRequestStats, RequestStatsType
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import (
    TASK_TYPE_INGEST,
    TASK_TYPE_INGEST_RAW,
    APIDependencies,
    GulpUploadResponse,
)
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpMappingParameters, GulpPluginParameters


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

    plugin_params: Annotated[
        Optional[GulpPluginParameters],
        Field(description="To customize plugin behaviour"),
    ] = GulpPluginParameters()
    original_file_path: Annotated[
        Optional[str], Field(description="The original file path.")
    ] = None
    file_sha1: Annotated[
        Optional[str], Field(description="The SHA1 of the file being ingested.")
    ] = None


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
    plugin: Annotated[str, Field(description="The plugin to use.")]
    files: Annotated[list[str], Field(description="The files to ingest.")]
    original_path: Annotated[
        Optional[str],
        Field(
            description="The original base path where `files` are taken from.",
        ),
    ] = None
    plugin_params: Annotated[
        Optional[GulpPluginParameters], Field(description="The plugin parameters.")
    ] = GulpPluginParameters()


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
    context_name: str,
    source_name: str,
    index: str,
    plugin: str,
    file_path: str,
    payload: GulpIngestPayload,
    file_total: int = 1,
    delete_after: bool = False,
    **kwargs: Any,
) -> tuple[GulpRequestStatus, list[dict]]:
    """
    runs in a worker process to ingest a single file

    Returns:
    tuple[GulpRequestStatus, list[dict]]: the ingestion return status and the preview chunk (empty if not preview_mode)

    Raises:
        any exception raised by the underlying plugin.ingest_file method.
    """
    # MutyLogger.get_instance().debug("---> _ingest_file_internal")
    mod: GulpPluginBase = None
    ctx_id: str = None
    src_id: str = None
    if payload.plugin_params.preview_mode:
        MutyLogger.get_instance().warning(
            "***PREVIEW MODE*** ingestion for file=%s", file_path
        )
        # these are set to "preview"
        ctx_id = context_name
        src_id = source_name

    async with GulpCollab.get_instance().session() as sess:
        try:
            status = GulpRequestStatus.DONE
            stats: GulpRequestStats = None

            if not payload.plugin_params.preview_mode:
                # create stats
                stats, _ = await GulpRequestStats.create_or_get_existing(
                    sess,
                    req_id,
                    user_id,
                    operation_id,
                    req_type=RequestStatsType.REQUEST_TYPE_INGESTION,
                    ws_id=ws_id,
                    data=GulpIngestionStats(source_total=file_total).model_dump(
                        exclude_none=True
                    ),
                )

                # create (or get, if they already exist) context and source on the collab db
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                ctx, _ = await op.add_context(
                    sess,
                    user_id=user_id,
                    name=context_name,
                    ws_id=ws_id,
                    req_id=req_id,
                )
                # MutyLogger.get_instance().debug("context in operation %s: %s", operation_id, ctx)
                src, _ = await ctx.add_source(
                    sess,
                    user_id=user_id,
                    name=source_name,
                    ws_id=ws_id,
                    req_id=req_id,
                )
                ctx_id = ctx.id
                src_id = src.id

            # run plugin and perform the ingestion
            mod = await GulpPluginBase.load(plugin)
            status = await mod.ingest_file(
                sess=sess,
                stats=stats,
                req_id=req_id,
                ws_id=ws_id,
                user_id=user_id,
                index=index,
                operation_id=operation_id,
                context_id=ctx_id,
                source_id=src_id,
                file_path=file_path,
                original_file_path=payload.original_file_path,
                plugin_params=payload.plugin_params,
                flt=payload.flt,
                **kwargs,
            )
            if payload.plugin_params.preview_mode:
                # get the accumulated preview chunk and we're done
                preview_chunk: list[dict] = deepcopy(mod.preview_chunk())
                return status, preview_chunk

            # this source is done
            await mod.update_final_stats_and_flush(flt=payload.flt)

            # broadcast internal event and update source field types
            await mod.broadcast_ingest_internal_event()
            return status, []
        except Exception as ex:
            if mod:
                # this source failed
                await mod.update_final_stats_and_flush(flt=payload.flt, ex=ex)
            raise
        finally:
            if mod:
                await mod.unload()
            if delete_after:
                # delete file
                await muty.file.delete_file_or_dir_async(file_path)


async def run_ingest_file_task(t: GulpTask):
    """
    runs in a task in a worker process and calls _ingest_file_internal

    :param t: a GulpTask dict to run
    """
    ingest_args: dict = t.params
    ingest_args["user_id"] = t.user_id
    ingest_args["operation_id"] = t.operation_id
    ingest_args["payload"] = GulpIngestPayload.model_validate(ingest_args["payload"])
    # MutyLogger.get_instance().debug(
    #     "run_ingest_file_task, t=%s", t)
    # )
    try:
        await _ingest_file_internal(**ingest_args)
    except:
        MutyLogger.get_instance().exception(
            "***ERROR*** in run_ingest_file_task, task=%s", t
        )


async def _preview_or_enqueue_ingest_task(
    sess: AsyncSession,
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    context_name: str,
    source_name: str,
    index: str,
    plugin: str,
    file_path: str,
    payload: GulpIngestPayload,
    file_total: int = 1,
    delete_after: bool = True,
) -> JSONResponse:
    """
    runs preview and returns directly or enqueue a file ingestion task which will run in a worker process then (returning data through websocket at ws_id)

    Returns:

    if plugin_params.preview_mode, a succesful response.
    if not plugin_params.preview_mode, returns a pending response

    Raises:
    any exception raised by the underlying _ingest_file_internal or enqueue methods.
    """
    kwds = dict(
        context_name=context_name,
        source_name=source_name,
        req_id=req_id,
        ws_id=ws_id,
        index=index,
        plugin=plugin,
        file_path=file_path,
        payload=payload,
        file_total=file_total,
        delete_after=delete_after,
    )

    # handle preview mode: run ingestion synchronously and return preview chunk
    if payload.plugin_params.preview_mode:
        MutyLogger.get_instance().warning(
            "PREVIEW MODE (context_name=%s, source_name=%s), no context and source are created on the collab database.",
            context_name,
            source_name,
        )

        # use "preview" as context and source
        context_name = "preview"
        source_name = "preview"
        status, preview_chunk = await _ingest_file_internal(
            **kwds, user_id=user_id, operation_id=operation_id
        )
        if status == GulpRequestStatus.DONE:
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=preview_chunk)
            )

        # error in preview
        return JSONResponse(JSendResponse.error(req_id=req_id))

    # enqueue ingestion task on collab
    kwds["payload"] = payload.model_dump(exclude_none=True)
    await GulpTask.enqueue(
        sess,
        task_type=TASK_TYPE_INGEST,
        operation_id=operation_id,
        user_id=user_id,
        ws_id=ws_id,
        req_id=req_id,
        params=kwds,
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
curl -v -X POST http://localhost:8080/ingest_file?operation_id=test_operation&context_name=test_context&plugin=win_evtx&ws_id=test_ws&req_id=test_req&token=6ed2eee7-cfbe-4f06-904a-414ed6e5a926 -H content-type: multipart/form-data -H token: 6ed2eee7-cfbe-4f06-904a-414ed6e5a926 -H size: 69632 -H continue_offset: 0 -F payload={"flt": {}, "plugin_params": {}, "original_file_path": "/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx"}; type=application/json -F f=@/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx;type=application/octet-stream
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
* `preview_mode` (bool): if `preview_mode` is set, this function runs synchronously and returns the preview chunk of documents generated by the plugin, **without streaming data on the websocket, without saving data to the index and without creating a `request_stats` object on the collab database**.

### response with resume handling

if the upload is interrupted before finishing, the **next** upload of the same file with the **same** `operation_id/context_id/filename` will return an **error** response
with `data` set to a `GulpUploadResponse`:

* `done` (bool): set to `False`
* `continue_offset` (int): the offset of the next chunk to be uploaded, if `done` is `False`

to resume file, the uploader must then forge a request (as in the example above) with the given `continue_offset` set.

once the upload is complete, the API will return a `pending` response and processing will start in the background.

### response

this function returns the following:

- `status=error`, http status code `206` if the upload is incomplete (see above)
- `status=pending, http status code `200` if the upload is complete and the ingestion task has been enqueued
- `status=success`, http status code `200` if the upload is complete and `preview_mode` is set, with `data` set to the preview chunk of documents

### tracking progress

during ingesstion, the following is the flow on data on the websocket `ws_id`:

- `WSDATA_STATS_CREATE`.payload: `GulpRequestStats`, data=`GulpIngestionStats` (at start)
- `WSDATA_STATS_UPDATE`.payload: `GulpRequestStats`, data=updated `GulpIngestionStats` (once every `ingestion_buffer_size` documents)
- `WSDATA_COLLAB_CREATE`.payload: data=`GulpContext`/`GulpSource`, when they are created on the collab database (if they do not exist yet)
- `WSDATA_DOCUMENTS_CHUNK`.payload: `GulpDocumentsChunkPacket`, the actual chunk of ingested `GulpDocuments` (once every `ingestion_buffer_size` documents)
- `WSDATA_INGEST_SOURCE_DONE`.payload: `GulpIngestSourceDonePacket`, when the ingestion is done
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
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id_optional),
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
            user_id = s.user.id

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
            # we have all we need, proceed with ingestion outside the session
            return await _preview_or_enqueue_ingest_task(
                sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                context_name=context_name,
                source_name=payload.original_file_path or os.path.basename(file_path),
                index=index,
                plugin=plugin,
                file_path=file_path,
                payload=payload,
                file_total=file_total,
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
- ingest `operation_id` is taken from the source and checked for access.
- if payload contains `plugin_params`, they will override the existing plugin parameters associated with the source.

### tracking progress

refer to `ingest_file`
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
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id_optional),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get source by id and check acl
            s: GulpUserSession
            src: GulpSource
            op: GulpOperation
            s, src, op = await GulpSource.get_by_id_wrapper(
                sess, token, source_id, GulpUserPermission.INGEST
            )
            user_id: str = s.user.id

            # get context
            ctx: GulpContext = await GulpContext.get_by_id(sess, src.context_id)
            context_name: str = ctx.name

            # handle multipart request manually
            file_path: str
            file_path, payload, result = (
                await ServerUtils.handle_multipart_chunked_upload(
                    r=r,
                    operation_id=src.operation_id,
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

            # get plugin parameters either from payload or from the existing source
            plugin_params = payload.plugin_params
            if plugin_params.is_empty():
                plugin_params = GulpPluginParameters(
                    mapping_parameters=GulpMappingParameters.model_validate(
                        src.mapping_parameters
                    )
                )
            MutyLogger.get_instance().debug(
                "ingesting to existing source %s: context_name=%s, operation_id=%s, index=%s, user_id=%s, plugin=%s, plugin_params=%s",
                src.id,
                context_name,
                src.operation_id,
                op.index,
                user_id,
                src.plugin,
                plugin_params,
            )

            return await _preview_or_enqueue_ingest_task(
                sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=src.operation_id,
                context_name=ctx.name,
                source_name=src.name,
                index=op.index,
                plugin=src.plugin,
                file_path=file_path,
                payload=payload,
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
this is basically a local version of the `ingest_file` function, which expects the file to be in the `$GULP_WORKING_DIR/ingest_local` directory on the server.

- this API does not require custom request handling by the server and can be used from the `FastAPI /docs` page.

### tracking progress

refer to `ingest_file`
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
            description="the path (relative to `$GULP_WORKING_DIR`) to the zip file to be ingested.",
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
        Depends(APIDependencies.param_plugin_params),
    ] = None,
    flt: Annotated[
        GulpIngestionFilter,
        Depends(APIDependencies.param_ingestion_flt_optional),
    ] = None,
    delete_after: Annotated[
        Optional[bool],
        Query(
            description="if set, the file will be deleted after processing.",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id_optional),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # compute local path
    path = muty.file.safe_path_join(GulpConfig.get_instance().path_ingest_local(), path)
    MutyLogger.get_instance().info("ingesting local file: %s", path)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation
            s: GulpUserSession
            s, op, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, permission=GulpUserPermission.INGEST
            )

            payload = GulpIngestPayload(
                flt=flt,
                plugin_params=plugin_params,
                original_file_path=path,
                file_sha1=None,  # not needed
            )
            return await _preview_or_enqueue_ingest_task(
                sess,
                user_id=s.user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                context_name=context_name,
                source_name=path,
                index=op.index,
                plugin=plugin,
                file_path=path,
                payload=payload,
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
same as `ingest_file_to_source` but expects the file to be in the `$GULP_WORKING_DIR/ingest_local` directory on the server.

- ingest `operation_id` is taken from the source and checked for access.
- this API does not require custom request handling by the server and can be used from the `FastAPI /docs` page.

### tracking progress

refer to `ingest_file`

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
            description="the path (relative to `$GULP_WORKING_DIR`) to the file to be ingested.",
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
    plugin_params: Annotated[
        Optional[GulpPluginParameters],
        Body(
            description="if set, they will override the existing plugin parameters associated with the source."
        ),
    ] = None,
    flt: Annotated[
        GulpIngestionFilter,
        Depends(APIDependencies.param_ingestion_flt_optional),
    ] = None,
    delete_after: Annotated[
        Optional[bool],
        Query(
            description="if set, the file will be deleted after processing.",
        ),
    ] = False,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id_optional),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    # compute local path
    path: str = muty.file.safe_path_join(
        GulpConfig.get_instance().path_ingest_local(), path
    )
    MutyLogger.get_instance().info(
        "ingesting local file=%s, to source=%s", path, source_id
    )

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get source by id and check acl
            s: GulpUserSession
            src: GulpSource
            op: GulpOperation
            s, src, op = await GulpSource.get_by_id_wrapper(
                sess, token, source_id, GulpUserPermission.INGEST
            )

            # get context
            ctx: GulpContext = await GulpContext.get_by_id(sess, src.context_id)

            # use existing plugin parameters if not overridden
            if plugin_params.is_empty():
                plugin_params = GulpPluginParameters(
                    mapping_parameters=GulpMappingParameters.model_validate(
                        src.mapping_parameters
                    )
                )
            payload = GulpIngestPayload(
                flt=flt,
                plugin_params=plugin_params,
                original_file_path=path,
                file_sha1=None,  # not needed
            )

            return await _preview_or_enqueue_ingest_task(
                sess,
                user_id=s.user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=op.id,
                context_name=ctx.name,
                source_name=src.name,
                index=op.index,
                plugin=src.plugin,
                file_path=path,
                payload=payload,
                delete_after=delete_after,
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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

by default, the `raw` plugin is used: the data `chunk` is expected as a JSON text with a list of `GulpDocument` dictionaries.
it is possible, however, to implement another plugin which takes a chunk of raw bytes as input.

### tracking progress

the flow is the same as `ingest_file`.

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
the plugin used to process the raw chunk. by default, the `raw` plugin is used: the data `chunk` is expected as a JSON text with a list of `GulpDocument` dictionaries.""",
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
        Depends(APIDependencies.ensure_req_id_optional),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    if not plugin:
        plugin = "raw"
        MutyLogger.get_instance().debug("using default 'raw' plugin")

    ServerUtils.dump_params(params)
    mod: GulpPluginBase = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation
                s: GulpUserSession
                s, op, _ = await GulpOperation.get_by_id_wrapper(
                    sess,
                    token,
                    operation_id,
                    permission=GulpUserPermission.INGEST,
                )

                # get body (contains chunk, and optionally flt and plugin_params)
                payload, chunk = await ServerUtils.handle_multipart_body(r)
                flt: GulpIngestionFilter = GulpIngestionFilter.model_validate(
                    payload.get("flt", GulpIngestionFilter())
                )
                plugin_params: GulpPluginParameters = (
                    GulpPluginParameters.model_validate(
                        payload.get("plugin_params", GulpPluginParameters())
                    )
                )

                # create (or get existing) stats
                stats: GulpRequestStats
                stats, _ = await GulpRequestStats.create_or_get_existing(
                    sess,
                    req_id,
                    s.user_id,
                    operation_id,
                    ws_id=ws_id,
                    never_expire=True,
                    data=GulpIngestionStats().model_dump(exclude_none=True),
                )

                # run plugin
                mod = await GulpPluginBase.load(plugin)
                await mod.ingest_raw(
                    sess,
                    stats,
                    s.user_id,
                    req_id,
                    ws_id,
                    op.index,
                    operation_id,
                    chunk,
                    flt=flt,
                    plugin_params=plugin_params,
                    last=last,
                )
                await mod.update_final_stats_and_flush(flt=flt)

                # done
                return JSONResponse(JSendResponse.success(req_id=req_id))
            except Exception as ex:
                if mod:
                    await mod.update_final_stats_and_flush(flt=flt, ex=ex)
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    finally:
        if mod:
            await mod.unload()


async def _process_metadata_json(
    unzipped_path: str, payload: GulpZipIngestPayload
) -> list[GulpIngestPayload]:
    """
    Process metadata.json file from unzipped path and extract files information.

    metadata.json is expected to be a list of GulpZipMetadataEntry dictionaries, each containing information about each source file.

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


async def _ingest_zip_internal(
    path: str,
    context_name: str,
    operation_id: str,
    index: str,
    user_id: str,
    ws_id: str,
    req_id: str,
    payload: GulpZipIngestPayload,
    delete_after: bool = True,
) -> None:
    """
    unzip the zip file at path, read metadata.json and enqueue ingestion tasks for each file in the zip.

    the ingestion tasks will be processed by workers.
    """

    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats object
            stats: GulpRequestStats
            stats, _ = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                ws_id=ws_id,
                data=GulpIngestionStats(source_total=0).model_dump(exclude_none=True),
            )

            try:
                # unzip in a temporary directory
                unzipped = await muty.file.unzip(path)

                # get file list and read metadata json
                files: list[GulpIngestPayload] = await _process_metadata_json(
                    unzipped, payload
                )

                # update with total number of files to ingest (update ORM object)
                stats.data = GulpIngestionStats(source_total=len(files)).model_dump(
                    exclude_none=True
                )
                await sess.commit()
            except Exception as ex:
                # set finished manually (we haven't reached the ingest loop yet)
                await stats.set_finished(
                    sess,
                    GulpRequestStatus.FAILED,
                    errors=muty.log.exception_to_string(ex),
                )
                raise

            # enqueue tasks (they will be processed by workers)
            for f in files:
                kwds = dict(
                    context_name=context_name,
                    source_name=f.original_file_path,
                    req_id=req_id,
                    ws_id=ws_id,
                    index=index,
                    plugin=f.model_extra.get("plugin"),
                    file_path=f.model_extra.get("local_file_path"),
                    payload=f.model_dump(exclude_none=True),
                    delete_after=delete_after,
                )
                await GulpTask.enqueue(
                    sess,
                    task_type=TASK_TYPE_INGEST,
                    operation_id=operation_id,
                    user_id=user_id,
                    ws_id=ws_id,
                    req_id=req_id,
                    params=kwds,
                )
        finally:
            # delete the zip file
            await muty.file.delete_file_or_dir_async(path)


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

### tracking progress

same as `ingest_file`, but the stats will contain the total number of files to be ingested and the number of files ingested so far.

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
        Depends(APIDependencies.ensure_req_id_optional),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation
                s: GulpUserSession
                s, op, _ = await GulpOperation.get_by_id_wrapper(
                    sess, token, operation_id, permission=GulpUserPermission.INGEST
                )

                # handle multipart request manually
                file_path: str = None
                result: GulpUploadResponse = None
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

                # process in background: the background task will unzip the file and enqueue the tasks
                coro = _ingest_zip_internal(
                    file_path,
                    context_name,
                    operation_id,
                    op.index,
                    s.user_id,
                    ws_id,
                    req_id,
                    payload,
                    delete_after=True,
                )
                from gulp.api.server_api import GulpServer

                GulpServer.get_instance().spawn_bg_task(coro)

                # and return pending
                return JSONResponse(JSendResponse.pending(req_id=req_id))

            except Exception as ex:
                await muty.file.delete_file_or_dir_async(file_path)
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
this is basically a local version of the `ingest_zip` function, which does not require a multipart request and can be used from the `FastAPI /docs` page.

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
            description="the path of the ZIP file to be ingested, relative to must be relative to `$WORKING_DIR/ingest_local` directory.",
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
        Depends(APIDependencies.ensure_req_id_optional),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        # compute local path
        path: str = muty.file.safe_path_join(
            GulpConfig.get_instance().path_ingest_local(), path
        )
        MutyLogger.get_instance().info("ingesting local ZIP file: %s", path)

        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation
                s: GulpUserSession
                s, op, _ = await GulpOperation.get_by_id_wrapper(
                    sess, token, operation_id, permission=GulpUserPermission.INGEST
                )

                payload = GulpZipIngestPayload(
                    flt=flt,
                )

                # process in background: the background task will unzip the file and enqueue the tasks
                coro = _ingest_zip_internal(
                    path,
                    context_name,
                    operation_id,
                    op.index,
                    s.user_id,
                    ws_id,
                    req_id,
                    payload,
                    delete_after=delete_after,
                )
                from gulp.api.server_api import GulpServer

                GulpServer.get_instance().spawn_bg_task(coro)

                # and return pending
                return JSONResponse(JSendResponse.pending(req_id=req_id))
            except Exception as ex:
                if delete_after:
                    # delete the zip file
                    await muty.file.delete_file_or_dir_async(path)
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    summary="lists the `$GULP_WORKING_DIR/ingest_local` directory.",
    description="""
- returned paths are relative to `$GULP_WORKING_DIR/ingest_local`
""",
)
async def ingest_local_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.INGEST
            )

            local_dir: str = GulpConfig.get_instance().path_ingest_local()
            MutyLogger.get_instance().info(
                "listing local ingestion directory: %s", local_dir
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
        raise JSendException(req_id=req_id) from ex
