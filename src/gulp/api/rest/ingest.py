import json
import os
from typing import Annotated, Optional, override

import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field
from muty.pydantic import (
    autogenerate_model_example,
    autogenerate_model_example_by_class,
)
from gulp.api.opensearch.structs import GulpRawDocument
import gulp.api.rest.defs as api_defs
from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpIngestionStats
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import GulpSharedWsQueue, GulpWsQueueDataType
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters


class GulpBaseIngestPayload(BaseModel):
    """
    base payload for ingestion
    """

    flt: Optional[GulpIngestionFilter] = Field(
        GulpIngestionFilter(), description="The ingestion filter."
    )


class GulpIngestPayload(GulpBaseIngestPayload):
    """
    payload for ingest_file
    """

    model_config = ConfigDict(extra="allow")

    plugin_params: Optional[GulpPluginParameters] = Field(
        GulpPluginParameters(), description="The plugin parameters."
    )
    original_file_path: Optional[str] = Field(
        None, description="The original file path."
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)


class GulpZipIngestPayload(GulpBaseIngestPayload):
    """
    payload for ingest_zip
    """


class GulpZipMetadataEntry(BaseModel):
    """
    metadata entry for ingest_zip
    """

    plugin: str = Field(
        ..., description="The plugin to use.", examples=[api_defs.EXAMPLE_PLUGIN]
    )
    files: list[str] = Field(
        ..., description="The files to ingest.", examples=["file1.evtx"]
    )
    original_path: Optional[str] = Field(
        None,
        description="The original base path where `files` are taken from.",
        examples=["c:\\logs"],
    )
    plugin_params: Optional[GulpPluginParameters] = Field(
        GulpPluginParameters(), description="The plugin parameters."
    )


class GulpIngestSourceDone(BaseModel):
    """
    to signal on the websocket that a source ingestion is done
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
    )
    source_id: str = Field(
        ...,
        description="The source ID.",
        alias="gulp.source_id",
        examples=[api_defs.EXAMPLE_SOURCE_ID],
    )
    context_id: str = Field(
        ...,
        description="The context ID.",
        alias="gulp.context_id",
        examples=[api_defs.EXAMPLE_CONTEXT_ID],
    )
    req_id: str = Field(
        ..., description="The request ID.", examples=[api_defs.EXAMPLE_REQ_ID]
    )
    status: GulpRequestStatus = Field(
        ..., description="The request status.", examples=["done"]
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)


router = APIRouter()

EXAMPLE_INCOMPLETE_UPLOAD = {
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

EXAMPLE_DONE_UPLOAD = {
    "content": {
        "application/json": {
            "example": {
                "status": "pending",
                "req_id": "2fe81cdf-5f0a-482e-a5b2-74684c6e05fb",
                "timestamp_msec": 1618220400000,
            }
        }
    }
}


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
) -> None:
    """
    runs in a worker process to ingest a single file
    """
    # MutyLogger.get_instance().debug("---> ingest_single_internal")
    async with GulpCollab.get_instance().session() as sess:
        # create stats (or get existing)
        stats: GulpIngestionStats = await GulpIngestionStats.create(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            context_id=context_id,
            source_total=file_total,
        )

    async with GulpCollab.get_instance().session() as sess:
        mod: GulpPluginBase = None
        status = GulpRequestStatus.DONE
        sess.add(stats)

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
            )
        except Exception as ex:
            status = GulpRequestStatus.FAILED
            d = dict(
                source_failed=1,
                error=ex,
            )
            await stats.update(sess, d, ws_id=ws_id, user_id=user_id)
        finally:
            # send done packet on the websocket
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.INGEST_SOURCE_DONE,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=operation_id,
                data=GulpIngestSourceDone(
                    source_id=source_id,
                    context_id=context_id,
                    req_id=req_id,
                    status=status,
                ),
            )

            # delete file
            await muty.file.delete_file_or_dir_async(file_path)

            # done
            if mod:
                await mod.unload()


@router.post(
    "/ingest_file",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        100: EXAMPLE_INCOMPLETE_UPLOAD,
        200: EXAMPLE_DONE_UPLOAD,
    },
    description="""
**NOTE**: This function cannot be used from the `FastAPI /docs` page since it needs custom request handling to support resume.

### internals

The following is an example CURL for the request:

```bash
curl -v -X POST "http://localhost:8080/ingest_file?index=testidx&token=&plugin=win_evtx&client_id=1&operation_id=1&context=testcontext&req_id=2fe81cdf-5f0a-482e-a5b2-74684c6e05fb&sync=0&ws_id=the_ws_id" \
-k \
-H "size: 69632" \
-H "continue_offset: 0" \
-F "payload={\"flt\":{},\"plugin_params\":{}};type=application/json" \
-F "f=@/home/valerino/repos/gulp/samples/win_evtx/new-user-security.evtx;type=application/octet-stream"
```

the request have the following headers:

* `size`: The total size of the file being uploaded
* `continue_offset`: The offset of the chunk being uploaded (must be 0 if this is the first chunk)

### payload

the json payload is a `GulpIngestPayload`, which may contain the following fields:

* `flt` (GulpIngestionFilter): the ingestion filter, to restrict ingestion to a subset of the data specifying a `time_range`
* `plugin_params` (GulpPluginParameters): the plugin parameters, specific for the plugin being used
* `original_file_path` (str): the original file path, to indicate the original full path of the file being ingested on the machine where it was acquired from

### response with resume handling

if the upload is interrupted before finishing, the **next** upload of the same file with the **same** `req_id` will return an **error** response
with `data` set to a `GulpUploadResponse`:

* `done` (bool): set to `False`
* `continue_offset` (int): the offset of the next chunk to be uploaded, if `done` is `False`

to resume file, the uploader must then forge a request (as in the example above) with the given `continue_offset` set.

once the upload is complete, the API will return a `pending` response and processing will start in the background.

### websocket

once the upload is complete, this function returns a `pending` response and the following will be sent on the `ws_id` websocket during processing, every `ingestion_buffer_size` documents (defined in the configuration):

- `GulpWsQueueDataType.STATS_UPDATE`: with totals and stats
- `GulpWsQueueDataType.DOCUMENTS_CHUNK`: the actual GulpDocuments chunk
""",
    summary="ingest file using the specified plugin.",
    openapi_extra={
        "parameters": [
            {
                "name": "size",
                "in": "header",
                "required": True,
                "schema": {"type": "integer"},
                "description": api_defs.API_DESC_HEADER_SIZE,
                "example": 69632,
            },
            {
                "name": "continue_offset",
                "in": "header",
                "required": False,
                "default": 0,
                "schema": {"type": "integer"},
                "description": api_defs.API_DESC_HEADER_CONTINUE_OFFSET,
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
        Depends(ServerUtils.param_token),
    ],
    operation_id: Annotated[
        str,
        Depends(ServerUtils.param_operation_id),
    ],
    context_id: Annotated[
        str,
        Depends(ServerUtils.param_context_id),
    ],
    index: Annotated[
        str,
        Depends(ServerUtils.param_index),
    ],
    plugin: Annotated[
        str,
        Depends(ServerUtils.param_plugin),
    ],
    ws_id: Annotated[
        str,
        Depends(ServerUtils.param_ws_id),
    ],
    file_total: Annotated[
        int,
        Query(
            description="set to the total number of files if this call is part of a multi-file upload, default=1.",
            example=1,
        ),
    ] = 1,
    req_id: Annotated[
        str,
        Depends(ServerUtils.ensure_req_id),
    ] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        # handle multipart request manually
        file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
            r=r, operation_id=operation_id, context_id=context_id, req_id=req_id
        )
        if not result.done:
            # must continue upload with a new chunk
            d = JSendResponse.error(
                req_id=req_id, data=result.model_dump(exclude_none=True)
            )
            return JSONResponse(d, status_code=100)

        # ensure payload is valid
        payload = GulpIngestPayload.model_validate(payload)

        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.INGEST]
            )
            user_id = s.user_id

            # create (and associate) context and source on the collab db, if they do not exist
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)

            ctx: GulpContext = await operation.add_context(
                sess, user_id=user_id, context_id=context_id
            )

            src: GulpSource = await ctx.add_source(
                sess,
                user_id=user_id,
                name=payload.original_file_path or os.path.basename(file_path),
            )

        # run ingestion in a coroutine in one of the workers
        MutyLogger.get_instance().debug("spawning ingestion task ...")
        kwds = dict(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            context_id=context_id,
            source_id=src.id,
            index=index,
            plugin=plugin,
            file_path=file_path,
            file_total=file_total,
            payload=payload,
        )
        # print(json.dumps(kwds, indent=2))

        # spawn a task which runs the ingestion in a worker process
        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _ingest_file_internal, kwds=kwds
            )

        await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))

    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


async def _ingest_raw_internal(
    req_id: str,
    ws_id: str,
    user_id: str,
    operation_id: str,
    context_id: str,
    source_id: str,
    index: str,
    chunk: dict,
    flt: GulpIngestionFilter,
    plugin: str,
    plugin_params: GulpPluginParameters,
) -> None:
    """
    runs in a worker process to ingest a raw chunk of data
    """
    # MutyLogger.get_instance().debug("---> ingest_raw_internal")

    async with GulpCollab.get_instance().session() as sess:
        stats: GulpIngestionStats = await GulpIngestionStats.create(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            context_id=context_id,
            source_total=1,
        )

        mod: GulpPluginBase = None
        status = GulpRequestStatus.DONE

        try:
            # run plugin
            plugin = plugin or "raw"
            mod = await GulpPluginBase.load(plugin)
            status = await mod.ingest_raw(
                sess,
                stats,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                index=index,
                operation_id=operation_id,
                context_id=context_id,
                source_id=source_id,
                chunk=chunk,
                flt=flt,
                plugin_params=plugin_params,
            )
        except Exception as ex:
            status = GulpRequestStatus.FAILED
            d = dict(
                source_failed=1,
                error=ex,
            )
            await stats.update(sess, d, ws_id=ws_id, user_id=user_id)
        finally:
            # send done packet on the websocket
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.INGEST_SOURCE_DONE,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=operation_id,
                data=GulpIngestSourceDone(
                    source_id=source_id,
                    context_id=context_id,
                    req_id=req_id,
                    status=status,
                ),
            )

            # done
            if mod:
                await mod.unload()


@router.post(
    "/ingest_raw",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        100: EXAMPLE_INCOMPLETE_UPLOAD,
        200: EXAMPLE_DONE_UPLOAD,
    },
    description="""
ingests a chunk of `raw` documents.

### plugin

by default, the `raw` plugin is used (**must be available**), but a different plugin can be specified using the `plugin` parameter.
""",
    summary="ingest raw documents.",
)
async def ingest_raw_handler(
    token: Annotated[
        str,
        Depends(ServerUtils.param_token),
    ],
    operation_id: Annotated[
        str,
        Depends(ServerUtils.param_operation_id),
    ],
    context_id: Annotated[
        str,
        Depends(ServerUtils.param_context_id),
    ],
    source: Annotated[
        str,
        Query(
            description="name of the source to associate the data with, a source on the collab db will be generated if it doesn't exist.",
            example="raw source",
        ),
    ],
    index: Annotated[
        str,
        Depends(ServerUtils.param_index),
    ],
    ws_id: Annotated[
        str,
        Depends(ServerUtils.param_ws_id),
    ],
    chunk: Annotated[
        list[GulpRawDocument],
        Body(description="a chunk of raw documents to be ingested."),
    ],
    flt: Annotated[
        GulpIngestionFilter, Depends(ServerUtils.param_gulp_ingestion_flt)
    ] = None,
    plugin: Annotated[
        str,
        Depends(ServerUtils.param_plugin),
    ] = "raw",
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(ServerUtils.param_gulp_plugin_params),
    ] = None,
    req_id: Annotated[
        str,
        Depends(ServerUtils.ensure_req_id),
    ] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.INGEST]
            )
            user_id = s.user_id

            # create (and associate) context and source on the collab db, if they do not exist
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            ctx: GulpContext = await operation.add_context(
                sess, user_id=user_id, context_id=context_id
            )
            src: GulpSource = await ctx.add_source(
                sess,
                user_id=user_id,
                name=source,
            )

        # run ingestion in a coroutine in one of the workers
        MutyLogger.get_instance().debug("spawning RAW ingestion task ...")
        kwds = dict(
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            context_id=context_id,
            source_id=src.id,
            index=index,
            chunk=chunk,
            flt=flt,
            plugin=plugin,
            plugin_params=plugin_params,
        )
        # print(json.dumps(kwds, indent=2))

        # spawn a task which runs the ingestion in a worker process
        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _ingest_raw_internal, kwds=kwds
            )

        await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


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
    metadata = json.loads(js)

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
    openapi_extra={
        "parameters": [
            {
                "name": "size",
                "in": "header",
                "required": True,
                "schema": {"type": "integer"},
                "description": api_defs.API_DESC_HEADER_SIZE,
                "example": 69632,
            },
            {
                "name": "continue_offset",
                "in": "header",
                "required": False,
                "default": 0,
                "schema": {"type": "integer"},
                "description": api_defs.API_DESC_HEADER_CONTINUE_OFFSET,
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
**WARNING**: This function cannot be used from the `FastAPI /docs` page since it needs custom request handling to support resume.

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
        Depends(ServerUtils.param_token),
    ],
    operation_id: Annotated[
        str,
        Depends(ServerUtils.param_operation_id),
    ],
    context_id: Annotated[
        str,
        Depends(ServerUtils.param_context_id),
    ],
    index: Annotated[
        str,
        Depends(ServerUtils.param_index),
    ],
    ws_id: Annotated[
        str,
        Depends(ServerUtils.param_ws_id),
    ],
    req_id: Annotated[
        str,
        Depends(ServerUtils.ensure_req_id),
    ] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    file_path: str = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.INGEST]
            )
            user_id = s.user_id

            # create (and associate) context on the collab db, if it does not exists
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)

            ctx: GulpContext = await operation.add_context(
                sess, user_id=user_id, context_id=context_id
            )

        # handle multipart request manually
        file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
            r=r, operation_id=operation_id, context_id=context_id, req_id=req_id
        )
        if not result.done:
            # must continue upload with a new chunk
            d = JSendResponse.error(
                req_id=req_id, data=result.model_dump(exclude_none=True)
            )
            return JSONResponse(d, status_code=100)

        # ensure payload is valid
        payload = GulpZipIngestPayload.model_validate(payload)

        # unzip in a temporary directory
        unzipped = await muty.file.unzip(file_path)

        # read metadata json
        files: list[GulpIngestPayload] = await _process_metadata_json(unzipped, payload)
        file_total = len(files)

        # ingest each file in a worker
        MutyLogger.get_instance().debug(
            "spawning %d ingestion tasks ..." % (file_total)
        )
        for f in files:
            # add source
            async with GulpCollab.get_instance().session() as sess:
                src: GulpSource = await ctx.add_source(
                    sess, user_id=user_id, name=f.original_file_path
                )

            kwds = dict(
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                context_id=context_id,
                source_id=src.id,
                index=index,
                plugin=f.model_extra.get("plugin"),
                file_path=f.model_extra.get("local_file_path"),
                file_total=file_total,
                payload=f,
            )

            # spawn a task which runs the ingestion in a worker process
            async def worker_coro(kwds: dict):
                await GulpProcess.get_instance().process_pool.apply(
                    _ingest_file_internal, kwds=kwds
                )

            await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))

    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
    finally:
        # cleanup
        await muty.file.delete_file_or_dir_async(file_path)
