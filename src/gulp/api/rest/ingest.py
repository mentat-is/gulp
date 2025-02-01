import asyncio
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
    autogenerate_model_example_by_class,
)
from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies, GulpUploadResponse
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import (
    GulpSharedWsQueue,
    GulpWsQueueDataType,
    GulpIngestSourceDonePacket,
)
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
            "example": {
                "status": "pending",
                "req_id": "2fe81cdf-5f0a-482e-a5b2-74684c6e05fb",
                "timestamp_msec": 1618220400000,
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
) -> None:
    """
    runs in a worker process to ingest a single file
    """
    # MutyLogger.get_instance().debug("---> _ingest_file_internal")

    async with GulpCollab.get_instance().session() as sess:
        mod: GulpPluginBase = None
        status = GulpRequestStatus.DONE

        stats: GulpRequestStats = await GulpRequestStats.create(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            source_total=file_total,
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
            )
        except Exception as ex:
            status = GulpRequestStatus.FAILED
            d = dict(
                source_failed=1,
                status=status,
                error=ex,
            )
            await stats.update(sess, d, ws_id=ws_id, user_id=user_id)
        finally:
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
        100: _EXAMPLE_INCOMPLETE_UPLOAD,
        200: _EXAMPLE_DONE_UPLOAD,
    },
    description="""

- **this function cannot be used from the `FastAPI /docs` page since it needs custom request handling to support resume**.

### internals

The following is an example CURL for the request:

```bash
curl -v -X POST http://localhost:8080/ingest_file?operation_id=test_operation&context_name=test_context&index=test_idx&plugin=win_evtx&ws_id=test_ws&req_id=test_req&file_total=1&token=6ed2eee7-cfbe-4f06-904a-414ed6e5a926 -H content-type: multipart/form-data -H token: 6ed2eee7-cfbe-4f06-904a-414ed6e5a926 -H size: 69632 -H continue_offset: 0 -F payload={"flt": {}, "plugin_params": {}, "original_file_path": "/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx"}; type=application/json -F f=@/home/valerino/repos/gulp/samples/win_evtx/Security_short_selected.evtx;type=application/octet-stream
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
    index: Annotated[
        str,
        Depends(APIDependencies.param_index),
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
            description="set to the total number of files if this call is part of a multi-file upload, default=1.",
            example=1,
        ),
    ] = 1,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params.pop("r")
    ServerUtils.dump_params(params)

    try:
        # handle multipart request manually
        file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
            r=r, operation_id=operation_id, context_name=context_name
        )
        if not result.done:
            # must continue upload with a new chunk
            d = JSendResponse.error(
                req_id=req_id, data=result.model_dump(exclude_none=True)
            )
            return JSONResponse(d, status_code=206)

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
                sess, user_id=user_id, name=context_name
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
            context_id=ctx.id,
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
        
        await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))
        
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
    chunk: list[dict],
    flt: GulpIngestionFilter,
    plugin: str,
    plugin_params: GulpPluginParameters,
) -> None:
    """
    runs in a worker process to ingest a raw chunk of GulpDocuments
    """
    # MutyLogger.get_instance().debug("---> ingest_raw_internal")

    async with GulpCollab.get_instance().session() as sess:
        # leave out stats in raw ingestion for now ...
        """stats: GulpRequestStats = await GulpRequestStatsreate(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            context_id=context_id,
            source_total=1,
        )"""

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
                context_id=context_id,
                source_id=source_id,
                chunk=chunk,
                flt=flt,
                plugin_params=plugin_params,
            )
        except Exception as ex:
            # leave out stats in raw ingestion for now ...
            """d = dict(
                source_failed=1,
                error=ex,
            )
            await stats.update(sess, d, ws_id=ws_id, user_id=user_id)"""
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
        100: _EXAMPLE_INCOMPLETE_UPLOAD,
        200: _EXAMPLE_DONE_UPLOAD,
    },
    description="""
ingests a chunk of raw `GulpDocument`s, i.e. coming from a gulp SIEM agent or similar.

### plugin

by default, the `raw` plugin is used (**must be available**), but a different plugin can be specified using the `plugin` parameter.
""",
    summary="ingest raw documents.",
)
async def ingest_raw_handler(
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
    source: Annotated[
        str,
        Query(
            description="name of the source to associate the data with, a source on the collab db will be generated if it doesn't exist.",
            example="raw source",
        ),
    ],
    index: Annotated[
        str,
        Depends(APIDependencies.param_index),
    ],
    ws_id: Annotated[
        str,
        Depends(APIDependencies.param_ws_id),
    ],
    chunk: Annotated[
        list[dict],
        Body(description="a chunk of raw `GulpDocument`s to be ingested."),
    ],
    flt: Annotated[
        GulpIngestionFilter, Depends(
            APIDependencies.param_ingestion_flt_optional)
    ] = None,
    plugin: Annotated[
        str,
        Depends(APIDependencies.param_plugin),
    ] = "raw",
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    req_id: Annotated[
        str,
        Depends(APIDependencies.ensure_req_id),
    ] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)

    ServerUtils.dump_params(params)
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
                sess, user_id=user_id, name=context_name
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
            context_id=ctx.id,
            source_id=src.id,
            index=index,
            chunk=chunk,
            flt=flt,
            plugin=plugin,
            plugin_params=plugin_params,
        )
        # print(json.dumps(kwds, indent=2))

        # spawn a task which runs the ingestion in a worker process's task
        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _ingest_raw_internal, kwds=kwds
            )

        await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))

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
                        os.path.join(e.original_path,
                                     f) if e.original_path else f
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
    index: Annotated[
        str,
        Depends(APIDependencies.param_index),
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
            # check token and get caller user id
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.INGEST]
            )
            user_id = s.user_id

            # create (and associate) context on the collab db, if it does not exists
            operation: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)

            ctx: GulpContext = await operation.add_context(
                sess, user_id=user_id, name=context_name
            )

        # handle multipart request manually
        file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
            r=r, operation_id=operation_id, context_name=context_name
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
                context_id=ctx.id,
                source_id=src.id,
                index=index,
                plugin=f.model_extra.get("plugin"),
                file_path=f.model_extra.get("local_file_path"),
                file_total=file_total,
                payload=f,
            )

            # spawn a task which runs the ingestion in a worker process's task
            async def worker_coro(kwds: dict):
                await GulpProcess.get_instance().process_pool.apply(
                    _ingest_file_internal, kwds=kwds
                )

            await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))
        
        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))

    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
    finally:
        # cleanup
        if result and result.done:
            MutyLogger.get_instance().debug("deleting uploaded file %s ..." % (file_path))
            await muty.file.delete_file_or_dir_async(file_path)
