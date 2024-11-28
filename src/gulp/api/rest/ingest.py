"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

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
from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field
from muty.pydantic import (
    autogenerate_model_example,
    autogenerate_model_example_by_class,
)
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

    flt: Optional[GulpIngestionFilter] = Field(
        GulpIngestionFilter(), description="The ingestion filter."
    )
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


class GulpAPIIngest:
    """
    handles rest entrypoint/s for ingestion
    """

    @staticmethod
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

    @staticmethod
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
**NOTE**: This function cannot be used from the `/docs` page since it needs custom request handling to support resume.

The following is an example CURL for the request, containing GulpIngestionFilter and GulpPluginParameters.

Headers:
* `size`: The total size of the file being uploaded
* `continue_offset`: The offset of the chunk being uploaded (must be 0 if this is the first chunk)

```bash
curl -v -X PUT "http://localhost:8080/ingest_file?index=testidx&token=&plugin=win_evtx&client_id=1&operation_id=1&context=testcontext&req_id=2fe81cdf-5f0a-482e-a5b2-74684c6e05fb&sync=0&ws_id=the_ws_id" \
    -k \
    -H "size: 69632" \
    -H "continue_offset: 0" \
    -F "payload={\"flt\":{},\"plugin_params\":{}};type=application/json" \
    -F "f=@/home/valerino/repos/gulp/samples/win_evtx/new-user-security.evtx;type=application/octet-stream"
```

the payload is a a `GulpIngestPayload`, which may contain the following fields:

* `flt` (GulpIngestionFilter): the ingestion filter, to restrict ingestion to a subset of the data specifying a `time_range`
* `plugin_params` (GulpPluginParameters): the plugin parameters, specific for the plugin being used
* `original_file_path` (str): the original file path, to indicate the original full path of the file being ingested on the machine where it was acquired from

response's `data` is a `ChunkedUploadResponse`, which contains the following fields:

* `done` (bool): indicates whether the upload is complete
* `continue_offset` (int): the offset of the next chunk to be uploaded, if `done` is `False`

once the file is fully uploaded, this function returns a `pending` response and `STATS_UPDATE`, `DOCUMENTS_CHUNK` are streamed to the `ws_id` websocket until done.

if the upload is interrupted, this API allows the upload resume `by sending a request with the same req_id`:

1. the server will check the `continue_offset` and `total_file_size` headers to verify the upload status
2. if the file is fully uploaded, the server will continue with the ingestion, processing the file.
3. if the file is not fully uploaded, the server will respond with an `error` status and `continue_offset` set to the next chunk to be uploaded.
4. once the upload is done, the server will automatically delete the uploaded file once processed.
            """,
        summary="ingest file using the specified plugin.",
    )
    async def ingest_file_handler(
        r: Request,
        token: Annotated[
            str,
            Header(
                description=api_defs.API_DESC_INGEST_TOKEN,
                examples=[api_defs.EXAMPLE_TOKEN],
            ),
        ],
        operation_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_OPERATION_ID,
                examples=[api_defs.EXAMPLE_OPERATION_ID],
            ),
        ],
        context_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_CONTEXT_ID,
                examples=[api_defs.EXAMPLE_CONTEXT_ID],
            ),
        ],
        index: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_INDEX,
                examples=[api_defs.EXAMPLE_INDEX],
            ),
        ],
        plugin: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_PLUGIN,
                examples=[api_defs.EXAMPLE_PLUGIN],
            ),
        ],
        ws_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_WS_ID, examples=[api_defs.EXAMPLE_WS_ID]
            ),
        ] = None,
        file_total: Annotated[
            int,
            Query(
                description="set to the total number of files if this call is part of a multi-file upload, default=1.",
                examples=[1],
            ),
        ] = 1,
        flt: Annotated[
            dict,
            Body(
                description=api_defs.API_DESC_INGESTION_FILTER,
                examples=[
                    autogenerate_model_example_by_class(GulpIngestionFilter)
                ],
            ),
        ] = None,
        plugin_params: Annotated[
            dict,
            Body(
                description=api_defs.API_DESC_PLUGIN_PARAMETERS,
                examples=[
                    autogenerate_model_example_by_class(GulpPluginParameters)
                ],
            ),
        ] = None,
        req_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_REQ_ID, examples=[api_defs.EXAMPLE_REQ_ID]
            ),
        ] = None,
    ) -> JSONResponse:
        # ensure a req_id exists
        MutyLogger.get_instance().debug("---> ingest_file_handler")
        req_id = GulpRestServer.ensure_req_id(req_id)

        try:
            # handle multipart request manually
            file_path, payload, result = (
                await ServerUtils.handle_multipart_chunked_upload(
                    r=r, operation_id=operation_id, context_id=context_id, req_id=req_id
                )
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
                operation: GulpOperation = await GulpOperation.get_by_id(
                    sess, operation_id
                )

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
                    GulpAPIIngest._ingest_file_internal, kwds=kwds
                )

            await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))

        except Exception as ex:
            raise JSendException(ex=ex, req_id=req_id)

    @staticmethod
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

    @staticmethod
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
ingests a chunk of data using the `raw` plugin (**must be available**).

the `chunks`array is a list of raw JSON documents to be ingested, each with the following format:

```json
{   // mandatory
    "__metadata__": {
        // mandatory, with a format supported by gulp
        "timestamp": "2021-01-01T00:00:00Z"
        // mandatory, the raw event as string
        "event_original": "raw event content",
        // optional, will be set to 0 if missing
        "event_code": "something"
    },
    // any other key/value pairs here, will be ingested according to plugin_params.ignore_mapping:
    "something": "value",
    "something_else": "value",
    "another_thing": 123,
}
```

if `plugin_params.ignore_mapping` is set to `True`, the mapping (if specified) will be ignored and fields in the resulting GulpDocuments will be ingested as is.
either, they will be prefixed with `gulp.unmapped`.
            """,
        summary="ingest a chunk of raw events.",
    )
    async def ingest_raw_handler(
        token: Annotated[
            str,
            Header(
                description=api_defs.API_DESC_INGEST_TOKEN,
                examples=[api_defs.EXAMPLE_TOKEN],
            ),
        ],
        operation_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_OPERATION_ID,
                examples=[api_defs.EXAMPLE_OPERATION_ID],
            ),
        ],
        context_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_CONTEXT_ID,
                examples=[api_defs.EXAMPLE_CONTEXT_ID],
            ),
        ],
        source: Annotated[
            str,
            Query(
                description="name of the source to associate the data with, a source on the collab db will be generated if it doesn't exist.",
                examples=["raw source"],
            ),
        ],
        index: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_INDEX,
                examples=[api_defs.EXAMPLE_INDEX],
            ),
        ],
        ws_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_WS_ID, examples=[api_defs.EXAMPLE_WS_ID]
            ),
        ],
        chunk: Annotated[
            list[dict],
            Body(description="chunk of raw JSON events to be ingested."),
        ],
        flt: Annotated[
            GulpIngestionFilter, Body(description=api_defs.API_DESC_INGESTION_FILTER)
        ] = None,
        plugin: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_PLUGIN,
                examples=[api_defs.EXAMPLE_PLUGIN],
            ),
        ] = "raw",
        plugin_params: Annotated[
            GulpPluginParameters,
            Body(description=api_defs.API_DESC_PLUGIN_PARAMETERS),
        ] = None,
        req_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_REQ_ID, examples=[api_defs.EXAMPLE_REQ_ID]
            ),
        ] = None,
    ) -> JSONResponse:
        # ensure a req_id exists
        MutyLogger.get_instance().debug("---> ingest_raw_handler")
        req_id = GulpRestServer.ensure_req_id(req_id)

        try:
            async with GulpCollab.get_instance().session() as sess:
                # check token and get caller user id
                s = await GulpUserSession.check_token(
                    sess, token, [GulpUserPermission.INGEST]
                )
                user_id = s.user_id

                # create (and associate) context and source on the collab db, if they do not exist
                operation: GulpOperation = await GulpOperation.get_by_id(
                    sess, operation_id
                )
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
                    GulpAPIIngest._ingest_raw_internal, kwds=kwds
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
        js = await muty.file.read_file_async(
            os.path.join(unzipped_path, "metadata.json")
        )
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

    @staticmethod
    @router.post(
        "/ingest_zip",
        tags=["ingest"],
        response_model=JSendResponse,
        response_model_exclude_none=True,
        description="""
**NOTE**: This function cannot be used from the `/docs` page since it needs custom request handling to support resume: refer to `ingest_file` for the request and response specifications.

- the uploaded zip file **must include** a `metadata.json` describing the file/s Gulp is going to ingest and the specific plugin/s to be used: look at `GulpZipMetadataEntry` for the format.

- the json payload is a `GulpZipIngestPayload`, which allows to specify a GulpIngestionFilter to be applied to all files in the zip.

for further information (headers, internals) refer to `ingest_file`.
""",
        summary="ingest a zip containing multiple sources, possibly using multiple plugins.",
    )
    async def ingest_zip_handler(
        r: Request,
        token: Annotated[
            str,
            Header(
                description=api_defs.API_DESC_INGEST_TOKEN,
                examples=[api_defs.EXAMPLE_TOKEN],
            ),
        ],
        operation_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_OPERATION_ID,
                examples=[api_defs.EXAMPLE_OPERATION_ID],
            ),
        ],
        context_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_CONTEXT_ID,
                examples=[api_defs.EXAMPLE_CONTEXT_ID],
            ),
        ],
        index: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_INDEX,
                examples=[api_defs.EXAMPLE_INDEX],
            ),
        ],
        ws_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_WS_ID, examples=[api_defs.EXAMPLE_WS_ID]
            ),
        ],
        flt: Annotated[
            GulpIngestionFilter, Body(description=api_defs.API_DESC_INGESTION_FILTER)
        ] = None,
        req_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_REQ_ID, examples=[api_defs.EXAMPLE_REQ_ID]
            ),
        ] = None,
    ) -> JSONResponse:
        # ensure a req_id exists
        MutyLogger.get_instance().debug("---> ingest_zip_handler")
        req_id = GulpRestServer.ensure_req_id(req_id)

        file_path: str = None
        try:
            async with GulpCollab.get_instance().session() as sess:
                # check token and get caller user id
                s = await GulpUserSession.check_token(
                    sess, token, [GulpUserPermission.INGEST]
                )
                user_id = s.user_id

                # create (and associate) context on the collab db, if it does not exists
                operation: GulpOperation = await GulpOperation.get_by_id(
                    sess, operation_id
                )

                ctx: GulpContext = await operation.add_context(
                    sess, user_id=user_id, context_id=context_id
                )

            # handle multipart request manually
            file_path, payload, result = (
                await ServerUtils.handle_multipart_chunked_upload(
                    r=r, operation_id=operation_id, context_id=context_id, req_id=req_id
                )
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
            files: list[GulpIngestPayload] = await GulpAPIIngest._process_metadata_json(
                unzipped, payload
            )
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
                        GulpAPIIngest._ingest_file_internal, kwds=kwds
                    )

                await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))

        except Exception as ex:
            raise JSendException(ex=ex, req_id=req_id)
        finally:
            # cleanup
            await muty.file.delete_file_or_dir_async(file_path)
