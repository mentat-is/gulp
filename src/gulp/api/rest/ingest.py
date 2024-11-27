"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import json
import os
from typing import Annotated, Optional

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

import gulp.api.rest.defs as api_defs
from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpIngestionStats
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.rest.server_utils import GulpUploadResponse, ServerUtils
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import GulpSharedWsQueue, GulpWsQueueDataType
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters


class GulpIngestPayload(BaseModel):
    """
    payload for an ingestion request
    """

    flt: Optional[GulpIngestionFilter] = Field(
        GulpIngestionFilter(), description="The ingestion filter."
    )
    plugin_params: Optional[GulpPluginParameters] = Field(
        GulpPluginParameters(), description="The plugin parameters."
    )
    original_file_path: Optional[str] = Field(
        None, description="The original file path."
    )


class GulpIngestSourceDone(BaseModel):
    """
    to signal on the websocket that a source ingestion is done
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
    )
    source_id: str = Field(..., description="The source ID.", alias="gulp.source_id")
    context_id: str = Field(..., description="The context ID.", alias="gulp.context_id")
    req_id: str = Field(..., description="The request ID.")
    status: GulpRequestStatus = Field(..., description="The request status.")


class GulpAPIIngest:
    """
    handles rest entrypoint/s for ingestion
    """

    @staticmethod
    def router() -> APIRouter:
        """
        Returns this module api-router, to add it to the main router

        Returns:
            APIRouter: The APIRouter instance
        """
        router = APIRouter()
        router.add_api_route(
            "/ingest_file",
            GulpAPIIngest.ingest_file_handler,
            methods=["PUT"],
            tags=["ingest"],
            response_model=JSendResponse,
            response_model_exclude_none=True,
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

        router.add_api_route(
            "/ingest_raw",
            GulpAPIIngest.ingest_raw_handler,
            methods=["PUT"],
            tags=["ingest"],
            response_model=JSendResponse,
            response_model_exclude_none=True,
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

        router.add_api_route(
            "/ingest_zip",
            GulpAPIIngest.ingest_zip_handler,
            methods=["PUT"],
            tags=["ingest"],
            response_model=JSendResponse,
            response_model_exclude_none=True,
            description="""
**NOTE**: This function cannot be used from the `/docs` page since it needs custom request handling to support resume: refer to `ingest_file` for the request and response specifications.

the zip file **must include** a `metadata.json` describing the file/s Gulp is going to ingest and the specific plugin/s to be used:

```json
[
    {
        // plugin to handle the ingestion with
        "plugin": "win_evtx",
        // the original path where these files were found
        "original_path": "c:\\some\\path",
        // the files to ingest, relative path in the zip file
        "files": [
            "win_evtx/2-system-Microsoft-Windows-LiveId%4Operational.evtx",
            "win_evtx/2-system-Security-dirty.evtx",
        ],
    },
    ...
]
```
            """,
            summary="ingest a zip with multiple sources.",
        )

        return router

    @staticmethod
    async def _handle_multipart_request(
        r: Request, operation_id: str, context_id: str, req_id: str
    ) -> tuple[str, GulpIngestPayload, GulpUploadResponse]:
        """
        handle a multipart request, return the file path and the payload

        the file is saved in a temporary directory and the path is returned

        Args:
            r (Request): the request
            operation_id (str): the operation id
            context_id (str): the context id
            req_id (str): the request id

        Returns:
            tuple[str, GulpIngestPayload, GulpUploadResponse]: the file path, the payload and the upload response to indicate if the upload is done
        """
        MutyLogger.get_instance().debug("headers=%s" % (r.headers))
        file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
            r=r, operation_id=operation_id, context_id=context_id, req_id=req_id
        )
        MutyLogger.get_instance().debug(
            "file_path=%s,\npayload=%s,\nresult=%s"
            % (file_path, json.dumps(payload, indent=2), result)
        )
        payload = GulpIngestPayload.model_validate(payload)
        return file_path, payload, result

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
            # create stats
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
    async def ingest_file_handler(
        r: Request,
        token: Annotated[str, Header(description=api_defs.API_DESC_INGEST_TOKEN)],
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
        ws_id: Annotated[str, Query(description=api_defs.API_DESC_WS_ID)],
        file_total: Annotated[
            int,
            Query(
                description="set to the total number of files if this call is part of a multi-file upload, default=1."
            ),
        ] = 1,
        # flt: Annotated[GulpIngestionFilter, Body()] = None,
        # plugin_params: Annotated[GulpPluginParameters, Body()] = None,
        req_id: Annotated[str, Query(description=api_defs.API_DESC_REQ_ID)] = None,
    ) -> JSONResponse:
        # ensure a req_id exists
        MutyLogger.get_instance().debug("---> ingest_file_handler")
        req_id = GulpRestServer.ensure_req_id(req_id)

        try:
            # handle multipart request manually
            MutyLogger.get_instance().debug("headers=%s" % (r.headers))
            file_path, payload, result = await GulpAPIIngest._handle_multipart_request(
                r=r,
                operation_id=operation_id,
                context_id=context_id,
                req_id=req_id,
            )
            if not result.done:
                # must continue upload with a new chunk
                d = JSendResponse.error(
                    req_id=req_id, data=result.model_dump(exclude_none=True)
                )
                return JSONResponse(d)

            async with GulpCollab.get_instance().session() as sess:
                # check permission and get user id
                s = await GulpUserSession.check_token(
                    sess, token, [GulpUserPermission.INGEST]
                )
                user_id = s.user_id

            async with GulpCollab.get_instance().session() as sess:
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
    async def ingest_raw_handler(
        token: Annotated[str, Header(description=api_defs.API_DESC_INGEST_TOKEN)],
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
        ws_id: Annotated[str, Query(description=api_defs.API_DESC_WS_ID)],
        chunk: Annotated[
            list[dict],
            Body(description="chunk of raw JSON events to be ingested."),
        ],
        flt: Annotated[
            GulpIngestionFilter, Body(description="to filter ingested data.")
        ] = None,
        plugin: Annotated[
            str,
            Query(
                description="the plugin to use for ingestion, default=raw",
            ),
        ] = None,
        plugin_params: Annotated[
            GulpPluginParameters,
            Body(
                description='optional parameters for the "raw" plugin:<br><br>'
                "- `ignore_mapping` (str): ignore mapping if set, and ingest raw data as is."
            ),
        ] = None,
        req_id: Annotated[str, Query(description=api_defs.API_DESC_REQ_ID)] = None,
    ) -> JSONResponse:
        # ensure a req_id exists
        MutyLogger.get_instance().debug("---> ingest_raw_handler")
        req_id = GulpRestServer.ensure_req_id(req_id)

        try:
            # check token and get caller user id
            async with GulpCollab.get_instance().session() as sess:
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

    @staticmethod
    async def ingest_zip_handler(
        r: Request,
        token: Annotated[str, Header(description=api_defs.API_DESC_INGEST_TOKEN)],
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
        ws_id: Annotated[str, Query(description=api_defs.API_DESC_WS_ID)],
        # flt: Annotated[GulpIngestionFilter, Body()] = None,
        # plugin_params: Annotated[GulpPluginParameters, Body()] = None,
        req_id: Annotated[str, Query(description=api_defs.API_DESC_REQ_ID)] = None,
    ) -> JSONResponse:
        # ensure a req_id exists
        MutyLogger.get_instance().debug("---> ingest_zip_handler")
        req_id = GulpRestServer.ensure_req_id(req_id)

        try:
            # check token and get caller user id
            s = await GulpUserSession.check_token(token, GulpUserPermission.INGEST)
            user_id = s.user_id

            # handle multipart request manually
            MutyLogger.get_instance().debug("headers=%s" % (r.headers))
            file_path, payload, result = await GulpAPIIngest._handle_multipart_request(
                r=r, req_id=req_id
            )
            if not result.done:
                # must continue upload with a new chunk
                d = JSendResponse.error(
                    req_id=req_id, data=result.model_dump(exclude_none=True)
                )
                return JSONResponse(d)

            # unzip in a temporary directory
            unzipped = await muty.file.unzip(file_path)

            # read metadata json
            js = await muty.file.read_file_async(
                os.path.join(unzipped, "metadata.json")
            )
            metadata = json.loads(js)

            # metadata.json doesn't count
            files -= 1

            # spawn ingestion tasks for each file
            for f in files:
                # create (and associate) context and source on the collab db, if they do not exist
                await GulpOperation.add_context_to_id(operation_id, context_id)
                source = await GulpContext.add_source_to_id(operation_id, context_id, f)

                # run ingestion in a coroutine in one of the workers
                MutyLogger.get_instance().debug("spawning ingestion task ...")
                kwds = dict(
                    req_id=req_id,
                    ws_id=ws_id,
                    user_id=user_id,
                    operation_id=operation_id,
                    context_id=context_id,
                    source_id=source.id,
                    index=index,
                    plugin=plugin,
                    file_path=file_path,
                    file_total=file_total,
                    flt=payload.flt,
                    plugin_params=payload.plugin_params,
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

        """



# @_app.put(
#     "/ingest_zip",
#     tags=["ingest"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     description="""


#     **NOTE**: this function cannot be used from the `/docs` page since it needs custom request handling (to support resume) which FastAPI (currently) does not support.
#     <br><br>
#     the zip file must include a `metadata.json` describing the file/s Gulp is going to ingest and the specific plugin/s to be used:
#     <br>
#     ```json
#     {
#         "win_evtx": {
#             "files": ["win_evtx/system.evtx", "win_evtx/security.evtx"],
#             // optional parameters to pass to the plugin
#             "plugin_params": {
#                 // GulpPluginParameters
#                 ...
#             }
#         },
#         "apache_clf": {
#             "files": ["apache_clf/access.log.sample"]
#         }
#     }
#     ```
#     for more details about the ingest process, look at *ingest_file* API description.""",
#     summary="ingest a zip file.",
# )
# async def ingest_zip_handler(
#     r: Request,
#     token: Annotated[str, Header(description=api_defs.API_DESC_INGEST_TOKEN)],
#     index: Annotated[
#         str,
#         Query(
#             description=api_defs.API_DESC_INDEX,
#             openapi_examples=api_defs.EXAMPLE_INDEX,
#         ),
#     ],
#     client_id: Annotated[
#         int,
#         Query(
#             description=api_defs.API_DESC_CLIENT,
#             openapi_examples=api_defs.EXAMPLE_CLIENT_ID,
#         ),
#     ],
#     operation_id: Annotated[
#         int,
#         Query(
#             description=api_defs.API_DESC_INGEST_OPERATION,
#             openapi_examples=api_defs.EXAMPLE_OPERATION_ID,
#         ),
#     ],
#     context: Annotated[
#         str,
#         Query(
#             description=api_defs.API_DESC_INGEST_CONTEXT,
#             openapi_examples=api_defs.EXAMPLE_CONTEXT,
#         ),
#     ],
#     ws_id: Annotated[str, Query(description=api_defs.API_DESC_WS_ID)],
#     # flt: Annotated[GulpIngestionFilter, Body()] = None,
#     req_id: Annotated[str, Query(description=api_defs.API_DESC_REQID)] = None,
# ) -> JSONResponse:
#     req_id = gulp.utils.ensure_req_id(req_id)
#     u, _, _ = await _check_parameters(
#         token,
#         req_id,
#         permission=GulpUserPermission.INGEST,
#         operation_id=operation_id,
#         client_id=client_id,
#     )

#     # handle multipart request manually
#     multipart_result = await _request_handle_multipart(r, req_id)
#     done: bool = multipart_result["done"]
#     file_path: str = multipart_result["file_path"]
#     continue_offset: int = multipart_result.get("continue_offset", 0)

#     if not done:
#         # must continue upload with a new chunk
#         d = muty.jsend.success_jsend(
#             req_id=req_id, data={"continue_offset": continue_offset}
#         )
#         return JSONResponse(d)

#     # get filter, if any
#     _, flt = _get_ingest_payload(multipart_result)

#     # process in background (may need to wait for pool space)
#     coro = process.ingest_zip_task(
#         ws_id=ws_id,
#         index=index,
#         req_id=req_id,
#         f=file_path,
#         client_id=client_id,
#         operation_id=operation_id,
#         context=context,
#         parent=os.path.basename(file_path),
#         flt=flt,
#         user_id=u.id,
#     )
#     await rest_api.aiopool().spawn(coro)

#     # and return pending
#     return muty.jsend.pending_jsend(req_id=req_id)


# def router() -> APIRouter:
#     """
#     Returns this module api-router, to add it to the main router

#     Returns:
#         APIRouter: The APIRouter instance
#     """
#     global _app
#     return _app
