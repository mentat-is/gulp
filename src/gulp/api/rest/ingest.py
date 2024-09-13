"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import json
import os
from typing import Annotated

import aiofiles
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
from requests_toolbelt.multipart import decoder

import gulp.api.collab_api as collab_api
import gulp.api.elastic_api as elastic_api
import gulp.api.rest_api as rest_api
import gulp.config as config
import gulp.defs
import gulp.plugin
import gulp.utils
import gulp.workers as workers
from gulp.api.collab.base import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.client import Client
from gulp.api.collab.operation import Operation
from gulp.api.collab.session import UserSession
from gulp.api.collab.user import User
from gulp.api.elastic.structs import GulpIngestionFilter
from gulp.plugin_internal import GulpPluginParams
from gulp.utils import _logger

_app: APIRouter = APIRouter()


def _handle_plugin_params(plugin_params: GulpPluginParams) -> None:
    rest_api.logger().debug("plugin_params: %s" % (plugin_params))

    if plugin_params is None:
        return

    if plugin_params.config_override is not None:
        # apply cfg overrides
        for k, v in plugin_params.config_override.items():
            config.override_runtime_parameter(k, v)


async def _check_parameters(
    token: str,
    req_id: str,
    permission: GulpUserPermission = GulpUserPermission.READ,
    operation_id: int = None,
    client_id: int = None,
) -> tuple[User, Operation, Client]:
    """
    A helper function to check the parameters of the request (token permission, operation and client) and raise a JSendException on error.

    returns: (User, Operation|None if operation_id is None, Client|None if client_id is None)
    """
    try:
        o = None
        c = None
        u, _ = await UserSession.check_token(
            await collab_api.collab(), token, permission
        )
        if operation_id is not None:
            o = await Operation.get(
                await collab_api.collab(), GulpCollabFilter(id=[operation_id])
            )
            o = o[0]

        if client_id is not None:
            c = await Client.get(
                await collab_api.collab(), GulpCollabFilter(id=[client_id])
            )
            c = c[0]

        return u, o, c

    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


async def _request_handle_multipart(r: Request, req_id: str) -> dict:
    """
    Handles a multipart/form-data request and saves the file chunk to disk, used by the ingest API.

    the multipart MUST be composed of two parts:
        a JSON payload (if empty, "{}" must be passed)
        file chunk, with filename and content

    headers must be:
        continue_offset: the offset of the next chunk to be uploaded (may be 0 if this is the first chunk)
        size: the total size of the file being uploaded
    Args:
        r (Request): The request object.
        req_id (str): The request ID.

    Returns:
        dict: A dictionary containing the file path, upload status ('done': bool), and optional payload dict.
    """

    # get headers and body
    _logger.debug("request headers: %s" % (r.headers))
    continue_offset: int = int(r.headers.get("continue_offset", 0))
    total_file_size: int = int(r.headers["size"])
    body = await r.body()

    # decode the multipart/form-data request
    data = decoder.MultipartDecoder(body, r.headers["content-type"])

    file_content: bytes = None
    json_payload_part = data.parts[0]
    file_part = data.parts[1]
    _logger.debug("json_payload_part.headers=\n%s" % (json_payload_part.headers))
    _logger.debug("file_part.headers=\n%s" % (file_part.headers))
    fsize: int = 0

    # ingestion filter
    payload = json_payload_part.content.decode("utf-8")
    payload_dict = None
    try:
        payload_dict = json.loads(payload)
        _logger.debug("ingestion json payload: %s" % (payload_dict))
        if len(payload_dict) == 0:
            _logger.warning('empty "payload" part')
            payload_dict = None
    except:
        _logger.exception('invalid or None "payload" part: %s' % (payload))

    # download file chunk (also ensure cache dir exists)
    content_disposition = file_part.headers[b"Content-Disposition"].decode("utf-8")
    _logger.debug("Content-Disposition: %s" % (content_disposition))
    fname_start: int = content_disposition.find("filename=") + len("filename=")
    fname_end: int = content_disposition.find(";", fname_start)
    filename: str = content_disposition[fname_start:fname_end]

    # if filename is quoted with single or double quotes, remove quotes
    if filename[0] in ['"', "'"]:
        filename = filename[1:]
    if filename[-1] in ['"', "'"]:
        filename = filename[:-1]

    _logger.debug("filename (extracted from Content-Disposition): %s" % (filename))
    cache_dir = config.upload_tmp_dir()
    cache_file_path = muty.file.safe_path_join(cache_dir, "%s/%s" % (req_id, filename), allow_relative=True)
    await aiofiles.os.makedirs(os.path.dirname(cache_file_path), exist_ok=True)
    fsize = await muty.file.get_size(cache_file_path)
    if fsize == total_file_size:
        # upload is already complete
        _logger.info("file size matches, upload is already complete!")
        js = {"file_path": cache_file_path, "done": True}
        if payload_dict is not None:
            # valid payload
            js["payload"] = payload_dict
        return js

    file_content = file_part.content
    # _logger.debug("filename=%s, file chunk size=%d" % (filename, len(file_content)))
    async with aiofiles.open(cache_file_path, "ab+") as f:
        _logger.debug(
            "writing chunk of size=%d at offset=%d in %s ..."
            % (len(file_content), continue_offset, cache_file_path)
        )
        await f.seek(continue_offset, os.SEEK_SET)
        await f.write(file_content)
        await f.flush()

    # get written file size
    fsize = await muty.file.get_size(cache_file_path)
    _logger.debug("current size of %s: %d" % (cache_file_path, fsize))
    if fsize == total_file_size:
        _logger.info("file size matches, upload complete!")
        js = {"file_path": cache_file_path, "done": True}
    else:
        _logger.warning(
            "file size mismatch(total=%d, current=%d), upload incomplete!"
            % (total_file_size, fsize)
        )
        js = {
            "file_path": cache_file_path,
            "continue_offset": fsize,
            "done": False,
        }
    if payload_dict is not None:
        # valid payload
        js["payload"] = payload_dict
    _logger.debug("type=%s, payload=%s" % (type(js), js))
    return js


def _get_ingest_payload(
    multipart_result: dict,
) -> tuple[
    GulpPluginParams,
    GulpIngestionFilter,
]:
    """
    get the plugin parameters and ingestion filter from the multipart result.

    returns: (GulpPluginParams, GulpIngestionFilter)

    NOTE: it is guaranteed that the returned values are not None. They are either the default values or the values from the payload.
    """
    payload = multipart_result.get("payload", None)
    if payload is None or len(payload) == 0:
        #  no payload
        rest_api.logger().debug("no payload found in multipart")
        return GulpPluginParams(), GulpIngestionFilter()

    # parse each part of the payload

    # ingestion filter
    flt = payload.get("flt", None)
    if flt is None or len(flt) == 0:
        flt = GulpIngestionFilter()
    else:
        flt = GulpIngestionFilter.from_dict(flt)

    # plugin parameters
    plugin_params = payload.get("plugin_params", None)
    if plugin_params is None:
        plugin_params = GulpPluginParams()
    else:
        plugin_params = GulpPluginParams.from_dict(plugin_params)
    _handle_plugin_params(plugin_params)

    if plugin_params.config_override is not None:
        # apply cfg overrides
        for k, v in plugin_params.config_override.items():
            config.override_runtime_parameter(k, v)

    rest_api.logger().debug("plugin_params=%s, flt=%s" % (plugin_params, flt))
    return plugin_params, flt


@_app.put(
    "/ingest_raw",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    description="""
        events may be an array of `GulpDocuments` or an array of arbitrary JSON documents.<br>
        <br><br>
        the following fields will be set if not present in each document:
        <br><br>
            - `agent.type` (str): set to `raw`.<br>
            - `@timestamp` (int): set to the ingestion time, in milliseconds from unix epoch.<br>
            - `operation_id` (int): set to provided `operation_id`.<br>
            - `agent.id` (int): set to provided `client_id`.<br>
            - `gulp.context` (str): set to provided `context`.
        <br><br>
        this function returns a `pending` response and `INGESTION_STATS_CREATE, INGESTION_CHUNK, INGESTION_STATS_UPDATE` are streamed on the websocket at `/ws` until done.
    """,
    summary="ingest a chunk of raw events.",
)
async def ingest_raw_handler(
    token: Annotated[
        str,
        Header(
            description=gulp.defs.API_DESC_TOKEN + " (must have INGEST permission)."
        ),
    ],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    operation_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_INGEST_OPERATION,
            openapi_examples=gulp.defs.EXAMPLE_OPERATION_ID,
        ),
    ],
    client_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_CLIENT,
            openapi_examples=gulp.defs.EXAMPLE_CLIENT_ID,
        ),
    ],
    context: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INGEST_CONTEXT,
            openapi_examples=gulp.defs.EXAMPLE_CONTEXT,
        ),
    ],
    events: Annotated[
        list[dict],
        Body(description="chunk of raw JSON events to be ingested."),
    ],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    plugin_params: Annotated[GulpPluginParams, Body()] = None,
    flt: Annotated[GulpIngestionFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    # check operation and client
    req_id = gulp.utils.ensure_req_id(req_id)
    u, _, _ = await _check_parameters(
        token,
        req_id,
        permission=GulpUserPermission.INGEST,
        operation_id=operation_id,
        client_id=client_id,
    )

    # process in background (may need to wait for pool space)
    coro = workers.ingest_single_file_or_events_task(
        await collab_api.collab(),
        elastic_api.elastic(),
        rest_api.process_executor(),
        index=index,
        req_id=req_id,
        f=events,
        plugin="raw",
        client=client_id,
        operation=operation_id,
        ws_id=ws_id,
        context=context,
        token=token,
        plugin_params=plugin_params,
        flt=flt,
        user_id=u.id,
    )
    await rest_api.aiopool().spawn(coro)

    # and return pending
    return muty.jsend.pending_jsend(req_id=req_id)


@_app.put(
    "/ingest_zip",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    description="""
    **NOTE**: this function cannot be used from the `/docs` page since it needs custom request handling (to support resume) which FastAPI (currently) does not support.
    <br><br>
    the zip file must include a `metadata.json` describing the file/s Gulp is going to ingest and the specific plugin/s to be used:
    <br>
    ```json
    {
        "win_evtx": {
            "files": ["win_evtx/system.evtx", "win_evtx/security.evtx"],
            // optional parameters to pass to the plugin
            "plugin_params": {
                // GulpPluginParams
                ...
            }
        },
        "apache_clf": {
            "files": ["apache_clf/access.log.sample"]
        }
    }
    ```
    for more details about the ingest process, look at *ingest_file* API description.""",
    summary="ingest a zip file.",
)
async def ingest_zip_handler(
    r: Request,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_INGEST_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    client_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_CLIENT,
            openapi_examples=gulp.defs.EXAMPLE_CLIENT_ID,
        ),
    ],
    operation_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_INGEST_OPERATION,
            openapi_examples=gulp.defs.EXAMPLE_OPERATION_ID,
        ),
    ],
    context: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INGEST_CONTEXT,
            openapi_examples=gulp.defs.EXAMPLE_CONTEXT,
        ),
    ],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    # flt: Annotated[GulpIngestionFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSONResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    u, _, _ = await _check_parameters(
        token,
        req_id,
        permission=GulpUserPermission.INGEST,
        operation_id=operation_id,
        client_id=client_id,
    )

    # handle multipart request manually
    multipart_result = await _request_handle_multipart(r, req_id)
    done: bool = multipart_result["done"]
    file_path: str = multipart_result["file_path"]
    continue_offset: int = multipart_result.get("continue_offset", 0)

    if not done:
        # must continue upload with a new chunk
        d = muty.jsend.success_jsend(
            req_id=req_id, data={"continue_offset": continue_offset}
        )
        return JSONResponse(d)

    # get filter, if any
    _, flt = _get_ingest_payload(multipart_result)

    # process in background (may need to wait for pool space)
    coro = workers.ingest_zip_task(
        await collab_api.collab(),
        elastic_api.elastic(),
        rest_api.process_executor(),
        ws_id=ws_id,
        index=index,
        req_id=req_id,
        f=file_path,
        client_id=client_id,
        operation_id=operation_id,
        context=context,
        parent=os.path.basename(file_path),
        flt=flt,
        user_id=u.id,
    )
    await rest_api.aiopool().spawn(coro)

    # and return pending
    return muty.jsend.pending_jsend(req_id=req_id)


@_app.put(
    "/ingest_zip_simple",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    description="""
    **NOTE**: this function cannot be used from the `/docs` page since it needs custom request handling (to support resume) which FastAPI (currently) does not support.
    <br><br>
    simpler version of `ingest_zip` which does not require a `metadata.json` file.
    <br><br>
    if the zip contains directories, they will be scanned recursively.
    <br>
    if `plugin` is not set, the zip file must have the following directory layout so the correct plugin is derived from the parent directory name:
    <br><br>
    - plugin1<br>
       - file1<br>
       - file2<br>
       - subdir1<br>
         - file3<br>
    - plugin2<br>
      - file4<br>
      - ...
    <br><br>
    for upload request details, look at the `ingest_file` API.""",
    summary="ingests a zip file.",
)
async def ingest_zip_simple_handler(
    r: Request,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_INGEST_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    client_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_CLIENT,
            openapi_examples=gulp.defs.EXAMPLE_CLIENT_ID,
        ),
    ],
    operation_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_INGEST_OPERATION,
            openapi_examples=gulp.defs.EXAMPLE_OPERATION_ID,
        ),
    ],
    context: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INGEST_CONTEXT,
            openapi_examples=gulp.defs.EXAMPLE_CONTEXT,
        ),
    ],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    plugin: Annotated[str, Query(description=gulp.defs.API_DESC_PLUGIN)] = None,
    mask: Annotated[
        str,
        Query(
            description='mask to filter files to be ingested, i.e. "*.evtx" (case insensitive).<br>leave empty to use "*" (all files, mismatching files will be skipped and counted as errors).'
        ),
    ] = None,
    # plugin_params: Annotated[GulpPluginParams, Body()] = None,
    # flt: Annotated[GulpIngestionFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSONResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    u, _, _ = await _check_parameters(
        token,
        req_id,
        permission=GulpUserPermission.INGEST,
        operation_id=operation_id,
        client_id=client_id,
    )

    # handle multipart request manually
    multipart_result = await _request_handle_multipart(r, req_id)
    done: bool = multipart_result["done"]
    file_path: str = multipart_result["file_path"]
    continue_offset: int = multipart_result.get("continue_offset", 0)

    if not done:
        # must continue upload with a new chunk
        d = muty.jsend.success_jsend(
            req_id=req_id, data={"continue_offset": continue_offset}
        )
        return JSONResponse(d)

    # get parameters and filter, if any
    plugin_params, flt = _get_ingest_payload(multipart_result)

    # process in background (may need to wait for pool space)
    coro = workers.ingest_zip_simple_task(
        await collab_api.collab(),
        elastic_api.elastic(),
        rest_api.process_executor(),
        index=index,
        req_id=req_id,
        f=file_path,
        client_id=client_id,
        operation_id=operation_id,
        ws_id=ws_id,
        context=context,
        parent=os.path.basename(file_path),
        plugin=plugin,
        plugin_params=plugin_params,
        flt=flt,
        user_id=u.id,
        mask=mask,
    )
    await rest_api.aiopool().spawn(coro)

    # and return pending
    return muty.jsend.pending_jsend(req_id=req_id)


@_app.put(
    "/ingest_file",
    tags=["ingest"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    description="""
    **NOTE**: this function cannot be used from the `/docs` page since it needs custom request handling (to support resume) which FastAPI (currently) does not support.
    <br><br>
    the following is an example CURL for the request:
    <br>
    `curl -v -X PUT http://localhost:8080/ingest_file?index=testidx&token=&plugin=win_evtx&client_id=1&operation_id=1&context=testcontext&req_id=2fe81cdf-5f0a-482e-a5b2-74684c6e05fb&sync=0&ws_id=the_ws_id
        -k
        -H size: 69632
        -H continue_offset: 0
        -F payload={
            "flt": {},
            "plugin_params": {}
        }; type=application/json
        -F f=@/home/valerino/repos/gulp/samples/win_evtx/new-user-security.evtx; type=application/octet-stream
    `
    <br><br>
    once the file is fully uploaded, this function returns a `pending` response and `INGESTION_STATS_CREATE, INGESTION_CHUNK, INGESTION_STATS_UPDATE` are streamed on the websocket at `/ws` until done.
    <br><br>
    **if the upload is interrupted, it may be resumed by using the same `req_id` in another request.**
    """,
    summary="ingest file using the specified plugin.",
)
async def ingest_file_handler(
    r: Request,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_INGEST_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    plugin: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_PLUGIN,
            openapi_examples=gulp.defs.EXAMPLE_PLUGIN,
        ),
    ],
    client_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_CLIENT,
            openapi_examples=gulp.defs.EXAMPLE_CLIENT_ID,
        ),
    ],
    operation_id: Annotated[
        int,
        Query(
            description=gulp.defs.API_DESC_INGEST_OPERATION,
            openapi_examples=gulp.defs.EXAMPLE_OPERATION_ID,
        ),
    ],
    context: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INGEST_CONTEXT,
            openapi_examples=gulp.defs.EXAMPLE_CONTEXT,
        ),
    ],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    # flt: Annotated[GulpIngestionFilter, Body()] = None,
    # plugin_params: Annotated[GulpPluginParams, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    u, _, _ = await _check_parameters(
        token,
        req_id,
        permission=GulpUserPermission.INGEST,
        operation_id=operation_id,
        client_id=client_id,
    )

    # handle multipart request manually
    rest_api.logger().debug("headers=%s" % (r.headers))
    multipart_result = await _request_handle_multipart(r, req_id)
    done: bool = multipart_result["done"]
    file_path: str = multipart_result["file_path"]
    continue_offset: int = multipart_result.get("continue_offset", 0)

    if not done:
        # must continue upload with a new chunk
        d = muty.jsend.success_jsend(
            req_id=req_id, data={"continue_offset": continue_offset}
        )
        return JSONResponse(d)

    # get parameters and filter, if any
    plugin_params, flt = _get_ingest_payload(multipart_result)

    # process in background (may need to wait for pool space)
    coro = workers.ingest_single_file_or_events_task(
        await collab_api.collab(),
        elastic_api.elastic(),
        rest_api.process_executor(),
        index=index,
        req_id=req_id,
        f=file_path,
        plugin=plugin,
        client=client_id,
        operation=operation_id,
        ws_id=ws_id,
        context=context,
        token=token,
        plugin_params=plugin_params,
        flt=flt,
        user_id=u.id,
    )
    await rest_api.aiopool().spawn(coro)

    # and return pending
    return muty.jsend.pending_jsend(req_id=req_id)


if __debug__:

    @_app.put(
        "/ingest_directory",
        tags=["ingest"],
        response_model=JSendResponse,
        response_model_exclude_none=True,
        description="ingest files in a local directory.<br><br>"
        "NOTE: `this API is mostly for debugging and is disabled in release builds`.",
        summary="ingest files in the given local directory, non recursively, using the specified plugin.",
    )
    async def ingest_directory_handler(
        token: Annotated[str, Header(description=gulp.defs.API_DESC_INGEST_TOKEN)],
        index: Annotated[
            str,
            Query(
                description=gulp.defs.API_DESC_INDEX,
                openapi_examples=gulp.defs.EXAMPLE_INDEX,
            ),
        ],
        directory: Annotated[
            str,
            Query(
                description="local (on the machine running gulp) directory with files to be ingested."
            ),
        ],
        plugin: Annotated[
            str,
            Query(
                description=gulp.defs.API_DESC_PLUGIN,
                openapi_examples=gulp.defs.EXAMPLE_PLUGIN,
            ),
        ],
        client_id: Annotated[
            int,
            Query(
                description=gulp.defs.API_DESC_CLIENT,
                openapi_examples=gulp.defs.EXAMPLE_CLIENT_ID,
            ),
        ],
        operation_id: Annotated[
            int,
            Query(
                description=gulp.defs.API_DESC_INGEST_OPERATION,
                openapi_examples=gulp.defs.EXAMPLE_OPERATION_ID,
            ),
        ],
        context: Annotated[
            str,
            Query(
                description=gulp.defs.API_DESC_INGEST_CONTEXT,
                openapi_examples=gulp.defs.EXAMPLE_CONTEXT,
            ),
        ],
        ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
        plugin_params: Annotated[
            GulpPluginParams,
            Body(),
        ] = None,
        flt: Annotated[GulpIngestionFilter, Body()] = None,
        req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
    ) -> JSendResponse:
        req_id = gulp.utils.ensure_req_id(req_id)
        u, _, _ = await _check_parameters(
            token,
            req_id,
            permission=GulpUserPermission.INGEST,
            operation_id=operation_id,
            client_id=client_id,
        )

        if plugin_params is not None:
            _handle_plugin_params(plugin_params)

        # process in background (may need to wait for pool space)
        coro = workers.ingest_directory_task(
            await collab_api.collab(),
            elastic_api.elastic(),
            rest_api.process_executor(),
            index=index,
            req_id=req_id,
            directory=directory,
            plugin=plugin,
            client_id=client_id,
            operation_id=operation_id,
            ws_id=ws_id,
            context=context,
            plugin_params=plugin_params,
            flt=flt,
            user_id=u.id,
        )
        await rest_api.aiopool().spawn(coro)

        # and return pending
        return muty.jsend.pending_jsend(req_id=req_id)


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
