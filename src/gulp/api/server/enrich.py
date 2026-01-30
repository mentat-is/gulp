"""
Module for handling document enrichment operations in the Gulp REST API.

This module provides endpoints for enriching documents with new data and adding tags to documents.
Key functionalities include:

1. Enriching multiple documents using a specified plugin.
2. Enriching a single document and returning the result directly.
3. Adding tags to documents to facilitate future queries.

Each operation updates the documents in OpenSearch and can be tracked through
websocket communication for asynchronous operations.
"""

from typing import Annotated

import muty.log
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from llvmlite.tests.test_ir import flt
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from scipy import stats
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpRequestStats,
    GulpUpdateDocumentsStats,
    RequestCanceledError,
    RequestStatsType,
)
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.structs import GulpDocument, GulpQueryParameters
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpPluginParameters

router: APIRouter = APIRouter()


async def _enrich_documents_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    flt: GulpQueryFilter,
    operation_id: str,
    index: str,
    plugin: str,
    fields: dict,
    plugin_params: GulpPluginParameters,
) -> None:
    """
    called by the enrich_documents API entrypoint, runs in a worker to enrich documents with the given plugin
    """
    # MutyLogger.get_instance().debug("---> _enrich_documents_internal")
    errors: list[str] = []
    enriched: int = 0
    stats: GulpRequestStats
    mod: GulpPluginBase = None
    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, just to allow request canceling
            stats, _ = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats().model_dump(exclude_none=True),
            )

            # call plugin, the engine will update stats internally
            mod = await GulpPluginBase.load(plugin)
            _, enriched, errs, _ = await mod.enrich_documents(
                sess,
                stats,
                user_id,
                req_id,
                ws_id,
                operation_id,
                index,
                fields,
                flt=flt,
                plugin_params=plugin_params,
            )
            errors.extend(errs)
        except Exception as ex:
            if stats:
                if not mod:
                    # close the stats as failed (module load failed)
                    errors.append(muty.log.exception_to_string(ex))
                    await stats.set_finished(
                        sess,
                        status=GulpRequestStatus.STATUS_FAILED,
                        errors=errors,
                        user_id=user_id,
                        ws_id=ws_id,
                    )
            raise
        finally:
            # done
            if mod:
                await mod.unload()

            if enriched:
                # if we enriched something, update source=>fields mappings on the collab db
                await GulpOpenSearch.get_instance().datastream_update_source_field_types_by_flt(
                    sess, index, user_id, flt
                )

@router.post(
    "/enrich_documents",
    response_model=JSendResponse,
    tags=["enrich"],
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
    summary="Enrich documents.",
    description="""
uses an `enrichment` plugin to augment data in multiple documents.

- token must have the `edit` permission.
- this funciton returns `pending` and the enriched documents are updated in the Gulp `operation_id.index` and  streamed on the websocket `ws_id` as `GulpDocumentsChunkPacket`.
- `flt.operation_ids` is ignored and set to `[operation_id]`
- `flt` is provided as a `GulpQueryFilter` to select the documents to enrich, i.e. using a `time_range` or `ids`.

### tracking progress

during enrichment, the following is sent on the websocket `ws_id`:

- `WSDATA_STATS_CREATE`.payload: `GulpRequestStats`, data=`GulpUpdateDocumentsStats`, data.req_type=`enrich` (at start)
- `WSDATA_STATS_UPDATE`.payload: `GulpRequestStats`, data=updated `GulpUpdateDocumentsStats` (once every 1000 documents and in the end)

""",
)
async def enrich_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    fields: Annotated[dict, Depends(APIDependencies.param_enrich_fields)],
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # enforce operation_id
            flt.operation_ids = [operation_id]

            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.EDIT
            )
            user_id = s.user.id
            index = op.index

            # offload to a worker process and return pending
            await GulpServer.get_instance().spawn_worker_task(
                _enrich_documents_internal,
                user_id,
                req_id,
                ws_id,
                flt,
                operation_id,
                index,
                plugin,
                fields, 
                plugin_params,
            )
            return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/enrich_single_id",
    response_model=JSendResponse,
    tags=["enrich"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": autogenerate_model_example_by_class(GulpDocument),
                    }
                }
            }
        }
    },
    summary="Enrich a single document.",
    description="""
uses an `enrichment` plugin to augment data in a single document and returns it directly.

- token must have the `edit` permission.
- the enriched document is updated in the Gulp `index`.
""",
)
async def enrich_single_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document to enrich."),
    ],
    fields: Annotated[dict, Depends(APIDependencies.param_enrich_fields)],
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)
    mod: GulpPluginBase = None

    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s: GulpUserSession = await GulpUserSession.check_token(
                    sess, token, obj=op, permission=GulpUserPermission.EDIT
                )
                index = op.index
                user_id = s.user.id

                # load plugin and enrich document
                mod = await GulpPluginBase.load(plugin)
                doc = await mod.enrich_single_document(
                    sess, doc_id, operation_id, index, fields, plugin_params
                )

                # rebuild source_fields mapping in a worker, to free up the API
                await GulpServer.get_instance().spawn_worker_task(
                    GulpOpenSearch.datastream_update_source_field_types_by_src_wrapper,
                    None,  # sess=None to create a temporary one (a worker can't use the current one)
                    index,
                    user_id,
                    operation_id=doc["gulp.operation_id"],
                    context_id=doc["gulp.context_id"],
                    source_id=doc["gulp.source_id"],
                )
                return JSONResponse(JSendResponse.success(req_id, data=doc))
            finally:
                if mod:
                    await mod.unload()

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

async def _update_documents_chunk(
    sess: AsyncSession,
    chunk: list[dict],
    chunk_num: int = 0,
    total_hits: int = 0,
    index: str = None,
    last: bool = False,
    req_id: str = None,
    q_name: str = None,
    q_group: str = None,
    **kwargs,
) -> list[dict]:
    """GulpDocumentsChunkCallback to update each chunk of documents"""
    cb_context = kwargs["cb_context"]
    data = cb_context["data"]
    stats: GulpRequestStats = cb_context["stats"]
    ws_id = cb_context["ws_id"]
    flt: GulpQueryFilter = cb_context["flt"]

    # tag documents
    for d in chunk:
        d.update(data)

    MutyLogger.get_instance().debug("updated %d documents, last=%r", len(chunk), last)

    # update the documents on opensearch
    # also ensure no highlight field is left from the query
    for d in chunk:
        d.pop("_highlight", None)

    dry_run: bool = GulpConfig.get_instance().debug_enrich_dry_run()
    if dry_run:
        # dry run, no update
        updated = len(chunk)
        errs = []
    else:
        updated, _, errs = await GulpOpenSearch.get_instance().update_documents(
            index, chunk, wait_for_refresh=last
        )
    num_updated = updated
    cb_context["total_updated"] += num_updated
    cb_context["errors"].extend(errs)

    # update running stats
    await stats.update_updatedocuments_stats(
        sess,
        user_id=stats.user_id,
        ws_id=ws_id,
        total_hits=total_hits,
        updated=num_updated,
        flt=flt,
        errors=errs,
        last=last,
    )
    return chunk

async def _update_documents_internal(
    user_id: str,
    ws_id: str,
    req_id: str,
    operation_id: str,
    index: str,
    flt: GulpQueryFilter,
    data: dict
) -> None:
    """
    called by the update_documents API entrypoint, runs in a worker process to update documents

    data must contain the fields to be updated in the document/s returned by the filter
    i.e. {
        "gulp.tags": ["tag1", "tag2"],
        "custom_field": "custom_value"
    }
    will add/update the `gulp.tags` and `custom_field` fields in the matched documents with the new values
    """

    MutyLogger.get_instance().debug("---> _update_documents_internal")

    # build query
    if flt.is_empty():
        # match all query
        q = {"query": {"match_all": {}}}
    else:
        # use the given filter
        q = flt.to_opensearch_dsl()

    # force return all fields
    q_options = GulpQueryParameters(fields="*")
    stats: GulpRequestStats = None
    enriched: int = 0
    total_hits: int = 0
    errors: list[str] = []

    cb_context = {
        "total_updated": 0,
        "flt": flt,
        "errors": errors,
        "data": data,
        "ws_id": ws_id,
    }
    stats: GulpRequestStats
    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, just to allow request canceling
            stats, _ = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats().model_dump(exclude_none=True),
            )
            cb_context["stats"] = stats
            enriched, total_hits = await GulpOpenSearch.get_instance().search_dsl(
                sess,
                index,
                q,
                req_id=req_id,
                q_options=q_options,
                callback=_update_documents_chunk,
                cb_context=cb_context,
            )
        except Exception as ex:
            if stats and not total_hits:
                if not isinstance(ex, RequestCanceledError):
                    # close the stats as failed
                    errors.append(muty.log.exception_to_string(ex))
                    await stats.set_finished(
                        sess,
                        status=GulpRequestStatus.FAILED,
                        errors=errors,
                        user_id=user_id,
                        ws_id=ws_id,
                    )
            raise
        finally:
            if enriched:
                # if we enriched something, update source=>fields mappings on the collab db
                await GulpOpenSearch.get_instance().datastream_update_source_field_types_by_flt(
                    sess, index, user_id, flt
                )

@router.post(
    "/update_documents",
    response_model=JSendResponse,
    tags=["enrich"],
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
    summary="Update documents.",
    description="""
Update documents with the given arbitrary data.

- token must have the `edit` permission.
- this funciton returns `pending` and the enriched documents are updated in the gulp index identified by `operation_id` and  streamed on the websocket `ws_id` as `GulpDocumentsChunkPacket`.
- `flt.operation_ids` is ignored and set to `[operation_id]`

### tracking progress

Updating is an `enrichment`, from gulp's point of view: so, the flow on `ws_id` is the same as the `enrich_documents` API.

- `WSDATA_STATS_CREATE`.payload: `GulpRequestStats`, data=`GulpUpdateDocumentsStats`, data.req_type=`enrich` (at start)
- `WSDATA_STATS_UPDATE`.payload: `GulpRequestStats`, data=updated `GulpUpdateDocumentsStats` (once every 1000 documents)

""",
)
async def update_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)],
    data: Annotated[dict, Body(description='The data to update the documents with.', example='{ "gulp.tags": ["tag1","tag2"], "custom_field": "custom_value" }')],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # enforce operation_id
            flt.operation_ids = [operation_id]

            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.EDIT
            )
            user_id = s.user.id
            index = op.index

            # offload to a worker process and return pending
            await GulpServer.get_instance().spawn_worker_task(
                _update_documents_internal,
                user_id,
                ws_id,
                req_id,
                operation_id,
                index,
                flt,
                data,
            )
            return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.post(
    "/update_single_id",
    response_model=JSendResponse,
    tags=["enrich"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": autogenerate_model_example_by_class(GulpDocument),
                    }
                }
            }
        }
    },
    summary="Updates a single document with arbitrary data.",
    description="""
same as `update_documents`, but for a single document.

- token must have the `edit` permission.
- the enriched document is updated in the gulp's index identified by `operation_id`.
""",
)
async def update_single_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document to update."),
    ],
    data: Annotated[dict, Body(description='The data to update the documents with.', example='{ "gulp.tags": ["tag1","tag2"], "custom_field": "custom_value" }')],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.EDIT
            )
            index = op.index
            user_id = s.user.id

            # get the document
            doc: dict = await GulpOpenSearch.get_instance().query_single_document(
                index, doc_id
            )

            # update the document
            doc.update(data)
            dry_run: bool = GulpConfig.get_instance().debug_enrich_dry_run()
            if not dry_run:
                # do not update if dry run is set ....
                await GulpOpenSearch.get_instance().update_documents(
                    index, [doc], wait_for_refresh=True
                )

                # rebuild source_fields mapping in a worker
                await GulpServer.get_instance().spawn_worker_task(
                    GulpOpenSearch.datastream_update_source_field_types_by_src_wrapper,
                    None,  # sess=None to create a temporary one (a worker can't use the current one)
                    index,
                    user_id,
                    operation_id=doc["gulp.operation_id"],
                    context_id=doc["gulp.context_id"],
                    source_id=doc["gulp.source_id"],
                )
            return JSONResponse(JSendResponse.success(req_id, data=doc))

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex

@router.post(
    "/tag_documents",
    response_model=JSendResponse,
    tags=["enrich"],
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
    summary="Add tags to document/s.",
    description="""
shortcut to `update_documents` to update the given documents with `gulp.tags`.
""",
)
async def tag_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)],
    tags: Annotated[list[str], Depends(APIDependencies.param_tags)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    return await update_documents_handler(
        token=token,
        operation_id=operation_id,
        flt=flt,
        data={"gulp.tags": tags},
        ws_id=ws_id,
        req_id=req_id,
    )


@router.post(
    "/tag_single_id",
    response_model=JSendResponse,
    tags=["enrich"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": autogenerate_model_example_by_class(GulpDocument),
                    }
                }
            }
        }
    },
    summary="Tag a single document.",
    description="""
shortcut to `update_single_id` to update the given document with `gulp.tags`.
""",
)
async def tag_single_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document to tag."),
    ],
    tags: Annotated[list[str], Depends(APIDependencies.param_tags)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    return await update_single_id_handler(
        token=token,
        operation_id=operation_id,
        doc_id=doc_id,
        data={"gulp.tags": tags},
        req_id=req_id,
    )
