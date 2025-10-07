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
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpRequestStats,
    GulpUpdateDocumentsStats,
    RequestStatsType,
)
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.structs import GulpDocument, GulpQueryParameters
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
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
    plugin_params: GulpPluginParameters,
) -> None:
    """
    called by the enrich_documents API entrypoint, runs in a worker process to enrich documents with the given plugin
    """
    # MutyLogger.get_instance().debug("---> _enrich_documents_internal")
    mod: GulpPluginBase = None
    errors: list[str] = []
    total_hits: int = 0
    enriched: int = 0
    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, just to allow request canceling
            stats: GulpRequestStats = await GulpRequestStats.create_stats(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats(),
            )

            # load plugin
            mod = await GulpPluginBase.load(plugin)

            # enrich
            total_hits, enriched, errs = await mod.enrich_documents(
                sess,
                stats,
                user_id,
                req_id,
                ws_id,
                operation_id=operation_id,
                index=index,
                flt=flt,
                plugin_params=plugin_params,
            )
            errors.extend(errs)
        except Exception as ex:
            await sess.rollback()
            errors.append(str(ex))
            raise
        finally:
            if stats:
                # finish stats
                p = GulpUpdateDocumentsStats(
                    total_hits=total_hits,
                    plugin=plugin,
                    updated=enriched,
                    errors=errors,
                    flt=flt,
                )
                MutyLogger.get_instance().debug(
                    "enrich_documents: finished, total_hits=%d, enriched=%d, errors=%s",
                    p.total_hits,
                    p.updated,
                    p.errors,
                )
                await stats.set_finished(
                    sess,
                    status=(
                        GulpRequestStatus.FAILED if errors else GulpRequestStatus.DONE
                    ),
                    data=p.model_dump(),
                    user_id=user_id,
                    ws_id=ws_id,
                )
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
- `flt` is provided as a `GulpQueryFilter` to select the documents to enrich.
""",
)
async def enrich_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt)],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # enforce operation_id
                flt.operation_ids = [operation_id]

                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s = await GulpUserSession.check_token(
                    sess, token, obj=op, permission=GulpUserPermission.EDIT
                )
                user_id = s.user.id
                index = op.index
            except Exception as ex:
                await sess.rollback()
                raise

            # offload to a worker process and return pending
            await GulpRestServer.get_instance().spawn_worker_task(
                _enrich_documents_internal,
                user_id,
                ws_id,
                req_id,
                operation_id,
                index,
                plugin,
                plugin_params,
                flt,
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
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
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
                    sess, doc_id, operation_id, index, plugin_params
                )

                # rebuild source_fields mapping in a worker, to free up the API
                await GulpRestServer.get_instance().spawn_worker_task(
                    GulpOpenSearch.get_instance().datastream_update_source_field_types_by_src,
                    None,  # sess=None to create a temporary one (a worker can't use the current one)
                    index,
                    user_id,
                    operation_id=doc["gulp.operation_id"],
                    context_id=doc["gulp.context_id"],
                    source_id=doc["gulp.source_id"],
                    doc_ids=[doc_id],
                )
                return JSONResponse(JSendResponse.success(req_id, data=doc))
            except Exception as ex:
                await sess.rollback()
                raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    finally:
        if mod:
            await mod.unload()


async def _tag_documents_chunk_wrapper(
    sess: AsyncSession,
    chunk: list[dict],
    chunk_num: int = 0,
    total_hits: int = 0,
    ws_id: str = None,
    user_id: str = None,
    req_id: str = None,
    operation_id: str = None,
    index: str = None,
    q_name: str = None,
    chunk_total: int = 0,
    q_group: str = None,
    last: bool = False,
    **kwargs,
) -> list[dict]:
    """callback called by GulpOpenSearch.search_dsl to tag each chunk of documents"""
    cb_context = kwargs["cb_context"]
    tags = kwargs["tags"]
    if not chunk:
        MutyLogger.get_instance().warning("empty chunk")
        return []

    # tag documents
    for d in chunk:
        d.update({"gulp.tags": tags})

    MutyLogger.get_instance().debug("tagged %d documents", len(chunk))

    # update the documents on opensearch
    # also ensure no highlight field is left from the query
    for d in chunk:
        d.pop("highlight", None)

    updated, _, errs = await GulpOpenSearch.get_instance().update_documents(
        index, chunk, wait_for_refresh=last
    )
    cb_context["total_enriched"] += len(updated)
    cb_context["errors"].extend(errs)

    return chunk


async def _tag_documents_internal(
    user_id: str,
    ws_id: str,
    req_id: str,
    operation_id: str,
    index: str,
    flt: GulpQueryFilter,
    tags: list[str],
) -> None:
    """
    called by the tag_documents API entrypoint, runs in a worker process to tag documents
    """

    MutyLogger.get_instance().debug("---> _tag_documents_internal")

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
    cb_context = {"total_enriched": 0, "flt": flt, "errors": errors, "tags": tags}

    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, just to allow request canceling
            stats: GulpRequestStats = await GulpRequestStats.create_stats(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats(),
            )

            enriched, total_hits = await GulpOpenSearch.get_instance().search_dsl(
                sess,
                index,
                q,
                operation_id=operation_id,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                q_options=q_options,
                callback=_tag_documents_chunk_wrapper,
                cb_context=cb_context,
            )
        except Exception as ex:
            await sess.rollback()
            errors.append(str(ex))
            raise
        finally:
            if stats:
                # finish stats
                p = GulpUpdateDocumentsStats(
                    total_hits=total_hits,
                    updated=cb_context["total_enriched"],
                    errors=errors,
                    flt=flt,
                )
                MutyLogger.get_instance().debug(
                    "tag_documents: finished, total_hits=%d, tagged=%d, errors=%s",
                    p.total_hits,
                    p.updated,
                    p.errors,
                )
                await stats.set_finished(
                    sess,
                    status=(
                        GulpRequestStatus.FAILED if errors else GulpRequestStatus.DONE
                    ),
                    data=p.model_dump(),
                    user_id=user_id,
                    ws_id=ws_id,
                )
                if enriched:
                    # if we enriched something, update source=>fields mappings on the collab db
                    await GulpOpenSearch.get_instance().datastream_update_source_field_types_by_flt(
                        sess, index, user_id, flt
                    )


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
Tag important documents, so they can be queried back via `gulp.tags` provided via `GulpQueryFilter` as custom key.

- token must have the `edit` permission.
- this funciton returns `pending` and the enriched documents are updated in the Gulp `operation_id.index` and  streamed on the websocket `ws_id` as `GulpDocumentsChunkPacket`.
- `flt.operation_ids` is ignored and set to `[operation_id]`
- the enriched documents are updated in the Gulp `index`.
""",
)
async def tag_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt)],
    tags: Annotated[list[str], Body(description="The tags to add.")],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            try:
                # enforce operation_id
                flt.operation_ids = [operation_id]

                # get operation and check acl
                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s = await GulpUserSession.check_token(
                    sess, token, obj=op, permission=GulpUserPermission.EDIT
                )
                user_id = s.user.id
                index = op.index

            except Exception as ex:
                await sess.rollback()
                raise

            # offload to a worker process and return pending
            await GulpRestServer.get_instance().spawn_worker_task(
                _tag_documents_internal,
                user_id,
                ws_id,
                req_id,
                operation_id,
                index,
                flt,
                tags,
            )
            return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
same as `tag_documents`, but for a single document.

- token must have the `edit` permission.
- the enriched document is updated in the Gulp `index`.
""",
)
async def tag_single_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document to tag."),
    ],
    tags: Annotated[list[str], Body(description="The tags to add.")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

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

                # get the document
                doc: dict = await GulpOpenSearch.get_instance().query_single_document(
                    index, doc_id
                )

                # update the document
                doc["gulp.tags"] = tags
                await GulpOpenSearch.get_instance().update_documents(
                    index, [doc], wait_for_refresh=True
                )

                # rebuild source_fields mapping in a worker
                await GulpRestServer.get_instance().spawn_worker_task(
                    GulpOpenSearch.get_instance().datastream_update_source_field_types_by_src,
                    None,  # sess=None to create a temporary one (a worker can't use the current one)
                    index,
                    user_id,
                    operation_id=doc["gulp.operation_id"],
                    context_id=doc["gulp.context_id"],
                    source_id=doc["gulp.source_id"],
                    doc_ids=[doc_id],
                )
                return JSONResponse(JSendResponse.success(req_id, data=doc))
            except Exception as ex:
                await sess.rollback()
                raise

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
