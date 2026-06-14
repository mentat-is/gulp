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
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
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
from gulp.api.prometheus_api import record_api_request_rejection
from gulp.api.redis_api import GulpRedis, TaskQueueFullError
from gulp.api.replay import (
    document_has_update_request,
    mark_document_update_request,
)
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies, TASK_TYPE_ENRICH
from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpPluginParameters

router: APIRouter = APIRouter()

ENRICH_TASK_ACTION_ENRICH_DOCUMENTS = "enrich_documents"
ENRICH_TASK_ACTION_UPDATE_DOCUMENTS = "update_documents"
ENRICH_TASK_ACTION_TAG_DOCUMENTS = "tag_documents"
ENRICH_TASK_ACTION_UNTAG_DOCUMENTS = "untag_documents"
ENRICH_TASK_ACTION_ENRICH_REMOVE = "enrich_remove"


def _task_queue_full_response(req_id: str, ex: TaskQueueFullError) -> JSONResponse:
    """Build a structured response for enrich/update queue-pressure rejections."""
    record_api_request_rejection(
        endpoint="enrich",
        reason="task_queue_full",
        task_type=ex.task_type,
        scope=ex.scope,
    )
    return JSONResponse(
        JSendResponse.error(
            req_id=req_id,
            ex={
                "error": "task_queue_full",
                "task_type": ex.task_type,
                "scope": ex.scope,
                "queue_depth": ex.queue_depth,
                "queue_limit": ex.queue_limit,
                "work_units": ex.work_units,
                "retry_after_msec": ex.retry_after_msec,
            },
        ),
        status_code=503,
    )


async def _enqueue_enrich_task(
    token: str,
    operation_id: str,
    ws_id: str,
    req_id: str,
    flt: GulpQueryFilter,
    action: str,
    *,
    data: dict | None = None,
    tags: list[str] | None = None,
    plugin: str | None = None,
    fields: dict | None = None,
    remove_fields: list[str] | None = None,
    plugin_params: GulpPluginParameters | None = None,
) -> JSONResponse:
    """Authorize and enqueue a multi-document enrich/update task."""
    async with GulpCollab.get_instance().session() as sess:
        flt.operation_ids = [operation_id]

        op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
        s = await GulpUserSession.check_token(
            sess, token, obj=op, permission=GulpUserPermission.EDIT
        )
        user_id = s.user.id

        params = {
            "action": action,
            "index": op.index,
            "flt": flt.model_dump(exclude_none=True),
        }
        if data is not None:
            params["data"] = data
        if tags is not None:
            params["tags"] = tags
        if plugin is not None:
            params["plugin"] = plugin
        if fields is not None:
            params["fields"] = fields
        if remove_fields is not None:
            params["remove_fields"] = remove_fields
        if plugin_params is not None:
            params["plugin_params"] = plugin_params.model_dump(exclude_none=True)

        task_msg = {
            "task_type": TASK_TYPE_ENRICH,
            "operation_id": operation_id,
            "user_id": user_id,
            "ws_id": ws_id,
            "req_id": req_id,
            "params": params,
        }
        custom_parameters = (
            plugin_params.custom_parameters if plugin_params is not None else {}
        )
        if isinstance(custom_parameters, dict) and custom_parameters.get(
            "task_timeout_sec"
        ):
            task_msg["__task_timeout_sec__"] = custom_parameters["task_timeout_sec"]
        try:
            await GulpRedis.get_instance().task_enqueue(task_msg)
        except TaskQueueFullError as ex:
            return _task_queue_full_response(req_id, ex)

        return JSONResponse(JSendResponse.pending(req_id=req_id))


async def run_enrich_task(t: dict) -> bool:
    """Run a queued multi-document enrich/update task."""
    params: dict = t.get("params", {})
    action = params.get("action")
    user_id: str = t.get("user_id")
    req_id: str = t.get("req_id")
    operation_id: str = t.get("operation_id")
    ws_id: str = t.get("ws_id")
    index: str = params.get("index")
    flt_payload = params.get("flt") or {}
    flt = (
        GulpQueryFilter.model_validate(flt_payload)
        if isinstance(flt_payload, dict)
        else flt_payload
    )

    if action == ENRICH_TASK_ACTION_ENRICH_DOCUMENTS:
        plugin_params_payload = params.get("plugin_params")
        plugin_params = (
            GulpPluginParameters.model_validate(plugin_params_payload)
            if isinstance(plugin_params_payload, dict)
            else plugin_params_payload
        ) or GulpPluginParameters()
        await GulpServer.get_instance().spawn_worker_task(
            _enrich_documents_internal,
            user_id,
            req_id,
            ws_id,
            flt,
            operation_id,
            index,
            params.get("plugin"),
            params.get("fields") or {},
            plugin_params,
            task_name=f"enrich_{req_id}",
            wait=True,
        )
        return True

    if action in {
        ENRICH_TASK_ACTION_UPDATE_DOCUMENTS,
        ENRICH_TASK_ACTION_TAG_DOCUMENTS,
    }:
        await GulpServer.get_instance().spawn_worker_task(
            _update_documents_internal,
            user_id,
            ws_id,
            req_id,
            operation_id,
            index,
            flt,
            params.get("data") or {},
            task_name=f"enrich_{req_id}",
            wait=True,
        )
        return True

    if action == ENRICH_TASK_ACTION_UNTAG_DOCUMENTS:
        await GulpServer.get_instance().spawn_worker_task(
            _untag_documents_internal,
            user_id,
            ws_id,
            req_id,
            operation_id,
            index,
            flt,
            params.get("tags") or [],
            task_name=f"enrich_{req_id}",
            wait=True,
        )
        return True

    if action == ENRICH_TASK_ACTION_ENRICH_REMOVE:
        await GulpServer.get_instance().spawn_worker_task(
            _enrich_remove_internal,
            user_id,
            ws_id,
            req_id,
            operation_id,
            index,
            flt,
            params.get("remove_fields") or [],
            task_name=f"enrich_{req_id}",
            wait=True,
        )
        return True

    raise ValueError(f"unknown enrich task action: {action!r}")


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
    total_hits: int = 0
    stats: GulpRequestStats = None
    mod: GulpPluginBase = None
    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, just to allow request canceling
            stats, stats_created = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats().model_dump(exclude_none=True),
            )
            if (
                stats
                and not stats_created
                and GulpRequestStats.is_terminal_status(stats.status)
            ):
                MutyLogger.get_instance().warning(
                    "enrich request %s already terminal with status=%s, skipping replay",
                    req_id,
                    stats.status,
                )
                return

            # call plugin, the engine will update stats internally
            mod = await GulpPluginBase.load(plugin)
            total_hits, enriched, errs, canceled = await mod.enrich_documents(
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
                defer_final_stats=True,
            )
            errors.extend(errs)
            if enriched:
                # if we enriched something, update source=>fields mappings on the collab db
                await GulpOpenSearch.get_instance().datastream_update_source_field_types_by_flt(
                    sess, index, user_id, flt
                )
            if not canceled:
                await _finalize_update_documents_stats(
                    sess,
                    stats,
                    user_id,
                    ws_id,
                    total_hits,
                    flt,
                    errors,
                )

        except Exception as ex:
            if stats and not isinstance(ex, RequestCanceledError):
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
            # done
            if mod:
                await mod.unload()


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
    fields: Annotated[dict, Depends(APIDependencies.param_enrich_fields_optional)],
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
        return await _enqueue_enrich_task(
            token,
            operation_id,
            ws_id,
            req_id,
            flt,
            ENRICH_TASK_ACTION_ENRICH_DOCUMENTS,
            plugin=plugin,
            fields=fields,
            plugin_params=plugin_params,
        )
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
    fields: Annotated[dict, Depends(APIDependencies.param_enrich_fields_optional)],
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


async def _modify_documents_chunk(
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
    """GulpDocumentsChunkCallback to modify each chunk of documents"""
    cb_context = kwargs["cb_context"]
    mutate_fn = cb_context["mutate_fn"]
    stats: GulpRequestStats = cb_context["stats"]
    ws_id = cb_context["ws_id"]
    flt: GulpQueryFilter = cb_context["flt"]
    stats_last = last and not cb_context.get("defer_final_stats", False)

    updated_docs = []
    already_updated = 0
    for d in chunk:
        if document_has_update_request(d, req_id):
            already_updated += 1
            continue

        if mutate_fn(d):
            mark_document_update_request(d, req_id)
            updated_docs.append(d)

    MutyLogger.get_instance().debug(
        "processed %d documents (updated %d), last=%r",
        len(chunk),
        len(updated_docs),
        last,
    )

    # update the documents on opensearch
    # also ensure no highlight field is left from the query
    for d in updated_docs:
        d.pop("_highlight", None)

    dry_run: bool = GulpConfig.get_instance().debug_enrich_dry_run()
    if dry_run:
        updated = len(updated_docs)
        errs = []
    else:
        if updated_docs:
            updated, _, errs = await GulpOpenSearch.get_instance().update_documents(
                index, updated_docs, wait_for_refresh=last
            )
        else:
            updated = 0
            errs = []

    num_updated = already_updated + updated
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
        last=stats_last,
        update_key=f"modify_documents:{req_id}:{chunk_num}:{stats_last}",
    )
    return chunk


async def _finalize_update_documents_stats(
    sess: AsyncSession,
    stats: GulpRequestStats,
    user_id: str,
    ws_id: str,
    total_hits: int,
    flt: GulpQueryFilter,
    errors: list[str],
) -> None:
    """Finalize document update stats after all post-update work is complete."""
    await stats.update_updatedocuments_stats(
        sess,
        user_id=user_id,
        ws_id=ws_id,
        total_hits=total_hits,
        updated=0,
        flt=flt,
        errors=errors,
        last=True,
        update_key=f"modify_documents:{stats.id}:final",
    )


def _mutate_update_document(data: dict):
    def _mutate(doc: dict) -> bool:
        doc.update(data)
        return True

    return _mutate


def _mutate_untag_document(tags: list[str]):
    def _mutate(doc: dict) -> bool:
        return _remove_tags_from_doc(doc, tags)

    return _mutate


async def _update_documents_internal(
    user_id: str,
    ws_id: str,
    req_id: str,
    operation_id: str,
    index: str,
    flt: GulpQueryFilter,
    data: dict,
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
        "mutate_fn": _mutate_update_document(data),
        "ws_id": ws_id,
        "defer_final_stats": True,
    }
    stats: GulpRequestStats
    async with GulpCollab.get_instance().session() as sess:
        try:
            # create a stats, just to allow request canceling
            stats, stats_created = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats().model_dump(exclude_none=True),
            )
            if (
                stats
                and not stats_created
                and GulpRequestStats.is_terminal_status(stats.status)
            ):
                MutyLogger.get_instance().warning(
                    "update-documents request %s already terminal with status=%s, skipping replay",
                    req_id,
                    stats.status,
                )
                return
            cb_context["stats"] = stats
            enriched, total_hits = await GulpOpenSearch.get_instance().search_dsl(
                sess,
                index,
                q,
                req_id=req_id,
                q_options=q_options,
                callback=_modify_documents_chunk,
                cb_context=cb_context,
            )
            if enriched:
                # Keep the terminal websocket stats update after source-field refresh,
                # so operation cleanup cannot race a still-running enrich worker.
                await GulpOpenSearch.get_instance().datastream_update_source_field_types_by_flt(
                    sess, index, user_id, flt
                )
            await _finalize_update_documents_stats(
                sess,
                stats,
                user_id,
                ws_id,
                total_hits,
                flt,
                errors,
            )
        except Exception as ex:
            if stats and not isinstance(ex, RequestCanceledError):
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


def _remove_tags_from_doc(doc: dict, tags_to_remove: list[str]) -> bool:
    """Remove tags in tags_to_remove from the document and return whether a change was made."""

    # Determine tags location(s) in the doc representation
    current_tags = list(doc.get("gulp.tags"))
    if current_tags is None:
        return False

    if not tags_to_remove:
        # remove all tags
        doc.pop("gulp.tags", None)
        return True

    # remove the tags to remove from the current tags
    new_tags = [t for t in current_tags if t not in tags_to_remove]
    if new_tags == current_tags:
        return False

    doc["gulp.tags"] = new_tags
    return True


async def _untag_documents_internal(
    user_id: str,
    ws_id: str,
    req_id: str,
    operation_id: str,
    index: str,
    flt: GulpQueryFilter,
    tags: list[str],
) -> None:
    """
    called by the untag_documents API entrypoint, runs in a worker process to remove tags from documents
    """

    MutyLogger.get_instance().debug("---> _untag_documents_internal")

    # build query
    if flt.is_empty():
        q = {"query": {"match_all": {}}}
    else:
        q = flt.to_opensearch_dsl()

    q_options = GulpQueryParameters(fields="*")
    stats: GulpRequestStats = None
    enriched: int = 0
    total_hits: int = 0
    errors: list[str] = []

    cb_context = {
        "total_updated": 0,
        "flt": flt,
        "errors": errors,
        "mutate_fn": _mutate_untag_document(tags),
        "ws_id": ws_id,
        "index": index,
        "defer_final_stats": True,
    }

    async with GulpCollab.get_instance().session() as sess:
        try:
            stats, stats_created = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats().model_dump(exclude_none=True),
            )
            if (
                stats
                and not stats_created
                and GulpRequestStats.is_terminal_status(stats.status)
            ):
                MutyLogger.get_instance().warning(
                    "untag-documents request %s already terminal with status=%s, skipping replay",
                    req_id,
                    stats.status,
                )
                return
            cb_context["stats"] = stats
            enriched, total_hits = await GulpOpenSearch.get_instance().search_dsl(
                sess,
                index,
                q,
                req_id=req_id,
                q_options=q_options,
                callback=_modify_documents_chunk,
                cb_context=cb_context,
            )
            if enriched:
                await GulpOpenSearch.get_instance().datastream_update_source_field_types_by_flt(
                    sess, index, user_id, flt
                )
            await _finalize_update_documents_stats(
                sess,
                stats,
                user_id,
                ws_id,
                total_hits,
                flt,
                errors,
            )
        except Exception as ex:
            if stats and not isinstance(ex, RequestCanceledError):
                errors.append(muty.log.exception_to_string(ex))
                await stats.set_finished(
                    sess,
                    status=GulpRequestStatus.FAILED,
                    errors=errors,
                    user_id=user_id,
                    ws_id=ws_id,
                )
            raise


def _build_enrich_remove_request(
    flt: GulpQueryFilter,
    fields: list[str] | None,
) -> dict:
    """Build the OpenSearch update_by_query request body for enrich_remove."""
    if fields:
        exists_clauses = [{"exists": {"field": f}} for f in fields]
        exists_clause = (
            exists_clauses[0]
            if len(exists_clauses) == 1
            else {
                "bool": {
                    "should": exists_clauses,
                    "minimum_should_match": 1,
                }
            }
        )
        if flt.is_empty():
            base_query = exists_clause
        else:
            base_query = {"bool": {"must": [flt.to_opensearch_dsl()["query"], exists_clause]}}
        script: dict = {
            "source": (
                "for (f in params.fields) {"
                " if (ctx._source.containsKey(f)) { ctx._source.remove(f); }"
                " }"
            ),
            "lang": "painless",
            "params": {"fields": fields},
        }
    else:
        exists_clause = {"exists": {"field": "gulp.enriched"}}
        if flt.is_empty():
            base_query = exists_clause
        else:
            base_query = {"bool": {"must": [flt.to_opensearch_dsl()["query"], exists_clause]}}
        script = {
            "source": (
                "if (ctx._source.containsKey('gulp.enriched')) {"
                " ctx._source.remove('gulp.enriched'); }"
            ),
            "lang": "painless",
        }

    return {
        "script": script,
        "query": base_query,
    }


async def _enrich_remove_internal(
    user_id: str,
    ws_id: str,
    req_id: str,
    operation_id: str,
    index: str,
    flt: GulpQueryFilter,
    fields: list[str] | None,
) -> None:
    """Remove enrichment fields from multiple documents in a queued worker."""
    stats: GulpRequestStats = None
    errors: list[str] = []
    fields = fields or []
    source_fields_changed = False

    async with GulpCollab.get_instance().session() as sess:
        try:
            stats, stats_created = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                req_type=RequestStatsType.REQUEST_TYPE_ENRICHMENT,
                ws_id=ws_id,
                data=GulpUpdateDocumentsStats(flt=flt).model_dump(exclude_none=True),
            )
            if (
                stats
                and not stats_created
                and GulpRequestStats.is_terminal_status(stats.status)
            ):
                MutyLogger.get_instance().warning(
                    "enrich-remove request %s already terminal with status=%s, skipping replay",
                    req_id,
                    stats.status,
                )
                return

            params_body = _build_enrich_remove_request(flt, fields)
            MutyLogger.get_instance().debug(
                "enrich_remove: update_by_query body=%s", params_body
            )
            res = await GulpOpenSearch.get_instance()._opensearch.update_by_query(
                index=index,
                body=params_body,
                params={
                    "conflicts": "proceed",
                    "wait_for_completion": "true",
                    "refresh": "true",
                },
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                },
            )

            num_deleted = int(res.get("updated", 0) or 0)
            source_fields_changed = num_deleted > 0
            data = GulpUpdateDocumentsStats(
                total_hits=num_deleted,
                updated=num_deleted,
                flt=flt,
            ).model_dump(exclude_none=True)
            data["num_deleted"] = num_deleted
            await stats.set_finished(
                sess,
                status=GulpRequestStatus.DONE,
                data=data,
                user_id=user_id,
                ws_id=ws_id,
            )
        except Exception as ex:
            if stats and not isinstance(ex, RequestCanceledError):
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
            if source_fields_changed:
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
    data: Annotated[
        dict,
        Body(
            description="The data to update the documents with.",
            examples=[{"gulp.tags": ["tag1", "tag2"], "custom_field": "custom_value"}],
        ),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        return await _enqueue_enrich_task(
            token,
            operation_id,
            ws_id,
            req_id,
            flt,
            ENRICH_TASK_ACTION_UPDATE_DOCUMENTS,
            data=data,
        )
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
    data: Annotated[
        dict,
        Body(
            description="The data to update the documents with.",
            examples=[{"gulp.tags": ["tag1", "tag2"], "custom_field": "custom_value"}],
        ),
    ],
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
    "/untag_documents",
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
    summary="Remove tags from document/s.",
    description="""
shortcut to `untag_documents` semantics to remove `gulp.tags` values from documents.
""",
)
async def untag_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)],
    tags: Annotated[list[str], Depends(APIDependencies.param_tags)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        return await _enqueue_enrich_task(
            token,
            operation_id,
            ws_id,
            req_id,
            flt,
            ENRICH_TASK_ACTION_UNTAG_DOCUMENTS,
            tags=tags,
        )

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
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        return await _enqueue_enrich_task(
            token,
            operation_id,
            ws_id,
            req_id,
            flt,
            ENRICH_TASK_ACTION_TAG_DOCUMENTS,
            data={"gulp.tags": tags},
        )
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


@router.post(
    "/enrich_remove",
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
    summary="Remove enriched data from documents.",
    description="""
shortcut to `update_documents` to remove enriched data from the given documents.

- token must have the `edit` permission.
- if no `fields` are provided, `gulp.enriched` field is removed from the GulpDocument/s. either, the specified `fields` are removed from the GulpDocument/s.
""",
)
async def enrich_remove_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_q_flt_optional)
    ] = None,
    fields: Annotated[
        list[str],
        Body(
            description="the enriched fields to remove, e.g. `['field1', 'field2']`.  If not provided, the entire `gulp.enriched` field is removed."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    """Remove the `gulp.enriched` field from documents matching the filter.

    The handler enforces the operation ACL and enqueues the multi-document
    mutation as an enrich Redis task.
    """

    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        return await _enqueue_enrich_task(
            token,
            operation_id,
            ws_id,
            req_id,
            flt,
            ENRICH_TASK_ACTION_ENRICH_REMOVE,
            remove_fields=fields or [],
        )

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
