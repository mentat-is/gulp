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

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryHelpers, GulpQueryParameters
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsQueueDataType, GulpWsSharedQueue
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters

router: APIRouter = APIRouter()


async def _tag_documents_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    index: str,
    flt: GulpQueryFilter,
    tags: list[str],
) -> None:
    """
    runs in a worker to tag the given documents
    """

    async def _tag_documents_chunk_wrapper(docs: list[dict], **kwargs):
        """
        tags a chunk of documents, called by GulpOpenSearch.search_dsl during loop over chunks

        :param docs: the documents to tag
        :param kwargs: the keyword arguments
        """

        # build documents list
        tags = kwargs["tags"]
        last = kwargs.get("last", False)
        flt = kwargs["flt"]
        req_id = kwargs["req_id"]
        ws_id = kwargs["ws_id"]
        user_id = kwargs["user_id"]
        index = kwargs["index"]

        MutyLogger.get_instance().debug(
            "---> _tagging chunk of %d documents with tags=%s, kwargs=%s ..."
            % (len(docs), tags, kwargs)
        )

        # add tags to documents
        for d in docs:
            d.update({"gulp.tags": tags})

        # update the documents
        last = kwargs.get("last", False)
        await GulpOpenSearch.get_instance().update_documents(
            index, docs, wait_for_refresh=last
        )

        if last:
            # also send a GulpQueryDonePacket
            p = GulpQueryDonePacket(
                status=GulpRequestStatus.DONE,
                total_hits=kwargs.get("total_hits", 0),
            )
            GulpWsSharedQueue.get_instance().put(
                type=GulpWsQueueDataType.ENRICH_DONE,
                ws_id=ws_id,
                user_id=user_id,
                req_id=req_id,
                data=p.model_dump(exclude_none=True),
            )

            if last:
                # update source -> fields mappings on the collab db
                await GulpOpenSearch.get_instance().datastream_update_mapping_by_operation(
                    index,
                    user_id,
                    operation_ids=flt.operation_ids,
                    context_ids=flt.context_ids,
                    source_ids=flt.source_ids,
                )

    MutyLogger.get_instance().debug("---> _tag_documents_internal")

    # build query
    if not flt:
        # match all query
        q = {"query": {"match_all": {}}}
    else:
        # use the given filter
        q = flt.to_opensearch_dsl()

    # we need id only
    q_options = GulpQueryParameters(fields=["_id"])

    # call query_raw, which in turn calls _tag_documents_chunk_wrapper
    errors: list[str] = []
    total = 0
    async with GulpCollab.get_instance().session() as sess:
        try:
            _, total, _ = await GulpQueryHelpers.query_raw(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                q=q,
                index=index,
                q_options=q_options,
                callback_chunk=_tag_documents_chunk_wrapper,
                callback_chunk_args={
                    "tags": tags,
                    "flt": flt,
                    "req_id": req_id,
                    "ws_id": ws_id,
                    "user_id": user_id,
                    "index": index,
                },
            )
        except Exception as ex:
            # record error
            errors = [muty.log.exception_to_string(ex, with_full_traceback=True)]
            MutyLogger.get_instance().exception(ex)
        finally:
            # also update stats
            await GulpRequestStats.finalize_query_stats(
                sess,
                req_id=req_id,
                ws_id=ws_id,
                user_id=user_id,
                hits=total,
                ws_queue_datatype=GulpWsQueueDataType.TAG_DONE,
                errors=errors,
            )


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
    runs in a worker process to enrich documents
    """
    # MutyLogger.get_instance().debug("---> _enrich_documents_internal")
    mod: GulpPluginBase = None
    failed = False
    error: str = None
    total: int = 0
    async with GulpCollab.get_instance().session() as sess:
        try:
            # load plugin
            mod = await GulpPluginBase.load(plugin)

            # enrich
            total = await mod.enrich_documents(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                index=index,
                flt=flt,
                plugin_params=plugin_params,
            )
        except Exception as ex:
            failed = True
            error = muty.log.exception_to_string(ex, with_full_traceback=True)
        finally:
            # also update stats
            await GulpRequestStats.finalize_query_stats(
                sess,
                req_id=req_id,
                ws_id=ws_id,
                user_id=user_id,
                hits=total,
                ws_queue_datatype=GulpWsQueueDataType.ENRICH_DONE,
                errors=[error] if error else [],
            )

            # done
            if mod:
                await mod.unload()

            if not failed:
                # update source -> fields mappings on the collab db
                await GulpOpenSearch.get_instance().datastream_update_mapping_by_operation(
                    index,
                    user_id,
                    operation_ids=flt.operation_ids,
                    context_ids=flt.context_ids,
                    source_ids=flt.source_ids,
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
- `flt.operation_ids` is ignored and set to `[operation_id]`
- the enriched documents are updated in the Gulp `operation_id.index` and  streamed on the websocket `ws_id` as `GulpDocumentsChunkPacket`.
- `flt` is provided as a `GulpQueryFilter` to select the documents to enrich.
""",
)
async def enrich_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)

    try:
        # enforce operation_id
        flt.operation_ids = [operation_id]

        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.EDIT
            )
            user_id = s.user_id
            index = op.index

            # create a stats, just to allow request canceling
            await GulpRequestStats.create(
                sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                context_id=None,
            )

        # spawn a task which runs the enrichment in a worker process
        # run ingestion in a coroutine in one of the workers
        MutyLogger.get_instance().debug("spawning enrichment task ...")
        kwds = dict(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            flt=flt,
            operation_id=operation_id,
            index=index,
            plugin=plugin,
            plugin_params=plugin_params,
        )

        # print(json.dumps(kwds, indent=2))
        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _enrich_documents_internal, kwds=kwds
            )

        await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))

        # and return pending
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

    mod = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.EDIT
            )
            index = op.index

            # load plugin
            mod = await GulpPluginBase.load(plugin)

            # query document
            doc = await mod.enrich_single_document(
                sess, doc_id, operation_id, index, plugin_params
            )

            # rebuild mapping
            await GulpOpenSearch.get_instance().datastream_update_mapping_by_src(
                index=index,
                operation_id=doc["gulp.operation_id"],
                context_id=doc["gulp.context_id"],
                source_id=doc["gulp.source_id"],
                doc_ids=[doc_id],
            )

        return JSONResponse(JSendResponse.success(req_id, data=doc))

    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
    finally:
        if mod:
            await mod.unload()


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
- `flt.operation_ids` is ignored and set to `[operation_id]`
- the enriched documents are updated in the Gulp `index`.
""",
)
async def tag_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)],
    tags: Annotated[list[str], Body(description="The tags to add.")],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
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
            user_id = s.user_id
            index = op.index

            # create a stats, just to allow request canceling
            await GulpRequestStats.create(
                sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                context_id=None,
            )

        # spawn a task which runs the enrichment in a worker process
        # run ingestion in a coroutine in one of the workers
        MutyLogger.get_instance().debug("spawning tagging task ...")
        kwds = dict(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            flt=flt,
            tags=tags,
        )

        # print(json.dumps(kwds, indent=2))
        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _tag_documents_internal, kwds=kwds
            )

        await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
