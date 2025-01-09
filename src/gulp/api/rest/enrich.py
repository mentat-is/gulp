from muty.jsend import JSendException, JSendResponse
from typing import Annotated
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import (
    GulpRequestStatus,
    GulpUserPermission,
)
from muty.pydantic import autogenerate_model_example_by_class
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import (
    GulpQueryParameters,
)
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from gulp.api.rest.structs import APIDependencies
from gulp.api.ws_api import GulpQueryDonePacket, GulpSharedWsQueue, GulpWsQueueDataType
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from muty.log import MutyLogger
import muty.log

from gulp.structs import GulpPluginParameters

router: APIRouter = APIRouter()


async def _enrich_documents_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    q: dict,
    index: str,
    plugin: str,
    q_options: GulpQueryParameters,
    plugin_params: GulpPluginParameters,
) -> None:
    """
    runs in a worker process to enrich documents
    """
    # MutyLogger.get_instance().debug("---> _enrich_documents_internal")
    mod: GulpPluginBase = None
    failed = False
    error = None
    async with GulpCollab.get_instance().session() as sess:
        try:
            # load plugin
            mod = await GulpPluginBase.load(plugin)

            # enrich
            await mod.enrich_documents(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                index=index,
                q=q,
                q_options=q_options,
                plugin_params=plugin_params,
            )
        except Exception as ex:
            failed = True
            error = muty.log.exception_to_string(ex, with_full_traceback=True)
            p = GulpQueryDonePacket(
                status=GulpRequestStatus.FAILED,
                error=error,
            )
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.ENRICH_DONE,
                ws_id=ws_id,
                user_id=user_id,
                req_id=req_id,
                data=p.model_dump(exclude_none=True),
            )
        finally:
            d = dict(
                status=(
                    GulpRequestStatus.DONE if not failed else GulpRequestStatus.FAILED
                ),
                error=error,
            )
            await GulpRequestStats.update_by_id(
                sess=sess, id=req_id, user_id=user_id, ws_id=ws_id, req_id=req_id, d=d
            )

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
- the enriched documents are updated in the Gulp `index` and  streamed on the websocket `ws_id` as `GulpDocumentsChunkPacket`.
- `q` is a `raw` query which may be provided to restrict the documents to enrich. by default, whole data is considered.
""",
)
async def enrich_documents_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    plugin: Annotated[str, Depends(APIDependencies.param_plugin)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    q: Annotated[
        dict,
        Body(
            examples=[{"query": {"match_all": {}}}],
        ),
    ] = None,
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_query_additional_parameters_optional),
    ] = None,
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(sess, token, GulpUserPermission.EDIT)
            user_id = s.user_id

            # create a stats, just to allow request canceling
            await GulpRequestStats.create(
                sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=None,
                context_id=None,
                source_total=0,
            )

        # spawn a task which runs the enrichment in a worker process
        # run ingestion in a coroutine in one of the workers
        MutyLogger.get_instance().debug("spawning enrichment task ...")
        kwds = dict(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            q=q,
            index=index,
            plugin=plugin,
            q_options=q_options,
            plugin_params=plugin_params,
        )

        # print(json.dumps(kwds, indent=2))
        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _enrich_documents_internal, kwds=kwds
            )

        await GulpProcess.get_instance().coro_pool.spawn(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


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
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document on Gulp `index`."),
    ],
    index: Annotated[str, Depends(APIDependencies.param_index)],
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
            # check token and get caller user id
            await GulpUserSession.check_token(sess, token, GulpUserPermission.EDIT)

        # load plugin
        mod = await GulpPluginBase.load(plugin)

        # query document
        async with GulpCollab.get_instance().session() as sess:
            doc = await mod.enrich_single_document(sess, doc_id, index, plugin_params)
        return JSONResponse(JSendResponse.success(req_id, data=doc))

    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
    finally:
        if mod:
            await mod.unload()
