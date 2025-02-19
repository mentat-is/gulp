import asyncio
from asyncio import Task
from copy import deepcopy
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class

from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQuery, GulpQueryHelpers, GulpQueryParameters
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import (
    GulpQueryGroupMatchPacket,
    GulpSharedWsQueue,
    GulpWsQueueDataType,
)
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from gulp.structs import GulpPluginParameters

router: APIRouter = APIRouter()

EXAMPLE_SIGMA_RULE = """title: Match All Events
id: 1a070ea4-87f4-467c-b1a9-f556c56b2449
status: test
description: Matches all events in the data source
logsource:
    category: *
    product: *
detection:
    selection:
        '*': '*'
    condition: selection
falsepositives:
    - 'This rule matches everything'
level: info
"""


async def _query_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    q: Any,
    q_options: GulpQueryParameters,
    plugin: str,
    plugin_params: GulpPluginParameters,
    flt: GulpQueryFilter,
) -> int:
    """
    runs in a worker process and perform a query, streaming results to the `ws_id` websocket
    """
    hits: int = 0
    mod: GulpPluginBase = None

    try:
        if plugin:
            # external query, load plugin (it is guaranteed to be the same for all queries)
            mod = await GulpPluginBase.load(plugin)

        async with GulpCollab.get_instance().session() as sess:
            # MutyLogger.get_instance().debug("mod=%s, running query %s " % (mod, gq))
            if not mod:
                # local query, gq.q is a dict
                _, hits = await GulpQueryHelpers.query_raw(
                    sess=sess,
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    index=index,
                    q=q,
                    q_options=q_options,
                    flt=flt,
                )
            else:
                # external query
                _, hits = await mod.query_external(
                    sess=sess,
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    q=q,
                    plugin_params=plugin_params,
                    q_options=q_options,
                    index=index,
                )

    except Exception as ex:
        MutyLogger.get_instance().exception(ex)

    finally:
        if mod:
            await mod.unload()
    return hits


async def _worker_coro(kwds: dict):
    """
    runs in background an spawn/waits query workers

    1. run queries
    2. wait each and collect totals
    3. if all match, update note tags with group names and signal websocket with QUERY_GROUP_MATCH
    """

    tasks: list[Task] = []
    queries: list[GulpQuery] = kwds["queries"]
    operation_id: str = kwds["operation_id"]
    q_options: GulpQueryParameters = kwds["q_options"]
    user_id: str = kwds["user_id"]
    req_id: str = kwds["req_id"]
    ws_id: str = kwds["ws_id"]
    index: str = kwds["index"]
    flt: GulpQueryFilter = kwds["flt"]
    plugin: str = kwds.get("plugin")
    plugin_params: GulpPluginParameters = kwds.get("plugin_params")

    try:
        for gq in queries:

            q_opt = deepcopy(q_options)

            # set name, i.e. for sigma rules we want the sigma rule name to be used (which has been set in the GulpQuery struct)
            q_opt.name = gq.name

            # note name set to query name
            q_opt.note_parameters.note_name = gq.name

            if not gq.name in q_opt.note_parameters.note_tags:
                # query name in note tags (this will allow to identify the results in the end)
                q_opt.note_parameters.note_tags.append(gq.name)

            # add task
            d = dict(
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                operation_id=operation_id,
                index=index,
                q=gq.q,
                q_options=q_opt,
                plugin=plugin,
                plugin_params=plugin_params,
                flt=flt,
            )

            tasks.append(
                GulpProcess.get_instance().process_pool.apply(_query_internal, kwds=d)
            )

        # run all and wait
        num_queries = len(queries)
        res = await asyncio.gather(*tasks, return_exceptions=True)

        # check if all queries matched
        query_matched = 0
        total_doc_matches = 0
        for r in res:
            if isinstance(r, int):
                query_matched += 1
                total_doc_matches += r

        if num_queries > 1 and query_matched == num_queries:
            # all queries in the group matched, update note tags with group name
            MutyLogger.get_instance().info(
                "query group '%s' matched, updating notes!" % (q_options.group)
            )
            if q_options.note_parameters.create_notes:
                async with GulpCollab.get_instance().session() as sess:
                    # look for tags = query name and update them with the group name
                    await GulpNote.bulk_update_tags(
                        sess, [q_opt.name], [q_options.group], operation_id=operation_id, user_id=user_id
                    )
            # and signal websocket
            p = GulpQueryGroupMatchPacket(
                name=q_options.group, total_hits=total_doc_matches
            )
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.QUERY_GROUP_MATCH,
                ws_id=ws_id,
                user_id=user_id,
                req_id=req_id,
                data=p.model_dump(exclude_none=True),
            )

        # also update stats
        d = dict(
            status=(
                GulpRequestStatus.DONE
                if query_matched >= 1
                else GulpRequestStatus.FAILED
            )
        )
        async with GulpCollab.get_instance().session() as sess:
            await GulpRequestStats.update_by_id(
                sess=sess, id=req_id, user_id=user_id, ws_id=ws_id, req_id=req_id, d=d
            )
    finally:
        tasks.clear()
        tasks = None


async def _spawn_query_group_workers(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    queries: list[GulpQuery],
    q_options: GulpQueryParameters = None,
    plugin: str = None,
    plugin_params: GulpPluginParameters = None,
    flt: GulpQueryFilter = None,
) -> None:
    """
    spawns worker tasks for each query and wait them all
    """
    MutyLogger.get_instance().debug("spawning %d queries ..." % (len(queries)))
    kwds = dict(
        user_id=user_id,
        req_id=req_id,
        ws_id=ws_id,
        operation_id=operation_id,
        index=index,
        queries=queries,
        q_options=q_options,
        plugin=plugin,
        plugin_params=plugin_params,
        flt=flt,
    )

    # create a stats, just to allow request canceling
    async with GulpCollab.get_instance().session() as sess:
        await GulpRequestStats.create(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            context_id=None,
        )

    # run _worker_coro in background, it will spawn a worker for each query and wait them
    await GulpRestServer.get_instance().spawn_bg_task(_worker_coro(kwds))


@router.post(
    "/query_raw",
    response_model=JSendResponse,
    tags=["query"],
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
    summary="Advanced query.",
    description="""
query Gulp with a raw OpenSearch DSL query.

- this API returns `pending` and results are streamed to the `ws_id` websocket.
- `q` must be one or more queries with a format according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/)
- if more than one query is provided, `q_options.group` must be set.
""",
)
async def query_raw_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    q: Annotated[
        list[dict],
        Body(
            description="""one or more queries according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/).
""",
            examples=[
                [{"query": {"match_all": {}}}]
            ],
        ),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        if len(q) > 1 and not q_options.group:
            raise ValueError(
                "if more than one query is provided, `q_options.group` must be set."
            )

        async with GulpCollab.get_instance().session() as sess:
            permission = GulpUserPermission.READ

            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, permission=permission, obj=op)
            user_id = s.user_id

        queries: list[GulpQuery] = []
        for qq in q:
            # build query
            gq = GulpQuery(
                name=q_options.name,
                q=qq
            )
            queries.append(gq)
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=op.index,
            queries=queries,
            q_options=q_options,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.post(
    "/query_external",
    response_model=JSendResponse,
    tags=["query"],
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
    summary="Query an external source.",
    description="""
query an external source using the target source query language, and optionally ingest data back into gulp.

- this API returns `pending` and results are streamed to the `ws_id` websocket.
- `plugin_params.custom_parameters` must include all the parameters needed to connect to the external source.
- token must have `ingest` permission if `ingest` is set.
""",
)
async def query_external_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    q: Annotated[
        Any,
        Body(
            description="""one or more queries according to the source language specifications.""",
            examples=[
                [{"query": {"match_all": {}}}]
            ],
        ),
    ],
    plugin: Annotated[str, Query(description="the plugin implementing `query_external` to handle the external query.")],
    plugin_params: Annotated[GulpPluginParameters, Body(description="parameters for the external plugin.")],
    ingest: Annotated[Optional[bool], Query(
        description="set to `True` to ingest data into gulp operation's index.")] = False,
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            if ingest:
                # external query with ingest, needs ingest permission
                permission = GulpUserPermission.INGEST
            else:
                # standard external query, read is enough
                permission = GulpUserPermission.READ

            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, permission=permission, obj=op)
            user_id = s.user_id
            index = op.index

        queries: list[GulpQuery] = []
        for qq in q:
            # build query
            gq = GulpQuery(
                name=q_options.name,
                q=qq
            )
            queries.append(gq)
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=index if ingest else None,
            queries=queries,
            q_options=q_options,
            plugin=plugin,
            plugin_params=plugin_params
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.post(
    "/query_sigma",
    response_model=JSendResponse,
    tags=["query"],
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
    summary="Query using sigma rules.",
    description="""
query using [sigma rules](https://github.com/SigmaHQ/sigma).

- this API returns `pending` and results are streamed to the `ws_id` websocket.
- `flt` may be used to restrict the query.

### q_options

- `create_notes` is set to `True` to create notes on match.
- if more than one query is provided, `q_options.group` must be set.

### plugin_params

- usually pass None here, unless the plugin requires specific parameters.
""",
)
async def query_sigma_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    plugin: Annotated[str, Query(description="the plugin implementing `sigma_convert` to convert the sigma rule.")],
    sigmas: Annotated[
        list[str],
        Body(
            description="one or more sigma rule YAML to create the queries with.",
            examples=[EXAMPLE_SIGMA_RULE],
        ),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_q_options),
    ] = None,
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    mod = None
    try:
        if len(sigmas) > 1 and not q_options.group:
            raise ValueError(
                "if more than one query is provided, `q_options.group` must be set."
            )

        # activate notes on match
        q_options.note_parameters.create_notes = True

        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(sess, token, obj=op)
            user_id = s.user_id
            index = op.index

        # convert sigma rule/s using pysigma
        mod = await GulpPluginBase.load(plugin)

        queries: list[GulpQuery] = []
        for s in sigmas:
            q: list[GulpQuery] = mod.sigma_convert(
                s, plugin, plugin_params)
            queries.extend(q)

        # spawn one aio task, it will spawn n multiprocessing workers and wait them
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=index,
            queries=queries,
            q_options=q_options,
            flt=flt,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
    finally:
        if mod:
            await mod.unload()


@router.post(
    "/sigma_convert",
    response_model=JSendResponse,
    tags=["query"],
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
    summary="Convert a sigma rule to raw query for the specific target.",
    description="""
to be used to build i.e. raw queries for `query_external` API from [sigma rules](https://github.com/SigmaHQ/sigma).

- use `plugin_params.custom_parameters` to customize the conversion, depending on the specific plugin options (i.e. `backend`, 'target query language`, ...)
""",
)
async def sigma_convert_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    sigma: Annotated[str,
        Body(
            description="the sigma rule YAML to be converted.",
            examples=[EXAMPLE_SIGMA_RULE],
        ),
    ],
    plugin: Annotated[str, Query(description="the plugin implementing `sigma_convert` to convert the sigma rule.")],
    plugin_params: Annotated[
        GulpPluginParameters,
        Depends(APIDependencies.param_plugin_params_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["plugin_params"] = plugin_params.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    mod = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            s = await GulpUserSession.check_token(sess, token)

        # convert sigma rule/s using pysigma
        mod = await GulpPluginBase.load(plugin)
        q: list[GulpQuery] = mod.sigma_convert(
            sigma, plugin, plugin_params)

        l: list[dict] = []
        for qq in q:    
            l.append(qq.model_dump(exclude_none=True))
        
        return JSONResponse(JSendResponse.success(req_id, data=l))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
    finally:
        if mod:
            await mod.unload()

@router.post(
    "/query_single_id",
    response_model=JSendResponse,
    tags=["query"],
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
    summary="Query a single document.",
    description="""
query Gulp for a single document.
""",
)
async def query_single_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document on Gulp `index`."),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index

        d = await GulpQueryHelpers.query_single(index, doc_id)
        return JSONResponse(JSendResponse.success(req_id, data=d))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.post(
    "/query_max_min_per_field",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": {
                            "buckets": [
                                {
                                    "*": {
                                        "doc_count": 98632,
                                        "max_event.code": 62171,
                                        "min_gulp.timestamp": 1289373941000000000,
                                        "max_gulp.timestamp": 1637340783836550912,
                                        "min_event.code": 0,
                                    }
                                }
                            ],
                            "total": 98632,
                        },
                    }
                }
            }
        }
    },
    summary="gets max/min `@timestamp` and `event.code` in the given `index`",
    description="""
- use `flt` to restrict the query.
""",
)
async def query_max_min_per_field(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    group_by: Annotated[
        Optional[str],
        Query(description="group by field (i.e. `event.code`), default=no grouping"),
    ] = None,
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index

        d = await GulpOpenSearch.get_instance().query_max_min_per_field(
            index, group_by=group_by, flt=flt
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


@router.get(
    "/query_operations",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1704380570434,
                        "req_id": "c4f7ae9b-1e39-416e-a78a-85264099abfb",
                        "data": [
                            {
                                "name": "example operation",
                                "id": "test_operation",
                                "index": "test_operation",
                                "contexts": [
                                    {
                                        "name": "test_context",
                                        "id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                                        "doc_count": 98632,
                                        "plugins": [
                                            {
                                                "name": "win_evtx",
                                                "sources": [
                                                    {
                                                        "name": "/home/valerino/repos/gulp/samples/win_evtx/security_big_sample.evtx",
                                                        "id": "fabae8858452af6c2acde7f90786b3de3a928289",
                                                        "doc_count": 62031,
                                                        "max_event.code": 5158,
                                                        "min_event.code": 1102,
                                                        "min_gulp.timestamp": 1475718427166301952,
                                                        "max_gulp.timestamp": 1475833104749409792,
                                                    },
                                                    {
                                                        "name": "/home/valerino/repos/gulp/samples/win_evtx/2-system-Security-dirty.evtx",
                                                        "id": "60213bb57e849a624b7989c448b7baec75043a1b",
                                                        "doc_count": 14621,
                                                        "max_event.code": 5061,
                                                        "min_event.code": 1100,
                                                        "min_gulp.timestamp": 1532738204663494144,
                                                        "max_gulp.timestamp": 1553118827379374080,
                                                    },
                                                ],
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        }
    },
    summary="query operations with aggregations.",
    description="""
for each `operation` returns `sources` and `contexts` with their max/min `event.code` and `gulp.timestamp`.
""",
)
async def query_operations(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    try:
        # check token and get its accessible operations
        ops: list[dict] = await GulpOperation.get_by_filter_wrapper(token, GulpCollabFilter())
        operations: list[dict] = []
        for o in ops:
            # get each op details by querying the associated index
            d = await GulpOpenSearch.get_instance().query_operations(o['index'])
            operations.extend(d)

        return JSONResponse(JSendResponse.success(req_id=req_id, data=operations))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


async def _create_mapping_by_src_internal(index: str, operation_id: str,
                                          context_id: str, source_id: str, user_id: str, req_id: str, ws_id: str) -> None:
    """
    this runs in a worker process to create the fields mapping for a source.
    """
    await GulpOpenSearch.get_instance().datastream_update_mapping_by_src(
        index=index,
        operation_id=operation_id,
        context_id=context_id,
        source_id=source_id,
        user_id=user_id,
        req_id=req_id,
        ws_id=ws_id,
    )


@router.get(
    "/query_fields_by_source",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701266243057,
                        "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
                        "data": {
                            "@timestamp": "date_nanos",
                            "agent.type": "keyword",
                            "destination.ip": "ip",
                            "destination.port": "long",
                            "event.category": "keyword",
                            "event.code": "keyword",
                            "event.duration": "long",
                            "event.original": "text",
                            "event.sequence": "long",
                            "event.type": "keyword",
                            "gulp.context_id": "keyword",
                            "gulp.event_code": "long",
                            "gulp.operation_id": "keyword",
                            "gulp.source_id": "keyword",
                            "gulp.timestamp": "long",
                            "gulp.timestamp_invalid": "boolean",
                            "gulp.unmapped.AccessList": "keyword",
                            "gulp.unmapped.AccessMask": "keyword",
                            "gulp.unmapped.AccountExpires": "keyword",
                            "gulp.unmapped.AdditionalInfo": "keyword",
                            "gulp.unmapped.AllowedToDelegateTo": "keyword",
                            "gulp.unmapped.AuthenticationPackageName": "keyword",
                            "gulp.unmapped.Data": "keyword",
                        },
                    }
                }
            }
        }
    },
    summary="get fields mapping.",
    description="""
get all `key=type` mappings for the given given `operation_id`, `context_id` and `source_id`.

this API initially `status="pending` on the first call for the tuple [`operation_id`, `context_id`, `source_id`], and `SOURCE_FIELDS_CHUNK` are streamed on the websocket `ws_id`.

on subsequent calls, when an entry has been created on the database, the API returns the fields mapping as a dict.
""",
)
async def query_fields_by_source_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
    source_id: Annotated[str, Depends(APIDependencies.param_source_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s: GulpUserSession = await GulpUserSession.check_token(sess, token, obj=op)
            index = op.index
            user_id = s.user_id

            # check if there is at least one document with operation_id, context_id and source_id
            await GulpOpenSearch.get_instance().search_dsl_sync(
                index=index,
                q={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"gulp.operation_id": operation_id}},
                                {"term": {"gulp.context_id": context_id}},
                                {"term": {"gulp.source_id": source_id}},
                            ]
                        }
                    }
                },
                q_options=GulpQueryParameters(limit=1)
            )

            m = await GulpOpenSearch.get_instance().datastream_get_mapping_by_src(
                sess, operation_id=operation_id, context_id=context_id, source_id=source_id
            )
            if m:
                # return immediately
                return JSONResponse(JSendResponse.success(req_id=req_id, data=m))

            # spawn a task to run fields mapping in a worker
            kwds = dict(
                index=index,
                operation_id=operation_id,
                context_id=context_id,
                source_id=source_id,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
            )

            async def worker_coro(kwds: dict):
                await GulpProcess.get_instance().process_pool.apply(
                    _create_mapping_by_src_internal, kwds=kwds
                )

            await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))

            # and return pending
            return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
