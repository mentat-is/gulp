from asyncio import Task
from copy import copy, deepcopy
import json
from muty.jsend import JSendException, JSendResponse
from typing import Annotated, Any, Optional
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.stored_query import GulpStoredQuery
from gulp.api.collab.structs import (
    GulpCollabFilter,
)
from muty.pydantic import autogenerate_model_example_by_class
from gulp.api.collab.note import GulpNote
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import (
    GulpQuery,
    GulpQueryHelpers,
    GulpQueryParameters,
)
from gulp.api.opensearch.sigma import GulpQuerySigmaParameters
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.rest.structs import APIDependencies
from gulp.api.ws_api import (
    GulpQueryGroupMatchPacket,
    GulpSharedWsQueue,
    GulpWsQueueDataType,
)
from gulp.plugin import GulpPluginBase
from gulp.process import GulpProcess
from muty.log import MutyLogger

import muty.crypto
import muty.dynload
import asyncio

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


async def _stored_query_ids_to_gulp_queries(
    sess: AsyncSession, stored_query_ids: list[str]
) -> list[GulpQuery]:
    """
    get stored queries from the collab db and convert to GulpQuery array

    Args:
        sess: the database session to use
        stored_query_ids (list[str]): list of stored query IDs
    Returns:
        list[GulpQuery]: list of GulpQuery
    """
    queries: list[GulpQuery] = []

    # get queries
    stored_queries: list[GulpStoredQuery] = await GulpStoredQuery.get_by_filter(
        sess, GulpCollabFilter(ids=stored_query_ids)
    )
    MutyLogger.get_instance().debug(
        "retrieved stored queries: %s"
        % (json.dumps([q.to_dict(exclude_none=True) for q in stored_queries], indent=2))
    )

    # convert to GulpQuery array
    for qs in stored_queries:
        MutyLogger.get_instance().debug(
            "converting stored query '%s' to GulpQuery ..."
            % (json.dumps(qs.to_dict(exclude_none=True), indent=2))
        )
        sigma_options = (
            GulpQuerySigmaParameters.model_validate(qs.s_options)
            if qs.s_options
            else GulpQuerySigmaParameters()
        )
        plugin_params = (
            GulpPluginParameters.model_validate(qs.plugin_params)
            if qs.plugin_params
            else GulpPluginParameters()
        )
        if sigma_options.plugin:
            # this is a sigma query, convert
            mod = await GulpPluginBase.load(sigma_options.plugin)
            if sigma_options.backend is None and not qs.external_plugin:
                # assume local, use opensearch
                sigma_options.backend = "opensearch"
            if sigma_options.output_format is None and not qs.external_plugin:
                # assume local, use dsl_lucene
                sigma_options.output_format = "dsl_lucene"

            # convert sigma
            lgq: list[GulpQuery] = mod.sigma_convert(qs.q, sigma_options)
            for q in lgq:
                # set external
                q.external_plugin = qs.external_plugin
                q.external_plugin_params = deepcopy(plugin_params)
                if qs.tags:
                    # add stored query tags too
                    [q.tags.append(t) for t in qs.tags if t not in q.tags]

            queries.extend(lgq)
            await mod.unload()
        else:
            # this is a raw query
            if not qs.external_plugin:
                # gulp local query, q is a json string
                queries.append(
                    GulpQuery(name=qs.name, q=json.loads(qs.q), tags=qs.tags)
                )
            else:
                # external query, pass q unaltered (the external plugin will handle it)
                queries.append(
                    GulpQuery(
                        name=qs.name,
                        q=qs.q,
                        tags=qs.tags,
                        external_plugin=qs.external_plugin,
                        external_plugin_params=deepcopy(plugin_params),
                    )
                )

    return queries


async def _query_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    index: str,
    queries: list[GulpQuery],
    q_options: GulpQueryParameters,
    flt: GulpQueryFilter,
) -> int:
    """
    runs in a worker and perform one or more queries of the same type, streaming results to the `ws_id` websocket
    """
    totals = 0
    mod = None

    try:
        if queries[0].external_plugin:
            # external query, load plugin (it is guaranteed it is the same for all queries)
            mod = await GulpPluginBase.load(queries[0].external_plugin)

        async with GulpCollab.get_instance().session() as sess:
            for gq in queries:
                # MutyLogger.get_instance().debug("mod=%s, running query %s " % (mod, gq))

                try:
                    if not mod:
                        # local query, gq.q is a dict
                        _, hits = await GulpQueryHelpers.query_raw(
                            sess=sess,
                            user_id=user_id,
                            req_id=req_id,
                            ws_id=ws_id,
                            index=index,
                            q=gq.q,
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
                            index=index,
                            q=gq.q,
                            q_options=q_options,
                            flt=flt,
                        )

                    totals += hits
                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)

    finally:
        if mod:
            await mod.unload()
    return totals


async def _spawn_query_group_workers(
    user_id: str,
    req_id: str,
    ws_id: str,
    index: str,
    queries: list[GulpQuery],
    q_options: GulpQueryParameters,
    flt: GulpQueryFilter,
) -> None:
    """
    spawns worker tasks for each query and wait them all
    """

    async def _worker_coro(kwds: dict):
        """
        runs in a worker

        1. run queries
        2. wait each and collect totals
        3. if all match, update note tags with group names and signal websocket with QUERY_GROUP_MATCH
        """

        tasks: list[Task] = []
        queries: list[GulpQuery] = kwds["queries"]

        for gq in queries:
            q_opt = deepcopy(q_options)

            # set name, i.e. for sigma rules we want the sigma rule name to be used (which has been set in the GulpQuery struct)
            q_opt.name = gq.name

            # note name set to query name
            q_opt.note_parameters.note_name = gq.name

            # query name in note tags (this will allow to identify the results in the end)
            gq.tags.append(gq.name)
            if q_opt.name not in gq.tags:
                gq.tags.append(q_opt.name)

            q_opt.note_parameters.note_tags = copy(gq.tags)
            q_opt.external_parameters.plugin_params = deepcopy(
                gq.external_plugin_params
            )

            # add task
            d = dict(
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                index=index,
                queries=[gq],
                q_options=q_opt,
                flt=flt,
            )
            tasks.append(
                GulpProcess.get_instance().process_pool.apply(_query_internal, kwds=d)
            )

        # run all and wait
        num_queries = len(queries)
        res = await asyncio.gather(*tasks, return_exceptions=True)

        # check if all sigmas matched
        query_matched = 0
        total_doc_matches = 0
        for r in res:
            if isinstance(r, int):
                query_matched += 1
                total_doc_matches += r

        if num_queries > 1 and query_matched == num_queries:
            # all queries in the group matched, change note names to query group name
            MutyLogger.get_instance().info(
                "query group '%s' matched, updating notes!" % (q_options.group)
            )
            if q_options.note_parameters.create_notes:
                async with GulpCollab.get_instance().session() as sess:
                    await GulpNote.bulk_update_tags(
                        sess, [q_opt.name], [q_options.group]
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

    MutyLogger.get_instance().debug("spawning %d queries ..." % (len(queries)))
    kwds = dict(
        user_id=user_id,
        req_id=req_id,
        ws_id=ws_id,
        index=index,
        queries=queries,
        q_options=q_options,
        flt=flt,
    )

    # create a stats, just to allow request canceling
    async with GulpCollab.get_instance().session() as sess:
        await GulpRequestStats.create(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=None,
            context_id=None,
            source_total=len(queries),
        )

    if q_options.external_parameters.ingest_index:
        # make sure the index to ingest into exists
        exists = await GulpOpenSearch.get_instance().datastream_exists(
            q_options.external_parameters.ingest_index
        )
        if not exists:
            # create
            await GulpOpenSearch.get_instance().datastream_create(
                q_options.external_parameters.ingest_index
            )

    # run queries
    await GulpProcess.get_instance().coro_pool.spawn(_worker_coro(kwds))


@router.post(
    "/query_gulp",
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
    summary="the default query type for Gulp.",
    description="""
query Gulp with filter.

- this API returns `pending` and results are streamed to the `ws_id` websocket.
""",
)
async def query_gulp_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_query_additional_parameters_optional),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(sess, token)
            user_id = s.user_id

        # convert gulp query to raw query
        dsl = flt.to_opensearch_dsl()

        # spawn task to spawn worker
        gq = GulpQuery(name=q_options.name, q=dsl)
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            queries=[gq],
            q_options=q_options,
            # flt has been converted to dsl already
            flt=None,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


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
query Gulp or an external source using a raw DSL query.

- this API returns `pending` and results are streamed to the `ws_id` websocket.

### gulp queries

- refer to [OpenSearch query DSL](https://opensearch.org/docs/latest/query-dsl/).
- `flt` may be used to restrict the query.

### external queries

- refer to the external source query DSL.
- at least `q_options.external_parameters.plugin` (the plugin to handle the external query) and `q_options.external_parameters.uri` must be set.
- if `flt` is set, `q` is ignored and the external plugin must handle the filter converting it to the external source DSL.
""",
)
async def query_raw_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    q: Annotated[
        Any,
        Body(
            description="""query according to the [OpenSearch DSL specifications](https://opensearch.org/docs/latest/query-dsl/),
or a query in the external source DSL.
""",
            examples=[{"query": {"match_all": {}}}],
        ),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_query_additional_parameters_optional),
    ] = None,
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(sess, token)
            user_id = s.user_id

        # build query
        gq = GulpQuery(
            name=q_options.name,
            q=q,
            external_plugin=q_options.external_parameters.plugin,
            external_plugin_params=q_options.external_parameters.plugin_params,
        )
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            queries=[gq],
            q_options=q_options,
            flt=flt,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)


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
    doc_id: Annotated[
        str,
        Query(description="the `_id` of the document on Gulp `index`."),
    ],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            await GulpUserSession.check_token(sess, token)

        d = await GulpQueryHelpers.query_single(index, doc_id)
        return JSONResponse(JSendResponse.success(req_id, data=d))
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
    summary="Query using sigma rule/s.",
    description="""
query using [sigma rules](https://github.com/SigmaHQ/sigma).

- this API returns `pending` and results are streamed to the `ws_id` websocket.

### q_options

- `create_notes` is set to `True` to create notes on match.
- `sigma_parameters.plugin` must be set to a plugin implementing `sigma_support` and `sigma_convert` to be used to convert the sigma rule.
- `sigma_parameters.backend` and `sigma_parameters.output_format` are ignored for non `external` queries (internally set to `opensearch` and `dsl_lucene` respectively)
- if `sigmas` contains more than one rule, `group` must be set to indicate a `query group`.
    - all rules must be handled by the same `sigma_parameters.plugin`.
    - if `group` is set and **all** the queries match, `QUERY_GROUP_MATCH` is sent to the websocket `ws_id` in the end and `group` is set into notes `tags`.

### gulp queries

- `flt` may be used to restrict the query.

### external queries

- at least `q_options.external_parameters.plugin` (the plugin to handle the external query) and `q_options.external_parameters.uri` must be set.
- `flt` is not supported.
""",
)
async def query_sigma_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    sigmas: Annotated[
        list[str],
        Body(
            description="one or more sigma rule YAML to create the queries with.",
            examples=[EXAMPLE_SIGMA_RULE],
        ),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_query_additional_parameters_optional),
    ] = None,
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    mod = None
    try:
        if not q_options.sigma_parameters.plugin:
            raise ValueError("q_options.sigma_parameters.plugin must be set")
        if len(sigmas) > 1 and not q_options.group:
            raise ValueError(
                "if more than one query is provided, `q_options.group` must be set."
            )

        # activate notes on match
        q_options.note_parameters.create_notes = True

        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(sess, token)
            user_id = s.user_id

        # convert sigma rule/s using pysigma
        mod = await GulpPluginBase.load(q_options.sigma_parameters.plugin)

        if not q_options.external_parameters.plugin:
            # local gulp query
            q_options.sigma_parameters.backend = "opensearch"
            q_options.sigma_parameters.output_format = "dsl_lucene"

        queries: list[GulpQuery] = []
        for s in sigmas:
            q: list[GulpQuery] = mod.sigma_convert(s, q_options.sigma_parameters)
            for gq in q:
                # set the external plugin to run the query with, if any
                gq.external_plugin = q_options.external_parameters.plugin
                gq.external_plugin_params = q_options.external_parameters.plugin_params
            queries.extend(q)

        # spawn one aio task, it will spawn n multiprocessing workers and wait them
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            queries=queries,
            q_options=q_options,
            flt=flt if not q_options.external_parameters.plugin else None,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
    finally:
        if mod:
            await mod.unload()


@router.post(
    "/query_stored",
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
    summary="Query using stored queries.",
    description="""
query using queries stored on the Gulp `collab` database.

- this API returns `pending` and results are streamed to the `ws_id` websocket.

### q_options

- `create_notes` is set to `True` to create notes on match.
- each `stored_query` is retrieved by id and converted if needed.
- if `stored_query_ids` contains more than one query, `group` must be set to indicate a `query group`.
    - all queries in the group must be handled by the same `q_options.external_parameters.plugin`.
    - if `group` is set and **all** the queries match, `QUERY_GROUP_MATCH` is sent to the websocket `ws_id` in the end and `group` is set into notes `tags`.
- to allow ingestion during query, `external_parameters.ingest_index` must be set.

### gulp queries

- `flt` may be used to restrict the query.

### external queries

- all `stored queries` must have the same `external_plugin` set.
- `q_options.external_parameters.plugin` is ignored (taken from the stored query).
- `flt` is not supported.
""",
)
async def query_stored_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    index: Annotated[str, Depends(APIDependencies.param_index)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    stored_query_ids: Annotated[
        list[str],
        Body(
            description="one or more stored query IDs.", examples=["query_2", "query_2"]
        ),
    ],
    q_options: Annotated[
        GulpQueryParameters,
        Depends(APIDependencies.param_query_additional_parameters_optional),
    ] = None,
    flt: Annotated[
        GulpQueryFilter, Depends(APIDependencies.param_query_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True)
    params["q_options"] = q_options.model_dump(exclude_none=True)
    ServerUtils.dump_params(params)

    try:
        if len(stored_query_ids) > 1 and not q_options.group:
            raise ValueError(
                "if more than one query is provided, `options.group` must be set."
            )

        # activate notes on match
        q_options.note_parameters.create_notes = True

        queries: list[GulpQuery] = []
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            s = await GulpUserSession.check_token(sess, token)
            user_id = s.user_id

            # get queries
            queries = await _stored_query_ids_to_gulp_queries(sess, stored_query_ids)

        # external queries check (all must refer to the same plugin)
        external_plugin: str = queries[0].external_plugin
        for q in queries:
            if external_plugin != q.external_plugin:
                raise ValueError(
                    "all queries must be handled by the same external plugin (external_plugin=%s, q.external_plugin=%s)"
                    % (external_plugin, q.external_plugin)
                )

        # spawn one aio task, it will spawn n multiprocessing workers and wait them
        await _spawn_query_group_workers(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            queries=queries,
            q_options=q_options,
            flt=flt if not external_plugin else None,
        )

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
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
    index: Annotated[str, Depends(APIDependencies.param_index)],
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
            # check token and get caller user id
            await GulpUserSession.check_token(sess, token)

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
    index: Annotated[str, Depends(APIDependencies.param_index)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check token and get caller user id
            await GulpUserSession.check_token(sess, token)

        d = await GulpOpenSearch.get_instance().query_operations(index)
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(ex=ex, req_id=req_id)
