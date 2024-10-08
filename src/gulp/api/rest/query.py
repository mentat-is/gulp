"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

import json
from typing import Annotated

import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.time
import muty.uploadfile
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    File,
    Form,
    Header,
    Query,
    UploadFile,
)
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.defs
import gulp.plugin
import gulp.utils
from gulp import workers
from gulp.api import collab_api, elastic_api, rest_api
from gulp.api.collab.base import GulpCollabType, GulpUserPermission
from gulp.api.collab.operation import Operation
from gulp.api.collab.session import UserSession
from gulp.api.collab.stats import GulpStats
from gulp.api.elastic import query_utils
from gulp.api.elastic.query import SigmaGroupFilter, SigmaGroupFiltersParam
from gulp.api.elastic.structs import (
    GulpQueryFilter,
    GulpQueryOptions,
    GulpQueryParameter,
    GulpQueryType,
)
from gulp.defs import (
    API_DESC_PYSYGMA_PLUGIN,
    API_DESC_WS_ID,
    GulpPluginType,
    InvalidArgument,
    ObjectNotFound,
)
from gulp.plugin_internal import GulpPluginParams
from gulp.utils import logger

_app: APIRouter = APIRouter()


def _sanitize_tags(tags: list[str]) -> list[str]:
    """
    remove empty tags
    """
    if tags is None:
        return []

    return [t.strip() for t in tags if t.strip() != ""]


@_app.post(
    "/query_multi",
    response_model=JSendResponse,
    tags=["query"],
    response_model_exclude_none=True,
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "1": {
                            "summary": "GulpQueryFilter",
                            "value": {
                                "q": [
                                    {
                                        "rule": {
                                            "start_msec": 1289373944000,
                                            "end_msec": 1289373944000,
                                        },
                                        "name": "gulpqueryfilter test",
                                        "type": 3,
                                    }
                                ],
                                "options": {"sort": {"@timestamp": "asc"}},
                            },
                        },
                        "2": {
                            "summary": "sigma rule",
                            "value": {
                                "q": [
                                    {
                                        "rule": "title: Test\nid: 2dcca7b4-4b3a-4db6-9364-a019d54904bf\nstatus: test\ndescription: This is a test\nreferences:\n  - ref1\n  - ref2\ntags:\n  - attack.execution\n  - attack.t1059\nauthor: me\ndate: 2020-07-12\nlogsource:\n  category: process_creation\n  product: windows\ndetection:\n  selection:\n    EventID: 4732\n    gulp.context|endswith: context\n  condition: selection\nfields:\n  - EventId\n  - gulp.context\nfalsepositives:\n  - Everything\nlevel: medium",
                                        "type": 1,
                                        "name": "sigma test",
                                        "pysigma_plugin": "windows",
                                    }
                                ],
                                "options": {"sort": {"@timestamp": "asc"}},
                                "sigma_group_flts": [
                                    {
                                        "name": "test dummy APT",
                                        "expr": "(87911521-7098-470b-a459-9a57fc80bdfd AND e22a6eb2-f8a5-44b5-8b44-a2dbd47b1144)",
                                    }
                                ],
                            },
                        },
                        "3": {
                            "summary": "multiple stored rules",
                            "value": {
                                "q": [
                                    {"rule": 1, "type": 4},
                                    {"rule": 2, "type": 4},
                                    {"rule": 3, "type": 4},
                                ],
                                "options": {"sort": {"@timestamp": "asc"}},
                            },
                        },
                        "4": {
                            "summary": "mixed",
                            "value": {
                                "q": [
                                    {
                                        "name": "test_sigma",
                                        "type": 1,
                                        "rule": "title: Test\nid: 2dcca7b4-4b3a-4db6-9364-a019d54904bf\nstatus: test\ndescription: Match all *context test\nreferences:\n  - ref1\n  - ref2\ntags:\n  - attack.execution\n  - attack.test\nauthor: me\ndate: 2020-07-12\nlogsource:\n  category: process_creation\n  product: windows\ndetection:\n  selection:\n    gulp.context|endswith: context\n  condition: selection\nfields:\n  - EventId\n  - gulp.context\nfalsepositives:\n  - Everything\nlevel: medium",
                                        "pysigma_plugin": "windows",
                                    },
                                    {"rule": 1, "type": 4},
                                    {
                                        "name": "test_dsl",
                                        "type": 2,
                                        "rule": {
                                            "bool": {
                                                "must": [
                                                    {
                                                        "query_string": {
                                                            "query": "event.code:4732 AND gulp.context:*context",
                                                            "analyze_wildcard": True,
                                                        }
                                                    }
                                                ]
                                            }
                                        },
                                    },
                                    {
                                        "name": "test_gulpflt",
                                        "type": 3,
                                        "rule": {
                                            "start_msec": 1289373944000,
                                            "end_msec": 1289373944000,
                                        },
                                    },
                                ],
                                "options": {"sort": {"@timestamp": "asc"}},
                            },
                        },
                    }
                }
            }
        }
    },
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
    summary="perform one or more queries, in parallel.",
    description="supported:<br><br>"
    '. **sigma rule YAML**: "rule": str<br>'
    '. **elasticsearch DSL**: "rule": dict, "name" is mandatory<br>'
    '. **GulpQueryFilter**: "rule": dict, "name" is mandatory<br>'
    '. **stored query ID**: "rule": int<br><br>'
    '*flt* may be used to restrict the query to only a subset of the data, i.e. *flt.context=["machine1"]* only.<br>'
    "*options* is intended **per single query**.",
)
async def query_multi_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    q: Annotated[list[GulpQueryParameter], Body()],
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    sigma_group_flts: Annotated[list[SigmaGroupFilter], Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    if flt is None:
        flt = GulpQueryFilter()
    if options is None:
        options = GulpQueryOptions()

    if options.fields_filter is None:
        # use default fields filter
        options.fields_filter = ",".join(query_utils.FIELDS_FILTER_MANDATORY)

    logger().debug(
        "query_multi_handler, q=%s,\nflt=%s,\noptions=%s,\nsigma_group_flts=%s"
        % (q, flt, options, sigma_group_flts)
    )
    user_id = None
    try:
        user, session = await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
        user_id = session.user_id
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    if sigma_group_flts is not None:
        # preprocess to avoid having to access the db while querying
        sigma_group_flts = await query_utils.preprocess_sigma_group_filters(
            sigma_group_flts
        )

    # FIXME: this is hackish ... maybe it is better to pass operation_id and client_id also for queries, without relying on the filter
    operation_id: int = None
    client_id: int = None
    if flt.operation_id is not None and len(flt.operation_id) == 1:
        operation_id = flt.operation_id[0]
    if flt.client_id is not None and len(flt.client_id) == 1:
        client_id = flt.client_id[0]

    # create the request stats
    try:
        await GulpStats.create(
            await collab_api.collab(),
            GulpCollabType.STATS_QUERY,
            req_id,
            ws_id,
            operation_id,
            client_id,
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    # run
    coro = workers.query_multi_task(
        username=user.name,
        user_id=user_id,
        req_id=req_id,
        flt=flt,
        index=index,
        q=q,
        options=options,
        sigma_group_flts=sigma_group_flts,
        ws_id=ws_id,
    )
    await rest_api.aiopool().spawn(coro)
    return muty.jsend.pending_jsend(req_id=req_id)


@_app.post(
    "/query_raw",
    tags=["query"],
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "1": {
                            "summary": "DSL",
                            "value": {
                                "query_raw": {
                                    "bool": {
                                        "must": [
                                            {
                                                "query_string": {
                                                    "query": "event.code:4732 AND gulp.contextntext",
                                                    "analyze_wildcard": True,
                                                }
                                            }
                                        ]
                                    }
                                },
                                "options": {"sort": {"@timestamp": "asc"}},
                            },
                        }
                    }
                }
            }
        }
    },
    response_model=JSendResponse,
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
    summary="shortcut for query_multi() to perform a single raw query.",
    description="[OpenSearch query DSL](https://opensearch.org/docs/latest/query-dsl/).",
)
async def query_raw_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    query_raw: Annotated[dict, Body()],
    name: Annotated[
        str, Query(description="name of the query (leave empty to autogenerate)")
    ] = None,
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    gqp = GulpQueryParameter(rule=query_raw, type=GulpQueryType.RAW)
    if name is None:
        # generate
        name = "raw-%s" % (muty.string.generate_unique())
    gqp.name = name
    return await query_multi_handler(
        bt,
        token,
        index,
        ws_id,
        [gqp],
        flt=flt,
        options=options,
        req_id=req_id,
    )


@_app.post(
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
    summary="shortcut for query_multi() to perform a query using GulpQueryFilter.",
    description="internally, GulpQueryFilter is converted to a [`query_string`](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query.",
)
async def query_gulp_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    flt: Annotated[GulpQueryFilter, Body()],
    name: Annotated[
        str, Query(description="name of the query (leave empty to autogenerate)")
    ] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    # print parameters
    logger().debug(
        "query_gulp_handler: flt=%s, name=%s, options=%s" % (flt, name, options)
    )
    gqp = GulpQueryParameter(rule=flt.to_dict(), type=GulpQueryType.GULP_FILTER)
    if name is None:
        # generate
        name = "gulpfilter-%s" % (muty.string.generate_unique())

    gqp.name = name
    return await query_multi_handler(
        bt, token, index, ws_id, [gqp], flt=flt, options=options, req_id=req_id
    )


@_app.post(
    "/query_stored_sigma_tags",
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
    summary="shortcut to *query_multi* to perform multiple queries by using STORED and TAGGED sigma queries (i.e. created with *stored_query_create_from_sigma_zip*)",
    description='*flt* may be used to restrict the query to only a subset of the data, i.e. *flt.context=["machine1"]* only.<br>'
    "*options* is intended **per single query**.",
)
async def query_stored_sigma_tags_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    tags: Annotated[list[str], Body()],
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    sigma_group_flts: Annotated[list[SigmaGroupFilter], Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    logger().debug(
        "query_sigma_tags_handler: flt=%s, options=%s, tags=%s, sigma_group_flts=%s"
        % (flt, options, tags, sigma_group_flts)
    )
    try:
        tags = _sanitize_tags(tags)
        if len(tags) == 0:
            raise ObjectNotFound("no tags provided")
        # get stored queries by tags
        gqp = await query_utils.stored_sigma_tags_to_gulpqueryparameters(
            await collab_api.collab(),
            tags,
            all_tags_must_match=options.all_tags_must_match,
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return await query_multi_handler(
        bt,
        token,
        index,
        ws_id,
        gqp,
        flt=flt,
        options=options,
        req_id=req_id,
        sigma_group_flts=sigma_group_flts,
    )


@_app.post(
    "/query_sigma",
    response_model=JSendResponse,
    tags=["query"],
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "1": {
                            "summary": "sigma rule YAML",
                            "value": {
                                "sigma": "title: Test\nid: 2dcca7b4-4b3a-4db6-9364-a019d54904bf\nstatus: test\ndescription: Match all *context test\nreferences:\n  - ref1\n  - ref2\ntags:\n  - attack.execution\n  - attack.test\nauthor: me\ndate: 2020-07-12\nlogsource:\n  category: process_creation\n  product: windows\ndetection:\n  selection:\n    gulp.context|endswith: context\n  condition: selection\nfields:\n  - EventId\n  - gulp.context\nfalsepositives:\n  - Everything\nlevel: medium",
                                "options": {"limit": 10, "sort": {"@timestamp": "asc"}},
                            },
                        }
                    }
                }
            }
        }
    },
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
    summary="shortcut for query_multi() to perform a query using a single sigma rule YAML.",
    description='*flt* may be used to restrict the query to only a subset of the data, i.e. *flt.context=["machine1"]* only.<br>'
    "*options* is intended **per single query**.",
)
async def query_sigma_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    sigma: Annotated[str, Body()],
    pysigma_plugin: Annotated[
        str,
        Query(
            description=API_DESC_PYSYGMA_PLUGIN,
        ),
    ] = None,
    plugin_params: Annotated[GulpPluginParams, Body()] = None,
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    gqp = GulpQueryParameter(
        rule=sigma,
        type=GulpQueryType.SIGMA_YAML,
        pysigma_plugin=pysigma_plugin,
        plugin_params=plugin_params,
    )
    return await query_multi_handler(
        bt,
        token,
        index,
        ws_id,
        [gqp],
        flt=flt,
        options=options,
        req_id=req_id,
    )


@_app.post(
    "/query_sigma_files",
    response_model=JSendResponse,
    tags=["query"],
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "1": {
                            "summary": "query options",
                            "value": {
                                "options": {"limit": 10, "sort": {"@timestamp": "asc"}},
                            },
                        }
                    }
                }
            }
        }
    },
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
    summary="same as query_sigma_zip, but allows to upload one or more YML files instead of a ZIP.",
    description='*flt* may be used to restrict the query to only a subset of the data, i.e. *flt.context=["machine1"]* only.<br>'
    "*options* is intended **per single query**.",
)
async def query_sigma_files_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    sigma_files: Annotated[list[UploadFile], File(description="sigma rule YAMLs.")],
    pysigma_plugin: Annotated[
        str,
        Query(description=API_DESC_PYSYGMA_PLUGIN),
    ] = None,
    plugin_params: Annotated[GulpPluginParams, Body()] = None,
    tags: Annotated[list[str], Body()] = None,
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    sigma_group_flts: Annotated[SigmaGroupFiltersParam, Body(...)] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    files_path = None

    tags = _sanitize_tags(tags)
    try:
        # download files to tmp
        files_path, files = await muty.uploadfile.to_path_multi(sigma_files)
        logger().debug("%d files downloaded to %s ..." % (len(files), files_path))

        # create queries and call query_multi
        l = await query_utils.sigma_directory_to_gulpqueryparams(
            files_path,
            pysigma_plugin,
            tags_from_directories=False,
            plugin_params=plugin_params,
            tags_filter=tags if len(tags) > 0 else None,
            options=options,
        )
        return await query_multi_handler(
            bt,
            token,
            index,
            ws_id,
            l,
            flt=flt,
            options=options,
            req_id=req_id,
            sigma_group_flts=(
                sigma_group_flts.sgf if sigma_group_flts is not None else None
            ),
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
    finally:
        if files_path is not None:
            await muty.file.delete_file_or_dir_async(files_path)


@_app.post(
    "/query_sigma_zip",
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
    summary="shortcut for query_multi() to perform multiple queries using sigma rule YAMLs from a zip file.",
    description="*tags* may be used to restrict the rules used.<br>"
    '*flt* may be used to restrict the query to only a subset of the data, i.e. *flt.context=["machine1"]* only.<br>.'
    "*options* is intended **per single query**.",
)
async def query_sigma_zip_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    z: Annotated[UploadFile, File(description="zip with sigma rules.")],
    pysigma_plugin: Annotated[
        str,
        Query(description=API_DESC_PYSYGMA_PLUGIN),
    ] = None,
    plugin_params: Annotated[GulpPluginParams, Body()] = None,
    tags: Annotated[list[str], Body()] = None,
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    sigma_group_flts: Annotated[SigmaGroupFiltersParam, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)
    files_path = None

    tags = _sanitize_tags(tags)
    try:
        # decompress
        files_path = await muty.uploadfile.unzip(z)
        logger().debug("zipfile unzipped to %s" % (files_path))
        l = await query_utils.sigma_directory_to_gulpqueryparams(
            files_path,
            pysigma_plugin,
            plugin_params=plugin_params,
            tags_filter=tags if len(tags) > 0 else None,
            options=options,
        )
        return await query_multi_handler(
            bt,
            token,
            index,
            ws_id,
            l,
            flt=flt,
            options=options,
            req_id=req_id,
            sigma_group_flts=(
                sigma_group_flts.sgf if sigma_group_flts is not None else None
            ),
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
    finally:
        if files_path is not None:
            await muty.file.delete_file_or_dir_async(files_path)


@_app.post(
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
    summary="shortcut for query_multi() to perform a query using (one or more) stored query IDs.",
    description='*flt* may be used to restrict the query to only a subset of the data, i.e. *flt.context=["machine1"]* only.<br>'
    "*options* is intended **per single query**.",
)
async def query_stored_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    ws_id: Annotated[str, Query(description=API_DESC_WS_ID)],
    q: Annotated[list[int], Body()],
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)

    # build parameters
    try:
        l = []
        for qq in q:
            # rule name will be set when database is queried with the rule id at conversion time
            gqp = GulpQueryParameter(rule=qq, type=GulpQueryType.INDEX)
            l.append(gqp)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    return await query_multi_handler(
        bt,
        token,
        index,
        ws_id,
        l,
        flt=flt,
        options=options,
        req_id=req_id,
    )


@_app.post(
    "/query_max_min",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1714474423829,
                        "req_id": "89821150-bdef-4ff7-8586-83b75b108fea",
                        "data": {
                            "buckets": [
                                {
                                    "*": {
                                        "doc_count": 70814,
                                        "max_event.code": 5158.0,
                                        "min_@timestamp": 1475730263242.0,
                                        "max_@timestamp": 1617234805762.0,
                                        "min_event.code": 0.0,
                                    }
                                }
                            ],
                            "total": 70814,
                        },
                    }
                }
            }
        }
    },
    summary='get the "@timestamp" and "gulp.event.code" range in an index, possibly aggregating per field.',
)
async def query_max_min_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    group_by: Annotated[str, Query(description="group by this field.")] = None,
    flt: Annotated[GulpQueryFilter, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # check token
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )

        # query
        try:
            res = await elastic_api.query_max_min_per_field(
                elastic_api.elastic(), index, group_by, flt
            )
            # logger().debug("query_max_min_handler: %s", json.dumps(res, indent=2))
            return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=res))
        except ObjectNotFound:
            # return an empty result
            res = {"total": 0, "buckets": []}
            return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=res))

    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


def _parse_time_histograms(d: dict) -> dict:
    new_buckets: list = []
    for bucket in d["buckets"]:
        new_bucket: dict = {
            "time_msec": bucket["key"],
            "doc_count": bucket["doc_count"],
        }

        if "by_type" in bucket:
            for item in bucket["by_type"]["buckets"]:
                new_bucket[item["key"]] = {"doc_count": item["doc_count"], "events": []}

        if d.get("results") is not None:
            for result in d["results"]:
                if (
                    bucket["key"]
                    <= result["@timestamp"]
                    < bucket["key"] + d["interval_msec"]
                ):
                    for key, _ in new_bucket.items():
                        if key != "time_msec" and key != "doc_count":
                            new_bucket[key]["events"].append(result)

        for key in list(new_bucket.keys()):
            if (
                key != "time_msec"
                and key != "doc_count"
                and not new_bucket[key]["events"]
            ):
                del new_bucket[key]["events"]

        new_buckets.append(new_bucket)

    res = {"buckets": new_buckets, "total": d["total"]}
    if d.get("search_after") is not None:
        res["search_after"] = d["search_after"]
    return res


@_app.post(
    "/query_time_histograms",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1712658429019,
                        "req_id": "d7cbf2f1-f95a-4e1e-a984-e4b795376401",
                        "data": {
                            "buckets": [
                                {
                                    "time_msec": 1475730000000,
                                    "doc_count": 245,
                                    "5152": {
                                        "doc_count": 245,
                                        "events": [
                                            {
                                                "@timestamp": 1475730263242,
                                                "_id": "f3a7f81a9786af368ddad3ee34021f4f",
                                            },
                                            {
                                                "@timestamp": 1475730293663,
                                                "_id": "3c998d207a870a6ea665ecbf72e5d622",
                                            },
                                        ],
                                    },
                                },
                                {
                                    "time_msec": 1475733600000,
                                    "doc_count": 281,
                                    "5152": {"doc_count": 281},
                                },
                                {
                                    "time_msec": 1475737200000,
                                    "doc_count": 305,
                                    "5152": {"doc_count": 305},
                                },
                            ],
                            "total": 8251,
                            "search_after": [1475730365657],
                        },
                    }
                }
            }
        }
    },
    description="**WARNING**: this function may result in an error if *interval* is too dense of events (would generate too many buckets).",
    summary='get "@timestamp" histograms (events aggregated by "interval" buckets) in a time range.',
)
async def query_time_histograms_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    interval: Annotated[
        str,
        Query(
            description="make bucket intervals as large, in units of **m**(illi**s**econds), **s**(econds), **h**(ours), **m**(inutes), **d**(ays)."
        ),
    ] = "1h",
    group_by: Annotated[
        str, Query(description="group by this field (add *.keyword* for text fields).")
    ] = "event.code.keyword",
    return_hits: Annotated[
        bool,
        Query(
            description="return hits (the events themself) in the response (use *options.fields_filter* to tune which fields to return)."
        ),
    ] = False,
    flt: Annotated[GulpQueryFilter, Body()] = None,
    options: Annotated[GulpQueryOptions, Body()] = None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # check token
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )

        # query
        res = await elastic_api.query_time_histograms(
            elastic_api.elastic(),
            index,
            interval,
            flt,
            options,
            group_by=group_by,
            include_hits=return_hits,
        )

        # adjust output
        interval_msec = muty.time.time_definition_to_milliseconds(interval)
        res["buckets"] = res["aggregations"]["histograms"]["buckets"]
        res["interval_msec"] = interval_msec
        del res["aggregations"]
        logger().debug("query_time_histograms: %s", json.dumps(res, indent=2))
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data=_parse_time_histograms(res))
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


async def _parse_operation_aggregation(d: dict):
    # get all operation first
    all_ops = await Operation.get(await collab_api.collab())

    # parse aggregations into a more readable format
    result = []
    for op in all_ops:
        operation_dict = {"name": op.name, "id": op.id, "contexts": []}
        for operation in d["aggregations"]["operations"]["buckets"]:
            operation_key = str(operation["key"])
            if int(operation_key) == op.id:
                for context in operation["context"]["buckets"]:
                    context_key = context["key"]
                    context_dict = {
                        "name": context_key,
                        "doc_count": context["doc_count"],
                        "plugins": [],
                    }
                    for plugin in context["plugin"]["buckets"]:
                        plugin_key = plugin["key"]
                        plugin_dict = {
                            "name": plugin_key,
                            "src_file": [
                                {
                                    "name": file["key"],
                                    "doc_count": file["doc_count"],
                                    "max_event.code": file["max_event.code"]["value"],
                                    "min_event.code": file["min_event.code"]["value"],
                                    "min_@timestamp": file["min_@timestamp"]["value"],
                                    "max_@timestamp": file["max_@timestamp"]["value"],
                                }
                                for file in plugin["src_file"]["buckets"]
                            ],
                        }
                        context_dict["plugins"].append(plugin_dict)
                    operation_dict["contexts"].append(context_dict)
        result.append(operation_dict)
    return result


@_app.get(
    "/query_operations",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "status": "success",
            "timestamp_msec": 1715268048724,
            "req_id": "07d30a97-1a4b-447a-99da-ab530402e91c",
            "data": [
                {
                    "name": "testoperation",
                    "id": 1,
                    "contexts": [
                        {
                            "name": "testcontext",
                            "doc_count": 98630,
                            "plugins": [
                                {
                                    "name": "win_evtx",
                                    "src_file": [
                                        {
                                            "name": "security_big_sample.evtx",
                                            "doc_count": 62031,
                                            "max_event.code": 5158.0,
                                            "min_event.code": 1102.0,
                                            "min_@timestamp": 1475718427166.0,
                                            "max_@timestamp": 1475833104749.0,
                                        },
                                        {
                                            "name": "2-system-Security-dirty.evtx",
                                            "doc_count": 14621,
                                            "max_event.code": 5061.0,
                                            "min_event.code": 1100.0,
                                            "min_@timestamp": 1532738204663.0,
                                            "max_@timestamp": 1553118827379.0,
                                        },
                                        {
                                            "name": "Application.evtx",
                                            "doc_count": 6419,
                                            "max_event.code": 12305.0,
                                            "min_event.code": 0.0,
                                            "min_@timestamp": 1289373941000.0,
                                            "max_@timestamp": 1333809953000.0,
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    },
    summary="query distinct operations in the given index.",
    description="for every *operation*, results are returned aggregated per (source) *plugin* and *context*."
    "<br><br>"
    "for every *context*, *src_file*s are returned as well.",
)
async def query_operations_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description=gulp.defs.API_DESC_INDEX,
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # check token
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
        # query
        res = await elastic_api.query_operations(elastic_api.elastic(), index)
        logger().debug(
            "query_operations (before parsing): %s", json.dumps(res, indent=2)
        )
        res = await _parse_operation_aggregation(res)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=res))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/query_single_event",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701879738287,
                        "req_id": "561b55c5-6d63-498c-bcae-3114782baee2",
                        "data": {
                            "gulp.operation.id": 1,
                            "@timestamp": 1573258569309,
                            "gulp.timestamp.nsec": 1573258569309000000,
                            "gulp.context": "testcontext2",
                            "agent.type": "win_evtx",
                            "agent.id": "client:test_test_1.0",
                            "event.id": "1447406958",
                            "log.level": 5,
                            "gulp.log.level": 5,
                            "gulp.source.file": "Archive-ForwardedEvents-test.evtx",
                            "event.category": "System",
                            "event.code": "4624",
                            "event.duration": 0,
                            "event.hash": "24866d6b3df4b2d2db230185e09a461886f552ae77a02c3812811e66f24a3c86",
                            "event.original": '<?xml version="1.0" encoding="utf-8"?>\n<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">\n  <System>\n    <Provider Name="Microsoft-Windows-Security-Auditing" Guid="{54849625-5478-4994-A5BA-3E3B0328C30D}">\n    </Provider>\n    <EventID>4624</EventID>\n    <Version>1</Version>\n    <Level>0</Level>\n    <Task>12544</Task>\n    <Opcode>0</Opcode>\n    <Keywords>0x8020000000000000</Keywords>\n    <TimeCreated SystemTime="2019-11-08T23:20:55.123639800Z">\n    </TimeCreated>\n    <EventRecordID>1447406958</EventRecordID>\n    <Correlation>\n    </Correlation>\n    <Execution ProcessID="512" ThreadID="5204">\n    </Execution>\n    <Channel>Security</Channel>\n    <Computer>slad1.saclink.csus.edu</Computer>\n    <Security>\n    </Security>\n  </System>\n  <EventData>\n    <Data Name="SubjectUserSid">S-1-0-0</Data>\n    <Data Name="SubjectUserName">-</Data>\n    <Data Name="SubjectDomainName">-</Data>\n    <Data Name="SubjectLogonId">0x0</Data>\n    <Data Name="TargetUserSid">S-1-5-21-6361574-1898399280-860360866-540175</Data>\n    <Data Name="TargetUserName">UL-DLN1010001$</Data>\n    <Data Name="TargetDomainName">CSUS</Data>\n    <Data Name="TargetLogonId">0x7856672d</Data>\n    <Data Name="LogonType">3</Data>\n    <Data Name="LogonProcessName">Kerberos</Data>\n    <Data Name="AuthenticationPackageName">Kerberos</Data>\n    <Data Name="WorkstationName">-</Data>\n    <Data Name="LogonGuid">{10A95F00-E9A4-D04E-4D35-8C58E0F5E502}</Data>\n    <Data Name="TransmittedServices">-</Data>\n    <Data Name="LmPackageName">-</Data>\n    <Data Name="KeyLength">0</Data>\n    <Data Name="ProcessId">0x0</Data>\n    <Data Name="ProcessName">-</Data>\n    <Data Name="IpAddress">130.86.40.27</Data>\n    <Data Name="IpPort">52497</Data>\n    <Data Name="ImpersonationLevel">%%1840</Data>\n  </EventData>\n  <RenderingInfo Culture="en-US">\n    <Message>An account was successfully logged on.\r\n\r\nSubject:\r\n\tSecurity ID:\t\tS-1-0-0\r\n\tAccount Name:\t\t-\r\n\tAccount Domain:\t\t-\r\n\tLogon ID:\t\t0x0\r\n\r\nLogon Type:\t\t\t3\r\n\r\nImpersonation Level:\t\tDelegation\r\n\r\nNew Logon:\r\n\tSecurity ID:\t\tS-1-5-21-6361574-1898399280-860360866-540175\r\n\tAccount Name:\t\tUL-DLN1010001$\r\n\tAccount Domain:\t\tCSUS\r\n\tLogon ID:\t\t0x7856672D\r\n\tLogon GUID:\t\t{10A95F00-E9A4-D04E-4D35-8C58E0F5E502}\r\n\r\nProcess Information:\r\n\tProcess ID:\t\t0x0\r\n\tProcess Name:\t\t-\r\n\r\nNetwork Information:\r\n\tWorkstation Name:\t-\r\n\tSource Network Address:\t130.86.40.27\r\n\tSource Port:\t\t52497\r\n\r\nDetailed Authentication Information:\r\n\tLogon Process:\t\tKerberos\r\n\tAuthentication Package:\tKerberos\r\n\tTransited Services:\t-\r\n\tPackage Name (NTLM only):\t-\r\n\tKey Length:\t\t0\r\n\r\nThis event is generated when a logon session is created. It is generated on the computer that was accessed.\r\n\r\nThe subject fields indicate the account on the local system which requested the logon. This is most commonly a service such as the Server service, or a local process such as Winlogon.exe or Services.exe.\r\n\r\nThe logon type field indicates the kind of logon that occurred. The most common types are 2 (interactive) and 3 (network).\r\n\r\nThe New Logon fields indicate the account for whom the new logon was created, i.e. the account that was logged on.\r\n\r\nThe network fields indicate where a remote logon request originated. Workstation name is not always available and may be left blank in some cases.\r\n\r\nThe impersonation level field indicates the extent to which a process in the logon session can impersonate.\r\n\r\nThe authentication information fields provide detailed information about this specific logon request.\r\n\t- Logon GUID is a unique identifier that can be used to correlate this event with a KDC event.\r\n\t- Transited services indicate which intermediate services have participated in this logon request.\r\n\t- Package name indicates which sub-protocol was used among the NTLM protocols.\r\n\t- Key length indicates the length of the generated session key. This will be 0 if no session key was requested.</Message>\n    <Level>Information</Level>\n    <Task>Logon</Task>\n    <Opcode>Info</Opcode>\n    <Channel>Security</Channel>\n    <Provider>Microsoft Windows security auditing.</Provider>\n    <Keywords>\n      <Keyword>Audit Success</Keyword>\n    </Keywords>\n  </RenderingInfo>\n</Event>',
                            "_id": "testop-testcontext2-24866d6b3df4b2d2db230185e09a461886f552ae77a02c3812811e66f24a3c86-1573258569309",
                        },
                    }
                }
            }
        }
    },
    summary="query a single event.",
)
async def query_single_event_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    index: Annotated[
        str,
        Query(
            description="name of the datastream to query.",
            openapi_examples=gulp.defs.EXAMPLE_INDEX,
        ),
    ],
    gulp_id: Annotated[
        str, Query(description='the elasticsearch "_id" of the event to be retrieved.')
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp.utils.ensure_req_id(req_id)
    try:
        # check token
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )

        # query
        res = await elastic_api.query_single_event(
            elastic_api.elastic(), index, gulp_id
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=res))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/query_external",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701879738287,
                        "req_id": "561b55c5-6d63-498c-bcae-3114782baee2",
                        "data": [{"GulpDocument"}, {"GulpDocument"}],
                    }
                }
            }
        }
    },
    summary="query an external source.",
    description="with this API you can query an external source (i.e. a SIEM) for data without it being ingested into GULP, using a `query_plugin` in `$PLUGIN_DIR/query`.<br><br>"
    "GulpQueryFilter is used to filter the data from the external source, only the following fields are used and the rest is ignored:<br>"
    '- `start_msec`: start "@timestamp"<br>'
    '- `end_msec`: end "@timestamp"<br>'
    '- `extra`: a dict with any extra filter to match, like: `{ "extra": { "key": "value" } }` (check `GulpBaseFilter` documentation)<br><br>'
    "GulpQueryOptions is used to specify the following (and, as above, the rest is ignored):<br>"
    "- `limit`: return max these entries **per chunk** on the websocket<br>"
    "- `sort`: defaults to sort by ASCENDING timestamp<br>"
    "- `fields_filter`: a CSV list of fields to include in the result.<br>"
    "external source specific parameters must be provided in the `plugin_params.extra` field as a dict, i.e.<br>"
    '`"extra": { "username": "...", "password": "...", "url": "...", "index": "...", "mapping": { "key": { "map_to": "..." } } }`',
)
async def query_external_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    operation_id: Annotated[int, Query(description=gulp.defs.API_DESC_OPERATION)],
    client_id: Annotated[int, Query(description=gulp.defs.API_DESC_CLIENT)],
    ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
    plugin: Annotated[str, Query(description=gulp.defs.API_DESC_PLUGIN)],
    plugin_params: Annotated[
        GulpPluginParams,
        Body(
            examples=[
                {
                    "extra": {
                        "username": "...",
                        "password": "...",
                        "url": "http://localhost:9200",
                        "index": "testidx",
                        "mapping": {"key": {"map_to": "...", "is_event_code": False}},
                    }
                }
            ]
        ),
    ],
    flt: Annotated[
        GulpQueryFilter,
        Body(examples=[{"start_msec": 1475730263242, "end_msec": 1475830263242}]),
    ],
    options: Annotated[
        GulpQueryOptions,
        Body(
            examples=[
                {
                    "fields_filter": "event.original",
                }
            ]
        ),
    ]=None,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)

    # print the request
    logger().debug(
        "query_external_handler: token=%s, operation_id=%s, client_id=%s, ws_id=%s, plugin=%s, plugin_params=%s, flt=%s, options=%s, req_id=%s"
        % (
            token,
            operation_id,
            client_id,
            ws_id,
            plugin,
            plugin_params,
            flt,
            options,
            req_id,
        )
    )
    if len(flt.to_dict()) == 0:
        raise JSendException(req_id=req_id, ex=InvalidArgument("flt is empty!"))
    if len(plugin_params.extra) == 0:
        raise JSendException(
            req_id=req_id, ex=InvalidArgument("plugin_params.extra is empty!")
        )
    if options is None:
        options = GulpQueryOptions()

    try:
        user, _ = await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    # create the request stats
    try:
        await GulpStats.create(
            await collab_api.collab(),
            GulpCollabType.STATS_QUERY,
            req_id,
            ws_id,
            operation_id,
            client_id,
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    # run
    coro = workers.query_external_task(
        req_id=req_id,
        ws_id=ws_id,
        operation_id=operation_id,
        client_id=client_id,
        username=user.name,
        user_id=user.id,
        plugin=plugin,
        plugin_params=plugin_params,
        flt=flt,
        options=options,
    )
    await rest_api.aiopool().spawn(coro)
    return muty.jsend.pending_jsend(req_id=req_id)


@_app.post(
    "/query_external_single",
    tags=["query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701879738287,
                        "req_id": "561b55c5-6d63-498c-bcae-3114782baee2",
                        "data": {"GulpDocument"},
                    }
                }
            }
        }
    },
    summary="query an external source **and return a single event**.",
    description="with this API you can query an external source (i.e. a SIEM) for data without it being ingested into GULP, using a `query_plugin` in `$PLUGIN_DIR/query`.<br><br>"
    "this API is used to return a single event **with all fields** if `query_external` has been used to retrieve only partial data (through `fields_filter`).<br><br>"
    "external source specific parameters must be provided in the `plugin_params.extra` field as a dict, i.e.<br>"
    '`"extra": { "username": "...", "password": "...", "url": "...", "index": "..." }`',
)
async def query_external_single_handler(
    bt: BackgroundTasks,
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    plugin: Annotated[str, Query(description=gulp.defs.API_DESC_PLUGIN)],
    plugin_params: Annotated[
        GulpPluginParams,
        Body(
            examples=[
                {
                    "extra": {
                        "username": "...",
                        "password": "...",
                        "url": "http://localhost:9200",
                        "index": "testidx"
                    }
                }
            ]
        ),
    ],
    event: Annotated[
        dict,
        Body(
            examples=[
                {
                    "gulp.operation.id": 1,
                    "@timestamp": 1573258569309,
                    "gulp.timestamp.nsec": 1573258569309000000,
                    "gulp.context": "testcontext2",
                    "agent.type": "win_evtx",
                    "agent.id": "client:test_test_1.0",
                    "event.id": "1447406958",
                }
            ]
        ),
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp.utils.ensure_req_id(req_id)

    # print the request
    logger().debug(
        "query_external_single_handler: token=%s, plugin=%s, plugin_params=%s, event=%s, req_id=%s"
        % (token, plugin, plugin_params, event, req_id)
    )
    if len(plugin_params.extra) == 0:
        raise JSendException(
            req_id=req_id, ex=InvalidArgument("plugin_params.extra is empty!")
        )
    try:
        await UserSession.check_token(
            await collab_api.collab(), token, GulpUserPermission.READ
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex

    # load plugin
    mod = None
    try:
        mod: gulp.plugin.PluginBase = gulp.plugin.load_plugin(
            plugin, plugin_type=GulpPluginType.QUERY
        )
    except Exception as ex:
        # can't load plugin ...
        raise JSendException(req_id=req_id, ex=ex) from ex

    try:
        ev = await mod.query_single(
            plugin_params,
            event,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=ev))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
    finally:
        gulp.plugin.unload_plugin(mod)


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
