"""
This module provides utility REST API endpoints for Gulp.

It includes various endpoints for:
- Request management (get, cancel, list)
- Server management (restart, status, garbage collection)
- Plugin management (list, get, upload, delete)
- Mapping file management (list, get, upload, delete)
- Version information

These endpoints support system administration, debugging, and customizing
the Gulp platform through plugin and mapping management.

"""

import base64
import os
from typing import Annotated, Literal

import muty.file
import orjson
import psutil
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.redis_api import GulpRedis
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.structs import ObjectNotFound

router: APIRouter = APIRouter()


@router.get(
    "/request_get_by_id",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": GulpRequestStats.example(),
                    }
                }
            }
        }
    },
    summary="gets a request stats.",
)
async def request_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpRequestStats
            _, obj, _ = await GulpRequestStats.get_by_id_wrapper(
                sess,
                token,
                obj_id,
            )
            return JSendResponse.success(req_id=req_id, data=obj.to_dict())
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/request_cancel",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="cancel a request.",
    description="""
cancel a running request by setting its `status` to `canceled` (or `failed` or `done`), so the engine can delete it after the expiration time.

- `token` needs `admin` permission or to be the owner of the request.
- any scheduled task for the request is deleted as well.
""",
)
async def request_cancel_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id_to_cancel: Annotated[
        str, Query(description="request id to cancel.", example="test_req")
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    expire_now: Annotated[
        bool,
        Query(
            description="if set, the request expiration time is set to now, so the stats are deleted immediately from the collab database (default is delete in 5 minutes)."
        ),
    ] = False,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpRequestStats
            _, obj, op = await GulpRequestStats.get_by_id_wrapper(
                sess,
                token,
                req_id_to_cancel,
                enforce_owner=True,
            )
            await obj.set_canceled(sess, expire_now=expire_now)

            # also delete related queued tasks (if any) in Redis
            d: int = await GulpRedis.get_instance().task_purge_by_filter(
                operation_id=op.id, req_id=req_id_to_cancel
            )
            MutyLogger.get_instance().debug(
                "also deleted %d pending tasks for request %s" % (d, req_id_to_cancel)
            )

            return JSendResponse.success(req_id=req_id, data={"id": req_id_to_cancel})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/request_set_completed",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="complete the request.",
    description="""
sets the request either as `done` or `failed`.

- `token` needs `admin` permission or to be the owner of the request.
- any scheduled task for the request is deleted as well.
""",
)
async def request_set_completed_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id_to_complete: Annotated[
        str, Query(description="request id to set completed.", example="test_req")
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    failed: Annotated[
        bool, Query(description="if set, the request is marked as failed.")
    ] = False,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)

    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpRequestStats
            _, obj, op = await GulpRequestStats.get_by_id_wrapper(
                sess,
                token,
                req_id_to_complete,
                enforce_owner=True,
            )
            await obj.set_finished(
                sess,
                status=GulpRequestStatus.FAILED if failed else GulpRequestStatus.DONE,
            )

            return JSendResponse.success(req_id=req_id, data={"id": req_id_to_complete})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/request_delete",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": {"deleted": 123},
                    }
                }
            }
        }
    },
    summary="deletes one or more requests on the collab database.",
    description="""
- `token` needs `admin` permission or to be the owner of the request/s.
""",
)
async def request_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    obj_id: Annotated[
        str,
        Query(
            description="the request id to delete: if not set, all the requests are deleted for the given `operation_id`.",
            example="obj_id",
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession
            op: GulpOperation
            s, op, _ = await GulpOperation.get_by_id_wrapper(sess, token, operation_id)
            flt = GulpCollabFilter()
            flt.operation_ids = [op.id]
            if obj_id:
                # delete only the given request id
                flt.ids = [obj_id]
            deleted = await GulpRequestStats.delete_by_filter(sess, flt, s.user_id)
            return JSendResponse.success(req_id=req_id, data={"deleted": deleted})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/request_list",
    tags=["stats"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="get requests list.",
    description="""
get a list of all requests (identified by their `req_id`) issued by the calling user.

- if token has `admin` permission, requests from all users are returned (according to the `operation_id` filter).
""",
)
async def request_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    running_only: Annotated[
        bool, Query(description="if set, only return requests that are still running.")
    ] = False,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        if running_only:
            # only return ongoing requests
            flt = GulpCollabFilter(status=["ongoing"])
        else:
            # all requests
            flt = GulpCollabFilter()

        # restrict to operation id
        flt.operation_ids = [operation_id]
        stats: list[dict] = await GulpRequestStats.get_by_filter_wrapper(
            token, flt, operation_id=operation_id
        )
        return JSendResponse.success(req_id=req_id, data=stats)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/server_status",
    tags=["utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1738176594947,
                        "req_id": "test_req",
                        "data": {
                            "memory": {
                                "total": 31490994176,
                                "available": 17274675200,
                                "percent": 45.1,
                                "used": 12644323328,
                                "free": 15703216128,
                            },
                            "cpu": {
                                "percent": 36.6,
                                "count": 8,
                                "freq": {
                                    "current": 3135.96325,
                                    "min": 1400.0,
                                    "max": 2100.0,
                                },
                            },
                            "disk": {
                                "total": 467783827456,
                                "used": 215005560832,
                                "free": 228940931072,
                                "percent": 48.4,
                            },
                            "processes": [
                                {
                                    "pid": 52220,
                                    "name": "gulp",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 148799488,
                                        "rss_mb": 141.90625,
                                        "vms": 964345856,
                                        "vms_mb": 919.671875,
                                        "shared": 5677056,
                                        "shared_mb": 5.4140625,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 /home/vscode/.local/bin/gulp",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52234,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 13586432,
                                        "rss_mb": 12.95703125,
                                        "vms": 19591168,
                                        "vms_mb": 18.68359375,
                                        "shared": 6639616,
                                        "shared_mb": 6.33203125,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.resource_tracker import main;main(14)",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52235,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 140615680,
                                        "rss_mb": 134.1015625,
                                        "vms": 332206080,
                                        "vms_mb": 316.81640625,
                                        "shared": 26664960,
                                        "shared_mb": 25.4296875,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=25) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52270,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145494016,
                                        "rss_mb": 138.75390625,
                                        "vms": 333705216,
                                        "vms_mb": 318.24609375,
                                        "shared": 29970432,
                                        "shared_mb": 28.58203125,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=23) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52274,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145592320,
                                        "rss_mb": 138.84765625,
                                        "vms": 409251840,
                                        "vms_mb": 390.29296875,
                                        "shared": 29945856,
                                        "shared_mb": 28.55859375,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=27) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52278,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145539072,
                                        "rss_mb": 138.796875,
                                        "vms": 333742080,
                                        "vms_mb": 318.28125,
                                        "shared": 29908992,
                                        "shared_mb": 28.5234375,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=29) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52282,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145559552,
                                        "rss_mb": 138.81640625,
                                        "vms": 409288704,
                                        "vms_mb": 390.328125,
                                        "shared": 30007296,
                                        "shared_mb": 28.6171875,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=31) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52286,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145637376,
                                        "rss_mb": 138.890625,
                                        "vms": 409243648,
                                        "vms_mb": 390.28515625,
                                        "shared": 29990912,
                                        "shared_mb": 28.6015625,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=33) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52290,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145661952,
                                        "rss_mb": 138.9140625,
                                        "vms": 333701120,
                                        "vms_mb": 318.2421875,
                                        "shared": 30146560,
                                        "shared_mb": 28.75,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=35) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52294,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145764352,
                                        "rss_mb": 139.01171875,
                                        "vms": 333684736,
                                        "vms_mb": 318.2265625,
                                        "shared": 30015488,
                                        "shared_mb": 28.625,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=37) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                                {
                                    "pid": 52298,
                                    "name": "python3.13",
                                    "cpu_percent": 0.0,
                                    "memory_info": {
                                        "rss": 145625088,
                                        "rss_mb": 138.87890625,
                                        "vms": 333623296,
                                        "vms_mb": 318.16796875,
                                        "shared": 29974528,
                                        "shared_mb": 28.5859375,
                                    },
                                    "status": "sleeping",
                                    "cmdline": "/usr/local/bin/python3.13 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=15, pipe_handle=39) --multiprocessing-fork",
                                    "username": "vscode",
                                },
                            ],
                        },
                    }
                }
            }
        }
    },
    summary="""gets gulp health status.
    """,
    description="""
gets detailed gulp server health status, including memory, cpu, disk usage, and running processes.

- token needs `admin` permission.
    """,
)
async def server_status_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    gulp_only: Annotated[
        bool,
        Query(
            description="if set, only return process information for gulp processes only. either, return process information for all the running processes."
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:

    params = locals()
    ServerUtils.dump_params(params)
    async with GulpCollab.get_instance().session() as sess:
        await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

    def _get_process_info(proc, gulp_only):
        # check if the process is the current pid, or a child of the current pid
        if gulp_only and proc.ppid() != os.getpid():
            return None

        try:
            mem_inffo = proc.memory_info()
            return {
                "pid": proc.pid,
                "name": proc.name(),
                "cpu_percent": proc.cpu_percent(),
                "memory_info": {
                    "rss": mem_inffo.rss,
                    "rss_mb": mem_inffo.rss / 1024 / 1024,
                    "vms": mem_inffo.vms,
                    "vms_mb": mem_inffo.vms / 1024 / 1024,
                    "shared": mem_inffo.shared,
                    "shared_mb": mem_inffo.shared / 1024 / 1024,
                },
                "status": proc.status(),
                "cmdline": " ".join(proc.cmdline()) if proc.cmdline() else "",
                "username": proc.username(),
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None

    # Get system stats
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    stats = {
        "memory": {
            "total": memory.total,
            "available": memory.available,
            "percent": memory.percent,
            "used": memory.used,
            "free": memory.free,
        },
        "cpu": {
            "percent": psutil.cpu_percent(interval=1),
            "count": psutil.cpu_count(),
            "freq": psutil.cpu_freq()._asdict() if psutil.cpu_freq() else {},
        },
        "disk": {
            "total": disk.total,
            "used": disk.used,
            "free": disk.free,
            "percent": disk.percent,
        },
        "processes": [
            info
            for info in [
                _get_process_info(proc, gulp_only) for proc in psutil.process_iter()
            ]
            if info is not None
        ],
    }

    return JSendResponse.success(data=stats, req_id=req_id)


@router.get(
    "/plugin_list",
    tags=["plugin"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734609216840,
                        "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                        "data": [
                            {
                                "display_name": "AI report generator",
                                "type": ["extension"],
                                "desc": "autogenerate security incident reports using AI.",
                                "path": "/Users/valerino/repos/gulp/gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension/ai_report.py",
                                "data": {},
                                "filename": "ai_report.py",
                                "sigma_support": False,
                                "custom_parameters": [],
                                "depends_on": ["story"],
                                "tags": [],
                                "version": "",
                            },
                            {
                                "display_name": "OpenSearch anomalies detector",
                                "type": ["extension"],
                                "desc": "find anomalies using Opensearch Anomaly Detection plugin.",
                                "path": "/Users/valerino/repos/gulp/gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension/os_anomaly_detector.py",
                                "data": {},
                                "filename": "os_anomaly_detector.py",
                                "sigma_support": False,
                                "custom_parameters": [],
                                "depends_on": [],
                                "tags": [],
                                "version": "",
                            },
                            {
                                "display_name": "enrich_abuse",
                                "type": ["enrichment"],
                                "desc": "abuse.ch url enrichment plugin",
                                "path": "/Users/valerino/repos/gulp/src/gulp/plugins/enrich_abuse.py",
                                "data": {},
                                "filename": "enrich_abuse.py",
                                "sigma_support": False,
                                "custom_parameters": [
                                    {
                                        "name": "url_fields",
                                        "type": "list",
                                        "default_value": [
                                            "url.full",
                                            "url.original",
                                            "http.request.referrer",
                                        ],
                                        "desc": "a list of url fields to enrich.",
                                        "location": "query",
                                        "required": False,
                                    },
                                    {
                                        "name": "auth_key",
                                        "type": "str",
                                        "desc": "abuse.ch auth-key (if not provided, the config file is checked for it)",
                                        "location": "query",
                                        "required": False,
                                    },
                                ],
                                "depends_on": [],
                                "tags": [],
                                "version": "",
                            },
                            {
                                "display_name": "chrome_history_sqlite_stacked",
                                "type": ["ingestion"],
                                "desc": "chrome based browsers history sqlite stacked plugin",
                                "path": "/Users/valerino/repos/gulp/src/gulp/plugins/chrome_history_sqlite_stacked.py",
                                "data": {},
                                "filename": "chrome_history_sqlite_stacked.py",
                                "sigma_support": False,
                                "custom_parameters": [],
                                "depends_on": ["sqlite"],
                                "tags": [],
                                "version": "",
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="list available plugins.",
)
async def plugin_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.READ)

            l = await GulpPluginBase.list_plugins()

            # turn to model_dump
            ll: list[dict] = []
            for p in l:
                ll.append(p.model_dump(exclude_none=True))
            return JSONResponse(JSendResponse.success(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/ui_plugin_list",
    tags=["plugin"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734609216840,
                        "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                        "data": {
                            "status": "success",
                            "timestamp_msec": 1748263060378,
                            "req_id": "test_req",
                            "data": [
                                {
                                    "display_name": "Test UI Plugin",
                                    "plugin": "some_gulp_plugin",
                                    "extension": True,
                                    "version": "1.0.0",
                                    "desc": "A plugin for testing UI components",
                                }
                            ],
                        },
                    }
                }
            }
        }
    },
    summary="list available UI plugins.",
)
async def ui_plugin_list_handler(
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            l = await GulpPluginBase.list_ui_plugins()

            # turn to model_dump
            ll: list[dict] = []
            for p in l:
                d: dict = p.model_dump(exclude_none=True)
                ll.append(d)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=ll))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/ui_plugin_get",
    tags=["plugin"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701546711919,
                        "req_id": "ddfc094f-4a5b-4a23-ad1c-5d1428b57706",
                        "data": {
                            "filename": "win_evtx.py",
                            "path": "/opt/gulp/plugins/win_evtx.py",
                            "content": "base64 file content here",
                        },
                    }
                }
            }
        }
    },
    summary="download UI plugin",
    description="""
- file is read from `GULP_WORKING_DIR/plugins/ui` directory if found, either from the main installation directory in `plugins/ui`.
- file content is returned as base64.
""",
)
async def ui_plugin_get_handler(
    plugin: Annotated[
        str,
        Query(
            description='filename of the plugin to retrieve content for, i.e. "plugin.tsx"'
        ),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())

    try:
        extra_path = GulpConfig.get_instance().path_plugins_extra()
        default_path = GulpConfig.get_instance().path_plugins_default()
        plugin_path: str = None
        if extra_path:
            # build the extra path
            plugin_path = os.path.join(extra_path, "ui", plugin.lower())
            if not os.path.exists(plugin_path):
                # if not found in extra_path, try default path
                plugin_path = None
        if not plugin_path:
            # use default path
            plugin_path = os.path.join(default_path, "ui", plugin.lower())
            if not os.path.exists(plugin_path):
                raise ObjectNotFound(
                    "%s not found both in %s or %s" % (plugin, extra_path, default_path)
                )

        # read file content
        f = await muty.file.read_file_async(plugin_path)
        filename = os.path.basename(plugin_path)

        return JSONResponse(
            JSendResponse.success(
                req_id=req_id,
                data={
                    "filename": filename,
                    "path": plugin_path,
                    "content": base64.b64encode(f).decode(),
                },
            )
        )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/version",
    tags=["utility"],
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
                        "data": {"version": "gulp v0.0.9 (muty v0.2)"},
                    }
                }
            }
        }
    },
    summary="get gULP version.",
)
async def get_version_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.READ)

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"version": GulpServer.get_instance().version_string()},
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/mapping_file_list",
    tags=["mapping"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1735294441540,
                        "req_id": "test_req",
                        "data": [
                            {
                                "metadata": {"plugin": ["splunk.py"]},
                                "filename": "splunk.json",
                                "path": "/opt/gulp/mapping_files/splunk.json",
                                "mapping_ids": ["splunk"],
                            },
                            {
                                "metadata": {"plugin": ["csv.py"]},
                                "filename": "mftecmd_csv.json",
                                "path": "/opt/gulp/mapping_files/mftecmd_csv.json",
                                "mapping_ids": ["record", "boot", "j", "sds"],
                            },
                            {
                                "metadata": {"plugin": ["win_evtx.py"]},
                                "filename": "windows.json",
                                "path": "/opt/gulp/mapping_files/windows.json",
                                "mapping_ids": ["windows"],
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="lists available mapping files.",
)
async def mapping_file_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())

    def _exists(d: list[dict], filename: str) -> bool:
        for f in d:
            if f["filename"].lower() == filename.lower():
                return True
        return False

    async def _list_mapping_files_internal(path: str, d: list[dict]) -> None:
        """
        lists mapping files in path and append to d
        """
        MutyLogger.get_instance().info("listing mapping files in %s" % (path))
        if not path:
            return

        files = await muty.file.list_directory_async(path)

        # for each file, get metadata and mapping_ids
        for f in files:
            if not f.lower().endswith(".json"):
                continue

            filename = os.path.basename(f)
            if _exists(d, filename):
                MutyLogger.get_instance().warning(
                    "skipping mapping file %s (already exists)" % (f)
                )
                continue

            # read file
            content = await muty.file.read_file_async(f)
            js = orjson.loads(content)

            # get metadata
            mtd = js.get("metadata", {})

            # get mapping_id for each mapping
            mappings: dict = js.get("mappings", {})
            mapping_ids = list(str(key) for key in mappings.keys())

            d.append(
                {
                    "metadata": mtd,
                    "filename": filename.lower(),
                    "path": f,
                    "mapping_ids": mapping_ids,
                }
            )

    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token)

        d: list[dict] = []
        default_path = GulpConfig.get_instance().path_mapping_files_default()
        extra_path = GulpConfig.get_instance().path_mapping_files_extra()

        # extra_path first
        await _list_mapping_files_internal(extra_path, d)

        # then default
        await _list_mapping_files_internal(default_path, d)

        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
        raise JSendException(req_id=req_id) from ex
