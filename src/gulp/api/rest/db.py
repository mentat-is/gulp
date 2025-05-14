"""
Database and Data Management REST API endpoints for Gulp.

This module provides endpoints to manage the underlying OpenSearch and PostgreSQL databases used by Gulp,
including operations like creating/deleting indexes, resetting the collaboration database, and rebasing indexes.

The endpoints are organized into several main operations:
- OpenSearch datastream operations (create, delete, list)
- PostgreSQL collaboration database management
- Complete system reset
- Index rebasing operations

Most operations require admin-level permissions, as they can potentially delete or modify significant amounts of data.
"""

import json
from typing import Annotated

import muty.log
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest.test_values import TEST_OPERATION_ID
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import WSDATA_REBASE_DONE, GulpRebaseDonePacket, GulpWsSharedQueue
from gulp.config import GulpConfig
from gulp.process import GulpProcess

router: APIRouter = APIRouter()


async def db_reset(delete_data: bool = False, user_id: str = None, create_operation_id: str=None) -> None:
    """
    resets the collab database

    Args:
        delete_data (bool, optional): if True, all data on OpenSearch related to the existing operations will be deleted. Defaults to False.
        user_id (str): user id to use to delete the data. If None, "admin" will be used.
        create_operation_id (str, optional): if set, a new operation with this id will be created.
    """
    # check if the database exists
    url = GulpConfig.get_instance().postgres_url()
    collab = GulpCollab.get_instance()
    exists = await collab.db_exists(url)

    if exists:
        MutyLogger.get_instance().info("collab database exists !")
        try:
            await collab.init(main_process=True)
            if delete_data:
                MutyLogger.get_instance().info("deleting data in all operations...")
                # enumerate all operations
                async with GulpCollab.get_instance().session() as sess:
                    ops = await GulpOperation.get_by_filter(
                        sess,
                        user_id=user_id or "admin",
                    )
                    for op in ops:
                        # delete all data related to the operation
                        MutyLogger.get_instance().info(
                            "deleting data for operation %s" % op.id
                        )
                        await GulpOpenSearch.get_instance().datastream_delete(op.index)
        except SchemaMismatch as ex:
            MutyLogger.get_instance().warning("collab database schema mismatch, will be recreated!")
    else:
        MutyLogger.get_instance().info("collab database does not exist, creating it...")

    # shutdown and recreate the database
    await GulpCollab.get_instance().shutdown()
    collab = GulpCollab.get_instance()
    await collab.init(main_process=True, force_recreate=True)
    await collab.create_default_users()
    await collab.create_default_glyphs()
    if create_operation_id:
        collab = GulpCollab.get_instance()
        await collab.init(main_process=True)
        await collab.create_default_operation(operation_id=create_operation_id, index=create_operation_id)

    MutyLogger.get_instance().info("collab database reset done !")


@router.post(
    "/gulp_reset",
    tags=["db"],
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
                    }
                }
            }
        }
    },
    summary="reset gulp.",
    description="""
> **WARNING: THE WHOLE COLLAB DATABASE WILL BE DELETED AND RECREATED**

- `token` needs to have `admin` permission.
- use this only when the database is corrupted (or the structure needs to be updated) and/or you want to start from scratch.
""",
)
async def gulp_reset_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    delete_data: Annotated[
        bool,
        Query(
            description="if set, all existing operations data on OpenSearch will be deleted.",
            example=True,
        ),
    ] = True,
    create_default_operation: Annotated[
        bool,
        Query(
            description='if set, a default operation named "test_operation" will be created.',
        ),
    ] = False,
    restart_processes: Annotated[
        bool,
        Query(
            description="if true, the process pool is restarted as well.",
        ),
    ] = True,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.ADMIN
            )

        # reset
        await db_reset(delete_data, user_id=s.user_id, create_operation_id=TEST_OPERATION_ID if create_default_operation else None)
        if restart_processes:
            # restart the process pool
            await GulpProcess.get_instance().init_gulp_process()

        return JSONResponse(JSendResponse.success(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


async def _rebase_internal(
    user_id: str,
    req_id: str,
    ws_id: str,
    operation_id: str,
    index: str,
    dest_index: str,
    offset_msec: int,
    flt: GulpQueryFilter,
    rebase_script: str,
):
    """
    runs in a worker process to rebase the index.
    """
    try:
        # get existing index datastream
        template = await GulpOpenSearch.get_instance().index_template_get(index)

        # create the destination datastream, applying the same template
        await GulpOpenSearch.get_instance().datastream_create_from_raw_dict(
            dest_index, template
        )

        # rebase
        res = await GulpOpenSearch.get_instance().rebase(
            index,
            dest_index=dest_index,
            offset_msec=offset_msec,
            flt=flt,
            rebase_script=rebase_script,
        )

        # the operation object must now point to the new index
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            d = {
                "index": dest_index,
            }
            await op.update(
                sess,
                d,
                ws_id=None,  # do not propagate on the websocket
                req_id=req_id,
                user_id=user_id,
            )

    except Exception as ex:
        # signal failure on the websocket
        MutyLogger.get_instance().exception(ex)
        GulpWsSharedQueue.get_instance().put(
            type=WSDATA_REBASE_DONE,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
            data=GulpRebaseDonePacket(
                operation_id=operation_id,
                src_index=index,
                dest_index=dest_index,
                status=GulpRequestStatus.FAILED,
                result=muty.log.exception_to_string(ex),
            ),
        )
        return

    # done
    MutyLogger.get_instance().debug(
        "rebase done, result=%s" % (json.dumps(res, indent=2))
    )
    # signal the websocket
    GulpWsSharedQueue.get_instance().put(
        type=WSDATA_REBASE_DONE,
        ws_id=ws_id,
        user_id=user_id,
        req_id=req_id,
        operation_id=operation_id,
        data=GulpRebaseDonePacket(
            operation_id=operation_id,
            src_index=index,
            dest_index=dest_index,
            status=GulpRequestStatus.DONE,
            result=res,
        ),
    )


@router.post(
    "/opensearch_rebase_index",
    response_model=JSendResponse,
    tags=["db"],
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
    summary="rebases operation's index to a different time.",
    description="""
rebases `operation_id.index` and creates a new `dest_index` with rebased `@timestamp` + `offset`, then set the operation's index to `dest_index` on success.

- `token` needs `ingest` permission.
- `flt` may be used to filter the documents to rebase.
- rebase happens in the background: when it is done, a `WSDATA_REBASE_DONE` event is sent to the `ws_id` websocket.
""",
)
async def opensearch_rebase_index_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
    dest_index: Annotated[
        str,
        Query(
            description="name of the destination index, will be reset or created if not exists.",
            example="new_index",
        ),
    ],
    offset_msec: Annotated[
        int,
        Query(
            example=3600000,
            description="""offset in milliseconds to add to document `@timestamp` and `gulp.timestamp`.

- to subtract, use a negative offset.
""",
        ),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    flt: Annotated[str, Depends(APIDependencies.param_query_flt_optional)] = None,
    rebase_script: Annotated[
        str,
        Body(
            description="""
optional custom rebase script to run on the documents.

- must be a [painless script](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-guide.html).
- the script receives as input a single parameter `offset_nsec`, the offset in `nanoseconds from the unix epoch` to be applied to the fields during the rebase operation.
- the example script is the one applied by default (rebases `@timestamp` and `gulp.timestamp`).
""",
            examples=[
                """if (ctx._source['@timestamp'] != 0) {
    def ts = ZonedDateTime.parse(ctx._source['@timestamp']);
    def new_ts = ts.plusNanos(params.offset_nsec);
    ctx._source['@timestamp'] = new_ts.toString();
    ctx._source["gulp.timestamp"] += params.offset_nsec;
}"""
            ],
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)

    try:
        async with GulpCollab.get_instance().session() as sess:
            # check permission and get user id and index
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op, permission=GulpUserPermission.INGEST
            )
            user_id = s.user_id
            index = op.index

        if index == dest_index:
            raise JSendException(req_id=req_id) from Exception(
                "index and dest_index must be different! (index=%s, dest_index=%s)"
                % (index, dest_index)
            )

        # spawn a task which runs the rebase in a worker process
        kwds = dict(
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            index=index,
            dest_index=dest_index,
            offset_msec=offset_msec,
            flt=flt,
            rebase_script=rebase_script,
        )

        async def worker_coro(kwds: dict):
            await GulpProcess.get_instance().process_pool.apply(
                _rebase_internal, kwds=kwds
            )

        await GulpRestServer.get_instance().spawn_bg_task(worker_coro(kwds))

        # and return pending
        return JSONResponse(JSendResponse.pending(req_id=req_id))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
