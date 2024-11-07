import asyncio
from typing import Annotated

import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import Header, Query
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.api.rest_api as rest_api
import gulp.defs
import gulp.utils
from gulp.api.collab.base import GulpCollabType, GulpUserPermission
from gulp.api.collab.session import GulpUserSession
from gulp.api.collab.stats import GulpStats
from gulp.api.elastic.query import QueryResult
from gulp.api.rest import ws as ws_api
from gulp.plugin import GulpPluginBase
from gulp.utils import GulpLogger

"""
# extension plugins

## loading

extension plugins are automatically loaded at startup from `PLUGIN_DIR/extension`.

## internals

- they may extend api through `rest_api.fastapi_app().add_api_route()`.
- `their init runs in the MAIN process context`
- they may use aiopool and process_executor from rest_api as usual, **as long as they are run from the MAIN process (in this example, "example_task" is running in the MAIN process)**.

"""


class Plugin(GulpPluginBase):
    def __init__(
        self,
        path: str,
        **kwargs,
    ) -> None:
        super().__init__(path, **kwargs)
        # add api routes for this plugin
        if not self._check_pickled():
            # in the first init, add api routes (we are in the MAIN process here)
            self._add_api_routes()
            GulpLogger().debug(
                "%s extension plugin initialized, aiopool=%s, executor=%s, fastapi_app=%s"
                % (
                    self.display_name(),
                    rest_api.aiopool(),
                    rest_api.process_executor(),
                    rest_api.fastapi_app(),
                )
            )
        else:
            # in the re-init, we are in the worker process here
            GulpLogger().debug("%s extension plugin re-initialized" % self.display_name())

    async def _run_in_worker(
        self,
        user_id: int,
        operation_id: int,
        client_id: int,
        ws_id: str,
        req_id: str,
        **kwargs,
    ) -> QueryResult:
        GulpLogger().debug(
            "IN WORKER PROCESS, for user_id=%s, operation_id=%s, client_id=%s, ws_id=%s, req_id=%s"
            % (user_id, operation_id, client_id, ws_id, req_id)
        )
        ws_api.shared_queue_add_data(
            ws_api.WsQueueDataType.QUERY_RESULT,
            req_id,
            {"hellooooooooooooo": "wooooooooorld"},
            ws_id=ws_id,
        )

        return QueryResult(query_raw={"result": "example"})

    async def _example_task(
        self,
        user_id: int,
        operation_id: int,
        client_id: int,
        ws_id: str,
        req_id: str,
        **kwargs,
    ):
        # create an example stats
        try:
            await GulpStats.create(
                await gulp.api.collab_api.collab(),
                GulpCollabType.STATS_QUERY,
                req_id,
                ws_id,
                operation_id,
                client_id,
            )
        except Exception as ex:
            raise JSendException(req_id=req_id, ex=ex) from ex

        # then run internal function in one of the tasks of the worker processes
        tasks = []
        executor = rest_api.process_executor()
        GulpLogger().debug(
            "spawning process for extension example for user_id=%s, operation_id=%s, client_id=%s, ws_id=%s, req_id=%s, executor=%s"
            % (user_id, operation_id, client_id, ws_id, req_id, executor)
        )
        try:
            tasks.append(
                executor.apply(
                    self._run_in_worker,
                    (
                        user_id,
                        operation_id,
                        client_id,
                        ws_id,
                        req_id,
                    ),
                )
            )

            # and async wait for it to finish
            qr: QueryResult = await asyncio.gather(*tasks, return_exceptions=True)
            print(qr)
        except Exception as ex:
            GulpLogger().exception(ex)
            raise JSendException(req_id=req_id, ex=ex) from ex

    def _add_api_routes(self):
        # setup own router
        fa = rest_api.fastapi_app()

        # add /example_extension API
        fa.add_api_route(
            "/example_extension",
            self.example_extension_handler,
            methods=["PUT"],
            response_model=JSendResponse,
            response_model_exclude_none=True,
            tags=["extensions"],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "status": "success",
                                "timestamp_msec": 1701278479259,
                                "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                                "data": {"result": "example"},
                            }
                        }
                    }
                }
            },
            summary="just an example.",
        )

    async def example_extension_handler(
        self,
        token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
        operation_id: Annotated[str, Query(description=gulp.defs.API_DESC_OPERATION)],
        client_id: Annotated[str, Query(description=gulp.defs.API_DESC_CLIENT)],
        ws_id: Annotated[str, Query(description=gulp.defs.API_DESC_WS_ID)],
        req_id: Annotated[str, Query(description=muty.jsend.API_DESC_REQID)] = None,
    ) -> JSendResponse:
        req_id = gulp.utils.ensure_req_id(req_id)

        try:
            user, session = await GulpUserSession.check_token(
                await collab_api.session(), token, GulpUserPermission.READ
            )
            user_id = session.user_id
        except Exception as ex:
            raise JSendException(req_id=req_id, ex=ex) from ex

        # run task in the background of the MAIN process
        coro = self._example_task(user_id, operation_id, client_id, ws_id, req_id)
        await rest_api.aiopool().spawn(coro)
        return muty.jsend.pending_jsend(req_id=req_id)

    def desc(self) -> str:
        return "Extension example."

    def type(self) -> gulp.defs.GulpPluginType:
        return gulp.defs.GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "extension_example"

    def version(self) -> str:
        return "1.0"
