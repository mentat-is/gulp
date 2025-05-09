"""
Extension Example Plugin Module

This module provides an example implementation of an extension plugin for the Gulp framework.
It demonstrates how to create an extension plugin that adds custom API routes and handles
both main process and worker process operations.

The plugin adds an `/example_extension` API endpoint that:
1. Handles PUT requests
2. Creates an example stats entry in the database
3. Spawns a task in a worker process
4. Demonstrates message passing between processes using shared queues

Extension plugins in Gulp are:
- Automatically loaded from `PLUGIN_DIR/extension` at startup
- Initialized in the main process context
- Able to extend the API through `GulpRestServer.get_instance().add_api_route()`
- Optionally re-initialized in worker processes if they support pickling

This example serves as a template for developing custom extensions for the Gulp framework.

"""

import asyncio
from typing import Annotated

from fastapi import Depends
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import WSDATA_COLLAB_UPDATE, GulpWsSharedQueue
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.process import GulpProcess


class Plugin(GulpPluginBase):
    def __init__(
        self,
        path: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:

        # extensions may support pickling to be able to be re-initialized in worker processes
        super().__init__(path, pickled, **kwargs)
        MutyLogger.get_instance().debug(
            "path=%s, pickled=%r, kwargs=%s" % (path, pickled, kwargs)
        )

        # by calling is_running_in_main_process() they can distinguish between main and worker process
        # add api routes only once, in the main process
        if self.is_running_in_main_process():
            # in the first init, add api routes (we are in the MAIN process here)
            self._add_api_routes()
            MutyLogger.get_instance().debug(
                "%s extension plugin initialized" % (self.display_name)
            )
        else:
            # in the re-init, we are in the worker process here
            MutyLogger.get_instance().debug(
                "%s extension plugin re-initialized" % self.display_name()
            )

    async def _run_in_worker(
        self,
        user_id: str,
        operation_id: str,
        ws_id: str,
        req_id: str,
        **kwargs,
    ) -> dict:
        # this runs in a task in a worker process
        MutyLogger.get_instance().error(
            "IN WORKER PROCESS, for user_id=%s, operation_id=%s, ws_id=%s, req_id=%s"
            % (user_id, operation_id, ws_id, req_id)
        )
        GulpWsSharedQueue.get_instance().put(
            WSDATA_COLLAB_UPDATE,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            user_id="dummy",
            data={"hello": "world"},
        )
        return {"done": True}

    async def _example_task(
        self,
        user_id: str,
        operation_id: str,
        context_id: str,
        ws_id: str,
        req_id: str,
        **kwargs,
    ):
        # this runs in the main process
        MutyLogger.get_instance().error(
            "IN MAIN PROCESS, for user_id=%s, operation_id=%s, context_id=%s, ws_id=%s, req_id=%s"
            % (user_id, operation_id, context_id, ws_id, req_id)
        )
        # create an example stats
        async with GulpCollab.get_instance().session() as sess:
            try:
                await GulpRequestStats.create(
                    token=None,
                    ws_id=ws_id,
                    req_id=req_id,
                    object_data={
                        "source_total": 33,
                    },
                    operation_id=operation_id,
                    sess=sess,
                    user_id=user_id,
                )

            except Exception as ex:
                raise JSendException(req_id=req_id) from ex

        # spawn coro in worker process
        tasks = []
        MutyLogger.get_instance().debug(
            "spawning process for extension example for user_id=%s, operation_id=%s, context_id=%s, ws_id=%s, req_id=%s"
            % (user_id, operation_id, context_id, ws_id, req_id)
        )
        try:
            tasks.append(
                GulpProcess.get_instance().process_pool.apply(
                    self._run_in_worker,
                    (user_id, operation_id, ws_id, req_id),
                )
            )

            # and async wait for it to finish
            res = await asyncio.gather(*tasks, return_exceptions=True)
            MutyLogger.get_instance().error("extension example done: %s" % res)

        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            raise JSendException(req_id=req_id) from ex

    def _add_api_routes(self):
        # add /example_extension API
        GulpRestServer.get_instance().add_api_route(
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
        token: Annotated[str, Depends(APIDependencies.param_token)],
        operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
        context_id: Annotated[str, Depends(APIDependencies.param_context_id)],
        ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
    ) -> JSendResponse:
        try:
            async with GulpCollab.get_instance().session() as sess:
                s = await GulpUserSession.check_token(sess, token)

                # spawn coroutine in the main process, will run asap
                coro = self._example_task(
                    s.user_id, operation_id, context_id, ws_id, req_id
                )
                await GulpRestServer.get_instance().spawn_bg_task(coro)
                return JSendResponse.pending(req_id=req_id)
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    def desc(self) -> str:
        return "Extension example."

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.EXTENSION]

    def display_name(self) -> str:
        return "extension_example"

    def version(self) -> str:
        return "1.0"
