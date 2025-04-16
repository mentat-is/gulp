import asyncio
from typing import Annotated

import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import Header, Query, Depends
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import GulpSharedWsQueue, GulpWsQueueDataType
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.process import GulpProcess

"""
# extension plugins

## loading

extension plugins are automatically loaded at startup from `PLUGIN_DIR/extension`.

## internals

- they may extend api through `GulpRestServer.get_instance().add_api_route()`.
- `their init runs in the MAIN process context`
"""

class Events:
    events: list[str]

class EventsData:
    affected_items: list[str]
    total_affected_items: int
    total_failed_items: int
    failed_items: list[str]

class EventsResponse:
    data: EventsData
    message: str
    error: int

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
        GulpSharedWsQueue.get_instance().put(
            GulpWsQueueDataType.COLLAB_UPDATE,
            req_id=req_id,
            ws_id=ws_id,
            operation_id=operation_id,
            user_id="dummy",
            data={"hello": "world"},
        )
        return {"done": True}

    async def _handle_wazuh_events(
        self,
        events: Events = [],
    ):
        print(events)
        return EventsResponse

    def _add_api_routes(self):
        GulpRestServer.get_instance().add_api_route(
            "/wazuh/events",
            self.wazuh_extension_handler,
            methods=["POST"],
            response_model=None, #TODO: use a valid pydantic response model (wip above)
            response_model_exclude_none=False,
            tags=["extensions"],
            responses={
                200: {}
            },
            summary="Send security events to be analyzed.",
        )

    async def wazuh_extension_handler(
        self,
        #token: Annotated[str, Depends(APIDependencies.param_token)], # for now its public, we need to integrate it somehow using the JWT token sent by hte wazuh agents
        #operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)], # this is from the config in this case
        #context_id: Annotated[str, Depends(APIDependencies.param_context_id)], # this is from the config/extracted from the events
        #ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
        #req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
        events: dict,
    ) -> JSendResponse:
        req_id=1

        try:
            # spawn coroutine in the main process, will run asap
            coro = self._handle_wazuh_events(
                events
            )
            await GulpProcess.get_instance().coro_pool.spawn(coro)
            return JSendResponse.pending(req_id=req_id)
        except Exception as ex:
            raise JSendException(req_id=req_id, ex=ex) from ex

    def desc(self) -> str:
        return "Wazuh sink API"

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.EXTENSION]

    def display_name(self) -> str:
        return "wazuh"

    def version(self) -> str:
        return "1.0"
