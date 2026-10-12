"""Live integration probe for the read-only request observers of extension plugins."""

import json
from collections import deque
from typing import Annotated, override

from fastapi import Query

from gulp.api.server.request_observer import (
    GulpObservation,
    GulpObservedRequest,
    GulpObservedResponse,
    GulpRequestObserver,
)
from gulp.api.server_api import GulpServer
from gulp.plugin import GulpPluginBase, GulpPluginType

# the requests the observer was told about, newest last
_seen: deque[dict] = deque(maxlen=500)


class _ProbeObserver(GulpRequestObserver):
    """Record every request but the health checks, with the head of the bodies of /login."""

    @override
    def on_request_start(self, request: GulpObservedRequest) -> GulpObservation | None:
        if request.path == "/docs":
            # not interested: the dispatcher does nothing else for this request
            return None
        if request.path == "/login":
            return GulpObservation(request_body_bytes=1024, response_body_bytes=16)
        return GulpObservation()

    @override
    def on_request_end(
        self, request: GulpObservedRequest, response: GulpObservedResponse
    ) -> None:
        entry = {
            "method": request.method,
            "path": request.path,
            "route": response.route,
            "status": response.status_code,
            "has_token": "token" in request.headers,
            "client_known": request.client_host is not None,
            "response_head": response.response_body.decode("utf-8", "replace"),
        }
        if response.request_body:
            # only the user id: a probe has no business keeping the password either
            entry["login_user_id"] = json.loads(response.request_body).get("user_id")
        _seen.append(entry)


class Plugin(GulpPluginBase):
    """Register and remove the probe observer on request, and report what it saw."""

    def __init__(self, path: str, module_name: str, pickled: bool = False, **kwargs):
        super().__init__(path, module_name, pickled, **kwargs)
        self._observer = _ProbeObserver()
        self._enabled = False

    @override
    async def post_init(self, **kwargs) -> None:
        if not self.is_running_in_main_process():
            return
        GulpServer.get_instance().add_api_route(
            "/request_observer_probe_configure",
            self.configure_handler,
            methods=["POST"],
            tags=["test"],
        )
        GulpServer.get_instance().add_api_route(
            "/request_observer_probe_status",
            self.status_handler,
            methods=["GET"],
            tags=["test"],
        )

    async def configure_handler(
        self,
        enabled: Annotated[bool, Query()],
        reset: Annotated[bool, Query()] = False,
    ) -> dict:
        if reset:
            _seen.clear()
        if enabled:
            GulpServer.get_instance().add_request_observer(self._observer)
        else:
            GulpServer.get_instance().remove_request_observer(self._observer)
        self._enabled = enabled
        return await self.status_handler()

    async def status_handler(self) -> dict:
        return {"enabled": self._enabled, "seen": list(_seen)}

    @override
    async def unload(self) -> None:
        if self.is_running_in_main_process():
            GulpServer.get_instance().remove_request_observer(self._observer)
        await super().unload()

    def desc(self) -> str:
        return "Integration-only probe for request observers."

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "Request observer integration probe"
