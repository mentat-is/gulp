"""Live integration probe for the ASGI middlewares registered by extension plugins."""

from collections import deque
from typing import Annotated, override

from fastapi import Query
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from gulp.api.server_api import GulpServer
from gulp.plugin import GulpPluginBase, GulpPluginType

PROBE_HEADER = b"x-extension-middleware-probe"

# the requests the middleware saw, newest last
_seen: deque[dict] = deque(maxlen=500)


class _ProbeMiddleware:
    """Record every http request and mark its response with PROBE_HEADER."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # stays None when the application raised before answering
        status_code: int | None = None

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                message["headers"] = [*message.get("headers", []), (PROBE_HEADER, b"1")]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            _seen.append(
                {
                    "method": scope["method"],
                    "path": scope["path"],
                    "status": status_code,
                }
            )


class Plugin(GulpPluginBase):
    """Register and remove the probe middleware on request, and report what it saw."""

    def __init__(self, path: str, module_name: str, pickled: bool = False, **kwargs):
        super().__init__(path, module_name, pickled, **kwargs)
        self._enabled = False

    @override
    async def post_init(self, **kwargs) -> None:
        if not self.is_running_in_main_process():
            return
        GulpServer.get_instance().add_api_route(
            "/extension_middleware_probe_configure",
            self.configure_handler,
            methods=["POST"],
            tags=["test"],
        )
        GulpServer.get_instance().add_api_route(
            "/extension_middleware_probe_status",
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
            GulpServer.get_instance().add_extension_middleware(_ProbeMiddleware)
        else:
            GulpServer.get_instance().remove_extension_middleware(_ProbeMiddleware)
        self._enabled = enabled
        return await self.status_handler()

    async def status_handler(self) -> dict:
        return {"enabled": self._enabled, "seen": list(_seen)}

    @override
    async def unload(self) -> None:
        if self.is_running_in_main_process():
            GulpServer.get_instance().remove_extension_middleware(_ProbeMiddleware)
        await super().unload()

    def desc(self) -> str:
        return "Integration-only probe for extension middlewares."

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "Extension middleware integration probe"
