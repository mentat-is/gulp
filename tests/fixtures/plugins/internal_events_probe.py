"""Live integration probe for worker-to-main chunk events."""

from typing import Annotated, override

from fastapi import Query

from gulp.api.server_api import GulpServer
from gulp.events import GulpCoreEvent, GulpEvent, events
from gulp.plugin import GulpPluginBase, GulpPluginType

BROADCAST_EVENT = "integration.internal_events.broadcast"


class Plugin(GulpPluginBase):
    """Expose controls and observations for the internal event integration test."""

    def __init__(self, path: str, module_name: str, pickled: bool = False, **kwargs):
        super().__init__(path, module_name, pickled, **kwargs)
        self._enabled = False
        self._pre_req_ids: list[str] = []
        self._post_req_ids: list[str] = []
        self._broadcast_events: list[dict[str, str]] = []

    @override
    async def post_init(self, **kwargs) -> None:
        if not self.is_running_in_main_process():
            return
        GulpServer.get_instance().add_api_route(
            "/internal_events_probe_configure",
            self.configure_handler,
            methods=["POST"],
            tags=["test"],
        )
        GulpServer.get_instance().add_api_route(
            "/internal_events_probe_status",
            self.status_handler,
            methods=["GET"],
            tags=["test"],
        )
        GulpServer.get_instance().add_api_route(
            "/internal_events_probe_broadcast",
            self.broadcast_handler,
            methods=["POST"],
            tags=["test"],
        )
        events.subscribe(self, [BROADCAST_EVENT])

    async def configure_handler(
        self,
        enabled: Annotated[bool, Query()],
        reset: Annotated[bool, Query()] = False,
    ) -> dict:
        if reset:
            self._pre_req_ids.clear()
            self._post_req_ids.clear()
            self._broadcast_events.clear()
        if enabled and not self._enabled:
            events.subscribe(
                self,
                [GulpCoreEvent.CHUNK_PRE_INGEST, GulpCoreEvent.CHUNK_POST_INGEST],
            )
        elif not enabled and self._enabled:
            events.unsubscribe(self)
            events.subscribe(self, [BROADCAST_EVENT])
        self._enabled = enabled
        await events.drain()
        return await self.status_handler()

    async def broadcast_handler(
        self,
        marker: Annotated[str, Query()],
        large: Annotated[bool, Query()] = True,
        wait: Annotated[bool, Query()] = False,
        broadcast: Annotated[bool, Query()] = True,
    ) -> dict:
        data = {"marker": marker}
        if large:
            data["blob"] = "x" * 300_000
        result = await events.emit(
            BROADCAST_EVENT,
            data=data,
            wait=wait,
            broadcast=broadcast,
        )
        return {
            "marker": marker,
            "large": large,
            "event_id": result.event_id if result else None,
        }

    async def status_handler(self) -> dict:
        return {
            "enabled": self._enabled,
            "mask": await events.listener_mask(),
            "pre_req_ids": list(self._pre_req_ids),
            "post_req_ids": list(self._post_req_ids),
            "broadcast_events": list(self._broadcast_events),
        }

    async def on_event(self, event: GulpEvent) -> dict | None:
        if event.event == GulpCoreEvent.CHUNK_PRE_INGEST:
            self._pre_req_ids.append(event.req_id)
            chunk = [
                {**document, "event.internal_event_probe": event.req_id}
                for document in event.data["chunk"]
            ]
            return {**event.data, "chunk": chunk}
        if event.event == GulpCoreEvent.CHUNK_POST_INGEST:
            self._post_req_ids.append(event.req_id)
        if event.event == BROADCAST_EVENT:
            self._broadcast_events.append(
                {"event_id": event.event_id, "marker": event.data["marker"]}
            )
        return None

    def desc(self) -> str:
        return "Integration-only probe for chunk event delivery."

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "Internal events integration probe"
