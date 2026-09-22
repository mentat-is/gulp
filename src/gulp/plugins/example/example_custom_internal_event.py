"""Example extension that emits custom events from main and worker processes."""

import asyncio
from typing import override

from gulp.api.server_api import GulpServer
from gulp.events import GulpEvent, events
from gulp.glug import Glug
from gulp.plugin import GulpPluginBase, GulpPluginType


EVENT_FROM_MAIN = "example_custom_internal_event.from_main"
EVENT_FROM_WORKER = "example_custom_internal_event.from_worker"


class Plugin(GulpPluginBase):
    """Demonstrate waited and notification custom event emission."""

    async def _my_event_task_worker(self) -> None:
        while True:
            await events.emit(EVENT_FROM_WORKER, data={"wait": False})
            await events.emit(EVENT_FROM_WORKER, data={"wait": True}, wait=True)
            await asyncio.sleep(5)

    async def _my_event_task_main(self) -> None:
        while True:
            await events.emit(EVENT_FROM_MAIN, data={"wait": False})
            await events.emit(EVENT_FROM_MAIN, data={"wait": True}, wait=True)
            await asyncio.sleep(5)

    @override
    async def post_init(self, **kwargs) -> None:
        if not self.is_running_in_main_process():
            return
        events.subscribe(self, [EVENT_FROM_MAIN, EVENT_FROM_WORKER])
        GulpServer.spawn_bg_task(
            self._my_event_task_main(), name="my_event_task_main"
        )
        await GulpServer.get_instance().spawn_worker_task(
            self._my_event_task_worker, task_name="my_event_task_worker"
        )

    async def on_event(self, event: GulpEvent) -> None:
        Glug.get_instance().debug(
            f"extension plugin={self.name} received {event.event}",
            detail=event.data,
        )

    def desc(self) -> str:
        return "Shows how to emit and handle custom events."

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "example_custom_internal_event"

    def version(self) -> str:
        return "1.0"
