from typing import Any, override

from muty.log import MutyLogger
import asyncio

from gulp.api.server_api import GulpServer
from gulp.api.ws_api import GulpRedisBroker
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.process import GulpProcess
from gulp.structs import GulpInternalEvent, GulpInternalEventResult

"""
this plugin demonstrates how to emit and receive custom internal events in both the main process and worker processes. 
It registers for two custom internal events "EVENT_FROM_MAIN" and "EVENT_FROM_WORKER" and emits these events periodically from both the main process 
and worker processes, using both the blocking (put_internal_event_wait) and non-blocking (put_internal_event) methods. 
The internal_event_callback receives these events in the main process and logs them.
"""
class Plugin(GulpPluginBase):

    def __init__(
        self,
        path: str,
        module_name: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, module_name, pickled, **kwargs)
        MutyLogger.get_instance().debug(
            "path=%s, pickled=%r, kwargs=%s" % (path, pickled, kwargs)
        )

    async def _my_event_task_worker(self):
        # this is running in a worker process
        redis_broker = GulpRedisBroker.get_instance()
        while True:
            # emit "EVENT_FROM_WORKER", internal_event_callback will be called in the main process and receive this event
            """MutyLogger.get_instance().debug(
                "_my_event_task_worker running in worker process, emitting EVENT_FROM_WORKER."
            )"""
            await redis_broker.put_internal_event(
                "EVENT_FROM_WORKER", data={"put_type": "put_internal_event"}
            )

            # emit "EVENT_FROM_WORKER" again but this time use put_internal_event_wait to wait for a response from the internal_event_callback before continuing
            """ MutyLogger.get_instance().debug(
                "_my_event_task_worker now emitting EVENT_FROM_WORKER and wait for response..."
            ) """
            await redis_broker.put_internal_event_wait(
                "EVENT_FROM_WORKER", data={"put_type": "put_internal_event_wait"}
            )
            """ MutyLogger.get_instance().debug(
                f"_my_event_task_worker received response from EVENT_FROM_WORKER: {res}"
            ) """
            await asyncio.sleep(5)

    async def _my_event_task_main(self):
        # this is running in the main process
        from gulp.structs import GulpInternalEventsManager

        redis_broker = GulpRedisBroker.get_instance()
        while True:
            # emit "EVENT_FROM_MAIN", internal_event_callback will be called in the main process and receive this event
            """MutyLogger.get_instance().debug(
                "_my_event_task_main running in main process, emitting EVENT_FROM_MAIN."
            )"""
            await redis_broker.put_internal_event(
                "EVENT_FROM_MAIN", data={"put_type": "put_internal_event"}
            )

            # emit "EVENT_FROM_MAIN" again but this time use put_internal_event_wait to wait for a response from the internal_event_callback before continuing
            """ MutyLogger.get_instance().debug(
                "_my_event_task_main now emitting EVENT_FROM_MAIN and wait for response..."
            ) """
            await redis_broker.put_internal_event_wait(
                "EVENT_FROM_MAIN", data={"put_type": "put_internal_event_wait"}
            )
            """ MutyLogger.get_instance().debug(
                f"_my_event_task_main received response from EVENT_FROM_MAIN: {res}"
            ) """
            await asyncio.sleep(5)

    @override
    async def post_init(self, *kwargs):
        if self.is_running_in_main_process():
            # when running in the main process we register for a custom events we emit periodically
            MutyLogger.get_instance().debug(
                "registering custom internal event callbacks in main process."
            )
            from gulp.structs import GulpInternalEventsManager

            GulpInternalEventsManager.get_instance().register(
                self, ["EVENT_FROM_MAIN", "EVENT_FROM_WORKER"]
            )

            # start a background task that emits "EVENT_FROM_MAIN" periodically: this task runs IN THE MAIN PROCESS.
            """ MutyLogger.get_instance().debug(
                "starting background task in MAIN PRODCESS that emits EVENT_FROM_MAIN periodically..."
            ) """
            GulpServer.spawn_bg_task(
                self._my_event_task_main(), name="my_event_task_main"
            )

            # start a worker task that emits "EVENT_FROM_WORKER" periodically: this task runs IN A WORKER PROCESS.
            """ MutyLogger.get_instance().debug(
                "starting background task in WORKER PROCESS that emits EVENT_FROM_WORKER periodically..."
            ) """
            await GulpServer.get_instance().spawn_worker_task(
                self._my_event_task_worker, task_name="my_event_task_worker"
            )

    def desc(self) -> str:
        return "Shows how to handle custom internal events."

    @override
    async def unload(self) -> None:
        MutyLogger.get_instance().debug(
            self.name
            + " is unloading, cancelling background tasks and deregistering internal event callbacks."
        )

        # cancel background tasks
        # self._set_termination_signal()  # signal background tasks to terminate by setting the termination signal in shared memory

        # call super
        await super().unload()
        # self._close_termination_signal()

    async def internal_event_callback(
        self, ev: GulpInternalEvent
    ) -> GulpInternalEventResult | None:
        from gulp.structs import GulpInternalEventsManager

        # receives the events, both from the main process and worker processes, and logs them. This callback runs IN THE MAIN PROCESS.
        if ev.type == "EVENT_FROM_MAIN":
            MutyLogger.get_instance().debug(
                f"extension plugin={self.name} received my_event_from_main: {ev.data}"
            )
        elif ev.type == "EVENT_FROM_WORKER":
            MutyLogger.get_instance().debug(
                f"extension plugin={self.name} received my_event_from_worker: {ev.data}"
            )
        return None

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "example_custom_internal_event"

    def version(self) -> str:
        return "1.0"
