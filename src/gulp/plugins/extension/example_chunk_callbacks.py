from typing import override

from muty.log import MutyLogger

from gulp.plugin import GulpPluginBase, GulpPluginType


class Plugin(GulpPluginBase):
    """Extension plugin that registers a global chunk ingestion callback."""
    
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
    
    @override
    async def post_init(self, *kwargs):
        if self.is_running_in_main_process():
            # when running in the main process we register for the EVENT_CHUNK_INGESTED event.
            # workers will publish the event via Redis after every chunk is flushed, dispatching happens in the
            # main process.
            MutyLogger.get_instance().debug("registering chunk callback in main process.")
            from gulp.plugin import GulpInternalEventsManager

            GulpInternalEventsManager.get_instance().register(
                self, [GulpInternalEventsManager.EVENT_CHUNK_INGESTED]
            )

    def desc(self) -> str:
        return "Registers a post-processing chunk ingestion callback."

    async def internal_event_callback(self, ev):
        from gulp.plugin import GulpInternalEventsManager

        if ev.type == GulpInternalEventsManager.EVENT_CHUNK_INGESTED:
            data = ev.data or {}
            chunk_len = len(data.get("chunk", []))
            operation_id = data.get("operation_id")
            user_id = data.get("user_id")
            req_id = data.get("req_id")
            plugin = data.get("plugin")
            MutyLogger.get_instance().debug(
                f"extension plugin={self.name} received EVENT_CHUNK_INGESTED: chunk_len={chunk_len}, "
                f"operation_id={operation_id}, user_id={user_id}, req_id={req_id}, "
                f"plugin={plugin}"
            )

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "example_chunk_callbacks"

    def version(self) -> str:
        return "1.0"
