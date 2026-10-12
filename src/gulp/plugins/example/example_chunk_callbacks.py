from typing import override

from gulp.events import GulpCoreEvent, GulpEvent, events
from gulp.glug import Glug
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
        Glug.get_instance().debug(
            f"path={path}, pickled={pickled!r}", detail=kwargs
        )

    @override
    async def post_init(self, *kwargs):
        if self.is_running_in_main_process():
            # when running in the main process we register for the EVENT_CHUNK_INGESTED event.
            # workers will publish the event via Redis after every chunk is flushed, dispatching happens in the
            # main process.
            Glug.get_instance().debug("registering chunk callback in main process")
            events.subscribe(
                self,
                [GulpCoreEvent.CHUNK_POST_INGEST, GulpCoreEvent.CHUNK_PRE_INGEST],
            )

    def desc(self) -> str:
        return "Registers a post-processing chunk ingestion callback."

    async def on_event(self, ev: GulpEvent) -> dict | None:
        if ev.event == GulpCoreEvent.CHUNK_POST_INGEST:
            data = ev.data or {}
            chunk_len = len(data.get("chunk", []))
            operation_id = data.get("operation_id")
            user_id = data.get("user_id")
            req_id = data.get("req_id")
            plugin = data.get("plugin")
            Glug.get_instance().debug(
                f"extension plugin={self.name} received EVENT_CHUNK_POST_INGEST: chunk_len={chunk_len}, "
                f"operation_id={operation_id}, user_id={user_id}, req_id={req_id}, "
                f"plugin={plugin}"
            )
            return None
        elif ev.event == GulpCoreEvent.CHUNK_PRE_INGEST:
            # this is synchronous
            chunk = ev.data.get("chunk", [])
            return {**ev.data, "chunk": chunk}
            
        return None

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "example_chunk_callbacks"

    def version(self) -> str:
        return "1.0"
