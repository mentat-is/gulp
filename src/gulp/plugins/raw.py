####################################################################################################
# TODO: needs full rework !
####################################################################################################


import muty.crypto
import muty.time
import muty.xml

import gulp.workers
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats
from gulp.api.elastic.structs import GulpIngestionFilter
from gulp.defs import GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams


class Plugin(PluginBase):
    """
    ingests raw events
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Raw events ingestion plugin."

    def name(self) -> str:
        return "raw"

    def version(self) -> str:
        return "1.0"

    async def ingest(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        fs = TmpIngestStats("raw")
        await self.ingest_plugin_initialize(
            index, source, skip_mapping=True)
        
        events: list[dict] = source
        for evt in events:
            # ensure these are set
            if "@timestamp" not in evt:
                self.logger().warning("no @timestamp, skipping!")
                fs = self._record_failed(fs, source, ex)
                continue

            if "event.original" not in evt:
                ori = str(evt)
                evt["event.original"] = ori
            if "operation_id" not in evt:
                evt["operation_id"] = operation_id
            if "agent.id" not in evt:
                evt["agent.id"] = client_id
            if "gulp.context" not in evt:
                evt["gulp.context"] = context
            if "agent.type" not in evt:
                evt["agent.type"] = self.name()
            if "event.hash" not in evt:
                # set event hash in the end
                evt["event.hash"] = muty.crypto.hash_blake2b(str(evt))

            fs = fs.update(processed=1)
            try:
                # bufferize, we will flush in the end
                fs = await self._ingest_record(
                    index,
                    evt,
                    fs,
                    ws_id,
                    req_id,
                    flt,
                )
            except Exception as ex:
                fs = self._record_failed(fs, evt, source, ex)

        # done
        return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)
