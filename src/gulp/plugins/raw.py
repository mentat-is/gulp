####################################################################################################
# TODO: needs full rework !
####################################################################################################


import json
import muty.crypto
import muty.time
import muty.xml
from muty.log import MutyLogger
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.plugin import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_internal import GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    ingests raw events
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Raw events ingestion plugin."

    def display_name(self) -> str:
        return "raw"

    def version(self) -> str:
        return "1.0"

    async def ingest_file(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context_id: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest_file(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context_id=context_id,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        fs = TmpIngestStats("raw")
        await self._initialize()(index, source, skip_mapping=True)

        events: list[dict] = source
        for evt in events:
            # MutyLogger.get_logger().debug("processing event: %s" % json.dumps(evt, indent=2))
            # ensure these are set
            if "@timestamp" not in evt:
                # MutyLogger.get_logger().warning("no @timestamp, skipping: %s" % json.dumps(evt, indent=2))
                fs = self._record_failed(fs, evt, source, "no @timestamp, skipping")
                continue

            # operation_id, client_id, context should already be set inside the event.
            # only if not, we set them here.

            if "event.original" not in evt:
                ori = str(evt)
                evt["event.original"] = ori
            if "gulp.operation_id.id" not in evt:
                evt["gulp.operation_id.id"] = operation_id
            if "agent.id" not in evt:
                evt["agent.id"] = client_id
            if "gulp.context_id" not in evt:
                evt["gulp.context_id"] = context_id
            if "agent.type" not in evt:
                evt["agent.type"] = self.display_name()
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
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs=fs, flt=flt
        )
