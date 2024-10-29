import muty.os
import muty.string
import muty.xml
import muty.time


import gulp.plugin as gulp_plugin
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.defs import GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams


class Plugin(PluginBase):
    """
    teamviewer connections_incoming.txt plugin stacked over the REGEX plugin
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return """teamviewer connections_incoming.txt regex stacked plugin"""

    def display_name(self) -> str:
        return "teamviewer_regex_stacked"

    def version(self) -> str:
        return "1.0"

    async def record_to_gulp_document(
        self,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginParams = None,
        **kwargs,
    ) -> list[GulpDocument]:

        for r in record:
            event: GulpDocument = r
            fme: list[GulpMappingField] = []
            for k, v in event.extra.items():
                e = self._map_source_key(
                    plugin_params,
                    custom_mapping,
                    k,
                    v,
                    index_type_mapping=index_type_mapping,
                    **kwargs,
                )
                fme.extend(e)

            # we receive nanos from the regex plugin
            # event.timestamp_nsec = event.timestamp
            # event.timestamp = muty.time.nanos_to_millis(event.timestamp)
            endtime = muty.time.string_to_epoch_nsec(
                event.extra.get("gulp.unmapped.endtime", None)
            )

            event.duration_nsec = endtime - event.timestamp_nsec

        return record

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

        if plugin_params is None:
            plugin_params = GulpPluginParams()
        fs = TmpIngestStats(source)

        # initialize mapping
        try:
            await self.initialize()(index, source, skip_mapping=True)
            mod = gulp_plugin.load_plugin("regex", **kwargs)
        except Exception as ex:
            fs = self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        # parse connections_incoming.txt
        # TODO: instead get regexes form mapping file based on mapping_id
        plugin_params.extra = {}
        regex = r"\s+".join(
            [
                r"(?P<userid>[0-9]+)",
                r"(?P<username>[^\s]+)",
                r"(?P<timestamp>([0-9]+-[0-9]+-[0-9]+ [0-9]+\:[0-9]+\:[0-9]+))",
                r"(?P<endtime>([0-9]+-[0-9]+-[0-9]+ [0-9]+\:[0-9]+\:[0-9]+))",
                r"(?P<local_user>[^\s]+)",
                r"(?P<session_type>[^\s]+)",
                r"(?P<guid>{.*})",
            ]
        )

        plugin_params.record_to_gulp_document_fun.append(self.record_to_gulp_document)
        plugin_params.extra["regex"] = regex
        # plugin_params.extra["flags"] = re.MULTILINE

        return await mod.ingest(
            index,
            req_id,
            client_id,
            operation_id,
            context,
            source,
            ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )
