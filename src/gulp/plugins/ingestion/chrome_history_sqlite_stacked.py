from enum import Enum

from gulp import plugin as gulp_plugin
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import GulpMapping
from gulp.defs import GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams


class Plugin(PluginBase):
    """
    chrome based browsers history plugin stacked over the SQLITE plugin
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return """chrome based browsers history sqlite stacked plugin"""

    def display_name(self) -> str:
        return "chrome_history_sqlite_stacked"

    def version(self) -> str:
        return "1.0"

    class ChromeHistoryTable(Enum):
        cluster_keywords = 0
        cluster_visit_duplicates = 1
        clusters = 2
        clusters_and_visits = 3
        content_annotations = 4
        context_annotations = 5
        downloads = 6
        downloads_slices = 7
        downloads_url_chains = 8
        history_sync_metadata = 9
        keyword_search_terms = 10
        meta = 11
        segment_usage = 12
        segments = 13
        sqlite_sequence = 14
        urls = 15
        visit_source = 16
        visited_links = 17
        visits = 18

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
            # if we are handling a download, we can calculate event.duration with start_time and end_time
            if "download_end_time" in event.extra.keys():
                end_time = event.extra.get("download_end_time", 0)
                start_time = event.extra.get("download_start_time", 0)

                if start_time > 0 and end_time > 0:
                    event.duration_nsec = end_time - start_time

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
            mod = gulp_plugin.load_plugin("sqlite", **kwargs)
        except Exception as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        plugin_params.record_to_gulp_document_fun.append(self.record_to_gulp_document)
        plugin_params.mapping_file = "chrome_history.json"
        plugin_params.extra = {
            "queries": {
                "visits": "SELECT * FROM {table} LEFT JOIN urls ON {table}.url = urls.id"
            }
        }
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
