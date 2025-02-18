from enum import Enum
from typing import Any, override

from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    chrome based browsers history plugin stacked over the SQLITE plugin

    ./test_scripts/ingest.py --plugin chrome_history_sqlite_stacked --path ~/Downloads/history.sqlite
    """

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return """chrome based browsers history sqlite stacked plugin"""

    def display_name(self) -> str:
        return "chrome_history_sqlite_stacked"

    @override
    def depends_on(self) -> list[str]:
        return ["sqlite"]

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

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> dict:
        event_code = record["event.code"]
        if event_code == "download_end":
            # download end event
            end_time = record["gulp.timestamp"]
            start_time = int(record["download.start_time"])
            if end_time and start_time:
                # calculate download duration
                record["event.duration"] = end_time - start_time

        return record

    async def ingest_file(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        file_path: str,
        original_file_path: str = None,
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:

        # set as stacked
        try:
            lower = await self.setup_stacked_plugin("sqlite")
        except Exception as ex:
            await self._source_failed(ex)
            return GulpRequestStatus.FAILED

        if not plugin_params:
            plugin_params = GulpPluginParameters()
        
        plugin_params.mapping_file = "chrome_history.json"
        plugin_params.custom_parameters["queries"] = {
            "visits": "SELECT * FROM {table} LEFT JOIN urls ON {table}.url = urls.id"
        }

        # call lower plugin, which in turn will call our record_to_gulp_document after its own processing
        res = await lower.ingest_file(
            sess=sess,
            stats=stats,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            file_path=file_path,
            original_file_path=original_file_path,
            plugin_params=plugin_params,
            flt=flt,
        )
        await lower.unload()
        return res
