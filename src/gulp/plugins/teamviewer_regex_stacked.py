"""
TeamViewer connections_incoming.txt regex stacked plugin for GULP.

This plugin processes TeamViewer connection logs by stacking over the base `regex` plugin.
It extracts structured data from TeamViewer's connections_incoming.txt files, including
user information, connection times, session details, and GUIDs.

The plugin calculates connection durations and sets appropriate event metadata.
"""

from typing import override

import muty.os
import muty.string
import muty.time
import muty.xml
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpMappingParameters, GulpPluginParameters


class Plugin(GulpPluginBase):
    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    @override
    def desc(self) -> str:
        return """teamviewer connections_incoming.txt regex stacked plugin"""

    def display_name(self) -> str:
        return "teamviewer_regex_stacked"

    @override
    def depends_on(self) -> list[str]:
        return ["regex"]

    def regex(self) -> str:
        """regex to identify this format"""
        return None

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> dict:
        end_time = record.pop("gulp.unmapped.endtime", 0)
        if end_time:
            # set connection.end_time and event.duration
            end_time = muty.time.string_to_nanos_from_unix_epoch(end_time)
            start_time = record["gulp.timestamp"]
            record["connection.end_time"] = end_time
            record["event.duration"] = end_time - start_time

        record["agent.type"] = self.display_name()  # override agent.type
        record["event.code"] = "teamviewer_connection"
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
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest_file(
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
            **kwargs,
        )

        mappings = self.selected_mapping()
        if not "timestamp" in mappings.fields:
            mappings.fields["timestamp"] = GulpMappingField(ecs="@timestamp")

        # set as stacked
        lower = await self.setup_stacked_plugin("regex")

        # TODO: instead get regexes form mapping file based on mapping_id
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
        plugin_params.custom_parameters["regex"] = regex

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
