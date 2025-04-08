"""
Windows Event Log (EVTX) Processing Plugin for Gulp

This module provides a plugin for processing Windows Event Log (EVTX) files. It parses EVTX files,
maps event codes to appropriate categories and types, and converts the events into a structured format
suitable for ingestion and analysis.

The plugin supports:
- Reading and parsing EVTX files
- Mapping Windows event codes to standardized event categories
- Converting Sigma rules to Gulp query format
- Extracting relevant information from complex event structures
- Integrating with Gulp's ingestion system
"""
import json
import os
from typing import Any, override
import muty.dict
import muty.file
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from evtx import PyEvtxParser
from muty.log import MutyLogger
from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.pipelines.elasticsearch.windows import ecs_windows
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.query import GulpQuery
from gulp.api.opensearch.sigma import to_gulp_query_struct
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpMappingParameters, GulpPluginParameters

# needs the following backends for sigma support (add others if needed)
muty.os.check_and_install_package("pysigma-backend-elasticsearch", ">=1.1.3,<2.0.0")
muty.os.check_and_install_package("pysigma-backend-opensearch", ">=1.0.3,<2.0.0")


class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    def display_name(self) -> str:
        return "win_evtx"

    @override
    def desc(self) -> str:
        return "Windows EVTX log file processor."

    def _map_evt_code(self, ev_code: str) -> dict:
        """
        better map an event code to fields

        Args:
            ev_code (str): The event code to be converted.
        Returns:
            dict: A dictionary with 'event.category' and 'event.type', or an empty dictionary.
        """
        codes = {
            "100": {"event.category": ["package"], "event.type": ["start"]},
            "106": {"event.category": ["package"], "event.type": ["install"]},
            "140": {"event.category": ["package"], "event.type": ["change"]},
            "141": {"event.category": ["package"], "event.type": ["delete"]},
            "1006": {"event.category": ["host"], "event.type": ["change"]},
            "4624": {  # eventid
                "event.category": ["authentication"],
                "event.type": ["start"],
            },
            "4672": {
                "event.category": ["authentication"],
            },
            "4648": {
                "event.category": ["authentication"],
            },
            "4798": {"event.category": ["iam"]},
            "4799": {"event.category": ["iam"]},
            "5379": {"event.category": ["iam"], "event.type": ["access"]},
            "5857": {"event.category": ["process"], "event.type": ["access"]},
            "5858": {"event.category": ["process"], "event.type": ["error"]},
            "5859": {"event.category": ["process"], "event.type": ["change"]},
            "5860": {"event.category": ["process"], "event.type": ["change"]},
            "5861": {"event.category": ["process"], "event.type": ["change"]},
            "7036": {
                "event.category": ["package"],
                "event.type": ["change"],
            },
            "7040": {
                "event.category": ["package"],
                "event.type": ["change"],
            },
            "7045": {
                "event.category": ["package"],
                "event.type": ["install"],
            },
            "13002": {"event.type": ["change"]},
        }
        if ev_code in codes:
            return codes[ev_code]

        return {}

    def _process_leaf(self, path: str, value: Any, result: dict):
        if not value or path.endswith("xmlns"):
            # skip these
            return
        if path.endswith("_Name"):
            # remove _Name from path, key name will be the previous segment (assuming "allow_prefixed" is set)
            path = path.replace("_Name", "")
        elif path.endswith("_#text"):
            # remove #text from path, key name will be the previous segment (assuming "allow_prefixed" is set)
            path = path.replace("_#text", "")
        elif "_Execution_" in path:
            # remove Execution from path, key name will be the previous segment (assuming "allow_prefixed" is set)
            path = path.replace("_Execution_", "_")

        if path.endswith("#attributes"):
            # skip
            return

        # map the key
        mapped = self._process_key(path, value)
        result.update(mapped)

    def _parse_dict(self, data: dict, path_segments: list = None) -> dict:
        """
        recursively parse dictionary and call process_leaf() for each leaf node

        Args:
            data: Dictionary to parse
            path_segments: List of path segments (used in recursion)
        Returns:
            dict
        """
        result = {}
        if path_segments is None:
            path_segments = []

        for key, value in data.items():
            current_path = path_segments + [key]

            if value is None:
                continue

            elif isinstance(value, dict):
                if value:
                    nested_results = self._parse_dict(value, current_path)
                    result.update(nested_results)

            elif isinstance(value, list):
                if value:
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            if item:
                                nested_results = self._parse_dict(
                                    item, current_path + [str(i)]
                                )
                                result.update(nested_results)
                        else:
                            if item not in (None, ""):
                                self._process_leaf(
                                    "_".join(current_path + [str(i)]), item, result
                                )
            else:
                if value != "":
                    self._process_leaf("_".join(current_path), value, result)

        return result

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        event_original: str = record["data"]
        timestamp = record["timestamp"]

        # parse record
        js_record = json.loads(event_original)
        d = self._parse_dict(js_record)

        # try to map event code to a more meaningful event category and type
        mapped = self._map_evt_code(d.get("event.code"))
        d.update(mapped)

        if d.get("event.code") == "0":
            MutyLogger.get_instance().debug(json.dumps(d, indent=2))
            muty.os.exit_now()

        return GulpDocument(
            self,
            timestamp=timestamp,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
        )

    @override
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
        **kwargs
  ) -> GulpRequestStatus:
        try:
            if not plugin_params or plugin_params.mapping_parameters.is_empty():
                plugin_params = GulpPluginParameters(
                    mapping_parameters=GulpMappingParameters(mapping_file="windows.json"),
                )
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

            # init parser
            parser = PyEvtxParser(file_path)
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in parser.records_json():
                try:
                    await self.process_record(rr, doc_idx, flt=flt)
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                    break
                except PreviewDone:
                    # preview done, stop processing
                    break
                doc_idx += 1
        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
