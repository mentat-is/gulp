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

import orjson
import os
from typing import Any, override
import muty.os
from evtx import PyEvtxParser
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpMappingParameters, GulpPluginParameters


class Plugin(GulpPluginBase):
    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def display_name(self) -> str:
        return "win_evtx"

    def regex(self) -> str:
        """regex to identify this format"""
        return "^\x45\x6c\x66\x46\x69\x6c\x65\x00"

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

    async def _process_leaf(
        self, path: str, value: Any, result: dict, **kwargs
    ) -> None:
        if not value or path.endswith("xmlns"):
            # skip these
            return

        if path.endswith(",,#attributes,,Name"):
            # this is a special case where the source key becomes the ,, separated value before _#attributes in the string
            # i.e. this happens for Provider,,#attributes,,Name: we want "Provider" to be the source key, not Name
            key = path.split(",,#attributes,,Name")[-2].split(",,")[-1]
        elif path.endswith(",,#text"):
            # further special case for #text, similar to above
            # i.e. this happens for Event,,System,,Execution,,#text: we want "Execution" to be the source key, not #text
            key = path.split(",,#text")[-2].split(",,")[-1]
        else:
            # the source key is the last part of the string (default)
            # i.e. Event,,System,,Execution,,#attributes,,ProcessID
            key = path.rsplit(",,", 1)[-1]

        # map the key
        mapped = await self._process_key(key, value, result, **kwargs)
        result.update(mapped)

    async def _parse_dict(self, data: dict, **kwargs) -> dict:
        """
        parses dictionary and call process_leaf() for each leaf node

        Args:
            data: Dictionary to parse
            **kwargs: Additional keyword arguments for processing
        Returns:
            dict
        """
        result = {}

        # stack for iterative processing: (value, path_segments)
        stack = [(data, [])]

        while stack:
            current_value, current_path_segments = stack.pop()

            if isinstance(current_value, dict):
                for key, value in current_value.items():
                    if value is not None:
                        stack.append((value, current_path_segments + [key]))
            elif isinstance(current_value, list):
                for i, item in enumerate(current_value):
                    if item is not None:
                        stack.append((item, current_path_segments + [str(i)]))
            else:
                # it's a leaf node
                if current_value != "":
                    await self._process_leaf(
                        ",,".join(current_path_segments),
                        current_value,
                        result,
                        **kwargs,
                    )
        return result

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        event_original: str = record["data"]

        # parse record
        js_record = orjson.loads(event_original)
        d = await self._parse_dict(js_record, **kwargs)

        # try to map event code to a more meaningful event category and type
        mapped = self._map_evt_code(d.get("event.code"))
        d.update(mapped)
        d["@timestamp"] = record["timestamp"]

        # if d.get("event.code") == "0":
        #     MutyLogger.get_instance().debug(orjson.dumps(d, option=orjson.OPT_INDENT_2).decode())

        return GulpDocument(
            self,
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
        **kwargs,
    ) -> GulpRequestStatus:
        try:
            plugin_params = self._ensure_plugin_params(
                plugin_params, mapping_file="windows.json"
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
            return await self._source_done(flt, ex)

        doc_idx = 0
        try:
            for rr in parser.records_json():
                try:
                    await self.process_record(rr, doc_idx, flt=flt)
                except (RequestCanceledError, SourceCanceledError) as ex:
                    raise
                except PreviewDone:
                    # preview done, stop processing
                    break
                doc_idx += 1
            return await self._source_done(flt)
        except Exception as ex:
            return await self._source_done(flt, ex)
