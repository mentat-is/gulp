####################################################################################################
# TODO: needs full rework !
####################################################################################################


import json
from typing import override

import muty.crypto
import muty.time
import muty.xml
from muty.log import MutyLogger

from gulp.api.collab.stats import GulpIngestionStats, RequestCanceledError
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginAdditionalParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    ingests raw events

    this plugin is used to ingest raw events, without any transformation.

    the input for this plugin is a list of dictionaries, each with the following structure:

    {
        "metadata": {
            # mandatory, with a format supported by gulp
            "timestamp": "2021-01-01T00:00:00Z"
            # mandatory, the raw event as string
            "event_original": "raw event content",
            # optional, will be set to 0 if missing
            "event_code": "something"
        },
        "doc" {
            # the document as key/value pairs, will be ingested according to plugin_params.ignore_mapping:
            # if set, mapping will be ignored and fields in the resulting GulpDocuments will be ingested as is.
            # (default: False, mapping works as usual and unmapped fields will be prefixed with 'gulp.unmapped')
            "something": "value",
            "something_else": "value",
            "another_thing": 123,
        }
    }
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def display_name(self) -> str:
        return "raw"

    @override
    def desc(self) -> str:
        return "Raw events ingestion plugin"

    @override
    def additional_parameters(self) -> list[GulpPluginAdditionalParameter]:
        return [
            GulpPluginAdditionalParameter(
                name="ignore_mapping",
                type="bool",
                default_value=False,
                desc="if set, mapping will be ignored and fields in the resulting GulpDocuments will be ingested as is. (default: False, mapping works as usual and unmapped fields will be prefixed with 'gulp.unmapped')",
            )
        ]

    @override
    async def _record_to_gulp_document(
        self, record: any, record_idx: int
    ) -> GulpDocument:
        # get mandatory fields from metadata (metadata and the doc dictionary itself)
        metadata: dict = record["metadata"]
        doc: dict = record["doc"]
        ts: str = metadata["@timestamp"]
        original: str = metadata["event.original"]
        event_code: str = metadata.get("event.code", "0")

        mapping = self.selected_mapping()
        if mapping.model_extra.get("ignore_mapping", False):
            # ignore mapping
            d = doc
        else:
            d = {}
            for k, v in doc.items():
                mapped = self._process_key(k, v)
                d.update(mapped)

        # create a gulp document
        return GulpDocument(
            self,
            timestamp=ts,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=str(original),
            event_sequence=record_idx,
            event_code=event_code,
            **d,
        )

    @override
    async def ingest_raw(
        self,
        req_id: str,
        ws_id: str,
        user_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        chunk: list[dict],
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
    ) -> GulpRequestStatus:
        await super().ingest_raw(
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            chunk=chunk,
            plugin_params=plugin_params,
            flt=flt,
        )
        # stats must be created by the caller, get it
        stats: GulpIngestionStats = await GulpIngestionStats.get_by_id(id=req_id)
        try:
            # initialize plugin
            if not plugin_params:
                plugin_params = GulpPluginParameters()
            await self._initialize(plugin_params)
        except Exception as ex:
            await self._source_failed(stats, ex)
            await self._source_done(stats, flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in chunk:
                doc_idx += 1

                try:
                    await self.process_record(stats, rr, doc_idx, flt)
                except RequestCanceledError:
                    break
        except Exception as ex:
            await self._source_failed(stats, ex)
        finally:
            stats = await self._source_done(stats, flt)
            return stats.status
