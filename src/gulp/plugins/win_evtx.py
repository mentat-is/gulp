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
from lxml import etree
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.opensearch.sigma import to_gulp_query_struct
from gulp.api.collab.stats import (
    GulpIngestionStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.query import (
    GulpQuery,
    GulpQuerySigmaParameters,
)
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import (
    GulpNameDescriptionEntry,
    GulpPluginParameters,
    GulpPluginSigmaSupport,
)
from muty.log import MutyLogger

# needs the following backends for sigma support (add others if needed)
muty.os.check_and_install_package("pysigma-backend-elasticsearch", ">=1.1.3,<2.0.0")
muty.os.check_and_install_package("pysigma-backend-opensearch", ">=1.0.3,<2.0.0")

from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.pipelines.elasticsearch.windows import ecs_windows, ecs_windows_old


class Plugin(GulpPluginBase):
    """
    windows evtx log file processor.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

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

    @override
    async def _record_to_gulp_document(
        self, record: any, record_idx: int, data: Any = None
    ) -> GulpDocument:

        event_original: str = record["data"]
        timestamp = record["timestamp"]
        data_elem = etree.fromstring(event_original.encode("utf-8"))
        e_tree: etree.ElementTree = etree.ElementTree(data_elem)

        d = {}
        for e in e_tree.iter():
            e.tag = muty.xml.strip_namespace(e.tag)
            # MutyLogger.get_instance().debug("found e_tag=%s, value=%s" % (e.tag, e.text))

            # map attrs and values
            if len(e.attrib) == 0:
                # no attribs, i.e. <Opcode>0</Opcode>
                if not e.text or not e.text.strip():
                    # none/empty text
                    # MutyLogger.get_instance().error('skipping e_tag=%s, value=%s' % (e.tag, e.text))
                    continue

                # MutyLogger.get_instance().warning('processing e.attrib=0: e_tag=%s, value=%s' % (e.tag, e.text))
                mapped = self._process_key(e.tag, e.text)
                d.update(mapped)
            else:
                # attribs, i.e. <TimeCreated SystemTime="2019-11-08T23:20:54.670500400Z" />
                for attr_k, attr_v in e.attrib.items():
                    if not attr_v or not attr_v.strip():
                        # MutyLogger.get_instance().error('skipping e_tag=%s, attr_k=%s, attr_v=%s' % (e.tag, attr_k, attr_v))
                        continue
                    if attr_k == "Name":
                        if e.text:
                            text = e.text.strip()
                            k = attr_v
                            v = text
                        else:
                            k = e.tag
                            v = attr_v
                        # MutyLogger.get_instance().warning('processing Name attrib: e_tag=%s, k=%s, v=%s' % (e.tag, k, v))
                    else:
                        k = "%s_%s" % (e.tag, attr_k)
                        v = attr_v
                        # MutyLogger.get_instance().warning('processing attrib: e_tag=%s, k=%s, v=%s' % (e.tag, k, v))
                    mapped = self._process_key(k, v)
                    d.update(mapped)

        # try to map event code to a more meaningful event category and type
        mapped = self._map_evt_code(d.get("event.code"))
        d.update(mapped)
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
        stats: GulpIngestionStats,
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
        )
        try:
            # initialize plugin
            if not plugin_params or plugin_params.is_empty():
                plugin_params = GulpPluginParameters(mapping_file="windows.json")
            await self._initialize(plugin_params)

            # init parser
            parser = PyEvtxParser(file_path)
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in parser.records():
                doc_idx += 1
                try:
                    await self.process_record(rr, doc_idx, flt)
                except RequestCanceledError as ex:
                    MutyLogger.get_instance().exception(ex)
                    break
                except SourceCanceledError as ex:
                    await self._source_failed(ex)
                    break

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()

    @override
    def sigma_support(self) -> list[GulpPluginSigmaSupport]:
        return [
            GulpPluginSigmaSupport(
                backends=[
                    GulpNameDescriptionEntry(
                        name="opensearch",
                        description="OpenSearch Lucene backend for pySigma",
                    )
                ],
                pipelines=[
                    GulpNameDescriptionEntry(
                        name="ecs_windows",
                        description="ECS Mapping for windows event logs ingested with Winlogbeat or Gulp.",
                    ),
                    GulpNameDescriptionEntry(
                        name="ecs_windows_old",
                        description="ECS Mapping for windows event logs ingested with Winlogbeat<=6.x",
                    ),
                ],
                output_formats=[
                    GulpNameDescriptionEntry(
                        name="dsl_lucene",
                        description="DSL with embedded Lucene queries.",
                    )
                ],
            ),
        ]

    @override
    def sigma_convert(
        self,
        sigma: str,
        s_options: GulpQuerySigmaParameters,
    ) -> list[GulpQuery]:

        # select pipeline, backend and output format to use
        backend, pipeline, output_format = self._check_sigma_support(s_options)
        if pipeline == "ecs_windows":
            pipeline = ecs_windows()
        else:
            pipeline = ecs_windows_old()

        backend = OpensearchLuceneBackend(processing_pipeline=pipeline)
        return to_gulp_query_struct(sigma, backend, output_format=output_format)
