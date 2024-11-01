import os
from typing import override

import muty.dict
import muty.file
import muty.jsend
import muty.log
import muty.string
import muty.time
import muty.xml
from evtx import PyEvtxParser
from lxml import etree
from sigma.pipelines.elasticsearch.windows import ecs_windows

from gulp.api.collab.stats import GulpIngestionStats, RequestAbortError, TmpIngestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.defs import GulpLogLevel, GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams
from gulp.utils import logger


class Plugin(PluginBase):
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
            dict: A dictionary with 'event.category' and 'event.type'
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

        return None

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
        # process record
        # logger().debug(record)
        evt_str: str = record["data"].encode()
        data_elem = None
        data_elem = etree.fromstring(evt_str)
        cat_tree = data_elem[0]

        # extra mapping from event code
        fme: list[GulpMappingField] = []
        entry = GulpMappingField(result={})

        evt_code = str(muty.xml.child_node_text(cat_tree, "EventID"))
        try:
            evt_channel = str(muty.xml.child_node_text(cat_tree, "Channel"))
        except:
            evt_channel = str(muty.xml.strip_namespace(cat_tree.tag))

        entry.result["winlog.channel"] = evt_channel
        try:
            evt_provider = str(muty.xml.child_attrib(cat_tree, "Provider", "Name"))
            entry.result["event.provider"] = evt_provider
        except:
            evt_provider = None

        d = self._map_evt_code(evt_code)
        if d is not None:
            entry.result.update(d)

        evt_id = muty.xml.child_node_text(cat_tree, "EventRecordID")
        original_log_level: str = muty.xml.child_node_text(cat_tree, "Level")
        gulp_log_level = self._normalize_loglevel(
            muty.xml.child_node_text(cat_tree, "Level")
        )

        # evtx timestamp is always UTC string, i.e.: '2021-11-19 16:53:03.836551 UTC'
        time_str = record["timestamp"]
        time_nanosec = muty.time.string_to_epoch_nsec(time_str)
        time_msec = muty.time.nanos_to_millis(time_nanosec)
        # logger().debug('%s -  %d' % (time_str, time_msec))

        #  raw event as text
        raw_text = str(record["data"])

        # append this entry
        fme.append(entry)

        e_tree: etree.ElementTree = etree.ElementTree(data_elem)

        # logger().debug("e_tree: %s" % (e_tree))

        for e in e_tree.iter():
            e.tag = muty.xml.strip_namespace(e.tag)
            # logger().debug("found e_tag=%s, value=%s" % (e.tag, e.text))

            # map attrs and values
            entries: list[GulpMappingField] = []
            if len(e.attrib) == 0:
                # no attribs, i.e. <Opcode>0</Opcode>
                if not e.text or not e.text.strip():
                    # none/empty text
                    # logger().error('skipping e_tag=%s, value=%s' % (e.tag, e.text))
                    continue
                # 1637340783836
                # logger().warning('processing e.attrib=0: e_tag=%s, value=%s' % (e.tag, e.text))
                entries = self._map_source_key(
                    plugin_params,
                    custom_mapping,
                    e.tag,
                    e.text,
                    index_type_mapping=index_type_mapping,
                    **kwargs,
                )
            else:
                # attribs, i.e. <TimeCreated SystemTime="2019-11-08T23:20:54.670500400Z" />
                for attr_k, attr_v in e.attrib.items():
                    if not attr_v or not attr_v.strip():
                        # logger().error('skipping e_tag=%s, attr_k=%s, attr_v=%s' % (e.tag, attr_k, attr_v))
                        continue
                    if attr_k == "Name":
                        k = attr_v
                        v = e.text
                        # logger().warning('processing Name attrib: e_tag=%s, k=%s, v=%s' % (e.tag, k, v))
                    else:
                        k = attr_k  # "%s.%s" % (e.tag, attr_k)
                        v = attr_v
                        # logger().warning('processing attrib: e_tag=%s, k=%s, v=%s' % (e.tag, k, v))
                    ee = self._map_source_key(
                        plugin_params,
                        custom_mapping,
                        k,
                        v,
                        index_type_mapping=index_type_mapping,
                        **kwargs,
                    )
                    entries.extend(ee)

            for entry in entries:
                fme.append(entry)

        # finally create documents
        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context,
            plugin=self.display_name(),
            client_id=client_id,
            raw_event=raw_text,
            original_id=evt_id,
            src_file=os.path.basename(source),
            timestamp=time_msec,
            timestamp_nsec=time_nanosec,
            event_code=evt_code,
            gulp_log_level=gulp_log_level,
            original_log_level=original_log_level,
        )
        return docs

    
    async def ingest(
        self,
        req_id: str,
        ws_id: str,
        user: str,
        index: str,
        operation: str,
        context: str,
        log_file_path: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:

        # initialize stats
        stats: GulpIngestionStats = GulpIngestionStats.create_or_get(req_id, user, operation=operation, context=context)
        try:
            # initialize plugin
            self.initialize(mapping_file='windows.json')

            # init parser
            parser = PyEvtxParser(log_file_path)
        except Exception as ex:
            await self._source_failed(stats, ex, ws_id, source=log_file_path)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in parser.records():
                doc_idx +=1
                try:
                    await self._process_record(stats, ws_id, index, rr, doc_idx, flt)
                except RequestAbortError as ex:
                    break
                except Exception as ex:
                    await self._record_failed(stats, ex, ws_id, source=log_file_path)

        except Exception as ex:
            await self._source_failed(stats, ex, ws_id, source=log_file_path)            
        finally:
            # in the end, flush buffer and wait for index refresh
            await self._flush_buffer(
                index, ws_id, stats, flt, wait_for_refresh=True
            )
        return stats.status
