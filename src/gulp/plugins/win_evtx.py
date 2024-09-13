import os

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

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping
from gulp.defs import GulpLogLevel, GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams


class Plugin(PluginBase):
    """
    windows evtx log file processor.
    """

    def _normalize_loglevel(self, l: int | str) -> GulpLogLevel:
        ll = int(l)
        if ll == 0:
            return GulpLogLevel.ALWAYS
        if ll == 1:
            return GulpLogLevel.CRITICAL
        if ll == 2:
            return GulpLogLevel.ERROR
        if ll == 3:
            return GulpLogLevel.WARNING
        if ll == 4:
            return GulpLogLevel.INFO
        if ll == 5:
            return GulpLogLevel.VERBOSE
        if ll > 5:
            # maps directly to GulpLogLevel.CUSTOM_n
            try:
                return GulpLogLevel(ll)
            except:
                return GulpLogLevel.UNEXPECTED

        # wtf ?!
        return GulpLogLevel.VERBOSE

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Windows EVTX log file processor."

    def name(self) -> str:
        return "win_evtx"

    def version(self) -> str:
        return "1.0"

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
        # self.logger().debug(record)
        evt_str: str = record["data"].encode()

        data_elem = None
        data_elem = etree.fromstring(evt_str)

        # check if we should ignore mapping
        ignore_mapping = False
        if plugin_params is not None and plugin_params.ignore_mapping_ingest:
            ignore_mapping = True

        cat_tree = data_elem[0]

        # extra mapping from event code
        fme: list[FieldMappingEntry] = []
        entry = FieldMappingEntry(result={})

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
        # Plugin.logger().debug('%s -  %d' % (time_str, time_msec))

        #  raw event as text
        raw_text = str(record["data"])

        if ignore_mapping:
            # persist these into the event
            entry.result["EventID"] = evt_code
            entry.result["Level"] = original_log_level
            entry.result["Provider"] = evt_provider
            entry.result["EventRecordID"] = evt_id
            entry.result["Channel"] = evt_channel

        # append this entry
        fme.append(entry)

        e_tree: etree.ElementTree = etree.ElementTree(data_elem)

        # self.logger().debug("e_tree: %s" % (e_tree))

        for e in e_tree.iter():
            e.tag = muty.xml.strip_namespace(e.tag)

            # these are already processed
            if e.tag in ["EventID", "EventRecordID", "Level", "Provider"]:
                continue

            # self.logger().debug(
            #     "found e_tag=%s, value=%s" % (e.tag, e.text)
            # )

            # Check XML tag name and set extra mapping
            entries = self._map_source_key(
                plugin_params,
                custom_mapping,
                e.tag,
                e.text,
                index_type_mapping=index_type_mapping,
                **kwargs,
            )
            for entry in entries:
                fme.append(entry)

                # add attributes as more extra data
                for attr_k, attr_v in e.attrib.items():
                    kk = "gulp.winevtx.xml.attr.%s" % (attr_k)
                    inner_entries = self._map_source_key(
                        plugin_params,
                        custom_mapping,
                        kk,
                        attr_v,
                        index_type_mapping=index_type_mapping,
                        **kwargs,
                    )
                    for entry in inner_entries:
                        fme.append(entry)

            # Check attributes and set extra mapping
            for attr_k, attr_v in e.attrib.items():
                # Plugin.logger().info("name: %s value: %s" % (attr_k, attr_v))
                # self.logger().debug("processing attr_v=%s, value=%s" % (attr_v, e.text))
                entries = self._map_source_key(
                    plugin_params,
                    custom_mapping,
                    attr_v,
                    e.text,
                    index_type_mapping=index_type_mapping,
                    **kwargs,
                )
                for entry in entries:
                    fme.append(entry)

                if e.text is None:
                    # no more processing
                    continue

                value = e.text.strip()
                if attr_v is not None:
                    # prefer attr_v
                    value = attr_v.strip()

                entries = self._map_source_key(
                    plugin_params,
                    custom_mapping,
                    attr_k,
                    value,
                    index_type_mapping=index_type_mapping,
                    **kwargs,
                )
                for entry in entries:
                    fme.append(entry)


        # finally create documents
        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context,
            plugin=self.name(),
            client_id=client_id,
            raw_event=raw_text,
            original_id=evt_id,
            src_file=os.path.basename(source),
            timestamp=time_msec,
            timestamp_nsec=time_nanosec,
            event_code=evt_code,
            gulp_log_level=gulp_log_level,
            original_log_level=original_log_level
        )
        return docs

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

        parser = None

        fs = TmpIngestStats(source)

        # initialize mapping
        try:
            index_type_mapping, custom_mapping = await self.ingest_plugin_initialize(
                index,
                source=source,
                pipeline=ecs_windows(),
                mapping_file="windows.json",
                plugin_params=plugin_params,
            )
            # Plugin.logger().debug("win_mappings: %s" % (win_mapping))
        except Exception as ex:
            fs=self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)

        # init parser
        try:
            parser = PyEvtxParser(source)
        except Exception as ex:
            # cannot parse this file at all
            fs=self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)

        ev_idx = 0

        try:
            for rr in parser.records():
                # process (ingest + update stats)
                try:
                    fs, must_break = await self._process_record(
                        index,
                        rr,
                        ev_idx,
                        self.record_to_gulp_document,
                        ws_id,
                        req_id,
                        operation_id,
                        client_id,
                        context,
                        source,
                        fs,
                        custom_mapping=custom_mapping,
                        index_type_mapping=index_type_mapping,
                        plugin_params=plugin_params,
                        flt=flt,
                        **kwargs,
                    )

                    ev_idx += 1
                    if must_break:
                        break

                except Exception as ex:
                    fs=self._record_failed(fs, rr, source, ex)

        except Exception as ex:
            fs=self._parser_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)
