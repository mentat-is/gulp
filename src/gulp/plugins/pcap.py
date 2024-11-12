import json
import os
import pathlib

import muty.crypto
import muty.dict
import muty.file
import muty.jsend
import muty.json
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.utils import GulpLogger

try:
    from scapy.all import EDecimal, FlagValue, Packet, PcapNgReader, PcapReader
    from scapy.packet import Raw
except Exception:
    muty.os.install_package("scapy")
    from scapy.all import PcapReader, PcapNgReader, Packet, FlagValue, EDecimal
    from scapy.packet import Raw

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.defs import GulpLogLevel, GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_internal import GulpPluginSpecificParam, GulpPluginGenericParams


class Plugin(GulpPluginBase):
    """
    windows evtx log file processor.
    """

    def _normalize_loglevel(self, l: int | str) -> GulpLogLevel:
        # TODO: if we really wanna be useful we should convert known-protocol error codes to loglevels here
        # or maybe just the lastlayer

        return GulpLogLevel.VERBOSE

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "PCAP log file processor."

    def display_name(self) -> str:
        return "pcap"

    def version(self) -> str:
        return "1.0"

    def additional_parameters(self) -> list[GulpPluginSpecificParam]:
        # since we are using scapy PCapNgReader sets PcapReader as alternative if file isnt a pcapng
        # hence a safe default could be pcapng regardless of type
        return [
            GulpPluginSpecificParam(
                "format", "str", "pcap format (pcap or pcapng)", default_value=None
            )
        ]

    def _pkt_to_dict(self, p: Packet) -> dict:
        """Transform a packet to a dict

        Args:
            p (Packet): packet to transform

        Returns:
            dict: json serializable dict
        """
        d = {}

        for layer in p.layers():
            layer_name = layer.__name__
            d[layer_name] = {}

            # get field names and map attributes
            field_names = [field.name for field in p.getlayer(layer_name).fields_desc]

            # GulpLogger.get_instance().debug(f"Dissecting layer: {layer_name}")
            # GulpLogger.get_instance().debug(f"Field names: {field_names}")

            fields = {}
            for field_name in field_names:
                try:
                    fields[field_name] = getattr(p.getlayer(layer_name), field_name)
                    # GulpLogger.get_instance().debug(f"Fields: {field_name} -> {getattr(layer, field_name)}")
                except Exception as ex:
                    # skip fields that cannot be accessed
                    # GulpLogger.get_instance().exception(ex)
                    # GulpLogger.get_instance().debug(f"Fields: {field_name} failed to access ({ex})")
                    pass

            # make sure we have a valid json serializable dict
            for field, value in fields.items():
                if value is None:
                    # no need to map a None value
                    continue

                if isinstance(value, bytes):
                    # print(field, value, "bytes found, hexing")
                    fields[field] = value.hex()
                elif isinstance(value, EDecimal):
                    # print(field, value, "edecimal found, normalizing")
                    fields[field] = float(value.normalize(20))
                elif isinstance(value, FlagValue):
                    fields[field] = value.flagrepr()
                else:
                    # fall back to str
                    fields[field] = str(value)

            d[layer_name].update(fields)

        # if this fails it is most likely a TypeError because of non JSON serializable type
        json.dumps(d)

        return d

    async def _record_to_gulp_document(
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
        plugin_params: GulpPluginGenericParams = None,
        **kwargs,
    ) -> list[GulpDocument]:
        # process record
        # GulpLogger.get_instance().debug(record)
        evt_json = self._pkt_to_dict(record)
        # evt_str: str = record.show2(dump=True)
        # GulpLogger.get_instance().debug(evt_str)

        # use the last layer as gradient (all TCP packets are gonna be the same color, etc)

        event_code = record.lastlayer()
        last_layer = event_code.name
        if isinstance(event_code, Raw) and (len(record.layers()) > 1):
            event_code = record.layers()[-2]
        else:
            event_code = record.lastlayer()

        # add top layer name to json
        evt_json["top_layer"] = (
            last_layer  # TODO: this sometimes is a Packet_metadata class instead of layer
        )

        event_code = str(muty.crypto.hash_crc24(last_layer))

        flattened = muty.json.flatten_json(evt_json)
        fme: list[GulpMappingField] = []
        for k, v in flattened.items():
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                v,
                index_type_mapping=index_type_mapping,
                **kwargs,
            )
            if e is not None:
                fme.extend(e)

        # normalize time, convert to millis and keep nanos in timestamp_nsec
        normalized = record.time.normalize(20)
        t_ns = muty.time.float_to_epoch_nsec(float(normalized))
        t = muty.time.nanos_to_millis(t_ns)

        #  raw event as text
        raw = record.build().hex()

        # print(f"TEST IS {dir(event_code)}")
        # print(f"NAME: {type(event_code.name)} ") #TODO: check if member_descriptor if so get value and/or place "unknown"

        docs = self._build_gulpdocuments(
            fme=fme,
            idx=record_idx,
            timestamp=t,
            timestamp_nsec=t_ns,
            operation_id=operation_id,
            context=context,
            plugin=self.display_name(),
            client_id=client_id,
            raw_event=raw,
            original_id=str(record_idx),
            src_file=os.path.basename(source),
            event_code=event_code,
            gulp_log_level=self._normalize_loglevel(0),
        )

        return docs

    async def ingest_file(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginGenericParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest_file(
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
            index_type_mapping, custom_mapping = await self._initialize()(
                index,
                source=source,
                # mapping_file="pcap.json",
                plugin_params=plugin_params,
            )
        except Exception as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        file_format = plugin_params.extra.get("format", None)
        if file_format is None:
            # attempt to get format from source name (TODO: do it by checking bytes header instead?)
            file_format = pathlib.Path(source).suffix.lower()[1:]

        # check if a valid input was received/inferred
        if file_format in ["cap", "pcap"]:
            file_format = "pcap"
        elif file_format in ["pcapng"]:
            file_format = "pcapng"
        else:
            # fallback to pcap
            file_format = "pcap"

        GulpLogger.get_instance().debug("detected file format: %s for file %s" % (file_format, source))

        try:
            GulpLogger.get_instance().debug("parsing file: %s" % source)
            if file_format == "pcapng":
                GulpLogger.get_instance().debug("using PcapNgReader reader on file: %s" % (source))
                parser = PcapNgReader(source)
            else:
                GulpLogger.get_instance().debug("using PcapReader reader on file: %s" % (source))
                parser = PcapReader(source)
            # TODO: support other scapy file readers like ERF?
        except Exception as ex:
            # cannot parse this file at all
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        ev_idx = 0

        try:
            for pkt in parser:
                try:
                    fs, must_break = await self.process_record(
                        index,
                        pkt,
                        ev_idx,
                        self._record_to_gulp_document,
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
                    fs = self._record_failed(fs, pkt, source, ex)

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs=fs, flt=flt
        )
