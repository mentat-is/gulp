import json
import os
import pathlib
from typing import Any, override

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
from muty.log import MutyLogger

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from sqlalchemy.ext.asyncio import AsyncSession

muty.os.check_and_install_package("scapy", ">=2.6.1,<3")
from scapy.all import EDecimal, FlagValue, Packet, PcapNgReader, PcapReader
from scapy.packet import Raw

from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    pcap file processor.
    """

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return "PCAP log file processor."

    def display_name(self) -> str:
        return "pcap"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        # since we are using scapy PCapNgReader sets PcapReader as alternative if file isnt a pcapng
        # hence a safe default could be pcapng regardless of type
        return [
            GulpPluginCustomParameter(
                name="format",
                type="str",
                desc="pcap format (pcap or pcapng)",
                default_value=None,
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

            # MutyLogger.get_instance().debug(f"Dissecting layer: {layer_name}")
            # MutyLogger.get_instance().debug(f"Field names: {field_names}")

            fields = {}
            for field_name in field_names:
                try:
                    fields[field_name] = getattr(p.getlayer(layer_name), field_name)
                    # MutyLogger.get_instance().debug(f"Fields: {field_name} -> {getattr(layer, field_name)}")
                except Exception as ex:
                    # skip fields that cannot be accessed
                    # MutyLogger.get_instance().exception(ex)
                    # MutyLogger.get_instance().debug(f"Fields: {field_name} failed to access ({ex})")
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

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, data: Any = None
    ) -> GulpDocument:

        # process record
        # MutyLogger.get_instance().debug(record)
        evt_json = self._pkt_to_dict(record)
        # evt_str: str = record.show2(dump=True)
        # MutyLogger.get_instance().debug(evt_str)

        # use the last layer as gradient (all TCP packets are gonna be the same color, etc)
        d: dict = {}
        event_code = record.lastlayer()
        last_layer = event_code.name
        d["event.code"] = str(muty.crypto.hash_crc24(last_layer))

        # add top layer name to json
        evt_json["top_layer"] = (
            last_layer  # TODO: this sometimes is a Packet_metadata class instead of layer
        )

        # event_code = str(muty.crypto.hash_xxh64_int(last_layer))
        flattened = muty.json.flatten_json(evt_json)

        # map
        for k, v in flattened.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        # normalize timestamp
        normalized: float = record.time.normalize(20)
        ts: str = str(muty.time.float_to_nanos_from_unix_epoch(float(normalized)))
        d["@timestamp"] = ts

        # print(f"TEST IS {dir(event_code)}")
        # print(f"NAME: {type(event_code.name)} ")
        # #TODO: check if member_descriptor if so get value and/or place "unknown"
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=record.build().hex(),
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
            await self._initialize(plugin_params)

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        try:

            file_format = self._custom_params.get("format")
            if file_format is None:
                # attempt to get format from source name (TODO: do it by checking bytes header instead?)
                file_format = pathlib.Path(file_path).suffix.lower()[1:]

            # check if a valid input was received/inferred
            if file_format in ["cap", "pcap"]:
                file_format = "pcap"
            elif file_format in ["pcapng"]:
                file_format = "pcapng"
            else:
                # fallback to pcap
                file_format = "pcap"

            MutyLogger.get_instance().debug(
                "detected file format: %s for file %s" % (file_format, file_path)
            )

            MutyLogger.get_instance().debug("parsing file: %s" % (file_path))
            if file_format == "pcapng":
                MutyLogger.get_instance().debug(
                    "using PcapNgReader reader on file: %s" % (file_path)
                )
                parser = PcapNgReader(file_path)
            else:
                MutyLogger.get_instance().debug(
                    "using PcapReader reader on file: %s" % (file_path)
                )
                parser = PcapReader(file_path)
            # TODO: support other scapy file readers like ERF?
        except Exception as ex:
            # cannot parse this file at all
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for pkt in parser:
                try:
                    await self.process_record(pkt, doc_idx, flt=flt)
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                    break
                doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()
