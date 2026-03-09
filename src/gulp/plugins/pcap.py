"""
PCAP log file processor plugin for Gulp.

This module provides a plugin for processing PCAP (packet capture) files in both
PCAP and PCAPNG formats. It leverages Scapy for packet parsing and converts
packet data into a structured format suitable for ingestion into search engines.

The plugin supports customizable parameters and implements the necessary methods
for the Gulp ingestion pipeline, converting network packet data into searchable documents.
"""

import orjson
import os
import string
import pathlib
from typing import Any, override

import muty.crypto
import muty.dict
import muty.file
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

muty.os.check_and_install_package("scapy", ">=2.6.1,<3")
from scapy.all import EDecimal, FlagValue, Packet, PcapNgReader, PcapReader, load_layer

load_layer("tls")
from scapy.layers.tls.all import TLSClientHello, TLS_Ext_ServerName


class Plugin(GulpPluginBase):

    # Fields that may contain body/request text content
    BODY_FIELDS = {"load", "payload", "body", "data", "msg"}

    # Minimum ratio of printable ASCII characters to consider content as text
    TEXT_PRINTABLE_THRESHOLD = 0.75

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    @override
    def desc(self) -> str:
        return "PCAP log file processor."

    def display_name(self) -> str:
        return "pcap"

    def regex(self) -> str:
        """regex to identify this format"""
        return "^(\xd4\xc3\xb2\xa1|\xa1\xb2\xc3\xd4|\x4d\x3c\xb2\xa1|\xa1\xb2\x3c\x4d|^\x0a\x0d\x0d\x0a)"

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

    @staticmethod
    def _is_text_content(data: bytes) -> bool:
        """Check if a byte sequence is likely text content (>= 75% printable ASCII).

        Args:
            data (bytes): byte sequence to check

        Returns:
            bool: True if the content is likely text
        """
        if not data:
            return False
        printable_set = set(string.printable.encode("ascii"))
        printable_count = sum(1 for b in data if b in printable_set)
        return (printable_count / len(data)) >= Plugin.TEXT_PRINTABLE_THRESHOLD

    def _pkt_to_dict(self, p: Packet) -> dict:
        """
        Converts a scapy packet into a flat dictionary suitable for ECS mapping.
        Extracts text, and intercepts TLS Handshakes to extract the SNI (Server Name Indication).
        """
        d = {}

        for layer in p.layers():
            layer_instance = p.getlayer(layer)
            layer_name = layer.__name__

            field_names = [field.name for field in layer_instance.fields_desc]

            for field_name in field_names:
                try:
                    value = getattr(layer_instance, field_name)
                    if value is None or not value:
                        continue

                    flat_key = f"{layer_name}.{field_name}"
                    if isinstance(value, bytes):
                        decoded_val: str = ""
                        if self._is_text_content(value):
                            decoded_val = value.decode("utf-8", errors="replace")
                        else:
                            decoded_val = value.hex()

                        if field_name not in self.BODY_FIELDS:
                            d[flat_key] = decoded_val

                    elif hasattr(value, "flagrepr"):
                        d[flat_key] = value.flagrepr()
                    else:
                        if field_name not in self.BODY_FIELDS:
                            d[flat_key] = str(value)
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error in _pkt_to_dict, value: {value} "
                    )
                    raise

        # intercept tls client hello to extract the sni (domain name) in cleartext
        # this lookup is o(n) on the layers list, which is very fast
        if p.haslayer(TLSClientHello):
            try:
                tls_hello = p.getlayer(TLSClientHello)

                # the sni is stored as an extension within the client hello message
                if hasattr(tls_hello, "ext") and tls_hello.ext:
                    for ext in tls_hello.ext:
                        # verify if the extension is the server name indication
                        if isinstance(ext, TLS_Ext_ServerName) and hasattr(
                            ext, "servernames"
                        ):
                            if ext.servernames:
                                # extract and decode the raw bytes of the domain name
                                raw_sni = ext.servernames[0].servername
                                if isinstance(raw_sni, bytes):
                                    # use errors='ignore' to prevent ingestion crash on malformed certs
                                    sni_str = raw_sni.decode("utf-8", errors="ignore")
                                    # assign to a custom key that we will map in pcap.json
                                    d["TLS.sni"] = sni_str
                                    break
            except Exception as ex:
                # log the error but do not crash the pipeline on malformed tls packets
                MutyLogger.get_instance().debug(f"failed to parse tls sni: {ex}")

        # intercept dns traffic over udp/tcp to extract clean domain names and responses
        # dns is heavily used for data exfiltration and c2 beacons
        if p.haslayer("DNS"):
            try:
                dns_layer = p.getlayer("DNS")

                # gracefully skip evaluation if there are no questions (avoids scapy dissection bugs)
                # using getattr with a default of 0 prevents hasattr from triggering deep dissection
                if getattr(dns_layer, "qdcount", 0) > 0:
                    qd = getattr(dns_layer, "qd", None)
                    if qd is not None:
                        raw_qname = getattr(qd, "qname", None)
                        if isinstance(raw_qname, bytes):
                            clean_domain = raw_qname.decode(
                                "utf-8", errors="ignore"
                            ).rstrip(".")
                            d["DNS.qd"] = clean_domain

                # gracefully skip evaluation if there are no answers
                if getattr(dns_layer, "ancount", 0) > 0:
                    an = getattr(dns_layer, "an", None)
                    if an is not None:
                        raw_rdata = getattr(an, "rdata", None)
                        if isinstance(raw_rdata, bytes):
                            clean_answer = raw_rdata.decode("utf-8", errors="ignore")
                            d["DNS.an"] = clean_answer
                        elif isinstance(raw_rdata, list):
                            # handling lists (e.g., txt records with multiple strings)
                            d["DNS.an"] = ", ".join(
                                [
                                    b.decode("utf-8", errors="ignore")
                                    for b in raw_rdata
                                    if isinstance(b, bytes)
                                ]
                            )
                        elif raw_rdata is not None:
                            d["DNS.an"] = str(raw_rdata)

                # gracefully extract authority records (ns)
                if getattr(dns_layer, "nscount", 0) > 0:
                    ns = getattr(dns_layer, "ns", None)
                    if ns is not None:
                        # extract the authoritative domain name
                        raw_rrname = getattr(ns, "rrname", None)
                        if isinstance(raw_rrname, bytes):
                            clean_rrname = raw_rrname.decode(
                                "utf-8", errors="ignore"
                            ).rstrip(".")

                            # if it's a soa (start of authority) record, extract the primary nameserver
                            if hasattr(ns, "mname") and isinstance(ns.mname, bytes):
                                clean_mname = ns.mname.decode(
                                    "utf-8", errors="ignore"
                                ).rstrip(".")
                                d["DNS.ns"] = (
                                    f"SOA: {clean_rrname} (Primary NS: {clean_mname})"
                                )

                            # if it's a standard ns (name server) delegation record
                            elif hasattr(ns, "rdata") and isinstance(ns.rdata, bytes):
                                clean_rdata = ns.rdata.decode(
                                    "utf-8", errors="ignore"
                                ).rstrip(".")
                                d["DNS.ns"] = f"NS: {clean_rrname} -> {clean_rdata}"

                            # fallback to just the domain name if other fields are missing
                            else:
                                d["DNS.ns"] = clean_rrname
            except IndexError:
                # scapy's dns label decompression throws IndexError on truncated pcaps.
                # we silently ignore this as it's a known artifact of incomplete network captures.
                pass

            except Exception as ex:
                # prevent pipeline block on corrupted dns packets (often seen in dns amplification attacks)
                MutyLogger.get_instance().debug(
                    f"failed to parse dns layer: {ex}\n dns_layer: {dns_layer}"
                )

        return d

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        # process record
        evt_json = self._pkt_to_dict(record)

        # use the last layer as gradient (all TCP packets are gonna be the same color, etc)
        d: dict = {}
        event_code = record.lastlayer()
        last_layer = event_code.name
        d["event.code"] = str(muty.crypto.hash_crc24(last_layer))

        # add top layer name to json
        evt_json["top_layer"] = (
            last_layer  # TODO: this sometimes is a Packet_metadata class instead of layer
        )

        # map fields through the mapping engine
        for k, v in evt_json.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        # normalize timestamp
        normalized: float = record.time.normalize(20)
        ns: str = str(muty.time.float_to_nanos_from_unix_epoch(float(normalized)))
        timestamp: str = muty.time.ensure_iso8601(ns)

        event_original = f"""network.protocol: {d.get("network.protocol","")}
        source.address: {d.get("source.address","")}
        source.port: {d.get("source.port","")}
        source.mac: {d.get("source.mac","")}
        destination.address: {d.get("destination.address","")}
        destination.port: {d.get("destination.port","")}
        destination.mac: {d.get("destination.mac","")}"""
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=event_original,
            event_sequence=record_idx,
            timestamp=timestamp,
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
        # set store_file and call super
        plugin_params = self._ensure_plugin_params(plugin_params)
        # set store_file and call super
        plugin_params.store_file = True
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

        file_format = self._plugin_params.custom_parameters.get("format")
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

        doc_idx = 0
        for pkt in parser:
            await self.process_record(pkt, doc_idx, flt=flt)
            doc_idx += 1

        return stats.status
