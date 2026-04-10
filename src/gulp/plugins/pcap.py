"""
PCAP log file processor plugin for Gulp.

This module provides a plugin for processing PCAP (packet capture) files in both
PCAP and PCAPNG formats. It leverages Scapy for packet parsing and converts
packet data into a structured format suitable for ingestion into search engines.

The plugin supports customizable parameters and implements the necessary methods
for the Gulp ingestion pipeline, converting network packet data into searchable documents.
"""

import os
import pathlib
import re
import string
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
from scapy.all import (
    BOOTP,
    DHCP,
    EDecimal,
    FlagValue,
    ICMP,
    ICMPv6EchoRequest,
    Packet,
    PcapNgReader,
    PcapReader,
    Raw,
    TCP,
    UDP,
    load_layer,
)

load_layer("http")
load_layer("tls")
from scapy.layers.http import HTTPRequest, HTTPResponse
from scapy.layers.tls.all import TLSClientHello, TLS_Ext_ServerName


class Plugin(GulpPluginBase):

    # Fields that may contain body/request text content
    BODY_FIELDS = {"load", "payload", "body", "data", "msg"}

    # Minimum ratio of printable ASCII characters to consider content as text
    TEXT_PRINTABLE_THRESHOLD = 0.75

    IGNORED_TOP_LAYERS = {
        "NoPayload",
        "Padding",
        "Packet_metadata",
        "Packet_metatada",
        "Raw",
    }

    PORT_PROTOCOLS = {
        20: "ftp-data",
        21: "ftp",
        22: "ssh",
        25: "smtp",
        53: "dns",
        67: "dhcp",
        68: "dhcp",
        69: "tftp",
        80: "http",
        88: "kerberos",
        110: "pop3",
        123: "ntp",
        137: "netbios-ns",
        138: "netbios-dgm",
        139: "netbios-ssn",
        143: "imap",
        161: "snmp",
        162: "snmptrap",
        389: "ldap",
        443: "tls",
        445: "smb",
        465: "smtps",
        514: "syslog",
        587: "smtp",
        636: "ldaps",
        993: "imaps",
        995: "pop3s",
        1433: "mssql",
        1883: "mqtt",
        3306: "mysql",
        3389: "rdp",
        5060: "sip",
        5061: "sips",
        5432: "postgresql",
        5672: "amqp",
        5900: "vnc",
        6379: "redis",
        8080: "http",
        8443: "tls",
        8883: "mqtts",
        9200: "opensearch",
    }

    HTTP_REQUEST_LINE_RE = re.compile(
        r"^(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|CONNECT|TRACE)\s+(\S+)\s+HTTP/(\d+(?:\.\d+)?)$",
        re.IGNORECASE,
    )
    HTTP_RESPONSE_LINE_RE = re.compile(
        r"^HTTP/(\d+(?:\.\d+)?)\s+(\d{3})(?:\s+(.*))?$",
        re.IGNORECASE,
    )
    SMTP_COMMAND_RE = re.compile(
        r"^(EHLO|HELO|MAIL FROM:|RCPT TO:|DATA|QUIT|STARTTLS|AUTH)\b",
        re.IGNORECASE,
    )
    SMTP_RESPONSE_RE = re.compile(r"^[245]\d\d(?:[ -].*)?$", re.IGNORECASE)
    FTP_COMMAND_RE = re.compile(
        r"^(USER|PASS|RETR|STOR|LIST|PASV|PORT|QUIT|TYPE|CWD|PWD|DELE|MKD|RMD)\b",
        re.IGNORECASE,
    )
    FTP_RESPONSE_RE = re.compile(r"^[12345]\d\d(?:[ -].*)?$", re.IGNORECASE)
    POP3_COMMAND_RE = re.compile(
        r"^(USER|PASS|STAT|LIST|RETR|DELE|NOOP|QUIT|TOP|UIDL|APOP|CAPA|STLS)\b",
        re.IGNORECASE,
    )
    POP3_RESPONSE_RE = re.compile(r"^(\+OK|-ERR)\b", re.IGNORECASE)
    IMAP_COMMAND_RE = re.compile(
        r"^[A-Z0-9._-]+\s+(LOGIN|SELECT|EXAMINE|FETCH|STORE|SEARCH|LOGOUT|CAPABILITY|STARTTLS|APPEND|IDLE)\b",
        re.IGNORECASE,
    )
    IMAP_RESPONSE_RE = re.compile(r"^(\*|[A-Z0-9._-]+)\s+(OK|NO|BAD|BYE)\b", re.IGNORECASE)
    SIP_REQUEST_RE = re.compile(
        r"^(INVITE|ACK|BYE|CANCEL|REGISTER|OPTIONS|MESSAGE|SUBSCRIBE|NOTIFY|INFO|PRACK|UPDATE|REFER)\s+sip:",
        re.IGNORECASE,
    )
    SIP_RESPONSE_RE = re.compile(r"^SIP/2.0\s+\d{3}(?:\s+.*)?$", re.IGNORECASE)

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
            ),
            GulpPluginCustomParameter(
                name="analyze",
                type="bool",
                desc="whether to perform additional analysis on packet contents to better detect protocols and add metadata (may not be accurate)",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="port_destination_only",
                type="bool",
                desc="when true, infer protocol from destination ports only; when false, allow source-port fallback",
                default_value=True,
            ),
            GulpPluginCustomParameter(
                name="wireshark_sequence_alignment",
                type="bool",
                desc="when true, shift packet event_sequence by +1 to align with Wireshark frame numbering",
                default_value=True,
            ),
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
                MutyLogger.get_instance().warning(f"failed to parse tls sni: {ex}")

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
                MutyLogger.get_instance().warning(
                    "skipping dns layer due to IndexError (likely from truncated pcap): %s" % (dns_layer.summary())
                )
                pass

            except Exception as ex:
                # prevent pipeline block on corrupted dns packets (often seen in dns amplification attacks)
                MutyLogger.get_instance().debug(
                    f"failed to parse dns layer: {ex}\n dns_layer: {dns_layer}"
                )

        return d

    @staticmethod
    def _decode_packet_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace").strip()
        return str(value).strip()

    @classmethod
    def _normalize_layer_name(cls, layer_name: str) -> str:
        normalized = (layer_name or "").strip()
        if not normalized:
            return "unknown"

        aliases = {
            "HTTP Request": "http",
            "HTTP Response": "http",
            "DNS": "dns",
            "TLS": "tls",
            "TLS Handshake - Client Hello": "tls",
            "BOOTP": "dhcp",
            "DHCP": "dhcp",
            "ICMP": "icmp",
            "ICMPv6 Echo Request": "icmpv6",
        }
        if normalized in aliases:
            return aliases[normalized]
        return normalized.lower().replace(" ", "_")

    @classmethod
    def _fallback_top_layer(cls, packet: Packet) -> str:
        for layer in reversed(packet.layers()):
            layer_name = getattr(layer, "__name__", "")
            if not layer_name or layer_name in cls.IGNORED_TOP_LAYERS:
                continue
            return cls._normalize_layer_name(layer_name)

        last_layer = packet.lastlayer()
        last_layer_name = getattr(last_layer, "name", None) or last_layer.__class__.__name__
        return cls._normalize_layer_name(last_layer_name)

    @staticmethod
    def _get_transport_ports(packet: Packet) -> tuple[int | None, int | None]:
        transport_layer = packet.getlayer(TCP) or packet.getlayer(UDP)
        if transport_layer is None:
            return None, None
        sport = getattr(transport_layer, "sport", None)
        dport = getattr(transport_layer, "dport", None)
        return sport, dport

    @classmethod
    def _get_payload_bytes(cls, packet: Packet) -> bytes:
        raw_layer = packet.getlayer(Raw)
        if raw_layer is None:
            return b""
        payload = getattr(raw_layer, "load", b"")
        return payload if isinstance(payload, bytes) else b""

    @classmethod
    def _get_payload_text(cls, packet: Packet) -> str:
        payload = cls._get_payload_bytes(packet)
        if not payload or not cls._is_text_content(payload):
            return ""
        return payload.decode("utf-8", errors="replace")

    @staticmethod
    def _normalize_http_version(version: str) -> str:
        if not version:
            return ""
        if version.upper().startswith("HTTP/"):
            return version.split("/", 1)[1]
        return version

    @staticmethod
    def _build_http_url(host: str, path: str) -> str:
        if not path:
            return ""
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not host:
            return ""
        normalized_path = path if path.startswith("/") else f"/{path}"
        return f"http://{host}{normalized_path}"

    @classmethod
    def _extract_http_metadata(cls, packet: Packet, payload_text: str) -> tuple[str | None, dict[str, str]]:
        request_layer = packet.getlayer(HTTPRequest)
        response_layer = packet.getlayer(HTTPResponse)
        fields: dict[str, str] = {}

        if request_layer is not None:
            method = cls._decode_packet_value(getattr(request_layer, "Method", None))
            host = cls._decode_packet_value(getattr(request_layer, "Host", None))
            path = cls._decode_packet_value(getattr(request_layer, "Path", None))
            version = cls._normalize_http_version(
                cls._decode_packet_value(getattr(request_layer, "Http_Version", None))
            )
            user_agent = cls._decode_packet_value(getattr(request_layer, "User_Agent", None))
            url = cls._build_http_url(host, path)

            if method:
                fields["HTTP.request.method"] = method.upper()
            if host:
                fields["HTTP.request.host"] = host
            if path:
                fields["HTTP.request.path"] = path
            if url:
                fields["HTTP.request.url"] = url
            if version:
                fields["HTTP.request.version"] = version
            if user_agent:
                fields["HTTP.request.user_agent"] = user_agent

        if response_layer is not None:
            version = cls._normalize_http_version(
                cls._decode_packet_value(getattr(response_layer, "Http_Version", None))
            )
            status_code = cls._decode_packet_value(getattr(response_layer, "Status_Code", None))
            reason = cls._decode_packet_value(getattr(response_layer, "Reason_Phrase", None))
            content_type = cls._decode_packet_value(getattr(response_layer, "Content_Type", None))

            if version:
                fields["HTTP.response.version"] = version
            if status_code:
                fields["HTTP.response.status_code"] = status_code
            if reason:
                fields["HTTP.response.reason"] = reason
            if content_type:
                fields["HTTP.response.content_type"] = content_type

        if fields:
            return "http", fields

        if not payload_text:
            return None, {}

        lines = payload_text.splitlines()
        if not lines:
            return None, {}

        request_match = cls.HTTP_REQUEST_LINE_RE.match(lines[0].strip())
        if request_match:
            method, path, version = request_match.groups()
            headers: dict[str, str] = {}
            for line in lines[1:]:
                stripped = line.strip()
                if not stripped:
                    break
                if ":" not in stripped:
                    continue
                header_name, header_value = stripped.split(":", 1)
                headers[header_name.strip().lower()] = header_value.strip()

            host = headers.get("host", "")
            user_agent = headers.get("user-agent", "")
            url = cls._build_http_url(host, path)
            fields = {
                "HTTP.request.method": method.upper(),
                "HTTP.request.path": path,
                "HTTP.request.version": version,
            }
            if host:
                fields["HTTP.request.host"] = host
            if user_agent:
                fields["HTTP.request.user_agent"] = user_agent
            if url:
                fields["HTTP.request.url"] = url
            return "http", fields

        response_match = cls.HTTP_RESPONSE_LINE_RE.match(lines[0].strip())
        if response_match:
            version, status_code, reason = response_match.groups()
            content_type = ""
            for line in lines[1:]:
                stripped = line.strip()
                if not stripped:
                    break
                if stripped.lower().startswith("content-type:"):
                    content_type = stripped.split(":", 1)[1].strip()
                    break

            fields = {
                "HTTP.response.version": version,
                "HTTP.response.status_code": status_code,
            }
            if reason:
                fields["HTTP.response.reason"] = reason
            if content_type:
                fields["HTTP.response.content_type"] = content_type
            return "http", fields

        return None, {}

    @classmethod
    def _extract_dhcp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        if not packet.haslayer(DHCP):
            return None, {}

        dhcp_layer = packet.getlayer(DHCP)
        fields: dict[str, str] = {}
        for option in getattr(dhcp_layer, "options", []):
            if not isinstance(option, tuple) or len(option) < 2:
                continue
            option_name, option_value = option[0], option[1]
            if option_name == "message-type":
                fields["DHCP.message_type"] = cls._decode_packet_value(option_value).lower()
            elif option_name == "hostname":
                fields["DHCP.hostname"] = cls._decode_packet_value(option_value)

        return "dhcp", fields

    @classmethod
    def _extract_ssh_metadata(cls, payload_text: str) -> tuple[str | None, dict[str, str]]:
        if not payload_text:
            return None, {}
        first_line = payload_text.splitlines()[0].strip()
        if not first_line.startswith("SSH-"):
            return None, {}
        return "ssh", {"SSH.banner": first_line}

    @classmethod
    def _extract_line_protocol_metadata(
        cls, packet: Packet, payload_text: str
    ) -> tuple[str | None, dict[str, str]]:
        if not payload_text:
            return None, {}

        first_line = payload_text.splitlines()[0].strip()
        if not first_line:
            return None, {}

        sport, dport = cls._get_transport_ports(packet)
        ports = {port for port in (sport, dport) if port is not None}
        configs = (
            ("smtp", cls.SMTP_COMMAND_RE, cls.SMTP_RESPONSE_RE, {25, 465, 587}),
            ("ftp", cls.FTP_COMMAND_RE, cls.FTP_RESPONSE_RE, {20, 21}),
            ("pop3", cls.POP3_COMMAND_RE, cls.POP3_RESPONSE_RE, {110, 995}),
            ("imap", cls.IMAP_COMMAND_RE, cls.IMAP_RESPONSE_RE, {143, 993}),
            ("sip", cls.SIP_REQUEST_RE, cls.SIP_RESPONSE_RE, {5060, 5061}),
        )

        for protocol, command_re, response_re, protocol_ports in configs:
            command_match = command_re.match(first_line)
            response_match = response_re.match(first_line)
            if not command_match and not response_match and not (ports & protocol_ports):
                continue

            fields = {"APP.summary": first_line}
            if command_match:
                command = command_match.group(1)
                if command:
                    fields["APP.command"] = command.lower()
            return protocol, fields

        return None, {}

    @classmethod
    def _infer_protocol_from_ports(
        cls, packet: Packet, destination_port_only: bool = True
    ) -> str | None:
        sport, dport = cls._get_transport_ports(packet)
        ports_to_check = (dport,) if destination_port_only else (dport, sport)
        for port in ports_to_check:
            if port in cls.PORT_PROTOCOLS:
                return cls.PORT_PROTOCOLS[port]
        return None

    @classmethod
    def _get_packet_protocol_metadata(
        cls,
        packet: Packet,
        analyze_packet: bool,
        destination_port_only: bool = True,
    ) -> dict[str, str]:
        fallback_top_layer = cls._fallback_top_layer(packet)
        if not analyze_packet:
            return {"top_layer": fallback_top_layer}

        payload_text = cls._get_payload_text(packet)
        protocol_extractors = (
            lambda: cls._extract_http_metadata(packet, payload_text),
            lambda: ("dns", {}) if packet.haslayer("DNS") else (None, {}),
            lambda: cls._extract_dhcp_metadata(packet),
            lambda: ("tls", {}) if packet.haslayer(TLSClientHello) or packet.haslayer("TLS") else (None, {}),
            lambda: cls._extract_ssh_metadata(payload_text),
            lambda: cls._extract_line_protocol_metadata(packet, payload_text),
            lambda: ("icmp", {}) if packet.haslayer(ICMP) else (None, {}),
            lambda: ("icmpv6", {}) if packet.haslayer(ICMPv6EchoRequest) else (None, {}),
            lambda: ("arp", {}) if packet.haslayer("ARP") else (None, {}),
            lambda: ("bootp", {}) if packet.haslayer(BOOTP) and not packet.haslayer(DHCP) else (None, {}),
        )

        for extractor in protocol_extractors:
            protocol, fields = extractor()
            if protocol:
                return {"top_layer": protocol, **fields}

        port_protocol = cls._infer_protocol_from_ports(
            packet, destination_port_only=destination_port_only
        )
        if port_protocol:
            return {"top_layer": port_protocol}

        return {"top_layer": fallback_top_layer}

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        # process record
        evt_json = self._pkt_to_dict(record)
        analyze_packet: bool = self._plugin_params.custom_parameters.get("analyze", False)
        port_destination_only: bool = self._plugin_params.custom_parameters.get(
            "port_destination_only", True
        )
        wireshark_sequence_alignment: bool = self._plugin_params.custom_parameters.get(
            "wireshark_sequence_alignment", False
        )
        evt_json.update(
            self._get_packet_protocol_metadata(
                record,
                analyze_packet,
                destination_port_only=port_destination_only,
            )
        )
        
        # use the last layer as gradient (all TCP packets are gonna be the same color, etc)
        d: dict = {}
        event_code = record.lastlayer()
        last_layer = event_code.name
        d["event.code"] = str(muty.crypto.hash_crc24(last_layer))

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
            event_sequence=record_idx + 1 if wireshark_sequence_alignment else record_idx,
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
