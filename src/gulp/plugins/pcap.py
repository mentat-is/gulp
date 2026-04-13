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
from datetime import datetime, timezone
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
    ICMP,
    NoPayload,
    Packet,
    PcapNgReader,
    PcapReader,
    Raw,
    TCP,
    UDP,
    load_layer,
)
from scapy.layers.inet6 import _ICMPv6

load_layer("http")
load_layer("tls")
from scapy.layers.http import HTTPRequest, HTTPResponse
from scapy.layers.tls.all import TLSClientHello, TLSServerHello, TLS_Ext_ServerName
from scapy.layers.tls.crypto.suites import _tls_cipher_suites


class Plugin(GulpPluginBase):

    # Fields that may contain body/request text content
    BODY_FIELDS = {"load", "payload", "body", "data", "msg"}

    TLS_VERSION_NAMES = {
        0x0300: "SSLv3",
        0x0301: "TLSv1.0",
        0x0302: "TLSv1.1",
        0x0303: "TLSv1.2",
        0x0304: "TLSv1.3",
    }

    SNMP_VERSION_NAMES = {0: "v1", 1: "v2c", 3: "v3"}

    # Minimum ratio of printable ASCII characters to consider content as text
    TEXT_PRINTABLE_THRESHOLD = 0.75

    # Precomputed lookup: bytes NOT in string.printable — used by _is_text_content for fast C-level filtering
    _NON_PRINTABLE_BYTES: bytes = bytes(
        b for b in range(256) if b not in frozenset(string.printable.encode("ascii"))
    )

    IGNORED_TOP_LAYERS = {
        "NoPayload",
        "Padding",
        "Packet_metadata",
        "Packet_metatada",
        "Raw",
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
        printable_count = len(data) - len(data.translate(None, Plugin._NON_PRINTABLE_BYTES))
        return (printable_count / len(data)) >= Plugin.TEXT_PRINTABLE_THRESHOLD

    def _pkt_to_dict(self, p: Packet) -> dict:
        """
        Converts a scapy packet into a flat dictionary suitable for ECS mapping.
        Extracts text, and intercepts TLS Handshakes to extract the SNI (Server Name Indication).
        """
        d = {}
        body_fields = self.BODY_FIELDS

        # Walk the layer chain directly (O(n)) instead of layers()+getlayer() (O(n²))
        layer_instance = p
        while not isinstance(layer_instance, NoPayload):
            layer_name = layer_instance.__class__.__name__

            for field in layer_instance.fields_desc:
                field_name = field.name
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

                        if field_name not in body_fields:
                            d[flat_key] = decoded_val

                    elif hasattr(value, "flagrepr"):
                        d[flat_key] = value.flagrepr()
                    else:
                        if field_name not in body_fields:
                            d[flat_key] = str(value)
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error in _pkt_to_dict, value: {value} "
                    )
                    raise

            layer_instance = layer_instance.payload

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
            "TLS Handshake - Server Hello": "tls",
            "BOOTP": "dhcp",
            "DHCP": "dhcp",
            "ICMP": "icmp",
            "NTP": "ntp",
            "NTPHeader": "ntp",
            "SNMP": "snmp",
        }
        if normalized in aliases:
            return aliases[normalized]
        # All ICMPv6 subclass names start with "ICMPv6"
        if normalized.startswith("ICMPv6") or normalized.lower().startswith("icmpv6"):
            return "icmpv6"
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
    def _extract_ssh_metadata(cls, packet: Packet, payload_text: str) -> tuple[str | None, dict[str, str]]:
        """Extract SSH banner (text) or structured SSH KEX / disconnect metadata."""
        from scapy.layers.ssh import SSH, SSHKexInit, SSHVersionExchange, SSHDisconnect

        fields: dict[str, str] = {}
        try:
            if packet.haslayer(SSHVersionExchange):
                ve = packet.getlayer(SSHVersionExchange)
                lines = getattr(ve, "lines", None)
                if lines:
                    first = lines[0]
                    banner = first.decode("utf-8", errors="ignore").strip() if isinstance(first, bytes) else str(first).strip()
                    if banner:
                        fields["SSH.banner"] = banner
                return "ssh", fields

            if packet.haslayer(SSHKexInit):
                ki = packet.getlayer(SSHKexInit)
                kex = getattr(ki, "kex_algorithms", None)
                if kex is not None:
                    names = getattr(kex, "names", None)
                    if names:
                        fields["SSH.kex_algorithms"] = ",".join(
                            n.decode("utf-8", errors="ignore") if isinstance(n, bytes) else str(n)
                            for n in names[:5]
                        )
                enc_c2s = getattr(ki, "encryption_algorithms_client_to_server", None)
                if enc_c2s is not None:
                    names = getattr(enc_c2s, "names", None)
                    if names:
                        fields["SSH.enc_c2s"] = ",".join(
                            n.decode("utf-8", errors="ignore") if isinstance(n, bytes) else str(n)
                            for n in names[:3]
                        )
                return "ssh", fields

            if packet.haslayer(SSHDisconnect):
                dc = packet.getlayer(SSHDisconnect)
                reason = dc.sprintf("%SSHDisconnect.reason_code%")
                if reason and reason not in ("??", "None"):
                    fields["SSH.disconnect_reason"] = reason
                return "ssh", fields

            if packet.haslayer(SSH):
                return "ssh", fields
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse ssh layer: {ex}")

        # Fallback: text banner heuristic
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
    def _extract_dns_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract DNS metadata: question name, query type, answer data, authority records, opcode, rcode."""
        if not packet.haslayer("DNS"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            dns_layer = packet.getlayer("DNS")

            # Question section: query name and record type
            qd = getattr(dns_layer, "qd", None)
            if qd is not None:
                raw_qname = getattr(qd, "qname", None)
                if isinstance(raw_qname, bytes):
                    fields["DNS.qd"] = raw_qname.decode("utf-8", errors="ignore").rstrip(".")
                # Record type (A, AAAA, MX, CNAME, SOA, TXT, etc.) via scapy's enum repr
                qtype_repr = qd.sprintf("%DNSQR.qtype%")
                if qtype_repr and qtype_repr not in ("0", "RESERVED"):
                    fields["DNS.qtype"] = qtype_repr

            # Answer section: response data
            an = getattr(dns_layer, "an", None)
            if an is not None:
                raw_rdata = getattr(an, "rdata", None)
                if isinstance(raw_rdata, bytes):
                    fields["DNS.an"] = raw_rdata.decode("utf-8", errors="ignore")
                elif isinstance(raw_rdata, list):
                    fields["DNS.an"] = ", ".join(
                        b.decode("utf-8", errors="ignore")
                        for b in raw_rdata
                        if isinstance(b, bytes)
                    )
                elif raw_rdata is not None:
                    fields["DNS.an"] = str(raw_rdata)

            # Authority section: NS / SOA records
            ns = getattr(dns_layer, "ns", None)
            if ns is not None:
                raw_rrname = getattr(ns, "rrname", None)
                if isinstance(raw_rrname, bytes):
                    clean_rrname = raw_rrname.decode("utf-8", errors="ignore").rstrip(".")
                    if hasattr(ns, "mname") and isinstance(ns.mname, bytes):
                        clean_mname = ns.mname.decode("utf-8", errors="ignore").rstrip(".")
                        fields["DNS.ns"] = f"SOA: {clean_rrname} (Primary NS: {clean_mname})"
                    elif hasattr(ns, "rdata") and isinstance(ns.rdata, bytes):
                        clean_rdata = ns.rdata.decode("utf-8", errors="ignore").rstrip(".")
                        fields["DNS.ns"] = f"NS: {clean_rrname} -> {clean_rdata}"
                    else:
                        fields["DNS.ns"] = clean_rrname

        except IndexError:
            MutyLogger.get_instance().warning(
                "skipping dns layer due to IndexError (likely truncated pcap): %s"
                % packet.summary()
            )
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse dns layer: {ex}")

        return "dns", fields

    @classmethod
    def _extract_tls_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract TLS metadata: SNI, client cipher suites, selected cipher, version."""
        if not (packet.haslayer(TLSClientHello) or packet.haslayer(TLSServerHello) or packet.haslayer("TLS")):
            return None, {}

        fields: dict[str, str] = {}
        try:
            if packet.haslayer(TLSClientHello):
                tls_hello = packet.getlayer(TLSClientHello)

                # SNI from Server Name Indication extension
                if hasattr(tls_hello, "ext") and tls_hello.ext:
                    for ext in tls_hello.ext:
                        if isinstance(ext, TLS_Ext_ServerName) and hasattr(ext, "servernames"):
                            if ext.servernames:
                                raw_sni = ext.servernames[0].servername
                                if isinstance(raw_sni, bytes):
                                    fields["TLS.sni"] = raw_sni.decode("utf-8", errors="ignore")
                                    break

                # Protocol version offered by the client
                version = getattr(tls_hello, "version", None)
                if version is not None:
                    fields["TLS.version"] = cls.TLS_VERSION_NAMES.get(version, f"0x{version:04x}")

                # Supported cipher suites (first 5 to keep the field compact)
                ciphers = getattr(tls_hello, "ciphers", None)
                if ciphers:
                    suite_names = [
                        _tls_cipher_suites.get(c, f"0x{c:04x}")
                        for c in ciphers[:5]
                    ]
                    fields["TLS.client.cipher_suites"] = ",".join(suite_names)

            if packet.haslayer(TLSServerHello):
                tls_server = packet.getlayer(TLSServerHello)

                # Negotiated version
                version = getattr(tls_server, "version", None)
                if version is not None:
                    fields["TLS.version"] = cls.TLS_VERSION_NAMES.get(version, f"0x{version:04x}")

                # Selected cipher suite
                cipher = getattr(tls_server, "cipher", None)
                if cipher is not None:
                    fields["TLS.cipher_suite"] = _tls_cipher_suites.get(cipher, f"0x{cipher:04x}")

        except Exception as ex:
            MutyLogger.get_instance().warning(f"failed to parse tls layer: {ex}")

        return "tls", fields

    @classmethod
    def _extract_icmp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract ICMP type name and code."""
        if not packet.haslayer(ICMP):
            return None, {}

        icmp_layer = packet.getlayer(ICMP)
        fields: dict[str, str] = {}
        type_repr = icmp_layer.sprintf("%ICMP.type%")
        if type_repr:
            fields["ICMP.type"] = type_repr
        code = getattr(icmp_layer, "code", None)
        if code is not None and code != 0:
            fields["ICMP.code"] = str(code)
        return "icmp", fields

    @classmethod
    def _extract_icmpv6_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract ICMPv6 type name by detecting the concrete ICMPv6 subclass."""
        for layer_cls in packet.layers():
            inst = packet.getlayer(layer_cls)
            if isinstance(inst, _ICMPv6):
                type_name = inst.__class__.__name__.replace("ICMPv6", "").strip()
                fields: dict[str, str] = {}
                if type_name:
                    fields["ICMPv6.type_name"] = type_name
                type_num = getattr(inst, "type", None)
                if type_num is not None:
                    fields["ICMPv6.type"] = str(type_num)
                return "icmpv6", fields
        return None, {}

    @classmethod
    def _extract_arp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract ARP operation (who-has / is-at)."""
        if not packet.haslayer("ARP"):
            return None, {}

        arp_layer = packet.getlayer("ARP")
        fields: dict[str, str] = {}
        op_repr = arp_layer.sprintf("%ARP.op%")
        if op_repr:
            fields["ARP.operation"] = op_repr
        return "arp", fields

    @classmethod
    def _extract_snmp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract SNMP version, community string, and PDU type."""
        from scapy.layers.snmp import SNMP as SNMPLayer  # lazy import to keep startup clean

        if not packet.haslayer(SNMPLayer):
            return None, {}

        snmp_layer = packet.getlayer(SNMPLayer)
        fields: dict[str, str] = {}
        try:
            version = getattr(snmp_layer, "version", None)
            if version is not None:
                try:
                    # network.protocol_version is always a string.
                    fields["SNMP.version"] = str(version)
                except (TypeError, ValueError):
                    # Some malformed captures can expose non-integer ASN.1 payloads.
                    # Skip the version in that case instead of indexing invalid values.
                    pass

            community = getattr(snmp_layer, "community", None)
            if isinstance(community, bytes) and community:
                community_str = community.decode("utf-8", errors="ignore")
                if community_str:
                    fields["SNMP.community"] = community_str

            pdu = getattr(snmp_layer, "PDU", None)
            if pdu is not None:
                fields["SNMP.pdu_type"] = type(pdu).__name__
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse snmp layer: {ex}")

        return "snmp", fields

    @classmethod
    def _extract_ntp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract NTP mode, stratum, and version from NTPHeader."""
        if not packet.haslayer("NTP"):
            return None, {}

        ntp_layer = packet.getlayer("NTP")
        fields: dict[str, str] = {}
        try:
            mode_repr = ntp_layer.sprintf("%NTPHeader.mode%")
            if mode_repr:
                fields["NTP.mode"] = mode_repr
            stratum = getattr(ntp_layer, "stratum", None)
            if stratum is not None:
                fields["NTP.stratum"] = str(stratum)
            version = getattr(ntp_layer, "version", None)
            if version is not None:
                fields["NTP.version"] = str(version)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse ntp layer: {ex}")

        return "ntp", fields

    @classmethod
    def _extract_gre_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract GRE tunnel encapsulated protocol name."""
        if not packet.haslayer("GRE"):
            return None, {}

        gre_layer = packet.getlayer("GRE")
        fields: dict[str, str] = {}
        try:
            proto_repr = gre_layer.sprintf("%GRE.proto%")
            if proto_repr and proto_repr != "0x0":
                fields["GRE.encapsulated_proto"] = proto_repr
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse gre layer: {ex}")
        return "gre", fields

    @classmethod
    def _extract_vxlan_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract VXLAN Network Identifier (VNI)."""
        if not packet.haslayer("VXLAN"):
            return None, {}

        vxlan_layer = packet.getlayer("VXLAN")
        fields: dict[str, str] = {}
        try:
            vni = getattr(vxlan_layer, "vni", None)
            if vni is not None:
                fields["VXLAN.vni"] = str(vni)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse vxlan layer: {ex}")
        return "vxlan", fields

    @classmethod
    def _extract_sctp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract SCTP source/destination ports."""
        if not packet.haslayer("SCTP"):
            return None, {}

        sctp_layer = packet.getlayer("SCTP")
        fields: dict[str, str] = {}
        try:
            sport = getattr(sctp_layer, "sport", None)
            dport = getattr(sctp_layer, "dport", None)
            if sport is not None:
                fields["SCTP.sport"] = str(sport)
            if dport is not None:
                fields["SCTP.dport"] = str(dport)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse sctp layer: {ex}")
        return "sctp", fields

    @classmethod
    def _extract_radius_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract RADIUS code (Access-Request / Accept / Reject) and optional username attribute."""
        from scapy.layers.radius import Radius, RadiusAttr_User_Name  # lazy import

        if not packet.haslayer(Radius):
            return None, {}

        radius_layer = packet.getlayer(Radius)
        fields: dict[str, str] = {}
        try:
            code_repr = radius_layer.sprintf("%Radius.code%")
            if code_repr:
                fields["RADIUS.code"] = code_repr
            # Walk attributes looking for User-Name
            attrs = getattr(radius_layer, "attributes", None) or []
            for attr in attrs:
                if isinstance(attr, RadiusAttr_User_Name):
                    raw_val = getattr(attr, "value", None)
                    if isinstance(raw_val, bytes) and raw_val:
                        fields["RADIUS.username"] = raw_val.decode("utf-8", errors="ignore")
                    break
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse radius layer: {ex}")
        return "radius", fields

    @classmethod
    def _extract_rtp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract RTP payload type (codec name) and synchronisation source."""
        from scapy.layers.rtp import RTP  # lazy import

        if not packet.haslayer(RTP):
            return None, {}

        rtp_layer = packet.getlayer(RTP)
        fields: dict[str, str] = {}
        try:
            pt_repr = rtp_layer.sprintf("%RTP.payload_type%")
            if pt_repr:
                fields["RTP.payload_type"] = pt_repr
            ssrc = getattr(rtp_layer, "sourcesync", None)
            if ssrc is not None:
                fields["RTP.ssrc"] = f"0x{ssrc:08x}"
            seq = getattr(rtp_layer, "sequence", None)
            if seq is not None:
                fields["RTP.sequence"] = str(seq)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse rtp layer: {ex}")
        return "rtp", fields

    @classmethod
    def _extract_dhcp6_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract DHCPv6 message type."""
        dhcp6_names = (
            "DHCP6_Solicit", "DHCP6_Advertise", "DHCP6_Request",
            "DHCP6_Reply", "DHCP6_Release", "DHCP6_Renew",
            "DHCP6_Rebind", "DHCP6_Decline", "DHCP6_InfoRequest",
        )
        found_layer = None
        for name in dhcp6_names:
            if packet.haslayer(name):
                found_layer = packet.getlayer(name)
                break

        if found_layer is None:
            return None, {}

        fields: dict[str, str] = {}
        try:
            msgtype = getattr(found_layer, "msgtype", None)
            if msgtype is not None:
                # Use the ByteEnumField i2s dict directly — sprintf with class names
                # containing underscores is fragile across scapy versions.
                for fd in found_layer.fields_desc:
                    if fd.name == "msgtype" and hasattr(fd, "i2s") and fd.i2s:
                        label = fd.i2s.get(int(msgtype))
                        fields["DHCP6.msgtype"] = label if label else str(msgtype)
                        break
                else:
                    fields["DHCP6.msgtype"] = str(msgtype)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse dhcp6 layer: {ex}")
        return "dhcp6", fields

    @classmethod
    def _extract_isakmp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract IKEv1/ISAKMP exchange type."""
        if not packet.haslayer("ISAKMP"):
            return None, {}

        isakmp_layer = packet.getlayer("ISAKMP")
        fields: dict[str, str] = {}
        try:
            exch_repr = isakmp_layer.sprintf("%ISAKMP.exch_type%")
            if exch_repr and exch_repr not in ("??", "None"):
                fields["ISAKMP.exchange_type"] = exch_repr
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse isakmp layer: {ex}")
        return "isakmp", fields

    @classmethod
    def _extract_rip_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract RIP command (req/resp) and version."""
        if not packet.haslayer("RIP"):
            return None, {}

        rip_layer = packet.getlayer("RIP")
        fields: dict[str, str] = {}
        try:
            cmd_repr = rip_layer.sprintf("%RIP.cmd%")
            if cmd_repr:
                fields["RIP.command"] = cmd_repr
            version = getattr(rip_layer, "version", None)
            if version is not None:
                # network.protocol_version is always a string.
                try:
                    fields["RIP.version"] = str(version)
                except (TypeError, ValueError):
                    if isinstance(version, bytes) and len(version) > 0:
                        fields["RIP.version"] = str(version[0])
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse rip layer: {ex}")
        return "rip", fields

    @classmethod
    def _extract_netbios_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract NetBIOS query name from NBNS packets."""
        if not packet.haslayer("NBNSQueryRequest") and not packet.haslayer("NBNSQueryResponse"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            if packet.haslayer("NBNSQueryRequest"):
                req = packet.getlayer("NBNSQueryRequest")
                name = getattr(req, "QUESTION_NAME", None)
                if isinstance(name, bytes):
                    fields["NetBIOS.query_name"] = name.decode("utf-8", errors="ignore").strip()
                elif name is not None:
                    fields["NetBIOS.query_name"] = str(name).strip()
            elif packet.haslayer("NBNSQueryResponse"):
                resp = packet.getlayer("NBNSQueryResponse")
                name = getattr(resp, "RR_NAME", None)
                if isinstance(name, bytes):
                    fields["NetBIOS.rr_name"] = name.decode("utf-8", errors="ignore").strip()
                elif name is not None:
                    fields["NetBIOS.rr_name"] = str(name).strip()
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse netbios layer: {ex}")
        return "netbios-ns", fields

    @classmethod
    def _extract_eap_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract EAP code (Request/Response/Success/Failure) and identity."""
        if not packet.haslayer("EAP"):
            return None, {}

        eap_layer = packet.getlayer("EAP")
        fields: dict[str, str] = {}
        try:
            code_repr = eap_layer.sprintf("%EAP.code%")
            if code_repr:
                fields["EAP.code"] = code_repr
            identity = getattr(eap_layer, "identity", None)
            if identity:
                if isinstance(identity, bytes):
                    fields["EAP.identity"] = identity.decode("utf-8", errors="ignore")
                else:
                    fields["EAP.identity"] = str(identity)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse eap layer: {ex}")
        return "eap", fields

    @classmethod
    def _extract_bootp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract legacy BOOTP fields for packets without DHCP option 53.

        Modern DHCP packets are always BOOTP+DHCP stacked, so this handler
        only fires for genuine legacy BOOTP exchanges.
        """
        if not packet.haslayer(BOOTP) or packet.haslayer(DHCP):
            return None, {}

        fields: dict[str, str] = {}
        try:
            bootp = packet.getlayer(BOOTP)
            op_repr = bootp.sprintf("%BOOTP.op%")
            if op_repr and op_repr != "??":
                fields["BOOTP.op"] = op_repr
            yiaddr = getattr(bootp, "yiaddr", None)
            if yiaddr and str(yiaddr) != "0.0.0.0":
                fields["BOOTP.yiaddr"] = str(yiaddr)
            siaddr = getattr(bootp, "siaddr", None)
            if siaddr and str(siaddr) != "0.0.0.0":
                fields["BOOTP.siaddr"] = str(siaddr)
            sname = getattr(bootp, "sname", b"")
            if isinstance(sname, bytes):
                sname_str = sname.rstrip(b"\x00").decode("utf-8", errors="ignore")
                if sname_str:
                    fields["BOOTP.sname"] = sname_str
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse bootp layer: {ex}")
        return "bootp", fields

    @classmethod
    def _extract_kerberos_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract Kerberos message type and realm."""
        _KRB_LAYER_NAMES = (
            "KRB_AS_REQ", "KRB_AS_REP", "KRB_TGS_REQ", "KRB_TGS_REP",
            "KRB_AP_REQ", "KRB_AP_REP", "KRB_ERROR",
        )
        _KRB_MSG_TYPES = {
            10: "AS-REQ", 11: "AS-REP", 12: "TGS-REQ", 13: "TGS-REP",
            14: "AP-REQ", 15: "AP-REP", 30: "KRB-ERROR",
        }

        found = None
        for name in _KRB_LAYER_NAMES:
            if packet.haslayer(name):
                found = packet.getlayer(name)
                break
        if found is None:
            return None, {}

        fields: dict[str, str] = {}
        try:
            msg_type_raw = getattr(found, "msgType", None)
            if msg_type_raw is not None:
                type_val = msg_type_raw.val if hasattr(msg_type_raw, "val") else int(msg_type_raw)
                fields["Kerberos.msg_type"] = _KRB_MSG_TYPES.get(type_val, str(type_val))
            # realm: in reqBody for REQ types, directly as .realm/.crealm for ERROR/REP
            realm_raw = None
            req_body = getattr(found, "reqBody", None)
            if req_body is not None:
                realm_raw = getattr(req_body, "realm", None)
            if realm_raw is None:
                realm_raw = getattr(found, "realm", None) or getattr(found, "crealm", None)
            if realm_raw is not None:
                realm_str = realm_raw.val if hasattr(realm_raw, "val") else str(realm_raw)
                if realm_str:
                    fields["Kerberos.realm"] = str(realm_str)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse kerberos layer: {ex}")
        return "kerberos", fields

    @classmethod
    def _extract_ldap_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract LDAP operation type, bind DN and search base/scope."""
        if not packet.haslayer("LDAP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.packet import NoPayload

            _OP_MAP = {
                "LDAP_BindRequest": "bind",
                "LDAP_BindResponse": "bind-response",
                "LDAP_SearchRequest": "search",
                "LDAP_SearchResponseEntry": "search-entry",
                "LDAP_SearchResponseResultDone": "search-done",
                "LDAP_AddRequest": "add",
                "LDAP_DelRequest": "delete",
                "LDAP_ModifyRequest": "modify",
                "LDAP_AbandonRequest": "abandon",
                "LDAP_UnbindRequest": "unbind",
            }

            # Walk payload chain once — avoids calling getlayer() with strings
            # which may trigger re-dissection from bytes and lose in-memory values.
            def _walk_payload(pkt: Packet):
                cur = pkt
                while cur is not None and not isinstance(cur, NoPayload):
                    yield cur
                    nxt = cur.payload if hasattr(cur, "payload") else None
                    # stop if payload is the same object or another NoPayload sentinel
                    if nxt is cur or nxt is None:
                        break
                    cur = nxt

            bind_req = None
            search_req = None
            for layer in _walk_payload(packet):
                cls_name = type(layer).__name__
                if cls_name in _OP_MAP and "LDAP.operation" not in fields:
                    fields["LDAP.operation"] = _OP_MAP[cls_name]
                if cls_name == "LDAP_BindRequest":
                    bind_req = layer
                if cls_name == "LDAP_SearchRequest":
                    search_req = layer

            if bind_req is not None:
                bn = getattr(bind_req, "bind_name", None)
                if bn is not None:
                    if isinstance(bn, bytes):
                        fields["LDAP.bind_dn"] = bn.decode("utf-8", errors="ignore")
                    else:
                        val = bn.val if hasattr(bn, "val") else str(bn)
                        if val:
                            fields["LDAP.bind_dn"] = str(val)

            if search_req is not None:
                base_obj = getattr(search_req, "baseObject", None)
                if base_obj is not None:
                    bval = base_obj.val if hasattr(base_obj, "val") else base_obj
                    if bval:
                        base_str = bval.decode("utf-8", errors="ignore") if isinstance(bval, bytes) else str(bval)
                        if base_str:
                            fields["LDAP.search_base"] = base_str
                scope = getattr(search_req, "scope", None)
                if scope is not None:
                    scope_int = scope.val if hasattr(scope, "val") else scope
                    _SCOPE_LABELS = {0: "baseObject", 1: "singleLevel", 2: "wholeSubtree"}
                    try:
                        fields["LDAP.search_scope"] = _SCOPE_LABELS.get(int(scope_int), str(scope_int))
                    except (TypeError, ValueError):
                        pass
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse ldap layer: {ex}")
        return "ldap", fields

    @classmethod
    def _extract_ipsec_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract IPsec ESP or AH SPI and sequence number."""
        has_esp = packet.haslayer("ESP")
        has_ah = packet.haslayer("AH")
        if not has_esp and not has_ah:
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.ipsec import AH, ESP

            if has_esp:
                esp = packet.getlayer(ESP)
                fields["IPsec.protocol"] = "ESP"
                fields["IPsec.spi"] = str(esp.spi)
                fields["IPsec.seq"] = str(esp.seq)
            else:
                ah = packet.getlayer(AH)
                fields["IPsec.protocol"] = "AH"
                fields["IPsec.spi"] = str(ah.spi)
                fields["IPsec.seq"] = str(ah.seq)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse ipsec layer: {ex}")
        return "ipsec", fields

    @classmethod
    def _extract_ppp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract PPP encapsulated protocol name."""
        if not packet.haslayer("PPP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.ppp import PPP

            ppp = packet.getlayer(PPP)
            proto_repr = ppp.sprintf("%PPP.proto%")
            if proto_repr and proto_repr != "??":
                fields["PPP.proto"] = proto_repr
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse ppp layer: {ex}")
        return "ppp", fields

    @classmethod
    def _extract_netflow_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract NetFlow version."""
        if not packet.haslayer("NetflowHeader"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.netflow import NetflowHeader

            nf = packet.getlayer(NetflowHeader)
            version = getattr(nf, "version", None)
            if version is not None:
                fields["NetFlow.version"] = str(version)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse netflow layer: {ex}")
        return "netflow", fields

    @classmethod
    def _extract_dcerpc_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract DCE/RPC packet type, version and operation number."""
        has_v4 = packet.haslayer("DceRpc4")
        has_v5 = packet.haslayer("DceRpc5")
        if not has_v4 and not has_v5:
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.dcerpc import DceRpc4, DceRpc5, DceRpc5Request

            if has_v5:
                rpc = packet.getlayer(DceRpc5)
                fields["DCERPC.version"] = "5"
            else:
                rpc = packet.getlayer(DceRpc4)
                fields["DCERPC.version"] = "4"
            ptype = rpc.sprintf("%%%s.ptype%%" % rpc.__class__.__name__)
            if ptype and ptype != "??":
                fields["DCERPC.ptype"] = ptype
            if has_v5 and packet.haslayer(DceRpc5Request):
                req = packet.getlayer(DceRpc5Request)
                opnum = getattr(req, "opnum", None)
                if opnum is not None:
                    fields["DCERPC.opnum"] = str(opnum)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse dcerpc layer: {ex}")
        return "dcerpc", fields

    @classmethod
    def _extract_smb2_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract SMB2 command and status."""
        if not packet.haslayer("SMB2_Header"):
            return None, {}

        from scapy.layers.smb2 import SMB2_Header

        fields: dict[str, str] = {}
        try:
            smb2 = packet.getlayer(SMB2_Header)
            cmd = smb2.sprintf("%SMB2_Header.Command%")
            if cmd and cmd != "??":
                fields["SMB2.command"] = cmd
            status = smb2.sprintf("%SMB2_Header.Status%")
            if status and status != "??":
                fields["SMB2.status"] = status
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse smb2 layer: {ex}")
        return "smb2", fields

    @classmethod
    def _extract_tftp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract TFTP operation code and filename for read/write requests."""
        if not packet.haslayer("TFTP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.tftp import TFTP

            tftp = packet.getlayer(TFTP)
            op_repr = tftp.sprintf("%TFTP.op%")
            if op_repr and op_repr != "??":
                fields["TFTP.op"] = op_repr
            # filename is present on RRQ and WRQ sub-layers
            for sub_name in ("TFTP_RRQ", "TFTP_WRQ"):
                if packet.haslayer(sub_name):
                    req = packet.getlayer(sub_name)
                    fname = getattr(req, "filename", None)
                    if fname:
                        if isinstance(fname, bytes):
                            fields["TFTP.filename"] = fname.decode("utf-8", errors="ignore")
                        else:
                            fields["TFTP.filename"] = str(fname)
                    break
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse tftp layer: {ex}")
        return "tftp", fields

    @classmethod
    def _extract_vrrp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract VRRP virtual-router ID, priority and version."""
        if not packet.haslayer("VRRP") and not packet.haslayer("VRRPv3"):
            return None, {}

        from scapy.layers.vrrp import VRRP

        fields: dict[str, str] = {}
        try:
            vrrp = packet.getlayer(VRRP) or packet.getlayer("VRRPv3")
            if vrrp is None:
                return None, {}
            vrid = getattr(vrrp, "vrid", None)
            if vrid is not None:
                fields["VRRP.vrid"] = str(vrid)
            priority = getattr(vrrp, "priority", None)
            if priority is not None:
                fields["VRRP.priority"] = str(priority)
            version = getattr(vrrp, "version", None)
            if version is not None:
                fields["VRRP.version"] = str(version)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse vrrp layer: {ex}")
        return "vrrp", fields

    @classmethod
    def _extract_hsrp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract HSRP opcode, state, group and virtual IP."""
        if not packet.haslayer("HSRP"):
            return None, {}

        from scapy.layers.hsrp import HSRP

        fields: dict[str, str] = {}
        try:
            hsrp = packet.getlayer(HSRP)
            opcode = hsrp.sprintf("%HSRP.opcode%")
            if opcode and opcode != "??":
                fields["HSRP.opcode"] = opcode
            state = hsrp.sprintf("%HSRP.state%")
            if state and state != "??":
                fields["HSRP.state"] = state
            group = getattr(hsrp, "group", None)
            if group is not None:
                fields["HSRP.group"] = str(group)
            vip = getattr(hsrp, "virtualIP", None)
            if vip:
                fields["HSRP.virtual_ip"] = str(vip)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse hsrp layer: {ex}")
        return "hsrp", fields

    @classmethod
    def _extract_l2tp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract L2TP tunnel ID, session ID and version."""
        if not packet.haslayer("L2TP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.l2tp import L2TP

            l2tp = packet.getlayer(L2TP)
            tunnel_id = getattr(l2tp, "tunnel_id", None)
            if tunnel_id is not None:
                fields["L2TP.tunnel_id"] = str(tunnel_id)
            session_id = getattr(l2tp, "session_id", None)
            if session_id is not None:
                fields["L2TP.session_id"] = str(session_id)
            version = getattr(l2tp, "version", None)
            if version is not None:
                fields["L2TP.version"] = str(version)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse l2tp layer: {ex}")
        return "l2tp", fields

    @classmethod
    def _extract_dot11_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract 802.11 (Wi-Fi) frame type, BSSID and SSID (from beacon/probe)."""
        if not packet.haslayer("Dot11"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.dot11 import Dot11, Dot11Elt

            dot11 = packet.getlayer(Dot11)
            frame_type = dot11.sprintf("%Dot11.type%")
            if frame_type and frame_type != "??":
                fields["Dot11.type"] = frame_type
            # addr2 is the BSSID for AP-originated frames; addr1 for client→AP
            bssid = getattr(dot11, "addr2", None)
            if bssid and bssid != "00:00:00:00:00:00":
                fields["Dot11.bssid"] = str(bssid)
            # Walk Dot11Elt elements looking for SSID (ID==0)
            elt = packet.getlayer(Dot11Elt)
            while elt is not None:
                if getattr(elt, "ID", None) == 0:
                    info = getattr(elt, "info", b"")
                    if info:
                        if isinstance(info, bytes):
                            fields["Dot11.ssid"] = info.decode("utf-8", errors="replace")
                        else:
                            fields["Dot11.ssid"] = str(info)
                    break
                elt = elt.payload.getlayer(Dot11Elt) if elt.payload else None
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse dot11 layer: {ex}")
        return "dot11", fields

    @classmethod
    def _extract_llmnr_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract LLMNR (Link-Local Multicast Name Resolution) query name and type."""
        if not packet.haslayer("LLMNRQuery") and not packet.haslayer("LLMNRResponse"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.dns import DNSQR

            layer = (
                packet.getlayer("LLMNRQuery")
                or packet.getlayer("LLMNRResponse")
            )
            # qd is a PacketListField in newer scapy versions — access first element
            qd_raw = getattr(layer, "qd", None)
            qd = qd_raw[0] if qd_raw and hasattr(qd_raw, "__getitem__") else qd_raw
            if qd is not None:
                qname = getattr(qd, "qname", None)
                if qname:
                    if isinstance(qname, bytes):
                        fields["LLMNR.qname"] = qname.decode("utf-8", errors="ignore").rstrip(".")
                    else:
                        fields["LLMNR.qname"] = str(qname).rstrip(".")
                if isinstance(qd, DNSQR):
                    qtype_repr = qd.sprintf("%DNSQR.qtype%")
                    if qtype_repr and qtype_repr != "??":
                        fields["LLMNR.qtype"] = qtype_repr
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse llmnr layer: {ex}")
        return "llmnr", fields

    @classmethod
    def _extract_pptp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract PPTP control message type."""
        if not packet.haslayer("PPTP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.pptp import PPTP

            pptp = packet.getlayer(PPTP)
            msg_type = pptp.sprintf("%PPTP.ctrl_msg_type%")
            if msg_type and msg_type != "??":
                fields["PPTP.ctrl_msg_type"] = msg_type
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse pptp layer: {ex}")
        return "pptp", fields

    @classmethod
    def _extract_mgcp_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract MGCP verb and endpoint."""
        if not packet.haslayer("MGCP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.mgcp import MGCP

            mgcp = packet.getlayer(MGCP)
            verb = getattr(mgcp, "verb", None)
            if verb:
                if isinstance(verb, bytes):
                    fields["MGCP.verb"] = verb.decode("utf-8", errors="ignore")
                else:
                    fields["MGCP.verb"] = str(verb)
            endpoint = getattr(mgcp, "endpoint", None)
            if endpoint:
                if isinstance(endpoint, bytes):
                    fields["MGCP.endpoint"] = endpoint.decode("utf-8", errors="ignore")
                else:
                    fields["MGCP.endpoint"] = str(endpoint)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse mgcp layer: {ex}")
        return "mgcp", fields

    @classmethod
    def _extract_skinny_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract Cisco SCCP (Skinny) message type."""
        if not packet.haslayer("Skinny"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.skinny import Skinny

            skinny = packet.getlayer(Skinny)
            msg = skinny.sprintf("%Skinny.msg%")
            if msg and msg != "??":
                fields["Skinny.msg"] = msg
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse skinny layer: {ex}")
        return "skinny", fields

    @classmethod
    def _extract_ntlm_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract NTLM message type (NEGOTIATE/CHALLENGE/AUTHENTICATE)."""
        ntlm_layer_names = ("NTLM_NEGOTIATE", "NTLM_CHALLENGE", "NTLM_AUTHENTICATE", "NTLM_AUTHENTICATE_V2")
        found: Packet | None = None
        for name in ntlm_layer_names:
            if packet.haslayer(name):
                found = packet.getlayer(name)
                break
        if found is None:
            return None, {}

        fields: dict[str, str] = {}
        try:
            msg_type = getattr(found, "MessageType", None)
            if msg_type is not None:
                for fd in type(found).fields_desc:
                    if fd.name == "MessageType" and hasattr(fd, "i2s") and fd.i2s:
                        label = fd.i2s.get(int(msg_type))
                        fields["NTLM.message_type"] = label if label else str(msg_type)
                        break
                else:
                    fields["NTLM.message_type"] = str(msg_type)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse ntlm layer: {ex}")
        return "ntlm", fields

    @classmethod
    def _extract_smb1_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract SMB1 command, status, and session-setup account/domain."""
        if not packet.haslayer("SMB_Header"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.smb import SMB_Header, SMBSession_Setup_AndX_Request

            hdr = packet.getlayer(SMB_Header)
            cmd = hdr.sprintf("%SMB_Header.Command%")
            if cmd and cmd != "??":
                fields["SMB1.command"] = cmd
            status = hdr.sprintf("%SMB_Header.Status%")
            if status and status != "??":
                fields["SMB1.status"] = status

            if packet.haslayer(SMBSession_Setup_AndX_Request):
                sess = packet.getlayer(SMBSession_Setup_AndX_Request)
                for attr, key in (("AccountName", "SMB1.account_name"), ("PrimaryDomain", "SMB1.domain")):
                    val = getattr(sess, attr, None)
                    if val:
                        if isinstance(val, bytes):
                            val = val.decode("utf-16-le" if b"\x00" in val else "utf-8", errors="ignore").strip("\x00")
                        else:
                            val = str(val).strip("\x00")
                        if val:
                            fields[key] = val
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse smb1 layer: {ex}")
        return "smb1", fields

    @classmethod
    def _extract_spnego_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract SPNEGO negotiation mechanism types and result."""
        _has_spnego = (
            packet.haslayer("SPNEGO_negTokenInit")
            or packet.haslayer("SPNEGO_negTokenResp")
            or packet.haslayer("GSSAPI_BLOB")
        )
        if not _has_spnego:
            return None, {}

        # Well-known SPNEGO mechanism OIDs
        _MECH_OID_NAMES = {
            "1.3.6.1.4.1.311.2.2.10": "NTLM",
            "1.2.840.48018.1.2.2": "Kerberos",
            "1.3.6.1.5.2.5": "Kerberos",
            "1.3.6.1.5.5.2": "SPNEGO",
            "1.3.6.1.4.1.311.2.2.30.1.1.1": "NegoEx",
        }
        _NEGTOKENRESP_RESULTS = {0: "accept-completed", 1: "accept-incomplete", 2: "reject", 3: "request-mic"}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.spnego import SPNEGO_negTokenInit, SPNEGO_negTokenResp

            if packet.haslayer(SPNEGO_negTokenInit):
                init = packet.getlayer(SPNEGO_negTokenInit)
                mechs_outer = getattr(init, "mechTypes", None)
                # mechTypes is a SPNEGO_MechTypes packet with a inner .mechTypes list
                if mechs_outer is not None:
                    mech_list = getattr(mechs_outer, "mechTypes", None)
                    if mech_list is None:
                        mech_list = mechs_outer if isinstance(mechs_outer, (list, tuple)) else [mechs_outer]
                    names = []
                    for m in mech_list:
                        oid_obj = getattr(m, "oid", m)
                        # ASN1_OID: prefer .oidname, fall back to .val, then string form
                        oidname = getattr(oid_obj, "oidname", None)
                        if oidname:
                            names.append(oidname.split(" - ")[0] if " - " in oidname else oidname)
                        else:
                            oid_str = str(getattr(oid_obj, "val", oid_obj))
                            names.append(_MECH_OID_NAMES.get(oid_str, oid_str))
                    if names:
                        fields["SPNEGO.mechanisms"] = ",".join(names)

            if packet.haslayer(SPNEGO_negTokenResp):
                resp = packet.getlayer(SPNEGO_negTokenResp)
                neg_result = getattr(resp, "negResult", None)
                if neg_result is not None:
                    result_int = neg_result.val if hasattr(neg_result, "val") else neg_result
                    try:
                        fields["SPNEGO.result"] = _NEGTOKENRESP_RESULTS.get(int(result_int), str(result_int))
                    except (TypeError, ValueError):
                        fields["SPNEGO.result"] = str(neg_result)
                supported = getattr(resp, "supportedMech", None)
                if supported is not None:
                    oid = str(getattr(supported, "oid", supported) if hasattr(supported, "oid") else supported)
                    fields["SPNEGO.selected_mech"] = _MECH_OID_NAMES.get(oid, oid)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse spnego layer: {ex}")
        return "spnego", fields

    @classmethod
    def _extract_lltd_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract LLTD function (Discover/Hello/…), hostname and IPv4 from attributes."""
        if not packet.haslayer("LLTD"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.lltd import LLTD, LLTDAttributeMachineName, LLTDAttributeIPv4Address

            lltd = packet.getlayer(LLTD)
            func = lltd.sprintf("%LLTD.function%")
            if func and func != "??":
                fields["LLTD.function"] = func

            if packet.haslayer(LLTDAttributeMachineName):
                attr = packet.getlayer(LLTDAttributeMachineName)
                hostname = getattr(attr, "hostname", None)
                if hostname:
                    fields["LLTD.hostname"] = str(hostname)

            if packet.haslayer(LLTDAttributeIPv4Address):
                attr = packet.getlayer(LLTDAttributeIPv4Address)
                ipv4 = getattr(attr, "ipv4", None)
                if ipv4:
                    fields["LLTD.ipv4"] = str(ipv4)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse lltd layer: {ex}")
        return "lltd", fields

    @classmethod
    def _extract_mobileip_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract Mobile IP message type and key addresses."""
        if not packet.haslayer("MobileIP"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.mobileip import MobileIP, MobileIPRRQ, MobileIPRRP

            mip = packet.getlayer(MobileIP)
            msg_type = mip.sprintf("%MobileIP.type%")
            if msg_type and msg_type != "??":
                fields["MobileIP.type"] = msg_type

            if packet.haslayer(MobileIPRRQ):
                req = packet.getlayer(MobileIPRRQ)
                for attr in ("homeaddr", "haaddr", "coaddr"):
                    val = getattr(req, attr, None)
                    if val:
                        fields[f"MobileIP.{attr}"] = str(val)

            if packet.haslayer(MobileIPRRP):
                rep = packet.getlayer(MobileIPRRP)
                code = getattr(rep, "code", None)
                if code is not None:
                    fields["MobileIP.code"] = str(int(code))
                for attr in ("homeaddr", "haaddr"):
                    val = getattr(rep, attr, None)
                    if val:
                        fields[f"MobileIP.{attr}"] = str(val)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse mobileip layer: {ex}")
        return "mobileip", fields

    @classmethod
    def _extract_gprs_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Detect GPRS/GTP encapsulation (scapy's GPRS layer is a minimal GTP stub)."""
        if not packet.haslayer("GPRS"):
            return None, {}
        return "gprs", {}

    # ------------------------------------------------------------------
    # IoT protocol extractors
    # ------------------------------------------------------------------

    @classmethod
    def _extract_bluetooth_hci_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract Bluetooth Classic HCI packet type, ACL handle, event code or command OGF/OCF."""
        from scapy.layers.bluetooth import HCI_Hdr, HCI_ACL_Hdr, HCI_Command_Hdr, HCI_Event_Hdr

        if not packet.haslayer(HCI_Hdr):
            return None, {}

        fields: dict[str, str] = {}
        try:
            hci = packet.getlayer(HCI_Hdr)
            pkt_type = hci.sprintf("%HCI_Hdr.type%")
            if pkt_type and pkt_type not in ("??", "None"):
                fields["BT.hci_type"] = pkt_type

            if packet.haslayer(HCI_ACL_Hdr):
                acl = packet.getlayer(HCI_ACL_Hdr)
                handle = getattr(acl, "handle", None)
                if handle is not None:
                    fields["BT.acl_handle"] = str(handle)

            if packet.haslayer(HCI_Event_Hdr):
                evt = packet.getlayer(HCI_Event_Hdr)
                code = evt.sprintf("%HCI_Event_Hdr.code%")
                if code and code not in ("??", "None"):
                    fields["BT.event_code"] = code

            if packet.haslayer(HCI_Command_Hdr):
                cmd = packet.getlayer(HCI_Command_Hdr)
                ogf = getattr(cmd, "ogf", None)
                ocf = getattr(cmd, "ocf", None)
                if ogf is not None:
                    fields["BT.cmd_ogf"] = str(ogf)
                if ocf is not None:
                    fields["BT.cmd_ocf"] = str(ocf)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse bluetooth hci layer: {ex}")
        return "bluetooth", fields

    @classmethod
    def _extract_btle_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract Bluetooth LE access address, advertising PDU type and data LLID."""
        from scapy.layers.bluetooth4LE import BTLE, BTLE_ADV, BTLE_DATA

        if not packet.haslayer(BTLE):
            return None, {}

        fields: dict[str, str] = {}
        try:
            btle = packet.getlayer(BTLE)
            access_addr = getattr(btle, "access_addr", None)
            if access_addr is not None:
                fields["BTLE.access_addr"] = f"0x{access_addr:08x}"

            if packet.haslayer(BTLE_ADV):
                adv = packet.getlayer(BTLE_ADV)
                pdu_type = adv.sprintf("%BTLE_ADV.PDU_type%")
                if pdu_type and pdu_type not in ("??", "None"):
                    fields["BTLE.pdu_type"] = pdu_type

            if packet.haslayer(BTLE_DATA):
                data = packet.getlayer(BTLE_DATA)
                llid = data.sprintf("%BTLE_DATA.LLID%")
                if llid and llid not in ("??", "None"):
                    fields["BTLE.llid"] = llid
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse btle layer: {ex}")
        return "btle", fields

    @classmethod
    def _extract_can_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract CAN / CAN-FD message identifier, length and variant."""
        from scapy.layers.can import CAN, CANFD

        has_canfd = packet.haslayer(CANFD)
        has_can = packet.haslayer(CAN)
        if not has_can and not has_canfd:
            return None, {}

        fields: dict[str, str] = {}
        try:
            can = packet.getlayer(CANFD) if has_canfd else packet.getlayer(CAN)
            identifier = getattr(can, "identifier", None)
            if identifier is not None:
                fields["CAN.identifier"] = f"0x{identifier:x}"

            length = getattr(can, "length", None)
            if length is not None:
                fields["CAN.length"] = str(length)

            flags = getattr(can, "flags", None)
            if flags is not None and int(flags) != 0:
                cls_name = type(can).__name__
                flags_repr = can.sprintf("%%%s.flags%%" % cls_name)
                if flags_repr and flags_repr not in ("??", "0x0", "0"):
                    fields["CAN.flags"] = flags_repr

            fields["CAN.variant"] = "CANFD" if has_canfd else "CAN"
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse can layer: {ex}")
        return "can", fields

    @classmethod
    def _extract_usb_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract USB bus number, device address, transfer function and direction from USBpcap header."""
        from scapy.layers.usb import USBpcap

        if not packet.haslayer(USBpcap):
            return None, {}

        fields: dict[str, str] = {}
        try:
            usb = packet.getlayer(USBpcap)
            bus = getattr(usb, "bus", None)
            if bus is not None:
                fields["USB.bus"] = str(bus)
            device = getattr(usb, "device", None)
            if device is not None:
                fields["USB.device"] = str(device)
            func_repr = usb.sprintf("%USBpcap.function%")
            if func_repr and func_repr not in ("??", "None"):
                fields["USB.function"] = func_repr
            info = getattr(usb, "info", None)
            if info is not None:
                fields["USB.direction"] = "device_to_host" if (int(info) & 1) else "host_to_device"
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse usb layer: {ex}")
        return "usb", fields

    @classmethod
    def _extract_dot15d4_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract IEEE 802.15.4 frame type, PAN ID and short addresses."""
        from scapy.layers.dot15d4 import Dot15d4, Dot15d4FCS, Dot15d4Data, Dot15d4Beacon

        has_fcs = packet.haslayer(Dot15d4FCS)
        has_base = packet.haslayer(Dot15d4)
        if not has_base and not has_fcs:
            return None, {}

        fields: dict[str, str] = {}
        try:
            frame = packet.getlayer(Dot15d4FCS) if has_fcs else packet.getlayer(Dot15d4)
            cls_name = type(frame).__name__
            frame_type = frame.sprintf("%%%s.fcf_frametype%%" % cls_name)
            if frame_type and frame_type not in ("??", "None"):
                fields["Dot15d4.frame_type"] = frame_type

            if packet.haslayer(Dot15d4Data):
                data_layer = packet.getlayer(Dot15d4Data)
                dest_panid = getattr(data_layer, "dest_panid", None)
                if dest_panid is not None:
                    fields["Dot15d4.dest_panid"] = f"0x{dest_panid:04x}"
                dest_addr = getattr(data_layer, "dest_addr", None)
                if dest_addr is not None:
                    fields["Dot15d4.dest_addr"] = str(dest_addr)
                src_addr = getattr(data_layer, "src_addr", None)
                if src_addr is not None:
                    fields["Dot15d4.src_addr"] = str(src_addr)

            if packet.haslayer(Dot15d4Beacon):
                beacon = packet.getlayer(Dot15d4Beacon)
                src_panid = getattr(beacon, "src_panid", None)
                if src_panid is not None:
                    fields["Dot15d4.src_panid"] = f"0x{src_panid:04x}"
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse dot15d4 layer: {ex}")
        return "dot15d4", fields

    @classmethod
    def _extract_zigbee_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract ZigBee NWK frame type, source/destination address and APS cluster/profile."""
        from scapy.layers.zigbee import ZigbeeNWK, ZigbeeAppDataPayload

        if not packet.haslayer(ZigbeeNWK):
            return None, {}

        fields: dict[str, str] = {}
        try:
            nwk = packet.getlayer(ZigbeeNWK)
            frame_type = nwk.sprintf("%ZigbeeNWK.frametype%")
            if frame_type and frame_type not in ("??", "None"):
                fields["ZigBee.frame_type"] = frame_type

            destination = getattr(nwk, "destination", None)
            if destination is not None:
                fields["ZigBee.destination"] = f"0x{destination:04x}"

            source = getattr(nwk, "source", None)
            if source is not None:
                fields["ZigBee.source"] = f"0x{source:04x}"

            radius = getattr(nwk, "radius", None)
            if radius is not None:
                fields["ZigBee.radius"] = str(radius)

            if packet.haslayer(ZigbeeAppDataPayload):
                app = packet.getlayer(ZigbeeAppDataPayload)
                cluster = getattr(app, "cluster", None)
                if cluster is not None:
                    fields["ZigBee.cluster"] = f"0x{cluster:04x}"
                profile = getattr(app, "profile", None)
                if profile is not None:
                    fields["ZigBee.profile"] = f"0x{profile:04x}"
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse zigbee layer: {ex}")
        return "zigbee", fields

    @classmethod
    def _extract_sixlowpan_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract 6LoWPAN fragmentation datagram size/tag and mesh routing hop count."""
        from scapy.layers.sixlowpan import (
            SixLoWPAN,
            LoWPANFragmentationFirst,
            LoWPANFragmentationSubsequent,
            LoWPANMesh,
        )

        if not packet.haslayer(SixLoWPAN):
            return None, {}

        fields: dict[str, str] = {}
        try:
            if packet.haslayer(LoWPANFragmentationFirst):
                frag = packet.getlayer(LoWPANFragmentationFirst)
                dg_size = getattr(frag, "datagramSize", None)
                if dg_size is not None:
                    fields["6LoWPAN.datagram_size"] = str(dg_size)
                dg_tag = getattr(frag, "datagramTag", None)
                if dg_tag is not None:
                    fields["6LoWPAN.datagram_tag"] = f"0x{dg_tag:04x}"
                fields["6LoWPAN.fragment"] = "first"

            elif packet.haslayer(LoWPANFragmentationSubsequent):
                frag = packet.getlayer(LoWPANFragmentationSubsequent)
                dg_size = getattr(frag, "datagramSize", None)
                if dg_size is not None:
                    fields["6LoWPAN.datagram_size"] = str(dg_size)
                offset = getattr(frag, "datagramOffset", None)
                if offset is not None:
                    fields["6LoWPAN.datagram_offset"] = str(offset)
                fields["6LoWPAN.fragment"] = "subsequent"

            if packet.haslayer(LoWPANMesh):
                mesh = packet.getlayer(LoWPANMesh)
                hops = getattr(mesh, "hopsLeft", None)
                if hops is not None:
                    fields["6LoWPAN.mesh_hops_left"] = str(hops)
                src = getattr(mesh, "src", None)
                if src is not None:
                    fields["6LoWPAN.mesh_src"] = str(src)
                dst = getattr(mesh, "dst", None)
                if dst is not None:
                    fields["6LoWPAN.mesh_dst"] = str(dst)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse sixlowpan layer: {ex}")
        return "sixlowpan", fields

    @classmethod
    def _extract_pflog_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract BSD pf firewall log: action, reason, interface, direction."""
        if not packet.haslayer("PFLog"):
            return None, {}

        fields: dict[str, str] = {}
        try:
            from scapy.layers.pflog import PFLog

            pf = packet.getlayer(PFLog)
            action = pf.sprintf("%PFLog.action%")
            if action and action not in ("??", "None"):
                fields["PFLog.action"] = action
            reason = pf.sprintf("%PFLog.reason%")
            if reason and reason not in ("??", "None"):
                fields["PFLog.reason"] = reason
            direction = pf.sprintf("%PFLog.direction%")
            if direction and direction not in ("??", "None", "inout"):
                fields["PFLog.direction"] = direction
            iface = getattr(pf, "iface", None)
            if iface:
                iface_str = iface.rstrip(b"\x00").decode("utf-8", errors="ignore") if isinstance(iface, bytes) else str(iface)
                if iface_str:
                    fields["PFLog.iface"] = iface_str
            ruleset = getattr(pf, "ruleset", None)
            if ruleset:
                ruleset_str = ruleset.rstrip(b"\x00").decode("utf-8", errors="ignore") if isinstance(ruleset, bytes) else str(ruleset)
                if ruleset_str:
                    fields["PFLog.ruleset"] = ruleset_str
            rule_num = getattr(pf, "rulenumber", None)
            if rule_num is not None:
                fields["PFLog.rule_number"] = str(rule_num)
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse pflog layer: {ex}")
        return "pflog", fields

    @classmethod
    def _extract_llc_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract IEEE 802.2 LLC DSAP/SSAP service access points."""
        if not packet.haslayer("LLC"):
            return None, {}

        # Well-known DSAP/SSAP values
        _SAP_NAMES = {
            0x00: "Null",
            0x02: "LLC-SLM",
            0x06: "IP",
            0x08: "SNA Path Control",
            0x0E: "PROWAY-LAN",
            0x42: "Spanning Tree BPDU",
            0x4E: "MMS (EIA-RS-511)",
            0x7E: "ISO 8208 (X.25)",
            0x80: "XNS",
            0x86: "Nestar",
            0x8E: "PROWAY-LAN",
            0x98: "ARP",
            0xAA: "SNAP",
            0xBC: "Banyan VINES",
            0xE0: "Novell NetWare",
            0xF0: "NetBIOS",
            0xF4: "LAN Management",
            0xF8: "Remote Program Load",
            0xFE: "ISO CLNS",
            0xFF: "Global DSAP",
        }

        fields: dict[str, str] = {}
        try:
            from scapy.layers.clns import LLC

            llc = packet.getlayer(LLC)
            dsap = getattr(llc, "dsap", None)
            ssap = getattr(llc, "ssap", None)
            if dsap is not None:
                fields["LLC.dsap"] = _SAP_NAMES.get(dsap, f"0x{dsap:02x}")
            if ssap is not None:
                fields["LLC.ssap"] = _SAP_NAMES.get(ssap, f"0x{ssap:02x}")
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse llc layer: {ex}")
        return "llc", fields

    @classmethod
    def _extract_irda_metadata(cls, packet: Packet) -> tuple[str | None, dict[str, str]]:
        """Extract IrDA IrLAP frame type and IrLMP device name."""
        from scapy.layers.ir import IrLAPHead, IrLAPCommand, IrLMP

        if not packet.haslayer(IrLAPHead):
            return None, {}

        fields: dict[str, str] = {}
        try:
            head = packet.getlayer(IrLAPHead)
            frame_type = head.sprintf("%IrLAPHead.Type%")
            if frame_type and frame_type not in ("??", "None"):
                fields["IrDA.frame_type"] = frame_type

            if packet.haslayer(IrLAPCommand):
                cmd = packet.getlayer(IrLAPCommand)
                src = getattr(cmd, "Source_address", None)
                if src is not None:
                    fields["IrDA.src_address"] = f"0x{src:08x}"

            if packet.haslayer(IrLMP):
                lmp = packet.getlayer(IrLMP)
                device_name = getattr(lmp, "Device_name", None)
                if device_name:
                    if isinstance(device_name, bytes):
                        name_str = device_name.decode("utf-8", errors="ignore").strip()
                    else:
                        name_str = str(device_name).strip()
                    if name_str:
                        fields["IrDA.device_name"] = name_str
        except Exception as ex:
            MutyLogger.get_instance().debug(f"failed to parse irda layer: {ex}")
        return "irda", fields

    @classmethod
    def _get_packet_protocol_metadata(
        cls,
        packet: Packet,
        analyze_packet: bool,
    ) -> dict[str, str]:
        fallback_top_layer = cls._fallback_top_layer(packet)
        if not analyze_packet:
            return {"top_layer": fallback_top_layer}

        payload_text = cls._get_payload_text(packet)
        protocol_extractors = (
            lambda: cls._extract_http_metadata(packet, payload_text),
            lambda: cls._extract_dns_metadata(packet),
            lambda: cls._extract_dhcp_metadata(packet),
            lambda: cls._extract_dhcp6_metadata(packet),
            lambda: cls._extract_tls_metadata(packet),
            lambda: cls._extract_snmp_metadata(packet),
            lambda: cls._extract_ntp_metadata(packet),
            lambda: cls._extract_radius_metadata(packet),
            lambda: cls._extract_rtp_metadata(packet),
            lambda: cls._extract_isakmp_metadata(packet),
            lambda: cls._extract_rip_metadata(packet),
            lambda: cls._extract_netbios_metadata(packet),
            lambda: cls._extract_eap_metadata(packet),
            lambda: cls._extract_smb2_metadata(packet),
            lambda: cls._extract_smb1_metadata(packet),
            lambda: cls._extract_tftp_metadata(packet),
            lambda: cls._extract_vrrp_metadata(packet),
            lambda: cls._extract_hsrp_metadata(packet),
            lambda: cls._extract_l2tp_metadata(packet),
            lambda: cls._extract_dot11_metadata(packet),
            lambda: cls._extract_llmnr_metadata(packet),
            lambda: cls._extract_pptp_metadata(packet),
            lambda: cls._extract_mgcp_metadata(packet),
            lambda: cls._extract_skinny_metadata(packet),
            lambda: cls._extract_ntlm_metadata(packet),
            lambda: cls._extract_spnego_metadata(packet),
            lambda: cls._extract_kerberos_metadata(packet),
            lambda: cls._extract_ldap_metadata(packet),
            lambda: cls._extract_dcerpc_metadata(packet),
            lambda: cls._extract_lltd_metadata(packet),
            lambda: cls._extract_mobileip_metadata(packet),
            lambda: cls._extract_ipsec_metadata(packet),
            lambda: cls._extract_ppp_metadata(packet),
            lambda: cls._extract_netflow_metadata(packet),
            lambda: cls._extract_gprs_metadata(packet),
            lambda: cls._extract_bluetooth_hci_metadata(packet),
            lambda: cls._extract_btle_metadata(packet),
            lambda: cls._extract_can_metadata(packet),
            lambda: cls._extract_usb_metadata(packet),
            lambda: cls._extract_dot15d4_metadata(packet),
            lambda: cls._extract_zigbee_metadata(packet),
            lambda: cls._extract_sixlowpan_metadata(packet),
            lambda: cls._extract_pflog_metadata(packet),
            lambda: cls._extract_llc_metadata(packet),
            lambda: cls._extract_irda_metadata(packet),
            lambda: cls._extract_ssh_metadata(packet, payload_text),
            lambda: cls._extract_line_protocol_metadata(packet, payload_text),
            lambda: cls._extract_icmp_metadata(packet),
            lambda: cls._extract_icmpv6_metadata(packet),
            lambda: cls._extract_arp_metadata(packet),
            lambda: cls._extract_gre_metadata(packet),
            lambda: cls._extract_vxlan_metadata(packet),
            lambda: cls._extract_sctp_metadata(packet),
            lambda: cls._extract_bootp_metadata(packet),
        )

        for extractor in protocol_extractors:
            protocol, fields = extractor()
            if protocol:
                return {"top_layer": protocol, **fields}

        return {"top_layer": fallback_top_layer}

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        # process record
        evt_json = self._pkt_to_dict(record)
        evt_json.update(
            self._get_packet_protocol_metadata(
                record,
                self._analyze_packet,
            )
        )

        # use the last layer as gradient (all TCP packets are gonna be the same color, etc)
        d: dict = {}
        last_layer = record.lastlayer().name
        event_code_str = self._event_code_cache.get(last_layer)
        if event_code_str is None:
            event_code_str = str(muty.crypto.hash_crc24(last_layer))
            self._event_code_cache[last_layer] = event_code_str
        d["event.code"] = event_code_str

        # map fields through the mapping engine
        for k, v in evt_json.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        # normalize timestamp
        timestamp: str = datetime.fromtimestamp(
            float(record.time), tz=timezone.utc
        ).isoformat()

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
            event_sequence=record_idx + 1 if self._wireshark_seq_align else record_idx,
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

        doc_idx = 0
        # Cache per-ingestion constants to avoid per-packet dict lookups and repeated hashes
        self._analyze_packet: bool = self._plugin_params.custom_parameters.get("analyze", False)
        self._wireshark_seq_align: bool = self._plugin_params.custom_parameters.get(
            "wireshark_sequence_alignment", False
        )
        self._event_code_cache: dict[str, str] = {}

        for pkt in parser:
            await self.process_record(pkt, doc_idx, flt=flt)
            doc_idx += 1

        return stats.status
