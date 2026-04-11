import pytest
from scapy.all import BOOTP, DHCP, EDecimal, Ether, ICMP, IP, Raw, TCP, UDP, load_layer
from scapy.layers.inet6 import ICMPv6EchoRequest, ICMPv6DestUnreach, IPv6

from gulp.plugins.pcap import Plugin
from gulp.structs import GulpPluginParameters


load_layer("http")
load_layer("tls")


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_http_from_payload_on_nonstandard_port():
    packet = (
        Ether()
        / IP(src="10.0.0.10", dst="10.0.0.20")
        / TCP(sport=49152, dport=8088)
        / Raw(
            load=(
                b"GET /index.html?x=1 HTTP/1.1\r\n"
                b"Host: example.com\r\n"
                b"User-Agent: curl/8.0\r\n\r\n"
            )
        )
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "http"
    assert metadata["HTTP.request.method"] == "GET"
    assert metadata["HTTP.request.path"] == "/index.html?x=1"
    assert metadata["HTTP.request.url"] == "http://example.com/index.html?x=1"
    assert metadata["HTTP.request.user_agent"] == "curl/8.0"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dhcp_fields():
    packet = (
        Ether()
        / IP(src="0.0.0.0", dst="255.255.255.255")
        / UDP(sport=68, dport=67)
        / BOOTP(chaddr=b"\xaa\xbb\xcc\xdd\xee\xff")
        / DHCP(options=[("message-type", "discover"), ("hostname", "lab-client"), "end"])
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dhcp"
    assert metadata["DHCP.message_type"] == "discover"
    assert metadata["DHCP.hostname"] == "lab-client"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ssh_banner():
    packet = (
        Ether()
        / IP(src="10.1.0.5", dst="10.1.0.7")
        / TCP(sport=22, dport=50123)
        / Raw(load=b"SSH-2.0-OpenSSH_9.8\r\n")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ssh"
    assert metadata["SSH.banner"] == "SSH-2.0-OpenSSH_9.8"


@pytest.mark.unit
def test_pcap_protocol_metadata_keeps_layer_based_top_layer_when_analysis_disabled():
    packet = Ether() / IP(src="192.168.1.2", dst="192.168.1.3") / TCP(sport=12345, dport=80) / Raw(load=b"GET / HTTP/1.1\r\n\r\n")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=False)

    assert metadata == {"top_layer": "tcp"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pcap_event_sequence_aligns_with_wireshark():
    packet = Ether() / IP(src="192.168.1.2", dst="192.168.1.3") / UDP(sport=1234, dport=5678)
    plugin = Plugin("/tmp/pcap.py", "gulp.plugins.pcap")
    plugin._plugin_params = GulpPluginParameters(
        custom_parameters={"wireshark_sequence_alignment": True}
    )
    plugin._operation_id = "op"
    plugin._context_id = "ctx"
    plugin._source_id = "src"
    plugin._file_path = "/tmp/file.pcap"
    packet.time = EDecimal(1)

    doc = await plugin._record_to_gulp_document(packet, 10)
    assert doc.event_sequence == 11


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pcap_event_sequence_default_is_zero_based():
    packet = Ether() / IP(src="192.168.1.2", dst="192.168.1.3") / UDP(sport=1234, dport=5678)
    plugin = Plugin("/tmp/pcap.py", "gulp.plugins.pcap")
    plugin._plugin_params = GulpPluginParameters(custom_parameters={})
    plugin._operation_id = "op"
    plugin._context_id = "ctx"
    plugin._source_id = "src"
    plugin._file_path = "/tmp/file.pcap"
    packet.time = EDecimal(1)

    doc = await plugin._record_to_gulp_document(packet, 10)
    assert doc.event_sequence == 10


@pytest.mark.unit
def test_pcap_protocol_layer_detection_ignores_port_on_tcp_only_packet():
    """Protocol must be identified by actual packet layers, not port numbers."""
    packet = Ether() / IP(src="10.0.0.2", dst="10.0.0.3") / TCP(sport=80, dport=53000)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    # No HTTP layer in packet, so falls back to transport-layer name
    assert metadata == {"top_layer": "tcp"}


# ---------------------------------------------------------------------------
# ICMP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_icmp_echo_request():
    packet = Ether() / IP(src="1.2.3.4", dst="5.6.7.8") / ICMP(type=8, code=0)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "icmp"
    assert metadata["ICMP.type"] == "echo-request"
    assert "ICMP.code" not in metadata  # code 0 is omitted


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_icmp_dest_unreachable_with_code():
    packet = Ether() / IP(src="1.2.3.4", dst="5.6.7.8") / ICMP(type=3, code=1)  # host unreachable

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "icmp"
    assert metadata["ICMP.type"] == "dest-unreach"
    assert metadata["ICMP.code"] == "1"


# ---------------------------------------------------------------------------
# ICMPv6
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_icmpv6_echo_request():
    packet = Ether() / IPv6(src="::1", dst="::2") / ICMPv6EchoRequest()

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "icmpv6"
    assert metadata["ICMPv6.type_name"] == "EchoRequest"
    assert metadata["ICMPv6.type"] == "128"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_icmpv6_dest_unreach():
    packet = Ether() / IPv6(src="::1", dst="::2") / ICMPv6DestUnreach()

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "icmpv6"
    assert metadata["ICMPv6.type_name"] == "DestUnreach"


# ---------------------------------------------------------------------------
# ARP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_arp_who_has():
    from scapy.all import ARP
    packet = Ether() / ARP(op=1, psrc="192.168.1.1", pdst="192.168.1.2")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "arp"
    assert metadata["ARP.operation"] == "who-has"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_arp_is_at():
    from scapy.all import ARP
    packet = Ether() / ARP(op=2, psrc="192.168.1.2", pdst="192.168.1.1")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "arp"
    assert metadata["ARP.operation"] == "is-at"


# ---------------------------------------------------------------------------
# DNS qtype
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dns_qtype():
    from scapy.all import DNS, DNSQR
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="8.8.8.8")
        / UDP(sport=12345, dport=53)
        / DNS(rd=1, qd=DNSQR(qname="example.com", qtype="AAAA"))
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dns"
    assert metadata["DNS.qd"] == "example.com"
    assert metadata["DNS.qtype"] == "AAAA"


# ---------------------------------------------------------------------------
# TLS metadata (version + cipher suites)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_tls_version_and_cipher_suites():
    from scapy.layers.tls.all import TLSClientHello
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(sport=12345, dport=443)
        / TLSClientHello(version=0x0303, ciphers=[0x002f, 0xc02c])
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "tls"
    assert metadata["TLS.version"] == "TLSv1.2"
    assert "TLS.client.cipher_suites" in metadata
    assert "TLS_RSA_WITH_AES_128_CBC_SHA" in metadata["TLS.client.cipher_suites"]


# ---------------------------------------------------------------------------
# SNMP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_snmp_v2c():
    from scapy.layers.snmp import SNMP
    packet = (
        Ether()
        / IP(src="10.0.0.5", dst="10.0.0.6")
        / UDP(sport=56789, dport=161)
        / SNMP(version=1, community=b"public")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "snmp"
    assert metadata["SNMP.version"] == "1"
    assert metadata["SNMP.community"] == "public"
    assert "SNMP.pdu_type" in metadata


# ---------------------------------------------------------------------------
# NTP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ntp_client():
    from scapy.layers.ntp import NTP
    packet = (
        Ether()
        / IP(src="192.168.0.10", dst="pool.ntp.org")
        / UDP(sport=54321, dport=123)
        / NTP()
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ntp"
    assert "NTP.mode" in metadata
    assert "NTP.stratum" in metadata
    assert "NTP.version" in metadata


# ---------------------------------------------------------------------------
# GRE
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_gre_encapsulated_proto():
    from scapy.layers.inet import GRE
    packet = Ether() / IP(src="10.0.0.1", dst="10.0.0.2") / GRE(proto=0x0800) / IP()

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "gre"
    assert metadata["GRE.encapsulated_proto"] == "IPv4"


# ---------------------------------------------------------------------------
# VXLAN
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_vxlan_vni():
    from scapy.all import VXLAN
    packet = (
        Ether()
        / IP(src="192.168.1.1", dst="192.168.1.2")
        / UDP(sport=12345, dport=4789)
        / VXLAN(vni=42)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "vxlan"
    assert metadata["VXLAN.vni"] == "42"


# ---------------------------------------------------------------------------
# SCTP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_sctp_ports():
    from scapy.layers.sctp import SCTP
    packet = Ether() / IP(src="10.0.0.3", dst="10.0.0.4") / SCTP(sport=2905, dport=2905)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "sctp"
    assert metadata["SCTP.sport"] == "2905"
    assert metadata["SCTP.dport"] == "2905"


# ---------------------------------------------------------------------------
# RADIUS
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_radius_access_request():
    from scapy.layers.radius import Radius
    packet = (
        Ether()
        / IP(src="10.0.0.10", dst="10.0.0.11")
        / UDP(sport=60000, dport=1812)
        / Radius(code=1, id=1, authenticator=b"\x00" * 16)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "radius"
    assert metadata["RADIUS.code"] == "Access-Request"


# ---------------------------------------------------------------------------
# RTP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_rtp_codec():
    from scapy.layers.rtp import RTP
    packet = (
        Ether()
        / IP(src="192.168.0.1", dst="192.168.0.2")
        / UDP(sport=5004, dport=5004)
        / RTP(payload_type=8)  # G.711 PCMA
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "rtp"
    assert "PCMA" in metadata["RTP.payload_type"]
    assert "RTP.ssrc" in metadata


# ---------------------------------------------------------------------------
# DHCPv6
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dhcp6_solicit():
    from scapy.layers.dhcp6 import DHCP6_Solicit
    packet = (
        Ether()
        / IPv6(src="fe80::1", dst="ff02::1:2")
        / UDP(sport=546, dport=547)
        / DHCP6_Solicit()
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dhcp6"
    assert metadata["DHCP6.msgtype"] == "SOLICIT"


# ---------------------------------------------------------------------------
# ISAKMP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_isakmp_aggressive():
    from scapy.layers.isakmp import ISAKMP
    packet = (
        Ether()
        / IP(src="1.2.3.4", dst="5.6.7.8")
        / UDP(sport=500, dport=500)
        / ISAKMP(exch_type=4)  # aggressive mode
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "isakmp"
    assert metadata["ISAKMP.exchange_type"] == "aggressive"


# ---------------------------------------------------------------------------
# RIP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_rip_response():
    from scapy.layers.rip import RIP
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="224.0.0.9")
        / UDP(sport=520, dport=520)
        / RIP(cmd=2, version=2)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "rip"
    assert metadata["RIP.command"] == "resp"
    assert metadata["RIP.version"] == "2"


# ---------------------------------------------------------------------------
# EAP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_eap_request():
    from scapy.layers.eap import EAP
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / UDP(dport=1812)
        / EAP(code=1)  # Request
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "eap"
    assert metadata["EAP.code"] == "Request"


# ---------------------------------------------------------------------------
# NetBIOS NS
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_netbios_query_name():
    from scapy.layers.netbios import NBNSQueryRequest
    packet = (
        Ether()
        / IP(src="192.168.1.5", dst="192.168.1.255")
        / UDP(sport=137, dport=137)
        / NBNSQueryRequest(QUESTION_NAME=b"WORKSTATION")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "netbios-ns"
    assert "WORKSTATION" in metadata["NetBIOS.query_name"]


# ---------------------------------------------------------------------------
# SMB2
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_smb2_command():
    from scapy.layers.smb2 import SMB2_Header
    packet = Ether() / IP(src="10.0.0.1", dst="10.0.0.2") / TCP(dport=445) / SMB2_Header(Command=0)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "smb2"
    assert metadata["SMB2.command"] == "SMB2_NEGOTIATE"


# ---------------------------------------------------------------------------
# TFTP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_tftp_rrq():
    from scapy.layers.tftp import TFTP, TFTP_RRQ
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / UDP(dport=69)
        / TFTP()
        / TFTP_RRQ(filename=b"firmware.bin", mode=b"octet")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "tftp"
    assert metadata["TFTP.op"] == "RRQ"
    assert metadata["TFTP.filename"] == "firmware.bin"


# ---------------------------------------------------------------------------
# VRRP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_vrrp():
    from scapy.layers.vrrp import VRRP
    packet = Ether() / IP(src="192.168.1.1", dst="224.0.0.18") / VRRP(vrid=5, priority=200, version=2)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "vrrp"
    assert metadata["VRRP.vrid"] == "5"
    assert metadata["VRRP.priority"] == "200"


# ---------------------------------------------------------------------------
# HSRP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_hsrp():
    from scapy.layers.hsrp import HSRP
    packet = Ether() / IP(src="10.0.0.1", dst="224.0.0.2") / HSRP(opcode=0, state=16, group=1, virtualIP="10.0.0.254")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "hsrp"
    assert metadata["HSRP.opcode"] == "Hello"
    assert metadata["HSRP.virtual_ip"] == "10.0.0.254"


# ---------------------------------------------------------------------------
# L2TP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_l2tp():
    from scapy.layers.l2tp import L2TP
    packet = Ether() / IP(src="1.2.3.4", dst="5.6.7.8") / UDP(dport=1701) / L2TP(tunnel_id=100, session_id=200)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "l2tp"
    assert metadata["L2TP.tunnel_id"] == "100"
    assert metadata["L2TP.session_id"] == "200"


# ---------------------------------------------------------------------------
# 802.11 (Dot11)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dot11_beacon():
    from scapy.layers.dot11 import Dot11, Dot11Beacon, Dot11Elt
    packet = (
        Dot11(
            type=0, subtype=8,
            addr1="ff:ff:ff:ff:ff:ff",
            addr2="aa:bb:cc:dd:ee:ff",
            addr3="aa:bb:cc:dd:ee:ff",
        )
        / Dot11Beacon()
        / Dot11Elt(ID="SSID", info=b"CorpWiFi")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dot11"
    assert metadata["Dot11.ssid"] == "CorpWiFi"
    assert metadata["Dot11.type"] == "Management"


# ---------------------------------------------------------------------------
# LLMNR
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_llmnr_query():
    from scapy.layers.llmnr import LLMNRQuery
    from scapy.layers.dns import DNSQR
    packet = (
        Ether()
        / IP(src="169.254.1.1", dst="224.0.0.252")
        / UDP(dport=5355)
        / LLMNRQuery(qd=DNSQR(qname="fileserver", qtype="A"))
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "llmnr"
    assert "fileserver" in metadata["LLMNR.qname"]
    assert metadata["LLMNR.qtype"] == "A"


# ---------------------------------------------------------------------------
# PPTP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_pptp():
    from scapy.layers.pptp import PPTP
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=1723)
        / PPTP(ctrl_msg_type=1)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "pptp"
    assert metadata["PPTP.ctrl_msg_type"] == "Start-Control-Connection-Request"


# ---------------------------------------------------------------------------
# MGCP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_mgcp():
    from scapy.layers.mgcp import MGCP
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / UDP(dport=2427)
        / MGCP(verb=b"CRCX", transaction_id=b"100", endpoint=b"aaln/1@gw.example.com", version=b"MGCP 1.0")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "mgcp"
    assert metadata["MGCP.verb"] == "CRCX"
    assert "aaln/1" in metadata["MGCP.endpoint"]


# ---------------------------------------------------------------------------
# Skinny (SCCP)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_skinny_register():
    from scapy.layers.skinny import Skinny
    packet = (
        Ether()
        / IP(src="10.0.0.5", dst="10.0.0.1")
        / TCP(dport=2000)
        / Skinny(msg=1)  # RegisterMessage
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "skinny"
    assert metadata["Skinny.msg"] == "RegisterMessage"


# ---------------------------------------------------------------------------
# NTLM
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ntlm_negotiate():
    from scapy.layers.ntlm import NTLM_NEGOTIATE
    packet = Ether() / IP(src="10.0.0.1", dst="10.0.0.2") / TCP(dport=445) / NTLM_NEGOTIATE()

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ntlm"
    assert metadata["NTLM.message_type"] == "NEGOTIATE_MESSAGE"


# ---------------------------------------------------------------------------
# BOOTP (legacy, no DHCP option 53)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_legacy_bootp():
    from scapy.all import BOOTP
    packet = (
        Ether()
        / IP(src="0.0.0.0", dst="255.255.255.255")
        / UDP(sport=68, dport=67)
        / BOOTP(op=1, yiaddr="10.0.0.50", siaddr="10.0.0.1", sname=b"bootserver\x00")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "bootp"
    assert metadata["BOOTP.op"] == "BOOTREQUEST"
    assert metadata["BOOTP.yiaddr"] == "10.0.0.50"
    assert metadata["BOOTP.siaddr"] == "10.0.0.1"
    assert metadata["BOOTP.sname"] == "bootserver"


@pytest.mark.unit
def test_pcap_protocol_metadata_dhcp_not_matched_as_bootp():
    """A modern DHCP packet should be classified as dhcp, not bootp."""
    from scapy.all import BOOTP, DHCP
    packet = (
        Ether()
        / IP(src="0.0.0.0", dst="255.255.255.255")
        / UDP(sport=68, dport=67)
        / BOOTP(op=1)
        / DHCP(options=[("message-type", "discover"), "end"])
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dhcp"


# ---------------------------------------------------------------------------
# Kerberos
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_kerberos_as_req():
    from scapy.layers.kerberos import KRB_AS_REQ, KRB_KDC_REQ_BODY
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=88)
        / KRB_AS_REQ(reqBody=KRB_KDC_REQ_BODY(realm="CORP.EXAMPLE.COM"))
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "kerberos"
    assert metadata["Kerberos.msg_type"] == "AS-REQ"
    assert metadata["Kerberos.realm"] == "CORP.EXAMPLE.COM"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_kerberos_tgs_req():
    from scapy.layers.kerberos import KRB_TGS_REQ, KRB_KDC_REQ_BODY
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=88)
        / KRB_TGS_REQ(reqBody=KRB_KDC_REQ_BODY(realm="CORP.EXAMPLE.COM"))
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "kerberos"
    assert metadata["Kerberos.msg_type"] == "TGS-REQ"


# ---------------------------------------------------------------------------
# LDAP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ldap_bind():
    from scapy.layers.ldap import LDAP, LDAP_BindRequest
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=389)
        / LDAP()
        / LDAP_BindRequest(bind_name=b"cn=admin,dc=example,dc=com")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ldap"
    assert metadata["LDAP.operation"] == "bind"
    assert "cn=admin" in metadata["LDAP.bind_dn"]


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ldap_search():
    from scapy.layers.ldap import LDAP, LDAP_SearchRequest
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=389)
        / LDAP()
        / LDAP_SearchRequest(baseObject=b"dc=example,dc=com", scope=2)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ldap"
    assert metadata["LDAP.operation"] == "search"
    assert metadata["LDAP.search_scope"] == "wholeSubtree"


# ---------------------------------------------------------------------------
# IPsec (ESP / AH)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ipsec_esp():
    from scapy.layers.ipsec import ESP
    packet = Ether() / IP(proto=50, src="1.2.3.4", dst="5.6.7.8") / ESP(spi=0xDEADBEEF, seq=42)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ipsec"
    assert metadata["IPsec.protocol"] == "ESP"
    assert metadata["IPsec.spi"] == str(0xDEADBEEF)
    assert metadata["IPsec.seq"] == "42"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ipsec_ah():
    from scapy.layers.ipsec import AH
    packet = Ether() / IP(proto=51, src="1.2.3.4", dst="5.6.7.8") / AH(spi=0xCAFEBABE, seq=7)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ipsec"
    assert metadata["IPsec.protocol"] == "AH"
    assert metadata["IPsec.spi"] == str(0xCAFEBABE)


# ---------------------------------------------------------------------------
# PPP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ppp_lcp():
    from scapy.layers.ppp import PPP
    packet = PPP(proto=0xC021)  # LCP

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ppp"
    assert metadata["PPP.proto"] == "Link Control Protocol"


# ---------------------------------------------------------------------------
# NetFlow
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_netflow_v5():
    from scapy.layers.netflow import NetflowHeader
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / UDP(dport=2055)
        / NetflowHeader(version=5)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "netflow"
    assert metadata["NetFlow.version"] == "5"


# ---------------------------------------------------------------------------
# DCE/RPC
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dcerpc_request():
    from scapy.layers.dcerpc import DceRpc5, DceRpc5Request
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=135)
        / DceRpc5(ptype=0)
        / DceRpc5Request(opnum=3)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dcerpc"
    assert metadata["DCERPC.version"] == "5"
    assert metadata["DCERPC.ptype"] == "request"
    assert metadata["DCERPC.opnum"] == "3"


# ---------------------------------------------------------------------------
# SMB1
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_smb1_negotiate():
    from scapy.layers.smb import SMB_Header, NBTSession
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=445)
        / NBTSession()
        / SMB_Header(Command=0x72, Status=0x00000000)  # 0x72 = SMB_COM_NEGOTIATE
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "smb1"
    assert metadata["SMB1.command"] == "SMB_COM_NEGOTIATE"
    assert metadata["SMB1.status"] == "STATUS_SUCCESS"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_smb1_session_setup():
    from scapy.layers.smb import SMB_Header, SMBSession_Setup_AndX_Request, NBTSession
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=445)
        / NBTSession()
        / SMB_Header(Command=0x73)  # SMB_COM_SESSION_SETUP_ANDX
        / SMBSession_Setup_AndX_Request(AccountName="TESTUSER", PrimaryDomain="CORP")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "smb1"
    assert "SMB_COM_SESSION_SETUP_ANDX" in metadata.get("SMB1.command", "")
    assert metadata["SMB1.account_name"] == "TESTUSER"
    assert metadata["SMB1.domain"] == "CORP"


# ---------------------------------------------------------------------------
# SPNEGO
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_spnego_init():
    from scapy.layers.spnego import SPNEGO_negTokenInit, SPNEGO_MechTypes, SPNEGO_MechType
    from scapy.asn1.asn1 import ASN1_OID
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / TCP(dport=445)
        / SPNEGO_negTokenInit(
            mechTypes=SPNEGO_MechTypes(
                mechTypes=[SPNEGO_MechType(oid=ASN1_OID("1.3.6.1.4.1.311.2.2.10"))]
            )
        )
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "spnego"
    assert "NTLMSSP" in metadata.get("SPNEGO.mechanisms", "")


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_spnego_response():
    from scapy.layers.spnego import SPNEGO_negTokenResp
    from scapy.asn1.asn1 import ASN1_ENUMERATED
    packet = (
        Ether()
        / IP(src="10.0.0.2", dst="10.0.0.1")
        / TCP(sport=445)
        / SPNEGO_negTokenResp(negResult=ASN1_ENUMERATED(0))
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "spnego"
    assert metadata["SPNEGO.result"] == "accept-completed"


# ---------------------------------------------------------------------------
# LLTD
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_lltd_discover():
    from scapy.layers.lltd import LLTD
    packet = Ether() / LLTD(function=0)  # 0 = Discover

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "lltd"
    assert metadata["LLTD.function"] == "Discover"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_lltd_hello_with_hostname():
    from scapy.layers.lltd import LLTD, LLTDHello, LLTDAttributeMachineName
    packet = (
        Ether()
        / LLTD(function=1)  # 1 = Hello
        / LLTDHello()
        / LLTDAttributeMachineName(hostname="DESKTOP-CORP01")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "lltd"
    assert metadata["LLTD.function"] == "Hello"
    assert metadata["LLTD.hostname"] == "DESKTOP-CORP01"


# ---------------------------------------------------------------------------
# Mobile IP
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_mobileip_rrq():
    from scapy.layers.mobileip import MobileIP, MobileIPRRQ
    packet = (
        Ether()
        / IP(src="10.0.0.1", dst="10.0.0.2")
        / UDP(dport=434)
        / MobileIP(type=1)  # 1 = RRQ
        / MobileIPRRQ(homeaddr="192.168.1.10", haaddr="10.0.0.254", coaddr="172.16.0.5")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "mobileip"
    assert metadata["MobileIP.type"] == "RRQ"
    assert metadata["MobileIP.homeaddr"] == "192.168.1.10"
    assert metadata["MobileIP.haaddr"] == "10.0.0.254"
    assert metadata["MobileIP.coaddr"] == "172.16.0.5"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_mobileip_rrp():
    from scapy.layers.mobileip import MobileIP, MobileIPRRP
    packet = (
        Ether()
        / IP(src="10.0.0.254", dst="10.0.0.1")
        / UDP(dport=434)
        / MobileIP(type=3)  # 3 = RRP
        / MobileIPRRP(code=0, homeaddr="192.168.1.10", haaddr="10.0.0.254")
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "mobileip"
    assert metadata["MobileIP.type"] == "RRP"
    assert metadata["MobileIP.code"] == "0"
    assert metadata["MobileIP.homeaddr"] == "192.168.1.10"


# ---------------------------------------------------------------------------
# GPRS
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_gprs():
    from scapy.layers.gprs import GPRS
    packet = Ether() / IP(src="10.0.0.1", dst="10.0.0.2") / UDP(dport=2152) / GPRS()

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "gprs"


# ---------------------------------------------------------------------------
# Bluetooth Classic (HCI)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_bluetooth_hci_acl():
    from scapy.layers.bluetooth import HCI_Hdr, HCI_ACL_Hdr
    packet = HCI_Hdr(type=2) / HCI_ACL_Hdr(handle=42)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "bluetooth"
    assert metadata["BT.hci_type"] == "ACL Data"
    assert metadata["BT.acl_handle"] == "42"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_bluetooth_hci_command():
    from scapy.layers.bluetooth import HCI_Hdr, HCI_Command_Hdr
    packet = HCI_Hdr(type=1) / HCI_Command_Hdr(ogf=4, ocf=1)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "bluetooth"
    assert metadata["BT.hci_type"] == "Command"
    assert metadata["BT.cmd_ogf"] == "4"
    assert metadata["BT.cmd_ocf"] == "1"


# ---------------------------------------------------------------------------
# Bluetooth LE (BTLE)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_btle_advertising_ind():
    from scapy.layers.bluetooth4LE import BTLE, BTLE_ADV, BTLE_ADV_IND
    packet = BTLE(access_addr=0x8E89BED6) / BTLE_ADV(PDU_type=0) / BTLE_ADV_IND(AdvA="aa:bb:cc:dd:ee:ff")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "btle"
    assert metadata["BTLE.access_addr"] == "0x8e89bed6"
    assert metadata["BTLE.pdu_type"] == "ADV_IND"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_btle_access_addr_only():
    from scapy.layers.bluetooth4LE import BTLE
    packet = BTLE(access_addr=0x12345678)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "btle"
    assert metadata["BTLE.access_addr"] == "0x12345678"


# ---------------------------------------------------------------------------
# CAN bus
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_can_identifier():
    from scapy.layers.can import CAN
    packet = CAN(identifier=0x123, length=4, data=b"\x01\x02\x03\x04")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "can"
    assert metadata["CAN.identifier"] == "0x123"
    assert metadata["CAN.length"] == "4"
    assert metadata["CAN.variant"] == "CAN"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_canfd_identifier():
    from scapy.layers.can import CANFD
    packet = CANFD(identifier=0x7ff, length=8, data=b"\xde\xad\xbe\xef" * 2)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "can"
    assert metadata["CAN.identifier"] == "0x7ff"
    assert metadata["CAN.variant"] == "CANFD"


# ---------------------------------------------------------------------------
# USB (USBpcap)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_usb_host_to_device():
    from scapy.layers.usb import USBpcap
    packet = USBpcap(bus=1, device=3, info=0)  # info=0 → host_to_device

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "usb"
    assert metadata["USB.bus"] == "1"
    assert metadata["USB.device"] == "3"
    assert metadata["USB.direction"] == "host_to_device"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_usb_device_to_host():
    from scapy.layers.usb import USBpcap
    packet = USBpcap(bus=2, device=5, info=1)  # info bit 0 = 1 → device_to_host

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "usb"
    assert metadata["USB.direction"] == "device_to_host"


# ---------------------------------------------------------------------------
# IEEE 802.15.4
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dot15d4_data_frame():
    from scapy.layers.dot15d4 import Dot15d4, Dot15d4Data
    packet = (
        Dot15d4(fcf_frametype=1, fcf_srcaddrmode=2, fcf_destaddrmode=2)  # 1 = Data, 16-bit addresses
        / Dot15d4Data(dest_panid=0x1234, dest_addr=0x5678, src_addr=0x9abc)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dot15d4"
    assert metadata["Dot15d4.frame_type"] == "Data"
    assert metadata["Dot15d4.dest_panid"] == "0x1234"
    assert metadata["Dot15d4.dest_addr"] == "22136"  # 0x5678 decimal
    assert metadata["Dot15d4.src_addr"] == "39612"   # 0x9abc decimal


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_dot15d4_beacon():
    from scapy.layers.dot15d4 import Dot15d4, Dot15d4Beacon
    packet = (
        Dot15d4(fcf_frametype=0)  # 0 = Beacon
        / Dot15d4Beacon(src_panid=0xABCD)
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "dot15d4"
    assert metadata["Dot15d4.frame_type"] == "Beacon"
    assert metadata["Dot15d4.src_panid"] == "0xabcd"


# ---------------------------------------------------------------------------
# ZigBee
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_zigbee_nwk_data():
    from scapy.layers.zigbee import ZigbeeNWK
    packet = ZigbeeNWK(frametype=0, destination=0x1234, source=0x5678, radius=3)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "zigbee"
    assert metadata["ZigBee.frame_type"] == "data"
    assert metadata["ZigBee.destination"] == "0x1234"
    assert metadata["ZigBee.source"] == "0x5678"
    assert metadata["ZigBee.radius"] == "3"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_zigbee_nwk_with_aps_cluster():
    from scapy.layers.zigbee import ZigbeeNWK, ZigbeeAppDataPayload
    packet = (
        ZigbeeNWK(frametype=0, destination=0x0001, source=0x0002, radius=5)
        / ZigbeeAppDataPayload(cluster=0x0006, profile=0x0104)  # OnOff cluster, HA profile
    )

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "zigbee"
    assert metadata["ZigBee.cluster"] == "0x0006"
    assert metadata["ZigBee.profile"] == "0x0104"


# ---------------------------------------------------------------------------
# 6LoWPAN
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_sixlowpan_fragmented():
    from scapy.layers.sixlowpan import SixLoWPAN, LoWPANFragmentationFirst

    packet = SixLoWPAN() / LoWPANFragmentationFirst(datagramSize=256, datagramTag=0xABCD)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "sixlowpan"
    assert metadata["6LoWPAN.datagram_size"] == "256"
    assert metadata["6LoWPAN.datagram_tag"] == "0xabcd"
    assert metadata["6LoWPAN.fragment"] == "first"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_sixlowpan_mesh():
    from scapy.layers.sixlowpan import SixLoWPAN, LoWPANMesh

    packet = SixLoWPAN() / LoWPANMesh(hopsLeft=3, v=1, f=1, src=0x1111, dst=0x2222)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "sixlowpan"
    assert metadata["6LoWPAN.mesh_hops_left"] == "3"


# ---------------------------------------------------------------------------
# PFLog
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_pflog_pass():
    from scapy.layers.pflog import PFLog
    from scapy.layers.inet import IP

    packet = PFLog(action=0, reason=0, direction=1, iface=b"em0\x00") / IP(src="10.0.0.1", dst="10.0.0.2")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "pflog"
    assert metadata["PFLog.action"] == "pass"
    assert metadata["PFLog.reason"] == "match"
    assert metadata["PFLog.iface"] == "em0"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_pflog_drop():
    from scapy.layers.pflog import PFLog

    packet = PFLog(action=1, reason=0, direction=2, iface=b"bge0\x00")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "pflog"
    assert metadata["PFLog.action"] == "drop"
    assert metadata["PFLog.direction"] == "out"


# ---------------------------------------------------------------------------
# SSH (structured layers)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ssh_version_exchange():
    from scapy.layers.inet import IP, TCP
    from scapy.layers.ssh import SSH, SSHVersionExchange

    packet = IP() / TCP() / SSH() / SSHVersionExchange(lines=[b"SSH-2.0-OpenSSH_9.8\r\n"])

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ssh"
    assert "SSH-2.0-OpenSSH_9.8" in metadata.get("SSH.banner", "")


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_ssh_banner_text():
    from scapy.layers.inet import IP, TCP
    from scapy.packet import Raw

    # Plain text SSH banner (no structured SSH layer)
    raw_banner = b"SSH-2.0-PuTTY_0.78\r\n"
    packet = IP() / TCP() / Raw(load=raw_banner)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "ssh"
    assert metadata["SSH.banner"].startswith("SSH-2.0-")


# ---------------------------------------------------------------------------
# LLC
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_llc_clns():
    from scapy.layers.clns import LLC

    # ISO CLNS: DSAP=0xFE, SSAP=0xFE
    packet = LLC(dsap=0xFE, ssap=0xFE, ctrl=3)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "llc"
    assert metadata["LLC.dsap"] == "ISO CLNS"
    assert metadata["LLC.ssap"] == "ISO CLNS"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_llc_netbios():
    from scapy.layers.clns import LLC

    # NetBIOS: DSAP=0xF0, SSAP=0xF0
    packet = LLC(dsap=0xF0, ssap=0xF0, ctrl=3)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "llc"
    assert metadata["LLC.dsap"] == "NetBIOS"
    assert metadata["LLC.ssap"] == "NetBIOS"


# ---------------------------------------------------------------------------
# IrDA
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pcap_protocol_metadata_detects_irda_command():
    from scapy.layers.ir import IrLAPHead

    packet = IrLAPHead(Address=0x7F, Type=1)

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "irda"
    assert metadata["IrDA.frame_type"] == "Command"


@pytest.mark.unit
def test_pcap_protocol_metadata_detects_irda_with_device_name():
    from scapy.layers.ir import IrLAPHead, IrLMP

    packet = IrLAPHead(Address=0x10, Type=0) / IrLMP(Device_name=b"MyPDA")

    metadata = Plugin._get_packet_protocol_metadata(packet, analyze_packet=True)

    assert metadata["top_layer"] == "irda"
    assert metadata["IrDA.frame_type"] == "Response"
    assert metadata["IrDA.device_name"] == "MyPDA"
