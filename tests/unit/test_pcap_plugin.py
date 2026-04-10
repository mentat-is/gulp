import pytest
from scapy.all import BOOTP, DHCP, Ether, IP, Raw, TCP, UDP, load_layer

from gulp.plugins.pcap import Plugin


load_layer("http")


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