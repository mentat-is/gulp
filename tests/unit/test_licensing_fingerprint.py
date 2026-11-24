"""Stable host identity selection for Docker licensing."""

import hashlib
import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

LICENSING = Path(__file__).resolve().parents[2] / "gulp-licensing"
pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(
        not LICENSING.is_dir(), reason="Internal gulp-licensing tools are not installed"
    ),
]


@pytest.fixture
def fingerprint() -> ModuleType:
    """Load the standalone helper without importing the gULP runtime."""
    path = LICENSING / "fingerprint.py"
    spec = importlib.util.spec_from_file_location("licensing_fingerprint", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _identity_sources(
    monkeypatch: pytest.MonkeyPatch,
    fingerprint: ModuleType,
    values: dict[str, str],
    container_marker: bool = True,
) -> None:
    """Simulate OS identity files while preserving unrelated filesystem checks."""
    exists = fingerprint.os.path.exists

    def identity_exists(path: str) -> bool:
        if path == fingerprint.HOST_MACHINE_ID_PATH:
            return path in values
        if path == "/.dockerenv":
            return container_marker
        return exists(path)

    monkeypatch.setattr(
        fingerprint, "_read", lambda path, **_kwargs: values.get(path, "")
    )
    monkeypatch.setattr(fingerprint.os.path, "exists", identity_exists)
    monkeypatch.setattr(fingerprint.platform, "node", lambda: "native-host")


def test_container_recreation_preserves_host_fingerprint(
    monkeypatch: pytest.MonkeyPatch, fingerprint: ModuleType
) -> None:
    """Container IDs and hostnames must not contribute to the Docker key."""
    machine_id = "1234567890abcdef1234567890abcdef"
    values = {fingerprint.HOST_MACHINE_ID_PATH: machine_id}
    _identity_sources(monkeypatch, fingerprint, values)
    expected = hashlib.sha256(f"host_machine_id={machine_id}".encode()).digest()

    for container_id in ("a" * 64, "b" * 64):
        values["/proc/self/cgroup"] = f"0::/docker/{container_id}"
        values["/proc/self/mountinfo"] = f"/docker/containers/{container_id}/hostname"
        values["/etc/hostname"] = container_id[:12]
        assert fingerprint.fingerprint_components() == (
            "docker_host_machine_id",
            [f"host_machine_id={machine_id}"],
        )
        assert fingerprint.machine_fingerprint() == expected

    values[fingerprint.HOST_MACHINE_ID_PATH] = "abcdef1234567890abcdef1234567890"
    assert fingerprint.machine_fingerprint() != expected


@pytest.mark.parametrize(
    "machine_id",
    [None, "", "uninitialized", "0" * 32, "a" * 31, "g" * 32, "A" * 32],
)
def test_container_requires_valid_host_identity(
    monkeypatch: pytest.MonkeyPatch,
    fingerprint: ModuleType,
    machine_id: str | None,
) -> None:
    """Missing or malformed host IDs must not select container or hardware IDs."""
    values = {
        "/proc/self/cgroup": f"0::/docker/{'a' * 64}",
        "/etc/machine-id": "abcdef1234567890abcdef1234567890",
        "/proc/cpuinfo": "model name: Example CPU",
    }
    if machine_id is not None:
        values[fingerprint.HOST_MACHINE_ID_PATH] = machine_id
    _identity_sources(monkeypatch, fingerprint, values)

    with pytest.raises(ValueError, match="bind-mount the host's /etc/machine-id"):
        fingerprint.machine_fingerprint()


def test_host_mount_is_used_without_container_markers(
    monkeypatch: pytest.MonkeyPatch, fingerprint: ModuleType
) -> None:
    """The explicit mount is sufficient when runtime markers are hidden."""
    values = {fingerprint.HOST_MACHINE_ID_PATH: "a" * 32}
    _identity_sources(monkeypatch, fingerprint, values, container_marker=False)
    assert fingerprint.fingerprint_components()[0] == "docker_host_machine_id"


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("/proc/self/cgroup", f"0::/docker/{'a' * 64}"),
        ("/proc/self/mountinfo", f"/docker/containers/{'a' * 64}/hostname"),
        ("/etc/hostname", "a" * 12),
    ],
)
def test_detected_containers_cannot_fall_back_to_native_identity(
    monkeypatch: pytest.MonkeyPatch,
    fingerprint: ModuleType,
    path: str,
    value: str,
) -> None:
    """A detectable container requires the mount even without /.dockerenv."""
    _identity_sources(monkeypatch, fingerprint, {path: value}, container_marker=False)
    with pytest.raises(ValueError, match="valid host machine-id"):
        fingerprint.fingerprint_components()


def test_native_hardware_fingerprint_is_unchanged(
    monkeypatch: pytest.MonkeyPatch, fingerprint: ModuleType
) -> None:
    """Native Linux retains the existing hardware component order and hash."""
    machine_id = "1234567890abcdef1234567890abcdef"
    values = {
        "/etc/machine-id": machine_id,
        "/proc/cpuinfo": "model name: Example CPU\nvendor_id: ExampleVendor",
        "/sys/class/dmi/id/product_uuid": "example-hardware-uuid",
    }
    _identity_sources(monkeypatch, fingerprint, values, container_marker=False)
    monkeypatch.setattr(
        fingerprint.platform,
        "uname",
        lambda: SimpleNamespace(system="Linux", machine="x86_64"),
    )
    components = [
        f"machine_id={machine_id}",
        "Linux",
        "x86_64",
        "Example CPU",
        "ExampleVendor",
        "example-hardware-uuid",
    ]
    assert fingerprint.fingerprint_components() == ("hardware_id", components)
    assert (
        fingerprint.machine_fingerprint()
        == hashlib.sha256("|".join(components).encode()).digest()
    )
