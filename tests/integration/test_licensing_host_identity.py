"""Exercise fingerprint generation, compilation and the real gULP plugin loader."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
LICENSING = REPOSITORY / "gulp-licensing"
RUNTIME = REPOSITORY / "tests/fixtures/licensing_runtime.py"
RESULT_PREFIX = "LICENSING_RESULT="
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not LICENSING.is_dir(), reason="Internal gulp-licensing tools are not installed"
    ),
]


def _run_runtime(
    mode: str, path: Path, machine_id: Path, container_id: str
) -> subprocess.CompletedProcess[str]:
    """Run the real generator or loader in a new process with OS identity inputs."""
    return subprocess.run(
        [sys.executable, str(RUNTIME), mode, str(path)],
        cwd=REPOSITORY,
        env={
            **os.environ,
            "TEST_HOST_MACHINE_ID_PATH": str(machine_id),
            "TEST_CONTAINER_ID": container_id,
            "GULP_WORKING_DIR": str(machine_id.parent / "runtime"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


@pytest.fixture
def licensed_plugin(tmp_path: Path) -> tuple[Path, Path, str]:
    """Generate a customer fingerprint and compile a real enrichment plugin."""
    machine_id = tmp_path / "host-machine-id"
    machine_id.write_text("1234567890abcdef1234567890abcdef\n", encoding="utf-8")
    generated = _run_runtime(
        "generate", LICENSING / "generate_fingerprint.py", machine_id, "a" * 64
    )
    assert generated.returncode == 0, generated.stderr
    data = json.loads(generated.stdout)
    assert data["method"] == "docker_host_machine_id"
    assert data["components"] == ["host_machine_id=1234567890abcdef1234567890abcdef"]
    assert len(bytes.fromhex(data["fingerprint"])) == 32

    output = tmp_path / "enrich_update_marker.pyc"
    built = subprocess.run(
        [
            sys.executable,
            str(LICENSING / "make_licensed.py"),
            "--plugin",
            str(REPOSITORY / "tests/fixtures/plugins/enrich_update_marker.py"),
            "--customer_id",
            "host-identity-test",
            "--fingerprint",
            data["fingerprint"],
            "--out",
            str(output),
        ],
        cwd=REPOSITORY,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert built.returncode == 0, built.stderr
    assert output.is_file()
    return output, machine_id, data["fingerprint"]


def test_licensed_plugin_survives_container_identity_change(
    licensed_plugin: tuple[Path, Path, str],
) -> None:
    """The same unmodified bytecode validates in newly created container contexts."""
    plugin, machine_id, fingerprint = licensed_plugin
    original_digest = hashlib.sha256(plugin.read_bytes()).digest()

    for container_id in ("a" * 64, "b" * 64):
        generated = _run_runtime(
            "generate", LICENSING / "generate_fingerprint.py", machine_id, container_id
        )
        assert generated.returncode == 0, generated.stderr
        assert json.loads(generated.stdout)["fingerprint"] == fingerprint

        loaded = _run_runtime("load", plugin, machine_id, container_id)
        assert loaded.returncode == 0, loaded.stderr
        result = json.loads(loaded.stdout.split(RESULT_PREFIX, 1)[1].splitlines()[0])
        assert result["name"] == "enrich_update_marker"
        assert result["display_name"] == "enrich_update_marker"
        assert result["valid"] is True
        assert result["license"]["customer_id"] == "host-identity-test"
        assert result["license"]["expire_msec"] == 0
        assert result["license"]["licensed_date"] > 0
        assert "plugin_py" not in result["license"]

    assert hashlib.sha256(plugin.read_bytes()).digest() == original_digest


@pytest.mark.parametrize(
    "identity",
    [None, "", "uninitialized", "b" * 32, b"\xff1234567890abcdef1234567890abcdef"],
)
def test_licensed_plugin_rejects_missing_invalid_or_different_host(
    licensed_plugin: tuple[Path, Path, str], identity: str | bytes | None
) -> None:
    """Generation and runtime validation enforce the mounted host identity."""
    plugin, machine_id, _ = licensed_plugin
    if identity is None:
        machine_id.unlink()
    elif isinstance(identity, bytes):
        machine_id.write_bytes(identity)
    else:
        machine_id.write_text(identity, encoding="utf-8")

    generated = _run_runtime(
        "generate", LICENSING / "generate_fingerprint.py", machine_id, "a" * 64
    )
    loaded = _run_runtime("load", plugin, machine_id, "a" * 64)
    assert loaded.returncode != 0
    assert RESULT_PREFIX not in loaded.stdout
    if identity == "b" * 32:
        assert generated.returncode == 0, generated.stderr
        assert "JSONDecodeError" in loaded.stderr
    else:
        assert generated.returncode != 0
        assert "valid host machine-id" in generated.stderr
        assert "valid host machine-id" in loaded.stderr
