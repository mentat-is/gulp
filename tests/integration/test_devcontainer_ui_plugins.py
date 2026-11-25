"""Verify devcontainer mounts keep runtime UI packages separate from paid sources."""

import json5
from pathlib import Path
import posixpath
import subprocess
import uuid

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
WORKING_DIR = "/home/vscode/.config/gulp"
pytestmark = pytest.mark.integration


def _configuration() -> dict:
    """Read the actual JSONC configuration, including its lifecycle commands."""
    return json5.loads((REPOSITORY / ".devcontainer/devcontainer.json").read_text())


def _mounts(configuration: dict) -> dict[str, dict[str, str]]:
    """Index the configured bind specifications by container destination."""
    mounts = [dict(field.split("=", 1) for field in spec.split(",")) for spec in configuration["mounts"]]
    return {mount["target"]: mount for mount in mounts}


def test_ui_manifest_mount_comes_from_host_working_directory() -> None:
    """The UI child mount restores host packages without changing the live BE mount."""
    configuration = _configuration()
    mounts = _mounts(configuration)
    assert mounts[f"{WORKING_DIR}/plugins"]["source"].endswith("/gulp-paid-plugins/src/gulp-paid-plugins/plugins")
    assert mounts[f"{WORKING_DIR}/plugins/ui"]["source"] == "${localEnv:HOME}/.config/gulp/plugins/ui"
    assert mounts[f"{WORKING_DIR}/mapping_files"]["source"].endswith("/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files")
    assert configuration["initializeCommand"] == ["mkdir", "-p", "${localEnv:HOME}/.config/gulp/plugins/ui"]


def test_docker_mounts_hide_ui_sources_and_preserve_backend_plugins() -> None:
    """Exercise the configured parent/child mounts with an isolated Docker volume."""
    probe = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=15)
    if probe.returncode:
        pytest.skip("Docker daemon access is required for the mount integration test")

    def docker(*arguments: str) -> str:
        """Run one Docker command and expose its diagnostics on assertion failure."""
        result = subprocess.run(["docker", *arguments], capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    volume = "gulp-dev-ui-test-" + uuid.uuid4().hex
    docker("volume", "create", volume)
    try:
        root = docker("volume", "inspect", volume, "--format", "{{.Mountpoint}}")
        # All fixture data lives in this new volume, never in host user/repo folders.
        docker("run", "--rm", "--mount", f"type=volume,source={volume},target=/fixture",
               "--entrypoint", "/bin/sh", "nginx:alpine", "-ec", """
mkdir -p /fixture/workspace/gulp /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/plugins/ui /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files /fixture/home/.config/gulp/plugins/ui
touch /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/plugins/query_example.py
touch /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension/example.py
touch /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files/example.json
touch /fixture/workspace/gulp-paid-plugins/src/gulp-paid-plugins/plugins/ui/Source.tsx
touch /fixture/home/.config/gulp/plugins/ui/compiled.json
mkdir -p /fixture/home/.config/gulp/plugins/ui/compiled/hash
touch /fixture/home/.config/gulp/plugins/ui/compiled/hash/plugin.js
""")
        configuration = _configuration()
        selected = {WORKING_DIR, f"{WORKING_DIR}/plugins", f"{WORKING_DIR}/plugins/ui",
                    f"{WORKING_DIR}/mapping_files", "${containerWorkspaceFolder}/gulp-paid-plugins"}
        arguments = ["run", "--rm"]
        for mount in _mounts(configuration).values():
            if mount["target"] not in selected:
                continue
            source = posixpath.normpath(mount["source"].replace("${localWorkspaceFolder}", f"{root}/workspace/gulp")
                                       .replace("${localEnv:HOME}", f"{root}/home"))
            target = mount["target"].replace("${containerWorkspaceFolder}", configuration["workspaceFolder"])
            arguments += ["--mount", f"type=bind,source={source},target={target}"]
        output = docker(*arguments, "--entrypoint", "/bin/sh", "nginx:alpine", "-ec", f"""
test -f {WORKING_DIR}/plugins/query_example.py
test -f {WORKING_DIR}/plugins/extension/example.py
test -f {WORKING_DIR}/mapping_files/example.json
test -f {WORKING_DIR}/plugins/ui/compiled.json
test -f {WORKING_DIR}/plugins/ui/compiled/hash/plugin.js
test ! -e {WORKING_DIR}/plugins/ui/Source.tsx
test -f /gulp/gulp-paid-plugins/src/gulp-paid-plugins/plugins/ui/Source.tsx
echo mounts-verified
""")
        assert output == "mounts-verified"
    finally:
        docker("volume", "rm", volume)
