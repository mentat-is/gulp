"""Process-level checks for the Docker entrypoint's bind-mount setup."""

import os
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize(
    ("mount_owner", "expect_chown"),
    [("0:0", True), ("1000:1000", False)],
)
def test_entrypoint_repairs_mount_before_starting_runtime_user(
    tmp_path: Path, mount_owner: str, expect_chown: bool
) -> None:
    """The full entrypoint repairs a root-owned mount and delegates once."""
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    runtime_home = tmp_path / "runtime-home"
    runtime_home.mkdir()
    working_dir = runtime_home / ".config" / "gulp"
    working_dir.mkdir(parents=True)
    chown_log = tmp_path / "chown.log"
    gosu_log = tmp_path / "gosu.log"

    for name in ("bash", "cut", "mkdir"):
        (fake_bin / name).symlink_to(Path("/usr/bin") / name)

    scripts = {
        "id": """#!/bin/sh
if [ "$#" -eq 1 ] && [ "$1" = "-u" ]; then echo 0; else echo 1000; fi
""",
        "getent": """#!/bin/sh
printf 'gulp:x:1000:1000::%s:/bin/sh\\n' "$RUNTIME_HOME"
""",
        "stat": """#!/bin/sh
printf '%s\\n' "$MOUNT_OWNER"
""",
        "chown": """#!/bin/sh
printf '%s\\n' "$*" >> "$CHOWN_LOG"
""",
        "gosu": """#!/bin/sh
printf '%s\\n' "$1" > "$GOSU_LOG"
shift
exec "$@"
""",
    }
    for name, content in scripts.items():
        script = fake_bin / name
        script.write_text(content, encoding="utf-8")
        script.chmod(0o755)

    environment = {
        **os.environ,
        "PATH": str(fake_bin),
        "GULP_RUNTIME_USER": "gulp",
        "RUNTIME_HOME": str(runtime_home),
        "MOUNT_OWNER": mount_owner,
        "CHOWN_LOG": str(chown_log),
        "GOSU_LOG": str(gosu_log),
    }
    environment.pop("GULP_WORKING_DIR", None)
    entrypoint = Path(__file__).resolve().parents[2] / "docker-entrypoint.sh"
    result = subprocess.run(
        ["/bin/bash", str(entrypoint), "/bin/sh", "-c", "printf started"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "started"
    assert gosu_log.read_text(encoding="utf-8").strip() == "gulp"
    if expect_chown:
        assert chown_log.read_text(encoding="utf-8").strip() == (
            f"-R 1000:1000 {working_dir}"
        )
    else:
        assert not chown_log.exists()
