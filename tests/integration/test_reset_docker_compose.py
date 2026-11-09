"""Check that the Docker reset stops every core application container."""

import os
from pathlib import Path
import shlex
import subprocess


def test_reset_stops_backend_and_ui_before_removing_network(tmp_path: Path) -> None:
    """The first Compose down must include the backend and the separate UI."""
    repository = Path(__file__).resolve().parents[2]
    working_dir = tmp_path / "gulp_working_dir"
    working_dir.mkdir()
    first_run_marker = working_dir / ".first_run_done"
    first_run_marker.touch()

    docker_stub = tmp_path / "docker"
    docker_stub.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$DOCKER_CALL_LOG"\n',
        encoding="utf-8",
    )
    docker_stub.chmod(0o755)
    call_log = tmp_path / "docker_calls.log"

    environment = os.environ.copy()
    environment["PATH"] = f"{tmp_path}:{environment['PATH']}"
    environment["DOCKER_CALL_LOG"] = str(call_log)
    environment["GULP_WORKING_DIR"] = str(working_dir)
    result = subprocess.run(
        ["bash", "reset_docker.sh"],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert not first_run_marker.exists()

    first_call = shlex.split(call_log.read_text(encoding="utf-8").splitlines()[0])
    assert first_call[0] == "compose"
    assert first_call[-1] == "down"
    for filename in ("docker-compose.yml", "docker-compose-docker-prod.yml"):
        services = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                filename,
                *first_call[1:-1],
                "config",
                "--services",
            ],
            cwd=repository,
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
        assert services.returncode == 0, services.stderr
        assert {"gulp", "gulp-ui"} <= set(services.stdout.splitlines())
