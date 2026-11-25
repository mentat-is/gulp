"""Render the private installer Compose configuration without starting services."""

import json
import os
from pathlib import Path
import subprocess

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
COMPOSE = REPOSITORY / "scripts/docker-compose-docker-prod.yml"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not COMPOSE.is_file(), reason="Private customer package is unavailable"),
]


@pytest.mark.parametrize("images", [
    ("mentatis/gulp-core:2.0.2", "mentatis/gulp-ui:2.1.0"),
    ("gulp-core:home-test", "gulp-ui:home-test"),
])
def test_customer_compose_serves_the_image_ui_and_backend_runtime_packages(
    tmp_path: Path, images: tuple[str, str],
) -> None:
    """Registry/local images share one working-dir bind and one stable host identity."""
    environment = {
        **os.environ,
        "GULP_WORKING_DIR": str(tmp_path / "customer package"),
        "GULP_IMAGE": images[0], "GULP_UI_IMAGE": images[1],
        "PYTHON_VERSION": "3.13", "POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test",
        "POSTGRES_DB": "test", "OPENSEARCH_USER": "test", "OPENSEARCH_PASSWORD": "test",
        "REDIS_PASSWORD": "test", "SEAWEEDFS_ACCESS_KEY": "test", "SEAWEEDFS_SECRET_KEY": "test",
        "SFTPD_USER": "test", "SFTPD_PASSWORD": "test",
    }
    empty_env = tmp_path / ".env"
    empty_env.touch()
    result = subprocess.run(
        ["docker", "compose", "--env-file", str(empty_env), "-f", str(COMPOSE),
         "--profile", "gulp", "--profile", "ui", "config", "--format", "json"],
        env=environment, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    services = json.loads(result.stdout)["services"]
    backend, ui = services["gulp"], services["gulp-ui"]
    assert backend["image"] == images[0]
    assert ui["image"] == images[1]
    assert "build" not in ui
    assert not ui.get("volumes")
    mounts = backend["volumes"]
    working_dir = [mount for mount in mounts if mount["target"] == "/home/gulp/.config/gulp"]
    assert len(working_dir) == 1
    assert working_dir[0]["source"] == environment["GULP_WORKING_DIR"]
    assert not working_dir[0].get("read_only", False)
    identity = [mount for mount in mounts if mount["target"] == "/run/gulp/host-machine-id"]
    assert identity == [{
        "type": "bind", "source": "/etc/machine-id", "target": "/run/gulp/host-machine-id",
        "read_only": True, "bind": {"create_host_path": False},
    }]
    assert all(not mount["target"].endswith("/plugins/ui") for mount in mounts)
    for name, hostname in (
        ("GULP_POSTGRES_URL", "@postgres:5432"), ("GULP_OPENSEARCH_URL", "@os01:9200"),
        ("GULP_REDIS_URL", "@redis:6379"), ("GULP_S3_URL", "@seaweedfs:9000"),
    ):
        assert hostname in backend["environment"][name]
