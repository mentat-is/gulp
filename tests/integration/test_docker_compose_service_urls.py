"""Check the Docker Compose variant against the existing service topology."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
from urllib.parse import urlsplit


def _render_compose(repository: Path, filename: str) -> dict:
    """Render one Compose file with the repository's .env variables."""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            filename,
            "--profile",
            "gulp",
            "--profile",
            "ui",
            "config",
            "--format",
            "json",
        ],
        cwd=repository,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_docker_compose_only_adds_backend_service_urls() -> None:
    """The Docker variant changes only gULP's dependency addresses."""
    repository = Path(__file__).resolve().parents[2]
    original = _render_compose(repository, "docker-compose.yml")
    docker = _render_compose(repository, "docker-compose-docker-prod.yml")
    services = docker["services"]
    backend = services["gulp"]["environment"]

    postgres = urlsplit(backend["GULP_POSTGRES_URL"])
    assert postgres.scheme == "postgresql+psycopg"
    assert postgres.hostname == "postgres"
    assert postgres.username == services["postgres"]["environment"]["POSTGRES_USER"]
    assert postgres.password == services["postgres"]["environment"]["POSTGRES_PASSWORD"]
    assert postgres.path == f"/{services['postgres']['environment']['POSTGRES_DB']}"

    opensearch = urlsplit(backend["GULP_OPENSEARCH_URL"])
    assert opensearch.hostname == "os01"
    assert opensearch.username == services["os01"]["environment"]["OPENSEARCH_USERNAME"]
    assert (
        opensearch.password
        == services["os01"]["environment"]["OPENSEARCH_INITIAL_ADMIN_PASSWORD"]
    )

    redis = urlsplit(backend["GULP_REDIS_URL"])
    assert redis.hostname == "redis"
    assert redis.password == services["redis"]["environment"]["REDIS_PASSWORD"]

    s3 = urlsplit(backend["GULP_S3_URL"])
    assert s3.hostname == "seaweedfs"
    assert s3.username == services["seaweedfs"]["environment"]["AWS_ACCESS_KEY_ID"]
    assert s3.password == services["seaweedfs"]["environment"]["AWS_SECRET_ACCESS_KEY"]

    unchanged = deepcopy(docker)
    for key in (
        "GULP_POSTGRES_URL",
        "GULP_OPENSEARCH_URL",
        "GULP_REDIS_URL",
        "GULP_S3_URL",
    ):
        unchanged["services"]["gulp"]["environment"].pop(key)
    assert unchanged == original
