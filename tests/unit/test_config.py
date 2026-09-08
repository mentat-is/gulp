import pytest

from gulp.config import GulpConfig


def _config_with(overrides: dict) -> GulpConfig:
    cfg = object.__new__(GulpConfig)
    cfg._config = overrides
    return cfg


@pytest.mark.unit
def test_workers_concurrency_max_defaults_and_clamps():
    cfg = _config_with({})

    assert cfg.workers_concurrency_max() == 16

    cfg = _config_with({"workers_concurrency_max": 0})

    assert cfg.workers_concurrency_max() == 1


@pytest.mark.unit
def test_postgres_pool_defaults_and_clamps():
    cfg = _config_with({})

    assert cfg.postgres_pool_size() == 4
    assert cfg.postgres_max_overflow() == 0

    cfg = _config_with({"workers_concurrency_max": 16})

    assert cfg.postgres_pool_size() == 16

    cfg = _config_with(
        {
            "postgres_pool_size": 0,
            "postgres_max_overflow": -1,
        }
    )

    assert cfg.postgres_pool_size() == 1
    assert cfg.postgres_max_overflow() == 0

    cfg = _config_with(
        {"postgres_pool_size": "invalid", "postgres_max_overflow": "invalid"}
    )

    assert cfg.postgres_pool_size() == 4
    assert cfg.postgres_max_overflow() == 0


@pytest.mark.unit
def test_opensearch_index_replicas_defaults_and_clamps():
    cfg = _config_with({})

    assert cfg.opensearch_index_replicas() == 0

    cfg = _config_with({"opensearch_index_replicas": 1})

    assert cfg.opensearch_index_replicas() == 1

    cfg = _config_with({"opensearch_index_replicas": -1})

    assert cfg.opensearch_index_replicas() == 0


@pytest.mark.unit
def test_opensearch_pool_config_defaults_and_clamps():
    cfg = _config_with({})

    assert cfg.opensearch_pool_maxsize() == 10

    cfg = _config_with(
        {
            "opensearch_pool_maxsize": 0,
        }
    )

    assert cfg.opensearch_pool_maxsize() == 1


@pytest.mark.unit
def test_query_pagination_pit_keep_alive_default_and_override() -> None:
    cfg = _config_with({})

    assert cfg.query_pagination_pit_keep_alive() == "5m"

    cfg = _config_with({"query_pagination_pit_keep_alive": "90s"})

    assert cfg.query_pagination_pit_keep_alive() == "90s"


@pytest.mark.unit
def test_instance_roles_environment_override(monkeypatch):
    cfg = _config_with({"instance_roles": ["ingest"]})

    monkeypatch.setenv("GULP_INSTANCE_ROLES", " query, ingest ,, ")

    assert cfg.instance_roles() == ["query", "ingest"]


@pytest.mark.unit
def test_ingestion_chunk_and_pipeline_defaults() -> None:
    """Ingestion keeps one valid count and enables one-ahead by default."""
    cfg = _config_with({})

    assert cfg.ingestion_documents_chunk_size() == 1000
    assert cfg.ingestion_pipeline_enabled() is True

    cfg = _config_with(
        {
            "ingestion_documents_chunk_size": 0,
            "ingestion_pipeline_enabled": False,
        }
    )

    assert cfg.ingestion_documents_chunk_size() == 1
    assert cfg.ingestion_pipeline_enabled() is False
