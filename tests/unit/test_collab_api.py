from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab_api import GulpCollab
from gulp.config import GulpConfig


def _config_with(overrides: dict) -> GulpConfig:
    cfg = object.__new__(GulpConfig)
    cfg._config = overrides
    cfg._concurrency_num_tasks = None
    return cfg


@pytest.mark.unit
@pytest.mark.asyncio
async def test_collab_shutdown_disposes_engine_and_clears_state():
    GulpCollab._instance = None
    collab = GulpCollab.get_instance()

    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    collab._initialized = True
    collab._engine = fake_engine
    collab._collab_sessionmaker = MagicMock()

    await collab.shutdown()

    fake_engine.dispose.assert_awaited_once()
    assert collab._initialized is False
    assert collab._engine is None
    assert collab._collab_sessionmaker is None

    with pytest.raises(Exception, match="collab not initialized"):
        collab.session()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_collab_init_shuts_down_previous_engine_before_recreate(monkeypatch):
    GulpCollab._instance = None
    collab = GulpCollab.get_instance()

    old_engine = MagicMock()
    old_engine.dispose = AsyncMock()
    new_engine = MagicMock()
    collab._engine = old_engine
    collab._collab_sessionmaker = MagicMock()
    collab._initialized = False

    monkeypatch.setattr(GulpCollab, "init_mappers", staticmethod(lambda: None))
    monkeypatch.setattr(
        "gulp.api.collab_api.GulpConfig.get_instance",
        lambda: MagicMock(
            postgres_url=MagicMock(return_value="postgresql+asyncpg://test")
        ),
    )
    monkeypatch.setattr(collab, "_create_engine", AsyncMock(return_value=new_engine))
    session_factory = MagicMock(name="session_factory")
    monkeypatch.setattr(
        "gulp.api.collab_api.async_sessionmaker",
        MagicMock(return_value=session_factory),
    )

    await collab.init(main_process=False)

    old_engine.dispose.assert_awaited_once()
    assert collab._engine is new_engine
    assert collab._collab_sessionmaker is session_factory
    assert collab._initialized is True


@pytest.mark.unit
def test_postgres_config_defaults_and_clamps():
    cfg = _config_with({})

    assert cfg.postgres_pool_timeout_sec() == 10
    assert cfg.postgres_pool_recycle_sec() == 3600
    assert cfg.postgres_lock_timeout_ms() == 5000
    assert cfg.postgres_statement_timeout_ms() == 0
    assert cfg.postgres_idle_in_transaction_session_timeout_ms() == 30000
    assert cfg.postgres_user_session_touch_interval_ms() == 60000

    cfg = _config_with(
        {
            "postgres_pool_timeout_sec": -5,
            "postgres_pool_recycle_sec": "invalid",
            "postgres_lock_timeout_ms": -1,
            "postgres_statement_timeout_ms": -1,
            "postgres_idle_in_transaction_session_timeout_ms": -1,
            "postgres_user_session_touch_interval_ms": -1,
        }
    )

    assert cfg.postgres_pool_timeout_sec() == 1
    assert cfg.postgres_pool_recycle_sec() == 3600
    assert cfg.postgres_lock_timeout_ms() == 0
    assert cfg.postgres_statement_timeout_ms() == 0
    assert cfg.postgres_idle_in_transaction_session_timeout_ms() == 0
    assert cfg.postgres_user_session_touch_interval_ms() == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_engine_uses_configured_postgres_pool_and_timeouts(monkeypatch):
    GulpCollab._instance = None
    collab = GulpCollab.get_instance()
    captured = {}
    fake_engine = MagicMock(name="engine")

    class _FakeConfig:
        def postgres_url(self):
            return "postgresql+psycopg://user:pass@localhost:5432/gulp"

        def path_certs(self):
            return None

        def postgres_ssl(self):
            return False

        def postgres_verify_certs(self):
            return False

        def postgres_lock_timeout_ms(self):
            return 1234

        def postgres_statement_timeout_ms(self):
            return 5678

        def postgres_idle_in_transaction_session_timeout_ms(self):
            return 9012

        def postgres_pool_size(self):
            return 3

        def postgres_max_overflow(self):
            return 2

        def postgres_adaptive_pool_size(self):
            return True

        def concurrency_num_tasks(self):
            return 1000

        def postgres_pool_timeout_sec(self):
            return 7

        def postgres_pool_recycle_sec(self):
            return 89

        def parallel_processes_max(self):
            return 1

        def debug_collab(self):
            return False

    def _create_async_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return fake_engine

    monkeypatch.setattr(
        "gulp.api.collab_api.GulpConfig.get_instance", lambda: _FakeConfig()
    )
    monkeypatch.setattr("gulp.api.collab_api.create_async_engine", _create_async_engine)

    engine = await collab._create_engine()

    assert engine is fake_engine
    assert captured["url"] == "postgresql+psycopg://user:pass@localhost:5432/gulp"
    kwargs = captured["kwargs"]
    assert kwargs["pool_size"] == 3
    assert kwargs["max_overflow"] == 2
    assert kwargs["pool_timeout"] == 7
    assert kwargs["pool_recycle"] == 89
    assert kwargs["pool_pre_ping"] is True
    options = kwargs["connect_args"]["options"]
    assert "-c lock_timeout=1234" in options
    assert "-c statement_timeout=5678" in options
    assert "-c idle_in_transaction_session_timeout=9012" in options


@pytest.mark.unit
@pytest.mark.asyncio
async def test_db_drop_terminates_postgres_backends_before_drop(monkeypatch):
    executed = []
    drop_calls = []

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, stmt, params=None):
            executed.append((str(stmt), params))

    class _FakeEngine:
        def __init__(self):
            self.disposed = False

        def connect(self):
            return _FakeConnection()

        def dispose(self):
            self.disposed = True

    fake_engine = _FakeEngine()

    async def _fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr("gulp.api.collab_api.asyncio.to_thread", _fake_to_thread)
    monkeypatch.setattr("gulp.api.collab_api.database_exists", lambda url: True)
    monkeypatch.setattr(
        "gulp.api.collab_api.create_engine", lambda *args, **kwargs: fake_engine
    )
    monkeypatch.setattr(
        "gulp.api.collab_api.drop_database", lambda url: drop_calls.append(url)
    )

    await GulpCollab.db_drop("postgresql+asyncpg://user:pass@localhost:5432/gulp")

    assert fake_engine.disposed is True
    assert len(executed) == 1
    assert "pg_terminate_backend" in executed[0][0]
    assert executed[0][1] == {"database_name": "gulp"}
    assert drop_calls == ["postgresql+asyncpg://user:pass@localhost:5432/gulp"]
