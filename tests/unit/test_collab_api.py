from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab_api import GulpCollab


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
