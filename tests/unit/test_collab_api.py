from unittest.mock import AsyncMock, MagicMock, call

import pytest

from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.collab_api import GulpCollab, SchemaMismatch
from gulp.config import GulpConfig


def _config_with(overrides: dict) -> GulpConfig:
    cfg = object.__new__(GulpConfig)
    cfg._config = overrides
    return cfg


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_default_users_adds_ingest_user(monkeypatch):
    from gulp.api.collab.structs import GulpUserPermission
    from gulp.api.collab.user import GulpUser
    from gulp.api.collab.user_group import GulpUserGroup

    GulpCollab._instance = None
    collab = GulpCollab.get_instance()
    created_users = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def refresh(self, _obj):
            return None

    class _FakeUser:
        id = "admin"

        def to_dict(self, nested=False):
            return {"id": self.id, "nested": nested}

    class _FakeGroup:
        async def add_user(self, _sess, user_id):
            assert user_id == "admin"

    async def _create_user(_sess, user_id, password, **kwargs):
        created_users.append((user_id, password, kwargs.get("permission")))
        return _FakeUser()

    async def _create_group(*_args, **_kwargs):
        return _FakeGroup()

    monkeypatch.setattr(GulpUser, "create_user", _create_user)
    monkeypatch.setattr(GulpUserGroup, "create_internal", _create_group)
    monkeypatch.setattr(GulpUserGroup, "get_by_filter", AsyncMock(return_value=[]))
    collab._collab_sessionmaker = lambda: _FakeSession()

    await collab.create_default_users()

    assert created_users == [
        ("admin", "admin", [GulpUserPermission.ADMIN]),
        ("guest", "guest", None),
        ("ingest", "ingest", [GulpUserPermission.INGEST]),
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_setup_collab_expirations_uses_named_pg_cron_jobs():
    GulpCollab._instance = None
    collab = GulpCollab.get_instance()
    executed = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, stmt):
            executed.append(str(stmt))

        async def commit(self):
            executed.append("commit")

    collab._collab_sessionmaker = lambda: _FakeSession()

    await collab._setup_collab_expirations()

    sql = "\n".join(executed)
    assert "cron.schedule('gulp-delete-expired-stats'" in sql
    assert "cron.schedule('gulp-delete-expired-tokens'" in sql
    assert "commit" in executed


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
async def test_server_startup_aborts_on_collab_schema_mismatch(monkeypatch):
    from gulp.api.server_api import GulpServer

    collab = MagicMock()
    collab.init = AsyncMock(side_effect=SchemaMismatch("missing table"))
    server = GulpServer()
    server._cleanup = AsyncMock()
    config = MagicMock()
    config.prometheus_enabled.return_value = False
    config.stats_delete_pending_on_shutdown.return_value = False

    monkeypatch.setattr(GulpConfig, "get_instance", lambda: config)
    monkeypatch.setattr(GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(server, "_check_first_run", lambda: False)

    with pytest.raises(SchemaMismatch, match="missing table"):
        await anext(server._lifespan_handler(MagicMock()))

    assert server._reset_collab is False
    server._cleanup.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_server_reset_recreates_collab_on_schema_mismatch(monkeypatch):
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.server_api import GulpServer

    collab = MagicMock()
    collab.init = AsyncMock(side_effect=[SchemaMismatch("missing table"), None])
    server = GulpServer()
    server._reset_collab = True
    server._cleanup = AsyncMock()
    config = MagicMock()
    config.prometheus_enabled.return_value = False
    config.stats_delete_pending_on_shutdown.return_value = False

    monkeypatch.setattr(GulpConfig, "get_instance", lambda: config)
    monkeypatch.setattr(GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(server, "_check_first_run", lambda: False)
    monkeypatch.setattr(
        GulpOpenSearch,
        "get_instance",
        MagicMock(side_effect=RuntimeError("stop after collab initialization")),
    )

    with pytest.raises(RuntimeError, match="stop after collab initialization"):
        await anext(server._lifespan_handler(MagicMock()))

    assert collab.init.await_args_list == [
        call(main_process=True),
        call(main_process=True, force_recreate=True),
    ]
    server._cleanup.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_object_count_handler_dispatches_to_registered_collab_type(monkeypatch):
    from gulp.api.server import utility

    seen = {}

    class _FakeObject:
        @classmethod
        async def count_by_filter_wrapper(
            cls, token, flt, operation_id=None, permission=None
        ):
            seen.update(
                {
                    "token": token,
                    "flt": flt,
                    "operation_id": operation_id,
                    "permission": permission,
                }
            )
            return 7

    monkeypatch.setattr(
        utility.GulpCollabBase,
        "object_type_to_class",
        staticmethod(lambda obj_type: _FakeObject if obj_type == "note" else None),
    )

    response = await utility.object_count_handler(
        token="tok",
        obj_type="note",
        operation_id="op-a",
        flt=GulpCollabFilter(tags=["auto"]),
        req_id="req-a",
    )

    assert response["data"] == {"count": 7}
    assert seen == {
        "token": "tok",
        "flt": GulpCollabFilter(tags=["auto"]),
        "operation_id": "op-a",
        "permission": [utility.GulpUserPermission.READ],
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_count_by_filter_strips_paging(monkeypatch):
    from gulp.api.collab.note import GulpNote

    captured = {}

    class _FakeResult:
        def scalar_one(self):
            return 7

    class _FakeSession:
        async def execute(self, query):
            captured["query"] = query
            return _FakeResult()

    async def _restrict(_sess, _user_id=None, flt=None):
        return flt

    monkeypatch.setattr(
        "gulp.api.collab.structs.GulpCollabBase._restrict_flt_to_user",
        _restrict,
    )

    count = await GulpNote.count_by_filter(
        _FakeSession(),
        GulpCollabFilter(operation_ids=["op-a"], limit=10, offset=20),
        user_id="user-a",
    )

    assert count == 7
    query = captured["query"]
    assert query._limit_clause is None
    assert query._offset_clause is None


@pytest.mark.unit
def test_note_doc_filter_uses_indexed_doc_id_column():
    from sqlalchemy.dialects import postgresql

    from gulp.api.collab.note import GulpNote

    GulpCollab.init_mappers()
    query = GulpCollabFilter(
        operation_ids=["op-a"],
        doc_ids=["ABC123"],
    ).to_select_query(GulpNote)
    compiled = str(
        query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "note.doc_id IN ('abc123')" in compiled
    assert "note.doc ->>" not in compiled


@pytest.mark.unit
def test_note_text_filter_searches_inline_and_shared_text():
    from sqlalchemy.dialects import postgresql

    from gulp.api.collab.note import GulpNote

    GulpCollab.init_mappers()
    query = GulpCollabFilter(texts=["*needle*"]).to_select_query(GulpNote)
    compiled = str(
        query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "note.text ILIKE '%%needle%%'" in compiled
    assert "note_text.text ILIKE '%%needle%%'" in compiled


@pytest.mark.unit
def test_note_fk_columns_are_indexed_for_large_cascades():
    from gulp.api.collab.note import GulpNote, GulpNoteText

    note_indexes = {idx.name for idx in GulpNote.__table__.indexes}
    note_text_indexes = {idx.name for idx in GulpNoteText.__table__.indexes}

    assert {"idx_note_context", "idx_note_source", "idx_note_text_id"} <= note_indexes
    assert "idx_note_text_operation" in note_text_indexes


def test_note_serialization_combines_inline_and_shared_text():
    from gulp.api.collab.note import GulpNote, GulpNoteText

    GulpCollab.init_mappers()
    common = {
        "user_id": "user-a",
        "time_created": 1,
        "time_updated": 1,
        "operation_id": "op-a",
        "glyph_id": None,
        "description": None,
        "tags": [],
        "color": None,
        "granted_user_ids": [],
        "granted_user_group_ids": [],
    }
    shared = GulpNoteText(
        id="text-a",
        type="note_text",
        name="shared",
        text="\nquery",
        **common,
    )
    note = GulpNote(
        id="note-a",
        type="note",
        name="note",
        context_id="context-a",
        source_id="source-a",
        text="match",
        text_id="text-a",
        doc={"_id": "doc-a"},
        doc_id="doc-a",
        time_pin=None,
        last_editor_id=None,
        **common,
    )
    note.shared_text = shared

    data = note.to_dict()

    assert data["text"] == "match\nquery"
    assert "text_id" not in data
    assert "shared_text" not in data


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

        def workers_concurrency_max(self):
            return 1000

        def postgres_pool_timeout_sec(self):
            return 7

        def postgres_pool_recycle_sec(self):
            return 89

        def workers_max(self):
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
