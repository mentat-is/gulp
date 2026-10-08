from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.collab.structs import GulpCollabBase, SessionExpired
from gulp.api.collab.user_session import GulpUserSession
from gulp.structs import ObjectNotFound


class _FakeSessionConfig:
    def __init__(self, time_expire: int, touch_interval: int = 60000):
        self._time_expire = time_expire
        self._touch_interval = touch_interval

    def token_expiration_time(self, is_admin: bool) -> int:
        return self._time_expire

    def postgres_user_session_touch_interval_ms(self) -> int:
        return self._touch_interval


def _user_session(time_expire: int):
    return SimpleNamespace(
        id="token-user-1",
        user=SimpleNamespace(id="user-1"),
        time_expire=time_expire,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_user_session_expiration_update_skips_fresh_touch(monkeypatch):
    sess = SimpleNamespace(commit=AsyncMock())
    user_session = _user_session(time_expire=100000)

    monkeypatch.setattr(
        "gulp.api.collab.user_session.GulpConfig.get_instance",
        lambda: _FakeSessionConfig(time_expire=100500, touch_interval=60000),
    )

    await GulpUserSession.update_expiration_time(user_session, sess, is_admin=False)

    sess.commit.assert_not_awaited()
    assert user_session.time_expire == 100000


@pytest.mark.unit
@pytest.mark.asyncio
async def test_user_session_expiration_update_commits_when_due(monkeypatch):
    sess = SimpleNamespace(commit=AsyncMock())
    user_session = _user_session(time_expire=100000)

    monkeypatch.setattr(
        "gulp.api.collab.user_session.GulpConfig.get_instance",
        lambda: _FakeSessionConfig(time_expire=200000, touch_interval=60000),
    )

    await GulpUserSession.update_expiration_time(user_session, sess, is_admin=False)

    sess.commit.assert_awaited_once()
    assert user_session.time_expire == 200000


@pytest.mark.unit
@pytest.mark.asyncio
async def test_user_session_expiration_update_id_always_commits(monkeypatch):
    sess = SimpleNamespace(commit=AsyncMock())
    user_session = _user_session(time_expire=100000)

    monkeypatch.setattr(
        "gulp.api.collab.user_session.GulpConfig.get_instance",
        lambda: _FakeSessionConfig(time_expire=100500, touch_interval=60000),
    )

    await GulpUserSession.update_expiration_time(
        user_session, sess, is_admin=False, update_id=True
    )

    sess.commit.assert_awaited_once()
    assert user_session.id != "token_user-1"
    assert user_session.time_expire == 100500


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_token_missing_session_raises_session_expired(monkeypatch):
    async def _raise_not_found(*args, **kwargs):
        raise ObjectNotFound("not found")

    monkeypatch.setattr(
        "gulp.api.collab.user_session.GulpConfig.get_instance",
        lambda: SimpleNamespace(debug_allow_any_token_as_admin=lambda: False),
    )
    monkeypatch.setattr(GulpUserSession, "get_by_id", _raise_not_found)

    with pytest.raises(SessionExpired, match="^token not logged in$"):
        await GulpUserSession.check_token(SimpleNamespace(), "missing-token")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_token_expired_session_raises_session_expired(monkeypatch):
    user_session = SimpleNamespace(
        time_expire=1000,
        check_permissions=AsyncMock(),
    )

    async def _get_expired_session(*args, **kwargs):
        return user_session

    monkeypatch.setattr(
        "gulp.api.collab.user_session.GulpConfig.get_instance",
        lambda: SimpleNamespace(debug_allow_any_token_as_admin=lambda: False),
    )
    monkeypatch.setattr("gulp.api.collab.user_session.muty.time.now_msec", lambda: 1001)
    monkeypatch.setattr(GulpUserSession, "get_by_id", _get_expired_session)

    with pytest.raises(SessionExpired, match="^token expired$"):
        await GulpUserSession.check_token(SimpleNamespace(), "expired-token")

    user_session.check_permissions.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_advisory_lock_rolls_back_on_exception(monkeypatch):
    acquire = AsyncMock()
    sess = SimpleNamespace(rollback=AsyncMock())

    monkeypatch.setattr(
        GulpCollabBase,
        "acquire_advisory_lock",
        classmethod(lambda cls, sess, obj_id: acquire(sess, obj_id)),
    )

    with pytest.raises(RuntimeError, match="boom"):
        async with GulpCollabBase.advisory_lock(sess, "obj-1"):
            raise RuntimeError("boom")

    acquire.assert_awaited_once_with(sess, "obj-1")
    sess.rollback.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_advisory_lock_rolls_back_when_acquisition_fails(monkeypatch):
    acquire = AsyncMock(side_effect=RuntimeError("lock timeout"))
    sess = SimpleNamespace(rollback=AsyncMock())

    monkeypatch.setattr(
        GulpCollabBase,
        "acquire_advisory_lock",
        classmethod(lambda cls, sess, obj_id: acquire(sess, obj_id)),
    )

    with pytest.raises(RuntimeError, match="lock timeout"):
        async with GulpCollabBase.advisory_lock(sess, "obj-1"):
            pass

    acquire.assert_awaited_once_with(sess, "obj-1")
    sess.rollback.assert_awaited_once()
