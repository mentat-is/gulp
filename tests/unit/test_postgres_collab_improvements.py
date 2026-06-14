from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.collab.structs import GulpCollabBase
from gulp.api.collab.user_session import GulpUserSession


class _FakeSessionConfig:
    def __init__(self, time_expire: int, touch_interval: int = 60000):
        self._time_expire = time_expire
        self._touch_interval = touch_interval

    def token_expiration_time(self, is_admin: bool) -> int:
        return self._time_expire

    def postgres_user_session_touch_interval_ms(self) -> int:
        return self._touch_interval

    def is_integration_test(self) -> bool:
        return True


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

    await GulpUserSession.update_expiration_time(
        user_session, sess, is_admin=False
    )

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

    await GulpUserSession.update_expiration_time(
        user_session, sess, is_admin=False
    )

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
    assert user_session.id == "token_user-1"
    assert user_session.time_expire == 100500


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
