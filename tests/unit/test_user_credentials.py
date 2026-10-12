"""Checking a user id and password: an unknown user and a wrong password look the same."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import muty.crypto
import pytest

from gulp.api.collab import user as user_module
from gulp.api.collab.structs import WrongUsernameOrPassword
from gulp.api.collab.user import GulpUser


@pytest.fixture
def users(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """one known user, "alice", and what is logged about refused logins"""
    alice = SimpleNamespace(id="alice", pwd_hash=muty.crypto.hash_sha256("Right-Passw0rd"))

    async def get_by_id(sess, user_id, throw_if_not_found=True):
        return alice if user_id == "alice" else None

    logged: list[str] = []
    monkeypatch.setattr(GulpUser, "get_by_id", AsyncMock(side_effect=get_by_id))
    monkeypatch.setattr(
        user_module.Glug,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(info=logged.append)),
    )
    return logged


@pytest.mark.unit
async def test_right_password_gives_the_user(users: list[str]):
    user = await GulpUser.check_credentials(None, "alice", "Right-Passw0rd")

    assert user.id == "alice"
    assert users == []


@pytest.mark.unit
async def test_unknown_user_and_wrong_password_fail_the_same_way(users: list[str]):
    failures = []
    for user_id, password in (
        ("alice", "wrong"),
        ("nobody", "wrong"),
        ("nobody", "Right-Passw0rd"),
        ("alice", ""),
        ("alice", None),
    ):
        with pytest.raises(WrongUsernameOrPassword) as raised:
            await GulpUser.check_credentials(None, user_id, password)
        failures.append(str(raised.value))

    assert set(failures) == {"wrong username or password"}
    # the difference is only for whoever reads the server log
    assert users[0] == "login refused for user_id=alice: wrong password"
    assert users[1] == "login refused for user_id=nobody: no such user"


@pytest.mark.unit
async def test_a_user_without_password_cannot_log_in_with_an_empty_one(
    users: list[str], monkeypatch: pytest.MonkeyPatch
):
    """a user created without a password has "-" as its hash: nothing hashes to it."""
    passwordless = SimpleNamespace(id="sso", pwd_hash="-")
    monkeypatch.setattr(GulpUser, "get_by_id", AsyncMock(return_value=passwordless))

    for password in ("", "-", None):
        with pytest.raises(WrongUsernameOrPassword):
            await GulpUser.check_credentials(None, "sso", password)
