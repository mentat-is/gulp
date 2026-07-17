"""Unit checks for stress harness setup readiness."""

from types import SimpleNamespace

import pytest

from tests.integration.test_stress import (
    _cleanup_worker_users,
    _create_worker_users,
    _delete_op,
    _setup_operation,
)


class _Users:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.ready_checked: list[str] = []

    async def create(self, *, user_id: str, password: str, permission: list[str]) -> dict:
        self.created.append(user_id)
        return {"id": user_id}

    async def get(self, user_id: str) -> dict:
        assert user_id in self.created
        self.ready_checked.append(user_id)
        return {"id": user_id}

    async def delete(self, user_id: str) -> dict:
        return {"id": user_id}


class _Operations:
    def __init__(self) -> None:
        self.created_id = "op-ready"
        self.ready_checked: list[str] = []

    async def create(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(id=self.created_id)

    async def get(self, operation_id: str) -> SimpleNamespace:
        self.ready_checked.append(operation_id)
        return SimpleNamespace(id=operation_id)


@pytest.mark.unit
async def test_create_worker_users_waits_until_each_user_is_readable() -> None:
    client = SimpleNamespace(users=_Users())

    user_ids = await _create_worker_users(client, 2, "TestPass!123")

    assert user_ids == client.users.ready_checked


@pytest.mark.unit
async def test_setup_operation_waits_until_operation_is_readable() -> None:
    client = SimpleNamespace(operations=_Operations())

    op_id = await _setup_operation(client)

    assert op_id == "op-ready"
    assert client.operations.ready_checked == ["op-ready"]


@pytest.mark.unit
async def test_delete_op_waits_without_client_timeout() -> None:
    client = SimpleNamespace(timeout=30.0)

    async def request_delete(_operation_id: str) -> None:
        return None

    async def delete(_operation_id: str) -> None:
        assert client.timeout is None

    client.plugins = SimpleNamespace(request_delete=request_delete)
    client.operations = SimpleNamespace(delete=delete)

    await _delete_op(client, "op-a")

    assert client.timeout == 30.0


@pytest.mark.unit
async def test_cleanup_worker_users_attempts_every_user() -> None:
    from gulp_sdk import NotFoundError

    deleted: list[str] = []
    client = SimpleNamespace(timeout=30.0)

    async def delete(user_id: str) -> None:
        assert client.timeout is None
        deleted.append(user_id)
        if user_id == "failed":
            raise RuntimeError("boom")
        if user_id == "missing":
            raise NotFoundError("not found")

    client.users = SimpleNamespace(delete=delete)

    with pytest.raises(RuntimeError, match="failed"):
        await _cleanup_worker_users(client, ["failed", "missing", "deleted"])

    assert deleted == ["failed", "missing", "deleted"]
    assert client.timeout == 30.0
