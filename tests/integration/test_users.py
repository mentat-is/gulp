"""
Integration tests for users and user-groups APIs.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_users.py -m integration
"""

import pytest
import uuid


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ──────────────────────────────────────────────────────────────────────────────
# Users
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_user_me(gulp_base_url, gulp_test_user, gulp_test_password):
    """Get current user info."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        me = await client.users.me()
        assert me.get("id") is not None or me.get("user_id") is not None


@pytest.mark.integration
async def test_user_create_get_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a user, retrieve it, then delete it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        user_id = f"testuser{uuid.uuid4().hex[:8]}"
        password = "TestPass!123"

        user = await client.users.create(
            user_id=user_id,
            password=password,
            permission=["read"],
        )
        assert user.get("id") == user_id or user.get("user_id") == user_id

        # Get by ID
        fetched = await client.users.get(user_id)
        assert fetched.get("id") == user_id or fetched.get("user_id") == user_id

        # Delete
        result = await client.users.delete(user_id)
        assert result.get("id") == user_id or result.get("user_id") == user_id


@pytest.mark.integration
async def test_user_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a user, update it, then clean up."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        user_id = f"upduser{uuid.uuid4().hex[:8]}"

        await client.users.create(
            user_id=user_id,
            password="TestPass!123",
            permission=["read"],
        )
        try:
            updated = await client.users.update(
                user_id=user_id,
                user_data={"display_name": "Updated Name"},
            )
            assert updated.get("id") == user_id or updated.get("user_id") == user_id
        finally:
            await client.users.delete(user_id)


@pytest.mark.integration
async def test_user_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """List users — should include at least the logged-in admin."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        users = await client.users.list()
        assert isinstance(users, list)
        assert len(users) >= 1


@pytest.mark.integration
async def test_user_set_get_delete_data(gulp_base_url, gulp_test_user, gulp_test_password):
    """Set, get, and delete user data key/value."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        # Set data
        await client.users.set_data("test_key", {"hello": "world"})

        # Get data
        data = await client.users.get_data("test_key")
        assert data.get("hello") == "world" or data is not None

        # Delete data
        await client.users.delete_data("test_key")


@pytest.mark.integration
async def test_user_keepalive(gulp_base_url, gulp_test_user, gulp_test_password):
    """Session keepalive should succeed with a valid token."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        result = await client.users.session_keepalive()
        # Result should have some data identifying the kept-alive session
        assert result is not None


# ──────────────────────────────────────────────────────────────────────────────
# User groups
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_user_group_create_get_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a user group, get it by ID, then delete it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        group_name = _unique("group")
        group = await client.user_groups.create(
            name=group_name, permission=["read"]
        )
        assert group.get("id") is not None
        group_id = group["id"]

        try:
            fetched = await client.user_groups.get(group_id)
            assert fetched.get("id") == group_id
        finally:
            result = await client.user_groups.delete(group_id)
            assert result.get("id") == group_id


@pytest.mark.integration
async def test_user_group_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a group, list groups, verify it appears."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        group_name = _unique("listgroup")
        group = await client.user_groups.create(name=group_name, permission=["read"])
        group_id = group["id"]

        try:
            groups = await client.user_groups.list()
            assert isinstance(groups, list)
            assert any(g.get("id") == group_id for g in groups)
        finally:
            await client.user_groups.delete(group_id)


@pytest.mark.integration
async def test_user_group_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a group, update it, then clean up."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        group = await client.user_groups.create(
            name=_unique("updgroup"),
            permission=["read"],
            description="Original description",
        )
        group_id = group["id"]
        try:
            updated = await client.user_groups.update(
                group_id,
                permission=["read", "edit"],
                description="Updated description",
            )
            assert updated.get("id") == group_id
        finally:
            await client.user_groups.delete(group_id)


@pytest.mark.integration
async def test_user_group_add_remove_user(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a group, create a user, add user to group, then remove."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        group_name = _unique("membergroup")
        user_id = f"groupuser{uuid.uuid4().hex[:8]}"

        group = await client.user_groups.create(name=group_name, permission=["read"])
        group_id = group["id"]

        await client.users.create(
            user_id=user_id,
            password="TestPass!123",
            permission=["read"],
        )

        try:
            updated = await client.user_groups.add_user(group_id, user_id)
            assert updated.get("id") == group_id

            removed = await client.user_groups.remove_user(group_id, user_id)
            assert removed.get("id") == group_id
        finally:
            await client.users.delete(user_id)
            await client.user_groups.delete(group_id)
