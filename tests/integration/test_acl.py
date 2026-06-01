"""Integration tests for object ACL APIs."""

import uuid

import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _setup_note(client) -> tuple[str, str]:
    op = await client.operations.create(_unique("acl_test_op"))
    ctx = await client.operations.context_create(op.id, _unique("acl_ctx"))
    src = await client.operations.source_create(op.id, ctx["id"], _unique("acl_src"))
    note = await client.collab.note_create(
        operation_id=op.id,
        context_id=ctx["id"],
        source_id=src["id"],
        name=_unique("acl_note"),
        text="ACL integration test note",
        time_pin=1234567890,
    )
    return op.id, note["id"]


async def _teardown_operation(client, operation_id: str) -> None:
    try:
        await client.operations.delete(operation_id)
    except Exception:
        pass


async def _teardown_user(client, user_id: str) -> None:
    try:
        await client.users.delete(user_id)
    except Exception:
        pass


@pytest.mark.integration
async def test_acl_make_private_and_public(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A note can be toggled between private and public."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id, note_id = await _setup_note(client)
        try:
            private_note = await client.acl.make_private(note_id, "note")
            assert private_note.get("id") == note_id
            assert gulp_test_user in (private_note.get("granted_user_ids") or [])

            public_note = await client.acl.make_public(note_id, "note")
            assert public_note.get("id") == note_id
            assert (public_note.get("granted_user_ids") or []) == []
            assert (public_note.get("granted_user_group_ids") or []) == []
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_acl_add_remove_granted_user(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A user can be granted and revoked on a collab object."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id, note_id = await _setup_note(client)
        granted_user_id = _unique("acl_user")
        await client.users.create(
            user_id=granted_user_id,
            password="TestPass!123",
            permission=["read"],
        )
        try:
            granted = await client.acl.add_granted_user(
                note_id, "note", granted_user_id
            )
            assert granted.get("id") == note_id
            assert granted_user_id in (granted.get("granted_user_ids") or [])

            revoked = await client.acl.remove_granted_user(
                note_id, "note", granted_user_id
            )
            assert revoked.get("id") == note_id
            assert granted_user_id not in (revoked.get("granted_user_ids") or [])
        finally:
            await client.users.delete(granted_user_id)
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_acl_add_remove_granted_group(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A group can be granted and revoked on a collab object."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id, note_id = await _setup_note(client)
        group = await client.user_groups.create(
            name=_unique("acl_group"),
            permission=["read"],
        )
        group_id = group["id"]
        try:
            granted = await client.acl.add_granted_group(note_id, "note", group_id)
            assert granted.get("id") == note_id
            assert group_id in (granted.get("granted_user_group_ids") or [])

            revoked = await client.acl.remove_granted_group(note_id, "note", group_id)
            assert revoked.get("id") == note_id
            assert group_id not in (revoked.get("granted_user_group_ids") or [])
        finally:
            await client.user_groups.delete(group_id)
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_acl_private_note_access_enforced_by_grant(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A read-only user cannot read a private note unless explicitly granted."""
    from gulp_sdk import (
        AuthenticationError,
        GulpClient,
        NotFoundError,
        PermissionError,
    )

    low_user_id = _unique("acllu")
    low_user_password = "TestPass!123"

    async with (
        GulpClient(gulp_base_url) as admin_client,
        GulpClient(gulp_base_url) as low_client,
    ):
        await admin_client.auth.login(gulp_test_user, gulp_test_password)
        op_id, note_id = await _setup_note(admin_client)

        await admin_client.users.create(
            user_id=low_user_id,
            password=low_user_password,
            permission=["read"],
        )
        await admin_client.acl.add_granted_user(op_id, "operation", low_user_id)

        try:
            await low_client.auth.login(low_user_id, low_user_password)
            await admin_client.acl.make_private(note_id, "note")

            with pytest.raises((AuthenticationError, PermissionError, NotFoundError)):
                await low_client.collab.note_get_by_id(note_id)

            granted = await admin_client.acl.add_granted_user(
                note_id, "note", low_user_id
            )
            assert low_user_id in (granted.get("granted_user_ids") or [])

            fetched = await low_client.collab.note_get_by_id(note_id)
            assert fetched.get("id") == note_id
        finally:
            await _teardown_user(admin_client, low_user_id)
            await _teardown_operation(admin_client, op_id)


@pytest.mark.integration
async def test_acl_granted_read_user_cannot_modify_acl(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A granted read-only user can read an object but cannot modify its ACL."""
    from gulp_sdk import AuthenticationError, GulpClient, PermissionError

    low_user_id = _unique("acllu")
    low_user_password = "TestPass!123"

    async with (
        GulpClient(gulp_base_url) as admin_client,
        GulpClient(gulp_base_url) as low_client,
    ):
        await admin_client.auth.login(gulp_test_user, gulp_test_password)
        op_id, note_id = await _setup_note(admin_client)

        await admin_client.users.create(
            user_id=low_user_id,
            password=low_user_password,
            permission=["read"],
        )
        await admin_client.acl.add_granted_user(op_id, "operation", low_user_id)

        try:
            await low_client.auth.login(low_user_id, low_user_password)
            await admin_client.acl.make_private(note_id, "note")
            await admin_client.acl.add_granted_user(note_id, "note", low_user_id)

            fetched = await low_client.collab.note_get_by_id(note_id)
            assert fetched.get("id") == note_id

            with pytest.raises((AuthenticationError, PermissionError)):
                await low_client.acl.make_public(note_id, "note")
        finally:
            await _teardown_user(admin_client, low_user_id)
            await _teardown_operation(admin_client, op_id)


@pytest.mark.integration
async def test_acl_user_without_operation_grant_cannot_access_operation_objects(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A user without operation grant cannot access objects from that operation."""
    from gulp_sdk import (
        AuthenticationError,
        GulpClient,
        NotFoundError,
        PermissionError,
    )

    low_user_id = _unique("aclnog")
    low_user_password = "TestPass!123"

    async with (
        GulpClient(gulp_base_url) as admin_client,
        GulpClient(gulp_base_url) as low_client,
    ):
        await admin_client.auth.login(gulp_test_user, gulp_test_password)
        op_id, note_id = await _setup_note(admin_client)

        await admin_client.users.create(
            user_id=low_user_id,
            password=low_user_password,
            permission=["read"],
        )

        try:
            await low_client.auth.login(low_user_id, low_user_password)
            await admin_client.acl.make_private(note_id, "note")

            with pytest.raises((AuthenticationError, PermissionError, NotFoundError)):
                await low_client.collab.note_get_by_id(note_id)

            granted = await admin_client.acl.add_granted_user(
                note_id, "note", low_user_id
            )
            assert low_user_id in (granted.get("granted_user_ids") or [])

            with pytest.raises((AuthenticationError, PermissionError, NotFoundError)):
                await low_client.collab.note_get_by_id(note_id)

            op_granted = await admin_client.acl.add_granted_user(
                op_id, "operation", low_user_id
            )
            assert low_user_id in (op_granted.get("granted_user_ids") or [])

            fetched = await low_client.collab.note_get_by_id(note_id)
            assert fetched.get("id") == note_id

            op_revoked = await admin_client.acl.remove_granted_user(
                op_id, "operation", low_user_id
            )
            assert low_user_id not in (op_revoked.get("granted_user_ids") or [])

            public_note = await admin_client.acl.make_public(note_id, "note")
            assert public_note.get("id") == note_id

            with pytest.raises((AuthenticationError, PermissionError, NotFoundError)):
                await low_client.collab.note_get_by_id(note_id)
        finally:
            await _teardown_user(admin_client, low_user_id)
            await _teardown_operation(admin_client, op_id)


@pytest.mark.integration
async def test_acl_creator_can_edit_and_delete_until_operation_access_removed(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """A creator can edit/delete own notes while in operation, but not after revoke."""
    from gulp_sdk import (
        AuthenticationError,
        GulpClient,
        NotFoundError,
        PermissionError,
    )

    creator_user_id = _unique("aclcr")
    creator_password = "TestPass!123"
    other_user_id = _unique("acloth")
    other_user_password = "TestPass!123"

    async with (
        GulpClient(gulp_base_url) as admin_client,
        GulpClient(gulp_base_url) as creator_client,
    ):
        await admin_client.auth.login(gulp_test_user, gulp_test_password)

        op = await admin_client.operations.create(_unique("acl_creator_op"))
        op_id = op.id
        ctx = await admin_client.operations.context_create(op_id, _unique("acl_ctx"))
        src = await admin_client.operations.source_create(
            op_id, ctx["id"], _unique("acl_src")
        )

        await admin_client.users.create(
            user_id=creator_user_id,
            password=creator_password,
            permission=["read", "edit", "delete"],
        )
        await admin_client.users.create(
            user_id=other_user_id,
            password=other_user_password,
            permission=["read", "edit"],
        )
        await admin_client.acl.add_granted_user(op_id, "operation", creator_user_id)
        await admin_client.acl.add_granted_user(op_id, "operation", other_user_id)

        try:
            await creator_client.auth.login(creator_user_id, creator_password)

            note_to_edit = await creator_client.collab.note_create(
                operation_id=op_id,
                context_id=ctx["id"],
                source_id=src["id"],
                name=_unique("creator_note_edit"),
                text="creator original text",
                time_pin=1234567890,
            )
            note_to_edit_id = note_to_edit["id"]

            note_to_delete = await creator_client.collab.note_create(
                operation_id=op_id,
                context_id=ctx["id"],
                source_id=src["id"],
                name=_unique("creator_note_delete"),
                text="creator delete text",
                time_pin=1234567891,
            )
            note_to_delete_id = note_to_delete["id"]

            updated = await creator_client.collab.note_update(
                note_to_edit_id, text="creator updated text"
            )
            assert updated.get("id") == note_to_edit_id

            deleted = await creator_client.collab.note_delete(note_to_delete_id)
            assert deleted.get("id") == note_to_delete_id

            async with GulpClient(gulp_base_url) as other_client:
                await other_client.auth.login(other_user_id, other_user_password)
                with pytest.raises(
                    (AuthenticationError, PermissionError, NotFoundError)
                ):
                    await other_client.collab.note_delete(note_to_edit_id)

            op_revoked = await admin_client.acl.remove_granted_user(
                op_id, "operation", creator_user_id
            )
            assert creator_user_id not in (op_revoked.get("granted_user_ids") or [])

            with pytest.raises((AuthenticationError, PermissionError, NotFoundError)):
                await creator_client.collab.note_update(
                    note_to_edit_id, text="creator update after revoke"
                )

            with pytest.raises((AuthenticationError, PermissionError, NotFoundError)):
                await creator_client.collab.note_delete(note_to_edit_id)
        finally:
            await _teardown_user(admin_client, other_user_id)
            await _teardown_user(admin_client, creator_user_id)
            await _teardown_operation(admin_client, op_id)
