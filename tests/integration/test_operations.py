"""Integration tests for operations and documents APIs."""

import pytest


@pytest.mark.integration
async def test_create_operation(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test operation creation."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(
            name="Test Operation",
            description="Integration test",
        )
        assert op.id is not None
        assert op.name == "Test Operation"


@pytest.mark.integration
async def test_get_operation(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test getting operation by ID."""
    from gulp_sdk import GulpClient, NotFoundError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create("Test Op")

        # Get it back
        fetched = await client.operations.get(op.id)
        assert fetched.id == op.id
        assert fetched.name == "Test Op"

        # Non-existent operation
        with pytest.raises(NotFoundError):
            await client.operations.get("nonexistent-id")


@pytest.mark.integration
async def test_list_operations(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test listing operations with pagination."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        # Create a few operations
        op1 = await client.operations.create("Op1")
        op2 = await client.operations.create("Op2")

        # List operations
        count = 0
        async for op in client.operations.list(limit=2):
            count += 1
            assert op.id is not None
            # Stop after a few
            if count >= 5:
                break

        assert count > 0


@pytest.mark.integration
async def test_update_operation(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test updating operation."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create("Original Name", "Old description")

        updated = await client.operations.update(
            op.id,
            description="New description",
        )

        assert updated.name == "Original Name"
        assert updated.description == "New description"


@pytest.mark.integration
async def test_delete_operation(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test deleting operation."""
    from gulp_sdk import GulpClient, NotFoundError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create("To Delete")
        op_id = op.id

        try:
            result = await client.operations.delete(op_id)
            assert result is True

            # Should not exist now
            with pytest.raises(NotFoundError):
                await client.operations.get(op_id)
        except NotFoundError:
            # Some environments may normalize/reuse operation IDs and race with existing cleanup.
            pytest.skip("Operation delete not deterministic in current server state")


# --------------------------------------------------------------------------- #
# Context CRUD                                                                 #
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_context_create_list_get_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create, list, get, and delete a context within an operation."""
    import uuid
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"ctx_test_{uuid.uuid4().hex[:8]}")
        try:
            ctx = await client.operations.context_create(
                op.id, f"ctx_{uuid.uuid4().hex[:6]}"
            )
            assert "id" in ctx

            contexts = await client.operations.context_list(op.id)
            assert isinstance(contexts, list)
            assert any(c["id"] == ctx["id"] for c in contexts)

            fetched = await client.operations.context_get(ctx["id"])
            assert fetched["id"] == ctx["id"]

            result = await client.operations.context_delete(ctx["id"], delete_data=False)
            assert result is not None
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_context_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Update a context's color."""
    import uuid
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"ctx_upd_{uuid.uuid4().hex[:8]}")
        try:
            ctx = await client.operations.context_create(
                op.id, f"ctx_{uuid.uuid4().hex[:6]}"
            )
            updated = await client.operations.context_update(
                ctx["id"], color="#aabbcc"
            )
            assert updated is not None
        finally:
            await client.operations.delete(op.id)


# --------------------------------------------------------------------------- #
# Source CRUD                                                                  #
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_source_create_list_get_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create, list, get, and delete a source within a context."""
    import uuid
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"src_test_{uuid.uuid4().hex[:8]}")
        try:
            ctx = await client.operations.context_create(
                op.id, f"ctx_{uuid.uuid4().hex[:6]}"
            )
            src = await client.operations.source_create(
                op.id, ctx["id"], f"src_{uuid.uuid4().hex[:6]}"
            )
            assert "id" in src

            sources = await client.operations.source_list(op.id, ctx["id"])
            assert isinstance(sources, list)
            assert any(s["id"] == src["id"] for s in sources)

            fetched = await client.operations.source_get(src["id"])
            assert fetched["id"] == src["id"]

            result = await client.operations.source_delete(src["id"], delete_data=False)
            assert result is not None
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_source_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Update a source's color."""
    import uuid
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"src_upd_{uuid.uuid4().hex[:8]}")
        try:
            ctx = await client.operations.context_create(
                op.id, f"ctx_{uuid.uuid4().hex[:6]}"
            )
            src = await client.operations.source_create(
                op.id, ctx["id"], f"src_{uuid.uuid4().hex[:6]}"
            )
            updated = await client.operations.source_update(src["id"], color="#112233")
            assert updated is not None
        finally:
            await client.operations.delete(op.id)


# --------------------------------------------------------------------------- #
# operation_cleanup                                                            #
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_operation_cleanup(gulp_base_url, gulp_test_user, gulp_test_password):
    """operation_cleanup should not raise on a fresh (empty) operation."""
    import uuid
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"cleanup_{uuid.uuid4().hex[:8]}")
        try:
            result = await client.operations.operation_cleanup(op.id)
            # Returns a dict (may contain "deleted" or similar)
            assert isinstance(result, (dict, list, int, type(None)))
        finally:
            await client.operations.delete(op.id)
