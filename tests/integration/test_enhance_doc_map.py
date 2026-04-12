"""
Integration tests for the enhance_document_map API.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_enhance_doc_map.py -m integration
"""

from gulp_sdk.exceptions import GulpSDKError
import pytest
import uuid


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


async def _cleanup_entry(client, obj_id: str) -> None:
    try:
        await client.plugins.enhance_map_delete(obj_id)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_enhance_map_create_get_delete(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Create an entry with multi-key criteria, fetch it, then delete it."""
    from gulp_sdk import GulpClient

    criteria = {
        "gulp.event_code": 4624,
        "winlog.provider_name": "Microsoft-Windows-Security-Auditing",
    }
    plugin = _unique("win_evtx")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        entry = await client.plugins.enhance_map_create(
            plugin=plugin,
            match_criteria=criteria,
            color="#ff0000",
        )
        assert entry.get("id") is not None
        entry_id = entry["id"]

        try:
            # Verify stored criteria
            fetched = await client.plugins.enhance_map_get(entry_id)
            assert fetched["id"] == entry_id
            assert fetched["match_criteria"] == criteria
            assert fetched["plugin"] == plugin
            assert fetched["color"] == "#ff0000"

            # Delete
            result = await client.plugins.enhance_map_delete(entry_id)
            assert result.get("id") == entry_id
        except Exception:
            await _cleanup_entry(client, entry_id)
            raise


@pytest.mark.integration
async def test_enhance_map_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create an entry, update its color, verify the change."""
    from gulp_sdk import GulpClient

    criteria = {"gulp.event_code": 4625}
    plugin = _unique("win_evtx")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        entry = await client.plugins.enhance_map_create(
            plugin=plugin,
            match_criteria=criteria,
            color="#00ff00",
        )
        entry_id = entry["id"]

        try:
            updated = await client.plugins.enhance_map_update(
                entry_id, color="#0000ff"
            )
            assert updated["id"] == entry_id
            assert updated["color"] == "#0000ff"
        finally:
            await _cleanup_entry(client, entry_id)


@pytest.mark.integration
async def test_enhance_map_list_by_plugin(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Create two entries for the same plugin and verify they appear in a filtered list."""
    from gulp_sdk import GulpClient

    plugin = _unique("csv")
    criteria_a = {"status": "error", "level": "critical"}
    criteria_b = {"status": "warning"}

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        entry_a = await client.plugins.enhance_map_create(
            plugin=plugin,
            match_criteria=criteria_a,
            color="#ff0000",
        )
        entry_b = await client.plugins.enhance_map_create(
            plugin=plugin,
            match_criteria=criteria_b,
            color="#ffff00",
        )

        try:
            # List with plugin filter (uses model_extra in GulpCollabFilter)
            entries = await client.plugins.enhance_map_list(flt={"plugin": plugin})
            ids = {e["id"] for e in entries}
            assert entry_a["id"] in ids
            assert entry_b["id"] in ids
        finally:
            await _cleanup_entry(client, entry_a["id"])
            await _cleanup_entry(client, entry_b["id"])


@pytest.mark.integration
async def test_enhance_map_idempotent_id(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Creating the same criteria+plugin combination twice should return the same ID."""
    from gulp_sdk import GulpClient

    criteria = {"gulp.event_code": 7045}
    plugin = _unique("win_evtx")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        entry1 = await client.plugins.enhance_map_create(
            plugin=plugin,
            match_criteria=criteria,
            color="#aabbcc",
        )
        entry_id = entry1["id"]

        try:
            # Second create with same criteria+plugin — expect conflict/same ID
            from gulp_sdk.exceptions import GulpSDKError

            try:
                entry2 = await client.plugins.enhance_map_create(
                    plugin=plugin,
                    match_criteria=criteria,
                    color="#112233",
                )
                # If the server returns success, the same ID must be returned
                assert entry2["id"] == entry_id
            except GulpSDKError:
                # A conflict error is also acceptable
                pass
        finally:
            await _cleanup_entry(client, entry_id)


@pytest.mark.integration
async def test_enhance_map_numeric_operators(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Create an entry with numeric comparison operators (eq, gte, lte)."""
    from gulp_sdk import GulpClient

    # Criteria with various operator types
    criteria = {
        "event_code": {"eq": 4624},  # exact match
        "severity_level": {"gte": 5, "lte": 10},  # range match
        "retry_count": {"gte": 0},  # greater than or equal
        "status": "active",  # simple string match (backwards compatible)
    }
    plugin = _unique("win_evtx")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        entry = await client.plugins.enhance_map_create(
            plugin=plugin,
            match_criteria=criteria,
            color="#ff00ff",
        )
        assert entry.get("id") is not None
        entry_id = entry["id"]

        try:
            # Verify stored criteria with operators
            fetched = await client.plugins.enhance_map_get(entry_id)
            assert fetched["id"] == entry_id
            assert fetched["match_criteria"]["event_code"] == {"eq": 4624}
            assert fetched["match_criteria"]["severity_level"] == {"gte": 5, "lte": 10}
            assert fetched["match_criteria"]["status"] == "active"
            assert fetched["color"] == "#ff00ff"

            # Delete
            result = await client.plugins.enhance_map_delete(entry_id)
            assert result.get("id") == entry_id
        except Exception:
            await _cleanup_entry(client, entry_id)
            raise


@pytest.mark.integration
async def test_enhance_map_empty_criteria_rejected(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Creating an entry with an empty match_criteria dict should be rejected."""
    from gulp_sdk import GulpClient
    
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        with pytest.raises(GulpSDKError):
            await client.plugins.enhance_map_create(
                plugin="win_evtx",
                match_criteria={},
                color="#ff0000",
            )


@pytest.mark.integration
async def test_enhance_map_no_color_or_glyph_rejected(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Creating an entry with neither color nor glyph_id should be rejected."""
    from gulp_sdk import GulpClient
    
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        with pytest.raises(GulpSDKError):
            await client.plugins.enhance_map_create(
                plugin="win_evtx",
                match_criteria={"gulp.event_code": 1},
            )
