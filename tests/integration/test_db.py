"""
Integration tests for the OpenSearch admin (db) API.

Requires a live Gulp server with an admin account.
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_db.py -m integration
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _wait_request_done(client, req_id: str, timeout: float = 180.0) -> dict:
    """Poll request status until it reaches a terminal state."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            stats = await client.plugins.request_get(req_id)
            status = str(stats.get("status", "")).lower()
            if status in {"done", "failed", "canceled"}:
                return stats
        except Exception:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request {req_id}")
        await asyncio.sleep(1.0)


async def _delete_operation_with_retry(
    client, operation_id: str, timeout: float = 30.0
) -> None:
    """Delete operation tolerating transient running-request state."""
    try:
        await client.plugins.request_delete(operation_id)
    except Exception:
        pass

    deadline = asyncio.get_running_loop().time() + timeout
    last_exc: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            await client.operations.delete(operation_id)
            return
        except Exception as exc:
            last_exc = exc
            if "running requests" not in str(exc).lower():
                raise
            await asyncio.sleep(1.0)
    if last_exc:
        raise last_exc


def _parse_utc_timestamp(value: str) -> datetime:
    """Parse a UTC ISO8601 timestamp emitted by Gulp/OpenSearch into a datetime."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    if "." in normalized:
        head, tail = normalized.split(".", 1)
        if "+" in tail:
            fraction, tz = tail.split("+", 1)
            normalized = f"{head}.{fraction[:6].ljust(6, '0')}+{tz}"
        elif "-" in tail:
            fraction, tz = tail.split("-", 1)
            normalized = f"{head}.{fraction[:6].ljust(6, '0')}-{tz}"

    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


async def _query_doc_by_id(client, operation_id: str, doc_id: str) -> dict:
    """Fetch a single preview document by OpenSearch _id."""
    result = await client.queries.query_single_id(
        operation_id=operation_id,
        doc_id=doc_id,
    )
    return result


@pytest.mark.integration
async def test_list_indexes(gulp_base_url, gulp_test_user, gulp_test_password):
    """list_indexes should return a list of index names."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            result = await client.db.list_indexes()
            assert isinstance(result, (list, dict))
        except GulpSDKError:
            pytest.skip("db.list_indexes not available in current server config")


@pytest.mark.integration
async def test_refresh_index(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    refresh_index on the default index should not raise.
    If the index does not exist the server may return 4xx — tolerate that.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"db_test_{uuid.uuid4().hex[:8]}")
        try:
            try:
                result = await client.db.refresh_index(op.id)
                assert result is not None
            except GulpSDKError:
                pass  # non-existent index returns 4xx
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_rebase_by_query_optional(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """rebase_by_query should be callable after a small ingest (optional by server config)."""
    from gulp_sdk import GulpClient, GulpSDKError

    one_day_msec = 24 * 60 * 60 * 1000
    one_day_nsec = one_day_msec * 1_000_000

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("db_rebase_test"))
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_db_rebase_context",
            )
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)

            # get first document's original timestamps for later verification
            results = await client.queries.query_raw(
                operation_id=op.id,
                q=[{"query": {"match_all": {}}}],
                q_options={"preview_mode": True},
            )
            original_doc = results["data"]["docs"][0]
            original_timestamp = original_doc["@timestamp"]
            target_doc_id = original_doc["_id"]
            # expected rebased @timestamp is original + 1 day
            original_gulp_timestamp = int(original_doc["gulp.timestamp"])
            expected_rebased_gulp_timestamp = original_gulp_timestamp + one_day_nsec

            try:
                result = await client.db.rebase_by_query(
                    operation_id=op.id,
                    ws_id=client.ws_id,
                    offset_msec=one_day_msec,
                    flt={"operation_ids": [op.id]},
                    wait=True,
                    timeout=300,
                )
                assert isinstance(result, dict)
                assert str(result.get("status", "")).lower() == "done"
                assert int(result.get("data", {}).get("updated", 0)) == 7

                rebased_doc = await client.queries.query_single_id(
                    operation_id=op.id,
                    doc_id=target_doc_id,
                )
                assert rebased_doc["_id"] == target_doc_id
                assert (
                    int(rebased_doc["gulp.timestamp"])
                    == expected_rebased_gulp_timestamp
                )
                assert _parse_utc_timestamp(rebased_doc["@timestamp"]) == (
                    _parse_utc_timestamp(original_timestamp) + timedelta(days=1)
                )
            except GulpSDKError as exc:
                pytest.skip(
                    f"db.rebase_by_query unavailable in current server config: {exc}"
                )
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_delete_index_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """delete_index endpoint is reachable (non-existent index is acceptable)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        fake_index = _unique("sdk_nonexistent_index")
        try:
            result = await client.db.delete_index(fake_index, delete_operation=False)
            assert isinstance(result, dict)
        except GulpSDKError:
            # 404 / unavailable is acceptable for this reachability check.
            pass
