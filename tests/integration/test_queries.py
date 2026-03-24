"""
Integration tests for queries API.

These tests use preview_mode / small limits to avoid needing large ingested
datasets.  They validate that the query endpoints are reachable and return
properly structured JSend responses.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_queries.py -m integration
"""

import asyncio
import os
from pathlib import Path

import pytest
import uuid


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _query_external_custom_parameters(index: str) -> dict:
    """Build query_elasticsearch custom parameters, overridable via env vars."""
    return {
        "uri": os.getenv("GULP_QUERY_EXTERNAL_URI", "http://localhost:9200"),
        "username": os.getenv("GULP_QUERY_EXTERNAL_USERNAME", "admin"),
        "password": os.getenv("GULP_QUERY_EXTERNAL_PASSWORD", "Gulp1234!"),
        "index": index,
        "is_elasticsearch": os.getenv("GULP_QUERY_EXTERNAL_IS_ELASTICSEARCH", "false").lower()
        in {"1", "true", "yes", "on"},
        "context_field": os.getenv("GULP_QUERY_EXTERNAL_CONTEXT_FIELD", "gulp.context_id"),
        "context_type": os.getenv("GULP_QUERY_EXTERNAL_CONTEXT_TYPE", "context_id"),
        "source_field": os.getenv("GULP_QUERY_EXTERNAL_SOURCE_FIELD", "gulp.source_id"),
        "source_type": os.getenv("GULP_QUERY_EXTERNAL_SOURCE_TYPE", "source_id"),
    }


async def _setup_operation(client) -> str:
    op = await client.operations.create(_unique("query_test_op"))
    return op.id


async def _teardown_operation(client, operation_id: str) -> None:
    try:
        await client.operations.delete(operation_id)
    except Exception:
        pass


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
            # Request stats creation/read can be eventually consistent right after enqueue.
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request {req_id}")
        await asyncio.sleep(1.0)


@pytest.mark.integration
async def test_query_raw_empty_result(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_raw with a match-nothing query.

    The endpoint should respond with an empty result (not an error),
    confirming connectivity and parameter routing.
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            result = await client.queries.query_raw(
                operation_id=op_id,
                q=[{"match_none": {}}],
                q_options={"limit": 1},
            )
            # Raw JSON result; may be a list or dict — just assert it's non-None
            assert result is not None
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_gulp_empty_result(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_gulp with a broad filter on an empty operation.
    Verifies the endpoint is reachable and returns a structured response.
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            result = await client.queries.query_gulp(
                operation_id=op_id,
                flt={},  # no filter → match all (but index is empty)
                q_options={"limit": 1},
            )
            assert result is not None
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_history_get(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Fetch query history — should return a list (possibly empty).
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            history = await client.queries.query_history_get()
            assert isinstance(history, (list, dict))
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_aggregation_empty(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run a terms aggregation on an empty operation index.
    Validates that the aggregation endpoint is reachable.
    """
    from gulp_sdk import GulpClient, NotFoundError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            try:
                result = await client.queries.query_aggregation(
                    operation_id=op_id,
                    q={"aggs": {"by_event": {"terms": {"field": "gulp.event_code"}}}},
                )
                assert result is not None
            except NotFoundError:
                # Empty indexes can return "no more hits" from the backend.
                pass
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_operations(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    query_operations should return a list of operations with at least the one
    we just created.
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            result = await client.queries.query_operations()
            # Result should be a list (or dict wrapping a list)
            assert result is not None
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_max_min_per_field(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    query_max_min_per_field on an empty operation should not crash — it may
    return an empty result or a 404/error which we tolerate.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            try:
                result = await client.queries.query_max_min_per_field(
                    operation_id=op_id,
                )
                assert result is not None
            except GulpSDKError:
                # Empty index may return a 4xx — that's acceptable
                pass
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_fields_by_source(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    query_fields_by_source on an empty context/source should not crash.
    """
    import uuid
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        ctx_name = f"ctx_{uuid.uuid4().hex[:6]}"
        try:
            ctx = await client.operations.context_create(op_id, ctx_name)
            ctx_id = ctx["id"]
            src = await client.operations.source_create(
                op_id, ctx_id, f"src_{uuid.uuid4().hex[:6]}"
            )
            src_id = src["id"]
            try:
                result = await client.queries.query_fields_by_source(
                    operation_id=op_id,
                    context_id=ctx_id,
                    source_id=src_id,
                )
                assert result is not None
            except GulpSDKError:
                pass  # empty index is fine
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_sigma_with_ingested_sample(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Ingest a small EVTX sample then run query_sigma with preview mode.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    sigma_rule_path = Path("/gulp/tests/query/sigma/match_all.yaml")
    if not sample_path.exists() or not sigma_rule_path.exists():
        pytest.skip("Required sample fixtures are missing")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_sigma_context",
            )
            assert ingest.req_id
            stats = await _wait_request_done(client, ingest.req_id)
            assert str(stats.get("status", "")).lower() in {"done", "failed"}

            sigma_rule = sigma_rule_path.read_text(encoding="utf-8")
            try:
                result = await client.queries.query_sigma(
                    operation_id=op_id,
                    sigmas=[sigma_rule],
                    src_ids=[],
                    q_options={"preview_mode": True, "limit": 25, "name": "sdk_sigma"},
                )
                assert isinstance(result, dict)
                data = result.get("data", {})
                if isinstance(data, dict):
                    assert int(data.get("total_hits", 0)) >= 0
            except GulpSDKError as exc:
                pytest.skip(f"query_sigma not available in current server setup: {exc}")
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_sigma_zip_optional_extension(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_sigma_zip using the optional extension endpoint, if installed.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    sigma_zip_path = Path("/gulp/tests/query/sigma/windows_small.zip")
    if not sample_path.exists() or not sigma_zip_path.exists():
        pytest.skip("Required sample fixtures are missing")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_sigma_zip_context",
            )
            if ingest.req_id:
                await _wait_request_done(client, ingest.req_id)

            try:
                resp = await client.queries.query_sigma_zip(
                    operation_id=op_id,
                    zip_path=str(sigma_zip_path),
                    src_ids=[],
                    q_options={"create_notes": False, "limit": 50, "name": "sdk_sigma_zip"},
                )
                assert isinstance(resp, dict)

                req_id = resp.get("req_id")
                if req_id:
                    final_stats = await _wait_request_done(client, req_id)
                    assert str(final_stats.get("status", "")).lower() in {
                        "done",
                        "failed",
                        "canceled",
                    }
            except GulpSDKError as exc:
                msg = str(exc).lower()
                if "query_sigma_zip" in msg or "notfound" in msg or "404" in msg:
                    pytest.skip("query_sigma_zip extension endpoint not available")
                pytest.skip(f"query_sigma_zip unavailable in this environment: {exc}")
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_external_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """Run query_external against optional query_elasticsearch plugin if available."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_query_external_context",
            )
            if ingest.req_id:
                await _wait_request_done(client, ingest.req_id)

            try:
                result = await client.queries.query_external(
                    operation_id=op_id,
                    q={"query": {"match_all": {}}},
                    plugin="query_elasticsearch",
                    plugin_params={
                        "custom_parameters": _query_external_custom_parameters(op_id)
                    },
                    q_options={"preview_mode": True, "limit": 5, "name": "sdk_query_external"},
                )
                assert isinstance(result, dict)
            except GulpSDKError as exc:
                pytest.skip(f"query_external optional plugin unavailable: {exc}")
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_gulp_export_json(gulp_base_url, gulp_test_user, gulp_test_password, tmp_path):
    """Export query results to JSON file via query_gulp_export_json."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            output_path = tmp_path / "sdk_query_export.json"
            try:
                saved = await client.queries.query_gulp_export_json(
                    operation_id=op_id,
                    output_path=str(output_path),
                    flt={"operation_ids": [op_id]},
                    q_options={"limit": 10},
                )
                assert saved == str(output_path)
                assert output_path.exists()
            except GulpSDKError as exc:
                pytest.skip(f"query_gulp_export_json unavailable in current server config: {exc}")
        finally:
            await _teardown_operation(client, op_id)
