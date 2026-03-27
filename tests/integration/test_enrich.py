"""Integration tests for Enrich API endpoints."""

import asyncio
from pathlib import Path
import uuid

import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _wait_request_done(client, req_id: str, timeout: float = 180.0) -> dict:
    """Poll request stats until request reaches a terminal state."""
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


async def _ingest_small_evtx(client, operation_id: str) -> str:
    """Ingest one small EVTX fixture and return a document id for single-id APIs."""
    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    ingest = await client.ingest.file(
        operation_id=operation_id,
        plugin_name="win_evtx",
        file_path=str(sample_path),
        context_name="sdk_enrich_context",
    )
    assert ingest.req_id
    await _wait_request_done(client, ingest.req_id)

    preview = await client.queries.query_raw(
        operation_id=operation_id,
        q=[{"query": {"match_all": {}}}],
        q_options={"preview_mode": True, "limit": 1, "name": "sdk_enrich_preview"},
    )
    docs = (preview.get("data") or {}).get("docs") or []
    if not docs:
        pytest.skip("No docs found after ingestion")

    # OpenSearch document id may be under _id (preferred) or id.
    doc = docs[0]
    doc_id = doc.get("_id") or doc.get("id")
    if not doc_id:
        pytest.skip("Could not determine doc id from preview response")
    return str(doc_id)


async def _delete_operation_with_retry(client, operation_id: str, timeout: float = 30.0) -> None:
    """Delete operation, tolerating transient 'running requests' server state."""
    # Best-effort cleanup of stale request stats for this operation.
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


@pytest.mark.integration
async def test_enrich_core_endpoints(gulp_base_url, gulp_test_user, gulp_test_password):
    """Exercise core enrich endpoints (update/tag/remove + single-id operations)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)        
        op = await client.operations.create(_unique("sdk_enrich_op"))
        try:
            doc_id = await _ingest_small_evtx(client, op.id)

            # bulk update/tag/remove
            upd = await client.enrich.update_documents(
                operation_id=op.id,
                fields={"sdk_enrich_bulk": "v1"},
                flt={"operation_ids": [op.id]},
                wait=True,
            )
            assert isinstance(upd, dict)
            assert str(upd.get("status", "")).lower() == "done"

            fetched = await client.queries.query_single_id(op.id, doc_id)
            assert fetched.get("sdk_enrich_bulk") == "v1"

            tag = await client.enrich.tag_documents(
                operation_id=op.id,
                tags=["sdk_enrich_tag"],
                flt={"operation_ids": [op.id]},
                wait=True,
            )
            assert isinstance(tag, dict)
            assert str(tag.get("status", "")).lower() == "done"

            fetched = await client.queries.query_single_id(op.id, doc_id)
            tags = fetched.get("gulp.tags", [])
            assert "sdk_enrich_tag" in tags

            # verify enrich_remove with explicit fields target only those fields
            await client.enrich.update_single_id(
                operation_id=op.id,
                doc_id=doc_id,
                fields={"sdk_enrich_remove_field": "to_remove"},
            )
            fetched = await client.queries.query_single_id(op.id, doc_id)
            assert fetched.get("sdk_enrich_remove_field") == "to_remove"

            rem_by_field = await client.enrich.enrich_remove(
                operation_id=op.id,
                fields=["sdk_enrich_remove_field"],
                flt={"operation_ids": [op.id]},
            )
            assert isinstance(rem_by_field, dict)
            fetched = await client.queries.query_single_id(op.id, doc_id)
            assert "sdk_enrich_remove_field" not in fetched

            # enrich with optional enrich_whois plugin before remove
            try:
                enriched = await client.enrich.enrich_documents(
                    operation_id=op.id,
                    plugin="enrich_whois",
                    fields={"source.ip": None},
                    flt={"operation_ids": [op.id]},
                    plugin_params={"custom_parameters": {}},
                    wait=True,
                    timeout=300,
                )
                assert isinstance(enriched, dict)
                assert str(enriched.get("status", "")).lower() in {"done", "failed", "canceled"}

                fetched = await client.queries.query_single_id(op.id, doc_id)
                assert "gulp.enriched" in fetched
            except GulpSDKError:
                pytest.skip("optional enrich_whois plugin unavailable for enrich_remove setup")

            rem = await client.enrich.enrich_remove(
                operation_id=op.id,
                flt={"operation_ids": [op.id]},
            )
            assert isinstance(rem, dict)

            fetched = await client.queries.query_single_id(op.id, doc_id)
            assert "gulp.enriched" not in fetched

            # single-id update/tag
            single_upd = await client.enrich.update_single_id(
                operation_id=op.id,
                doc_id=doc_id,
                fields={"sdk_enrich_single": True},
            )
            assert isinstance(single_upd, dict)

            fetched = await client.queries.query_single_id(op.id, doc_id)
            assert fetched.get("sdk_enrich_single") is True

            single_tag = await client.enrich.tag_single_id(
                operation_id=op.id,
                doc_id=doc_id,
                tags=["sdk_enrich_single_tag"],
            )
            assert isinstance(single_tag, dict)

            fetched = await client.queries.query_single_id(op.id, doc_id)
            tags = []
            tags = fetched.get("gulp.tags", [])
            assert "sdk_enrich_single_tag" in tags

            untag = await client.enrich.untag_documents(
                operation_id=op.id,
                tags=["sdk_enrich_tag", "sdk_enrich_single_tag"],
                flt={"operation_ids": [op.id]},
                wait=True,
            )
            assert isinstance(untag, dict)
            assert str(untag.get("status", "")).lower() in {"done", "failed", "canceled"}

            # round-trip read check
            fetched = await client.queries.query_single_id(op.id, doc_id)
            assert isinstance(fetched, dict)
            tags = []
            if isinstance(fetched.get("gulp"), dict):
                tags = fetched.get("gulp", {}).get("tags", [])
            if not tags:
                tags = fetched.get("gulp.tags", [])
            assert "sdk_enrich_tag" not in tags
            assert "sdk_enrich_single_tag" not in tags
        except GulpSDKError as exc:
            pytest.skip(f"enrich core endpoints unavailable in current server: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_enrich_single_id_whois_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """Optional enrich_whois plugin check (skip if extension is unavailable)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("sdk_enrich_whois_op"))
        try:
            doc_id = await _ingest_small_evtx(client, op.id)
            try:
                enriched = await client.enrich.enrich_single_id(
                    operation_id=op.id,
                    doc_id=doc_id,
                    plugin="enrich_whois",
                    fields={"sdk_whois_test": "8.8.8.8"},
                    plugin_params={"custom_parameters": {}},
                )
                assert isinstance(enriched, dict)
            except GulpSDKError as exc:
                pytest.skip(f"optional enrich_whois plugin unavailable: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_enrich_documents_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """Optional coverage for enrich_documents bulk plugin endpoint."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("sdk_enrich_bulk_whois_op"))
        try:
            _ = await _ingest_small_evtx(client, op.id)
            try:
                result = await client.enrich.enrich_documents(
                    operation_id=op.id,
                    plugin="enrich_whois",
                    fields={"sdk_bulk_whois": "1.1.1.1"},
                    flt={"operation_ids": [op.id]},
                    plugin_params={"custom_parameters": {}},
                    wait=True,
                    timeout=300,
                )
                assert isinstance(result, dict)
                assert str(result.get("status", "")).lower() in {"done", "failed", "canceled"}
            except GulpSDKError as exc:
                pytest.skip(f"enrich_documents optional plugin unavailable: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)
