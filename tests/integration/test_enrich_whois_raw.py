"""Integration test coverage for enrich_whois on raw documents."""

import asyncio
import uuid

import pytest

from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.structs import GulpMappingParameters, GulpPluginParameters


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _delete_operation_with_conflict_wait(
    client, operation_id: str, timeout: float = 30.0
) -> None:
    """Delete operation, tolerating transient 'running requests' state."""
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
async def test_enrich_whois_raw_documents(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """
    Ingest a raw document that materializes ip.destination via mapping, then test:
    - enrich_documents on ip.destination
    - enrich_single_id with an explicit destination.ip value
    """
    import json as _json

    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("enrich_whois_raw_op"))
        try:
            payload = [
                {
                    "ts": "2024-01-01T00:00:00Z",
                    "ctx": "whois_raw_ctx",
                    "src": "whois_raw_src",
                    "dst_ip": "1.1.1.1",
                    "event.original": "whois raw mapping test",
                }
            ]

            mapping = GulpMapping(
                fields={
                    "ts": GulpMappingField(ecs=["@timestamp"]),
                    "ctx": GulpMappingField(is_gulp_type="context_name"),
                    "src": GulpMappingField(is_gulp_type="source_name"),
                    "dst_ip": GulpMappingField(ecs=["ip.destination"]),
                }
            )
            plugin_params = GulpPluginParameters(
                mapping_parameters=GulpMappingParameters(
                    mappings={"whois_raw": mapping},
                    mapping_id="whois_raw",
                )
            )

            ingest = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=_json.dumps(payload).encode(),
                params={
                    "plugin_params": plugin_params.model_dump(by_alias=True),
                    "last": True,
                },
                wait=True,
            )
            assert ingest.req_id

            await asyncio.sleep(2.0)

            preview = await client.queries.query_raw(
                operation_id=op.id,
                q=[{"query": {"match_all": {}}}],
                q_options={
                    "preview_mode": True,
                    "limit": 10,
                    "name": "whois_raw_preview",
                },
            )
            docs = (preview.get("data") or {}).get("docs") or []
            assert len(docs) >= 1, f"Expected >=1 ingested doc, got {len(docs)}"
            doc_id = docs[0].get("_id") or docs[0].get("id")
            assert doc_id, "Could not determine document id from preview response"

            fetched = await client.queries.query_single_id(op.id, str(doc_id))
            assert fetched.get("ip.destination") == "1.1.1.1"

            try:
                bulk_result = await client.enrich.enrich_documents(
                    operation_id=op.id,
                    plugin="enrich_whois",
                    fields={"ip.destination": None},
                    flt={"operation_ids": [op.id]},
                    plugin_params={"custom_parameters": {}},
                    wait=True,
                    timeout=300,
                )
                assert isinstance(bulk_result, dict)
                assert str(bulk_result.get("status", "")).lower() in {
                    "done",
                    "failed",
                    "canceled",
                }

                fetched = await client.queries.query_single_id(op.id, str(doc_id))
                enriched = fetched.get("gulp.enriched", {})
                assert "enrich_whois" in enriched
                assert "ip.destination" in enriched.get("enrich_whois", {})

                single_result = await client.enrich.enrich_single_id(
                    operation_id=op.id,
                    doc_id=str(doc_id),
                    plugin="enrich_whois",
                    fields={"ip.destination": None},
                    plugin_params={"custom_parameters": {}},
                )
                assert isinstance(single_result, dict)
                assert "gulp.enriched" in single_result
                single_enriched = single_result.get("gulp.enriched", {})
                assert "enrich_whois" in single_enriched
                assert "ip.destination" in single_enriched.get("enrich_whois", {})

                fetched = await client.queries.query_single_id(op.id, str(doc_id))
                enriched = fetched.get("gulp.enriched", {})
                assert "ip.destination" in enriched.get("enrich_whois", {})

                # again but with explicitly provided value, to test that path of enrich_single_id
                single_result = await client.enrich.enrich_single_id(
                    operation_id=op.id,
                    doc_id=str(doc_id),
                    plugin="enrich_whois",
                    fields={"ip_to_test": "151.1.1.1"}, # this may or not may exist in the doc
                    plugin_params={"custom_parameters": {}},
                )
                print(single_result)
                assert isinstance(single_result, dict)
                assert "gulp.enriched" in single_result
                single_enriched = single_result.get("gulp.enriched", {})
                assert "enrich_whois" in single_enriched
                assert "ip_to_test" in single_enriched.get("enrich_whois", {})

                fetched = await client.queries.query_single_id(op.id, str(doc_id))
                enriched = fetched.get("gulp.enriched", {})
                assert "ip_to_test" in enriched.get("enrich_whois", {})

            except GulpSDKError as exc:
                pytest.skip(f"enrich_whois plugin unavailable: {exc}")
        finally:
            await _delete_operation_with_conflict_wait(client, op.id)
