"""Integration tests for raw plugin mapping behaviors."""

import asyncio
import uuid

import pytest

from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.structs import GulpMappingParameters, GulpPluginParameters


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _delete_operation_with_retry(
    client, operation_id: str, timeout: float = 30.0
) -> None:
    """Delete operation tolerating transient running-request state."""
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


async def _query_docs(client, operation_id: str, timeout: float = 15.0) -> list[dict]:
    """Poll preview query until docs are visible or timeout expires."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = await client.queries.query_raw(
            operation_id=operation_id,
            q=[{"query": {"match_all": {}}}],
            q_options={"preview_mode": True, "limit": 50, "name": "raw_plugin_preview"},
        )
        docs = (result.get("data") or {}).get("docs") or []
        if docs:
            return docs
        if asyncio.get_running_loop().time() >= deadline:
            return docs
        await asyncio.sleep(0.5)


@pytest.mark.integration
async def test_raw_ingest_without_explicit_mapping(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Raw plugin should ingest docs with default mapping and synthesize event.original."""
    from gulp_sdk import GulpClient

    marker = _unique("raw_no_mapping")
    payload = [
        {
            "gulp.context_id": f"ctx_{marker}",
            "gulp.source_id": f"src_{marker}",
            "custom.marker": marker,
            "custom.value": 42,
        }
    ]

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("raw_no_mapping_op"))

        try:
            result = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=payload,
                params={"last": True},
                wait=True,
                timeout=300,
            )
            assert result.req_id
            assert str(result.status).lower() in {
                "done",
                "failed",
                "canceled",
                "success",
            }

            docs = await _query_docs(client, op.id)
            assert len(docs) >= 1

            event_original = docs[0].get("event.original")
            assert isinstance(event_original, str)
            assert marker in event_original
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_raw_ingest_with_inline_mapping(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Inline mapping should transform timestamp/event code and context/source names."""
    from gulp_sdk import GulpClient

    marker = _unique("raw_inline")
    payload = [
        {
            "ts": "2024-01-01T00:00:00Z",
            "evt": "4711",
            "ctx": f"ctx_{marker}",
            "src": f"src_{marker}",
            "host": f"host_{marker}",
        }
    ]

    mapping = GulpMapping(
        fields={
            "ts": GulpMappingField(ecs=["@timestamp"]),
            "evt": GulpMappingField(ecs=["event.code"]),
            "ctx": GulpMappingField(is_gulp_type="context_name"),
            "src": GulpMappingField(is_gulp_type="source_name"),
            "host": GulpMappingField(ecs=["host.name"]),
        }
    )
    plugin_params = GulpPluginParameters(
        mapping_parameters=GulpMappingParameters(
            mappings={"raw_inline": mapping},
            mapping_id="raw_inline",
        )
    )

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("raw_inline_op"))

        try:
            result = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=payload,
                params={
                    "plugin_params": plugin_params.model_dump(by_alias=True),
                    "last": True,
                },
                wait=True,
                timeout=300,
            )
            assert result.req_id

            docs = await _query_docs(client, op.id)
            assert len(docs) >= 1

            doc = docs[0]
            assert str(doc.get("event.code")) == "4711"
            assert doc.get("host.name") == f"host_{marker}"
            assert doc.get("agent.type") == "raw"
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_raw_ingest_uses_selected_mapping_id(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """When multiple mappings are provided, mapping_id must select the active one."""
    from gulp_sdk import GulpClient

    marker = _unique("raw_map_id")
    payload = [
        {
            "ts": "2024-02-01T00:00:00Z",
            "ctx": f"ctx_{marker}",
            "src": f"src_{marker}",
            "code_a": "CODE_A",
            "code_b": "CODE_B",
        }
    ]

    mapping_a = GulpMapping(
        fields={
            "ts": GulpMappingField(ecs=["@timestamp"]),
            "ctx": GulpMappingField(is_gulp_type="context_name"),
            "src": GulpMappingField(is_gulp_type="source_name"),
            "code_a": GulpMappingField(ecs=["event.code"]),
        }
    )
    mapping_b = GulpMapping(
        fields={
            "ts": GulpMappingField(ecs=["@timestamp"]),
            "ctx": GulpMappingField(is_gulp_type="context_name"),
            "src": GulpMappingField(is_gulp_type="source_name"),
            "code_b": GulpMappingField(ecs=["event.code"]),
        }
    )

    plugin_params = GulpPluginParameters(
        mapping_parameters=GulpMappingParameters(
            mappings={"map_a": mapping_a, "map_b": mapping_b},
            mapping_id="map_b",
        )
    )

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("raw_map_id_op"))

        try:
            result = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=payload,
                params={
                    "plugin_params": plugin_params.model_dump(by_alias=True),
                    "last": True,
                },
                wait=True,
                timeout=300,
            )
            assert result.req_id

            docs = await _query_docs(client, op.id)
            assert len(docs) >= 1

            doc = docs[0]
            assert str(doc.get("event.code")) == "CODE_B"
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_raw_ingest_merges_additional_mappings(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """additional_mappings should merge with the selected mapping and affect output."""
    from gulp_sdk import GulpClient

    marker = _unique("raw_additional")
    payload = [
        {
            "ts": "2024-03-01T00:00:00Z",
            "ctx": f"ctx_{marker}",
            "src": f"src_{marker}",
            "code": "7777",
            "severity": "high",
        }
    ]

    base_mapping = GulpMapping(
        fields={
            "ts": GulpMappingField(ecs=["@timestamp"]),
            "ctx": GulpMappingField(is_gulp_type="context_name"),
            "src": GulpMappingField(is_gulp_type="source_name"),
        }
    )
    additional_mapping = GulpMapping(
        fields={
            "code": GulpMappingField(ecs=["event.code"]),
            "severity": GulpMappingField(ecs=["log.level"]),
        }
    )

    plugin_params = GulpPluginParameters(
        mapping_parameters=GulpMappingParameters(
            mappings={"base": base_mapping},
            mapping_id="base",
            additional_mappings={"addon": additional_mapping},
        )
    )

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("raw_additional_op"))

        try:
            result = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=payload,
                params={
                    "plugin_params": plugin_params.model_dump(by_alias=True),
                    "last": True,
                },
                wait=True,
                timeout=300,
            )
            assert result.req_id

            docs = await _query_docs(client, op.id)
            assert len(docs) >= 1

            doc = docs[0]
            assert str(doc.get("event.code")) == "7777"
            assert doc.get("log.level") == "high"
        finally:
            await _delete_operation_with_retry(client, op.id)
