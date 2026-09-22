"""Integration coverage for JSON plugin mapping options."""

import json
import uuid

import pytest


@pytest.mark.integration
@pytest.mark.parametrize(
    "unmapped_as_is", [None, False, True], ids=["default", "disabled", "enabled"]
)
async def test_json_unmapped_as_is_custom_parameter(
    gulp_base_url: str,
    gulp_test_user: str,
    gulp_test_password: str,
    tmp_path,
    unmapped_as_is: bool | None,
) -> None:
    """Keep unmapped source names only when the JSON request enables the option."""
    from gulp_sdk import GulpClient

    source_file = tmp_path / "ecs.jsonl"
    source_file.write_text(
        json.dumps(
            {
                "@timestamp": "2025-03-25T13:35:57.406Z",
                "host": {"name": "sensor"},
                "destination": {
                    "geo": {"location": {"lon": -87.6305, "lat": 41.8835}}
                },
                "event": {"category": ["network"], "type": ["connection", "start"]},
                "related": {"ip": ["10.135.85.18", "52.159.127.243"]},
                "zeek": {"session_id": "test-session"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    custom_parameters = {"mode": "line"}
    if unmapped_as_is is not None:
        custom_parameters["unmapped_as_is"] = unmapped_as_is

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        operation = await client.operations.create(
            f"json_unmapped_as_is_{uuid.uuid4().hex[:8]}"
        )
        try:
            result = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="json",
                file_path=str(source_file),
                context_name="json_mapping_test",
                params={
                    "plugin_params": {
                        "mapping_parameters": {
                            "mapping_file": "elastic.json",
                            "mapping_id": "default",
                        },
                        "custom_parameters": custom_parameters,
                    }
                },
                wait=True,
                timeout=300,
            )
            assert result.status == "done", result.model_dump()

            response = await client.queries.query_raw(
                operation_id=operation.id,
                q={"query": {"match_all": {}}},
                q_options={"preview_mode": True, "name": "json_mapping_verify"},
            )
            documents = response["data"]["docs"]
            assert len(documents) == 1
            document = documents[0]
            assert document["@timestamp"].startswith("2025-03-25T13:35:57.406")
            if unmapped_as_is:
                assert document["host.name"] == "sensor"
                assert document["zeek.session_id"] == "test-session"
                assert document["destination.geo.location"] == {
                    "lon": -87.6305,
                    "lat": 41.8835,
                }
                assert document["event.category"] == ["network"]
                assert document["event.type"] == ["connection", "start"]
                assert document["related.ip"] == ["10.135.85.18", "52.159.127.243"]
                assert "gulp.unmapped" not in document
            else:
                assert "host.name" not in document
                assert "zeek.session_id" not in document
                assert document["gulp.unmapped"]["host.name"] == "sensor"
                assert document["gulp.unmapped"]["zeek.session_id"] == "test-session"
                assert (
                    document["gulp.unmapped"]["destination.geo.location.lon"]
                    == -87.6305
                )
                assert (
                    document["gulp.unmapped"]["destination.geo.location.lat"]
                    == 41.8835
                )
        finally:
            await client.operations.delete(operation.id)


@pytest.mark.integration
async def test_json_unmapped_as_is_keeps_numeric_conflicts_in_flat_object(
    gulp_base_url: str,
    gulp_test_user: str,
    gulp_test_password: str,
    tmp_path,
) -> None:
    """A string in an existing numeric field must not fail the whole bulk."""
    from gulp_sdk import GulpClient

    source_file = tmp_path / "numeric_conflict.jsonl"
    source_file.write_text(
        json.dumps(
            {
                "@timestamp": "2025-03-25T13:35:57.406Z",
                "destination": {"port": r"\pipe\lsass"},
                "source": {"port": 443},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        operation = await client.operations.create(
            f"json_numeric_conflict_{uuid.uuid4().hex[:8]}"
        )
        try:
            result = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="json",
                file_path=str(source_file),
                context_name="json_numeric_conflict_test",
                params={
                    "plugin_params": {
                        "mapping_parameters": {
                            "mapping_file": "elastic.json",
                            "mapping_id": "default",
                        },
                        "custom_parameters": {
                            "mode": "line",
                            "unmapped_as_is": True,
                        },
                    }
                },
                wait=True,
                timeout=300,
            )
            if result.status != "done":
                stats = await client.plugins.request_get(result.req_id)
                pytest.fail(f"ingest status={result.status}, stats={stats}")

            response = await client.queries.query_raw(
                operation_id=operation.id,
                q={"query": {"match_all": {}}},
                q_options={"preview_mode": True, "name": "json_numeric_verify"},
            )
            documents = response["data"]["docs"]
            assert len(documents) == 1
            document = documents[0]
            assert document["source.port"] == 443
            assert "destination.port" not in document
            assert document["gulp.unmapped"]["destination.port"] == r"\pipe\lsass"
        finally:
            await client.operations.delete(operation.id)
