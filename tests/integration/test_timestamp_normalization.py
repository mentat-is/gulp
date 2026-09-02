"""Integration coverage for one-pass main timestamp normalization."""

import datetime
import uuid

import pytest


@pytest.mark.integration
async def test_apache_and_regex_custom_timestamps_are_normalized_once(
    gulp_base_url, gulp_test_user, gulp_test_password, tmp_path
) -> None:
    """Ingest formatted timestamps and verify their indexed time fields."""
    from gulp_sdk import GulpClient

    apache_path = tmp_path / "access.log"
    apache_path.write_text(
        '172.18.1.2 - - [20/Nov/2024:17:15:42 +0100] "GET / HTTP/1.1" '
        '200 12 "-" "agent"\n',
        encoding="utf-8",
    )
    regex_path = tmp_path / "custom.log"
    regex_path.write_text(
        "2024/11/20-16:15:43 accepted\n",
        encoding="utf-8",
    )

    expected_apache = datetime.datetime.strptime(
        "20/Nov/2024:17:15:42 +0100", "%d/%b/%Y:%H:%M:%S %z"
    ).astimezone(datetime.timezone.utc)
    expected_regex = datetime.datetime.strptime(
        "2024/11/20-16:15:43", "%Y/%m/%d-%H:%M:%S"
    ).replace(tzinfo=datetime.timezone.utc)

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        operation = await client.operations.create(
            f"timestamp_normalization_{uuid.uuid4().hex[:8]}"
        )
        try:
            apache_ingest = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="apache_access_clf",
                file_path=str(apache_path),
                context_name="timestamp_apache",
                wait=True,
                timeout=300,
            )
            regex_ingest = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="regex",
                file_path=str(regex_path),
                context_name="timestamp_regex",
                params={
                    "plugin_params": {
                        "custom_parameters": {
                            "regex": r"(?P<timestamp>\S+) (?P<message>.*)",
                            "timestamp_format": "%Y/%m/%d-%H:%M:%S",
                        }
                    }
                },
                wait=True,
                timeout=300,
            )

            assert apache_ingest.status == "done"
            assert regex_ingest.status == "done"

            response = await client.queries.query_raw(
                operation_id=operation.id,
                q={"query": {"match_all": {}}},
                q_options={
                    "preview_mode": True,
                    "preview_mode_max_records": 10,
                    "name": "timestamp_normalization_verify",
                },
            )
            docs = response["data"]["docs"]
            assert response["data"]["total_hits"] == 2

            docs_by_agent = {doc["agent.type"]: doc for doc in docs}
            apache_doc = docs_by_agent["apache_access_clf"]
            regex_doc = docs_by_agent["regex"]

            assert apache_doc["@timestamp"] == expected_apache.isoformat()
            assert apache_doc["gulp.timestamp"] == int(
                expected_apache.timestamp() * 1_000_000_000
            )
            assert regex_doc["@timestamp"] == expected_regex.isoformat()
            assert regex_doc["gulp.timestamp"] == int(
                expected_regex.timestamp() * 1_000_000_000
            )
            assert not apache_doc.get("gulp.timestamp_invalid", False)
            assert not regex_doc.get("gulp.timestamp_invalid", False)
        finally:
            await client.operations.delete(operation.id)


@pytest.mark.integration
async def test_apache_ingestion_applies_complete_time_range(
    gulp_base_url, gulp_test_user, gulp_test_password, tmp_path
) -> None:
    """A complete range excludes timestamps on both sides of its bounds."""
    from gulp_sdk import GulpClient

    apache_path = tmp_path / "filtered-access.log"
    apache_path.write_text(
        "".join(
            [
                "172.18.1.2 - - [20/Nov/2024:17:15:41 +0100] "
                '"GET /before HTTP/1.1" 200 12 "-" "agent"\n',
                "172.18.1.2 - - [20/Nov/2024:17:15:42 +0100] "
                '"GET /inside HTTP/1.1" 200 12 "-" "agent"\n',
                "172.18.1.2 - - [20/Nov/2024:17:15:43 +0100] "
                '"GET /after HTTP/1.1" 200 12 "-" "agent"\n',
            ]
        ),
        encoding="utf-8",
    )
    included_timestamp = datetime.datetime.strptime(
        "20/Nov/2024:17:15:42 +0100", "%d/%b/%Y:%H:%M:%S %z"
    ).astimezone(datetime.timezone.utc)
    included_timestamp_ns = int(included_timestamp.timestamp() * 1_000_000_000)

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        operation = await client.operations.create(
            f"timestamp_filter_{uuid.uuid4().hex[:8]}"
        )
        try:
            ingest = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="apache_access_clf",
                file_path=str(apache_path),
                context_name="timestamp_filter",
                params={
                    "flt": {
                        "time_range": [
                            included_timestamp_ns,
                            included_timestamp_ns,
                        ]
                    }
                },
                wait=True,
                timeout=300,
            )

            assert ingest.status == "done"

            response = await client.queries.query_raw(
                operation_id=operation.id,
                q={"query": {"match_all": {}}},
                q_options={
                    "preview_mode": True,
                    "preview_mode_max_records": 10,
                    "name": "timestamp_filter_verify",
                },
            )
            assert response["data"]["total_hits"] == 1
            assert len(response["data"]["docs"]) == 1
            assert response["data"]["docs"][0]["url.path"] == "/inside"
            assert (
                response["data"]["docs"][0]["gulp.timestamp"] == included_timestamp_ns
            )
        finally:
            await client.operations.delete(operation.id)
