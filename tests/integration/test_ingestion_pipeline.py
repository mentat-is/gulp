"""Live coverage for the configured file-ingestion pipeline."""

import uuid
from pathlib import Path

import pytest


@pytest.mark.integration
async def test_apache_pipeline_preserves_all_documents(
    gulp_base_url: str,
    gulp_test_user: str,
    gulp_test_password: str,
) -> None:
    """Exercise an initial full chunk plus a final chunk through the backend."""
    from gulp_sdk import GulpClient

    sample_path = Path("/gulp/samples/apache_clf/access.log")
    if not sample_path.exists():
        pytest.skip(f"Apache sample missing: {sample_path}")
    expected_records = sum(1 for _line in sample_path.open(encoding="utf-8"))

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        operation = await client.operations.create(
            f"ingestion_pipeline_{uuid.uuid4().hex[:8]}"
        )
        try:
            result = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="apache_access_clf",
                file_path=str(sample_path),
                context_name="pipeline_apache",
                wait=True,
                timeout=300,
            )
            assert result.status == "done"

            stats = await client.ingest.status("unused", result.req_id)
            assert stats["status"] == "done"
            ingestion_stats = stats["data"]
            assert ingestion_stats["records_processed"] == expected_records
            assert ingestion_stats["records_ingested"] == expected_records
            assert ingestion_stats["records_failed"] == 0
            assert ingestion_stats["records_skipped"] == 0

            query = await client.queries.query_raw(
                operation_id=operation.id,
                q={"query": {"match_all": {}}},
                q_options={
                    "preview_mode": True,
                    "preview_mode_max_records": expected_records,
                    "name": "pipeline_count_verify",
                },
            )
            assert query["data"]["total_hits"] == expected_records
            documents = query["data"]["docs"]
            assert len(documents) == expected_records
            assert len({document["_id"] for document in documents}) == expected_records
            assert sorted(
                int(document["event.sequence"]) for document in documents
            ) == list(range(expected_records))
        finally:
            await client.operations.delete(operation.id)
