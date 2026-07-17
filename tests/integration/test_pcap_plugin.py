"""Integration test for the PCAP ingestion plugin."""

from pathlib import Path
import uuid

import pytest


@pytest.mark.integration
async def test_ingest_pcap_sample(gulp_base_url, gulp_test_user, gulp_test_password):
    """Ingest the bundled PCAPNG sample and verify its packet documents."""
    from gulp_sdk import GulpClient

    sample_path = Path("/gulp/samples/pcap/220614_ip_flags_google.pcapng")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        operation = await client.operations.create(
            f"pcap_integration_{uuid.uuid4().hex[:8]}"
        )
        try:
            ingest = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="pcap",
                file_path=str(sample_path),
                context_name="pcap_sample",
                wait=True,
                timeout=300,
            )
            print("pcap ingest:", ingest.model_dump())
            assert ingest.req_id
            assert ingest.status == "done"

            response = await client.queries.query_raw(
                operation_id=operation.id,
                q={"query": {"match_all": {}}},
                q_options={
                    "preview_mode": True,
                    "preview_mode_max_records": 100,
                    "name": "pcap_sample_verify",
                },
            )
            docs = response["data"]["docs"]
            print(
                "pcap query:",
                {"total_hits": response["data"]["total_hits"], "doc": docs[0]},
            )

            assert response["data"]["total_hits"] == 58
            assert len(docs) == 58
            assert all(doc["agent.type"] == "pcap" for doc in docs)
            assert sorted(int(doc["event.sequence"]) for doc in docs) == list(
                range(1, 59)
            )
            assert (
                sum(doc.get("source.address") == "192.168.200.21" for doc in docs) == 36
            )
        finally:
            await client.operations.delete(operation.id)
