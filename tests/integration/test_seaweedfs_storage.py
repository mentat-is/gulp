"""Live SeaweedFS parity coverage through the existing aiobotocore wrapper."""

import uuid

import pytest

from gulp.api.s3_api import GulpS3


@pytest.mark.integration
async def test_seaweedfs_storage_parity(tmp_path):
    """Exercise authenticated tags, multipart transfer, listing, and deletion."""
    s3 = GulpS3.get_instance()
    bucket = f"gulp-test-{uuid.uuid4().hex}"
    cleanup_errors: list[str] = []

    try:
        created = await s3.create_bucket(bucket)
        print("SeaweedFS bucket create:", created)
        assert created["status"] == "created"

        uploaded = await s3.upload_data(
            b"attachment",
            "small",
            bucket_name=bucket,
            tags={"operation_id": "op-1", "kind": "note_attachment"},
        )
        print("SeaweedFS small upload:", uploaded)
        assert uploaded["status"] == "uploaded"

        large_file = tmp_path / "large.bin"
        large_file.write_bytes(b"x" * (6 * 1024 * 1024))
        multipart = await s3.upload_file(
            str(large_file),
            bucket_name=bucket,
            object_name="large",
            tags={"operation_id": "op-1", "context_id": "ctx-1"},
        )
        print("SeaweedFS multipart upload:", multipart)
        assert multipart["status"] == "uploaded"

        listed = await s3.list_by_tags({"operation_id": "op-1"}, bucket)
        print("SeaweedFS tagged listing:", listed)
        assert {obj["storage_id"] for obj in listed["objects"]} == {
            "small",
            "large",
        }
        assert await s3.download_data("small", bucket) == b"attachment"

        deleted = await s3.delete_by_tags(
            {"kind": "note_attachment"}, bucket
        )
        print("SeaweedFS selective deletion:", deleted)
        assert deleted["num_deleted"] == 1
        remaining = await s3.list_by_tags(bucket_name=bucket)
        assert [obj["storage_id"] for obj in remaining["objects"]] == ["large"]
    finally:
        try:
            await s3.delete_by_tags(bucket_name=bucket)
        except Exception as ex:
            cleanup_errors.append(f"object cleanup failed: {ex}")
        try:
            await s3.delete_bucket(bucket)
        except Exception as ex:
            cleanup_errors.append(f"bucket cleanup failed: {ex}")
        try:
            await s3.shutdown()
        except Exception as ex:
            cleanup_errors.append(f"client cleanup failed: {ex}")
        if cleanup_errors:
            raise AssertionError("; ".join(cleanup_errors))
