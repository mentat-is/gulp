from types import SimpleNamespace

import pytest

from gulp.api.s3_api import GulpS3


class NoSuchBucket(Exception):
    pass


class MissingBucketClient:
    exceptions = SimpleNamespace(NoSuchBucket=NoSuchBucket)

    async def list_objects_v2(self, **_kwargs):
        raise NoSuchBucket


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_by_tags_ignores_missing_bucket(monkeypatch):
    s3 = object.__new__(GulpS3)
    monkeypatch.setattr(s3, "_get_client", lambda: _client(MissingBucketClient()))

    result = await s3.delete_by_tags({"operation_id": "op-a"})

    print(result)
    assert result == {
        "status": "not_found",
        "bucket": "gulp",
        "tags": {"operation_id": "op-a"},
        "num_deleted": 0,
    }


async def _client(client):
    return client
