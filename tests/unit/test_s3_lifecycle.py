"""S3 transfer resource lifecycle regressions."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from gulp.api.s3_api import GulpS3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancelled_multipart_upload_is_aborted(monkeypatch, tmp_path):
    upload_started = asyncio.Event()

    async def _upload_part(**_kwargs):
        upload_started.set()
        await asyncio.Event().wait()

    client = SimpleNamespace(
        create_multipart_upload=AsyncMock(return_value={"UploadId": "upload-1"}),
        upload_part=AsyncMock(side_effect=_upload_part),
        abort_multipart_upload=AsyncMock(),
    )
    s3 = object.__new__(GulpS3)

    async def _get_client():
        return client

    monkeypatch.setattr(s3, "_get_client", _get_client)
    file_path = tmp_path / "multipart.bin"
    file_path.write_bytes(b"x" * (6 * 1024 * 1024))

    task = asyncio.create_task(s3.upload_file(str(file_path), object_name="object"))
    await upload_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    print(client.abort_multipart_upload.await_args)
    client.abort_multipart_upload.assert_awaited_once_with(
        Bucket="gulp", Key="object", UploadId="upload-1"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_closes_body_when_stream_read_fails(monkeypatch, tmp_path):
    body = SimpleNamespace(
        read=AsyncMock(side_effect=RuntimeError("read failed")),
        close=Mock(),
    )
    client = SimpleNamespace(get_object=AsyncMock(return_value={"Body": body}))
    s3 = object.__new__(GulpS3)

    async def _get_client():
        return client

    monkeypatch.setattr(s3, "_get_client", _get_client)

    with pytest.raises(Exception, match="read failed"):
        await s3.download_file("object", str(tmp_path / "download"))

    print(body.close.call_args)
    body.close.assert_called_once_with()
