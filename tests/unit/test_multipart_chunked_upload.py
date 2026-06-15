import asyncio
from types import SimpleNamespace

import pytest

from gulp.api.server import server_utils
from gulp.api.server.server_utils import ServerUtils


class _FakeConfig:
    def __init__(self, upload_dir: str):
        self._upload_dir = upload_dir

    def path_tmp_upload(self) -> str:
        return self._upload_dir


def _request(total_size: int, continue_offset: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        headers={"size": str(total_size), "continue_offset": str(continue_offset)}
    )


def _parts(filename: str, payload: bytes, chunk: bytes) -> tuple[SimpleNamespace, SimpleNamespace]:
    return (
        SimpleNamespace(content=payload, headers={}),
        SimpleNamespace(
            content=chunk,
            headers={
                b"Content-Disposition": (
                    f'form-data; name="f"; filename="{filename}"'
                ).encode("utf-8")
            },
        ),
    )


@pytest.fixture(autouse=True)
def _reset_upload_locks():
    ServerUtils._multipart_upload_locks.clear()
    yield
    ServerUtils._multipart_upload_locks.clear()


@pytest.fixture
def _patch_upload_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(
        server_utils.GulpConfig,
        "get_instance",
        lambda: _FakeConfig(str(tmp_path)),
    )
    return tmp_path


@pytest.mark.asyncio
async def test_multipart_chunked_upload_resumes_from_reported_offset(
    monkeypatch, _patch_upload_dir
):
    chunks = [b"abc", b"def"]

    async def _fake_get_parts(_r):
        return _parts("sample.evtx", b'{"file_sha1": "1f8ac10f23c5b5bc1167bda84b833e5c057a77d2"}', chunks.pop(0))

    monkeypatch.setattr(ServerUtils, "_get_parts", _fake_get_parts)

    file_path, payload, result = await ServerUtils.handle_multipart_chunked_upload(
        _request(total_size=6),
        operation_id="op1",
        context_name="ctx1",
        prefix="req1",
    )
    assert payload["file_sha1"] == "1f8ac10f23c5b5bc1167bda84b833e5c057a77d2"
    assert result.done is False
    assert result.continue_offset == 3

    _, _, result = await ServerUtils.handle_multipart_chunked_upload(
        _request(total_size=6, continue_offset=3),
        operation_id="op1",
        context_name="ctx1",
        prefix="req1",
    )
    assert result.done is True
    assert result.continue_offset == 0
    assert open(file_path, "rb").read() == b"abcdef"


@pytest.mark.asyncio
async def test_multipart_chunked_upload_returns_current_offset_without_appending(
    monkeypatch, _patch_upload_dir
):
    chunks = [b"abc", b"def"]

    async def _fake_get_parts(_r):
        return _parts("sample.evtx", b"{}", chunks.pop(0))

    monkeypatch.setattr(ServerUtils, "_get_parts", _fake_get_parts)

    file_path, _, result = await ServerUtils.handle_multipart_chunked_upload(
        _request(total_size=6),
        operation_id="op1",
        context_name="ctx1",
        prefix="req1",
    )
    assert result.done is False
    assert result.continue_offset == 3

    _, _, result = await ServerUtils.handle_multipart_chunked_upload(
        _request(total_size=6, continue_offset=0),
        operation_id="op1",
        context_name="ctx1",
        prefix="req1",
    )
    assert result.done is False
    assert result.continue_offset == 3
    assert open(file_path, "rb").read() == b"abc"


@pytest.mark.asyncio
async def test_multipart_chunked_upload_rejects_oversized_resume_chunk(
    monkeypatch, _patch_upload_dir
):
    chunks = [b"abc", b"defg"]

    async def _fake_get_parts(_r):
        return _parts("sample.evtx", b"{}", chunks.pop(0))

    monkeypatch.setattr(ServerUtils, "_get_parts", _fake_get_parts)

    file_path, _, result = await ServerUtils.handle_multipart_chunked_upload(
        _request(total_size=6),
        operation_id="op1",
        context_name="ctx1",
        prefix="req1",
    )
    assert result.continue_offset == 3

    with pytest.raises(ValueError, match="exceeds remaining upload size"):
        await ServerUtils.handle_multipart_chunked_upload(
            _request(total_size=6, continue_offset=3),
            operation_id="op1",
            context_name="ctx1",
            prefix="req1",
        )
    assert open(file_path, "rb").read() == b"abc"


@pytest.mark.asyncio
async def test_multipart_chunked_upload_serializes_duplicate_concurrent_chunks(
    monkeypatch, _patch_upload_dir
):
    async def _fake_get_parts(_r):
        await asyncio.sleep(0)
        return _parts("sample.evtx", b"{}", b"abc")

    monkeypatch.setattr(ServerUtils, "_get_parts", _fake_get_parts)

    results = await asyncio.gather(
        ServerUtils.handle_multipart_chunked_upload(
            _request(total_size=6),
            operation_id="op1",
            context_name="ctx1",
            prefix="req1",
        ),
        ServerUtils.handle_multipart_chunked_upload(
            _request(total_size=6),
            operation_id="op1",
            context_name="ctx1",
            prefix="req1",
        ),
    )

    file_path = results[0][0]
    offsets = sorted(result.continue_offset for _, _, result in results)
    assert offsets == [3, 3]
    assert open(file_path, "rb").read() == b"abc"
