import asyncio
import bz2
import gzip
import lzma
import os
import zipfile

import pytest

from gulp.api.server.ingest import _decompress_file_for_ingestion
from gulp.structs import GulpPluginParameters


@pytest.mark.parametrize(
    ("suffix", "writer"),
    [
        (".log.gz", gzip.open),
        (".log.bz2", bz2.open),
        (".log.xz", lzma.open),
    ],
)
def test_decompress_file_for_ingestion(tmp_path, suffix, writer):
    payload = b"line 1\nline 2\n"
    compressed_path = tmp_path / f"sample{suffix}"
    with writer(compressed_path, "wb") as f:
        f.write(payload)

    decompressed_path = asyncio.run(
        _decompress_file_for_ingestion(str(compressed_path))
    )
    try:
        assert decompressed_path != str(compressed_path)
        assert decompressed_path.endswith(".log")
        with open(decompressed_path, "rb") as f:
            assert f.read() == payload
    finally:
        os.unlink(decompressed_path)


def test_decompress_file_for_ingestion_rejects_unknown_format(tmp_path):
    compressed_path = tmp_path / "sample.log.gz"
    compressed_path.write_bytes(b"not really compressed")

    with pytest.raises(ValueError, match="unsupported compressed file format"):
        asyncio.run(_decompress_file_for_ingestion(str(compressed_path)))


def test_decompress_file_for_ingestion_supports_single_file_zip(tmp_path):
    payload = b"line 1\nline 2\n"
    compressed_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(compressed_path, "w") as zf:
        zf.writestr("sample.log", payload)

    decompressed_path = asyncio.run(
        _decompress_file_for_ingestion(str(compressed_path))
    )
    try:
        assert decompressed_path != str(compressed_path)
        assert decompressed_path.endswith(".log")
        with open(decompressed_path, "rb") as f:
            assert f.read() == payload
    finally:
        os.unlink(decompressed_path)


def test_decompress_file_for_ingestion_rejects_multi_file_zip(tmp_path):
    compressed_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(compressed_path, "w") as zf:
        zf.writestr("first.log", b"first")
        zf.writestr("second.log", b"second")

    with pytest.raises(ValueError, match="exactly one file"):
        asyncio.run(_decompress_file_for_ingestion(str(compressed_path)))


def test_compressed_plugin_params_make_params_non_empty():
    assert GulpPluginParameters(compressed=True).compressed is True
    assert not GulpPluginParameters(compressed=True).is_empty()
