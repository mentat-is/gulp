"""Tests for the shared muty file utilities."""

import asyncio
import os
from pathlib import Path
from zipfile import ZipFile

import pytest

import muty.file


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_only_file_helpers(tmp_path):
    path = tmp_path / "data.bin"
    await muty.file.write_file(str(path), b"one")
    await muty.file.write_file(str(path), b"-two", append=True)

    assert await muty.file.read_file(str(path)) == b"one-two"
    assert await muty.file.exists(str(path))
    assert await muty.file.get_size(str(path)) == 7
    assert await muty.file.get_size(str(tmp_path / "missing")) == 0
    with pytest.raises(FileNotFoundError):
        await muty.file.get_size(str(tmp_path / "missing"), error_if_not_exists=True)
    assert await muty.file.abspath(str(path)) == str(path)

    copied_file = tmp_path / "copied.bin"
    muty.file.copy_file(str(path), str(copied_file))
    assert copied_file.read_bytes() == b"one-two"
    with pytest.raises(FileExistsError):
        muty.file.copy_file(str(path), str(copied_file), overwrite=False)

    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "nested.txt").write_text("nested")
    copied_dir = tmp_path / "copied"
    await muty.file.copy_dir(str(source_dir), str(copied_dir))
    assert (copied_dir / "nested.txt").read_text() == "nested"

    temporary = await muty.file.write_temporary_file(b"temporary", ".bin")
    try:
        assert temporary.endswith(".bin")
        assert await muty.file.read_file(temporary) == b"temporary"
    finally:
        os.unlink(temporary)

    archive = tmp_path / "archive.zip"
    with ZipFile(archive, "w") as zip_file:
        zip_file.writestr("inside.txt", "unzipped")
    extracted = await muty.file.unzip(str(archive))
    try:
        assert (tmp_path / "archive.zip").is_file()
        assert Path(extracted, "inside.txt").read_text() == "unzipped"
    finally:
        await muty.file.delete_file_or_dir(extracted)

    print("validated direct async file helpers")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dual_file_helper_uses_the_sync_implementation(monkeypatch, tmp_path):
    calls = []
    real_to_thread = asyncio.to_thread

    async def _to_thread(function, *args):
        calls.append(function)
        return await real_to_thread(function, *args)

    monkeypatch.setattr(muty.file.asyncio, "to_thread", _to_thread)
    path = tmp_path / "delete-me"
    path.write_text("data")

    listed = await muty.file.list_directory_async(str(tmp_path), files_only=True)
    await muty.file.delete_file_or_dir(str(path))

    assert listed == [str(path)]
    assert not path.exists()
    assert calls == [muty.file.list_directory]
    print("offloaded helpers:", [function.__name__ for function in calls])
