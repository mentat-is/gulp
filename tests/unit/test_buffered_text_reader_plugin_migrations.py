"""Coverage for point-3 buffered text-reader plugin migrations."""

from pathlib import Path

import pytest

from gulp.plugins.mysql_general import MySQLLogParser


_BUFFERED_TEXT_PLUGINS = (
    Path("src/gulp/plugins/apache_access_clf.py"),
    Path("src/gulp/plugins/apache_error_clf.py"),
    Path("src/gulp/plugins/iis_access.py"),
    Path("src/gulp/plugins/iis_access_ncsa.py"),
    Path("src/gulp/plugins/iis_access_w3c.py"),
    Path("src/gulp/plugins/json.py"),
    Path("src/gulp/plugins/mem_proc_fs.py"),
    Path("src/gulp/plugins/mem_proc_fs_ntfs.py"),
    Path("src/gulp/plugins/mem_proc_fs_web.py"),
    Path("src/gulp/plugins/mysql_error.py"),
    Path("src/gulp/plugins/mysql_general.py"),
    Path("src/gulp/plugins/pfsense.py"),
    Path("src/gulp/plugins/regex.py"),
    Path("src/gulp/plugins/suricata_fast_log.py"),
)


@pytest.mark.unit
@pytest.mark.parametrize("plugin_path", _BUFFERED_TEXT_PLUGINS)
def test_text_ingestion_plugins_use_shared_buffered_reader(plugin_path: Path) -> None:
    """Every compatible core text ingestion reader uses muty.file."""
    source = plugin_path.read_text(encoding="utf-8")

    assert "muty.file.iter_text_file_lines" in source
    assert "aiofiles.open" not in source


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mysql_general_reader_preserves_multiline_records(tmp_path: Path) -> None:
    """The buffered physical-line source retains MySQL record grouping."""
    path = tmp_path / "mysql-general.log"
    path.write_bytes(
        b"240101 12:00:00 1 Query SELECT 1\n"
        b"FROM dual\n"
        b"240101 12:00:01 2 Query SELECT 2\r\n"
        b"240101 12:00:02 3 Quit\n"
    )

    async with MySQLLogParser(
        str(path), type="query", preserve_newlines=True, encoding="utf-8"
    ) as parser:
        records = [record async for record in parser]

    assert [record["query"] for record in records] == [
        "SELECT 1\nFROM dual",
        "SELECT 2",
    ]
    assert [record["original"] for record in records] == [
        "240101 12:00:00 1 Query SELECT 1\nFROM dual\n",
        "240101 12:00:01 2 Query SELECT 2\n",
    ]
