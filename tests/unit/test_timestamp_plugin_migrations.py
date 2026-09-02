"""Regression coverage for plugins migrated to one-pass timestamps."""

import datetime
import re

import pytest

from gulp.api.opensearch.structs import GulpDocument
from gulp.plugins.apache_access_clf import Plugin as ApacheAccessPlugin
from gulp.plugins.mysql_general import Plugin as MySQLGeneralPlugin
from gulp.plugins.regex import Plugin as RegexPlugin
from gulp.structs import GulpPluginParameters


def _ready_plugin(plugin) -> None:
    plugin._operation_id = "operation"
    plugin._context_id = "context"
    plugin._source_id = "source"
    plugin._file_path = "/tmp/source.log"
    plugin._plugin_params = GulpPluginParameters()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_apache_access_timestamp_matches_legacy_output() -> None:
    plugin = ApacheAccessPlugin(
        "/tmp/apache_access_clf.py", "gulp.plugins.apache_access_clf"
    )
    _ready_plugin(plugin)
    line = (
        '172.18.1.2 - - [20/Nov/2024:17:15:42 +0100] "GET / HTTP/1.1" '
        '304 0 "-" "agent"'
    )
    parts = [
        r"(?P<host>\S+)",
        r"\S+",
        r"(?P<user>\S+)",
        r"\[(?P<datetime>(?P<date>.*?)(?= ) (?P<timezone>.*?))\]",
        r'"(?P<request_method>.*?) (?P<path>.*?)(?P<request_version> HTTP\/.*)?"',
        r"(?P<status>[0-9]+)",
        r"(?P<size>\S+)",
        r'"(?P<referrer>.*)"',
        r'"(?P<agent>.*)"',
    ]
    pattern = re.compile(r"\s+".join(parts) + r".*\Z")
    timestamp_format = "%d/%b/%Y:%H:%M:%S %z"
    legacy_iso = datetime.datetime.strptime(
        "20/Nov/2024:17:15:42 +0100", timestamp_format
    ).isoformat()
    expected = GulpDocument.ensure_timestamp(legacy_iso)

    document = await plugin._record_to_gulp_document(
        line, 3, regex=pattern, date_format=timestamp_format
    )

    assert (
        document.timestamp,
        document.gulp_timestamp,
        document.invalid_timestamp,
    ) == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_regex_custom_timestamp_rejects_invalid_record() -> None:
    plugin = RegexPlugin("/tmp/regex.py", "gulp.plugins.regex")
    _ready_plugin(plugin)
    timestamp_format = "%Y/%m/%d-%H:%M:%S"
    valid = re.match(
        r"(?P<timestamp>\S+) (?P<message>.*)",
        "2024/11/20-16:15:42 accepted",
    )
    invalid = re.match(
        r"(?P<timestamp>\S+) (?P<message>.*)",
        "not-a-time rejected",
    )

    document = await plugin._record_to_gulp_document(
        valid,
        1,
        line=valid.string,
        timestamp_format=timestamp_format,
    )

    assert document.invalid_timestamp is False
    with pytest.raises(ValueError, match="invalid timestamp"):
        await plugin._record_to_gulp_document(
            invalid,
            2,
            line=invalid.string,
            timestamp_format=timestamp_format,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mysql_general_keeps_invalid_timestamp_document_semantics() -> None:
    plugin = MySQLGeneralPlugin("/tmp/mysql_general.py", "gulp.plugins.mysql_general")
    _ready_plugin(plugin)
    record = {
        "datetime": "not-a-time",
        "cmd_type": "Query",
        "original": "invalid mysql record",
    }

    document = await plugin._record_to_gulp_document(record, 4)

    assert document.timestamp == "1970-01-01T00:00:00Z"
    assert document.gulp_timestamp == 0
    assert document.invalid_timestamp is True
