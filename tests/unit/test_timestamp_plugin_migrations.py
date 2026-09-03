"""Regression coverage for plugins migrated to one-pass timestamps."""

import datetime
import re
from unittest.mock import patch

import pytest

from gulp.api.opensearch.structs import GulpDocument
from gulp.plugins.apache_access_clf import Plugin as ApacheAccessPlugin
from gulp.plugins.apache_error_clf import Plugin as ApacheErrorPlugin
from gulp.plugins.iis_access import Plugin as IISAccessPlugin
from gulp.plugins.iis_access_ncsa import Plugin as IISNCSAPlugin
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
async def test_apache_access_reuses_only_consecutive_timestamps() -> None:
    plugin = ApacheAccessPlugin(
        "/tmp/apache_access_clf.py", "gulp.plugins.apache_access_clf"
    )
    _ready_plugin(plugin)
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
    lines = [
        '172.18.1.2 - - [20/Nov/2024:17:15:42 +0100] "GET /a HTTP/1.1" '
        '200 12 "-" "agent"',
        '172.18.1.2 - - [20/Nov/2024:17:15:42 +0100] "GET /b HTTP/1.1" '
        '200 12 "-" "agent"',
        '172.18.1.2 - - [20/Nov/2024:17:15:43 +0100] "GET /c HTTP/1.1" '
        '200 12 "-" "agent"',
    ]

    original_normalize = GulpDocument.normalize_timestamp
    with patch.object(
        GulpDocument, "normalize_timestamp", wraps=original_normalize
    ) as normalize_timestamp:
        documents = [
            await plugin._record_to_gulp_document(
                line,
                record_idx,
                regex=pattern,
                date_format=timestamp_format,
            )
            for record_idx, line in enumerate(lines)
        ]

    assert normalize_timestamp.call_count == 2
    assert documents[0].gulp_timestamp == documents[1].gulp_timestamp
    assert documents[1].gulp_timestamp != documents[2].gulp_timestamp
    assert len({document.id for document in documents}) == 3


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plugin_class", "record", "timestamp_format"),
    [
        (
            ApacheErrorPlugin,
            "[Fri Sep 09 10:42:29.902022 2011] [core:error] "
            "[pid 35708:tid 4328636416] [client 72.15.99.187] missing file",
            None,
        ),
        (
            IISAccessPlugin,
            "127.0.0.1,user,11/20/24,17:15:42,W3SVC1,server,127.0.0.1,"
            "15,10,20,200,0,GET,/index.html,-",
            "%m/%d/%y %H:%M:%S",
        ),
        (
            IISNCSAPlugin,
            '127.0.0.1 - alice [20/Nov/2024:17:15:42 +0100] '
            '"GET /index.html HTTP/1.1" 200 123',
            "%d/%b/%Y:%H:%M:%S %z",
        ),
    ],
)
async def test_other_line_parsers_reuse_consecutive_timestamps(
    plugin_class, record: str, timestamp_format: str | None
) -> None:
    """Parser-local fast paths retain documents and use the shared cache."""
    plugin = plugin_class("/tmp/parser.py", "parser_timestamp_test")
    _ready_plugin(plugin)

    original_normalize = GulpDocument.normalize_timestamp
    with patch.object(
        GulpDocument, "normalize_timestamp", wraps=original_normalize
    ) as normalize_timestamp:
        documents = [
            await plugin._record_to_gulp_document(
                record,
                record_idx,
                date_format=timestamp_format,
            )
            for record_idx in range(2)
        ]

    assert normalize_timestamp.call_count == 1
    assert documents[0].gulp_timestamp == documents[1].gulp_timestamp
    assert documents[0].event_sequence == 0
    assert documents[1].event_sequence == 1


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
