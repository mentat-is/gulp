"""
Apache Access Log Common Log Format (CLF) File Processor

This module provides a plugin for processing Apache access logs in Common Log Format (CLF).
It parses log lines using regular expressions to extract fields like host, user, datetime,
request method, path, status, size, referrer, and user agent.

The plugin supports:
- Parsing Apache access.log files in CLF format
- Converting log entries to structured GulpDocument objects
- Custom date format specification
- Query parameter extraction from URLs
- Field normalization for Elasticsearch compatibility

# TODO support gzipped logs from rotated configurations, same for error logs

The main class, Plugin, inherits from GulpPluginBase and implements the necessary methods
for ingesting and processing log files.

"""

import datetime
import os
import re
from typing import Any, override
from urllib.parse import parse_qs, urlparse

import aiofiles
import muty.elastic
import muty.string
import muty.time
import muty.xml
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return "Apache access.log CLF file processor."

    def display_name(self) -> str:
        return "apache_access_clf"

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="date_format",
                type="str",
                desc="server date log format",
                default_value="%d/%b/%Y:%H:%M:%S %z",
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        pattern = kwargs.get("regex")

        matches: re.Match = pattern.match(record.strip("\n"))
        if not matches:
            return None

        event = {
            name: matches.group(name) if matches.groups(name) else None
            for name in [
                "host",
                "user",
                "datetime",
                "date",
                "timezone",
                "request_method",
                "path",
                "request_version",
                "status",
                "size",
                "referrer",
                "agent",
            ]
        }

        url = urlparse(event.get("path"))
        if url:
            query = parse_qs(url.query)
        else:
            query: dict = {}

        # TODO: split netloc into user, pass, port and assign to event
        d: dict = {}

        # map timestamp manually
        time_str = event.pop("datetime")

        # map
        for k, v in event.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        null_param_key = 0
        for pk, pv in query.items():
            if not pk:
                null_param_key += 1
            pk = muty.elastic.normalize_elastic_fieldname(
                pk, null_field_prefix=f"null_param_key{null_param_key}"
            )

            k = "gulp.http.query.params.%s" % (pk)
            mapped = self._process_key(k, pv)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=record,
            timestamp=time_str,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
        )

    @override
    async def ingest_file(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        file_path: str,
        original_file_path: str = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> GulpRequestStatus:
        try:
            if not plugin_params or plugin_params.is_empty():
                plugin_params = GulpPluginParameters(
                    mapping_file="apache_access_clf.json"
                )
            await super().ingest_file(
                sess=sess,
                stats=stats,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                index=index,
                operation_id=operation_id,
                context_id=context_id,
                source_id=source_id,
                file_path=file_path,
                original_file_path=original_file_path,
                flt=flt,
                plugin_params=plugin_params,
                **kwargs,
            )
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        parts = [
            r"(?P<host>\S+)",  # host %h
            r"\S+",  # indent %l (unused)
            r"(?P<user>\S+)",  # user %u
            # date and timezone %t
            # TODO: group timezone sould be optional
            r"\[(?P<datetime>(?P<date>.*?)(?= ) (?P<timezone>.*?))\]",
            # request "%r"
            r"\"(?P<request_method>.*?) (?P<path>.*?)(?P<request_version> HTTP\/.*)?\"",
            r"(?P<status>[0-9]+)",  # status %>s
            r"(?P<size>\S+)",  # size %b (might be '-')
            r"\"(?P<referrer>.*)\"",  # referrer "%{Referer}i"
            r"\"(?P<agent>.*)\"",  # user agent "%{User-agent}i"
        ]
        pattern = re.compile(r"\s+".join(parts) + r".*\Z")

        doc_idx = 0
        l: list[dict] = []
        try:
            async with aiofiles.open(file_path, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    l = l.strip()
                    if not l:
                        continue

                    try:
                        await self.process_record(l, doc_idx, flt=flt, regex=pattern)
                    except (RequestCanceledError, SourceCanceledError) as ex:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)
                        break
                    except PreviewDone:
                        # preview done, stop processing
                        break

                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
