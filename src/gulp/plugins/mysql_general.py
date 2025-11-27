"""
Plugin for processing MySQL general log files and ingesting their contents into the Gulp platform.

This plugin reads MySQL general log files, parses each record, and converts them into GulpDocument objects for further processing or indexing. It supports asynchronous file reading and integrates with the Gulp ingestion pipeline, handling errors and maintaining ingestion statistics.
"""

import datetime, dateutil
import os
import re
from typing import Any, override

import aiofiles
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import (
    GulpRequestStats
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

class MySQLLogParser:
    def __init__(self, filename, type=None, preserve_newlines=False, separator='', pattern=None):
        self.filename = filename
        self.line_buffer = []
        self.log_pattern = re.compile(r'^(\d{6} [\d:]{8})?\s+(\d+)\s(\w+)(\s+(.*))?', re.DOTALL)
        self._file = None
        self._closed = False

        self.preserve_newlines = preserve_newlines
        self.separator = separator
        self.pattern = pattern
        self.type = type
        
        # track last timestamp per thread ID
        self.thread_timestamps = {}
        self.last_global_timestamp = None
        
    def __aiter__(self):
        return self
        
    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration
            
        if self._file is None:
            self._file = await aiofiles.open(self.filename, 'r')
            
        try:
            while True:
                query_block = await self._get_next_query()
                if query_block == -1:
                    await self._close()
                    raise StopAsyncIteration
                    
                processed = self._process_query(query_block)
                if processed is not None:
                    return processed
        except Exception as e:
            await self._close()
            raise e
            
    async def _close(self):
        if self._file and not self._closed:
            await self._file.close()
            self._closed = True
            
    async def _get_next_query(self):
        query_found = False
        error = False
        in_block = len(self.line_buffer) == 1
        
        while not query_found and not error:
            line = await self._file.readline()
            if not line:  # EOF
                return -1
                
            self.line_buffer.append(line)
            
            # check if line is valid entry
            line_matches_pattern = bool(self.log_pattern.match(self.line_buffer[-1]))

            if not in_block and line_matches_pattern:
                if len(self.line_buffer) == 1:
                    in_block = True
                    
            elif in_block:
                if line_matches_pattern:
                    if len(self.line_buffer) > 1:
                        query_found = ''.join(self.line_buffer[:-1])
                        self.line_buffer = [self.line_buffer[-1]]
                        return query_found
                
            else:
                # Not in block and line doesn't match pattern - so idontcare.
                self.line_buffer.pop(0)
                
        return query_found

    def _parse_log_entry_to_dict(self, query_block):
        """
        Parse a query block into a dict
        """
        if not query_block:
            return None

        result = {
            "original": query_block,
        } 
        query_block = query_block.rstrip('\n\r')
        
        match = self.log_pattern.match(query_block)
        if not match:
            return None
            
        timestamp_str = match.group(1) or ''
        thread_id = match.group(2) or ''
        command_type = (match.group(3) or '').lower()
        query_content = match.group(5) or ''
        
        # inherit timestamp
        if timestamp_str:
            # entry has a timestamp we can save it
            self.last_global_timestamp = timestamp_str
            self.thread_timestamps[thread_id] = timestamp_str
            final_timestamp = timestamp_str
        else:
            # no timestamp, inherit from thread or global
            if thread_id in self.thread_timestamps:
                final_timestamp = self.thread_timestamps[thread_id]
            elif self.last_global_timestamp:
                # fallback to last global timestamp
                final_timestamp = self.last_global_timestamp
                self.thread_timestamps[thread_id] = final_timestamp
            else:
                # no timestamp available
                final_timestamp = None
        
        if not self.preserve_newlines:
            query_content = re.sub(r'[\r\n]', ' ', query_content)

        result.update({
            'thread_id': thread_id,
            'cmd_type': command_type,
            'query': query_content + self.separator,
            'datetime': final_timestamp,
        })

        return result
        
    def _process_query(self, query_block):
        """
        Process query-block, extract query type, content and apply filtering
        """
        entry_dict = self._parse_log_entry_to_dict(query_block)
        if not entry_dict:
            return None
            
        if self.type is None or self.type == '' or entry_dict['cmd_type'] == self.type.lower():
            if self.pattern is not None:
                if re.search(self.pattern, entry_dict['query']):
                    return entry_dict
            else:
                return entry_dict
                
        return None
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close()

class Plugin(GulpPluginBase):
    """
    mysql general logs file processor.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    @override
    def desc(self) -> str:
        return "mysql general logs file processor."

    def display_name(self) -> str:
        return "mysql_general"

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [            
            GulpPluginCustomParameter(
                name="preserve_newlines",
                type="bool",
                desc="preserve newlines in the query",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="pattern",
                type="str",
                desc="if specified, queries must match this pattern",
                default_value=None
            ),
            GulpPluginCustomParameter(
                name="separator",
                type="str",
                desc="separator to use",
                default_value=""
            ),
            GulpPluginCustomParameter(
                name="type",
                type="str",
                desc="type to match (if not specified, returns all types)",
                default_value=None
            )
        ]

    def regex(self) -> str:
        """regex to identify this format"""
        return None

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        d = {}
        print(record)
        time_str = record["datetime"] or 0
        
        try:
            timestamp = datetime.datetime.strptime(time_str, "%y%m%d %H:%M:%S").isoformat()
        except ValueError:
            timestamp = 0
        
        for k, v in record.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_code=record["cmd_type"],
            event_original=record["original"],
            event_sequence=record_idx,
            timestamp=timestamp,
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
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
        **kwargs: Any,
    ) -> GulpRequestStatus:

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
            plugin_params=plugin_params,
            flt=flt,
        )

        preserve_newlines = self._plugin_params.custom_parameters.get("preserve_newlines", False)
        pattern = self._plugin_params.custom_parameters.get("pattern", None)
        separator = self._plugin_params.custom_parameters.get("separator", "")
        mtype = self._plugin_params.custom_parameters.get("type", None)

        doc_idx = 0

        async with MySQLLogParser(
            file_path, 
            type=mtype, 
            preserve_newlines=preserve_newlines, 
            separator=separator, 
            pattern=pattern
            ) as log_src:
            
            async for l in log_src:
                await self.process_record(l, doc_idx, flt=flt)
                doc_idx += 1

        return stats.status