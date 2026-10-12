"""
JSON plugin for GULP - a generic json file processor.

This module provides a plugin to process and ingest JSON data in different formats:

- JSON Lines (one JSON object per line)
- JSON List (a list of JSON objects)
- JSON Dictionary (a dictionary with (one or more) keys containing lists of JSON objects)

NOTE: Used as standalone, it is mandatory to provide a mapping that defines how the JSON keys should be mapped to GULP fields.

It can also be used as base for stacked plugins dealing with specific JSON formats.
"""

import asyncio
import math
import os
import queue
import threading
from collections.abc import Iterator
from typing import Any, override

import orjson

import muty.dict
import muty.file
import muty.os
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.glug import Glug
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.process import GulpProcess
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

# pylint: disable=C0411
muty.os.check_and_install_package("json-stream", ">=2.3.3,<3.0.0")
import json_stream
import json_stream.base


class Plugin(GulpPluginBase):
    JSON_QUEUE_MAX_SIZE = 100
    NUMERIC_INDEX_TYPES = frozenset(
        {
            "byte",
            "short",
            "integer",
            "long",
            "unsigned_long",
            "half_float",
            "float",
            "double",
            "scaled_float",
        }
    )

    @override
    def desc(self) -> str:
        return """generic json file processor"""

    def display_name(self) -> str:
        return "json"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def regex(self) -> str:
        """regex to identify this format"""
        return r'^\s*(\{[\s\n\r]*"[^"]+"\s*:|^\s*\[\s*(\{[\s\n\r]*"[^"]+"\s*:|\d|"|\[|null|true|false))'

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="mode",
                type="str",
                desc="'line' for jsonline, 'list' for a list of JSON objects, 'dict' for a JSON object with a key containing the list of objects",
                default_value="line",
            ),
            GulpPluginCustomParameter(
                name="keys",
                type="list",
                desc="if set and 'mode' is 'dict', only include the keys in the list",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="unmapped_as_is",
                type="bool",
                desc="if set, keep fields absent from the selected mapping under their original names instead of gulp.unmapped",
                default_value=False,
            ),
        ]

    def _iter_preserved_fields(self, record: dict[str, Any]) -> Iterator[tuple[str, Any]]:
        """Walk JSON fields while keeping arrays and indexed geo points intact."""
        mapped_fields = self.selected_mapping().fields
        index_types = self._index_type_mapping

        def walk(value: Any, path: str) -> Iterator[tuple[str, Any]]:
            if isinstance(value, dict):
                if path and index_types.get(path) == "geo_point":
                    yield path, value
                    # Explicit mappings of latitude/longitude must still run.
                    for key, child in value.items():
                        child_path = f"{path}.{key}"
                        if child_path in mapped_fields:
                            yield child_path, child
                    return

                for key, child in value.items():
                    child_path = f"{path}.{key}" if path else key
                    yield from walk(child, child_path)
            elif path:
                yield path, value

        yield from walk(record, "")

    @staticmethod
    def _is_numeric_value(value: Any) -> bool:
        """Recognize a finite JSON number or numeric string."""
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        if isinstance(value, float):
            return math.isfinite(value)
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError, OverflowError):
            return False

    def _unmapped_numeric_conflict(self, field: str, value: Any) -> bool:
        """Detect numeric type conflicts, including fields first seen in this request."""
        index_type = self._index_type_mapping.get(field)
        if index_type is not None and index_type not in self.NUMERIC_INDEX_TYPES:
            return False

        values = value if isinstance(value, list) else (value,)
        non_null_values = (item for item in values if item is not None)
        if index_type in self.NUMERIC_INDEX_TYPES:
            return any(not self._is_numeric_value(item) for item in non_null_values)

        inferred_types = getattr(self, "_json_inferred_numeric_types", None)
        if inferred_types is None:
            inferred_types = self._json_inferred_numeric_types = {}
        if field in inferred_types:
            # False means the first value was text, so dynamic mapping is not numeric.
            return inferred_types[field] and any(
                not self._is_numeric_value(item) for item in non_null_values
            )

        first_value = next(non_null_values, None)
        if first_value is None:
            return False
        if not self._is_numeric_value(first_value):
            inferred_types[field] = False
            return False
        if any(not self._is_numeric_value(item) for item in non_null_values):
            return True
        inferred_types[field] = True
        return False

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        line: str = kwargs.get("__line__")
        preserve_unmapped = (
            self._plugin_params.custom_parameters.get("unmapped_as_is") is True
            and isinstance(record, dict)
        )
        if preserve_unmapped:
            fields = self._iter_preserved_fields(record)
            mapped_fields = self.selected_mapping().fields
        else:
            # Preserve the existing traversal for every other JSON ingestion.
            fields = muty.dict.flatten(record).items()

        # map
        final: dict = {}
        # print("processing record %d:\n%s" % (record_idx, orjson.dumps(record, option=orjson.OPT_INDENT_2).decode()))
        for k, v in fields:
            mapped = await self._process_key(k, v, final, **kwargs)
            if (
                preserve_unmapped
                and k not in mapped_fields
                and k in mapped
                and self._unmapped_numeric_conflict(k, v)
            ):
                # The index cannot change an existing numeric field's type.
                # Keep the original value in its flat_object fallback instead.
                mapped = self.build_or_update_unmapped_obj(final, k, v)
            final.update(mapped)

        # Glug.get_instance().debug("final mapped record", detail=final)
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=line,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **final,
        )

    async def _ingest_jsonline(
        self,
        file_path: str,
        stats: GulpRequestStats,
        encoding: str = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        doc_idx: int = 0
        # one record per line:
        # {"a": "b"}\n
        # {"b": "c"}\n
        async for line in muty.file.iter_text_file_lines(
            file_path, encoding=encoding or "utf-8", newline=None
        ):
            parsed = orjson.loads(line)

            if not await self.process_record(
                parsed,
                doc_idx,
                flt=flt,
                __line__=line,
            ):
                # stop processing (preview mode)
                return None
            doc_idx += 1
        return stats.status

    def _read_file(
        self,
        q: queue.Queue[Any | None],
        stop_event: threading.Event,
        file_path: str,
        encoding: str,
        keys: list[str] | None = None,
    ) -> None:
        """
        Read JSON records into a bounded queue without dropping records.

        A full queue blocks this producer until the consumer makes room or stops.

        Args:
            q (queue.Queue): the bounded queue to put the JSON objects in
            stop_event (threading.Event): set when the consumer stops early
            file_path (str): the path to the file to read
            encoding (str): the encoding to use to read the file
            keys (list[str]): if provided, only include the keys in the list
        """
        def _put(record: Any | None) -> bool:
            while not stop_event.is_set():
                try:
                    q.put(record, timeout=0.1)
                    return True
                except queue.Full:
                    continue
            return False

        f = None
        try:
            # read the file and put each JSON object in the queue
            f = open(file_path, mode="r", encoding=encoding)
            data = json_stream.load(f)
            if isinstance(data, json_stream.base.TransientStreamingJSONList):
                # if the data is a list, put each item in the queue
                for d in data:
                    if not _put(json_stream.to_standard_types(d)):
                        return
            elif isinstance(data, json_stream.base.TransientStreamingJSONObject):
                for k, v in data.items():
                    if keys and k not in keys:
                        # skip keys not in the list
                        continue

                    # if the value is a list, put each item in the queue
                    if isinstance(v, json_stream.base.TransientStreamingJSONList):
                        for vv in v:
                            if not _put(json_stream.to_standard_types(vv)):
                                return
                    elif isinstance(v, json_stream.base.TransientStreamingJSONObject):
                        # otherwise, put the value as a single item
                        if not _put(json_stream.to_standard_types(v)):
                            return

        except Exception:
            # close file
            Glug.get_instance().exception("error reading json stream")
        finally:
            if f:
                f.close()
            _put(None)

            Glug.get_instance().debug(f"file {file_path} finished stream reading")

    async def _ingest_jsonlist_and_jsondict(
        self,
        file_path: str,
        stats: GulpRequestStats,
        encoding: str = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:

        # one record per object:
        # [{"a": "b"}, {"b": "c"}]
        # or multiple dicts/lists in a dict:
        # {"key": [{"a": "b"}, {"b": "c"}], "other_key": {"c": "d"}, "another_key": [{"e": "f"}]}
        doc_idx: int = 0
        keys: list[str] = self._plugin_params.custom_parameters.get("keys")

        # offload processing to a thread which fills the queue
        q: queue.Queue[Any | None] = queue.Queue(
            maxsize=self.JSON_QUEUE_MAX_SIZE
        )
        stop_event = threading.Event()
        loop = asyncio.get_running_loop()
        Glug.get_instance().debug(f"starting thread to read file {file_path}")
        fut: asyncio.Future = loop.run_in_executor(
            GulpProcess.get_instance().thread_pool,
            self._read_file,
            q,
            stop_event,
            file_path,
            encoding,
            keys,
        )
        Glug.get_instance().debug(f"starting processing file {file_path}")

        # and process it asynchronously until the queue is empty

        try:
            while True:
                try:
                    record = await loop.run_in_executor(None, q.get, True, 0.1)
                except queue.Empty:
                    continue
                if record is None:
                    Glug.get_instance().debug(
                        f"end of file {file_path} reached, breaking loop"
                    )
                    break
                if not await self.process_record(
                    record,
                    doc_idx,
                    flt=flt,
                    __line__=str(record),
                ):
                    return None

                doc_idx += 1
        finally:
            stop_event.set()
            await fut

        return stats.status

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
            **kwargs,
        )

        mapping: GulpMapping = self.selected_mapping()
        if self._plugin_params.custom_parameters.get("unmapped_as_is") is True:
            # Override only this request's loaded mapping; an unset/false value
            # leaves the mapping file's existing behavior intact.
            mapping.unmapped_as_is = True
            # Track fields the index may map dynamically after the initial lookup.
            self._json_inferred_numeric_types: dict[str, bool] = {}
        encoding = mapping.default_encoding or "utf-8"
        # Glug.get_instance().debug(f"selected mapping '{mapping}' with default encoding '{encoding}'")
        mode: str = self._plugin_params.custom_parameters.get("mode")
        if mode == "line":
            if not await self._ingest_jsonline(
                file_path, encoding=encoding, flt=flt, stats=stats
            ):
                # stop processing (preview mode)
                return stats.status
        elif mode == "list" or mode == "dict":
            if not await self._ingest_jsonlist_and_jsondict(
                file_path, encoding=encoding, flt=flt, stats=stats
            ):
                # stop processing (preview mode)
                return stats.status
        else:
            Glug.get_instance().error(
                f"invalid mode '{mode}' for json plugin, should be 'line', 'list' or 'dict'"
            )
            raise ValueError(
                "invalid mode '%s' for json plugin, should be 'line', 'list' or 'dict'"
                % (mode)
            )
