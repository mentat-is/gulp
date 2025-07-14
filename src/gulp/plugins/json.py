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
import orjson
import os
from typing import Any, override

import aiofiles
import json_stream.base
import muty.dict
import muty.os
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
from gulp.process import GulpProcess
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

# pylint: disable=C0411
muty.os.check_and_install_package("json-stream", ">=2.3.3,<3.0.0")
import json_stream


class Plugin(GulpPluginBase):
    @override
    def desc(self) -> str:
        return """generic json file processor"""

    def display_name(self) -> str:
        return "json"

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use",
                default_value="utf-8",
            ),
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
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        line: str = kwargs.get("__line__")
        d: dict = muty.dict.flatten(record)

        # map
        final: dict = {}
        #print("processing record %d:\n%s" % (record_idx, orjson.dumps(d, option=orjson.OPT_INDENT_2).decode()))
        for k, v in d.items():
            mapped = await self._process_key(k, v, final, **kwargs)
            final.update(mapped)

        # MutyLogger.get_instance().debug("final mapped record:\n%s" % (orjson.dumps(final, option=orjson.OPT_INDENT_2).decode()))
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
        self, file_path: str, encoding: str = None, flt: GulpIngestionFilter = None
    ) -> GulpRequestStatus:
        doc_idx: int = 0
        try:
            # one record per line:
            # {"a": "b"}\n
            # {"b": "c"}\n
            async with aiofiles.open(file_path, mode="r", encoding=encoding) as file:
                async for line in file:
                    try:
                        parsed = orjson.loads(line)

                        await self.process_record(
                            parsed,
                            doc_idx,
                            flt=flt,
                            __line__=line,
                        )
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

    def _read_file(
        self, q: asyncio.Queue, file_path: str, encoding: str, keys: list[str] = None
    ):
        """
        runs in a thread to read the file and put each JSON object in an asyncio queue, to be spooled by the main thread.

        Args:
            q (asyncio.Queue): the queue to put the JSON objects in
            file_path (str): the path to the file to read
            encoding (str): the encoding to use to read the file
            keys (list[str]): if provided, only include the keys in the list
        """
        f = None
        try:
            # read the file and put each JSON object in the queue
            f = open(file_path, mode="r", encoding=encoding)
            data = json_stream.load(f)
            if isinstance(data, json_stream.base.TransientStreamingJSONList):
                # if the data is a list, put each item in the queue
                for d in data:
                    # put the JSON object in the queue
                    q.put_nowait(json_stream.to_standard_types(d))
            elif isinstance(data, json_stream.base.TransientStreamingJSONObject):
                for k, v in data.items():
                    if keys and k not in keys:
                        # skip keys not in the list
                        continue

                    # if the value is a list, put each item in the queue
                    if isinstance(v, json_stream.base.TransientStreamingJSONList):
                        for vv in v:
                            q.put_nowait(json_stream.to_standard_types(vv))
                    elif isinstance(v, json_stream.base.TransientStreamingJSONObject):
                        # otherwise, put the value as a single item
                        q.put_nowait(json_stream.to_standard_types(v))

        except Exception as ex:
            # close file
            MutyLogger.get_instance().exception(ex)
        finally:
            if f:
                f.close()
            q.put_nowait(None)

            MutyLogger.get_instance().debug(
                "file %s finished stream reading!" % (file_path)
            )

    async def _ingest_jsonlist_and_jsondict(
        self,
        file_path: str,
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
        q: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        MutyLogger.get_instance().debug("starting thread to read file %s" % file_path)
        fut: asyncio.Future = loop.run_in_executor(
            GulpProcess.get_instance().thread_pool,
            self._read_file,
            q,
            file_path,
            encoding,
            keys,
        )
        MutyLogger.get_instance().debug("starting processing file %s" % file_path)

        # and process it asynchronously until the queue is empty
        try:
            while True:
                record: dict = None
                record = await q.get()
                q.task_done()
                if record is None:
                    # end of file reached
                    MutyLogger.get_instance().debug(
                        "end of file %s reached, breaking loop!" % (file_path)
                    )
                    break
                try:
                    await self.process_record(
                        record,
                        doc_idx,
                        flt=flt,
                        __line__=str(record),
                    )
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                    break
                except PreviewDone:
                    # preview done, stop processing
                    break

                doc_idx += 1

            # after the loop, wait for the reader task (thread) to fully complete.
            # this also allows any exception from _read_file to be propagated here.
            MutyLogger.get_instance().debug(
                "processing loop finished for %s, awaiting reader thread completion."
                % (file_path)
            )
            await fut

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()

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
            if not plugin_params:
                plugin_params = GulpPluginParameters()

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

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        encoding: str = self._plugin_params.custom_parameters.get("encoding")
        mode: str = self._plugin_params.custom_parameters.get("mode")
        if mode == "line":
            return await self._ingest_jsonline(file_path, encoding=encoding, flt=flt)
        elif mode == "list" or mode == "dict":
            return await self._ingest_jsonlist_and_jsondict(
                file_path, encoding=encoding, flt=flt
            )

        # failed
        await self._source_failed(ValueError(f"Unsupported mode: {mode}"))
        await self._source_done(flt)
        return GulpRequestStatus.FAILED
