import datetime
import hashlib
import json
import mimetypes
import zipfile
from typing import Any, override

import muty.crypto
import muty.json
import muty.time
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    this plugin demonstrates how to use another plugin to process the data, using the GulpPluginBase.load_plugin method

    this allow to stack one plugin on top of another the data is processed by calling the lower plugin directly, bypassing the engine
    """

    @override
    def desc(self) -> str:
        return """generic zip file processor"""

    def display_name(self) -> str:
        return "zip"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="password",
                type="str",
                desc="password to decrypt the zip file",
            ),
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use to decode strings",
                default_value="utf8",
            ),
            GulpPluginCustomParameter(
                name="hash_type",
                type="str",
                desc="algorithm to use to calculate hash of zip files content",
                default_value="sha1",
            ),
            GulpPluginCustomParameter(
                name="chunk size",
                type="int",
                desc="chunk size",
                default_value=2048,
            ),
            GulpPluginCustomParameter(
                name="keep_files",
                type="bool",
                desc="if True, event.original will contain the file extracted from the zip",
                default_value=False
            )
        ]

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        zip: zipfile.ZipFile = kwargs.get("zip")
        f: zipfile.ZipInfo = kwargs.get("file")
        encoding: str = kwargs.get("encoding")
        hash_type: str = kwargs.get("hash_type")
        chunk_size: int = kwargs.get("chunk_size")
        password: str = kwargs.get("password")
        d = {}
        for attr in dir(record):
            v = getattr(f, attr)
            # ignore functions and private methods while collecting attributes
            # not (attr.startswith("__") or attr.endswith("__")):
            if not callable(v) and not (attr.startswith("__")):
                if isinstance(v, bytes):
                    v = v.decode(encoding)
                d[attr] = v
            d["is_dir"] = f.is_dir()

        # TODO: get metadata for each file... (creation date, etc)
        #      could we pass this file to an enrich plugin which handles files based on known
        #      file formats to get extra information?
        event_original = ""
        content = zip.read(f, pwd=password.encode(encoding)
                           if password is not None else None)
        hash = hashlib.__dict__[hash_type]()
        with zip.open(f) as i:
            while True:
                chunk = i.read(chunk_size)
                event_original += chunk.hex()
                if not chunk:
                    break
                hash.update(chunk)
        d[hash_type] = hash.hexdigest()

        mimetype = mimetypes.guess_file_type(f.filename)
        guessed_mimetype = mimetype[0] if mimetype[0] is not None else "file/unknown"
        d["guessed_mimetype"] = guessed_mimetype
        event_code = str()

        timestamp = datetime.datetime(
            *d["date_time"], tzinfo=datetime.timezone.utc).isoformat()
        d["date_time"] = str(d["date_time"])

        # if keep_file is false, discard original files and only keep raw metadata
        if not self._plugin_params.custom_parameters.get("keep_files"):
            event_original = json.dumps(d)

        # apply mappings
        final = {}
        for k, v in muty.json.flatten_json(d).items():
            mapped = self._process_key(k, v)
            final.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_code=event_code,
            timestamp=timestamp,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(
                self._file_path),
            **final,
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
    ) -> GulpRequestStatus:
        try:

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
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        password = self._plugin_params.custom_parameters.get("password")
        encoding = self._plugin_params.custom_parameters.get("encoding")
        hash_type = self._plugin_params.custom_parameters.get("hash_type")
        chunk_size = self._plugin_params.custom_parameters.custom_parameters.custom_parameters.get(
            "chunk_size")
        doc_idx = 0
        try:
            d = {}
            with zipfile.ZipFile(file_path) as zip:
                for f in zip.filelist:
                    try:
                        await self.process_record(
                            f, doc_idx,
                            flt=flt,
                            zip=zip,
                            file=f,
                            encoding=encoding,
                            password=password,
                            hash_type=hash_type,
                            chunk_size=chunk_size
                        )
                    except (RequestCanceledError, SourceCanceledError) as expand_lists:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)

                    doc_idx += 1
        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()
