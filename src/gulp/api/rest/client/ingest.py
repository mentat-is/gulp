import json
import os
from typing import Dict, Optional

import muty.crypto
from muty.log import MutyLogger

from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.rest.client.common import GulpAPICommon
from gulp.structs import GulpPluginParameters


class GulpAPIIngest:
    """Bindings to call gulp's ingest related API endpoints"""

    @staticmethod
    async def ingest_file(
        token: str,
        file_path: str,
        operation_id: str,
        context_name: str,
        plugin: str,
        file_total: int = 1,
        flt: Optional[GulpIngestionFilter] = None,
        plugin_params: Optional[GulpPluginParameters] = None,
        ws_id: str = None,
        req_id: str = None,
        restart_from: int = 0,
        file_sha1: str = None,
        total_file_size: int = 0,
        expected_status: int = 200,
        preview_mode: bool = False,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        if not total_file_size:
            total_file_size = os.path.getsize(file_path)

        if not file_sha1:
            file_sha1 = await muty.crypto.hash_sha1_file(file_path)

        params = {
            "operation_id": operation_id,
            "context_name": context_name,
            "plugin": plugin,
            "preview_mode": preview_mode,
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
            "file_total": file_total,
        }

        payload = {
            "flt": flt.model_dump(exclude_none=True) if flt else {},
            "file_sha1": file_sha1,
            "plugin_params": (
                plugin_params.model_dump(exclude_none=True) if plugin_params else {}
            ),
            "original_file_path": file_path,
        }

        f = open(file_path, "rb")
        if restart_from > 0:
            # advance to the restart offset
            f.seek(restart_from)

        files = {
            "payload": ("payload.json", json.dumps(payload), "application/json"),
            "f": (
                os.path.basename(file_path),
                f,
                "application/octet-stream",
            ),
        }

        headers = {"size": str(total_file_size), "continue_offset": str(restart_from)}

        return await api_common.make_request(
            "POST",
            "ingest_file",
            params=params,
            token=token,
            files=files,
            headers=headers,
            expected_status=expected_status,
        )


    @staticmethod
    async def ingest_zip(
        token: str,
        file_path: str,
        operation_id: str,
        context_name: str,
        flt: GulpIngestionFilter = None,
        ws_id: str = None,
        req_id: str = None,
        restart_from: int = 0,
        file_sha1: str = None,
        total_file_size: int = 0,
        expected_status: int = 200,
    ) -> dict:
        """Ingest a ZIP archive containing files to process"""
        api_common = GulpAPICommon.get_instance()
        if not total_file_size:
            total_file_size = os.path.getsize(file_path)

        if not file_sha1:
            file_sha1 = await muty.crypto.hash_sha1_file(file_path)

        params = {
            "operation_id": operation_id,
            "context_name": context_name,
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        payload = {
            "flt": flt.model_dump(exclude_none=True) if flt else {},
            "file_sha1": file_sha1,
            "original_file_path": file_path,
        }

        f = open(file_path, "rb")
        if restart_from > 0:
            # advance to the restart offset
            f.seek(restart_from)

        files = {
            "payload": ("payload.json", json.dumps(payload), "application/json"),
            "f": (
                os.path.basename(file_path),
                f,
                "application/zip",
            ),
        }

        headers = {"size": str(total_file_size), "continue_offset": str(restart_from)}

        return await api_common.make_request(
            "POST",
            "ingest_zip",
            params=params,
            token=token,
            files=files,
            headers=headers,
            expected_status=expected_status,
        )

    @staticmethod
    async def ingest_raw(
        token: str,
        raw_data: Dict,
        operation_id: str,
        plugin: str = None,
        plugin_params: Optional[GulpPluginParameters] = None,
        flt: Optional[GulpIngestionFilter] = None,
        ws_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """Ingest raw data using the raw plugin"""
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "plugin": plugin or "raw",
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "flt": flt.model_dump(exclude_none=True) if flt else {},
            "chunk": raw_data,
            "plugin_params": (
                plugin_params.model_dump(exclude_none=True) if plugin_params else {}
            ),
        }

        return await api_common.make_request(
            "POST",
            "ingest_raw",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
