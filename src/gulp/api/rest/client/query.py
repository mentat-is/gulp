import json
import os
from typing import Any, Optional

import muty.crypto
import muty.file
from muty.log import MutyLogger

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.rest.client.common import GulpAPICommon
from gulp.structs import GulpMappingParameters, GulpPluginParameters


class GulpAPIQuery:
    """
    bindings to call gulp's query related API endpoints
    """

    @staticmethod
    async def query_fields_by_source(
        token: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "GET",
            "query_fields_by_source",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_gulp(
        token: str,
        operation_id: str,
        flt: GulpQueryFilter = None,
        q_options: GulpQueryParameters = None,
        expected_status: int = 200,
        req_id: str = None,
        ws_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "req_id": req_id or api_common.req_id,
            "ws_id": ws_id or api_common.ws_id,
        }
        body = {
            "flt": (
                flt.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
                if flt
                else None
            ),
            "q_options": (
                q_options.model_dump(
                    by_alias=True, exclude_none=True, exclude_defaults=True
                )
                if q_options
                else None
            ),
        }

        res = await api_common.make_request(
            "POST",
            "query_gulp",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_sigma_zip(
        token: str,
        zip_file_path: str,
        operation_id: str,
        mapping_parameters: GulpMappingParameters,
        q_options: Optional[GulpQueryParameters] = None,
        flt: Optional[GulpQueryFilter] = None,
        ws_id: str = None,
        req_id: str = None,
        restart_from: int = 0,
        file_sha1: str = None,
        total_file_size: int = 0,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        if not total_file_size:
            total_file_size = os.path.getsize(zip_file_path)

        if not file_sha1:
            file_sha1 = await muty.crypto.hash_sha1_file(zip_file_path)

        params = {
            "operation_id": operation_id,
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        payload = {
            "flt": flt.model_dump(exclude_none=True) if flt else {},
            "q_options": (q_options.model_dump(exclude_none=True) if q_options else {}),
            "file_sha1": file_sha1,
            "mapping_parameters": (
                mapping_parameters.model_dump(exclude_none=True)
                if mapping_parameters
                else {}
            ),
            "original_file_path": zip_file_path,
        }

        f = open(zip_file_path, "rb")
        if restart_from > 0:
            # advance to the restart offset
            f.seek(restart_from)

        files = {
            "payload": ("payload.json", json.dumps(payload), "application/json"),
            "f": (
                os.path.basename(zip_file_path),
                f,
                "application/octet-stream",
            ),
        }

        headers = {"size": str(total_file_size), "continue_offset": str(restart_from)}

        return await api_common.make_request(
            "POST",
            "query_sigma_zip",
            params=params,
            token=token,
            files=files,
            headers=headers,
            expected_status=expected_status,
        )

    @staticmethod
    async def query_sigma(
        token: str,
        operation_id: str,
        sigmas: list[str],
        mapping_parameters: GulpPluginParameters = None,
        q_options: GulpQueryParameters = None,
        flt: GulpQueryFilter = None,
        expected_status: int = 200,
        req_id: str = None,
        ws_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "req_id": req_id or api_common.req_id,
            "ws_id": ws_id or api_common.ws_id,
        }
        body = {
            "sigmas": sigmas,
            "flt": (flt.model_dump(by_alias=True, exclude_none=True) if flt else None),
            "mapping_parameters": (
                mapping_parameters.model_dump(by_alias=True, exclude_none=True)
                if mapping_parameters
                else None
            ),
            "q_options": (
                q_options.model_dump(by_alias=True, exclude_none=True)
                if q_options
                else None
            ),
        }

        res = await api_common.make_request(
            "POST",
            "query_sigma",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def sigma_convert(
        token: str,
        sigma: str,
        mapping_parameters: GulpMappingParameters = None,
        plugin: str = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "req_id": req_id or api_common.req_id,
            "plugin": plugin,
        }
        body = {
            "sigma": sigma,
            "mapping_parameters": (
                mapping_parameters.model_dump(by_alias=True, exclude_none=True)
                if mapping_parameters
                else None
            ),
        }

        res = await api_common.make_request(
            "POST",
            "sigma_convert",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_single_id(
        token: str,
        operation_id: str,
        doc_id: str,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "req_id": req_id or api_common.req_id,
            "doc_id": doc_id,
            "operation_id": operation_id,
        }

        res = await api_common.make_request(
            "POST",
            "query_single_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_raw(
        token: str,
        operation_id: str,
        q: list[dict],
        q_options: GulpQueryParameters = None,
        expected_status: int = 200,
        req_id: str = None,
        ws_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "req_id": req_id or api_common.req_id,
            "ws_id": ws_id or api_common.ws_id,
        }
        body = {
            "q": q,
            "q_options": (
                q_options.model_dump(by_alias=True, exclude_none=True)
                if q_options
                else None
            ),
        }

        res = await api_common.make_request(
            "POST",
            "query_raw",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_external(
        token: str,
        operation_id: str,
        q: Any,
        plugin: str,
        q_options: GulpQueryParameters,
        plugin_params: GulpPluginParameters,
        ingest: bool = False,
        ws_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "ingest": ingest,
            "plugin": plugin,
            "req_id": req_id or api_common.req_id,
            "ws_id": ws_id or api_common.ws_id,
        }
        body = {
            "q": q,
            "q_options": (
                q_options.model_dump(by_alias=True, exclude_none=True)
                if q_options
                else None
            ),
            "plugin_params": (
                plugin_params.model_dump(by_alias=True, exclude_none=True)
                if plugin_params
                else None
            ),
        }

        res = await api_common.make_request(
            "POST",
            "query_external",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_external_sigma(
        token: str,
        operation_id: str,
        sigmas: list[str],
        plugin: str,
        q_options: GulpQueryParameters,
        plugin_params: GulpPluginParameters,
        ingest: bool = False,
        ws_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "ingest": ingest,
            "plugin": plugin,
            "req_id": req_id or api_common.req_id,
            "ws_id": ws_id or api_common.ws_id,
        }
        body = {
            "sigmas": sigmas,
            "q_options": (
                q_options.model_dump(by_alias=True, exclude_none=True)
                if q_options
                else None
            ),
            "plugin_params": (
                plugin_params.model_dump(by_alias=True, exclude_none=True)
            ),
        }

        res = await api_common.make_request(
            "POST",
            "query_external_sigma",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_operations(
        token: str,
        expected_status: int = 200,
        req_id: str = None,
    ) -> list[dict]:
        """
        Get operations with aggregations
        """
        api_common = GulpAPICommon.get_instance()
        params = {
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "GET",
            "query_operations",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_max_min_per_field(
        token: str,
        operation_id: str,
        group_by: str = None,
        flt: GulpQueryFilter = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        """
        Get max/min values per field with optional grouping
        """
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "group_by": group_by,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "flt": (
                flt.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
                if flt
                else None
            ),
        }

        res = await api_common.make_request(
            "POST",
            "query_max_min_per_field",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res
