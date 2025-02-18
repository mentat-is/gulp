from typing import Any, Optional

from muty.log import MutyLogger

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from tests.api.common import GulpAPICommon


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
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "ws_id": api_common.ws_id,
            "req_id": api_common.req_id,
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
    async def query_sigma(
        token: str,
        operation_id: str,
        plugin: str,
        sigmas: list[str],
        q_options: GulpQueryParameters = None,
        flt: GulpQueryFilter = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "plugin": plugin,
            "req_id": req_id or api_common.req_id,
            "ws_id": api_common.ws_id,
        }
        body = {
            "sigmas": sigmas,
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
            "query_sigma",
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
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "req_id": req_id or api_common.req_id,
            "ws_id": api_common.ws_id,
        }
        body = {
            "q": q,
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
            "query_raw",
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
