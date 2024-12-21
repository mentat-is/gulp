from typing import Any, Optional
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryAdditionalParameters
from tests.api.common import GulpAPICommon
from muty.log import MutyLogger


class GulpAPIQuery:
    """
    bindings to call gulp's query related API endpoints
    """

    @staticmethod
    async def query_stored(
        token: str,
        index: str,
        stored_query_ids: list[str],
        q_options: GulpQueryAdditionalParameters = None,
        flt: GulpQueryFilter = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
            "req_id": req_id or api_common.req_id,
            "ws_id": api_common.ws_id,
        }
        body = {
            "stored_query_ids": stored_query_ids,
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
            "query_stored",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_sigma(
        token: str,
        index: str,
        sigmas: list[str],
        q_options: GulpQueryAdditionalParameters = None,
        flt: GulpQueryFilter = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
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
    async def query_gulp(
        token: str,
        index: str,
        flt: GulpQueryFilter = None,
        q_options: GulpQueryAdditionalParameters = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
            "req_id": req_id or api_common.req_id,
            "ws_id": api_common.ws_id,
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
    async def query_single_id(
        token: str,
        doc_id: Any,
        index: str = None,
        q_options: GulpQueryAdditionalParameters = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "req_id": req_id or api_common.req_id,
            "ws_id": api_common.ws_id,
            "doc_id": doc_id,
            "index": index,
        }
        body = {
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
            "query_single_id",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def query_raw(
        token: str,
        index: str,
        q: Any,
        flt: GulpQueryFilter = None,
        q_options: GulpQueryAdditionalParameters = None,
        expected_status: int = 200,
        req_id: str = None,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
            "req_id": req_id or api_common.req_id,
            "ws_id": api_common.ws_id,
        }
        body = {
            "q": q,
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
            "query_raw",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res
