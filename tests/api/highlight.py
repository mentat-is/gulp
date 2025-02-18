from gulp.api.collab.structs import GulpCollabFilter
from tests.api.common import GulpAPICommon


class GulpAPIHighlight:
    """
    bindings to call gulp's highlight related API endpoints
    """

    @staticmethod
    async def highlight_create(
        token: str,
        operation_id: str,
        ws_id: str,
        source_id: str,
        time_range: tuple[int, int],
        name: str = None,
        tags: list[str] = None,
        glyph_id: str = None,
        color: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "ws_id": ws_id,
            "source_id": source_id,
            "name": name,
            "glyph_id": glyph_id,
            "color": color,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "time_range": time_range,
            "tags": tags,
        }

        res = await api_common.make_request(
            "POST",
            "highlight_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def highlight_update(
        token: str,
        object_id: str,
        ws_id: str,
        time_range: tuple[int, int] = None,
        name: str = None,
        tags: list[str] = None,
        glyph_id: str = None,
        color: str = None,
        private: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "object_id": object_id,
            "ws_id": ws_id,
            "name": name,
            "private": private,
            "glyph_id": glyph_id,
            "color": color,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "time_range": time_range,
            "tags": tags,
        }

        res = await api_common.make_request(
            "PATCH",
            "highlight_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def highlight_delete(
        token: str,
        object_id: str,
        ws_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "object_id": object_id,
            "ws_id": ws_id,
            "req_id": req_id or api_common.req_id,
        }
        return await api_common.make_request(
            "DELETE",
            "highlight_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    @staticmethod
    async def highlight_get_by_id(
        token: str,
        object_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            req_id=req_id,
            api="highlight_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def highlight_list(
        token: str,
        flt: GulpCollabFilter = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="highlight_list",
            flt=flt,
            req_id=req_id,
            expected_status=expected_status,
        )
