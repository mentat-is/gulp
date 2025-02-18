from gulp.api.collab.structs import GulpCollabFilter
from tests.api.common import GulpAPICommon


class GulpAPILink:
    """
    bindings to call gulp's link related API endpoints
    """

    @staticmethod
    async def link_create(
        token: str,
        operation_id: str,
        doc_id_from: str,
        doc_ids: list,
        name: str = None,
        tags: list[str] = None,
        glyph_id: str = None,
        color: str = None,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "ws_id": ws_id or api_common.ws_id,
            "doc_id_from": doc_id_from,
            "name": name,
            "color": color,
            "glyph_id": glyph_id,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "doc_ids": doc_ids,
            "tags": tags,
        }

        res = await api_common.make_request(
            "POST",
            "link_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def link_update(
        token: str,
        object_id: str,
        doc_ids: list = None,
        name: str = None,
        tags: list[str] = None,
        glyph_id: str = None,
        color: str = None,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "object_id": object_id,
            "ws_id": ws_id or api_common.ws_id,
            "name": name,
            "color": color,
            "glyph_id": glyph_id,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "doc_ids": doc_ids,
            "tags": tags,
        }

        res = await api_common.make_request(
            "PATCH",
            "link_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def link_delete(
        token: str,
        object_id: str,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_delete(
            token=token,
            object_id=object_id,
            api="link_delete",
            req_id=req_id,
            ws_id=ws_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def link_get_by_id(
        token: str,
        object_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            api="link_get_by_id",
            req_id=req_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def link_list(
        token: str,
        flt: GulpCollabFilter = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="link_list",
            flt=flt,
            req_id=req_id,
            expected_status=expected_status,
        )
