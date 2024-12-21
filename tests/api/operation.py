from gulp.api.collab.structs import GulpCollabFilter
from tests.api.common import GulpAPICommon


class GulpAPIOperation:
    """
    bindings to call gulp's operation related API endpoints
    """

    @staticmethod
    async def operation_create(
        token: str,
        name: str,
        index: str,
        description: str = None,
        glyph_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "name": name,
            "index": index,
            "glyph_id": glyph_id,
            "req_id": api_common.req_id,
        }
        body = description

        res = await api_common.make_request(
            "POST",
            "operation_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def operation_update(
        token: str,
        object_id: str,
        index: str = None,
        description: str = None,
        glyph_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "object_id": object_id,
            "index": index,
            "glyph_id": glyph_id,
            "req_id": api_common.req_id,
        }
        body = description

        res = await api_common.make_request(
            "PATCH",
            "operation_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def operation_delete(
        token: str,
        object_id: str,
        delete_data: bool = True,
        index: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "object_id": object_id,
            "delete_data": delete_data,
            "index": index,
            "req_id": api_common.req_id,
        }

        return await api_common.make_request(
            "DELETE",
            "operation_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    @staticmethod
    async def operation_get_by_id(
        token: str,
        object_id: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            api="operation_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def operation_list(
        token: str,
        flt: GulpCollabFilter = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="operation_list",
            flt=flt,
            expected_status=expected_status,
        )
