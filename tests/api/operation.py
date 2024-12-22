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
        operation_id: str,
        index: str = None,
        description: str = None,
        glyph_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
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
        operation_id: str,
        delete_data: bool = True,
        index: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
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
        operation_id: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=operation_id,
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

    @staticmethod
    async def context_list(
        token: str,
        operation_id: str,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "req_id": api_common.req_id,
        }
        res = await api_common.make_request(
            "GET",
            "context_list",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def context_delete(
        token: str,
        operation_id: str,
        context_id: str,
        delete_data: bool = True,
        index: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "delete_data": delete_data,
            "index": index,
            "req_id": api_common.req_id,
        }
        return await api_common.make_request(
            "DELETE",
            "context_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    @staticmethod
    async def source_list(
        token: str,
        operation_id: str,
        context_id: str,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "req_id": api_common.req_id,
        }
        return await api_common.make_request(
            "GET",
            "source_list",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    @staticmethod
    async def source_delete(
        token: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        delete_data: bool = True,
        index: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "delete_data": delete_data,
            "index": index,
            "req_id": api_common.req_id,
        }
        return await api_common.make_request(
            "DELETE",
            "source_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )
