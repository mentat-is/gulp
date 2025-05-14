from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.rest.client.common import GulpAPICommon


class GulpAPIOperation:
    """
    bindings to call gulp's operation related API endpoints
    """

    @staticmethod
    async def operation_reset(
        token: str,
        operation_id: str,
        delete_data: bool = False,
        restart_processeses: bool = True,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "delete_data": delete_data,
            "restart_processes": restart_processeses,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "POST",
            "operation_reset",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def operation_create(
        token: str,
        name: str,
        index: str = None,
        description: str = None,
        glyph_id: str = None,
        set_default_grants: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "name": name,
            "index": index,
            "glyph_id": glyph_id,
            "set_default_grants": set_default_grants,
            "req_id": req_id or api_common.req_id,
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
        operation_data: dict = None,
        merge_operation_data: bool = True,
        glyph_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "index": index,
            "glyph_id": glyph_id,
            "req_id": req_id or api_common.req_id,
            "merge_operation_data": merge_operation_data,
        }
        body = {
            "description": description,
            "operation_data": operation_data,
        }

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
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "delete_data": delete_data,
            "req_id": req_id or api_common.req_id,
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
        get_count: bool = True,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            obj_id=operation_id,
            api="operation_get_by_id",
            req_id=req_id,
            expected_status=expected_status,
            operation_id=operation_id,
            get_count=get_count,
        )

    @staticmethod
    async def operation_list(
        token: str,
        flt: GulpCollabFilter = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="operation_list",
            req_id=req_id,
            flt=flt,
            expected_status=expected_status,
        )

    @staticmethod
    async def context_list(
        token: str,
        operation_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "req_id": req_id or api_common.req_id,
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
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "delete_data": delete_data,
            "req_id": req_id or api_common.req_id,
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
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "req_id": req_id or api_common.req_id,
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
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "delete_data": delete_data,
            "req_id": req_id or api_common.req_id,
        }
        return await api_common.make_request(
            "DELETE",
            "source_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )
