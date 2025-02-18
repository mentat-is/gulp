import io
import os

from tests.api.common import GulpAPICommon


class GulpAPIUtility:
    """
    bindings to call gulp's utility related API endpoints
    """

    @staticmethod
    async def server_restart(
        token: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        res = await api_common.make_request(
            "POST",
            "server_restart",
            params={"req_id": req_id or api_common.req_id},
            token=token,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def trigger_gc(
        token: str,
        gulp_only: bool = True,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        res = await api_common.make_request(
            "POST",
            "server_status",
            params={"gulp_only": gulp_only, "req_id": req_id or api_common.req_id},
            token=token,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def server_status(
        token: str,
        gulp_only: bool = True,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        res = await api_common.make_request(
            "GET",
            "server_status",
            params={"gulp_only": gulp_only, "req_id": req_id or api_common.req_id},
            token=token,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def request_get_by_id(
        token: str,
        object_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """Get stored query by ID"""
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            req_id=req_id,
            api="request_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def request_cancel(
        token: str,
        req_id_to_cancel: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "PATCH",
            "plugin_list",
            {
                "req_id_to_cancel": req_id_to_cancel,
                "req_id": req_id or api_common.req_id,
            },
            token=token,
            body=None,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def request_list(
        token: str,
        operation_id: str = None,
        running_only: bool = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "GET",
            "request_list",
            {
                "operation_id": operation_id,
                "running_only": running_only,
                "req_id": req_id or api_common.req_id,
            },
            token=token,
            body=None,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def plugin_list(
        token: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "GET",
            "plugin_list",
            {"req_id": req_id or api_common.req_id},
            token=token,
            body=None,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def plugin_get(
        token: str,
        plugin: str,
        is_extension: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "plugin": plugin,
            "is_extension": is_extension,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "GET",
            "plugin_get",
            params=params,
            token=token,
            body=None,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def plugin_delete(
        token: str,
        plugin: str,
        is_extension: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "plugin": plugin,
            "is_extension": is_extension,
            "req_id": req_id or api_common.req_id,
        }

        """Delete plugin"""
        res = await api_common.make_request(
            "DELETE",
            "plugin_delete",
            params=params,
            token=token,
            body=None,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def plugin_upload(
        token: str,
        plugin_path: str,
        allow_overwrite: bool = False,
        is_extension: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        filename = os.path.basename(plugin_path)
        params = {
            "filename": filename,
            "is_extension": is_extension,
            "req_id": req_id or api_common.req_id,
            "allow_overwrite": allow_overwrite,
        }

        files = {
            "plugin": (
                filename,
                open(plugin_path, "rb"),
                "application/octet-stream",
            ),
        }

        res = await api_common.make_request(
            "POST",
            "plugin_upload",
            params=params,
            token=token,
            files=files,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def version(
        token: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        """Get gulp version"""
        res = await api_common.make_request(
            "GET",
            "version",
            params={"req_id": req_id or api_common.req_id},
            token=token,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def mapping_file_list(
        token: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        res = await api_common.make_request(
            "GET",
            "mapping_file_list",
            params={"req_id": req_id or api_common.req_id},
            token=token,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def mapping_file_get(
        token: str, mapping_file: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"mapping_file": mapping_file, "req_id": req_id or api_common.req_id}

        res = await api_common.make_request(
            "GET",
            "mapping_file_get",
            token=token,
            params=params,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def mapping_file_delete(
        token: str, mapping_file: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"mapping_file": mapping_file, "req_id": req_id or api_common.req_id}
        res = await api_common.make_request(
            "DELETE",
            "mapping_file_delete",
            token=token,
            params=params,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def mapping_file_upload(
        token: str,
        mapping_file_path: str,
        allow_overwrite: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "allow_overwrite": allow_overwrite,
            req_id: req_id or api_common.req_id,
        }

        filename = os.path.basename(mapping_file_path)

        files = {
            "mapping_file": (
                filename,
                open(mapping_file_path, "rb"),
                "application/octet-stream",
            ),
        }

        res = await api_common.make_request(
            "POST",
            "mapping_file_upload",
            token=token,
            params=params,
            files=files,
            body=None,
            expected_status=expected_status,
        )

        return res
