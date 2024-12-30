from tests.api.common import GulpAPICommon
import io
import os


class GulpAPIUtility:
    """
    bindings to call gulp's utility related API endpoints
    """

    @staticmethod
    async def plugin_list(token: str, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "GET",
            "plugin_list",
            {},
            token=token,
            body=None,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def plugin_get(
        token: str, plugin: str, is_extension: bool = False, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"plugin": plugin, "is_extension": is_extension}

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
        token: str, plugin: str, is_extension: bool = False, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"plugin": plugin, "is_extension": is_extension}

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
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        close_file = False
        filename = os.path.basename(plugin_path)
        params = {
            "filename": filename,
            "is_extension": is_extension,
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
    async def plugin_tags(
        token: str, plugin: str, is_extension: bool = False, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"plugin": plugin, "is_extension": is_extension}

        """Get plugin tags"""
        res = await api_common.make_request(
            "GET",
            "plugin_tags",
            params=params,
            token=token,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def version(token: str, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instance()

        """Get gulp version"""
        res = await api_common.make_request(
            "GET",
            "version",
            params={},
            token=token,
            body=None,
            expected_status=expected_status,
        )

        return res

    @staticmethod
    async def mapping_file_list(token: str, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instance()

        res = await api_common.make_request(
            "GET",
            "mapping_file_list",
            params={},
            token=token,
            body=None,
            expected_status=expected_status
        )

        return res

    @staticmethod
    async def mapping_file_get(token: str, mapping_file: str, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instnace()

        params = {"mapping_file": mapping_file}

        res = await api_common.make_request(
            "GET",
            "mapping_file_get",
            params=params,
            body=None,
            expected_status=expected_status
        )

        return res

    @staticmethod
    async def mapping_file_delete(token: str, mapping_file: str, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"mapping_file": mapping_file}

        res = await api_common.make_request(
            "GET",
            "mapping_file_delete",
            params=params,
            body=None,
            expected_status=expected_status
        )

        return res

    @staticmethod
    async def mapping_file_upload(token: str, mapping_file_path: str, allow_overwrite: bool = False, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"allow_overwrite": allow_overwrite}

        filename = os.path.basename(mapping_file_path)

        files = {
            "plugin": (
                filename,
                open(mapping_file_path, "rb"),
                "application/octet-stream",
            ),
        }

        res = await api_common.make_request(
            "POST",
            "mapping_file_upload",
            params=params,
            files=files,
            body=None,
            expected_status=expected_status
        )

        return res
