from tests.api.common import GulpAPICommon
import io, os

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
    async def plugin_get(token: str, plugin: str, is_extension:bool = False, expected_status: int = 200) -> dict:
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
        token: str, plugin: str, is_extension:bool = False, expected_status: int = 200
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
        token: str, plugin:str|io.BufferedReader, allow_override: bool = False, is_extension:bool = False, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        close_file = False
        # if a string is passed, attempt to open the file
        if isinstance(plugin, str):
            plugin: io.BufferedReader = open(plugin, "rb") 
            close_file = True

        name = os.path.basename(plugin.name)
        params = {"filename": name, "is_extension": is_extension, "allow_override": allow_override}

        """Upload plugin"""
        res = await api_common.make_request(
            "POST",
            "plugin_upload",
            params=params,
            token=token,
            files=[(name, plugin)],
            expected_status=expected_status,
        )

        # close file if we had to open it
        if close_file:
            plugin.close()

        return res
    

    @staticmethod
    async def plugin_tags(
        token: str, plugin:str, is_extension:bool = False, expected_status: int = 200
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
    async def version(
        token: str, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        """Get gulp version"""
        res = await api_common.make_request(
            "GET",
            "version",
            token=token,
            body=None,
            expected_status=expected_status,
        )

        return res
    