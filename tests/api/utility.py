from tests.api.common import GulpAPICommon


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
    async def plugin_get(token: str, plugin: str, expected_status: int = 200) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"plugin": plugin}

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
        token: str, plugin: str, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {"plugin": plugin}

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
    