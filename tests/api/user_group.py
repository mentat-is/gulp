from tests.api.common import GulpAPICommon
from muty.log import MutyLogger


class GulpAPIUserGroup:
    """
    bindings to call gulp's user group related API endpoints
    """

    @staticmethod
    async def _usergroup_add_remove_user(
        token: str,
        user_id: str,
        group_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(
            "Adding user %s to group %s" % (user_id, group_id)
            if not remove
            else "Removing user %s from group %s" % (user_id, group_id)
        )
        api_common = GulpAPICommon.get_instance()
        if remove:
            api = "user_group_remove_user"
        else:
            api = "user_group_add_user"
        params = {
            "group_id": group_id,
            "user_id": user_id,
            "req_id": api_common.req_id,
        }
        res = await api_common.make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def usergroup_add_user(
        token: str,
        user_id: str,
        group_id: str,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIUserGroup._usergroup_add_remove_user(
            token, user_id, group_id, remove=False, expected_status=expected_status
        )

    @staticmethod
    async def usergroup_remove_user(
        token: str,
        user_id: str,
        group_id: str,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIUserGroup._usergroup_add_remove_user(
            token, user_id, group_id, remove=True, expected_status=expected_status
        )
