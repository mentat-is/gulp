from muty.log import MutyLogger

from gulp.api.rest.client.common import GulpAPICommon


class GulpAPIUserGroup:
    """
    bindings to call gulp's user group related API endpoints
    """

    @staticmethod
    async def usergroup_create(
        token: str,
        name: str,
        permission: list,
        description: str = None,
        glyph_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "name": name,
            "glyph_id": glyph_id,
            "req_id": req_id or api_common.req_id,
        }
        body = {
            "description": description,
            "permission": permission,
        }
        res = await api_common.make_request(
            "POST",
            "user_group_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def usergroup_update(
        token: str,
        group_id: str,
        permission: list = None,
        description: str = None,
        glyph_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "group_id": group_id,
            "glyph_id": glyph_id,
            "req_id": req_id or api_common.req_id,
        }
        body = {
            "description": description,
            "permission": permission,
        }
        res = await api_common.make_request(
            "PATCH",
            "user_group_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def usergroup_delete(
        token: str,
        group_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "group_id": group_id,
            "req_id": req_id or api_common.req_id,
        }
        res = await api_common.make_request(
            "DELETE",
            "user_group_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def usergroup_get_by_id(
        token: str,
        group_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "group_id": group_id,
            "req_id": req_id or api_common.req_id,
        }
        res = await api_common.make_request(
            "GET",
            "user_group_get_by_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def usergroup_list(
        token: str,
        flt: dict = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        params = {
            "req_id": req_id or api_common.req_id,
        }
        body = {
            "flt": flt,
        }
        res = await api_common.make_request(
            "POST",
            "user_group_list",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def _usergroup_add_remove_user(
        token: str,
        user_id: str,
        group_id: str,
        remove: bool,
        req_id: str = None,
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
            "req_id": req_id or api_common.req_id,
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
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIUserGroup._usergroup_add_remove_user(
            token,
            user_id,
            group_id,
            remove=False,
            expected_status=expected_status,
            req_id=req_id,
        )

    @staticmethod
    async def usergroup_remove_user(
        token: str,
        user_id: str,
        group_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIUserGroup._usergroup_add_remove_user(
            token,
            user_id,
            group_id,
            remove=True,
            expected_status=expected_status,
            req_id=req_id,
        )
