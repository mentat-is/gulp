from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabType
from gulp.api.rest.client.common import GulpAPICommon


class GulpAPIObjectACL:
    """
    bindings to call gulp's object acl related API endpoints
    """

    @staticmethod
    async def _object_make_public_or_private(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        private: bool,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        MutyLogger.get_instance().info(
            "Making object %s public/private, private=%r" % (obj_id, private)
        )
        if private:
            api = "object_make_private"
        else:
            api = "object_make_public"
        params = {
            "obj_id": obj_id,
            "obj_type": obj_type.value,
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
    async def object_make_public(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIObjectACL._object_make_public_or_private(
            token,
            obj_id=obj_id,
            obj_type=obj_type,
            private=False,
            req_id=req_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def object_make_private(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIObjectACL._object_make_public_or_private(
            token,
            obj_id=obj_id,
            obj_type=obj_type,
            private=True,
            req_id=req_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def _object_add_remove_granted_user(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        user_id: str,
        remove: bool,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        MutyLogger.get_instance().info(
            "Adding/removing user grant on object %s, user %s, remove=%r"
            % (obj_id, user_id, remove)
        )
        if remove:
            api = "object_remove_granted_user"
        else:
            api = "object_add_granted_user"
        params = {
            "obj_id": obj_id,
            "obj_type": obj_type.value,
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
    async def object_add_granted_user(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        user_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIObjectACL._object_add_remove_granted_user(
            token,
            obj_id=obj_id,
            obj_type=obj_type,
            user_id=user_id,
            req_id=req_id,
            remove=False,
            expected_status=expected_status,
        )

    @staticmethod
    async def object_remove_granted_user(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        user_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIObjectACL._object_add_remove_granted_user(
            token,
            obj_id=obj_id,
            obj_type=obj_type,
            user_id=user_id,
            req_id=req_id,
            remove=True,
            expected_status=expected_status,
        )

    @staticmethod
    async def _object_add_remove_granted_group(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        group_id: str,
        remove: bool,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        MutyLogger.get_instance().info(
            "Adding group grant, object %s, group %s, remove=%r"
            % (obj_id, group_id, remove)
        )
        if remove:
            api = "object_remove_granted_group"
        else:
            api = "object_add_granted_group"
        params = {
            "obj_id": obj_id,
            "obj_type": obj_type.value,
            "group_id": group_id,
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
    async def object_add_granted_group(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        group_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIObjectACL._object_add_remove_granted_group(
            token,
            obj_id=obj_id,
            obj_type=obj_type,
            group_id=group_id,
            remove=False,
            req_id=req_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def object_remove_granted_group(
        token: str,
        obj_id: str,
        obj_type: GulpCollabType,
        group_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        return await GulpAPIObjectACL._object_add_remove_granted_group(
            token,
            obj_id=obj_id,
            obj_type=obj_type,
            group_id=group_id,
            remove=True,
            req_id=req_id,
            expected_status=expected_status,
        )
