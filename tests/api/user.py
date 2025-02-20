from typing import Optional

from muty.log import MutyLogger

from gulp.api.rest.test_values import TEST_OPERATION_ID
from tests.api.common import GulpAPICommon
from tests.api.operation import GulpAPIOperation


class GulpAPIUser:
    """
    bindings to call gulp's user related API endpoints
    """

    @staticmethod
    async def login_admin_and_reset_operation(req_id: str, ws_id: str = None, operation_id: str = None) -> str:
        admin_token = await GulpAPIUser.login_admin(req_id=req_id, ws_id=ws_id)
        assert admin_token
        op = operation_id or TEST_OPERATION_ID
        res = await GulpAPIOperation.operation_reset(admin_token, op, req_id=req_id)
        assert res['id'] == op
        return admin_token

        
    @staticmethod
    async def login_admin(req_id: str = None, ws_id: str = None) -> str:
        MutyLogger.get_instance().info("Logging in as admin...")
        api_common = GulpAPICommon.get_instance()
        params = {
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }
        body = {
            "user_id": "admin",
            "password": "admin",
        }
        res = await api_common.make_request("POST", "login", params=params, body=body)
        token = res.get("token")
        assert token
        return token

    @staticmethod
    async def logout(token: str, req_id: str = None, ws_id: str = None) -> str:
        """
        Returns:

        the logged out token
        """
        api_common = GulpAPICommon.get_instance()
        params = {
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "POST", "logout", params=params, token=token
        )
        t = res.get("token")
        assert t
        return t

    @staticmethod
    async def get_available_login_api_handler() -> dict:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request("GET", "get_available_login_api", params={})
        assert res
        return res

    @staticmethod
    async def login(
        user_id: str, password: str, req_id: str = None, ws_id: str = None
    ) -> str:
        api_common = GulpAPICommon.get_instance()
        params = {
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }
        body = {
            "password": password,
            "user_id": user_id,
        }
        res = await api_common.make_request("POST", "login", params=params, body=body)
        token = res.get("token")
        assert token
        return token

    @staticmethod
    async def user_get_by_id(
        token: str, user_id: str, req_id: str = None, expected_status: int = 200
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {"user_id": user_id, "req_id": req_id or api_common.req_id}
        res = await api_common.make_request(
            "GET",
            "user_get_by_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def user_delete(
        token: str, user_id: str, req_id: str = None, expected_status: int = 200
    ) -> str:
        api_common = GulpAPICommon.get_instance()
        params = {"user_id": user_id, "req_id": req_id or api_common.req_id}
        res = await api_common.make_request(
            "DELETE",
            "user_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def user_update(
        token: str,
        username: str,
        password: Optional[str] = None,
        permission: Optional[list[str]] = None,
        email: Optional[str] = None,
        user_data: Optional[dict] = None,
        merge_user_data: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        body = {}
        params = {
            "user_id": username,
            "merge_user_data": merge_user_data,
            "req_id": req_id or api_common.req_id,
        }
        if password:
            params["password"] = password
        if permission:
            body["permission"] = permission
        if email:
            params["email"] = email
        if user_data:
            body["user_data"] = user_data

        res = await api_common.make_request(
            "PATCH",
            "user_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def user_list(
        token: str, req_id: str = None, expected_status: int = 200
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "GET",
            "user_list",
            params={"req_id": req_id or api_common.req_id},
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def user_create(
        token: str,
        user_id: str,
        password: str,
        permission: list[str],
        email: Optional[str] = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "user_id": user_id,
            "password": password,
            "req_id": req_id or api_common.req_id,
        }
        body = permission
        if email:
            params["email"] = email

        res = await api_common.make_request(
            "POST",
            "user_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res
