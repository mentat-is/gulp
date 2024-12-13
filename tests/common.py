from typing import Optional
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from muty.log import MutyLogger
import requests
import json

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryAdditionalParameters


class GulpAPICommon:
    def __init__(
        self,
        host: str,
        ws_id: str = None,
        req_id: str = None,
        index: str = None,
        query_file: str = None,
    ):
        self._index = index
        self._ws_id = ws_id
        self._req_id = req_id
        self._host = host
        self._query_file = query_file
        self._log_req = False
        self._log_res = False
        MutyLogger.get_instance("gulp_test")

    def _make_url(self, endpoint: str) -> str:
        return f"{self._host}/{endpoint}"

    def _log_request(self, method: str, url: str, params: dict):
        MutyLogger.get_instance().debug(f"REQUEST {method} {url}")
        MutyLogger.get_instance().debug(
            f"REQUEST PARAMS: {json.dumps(params, indent=2)}"
        )

    def _log_response(self, r: requests.Response):
        MutyLogger.get_instance().debug(f"RESPONSE Status: {r.status_code}")
        MutyLogger.get_instance().debug(
            f"RESPONSE Body: {json.dumps(r.json(), indent=2)}"
        )

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict,
        token: str = None,
        body: dict = None,
        expected_status: int = 200,
    ) -> dict:
        """
        make request, verify status, return "data" member or None
        """
        url = self._make_url(endpoint)
        headers = {"token": token} if token else None

        self._log_request(
            method, url, {"params": params, "body": body, "headers": headers}
        )

        if method in ["POST", "PATCH", "PUT"] and body:
            r = requests.request(method, url, headers=headers, params=params, json=body)
        else:
            r = requests.request(method, url, headers=headers, params=params)

        self._log_response(r)
        assert r.status_code == expected_status

        return r.json().get("data") if r.status_code == 200 else {}

    async def reset_gulp(self) -> None:
        MutyLogger.get_instance().info("Resetting gULP...")

        # logging in as admin
        params = {
            "user_id": "admin",
            "password": "admin",
            "ws_id": self._ws_id,
            "req_id": self._req_id,
        }
        res = await self._make_request("PUT", "login", params=params)
        token = res.get("token")
        assert token

        # reset
        await self._make_request(
            "POST",
            "gulp_:reset",
            params={"restart_processes": False, "index": self._index},
            token=token,
        )

    async def reset_gulp_collab(self) -> None:
        MutyLogger.get_instance().info("Resetting gULP collab database ...")

        # logging in as admin
        params = {
            "user_id": "admin",
            "password": "admin",
            "ws_id": self._ws_id,
            "req_id": self._req_id,
        }
        result = await self._make_request("PUT", "login", params=params)
        token = result.get("token")
        assert token

        # reset
        await self._make_request(
            "POST",
            "postgres_init",
            params={"restart_processes": False},
            token=token,
        )

    async def logout(self, token: str) -> str:
        MutyLogger.get_instance().info(f"Logging out token {token}...")
        """
        Returns:

        the logged out token
        """
        params = {"ws_id": self._ws_id, "req_id": self._req_id}

        res = await self._make_request("PUT", "logout", params=params, token=token)
        t = res.get("token")
        assert t
        return t

    async def login(self, username: str, password: str) -> str:
        """
        Returns:

        the logged in token
        """
        MutyLogger.get_instance().info(f"Logging in as {username}...")
        params = {
            "user_id": username,
            "password": password,
            "ws_id": self._ws_id,
            "req_id": self._req_id,
        }
        res = await self._make_request("PUT", "login", params=params)
        token = res.get("token")
        assert token
        return token

    async def query_sigma(
        self,
        token: str,
        index: str,
        sigmas: list[str],
        q_options: GulpQueryAdditionalParameters = None,
        flt: GulpQueryFilter = None,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(
            "Querying sigma %s, plugin=%s, index=%s..." % (sigmas, q_options.external_parameters.plugin, index)
        )
        params = {
            "index": index,
            "req_id": self._req_id,
            "ws_id": self._ws_id,
        }
        body = {
            "sigmas": sigmas,
            "flt": (
                flt.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
                if flt
                else None
            ),
            "q_options": (
                q_options.model_dump(
                    by_alias=True, exclude_none=True, exclude_defaults=True
                )
                if q_options
                else None
            ),
        }

        res = await self._make_request(
            "POST",
            "query_sigma",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def object_delete(
        self,
        token: str,
        object_id: str,
        api: str,
        expected_status: int = 200,
    ) -> str:
        """
        Returns:

        the deleted id
        """
        MutyLogger.get_instance().info(f"Deleting object {object_id}, api={api}...")
        params = {"object_id": object_id, "ws_id": self._ws_id, "req_id": self._req_id}
        res = await self._make_request(
            "DELETE", api, params=params, token=token, expected_status=expected_status
        )
        id = res.get("id")
        return id

    async def object_get(
        self, token: str, object_id: str, api: str, expected_status: int = 200
    ) -> dict:
        MutyLogger.get_instance().info(f"Getting object {object_id}, api={api}...")
        params = {"object_id": object_id, "req_id": self._req_id}
        res = await self._make_request(
            "GET",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def object_make_public_or_private(
        self,
        token: str,
        object_id: str,
        object_type: GulpCollabType,
        private: bool,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(
            "Making object %s private: %r" % (object_id, private)
        )
        if private:
            api = "object_make_private"
        else:
            api = "object_make_public"
        params = {
            "object_id": object_id,
            "type": object_type.value,
            "req_id": self._req_id,
        }
        res = await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def object_add_remove_user_grant(
        self,
        token: str,
        object_id: str,
        object_type: GulpCollabType,
        user_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(
            "Adding grant on object %s to user %s" % (object_id, user_id)
        )
        if remove:
            api = "object_remove_granted_user"
        else:
            api = "object_add_granted_user"
        params = {
            "object_id": object_id,
            "type": object_type.value,
            "user_id": user_id,
            "req_id": self._req_id,
        }
        res = await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def object_add_remove_group_grant(
        self,
        token: str,
        object_id: str,
        object_type: GulpCollabType,
        group_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(
            "Adding grant on object %s to group %s" % (object_id, group_id)
        )
        if remove:
            api = "object_remove_granted_group"
        else:
            api = "object_add_granted_group"
        params = {
            "object_id": object_id,
            "type": object_type.value,
            "group_id": group_id,
            "req_id": self._req_id,
        }
        res = await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def usergroup_add_remove_user(
        self,
        token: str,
        user_id: str,
        group_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(
            "Adding user_id %s to group %s" % (user_id, group_id)
        )
        if remove:
            api = "user_group_remove_user"
        else:
            api = "user_group_add_user"
        params = {
            "group_id": group_id,
            "user_id": user_id,
            "req_id": self._req_id,
        }
        res = await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def object_list(
        self,
        token: str,
        api: str,
        flt: GulpCollabFilter = None,
        expected_status: int = 200,
    ) -> list[dict]:
        """List objects with optional filters"""
        MutyLogger.get_instance().info("Listing objects: api=%s ..." % (api))
        res = await self._make_request(
            "POST",
            api,
            params={"req_id": self._req_id},
            body=(
                flt.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
                if flt
                else None
            ),
            token=token,
            expected_status=expected_status,
        )
        return res

    async def note_create(
        self,
        token: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        text: str,
        time_pin: int = None,
        docs: list = None,
        name: str = None,
        tags: list[str] = None,
        color: str = None,
        private: bool = False,
        expected_status: int = 200,
    ) -> dict:
        """Create a new note"""
        MutyLogger.get_instance().info("Creating note...")
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "time_pin": time_pin,
            "name": name,
            "color": color,
            "private": private,
            "ws_id": self._ws_id,
            "req_id": self._req_id,
        }

        body = {
            "docs": docs,
            "text": text,
            "tags": tags,
        }

        res = await self._make_request(
            "POST",
            "note_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def note_update(
        self,
        token: str,
        note_id: str,
        text: str = None,
        time_pin: int = None,
        docs: list = None,
        name: str = None,
        tags: list[str] = None,
        color: str = None,
        private: bool = None,
        expected_status: int = 200,
    ) -> dict:
        """Update an existing note"""
        MutyLogger.get_instance().info(f"Updating note {note_id}...")
        params = {
            "object_id": note_id,
            "time_pin": time_pin,
            "color": color,
            "private": private,
            "name": name,
            "ws_id": self._ws_id,
            "req_id": self._req_id,
        }

        body = {
            "docs": docs,
            "tags": tags,
            "text": text,
        }

        res = await self._make_request(
            "PATCH",
            "note_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def user_get(
        self, token: str, username: str, expected_status: int = 200
    ) -> dict:
        MutyLogger.get_instance().info(f"Getting user {username}...")
        params = {"user_id": username}
        res = await self._make_request(
            "GET",
            "user_get_by_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def user_delete(
        self, token: str, username: str, expected_status: int = 200
    ) -> str:
        """Delete user"""
        MutyLogger.get_instance().info(f"Deleting user {username}...")
        params = {"user_id": username}
        res = await self._make_request(
            "DELETE",
            "user_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        id = res.get("id")
        return id

    async def user_update(
        self,
        token: str,
        username: str,
        password: Optional[str] = None,
        permission: Optional[list[str]] = None,
        email: Optional[str] = None,
        user_data: Optional[dict] = None,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(f"Updating user {username} ...")
        body = {}
        params = {"user_id": username}
        if password:
            params["password"] = password
        if permission:
            body["permission"] = permission
        if email:
            params["email"] = email
        if user_data:
            body["user_data"] = user_data

        res = await self._make_request(
            "PATCH",
            "user_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def user_list(self, token: str, expected_status: int = 200) -> Optional[list]:
        MutyLogger.get_instance().info("Listing users...")
        """List all users"""
        res = await self._make_request(
            "GET", "user_list", params={}, token=token, expected_status=expected_status
        )
        return res

    async def user_get(
        self, token: str, username: str, expected_status: int = 200
    ) -> dict:
        MutyLogger.get_instance().info(f"Getting user {username}...")
        """Get user details"""
        params = {"user_id": username}
        res = await self._make_request(
            "GET",
            "user_get_by_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    async def user_create(
        self,
        token: str,
        username: str,
        password: str,
        permission: list[str],
        email: Optional[str] = None,
        expected_status: int = 200,
    ) -> dict:
        MutyLogger.get_instance().info(f"Creating user {username}...")

        """Create new user"""
        params = {"user_id": username, "password": password}
        body = permission
        if email:
            params["email"] = email

        res = await self._make_request(
            "POST",
            "user_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res
