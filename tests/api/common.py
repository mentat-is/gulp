import json
import os
from typing import Any

import requests
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter


class GulpAPICommon:
    _instance = None

    @classmethod
    def get_instance(cls):
        # get singleton
        if cls._instance is None:
            MutyLogger.get_instance("gulp_test")
            cls._instance = GulpAPICommon()

        return cls._instance

    def init(
        self,
        host: str,
        ws_id: str = None,
        req_id: str = None,
        index: str = None,
        log_request: bool = False,
        log_response: bool = False,
    ):
        """
        must be called before any other method
        """
        self.index = index
        self.ws_id = ws_id
        self.req_id = req_id
        self.host = host
        self._log_res = log_request
        self._log_req = log_response

    def _make_url(self, endpoint: str) -> str:
        return f"{self.host}/{endpoint}"

    def _log_request(self, method: str, url: str, params: dict):
        if not self._log_req:
            return
        MutyLogger.get_instance().debug(f"REQUEST {method} {url}")
        MutyLogger.get_instance().debug(
            f"REQUEST PARAMS: {json.dumps(params, indent=2)}"
        )

    def _log_response(self, r: requests.Response):
        if not self._log_res:
            return
        MutyLogger.get_instance().debug(f"RESPONSE Status: {r.status_code}")
        MutyLogger.get_instance().debug(
            f"RESPONSE Body: {json.dumps(r.json(), indent=2)}"
        )

    async def make_request(
        self,
        method: str,
        endpoint: str,
        params: dict,
        token: str = None,
        body: Any = None,
        files: dict = None,
        headers: dict = None,
        expected_status: int = 200,
    ) -> dict:
        """
        make request, verify status, return "data" member or {}
        params:
            files: dict of file objects, e.g. {'file': ('filename.txt', open('file.txt', 'rb'))}
        """
        url = self._make_url(endpoint)
        if headers:
            headers.update({"token": token})
        else:
            headers = {"token": token} if token else {}

        self._log_request(
            method,
            url,
            {"params": params, "body": body, "headers": headers, "files": files},
        )

        # handle file uploads and regular requests
        if files:
            # for file uploads, body data needs to be part of form-data
            data = body if body else None
            r = requests.request(
                method, url, headers=headers, params=params, files=files, data=data
            )
        elif method in ["POST", "PATCH", "PUT"] and body:
            r = requests.request(method, url, headers=headers, params=params, json=body)
        else:
            r = requests.request(method, url, headers=headers, params=params)

        self._log_response(r)
        assert r.status_code == expected_status

        return r.json().get("data") if r.status_code == 200 else {}

    async def object_delete(
        self,
        token: str,
        object_id: str,
        api: str,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """
        common object deletion
        """
        MutyLogger.get_instance().info(f"Deleting object {object_id}, api={api}...")
        params = {
            "object_id": object_id,
            "ws_id": req_id or self.ws_id,
            "req_id": req_id or self.req_id,
        }
        res = await self.make_request(
            "DELETE", api, params=params, token=token, expected_status=expected_status
        )
        return res

    async def object_get_by_id(
        self,
        token: str,
        object_id: str,
        api: str,
        req_id: str = None,
        expected_status: int = 200,
        **kwargs,
    ) -> dict:
        """
        common object get
        """
        MutyLogger.get_instance().info(f"Getting object {object_id}, api={api}...")
        params = {"object_id": object_id, "req_id": req_id or self.req_id, **kwargs}
        res = await self.make_request(
            "GET",
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
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        """
        common object list
        """
        MutyLogger.get_instance().info("Listing objects: api=%s ..." % (api))
        res = await self.make_request(
            "POST",
            api,
            params={"req_id": req_id or self.req_id},
            body=(
                flt.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
                if flt
                else None
            ),
            token=token,
            expected_status=expected_status,
        )
        return res
