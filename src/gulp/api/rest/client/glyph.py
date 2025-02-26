import os
from io import BytesIO

import muty.file

from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.rest.client.common import GulpAPICommon


class GulpAPIGlyph:
    """
    bindings to call gulp's glyph related API endpoints
    """

    @staticmethod
    async def glyph_create(
        token: str,
        img_path: str,
        name: str = None,
        private: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        buffer = await muty.file.read_file_async(img_path)
        files = {"img": (os.path.basename(img_path), BytesIO(buffer))}
        params = {
            "name": name,
            "private": private,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "POST",
            "glyph_create",
            params=params,
            files=files,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def glyph_update(
        token: str,
        object_id: str,
        img_path: str = None,
        name: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        if img_path:
            buffer = await muty.file.read_file_async(img_path)
            files = {"img": (os.path.basename(img_path), BytesIO(buffer))}
        else:
            files = None

        params = {
            "name": name,
            "object_id": object_id,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "PATCH",
            "glyph_update",
            params=params,
            files=files,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def glyph_delete(
        token: str,
        object_id: str,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_delete(
            token=token,
            object_id=object_id,
            api="glyph_delete",
            req_id=req_id,
            ws_id=ws_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def glyph_get_by_id(
        token: str,
        object_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            api="glyph_get_by_id",
            req_id=req_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def glyph_list(
        token: str,
        flt: GulpCollabFilter = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="glyph_list",
            flt=flt,
            req_id=req_id,
            expected_status=expected_status,
        )
