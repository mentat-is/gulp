import os
from gulp.api.collab.structs import GulpCollabFilter
from tests.api.common import GulpAPICommon
import muty.file
from io import BytesIO


class GulpAPIGlyph:
    """
    bindings to call gulp's glyph related API endpoints
    """

    @staticmethod
    async def glyph_create(
        token: str,
        img: str,
        name: str = None,
        private: bool = False,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        buffer = await muty.file.read_file_async(img)
        files = {"img": (os.path.basename(img), BytesIO(buffer))}
        params = {
            "name": name,
            "private": private,
            "req_id": api_common.req_id,
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
        img: str = None,
        name: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        if img:
            buffer = await muty.file.read_file_async(img)
            files = {"img": (os.path.basename(img), BytesIO(buffer))}
        else:
            files = None

        params = {
            "name": name,
            "object_id": object_id,
            "req_id": api_common.req_id,
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
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_delete(
            token=token,
            object_id=object_id,
            api="glyph_delete",
            expected_status=expected_status,
        )

    @staticmethod
    async def glyph_get_by_id(
        token: str,
        object_id: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            api="glyph_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def glyph_list(
        token: str,
        flt: GulpCollabFilter = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="glyph_list",
            flt=flt,
            expected_status=expected_status,
        )
