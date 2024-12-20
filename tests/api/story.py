from gulp.api.collab.structs import GulpCollabFilter
from tests.api.common import GulpAPICommon


class GulpAPIStory:
    """
    bindings to call gulp's story related API endpoints
    """

    @staticmethod
    async def story_create(
        token: str,
        operation_id: str,
        doc_ids: list[str],
        name: str = None,
        tags: list[str] = None,
        glyph_id: str = None,
        color: str = None,
        private: bool = False,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "name": name,
            "color": color,
            "private": private,
            "glyph_id": glyph_id,
            "ws_id": api_common.ws_id,
            "req_id": api_common.req_id,
        }

        body = {
            "doc_ids": doc_ids,
            "tags": tags,
        }

        res = await api_common.make_request(
            "POST",
            "story_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def story_update(
        token: str,
        object_id: str,
        doc_ids: list[str] = None,
        name: str = None,
        tags: list[str] = None,
        glyph_id: str = None,
        color: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "object_id": object_id,
            "color": color,
            "name": name,
            "glyph_id": glyph_id,
            "ws_id": api_common.ws_id,
            "req_id": api_common.req_id,
        }

        body = {
            "doc_ids": doc_ids,
            "tags": tags,
        }

        res = await api_common.make_request(
            "PATCH",
            "story_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def story_delete(
        token: str,
        object_id: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_delete(
            token=token,
            object_id=object_id,
            api="story_delete",
            expected_status=expected_status,
        )

    @staticmethod
    async def story_get_by_id(
        token: str,
        object_id: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            api="story_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def story_list(
        token: str,
        flt: GulpCollabFilter = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="story_list",
            flt=flt,
            expected_status=expected_status,
        )
