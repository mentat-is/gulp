from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.opensearch.sigma import GulpQuerySigmaParameters
from tests.api.common import GulpAPICommon


class GulpAPIStoredQuery:
    """
    Bindings to call gulp's stored query related API endpoints
    """

    @staticmethod
    async def stored_query_create(
        token: str,
        name: str,
        q: str,
        q_groups: list[str] = None,
        plugin: str = None,
        tags: list[str] = None,
        description: str = None,
        glyph_id: str = None,
        private: bool = False,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """Create a new stored query"""
        api_common = GulpAPICommon.get_instance()

        params = {
            "name": name,
            "req_id": req_id or api_common.req_id,
            "plugin": plugin,
            "glyph_id": glyph_id,
            "private": private,
        }

        body = {
            "q": q,
            "q_groups": q_groups,
            "tags": tags,
            "description": description,
        }

        res = await api_common.make_request(
            "POST",
            "stored_query_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def stored_query_update(
        token: str,
        object_id: str,
        name: str = None,
        q: list[str] = None,
        q_groups: list[str] = None,
        plugin: str = None,
        tags: list[str] = None,
        description: str = None,
        glyph_id: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """Update an existing stored query"""
        api_common = GulpAPICommon.get_instance()
        params = {
            "object_id": object_id,
            "name": name,
            "plugin": plugin,
            "req_id": req_id or api_common.req_id,
            "glyph_id": glyph_id,
        }

        body = {
            "q": q,
            "q_groups": q_groups,
            "tags": tags,
            "description": description,
        }

        res = await api_common.make_request(
            "PATCH",
            "stored_query_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def stored_query_delete(
        token: str,
        object_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """Delete a stored query"""
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_delete(
            token=token,
            object_id=object_id,
            api="stored_query_delete",
            req_id=req_id,
            expected_status=expected_status,
        )

    @staticmethod
    async def stored_query_get_by_id(
        token: str,
        object_id: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        """Get stored query by ID"""
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            req_id=req_id,
            api="stored_query_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def stored_query_list(
        token: str,
        flt: GulpCollabFilter = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        """List stored queries"""
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="stored_query_list",
            flt=flt,
            req_id=req_id,
            expected_status=expected_status,
        )
