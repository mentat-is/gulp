from gulp.api.collab.structs import GulpCollabFilter
from tests.api.common import GulpAPICommon


class GulpAPINote:
    """
    bindings to call gulp's note related API endpoints
    """

    @staticmethod
    async def note_create(
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
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()

        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "time_pin": time_pin,
            "name": name,
            "color": color,
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "docs": docs,
            "text": text,
            "tags": tags,
        }

        res = await api_common.make_request(
            "POST",
            "note_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def note_update(
        token: str,
        object_id: str,
        text: str = None,
        time_pin: int = None,
        docs: list = None,
        name: str = None,
        tags: list[str] = None,
        color: str = None,
        req_id: str = None,
        ws_id: str = None,
        expected_status: int = 200,
    ) -> dict:

        api_common = GulpAPICommon.get_instance()
        params = {
            "object_id": object_id,
            "time_pin": time_pin,
            "color": color,
            "name": name,
            "ws_id": ws_id or api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        body = {
            "docs": docs,
            "tags": tags,
            "text": text,
        }

        res = await api_common.make_request(
            "PATCH",
            "note_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def note_delete(
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
            req_id=req_id,
            ws_id=ws_id,
            api="note_delete",
            expected_status=expected_status,
        )

    @staticmethod
    async def note_get_by_id(
        token: str,
        object_id: str,
        req_id: str = None, 
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_get_by_id(
            token=token,
            object_id=object_id,
            req_id=req_id,
            api="note_get_by_id",
            expected_status=expected_status,
        )

    @staticmethod
    async def note_list(
        token: str,
        flt: GulpCollabFilter = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        return await api_common.object_list(
            token=token,
            api="note_list",
            flt=flt,
            req_id=req_id,
            expected_status=expected_status,
        )
