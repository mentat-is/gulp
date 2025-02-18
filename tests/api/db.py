from muty.log import MutyLogger

from gulp.api.opensearch.filters import GulpQueryFilter
from tests.api.common import GulpAPICommon
from tests.api.user import GulpAPIUser


class GulpAPIDb:
    """
    bindings to call gulp's db related API endpoints
    """

    @staticmethod
    async def opensearch_list_index(
        token: str,
        req_id: str = None,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "GET",
            "opensearch_list_index",
            params={
                "req_id": req_id or api_common.req_id,
            },
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def opensearch_delete_index(
        token: str,
        index: str,
        delete_operation: bool = True,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
            "delete_operation": delete_operation,
            "req_id": req_id or api_common.req_id,
        }

        res = await api_common.make_request(
            "DELETE",
            "opensearch_delete_index",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def opensearch_rebase_index(
        token: str,
        operation_id: str,
        dest_index: str,
        offset_msec: int,
        flt: GulpQueryFilter = None,
        rebase_script: str = None,
        req_id: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "dest_index": dest_index,
            "offset_msec": offset_msec,
            "ws_id": api_common.ws_id,
            "req_id": req_id or api_common.req_id,
        }

        body = {}
        if flt:
            body["flt"] = flt.model_dump(exclude_none=True)
        if rebase_script:
            body["rebase_script"] = rebase_script

        res = await api_common.make_request(
            "POST",
            "opensearch_rebase_index",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def gulp_reset(token: str, req_id: str = None) -> None:
        api_common = GulpAPICommon.get_instance()
        await api_common.make_request(
            "POST",
            "gulp_reset",
            params={"req_id": req_id or api_common.req_id},
            token=token,
        )

    @staticmethod
    async def postgres_reset_collab(
        token: str,
        full_reset: bool = False,
        restart_process: bool = False,
        req_id: str = None,
    ) -> None:
        api_common = GulpAPICommon.get_instance()
        await api_common.make_request(
            "POST",
            "postgres_reset_collab",
            params={
                "restart_processes": restart_process,
                "full_reset": full_reset,
                "req_id": req_id or api_common.req_id,
            },
            token=token,
        )

    @staticmethod
    async def reset_as_admin(req_id: str = None) -> None:
        MutyLogger.get_instance().info("Resetting gULP ...")
        token = await GulpAPIUser.login_admin()
        await GulpAPIDb.gulp_reset(token, req_id=req_id)

    @staticmethod
    async def reset_collab_as_admin(full_reset: bool=False, req_id: str = None) -> None:
        """
        NOTE: using full_reset=True means also the test operation must be recreated
        """
        MutyLogger.get_instance().info("Resetting gULP collab database ...")
        token = await GulpAPIUser.login_admin(req_id=req_id)
        await GulpAPIDb.postgres_reset_collab(token, full_reset=full_reset, restart_process=False, req_id=req_id)
