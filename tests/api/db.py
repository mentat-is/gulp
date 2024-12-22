from gulp.api.opensearch.filters import GulpQueryFilter
from tests.api.common import GulpAPICommon
from muty.log import MutyLogger

from tests.api.user import GulpAPIUser


class GulpAPIDb:
    """
    bindings to call gulp's db related API endpoints
    """

    @staticmethod
    async def opensearch_list_index(
        token: str,
        expected_status: int = 200,
    ) -> list[dict]:
        api_common = GulpAPICommon.get_instance()
        res = await api_common.make_request(
            "GET",
            "opensearch_list_index",
            params={
                "ws_id": api_common.ws_id,
                "req_id": api_common.req_id,
            },
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def opensearch_get_mapping_by_src(
        token: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "ws_id": api_common.ws_id,
            "req_id": api_common.req_id,
        }

        res = await api_common.make_request(
            "GET",
            "opensearch_get_mapping_by_src",
            params=params,
            token=token,
            expected_status=expected_status,
        )
        return res

    @staticmethod
    async def opensearch_delete_index(
        token: str,
        index: str,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "index": index,
            "ws_id": api_common.ws_id,
            "req_id": api_common.req_id,
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
        index: str,
        dest_index: str,
        offset_msec: int,
        create_dest_index: bool = True,
        overwrite_dest_index: bool = True,
        flt: GulpQueryFilter = None,
        rebase_script: str = None,
        expected_status: int = 200,
    ) -> dict:
        api_common = GulpAPICommon.get_instance()
        params = {
            "operation_id": operation_id,
            "index": index,
            "dest_index": dest_index,
            "offset_msec": offset_msec,
            "create_dest_index": create_dest_index,
            "overwrite_dest_index": overwrite_dest_index,
            "ws_id": api_common.ws_id,
            "req_id": api_common.req_id,
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
    async def gulp_reset(token: str, index: str, restart_process: bool = False) -> None:
        api_common = GulpAPICommon.get_instance()
        await api_common.make_request(
            "POST",
            "gulp_reset",
            params={"restart_processes": False, "index": api_common.index},
            token=token,
        )

    @staticmethod
    async def postgres_init_collab(token: str, restart_process: bool = False) -> None:
        api_common = GulpAPICommon.get_instance()
        await api_common.make_request(
            "POST",
            "postgres_init_collab",
            params={"restart_processes": restart_process},
            token=token,
        )

    @staticmethod
    async def reset_as_admin() -> None:
        MutyLogger.get_instance().info("Resetting gULP...")
        token = await GulpAPIUser.login_admin()

        api_common = GulpAPICommon.get_instance()
        await GulpAPIDb.gulp_reset(token, api_common.index, restart_process=True)

    @staticmethod
    async def reset_collab_as_admin() -> None:
        MutyLogger.get_instance().info("Resetting gULP collab database ...")
        token = await GulpAPIUser.login_admin()
        await GulpAPIDb.postgres_init_collab(token, restart_process=False)
