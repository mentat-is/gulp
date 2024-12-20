from tests.api.common import GulpAPICommon
from muty.log import MutyLogger

from tests.api.user import GulpAPIUser


class GulpAPIDb:
    """
    bindings to call gulp's db related API endpoints
    """

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
        await GulpAPIDb.gulp_reset(token, api_common.index, False)

    @staticmethod
    async def reset_collab_as_admin() -> None:
        MutyLogger.get_instance().info("Resetting gULP collab database ...")
        token = await GulpAPIUser.login_admin()
        await GulpAPIDb.postgres_init_collab(token, restart_process=False)
