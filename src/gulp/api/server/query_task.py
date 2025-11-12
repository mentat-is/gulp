from typing import Any

import muty.log
import orjson
from muty.log import MutyLogger

from gulp.api.collab.stats import GulpRequestStats, RequestStatsType
from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpQuery, GulpQueryParameters
from gulp.api.redis_api import GulpRedis
from gulp.api.server.query import run_query
from gulp.api.server.server_utils import ServerUtils


async def run_query_task(t: dict) -> None:
    """
    runs in a worker process and executes a queued query task.

    Expected task dict (same shape used by enqueue in handlers):
      {
        "task_type": "query",
        "operation_id": <str>,
        "user_id": <str>,
        "ws_id": <str>,
        "req_id": <str>,
        "params": {
            "queries": [<GulpQuery dict>],
            "q_options": <dict>,
            "index": <str|null>,
            "plugin": <str|null>,
            "plugin_params": <dict|null>
         }
      }

    This function rehydrates the payload and calls the existing `process_queries` routine.
    """
    MutyLogger.get_instance().debug("run_query_task, t=%s", t)
    try:
        params: dict = t.get("params", {})
        user_id: str = t.get("user_id")
        req_id: str = t.get("req_id")
        operation_id: str = t.get("operation_id")
        ws_id: str = t.get("ws_id")

        queries = params.get("queries", [])
        q_options = params.get("q_options", {})
        index = params.get("index")
        plugin = params.get("plugin")
        plugin_params = params.get("plugin_params")

        # rebuild pydantic models from dict payloads
        q_models: list[GulpQuery] = []
        for qq in queries:
            if isinstance(qq, dict):
                q_models.append(GulpQuery.model_validate(qq))
            elif isinstance(qq, GulpQuery):
                q_models.append(qq)
            else:
                # try to coerce
                q_models.append(GulpQuery(q=qq))

        q_options_model: GulpQueryParameters = (
            GulpQueryParameters.model_validate(q_options)
            if isinstance(q_options, dict)
            else q_options
        )

        # run each query sequentially inside this worker process
        for gq in q_models:
            try:
                await run_query(
                    user_id=user_id,
                    req_id=req_id,
                    operation_id=operation_id,
                    ws_id=ws_id,
                    gq=gq,
                    q_options=q_options_model,
                    index=index,
                    plugin=plugin,
                    plugin_params=plugin_params,
                )
            except Exception as ex:
                MutyLogger.get_instance().exception("query task inner error: %s", ex)
    except Exception as ex:
        MutyLogger.get_instance().exception("error in run_query_task: %s", ex)
