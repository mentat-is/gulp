import asyncio
import json

import pytest
import pytest_asyncio
import websockets
import os
from muty.log import MutyLogger

from gulp.api.collab.stats import GulpRequestStats, GulpUpdateDocumentsStats
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp_client.common import (
    _ensure_test_operation,
    GulpAPICommon,
    _cleanup_test_operation,
)
from gulp_client.db import GulpAPIDb
from gulp_client.operation import GulpAPIOperation
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_HOST,
    TEST_OPERATION_ID,
    TEST_WS_ID,
    TEST_REQ_ID,
    TEST_INDEX,
)
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    if os.getenv("SKIP_RESET") == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


async def _ws_loop_rebase_by_query(total: int = None):
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False
    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token="monitor", ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                payload = data.get("payload", {})

                if data["type"] == "ws_connected":
                    # ws connected
                    MutyLogger.get_instance().debug("ws connected: %s", data)
                elif (
                    payload
                    and data["type"] == "stats_update"
                    and payload["obj"]["req_type"] == "rebase"
                ):
                    # stats update
                    stats: GulpRequestStats = GulpRequestStats.from_dict(payload["obj"])
                    stats_data: GulpUpdateDocumentsStats = (
                        GulpUpdateDocumentsStats.model_validate(payload["obj"]["data"])
                    )
                    MutyLogger.get_instance().info("stats: %s", stats)
                elif data["type"] == "rebase_done":
                    test_completed = True
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed:
            MutyLogger.get_instance().warning("WebSocket connection closed")

    assert test_completed


@pytest.mark.asyncio
async def test_db_api():
    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    # login users
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # clear indexes to start clean
    indexes = await GulpAPIDb.opensearch_list_index(admin_token)
    for l in indexes:
        await GulpAPIDb.opensearch_delete_index(
            admin_token, l["name"], delete_operation=False
        )

    # recreate operation
    await GulpAPIOperation.operation_delete(admin_token, TEST_OPERATION_ID)
    await GulpAPIOperation.operation_create(
        admin_token, TEST_OPERATION_ID, set_default_grants=True
    )

    MutyLogger.get_instance().info(test_db_api.__name__ + " passed")


@pytest.mark.asyncio
async def test_rebase_by_query():
    skip_reset = os.getenv("SKIP_RESET") == "1"
    if not skip_reset:
        # ingest some data
        from tests.ingest.test_ingest import test_win_evtx  # test_win_evtx_multiple

        # ingest some data
        await test_win_evtx()  # test_win_evtx_multiple()

    # login users
    ingest_token = await GulpAPIUser.login("admin", "admin")
    assert ingest_token

    # get doc by id
    source_id = "64e7c3a4013ae243aa13151b5449aac884e36081"
    doc_id = "4905967cfcaf2abe0e28322ff085619d"
    d = await GulpAPIQuery.query_single_id(ingest_token, TEST_OPERATION_ID, doc_id)
    assert d["_id"] == doc_id
    assert d["@timestamp"] == "2016-06-29T15:24:34.346000+00:00"
    assert d["gulp.timestamp"] == 1467213874345999872

    one_day_msec = 1000 * 60 * 60 * 24
    await GulpAPIDb.opensearch_rebase_by_query(
        token=ingest_token,
        operation_id=TEST_OPERATION_ID,
        offset_msec=one_day_msec,
        ws_id=TEST_WS_ID,
        flt=GulpQueryFilter(operation_ids=[TEST_OPERATION_ID], source_ids=[source_id]),
    )
    await _ws_loop_rebase_by_query()

    # check same document again (should be 1 day ahead)
    doc = await GulpAPIQuery.query_single_id(ingest_token, TEST_OPERATION_ID, doc_id)
    assert doc["_id"] == doc_id
    assert doc["@timestamp"] == "2016-06-30T15:24:34.346000000Z"
    assert doc["gulp.timestamp"] == 1467300274345999872  # 1467213874345999872 + 1 day
    MutyLogger.get_instance().info(test_rebase_by_query.__name__ + " passed")
