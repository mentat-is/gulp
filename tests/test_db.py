import asyncio
import json

import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.rest.client.common import _ensure_test_operation
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.operation import GulpAPIOperation
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_OPERATION_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket


async def _ws_loop(total: int = None):
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
                if data["type"] == "rebase_done":
                    # stats update
                    MutyLogger.get_instance().debug(f"rebase done received: {data}")
                    rebase_packet = data["data"]
                    if rebase_packet["status"] == "done":
                        test_completed = True
                    else:
                        raise ValueError(f"unexpected rebase status: {rebase_packet}")
                    break
                elif data["type"] == "ws_connected":
                    # ws connected
                    MutyLogger.get_instance().debug("ws connected: %s", data)

                elif data["type"] == "query_done":
                    # query done
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(data["data"])
                    )
                    if q_done_packet.total_hits == total:
                        test_completed = True
                    else:
                        raise ValueError(
                            f"unexpected total hits: {q_done_packet.total_hits}, requested_total={total}"
                        )
                    break
                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed:
            MutyLogger.get_instance().warning("WebSocket connection closed")

    assert test_completed


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_db_api():
    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    # login users
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    ingest_token = await GulpAPIUser.login("admin", "admin")
    assert ingest_token

    # clear indexes to start clean
    indexes = await GulpAPIDb.opensearch_list_index(admin_token)
    for l in indexes:
        await GulpAPIDb.opensearch_delete_index(admin_token, l["name"], delete_operation=False)

    # recreate operation
    await GulpAPIOperation.operation_delete(admin_token, TEST_OPERATION_ID)
    await GulpAPIOperation.operation_create(admin_token, TEST_OPERATION_ID, set_default_grants=True)

    # ingest some data
    await test_win_evtx()

    # get doc by id
    target_id = "50edff98db7773ef04378ec20a47f622"
    d = await GulpAPIQuery.query_single_id(guest_token, TEST_OPERATION_ID, target_id)
    assert d["_id"] == target_id
    assert d["@timestamp"] == "2016-06-29T15:24:34.346000+00:00"
    assert d["gulp.timestamp"] == 1467213874345999872

    # rebase on another index (guest cannot rebase)
    one_day_msec = 1000 * 60 * 60 * 24
    new_index = "new_idx"
    # onoy 1 document should be rebased
    flt: GulpQueryFilter = GulpQueryFilter(
        time_range=[1467213874345999870, 1467213874345999875]
    )
    await GulpAPIDb.opensearch_rebase_index(
        token=guest_token,
        operation_id=TEST_OPERATION_ID,
        dest_index=new_index,
        offset_msec=one_day_msec,
        flt=flt,
        expected_status=401,
    )

    # ingest can do it
    await GulpAPIDb.opensearch_rebase_index(
        token=ingest_token,
        operation_id=TEST_OPERATION_ID,
        dest_index=new_index,
        offset_msec=one_day_msec,
        flt=flt,
    )

    # wait rebase done
    await _ws_loop()

    # check rebase
    await GulpAPIQuery.query_gulp(guest_token, operation_id=TEST_OPERATION_ID)
    await _ws_loop(total=1)

    # check same document on new idx (should be 1 day ahead)
    doc = await GulpAPIQuery.query_single_id(guest_token, TEST_OPERATION_ID, target_id)
    assert doc["_id"] == target_id
    assert doc["@timestamp"] == "2016-06-30T15:24:34.346000000Z"
    assert doc["gulp.timestamp"] == 1467300274345999872  # 1467213874345999872 + 1 day

    # list indexes (should be 2)
    indexes = await GulpAPIDb.opensearch_list_index(admin_token)
    assert len(indexes) == 2
    for i in indexes:
        # there should be an indexes with doc_count=1 (the new index)
        assert i["name"] in [TEST_OPERATION_ID, new_index]
        if i["name"] == new_index:
            assert i["doc_count"] == 1

    # delete the new index (this will also delete the operation test_operation)
    await GulpAPIDb.opensearch_delete_index(ingest_token, new_index)

    # only the original stale index should be left
    indexes = await GulpAPIDb.opensearch_list_index(admin_token)
    assert len(indexes) == 1
    assert indexes[0]["name"] == TEST_OPERATION_ID
    assert indexes[0]["doc_count"] == 7

    # delete the old stale index (test_operation)
    await GulpAPIDb.opensearch_delete_index(admin_token, TEST_OPERATION_ID)

    MutyLogger.get_instance().info(test_db_api.__name__ + " passed")
