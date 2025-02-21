import asyncio
import json

import pytest
import websockets
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.rest.client.common import GulpAPICommon
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.operation import GulpAPIOperation
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
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
                            f"unexpected total hits: {q_done_packet.total_hits}"
                        )
                    break
                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed:
            MutyLogger.get_instance().warning("WebSocket connection closed")

    assert test_completed


@pytest.mark.asyncio
async def test():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # ingest some data
    from tests.ingest.test_ingest import test_csv_file_mapping

    await test_csv_file_mapping()

    # login users
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    ingest_token = await GulpAPIUser.login("admin", "admin")
    assert ingest_token

    # get doc by id
    target_doc_id = "6d9398a9feedbf0ca661f0c41c3cc145"

    doc = await GulpAPIQuery.query_single_id(guest_token, target_doc_id, TEST_INDEX)
    assert doc["_id"] == target_doc_id
    assert doc["@timestamp"] == "2014-07-16T06:04:24.614482+00:00"
    assert doc["gulp.timestamp"] == 1405490664614481920

    # rebase on another index (guest cannot rebase)
    one_day_msec = 1000 * 60 * 60 * 24
    new_index = "new_idx"
    await GulpAPIDb.opensearch_rebase_index(
        guest_token,
        TEST_OPERATION_ID,
        TEST_INDEX,
        new_index,
        one_day_msec,
        flt=GulpQueryFilter(int_filter=(1290012898794248960, 1447779298794248960)),
        expected_status=401,
    )

    # ingest can do it
    await GulpAPIDb.opensearch_rebase_index(
        ingest_token,
        TEST_OPERATION_ID,
        TEST_INDEX,
        new_index,
        one_day_msec,
        flt=GulpQueryFilter(int_filter=(1290012898794248960, 1447779298794248960)),
    )

    # wait rebase done
    await _ws_loop()

    # check rebase (only 6 docs should have been rebased)
    await GulpAPIQuery.query_gulp(
        guest_token, new_index, flt=GulpQueryFilter(operation_ids=[TEST_OPERATION_ID])
    )
    await _ws_loop(total=5)

    # check same document on new idx (should be 1 day ahead)
    doc = await GulpAPIQuery.query_single_id(guest_token, target_doc_id, new_index)
    assert doc["_id"] == target_doc_id
    assert (
        doc["@timestamp"] == "2014-07-17T06:04:24.614482Z"
    )  # was "2014-07-16T06:04:24.614482+00:00"
    assert doc["gulp.timestamp"] == 1405577064614481920  # was 1405490664614481920

    # list indexes (should be 2)
    indexes = await GulpAPIDb.opensearch_list_index(guest_token)
    assert len(indexes) == 2

    # delete the new index
    await GulpAPIDb.opensearch_delete_index(ingest_token, new_index)
    MutyLogger.get_instance().info("all tests succeeded!")

    # verify deleted
    indexes = await GulpAPIDb.opensearch_list_index(guest_token)
    assert len(indexes) == 1
