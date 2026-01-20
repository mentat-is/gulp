#!/usr/bin/env python3
import asyncio
import json
import os

import muty.file
import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger
from gulp.api.collab.stats import GulpRequestStats, GulpUpdateDocumentsStats
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp_client.common import (
    GulpAPICommon,
    _ensure_test_operation,
    _cleanup_test_operation,
)
from gulp_client.enrich import GulpAPIEnrich
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import (
    GulpQueryDonePacket,
    GulpWsAuthPacket,
)
from gulp.structs import GulpPluginParameters


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    skip_reset = os.getenv("SKIP_RESET", "0")
    if skip_reset == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


@pytest.mark.asyncio
async def test_tag_documents():

    skip_reset = os.getenv("SKIP_RESET", "0")
    if skip_reset != "1":
        # ingest some data
        from tests.ingest.test_ingest import test_win_evtx

        await test_win_evtx()

    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert edit_token

    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=edit_token, ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                payload = data.get("payload", {})

                if data["type"] == "ws_connected":
                    # run test
                    await GulpAPIEnrich.tag_documents(
                        edit_token,
                        TEST_OPERATION_ID,
                        tags=["test_tag1", "test_tag2"],
                        flt=GulpQueryFilter(
                            time_range=[1467213874345999870, 1467213874345999873]
                        ),
                        req_id="req_tag_documents",
                    )
                elif (
                    data["type"] == "stats_update"
                    and payload
                    and payload["obj"]["req_type"] == "enrich"
                ):
                    stats: GulpRequestStats = GulpRequestStats.from_dict(payload["obj"])
                    stats_data: GulpUpdateDocumentsStats = (
                        GulpUpdateDocumentsStats.model_validate(payload["obj"]["data"])
                    )
                    MutyLogger.get_instance().info("stats: %s", stats)

                    # query done
                    if stats.status == "done" and stats_data.updated == 1:
                        test_completed = True
                    else:
                        assert False, (
                            "enrich done, req_id=%s, expected updated=7 but received updated=%d",
                            stats.id,
                            stats_data.updated,
                        )
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    MutyLogger.get_instance().info(test_tag_documents.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_tag_single_id():

    skip_reset = os.getenv("SKIP_RESET", "0")
    if skip_reset != "1":
        # ingest some data
        from tests.ingest.test_ingest import test_win_evtx

        await test_win_evtx()

    doc_id: str = "4905967cfcaf2abe0e28322ff085619d"
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert edit_token

    # guest cannot enrich, verify that
    await GulpAPIEnrich.tag_single_id(
        guest_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        tags=["test_tag1", "test_tag2"],
        expected_status=401,
    )

    doc = await GulpAPIEnrich.tag_single_id(
        edit_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        tags=["test_tag1", "test_tag2"],
    )

    assert "test_tag2" in doc["gulp.tags"] and len(doc["gulp.tags"]) == 2

    # test query by tags

    MutyLogger.get_instance().info(test_tag_single_id.__name__ + " succeeded!")

