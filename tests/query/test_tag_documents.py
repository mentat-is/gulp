#!/usr/bin/env python3
import asyncio
import json
import os

import muty.file
import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp_client.common import GulpAPICommon, _ensure_test_operation
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
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_tag_documents():

    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert edit_token

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()

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

                if data["type"] == "ws_connected":
                    # run test
                    await GulpAPIEnrich.tag_documents(
                        edit_token,
                        TEST_OPERATION_ID,
                        tags=["test_tag1", "test_tag2"],
                        flt=GulpQueryFilter(
                            time_range=[1467213874345999870, 1467213874345999873]
                        ),
                    )
                elif data["type"] == "enrich_done":
                    # query done
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(data["data"])
                    )
                    MutyLogger.get_instance().debug(
                        "GulpQueryDonePacket=%s" % (q_done_packet)
                    )
                    if q_done_packet.total_hits == 1:
                        test_completed = True
                    else:
                        assert False, "tagging done, but total_hits=%d" % (
                            q_done_packet.total_hits
                        )
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    MutyLogger.get_instance().info(test_tag_documents.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_tag_document_single():

    doc_id: str = "50edff98db7773ef04378ec20a47f622"
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert edit_token

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()

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

    MutyLogger.get_instance().info(test_tag_document_single.__name__ + " succeeded!")
