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
from gulp.api.rest.client.common import GulpAPICommon, _ensure_test_operation
from gulp.api.rest.client.enrich import GulpAPIEnrich
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
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
async def test_enrich_whois_documents():

    edit_token = await GulpAPIUser.login("editor", "editor")
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
                    await GulpAPIEnrich.enrich_documents(
                        edit_token,
                        TEST_OPERATION_ID,
                        flt=GulpQueryFilter(
                            time_range=[1467213874345999870, 1467213874345999873]
                        ),
                        plugin="enrich_whois",
                        req_id="req_enrich_whois",
                    )
                elif data["type"] == "enrich_done":
                    # query done
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(data["data"])
                    )
                    MutyLogger.get_instance().debug(
                        "GulpQueryDonePacket=%s" % (q_done_packet)
                    )
                    MutyLogger.get_instance().debug(
                        "enrich done, total_enriched=%d", q_done_packet.total_enriched
                    )
                    if q_done_packet.total_enriched == 1:
                        test_completed = True
                    else:
                        assert False, "enrich done, but total_enriched=%d" % (
                            q_done_packet.total_enriched
                        )
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    MutyLogger.get_instance().info(test_enrich_whois_single.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_enrich_whois_single():

    doc_id: str = "50edff98db7773ef04378ec20a47f622"
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

    plugin_params: GulpPluginParameters = GulpPluginParameters()
    plugin_params.custom_parameters = {"host_fields": ["source.ip"]}

    # guest cannot enrich, verify that
    await GulpAPIEnrich.enrich_single_id(
        guest_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        plugin="enrich_whois",
        expected_status=401,
        plugin_params=plugin_params
    )
    doc = await GulpAPIEnrich.enrich_single_id(
        edit_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        plugin="enrich_whois",
        plugin_params=plugin_params
    )

    assert doc.get("gulp.enrich_whois.source_ip.unified_dump") != None
    MutyLogger.get_instance().info(test_enrich_whois_single.__name__ + " succeeded!")
