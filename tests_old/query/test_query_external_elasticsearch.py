#!/usr/bin/env python3
import asyncio
import json

import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.structs import GulpQueryParameters
from gulp_client.common import _ensure_test_operation
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket
from gulp.structs import GulpMappingParameters, GulpPluginParameters


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_elasticsearch():
    async def _test_elastic_external(token: str):
        _, host = TEST_HOST.split("://")
        ws_url = f"ws://{host}/ws"
        test_completed = False

        async with websockets.connect(ws_url) as ws:
            # connect websocket
            p: GulpWsAuthPacket = GulpWsAuthPacket(token=token, ws_id=TEST_WS_ID)
            await ws.send(p.model_dump_json(exclude_none=True))

            # receive responses
            try:
                while True:
                    response = await ws.recv()
                    data = json.loads(response)

                    if data["type"] == "ws_connected":
                        # run test
                        q_options = GulpQueryParameters()
                        q_options.name = "test_external_elasticsearch"
                        plugin_params = GulpPluginParameters(
                            custom_parameters={
                                "uri": "http://localhost:9200",
                                "username": "admin",
                                "password": "Gulp1234!",
                                "index": TEST_INDEX,
                                "is_elasticsearch": False,  # we are querying gulp's opensearch
                                "context_field": "gulp.context_id",
                                "source_field": "gulp.source_id",
                            },
                        )

                        # 1 hits
                        from tests.query.test_query_api import TEST_QUERY_RAW

                        await GulpAPIQuery.query_external(
                            token,
                            TEST_OPERATION_ID,
                            q=json.dumps(TEST_QUERY_RAW),
                            plugin="query_elasticsearch",
                            plugin_params=plugin_params,
                            q_options=q_options,
                            ws_id=TEST_WS_ID,
                            req_id="test_query_ext",
                        )
                    elif data["type"] == "query_done":
                        # query done
                        q_done_packet: GulpQueryDonePacket = (
                            GulpQueryDonePacket.model_validate(data["payload"])
                        )
                        MutyLogger.get_instance().debug(
                            "query done, packet=%s", q_done_packet
                        )
                        if q_done_packet.q_name == "test_external_elasticsearch":
                            assert q_done_packet.total_hits == 1
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.q_name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as ex:
                MutyLogger.get_instance().exception(ex)

        assert test_completed
        MutyLogger.get_instance().info(_test_elastic_external.__name__ + " succeeded!")

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()

    # TODO: better test, this uses gulp's opensearch .... should work, but better to be sure
    # login
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token
    await _test_elastic_external(token=ingest_token)
